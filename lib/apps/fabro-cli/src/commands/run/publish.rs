//! A GitHub-target run's repository work in its worker: the read credential
//! its workspaces are fetched with, and its publication when it succeeds.
//!
//! The worker resolves the server's GitHub credentials itself, as the
//! legacy worker did: the strategy and App id from the server settings the
//! server named (`FABRO_CONFIG`), the App key the server hands the worker,
//! or `GITHUB_TOKEN` from the worker's vault snapshot. From them it keeps two
//! cached token sources for the run: a read-only one for fetches inside the
//! sandbox and a `contents: write` one for pushes. Each fetch and push asks
//! its source, which reuses one installation token until it nears expiry and
//! then mints the next. Reuse matters: GitHub can reject a token minted
//! moments earlier, before it has replicated, so minting per push turns a
//! long run's pushes into repeated failures. Re-minting near expiry keeps a
//! run of any length on a live token.
//!
//! Publication runs in Fabro's required `finalize_run` hook, after the last
//! stage and before the run's terminal record:
//! the final checkpoint is pushed from inside the sandbox to the run
//! branch on GitHub, and, when the run changed files and its settings ask
//! for one, a pull request is opened and recorded. A failure fails the run
//! with `publish_failed`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use fabro_config::ServerSettingsBuilder;
use fabro_github::token_source::{InstallationTokenSource, SecretString};
use fabro_github::{GitHubAppCredentials, GitHubContext, GitHubCredentials};
use fabro_llm::credentials::{self, CredentialProvider};
use fabro_llm::lithos_catalog::Catalog;
use fabro_petri::checkpoint::Site;
use fabro_petri::hooks::{Publication, RunPublisher};
use fabro_petri::platform_records::PlatformRecords;
use fabro_petri::source::{SourceCredential, SourceCredentials};
use fabro_static::EnvVars;
use fabro_store::platform_records::{PlatformRecord, PullRequestCreatedRecord};
use fabro_types::settings::run::{PullRequestSettings, RunGithubAppSource, RunMode};
use fabro_types::settings::server::GithubIntegrationStrategy;
use fabro_types::{GitHubRepositorySlug, RunId, RunSpec, RunTarget};
use fabro_vault::Vault;
use fabro_workflow::pull_request::{self, AutoMergeOptions, OpenPullRequestRequest};
use tokio::time;
use tracing::warn;

/// How long one push to the repository may take.
const PUSH_TIMEOUT: Duration = Duration::from_mins(5);
/// Attempts at the push, all with the one token resolved for it: a freshly
/// minted token can take a moment to reach every GitHub replica.
const PUSH_ATTEMPTS: u32 = 3;
const PUSH_RETRY_DELAY: Duration = Duration::from_secs(2);

/// The server's GitHub credentials as the worker reaches them, or `None`
/// when none are configured.
pub(super) fn github_credentials(vault: &Vault) -> Result<Option<GitHubCredentials>> {
    let settings = ServerSettingsBuilder::load_default().context("loading the server settings")?;
    let github = &settings.server.integrations.github;
    match github.strategy {
        GithubIntegrationStrategy::App => {
            GitHubCredentials::from_env_with_slug(github.app_id.as_deref(), github.slug.as_deref())
                .map_err(anyhow::Error::msg)
        }
        GithubIntegrationStrategy::Token => {
            let Some(token) = vault
                .get(EnvVars::GITHUB_TOKEN)
                .map(str::trim)
                .filter(|token| !token.is_empty())
            else {
                return Ok(None);
            };
            fabro_github::validate_static_github_token(token)?;
            Ok(Some(GitHubCredentials::Pat(token.to_string())))
        }
    }
}

/// Why a run's selected GitHub App source could not be used. Each variant
/// names the selected secret names and never a secret value.
#[derive(Debug, thiserror::Error)]
pub(super) enum SelectedAppError {
    #[error(
        "the run selects GitHub App secrets `{app_id_secret}` and `{private_key_secret}`, but \
         `{missing}` is absent or empty in the server's vault"
    )]
    Missing {
        app_id_secret:      String,
        private_key_secret: String,
        missing:            String,
    },
    #[error(
        "the run selects GitHub App id secret `{app_id_secret}`, whose value is not a numeric \
         App id"
    )]
    InvalidAppId { app_id_secret: String },
    #[error(
        "the run selects GitHub App private key secret `{private_key_secret}`, whose value is \
         not a usable RSA private key: {reason}"
    )]
    InvalidKey {
        private_key_secret: String,
        reason:             String,
    },
}

/// The GitHub credentials a run authenticates with: the App its settings
/// select, resolved from the worker's vault snapshot, or the server's
/// global integration when the run selects none.
///
/// A selected source that does not resolve is an error and never falls
/// back to the server's integration: the run asked to act as one App, and
/// acting as another would publish under the wrong identity.
pub(super) fn run_github_credentials(
    spec: &RunSpec,
    vault: &Vault,
) -> std::result::Result<Option<GitHubCredentials>, RunCredentialsError> {
    match &spec.settings.run.integrations.github.app {
        Some(source) => selected_app_credentials(source, vault)
            .map(Some)
            .map_err(RunCredentialsError::Selected),
        None => github_credentials(vault).map_err(RunCredentialsError::Server),
    }
}

/// The two ways a run's credentials can fail to resolve, kept apart because
/// they are handled oppositely: a selected source refuses the run, while the
/// server's own integration failing leaves the run to proceed anonymously
/// as before.
#[derive(Debug)]
pub(super) enum RunCredentialsError {
    Selected(SelectedAppError),
    Server(anyhow::Error),
}

/// The App credentials a run selects, from the secrets its source names.
fn selected_app_credentials(
    source: &RunGithubAppSource,
    vault: &Vault,
) -> std::result::Result<GitHubCredentials, SelectedAppError> {
    let value = |name: &str| {
        vault
            .get(name)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let missing = |name: &str| SelectedAppError::Missing {
        app_id_secret:      source.app_id_secret.clone(),
        private_key_secret: source.private_key_secret.clone(),
        missing:            name.to_string(),
    };
    let app_id = value(&source.app_id_secret).ok_or_else(|| missing(&source.app_id_secret))?;
    if !app_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SelectedAppError::InvalidAppId {
            app_id_secret: source.app_id_secret.clone(),
        });
    }
    let raw_key =
        value(&source.private_key_secret).ok_or_else(|| missing(&source.private_key_secret))?;
    let invalid_key = |reason: String| SelectedAppError::InvalidKey {
        private_key_secret: source.private_key_secret.clone(),
        reason,
    };
    let private_key_pem =
        fabro_github::decode_private_key_pem(&source.private_key_secret, &raw_key)
            .map_err(invalid_key)?;
    // Signing proves the key parses now, before any forge operation, rather
    // than at the first mint inside the clone.
    fabro_github::sign_app_jwt(&app_id, &private_key_pem)
        .map_err(|err| invalid_key(format!("{err:#}")))?;
    Ok(GitHubCredentials::App(GitHubAppCredentials {
        app_id,
        private_key_pem,
        slug: None,
    }))
}

/// The run's GitHub repository, when its target names one.
fn repository(spec: &RunSpec) -> Option<GitHubRepositorySlug> {
    let Some(RunTarget::Git(target)) = spec.target.as_ref() else {
        return None;
    };
    Some(target.clone().validate().ok()?.repository().clone())
}

/// A cached token source for the run's repository with `permissions`, or
/// `None` when the run has no GitHub target or no credentials resolve.
fn token_source(
    spec: &RunSpec,
    credentials: Option<&GitHubCredentials>,
    permissions: serde_json::Value,
) -> Option<Arc<InstallationTokenSource>> {
    let repository = repository(spec)?;
    let credentials = credentials?;
    match InstallationTokenSource::for_repository(
        credentials,
        repository.owner().to_string(),
        repository.repo().to_string(),
        permissions,
    ) {
        Ok(source) => Some(source),
        Err(err) => {
            warn!(repository = %repository, error = %format!("{err:#}"), "no GitHub token source for the run's repository");
            None
        }
    }
}

/// The read-only token source the run's workspaces are fetched with, and
/// its stages' Git reads the origin with. `None` when the run has no GitHub
/// target or no credentials resolve; a public repository is then fetched
/// anonymously.
pub(super) fn read_token_source(
    spec: &RunSpec,
    credentials: Option<&GitHubCredentials>,
) -> Option<Arc<InstallationTokenSource>> {
    token_source(spec, credentials, serde_json::json!({ "contents": "read" }))
}

/// The read-only token source of a run that selects its own App, with its
/// first token already minted: minting is what proves the selected App is
/// installed on, and authorized for, the run's repository, so a selection
/// that cannot reach it is refused here rather than at the clone. `Ok(None)`
/// for a run with no GitHub target.
pub(super) async fn selected_read_token_source(
    spec: &RunSpec,
    credentials: Option<&GitHubCredentials>,
) -> std::result::Result<Option<Arc<InstallationTokenSource>>, String> {
    let Some(repository) = repository(spec) else {
        return Ok(None);
    };
    let credentials = credentials
        .ok_or_else(|| "the run selects a GitHub App but no credentials resolved".to_string())?;
    let source = InstallationTokenSource::for_repository(
        credentials,
        repository.owner().to_string(),
        repository.repo().to_string(),
        serde_json::json!({ "contents": "read" }),
    )
    .map_err(|err| {
        format!("no token source over the selected GitHub App for {repository}: {err:#}")
    })?;
    source
        .resolve()
        .await
        .map_err(|err| format!("the selected GitHub App cannot reach {repository}: {err:#}"))?;
    Ok(Some(source))
}

/// Fetch credentials over the run's read-only token source.
pub(super) fn source_credentials(
    tokens: Arc<InstallationTokenSource>,
) -> Arc<dyn SourceCredentials> {
    Arc::new(ReadCredentials(tokens))
}

/// Fetch credentials resolved from the run's read-only token source.
struct ReadCredentials(Arc<InstallationTokenSource>);

#[async_trait::async_trait]
impl SourceCredentials for ReadCredentials {
    async fn credential(&self) -> Option<SourceCredential> {
        match self.0.resolve().await {
            Ok(resolved) => basic(&resolved.token),
            Err(err) => {
                warn!(error = %format!("{err:#}"), "no read credential for the run's repository; it is fetched anonymously");
                None
            }
        }
    }
}

/// The HTTP basic credential `git` presents for an installation token or
/// personal access token.
fn basic(token: &SecretString) -> Option<SourceCredential> {
    SourceCredential::from_encoded(
        BASE64_STANDARD.encode(format!("x-access-token:{}", token.expose())),
    )
}

/// A successful GitHub-target run's publication: the run branch pushed, and
/// the pull request its settings ask for opened.
pub(super) struct GitHubPublisher {
    run_id:       RunId,
    repository:   GitHubRepositorySlug,
    /// The branch the target names: the pull request's base.
    base_branch:  String,
    goal:         String,
    /// The run's model, when its settings name one.
    model:        Option<String>,
    credentials:  Option<GitHubCredentials>,
    /// The run's `contents: write` token source; `None` without credentials.
    push_tokens:  Option<Arc<InstallationTokenSource>>,
    pull_request: Option<PullRequestSettings>,
    llm_source:   Arc<dyn CredentialProvider>,
    catalog:      Arc<Catalog>,
    records:      Arc<dyn PlatformRecords>,
    client:       fabro_client::Client,
}

impl GitHubPublisher {
    /// The publisher of a run whose target is a GitHub repository and whose
    /// run branch is pushed; `None` for a dry run or any other run.
    pub(super) fn for_run(
        run_id: RunId,
        spec: &RunSpec,
        credentials: Option<GitHubCredentials>,
        llm_source: Arc<dyn CredentialProvider>,
        catalog: Arc<Catalog>,
        records: Arc<dyn PlatformRecords>,
        client: fabro_client::Client,
    ) -> Option<Self> {
        let settings = &spec.settings.run;
        if settings.execution.mode == RunMode::DryRun
            || !settings.clone.enabled
            || !settings.run_branch.enabled
            || !settings.run_branch.push
        {
            return None;
        }
        let Some(RunTarget::Git(target)) = spec.target.as_ref() else {
            return None;
        };
        let repository = repository(spec)?;
        let push_tokens = token_source(
            spec,
            credentials.as_ref(),
            serde_json::json!({ "contents": "write" }),
        );
        Some(Self {
            run_id,
            repository,
            base_branch: target.branch.clone(),
            goal: spec.graph.goal.clone(),
            model: settings.model.name.clone(),
            credentials,
            push_tokens,
            pull_request: settings
                .pull_request
                .clone()
                .filter(|pull_request| pull_request.enabled),
            llm_source,
            catalog,
            records,
            client,
        })
    }

    /// The model that writes the pull request: the run's, or the catalog's
    /// default among the providers whose credentials resolve.
    async fn model(&self) -> Option<String> {
        if let Some(model) = &self.model {
            return Some(model.clone());
        }
        let ready =
            credentials::readiness(self.catalog.enabled_providers(), self.llm_source.as_ref())
                .await;
        self.catalog
            .default_offering_for(&ready.ready)
            .map(|entry| entry.model.id().to_string())
    }

    async fn open_pull_request(
        &self,
        context: GitHubContext<'_>,
        settings: &PullRequestSettings,
        publication: &Publication,
    ) -> Result<(), String> {
        let model = self
            .model()
            .await
            .ok_or_else(|| "no LLM model is available to write the pull request".to_string())?;
        // The run so far, for the pull request's details: best effort.
        let run_state = self.client.get_run_state(&self.run_id).await.ok();
        let origin_url = self.repository.https_url();
        let created = pull_request::open_pull_request(OpenPullRequestRequest {
            run_id:            self.run_id,
            github:            context,
            origin_url:        &origin_url,
            base_branch:       &self.base_branch,
            head_branch:       &publication.run_branch,
            expected_head_sha: &publication.head_sha,
            goal:              &self.goal,
            diff:              &publication.patch,
            model:             &model,
            draft:             settings.draft,
            auto_merge:        settings.auto_merge.then_some(AutoMergeOptions {
                merge_strategy: settings.merge_strategy,
            }),
            llm_source:        Arc::clone(&self.llm_source),
            catalog:           Arc::clone(&self.catalog),
            conclusion:        None,
            run_state:         run_state.as_ref(),
        })
        .await
        .map_err(|err| format!("failed to create pull request: {err}"))?;
        let link = &created.link;
        let record = PlatformRecord::PullRequestCreated(PullRequestCreatedRecord {
            number:    link.number,
            owner:     link.owner.clone(),
            repo:      link.repo.clone(),
            html_url:  link.html_url(),
            head_sha:  Some(publication.head_sha.clone()),
            draft:     settings.draft,
            operation: None,
        });
        self.records
            .append(&self.run_id, &record, None)
            .await
            .map_err(|err| format!("the pull request was opened but not recorded: {err}"))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl RunPublisher for GitHubPublisher {
    async fn push(&self, site: &Site, branch: &str, sha: &str) -> Result<(), String> {
        let tokens = self.push_tokens.as_ref().ok_or_else(|| {
            "pushing the run branch requires the server's GitHub credentials".to_string()
        })?;
        let resolved = tokens
            .resolve()
            .await
            .map_err(|err| format!("no push credential for {}: {err:#}", self.repository))?;
        push(&self.repository, &resolved.token, site, branch, sha).await
    }

    async fn publish(&self, publication: &Publication) -> Result<(), String> {
        self.push(
            &publication.site,
            &publication.run_branch,
            &publication.head_sha,
        )
        .await?;
        let Some(settings) = &self.pull_request else {
            return Ok(());
        };
        if publication.patch.trim().is_empty() {
            return Ok(());
        }
        let credentials = self
            .credentials
            .as_ref()
            .ok_or_else(|| "GitHub credentials are unavailable".to_string())?;
        let base_url = fabro_github::github_api_base_url();
        let context = GitHubContext::new(credentials, &base_url);
        Box::pin(self.open_pull_request(context, settings, publication)).await
    }
}

/// Push a checkpoint from inside its workspace to the run
/// branch on GitHub, retrying a failure that may be a token still
/// replicating. The token reaches `git` as an HTTP header for the
/// repository alone and never appears in the error.
async fn push(
    repository: &GitHubRepositorySlug,
    token: &SecretString,
    site: &Site,
    branch: &str,
    sha: &str,
) -> Result<(), String> {
    let mut url = repository.https_url();
    url.push_str(".git");
    let credential = basic(token);
    let env = credential
        .as_ref()
        .map(|credential| credential.header_env(&url))
        .unwrap_or_default();
    let refspec = format!("{sha}:refs/heads/{branch}");
    let mut last = String::new();
    for attempt in 1..=PUSH_ATTEMPTS {
        match site.push(&url, &refspec, &env, PUSH_TIMEOUT).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                last = error.to_string().replace(token.expose(), "***");
                if let Some(credential) = &credential {
                    last = last.replace(credential.encoded(), "***");
                }
            }
        }
        warn!(
            attempt,
            branch,
            error = last,
            "pushing the run branch failed"
        );
        if attempt < PUSH_ATTEMPTS {
            time::sleep(PUSH_RETRY_DELAY).await;
        }
    }
    Err(format!(
        "the run branch {branch} could not be pushed to {repository}: {last}"
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::Utc;
    use fabro_github::InstallationToken;
    use fabro_github::test_support::{InstallationTokenMinter, installation_token_source};
    use fabro_llm::credentials::NoCredentials;
    use fabro_llm::test_support;
    use fabro_petri::test_support::MemoryPlatformRecords;
    use fabro_types::test_support::test_run_spec;
    use fabro_types::{GitRunTarget, SecretType};
    use tokio::runtime;

    use super::*;

    fn spec() -> RunSpec {
        let mut spec = test_run_spec();
        spec.target = Some(RunTarget::Git(GitRunTarget {
            repo:   "acme/widgets".to_string(),
            branch: "main".to_string(),
            tag:    None,
            sha:    None,
        }));
        spec.settings.run.run_branch.enabled = true;
        spec.settings.run.run_branch.push = true;
        spec
    }

    #[test]
    fn only_a_pushed_github_target_run_is_published() {
        let publishes = |spec: &RunSpec| {
            GitHubPublisher::for_run(
                RunId::new(),
                spec,
                None,
                Arc::new(NoCredentials),
                Arc::new(test_support::test_catalog()),
                Arc::new(MemoryPlatformRecords::new()),
                fabro_client::Client::new_no_proxy("http://127.0.0.1:9").unwrap(),
            )
            .is_some()
        };
        assert!(publishes(&spec()));

        let mut dry = spec();
        dry.settings.run.execution.mode = RunMode::DryRun;
        assert!(!publishes(&dry));

        let mut unpushed = spec();
        unpushed.settings.run.run_branch.push = false;
        assert!(!publishes(&unpushed));

        let mut empty = spec();
        empty.target = Some(RunTarget::None {});
        assert!(!publishes(&empty));
    }

    const TEST_KEY: &str =
        include_str!("../../../../../components/fabro-github/src/testdata/rsa_private.pem");

    fn vault_with(entries: &[(&str, &str)]) -> (tempfile::TempDir, Vault) {
        let temp = tempfile::tempdir().unwrap();
        let mut vault = Vault::load(temp.path().join("secrets.json")).unwrap();
        for (name, value) in entries {
            vault.set(name, value, SecretType::Token, None).unwrap();
        }
        (temp, vault)
    }

    fn selecting(spec: RunSpec) -> RunSpec {
        let mut spec = spec;
        spec.settings.run.integrations.github.app = Some(RunGithubAppSource {
            app_id_secret:      "HOMELAB_APP_ID".to_string(),
            private_key_secret: "HOMELAB_APP_KEY".to_string(),
        });
        spec
    }

    #[test]
    fn a_selected_app_resolves_from_the_named_vault_secrets() {
        let (_temp, vault) =
            vault_with(&[("HOMELAB_APP_ID", "4688510"), ("HOMELAB_APP_KEY", TEST_KEY)]);
        let credentials = run_github_credentials(&selecting(spec()), &vault)
            .unwrap()
            .expect("the selected App resolves");
        let GitHubCredentials::App(app) = credentials else {
            panic!("a selected source resolves to App credentials");
        };
        assert_eq!(app.app_id, "4688510");
        // The vault value is trimmed, as every other vault read is.
        assert_eq!(app.private_key_pem, TEST_KEY.trim());
    }

    #[test]
    fn a_selected_app_accepts_a_base64_private_key() {
        let encoded = BASE64_STANDARD.encode(TEST_KEY);
        let (_temp, vault) =
            vault_with(&[("HOMELAB_APP_ID", "4688510"), ("HOMELAB_APP_KEY", &encoded)]);
        let credentials = run_github_credentials(&selecting(spec()), &vault).unwrap();
        assert!(
            matches!(credentials, Some(GitHubCredentials::App(app)) if app.private_key_pem == TEST_KEY)
        );
    }

    fn selected_error(entries: &[(&str, &str)]) -> String {
        let (_temp, vault) = vault_with(entries);
        match run_github_credentials(&selecting(spec()), &vault) {
            Err(RunCredentialsError::Selected(err)) => err.to_string(),
            Err(RunCredentialsError::Server(err)) => panic!("selected source fell back: {err}"),
            Ok(_) => panic!("an unresolvable selected source must not resolve"),
        }
    }

    #[test]
    fn a_selected_app_with_a_missing_secret_refuses_naming_it() {
        let message = selected_error(&[("HOMELAB_APP_ID", "4688510")]);
        assert!(
            message.contains("`HOMELAB_APP_KEY` is absent or empty"),
            "{message}"
        );
        let message = selected_error(&[("HOMELAB_APP_KEY", TEST_KEY)]);
        assert!(
            message.contains("`HOMELAB_APP_ID` is absent or empty"),
            "{message}"
        );
        let message = selected_error(&[("HOMELAB_APP_ID", "  "), ("HOMELAB_APP_KEY", TEST_KEY)]);
        assert!(
            message.contains("`HOMELAB_APP_ID` is absent or empty"),
            "{message}"
        );
    }

    #[test]
    fn a_selected_app_with_an_invalid_value_refuses_without_echoing_it() {
        let message = selected_error(&[
            ("HOMELAB_APP_ID", "not-a-number"),
            ("HOMELAB_APP_KEY", TEST_KEY),
        ]);
        assert!(message.contains("`HOMELAB_APP_ID`"), "{message}");
        assert!(!message.contains("not-a-number"), "{message}");

        let bogus = "-----BEGIN RSA PRIVATE KEY-----\nbm90IGEga2V5\n-----END RSA PRIVATE KEY-----";
        let message = selected_error(&[("HOMELAB_APP_ID", "4688510"), ("HOMELAB_APP_KEY", bogus)]);
        assert!(message.contains("`HOMELAB_APP_KEY`"), "{message}");
        assert!(!message.contains("bm90IGEga2V5"), "{message}");
    }

    #[test]
    fn a_selected_app_never_falls_back_to_the_server_integration() {
        // The server's own App key is in the vault, and the selected secrets
        // are not: the run is refused rather than served under the server's App.
        let (_temp, vault) = vault_with(&[
            ("GITHUB_APP_PRIVATE_KEY", TEST_KEY),
            ("GITHUB_TOKEN", "ghs_serverwide"),
        ]);
        assert!(matches!(
            run_github_credentials(&selecting(spec()), &vault),
            Err(RunCredentialsError::Selected(
                SelectedAppError::Missing { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn a_run_without_a_github_target_needs_no_selected_read_source() {
        let mut spec = selecting(spec());
        spec.target = Some(RunTarget::None {});
        let (_temp, vault) =
            vault_with(&[("HOMELAB_APP_ID", "4688510"), ("HOMELAB_APP_KEY", TEST_KEY)]);
        let credentials = run_github_credentials(&spec, &vault).unwrap();
        assert!(
            selected_read_token_source(&spec, credentials.as_ref())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_selected_app_that_cannot_mint_for_the_repository_refuses() {
        // No GitHub answers at this API base, so the first mint fails: the
        // selection is refused before any checkout or push.
        let (_temp, vault) =
            vault_with(&[("HOMELAB_APP_ID", "4688510"), ("HOMELAB_APP_KEY", TEST_KEY)]);
        let spec = selecting(spec());
        let credentials = run_github_credentials(&spec, &vault).unwrap();
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let error =
            temp_env::with_var(EnvVars::GITHUB_BASE_URL, Some("http://127.0.0.1:9"), || {
                runtime.block_on(selected_read_token_source(&spec, credentials.as_ref()))
            })
            .err()
            .expect("an App that cannot mint is refused");
        assert!(error.contains("cannot reach acme/widgets"), "{error}");
    }

    /// Mints `token-<n>` for its n-th mint, valid for an hour.
    #[derive(Default)]
    struct CountingMinter(AtomicUsize);

    #[async_trait::async_trait]
    impl InstallationTokenMinter for CountingMinter {
        async fn mint(&self) -> anyhow::Result<InstallationToken> {
            let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(InstallationToken {
                token:      format!("token-{n}"),
                expires_at: Utc::now() + chrono::Duration::hours(1),
            })
        }
    }

    #[tokio::test]
    async fn fetches_reuse_one_cached_read_token() {
        let minter = Arc::new(CountingMinter::default());
        let credentials = ReadCredentials(installation_token_source(
            "acme/widgets",
            Arc::clone(&minter) as Arc<dyn InstallationTokenMinter>,
        ));
        let first = credentials.credential().await.unwrap();
        let second = credentials.credential().await.unwrap();
        assert_eq!(first, second);
        assert_eq!(minter.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            first.encoded(),
            BASE64_STANDARD.encode("x-access-token:token-1")
        );
    }
}
