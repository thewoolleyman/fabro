//! A run's prepare block on a resumed run's re-acquired sandbox.
//!
//! `[run.prepare]` steps lower to the root graph's `run_prepare_1..N`
//! command nodes, which run once, first, in the root scope's sandbox. What
//! they leave outside the workspace (a plugin cache, helper scripts under
//! the sandbox user's home) lives only as long as that sandbox. A resume
//! brings the workspace back to its durable snapshot, but it replays the
//! run's records: the prepare firings are recorded complete, so they never
//! fire again. When the resume lands on a re-acquired sandbox (a recreated
//! pod or container that kept only its workspace), everything the prepare
//! block put elsewhere is gone.
//!
//! So the sandbox carries a marker, outside the workspace, saying the run's
//! prepare block belongs to it: written when a fresh run first acquires the
//! prepare nodes' scope, and lost with the sandbox. A resume that acquires
//! that scope and finds no marker runs the stored root graph's prepare
//! commands again, in order, before the first node the resume executes, and
//! writes the marker; a failed step fails the scope. A run that is never
//! interrupted runs its prepare block exactly once, through its nodes; a
//! resume onto the surviving sandbox finds the marker and runs nothing.
//! Prepare steps must therefore leave a sandbox the same whether they run
//! once or twice.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use petri_attractor_steps::command::{CommandConfig, DEFAULT_TIMEOUT_MS};
use petri_execution::RunKey;
use petri_frontend_attractor::PREPARE_NODE_PREFIX;
use petri_frontend_attractor::kinds::COMMAND_KIND;
use petri_runtime::executor::{ExecEnv, MapSecrets, SecretProvider};
use petri_runtime::ir::{Graph, Node, ScopeId};
use petri_runtime::steps::resolve_env_refs;

use crate::checkpoint::RunWorkspaces;

/// Where the markers live inside a sandbox: outside the workspace, in
/// storage that goes with the sandbox.
pub const MARKER_DIR: &str = "/tmp/fabro-run-prepared";

/// How long a marker check or write may take.
const MARKER_TIMEOUT: Duration = Duration::from_mins(1);

/// The marker a sandbox carries once it holds the prepare block of the run
/// stored under `run_key`.
#[must_use]
pub fn marker_path(run_key: &RunKey) -> String {
    let name: String = run_key
        .as_str()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{MARKER_DIR}/{name}")
}

/// One prepare step: its node and the command config the node runs.
struct Step {
    node:   String,
    config: CommandConfig,
}

/// The root graph's prepare block: the scope its nodes run in, and the
/// steps in order, or why they could not be read.
pub struct PrepareBlock {
    scope: ScopeId,
    steps: Result<Vec<Step>, String>,
}

impl std::fmt::Debug for PrepareBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrepareBlock")
            .field("scope", &self.scope)
            .field("steps", &self.steps.as_ref().map(Vec::len))
            .finish()
    }
}

impl PrepareBlock {
    /// The prepare block of a run's root graph, or `None` when the run has
    /// no prepare steps.
    #[must_use]
    pub fn of(graph: &Graph) -> Option<Self> {
        let mut nodes: Vec<(u64, &Node)> = graph
            .body
            .nodes
            .iter()
            .filter(|node| node.step.kind == COMMAND_KIND)
            .filter_map(|node| {
                let index = node.name.strip_prefix(PREPARE_NODE_PREFIX)?;
                Some((index.parse().ok()?, node))
            })
            .collect();
        nodes.sort_by_key(|(index, _)| *index);
        let scope = nodes.first()?.1.scope;
        let steps = nodes
            .into_iter()
            .map(|(_, node)| {
                let config: CommandConfig = serde_json::from_value(node.step.config.clone())
                    .map_err(|error| {
                        format!(
                            "the prepare step `{}` could not be read: {error}",
                            node.name
                        )
                    })?;
                Ok(Step {
                    node: node.name.to_string(),
                    config,
                })
            })
            .collect();
        Some(Self { scope, steps })
    }

    /// Whether `scope` is the one the prepare nodes run in.
    #[must_use]
    pub fn runs_in(&self, scope: ScopeId) -> bool {
        self.scope == scope
    }

    /// Whether the sandbox already carries the run's marker.
    pub async fn marked(env: &Arc<dyn ExecEnv>, marker: &str) -> Result<bool, String> {
        let output = RunWorkspaces::run_in_env(
            env,
            "sh",
            &["-c", "test -e \"$1\"", "sh", marker],
            "prepare marker check",
            &[],
            MARKER_TIMEOUT,
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(output.success)
    }

    /// Write the marker into the sandbox.
    pub async fn mark(env: &Arc<dyn ExecEnv>, marker: &str) -> Result<(), String> {
        let output = RunWorkspaces::run_in_env(
            env,
            "sh",
            &[
                "-c",
                "mkdir -p \"$(dirname \"$1\")\" && : > \"$1\"",
                "sh",
                marker,
            ],
            "prepare marker write",
            &[],
            MARKER_TIMEOUT,
        )
        .await
        .map_err(|error| error.to_string())?;
        if output.success {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
        }
    }

    /// Run every prepare step in the sandbox, in order, as its node runs
    /// it: the same interpreter, environment and deadline, in the
    /// workspace. The first step that fails ends the block with its reason.
    pub async fn run(
        &self,
        env: &Arc<dyn ExecEnv>,
        secrets: Option<&Arc<dyn SecretProvider>>,
    ) -> Result<(), String> {
        let steps = self.steps.as_ref().map_err(Clone::clone)?;
        let none = MapSecrets::new(BTreeMap::new());
        let secrets: &dyn SecretProvider = match secrets {
            Some(secrets) => secrets.as_ref(),
            None => &none,
        };
        for step in steps {
            let resolved = resolve_env_refs(&step.config.env, secrets).map_err(|failure| {
                format!(
                    "the prepare step `{}` failed: {}",
                    step.node, failure.message
                )
            })?;
            let extra: Vec<(String, String)> = resolved
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect();
            let config = &step.config;
            let shell = format!("exec 2>&1\n{}", config.script);
            let (program, args): (&str, Vec<&str>) = match config.language.as_str() {
                "python" => ("python3", vec!["-c", config.script.as_str()]),
                _ => ("bash", vec!["-c", shell.as_str()]),
            };
            let timeout = Duration::from_millis(config.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));
            let output = RunWorkspaces::run_in_env(
                env,
                program,
                &args,
                &format!("prepare step {}", step.node),
                &extra,
                timeout,
            )
            .await
            .map_err(|error| format!("the prepare step `{}` failed: {error}", step.node))?;
            if !output.success {
                let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&output.stderr));
                let tail: String = {
                    let chars: Vec<char> = text.trim_end().chars().collect();
                    chars[chars.len().saturating_sub(2000)..].iter().collect()
                };
                return Err(format!(
                    "the prepare step `{}` exited unsuccessfully: {tail}",
                    step.node
                ));
            }
        }
        Ok(())
    }
}
