//! Behavioural tests for the ACP fallback chain, driven through the real
//! backend against the fake ACP agent: ordered attempts, typed eligibility,
//! the redacted failover event, non-retryability after a transition, the
//! side-effect gate, and the legacy path's byte-identical behaviour.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use fabro_acp::test_support::fake_acp_agent_script;
use fabro_agent::{LocalSandbox, Sandbox, shell_quote};
use fabro_graphviz::graph::{AttrValue, Node};
use fabro_types::{EventBody, RunEvent};
use tokio_util::sync::CancellationToken;

use super::AgentAcpBackend;
use crate::context::Context;
use crate::error::Error;
use crate::event::Emitter;
use crate::handler::agent::{CodergenBackend, CodergenResult, CodergenRunRequest};

fn expect_err(result: Result<CodergenResult, Error>) -> Error {
    match result {
        Ok(_) => panic!("expected an error"),
        Err(err) => err,
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "synchronous git init in a synchronous test fixture helper"
)]
fn init_git(dir: &std::path::Path) {
    let output = std::process::Command::new("git")
        .args(["init", "-q"])
        .current_dir(dir)
        .output()
        .expect("git init should run");
    assert!(output.status.success());
}

struct Harness {
    tempdir: tempfile::TempDir,
    script:  String,
    events:  Arc<Mutex<Vec<RunEvent>>>,
    emitter: Arc<Emitter>,
}

impl Harness {
    async fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        init_git(tempdir.path());
        let script_path = tempdir.path().join("fake_acp_agent.py");
        tokio::fs::write(&script_path, fake_acp_agent_script())
            .await
            .unwrap();
        let script = shell_quote(&script_path.to_string_lossy());
        let events: Arc<Mutex<Vec<RunEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let emitter = Arc::new(Emitter::default());
        let sink = Arc::clone(&events);
        emitter.on_event(move |event| sink.lock().unwrap().push(event.clone()));
        Self {
            tempdir,
            script,
            events,
            emitter,
        }
    }

    /// A candidate command running the fake agent in `mode`, with extra env.
    fn command(&self, mode: &str, extra: &[(&str, &str)]) -> String {
        let mut parts = vec![format!("ACP_MODE={mode}")];
        for (key, value) in extra {
            parts.push(format!("{key}={}", shell_quote(value)));
        }
        parts.push(format!("python3 {}", self.script));
        parts.join(" ")
    }

    fn node(primary: &str, chain: Option<serde_json::Value>) -> Node {
        let mut node = Node::new("work");
        node.attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        node.attrs.insert(
            "acp.command".to_string(),
            AttrValue::String(primary.to_string()),
        );
        if let Some(chain) = chain {
            node.attrs.insert(
                "acp.fallback_chain".to_string(),
                AttrValue::String(chain.to_string()),
            );
        }
        node
    }

    async fn run(&self, node: &Node) -> Result<CodergenResult, Error> {
        let backend = AgentAcpBackend::new();
        let sandbox: Arc<dyn Sandbox> =
            Arc::new(LocalSandbox::new(self.tempdir.path().to_path_buf()));
        let context = Context::new();
        backend
            .run(CodergenRunRequest {
                node,
                prompt: "write hello",
                context: &context,
                thread_id: None,
                emitter: &self.emitter,
                sandbox: &sandbox,
                tool_hooks: None,
                cancel_token: CancellationToken::new(),
                agent_tool_runtime: fabro_agent::AgentToolRuntime::default(),
                durable_events: None,
                run_store: None,
                publish_branch: None,
            })
            .await
    }

    fn events(&self) -> Vec<RunEvent> {
        self.events.lock().unwrap().clone()
    }

    fn failovers(&self) -> Vec<fabro_types::AgentAcpFailoverProps> {
        self.events()
            .into_iter()
            .filter_map(|event| match event.body {
                EventBody::AgentAcpFailover(props) => Some(props),
                _ => None,
            })
            .collect()
    }

    fn started_indexes(&self) -> Vec<Option<u32>> {
        self.events()
            .into_iter()
            .filter_map(|event| match event.body {
                EventBody::AgentAcpStarted(props) => Some(props.candidate_index),
                _ => None,
            })
            .collect()
    }
}

fn model_unsupported_signature() -> serde_json::Value {
    serde_json::json!({
        "source": "process.terminal_diagnostic",
        "cause": "model_unsupported",
        "scope": "candidate",
        "all_literals": ["requested model", "is not supported"]
    })
}

fn chain(candidates: &[serde_json::Value]) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "primary_generation": "p".repeat(32),
        "full_chain": "f".repeat(32),
        "candidates": candidates,
    })
}

fn candidate(
    index: u32,
    key: &str,
    command: &str,
    signatures: &[serde_json::Value],
) -> serde_json::Value {
    serde_json::json!({
        "candidate_index": index,
        "display_name": format!("candidate {key}"),
        "candidate_key": key,
        "availability_key": key,
        "command": command,
        "availability_signatures": signatures,
    })
}

const REFUSAL: &str =
    "HTTP 400: The requested model gpt-x is not supported when using Codex with a ChatGPT account";

#[tokio::test]
async fn eligible_primary_failure_advances_to_the_next_candidate_in_one_visit() {
    let h = Harness::new().await;
    let primary = h.command("diagnostic_exit", &[("ACP_EXIT_DIAGNOSTIC", REFUSAL)]);
    let fallback = h.command("write_file", &[]);
    let node = Harness::node(
        &primary,
        Some(chain(&[
            candidate(0, "codex", &primary, &[model_unsupported_signature()]),
            candidate(1, "anthropic", &fallback, &[]),
        ])),
    );
    let result = h.run(&node).await.unwrap();
    let CodergenResult::Text {
        text,
        files_touched,
        ..
    } = result
    else {
        panic!("expected text result");
    };
    assert_eq!(text, "hello from acp");
    assert_eq!(files_touched, vec!["hello.txt"]);

    assert_eq!(h.started_indexes(), vec![Some(0), Some(1)]);
    let failovers = h.failovers();
    assert_eq!(failovers.len(), 1, "exactly one transition: {failovers:?}");
    let props = &failovers[0];
    assert_eq!(props.schema_version, 1);
    assert_eq!(props.transition, "reactive");
    assert_eq!(
        (props.from_candidate_index, props.to_candidate_index),
        (0, 1)
    );
    assert_eq!(props.cause, "model_unsupported");
    assert_eq!(props.scope, "candidate");
    assert_eq!(props.hold_key, "codex");
    assert_eq!(props.from_availability_key, "codex");
    assert_eq!(props.to_availability_key, "anthropic");
    assert_eq!(props.attempted, vec![0]);
    assert!(!props.event_id.is_empty());
    let json = serde_json::to_string(props).unwrap();
    assert!(
        !json.contains("python3"),
        "command leaked into the event: {json}"
    );
    assert!(
        !json.contains("requested model"),
        "diagnostic leaked into the event: {json}"
    );
}

#[tokio::test]
async fn non_eligible_primary_failure_terminates_with_its_own_identity_and_stays_retryable() {
    let h = Harness::new().await;
    // The diagnostic carries the generic status only: no discriminator.
    let primary = h.command("diagnostic_exit", &[(
        "ACP_EXIT_DIAGNOSTIC",
        "HTTP 400 Bad Request",
    )]);
    let fallback = h.command("write_file", &[]);
    let node = Harness::node(
        &primary,
        Some(chain(&[
            candidate(0, "codex", &primary, &[model_unsupported_signature()]),
            candidate(1, "anthropic", &fallback, &[]),
        ])),
    );
    let err = expect_err(h.run(&node).await);
    let rendered = err.display_with_causes();
    assert!(
        rendered.contains("ACP process exited"),
        "original identity must be preserved: {rendered}"
    );
    assert!(
        err.is_retryable(),
        "before any transition the outer retry stays available: {err:?}"
    );
    assert!(h.failovers().is_empty());
    assert_eq!(h.started_indexes(), vec![Some(0)]);
}

#[tokio::test]
async fn failure_after_a_transition_is_non_retryable_and_exhaustion_keeps_the_final_cause() {
    let h = Harness::new().await;
    let primary = h.command("diagnostic_exit", &[("ACP_EXIT_DIAGNOSTIC", REFUSAL)]);
    let second = h.command("diagnostic_exit", &[("ACP_EXIT_DIAGNOSTIC", REFUSAL)]);
    let node = Harness::node(
        &primary,
        Some(chain(&[
            candidate(0, "codex", &primary, &[model_unsupported_signature()]),
            candidate(1, "codex-b", &second, &[model_unsupported_signature()]),
        ])),
    );
    let err = expect_err(h.run(&node).await);
    assert!(!err.is_retryable(), "exhaustion is non-retryable: {err:?}");
    assert_eq!(
        err.failure_category(),
        fabro_types::FailureCategory::Deterministic
    );
    let text = err.to_string();
    assert!(text.contains("exhausted"), "{text}");
    assert!(
        text.contains("model_unsupported"),
        "final cause preserved: {text}"
    );
    assert_eq!(h.started_indexes(), vec![Some(0), Some(1)]);
    assert_eq!(h.failovers().len(), 1);
    let exhausted: Vec<_> = h
        .events()
        .into_iter()
        .filter_map(|event| match event.body {
            EventBody::AgentAcpExhausted(props) => Some(props),
            _ => None,
        })
        .collect();
    assert_eq!(exhausted.len(), 1, "one structured exhaustion record");
    assert_eq!(exhausted[0].candidate_index, 1);
    assert_eq!(exhausted[0].cause, "model_unsupported");
    assert_eq!(exhausted[0].terminal, "reactive");
    assert_eq!(exhausted[0].attempted, vec![0, 1]);
    assert_eq!(exhausted[0].attempted_durations_ms.len(), 2);
}

#[tokio::test]
async fn a_chain_with_every_candidate_preflight_skipped_terminates_the_run_before_any_adapter() {
    let h = Harness::new().await;
    let primary = h.command("diagnostic_exit", &[(
        "ACP_EXIT_DIAGNOSTIC",
        "must not run",
    )]);
    let mut zero = candidate(0, "codex", &primary, &[]);
    zero["preflight_skipped"] = serde_json::json!({
        "cause": "quota", "scope": "availability-domain", "hold_key": "codex"
    });
    let node = Harness::node(&primary, Some(chain(&[zero])));
    let err = expect_err(h.run(&node).await);
    assert!(matches!(err, Error::TerminateRun { .. }), "{err:?}");
    assert_eq!(
        err.failure_category(),
        fabro_types::FailureCategory::BudgetExhausted
    );
    assert!(err.to_string().contains("quota"), "{err}");
    assert!(h.started_indexes().is_empty(), "no adapter may start");
    assert!(
        h.events()
            .iter()
            .any(|event| matches!(event.body, EventBody::AgentAcpExhausted(_)))
    );
}

#[tokio::test]
async fn external_tool_before_an_eligible_failure_fails_closed_without_fallback() {
    let h = Harness::new().await;
    let primary = h.command("tool_then_exit", &[
        ("ACP_EXIT_DIAGNOSTIC", REFUSAL),
        ("ACP_TOOL_KIND", "execute"),
    ]);
    let fallback = h.command("write_file", &[]);
    let node = Harness::node(
        &primary,
        Some(chain(&[
            candidate(0, "codex", &primary, &[model_unsupported_signature()]),
            candidate(1, "anthropic", &fallback, &[]),
        ])),
    );
    let err = expect_err(h.run(&node).await);
    let text = err.to_string();
    assert!(text.contains("ACP fallback refused"), "{text}");
    assert!(text.contains("sandbox-local"), "{text}");
    assert!(
        h.failovers().is_empty(),
        "no transition may happen past an unknown side effect"
    );
    assert_eq!(h.started_indexes(), vec![Some(0)]);
    let side_effects: Vec<_> = h
        .events()
        .into_iter()
        .filter_map(|event| match event.body {
            EventBody::AgentAcpSideEffect(props) => Some(props),
            _ => None,
        })
        .collect();
    assert_eq!(side_effects.len(), 1);
    assert_eq!(side_effects[0].classification, "external_or_unknown");
    assert_eq!(side_effects[0].observed_via, "tool_started");
}

#[tokio::test]
async fn sandbox_local_tool_before_an_eligible_failure_hands_over_with_a_recovery_preamble() {
    let h = Harness::new().await;
    let record = h.tempdir.path().join("prompt.json");
    let primary = h.command("tool_then_exit", &[
        ("ACP_EXIT_DIAGNOSTIC", REFUSAL),
        ("ACP_TOOL_KIND", "edit"),
    ]);
    let fallback = h.command("write_file", &[(
        "ACP_PROMPT_RECORD",
        &record.to_string_lossy(),
    )]);
    let node = Harness::node(
        &primary,
        Some(chain(&[
            candidate(0, "codex", &primary, &[model_unsupported_signature()]),
            candidate(1, "anthropic", &fallback, &[]),
        ])),
    );
    h.run(&node).await.unwrap();
    assert_eq!(h.failovers().len(), 1);
    let recorded = tokio::fs::read_to_string(&record).await.unwrap();
    assert!(
        recorded.contains("FABRO_ACP_FALLBACK_RECOVERY"),
        "successor must receive the delimited recovery preamble: {recorded}"
    );
    assert!(
        recorded.contains("write hello"),
        "original task preserved: {recorded}"
    );
}

#[tokio::test]
async fn preflight_skipped_primary_emits_one_preflight_transition_and_never_runs() {
    let h = Harness::new().await;
    let primary = h.command("diagnostic_exit", &[(
        "ACP_EXIT_DIAGNOSTIC",
        "must not run",
    )]);
    let fallback = h.command("write_file", &[]);
    let mut zero = candidate(0, "codex", &primary, &[]);
    zero["preflight_skipped"] = serde_json::json!({
        "cause": "quota", "scope": "availability-domain", "hold_key": "codex"
    });
    let node = Harness::node(
        &primary,
        Some(chain(&[zero, candidate(1, "anthropic", &fallback, &[])])),
    );
    h.run(&node).await.unwrap();
    assert_eq!(h.started_indexes(), vec![Some(1)]);
    let failovers = h.failovers();
    assert_eq!(failovers.len(), 1);
    assert_eq!(failovers[0].transition, "preflight");
    assert_eq!(failovers[0].cause, "quota");
    assert_eq!(failovers[0].skipped, vec![0]);
    assert_eq!(failovers[0].from_duration_ms, 0);
}

#[tokio::test]
async fn malformed_chain_refuses_before_any_adapter_starts() {
    let h = Harness::new().await;
    let primary = h.command("write_file", &[]);
    let node = Harness::node(
        &primary,
        Some(chain(&[candidate(0, "codex", "something-else", &[])])),
    );
    let err = expect_err(h.run(&node).await);
    assert!(matches!(err, Error::Validation(_)), "{err:?}");
    assert!(
        h.started_indexes().is_empty(),
        "no adapter may start on a malformed chain"
    );
}

#[tokio::test]
async fn legacy_node_without_a_chain_carries_no_candidate_fields() {
    let h = Harness::new().await;
    let primary = h.command("write_file", &[]);
    let node = Harness::node(&primary, None);
    let CodergenResult::Text { text, .. } = h.run(&node).await.unwrap() else {
        panic!("expected text result");
    };
    assert_eq!(text, "hello from acp");
    assert_eq!(h.started_indexes(), vec![None]);
    assert!(h.failovers().is_empty());
    let map: HashMap<String, usize> = HashMap::new();
    assert!(map.is_empty());
}

/// The fake agent's `ACP_CONFIG_OPTIONS` advertisement: a model select with
/// `m1` current and `m1`/`m2` offered, and an effort select with `low`
/// current and `low`/`high` offered.
fn advertised_model_and_effort() -> String {
    serde_json::json!([
        {"id": "model", "current": "m1", "values": ["m1", "m2"]},
        {"id": "effort", "current": "low", "values": ["low", "high"]}
    ])
    .to_string()
}

fn candidate_with_options(
    index: u32,
    key: &str,
    command: &str,
    options: serde_json::Value,
) -> serde_json::Value {
    let mut value = candidate(index, key, command, &[]);
    value["config_options"] = options;
    value
}

impl Harness {
    fn exhausted_count(&self) -> usize {
        self.events()
            .into_iter()
            .filter(|event| matches!(event.body, EventBody::AgentAcpExhausted(_)))
            .count()
    }

    fn started_props(&self) -> Vec<fabro_types::run_event::AgentAcpStartedProps> {
        self.events()
            .into_iter()
            .filter_map(|event| match event.body {
                EventBody::AgentAcpStarted(props) => Some(props),
                _ => None,
            })
            .collect()
    }
}

#[tokio::test]
async fn config_options_are_set_before_the_first_prompt_and_confirmed_on_started() {
    let h = Harness::new().await;
    let set_record = h.tempdir.path().join("set-config.txt");
    let prompt_record = h.tempdir.path().join("prompt.json");
    let advertised = advertised_model_and_effort();
    let primary = h.command("write_file", &[
        ("ACP_CONFIG_OPTIONS", advertised.as_str()),
        ("ACP_SET_CONFIG_RECORD", &set_record.to_string_lossy()),
        ("ACP_PROMPT_RECORD", &prompt_record.to_string_lossy()),
    ]);
    let node = Harness::node(
        &primary,
        Some(chain(&[candidate_with_options(
            0,
            "codex",
            &primary,
            serde_json::json!({"model": "m2", "effort": "high"}),
        )])),
    );
    let CodergenResult::Text { text, .. } = h.run(&node).await.unwrap() else {
        panic!("expected text result");
    };
    assert_eq!(text, "hello from acp");
    assert_eq!(
        tokio::fs::read_to_string(&set_record).await.unwrap(),
        "model=m2\neffort=high\n",
        "every requested option is set through session/set_config_option, model first"
    );
    assert!(
        prompt_record.exists(),
        "the prompt runs once every option is confirmed"
    );
    let started = h.started_props();
    assert_eq!(started.len(), 1, "one started event for the one candidate");
    assert_eq!(started[0].candidate_index, Some(0));
    assert_eq!(started[0].model.as_deref(), Some("m2"));
    assert_eq!(started[0].effort.as_deref(), Some("high"));
    assert!(h.failovers().is_empty());
}

#[tokio::test]
async fn unadvertised_model_refuses_before_any_prompt_as_model_unsupported_without_fallback() {
    let h = Harness::new().await;
    let prompt_record = h.tempdir.path().join("prompt.json");
    let advertised = advertised_model_and_effort();
    let primary = h.command("write_file", &[
        ("ACP_CONFIG_OPTIONS", advertised.as_str()),
        ("ACP_PROMPT_RECORD", &prompt_record.to_string_lossy()),
    ]);
    let fallback_prompt_record = h.tempdir.path().join("fallback-prompt.json");
    let fallback = h.command("write_file", &[(
        "ACP_PROMPT_RECORD",
        &fallback_prompt_record.to_string_lossy(),
    )]);
    let node = Harness::node(
        &primary,
        Some(chain(&[
            candidate_with_options(0, "codex", &primary, serde_json::json!({"model": "m9"})),
            candidate(1, "anthropic", &fallback, &[]),
        ])),
    );
    let err = expect_err(h.run(&node).await);
    let rendered = err.to_string();
    assert!(
        rendered.contains("model_unsupported (candidate)"),
        "the refusal is typed model_unsupported at candidate scope: {rendered}"
    );
    assert!(
        !fallback_prompt_record.exists(),
        "the fallback candidate never runs after a pre-turn refusal"
    );
    assert_eq!(
        h.exhausted_count(),
        0,
        "a pre-turn refusal is not an exhaustion"
    );
    assert!(
        rendered.contains("model=m9"),
        "the refusal names the option it refused: {rendered}"
    );
    assert!(
        !matches!(err, Error::Validation(_)),
        "a run-time advertisement mismatch is not a chain validation error: {err:?}"
    );
    assert!(
        !prompt_record.exists(),
        "no prompt may be sent to a candidate whose model was refused"
    );
    assert!(
        h.failovers().is_empty(),
        "a pre-turn refusal never triggers reactive fallback"
    );
    assert!(
        h.started_indexes().is_empty(),
        "a candidate refused before its prompt never records a started event"
    );
}

#[tokio::test]
async fn unadvertised_effort_refuses_before_any_prompt_as_malformed_configuration() {
    let h = Harness::new().await;
    let set_record = h.tempdir.path().join("set-config.txt");
    let prompt_record = h.tempdir.path().join("prompt.json");
    let advertised = serde_json::json!([
        {"id": "model", "current": "m1", "values": ["m1", "m2"]}
    ])
    .to_string();
    let primary = h.command("write_file", &[
        ("ACP_CONFIG_OPTIONS", advertised.as_str()),
        ("ACP_SET_CONFIG_RECORD", &set_record.to_string_lossy()),
        ("ACP_PROMPT_RECORD", &prompt_record.to_string_lossy()),
    ]);
    let node = Harness::node(
        &primary,
        Some(chain(&[candidate_with_options(
            0,
            "codex",
            &primary,
            serde_json::json!({"model": "m2", "effort": "high"}),
        )])),
    );
    let err = expect_err(h.run(&node).await);
    let rendered = err.to_string();
    assert!(
        rendered.contains("malformed_configuration"),
        "an unadvertised non-model option is a malformed configuration: {rendered}"
    );
    assert_eq!(
        tokio::fs::read_to_string(&set_record).await.unwrap(),
        "model=m2\n",
        "the model was set before the effort was found unadvertised"
    );
    assert!(!prompt_record.exists(), "no prompt after a refusal");
    assert!(h.failovers().is_empty());
    assert!(h.started_indexes().is_empty());
}

#[tokio::test]
async fn an_option_the_agent_acknowledges_but_never_applies_is_not_confirmed() {
    let h = Harness::new().await;
    let prompt_record = h.tempdir.path().join("prompt.json");
    let advertised = advertised_model_and_effort();
    let primary = h.command("write_file", &[
        ("ACP_CONFIG_OPTIONS", advertised.as_str()),
        ("ACP_CONFIG_IGNORE_SET", "model"),
        ("ACP_PROMPT_RECORD", &prompt_record.to_string_lossy()),
    ]);
    let node = Harness::node(
        &primary,
        Some(chain(&[candidate_with_options(
            0,
            "codex",
            &primary,
            serde_json::json!({"model": "m2"}),
        )])),
    );
    let err = expect_err(h.run(&node).await);
    let rendered = err.to_string();
    assert!(
        rendered.contains("not_confirmed") && rendered.contains("model_unsupported (candidate)"),
        "an unconfirmed model is refused as model_unsupported: {rendered}"
    );
    assert!(!prompt_record.exists(), "the prompt waits for confirmation");
    assert!(h.started_indexes().is_empty());
}

#[tokio::test]
async fn a_set_answered_with_an_error_refuses_typed_even_when_a_signature_would_match() {
    // The agent advertises the requested model but answers the set with a
    // JSON-RPC error. Without the typed refusal that error text would reach
    // the classifier as provider evidence, and a matching signature would
    // turn it into a failover onto the next candidate plus a minted hold.
    let h = Harness::new().await;
    let prompt_record = h.tempdir.path().join("prompt.json");
    let fallback_prompt_record = h.tempdir.path().join("fallback-prompt.json");
    let advertised = advertised_model_and_effort();
    let primary = h.command("write_file", &[
        ("ACP_CONFIG_OPTIONS", advertised.as_str()),
        ("ACP_CONFIG_REFUSE_SET", "model"),
        ("ACP_PROMPT_RECORD", &prompt_record.to_string_lossy()),
    ]);
    let fallback = h.command("write_file", &[(
        "ACP_PROMPT_RECORD",
        &fallback_prompt_record.to_string_lossy(),
    )]);
    let mut zero = candidate_with_options(0, "codex", &primary, serde_json::json!({"model": "m2"}));
    zero["availability_signatures"] = serde_json::json!([{
        "source": "protocol.message",
        "cause": "model_unsupported",
        "scope": "candidate",
        "all_literals": ["unknown config option"]
    }]);
    let node = Harness::node(
        &primary,
        Some(chain(&[zero, candidate(1, "anthropic", &fallback, &[])])),
    );
    let err = expect_err(h.run(&node).await);
    let rendered = err.to_string();
    assert!(
        rendered.contains("model_unsupported (candidate)") && rendered.contains("set_refused"),
        "an error-answered set is the typed pre-turn refusal: {rendered}"
    );
    assert!(!prompt_record.exists(), "no prompt after a refused set");
    assert!(
        !fallback_prompt_record.exists(),
        "no fallback candidate runs after a refused set"
    );
    assert!(
        h.failovers().is_empty(),
        "no failover event, so nothing to mint a hold from"
    );
    assert_eq!(h.exhausted_count(), 0);
    assert!(h.started_indexes().is_empty());
}
