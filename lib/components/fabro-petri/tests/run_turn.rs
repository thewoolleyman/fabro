//! A real run with an ACP agent node emits one `run_turn` span through the
//! global OpenTelemetry pipeline: the executor layer saw the launch, Fabro's
//! hooks opened and closed the attempt, and no environment value reached the
//! span. The agent is a scripted ACP 1 speaker on the host sandbox.
//!
//! The global tracer provider is process-wide; nextest runs one test per
//! process.

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use fabro_petri::artifacts::StoreArtifactWriter;
use fabro_petri::check::Launch;
use fabro_petri::checkpoint::RunGitSettings;
use fabro_petri::engine::{self, RunStatus};
use fabro_petri::hooks::HooksSpec;
use fabro_petri::run_turn::{self, SPAN_NAME};
use fabro_petri::runtime::RuntimeSpec;
use fabro_petri::test_support::MemoryPlatformRecords;
use fabro_store::ArtifactStore;
use object_store::memory::InMemory;
use opentelemetry::global;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use petri_runtime::executor::ProcessSpec;
use petri_store::MemoryRunStore;
use smol_str::SmolStr;
use support::{Silent, admit, no_questions, run_request};
use tokio::fs;

const TOKEN: &str = "acp-env-value-that-must-never-leak-42";

/// A plain ACP turn: initialize, session/new, one prompt answered with
/// `end_turn` and usage, then exit.
const AGENT: &str = r#"
import json, sys

def send(message):
    print(json.dumps(message), flush=True)

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": message["id"], "result": {"protocolVersion": 1, "agentCapabilities": {}}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": message["id"], "result": {"sessionId": "s1"}})
    elif method == "session/prompt":
        send({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": "s1", "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "done"}}}})
        send({"jsonrpc": "2.0", "id": message["id"], "result": {"stopReason": "end_turn", "usage": {"inputTokens": 3, "outputTokens": 2}}})
        break
    elif method == "session/cancel":
        continue
    else:
        send({"jsonrpc": "2.0", "id": message.get("id"), "error": {"code": -32601, "message": "method not found"}})
"#;

#[derive(Clone, Debug, Default)]
struct Recorded(Arc<Mutex<Vec<SpanData>>>);

impl SpanExporter for Recorded {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.lock().unwrap().extend(batch);
        Ok(())
    }
}

fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.as_str().into_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_acp_agent_attempt_emits_one_run_turn_span() {
    let recorded = Recorded::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(recorded.clone())
        .build();
    global::set_tracer_provider(provider.clone());

    let root = tempfile::tempdir().expect("a temp dir");
    let agent = root.path().join("agent.py");
    fs::write(&agent, AGENT).await.expect("the agent writes");
    let config = serde_json::json!({
        "command": "python3",
        "args": [agent.display().to_string(), "--mode", "plain"],
        "env": { "FAKE_AGENT_TOKEN": TOKEN },
    })
    .to_string()
    .replace('"', "\\\"");
    let workflow = format!(
        "digraph Turn {{\n  graph [goal=\"Run one ACP turn\", default_max_retries=0]\n  start \
         [shape=Mdiamond]\n  exit [shape=Msquare]\n  work [shape=box, backend=\"acp\", \
         acp.config=\"{config}\", prompt=\"Say done\"]\n  start -> work -> exit\n}}\n"
    );
    let runtime = RuntimeSpec::default();
    let graphs = admit(
        &[
            ("workflow.fabro", workflow.as_str()),
            ("workflow.toml", support::SETTINGS),
        ],
        Launch::default(),
        &runtime,
    );
    let mut request = run_request(
        "run-turn",
        &root.path().join("run"),
        graphs,
        Arc::new(MemoryRunStore::new()),
        runtime,
        no_questions(Arc::new(Silent)),
    );
    request.hooks = Some(HooksSpec {
        records:         Arc::new(MemoryPlatformRecords::new()),
        git:             RunGitSettings {
            enabled: false,
            host_workspaces: true,
            ..RunGitSettings::default()
        },
        artifacts:       Vec::new(),
        test_gates:      None,
        artifact_writer: Arc::new(StoreArtifactWriter::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            "run-turn",
        ))),
        source:          None,
        publisher:       None,
    });

    let outcome = engine::run(request).await.expect("the run ends");
    assert_eq!(outcome.status, RunStatus::Success, "{outcome:?}");
    let _ = provider.force_flush();

    let spans: Vec<SpanData> = recorded
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|span| span.name == SPAN_NAME)
        .cloned()
        .collect();
    assert_eq!(spans.len(), 1, "one ACP attempt, one turn: {spans:?}");
    let span = &spans[0];
    assert_eq!(attribute(span, "fabro.run_id").as_deref(), Some("run-turn"));
    assert_eq!(attribute(span, "fabro.node").as_deref(), Some("work"));
    assert_eq!(attribute(span, "fabro.attempt").as_deref(), Some("1"));
    assert_eq!(attribute(span, "run_turn.status").as_deref(), Some("ok"));
    let command = attribute(span, "run_turn.command").expect("the launch was seen");
    assert!(command.starts_with("python3 "), "{command}");
    assert!(command.ends_with("--mode plain"), "{command}");
    let keys = attribute(span, "run_turn.env.keys").expect("the env keys were seen");
    assert!(
        keys.split(',').any(|key| key == "FAKE_AGENT_TOKEN"),
        "{keys}"
    );
    let digest = attribute(span, "run_turn.command.digest").expect("a digest");
    assert!(digest.starts_with("sha256:"), "{digest}");
    assert_eq!(attribute(span, "acp.turns").as_deref(), Some("1"));
    assert_eq!(
        attribute(span, "acp.usage.tokens.input"),
        Some("3".to_owned()),
        "{:?}",
        span.attributes
    );
    for kv in &span.attributes {
        assert!(
            !kv.value.as_str().contains(TOKEN),
            "an env value reached `{}`",
            kv.key
        );
    }
    // The digest is recomputable from the launch the workflow rendered: the
    // program, the arguments, and each variable's name and value hash, with
    // nothing the credential layer or the scope's ambient env adds.
    let rendered = ProcessSpec::new("python3", &[
        agent.display().to_string().as_str(),
        "--mode",
        "plain",
    ])
    .with_env(BTreeMap::from([(
        SmolStr::new("FAKE_AGENT_TOKEN"),
        SmolStr::new(TOKEN),
    )]));
    assert_eq!(digest, run_turn::command_digest(&rendered));
}
