//! The `run_turn` span: one OpenTelemetry span per ACP agent-node attempt,
//! carrying the command the agent was launched with and a digest of what was
//! launched, never an environment value.
//!
//! Petri emits no spans on its agent or ACP paths, so Fabro observes the two
//! boundaries it owns and joins them here:
//!
//! - **The launch.** [`RunTurns::executor`] is an executor layer, installed
//!   outermost through `Runtime::executor_layer` (after the GitHub credential
//!   layer). It wraps every acquired scope's environment, so it sees each
//!   [`ProcessSpec`] a step spawns: after the agent step resolved its `$secret`
//!   references, before Fabro's credential layer adds the managed GitHub token
//!   and before the executor puts the scope's ambient environment beneath it.
//!   That is exactly the launch the workflow rendered, which is what the
//!   Dispatcher can recompute from its own rendering of the adapter string and
//!   the overlay it wrote.
//!
//!   Petri's own layering seam (`EnvHandle::with_spawn_env`) hands a layer the
//!   environment map alone, not the program or its arguments, so this layer
//!   builds its own [`EnvHandle`] around the acquired one and keeps the
//!   acquired handle as its teardown record, handing it back to the inner
//!   executor on release.
//! - **The attempt.** Fabro's hooks open a turn at `before_attempt` for an
//!   admitted `attractor/agent` attempt whose backend is `acp`, and close it at
//!   `prepare_result`, which runs once per attempt (`after_record` runs once
//!   per firing, after the final attempt only, so it cannot close a retried
//!   attempt's span).
//!
//! The two meet through the scope. An acquisition carries no node, firing or
//! attempt (`AcquireContext` holds secrets, progress and a lease), and one
//! scope's environment serves every node that runs in it, so the hooks bind
//! each wrapped environment to its execution at `scope_acquired` (the
//! environment the hook receives is the wrapper itself), and a spawn with a
//! piped stdin in that environment is attributed to the oldest open agent
//! turn of the same execution and scope that has not launched yet. An ACP
//! agent is the only spawn an agent attempt makes with a piped stdin.
//!
//! # Span model
//!
//! Name `run_turn`; parent the worker's `run` span when export is on (which
//! the server's `run` span parents in turn, through `TRACEPARENT`).
//! Attributes: `fabro.run_id`, `fabro.node`, `fabro.firing`, `fabro.attempt`,
//! `run_turn.command` (the program and arguments, shell-quoted, through the
//! run's secret masker), `run_turn.command.digest`, `run_turn.env.keys`
//! (the sorted variable NAMES, comma-joined), `run_turn.status`
//! (`ok` / `error` / `cancelled`), every `acp.*` metric on the attempt's
//! record flattened to dotted scalars (`acp.turns`, `acp.usage.*`,
//! `acp.context.*`), and `acp.stop_reason` when the record names one. The
//! span starts at `before_attempt` and ends when the agent process exits, if
//! the process was waited on, else at `prepare_result`. Correlation
//! attributes (`work.item.id`, `livespec.dispatch.id`,
//! `livespec.dispatch.factory`) are resource attributes from
//! `OTEL_RESOURCE_ATTRIBUTES`, not span attributes this module sets.
//!
//! # Digest
//!
//! [`command_digest`]: `sha256:` and the lowercase hex SHA-256 of a sequence
//! of netstring-framed fields, each written as `<decimal byte length>:<bytes>`
//! with no separator between fields:
//!
//! 1. the scheme tag `fabro-run-turn-command-v1`;
//! 2. the program;
//! 3. the argument count, in decimal, then each argument in order;
//! 4. the environment entry count, in decimal, then for each variable in
//!    byte-wise order of its name: the name, then the lowercase hex SHA-256 of
//!    its value.
//!
//! An environment value enters the digest only as its own SHA-256, so
//! changing one value changes the digest while no record carries the value.
//!
//! # Export off
//!
//! With no OTLP endpoint configured the global tracer is the no-op tracer:
//! the bookkeeping still runs and every span is non-recording, and nothing
//! here can fail a spawn, a release or an attempt.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use fabro_util::{shell, sync};
use opentelemetry::global::{self, BoxedSpan, BoxedTracer};
use opentelemetry::trace::{Span as _, SpanKind, Status as SpanStatus, Tracer as _};
use opentelemetry::{Context, KeyValue};
use petri_runtime::driver::FiringView;
use petri_runtime::driver::lifecycle::HookContext;
use petri_runtime::executor::{
    AcquireContext, ByteStream, DirectoryEntry, EnvError, EnvHandle, ExecEnv, Executor, ExitStatus,
    LineStream, Masker, PreviewUrl, ProcessHandle, ProcessSpec, ReleaseReport, ScopeOutcome,
    ScopeSpec, Sig, StdinMode, StdinWriter,
};
use petri_runtime::ir::{Outcome, Status, UnderlyingFailure, Value};
use sha2::{Digest as _, Sha256};
use smol_str::SmolStr;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// The span name, kept from the 0.254 carrier for its consumers.
pub const SPAN_NAME: &str = "run_turn";
/// The scheme tag the digest's first field carries.
pub const DIGEST_SCHEME: &str = "fabro-run-turn-command-v1";

const AGENT_KIND: &str = "attractor/agent";
const ACP_BACKEND: &str = "acp";
const ACP_METRIC_PREFIX: &str = "acp.";
/// How deep a nested `acp.*` metric is flattened.
const FLATTEN_DEPTH: usize = 4;

/// The digest of a launch: see the module docs for the exact encoding.
#[must_use]
pub fn command_digest(spec: &ProcessSpec) -> String {
    let mut hasher = Sha256::new();
    field(&mut hasher, DIGEST_SCHEME.as_bytes());
    field(&mut hasher, spec.program.as_bytes());
    field(&mut hasher, spec.args.len().to_string().as_bytes());
    for arg in &spec.args {
        field(&mut hasher, arg.as_bytes());
    }
    field(&mut hasher, spec.env.len().to_string().as_bytes());
    for (name, value) in &spec.env {
        field(&mut hasher, name.as_bytes());
        field(
            &mut hasher,
            hex::encode(Sha256::digest(value.as_bytes())).as_bytes(),
        );
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(bytes.len().to_string().as_bytes());
    hasher.update(b":");
    hasher.update(bytes);
}

/// The program and its arguments as one shell-quoted line.
#[must_use]
pub fn command_line(spec: &ProcessSpec) -> String {
    shell::shell_join(std::iter::once(&spec.program).chain(&spec.args))
}

/// The environment's variable names, sorted and comma-joined. Never a value.
#[must_use]
pub fn env_keys(spec: &ProcessSpec) -> String {
    spec.env
        .keys()
        .map(SmolStr::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

/// Whether a node attempt is an ACP agent turn: the agent step kind with the
/// `acp` backend in its resolved config.
#[must_use]
pub fn is_acp_agent(kind: &str, config: Option<&Value>) -> bool {
    kind == AGENT_KIND
        && config
            .and_then(|config| config.get("backend"))
            .and_then(Value::as_str)
            == Some(ACP_BACKEND)
}

/// Where a turn sits in the run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnAt {
    pub execution: u64,
    pub scope:     u32,
    pub node:      String,
    pub firing:    u64,
    pub attempt:   u32,
}

impl TurnAt {
    fn key(&self) -> TurnKey {
        TurnKey {
            execution: self.execution,
            firing:    self.firing,
            attempt:   self.attempt,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct TurnKey {
    execution: u64,
    firing:    u64,
    attempt:   u32,
}

/// How a turn ended, for `run_turn.status`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnStatus {
    Ok,
    Error,
    Cancelled,
}

impl TurnStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

struct Turn {
    scope:     u32,
    seq:       u64,
    span:      Option<BoxedSpan>,
    launched:  bool,
    exited_at: Option<SystemTime>,
}

type TurnCell = Arc<Mutex<Turn>>;

struct EnvBinding {
    scope:     u32,
    /// The execution the hooks bound this environment to at
    /// `scope_acquired`; `None` before (or without) that callback, when a
    /// spawn matches the scope in any execution.
    execution: Option<u64>,
}

#[derive(Default)]
struct State {
    envs:     HashMap<usize, EnvBinding>,
    turns:    HashMap<TurnKey, TurnCell>,
    next_seq: u64,
}

/// One run's `run_turn` spans: the registry the executor layer and Fabro's
/// hooks share.
pub struct RunTurns {
    tracer:      BoxedTracer,
    parent:      Context,
    run_id:      String,
    masker:      Masker,
    /// The run's dispatch correlation attributes, on every turn.
    correlation: Vec<KeyValue>,
    state:       Mutex<State>,
}

impl RunTurns {
    /// Spans for `run_id` through `tracer`, parented on `parent`.
    #[must_use]
    pub fn new(
        tracer: BoxedTracer,
        parent: Context,
        run_id: impl Into<String>,
        masker: Masker,
    ) -> Arc<Self> {
        Arc::new(Self {
            tracer,
            parent,
            run_id: run_id.into(),
            masker,
            correlation: Vec::new(),
            state: Mutex::default(),
        })
    }

    /// Put `correlation` (the run's dispatch correlation attributes, from
    /// `fabro_types::trace_link::correlation_attributes`) on every turn. Call
    /// before the registry is shared: a shared registry is left unchanged.
    #[must_use]
    pub fn with_correlation(mut self: Arc<Self>, correlation: Vec<(String, String)>) -> Arc<Self> {
        if let Some(turns) = Arc::get_mut(&mut self) {
            turns.correlation = correlation
                .into_iter()
                .map(|(name, value)| KeyValue::new(name, value))
                .collect();
        }
        self
    }

    /// Spans through the global tracer (the OTLP pipeline when export is on,
    /// the no-op tracer otherwise), parented on the current `tracing` span:
    /// the worker's `run` span when called from the run's future.
    #[must_use]
    pub fn for_current_run(run_id: impl Into<String>, masker: Masker) -> Arc<Self> {
        Self::new(
            global::tracer("fabro"),
            tracing::Span::current().context(),
            run_id,
            masker,
        )
    }

    /// The executor layer: wrap `inner` so every scope's spawns are observed.
    pub fn executor(self: &Arc<Self>, inner: Arc<dyn Executor>) -> Arc<dyn Executor> {
        Arc::new(TelemetryExecutor {
            inner,
            turns: Arc::clone(self),
        })
    }

    /// Wrap one scope's environment so its spawns are observed.
    pub fn wrap_env(self: &Arc<Self>, inner: Arc<dyn ExecEnv>, scope: u32) -> Arc<dyn ExecEnv> {
        let env = Arc::new(TelemetryEnv {
            inner,
            turns: Arc::clone(self),
        });
        sync::lock(&self.state)
            .envs
            .insert(env_id(env.as_ref()), EnvBinding {
                scope,
                execution: None,
            });
        env
    }

    /// Bind a wrapped environment to the execution it was acquired for. An
    /// environment this registry did not wrap is ignored.
    pub fn bind_env(&self, execution: u64, env: &Arc<dyn ExecEnv>) {
        if let Some(binding) = sync::lock(&self.state).envs.get_mut(&arc_id(env)) {
            binding.execution = Some(execution);
        }
    }

    fn forget_env(&self, env: &Arc<dyn ExecEnv>) {
        sync::lock(&self.state).envs.remove(&arc_id(env));
    }

    /// `before_attempt` for an admitted attempt: open its turn when it is an
    /// ACP agent attempt.
    pub fn open(&self, context: &HookContext, view: &FiringView) {
        if is_acp_agent(view.node.step.kind.as_str(), view.config.as_ref()) {
            self.open_at(&TurnAt {
                execution: context.execution.raw(),
                scope:     view.scope.raw(),
                node:      view.node_name().to_owned(),
                firing:    view.firing.raw(),
                attempt:   view.attempt.raw(),
            });
        }
    }

    /// Open the turn at `at`. A turn already open there is ended first.
    pub fn open_at(&self, at: &TurnAt) {
        let builder = self
            .tracer
            .span_builder(SPAN_NAME)
            .with_kind(SpanKind::Internal)
            .with_start_time(SystemTime::now())
            .with_attributes(
                vec![
                    KeyValue::new("fabro.run_id", self.run_id.clone()),
                    KeyValue::new("fabro.node", at.node.clone()),
                    KeyValue::new("fabro.firing", i64::try_from(at.firing).unwrap_or(i64::MAX)),
                    KeyValue::new("fabro.attempt", i64::from(at.attempt)),
                ]
                .into_iter()
                .chain(self.correlation.iter().cloned())
                .collect::<Vec<_>>(),
            );
        let span = self.tracer.build_with_context(builder, &self.parent);
        let previous = {
            let mut state = sync::lock(&self.state);
            let seq = state.next_seq;
            state.next_seq += 1;
            state.turns.insert(
                at.key(),
                Arc::new(Mutex::new(Turn {
                    scope: at.scope,
                    seq,
                    span: Some(span),
                    launched: false,
                    exited_at: None,
                })),
            )
        };
        if let Some(previous) = previous {
            end(
                &previous,
                TurnStatus::Error,
                Some("the attempt was opened again"),
            );
        }
    }

    /// `prepare_result`: close the attempt's turn with its outcome. An
    /// attempt with no open turn is ignored.
    pub fn close(&self, context: &HookContext, view: &FiringView, outcome: &Outcome) {
        self.close_at(
            context.execution.raw(),
            view.firing.raw(),
            view.attempt.raw(),
            outcome,
        );
    }

    /// Close the turn at `(execution, firing, attempt)` with `outcome`.
    pub fn close_at(&self, execution: u64, firing: u64, attempt: u32, outcome: &Outcome) {
        let key = TurnKey {
            execution,
            firing,
            attempt,
        };
        let Some(turn) = sync::lock(&self.state).turns.remove(&key) else {
            return;
        };
        let (status, message) = classify(&outcome.status);
        {
            let mut turn = sync::lock(&turn);
            if let Some(span) = turn.span.as_mut() {
                for attribute in acp_attributes(&outcome.metrics.custom) {
                    span.set_attribute(attribute);
                }
                if let Some(reason) = stop_reason(&outcome.status) {
                    span.set_attribute(KeyValue::new("acp.stop_reason", reason));
                }
            }
        }
        let message = message.map(|message| self.masker.mask(&message));
        end(&turn, status, message.as_deref());
    }

    /// The run ended: end every turn no result closed.
    pub fn finish(&self) {
        let open = std::mem::take(&mut sync::lock(&self.state).turns);
        for turn in open.into_values() {
            end(
                &turn,
                TurnStatus::Cancelled,
                Some("the run ended before the attempt recorded a result"),
            );
        }
    }

    /// A spawn in a wrapped environment: attribute it to the turn it
    /// launches, when it launches one.
    fn launched(&self, env: usize, spec: &ProcessSpec) -> Option<TurnCell> {
        if spec.stdin != StdinMode::Piped {
            return None;
        }
        let turn = {
            let state = sync::lock(&self.state);
            let binding = state.envs.get(&env)?;
            let candidates = state.turns.iter().filter(|(key, turn)| {
                binding
                    .execution
                    .is_none_or(|execution| execution == key.execution)
                    && {
                        let turn = sync::lock(turn);
                        turn.scope == binding.scope && !turn.launched
                    }
            });
            let (_, turn) = candidates.min_by_key(|(_, turn)| sync::lock(turn).seq)?;
            Arc::clone(turn)
        };
        {
            let mut cell = sync::lock(&turn);
            cell.launched = true;
            if let Some(span) = cell.span.as_mut() {
                span.set_attribute(KeyValue::new(
                    "run_turn.command",
                    self.masker.mask(&command_line(spec)),
                ));
                span.set_attribute(KeyValue::new(
                    "run_turn.command.digest",
                    command_digest(spec),
                ));
                span.set_attribute(KeyValue::new("run_turn.env.keys", env_keys(spec)));
            }
        }
        Some(turn)
    }
}

impl Drop for RunTurns {
    fn drop(&mut self) {
        let open = std::mem::take(&mut sync::lock(&self.state).turns);
        for turn in open.into_values() {
            end(
                &turn,
                TurnStatus::Cancelled,
                Some("the run's telemetry was dropped"),
            );
        }
    }
}

/// End a turn's span once: status, then the process exit time when the
/// process was waited on, else now.
fn end(turn: &TurnCell, status: TurnStatus, message: Option<&str>) {
    let mut turn = sync::lock(turn);
    let ended_at = turn
        .exited_at
        .filter(|exited| *exited <= SystemTime::now())
        .unwrap_or_else(SystemTime::now);
    let Some(mut span) = turn.span.take() else {
        return;
    };
    span.set_attribute(KeyValue::new("run_turn.status", status.as_str()));
    span.set_status(match status {
        TurnStatus::Ok => SpanStatus::Ok,
        TurnStatus::Error => SpanStatus::error(message.unwrap_or("the turn failed").to_owned()),
        // A cancellation is not an error; `run_turn.status` says what it was.
        TurnStatus::Cancelled => SpanStatus::Unset,
    });
    span.end_with_timestamp(ended_at);
}

fn classify(status: &Status) -> (TurnStatus, Option<String>) {
    match status {
        Status::Success | Status::PartialSuccess { underlying: None } => (TurnStatus::Ok, None),
        Status::PartialSuccess {
            underlying: Some(UnderlyingFailure::Failure(info)),
        }
        | Status::Failure(info) => (TurnStatus::Error, Some(info.message.clone())),
        Status::PartialSuccess {
            underlying: Some(UnderlyingFailure::TimedOut),
        }
        | Status::TimedOut => (TurnStatus::Error, Some("the turn timed out".to_owned())),
        Status::Cancelled | Status::Skipped => (TurnStatus::Cancelled, None),
    }
}

/// The ACP stop reason a record names: the backtick-quoted reason Petri's
/// agent step writes for a turn that stopped with anything but `end_turn` or
/// `refusal`, and `cancelled` for a cancellation. A successful turn's reason
/// is not recorded, so none is claimed.
fn stop_reason(status: &Status) -> Option<String> {
    const MARKERS: [&str; 2] = ["the agent stopped with `", "stop reason `"];
    let message = match status {
        Status::Cancelled => return Some("cancelled".to_owned()),
        Status::Failure(info)
        | Status::PartialSuccess {
            underlying: Some(UnderlyingFailure::Failure(info)),
        } => &info.message,
        _ => return None,
    };
    MARKERS.iter().find_map(|marker| {
        let rest = &message[message.find(marker)? + marker.len()..];
        Some(rest[..rest.find('`')?].to_owned())
    })
}

/// Every `acp.*` metric, flattened to dotted scalar attributes.
fn acp_attributes(custom: &BTreeMap<SmolStr, Value>) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    for (key, value) in custom {
        if key.starts_with(ACP_METRIC_PREFIX) {
            flatten(key, value, FLATTEN_DEPTH, &mut attributes);
        }
    }
    attributes
}

fn flatten(key: &str, value: &Value, depth: usize, out: &mut Vec<KeyValue>) {
    match value {
        Value::Bool(value) => out.push(KeyValue::new(key.to_owned(), *value)),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                out.push(KeyValue::new(key.to_owned(), value));
            } else if let Some(value) = number.as_f64() {
                out.push(KeyValue::new(key.to_owned(), value));
            }
        }
        Value::String(value) => out.push(KeyValue::new(key.to_owned(), value.clone())),
        Value::Object(map) if depth > 0 => {
            for (name, value) in map {
                flatten(&format!("{key}.{name}"), value, depth - 1, out);
            }
        }
        Value::Object(_) | Value::Array(_) | Value::Null => {}
    }
}

/// An environment's identity: the address of the value behind its `Arc`, the
/// same for the wrapper itself and for every `Arc<dyn ExecEnv>` cloned from it.
fn env_id(env: &TelemetryEnv) -> usize {
    std::ptr::from_ref(env).cast::<()>() as usize
}

fn arc_id(env: &Arc<dyn ExecEnv>) -> usize {
    Arc::as_ptr(env).cast::<()>() as usize
}

struct TelemetryExecutor {
    inner: Arc<dyn Executor>,
    turns: Arc<RunTurns>,
}

/// The acquired handle, kept as this layer's teardown record.
#[derive(Debug)]
struct Acquired(EnvHandle);

#[async_trait]
impl Executor for TelemetryExecutor {
    async fn acquire(
        &self,
        scope: &ScopeSpec,
        ctx: &AcquireContext,
    ) -> Result<EnvHandle, EnvError> {
        let acquired = self.inner.acquire(scope, ctx).await?;
        let env = self.turns.wrap_env(acquired.exec(), acquired.scope().raw());
        let mut handle = EnvHandle::new(
            acquired.scope(),
            SmolStr::new(acquired.instance()),
            acquired.sandbox().clone(),
            env,
            Acquired(acquired.clone()),
        );
        if let Some(runner) = acquired.container_runner() {
            handle = handle.with_runner(runner);
        }
        Ok(handle)
    }

    async fn release(&self, env: EnvHandle, outcome: ScopeOutcome) -> ReleaseReport {
        self.turns.forget_env(&env.exec());
        let inner = env
            .teardown::<Acquired>()
            .map(|acquired| acquired.0.clone());
        self.inner.release(inner.unwrap_or(env), outcome).await
    }
}

struct TelemetryEnv {
    inner: Arc<dyn ExecEnv>,
    turns: Arc<RunTurns>,
}

/// Forwards every method, the provided ones included, so the executor's own
/// answers survive the layer.
#[async_trait]
impl ExecEnv for TelemetryEnv {
    async fn spawn(&self, spec: ProcessSpec) -> Result<Box<dyn ProcessHandle>, EnvError> {
        let turn = self.turns.launched(env_id(self), &spec);
        let handle = self.inner.spawn(spec).await?;
        Ok(match turn {
            Some(turn) => Box::new(TelemetryProcess {
                inner: handle,
                turn,
            }),
            None => handle,
        })
    }

    fn workspace_path(&self) -> &str {
        self.inner.workspace_path()
    }

    async fn read_file(&self, relative: &Path) -> Result<Option<Vec<u8>>, EnvError> {
        self.inner.read_file(relative).await
    }

    async fn read_file_limited(
        &self,
        relative: &Path,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, EnvError> {
        self.inner.read_file_limited(relative, limit).await
    }

    async fn write_file(&self, relative: &Path, contents: &[u8]) -> Result<(), EnvError> {
        self.inner.write_file(relative, contents).await
    }

    async fn list_directory(
        &self,
        path: &Path,
        depth: usize,
    ) -> Result<Vec<DirectoryEntry>, EnvError> {
        self.inner.list_directory(path, depth).await
    }

    fn grace(&self) -> Duration {
        self.inner.grace()
    }

    fn host_address(&self) -> Result<&str, EnvError> {
        self.inner.host_address()
    }

    fn ambient_env(&self, name: &str) -> Option<String> {
        self.inner.ambient_env(name)
    }

    fn shares_host_filesystem(&self) -> bool {
        self.inner.shares_host_filesystem()
    }

    async fn preview_url(&self, port: u16) -> Result<Option<PreviewUrl>, EnvError> {
        self.inner.preview_url(port).await
    }

    async fn release_preview_url(&self, port: u16) -> Result<(), EnvError> {
        self.inner.release_preview_url(port).await
    }
}

/// The agent process, with its exit time recorded on its turn.
struct TelemetryProcess {
    inner: Box<dyn ProcessHandle>,
    turn:  TurnCell,
}

#[async_trait]
impl ProcessHandle for TelemetryProcess {
    fn lines(&mut self) -> Option<LineStream> {
        self.inner.lines()
    }

    fn bytes(&mut self) -> Option<ByteStream> {
        self.inner.bytes()
    }

    fn stdin(&mut self) -> Option<StdinWriter> {
        self.inner.stdin()
    }

    async fn wait(&mut self) -> Result<ExitStatus, EnvError> {
        let status = self.inner.wait().await;
        if status.is_ok() {
            let mut turn = sync::lock(&self.turn);
            turn.exited_at.get_or_insert_with(SystemTime::now);
        }
        status
    }

    async fn signal(&mut self, sig: Sig) -> Result<(), EnvError> {
        self.inner.signal(sig).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use fabro_types::trace_link::correlation_attributes;
    use opentelemetry::Value as OtelValue;
    use opentelemetry::trace::noop::NoopTracer;
    use opentelemetry::trace::{SpanId, TraceContextExt as _, TracerProvider as _};
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
    use petri_runtime::executor::{MapSecrets, SecretProvider as _};
    use petri_runtime::ir::{FailureInfo, Metrics};
    use serde_json::json;
    use tokio::time::sleep;

    use super::*;

    const SECRET: &str = "s3cr3t-token-value-0123456789";

    /// Collects every exported span.
    #[derive(Clone, Debug, Default)]
    struct Recorded(Arc<StdMutex<Vec<SpanData>>>);

    impl SpanExporter for Recorded {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            self.0.lock().unwrap().extend(batch);
            Ok(())
        }
    }

    impl Recorded {
        fn spans(&self) -> Vec<SpanData> {
            self.0.lock().unwrap().clone()
        }
    }

    fn recording() -> (SdkTracerProvider, Recorded, BoxedTracer) {
        let recorded = Recorded::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(recorded.clone())
            .build();
        let tracer = BoxedTracer::new(Box::new(provider.tracer("test")));
        (provider, recorded, tracer)
    }

    fn masker() -> Masker {
        MapSecrets::empty().masker()
    }

    fn agent_spec(token: &str) -> ProcessSpec {
        ProcessSpec::new("claude-code-acp", &["--model", "sonnet with space"])
            .with_env(BTreeMap::from([
                (SmolStr::new("ZED_MODE"), SmolStr::new("acp")),
                (SmolStr::new("ANTHROPIC_AUTH"), SmolStr::new(token)),
            ]))
            .with_stdin(StdinMode::Piped)
    }

    fn at(attempt: u32) -> TurnAt {
        TurnAt {
            execution: 1,
            scope: 0,
            node: "implement".to_owned(),
            firing: 7,
            attempt,
        }
    }

    fn attribute(span: &SpanData, key: &str) -> Option<OtelValue> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.clone())
    }

    fn text(span: &SpanData, key: &str) -> Option<String> {
        attribute(span, key).map(|value| value.as_str().into_owned())
    }

    fn every_attribute_text(span: &SpanData) -> Vec<String> {
        span.attributes
            .iter()
            .map(|kv| kv.value.as_str().into_owned())
            .chain(std::iter::once(format!("{:?}", span.status)))
            .collect()
    }

    /// An environment whose processes exit at once with status zero.
    struct FakeEnv {
        spawned: StdMutex<Vec<ProcessSpec>>,
    }

    struct FakeProcess;

    #[async_trait]
    impl ProcessHandle for FakeProcess {
        fn lines(&mut self) -> Option<LineStream> {
            None
        }

        async fn wait(&mut self) -> Result<ExitStatus, EnvError> {
            Ok(ExitStatus::code(0))
        }

        async fn signal(&mut self, _sig: Sig) -> Result<(), EnvError> {
            Ok(())
        }
    }

    #[async_trait]
    impl ExecEnv for FakeEnv {
        async fn spawn(&self, spec: ProcessSpec) -> Result<Box<dyn ProcessHandle>, EnvError> {
            self.spawned.lock().unwrap().push(spec);
            Ok(Box::new(FakeProcess))
        }

        fn workspace_path(&self) -> &'static str {
            "/workspace"
        }

        async fn read_file(&self, _relative: &Path) -> Result<Option<Vec<u8>>, EnvError> {
            Ok(None)
        }

        async fn write_file(&self, _relative: &Path, _contents: &[u8]) -> Result<(), EnvError> {
            Ok(())
        }

        fn grace(&self) -> Duration {
            Duration::from_millis(10)
        }
    }

    fn fake_env() -> Arc<FakeEnv> {
        Arc::new(FakeEnv {
            spawned: StdMutex::new(Vec::new()),
        })
    }

    #[test]
    fn changing_one_env_value_changes_the_digest() {
        let base = command_digest(&agent_spec(SECRET));
        let changed = command_digest(&agent_spec("a-different-token"));
        assert_ne!(base, changed);
        assert_eq!(base, command_digest(&agent_spec(SECRET)), "stable");
        assert!(base.starts_with("sha256:"));
        assert_eq!(base.len(), "sha256:".len() + 64);
        assert!(!base.contains(SECRET));
    }

    #[test]
    fn the_digest_distinguishes_argument_boundaries_and_keys() {
        let joined = ProcessSpec::new("agent", &["a b"]);
        let split = ProcessSpec::new("agent", &["a", "b"]);
        assert_ne!(command_digest(&joined), command_digest(&split));
        let renamed = agent_spec(SECRET).with_env(BTreeMap::from([
            (SmolStr::new("ZED_MODE"), SmolStr::new("acp")),
            (SmolStr::new("ANTHROPIC_KEY"), SmolStr::new(SECRET)),
        ]));
        assert_ne!(
            command_digest(&renamed),
            command_digest(&agent_spec(SECRET))
        );
    }

    /// Pins the encoding the Dispatcher recomputes against a value computed
    /// OUTSIDE this code, with coreutils:
    /// `printf '25:fabro-run-turn-command-v15:agent1:15:--acp1:11:K64:%s' \
    ///   "$(printf v | sha256sum | cut -d' ' -f1)" | sha256sum`.
    /// A change to the framing fails here, not in a Dispatcher comparison.
    #[test]
    fn the_digest_matches_the_documented_encoding() {
        let spec = ProcessSpec::new("agent", &["--acp"])
            .with_env(BTreeMap::from([(SmolStr::new("K"), SmolStr::new("v"))]));
        assert_eq!(
            command_digest(&spec),
            "sha256:a8a198ef98d4dcd2a6d2e225b040ae02f631a5586953c6fa0aaacdb0e2225426"
        );
    }

    #[test]
    fn only_an_acp_agent_attempt_is_a_turn() {
        assert!(is_acp_agent(AGENT_KIND, Some(&json!({ "backend": "acp" }))));
        assert!(!is_acp_agent(
            AGENT_KIND,
            Some(&json!({ "backend": "api" }))
        ));
        assert!(!is_acp_agent(AGENT_KIND, Some(&json!({}))));
        assert!(!is_acp_agent(AGENT_KIND, None));
        assert!(!is_acp_agent("process", Some(&json!({ "backend": "acp" }))));
    }

    #[tokio::test]
    async fn a_turn_carries_the_launch_and_no_env_value() {
        let (_provider, recorded, tracer) = recording();
        let turns = RunTurns::new(tracer, Context::new(), "01RUN", masker());
        let env = turns.wrap_env(fake_env(), 0);
        turns.bind_env(1, &env);

        turns.open_at(&at(1));
        let mut process = env.spawn(agent_spec(SECRET)).await.unwrap();
        process.wait().await.unwrap();
        let mut outcome = Outcome::success(Value::Null);
        outcome.metrics = Metrics::default();
        outcome.metrics.custom = BTreeMap::from([
            (SmolStr::new("acp.turns"), json!(1)),
            (
                SmolStr::new("acp.usage"),
                json!({ "tokens": { "input": 10, "output": 4 } }),
            ),
            (
                SmolStr::new("acp.context"),
                json!({ "used": 5, "size": 200 }),
            ),
            (SmolStr::new("other.metric"), json!(9)),
        ]);
        turns.close_at(1, 7, 1, &outcome);

        let spans = recorded.spans();
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.name, SPAN_NAME);
        assert_eq!(text(span, "fabro.run_id").as_deref(), Some("01RUN"));
        assert_eq!(text(span, "fabro.node").as_deref(), Some("implement"));
        assert_eq!(attribute(span, "fabro.firing"), Some(OtelValue::I64(7)));
        assert_eq!(attribute(span, "fabro.attempt"), Some(OtelValue::I64(1)));
        assert_eq!(
            text(span, "run_turn.command").as_deref(),
            Some("claude-code-acp --model 'sonnet with space'")
        );
        assert_eq!(
            text(span, "run_turn.command.digest"),
            Some(command_digest(&agent_spec(SECRET)))
        );
        assert_eq!(
            text(span, "run_turn.env.keys").as_deref(),
            Some("ANTHROPIC_AUTH,ZED_MODE")
        );
        assert_eq!(text(span, "run_turn.status").as_deref(), Some("ok"));
        assert_eq!(span.status, SpanStatus::Ok);
        assert_eq!(attribute(span, "acp.turns"), Some(OtelValue::I64(1)));
        assert_eq!(
            attribute(span, "acp.usage.tokens.input"),
            Some(OtelValue::I64(10))
        );
        assert_eq!(
            attribute(span, "acp.context.size"),
            Some(OtelValue::I64(200))
        );
        assert_eq!(attribute(span, "other.metric"), None);
        assert_eq!(attribute(span, "acp.stop_reason"), None);
        for value in every_attribute_text(span) {
            assert!(!value.contains(SECRET), "an env value leaked: {value}");
        }
        assert!(
            !span.attributes.iter().any(|kv| kv.value.as_str() == "acp"),
            "the literal env value `acp` must not appear as an attribute"
        );
    }

    #[tokio::test]
    async fn a_turn_carries_the_runs_dispatch_correlation() {
        let (_provider, recorded, tracer) = recording();
        let turns = RunTurns::new(tracer, Context::new(), "01RUN", masker()).with_correlation(
            correlation_attributes(&HashMap::from([
                ("work.item.id".to_owned(), "bd-ib-6vhgqg".to_owned()),
                (
                    "livespec.dispatch.id".to_owned(),
                    "0f1e2d3c4b5a69788796a5b4c3d2e1f0".to_owned(),
                ),
                ("livespec.dispatch.factory".to_owned(), "hp".to_owned()),
                ("team".to_owned(), "platform".to_owned()),
            ])),
        );
        turns.open_at(&at(1));
        turns.close_at(1, 7, 1, &Outcome::success(Value::Null));

        let span = &recorded.spans()[0];
        assert_eq!(text(span, "fabro.run_id").as_deref(), Some("01RUN"));
        assert_eq!(text(span, "work.item.id").as_deref(), Some("bd-ib-6vhgqg"));
        assert_eq!(
            text(span, "livespec.dispatch.id").as_deref(),
            Some("0f1e2d3c4b5a69788796a5b4c3d2e1f0")
        );
        assert_eq!(
            text(span, "livespec.dispatch.factory").as_deref(),
            Some("hp")
        );
        assert_eq!(attribute(span, "team"), None);
    }

    #[tokio::test]
    async fn a_failed_turn_is_an_error_with_its_stop_reason() {
        let (_provider, recorded, tracer) = recording();
        let turns = RunTurns::new(tracer, Context::new(), "01RUN", masker());
        let env = turns.wrap_env(fake_env(), 0);
        turns.bind_env(1, &env);

        turns.open_at(&at(1));
        let _process = env.spawn(agent_spec(SECRET)).await.unwrap();
        let outcome = Outcome::new(
            Status::Failure(
                FailureInfo::new("the agent stopped with `max_tokens`")
                    .with_class("retry_requested"),
            ),
            Value::Null,
        );
        turns.close_at(1, 7, 1, &outcome);

        let span = &recorded.spans()[0];
        assert_eq!(text(span, "run_turn.status").as_deref(), Some("error"));
        assert_eq!(text(span, "acp.stop_reason").as_deref(), Some("max_tokens"));
        assert!(
            matches!(&span.status, SpanStatus::Error { description } if description.contains("max_tokens"))
        );
    }

    #[tokio::test]
    async fn a_cancelled_turn_is_cancelled_not_an_error() {
        let (_provider, recorded, tracer) = recording();
        let turns = RunTurns::new(tracer, Context::new(), "01RUN", masker());
        turns.open_at(&at(1));
        turns.close_at(1, 7, 1, &Outcome::new(Status::Cancelled, Value::Null));

        let span = &recorded.spans()[0];
        assert_eq!(text(span, "run_turn.status").as_deref(), Some("cancelled"));
        assert_eq!(text(span, "acp.stop_reason").as_deref(), Some("cancelled"));
        assert_eq!(span.status, SpanStatus::Unset);
        // No launch was seen, so no launch attributes are claimed.
        assert_eq!(attribute(span, "run_turn.command"), None);
    }

    #[tokio::test]
    async fn each_retry_is_its_own_span_with_its_own_launch() {
        let (_provider, recorded, tracer) = recording();
        let turns = RunTurns::new(tracer, Context::new(), "01RUN", masker());
        let env = turns.wrap_env(fake_env(), 0);
        turns.bind_env(1, &env);

        turns.open_at(&at(1));
        let _first = env.spawn(agent_spec("first")).await.unwrap();
        turns.close_at(
            1,
            7,
            1,
            &Outcome::new(
                Status::Failure(FailureInfo::new("the agent stopped with `max_tokens`")),
                Value::Null,
            ),
        );
        turns.open_at(&at(2));
        let _second = env.spawn(agent_spec("second")).await.unwrap();
        turns.close_at(1, 7, 2, &Outcome::success(Value::Null));

        let spans = recorded.spans();
        assert_eq!(spans.len(), 2);
        assert_eq!(
            attribute(&spans[0], "fabro.attempt"),
            Some(OtelValue::I64(1))
        );
        assert_eq!(
            attribute(&spans[1], "fabro.attempt"),
            Some(OtelValue::I64(2))
        );
        assert_ne!(
            text(&spans[0], "run_turn.command.digest"),
            text(&spans[1], "run_turn.command.digest")
        );
    }

    #[tokio::test]
    async fn only_a_piped_spawn_in_the_bound_execution_launches_a_turn() {
        let (_provider, recorded, tracer) = recording();
        let turns = RunTurns::new(tracer, Context::new(), "01RUN", masker());
        let env = turns.wrap_env(fake_env(), 0);
        let other = turns.wrap_env(fake_env(), 0);
        turns.bind_env(1, &env);
        turns.bind_env(2, &other);

        turns.open_at(&at(1));
        // A script with no stdin is not the agent.
        let _script = env
            .spawn(ProcessSpec::new("sh", &["-c", "true"]))
            .await
            .unwrap();
        // The same scope in another execution is not this turn's scope.
        let _elsewhere = other.spawn(agent_spec(SECRET)).await.unwrap();
        turns.close_at(1, 7, 1, &Outcome::success(Value::Null));

        let span = &recorded.spans()[0];
        assert_eq!(attribute(span, "run_turn.command"), None);
    }

    #[tokio::test]
    async fn the_span_ends_when_the_agent_process_exits() {
        let (_provider, recorded, tracer) = recording();
        let turns = RunTurns::new(tracer, Context::new(), "01RUN", masker());
        let env = turns.wrap_env(fake_env(), 0);
        turns.bind_env(1, &env);

        turns.open_at(&at(1));
        let mut process = env.spawn(agent_spec(SECRET)).await.unwrap();
        process.wait().await.unwrap();
        let exited = SystemTime::now();
        sleep(Duration::from_millis(50)).await;
        turns.close_at(1, 7, 1, &Outcome::success(Value::Null));

        let span = &recorded.spans()[0];
        assert!(
            span.end_time <= exited,
            "the end is the exit, not the record"
        );
        assert!(span.start_time <= span.end_time);
    }

    #[tokio::test]
    async fn a_turn_is_a_child_of_the_run_span() {
        let (provider, recorded, tracer) = recording();
        let run_tracer = provider.tracer("run");
        let run_span = run_tracer.start("run");
        let parent = Context::new().with_span(run_span);
        let parent_id = parent.span().span_context().span_id();
        let turns = RunTurns::new(tracer, parent.clone(), "01RUN", masker());

        turns.open_at(&at(1));
        turns.close_at(1, 7, 1, &Outcome::success(Value::Null));

        let span = &recorded.spans()[0];
        assert_ne!(parent_id, SpanId::INVALID);
        assert_eq!(span.parent_span_id, parent_id);
        assert_eq!(
            span.span_context.trace_id(),
            parent.span().span_context().trace_id()
        );
    }

    #[tokio::test]
    async fn unfinished_turns_end_when_the_run_finishes() {
        let (_provider, recorded, tracer) = recording();
        let turns = RunTurns::new(tracer, Context::new(), "01RUN", masker());
        turns.open_at(&at(1));
        turns.finish();

        let span = &recorded.spans()[0];
        assert_eq!(text(span, "run_turn.status").as_deref(), Some("cancelled"));
    }

    /// Export off: the global no-op tracer. Spawns, waits and records pass
    /// through untouched, nothing is exported, and nothing fails.
    #[tokio::test]
    async fn with_export_disabled_the_run_is_unaffected() {
        let (_provider, recorded, _tracer) = recording();
        let noop = BoxedTracer::new(Box::new(NoopTracer::new()));
        let turns = RunTurns::new(noop, Context::new(), "01RUN", masker());
        let inner = fake_env();
        let env = turns.wrap_env(Arc::clone(&inner) as Arc<dyn ExecEnv>, 0);
        turns.bind_env(1, &env);

        turns.open_at(&at(1));
        let mut process = env.spawn(agent_spec(SECRET)).await.unwrap();
        assert_eq!(process.wait().await.unwrap(), ExitStatus::code(0));
        turns.close_at(1, 7, 1, &Outcome::success(Value::Null));
        turns.finish();

        assert_eq!(inner.spawned.lock().unwrap().as_slice(), &[agent_spec(
            SECRET
        )]);
        assert!(recorded.spans().is_empty());
        assert_eq!(env.workspace_path(), "/workspace");
        assert_eq!(env.grace(), Duration::from_millis(10));
    }
}
