//! A resume through the engine assembly: a run whose creation a crash cut
//! short starts again from its admitted graphs, and a run whose store
//! failed ends its lifetime without an end of its own; a run resumed onto
//! a re-acquired sandbox runs its prepare block again before any node.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fabro_petri::admission::AdmittedGraphs;
use fabro_petri::artifacts::StoreArtifactWriter;
use fabro_petri::check::Launch;
use fabro_petri::checkpoint::RunGitSettings;
use fabro_petri::engine::{self, Conclusion, Execution, RunError, RunStatus};
use fabro_petri::hooks::HooksSpec;
use fabro_petri::prepare;
use fabro_petri::runtime::RuntimeSpec;
use fabro_petri::test_support::MemoryPlatformRecords;
use fabro_store::ArtifactStore;
use object_store::memory::InMemory;
use petri_execution::{CancelReason, InvocationId, host};
use petri_store::{
    Access, Digest, LogId, MemoryRunStore, OwnerId, Record, RunKey, RunLogs, RunStore, StoreError,
};
use support::{SETTINGS, Silent, admit, all_records, no_questions, run_request};
use tokio::sync::watch;

/// One command stage between start and exit.
const COMMAND: &str = r#"digraph Command {
    start [shape=Mdiamond]
    exit [shape=Msquare]
    say [shape=parallelogram, script="true"]
    start -> say -> exit
}"#;

/// A crash cut the run's creation short: its key is stored, and nothing
/// else. The resume Petri refuses as never started becomes a start from
/// the admitted graphs, under the same key.
#[tokio::test]
async fn a_resume_of_a_run_that_never_started_starts_it_again() {
    let root = tempfile::tempdir().expect("a temp dir");
    let store = Arc::new(MemoryRunStore::new());
    drop(
        store
            .open(&RunKey::new("cut-short"), Access::Create {
                owner: OwnerId::new("crashed"),
            })
            .await
            .expect("the key is stored"),
    );
    let runtime = RuntimeSpec::default();
    let graphs = admit(
        &[("workflow.fabro", COMMAND), ("workflow.toml", SETTINGS)],
        Launch::default(),
        &runtime,
    );
    let outcome = engine::run(resume_request(
        "cut-short",
        root.path(),
        graphs,
        store,
        runtime,
    ))
    .await
    .expect("the run ends");

    assert_eq!(outcome.status, RunStatus::Success, "{outcome:?}");
    assert!(outcome.complete, "{:?}", outcome.incomplete);
}

/// A write to the run's store fails partway through the run: the lifetime
/// ends with the store's failure, which interrupts the run rather than
/// failing it, and nothing records an end. A resume over the same records
/// finishes the run.
#[tokio::test]
async fn a_failed_store_write_interrupts_the_run_and_a_resume_finishes_it() {
    let root = tempfile::tempdir().expect("a temp dir");
    let memory = Arc::new(MemoryRunStore::new());
    let failing = Arc::new(FailingStore {
        inner:    Arc::clone(&memory),
        log:      LogId::Resources,
        appended: Arc::new(AtomicUsize::new(0)),
        open:     OpenHandles::new().hold(),
    });
    let runtime = RuntimeSpec::default();
    let graphs = || {
        admit(
            &[("workflow.fabro", COMMAND), ("workflow.toml", SETTINGS)],
            Launch::default(),
            &runtime,
        )
    };

    let first = engine::run(run_request(
        "interrupted",
        root.path(),
        graphs(),
        failing,
        runtime.clone(),
        no_questions(Arc::new(Silent)),
    ))
    .await;

    assert!(
        matches!(&first, Err(RunError::StoreFailed(message)) if message.contains("the disk is full")),
        "{first:?}"
    );
    assert!(
        matches!(engine::conclusion(&first), Conclusion::Interrupted { .. }),
        "{first:?}"
    );
    assert!(!finished(&memory).await, "nothing ended the run");

    let outcome = engine::run(resume_request(
        "interrupted",
        root.path(),
        graphs(),
        Arc::clone(&memory) as Arc<dyn RunStore>,
        runtime,
    ))
    .await
    .expect("the resumed run ends");

    assert_eq!(outcome.status, RunStatus::Success, "{outcome:?}");
    assert!(outcome.complete, "{:?}", outcome.incomplete);
    assert!(finished(&memory).await, "the resume ended the run");
}

/// Whether the interrupted run's records hold Petri's own finish.
async fn finished(store: &MemoryRunStore) -> bool {
    all_records(store, "interrupted")
        .await
        .iter()
        .any(|record| record["body"]["event"] == "run.finished")
}

/// A resume of the run over `store`, with the admitted graphs to start it
/// from should its creation have been cut short.
fn resume_request(
    run_id: &str,
    run_dir: &std::path::Path,
    graphs: AdmittedGraphs,
    store: Arc<dyn RunStore>,
    runtime: RuntimeSpec,
) -> engine::RunRequest {
    let mut request = run_request(
        run_id,
        run_dir,
        graphs,
        store,
        runtime,
        no_questions(Arc::new(Silent)),
    );
    let Execution::Start(graphs) = request.execution else {
        unreachable!("run_request builds a start");
    };
    request.execution = Execution::Resume(graphs);
    request
}

/// The in-memory store, failing its first append to `log` as a full disk
/// would, before anything is stored.
struct FailingStore {
    inner:    Arc<MemoryRunStore>,
    log:      LogId,
    appended: Arc<AtomicUsize>,
    open:     Held,
}

#[async_trait::async_trait]
impl RunStore for FailingStore {
    async fn open(&self, key: &RunKey, access: Access) -> Result<Arc<dyn RunLogs>, StoreError> {
        Ok(Arc::new(FailingLogs {
            inner:    self.inner.open(key, access).await?,
            log:      self.log,
            appended: Arc::clone(&self.appended),
            _open:    self.open.again(),
        }))
    }
}

struct FailingLogs {
    inner:    Arc<dyn RunLogs>,
    log:      LogId,
    appended: Arc<AtomicUsize>,
    _open:    Held,
}

#[async_trait::async_trait]
impl RunLogs for FailingLogs {
    fn locator(&self) -> String {
        self.inner.locator()
    }

    async fn append(&self, log: &LogId, records: &[Record]) -> Result<(), StoreError> {
        if *log == self.log && self.appended.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(StoreError::backend(
                self.locator(),
                "append",
                "the disk is full",
            ));
        }
        self.inner.append(log, records).await
    }

    async fn read(&self, log: &LogId) -> Result<Vec<Record>, StoreError> {
        self.inner.read(log).await
    }

    async fn put_blob(&self, bytes: &[u8]) -> Result<Digest, StoreError> {
        self.inner.put_blob(bytes).await
    }

    async fn get_blob(&self, digest: Digest) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get_blob(digest).await
    }
}

#[tokio::test]
async fn resume_uses_the_persisted_stall_budget_not_the_fallback_graph() {
    resumed_stall_policy("1s", "0s", RunStatus::Cancelled).await;
}

#[tokio::test]
async fn resume_preserves_a_persisted_disabled_stall_budget() {
    resumed_stall_policy("0s", "1s", RunStatus::Success).await;
}

async fn resumed_stall_policy(persisted: &str, fallback: &str, expected: RunStatus) {
    let root = tempfile::tempdir().expect("a temporary run directory");
    let memory = Arc::new(MemoryRunStore::new());
    let runtime = RuntimeSpec::default();
    let graphs = |budget| {
        admit(
            &[
                (
                    "workflow.fabro",
                    &format!(
                        r#"digraph Resume {{
            graph [stall_timeout="{budget}"]
            start [shape=Mdiamond]
            work [shape=parallelogram, script="sleep 2", timeout="10s"]
            exit [shape=Msquare]
            start -> work -> exit
        }}"#
                    ),
                ),
                ("workflow.toml", SETTINGS),
            ],
            Launch::default(),
            &runtime,
        )
    };
    // Fail after the root graph is durable but before acquiring a workspace.
    // This exercises cleanup on host error and a genuine persisted resume.
    let first_handles = OpenHandles::new();
    let failing = Arc::new(FailingStore {
        inner:    memory.clone(),
        log:      LogId::Resources,
        appended: Arc::new(AtomicUsize::new(0)),
        open:     first_handles.hold(),
    });
    let first = engine::run(run_request(
        "stall-resume",
        root.path(),
        graphs(persisted),
        failing,
        runtime.clone(),
        no_questions(Arc::new(Silent)),
    ))
    .await;
    assert!(matches!(first, Err(RunError::StoreFailed(_))), "{first:?}");
    first_handles.all_closed().await;
    let resume_handles = OpenHandles::new();
    let outcome = engine::run(resume_request(
        "stall-resume",
        root.path(),
        graphs(fallback),
        Arc::new(Tracked {
            inner: memory.clone(),
            open:  resume_handles.hold(),
        }),
        runtime,
    ))
    .await
    .expect("the resumed run completes");
    assert_eq!(outcome.status, expected, "{outcome:?}");
    let logs = memory
        .open(&RunKey::new("stall-resume"), Access::Read)
        .await
        .expect("the resumed record opens");
    let state = host::stored_state(&*logs)
        .await
        .expect("the stored coordinator state replays");
    let reason = &state.invocations[&InvocationId::ROOT].cancel_reason;
    if expected == RunStatus::Cancelled {
        assert!(
            matches!(
                reason,
                Some(CancelReason::StallTimeout {
                    stall_timeout_ms: 1000,
                    ..
                })
            ),
            "{reason:?}"
        );
    } else {
        assert_eq!(*reason, None);
    }
    resume_handles.all_closed().await;
}

// ---------------------------------------------------------------------------
// The prepare block on a resumed run's sandbox (bd-ib-ljw2xg).
//
// Two prepare steps write files outside the workspace, the way the
// Dispatcher materializes its plugin cache and helpers under the sandbox
// user's home; the `work` node fails unless both are present. Every script
// appends its name to an order ledger kept apart from the "sandbox" state,
// so the tests read what ran, in what order. A re-acquired sandbox is
// simulated by removing the sandbox state and the run's prepare marker
// between the interrupted run and its resume. Interruptions land on store
// appends that carry a script's own output (spelled so that the script
// text itself never carries it), so they follow the run's events, never a
// clock.
// ---------------------------------------------------------------------------

/// The paths one prepare test works with.
struct PrepareFixture {
    root:    tempfile::TempDir,
    run_id:  String,
    /// The state the prepare steps leave outside the workspace.
    sandbox: std::path::PathBuf,
    /// Which scripts ran, one name per line, in order.
    ledger:  std::path::PathBuf,
    /// When set, the setup is authored as command nodes with this name
    /// prefix in the graph, and the settings declare no `[run.prepare]`.
    authored: Option<&'static str>,
}

impl PrepareFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("a temp dir");
        let sandbox = root.path().join("home/.claude/plugins");
        let ledger = root.path().join("order.log");
        let run_id = format!(
            "prepare-{}",
            root.path()
                .file_name()
                .and_then(|name| name.to_str())
                .expect("the temp dir has a name")
        );
        Self {
            root,
            run_id,
            sandbox,
            ledger,
            authored: None,
        }
    }

    /// Setup authored in the graph as `<prefix>1` and `<prefix>2` command
    /// nodes after `start`, the form upstream's removal of `run.prepare` asks for.
    fn authored(prefix: &'static str) -> Self {
        let mut fixture = Self::new();
        fixture.authored = Some(prefix);
        fixture
    }

    fn run_dir(&self) -> std::path::PathBuf {
        self.root.path().join("run")
    }

    fn files(&self) -> Vec<(&str, String)> {
        let sandbox = self.sandbox.display();
        let ledger = self.ledger.display();
        if let Some(prefix) = self.authored {
            let graph = format!(
                r#"digraph Authored {{
    graph [default_max_retries=0]
    start [shape=Mdiamond]
    exit [shape=Msquare]
    {prefix}1 [shape=parallelogram, script="mkdir -p {sandbox} && printf 'helper' > {sandbox}/helper.py && echo prepare1 >> {ledger} && printf 'PREPARE-ONE-%s' RAN"]
    {prefix}2 [shape=parallelogram, script="mkdir -p {sandbox}/cache && printf '{{}}' > {sandbox}/cache/config.json && echo prepare2 >> {ledger} && printf 'PREPARE-TWO-%s' RAN"]
    work [shape=parallelogram, script="test -f {sandbox}/helper.py && test -f {sandbox}/cache/config.json && echo work >> {ledger} && printf 'WORK-%s' RAN"]
    start -> {prefix}1 -> {prefix}2 -> work -> exit
}}"#
            );
            return vec![("workflow.fabro", graph), ("workflow.toml", SETTINGS.to_owned())];
        }
        let graph = format!(
            r#"digraph Prepared {{
    graph [default_max_retries=0]
    start [shape=Mdiamond]
    exit [shape=Msquare]
    work [shape=parallelogram, script="test -f {sandbox}/helper.py && test -f {sandbox}/cache/config.json && echo work >> {ledger} && printf 'WORK-%s' RAN"]
    start -> work -> exit
}}"#
        );
        let settings = format!(
            "{SETTINGS}\n[[run.prepare.steps]]\nscript = \"mkdir -p {sandbox} && printf \
             'helper' > {sandbox}/helper.py && echo prepare1 >> {ledger} && printf \
             'PREPARE-ONE-%s' RAN\"\n\n[[run.prepare.steps]]\nscript = \"mkdir -p {sandbox}/cache && \
             printf '{{}}' > {sandbox}/cache/config.json && echo prepare2 >> {ledger} && printf \
             'PREPARE-TWO-%s' RAN\"\n"
        );
        vec![("workflow.fabro", graph), ("workflow.toml", settings)]
    }

    fn graphs(&self, runtime: &RuntimeSpec) -> AdmittedGraphs {
        let files = self.files();
        let files: Vec<(&str, &str)> = files
            .iter()
            .map(|(path, text)| (*path, text.as_str()))
            .collect();
        admit(&files, Launch::default(), runtime)
    }

    fn marker(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(prepare::marker_path(&RunKey::new(&self.run_id)))
    }

    /// The order ledger, one entry per script run.
    #[expect(
        clippy::disallowed_methods,
        reason = "the test reads its small ledger between runs, with no run in flight"
    )]
    fn order(&self) -> Vec<String> {
        std::fs::read_to_string(&self.ledger)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    /// Every file the prepare steps left, with its contents.
    #[expect(
        clippy::disallowed_methods,
        reason = "the test reads the prepare block's few files between runs, with no run in flight"
    )]
    fn sandbox_state(&self) -> Vec<(String, String)> {
        let mut state = Vec::new();
        let mut pending = vec![self.sandbox.clone()];
        while let Some(dir) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries {
                let path = entry.expect("a directory entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    state.push((
                        path.display().to_string(),
                        std::fs::read_to_string(&path).expect("the file reads"),
                    ));
                }
            }
        }
        state.sort();
        state
    }

    /// Lose what a recreated sandbox loses: everything outside the
    /// workspace, the run's marker included.
    fn reacquire_sandbox(&self) {
        std::fs::remove_dir_all(&self.sandbox).expect("the sandbox state is removed");
        self.forget_marker();
    }

    fn forget_marker(&self) {
        let _ = std::fs::remove_file(self.marker());
    }

    fn request(
        &self,
        store: Arc<dyn RunStore>,
        runtime: &RuntimeSpec,
        resume: bool,
    ) -> engine::RunRequest {
        let mut request = run_request(
            &self.run_id,
            &self.run_dir(),
            self.graphs(runtime),
            store,
            runtime.clone(),
            no_questions(Arc::new(Silent)),
        );
        request.hooks = Some(hooks_spec());
        if resume {
            let Execution::Start(graphs) = request.execution else {
                unreachable!("run_request builds a start");
            };
            request.execution = Execution::Resume(graphs);
        }
        request
    }

    /// Run until the store fails on the append that carries `needle`, then
    /// wait for the interrupted run's teardown to finish: every handle it
    /// held on the run's store is dropped.
    async fn interrupt_on(&self, memory: &Arc<MemoryRunStore>, needle: &'static str) {
        let runtime = RuntimeSpec::default();
        let handles = OpenHandles::new();
        let failing = Arc::new(FailOnText {
            inner: Arc::clone(memory),
            needle,
            failed: Arc::new(AtomicUsize::new(0)),
            open: handles.hold(),
        });
        let first = engine::run(self.request(failing, &runtime, false)).await;
        assert!(
            matches!(&first, Err(RunError::StoreFailed(message)) if message.contains("the disk is full")),
            "{first:?}"
        );
        handles.all_closed().await;
    }

    async fn resume(&self, memory: &Arc<MemoryRunStore>) -> engine::RunOutcome {
        let runtime = RuntimeSpec::default();
        engine::run(self.request(Arc::clone(memory) as Arc<dyn RunStore>, &runtime, true))
            .await
            .expect("the resumed run ends")
    }
}

impl Drop for PrepareFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.marker());
    }
}

/// Fabro's hooks over in-memory records, with no repository source.
fn hooks_spec() -> HooksSpec {
    HooksSpec {
        records:         Arc::new(MemoryPlatformRecords::new()),
        git:             RunGitSettings::default(),
        artifacts:       Vec::new(),
        test_gates:      None,
        artifact_writer: Arc::new(StoreArtifactWriter::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            "artifacts",
        ))),
        source:          None,
        publisher:       None,
    }
}

/// A run that is never interrupted runs its prepare block once, through
/// its nodes, and leaves its sandbox marked.
#[tokio::test]
async fn a_run_never_interrupted_runs_its_prepare_block_once() {
    let fixture = PrepareFixture::new();
    let memory = Arc::new(MemoryRunStore::new());
    let runtime = RuntimeSpec::default();

    let outcome =
        engine::run(fixture.request(Arc::clone(&memory) as Arc<dyn RunStore>, &runtime, false))
            .await
            .expect("the run ends");

    assert_eq!(outcome.status, RunStatus::Success, "{outcome:?}");
    assert_eq!(fixture.order(), ["prepare1", "prepare2", "work"]);
    assert!(fixture.marker().exists(), "the sandbox carries the marker");
}

/// The run is interrupted in `work`, after its prepare block completed, and
/// resumes onto a re-acquired sandbox that kept only its workspace: the
/// prepare block runs again, in order, before the first node the resume
/// executes, and the run completes.
#[tokio::test]
async fn a_resume_onto_a_reacquired_sandbox_runs_the_prepare_block_before_its_first_node() {
    let fixture = PrepareFixture::new();
    let memory = Arc::new(MemoryRunStore::new());
    fixture.interrupt_on(&memory, "WORK-RAN").await;
    assert_eq!(fixture.order(), ["prepare1", "prepare2", "work"]);
    fixture.reacquire_sandbox();

    let outcome = fixture.resume(&memory).await;

    assert_eq!(outcome.status, RunStatus::Success, "{outcome:?}");
    assert!(outcome.complete, "{:?}", outcome.incomplete);
    assert_eq!(fixture.order(), [
        "prepare1", "prepare2", "work", "prepare1", "prepare2", "work"
    ]);
    assert!(fixture.marker().exists(), "the resumed sandbox is marked");
}

/// A resume onto the sandbox the run left, marker and all, runs nothing
/// again: only the interrupted node fires.
#[tokio::test]
async fn a_resume_onto_the_surviving_sandbox_does_not_run_the_prepare_block_again() {
    let fixture = PrepareFixture::new();
    let memory = Arc::new(MemoryRunStore::new());
    fixture.interrupt_on(&memory, "WORK-RAN").await;

    let outcome = fixture.resume(&memory).await;

    assert_eq!(outcome.status, RunStatus::Success, "{outcome:?}");
    assert_eq!(fixture.order(), ["prepare1", "prepare2", "work", "work"]);
}

/// The full prepare block runs a second time in the sandbox it already
/// ran in, and leaves the sandbox exactly as one run left it.
#[tokio::test]
async fn running_the_prepare_block_twice_in_one_sandbox_leaves_the_same_state() {
    let fixture = PrepareFixture::new();
    let memory = Arc::new(MemoryRunStore::new());
    fixture.interrupt_on(&memory, "WORK-RAN").await;
    let once = fixture.sandbox_state();
    assert!(!once.is_empty(), "the prepare block left its files");
    fixture.forget_marker();

    let outcome = fixture.resume(&memory).await;

    assert_eq!(outcome.status, RunStatus::Success, "{outcome:?}");
    assert_eq!(fixture.order(), [
        "prepare1", "prepare2", "work", "prepare1", "prepare2", "work"
    ]);
    assert_eq!(
        fixture.sandbox_state(),
        once,
        "a second run changed nothing"
    );
}

/// The host error lands while the second prepare step is in flight, after
/// the first one completed; the resume lands on a re-acquired sandbox. The
/// prepare block runs again from its first step, the in-flight step fires
/// again, and the run completes.
#[tokio::test]
async fn a_host_error_during_an_in_flight_prepare_step_still_resumes_and_completes() {
    let fixture = PrepareFixture::new();
    let memory = Arc::new(MemoryRunStore::new());
    fixture.interrupt_on(&memory, "PREPARE-TWO-RAN").await;
    assert_eq!(fixture.order(), ["prepare1", "prepare2"]);
    fixture.reacquire_sandbox();

    let outcome = fixture.resume(&memory).await;

    assert_eq!(outcome.status, RunStatus::Success, "{outcome:?}");
    assert!(outcome.complete, "{:?}", outcome.incomplete);
    assert_eq!(fixture.order(), [
        "prepare1", "prepare2", "prepare1", "prepare2", "prepare2", "work"
    ]);
}

/// The in-memory store, failing the first append whose records carry
/// `needle` (a script's own output) as a full disk would.
struct FailOnText {
    inner:  Arc<MemoryRunStore>,
    needle: &'static str,
    failed: Arc<AtomicUsize>,
    open:   Held,
}

#[async_trait::async_trait]
impl RunStore for FailOnText {
    async fn open(&self, key: &RunKey, access: Access) -> Result<Arc<dyn RunLogs>, StoreError> {
        Ok(Arc::new(FailOnTextLogs {
            inner:  self.inner.open(key, access).await?,
            needle: self.needle,
            failed: Arc::clone(&self.failed),
            _open:  self.open.again(),
        }))
    }
}

struct FailOnTextLogs {
    inner:  Arc<dyn RunLogs>,
    needle: &'static str,
    failed: Arc<AtomicUsize>,
    _open:  Held,
}

#[async_trait::async_trait]
impl RunLogs for FailOnTextLogs {
    fn locator(&self) -> String {
        self.inner.locator()
    }

    async fn append(&self, log: &LogId, records: &[Record]) -> Result<(), StoreError> {
        let carries = records
            .iter()
            .any(|record| record.record.to_string().contains(self.needle));
        if carries && self.failed.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(StoreError::backend(
                self.locator(),
                "append",
                "the disk is full",
            ));
        }
        self.inner.append(log, records).await
    }

    async fn read(&self, log: &LogId) -> Result<Vec<Record>, StoreError> {
        self.inner.read(log).await
    }

    async fn put_blob(&self, bytes: &[u8]) -> Result<Digest, StoreError> {
        self.inner.put_blob(bytes).await
    }

    async fn get_blob(&self, digest: Digest) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get_blob(digest).await
    }
}

/// The handles a run holds on its store: the store itself and every log
/// handle opened through it. A run's teardown is over when the last one
/// drops, whatever task or thread held it, so a test waits on that event
/// rather than on a count of live tasks or a clock. Once the test has handed
/// the store over and the run has returned, nothing opens a new handle
/// except through one still held, so the count reaching zero is final.
#[derive(Clone)]
struct OpenHandles(Arc<watch::Sender<usize>>);

impl OpenHandles {
    fn new() -> Self {
        Self(Arc::new(watch::channel(0).0))
    }

    /// Count one more handle, until the guard drops.
    fn hold(&self) -> Held {
        self.0.send_modify(|open| *open += 1);
        Held(self.clone())
    }

    /// Wait until every handle has dropped.
    async fn all_closed(&self) {
        self.0
            .subscribe()
            .wait_for(|open| *open == 0)
            .await
            .expect("the sender lives as long as these handles");
    }
}

/// One counted handle, released on drop.
struct Held(OpenHandles);

impl Held {
    /// A handle opened through this one.
    fn again(&self) -> Self {
        self.0.hold()
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.0.0.send_modify(|open| *open -= 1);
    }
}

/// The in-memory store, passed through, with its handles counted.
struct Tracked {
    inner: Arc<MemoryRunStore>,
    open:  Held,
}

#[async_trait::async_trait]
impl RunStore for Tracked {
    async fn open(&self, key: &RunKey, access: Access) -> Result<Arc<dyn RunLogs>, StoreError> {
        Ok(Arc::new(TrackedLogs {
            inner: self.inner.open(key, access).await?,
            _open: self.open.again(),
        }))
    }
}

struct TrackedLogs {
    inner: Arc<dyn RunLogs>,
    _open: Held,
}

#[async_trait::async_trait]
impl RunLogs for TrackedLogs {
    fn locator(&self) -> String {
        self.inner.locator()
    }

    async fn append(&self, log: &LogId, records: &[Record]) -> Result<(), StoreError> {
        self.inner.append(log, records).await
    }

    async fn read(&self, log: &LogId) -> Result<Vec<Record>, StoreError> {
        self.inner.read(log).await
    }

    async fn put_blob(&self, bytes: &[u8]) -> Result<Digest, StoreError> {
        self.inner.put_blob(bytes).await
    }

    async fn get_blob(&self, digest: Digest) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get_blob(digest).await
    }
}

/// Setup authored as `run_prepare_<N>` command nodes, with no `[run.prepare]`
/// in the settings, runs again on a re-acquired sandbox before the first
/// resumed node, exactly as the lowered form does.
#[tokio::test]
async fn authored_run_prepare_nodes_run_again_on_a_reacquired_sandbox() {
    let fixture = PrepareFixture::authored("run_prepare_");
    let memory = Arc::new(MemoryRunStore::new());
    fixture.interrupt_on(&memory, "WORK-RAN").await;
    assert_eq!(fixture.order(), ["prepare1", "prepare2", "work"]);
    fixture.reacquire_sandbox();

    let outcome = fixture.resume(&memory).await;

    assert_eq!(outcome.status, RunStatus::Success, "{outcome:?}");
    assert!(outcome.complete, "{:?}", outcome.incomplete);
    assert_eq!(fixture.order(), [
        "prepare1", "prepare2", "work", "prepare1", "prepare2", "work"
    ]);
    assert!(fixture.marker().exists(), "the resumed sandbox is marked");
}

/// A resume onto the surviving sandbox runs authored setup nodes no second time.
#[tokio::test]
async fn authored_run_prepare_nodes_do_not_run_again_on_the_surviving_sandbox() {
    let fixture = PrepareFixture::authored("run_prepare_");
    let memory = Arc::new(MemoryRunStore::new());
    fixture.interrupt_on(&memory, "WORK-RAN").await;

    let outcome = fixture.resume(&memory).await;

    assert_eq!(outcome.status, RunStatus::Success, "{outcome:?}");
    assert_eq!(fixture.order(), ["prepare1", "prepare2", "work", "work"]);
}

/// The name is the contract: authored setup under any other prefix is never
/// run again, so a migrated workflow must keep the `run_prepare_<N>` names.
#[tokio::test]
async fn authored_setup_under_another_name_does_not_run_again() {
    let fixture = PrepareFixture::authored("setup_");
    let memory = Arc::new(MemoryRunStore::new());
    fixture.interrupt_on(&memory, "WORK-RAN").await;
    assert_eq!(fixture.order(), ["prepare1", "prepare2", "work"]);
    fixture.reacquire_sandbox();

    let _outcome = fixture.resume(&memory).await;

    assert_eq!(fixture.order(), ["prepare1", "prepare2", "work"]);
}
