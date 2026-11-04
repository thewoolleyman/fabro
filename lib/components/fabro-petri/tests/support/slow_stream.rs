use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::StreamExt as _;
use futures_util::stream::once;
use lithos_llm::Client;
use lithos_llm::adapter::{ProviderAdapter, ResolvedCall};
use lithos_llm::catalog::AdapterId;
use lithos_llm::types::{Error, Response, ResponseStream, StreamEvent};
use pebble_coding_agent::test_support::{self, ScriptedCall, ScriptedProvider};
use petri_execution::watchdog::ManualClock;
use petri_execution::{CoordinatorRecord, ExecutionId, ExecutionObserver};
use petri_runtime::engine::{EngineState, EventRecord};
use tokio::sync::watch;

/// How the fake model spends time on the run's manual stall clock.
///
/// It advances the clock by `gap` only once the run has recorded something
/// since its last advance, so it can never run ahead of the run's own
/// records: however long the host takes to start the sandbox or to record an
/// event, the stall watchdog sees exactly `gap` between the model's steps
/// and the activity they follow. It observes the run after the watchdog
/// (`engine::run_with_stall_clock`), so it never moves the clock on a record
/// the watchdog has not yet counted.
pub(crate) struct Pace {
    clock:   ManualClock,
    gap:     Duration,
    records: watch::Sender<u64>,
    mark:    Mutex<u64>,
}

impl Pace {
    pub(crate) fn new(clock: ManualClock, gap: Duration) -> Arc<Self> {
        Arc::new(Self {
            clock,
            gap,
            records: watch::Sender::new(0),
            mark: Mutex::new(0),
        })
    }

    /// Wait for a record newer than the last advance, then advance `gap`.
    async fn step(&self) {
        let mut records = self.records.subscribe();
        loop {
            {
                let seen = *records.borrow_and_update();
                let mut mark = self.mark.lock().unwrap_or_else(PoisonError::into_inner);
                if seen > *mark {
                    *mark = seen;
                    self.clock.advance(self.gap);
                    return;
                }
            }
            if records.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    fn recorded(&self) {
        self.records.send_modify(|count| *count += 1);
    }
}

impl ExecutionObserver for Pace {
    fn on_engine_record(&self, _: ExecutionId, _: &EventRecord, _: u64, _: &EngineState) {
        self.recorded();
    }

    fn on_lifecycle(&self, _: &CoordinatorRecord) {
        self.recorded();
    }
}

struct PacedStream {
    inner:  ScriptedProvider,
    pace:   Arc<Pace>,
    /// The model never streams: it only lets time pass, after every record.
    silent: bool,
}

#[async_trait::async_trait]
impl ProviderAdapter for PacedStream {
    fn id(&self) -> &AdapterId {
        self.inner.id()
    }
    async fn complete(&self, call: &ResolvedCall) -> Result<Response, Error> {
        self.inner.complete(call).await
    }
    async fn stream(&self, call: &ResolvedCall) -> Result<ResponseStream, Error> {
        let stream = self.inner.stream(call).await?;
        let pace = self.pace.clone();
        if self.silent {
            return Ok(ResponseStream::new(once(async move {
                loop {
                    pace.step().await;
                }
            })));
        }
        Ok(ResponseStream::new(stream.then(move |event| {
            let pace = pace.clone();
            async move {
                if matches!(event, Ok(StreamEvent::TextDelta { .. })) {
                    pace.step().await;
                }
                event
            }
        })))
    }
}

/// A client whose model streams fifteen text deltas, each one `pace` step
/// after the last; with `silent`, a model that streams nothing at all.
pub(crate) fn client(pace: Arc<Pace>, silent: bool) -> Client {
    let response = test_support::text_response(&"tick ".repeat(15));
    let mut events = Vec::new();
    for event in test_support::events_for(&response) {
        if let Ok(StreamEvent::TextDelta { id, .. }) = &event {
            for _ in 0..15 {
                events.push(Ok(StreamEvent::TextDelta {
                    id:   id.clone(),
                    text: "tick ".to_string(),
                }));
            }
        } else {
            events.push(event);
        }
    }
    let adapter = Arc::new(PacedStream {
        inner: ScriptedProvider::new(vec![ScriptedCall::Events(events)]),
        pace,
        silent,
    });
    Client::builder()
        .catalog(test_support::test_catalog())
        .adapter_arc("test", adapter)
        .build()
        .expect("paced streaming client builds")
        .client
}
