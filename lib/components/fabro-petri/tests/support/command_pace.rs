use std::sync::Arc;
use std::time::Duration;

use petri_execution::watchdog::ManualClock;
use petri_execution::{CoordinatorRecord, ExecutionId, ExecutionObserver};
use petri_runtime::engine::{EngineState, EventRecord};

/// How a shell command spends time on the run's manual stall clock.
pub(crate) struct OutputPace {
    #[expect(dead_code, reason = "RED stub: the clock never moves")]
    clock: ManualClock,
    #[expect(dead_code, reason = "RED stub: the clock never moves")]
    gap:   Duration,
}

impl OutputPace {
    pub(crate) fn new(clock: ManualClock, gap: Duration) -> Arc<Self> {
        Arc::new(Self { clock, gap })
    }
}

impl ExecutionObserver for OutputPace {
    fn on_engine_record(&self, _: ExecutionId, _: &EventRecord, _: u64, _: &EngineState) {}

    fn on_lifecycle(&self, _: &CoordinatorRecord) {}
}
