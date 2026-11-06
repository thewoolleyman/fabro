use std::sync::Arc;
use std::time::Duration;

use petri_execution::watchdog::ManualClock;
use petri_execution::{CoordinatorRecord, ExecutionId, ExecutionObserver};
use petri_runtime::engine::{EngineState, Event, EventRecord};
use petri_runtime::ir::StepEvent;

/// How a shell command spends time on the run's manual stall clock.
///
/// Each line a command prints is recorded as a `StepEvent::Log`, and every
/// such record advances the clock by `gap`. Nothing else moves it: however
/// long the host takes to start the sandbox, or the script takes to print
/// its next line, the stall watchdog sees exactly `gap` between one line and
/// the next. It observes the run after the watchdog
/// (`engine::run_with_stall_clock`), so the watchdog has already counted the
/// line as activity when the clock moves past it.
pub(crate) struct OutputPace {
    clock: ManualClock,
    gap:   Duration,
}

impl OutputPace {
    pub(crate) fn new(clock: ManualClock, gap: Duration) -> Arc<Self> {
        Arc::new(Self { clock, gap })
    }
}

impl ExecutionObserver for OutputPace {
    fn on_engine_record(&self, _: ExecutionId, record: &EventRecord, _: u64, _: &EngineState) {
        if matches!(record.event, Event::StepProgressRecorded {
            ev: StepEvent::Log { .. },
            ..
        }) {
            self.clock.advance(self.gap);
        }
    }

    fn on_lifecycle(&self, _: &CoordinatorRecord) {}
}
