pub mod command;

#[cfg(feature = "runtime")]
pub mod error;
#[cfg(feature = "runtime")]
pub mod session;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(feature = "runtime")]
mod transport;

pub use command::{AcpCommandError, AcpProcessSpec};
#[cfg(feature = "runtime")]
pub use error::{AcpError, AcpProcessExit, ConfigOptionRefusal};
#[cfg(feature = "runtime")]
pub use session::{
    AcpConfigOptionRequests, AcpControlHandle, AcpLiveControl, AcpPermissionAnswer,
    AcpPermissionObserver, AcpPermissionOption, AcpPermissionQuestion, AcpPermissionResolver,
    AcpRunRequest, AcpRunResult, AcpSessionConfigured, AcpSessionConfiguredHook, AcpToolEvent,
    AcpToolEventCallback, AcpTurnProgress, check_config_option, current_config_value,
    render_stop_reason, run_acp_turn,
};
