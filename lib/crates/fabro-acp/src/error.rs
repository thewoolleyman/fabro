use fabro_types::{CommandTermination, ExecOutputTail};

use crate::command::AcpCommandError;
use crate::session::AcpTurnProgress;

#[derive(Debug)]
pub struct AcpProcessExit {
    pub termination:      CommandTermination,
    pub exit_code:        Option<i32>,
    pub exec_output_tail: Option<ExecOutputTail>,
}

impl std::fmt::Display for AcpProcessExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let exit_code = self
            .exit_code
            .map_or_else(|| "unknown".to_string(), |code| code.to_string());
        write!(
            f,
            "ACP process exited before protocol completed: termination={}, exit_code={exit_code}",
            self.termination
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AcpError {
    #[error(transparent)]
    Command(#[from] AcpCommandError),

    #[error(transparent)]
    Sandbox(#[from] fabro_sandbox::Error),

    #[error("ACP protocol error")]
    Protocol(#[source] agent_client_protocol::Error),

    #[error("ACP turn was cancelled")]
    Cancelled,

    #[error("ACP turn timed out")]
    TimedOut {
        exec_output_tail: Option<ExecOutputTail>,
        /// What the agent had done before the deadline, so a working agent
        /// is never reported as zero-output.
        progress:         AcpTurnProgress,
    },

    #[error("{0}")]
    ProcessExited(AcpProcessExit),

    /// A `session/request_permission` parked on a human question that was
    /// not answered before the question's deadline; the turn was ended so
    /// the node can route to its human-gate edge instead of hanging.
    #[error(
        "ACP permission question for tool call {tool_call_id} ({title}) went unanswered past its deadline"
    )]
    PermissionTimedOut {
        tool_call_id: String,
        title:        String,
    },

    #[error("ACP prompt stopped with {stop_reason}: {text}")]
    StopReason {
        stop_reason: String,
        text:        String,
    },
}

impl AcpError {
    #[must_use]
    pub fn exec_output_tail(&self) -> Option<ExecOutputTail> {
        match self {
            Self::TimedOut {
                exec_output_tail, ..
            } => exec_output_tail.clone(),
            Self::ProcessExited(exit) => exit.exec_output_tail.clone(),
            Self::Sandbox(source) => source.default_redacted_output_tail(),
            _ => None,
        }
    }
}

impl From<agent_client_protocol::Error> for AcpError {
    fn from(error: agent_client_protocol::Error) -> Self {
        Self::Protocol(error)
    }
}
