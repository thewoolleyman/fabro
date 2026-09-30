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

/// Why a requested session config option was refused before the first prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigOptionRefusal {
    /// The agent's `configOptions` carry no option with the requested id.
    OptionNotAdvertised,
    /// The option exists but the requested value is not among its offered
    /// values.
    ValueNotOffered,
    /// `session/set_config_option` answered without reporting the requested
    /// value current.
    NotConfirmed,
}

impl ConfigOptionRefusal {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OptionNotAdvertised => "option_not_advertised",
            Self::ValueNotOffered => "value_not_offered",
            Self::NotConfirmed => "not_confirmed",
        }
    }
}

impl std::fmt::Display for ConfigOptionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
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

    #[error(
        "ACP tool call {tool_call_id} ({title}) continued in background as task {task_id}; Fabro terminated the turn before the agent could start conflicting work"
    )]
    BackgroundedTool {
        tool_call_id: String,
        title:        String,
        task_id:      String,
    },

    #[error("ACP process cleanup failed")]
    Cleanup(#[source] fabro_sandbox::Error),

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

    /// A requested session config option could not be set BEFORE the first
    /// prompt: the agent did not advertise the option id, did not offer the
    /// requested value, or did not report it current after
    /// `session/set_config_option`. The candidate has done no work, so this
    /// is a typed pre-turn refusal with its own identity.
    #[error(
        "ACP session config option {option_id}={requested} refused before any prompt ({reason}); the agent advertised [{}]",
        .advertised.join(", ")
    )]
    ConfigOptionRefused {
        option_id:  String,
        requested:  String,
        reason:     ConfigOptionRefusal,
        advertised: Vec<String>,
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
