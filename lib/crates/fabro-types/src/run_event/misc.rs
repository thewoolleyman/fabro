use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::ExecOutputTail;
use crate::{CommandTermination, PullRequestLink};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct InterviewOption {
    pub key:         String,
    pub label:       String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview:     Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParallelStartedProps {
    pub visit:        u32,
    pub branch_count: usize,
    pub join_policy:  String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParallelBranchStartedProps {
    pub index: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParallelBranchCompletedProps {
    pub index:       usize,
    pub duration_ms: u64,
    pub status:      String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_sha:    Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParallelCompletedProps {
    pub visit:         u32,
    pub duration_ms:   u64,
    pub success_count: usize,
    pub failure_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results:       Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterviewStartedProps {
    #[serde(default)]
    pub question_id:     String,
    pub question:        String,
    #[serde(default)]
    pub stage:           String,
    pub question_type:   String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options:         Vec<InterviewOption>,
    #[serde(default)]
    pub allow_freeform:  bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_display: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterviewCompletedProps {
    #[serde(default)]
    pub question_id: String,
    pub question:    String,
    pub answer:      String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterviewTimeoutProps {
    #[serde(default)]
    pub question_id: String,
    pub question:    String,
    #[serde(default)]
    pub stage:       String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterviewInterruptedProps {
    #[serde(default)]
    pub question_id: String,
    pub question:    String,
    #[serde(default)]
    pub stage:       String,
    pub reason:      String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitCommitProps {
    pub sha: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitPushProps {
    pub branch:           String,
    pub success:          bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec_output_tail: Option<ExecOutputTail>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitBranchProps {
    pub branch: String,
    pub sha:    String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitWorktreeAddProps {
    pub path:   String,
    pub branch: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitWorktreeRemoveProps {
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitFetchProps {
    pub branch:  String,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitResetProps {
    pub sha: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeSelectedProps {
    pub from_node:          String,
    pub to_node:            String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label:              Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition:          Option<String>,
    pub reason:             String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_label:    Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suggested_next_ids: Vec<String>,
    pub stage_status:       String,
    pub is_jump:            bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoopRestartProps {
    pub from_node: String,
    pub to_node:   String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubgraphStartedProps {
    pub start_node: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubgraphCompletedProps {
    pub steps_executed: usize,
    pub status:         String,
    pub duration_ms:    u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StallWatchdogTimeoutProps {
    pub idle_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactCapturedProps {
    pub attempt:        u32,
    pub node_slug:      String,
    pub path:           String,
    pub mime:           String,
    pub content_md5:    String,
    pub content_sha256: String,
    pub bytes:          u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SshAccessReadyProps {
    pub ssh_command: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FailoverProps {
    pub from_provider: String,
    pub from_model:    String,
    pub to_provider:   String,
    pub to_model:      String,
    pub error:         String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandStartedProps {
    pub script:     String,
    pub command:    String,
    pub language:   String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandCompletedProps {
    pub output:         String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code:      Option<i32>,
    pub duration_ms:    u64,
    pub termination:    CommandTermination,
    #[serde(default)]
    pub output_bytes:   u64,
    #[serde(default)]
    pub live_streaming: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAcpStartedProps {
    pub visit:                   u32,
    pub command:                 String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_name:             Option<String>,
    /// Position in the node's ACP fallback chain when the node carries one;
    /// absent for a legacy single-adapter node. Additive: stored runs written
    /// before this field existed read back as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_index:         Option<u32>,
    /// The node visit's ORIGINAL wall-clock deadline (epoch milliseconds)
    /// shared by every candidate of a fallback chain, when the node carries a
    /// timeout. Persisted here so a resumed visit reuses it rather than
    /// starting a fresh clock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_deadline_epoch_ms: Option<u64>,
}

/// The typed terminal record of an ACP fallback chain that ran out of
/// candidates: the final candidate's identity and typed availability cause,
/// so a consumer projects the last hold from a structured field rather than
/// from prose. Emitted once per exhausted visit, before the node's
/// non-retryable outcome is reported. Versioned and redacted like the
/// failover event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAcpExhaustedProps {
    pub schema_version:          u32,
    pub event_id:                String,
    pub occurred_at_ms:          u64,
    pub visit:                   u32,
    pub engine_attempt:          u32,
    /// `reactive` when the final candidate failed on a typed condition;
    /// `preflight` when every candidate was skipped before any adapter ran.
    pub terminal:                String,
    pub candidate_index:         u32,
    pub display_name:            String,
    pub candidate_key:           String,
    pub availability_key:        String,
    pub duration_ms:             u64,
    pub hold_key:                String,
    pub cause:                   String,
    pub scope:                   String,
    pub signature_source:        String,
    pub primary_generation:      String,
    pub full_chain:              String,
    #[serde(default)]
    pub attempted:               Vec<u32>,
    #[serde(default)]
    pub attempted_durations_ms:  Vec<u64>,
    #[serde(default)]
    pub skipped:                 Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_deadline_epoch_ms: Option<u64>,
}

/// One entry of an ACP node's side-effect ledger: the engine observed that a
/// candidate is about to run (or has started running) a tool, BEFORE the
/// tool's effect can reach anything outside the sandbox. Written durably so a
/// later fallback decision, or a resumed visit, can prove whether every
/// completed operation of a failed candidate stayed sandbox-local. Versioned
/// and redacted: it names the tool kind and a bounded title, never the tool's
/// arguments or output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAcpSideEffectProps {
    /// Wire schema version of this event body; `1` today.
    pub schema_version:  u32,
    /// Node visit count (1-based).
    pub visit:           u32,
    /// Position in the node's ACP fallback chain.
    pub candidate_index: u32,
    /// The adapter's tool call id.
    pub tool_call_id:    String,
    /// The ACP tool kind (`read`, `edit`, `execute`, `fetch`, ...).
    pub tool_kind:       String,
    /// Bounded tool title as the adapter reported it.
    pub title:           String,
    /// `sandbox_local` when the kind provably cannot reach outside the
    /// sandbox, `external_or_unknown` otherwise.
    pub classification:  String,
    /// `permission` when recorded from the pre-execution permission request,
    /// `tool_started` when recorded from the tool-started notification.
    pub observed_via:    String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAcpCompletedProps {
    pub stdout:      String,
    pub stderr:      String,
    pub stop_reason: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAcpCancelledProps {
    pub stdout:      String,
    pub stderr:      String,
    pub duration_ms: u64,
}

/// Emitted when an ACP turn exceeds its deadline.
///
/// `stdout` is NOT the adapter process's stdout: it is the bounded tail of the
/// agent's message text (the `session/update` agent-message chunks streamed
/// before the deadline), or the explicit marker
/// `output not captured: no agent message text before the timeout` when no
/// text arrived. It is never empty.
///
/// `update_count == 0` is the zero-activity discriminator: the adapter sent
/// no `session/update` at all before the deadline (a turn that never
/// started), as opposed to a turn that was working and ran out of time, which
/// carries a positive count and a `last_activity_ms`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAcpTimedOutProps {
    /// Bounded tail of the agent's message text, or the explicit no-output
    /// marker; never the adapter process's stdout and never empty.
    pub stdout:           String,
    /// Tail of the adapter process's stderr, when one was captured.
    pub stderr:           String,
    /// Milliseconds from launch to the deadline.
    pub duration_ms:      u64,
    /// Tool calls the agent started before the deadline.
    #[serde(default)]
    pub tool_call_count:  u64,
    /// `session/update` notifications received before the deadline.
    #[serde(default)]
    pub update_count:     u64,
    /// Milliseconds from launch to the last update, if any arrived.
    #[serde(default)]
    pub last_activity_ms: Option<u64>,
}

/// One transition between two candidates of an ACP node's ordered fallback
/// chain: reactive (the executing candidate failed on a typed availability
/// condition) or preflight (the Dispatcher's preflight skipped the primary,
/// so the first EXECUTED candidate is not candidate zero).
///
/// Versioned (`schema_version`) and redacted BY CONSTRUCTION: identities,
/// indexes, durations, the typed cause and the chain digests only. It carries
/// no command, env value, credential, prompt, raw error or diagnostic text,
/// and a unit test in `fabro-workflow` pins that. Distinct from the native
/// API-agent `agent.failover` event, which is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAcpFailoverProps {
    /// Wire schema version of this event body; `1` today.
    pub schema_version:          u32,
    /// Stable per-transition id, minted once at emission; consumers project
    /// by it, so a replayed stream yields one hold and one record.
    pub event_id:                String,
    /// Epoch milliseconds at which the transition was decided.
    pub occurred_at_ms:          u64,
    /// Node visit count (1-based) the transition belongs to.
    pub visit:                   u32,
    /// Engine handler attempt within the visit (1-based); distinct from the
    /// candidate index.
    pub engine_attempt:          u32,
    /// `reactive` or `preflight`.
    pub transition:              String,
    pub from_candidate_index:    u32,
    pub to_candidate_index:      u32,
    pub from_display_name:       String,
    pub to_display_name:         String,
    pub from_candidate_key:      String,
    pub from_availability_key:   String,
    pub to_candidate_key:        String,
    pub to_availability_key:     String,
    /// Milliseconds the failed candidate consumed before the transition;
    /// `0` for a preflight transition, where nothing executed.
    pub from_duration_ms:        u64,
    /// The hold key the typed cause resolved to (the domain key or override
    /// at domain scope; the availability key at candidate scope).
    pub hold_key:                String,
    /// One of the ratified availability causes.
    pub cause:                   String,
    /// `availability-domain` or `candidate`.
    pub scope:                   String,
    /// The signature source that matched (`process.terminal_diagnostic`,
    /// `protocol.machine_code`, `protocol.message`), or `preflight`.
    pub signature_source:        String,
    /// Primary-generation fingerprint: candidate zero alone.
    pub primary_generation:      String,
    /// Full-chain digest: every candidate in order.
    pub full_chain:              String,
    /// Candidate indexes attempted so far in this visit, including `from`.
    #[serde(default)]
    pub attempted:               Vec<u32>,
    /// Milliseconds each attempted candidate consumed, parallel to
    /// `attempted`.
    #[serde(default)]
    pub attempted_durations_ms:  Vec<u64>,
    /// Candidate indexes the preflight skipped for this visit.
    #[serde(default)]
    pub skipped:                 Vec<u32>,
    /// The node's ORIGINAL wall-clock deadline for this visit, when the node
    /// carries a timeout. Every candidate shares it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_deadline_epoch_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PullRequestCreatedProps {
    pub pr_url:      String,
    pub pr_number:   u64,
    pub owner:       String,
    pub repo:        String,
    pub base_branch: String,
    pub head_branch: String,
    pub title:       String,
    pub draft:       bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestLinkedProps {
    pub pull_request: PullRequestLink,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestUnlinkedProps {
    pub pull_request: PullRequestLink,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PullRequestFailedProps {
    pub error: String,
}
