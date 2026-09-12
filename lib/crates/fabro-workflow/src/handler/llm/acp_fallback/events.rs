//! Assembly of the versioned, redacted `agent.acp.failover` and
//! `agent.acp.side_effect` bodies.
//!
//! Redaction is BY CONSTRUCTION: these functions take the candidate records
//! and the typed verdict, and copy only identities, indexes, durations, the
//! typed cause and the chain digests. A candidate's `command` never reaches a
//! props field, and the test below pins that by serializing a body built from
//! a chain whose command carries a fake credential.

use fabro_types::{AgentAcpExhaustedProps, AgentAcpFailoverProps, AgentAcpSideEffectProps};

use super::FAILOVER_EVENT_SCHEMA_VERSION;
use super::chain::{AcpChainCandidate, AcpFallbackChain};
use super::classify::AvailabilityFailure;

/// Why a transition happened, for the event's `transition` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionKind {
    /// The executing candidate failed on a typed availability condition.
    Reactive,
    /// The Dispatcher's preflight had already skipped the earlier candidate.
    Preflight,
}

impl TransitionKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reactive => "reactive",
            Self::Preflight => "preflight",
        }
    }
}

/// What the assembled event says about the typed cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionCause {
    pub cause:    String,
    pub scope:    String,
    pub hold_key: String,
    pub source:   String,
}

impl From<&AvailabilityFailure> for TransitionCause {
    fn from(failure: &AvailabilityFailure) -> Self {
        Self {
            cause:    failure.cause.to_string(),
            scope:    failure.scope.to_string(),
            hold_key: failure.hold_key.clone(),
            source:   failure.source.to_string(),
        }
    }
}

/// Everything one transition needs beyond the two candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionRecord {
    pub kind:                    TransitionKind,
    pub event_id:                String,
    pub occurred_at_ms:          u64,
    pub visit:                   u32,
    pub engine_attempt:          u32,
    pub from_duration_ms:        u64,
    pub cause:                   TransitionCause,
    pub attempted:               Vec<u32>,
    pub attempted_durations_ms:  Vec<u64>,
    pub skipped:                 Vec<u32>,
    pub chain_deadline_epoch_ms: Option<u64>,
}

/// Build the redacted failover body for a transition from `from` to `to`.
#[must_use]
pub fn failover_props(
    chain: &AcpFallbackChain,
    from: &AcpChainCandidate,
    to: &AcpChainCandidate,
    record: &TransitionRecord,
) -> AgentAcpFailoverProps {
    AgentAcpFailoverProps {
        schema_version:          FAILOVER_EVENT_SCHEMA_VERSION,
        event_id:                record.event_id.clone(),
        occurred_at_ms:          record.occurred_at_ms,
        visit:                   record.visit,
        engine_attempt:          record.engine_attempt,
        transition:              record.kind.as_str().to_string(),
        from_candidate_index:    from.candidate_index,
        to_candidate_index:      to.candidate_index,
        from_display_name:       from.display_name.clone(),
        to_display_name:         to.display_name.clone(),
        from_candidate_key:      from.candidate_key.clone(),
        from_availability_key:   from.availability_key.clone(),
        to_candidate_key:        to.candidate_key.clone(),
        to_availability_key:     to.availability_key.clone(),
        from_duration_ms:        record.from_duration_ms,
        hold_key:                record.cause.hold_key.clone(),
        cause:                   record.cause.cause.clone(),
        scope:                   record.cause.scope.clone(),
        signature_source:        record.cause.source.clone(),
        primary_generation:      chain.primary_generation.clone(),
        full_chain:              chain.full_chain.clone(),
        attempted:               record.attempted.clone(),
        attempted_durations_ms:  record.attempted_durations_ms.clone(),
        skipped:                 record.skipped.clone(),
        chain_deadline_epoch_ms: record.chain_deadline_epoch_ms,
    }
}

/// Build the redacted exhaustion body for the FINAL candidate of a visit.
#[must_use]
pub fn exhausted_props(
    chain: &AcpFallbackChain,
    last: &AcpChainCandidate,
    record: &TransitionRecord,
) -> AgentAcpExhaustedProps {
    AgentAcpExhaustedProps {
        schema_version:          FAILOVER_EVENT_SCHEMA_VERSION,
        event_id:                record.event_id.clone(),
        occurred_at_ms:          record.occurred_at_ms,
        visit:                   record.visit,
        engine_attempt:          record.engine_attempt,
        terminal:                record.kind.as_str().to_string(),
        candidate_index:         last.candidate_index,
        display_name:            last.display_name.clone(),
        candidate_key:           last.candidate_key.clone(),
        availability_key:        last.availability_key.clone(),
        duration_ms:             record.from_duration_ms,
        hold_key:                record.cause.hold_key.clone(),
        cause:                   record.cause.cause.clone(),
        scope:                   record.cause.scope.clone(),
        signature_source:        record.cause.source.clone(),
        primary_generation:      chain.primary_generation.clone(),
        full_chain:              chain.full_chain.clone(),
        attempted:               record.attempted.clone(),
        attempted_durations_ms:  record.attempted_durations_ms.clone(),
        skipped:                 record.skipped.clone(),
        chain_deadline_epoch_ms: record.chain_deadline_epoch_ms,
    }
}

/// Bound for the tool title carried in a side-effect ledger entry.
const SIDE_EFFECT_TITLE_MAX_BYTES: usize = 120;

/// How a ledger entry was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedVia {
    /// From the adapter's `session/request_permission`, BEFORE the answer.
    Permission,
    /// From the adapter's tool-started notification.
    ToolStarted,
}

impl ObservedVia {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Permission => "permission",
            Self::ToolStarted => "tool_started",
        }
    }
}

/// ACP tool kinds that provably cannot reach outside the sandbox.
const SANDBOX_LOCAL_TOOL_KINDS: &[&str] = &[
    "read",
    "search",
    "think",
    "edit",
    "delete",
    "move",
    "switch_mode",
];

/// Whether a tool of this kind is provably sandbox-local. `execute`, `fetch`,
/// `other` and anything this build does not recognise are NOT: there is no
/// way to prove a shell command or a network fetch stayed inside the sandbox.
#[must_use]
pub fn tool_kind_is_sandbox_local(kind: &str) -> bool {
    SANDBOX_LOCAL_TOOL_KINDS.contains(&kind)
}

/// Classification string for a side-effect ledger entry.
#[must_use]
pub fn classification_for(kind: &str) -> &'static str {
    if tool_kind_is_sandbox_local(kind) {
        "sandbox_local"
    } else {
        "external_or_unknown"
    }
}

/// Build the redacted side-effect ledger body.
#[must_use]
pub fn side_effect_props(
    visit: u32,
    candidate_index: u32,
    tool_call_id: &str,
    tool_kind: &str,
    title: &str,
    observed_via: ObservedVia,
) -> AgentAcpSideEffectProps {
    let mut bounded = title.to_string();
    if bounded.len() > SIDE_EFFECT_TITLE_MAX_BYTES {
        let mut cut = SIDE_EFFECT_TITLE_MAX_BYTES;
        while !bounded.is_char_boundary(cut) {
            cut -= 1;
        }
        bounded.truncate(cut);
    }
    AgentAcpSideEffectProps {
        schema_version: FAILOVER_EVENT_SCHEMA_VERSION,
        visit,
        candidate_index,
        tool_call_id: tool_call_id.to_string(),
        tool_kind: tool_kind.to_string(),
        title: bounded,
        classification: classification_for(tool_kind).to_string(),
        observed_via: observed_via.as_str().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::llm::acp_fallback::chain::parse_chain;

    const FAKE_SECRET: &str = "sk-ant-FAKE-SECRET-0123456789";

    fn chain_with_secret() -> AcpFallbackChain {
        let primary = format!("ANTHROPIC_AUTH_TOKEN={FAKE_SECRET} npx -y claude-agent-acp");
        let raw = serde_json::json!({
            "schema_version": 1,
            "primary_generation": "p".repeat(32),
            "full_chain": "f".repeat(32),
            "candidates": [
                {"candidate_index": 0, "display_name": "primary", "candidate_key": "k0",
                 "availability_key": "anthropic", "command": primary},
                {"candidate_index": 1, "display_name": "fallback", "candidate_key": "k1",
                 "availability_key": "codex", "command": format!("OPENAI_API_KEY={FAKE_SECRET} codex-acp")}
            ]
        })
        .to_string();
        parse_chain(&raw, Some(&primary), None).unwrap()
    }

    fn record() -> TransitionRecord {
        TransitionRecord {
            kind:                    TransitionKind::Reactive,
            event_id:                "evt-1".to_string(),
            occurred_at_ms:          1_789_000_000_000,
            visit:                   1,
            engine_attempt:          1,
            from_duration_ms:        4120,
            cause:                   TransitionCause {
                cause:    "model_unsupported".to_string(),
                scope:    "candidate".to_string(),
                hold_key: "anthropic".to_string(),
                source:   "process.terminal_diagnostic".to_string(),
            },
            attempted:               vec![0],
            attempted_durations_ms:  vec![4120],
            skipped:                 vec![],
            chain_deadline_epoch_ms: Some(1_789_001_800_000),
        }
    }

    #[test]
    fn failover_body_carries_identities_and_digests_and_nothing_secret() {
        let chain = chain_with_secret();
        let props = failover_props(
            &chain,
            chain.candidate(0).unwrap(),
            chain.candidate(1).unwrap(),
            &record(),
        );
        let json = serde_json::to_string(&props).unwrap();
        assert!(!json.contains(FAKE_SECRET), "credential leaked: {json}");
        assert!(!json.contains("npx"), "command leaked: {json}");
        assert!(!json.contains("codex-acp"), "command leaked: {json}");
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        for forbidden in [
            "command",
            "env",
            "prompt",
            "error",
            "diagnostic",
            "stderr",
            "stdout",
        ] {
            assert!(
                value.get(forbidden).is_none(),
                "forbidden key {forbidden} present"
            );
        }
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["transition"], "reactive");
        assert_eq!(value["from_candidate_index"], 0);
        assert_eq!(value["to_candidate_index"], 1);
        assert_eq!(value["from_availability_key"], "anthropic");
        assert_eq!(value["to_availability_key"], "codex");
        assert_eq!(value["primary_generation"], "p".repeat(32));
        assert_eq!(value["full_chain"], "f".repeat(32));
        assert_eq!(value["attempted"], serde_json::json!([0]));
        assert_eq!(value["chain_deadline_epoch_ms"], 1_789_001_800_000_u64);
    }

    #[test]
    fn side_effect_body_classifies_kinds_and_bounds_the_title() {
        let long_title = "x".repeat(500);
        let props = side_effect_props(
            2,
            1,
            "call-1",
            "execute",
            &long_title,
            ObservedVia::Permission,
        );
        assert_eq!(props.classification, "external_or_unknown");
        assert_eq!(props.observed_via, "permission");
        assert_eq!(props.title.len(), SIDE_EFFECT_TITLE_MAX_BYTES);
        for kind in [
            "read",
            "search",
            "think",
            "edit",
            "delete",
            "move",
            "switch_mode",
        ] {
            assert_eq!(classification_for(kind), "sandbox_local", "{kind}");
        }
        for kind in ["execute", "fetch", "other", "", "teleport"] {
            assert_eq!(classification_for(kind), "external_or_unknown", "{kind:?}");
        }
    }
}
