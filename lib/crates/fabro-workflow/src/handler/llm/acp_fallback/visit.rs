//! The durable state of one node VISIT of a fallback-enabled ACP node, and
//! its reconstruction from the run's own event stream.
//!
//! The event stream is the ONE durable source: `agent.acp.started` (with a
//! candidate index) marks a candidate attempted and carries the visit's
//! original deadline; `agent.acp.failover` carries the attempted and skipped
//! sets, the event ids and the deadline; `agent.acp.side_effect` entries
//! rebuild each candidate's onset ledger; the ACP terminal events
//! (`completed`, `cancelled`, `timed_out`) close the most recently started
//! candidate. A started candidate with no later terminal record is attempted
//! with UNKNOWN onset, which the fallback gate treats as past onset.
//!
//! Nothing attempted or skipped is ever launched again, across outer retry,
//! checkpoint or resume: that is the contract's "never cycles to an attempted
//! or preflight-skipped candidate", and it is a property of this state rather
//! than of any caller remembering to check.

use std::collections::BTreeMap;
use std::time::Duration;

use fabro_types::{EventBody, RunEvent};

use super::chain::AcpFallbackChain;
use super::events::tool_kind_is_sandbox_local;

/// One side-effect ledger entry, as reconstructed or as recorded live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    pub tool_call_id:  String,
    pub tool_kind:     String,
    pub sandbox_local: bool,
    pub observed_via:  String,
}

/// How a candidate attempt ended, as far as the durable record shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateTerminal {
    /// Started, no terminal record: the attempt may still be running, or the
    /// engine died mid-attempt. Onset is UNKNOWN.
    InFlight,
    /// The turn completed normally.
    Completed,
    /// The turn ended abnormally (cancelled, timed out, or failed over).
    Failed,
}

/// The per-candidate record inside a visit.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CandidateRecord {
    pub ledger:       Vec<LedgerEntry>,
    pub terminal:     Option<CandidateTerminal>,
    /// Whether the adapter reported ANY activity after launch. An empty
    /// ledger is proof of nothing once the turn started: the ledger is fed by
    /// the adapter's own reports, so a started turn with no entries may have
    /// run an unreported tool. Reconstructed candidates are conservatively
    /// marked started.
    pub turn_started: bool,
    /// A durable `agent.acp.failover` from this candidate proves the gate was
    /// passed at decision time; a resumed visit must not re-litigate it.
    pub gate_passed:  bool,
    /// Milliseconds the attempt consumed, when known.
    pub duration_ms:  u64,
}

impl CandidateRecord {
    /// Whether the durable record PROVES no external effect began: the gate
    /// already passed, or the attempt reached a terminal record with every
    /// entry sandbox-local and the ledger non-empty unless the turn never
    /// started. `InFlight`, an external-or-unknown entry, or an empty ledger
    /// after a started turn all fail closed.
    #[must_use]
    pub fn onset_proven_absent(&self) -> bool {
        if self.gate_passed {
            return true;
        }
        matches!(
            self.terminal,
            Some(CandidateTerminal::Completed | CandidateTerminal::Failed)
        ) && self.ledger.iter().all(|entry| entry.sandbox_local)
            && (!self.ledger.is_empty() || !self.turn_started)
    }

    /// Whether the candidate performed any (sandbox-local) work that a
    /// successor should be told to inspect.
    #[must_use]
    pub fn has_local_work(&self) -> bool {
        !self.ledger.is_empty() && self.ledger.iter().all(|entry| entry.sandbox_local)
    }
}

/// Everything one visit knows about its chain execution.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VisitState {
    pub visit:                   u32,
    /// The ORIGINAL wall-clock deadline (epoch milliseconds), when the node
    /// carries a timeout.
    pub chain_deadline_epoch_ms: Option<u64>,
    /// Candidate indexes attempted so far, in attempt order.
    pub attempted:               Vec<u32>,
    /// Candidate indexes the preflight skipped, in chain order.
    pub skipped:                 Vec<u32>,
    /// `agent.acp.failover` event ids emitted so far, in order.
    pub event_ids:               Vec<String>,
    pub candidates:              BTreeMap<u32, CandidateRecord>,
    /// Whether at least one reactive transition happened in this visit.
    pub transitioned:            bool,
    /// Candidate indexes launched by THIS handler entry (never persisted):
    /// within one entry each candidate is tried at most once, whatever the
    /// pre-transition replay rule says about earlier entries.
    pub launched_here:           Vec<u32>,
    /// The durable exhaustion record, when the visit already ran out of
    /// candidates before this entry (a resume after exhaustion).
    pub exhausted:               Option<ExhaustedRecord>,
}

/// What a durable `agent.acp.exhausted` event recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExhaustedRecord {
    pub candidate_index: u32,
    pub display_name:    String,
    pub cause:           String,
    pub scope:           String,
}

impl VisitState {
    #[must_use]
    pub fn new(visit: u32, chain: &AcpFallbackChain) -> Self {
        Self {
            visit,
            skipped: chain
                .candidates
                .iter()
                .filter(|candidate| candidate.is_preflight_skipped())
                .map(|candidate| candidate.candidate_index)
                .collect(),
            ..Self::default()
        }
    }

    /// Reconstruct from the run's stored events, keeping those of `node_id`
    /// that belong to `visit`. Events are consumed in `seq` order, which is
    /// how the stored stream is listed.
    #[must_use]
    pub fn reconstruct(
        visit: u32,
        node_id: &str,
        chain: &AcpFallbackChain,
        events: &[RunEvent],
    ) -> Self {
        let mut state = Self::new(visit, chain);
        let mut current: Option<u32> = None;
        for event in events
            .iter()
            .filter(|event| event.node_id.as_deref() == Some(node_id))
        {
            match &event.body {
                EventBody::AgentAcpStarted(props) if props.visit == visit => {
                    let Some(index) = props.candidate_index else {
                        continue;
                    };
                    state.mark_attempted(index);
                    if state.chain_deadline_epoch_ms.is_none() {
                        state.chain_deadline_epoch_ms = props.chain_deadline_epoch_ms;
                    }
                    let record = state.record(index);
                    record.terminal = Some(CandidateTerminal::InFlight);
                    // Conservative: the stream cannot say whether the adapter
                    // did anything, so an empty ledger proves nothing.
                    record.turn_started = true;
                    current = Some(index);
                }
                EventBody::AgentAcpCompleted(_) => {
                    if let Some(index) = current.take() {
                        state.record(index).terminal = Some(CandidateTerminal::Completed);
                    }
                }
                EventBody::AgentAcpCancelled(_) | EventBody::AgentAcpTimedOut(_) => {
                    if let Some(index) = current.take() {
                        state.record(index).terminal = Some(CandidateTerminal::Failed);
                    }
                }
                EventBody::AgentAcpFailover(props) if props.visit == visit => {
                    for index in &props.attempted {
                        state.mark_attempted(*index);
                    }
                    for index in &props.skipped {
                        if !state.skipped.contains(index) {
                            state.skipped.push(*index);
                        }
                    }
                    if !state.event_ids.contains(&props.event_id) {
                        state.event_ids.push(props.event_id.clone());
                    }
                    if state.chain_deadline_epoch_ms.is_none() {
                        state.chain_deadline_epoch_ms = props.chain_deadline_epoch_ms;
                    }
                    for (index, duration) in props
                        .attempted
                        .iter()
                        .zip(props.attempted_durations_ms.iter())
                    {
                        state.record(*index).duration_ms = *duration;
                    }
                    if props.transition == "reactive" {
                        state.transitioned = true;
                        let record = state.record(props.from_candidate_index);
                        record.terminal = Some(CandidateTerminal::Failed);
                        record.gate_passed = true;
                        if current == Some(props.from_candidate_index) {
                            current = None;
                        }
                    }
                }
                EventBody::AgentAcpExhausted(props) if props.visit == visit => {
                    for index in &props.attempted {
                        state.mark_attempted(*index);
                    }
                    state.record(props.candidate_index).terminal = Some(CandidateTerminal::Failed);
                    state.exhausted = Some(ExhaustedRecord {
                        candidate_index: props.candidate_index,
                        display_name:    props.display_name.clone(),
                        cause:           props.cause.clone(),
                        scope:           props.scope.clone(),
                    });
                    current = None;
                }
                EventBody::AgentAcpSideEffect(props) if props.visit == visit => {
                    state
                        .record(props.candidate_index)
                        .ledger
                        .push(LedgerEntry {
                            tool_call_id:  props.tool_call_id.clone(),
                            tool_kind:     props.tool_kind.clone(),
                            sandbox_local: tool_kind_is_sandbox_local(&props.tool_kind),
                            observed_via:  props.observed_via.clone(),
                        });
                }
                _ => {}
            }
        }
        state
    }

    pub fn mark_attempted(&mut self, index: u32) {
        if !self.attempted.contains(&index) {
            self.attempted.push(index);
        }
    }

    /// Record a launch made by this handler entry.
    pub fn mark_launched(&mut self, index: u32) {
        self.mark_attempted(index);
        if !self.launched_here.contains(&index) {
            self.launched_here.push(index);
        }
    }

    pub fn record(&mut self, index: u32) -> &mut CandidateRecord {
        self.candidates.entry(index).or_default()
    }

    /// Whether this candidate may still be launched in this visit.
    ///
    /// Before the first reactive transition the contract keeps the legacy
    /// posture: an outer retry (or a resume) MAY retry the preflight-selected
    /// candidate, so an attempted-but-not-transitioned candidate stays
    /// launchable. After the first transition nothing attempted is ever
    /// launched again. Preflight-skipped candidates are never launched.
    #[must_use]
    pub fn is_launchable(&self, index: u32) -> bool {
        if self.skipped.contains(&index) || self.launched_here.contains(&index) {
            return false;
        }
        !self.transitioned || !self.attempted.contains(&index)
    }

    /// Milliseconds each attempted candidate consumed, parallel to `attempted`.
    #[must_use]
    pub fn attempted_durations_ms(&self) -> Vec<u64> {
        self.attempted
            .iter()
            .map(|index| {
                self.candidates
                    .get(index)
                    .map_or(0, |record| record.duration_ms)
            })
            .collect()
    }

    /// The next launchable candidate index in chain order, if any.
    #[must_use]
    pub fn next_launchable(&self, chain: &AcpFallbackChain) -> Option<u32> {
        chain
            .candidates
            .iter()
            .map(|candidate| candidate.candidate_index)
            .find(|index| self.is_launchable(*index))
    }

    /// Time left before the visit's original deadline, or `None` when the
    /// node carries no timeout. Zero once the deadline has passed.
    #[must_use]
    pub fn remaining(&self, now_epoch_ms: u64) -> Option<Duration> {
        self.chain_deadline_epoch_ms
            .map(|deadline| Duration::from_millis(deadline.saturating_sub(now_epoch_ms)))
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use fabro_types::RunId;
    use fabro_types::run_event::{
        AgentAcpCompletedProps, AgentAcpFailoverProps, AgentAcpSideEffectProps,
        AgentAcpStartedProps, AgentAcpTimedOutProps,
    };

    use super::*;
    use crate::handler::llm::acp_fallback::chain::parse_chain;

    const PRIMARY: &str = "A=1 primary-acp";

    fn chain() -> AcpFallbackChain {
        let raw = serde_json::json!({
            "schema_version": 1, "primary_generation": "p", "full_chain": "f",
            "candidates": [
                {"candidate_index": 0, "display_name": "zero", "candidate_key": "k0",
                 "availability_key": "a", "command": PRIMARY},
                {"candidate_index": 1, "display_name": "one", "candidate_key": "k1",
                 "availability_key": "b", "command": "B=1 one",
                 "preflight_skipped": {"cause": "quota", "scope": "availability-domain", "hold_key": "b"}},
                {"candidate_index": 2, "display_name": "two", "candidate_key": "k2",
                 "availability_key": "c", "command": "C=1 two"}
            ]
        })
        .to_string();
        parse_chain(&raw, Some(PRIMARY), None).unwrap()
    }

    fn event(node: &str, body: EventBody) -> RunEvent {
        RunEvent {
            id: uuid::Uuid::new_v4().to_string(),
            ts: Utc::now(),
            run_id: RunId::new(),
            node_id: Some(node.to_string()),
            node_label: None,
            stage_id: None,
            parallel_group_id: None,
            parallel_branch_id: None,
            session_id: None,
            parent_session_id: None,
            tool_call_id: None,
            actor: None,
            body,
        }
    }

    fn started(visit: u32, index: u32, deadline: Option<u64>) -> EventBody {
        EventBody::AgentAcpStarted(AgentAcpStartedProps {
            visit,
            command: "redacted".to_string(),
            config_name: None,
            candidate_index: Some(index),
            chain_deadline_epoch_ms: deadline,
        })
    }

    fn side_effect(visit: u32, index: u32, kind: &str) -> EventBody {
        EventBody::AgentAcpSideEffect(AgentAcpSideEffectProps {
            schema_version: 1,
            visit,
            candidate_index: index,
            tool_call_id: "t".to_string(),
            tool_kind: kind.to_string(),
            title: "t".to_string(),
            classification: "x".to_string(),
            observed_via: "permission".to_string(),
        })
    }

    fn failover(visit: u32, from: u32, to: u32, attempted: Vec<u32>) -> EventBody {
        EventBody::AgentAcpFailover(AgentAcpFailoverProps {
            schema_version: 1,
            event_id: format!("evt-{from}-{to}"),
            occurred_at_ms: 1,
            visit,
            engine_attempt: 1,
            transition: "reactive".to_string(),
            from_candidate_index: from,
            to_candidate_index: to,
            from_display_name: String::new(),
            to_display_name: String::new(),
            from_candidate_key: String::new(),
            from_availability_key: String::new(),
            to_candidate_key: String::new(),
            to_availability_key: String::new(),
            from_duration_ms: 0,
            hold_key: String::new(),
            cause: "quota".to_string(),
            scope: "availability-domain".to_string(),
            signature_source: "protocol.message".to_string(),
            primary_generation: "p".to_string(),
            full_chain: "f".to_string(),
            attempted,
            attempted_durations_ms: vec![],
            skipped: vec![1],
            chain_deadline_epoch_ms: Some(500),
        })
    }

    #[test]
    fn fresh_state_skips_preflight_skipped_candidates_and_launches_in_order() {
        let chain = chain();
        let state = VisitState::new(1, &chain);
        assert_eq!(state.skipped, vec![1]);
        assert_eq!(state.next_launchable(&chain), Some(0));
        let mut state = state;
        state.mark_launched(0);
        assert_eq!(state.next_launchable(&chain), Some(2));
        state.mark_launched(2);
        assert_eq!(state.next_launchable(&chain), None);
    }

    #[test]
    fn reconstruction_never_replays_an_attempted_or_skipped_candidate() {
        let chain = chain();
        let events = vec![
            event("pr", started(1, 0, Some(500))),
            event("pr", side_effect(1, 0, "read")),
            event("other", started(1, 0, Some(1))),
            event("pr", failover(1, 0, 2, vec![0])),
            event("pr", started(1, 2, Some(500))),
        ];
        let state = VisitState::reconstruct(1, "pr", &chain, &events);
        assert_eq!(state.attempted, vec![0, 2]);
        assert_eq!(state.skipped, vec![1]);
        assert_eq!(state.event_ids, vec!["evt-0-2"]);
        assert_eq!(state.chain_deadline_epoch_ms, Some(500));
        assert!(state.transitioned);
        assert_eq!(state.next_launchable(&chain), None);
        // Candidate zero: a durable failover proves the gate already passed.
        assert!(state.candidates[&0].gate_passed);
        assert!(state.candidates[&0].onset_proven_absent());
        assert!(state.candidates[&0].has_local_work());
        // Candidate two: started, no terminal: UNKNOWN, fails closed.
        assert_eq!(
            state.candidates[&2].terminal,
            Some(CandidateTerminal::InFlight)
        );
        assert!(!state.candidates[&2].onset_proven_absent());
    }

    #[test]
    fn other_visits_are_ignored_and_terminal_events_close_the_current_candidate() {
        let chain = chain();
        let events = vec![
            event("pr", started(1, 0, Some(9))),
            event(
                "pr",
                EventBody::AgentAcpTimedOut(AgentAcpTimedOutProps {
                    stdout:           String::new(),
                    stderr:           String::new(),
                    duration_ms:      1,
                    tool_call_count:  0,
                    update_count:     0,
                    last_activity_ms: None,
                }),
            ),
            event("pr", started(2, 0, Some(900))),
            event("pr", side_effect(2, 0, "execute")),
            event(
                "pr",
                EventBody::AgentAcpCompleted(AgentAcpCompletedProps {
                    stdout:      String::new(),
                    stderr:      String::new(),
                    stop_reason: "end_turn".to_string(),
                    duration_ms: 1,
                }),
            ),
        ];
        let first = VisitState::reconstruct(1, "pr", &chain, &events);
        assert_eq!(first.attempted, vec![0]);
        assert_eq!(first.chain_deadline_epoch_ms, Some(9));
        assert_eq!(
            first.candidates[&0].terminal,
            Some(CandidateTerminal::Failed)
        );
        // Reconstructed: the turn is conservatively started and the ledger is
        // empty, so absence of onset is NOT proven.
        assert!(!first.candidates[&0].onset_proven_absent());
        let second = VisitState::reconstruct(2, "pr", &chain, &events);
        assert_eq!(second.chain_deadline_epoch_ms, Some(900));
        assert_eq!(
            second.candidates[&0].terminal,
            Some(CandidateTerminal::Completed)
        );
        // An execute tool is not provably sandbox-local.
        assert!(!second.candidates[&0].onset_proven_absent());
    }

    #[test]
    fn before_a_transition_an_attempted_primary_stays_launchable_for_an_outer_retry() {
        let chain = chain();
        let events = vec![event("pr", started(1, 0, Some(500)))];
        let state = VisitState::reconstruct(1, "pr", &chain, &events);
        assert_eq!(state.attempted, vec![0]);
        assert!(!state.transitioned);
        assert_eq!(state.next_launchable(&chain), Some(0));
        let mut after = state.clone();
        after.transitioned = true;
        assert_eq!(after.next_launchable(&chain), Some(2));
    }

    #[test]
    fn empty_ledger_after_a_started_turn_is_not_proof() {
        let mut record = CandidateRecord {
            terminal: Some(CandidateTerminal::Failed),
            turn_started: true,
            ..CandidateRecord::default()
        };
        assert!(!record.onset_proven_absent());
        record.turn_started = false;
        assert!(record.onset_proven_absent());
        record.turn_started = true;
        record.ledger.push(LedgerEntry {
            tool_call_id:  "t".to_string(),
            tool_kind:     "read".to_string(),
            sandbox_local: true,
            observed_via:  "permission".to_string(),
        });
        assert!(record.onset_proven_absent());
        record.gate_passed = true;
        record.ledger.clear();
        assert!(record.onset_proven_absent());
    }

    #[test]
    fn remaining_time_is_clamped_and_absent_without_a_deadline() {
        let mut state = VisitState::new(1, &chain());
        assert_eq!(state.remaining(10), None);
        state.chain_deadline_epoch_ms = Some(1_000);
        assert_eq!(state.remaining(400), Some(Duration::from_millis(600)));
        assert_eq!(state.remaining(5_000), Some(Duration::ZERO));
    }
}
