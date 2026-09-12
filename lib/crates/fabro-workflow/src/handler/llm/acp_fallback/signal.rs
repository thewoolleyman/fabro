//! What one candidate attempt reported, and what outranks configured matching.
//!
//! The contract names a closed list of failures that are "non-eligible before
//! configured matching and MUST terminate with their original identity":
//! authentication, command-not-found, cancellation, node deadline or stall
//! expiry, generic HTTP 400/404, remote-compaction 404, malformed
//! command/configuration, signal exit, unattributed sandbox/DNS/transport
//! failure, code/test/review/tool failure, and non-convergence. It closes
//! with "A mere pre-turn failure is not provider evidence."
//!
//! Two detection routes, in a fixed order. Some classes are STRUCTURAL FACTS
//! the engine already holds (the turn was cancelled, the deadline expired);
//! those arrive as `declared_class` and are believed. The rest are TEXT FACTS
//! matched through the same normalized conjunction configured signatures
//! use, so a guard marker and a configured literal are never compared by two
//! different rules.
//!
//! Generic HTTP 400/404 is deliberately NOT a guard here: "generic" is defined
//! as "with no exact eligible discriminator", which is only knowable after
//! matching has run. `classify` owns that reason.

use std::fmt;

use super::text::matches_any_conjunction;

/// Exit statuses that ARE their own classification. A configured
/// `exit_code` is bounded to 1..=125 so it cannot collide with these.
const NOT_EXECUTABLE_EXIT: i64 = 126;
const NOT_FOUND_EXIT: i64 = 127;
const SIGNAL_EXIT_FLOOR: i64 = 128;

/// Every reason a classification can be non-eligible: the guard classes, the
/// two generic-status reasons, and the verdicts the classifier itself
/// reaches. The vocabulary is closed and shared with the Dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NonEligibleReason {
    Ambiguous,
    Authentication,
    Cancellation,
    CodeTestReviewOrToolFailure,
    CommandNotFound,
    DeclaredNonEligible,
    GenericHttp400,
    GenericHttp404,
    IdentityAbsent,
    MalformedConfiguration,
    NodeDeadline,
    NonConvergence,
    PreTurnWithoutProviderEvidence,
    RemoteCompaction404,
    SignalExit,
    StallExpiry,
    UnattributedSandboxOrTransport,
    Unmatched,
}

impl NonEligibleReason {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ambiguous => "ambiguous",
            Self::Authentication => "authentication",
            Self::Cancellation => "cancellation",
            Self::CodeTestReviewOrToolFailure => "code_test_review_or_tool_failure",
            Self::CommandNotFound => "command_not_found",
            Self::DeclaredNonEligible => "declared_non_eligible",
            Self::GenericHttp400 => "generic_http_400",
            Self::GenericHttp404 => "generic_http_404",
            Self::IdentityAbsent => "identity_absent",
            Self::MalformedConfiguration => "malformed_configuration",
            Self::NodeDeadline => "node_deadline",
            Self::NonConvergence => "non_convergence",
            Self::PreTurnWithoutProviderEvidence => "pre_turn_without_provider_evidence",
            Self::RemoteCompaction404 => "remote_compaction_404",
            Self::SignalExit => "signal_exit",
            Self::StallExpiry => "stall_expiry",
            Self::UnattributedSandboxOrTransport => "unattributed_sandbox_or_transport",
            Self::Unmatched => "unmatched",
        }
    }
}

impl fmt::Display for NonEligibleReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A terminal class the ENGINE established structurally before any text was
/// read. Believed over the text, because the engine observed the mechanism
/// while the classifier can only read what the mechanism printed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredClass {
    Cancellation,
    NodeDeadline,
    StallExpiry,
    MalformedConfiguration,
    UnattributedSandboxOrTransport,
    CodeTestReviewOrToolFailure,
    /// Something terminal the engine established that this vocabulary does
    /// not model; still non-eligible, still its own identity.
    Other,
}

impl DeclaredClass {
    fn reason(self) -> NonEligibleReason {
        match self {
            Self::Cancellation => NonEligibleReason::Cancellation,
            Self::NodeDeadline => NonEligibleReason::NodeDeadline,
            Self::StallExpiry => NonEligibleReason::StallExpiry,
            Self::MalformedConfiguration => NonEligibleReason::MalformedConfiguration,
            Self::UnattributedSandboxOrTransport => {
                NonEligibleReason::UnattributedSandboxOrTransport
            }
            Self::CodeTestReviewOrToolFailure => NonEligibleReason::CodeTestReviewOrToolFailure,
            Self::Other => NonEligibleReason::DeclaredNonEligible,
        }
    }
}

/// One candidate attempt's terminal report, as the classifier reads it.
///
/// THREE READABLE FIELDS AND NOTHING ELSE, which is the contract's "never an
/// arbitrary exec-output tail, agent response, prompt, tool/test/review
/// output, or run log" made structural: there is nowhere to put a run log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FailureSignal {
    /// The failure's own name, carried so a non-eligible verdict terminates
    /// WITH it rather than with a provider cause nobody measured.
    pub original_identity:   String,
    pub machine_code:        Option<String>,
    pub protocol_message:    Option<String>,
    pub terminal_diagnostic: Option<String>,
    pub exit_code:           Option<i64>,
    /// False for a pre-turn failure: nothing was observed from the adapter
    /// after launch.
    pub turn_started:        bool,
    pub declared_class:      Option<DeclaredClass>,
}

impl FailureSignal {
    /// Whether the provider said anything at all on any readable field.
    #[must_use]
    pub fn has_provider_evidence(&self) -> bool {
        [
            &self.machine_code,
            &self.protocol_message,
            &self.terminal_diagnostic,
        ]
        .into_iter()
        .any(|field| field.as_deref().is_some_and(|text| !text.trim().is_empty()))
    }

    /// The two text fields a guard marker or generic-status marker may read.
    pub(super) fn text_fields(&self) -> impl Iterator<Item = &str> {
        [&self.protocol_message, &self.terminal_diagnostic]
            .into_iter()
            .filter_map(|field| field.as_deref())
    }
}

/// The text-readable guard classes, each a set of conjunctions; a class fires
/// when ANY one of its conjunctions matches in full. The remote-compaction
/// conjunction is three literals, not one, because "404 not found" alone is
/// the generic status the contract keeps non-eligible on its own terms.
const GUARD_MARKERS: &[(NonEligibleReason, &[&[&str]])] = &[
    (NonEligibleReason::RemoteCompaction404, &[&[
        "error running remote compact task",
        "404 not found",
        "responses/compact",
    ]]),
    (NonEligibleReason::Authentication, &[
        &["401 unauthorized"],
        &["authentication_error"],
        &["invalid api key"],
        &["invalid bearer token"],
        &["oauth token has expired"],
    ]),
    (NonEligibleReason::CommandNotFound, &[
        &["command not found"],
        &["executable file not found"],
        &["no such file or directory"],
    ]),
    (NonEligibleReason::Cancellation, &[
        &["operation was cancelled"],
        &["cancelled by the operator"],
    ]),
    (NonEligibleReason::NodeDeadline, &[
        &["node deadline exceeded"],
        &["deadline exceeded"],
        &["node timeout"],
    ]),
    (NonEligibleReason::StallExpiry, &[&["stall timeout"], &[
        "no progress for",
    ]]),
    (NonEligibleReason::MalformedConfiguration, &[
        &["failed to parse config"],
        &["invalid configuration"],
        &["malformed command"],
        &["malformed configuration"],
    ]),
    (NonEligibleReason::UnattributedSandboxOrTransport, &[
        &["connection refused"],
        &["connection reset by peer"],
        &["could not resolve host"],
        &["dns error"],
        &["temporary failure in name resolution"],
        &["tls handshake"],
    ]),
    (NonEligibleReason::NonConvergence, &[
        &["non-convergence"],
        &["did not converge"],
    ]),
];

/// The guard class that outranks configured matching, or `None`.
///
/// Ordered most-decisive first: a class the engine DECLARED, then the exit
/// statuses that are their own classification, then the text markers.
#[must_use]
pub fn non_eligible_reason(signal: &FailureSignal) -> Option<NonEligibleReason> {
    if let Some(declared) = signal.declared_class {
        return Some(declared.reason());
    }
    if let Some(reason) = exit_status_reason(signal.exit_code) {
        return Some(reason);
    }
    marker_reason(signal)
}

fn exit_status_reason(exit_code: Option<i64>) -> Option<NonEligibleReason> {
    let code = exit_code?;
    if code >= SIGNAL_EXIT_FLOOR {
        return Some(NonEligibleReason::SignalExit);
    }
    if code == NOT_EXECUTABLE_EXIT || code == NOT_FOUND_EXIT {
        return Some(NonEligibleReason::CommandNotFound);
    }
    None
}

fn marker_reason(signal: &FailureSignal) -> Option<NonEligibleReason> {
    for text in signal.text_fields() {
        for (reason, conjunctions) in GUARD_MARKERS {
            if matches_any_conjunction(conjunctions, text) {
                return Some(*reason);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signal(message: Option<&str>, diagnostic: Option<&str>) -> FailureSignal {
        FailureSignal {
            original_identity: "ACP turn failed".to_string(),
            protocol_message: message.map(str::to_string),
            terminal_diagnostic: diagnostic.map(str::to_string),
            turn_started: true,
            ..FailureSignal::default()
        }
    }

    #[test]
    fn declared_class_outranks_text_and_exit_status() {
        let mut s = signal(Some("401 Unauthorized"), None);
        s.declared_class = Some(DeclaredClass::NodeDeadline);
        s.exit_code = Some(127);
        assert_eq!(
            non_eligible_reason(&s),
            Some(NonEligibleReason::NodeDeadline)
        );
        s.declared_class = Some(DeclaredClass::Other);
        assert_eq!(
            non_eligible_reason(&s),
            Some(NonEligibleReason::DeclaredNonEligible)
        );
    }

    #[test]
    fn exit_status_is_its_own_classification() {
        let mut s = signal(None, Some("hit your usage limit"));
        s.exit_code = Some(127);
        assert_eq!(
            non_eligible_reason(&s),
            Some(NonEligibleReason::CommandNotFound)
        );
        s.exit_code = Some(126);
        assert_eq!(
            non_eligible_reason(&s),
            Some(NonEligibleReason::CommandNotFound)
        );
        s.exit_code = Some(137);
        assert_eq!(non_eligible_reason(&s), Some(NonEligibleReason::SignalExit));
        s.exit_code = Some(2);
        assert_eq!(non_eligible_reason(&s), None);
    }

    #[test]
    fn text_markers_fire_on_either_field_with_normalization() {
        let s = signal(None, Some("  AUTHENTICATION_ERROR:\tbad key "));
        assert_eq!(
            non_eligible_reason(&s),
            Some(NonEligibleReason::Authentication)
        );
        let s = signal(
            Some("error running remote compact task: 404 Not Found /responses/compact"),
            None,
        );
        assert_eq!(
            non_eligible_reason(&s),
            Some(NonEligibleReason::RemoteCompaction404)
        );
        let s = signal(Some("404 Not Found"), None);
        assert_eq!(non_eligible_reason(&s), None, "bare 404 is not a guard");
        let s = signal(Some("connection reset by peer"), None);
        assert_eq!(
            non_eligible_reason(&s),
            Some(NonEligibleReason::UnattributedSandboxOrTransport)
        );
    }

    #[test]
    fn provider_evidence_ignores_blank_fields() {
        let s = signal(Some("   "), None);
        assert!(!s.has_provider_evidence());
        let s = signal(None, Some("x"));
        assert!(s.has_provider_evidence());
        let mut s = signal(None, None);
        s.machine_code = Some("usage_limit_exceeded".to_string());
        assert!(s.has_provider_evidence());
    }
}
