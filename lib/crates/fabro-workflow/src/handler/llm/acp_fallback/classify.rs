//! The CLOSED decision: is this candidate failure an availability failure?
//!
//! The contract gives this decision a fixed shape, and the ORDER of the steps
//! is the contract's own order:
//!
//! 1. The non-eligible classes are checked first so no configured literal can
//!    ever claim one of them.
//! 2. "A mere pre-turn failure is not provider evidence."
//! 3. "Structured machine codes take precedence." A matching machine code
//!    settles it and text signatures are not consulted.
//! 4. "If distinct signatures nevertheless match one runtime diagnostic with
//!    different cause/scope/key dispositions, the failure is non-eligible and
//!    surfaced as ambiguous, and it creates no hold; runtime first-match choice
//!    is forbidden."
//!
//! PRECEDENCE IS A FILTER, NOT A SORT. Taking the first machine-code match
//! would resolve an ambiguity by ordering, which is the forbidden choice;
//! taking the SET of machine-code matches and applying the ambiguity rule to
//! it keeps two disagreeing codes refusing, exactly as two disagreeing
//! literals do.
//!
//! The candidates here always carry explicit identity (the chain grammar
//! requires it), so the Dispatcher's "identity absent" verdict cannot arise
//! and is not modelled.

use std::collections::BTreeSet;

use super::chain::{
    AcpChainCandidate, AvailabilitySignature, SignatureCause, SignatureScope, SignatureSource,
};
use super::signal::{FailureSignal, NonEligibleReason, non_eligible_reason};
use super::text::{conjunction_matches, matches_any_conjunction, normalized};

/// The vocabulary a bare HTTP 400/404 already contains. A signature naming
/// nothing else is claiming the status itself, which the contract forbids.
const GENERIC_STATUS_LITERALS: &[&str] = &[
    "400",
    "404",
    "bad request",
    "http 400",
    "http 404",
    "http status 400",
    "http status 404",
    "not found",
    "status 400",
    "status 404",
];

/// What makes a diagnostic an HTTP 400 or 404 AT ALL. The bare digits are
/// deliberately absent: "400" alone appears in token counts, byte sizes and
/// model names.
const STATUS_MARKERS: &[(NonEligibleReason, &[&[&str]])] = &[
    (NonEligibleReason::GenericHttp400, &[
        &["http 400"],
        &["status 400"],
        &["400 bad request"],
        &["bad request"],
    ]),
    (NonEligibleReason::GenericHttp404, &[
        &["http 404"],
        &["status 404"],
        &["404 not found"],
    ]),
];

/// An ELIGIBLE typed availability failure, already scoped to its hold key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AvailabilityFailure {
    pub cause:            SignatureCause,
    pub scope:            SignatureScope,
    pub hold_key:         String,
    pub availability_key: String,
    /// `None` at domain scope: the hold covers the whole domain.
    pub candidate_key:    Option<String>,
    pub source:           SignatureSource,
}

/// The classifier's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Eligible(AvailabilityFailure),
    /// Mints no hold and terminates WITH the failure's original identity.
    NonEligible {
        reason:            NonEligibleReason,
        original_identity: String,
    },
}

/// Decide whether one candidate attempt failed on provider availability.
#[must_use]
pub fn classify(signal: &FailureSignal, candidate: &AcpChainCandidate) -> Verdict {
    if let Some(reason) = non_eligible_reason(signal) {
        return non_eligible(reason, signal);
    }
    if !signal.turn_started && !signal.has_provider_evidence() {
        return non_eligible(NonEligibleReason::PreTurnWithoutProviderEvidence, signal);
    }
    let status = generic_status_reason(signal);
    let matched: Vec<&AvailabilitySignature> = candidate
        .availability_signatures
        .iter()
        .filter(|signature| signature_matches(signature, signal))
        .filter(|signature| status.is_none() || signature_discriminates(signature))
        .collect();
    let codes: Vec<&AvailabilitySignature> = matched
        .iter()
        .copied()
        .filter(|signature| signature.source == SignatureSource::ProtocolMachineCode)
        .collect();
    let selected = if codes.is_empty() { matched } else { codes };
    if selected.is_empty() {
        return non_eligible(status.unwrap_or(NonEligibleReason::Unmatched), signal);
    }
    let verdicts: BTreeSet<AvailabilityFailure> = selected
        .into_iter()
        .map(|signature| failure(signature, candidate))
        .collect();
    if verdicts.len() > 1 {
        return non_eligible(NonEligibleReason::Ambiguous, signal);
    }
    Verdict::Eligible(
        verdicts
            .into_iter()
            .next()
            .expect("a non-empty set has a first element"),
    )
}

/// Whether this signature matches this observed failure. `exit_code` REFINES
/// whichever discriminator the source requires and is never consulted alone.
fn signature_matches(signature: &AvailabilitySignature, signal: &FailureSignal) -> bool {
    if signature.exit_code.is_some() && signature.exit_code != signal.exit_code {
        return false;
    }
    let Some(observed) = field(signature.source, signal) else {
        return false;
    };
    if signature.source == SignatureSource::ProtocolMachineCode {
        return signature
            .machine_code
            .as_deref()
            .is_some_and(|code| code.trim() == observed.trim());
    }
    conjunction_matches(&signature.all_literals, observed)
}

/// Whether this signature names anything beyond bare HTTP status vocabulary.
fn signature_discriminates(signature: &AvailabilitySignature) -> bool {
    if signature.source == SignatureSource::ProtocolMachineCode {
        return true;
    }
    signature
        .all_literals
        .iter()
        .any(|literal| !GENERIC_STATUS_LITERALS.contains(&normalized(literal).as_str()))
}

/// The non-eligible reason for an HTTP 400/404 diagnostic, or `None`. Both
/// text fields are scanned because a status arrives on either transport.
fn generic_status_reason(signal: &FailureSignal) -> Option<NonEligibleReason> {
    for text in signal.text_fields() {
        for (reason, conjunctions) in STATUS_MARKERS {
            if matches_any_conjunction(conjunctions, text) {
                return Some(*reason);
            }
        }
    }
    None
}

/// The ONE observed field this source names.
fn field(source: SignatureSource, signal: &FailureSignal) -> Option<&str> {
    match source {
        SignatureSource::ProtocolMachineCode => signal.machine_code.as_deref(),
        SignatureSource::ProtocolMessage => signal.protocol_message.as_deref(),
        SignatureSource::ProcessTerminalDiagnostic => signal.terminal_diagnostic.as_deref(),
    }
}

/// The typed verdict one matching signature produces for this candidate.
/// Domain scope keys on the signature's `hold_key` override when it declares
/// one and on the candidate's own `availability_key` otherwise; candidate
/// scope keys on the exact entitlement pair.
fn failure(
    signature: &AvailabilitySignature,
    candidate: &AcpChainCandidate,
) -> AvailabilityFailure {
    let domain = signature.scope == SignatureScope::AvailabilityDomain;
    AvailabilityFailure {
        cause:            signature.cause,
        scope:            signature.scope,
        hold_key:         if domain {
            signature
                .hold_key
                .clone()
                .unwrap_or_else(|| candidate.availability_key.clone())
        } else {
            candidate.availability_key.clone()
        },
        availability_key: candidate.availability_key.clone(),
        candidate_key:    (!domain).then(|| candidate.candidate_key.clone()),
        source:           signature.source,
    }
}

fn non_eligible(reason: NonEligibleReason, signal: &FailureSignal) -> Verdict {
    Verdict::NonEligible {
        reason,
        original_identity: signal.original_identity.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::llm::acp_fallback::signal::DeclaredClass;

    fn signature(
        source: SignatureSource,
        cause: SignatureCause,
        scope: SignatureScope,
        literals: &[&str],
        code: Option<&str>,
    ) -> AvailabilitySignature {
        AvailabilitySignature {
            source,
            cause,
            scope,
            machine_code: code.map(str::to_string),
            all_literals: literals.iter().map(|s| (*s).to_string()).collect(),
            exit_code: None,
            hold_key: None,
        }
    }

    fn codex_candidate(signatures: Vec<AvailabilitySignature>) -> AcpChainCandidate {
        AcpChainCandidate {
            candidate_index:         0,
            display_name:            "built-in Codex ACP adapter".to_string(),
            candidate_key:           "builtin-codex-abc".to_string(),
            availability_key:        "codex".to_string(),
            command:                 "codex-acp".to_string(),
            availability_signatures: signatures,
            preflight_skipped:       None,
        }
    }

    fn measured_codex_signatures() -> Vec<AvailabilitySignature> {
        vec![
            signature(
                SignatureSource::ProcessTerminalDiagnostic,
                SignatureCause::ModelUnsupported,
                SignatureScope::Candidate,
                &[
                    "requested model",
                    "is not supported when using codex with a chatgpt account",
                ],
                None,
            ),
            signature(
                SignatureSource::ProcessTerminalDiagnostic,
                SignatureCause::ModelNotFound,
                SignatureScope::Candidate,
                &[
                    "the model",
                    "does not exist or you do not have access to it",
                ],
                None,
            ),
            signature(
                SignatureSource::ProtocolMessage,
                SignatureCause::Quota,
                SignatureScope::AvailabilityDomain,
                &["hit your usage limit"],
                None,
            ),
        ]
    }

    fn terminal(diagnostic: &str) -> FailureSignal {
        FailureSignal {
            original_identity: "ACP process exited before protocol completed".to_string(),
            terminal_diagnostic: Some(diagnostic.to_string()),
            exit_code: Some(1),
            turn_started: false,
            ..FailureSignal::default()
        }
    }

    #[test]
    fn measured_removed_model_400_is_candidate_scoped_model_unsupported() {
        let candidate = codex_candidate(measured_codex_signatures());
        let signal = terminal(
            "HTTP 400 Bad Request: The requested model gpt-5.4-mini is not supported when using Codex with a ChatGPT account",
        );
        let Verdict::Eligible(failure) = classify(&signal, &candidate) else {
            panic!("expected eligible");
        };
        assert_eq!(failure.cause, SignatureCause::ModelUnsupported);
        assert_eq!(failure.scope, SignatureScope::Candidate);
        assert_eq!(failure.hold_key, "codex");
        assert_eq!(failure.candidate_key.as_deref(), Some("builtin-codex-abc"));
    }

    #[test]
    fn generic_400_without_discriminator_terminates_with_its_own_identity() {
        let candidate = codex_candidate(measured_codex_signatures());
        let signal = terminal("HTTP 400 Bad Request");
        assert_eq!(classify(&signal, &candidate), Verdict::NonEligible {
            reason:            NonEligibleReason::GenericHttp400,
            original_identity: signal.original_identity.clone(),
        });
    }

    #[test]
    fn status_only_configured_literal_cannot_claim_a_generic_status() {
        let candidate = codex_candidate(vec![signature(
            SignatureSource::ProcessTerminalDiagnostic,
            SignatureCause::ModelNotFound,
            SignatureScope::Candidate,
            &["not found", "status 404"],
            None,
        )]);
        let signal = terminal("status 404 not found");
        assert!(matches!(
            classify(&signal, &candidate),
            Verdict::NonEligible {
                reason: NonEligibleReason::GenericHttp404,
                ..
            }
        ));
    }

    #[test]
    fn non_eligible_guards_outrank_configured_literal_overlap() {
        let candidate = codex_candidate(vec![signature(
            SignatureSource::ProtocolMessage,
            SignatureCause::Quota,
            SignatureScope::AvailabilityDomain,
            &["unauthorized"],
            None,
        )]);
        let mut signal = FailureSignal {
            original_identity: "ACP turn failed".to_string(),
            protocol_message: Some("401 Unauthorized".to_string()),
            turn_started: true,
            ..FailureSignal::default()
        };
        assert!(matches!(
            classify(&signal, &candidate),
            Verdict::NonEligible {
                reason: NonEligibleReason::Authentication,
                ..
            }
        ));
        signal.protocol_message = Some("hit your usage limit".to_string());
        signal.exit_code = Some(127);
        assert!(matches!(
            classify(&signal, &candidate),
            Verdict::NonEligible {
                reason: NonEligibleReason::CommandNotFound,
                ..
            }
        ));
        signal.exit_code = None;
        signal.declared_class = Some(DeclaredClass::NodeDeadline);
        assert!(matches!(
            classify(&signal, &candidate),
            Verdict::NonEligible {
                reason: NonEligibleReason::NodeDeadline,
                ..
            }
        ));
    }

    #[test]
    fn pre_turn_failure_without_evidence_is_not_provider_evidence() {
        let candidate = codex_candidate(measured_codex_signatures());
        let signal = FailureSignal {
            original_identity: "ACP process exited".to_string(),
            exit_code: Some(1),
            ..FailureSignal::default()
        };
        assert!(matches!(
            classify(&signal, &candidate),
            Verdict::NonEligible {
                reason: NonEligibleReason::PreTurnWithoutProviderEvidence,
                ..
            }
        ));
    }

    #[test]
    fn machine_code_precedence_is_a_filter_and_ambiguity_refuses() {
        let text_quota = signature(
            SignatureSource::ProtocolMessage,
            SignatureCause::RateLimit,
            SignatureScope::AvailabilityDomain,
            &["usage limit"],
            None,
        );
        let code_quota = signature(
            SignatureSource::ProtocolMachineCode,
            SignatureCause::Quota,
            SignatureScope::AvailabilityDomain,
            &[],
            Some("usage_limit_exceeded"),
        );
        let signal = FailureSignal {
            original_identity: "ACP turn failed".to_string(),
            machine_code: Some("usage_limit_exceeded".to_string()),
            protocol_message: Some("You've hit your usage limit".to_string()),
            turn_started: true,
            ..FailureSignal::default()
        };
        // Text and code both match with different causes: the code SET wins,
        // and the set holds one disposition, so the verdict is the code's.
        let candidate = codex_candidate(vec![text_quota.clone(), code_quota.clone()]);
        let Verdict::Eligible(failure) = classify(&signal, &candidate) else {
            panic!("expected eligible");
        };
        assert_eq!(failure.cause, SignatureCause::Quota);
        assert_eq!(failure.source, SignatureSource::ProtocolMachineCode);
        // Two machine codes with the same matcher but different dispositions
        // are refused statically; two DIFFERENT matchers both matching one
        // diagnostic with different dispositions are ambiguous at runtime.
        let code_rate = AvailabilitySignature {
            exit_code: None,
            ..signature(
                SignatureSource::ProtocolMachineCode,
                SignatureCause::RateLimit,
                SignatureScope::Candidate,
                &[],
                Some("usage_limit_exceeded"),
            )
        };
        let candidate = codex_candidate(vec![code_quota, code_rate]);
        assert!(matches!(
            classify(&signal, &candidate),
            Verdict::NonEligible {
                reason: NonEligibleReason::Ambiguous,
                ..
            }
        ));
        // Two text signatures that both match with the SAME disposition are
        // merely redundant.
        let candidate = codex_candidate(vec![
            text_quota.clone(),
            signature(
                SignatureSource::ProtocolMessage,
                SignatureCause::RateLimit,
                SignatureScope::AvailabilityDomain,
                &["hit your"],
                None,
            ),
        ]);
        assert!(matches!(
            classify(&signal, &candidate),
            Verdict::Eligible(_)
        ));
    }

    #[test]
    fn domain_hold_key_override_and_exit_code_refinement() {
        let mut sig = signature(
            SignatureSource::ProcessTerminalDiagnostic,
            SignatureCause::ProviderCapacity,
            SignatureScope::AvailabilityDomain,
            &["overloaded"],
            None,
        );
        sig.hold_key = Some("shared-router".to_string());
        sig.exit_code = Some(3);
        let candidate = codex_candidate(vec![sig]);
        let mut signal = terminal("provider overloaded");
        signal.turn_started = true;
        signal.exit_code = Some(1);
        assert!(matches!(
            classify(&signal, &candidate),
            Verdict::NonEligible {
                reason: NonEligibleReason::Unmatched,
                ..
            }
        ));
        signal.exit_code = Some(3);
        let Verdict::Eligible(failure) = classify(&signal, &candidate) else {
            panic!("expected eligible");
        };
        assert_eq!(failure.hold_key, "shared-router");
        assert_eq!(failure.candidate_key, None);
    }
}
