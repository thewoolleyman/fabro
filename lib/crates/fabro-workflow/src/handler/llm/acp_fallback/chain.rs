//! The `acp.fallback_chain` node attribute: the ordered candidate chain one ACP
//! node may carry, and the CLOSED grammar it is parsed against.
//!
//! The consumer contract (livespec-orchestrator-beads-fabro
//! `SPECIFICATION/contracts.md`, "Factory-configurable ACP fallback priority")
//! fixes every shape here. The Dispatcher owns resolution, preflight
//! filtering and the measured built-in signature table; it renders the
//! EFFECTIVE chain into this attribute. Fabro therefore classifies against
//! exactly what the chain declares and carries no signature table of its own,
//! so there is one owner for every measured diagnostic and nothing to drift.
//!
//! Every refusal here fires BEFORE any adapter process starts, as a
//! deterministic validation error, so a malformed chain never becomes a run
//! that fails halfway through a node.

use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};

/// The one schema version this build understands.
pub const CHAIN_SCHEMA_VERSION: u32 = 1;

/// Lower and upper bound (inclusive) of a refining `exit_code`. Zero is
/// success, 126 upward is the shell's own command-not-found / signal
/// encoding, which the contract lists as non-eligible before matching runs.
const EXIT_CODE_LOW: i64 = 1;
const EXIT_CODE_HIGH: i64 = 125;

/// The field a signature reads. Chosen by the signature, never searched for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SignatureSource {
    #[serde(rename = "process.terminal_diagnostic")]
    ProcessTerminalDiagnostic,
    #[serde(rename = "protocol.machine_code")]
    ProtocolMachineCode,
    #[serde(rename = "protocol.message")]
    ProtocolMessage,
}

impl SignatureSource {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProcessTerminalDiagnostic => "process.terminal_diagnostic",
            Self::ProtocolMachineCode => "protocol.machine_code",
            Self::ProtocolMessage => "protocol.message",
        }
    }
}

impl fmt::Display for SignatureSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The eight ratified availability causes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignatureCause {
    ModelNotEntitled,
    ModelNotFound,
    ModelUnavailable,
    ModelUnsupported,
    ProviderCapacity,
    ProviderServerUnavailable,
    Quota,
    RateLimit,
}

impl SignatureCause {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ModelNotEntitled => "model_not_entitled",
            Self::ModelNotFound => "model_not_found",
            Self::ModelUnavailable => "model_unavailable",
            Self::ModelUnsupported => "model_unsupported",
            Self::ProviderCapacity => "provider_capacity",
            Self::ProviderServerUnavailable => "provider_server_unavailable",
            Self::Quota => "quota",
            Self::RateLimit => "rate_limit",
        }
    }
}

impl fmt::Display for SignatureCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a matched signature holds: the whole availability domain, or the
/// exact `(availability_key, candidate_key)` entitlement pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SignatureScope {
    #[serde(rename = "availability-domain")]
    AvailabilityDomain,
    #[serde(rename = "candidate")]
    Candidate,
}

impl SignatureScope {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AvailabilityDomain => "availability-domain",
            Self::Candidate => "candidate",
        }
    }
}

impl fmt::Display for SignatureScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One declared mapping from an observed diagnostic to a typed cause.
///
/// The discriminator rule is the point of the shape: a machine-code source
/// requires one exact `machine_code` and forbids `all_literals`; every other
/// source requires a non-empty conjunction of literals and forbids
/// `machine_code`. `exit_code` only refines and is never sufficient alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AvailabilitySignature {
    pub source:       SignatureSource,
    pub cause:        SignatureCause,
    pub scope:        SignatureScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_code: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub all_literals: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code:    Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_key:     Option<String>,
}

impl AvailabilitySignature {
    /// What this signature MATCHES, independent of what it then decides.
    /// Literal order is irrelevant because the contract makes the list a
    /// conjunction.
    fn matcher(&self) -> (SignatureSource, String, Vec<String>, Option<i64>) {
        let mut literals: Vec<String> = self.all_literals.clone();
        literals.sort();
        literals.dedup();
        (
            self.source,
            self.machine_code.clone().unwrap_or_default(),
            literals,
            self.exit_code,
        )
    }

    /// What this signature DECIDES once it has matched.
    fn disposition(&self) -> (SignatureCause, SignatureScope, String) {
        (
            self.cause,
            self.scope,
            self.hold_key.clone().unwrap_or_default(),
        )
    }
}

/// Why the Dispatcher's preflight left this candidate out of the executable
/// set. Carried so an "actually executed preflight transition" event can name
/// the typed cause without Fabro re-deriving it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreflightSkip {
    pub cause:    SignatureCause,
    pub scope:    SignatureScope,
    pub hold_key: String,
}

/// The remote observation that proves side-effect onset for a publishing
/// node. The Dispatcher sets it on the `pr` node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum OnsetProbe {
    /// A publish branch present on `origin` at takeover proves onset; an
    /// unreadable remote is treated as already past onset.
    #[serde(rename = "remote_publish_branch")]
    RemotePublishBranch,
}

/// One position in the node's ordered chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpChainCandidate {
    pub candidate_index:         u32,
    pub display_name:            String,
    pub candidate_key:           String,
    pub availability_key:        String,
    /// The complete adapter command line (env prefix, program, args) exactly
    /// as `acp.command` would carry it. NEVER copied into an event.
    pub command:                 String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub availability_signatures: Vec<AvailabilitySignature>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preflight_skipped:       Option<PreflightSkip>,
}

impl AcpChainCandidate {
    /// The `(availability_key, candidate_key)` entitlement pair.
    #[must_use]
    pub fn pair(&self) -> (&str, &str) {
        (&self.availability_key, &self.candidate_key)
    }

    /// Whether the Dispatcher's preflight already excluded this candidate.
    #[must_use]
    pub fn is_preflight_skipped(&self) -> bool {
        self.preflight_skipped.is_some()
    }
}

/// The parsed, validated chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpFallbackChain {
    pub schema_version:     u32,
    pub primary_generation: String,
    pub full_chain:         String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub onset_probe:        Option<OnsetProbe>,
    pub candidates:         Vec<AcpChainCandidate>,
}

impl AcpFallbackChain {
    /// The candidate at `index`, when the chain has one.
    #[must_use]
    pub fn candidate(&self, index: u32) -> Option<&AcpChainCandidate> {
        self.candidates
            .iter()
            .find(|candidate| candidate.candidate_index == index)
    }

    /// Candidate zero: the resolved primary the legacy `acp.command` carries.
    #[must_use]
    pub fn primary(&self) -> &AcpChainCandidate {
        &self.candidates[0]
    }
}

/// Why a chain was refused. Every message names the offending key so an
/// operator can find the line; none of them echoes a command or env value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChainError {
    #[error("acp.fallback_chain is not valid JSON for the chain grammar: {0}")]
    Malformed(String),
    #[error("acp.fallback_chain.schema_version must be {CHAIN_SCHEMA_VERSION}; got {0}")]
    UnsupportedSchemaVersion(u32),
    #[error(
        "acp.fallback_chain requires acp.command to carry candidate zero; acp.command is absent"
    )]
    CommandAbsent,
    #[error("acp.fallback_chain cannot be combined with acp.config; candidates carry command form")]
    ConfigPresent,
    #[error(
        "acp.fallback_chain.candidates[0].command must equal acp.command byte-for-byte; candidate zero is the resolved primary and a chain may not replace it"
    )]
    PrimaryMismatch,
    #[error("acp.fallback_chain.candidates must carry at least one candidate")]
    EmptyChain,
    #[error(
        "acp.fallback_chain.candidates[{position}].candidate_index must be {position}; got {found}"
    )]
    NonContiguousIndex { position: usize, found: u32 },
    #[error("acp.fallback_chain.candidates[{index}].{field} must be non-empty text")]
    EmptyField { index: u32, field: &'static str },
    #[error("acp.fallback_chain.{field} must be non-empty text")]
    EmptyChainField { field: &'static str },
    #[error(
        "acp.fallback_chain.candidates[{index}] repeats the (availability_key, candidate_key) pair of candidates[{first}]; one node may not hold the same entitlement twice"
    )]
    DuplicatePair { index: u32, first: u32 },
    #[error("acp.fallback_chain.candidates[{index}].availability_signatures[{position}]: {detail}")]
    Signature {
        index:    u32,
        position: usize,
        detail:   String,
    },
    #[error(
        "acp.fallback_chain.candidates[{index}].availability_signatures declares two signatures matching identically on source {signature_source} but disposing differently ({first} versus {second}); a runtime multi-match may never be resolved by first match"
    )]
    ConflictingSignatures {
        index:            u32,
        signature_source: SignatureSource,
        first:            String,
        second:           String,
    },
    #[error(
        "acp.fallback_chain.candidates[{index}].preflight_skipped.hold_key must be non-empty text"
    )]
    EmptySkipHoldKey { index: u32 },
}

/// Parse and validate the attribute against the node's other ACP attributes.
///
/// `acp_command` and `acp_config` are the node's own attributes, passed in so
/// the byte-identity rule ("candidate zero IS the resolved primary") is
/// checked here, before any process launches, rather than discovered when the
/// two disagree at run time.
pub fn parse_chain(
    raw: &str,
    acp_command: Option<&str>,
    acp_config: Option<&str>,
) -> Result<AcpFallbackChain, ChainError> {
    let chain: AcpFallbackChain =
        serde_json::from_str(raw.trim()).map_err(|err| ChainError::Malformed(err.to_string()))?;
    if chain.schema_version != CHAIN_SCHEMA_VERSION {
        return Err(ChainError::UnsupportedSchemaVersion(chain.schema_version));
    }
    if acp_config.is_some() {
        return Err(ChainError::ConfigPresent);
    }
    let Some(command) = acp_command else {
        return Err(ChainError::CommandAbsent);
    };
    if chain.candidates.is_empty() {
        return Err(ChainError::EmptyChain);
    }
    if chain.primary_generation.trim().is_empty() {
        return Err(ChainError::EmptyChainField {
            field: "primary_generation",
        });
    }
    if chain.full_chain.trim().is_empty() {
        return Err(ChainError::EmptyChainField {
            field: "full_chain",
        });
    }
    if chain.candidates[0].command != command {
        return Err(ChainError::PrimaryMismatch);
    }
    let mut seen: Vec<(String, String)> = Vec::with_capacity(chain.candidates.len());
    for (position, candidate) in chain.candidates.iter().enumerate() {
        let expected = u32::try_from(position).unwrap_or(u32::MAX);
        if candidate.candidate_index != expected {
            return Err(ChainError::NonContiguousIndex {
                position,
                found: candidate.candidate_index,
            });
        }
        validate_candidate(candidate)?;
        let pair = (
            candidate.availability_key.clone(),
            candidate.candidate_key.clone(),
        );
        if let Some(first) = seen.iter().position(|known| *known == pair) {
            return Err(ChainError::DuplicatePair {
                index: candidate.candidate_index,
                first: u32::try_from(first).unwrap_or(u32::MAX),
            });
        }
        seen.push(pair);
    }
    Ok(chain)
}

fn validate_candidate(candidate: &AcpChainCandidate) -> Result<(), ChainError> {
    let index = candidate.candidate_index;
    for (field, value) in [
        ("display_name", &candidate.display_name),
        ("candidate_key", &candidate.candidate_key),
        ("availability_key", &candidate.availability_key),
        ("command", &candidate.command),
    ] {
        if value.trim().is_empty() {
            return Err(ChainError::EmptyField { index, field });
        }
    }
    if let Some(skip) = &candidate.preflight_skipped {
        if skip.hold_key.trim().is_empty() {
            return Err(ChainError::EmptySkipHoldKey { index });
        }
    }
    for (position, signature) in candidate.availability_signatures.iter().enumerate() {
        validate_signature(signature).map_err(|detail| ChainError::Signature {
            index,
            position,
            detail,
        })?;
    }
    conflicting_signature_refusal(index, &candidate.availability_signatures)
}

fn validate_signature(signature: &AvailabilitySignature) -> Result<(), String> {
    if let Some(code) = signature.exit_code {
        if !(EXIT_CODE_LOW..=EXIT_CODE_HIGH).contains(&code) {
            return Err(format!(
                "exit_code must be an integer from {EXIT_CODE_LOW} through {EXIT_CODE_HIGH}; got {code}"
            ));
        }
    }
    if let Some(hold_key) = &signature.hold_key {
        if signature.scope != SignatureScope::AvailabilityDomain {
            return Err(format!(
                "hold_key is only valid at scope {}; a candidate-scoped signature keys on its own (availability_key, candidate_key) pair",
                SignatureScope::AvailabilityDomain
            ));
        }
        if hold_key.trim().is_empty() {
            return Err("hold_key must be non-empty text".to_string());
        }
    }
    if signature.source == SignatureSource::ProtocolMachineCode {
        if !signature.all_literals.is_empty() {
            return Err(format!(
                "all_literals is forbidden for source {}",
                SignatureSource::ProtocolMachineCode
            ));
        }
        match &signature.machine_code {
            Some(code) if !code.trim().is_empty() => Ok(()),
            _ => Err(format!(
                "machine_code must be one exact non-empty code for source {}",
                SignatureSource::ProtocolMachineCode
            )),
        }
    } else {
        if signature.machine_code.is_some() {
            return Err(format!(
                "machine_code is forbidden for source {}",
                signature.source
            ));
        }
        if signature.all_literals.is_empty()
            || signature
                .all_literals
                .iter()
                .any(|literal| literal.trim().is_empty())
        {
            return Err(format!(
                "all_literals must be a non-empty array of non-empty strings for source {}",
                signature.source
            ));
        }
        Ok(())
    }
}

/// Refuse two STATICALLY identical matchers that disagree on disposition.
/// Identical matcher and identical disposition is merely redundant and
/// passes; a disposition difference can only ever produce the runtime
/// ambiguity the contract forbids resolving by first match.
fn conflicting_signature_refusal(
    index: u32,
    signatures: &[AvailabilitySignature],
) -> Result<(), ChainError> {
    let mut seen: Vec<(
        (SignatureSource, String, Vec<String>, Option<i64>),
        (SignatureCause, SignatureScope, String),
    )> = Vec::new();
    let mut distinct: HashSet<String> = HashSet::new();
    for signature in signatures {
        let matcher = signature.matcher();
        let disposition = signature.disposition();
        if let Some((_, previous)) = seen.iter().find(|(known, _)| *known == matcher) {
            if *previous != disposition {
                return Err(ChainError::ConflictingSignatures {
                    index,
                    signature_source: signature.source,
                    first: format!("{}/{}", previous.0, previous.1),
                    second: format!("{}/{}", disposition.0, disposition.1),
                });
            }
        } else {
            seen.push((matcher, disposition));
        }
        distinct.insert(format!("{signature:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRIMARY: &str =
        "ANTHROPIC_MODEL=claude-opus-5 npx -y @agentclientprotocol/claude-agent-acp";

    fn chain_json(candidates: serde_json::Value) -> String {
        serde_json::json!({
            "schema_version": 1,
            "primary_generation": "a".repeat(32),
            "full_chain": "b".repeat(32),
            "candidates": candidates,
        })
        .to_string()
    }

    fn candidate(index: u32, command: &str, key: &str) -> serde_json::Value {
        serde_json::json!({
            "candidate_index": index,
            "display_name": format!("candidate {index}"),
            "candidate_key": key,
            "availability_key": "anthropic",
            "command": command,
        })
    }

    #[test]
    fn parses_a_two_candidate_chain_and_preserves_order() {
        let raw = chain_json(serde_json::json!([
            candidate(0, PRIMARY, "primary"),
            candidate(1, "OTHER=1 codex-acp", "fallback"),
        ]));
        let chain = parse_chain(&raw, Some(PRIMARY), None).unwrap();
        assert_eq!(chain.candidates.len(), 2);
        assert_eq!(chain.primary().candidate_key, "primary");
        assert_eq!(chain.candidate(1).unwrap().command, "OTHER=1 codex-acp");
        assert!(chain.onset_probe.is_none());
    }

    #[test]
    fn refuses_primary_that_does_not_match_acp_command() {
        let raw = chain_json(serde_json::json!([candidate(
            0,
            "something-else",
            "primary"
        )]));
        assert_eq!(
            parse_chain(&raw, Some(PRIMARY), None),
            Err(ChainError::PrimaryMismatch)
        );
    }

    #[test]
    fn refuses_absent_command_and_present_config() {
        let raw = chain_json(serde_json::json!([candidate(0, PRIMARY, "primary")]));
        assert_eq!(
            parse_chain(&raw, None, None),
            Err(ChainError::CommandAbsent)
        );
        assert_eq!(
            parse_chain(&raw, Some(PRIMARY), Some("{}")),
            Err(ChainError::ConfigPresent)
        );
    }

    #[test]
    fn refuses_unknown_keys_and_wrong_schema_version() {
        let mut value: serde_json::Value =
            serde_json::from_str(&chain_json(serde_json::json!([candidate(
                0, PRIMARY, "primary"
            )])))
            .unwrap();
        value["surprise"] = serde_json::json!(true);
        let err = parse_chain(&value.to_string(), Some(PRIMARY), None).unwrap_err();
        assert!(matches!(err, ChainError::Malformed(ref m) if m.contains("surprise")));

        let mut value: serde_json::Value =
            serde_json::from_str(&chain_json(serde_json::json!([candidate(
                0, PRIMARY, "primary"
            )])))
            .unwrap();
        value["schema_version"] = serde_json::json!(2);
        assert_eq!(
            parse_chain(&value.to_string(), Some(PRIMARY), None),
            Err(ChainError::UnsupportedSchemaVersion(2))
        );
    }

    #[test]
    fn refuses_non_contiguous_indexes_and_duplicate_pairs() {
        let raw = chain_json(serde_json::json!([
            candidate(0, PRIMARY, "primary"),
            candidate(2, "x", "fallback"),
        ]));
        assert_eq!(
            parse_chain(&raw, Some(PRIMARY), None),
            Err(ChainError::NonContiguousIndex {
                position: 1,
                found:    2,
            })
        );
        let raw = chain_json(serde_json::json!([
            candidate(0, PRIMARY, "same"),
            candidate(1, "x", "same"),
        ]));
        assert_eq!(
            parse_chain(&raw, Some(PRIMARY), None),
            Err(ChainError::DuplicatePair { index: 1, first: 0 })
        );
    }

    #[test]
    fn refuses_empty_identity_fields() {
        let mut c = candidate(0, PRIMARY, "primary");
        c["display_name"] = serde_json::json!("   ");
        let raw = chain_json(serde_json::json!([c]));
        assert_eq!(
            parse_chain(&raw, Some(PRIMARY), None),
            Err(ChainError::EmptyField {
                index: 0,
                field: "display_name",
            })
        );
    }

    fn with_signatures(signatures: serde_json::Value) -> String {
        let mut c = candidate(0, PRIMARY, "primary");
        c["availability_signatures"] = signatures;
        chain_json(serde_json::json!([c]))
    }

    #[test]
    fn signature_discriminator_rules_are_enforced() {
        // machine-code source with literals: refused.
        let raw = with_signatures(serde_json::json!([{
            "source": "protocol.machine_code", "cause": "quota", "scope": "availability-domain",
            "machine_code": "usage_limit_exceeded", "all_literals": ["x"]
        }]));
        let err = parse_chain(&raw, Some(PRIMARY), None).unwrap_err();
        assert!(
            matches!(err, ChainError::Signature { detail, .. } if detail.contains("all_literals is forbidden"))
        );
        // text source without literals: refused.
        let raw = with_signatures(serde_json::json!([{
            "source": "protocol.message", "cause": "quota", "scope": "availability-domain"
        }]));
        let err = parse_chain(&raw, Some(PRIMARY), None).unwrap_err();
        assert!(
            matches!(err, ChainError::Signature { detail, .. } if detail.contains("all_literals must be"))
        );
        // exit_code out of bounds: refused.
        let raw = with_signatures(serde_json::json!([{
            "source": "process.terminal_diagnostic", "cause": "quota", "scope": "availability-domain",
            "all_literals": ["hit your usage limit"], "exit_code": 127
        }]));
        let err = parse_chain(&raw, Some(PRIMARY), None).unwrap_err();
        assert!(
            matches!(err, ChainError::Signature { detail, .. } if detail.contains("exit_code must be"))
        );
        // hold_key at candidate scope: refused.
        let raw = with_signatures(serde_json::json!([{
            "source": "protocol.message", "cause": "model_not_found", "scope": "candidate",
            "all_literals": ["does not exist"], "hold_key": "codex"
        }]));
        let err = parse_chain(&raw, Some(PRIMARY), None).unwrap_err();
        assert!(
            matches!(err, ChainError::Signature { detail, .. } if detail.contains("hold_key is only valid"))
        );
        // unknown cause: refused by the closed enum.
        let raw = with_signatures(serde_json::json!([{
            "source": "protocol.message", "cause": "meteor_strike", "scope": "candidate",
            "all_literals": ["x"]
        }]));
        assert!(matches!(
            parse_chain(&raw, Some(PRIMARY), None),
            Err(ChainError::Malformed(_))
        ));
    }

    #[test]
    fn statically_conflicting_signatures_refuse_but_redundant_ones_pass() {
        let raw = with_signatures(serde_json::json!([
            {"source": "protocol.message", "cause": "quota", "scope": "availability-domain",
             "all_literals": ["b", "a"]},
            {"source": "protocol.message", "cause": "rate_limit", "scope": "availability-domain",
             "all_literals": ["a", "b"]}
        ]));
        assert!(matches!(
            parse_chain(&raw, Some(PRIMARY), None),
            Err(ChainError::ConflictingSignatures { index: 0, .. })
        ));
        let raw = with_signatures(serde_json::json!([
            {"source": "protocol.message", "cause": "quota", "scope": "availability-domain",
             "all_literals": ["a", "b"]},
            {"source": "protocol.message", "cause": "quota", "scope": "availability-domain",
             "all_literals": ["b", "a"]}
        ]));
        assert!(parse_chain(&raw, Some(PRIMARY), None).is_ok());
    }

    #[test]
    fn preflight_skip_and_onset_probe_round_trip() {
        let mut c1 = candidate(1, "x", "fallback");
        c1["preflight_skipped"] = serde_json::json!({
            "cause": "quota", "scope": "availability-domain", "hold_key": "codex"
        });
        let mut value: serde_json::Value = serde_json::from_str(&chain_json(serde_json::json!([
            candidate(0, PRIMARY, "primary"),
            c1
        ])))
        .unwrap();
        value["onset_probe"] = serde_json::json!({"kind": "remote_publish_branch"});
        let chain = parse_chain(&value.to_string(), Some(PRIMARY), None).unwrap();
        assert_eq!(chain.onset_probe, Some(OnsetProbe::RemotePublishBranch));
        assert!(chain.candidate(1).unwrap().is_preflight_skipped());
        let reparsed: AcpFallbackChain =
            serde_json::from_str(&serde_json::to_string(&chain).unwrap()).unwrap();
        assert_eq!(reparsed, chain);
    }
}
