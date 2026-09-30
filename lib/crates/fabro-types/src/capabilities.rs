//! Engine capabilities a server advertises through `GET /system/info`.
//!
//! A capability is a stable string a client can test for before sending
//! grammar an older server would silently ignore. The list is ADDITIVE: a
//! server that predates the field omits it entirely, and a client generated
//! from an older spec ignores the unknown field, so neither side breaks the
//! other.

/// Ordered ACP candidate failover within one node visit: the server
/// understands the `acp.fallback_chain` node attribute and emits the
/// versioned `agent.acp.failover` event.
pub const ACP_FALLBACK_CHAIN_CAPABILITY: &str = "acp.fallback_chain.v1";

/// In-protocol model and effort selection: a chain candidate may carry a
/// `config_options` object, and the ACP handler sets each requested option
/// through `session/set_config_option` after `session/new` and before the
/// first `session/prompt`, refusing the candidate before any prompt when the
/// agent does not advertise what was asked. A Dispatcher MUST refuse a chain
/// carrying `config_options` against a server that omits this string.
pub const ACP_CANDIDATE_CONFIG_OPTIONS_CAPABILITY: &str = "acp.candidate_config_options.v1";

/// Every capability this build advertises, in a stable order.
#[must_use]
pub fn advertised_capabilities() -> Vec<String> {
    vec![
        ACP_FALLBACK_CHAIN_CAPABILITY.to_string(),
        ACP_CANDIDATE_CONFIG_OPTIONS_CAPABILITY.to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertises_the_acp_fallback_chain_capability() {
        assert!(advertised_capabilities().contains(&"acp.fallback_chain.v1".to_string()));
    }

    #[test]
    fn advertises_the_candidate_config_options_capability_beside_the_chain() {
        assert_eq!(advertised_capabilities(), vec![
            "acp.fallback_chain.v1",
            "acp.candidate_config_options.v1"
        ]);
    }
}
