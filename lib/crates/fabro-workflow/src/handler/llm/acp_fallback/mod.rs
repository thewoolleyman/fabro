//! Bounded in-node ACP candidate failover.
//!
//! An ACP node may carry an ordered candidate chain (`acp.fallback_chain`,
//! see [`chain`]). When the executing candidate fails on a provider
//! AVAILABILITY condition (see [`classify`]), the same node visit advances to
//! the next candidate under the node's original deadline, in the same
//! sandbox, with no predecessor-node replay. Every other failure terminates
//! with its own identity, exactly as it does for a node with no chain.
//!
//! Design record: `docs/plans/2026-09-12-acp-fallback-chain.md`. Consumer
//! contract: livespec-orchestrator-beads-fabro `SPECIFICATION/contracts.md`,
//! "Factory-configurable ACP fallback priority".

pub mod chain;
pub mod classify;
pub mod events;
pub mod signal;
pub mod text;
pub mod visit;

/// The capability the server advertises through `system info` once this
/// grammar is understood. A Dispatcher refuses to send new grammar to a
/// server that does not list it. Defined in `fabro-types` so the server
/// handler and this engine module name the one constant.
pub use fabro_types::capabilities::ACP_FALLBACK_CHAIN_CAPABILITY;

/// The `schema_version` every `agent.acp.failover` event carries.
pub const FAILOVER_EVENT_SCHEMA_VERSION: u32 = 1;
