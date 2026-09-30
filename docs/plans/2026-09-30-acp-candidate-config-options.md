# ACP candidate config options: in-protocol model and effort selection

Fork slice for livespec-orchestrator-beads-fabro plan `bd-ib-jxvgq5`
(work-item `bd-ib-afcn3d`; spec v112 "Factory-configurable ACP fallback
priority" → "In-protocol model and effort selection"). Builds on the S4
chain handler recorded in `2026-09-12-acp-fallback-chain.md`.

## Problem

A chain candidate fixes its model on the command line. An agent that selects
its model through the ACP session config options (`configOptions` on
`session/new`, `session/set_config_option`) cannot be asked for one without
the handler speaking that part of the protocol, and a reader of the run's
events cannot see which model the agent actually confirmed.

## Externally observable contract

- **Grammar.** `acp.fallback_chain.candidates[i]` MAY carry
  `config_options: {"model": <value>, "effort": <value>}`; both keys are
  optional, at least one is required, values are non-empty text, and any
  other key refuses at chain validation (`deny_unknown_fields`). The chain
  is validated before any adapter starts, as before.
- **Handler order.** For a candidate with `config_options`, after
  `session/new` answers and before the first `session/prompt`, the handler
  reads the agent's advertised `configOptions`, then for each requested
  option in order (model, then effort): checks the option id is advertised
  and the value is among its offered values; sends
  `session/set_config_option`; and confirms the answer reports the requested
  value current. The prompt is not sent until every option is confirmed.
- **Refusal.** A missing option id, an unoffered value, or an unconfirmed
  set terminates the candidate BEFORE any prompt as a typed pre-turn
  refusal with its own identity: cause `model_unsupported` at candidate
  scope when the option is `model`, `malformed_configuration` otherwise.
  It is non-retryable and deterministic, emits no `agent.acp.failover` and
  no `agent.acp.started`, mints no hold, and never triggers reactive
  fallback (the classifier reports it non-eligible through
  `DeclaredClass::ModelUnsupported` / `MalformedConfiguration`).
- **Event.** `agent.acp.started` gains additive non-secret `model` and
  `effort` fields carrying the CONFIRMED values (absent when none were
  requested). It is now emitted from the ACP turn once the session is
  configured and flushed durably before the first prompt, rather than
  before the process spawns: the prompt is the first moment the agent can
  do work, so the durable "attempted" marker still precedes every side
  effect, and a crash during session setup legitimately re-launches a
  candidate that did nothing. Stored historical events round-trip.
- **Capability.** `GET /system/info` advertises
  `acp.candidate_config_options.v1` beside `acp.fallback_chain.v1`.

## Design notes

- **The "attempted before any side effect" barrier moves to the prompt.**
  The S4 record's D7 ("candidate i is launched only after started is
  stored") is restated as "no prompt is sent before started is stored".
  A candidate that fails before its prompt (spawn, initialize, session/new,
  a setup timeout, a config refusal) has done no work, so leaving no started
  record for it is correct: reconstruct re-launches it only where the chain
  contract already allows a legacy retry, and after a transition the chain
  terminates non-retryably on any such failure. Consequences a reader must
  expect: `AgentSessionActivated` now precedes `agent.acp.started` in the
  stream, and a legacy node whose process fails before `session/new`
  emits no started event at all.
- **An error answer to `session/set_config_option` is a typed refusal.**
  It is recorded as `set_refused` and never reaches the classifier: a
  protocol error there carries provider evidence, so an availability
  signature matching the agent's text would otherwise turn it into a
  failover and a minted hold (found in review before merge).
- **Confirmation reads the agent's final advertisement.** After every set,
  each requested option is checked current in the LAST `configOptions`
  list, so an agent that resets an earlier option when a later one changes
  cannot be prompted with a model the started event no longer describes.
- **Option ids are the spec's.** `model` and `effort` are the ids the
  ratified contract names; an agent advertising effort under another id
  refuses as `malformed_configuration`, which is the contract's answer.

- `attach_session` in agent-client-protocol 0.11.1 drops `configOptions`
  from the `session/new` response, so `run_acp_turn` sends
  `NewSessionRequest` itself, negotiates on the raw response, and only then
  calls `attach_session`. Legacy adapters with no options take the same
  path with zero `session/set_config_option` calls.
- The connection closure can only return a protocol error, so the typed
  refusal travels through a slot (`config_refusal`) checked ahead of the
  generic protocol mapping, the same shape as the permission-timeout slot.
- `AcpRunRequest.on_session_configured` is the seam the workflow uses to
  emit and flush `agent.acp.started`; it is awaited inside the turn.
- The `unstable_boolean_config` feature is off, so `SetSessionConfigOptionRequest`
  carries a `SessionConfigValueId`; select is the only option kind read.

## Out of scope

Deploying the build to hp and vps, the release floor and the end-to-end
proof (S7); the Dispatcher-side rendering of `config_options`
(`acp-structured-candidate-dispatcher`).

## Test plan

- `fabro-workflow` `acp_fallback::chain` unit tests: grammar accepted in
  model-then-effort order; unknown id, empty object and blank value refuse.
- `fabro-acp` unit tests for `check_config_option` / `current_config_value`;
  integration tests with the fake agent proving the wire order
  (`initialize`, `session/new`, `set_config_option` ×2, `prompt`), the hook
  running before the prompt, and a typed `ConfigOptionRefused`.
- `fabro-workflow` `acp_chain_tests`: options confirmed on `started`; an
  unadvertised model refuses as `model_unsupported (candidate)` with no
  prompt, no failover, no started event, and a fallback candidate that never
  runs; an unadvertised effort refuses as `malformed_configuration` after
  the model was set; an acknowledged-but-unapplied option is not confirmed.
- `fabro-types` round-trip with and without the new fields; server and CLI
  `system info` capability lists.
