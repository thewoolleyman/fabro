# ACP fallback chain: bounded in-node candidate failover (factory S4)

Status: design, 2026-09-12. Base: `factory-integration` at `4b8cc85e0`.
Consumer contract: livespec-orchestrator-beads-fabro `SPECIFICATION/contracts.md`
section "Factory-configurable ACP fallback priority" (ratified v109), the
paragraphs "Reactive fallback is one bounded node visit" and the event half of
"Events and projection are compatible, idempotent, and leak-free"; Scenario 127.
Ledger item: `bd-ib-mujvyn` (plan epic `bd-ib-jxvgq5`).

## Problem

An ACP node resolves exactly one `acp.command` and calls `run_acp_turn` once.
When that one adapter's model is removed from the account catalog, or the
provider is out of quota, the node dies and the whole run parks at its human
gate after every earlier node has already done its work. The Dispatcher
(slices S1 to S3, already merged) now resolves an ORDERED CANDIDATE CHAIN per
node and filters it through observed availability holds before admission, but
Fabro still consumes a single adapter, so a failure that only surfaces at run
time cannot advance to the next candidate.

## Externally observable contract (what this slice must exhibit)

- One run id, one sandbox identity, one node deadline, no predecessor-node
  replay, ordered candidate attempts, normal continuation on success.
- Every candidate shares the node's ORIGINAL wall-clock deadline and receives
  only the remaining time. One node timeout, never `candidates × timeout`.
- Within one engine handler attempt each candidate is tried at most once, in
  order. `engine_attempt` and `candidate_index` are distinct.
- Before any failover, an outer retry for a NON-eligible transient failure may
  retry the preflight-selected candidate. After the first reactive transition
  the node visit is NON-retryable: neither outer retry nor checkpoint/resume
  may replay an attempted or preflight-skipped candidate.
- The attempted set, original deadline and event ids survive resume.
- Exhaustion is a typed, non-retryable node outcome preserving the final
  cause.
- Fallback eligibility is a CLOSED typed decision, separate from
  `classify_failure_reason`: non-eligible classes first, then configured
  signature matching with machine-code precedence, generic 400/404 needs a
  discriminator, runtime multi-match with distinct dispositions is ambiguous
  and non-eligible, never first-match.
- Side-effect onset gate: a durable ledger writes onset before an external
  mutation; missing, incomplete or unknown evidence FAILS CLOSED (terminate
  without fallback). Sandbox-local partial changes may pass to the successor
  with a delimited recovery preamble only with durable proof every completed
  tool operation was sandbox-local. For the `pr` node a publish branch or PR
  present on the remote at takeover proves onset; an unreadable remote counts
  as already past onset.
- Each reactive (or actually executed preflight) transition emits ONE
  `agent.acp.failover` event, `schema_version: 1`, stable event id, occurrence
  time, node visit, engine attempt, candidate indexes and durations, from/to
  display and machine identities, selected hold key, typed cause/scope,
  primary-generation fingerprint and full-chain digest, and NOTHING else: no
  command, env value, credential, prompt, raw error or unredacted diagnostic.
  The native `agent.failover` event and its stored-run consumers stay
  byte-compatible.
- The server advertises the capability explicitly through the system-info
  surface so a Dispatcher can refuse new grammar against an older engine.

## Design

### D1. Transport: `acp.fallback_chain` node attribute

A new optional string attribute on an ACP node carrying a JSON document. It is
an ordinary string attribute, so `{{ inputs.<node>_chain }}` renders into it
post-parse exactly as `acp.command` does today (template expansion runs over
parsed attribute VALUES, so JSON quotes are not a DOT-quoting hazard).

```json
{
  "schema_version": 1,
  "primary_generation": "<32 hex>",
  "full_chain": "<32 hex>",
  "onset_probe": null | {"kind": "remote_publish_branch"},
  "candidates": [
    {
      "candidate_index": 0,
      "display_name": "built-in Anthropic ACP adapter",
      "candidate_key": "builtin-anthropic-…",
      "availability_key": "anthropic",
      "command": "ANTHROPIC_MODEL=… npx -y @agentclientprotocol/claude-agent-acp",
      "availability_signatures": [
        {"source": "process.terminal_diagnostic", "cause": "model_unsupported",
         "scope": "candidate",
         "all_literals": ["requested model",
                          "is not supported when using codex with a chatgpt account"]}
      ],
      "preflight_skipped": null | {"cause": "quota", "scope": "availability-domain",
                                   "hold_key": "codex"}
    }
  ]
}
```

Rules, all refused with `Error::Validation` BEFORE any adapter starts:

- `schema_version` must be exactly 1; unknown top-level or candidate keys
  refuse (closed grammar, same posture as the Dispatcher's S1 parser).
- `acp.command` MUST still be present and MUST equal
  `candidates[0].command` byte-for-byte. `acp.config` with a chain refuses.
  This keeps candidate zero byte-identical with the legacy path and keeps
  `fabro inspect` showing what actually ran.
- `candidate_index` values are contiguous from 0 in array order; every
  candidate carries all three identity fields, non-empty; `(availability_key,
  candidate_key)` pairs are unique within the chain.
- Signatures use the closed grammar: `source` in
  {`process.terminal_diagnostic`, `protocol.machine_code`,
  `protocol.message`}; `cause` in the eight ratified causes; `scope` in
  {`availability-domain`, `candidate`}; machine-code source requires
  `machine_code` and forbids `all_literals`, every other source the reverse;
  `exit_code` 1..=125 refines only; `hold_key` only at domain scope; two
  statically identical matchers with different dispositions refuse.
- A node with NO `acp.fallback_chain` runs the existing single-adapter path
  unchanged (byte-identical behaviour, no new events).

Each candidate carries ONE `command` string: the Dispatcher's rendered
adapter (`render_adapter`: env pairs as `KEY=value` with shell quoting, sorted
by key, then the program and args), which `AcpProcessSpec::from_command_attr`
parses back into env, program and args. That is the flattening rule for the
contract's `command`/`args`/`env`; the string is never copied into an event.
An `acp.config` (JSON) node cannot carry a chain in this slice: candidates are
command-form, and the Dispatcher only ever renders `acp.command`. This is a
narrowing recorded here, not a contract requirement.

Fabro carries NO built-in measured-signature table. The Dispatcher already
owns the measured built-in table (`_acp_builtin_signatures.py`) and renders
the EFFECTIVE signatures (configured plus built-in) into each candidate when
it emits the chain. One table, one owner, no drift.

### D2. Eligibility decision surface

New module `fabro-workflow/src/handler/llm/acp_fallback/` (chain parsing,
signal, classifier, visit state, event assembly). The classifier is
equivalent to the Dispatcher's S2 modules on every input they share; the one
Dispatcher step it omits, "identity absent", cannot arise because the chain
grammar requires identity on every candidate:

1. Build an `AcpFailureSignal` from the `AcpError`: `Cancelled` declares
   `cancellation`; `TimedOut` declares `node_deadline`; `PermissionTimedOut`
   and `BackgroundedTool` declare `declared_non_eligible`; `Command(_)`
   declares `malformed_configuration`; `Sandbox`/`Cleanup` declare
   `unattributed_sandbox_or_transport`; `StopReason` is non-eligible
   `unmatched` (a model stop is not availability); `ProcessExited` supplies
   `terminal_diagnostic` (stderr then stdout tail) and `exit_code`;
   `Protocol` supplies `machine_code` (the JSON-RPC error code, when present)
   and `protocol_message` (message, with `data` rendered compactly).
   `turn_started` is true once any session update, tool event or activity
   tick was observed for the candidate.
2. Non-eligible pre-checks in the contract's order: declared class, exit
   status 126/127 (command not found) and >= 128 (signal), then the text
   marker conjunctions (remote-compaction 404, authentication, command not
   found, cancellation, node deadline, stall, malformed configuration,
   sandbox/transport, non-convergence) using the same normalized conjunction
   matching as configured literals.
3. Pre-turn failure with nothing on any readable field is not provider
   evidence.
4. Match configured signatures against the ONE field the source names;
   `exit_code` refines. On a generic HTTP 400/404 diagnostic a text signature
   counts only if it names a literal outside the generic-status vocabulary;
   a machine-code signature always discriminates.
5. Machine-code matches, when any, are the selected SET (precedence is a
   filter, not a sort); distinct dispositions in the selected set are
   ambiguous and non-eligible; one disposition is the typed verdict with its
   hold key resolved (domain scope: `hold_key` override or the candidate's
   `availability_key`; candidate scope: the exact pair).

`classify_failure_reason` is untouched and keeps categorizing terminal
failures for routing.

### D3. One bounded visit

`AgentAcpBackend::run_turn` becomes a loop over the chain when one is present:

- Visit entry: parse chain; compute the visit deadline
  `visit_start + node.timeout()` (none when the node has no timeout) UNLESS a
  durable deadline for this `(node, visit)` already exists (see D6), in which
  case the ORIGINAL deadline is reused.
- Skip candidates that are `preflight_skipped` or already launched by this
  handler entry. BEFORE the first reactive transition the legacy posture
  holds: an outer retry (or a resume) may re-launch the preflight-selected
  candidate and receives a full node timeout of its own, exactly as a node
  without a chain does. AFTER the first reactive transition nothing attempted
  is ever launched again and the visit's ORIGINAL deadline binds every later
  candidate, across retry and resume. When the first EXECUTED candidate index
  is above 0 because of preflight skips, emit one `agent.acp.failover` with
  `transition: "preflight"` carrying the skip's typed cause/scope/hold key.
- Per candidate: `timeout_ms = remaining(deadline)`; if none remains, stop
  with the node-deadline identity. Run `run_acp_turn` for that candidate's
  `AcpProcessSpec` (parsed from its `command` with the existing
  `from_command_attr`). Emit `agent.acp.started` as today, now also carrying
  the additive `candidate_index` and `chain_deadline_epoch_ms`.
- Success: normal `CodergenResult`; clear nothing (the durable state records
  the successful index for the warning-lifecycle consumer in S5).
- Failure: classify (D2). Non-eligible: terminate with the ORIGINAL error
  mapping (`acp_error_to_workflow`) if no transition happened yet in this
  visit, else wrap the same category as non-retryable (D4). Eligible: consult
  the onset ledger (D7); not proven sandbox-local: terminate without fallback,
  preserving the original identity and the fail-closed reason in the message.
  Proven: emit `agent.acp.failover` (`transition: "reactive"`), append the
  index to `attempted`, persist state, advance to the next candidate with the
  original prompt plus the recovery preamble when sandbox-local partial
  changes exist.
- Exhaustion (no candidate remains after at least one attempt): one durable
  `agent.acp.exhausted` event (schema_version 1) records the final
  candidate's identity and typed cause structurally, then the node reports a
  typed, non-retryable outcome whose category follows the final eligible
  cause (`quota` -> BudgetExhausted; `rate_limit`, `provider_capacity`,
  `provider_server_unavailable` -> TransientInfra; the four `model_*` causes
  -> Deterministic) and whose message names the typed cause and the attempted
  candidates by display name only. A resumed visit whose durable record is
  already exhausted launches nothing and reports the same typed outcome.
- A reached node whose chain has NO launchable candidate at all (every
  candidate preflight-skipped) is the contract's conditional-node case: it
  emits the exhausted event with the skip's typed cause and then terminates
  the RUN through `Error::TerminateRun`, which the node handler maps to the
  engine's run-blocking error, so the node never traverses a continuation or
  failure edge into janitor, non-convergence or a human gate.
- `engine_attempt` comes from `internal.acp_attempt.<node>`, which the
  lifecycle writes at every attempt boundary (`after_attempt`: the attempt
  number when a retry follows, `0` when the node's attempts end), so a
  handler entered by the next attempt can name its own attempt; the retry
  counter the lifecycle writes after the node completes is too late.

### D4. Non-retryable typed outcome

Add `Error::HandlerNonRetryable { message, failure_class, exec_output_tail,
source }` to `fabro-workflow::error::Error`. It is `Handler`-shaped but
`is_retryable()` is false while `failure_category()` returns `failure_class`,
so `should_retry` (default `err.is_retryable()`) refuses an outer retry and
`fabro-core` turns it into a fail outcome that routes through the normal
`outcome=failed` edges. Used for (a) any failure after the first reactive
transition and (b) exhaustion. Nothing else changes for legacy nodes.

### D5. Event

`Event::AgentAcpFailover { node_id, visit, props }` in fabro-workflow and
`EventBody::AgentAcpFailover(AgentAcpFailoverProps)` in fabro-types with serde
name `agent.acp.failover`. Fields (all non-secret): `schema_version: u32 = 1`,
`event_id: String` (UUID v4 minted at emission and persisted with the visit
state), `occurred_at_ms: u64`, `visit: u32`, `engine_attempt: u32`,
`transition: "reactive" | "preflight"`, `from_candidate_index`,
`to_candidate_index`, `from_display_name`, `to_display_name`,
`from_candidate_key`, `from_availability_key`, `to_candidate_key`,
`to_availability_key`, `from_duration_ms: u64`, `hold_key: String`,
`cause: String`, `scope: String`, `signature_source: String`,
`primary_generation: String`, `full_chain: String`,
`attempted: Vec<u32>`, `attempted_durations_ms: Vec<u64>` (parallel to
`attempted`), `skipped: Vec<u32>`, `chain_deadline_epoch_ms: Option<u64>`.
A second versioned event, `agent.acp.side_effect` (schema_version 1: visit,
candidate index, tool call id, tool kind, bounded title, classification,
observed_via), is the durable side-effect ledger (D7). Both versioned bodies
are declared in `fabro-types` beside the native props and are exported at the
crate root.
A unit test serializes an event assembled from a chain carrying a fake
credential in its command and asserts the JSON contains no `command`, `env`,
prompt, credential literal, or diagnostic text. `Event::Failover` /
`FailoverProps` are not touched; a round-trip test over a stored native
`agent.failover` JSON fixture pins byte compatibility. `docs/internal/events.md`
gains the new section. Consumers that enumerate ACP events (server steering
cleanup, fork replay filter, run-state projection) are updated where the
compiler demands; the new event is not a terminal event for any of them.

### D6. Durable visit state and resume (revised after review)

The run's event stream is the ONE durable source for visit state, and every
ledger write is made durable SYNCHRONOUSLY before the action it records is
allowed to proceed. The first draft kept an in-memory ledger plus a context
key written at candidate boundaries; review showed that `Emitter::emit`
only hands the event to listeners, the store logger (`RunEventLogger`)
queues asynchronously, and the node handler diffs the context back only after
the handler returns, so a crash inside that window could replay an attempted
candidate. The revision closes the window:

- `RunEventLogger` (the store-backed listener created in
  `operations/start.rs` before initialization, `Clone`, with an existing
  `flush().await` that resolves once every queued event is written) is kept
  on `RunServices` as `progress_logger`.
- `CodergenRunRequest` gains three fields the agent handler fills from
  `EngineServices`: `durable_events: Option<RunEventLogger>`,
  `run_store: Option<RunStoreHandle>`, and
  `publish_branch: Option<String>` (from `services.git_state()`, which is set
  before the executor runs any node). This is the request-shape change the
  review asked for instead of construction-time wiring, which happens before
  git state exists.
- Every durable write is `emit` + `flush().await`, and the awaited flush is
  the barrier: candidate `i` is launched only after
  `agent.acp.started{candidate_index: i, chain_deadline_epoch_ms}` is stored;
  the next candidate is launched only after the `agent.acp.failover` event is
  stored; a tool is allowed to run only after its side-effect ledger entry is
  stored (D7).
- On visit entry the handler lists the run's events (`run_store.list_events`)
  and keeps those for this `node_id` whose props carry this `visit`:
  `agent.acp.started` with a `candidate_index` marks that candidate ATTEMPTED
  and yields the original deadline; `agent.acp.failover` yields the attempted
  and skipped sets, the event ids and the deadline; `agent.acp.side_effect`
  entries rebuild each candidate's onset ledger; a started candidate with no
  later completion, failover or terminal ACP event for that index is treated
  as attempted with UNKNOWN onset. Nothing attempted or skipped is ever
  launched again, across outer retry, checkpoint or resume.
- Without a run store or logger (unit tests that construct the backend
  directly) the in-memory state alone applies, which those tests declare.

Versioned bodies stay observable: a stored `agent.acp.failover` or
`agent.acp.side_effect` whose `schema_version` this build does not understand,
or whose body fails to parse, is surfaced as `EventBody::Unknown` with its
properties preserved byte-for-byte instead of a parse failure. Every other
known event keeps its strict parse.

### D7. Side-effect onset ledger (revised after review)

The ledger is the durable `agent.acp.side_effect` event (schema_version 1:
visit, candidate index, tool call id, tool kind, bounded title,
classification, observed_via). "Write onset before issuing an external
mutation" is satisfied at the only pre-execution hook ACP offers:

- `run_acp_turn` gains `on_permission_observed`, an ASYNC callback awaited
  for every `session/request_permission` BEFORE the answer is sent, under
  both the inline `auto` policy and the parked `ask` policy. The ACP backend's
  callback emits the ledger entry and awaits the flush barrier, so the answer
  that lets the adapter execute the tool is sent only once the entry is
  stored. `on_tool_event(Started)` is the backstop for an adapter that runs a
  tool without requesting permission; it records the same entry, but by then
  onset may already have happened, so a candidate whose ledger holds an
  external-or-unknown entry recorded only via `tool_started` is treated as
  past onset like any other.
- Tool kinds `read`, `search`, `think`, `edit`, `delete`, `move`,
  `switch_mode` are sandbox-local; `execute`, `fetch`, `other` and anything
  unknown are external-or-unknown. There is no way to prove a shell command
  was sandbox-local, so the gate is conservative by design; the main case
  this slice serves (a model refused at the first request) fails BEFORE any
  tool call and passes trivially.
- A failed candidate whose ledger holds only sandbox-local entries passes its
  partial state to the successor with the delimited recovery preamble (D3).
  An EMPTY ledger is proof only when the turn never started (no session
  update, tool event or activity tick after launch): the ledger is fed by the
  adapter's own reports, so a started turn with no entries may have run an
  unreported tool and fails closed. Any external-or-unknown entry: terminate
  without fallback, preserving the failure's original identity. The tool
  kind is the adapter's claim; there is no stronger evidence available to the
  engine, which is why the allowlist is narrow and everything else fails
  closed. A durable `agent.acp.failover` from a candidate proves its gate was
  passed at decision time, so a resumed visit does not re-litigate it; a
  candidate whose only durable record is `started` is unknown and fails
  closed once a transition is attempted.
- `onset_probe: {"kind": "remote_publish_branch"}` (set by the Dispatcher on
  any publishing node, the `pr` node today) runs before any transition, in the sandbox, with
  the ACP launch env (the same credentials the publishing agent uses):
  `git ls-remote --exit-code --heads origin <publish_branch>` and
  `gh pr list --head <publish_branch> --state all --json number --limit 1`.
  A branch present (exit 0) or a non-empty pull-request list proves onset
  and terminates. Branch absent (exit 2) AND an empty list: no onset,
  continue. Any other outcome, a missing `publish_branch`, a missing `gh`, or
  an exec failure is unreadable and treated as already past onset. This
  covers "a publish branch or its pull request present on the remote".

### D8. Deadline

`remaining = deadline.saturating_duration_since(now)`; passed as
`timeout_ms`. `NodeTimeoutPolicy::HandlerManaged` is unchanged. A candidate
never receives more than the node's remaining time, so the node timeout is
never multiplied.

### D9. Capability advertisement

OpenAPI `SystemInfoResponse` gains optional `capabilities: string[]`;
`fabro-api` regenerates the Rust type at build; the server handler emits
`["acp.fallback_chain.v1"]` from a constant exported by fabro-workflow; `fabro
system info` prints a `Capabilities:` line and the JSON carries the array. An
older generated client ignores the unknown field (serde default posture), and
a server-side test plus a CLI integration test assert the value. The
TypeScript client is regenerated from the spec.

## Out of scope (owned by other slices)

Dispatcher-side event fetch, projection, holds and the model-fallback warning
lifecycle (S5); per-attempt cost (S6); pinning this build on hp/vps, the
`dispatcher.minimum_release` floor, the remote capability probe, rendering the
chain input from the resolved S3 chain, and the end-to-end factory proof
(S7). This slice enables no real fallback configuration by itself.

## Test plan

Unit (fabro-workflow): chain parse (byte-identity refusal, schema version,
unknown keys, identity, contiguous indexes, duplicate pairs, signature
grammar, config+chain refusal); classifier (measured Codex 400 falls back,
generic 400 does not, exit 127, auth literal overlap, machine-code
precedence, ambiguity, pre-turn, declared classes); deadline arithmetic;
event assembly and redaction; native failover fixture round-trip; visit
state persistence and no-replay on resume; onset ledger transitions;
non-retryable error variant. Integration (fabro-cli `tests/it/workflow`): a
fake ACP agent whose first candidate exits with a scripted terminal
diagnostic and whose second candidate succeeds, asserting one
`agent.acp.failover` event, node success, unchanged run/sandbox, and no
predecessor replay; system-info capability in both the server and CLI tests.
