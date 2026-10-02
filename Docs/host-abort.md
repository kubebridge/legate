# Targeted host abort

Host abort acceptance is independent of session activation and completion routing.
The configured `ISessionStore` must implement `ISessionAbortControlStore`.
Unsupported custom providers fail startup rather than accepting undurable requests.

Read `SessionClient.ReadAbortTargetAsync(sessionId, cancellationToken)` without
activating an actor. If a target exists, capture its `TurnId` and call
`AbortAsync(sessionId, expectedTurnId, cause, reason, cancellationToken)`.
Only `ExplicitAbort` and `HostShutdown` are host stop causes. Reasons must contain
at most 512 non-control characters and must not contain credentials or tool payloads.

`HostAbortReceipt` distinguishes Accepted, AlreadyAccepted, NoCurrentTurn,
TargetChanged and AlreadyTerminal. Accepted means the exact-target intent committed,
not that the turn completed, its inbox entry was consumed, an observer was notified,
or its completion destination received anything. WaitingForInput and Closed refuse
new requests, except that existing exact-target intent/terminal evidence remains
readable according to its retry precedence. Tenant identity comes from the client,
not from a request payload or execution token.

The original accepted timestamp, cause and reason are immutable. Cancellation or
transport loss after commit can leave the caller uncertain. Retry the **same target**;
never reread and silently send the old request to a newer entry. A retry returns
AlreadyAccepted with the original receipt, including after retirement or session close.
A missing/purged session remains missing. AlreadyTerminal identifies a committed
control verdict, not complete cleanup or publication.

## Runtime control attribution

The journal prime is retained as the execution authority. Each selected real inbox
entry gets a separate stable control target and retains its original inbox position
through suspension and consumption. It has no separate lease or heartbeat.

Providers serialize intent acceptance with binding, admission, control decision and
retirement. A host stop committed before `TryDecideControlTarget` selects Aborted,
even if a previously admitted call reports success. A decision committed first is
retained and subsequent host requests observe AlreadyTerminal. Decision identifiers
provide idempotency, never authority. Retirement requires the exact decision,
genuine current prime authority and provably consumed original entry.

An accepted target or a TerminalPendingRetirement target found after restart remains
blocked before any prime, recovery reset, model call or queue drain. Restoring the
execution configuration alone does not resume it. Mandatory decision, consume or
retirement failure preserves the control barrier. Complete atomic terminal cleanup
and publication are separate work; a control verdict must not be used to infer
that downstream effects happened or to replay them after a crash.

Actual model/tool admission checks honor durable stop alongside claim fencing.
Unstopped Active targets are not control barriers. `TryRecoverControlTarget` can
re-fence their expired existing prime, preserving the same target and original entry,
including consumed entries. A live owner refuses takeover. Recovery never consumes
queued work to acquire authority; both crash policies and suspended Reply recovery
use the original association. Accepted stops and pending terminal verdicts cannot
use this recovery operation. Dispatcher refusals are isolated to their session so
unrelated eligible sessions continue through subsequent sweeps.

Requests already admitted before acceptance may finish and external effects cannot
universally be recalled. There is no rollback or exactly-once transport promise.

## Breaking 0.1.0 contracts and supported data

The untargeted public AbortAsync overload is removed. Host code must supply an exact
target and handle a receipt rather than Task-only or snapshot-only success. Custom
stores must implement the required control capability with the same atomicity rules.

Abort actor/entity wire payloads are version 2 exact-tenant/session/target wake hints.
Their old untargeted version 1 manifests and payloads are rejected, including the usual
current-minus-one reader exception. A wake hint reads stored intent and cannot create
authority or stop a different target. Local-only legacy actor helpers are not the
supported public host-control API and cannot be serialized remotely.

Current control records use explicit format version 1. The additive shared migration
creates `session_control`; new session creation writes the current version. Existing
sessions without control records, unknown versions, Running rows without a provable
binding and ambiguous original-entry associations fail closed with a clean-start
diagnostic. There is no guessed backfill, legacy rebinding, manufactured history or
automatic database deletion. Preserve old databases for inspection and use new
sessions/current-format storage for execution. Landed migrations are not changed.
