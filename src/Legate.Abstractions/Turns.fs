// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Text.Json.Serialization

// Turn contracts. A Turn is one run of the ReAct loop triggered by a
// UserMessage: it ends when the model stops calling tools, the host aborts,
// or the budget runs out, and it suspends while waiting on a Reply. The
// turn is the lease and claim unit of the runtime; the claim token and
// owner that fence its side effects belong to the store contract's claim
// type and stay off this public surface. TurnResult is what a host that
// waits on a turn receives when the turn settles. Usage never carries cost:
// pricing is a host concern. All types serialise with System.Text.Json and
// are constructible from C# through property setters.

/// The lifecycle of a turn. A turn opens Pending, enters Running while the
/// ReAct loop executes, may suspend while waiting on a host reply, and ends
/// in one of the three terminal states.
type TurnStatus =

    /// The turn is accepted and journaled but not yet claimed by the
    /// session. Transitions: to <see cref="F:Legate.TurnStatus.Running" />
    /// when the session claims the turn under a lease.
    | Pending = 0

    /// The ReAct loop is executing: model iterations, tool calls, and event
    /// journaling. Transitions: from
    /// <see cref="F:Legate.TurnStatus.Pending" /> when the turn is claimed;
    /// to <see cref="F:Legate.TurnStatus.Suspended" /> when the turn waits
    /// on a reply; to a terminal state when the turn settles.
    | Running = 1

    /// The turn is suspended awaiting a host reply (a permission decision
    /// or a question answer). Transitions: from
    /// <see cref="F:Legate.TurnStatus.Running" /> when the agent asks; back
    /// to <see cref="F:Legate.TurnStatus.Running" /> when the host replies.
    /// Messages prompted while waiting join the inbox instead of starting
    /// a turn.
    | Suspended = 2

    /// Terminal: the model stopped calling tools and the turn finished
    /// normally. No transitions leave Completed.
    | Completed = 3

    /// Terminal: the host aborted the turn. No transitions leave Aborted.
    | Aborted = 4

    /// Terminal: the turn failed inside the loop (a tool or provider
    /// error) or its budget ran out (model iterations or wall-clock time).
    /// No transitions leave Failed.
    | Failed = 5

/// Why a turn stopped instead of completing: the exactly-one-winner of the
/// stop-cause arbitration (issue 35). Settlement and stop are mutually
/// exclusive: at most one of the turn's completion and these causes lands
/// on the turn, and the winner is recorded on
/// <see cref="T:Legate.Turn" />.StopCause plus
/// <see cref="T:Legate.TurnAborted" /> or
/// <see cref="T:Legate.TurnFailed" />. A flat enum, never a stringly reason,
/// so hosts branch on it without parsing messages.
type StopCause =

    /// The host aborted the turn through <c>Abort</c> while it was running.
    /// Settles the turn as <see cref="F:Legate.TurnStatus.Aborted" /> with a
    /// <see cref="T:Legate.TurnAborted" /> outcome.
    | ExplicitAbort = 0

    /// The turn spent its wall-clock budget: the hard deadline enforced
    /// through the injected <see cref="T:Legate.ILlmDelay" /> seam fired.
    /// Settles the turn as <see cref="F:Legate.TurnStatus.Failed" /> with a
    /// <see cref="T:Legate.TurnFailed" /> outcome carrying the timeout
    /// reason.
    | Deadline = 1

    /// The turn lost its lease (expiry or takeover): the loser stops with
    /// zero further effects and never settles, so this cause propagates
    /// instead of landing on the turn.
    | LeaseLoss = 2

    /// The host shut down while the turn was running. Settles the turn as
    /// <see cref="F:Legate.TurnStatus.Aborted" /> with a
    /// <see cref="T:Legate.TurnAborted" /> outcome, like an explicit abort.
    | HostShutdown = 3

/// Token usage for one turn: input and output token counts, aligned with
/// the long counters Microsoft.Extensions.AI reports. Usage never carries
/// cost: pricing is a host concern, computed from these counts outside the
/// runtime. Serialises with System.Text.Json.
[<CLIMutable>]
type UsageSummary =
    {
        /// The number of input tokens the turn consumed.
        InputTokens: int64
        /// The number of output tokens the turn produced.
        OutputTokens: int64
    }

/// One run of the ReAct loop in one session: the lease and claim unit of
/// the runtime. A turn is claimed under a lease, renewed on a heartbeat,
/// and fenced by a claim token; the token and its owner live in the store
/// contract's claim type, never on this public surface. Attempt is 1-based
/// and incremented on resume, so a resumed run continues the same turn id.
/// Error is populated on terminal failure and never contains secrets or
/// tool arguments (the exception-message rule). Constructible from C#
/// through property setters and serialises with System.Text.Json.
[<CLIMutable; NoComparison>]
type Turn =
    {
        /// The turn's durable identifier.
        Id: TurnId
        /// The session the turn runs in.
        SessionId: SessionId
        /// The 1-based attempt number: 1 when the turn first runs,
        /// incremented each time a resumed run continues it.
        Attempt: int
        /// The turn's current lifecycle state.
        Status: TurnStatus
        /// When the turn was first claimed and started.
        StartedAt: DateTimeOffset
        /// When the turn settled (Completed, Aborted, or Failed), or empty
        /// (HasValue is false) while the turn is still in flight.
        CompletedAt: Nullable<DateTimeOffset>
        /// The number of model iterations the loop has spent.
        Iterations: int
        /// The token usage checkpointed for the turn so far.
        Usage: UsageSummary
        /// Why the turn failed, populated when Status is Failed and null
        /// otherwise. Never contains secrets or tool arguments.
        Error: string | null
        /// Which stop cause settled the turn, or empty (HasValue is false)
        /// when the turn completed normally or is still in flight. One of
        /// <see cref="T:Legate.StopCause" />: abort and host-shutdown causes
        /// land with Status Aborted, the deadline cause with Status Failed,
        /// and lease loss never lands (the loser stops with zero effects).
        StopCause: Nullable<StopCause>
    }

/// Why a turn refused to run without invoking the runner: the actor-side
/// per-turn authority gate's exactly-one reason, so hosts branch on it
/// without parsing messages.
/// <see cref="T:Legate.TurnAgentRejected" /> carries the winner plus the
/// human-readable reason.
type AgentAuthorityFailure =

    /// No agent with the session's agent id exists in the session's tenant.
    /// Settles the turn as <see cref="F:Legate.TurnStatus.Failed" /> with a
    /// <see cref="T:Legate.TurnAgentRejected" /> outcome.
    | NotFound = 0

    /// The agent exists but is disabled for new turns. Settles the turn as
    /// <see cref="F:Legate.TurnStatus.Failed" /> with a
    /// <see cref="T:Legate.TurnAgentRejected" /> outcome.
    | Disabled = 1

    /// The agent exists but belongs to another tenant than the session.
    /// Settles the turn as <see cref="F:Legate.TurnStatus.Failed" /> with a
    /// <see cref="T:Legate.TurnAgentRejected" /> outcome.
    | TenantMismatch = 2

/// The structured completion of a settled turn, delivered only when the
/// session ran with the structured outcome mode. Serialises
/// polymorphically: every concrete outcome carries a stable <c>$type</c>
/// discriminator on the wire, mirroring <see cref="T:Legate.Reply" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<TurnFinished>, "turnFinished")>]
[<JsonDerivedType(typeof<TurnPartiallyFinished>, "turnPartiallyFinished")>]
[<JsonDerivedType(typeof<TurnAborted>, "turnAborted")>]
[<JsonDerivedType(typeof<TurnFailed>, "turnFailed")>]
[<JsonDerivedType(typeof<TurnAgentRejected>, "turnAgentRejected")>]
type TurnOutcome() = class end

/// The turn settled normally: the model stopped calling tools.
/// <param name="summary">A host-readable summary of what the turn accomplished.</param>
and [<Sealed>] TurnFinished(summary: string) =
    inherit TurnOutcome()

    /// A host-readable summary of what the turn accomplished.
    member _.Summary = summary

    /// True when the runtime synthesized this outcome because a structured
    /// turn stopped without calling finish or fail: the summary carries the
    /// final assistant text. False when the model called finish explicitly.
    /// Defaults to false, so payloads written before the flag existed read
    /// as explicit and the $type discriminator is unchanged.
    member val IsImplicit = false with get, set

/// The turn made progress but ended early: its budget (model iterations or
/// wall-clock time) ran out before the model stopped calling tools.
/// <param name="summary">A host-readable summary of the progress the turn made.</param>
and [<Sealed>] TurnPartiallyFinished(summary: string) =
    inherit TurnOutcome()

    /// A host-readable summary of the progress the turn made.
    member _.Summary = summary

/// The turn stopped before completing: the host aborted it or shut down
/// while it ran. Carries the typed stop cause (who stopped the turn) and
/// the reason (why), never a stringly reason alone: the cause is the
/// exactly-one-winner of the stop-cause arbitration.
/// <param name="cause">Which abort-family stop cause won: ExplicitAbort or HostShutdown.</param>
/// <param name="reason">Why the turn stopped. Never contains secrets or tool arguments.</param>
and [<Sealed>] TurnAborted(cause: StopCause, reason: string) =
    inherit TurnOutcome()

    /// Which abort-family stop cause won the arbitration for the turn.
    member _.Cause = cause

    /// Why the turn stopped. Never contains secrets or tool arguments.
    member _.Reason = reason

/// The turn failed inside the loop (a tool or provider error).
/// <param name="reason">Why the turn failed. Never contains secrets or tool arguments.</param>
and [<Sealed>] TurnFailed(reason: string) =
    inherit TurnOutcome()

    /// Why the turn failed. Never contains secrets or tool arguments.
    member _.Reason = reason

/// The turn refused to run: the agent was missing, disabled, or belongs to
/// another tenant, so the actor-side authority gate settled the turn without
/// ever invoking the runner. Carries the typed failure (which branch refused)
/// and the reason (why), never a stringly reason alone.
/// <param name="failure">Which authority branch refused the turn.</param>
/// <param name="reason">Why the turn refused to run. Never contains secrets or tool arguments.</param>
and [<Sealed>] TurnAgentRejected(failure: AgentAuthorityFailure, reason: string) =
    inherit TurnOutcome()

    /// Which authority branch refused the turn.
    member _.Failure = failure

    /// Why the turn refused to run. Never contains secrets or tool arguments.
    member _.Reason = reason

/// What a host that waits on a turn receives when the turn settles: the
/// final assistant text, the status the result was produced under, the
/// iterations and usage the turn spent, and, when the session ran with the
/// structured outcome mode, the <see cref="T:Legate.TurnOutcome" />.
/// AssistantText is never null: it is the empty string when the turn
/// produced no assistant text (the Session.Title precedent).
/// Constructible from C# through property setters and serialises with
/// System.Text.Json.
[<CLIMutable; NoComparison>]
type TurnResult =
    {
        /// The final assistant text. Never null: the empty string when the
        /// turn produced no assistant text.
        AssistantText: string
        /// The turn's status when the result was produced; a result waited
        /// on to settle carries a terminal status.
        Status: TurnStatus
        /// The number of model iterations the turn spent.
        Iterations: int
        /// The token usage the turn spent.
        Usage: UsageSummary
        /// The structured outcome, set only when the session ran with the
        /// structured outcome mode, or null otherwise.
        Outcome: TurnOutcome | null
    }
