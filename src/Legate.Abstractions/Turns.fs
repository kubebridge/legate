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
    }

/// The structured completion of a settled turn, delivered only when the
/// session ran with the structured outcome mode. Serialises
/// polymorphically: every concrete outcome carries a stable <c>$type</c>
/// discriminator on the wire, mirroring <see cref="T:Legate.Reply" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<TurnFinished>, "turnFinished")>]
[<JsonDerivedType(typeof<TurnPartiallyFinished>, "turnPartiallyFinished")>]
[<JsonDerivedType(typeof<TurnFailed>, "turnFailed")>]
type TurnOutcome() = class end

/// The turn settled normally: the model stopped calling tools.
/// <param name="summary">A host-readable summary of what the turn accomplished.</param>
and [<Sealed>] TurnFinished(summary: string) =
    inherit TurnOutcome()

    /// A host-readable summary of what the turn accomplished.
    member _.Summary = summary

/// The turn made progress but ended early: its budget (model iterations or
/// wall-clock time) ran out before the model stopped calling tools.
/// <param name="summary">A host-readable summary of the progress the turn made.</param>
and [<Sealed>] TurnPartiallyFinished(summary: string) =
    inherit TurnOutcome()

    /// A host-readable summary of the progress the turn made.
    member _.Summary = summary

/// The turn failed inside the loop (a tool or provider error).
/// <param name="reason">Why the turn failed. Never contains secrets or tool arguments.</param>
and [<Sealed>] TurnFailed(reason: string) =
    inherit TurnOutcome()

    /// Why the turn failed. Never contains secrets or tool arguments.
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
