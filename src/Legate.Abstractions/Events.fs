// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Text.Json.Serialization

// Session event contracts. A SessionEvent is one entry of the fine-grained,
// ordered, journaled stream a session emits while a turn runs: what hosts
// render live (a CLI printing text as it arrives, a web UI showing tool
// progress) and what stores persist so Subscribe(fromSequence) can resume.
// Sequence numbers are per-session, monotonic, and assigned by the store on
// append: an event carries an empty Sequence while it is in flight, and the
// store stamps the next per-session value when it journals the event, so
// replay hydrates the same number through the constructor. The hierarchy
// mirrors the TurnOutcome and Reply precedents: an abstract base annotated
// with JsonPolymorphic and one sealed subtype per event kind, each carrying
// only the data that kind needs, serialised polymorphically with
// System.Text.Json. Sub-agent activity is a tool call, so events raised
// inside a sub-agent turn carry the tool-call id of the parent call that
// spawned them on ToolCallId; events raised by a tool call itself carry it
// on ToolCallId too. Redaction and size bounds are applied by the journal
// writer, not by these types: deltas and outputs here are already the
// shapes a host may render. Usage never carries cost: pricing is a host
// concern.

/// One entry of the fine-grained, ordered, journaled event stream a session
/// emits while a turn runs. Hosts render these events and stores persist
/// them. Serialises polymorphically with System.Text.Json: every concrete
/// event carries a stable <c>$type</c> discriminator on the wire, mirroring
/// <see cref="T:Legate.TurnOutcome" /> and <see cref="T:Legate.Reply" />.
/// The discriminator set is a wire contract:
/// <list type="table">
/// <item><term>turnStarted</term><description>a turn began running.</description></item>
/// <item><term>textDelta</term><description>assistant text arrived.</description></item>
/// <item><term>reasoningDelta</term><description>assistant reasoning arrived.</description></item>
/// <item><term>toolCallStarted</term><description>a tool call began.</description></item>
/// <item><term>toolCallOutput</term><description>tool output arrived.</description></item>
/// <item><term>toolCallCompleted</term><description>a tool call settled.</description></item>
/// <item><term>permissionRequested</term><description>a tool call needs a decision.</description></item>
/// <item><term>permissionResolved</term><description>a decision was applied.</description></item>
/// <item><term>questionAsked</term><description>the agent asked the host a question.</description></item>
/// <item><term>questionAnswered</term><description>the host's answer was applied.</description></item>
/// <item><term>usage</term><description>token usage was checkpointed.</description></item>
/// <item><term>compacted</term><description>the conversation was summarised.</description></item>
/// <item><term>turnCompleted</term><description>the turn settled normally.</description></item>
/// <item><term>turnAborted</term><description>the turn stopped with its stop cause (abort or host shutdown).</description></item>
/// <item><term>turnFailed</term><description>the turn failed inside the loop.</description></item>
/// <item><term>sessionClosed</term><description>the session closed.</description></item>
/// </list>
/// Every subtype is named <c>&lt;Kind&gt;Event</c> so no kind name collides
/// with an existing Legate type (for example <see cref="T:Legate.TurnFailed" />
/// is the turn outcome, <see cref="T:Legate.TurnFailedEvent" /> the event).
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<TurnStartedEvent>, "turnStarted")>]
[<JsonDerivedType(typeof<TextDeltaEvent>, "textDelta")>]
[<JsonDerivedType(typeof<ReasoningDeltaEvent>, "reasoningDelta")>]
[<JsonDerivedType(typeof<ToolCallStartedEvent>, "toolCallStarted")>]
[<JsonDerivedType(typeof<ToolCallOutputEvent>, "toolCallOutput")>]
[<JsonDerivedType(typeof<ToolCallCompletedEvent>, "toolCallCompleted")>]
[<JsonDerivedType(typeof<PermissionRequestedEvent>, "permissionRequested")>]
[<JsonDerivedType(typeof<PermissionResolvedEvent>, "permissionResolved")>]
[<JsonDerivedType(typeof<QuestionAskedEvent>, "questionAsked")>]
[<JsonDerivedType(typeof<QuestionAnsweredEvent>, "questionAnswered")>]
[<JsonDerivedType(typeof<UsageEvent>, "usage")>]
[<JsonDerivedType(typeof<CompactedEvent>, "compacted")>]
[<JsonDerivedType(typeof<TurnCompletedEvent>, "turnCompleted")>]
[<JsonDerivedType(typeof<TurnAbortedEvent>, "turnAborted")>]
[<JsonDerivedType(typeof<TurnFailedEvent>, "turnFailed")>]
[<JsonDerivedType(typeof<SessionClosedEvent>, "sessionClosed")>]
type SessionEvent(sessionId: SessionId, turnId: TurnId, sequence: Nullable<int64>, timestamp: DateTimeOffset) =

    /// The session the event belongs to. Sequence numbers are per-session,
    /// so this id scopes the ordering.
    member _.SessionId = sessionId

    /// The turn the event was raised inside.
    member _.TurnId = turnId

    /// The event's per-session, monotonic sequence number, or empty
    /// (HasValue is false) while the event is in flight: the store assigns
    /// the sequence when it appends the event to the journal, so
    /// <c>Subscribe(fromSequence)</c> can resume from a stamped event.
    member _.Sequence = sequence

    /// When the event was raised.
    member _.Timestamp = timestamp

/// A turn began running in the session.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn that began.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
and [<Sealed>] TurnStartedEvent
    (sessionId: SessionId, turnId: TurnId, sequence: Nullable<int64>, timestamp: DateTimeOffset) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

/// Assistant text arrived. Carries the delta, not the accumulated text, so
/// hosts render progressively without re-reading the transcript.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn producing the text.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="text">The text fragment that arrived.</param>
and [<Sealed>] TextDeltaEvent
    (sessionId: SessionId, turnId: TurnId, sequence: Nullable<int64>, timestamp: DateTimeOffset, text: string) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The text fragment that arrived.
    member _.Text = text

/// Assistant reasoning arrived, for models that surface it. Carries the
/// delta, not the accumulated reasoning.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn producing the reasoning.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="text">The reasoning fragment that arrived.</param>
and [<Sealed>] ReasoningDeltaEvent
    (sessionId: SessionId, turnId: TurnId, sequence: Nullable<int64>, timestamp: DateTimeOffset, text: string) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The reasoning fragment that arrived.
    member _.Text = text

/// A tool call began. Every other event tied to the call carries the same
/// id on its ToolCallId: the call's own output and completion events, the
/// permission request it raised, and every event raised inside a sub-agent
/// turn the call spawned, so hosts can nest them under this call.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn calling the tool.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="toolCallId">The id of the tool call that began.</param>
/// <param name="toolName">The name of the tool being called.</param>
and [<Sealed>] ToolCallStartedEvent
    (
        sessionId: SessionId,
        turnId: TurnId,
        sequence: Nullable<int64>,
        timestamp: DateTimeOffset,
        toolCallId: string,
        toolName: string
    ) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The id of the tool call that began.
    member _.ToolCallId = toolCallId

    /// The name of the tool being called.
    member _.ToolName = toolName

/// Tool output arrived. Output streams through deltas exactly like
/// assistant text; the arguments never appear here (they are journaled with
/// the call, not streamed).
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn whose tool call produced the output.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="toolCallId">The id of the tool call producing the output.</param>
/// <param name="output">The output fragment that arrived. Already redacted and bounded by the journal writer.</param>
and [<Sealed>] ToolCallOutputEvent
    (
        sessionId: SessionId,
        turnId: TurnId,
        sequence: Nullable<int64>,
        timestamp: DateTimeOffset,
        toolCallId: string,
        output: string
    ) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The id of the tool call producing the output.
    member _.ToolCallId = toolCallId

    /// The output fragment that arrived. Already redacted and bounded by
    /// the journal writer.
    member _.Output = output

/// A tool call settled. Tool errors surface here as a non-null reason;
/// they do not fail the turn (the model sees the error and continues).
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn whose tool call settled.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="toolCallId">The id of the tool call that settled.</param>
/// <param name="error">Why the call failed, or null when it succeeded. Never contains secrets or tool arguments.</param>
and [<Sealed>] ToolCallCompletedEvent
    (
        sessionId: SessionId,
        turnId: TurnId,
        sequence: Nullable<int64>,
        timestamp: DateTimeOffset,
        toolCallId: string,
        error: string | null
    ) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The id of the tool call that settled.
    member _.ToolCallId = toolCallId

    /// Why the call failed, or null when it succeeded. Never contains
    /// secrets or tool arguments.
    member _.Error: string | null = error

/// A tool call needs a decision the configured policy could not make alone:
/// the turn suspends until the host answers. Mirrors
/// <see cref="T:Legate.PermissionDecision" />.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn whose tool call is waiting.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="requestId">The id the host answers a PermissionDecision with.</param>
/// <param name="toolName">The name of the tool awaiting permission.</param>
and [<Sealed>] PermissionRequestedEvent
    (
        sessionId: SessionId,
        turnId: TurnId,
        sequence: Nullable<int64>,
        timestamp: DateTimeOffset,
        requestId: string,
        toolName: string
    ) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The id the host answers a
    /// <see cref="T:Legate.PermissionDecision" /> with.
    member _.RequestId = requestId

    /// The name of the tool awaiting permission.
    member _.ToolName = toolName

/// A decision was applied to a permission request; the turn resumes (or the
/// call is skipped on Deny).
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn whose suspended call resumes.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="requestId">The id of the request that was resolved.</param>
/// <param name="decision">What the host decided.</param>
and [<Sealed>] PermissionResolvedEvent
    (
        sessionId: SessionId,
        turnId: TurnId,
        sequence: Nullable<int64>,
        timestamp: DateTimeOffset,
        requestId: string,
        decision: PermissionDecisionKind
    ) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The id of the request that was resolved.
    member _.RequestId = requestId

    /// What the host decided.
    member _.Decision = decision

/// The agent asked the host a question: the turn suspends until the host
/// answers. Mirrors <see cref="T:Legate.QuestionAnswer" />.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn that is waiting.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="questionId">The id the host answers a QuestionAnswer with.</param>
/// <param name="question">The question the agent asked.</param>
and [<Sealed>] QuestionAskedEvent
    (
        sessionId: SessionId,
        turnId: TurnId,
        sequence: Nullable<int64>,
        timestamp: DateTimeOffset,
        questionId: string,
        question: string
    ) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The id the host answers a <see cref="T:Legate.QuestionAnswer" /> with.
    member _.QuestionId = questionId

    /// The question the agent asked.
    member _.Question = question

/// The host's answer was applied to a question; the turn resumes.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn that resumes.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="questionId">The id of the question that was answered.</param>
/// <param name="answer">The host's answer, verbatim.</param>
and [<Sealed>] QuestionAnsweredEvent
    (
        sessionId: SessionId,
        turnId: TurnId,
        sequence: Nullable<int64>,
        timestamp: DateTimeOffset,
        questionId: string,
        answer: string
    ) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The id of the question that was answered.
    member _.QuestionId = questionId

    /// The host's answer, verbatim.
    member _.Answer = answer

/// Token usage was checkpointed for the turn so far. Emitted at iteration
/// boundaries, so a host can budget on it without waiting for the turn to
/// settle. Usage never carries cost: pricing is a host concern.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn the usage was checkpointed for.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="inputTokens">The input tokens the turn had consumed.</param>
/// <param name="outputTokens">The output tokens the turn had produced.</param>
and [<Sealed>] UsageEvent
    (
        sessionId: SessionId,
        turnId: TurnId,
        sequence: Nullable<int64>,
        timestamp: DateTimeOffset,
        inputTokens: int64,
        outputTokens: int64
    ) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The input tokens the turn had consumed.
    member _.InputTokens = inputTokens

    /// The output tokens the turn had produced.
    member _.OutputTokens = outputTokens

/// The conversation was summarised by compaction: the summary replaced
/// everything between the system message and the last kept messages. The
/// estimates bound the size the compaction changed, not exact token counts.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn that performed the compaction.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="beforeEstimate">The estimated token count of the conversation before compaction.</param>
/// <param name="afterEstimate">The estimated token count after compaction, summary included.</param>
and [<Sealed>] CompactedEvent
    (
        sessionId: SessionId,
        turnId: TurnId,
        sequence: Nullable<int64>,
        timestamp: DateTimeOffset,
        beforeEstimate: int64,
        afterEstimate: int64
    ) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// The estimated token count of the conversation before compaction.
    member _.BeforeEstimate = beforeEstimate

    /// The estimated token count after compaction, summary included.
    member _.AfterEstimate = afterEstimate

/// The turn settled normally: the model stopped calling tools. Mirrors the
/// terminal status on <see cref="T:Legate.Turn" />; the structured outcome,
/// when the session ran with the structured outcome mode, travels on the
/// turn result, not here.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn that completed.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
and [<Sealed>] TurnCompletedEvent
    (sessionId: SessionId, turnId: TurnId, sequence: Nullable<int64>, timestamp: DateTimeOffset) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

/// The turn stopped before completing: the host aborted it or shut down
/// while it ran. Carries the typed stop cause that won the arbitration,
/// mirroring <see cref="T:Legate.TurnAborted" />.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn that was stopped.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="cause">Which abort-family stop cause won: ExplicitAbort or HostShutdown.</param>
/// <param name="reason">Why the turn stopped. Never contains secrets or tool arguments.</param>
and [<Sealed>] TurnAbortedEvent
    (
        sessionId: SessionId,
        turnId: TurnId,
        sequence: Nullable<int64>,
        timestamp: DateTimeOffset,
        cause: StopCause,
        reason: string
    ) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// Which abort-family stop cause won the arbitration for the turn.
    member _.Cause = cause

    /// Why the turn stopped. Never contains secrets or tool arguments.
    member _.Reason = reason

/// The turn failed inside the loop (a tool or provider error) or its budget
/// ran out. Carries the same never-secrets reason
/// <see cref="T:Legate.TurnResult" /> reports.
/// <param name="sessionId">The session the event belongs to.</param>
/// <param name="turnId">The turn that failed.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
/// <param name="reason">Why the turn failed. Never contains secrets or tool arguments.</param>
and [<Sealed>] TurnFailedEvent
    (sessionId: SessionId, turnId: TurnId, sequence: Nullable<int64>, timestamp: DateTimeOffset, reason: string) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)

    /// Why the turn failed. Never contains secrets or tool arguments.
    member _.Reason = reason

/// The session closed: terminal for the session and its stream. The event
/// still carries the closing turn's id when a turn was in flight.
/// <param name="sessionId">The session that closed.</param>
/// <param name="turnId">The turn in flight when the session closed, or the last turn; hosts that need the distinction read the session.</param>
/// <param name="sequence">The event's per-session sequence number, or empty while in flight.</param>
/// <param name="timestamp">When the event was raised.</param>
and [<Sealed>] SessionClosedEvent
    (sessionId: SessionId, turnId: TurnId, sequence: Nullable<int64>, timestamp: DateTimeOffset) =
    inherit SessionEvent(sessionId, turnId, sequence, timestamp)
