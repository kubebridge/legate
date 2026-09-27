// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open Microsoft.Extensions.AI

// Token-less wire DTOs for the versioned envelope (issue 130): one class
// per wire case. DTOs are classes with a public parameterless constructor
// and public read/write properties so System.Text.Json round-trips them
// while the types stay internal: records nested in an internal module get
// assembly-only constructors, which STJ refuses. CancellationToken is never
// serialised and is re-attached by the receiver from its own scope; live
// Exception values are mapped to reason strings on the wire and the
// receiver rebuilds a generic fault carrying the reason, preserving the
// fault/consume-and-drain path without a live stack (both fault handlers
// ignore the payload). Suspension resume delegates (Nested) never cross
// either: they are parent-local continuations, so the wire form carries no
// nested cursor and the receiver rebuilds with Nested None. The actor
// boundary translates actor message <-> DTO at send/receive (see
// WireSerializer), so SessionActor logic stays shared.
//
// Every DTO is System.Text.Json-shaped on purpose: BCL primitives, arrays,
// Nullable, and domain types that already carry STJ contracts
// (identifiers, InboxPayload, TurnOutcome, AIContent). No F# option, list,
// or union crosses the wire.
module internal WireDtos =

    // ────────────────── Actor family: base session protocol ──────────────────

    /// Wire form of SessionActorMessage.QueuePrompt: the payload only. The
    /// caller's token never crosses; the receiver re-attaches its own scope.
    type QueuePromptDto() =

        /// What the entry carries: a user message.
        member val Payload: InboxPayload = Unchecked.defaultof<InboxPayload> with get, set

    /// Wire form of SessionActorMessage.InjectPrompt: the payload only.
    type InjectPromptDto() =

        /// What the entry carries: a user message.
        member val Payload: InboxPayload = Unchecked.defaultof<InboxPayload> with get, set

    /// Wire form of SessionActorMessage.InterruptPrompt: the payload only.
    type InterruptPromptDto() =

        /// What the entry carries: a user message.
        member val Payload: InboxPayload = Unchecked.defaultof<InboxPayload> with get, set

    /// Wire form of SessionActorMessage.CloseSession: no payload. The
    /// caller's token never crosses; the receiver re-attaches its own scope.
    type CloseSessionDto() =

        /// Marker so the empty case has a stable JSON shape.
        member val Placeholder: bool = false with get, set

    /// Wire form of SessionActorMessage.AbortSession: the typed stop cause
    /// and reason. The caller's token never crosses.
    type AbortSessionDto() =

        /// Which abort-family stop cause won.
        member val Cause: StopCause = StopCause.ExplicitAbort with get, set

        /// Why the turn stopped. Never contains secrets or tool arguments.
        member val Reason: string = Unchecked.defaultof<string> with get, set

    /// Wire form of SessionActorMessage.CompactSession: no payload.
    type CompactSessionDto() =

        /// Marker so the empty case has a stable JSON shape.
        member val Placeholder: bool = false with get, set

    /// Wire form of SessionActorMessage.GetSnapshot: no payload.
    type GetSnapshotDto() =

        /// Marker so the empty case has a stable JSON shape.
        member val Placeholder: bool = false with get, set

    /// Wire form of SessionActorMessage.SessionTurnSettled.
    type TurnSettledDto() =

        /// The entry the finished attempt executed.
        member val Entry: InboxEntry = Unchecked.defaultof<InboxEntry> with get, set

        /// The settled turn result.
        member val Result: TurnResult = Unchecked.defaultof<TurnResult> with get, set

    /// Wire form of SessionActorMessage.SessionTurnFaulted: the entry plus
    /// the fault reason string. The live exception never crosses.
    type TurnFaultedDto() =

        /// The entry the faulted attempt executed.
        member val Entry: InboxEntry = Unchecked.defaultof<InboxEntry> with get, set

        /// Why the turn faulted: the exception message, or the exception
        /// type name when the message was empty.
        member val Reason: string = Unchecked.defaultof<string> with get, set

    /// Wire form of SessionPromptReply.PromptAccepted.
    type PromptAcceptedDto() =

        /// The appended inbox entry.
        member val Entry: InboxEntry = Unchecked.defaultof<InboxEntry> with get, set

    /// Wire form of SessionPromptReply.PromptRejected.
    type PromptRejectedDto() =

        /// The state the session was in.
        member val State: SessionState = SessionState.Idle with get, set

    /// Wire form of SessionCompactReply.CompactCompleted.
    type CompactCompletedDto() =

        /// The before estimate from the CompactionOutcome.
        member val BeforeEstimate: int64 = 0L with get, set

        /// The after estimate from the CompactionOutcome.
        member val AfterEstimate: int64 = 0L with get, set

    /// Wire form of SessionCompactReply.CompactNotNeeded: no payload.
    type CompactNotNeededDto() =

        /// Marker so the empty case has a stable JSON shape.
        member val Placeholder: bool = false with get, set

    /// Wire form of SessionCompactReply.CompactDeferred: no payload.
    type CompactDeferredDto() =

        /// Marker so the empty case has a stable JSON shape.
        member val Placeholder: bool = false with get, set

    /// Wire form of SessionCompactReply.CompactFenced: no payload.
    type CompactFencedDto() =

        /// Marker so the empty case has a stable JSON shape.
        member val Placeholder: bool = false with get, set

    /// Wire form of SessionCompactReply.CompactRejected.
    type CompactRejectedDto() =

        /// The state the session was in.
        member val State: SessionState = SessionState.Idle with get, set

    /// Wire form of SessionSnapshot: the observable snapshot with the F#
    /// option projected onto Nullable, so plain System.Text.Json suffices.
    type SnapshotDto() =

        /// The session the snapshot was taken for.
        member val SessionId: SessionId = Unchecked.defaultof<SessionId> with get, set

        /// The actor's current lifecycle state.
        member val State: SessionState = SessionState.Idle with get, set

        /// How many inbox entries the store reports pending.
        member val PendingCount: int = 0 with get, set

        /// The position of the entry the running turn executes, or empty
        /// when no turn is in flight.
        member val RunningPosition: Nullable<int64> = Nullable<int64>() with get, set

        /// The pending suspend request id while WaitingForInput, or null
        /// when no turn is suspended.
        member val PendingRequestId: string | null = null with get, set

    /// Wire form of the stored-session answer to CloseSession.
    type SessionClosedDto() =

        /// The stored session after the close.
        member val Session: Session = Unchecked.defaultof<Session> with get, set

    // ────────────────── Router family ──────────────────

    /// Wire form of SessionRouterMessage.ResolveSession.
    type ResolveSessionDto() =

        /// The session id to resolve.
        member val SessionId: string = Unchecked.defaultof<string> with get, set

    // ────────────────── Entity family: suspendable protocol ──────────────────

    /// Wire form of SuspendableActorMessage.SuspendableQueuePrompt.
    type SuspendableQueuePromptDto() =

        /// What the entry carries: a user message.
        member val Payload: InboxPayload = Unchecked.defaultof<InboxPayload> with get, set

    /// Wire form of SuspendableActorMessage.SuspendableInjectPrompt.
    type SuspendableInjectPromptDto() =

        /// What the entry carries: a user message.
        member val Payload: InboxPayload = Unchecked.defaultof<InboxPayload> with get, set

    /// Wire form of SuspendableActorMessage.SuspendableInterruptPrompt.
    type SuspendableInterruptPromptDto() =

        /// What the entry carries: a user message.
        member val Payload: InboxPayload = Unchecked.defaultof<InboxPayload> with get, set

    /// Wire form of one suspended cursor: every field crosses except the
    /// nested resume, which is parent-local and rebuilds as None.
    type SuspensionDto() =

        /// The stable id the host answers.
        member val RequestId: string = Unchecked.defaultof<string> with get, set

        /// The tool whose call raised the request.
        member val ToolName: string = Unchecked.defaultof<string> with get, set

        /// The tool-call id that raised the request.
        member val ToolCallId: string = Unchecked.defaultof<string> with get, set

        /// Which reply resumes the turn: permission or question.
        member val Kind: string = Unchecked.defaultof<string> with get, set

        /// The question the agent asked, or empty for permissions.
        member val QuestionText: string = Unchecked.defaultof<string> with get, set

        /// The options hint the ask_user call carried.
        member val QuestionOptions: string[] = [||] with get, set

        /// The history up to the suspend point, copied at suspend time.
        member val History: ChatMessage[] = [||] with get, set

        /// Input tokens spent up to the suspend point.
        member val InputTokens: int64 = 0L with get, set

        /// Output tokens spent up to the suspend point.
        member val OutputTokens: int64 = 0L with get, set

        /// Model iterations spent up to the suspend point.
        member val Iterations: int = 0 with get, set

        /// The tool call that raised the request.
        member val PendingCall: FunctionCallContent = Unchecked.defaultof<FunctionCallContent> with get, set

    /// Wire form of SuspendableActorMessage.SuspendableFinished: the entry,
    /// the completion with its suspension in wire form (null when the turn
    /// settled), the attempt, and the granted tool names as an array.
    type SuspendableFinishedDto() =

        /// The inbox entry the parked turn executed.
        member val Entry: InboxEntry = Unchecked.defaultof<InboxEntry> with get, set

        /// The settled turn result.
        member val Result: TurnResult = Unchecked.defaultof<TurnResult> with get, set

        /// True when Inject entries stayed pending past a would-complete
        /// turn and the actor must start a new turn.
        member val HasPendingInjects: bool = false with get, set

        /// The suspend cursor in wire form; null when the turn settled.
        /// Plain STJ maps JSON null back to null, which ofWire reads as no
        /// suspension.
        member val Suspension: SuspensionDto = Unchecked.defaultof<SuspensionDto> with get, set

        /// The 1-based attempt the parked run used.
        member val Attempt: int = 0 with get, set

        /// Tool names the host already allowed for the session.
        member val Allowed: string[] = [||] with get, set

    /// Wire form of SuspendableActorMessage.SuspendableFaulted: the entry,
    /// the fault reason string, and the attempt. The live exception never
    /// crosses.
    type SuspendableFaultedDto() =

        /// The inbox entry the faulted turn executed.
        member val Entry: InboxEntry = Unchecked.defaultof<InboxEntry> with get, set

        /// Why the turn faulted: the exception message, or the exception
        /// type name when the message was empty.
        member val Reason: string = Unchecked.defaultof<string> with get, set

        /// The 1-based attempt the faulted run used.
        member val Attempt: int = 0 with get, set

    /// Wire form of SuspendableActorMessage.ReplyEntry.
    type ReplyEntryDto() =

        /// A Reply inbox entry arrived for the suspended turn.
        member val Entry: InboxEntry = Unchecked.defaultof<InboxEntry> with get, set

    /// Wire form of SuspendableActorMessage.SuspendableGetSnapshot.
    type SuspendableGetSnapshotDto() =

        /// Marker so the empty case has a stable JSON shape.
        member val Placeholder: bool = false with get, set

    /// Wire form of SuspendableActorMessage.SuspendTimedOut.
    type SuspendTimedOutDto() =

        /// The pending request id whose deadline fired.
        member val RequestId: string = Unchecked.defaultof<string> with get, set

    /// Wire form of SuspendableActorMessage.SuspendableCloseSession.
    type SuspendableCloseSessionDto() =

        /// Marker so the empty case has a stable JSON shape.
        member val Placeholder: bool = false with get, set

    /// Wire form of SuspendableActorMessage.SuspendableAbortSession.
    type SuspendableAbortSessionDto() =

        /// Which abort-family stop cause won.
        member val Cause: StopCause = StopCause.ExplicitAbort with get, set

        /// Why the turn stopped. Never contains secrets or tool arguments.
        member val Reason: string = Unchecked.defaultof<string> with get, set

    /// Wire form of SuspendableActorMessage.SuspendableCompactSession.
    type SuspendableCompactSessionDto() =

        /// Marker so the empty case has a stable JSON shape.
        member val Placeholder: bool = false with get, set

    /// Wire form of SuspendableActorMessage.SuspendableCheckInbox.
    type SuspendableCheckInboxDto() =

        /// Marker so the empty case has a stable JSON shape.
        member val Placeholder: bool = false with get, set

    /// Wire form of SuspendableActorMessage.SuspendableSetAgent.
    type SuspendableSetAgentDto() =

        /// The agent the session converses with from now on.
        member val AgentId: AgentId = Unchecked.defaultof<AgentId> with get, set

    /// Wire form of SessionReplyReply.ReplyAccepted.
    type ReplyAcceptedDto() =

        /// The appended Reply inbox entry.
        member val Entry: InboxEntry = Unchecked.defaultof<InboxEntry> with get, set

    /// Wire form of SessionReplyReply.ReplyRejected: the typed error
    /// projected onto strings. The live exception never crosses.
    type ReplyRejectedDto() =

        /// The id of the session the reply targeted.
        member val SessionId: string = Unchecked.defaultof<string> with get, set

        /// The request or question id the reply carried.
        member val RequestId: string = Unchecked.defaultof<string> with get, set

        /// The rejection message.
        member val Message: string = Unchecked.defaultof<string> with get, set

    /// Wire form of SessionSetAgentReply.SetAgentApplied.
    type SetAgentAppliedDto() =

        /// The stored session after the rebind.
        member val Session: Session = Unchecked.defaultof<Session> with get, set

    /// Wire form of SessionSetAgentReply.SetAgentPending.
    type SetAgentPendingDto() =

        /// The stored session as it stands, still conversing with the
        /// previous agent.
        member val Session: Session = Unchecked.defaultof<Session> with get, set

    /// Wire form of SessionSetAgentReply.SetAgentRejected.
    type SetAgentRejectedDto() =

        /// The state the session was in.
        member val State: SessionState = SessionState.Idle with get, set

    // ────────────────── Subscription family: cross-node subscribe (issue 133) ──────────────────

    /// Wire form of CrossNodeSubscriptions.CrossNodeSubscribeRequest: the
    /// tenant, session, and exclusive cursor routed to the owning entity
    /// through the session shard region. The caller resumes from its last
    /// sequence on rebind; duplicates are acceptable, gaps are not.
    type SubscribeDto() =

        /// The tenant the session belongs to.
        member val Tenant: string = Unchecked.defaultof<string> with get, set

        /// The session to subscribe to.
        member val SessionId: string = Unchecked.defaultof<string> with get, set

        /// The exclusive cursor: events strictly greater than it stream back.
        member val FromSequence: int64 = 0L with get, set

        /// The subscriber token identifying this stream across re-polls
        /// and rebinds.
        member val SubscriberToken: string = Unchecked.defaultof<string> with get, set

    /// Wire form of CrossNodeSubscriptions.CrossNodeUnsubscribe: the
    /// tenant and session to detach from. Best-effort on dispose; a lost
    /// unsubscribe only holds one subscriber slot until the entity
    /// restarts.
    type UnsubscribeDto() =

        /// The tenant the session belongs to.
        member val Tenant: string = Unchecked.defaultof<string> with get, set

        /// The session to detach from.
        member val SessionId: string = Unchecked.defaultof<string> with get, set

        /// The subscriber token to detach.
        member val SubscriberToken: string = Unchecked.defaultof<string> with get, set

    /// Wire form of CrossNodeSubscriptions.CrossNodeEventBatch: one
    /// entity-to-subscriber batch in sequence order with its resume
    /// cursor. Bounded batches keep one Ask reply bounded; the consumer
    /// follows NextCursor while EndOfStream is false.
    type EventBatchDto() =

        /// The session the events belong to.
        member val SessionId: string = Unchecked.defaultof<string> with get, set

        /// The events in sequence order; empty at end of stream.
        member val Events: SessionEvent[] = [||] with get, set

        /// The cursor to resume from.
        member val NextCursor: int64 = 0L with get, set

        /// True when the journal holds nothing more past the cursor now.
        member val EndOfStream: bool = true with get, set

    // ────────────────── Event family: session event stream (issue 133) ──────────────────

    /// Wire form of one journaled SessionEvent streamed from the owning
    /// entity to the subscribing node. The polymorphic SessionEvent
    /// payload round-trips through its $type discriminator; the receiver
    /// rebuilds the live event verbatim (sequences are store-stamped, so
    /// no token or fence crosses).
    type SessionEventDto() =

        /// The journaled event.
        member val Event: SessionEvent = Unchecked.defaultof<SessionEvent> with get, set

    // ────────────────── Translation ──────────────────

    /// Maps a live exception to its wire reason string: the message, or the
    /// type name when the message was empty (the tool-call Error precedent:
    /// message only, never secrets or arguments).
    /// <param name="error">The live error. Must not be null.</param>
    /// <returns>The reason string, never blank.</returns>
    let private reasonOf (error: Exception) : string =
        if isNull (box error) then
            raise (ArgumentNullException(nameof error))

        if String.IsNullOrEmpty error.Message then
            error.GetType().Name
        else
            error.Message

    /// Rebuilds a fault exception from its wire reason: a generic fault
    /// carrying the reason. Both fault handlers ignore the payload, so the
    /// fault/consume-and-drain path is preserved without a live stack.
    /// <param name="reason">The wire reason. Must not be null.</param>
    /// <returns>A generic exception carrying the reason.</returns>
    let private faultOf (reason: string) : Exception =
        if isNull (box reason) then
            raise (ArgumentNullException(nameof reason))

        Exception(reason)

    /// Maps a suspension kind to its wire name.
    /// <param name="kind">The suspension kind.</param>
    /// <returns>permission or question.</returns>
    let private kindToWire (kind: TurnLoop.SuspensionKind) : string =
        match kind with
        | TurnLoop.SuspensionKind.PermissionSuspension -> "permission"
        | TurnLoop.SuspensionKind.QuestionSuspension -> "question"

    /// Maps a wire kind name back onto its suspension kind.
    /// <param name="kind">The wire name.</param>
    /// <returns>The suspension kind.</returns>
    let private kindOfWire (kind: string) : TurnLoop.SuspensionKind =
        if String.Equals(kind, "permission", StringComparison.Ordinal) then
            TurnLoop.SuspensionKind.PermissionSuspension
        elif String.Equals(kind, "question", StringComparison.Ordinal) then
            TurnLoop.SuspensionKind.QuestionSuspension
        else
            raise (InvalidOperationException($"Unknown suspension kind '{kind}'. Expected permission or question."))

    /// Builds one DTO of the given class and sets it through the setter.
    /// <param name="set">Sets the fresh DTO's fields.</param>
    /// <returns>The built DTO.</returns>
    let private buildDto<'T when 'T: (new: unit -> 'T)> (set: 'T -> unit) : 'T =
        if isNull (box set) then
            raise (ArgumentNullException(nameof set))

        let dto = new 'T()
        set dto
        dto

    /// Maps a live suspend cursor onto its wire form. The nested resume is
    /// parent-local and never crosses: the wire form drops it.
    /// <param name="cursor">The live cursor. Must not be null.</param>
    /// <returns>The wire cursor.</returns>
    let private suspensionToWire (cursor: TurnLoop.TurnLoopSuspension) : SuspensionDto =
        if isNull (box cursor) then
            raise (ArgumentNullException(nameof cursor))

        ArgumentNullException.ThrowIfNull(cursor.PendingCall)

        let history =
            if isNull (box cursor.HistorySnapshot) then
                [||]
            else
                cursor.HistorySnapshot
                |> Seq.filter (fun message -> not (isNull (box message)))
                |> Array.ofSeq

        let options =
            match cursor.QuestionOptions with
            | [] -> [||]
            | values -> values |> List.toArray

        buildDto (fun (wire: SuspensionDto) ->
            wire.RequestId <- cursor.RequestId
            wire.ToolName <- cursor.ToolName
            wire.ToolCallId <- cursor.ToolCallId
            wire.Kind <- kindToWire cursor.Kind
            wire.QuestionText <- cursor.QuestionText
            wire.QuestionOptions <- options
            wire.History <- history
            wire.InputTokens <- cursor.InputTokens
            wire.OutputTokens <- cursor.OutputTokens
            wire.Iterations <- cursor.Iterations
            wire.PendingCall <- cursor.PendingCall)

    /// Rebuilds a live suspend cursor from its wire form. The nested resume
    /// rebuilds as None: nested continuations are parent-local and never
    /// cross a node boundary.
    /// <param name="wire">The wire cursor. Must not be null.</param>
    /// <returns>The live cursor with no nested resume.</returns>
    let private suspensionOfWire (wire: SuspensionDto) : TurnLoop.TurnLoopSuspension =
        if isNull (box wire) then
            raise (ArgumentNullException(nameof wire))

        ArgumentNullException.ThrowIfNull(wire.PendingCall)

        let history =
            if isNull (box wire.History) then
                ResizeArray<ChatMessage>() :> IList<ChatMessage>
            else
                ResizeArray<ChatMessage>(wire.History |> Array.filter (fun message -> not (isNull (box message))))
                :> IList<ChatMessage>

        let options =
            if isNull (box wire.QuestionOptions) then
                []
            else
                wire.QuestionOptions |> Array.toList

        {
            RequestId = wire.RequestId
            ToolName = wire.ToolName
            ToolCallId = wire.ToolCallId
            Kind = kindOfWire wire.Kind
            QuestionText = wire.QuestionText
            QuestionOptions = options
            HistorySnapshot = history
            InputTokens = wire.InputTokens
            OutputTokens = wire.OutputTokens
            Iterations = wire.Iterations
            PendingCall = wire.PendingCall
            Nested = None
        }

    /// Maps a live completion onto its wire result, inject flag, and
    /// suspension form (null when the turn settled).
    /// <param name="completion">The live completion. Must not be null.</param>
    /// <returns>The result, the pending-injects flag, and the wire cursor or null.</returns>
    let private completionToWire (completion: TurnLoop.TurnLoopCompletion) : TurnResult * bool * SuspensionDto =
        if isNull (box completion) then
            raise (ArgumentNullException(nameof completion))

        ArgumentNullException.ThrowIfNull(completion.Result)

        let suspension =
            match completion.Suspension with
            | Some cursor -> suspensionToWire cursor
            | None -> Unchecked.defaultof<SuspensionDto>

        (completion.Result, completion.HasPendingInjects, suspension)

    /// Rebuilds a live completion from its wire parts.
    /// <param name="result">The settled turn result. Must not be null.</param>
    /// <param name="hasPendingInjects">Whether Inject entries stayed pending.</param>
    /// <param name="suspension">The wire cursor, or null when the turn settled.</param>
    /// <returns>The live completion.</returns>
    let private completionOfWire
        (result: TurnResult)
        (hasPendingInjects: bool)
        (suspension: SuspensionDto)
        : TurnLoop.TurnLoopCompletion =
        ArgumentNullException.ThrowIfNull(result)

        let cursor =
            if isNull (box suspension) then
                None
            else
                Some(suspensionOfWire suspension)

        {
            Result = result
            HasPendingInjects = hasPendingInjects
            Suspension = cursor
        }

    /// Maps a live snapshot onto its wire form.
    /// <param name="snapshot">The live snapshot.</param>
    /// <returns>The wire snapshot.</returns>
    let private snapshotToWire (snapshot: SessionSnapshot) : SnapshotDto =
        buildDto (fun (wire: SnapshotDto) ->
            wire.SessionId <- snapshot.SessionId
            wire.State <- snapshot.State
            wire.PendingCount <- snapshot.PendingCount

            wire.RunningPosition <-
                match snapshot.RunningPosition with
                | Some position -> Nullable<int64>(position)
                | None -> Nullable<int64>()

            wire.PendingRequestId <- snapshot.PendingRequestId)

    /// Rebuilds a live snapshot from its wire form.
    /// <param name="wire">The wire snapshot. Must not be null.</param>
    /// <returns>The live snapshot.</returns>
    let private snapshotOfWire (wire: SnapshotDto) : SessionSnapshot =
        if isNull (box wire) then
            raise (ArgumentNullException(nameof wire))

        {
            SessionId = wire.SessionId
            State = wire.State
            PendingCount = wire.PendingCount
            RunningPosition =
                if wire.RunningPosition.HasValue then
                    Some wire.RunningPosition.Value
                else
                    None
            PendingRequestId = wire.PendingRequestId
        }

    /// Requires a non-null payload.
    /// <param name="payload">The payload. Must not be null.</param>
    /// <returns>The payload.</returns>
    let private requirePayload (payload: InboxPayload) : InboxPayload =
        ArgumentNullException.ThrowIfNull(payload)
        payload

    /// Requires a non-null inbox entry.
    /// <param name="entry">The entry. Must not be null.</param>
    /// <returns>The entry.</returns>
    let private requireEntry (entry: InboxEntry) : InboxEntry =
        if isNull (box entry) then
            raise (ArgumentNullException(nameof entry))

        entry

    /// Requires a non-null session.
    /// <param name="session">The session. Must not be null.</param>
    /// <returns>The session.</returns>
    let private requireSession (session: Session) : Session =
        if isNull (box session) then
            raise (ArgumentNullException(nameof session))

        session

    /// Requires a non-null agent id.
    /// <param name="agentId">The agent id.</param>
    /// <returns>The agent id.</returns>
    let private requireAgentId (agentId: AgentId) : AgentId = agentId

    /// Maps one actor-protocol message onto its token-less wire DTO. Every
    /// SessionActorMessage case, every SuspendableActorMessage case, every
    /// reply type, SessionSnapshot, Session, and the router message map;
    /// anything else raises. CancellationToken values never cross: the
    /// receiver re-attaches its own scope in ofWire.
    /// <param name="message">The live message. Must not be null.</param>
    /// <returns>The wire DTO.</returns>
    let toWire (message: obj) : obj =
        if isNull (box message) then
            raise (ArgumentNullException(nameof message))

        if message :? SessionActorMessage then
            match message :?> SessionActorMessage with
            | QueuePrompt(payload, _) ->
                buildDto (fun (dto: QueuePromptDto) -> dto.Payload <- requirePayload payload) :> obj
            | InjectPrompt(payload, _) ->
                buildDto (fun (dto: InjectPromptDto) -> dto.Payload <- requirePayload payload) :> obj
            | InterruptPrompt(payload, _) ->
                buildDto (fun (dto: InterruptPromptDto) -> dto.Payload <- requirePayload payload) :> obj
            | CloseSession _ -> CloseSessionDto() :> obj
            | AbortSession(cause, reason, _) ->
                buildDto (fun (dto: AbortSessionDto) ->
                    dto.Cause <- cause
                    dto.Reason <- reason)
                :> obj
            | CompactSession _ -> CompactSessionDto() :> obj
            | GetSnapshot -> GetSnapshotDto() :> obj
            | SessionTurnSettled(entry, result) ->
                ArgumentNullException.ThrowIfNull(result)

                buildDto (fun (dto: TurnSettledDto) ->
                    dto.Entry <- requireEntry entry
                    dto.Result <- result)
                :> obj
            | SessionTurnFaulted(entry, error) ->
                buildDto (fun (dto: TurnFaultedDto) ->
                    dto.Entry <- requireEntry entry
                    dto.Reason <- reasonOf error)
                :> obj
        elif message :? SessionActor.SuspendableActorMessage then
            match message :?> SessionActor.SuspendableActorMessage with
            | SessionActor.SuspendableQueuePrompt(payload, _) ->
                buildDto (fun (dto: SuspendableQueuePromptDto) -> dto.Payload <- requirePayload payload) :> obj
            | SessionActor.SuspendableInjectPrompt(payload, _) ->
                buildDto (fun (dto: SuspendableInjectPromptDto) -> dto.Payload <- requirePayload payload) :> obj
            | SessionActor.SuspendableInterruptPrompt(payload, _) ->
                buildDto (fun (dto: SuspendableInterruptPromptDto) -> dto.Payload <- requirePayload payload) :> obj
            | SessionActor.SuspendableFinished(entry, completion, attempt, allowed) ->
                let result, hasPendingInjects, suspension = completionToWire completion

                buildDto (fun (dto: SuspendableFinishedDto) ->
                    dto.Entry <- requireEntry entry
                    dto.Result <- result
                    dto.HasPendingInjects <- hasPendingInjects
                    dto.Suspension <- suspension
                    dto.Attempt <- attempt

                    dto.Allowed <-
                        if isNull (box allowed) then
                            [||]
                        else
                            allowed |> Seq.filter (fun name -> not (isNull (box name))) |> Array.ofSeq)
                :> obj
            | SessionActor.SuspendableFaulted(entry, error, attempt) ->
                buildDto (fun (dto: SuspendableFaultedDto) ->
                    dto.Entry <- requireEntry entry
                    dto.Reason <- reasonOf error
                    dto.Attempt <- attempt)
                :> obj
            | SessionActor.ReplyEntry entry ->
                buildDto (fun (dto: ReplyEntryDto) -> dto.Entry <- requireEntry entry) :> obj
            | SessionActor.SuspendableGetSnapshot -> SuspendableGetSnapshotDto() :> obj
            | SessionActor.SuspendTimedOut requestId ->
                buildDto (fun (dto: SuspendTimedOutDto) -> dto.RequestId <- requestId) :> obj
            | SessionActor.SuspendableCloseSession _ -> SuspendableCloseSessionDto() :> obj
            | SessionActor.SuspendableAbortSession(cause, reason, _) ->
                buildDto (fun (dto: SuspendableAbortSessionDto) ->
                    dto.Cause <- cause
                    dto.Reason <- reason)
                :> obj
            | SessionActor.SuspendableCompactSession _ -> SuspendableCompactSessionDto() :> obj
            | SessionActor.SuspendableCheckInbox -> SuspendableCheckInboxDto() :> obj
            | SessionActor.SuspendableSetAgent(agentId, _) ->
                buildDto (fun (dto: SuspendableSetAgentDto) -> dto.AgentId <- requireAgentId agentId) :> obj
        elif message :? SessionPromptReply then
            match message :?> SessionPromptReply with
            | PromptAccepted entry -> buildDto (fun (dto: PromptAcceptedDto) -> dto.Entry <- requireEntry entry) :> obj
            | PromptRejected state -> buildDto (fun (dto: PromptRejectedDto) -> dto.State <- state) :> obj
        elif message :? SessionCompactReply then
            match message :?> SessionCompactReply with
            | CompactCompleted(beforeEstimate, afterEstimate) ->
                buildDto (fun (dto: CompactCompletedDto) ->
                    dto.BeforeEstimate <- beforeEstimate
                    dto.AfterEstimate <- afterEstimate)
                :> obj
            | CompactNotNeeded -> CompactNotNeededDto() :> obj
            | CompactDeferred -> CompactDeferredDto() :> obj
            | CompactFenced -> CompactFencedDto() :> obj
            | CompactRejected state -> buildDto (fun (dto: CompactRejectedDto) -> dto.State <- state) :> obj
        elif message :? SessionActor.SessionReplyReply then
            match message :?> SessionActor.SessionReplyReply with
            | SessionActor.ReplyAccepted entry ->
                buildDto (fun (dto: ReplyAcceptedDto) -> dto.Entry <- requireEntry entry) :> obj
            | SessionActor.ReplyRejected error ->
                if isNull (box error) then
                    raise (ArgumentNullException("error"))

                buildDto (fun (dto: ReplyRejectedDto) ->
                    dto.SessionId <- error.SessionId.ToString()
                    dto.RequestId <- error.RequestId
                    dto.Message <- error.Message)
                :> obj
        elif message :? SessionActor.SessionSetAgentReply then
            match message :?> SessionActor.SessionSetAgentReply with
            | SessionActor.SetAgentApplied session ->
                buildDto (fun (dto: SetAgentAppliedDto) -> dto.Session <- requireSession session) :> obj
            | SessionActor.SetAgentPending session ->
                buildDto (fun (dto: SetAgentPendingDto) -> dto.Session <- requireSession session) :> obj
            | SessionActor.SetAgentRejected state ->
                buildDto (fun (dto: SetAgentRejectedDto) -> dto.State <- state) :> obj
        elif message :? SessionSnapshot then
            snapshotToWire (message :?> SessionSnapshot) :> obj
        elif message :? Session then
            buildDto (fun (dto: SessionClosedDto) -> dto.Session <- requireSession (message :?> Session)) :> obj
        elif message :? SessionRouterMessage then
            match message :?> SessionRouterMessage with
            | ResolveSession sessionId -> buildDto (fun (dto: ResolveSessionDto) -> dto.SessionId <- sessionId) :> obj
        elif message :? CrossNodeSubscriptions.CrossNodeSubscribeRequest then
            let request = message :?> CrossNodeSubscriptions.CrossNodeSubscribeRequest

            buildDto (fun (dto: SubscribeDto) ->
                dto.Tenant <- request.Tenant.ToString()
                dto.SessionId <- request.SessionId.ToString()
                dto.FromSequence <- request.FromSequence
                dto.SubscriberToken <- request.SubscriberToken)
            :> obj
        elif message :? CrossNodeSubscriptions.CrossNodeUnsubscribe then
            let request = message :?> CrossNodeSubscriptions.CrossNodeUnsubscribe

            buildDto (fun (dto: UnsubscribeDto) ->
                dto.Tenant <- request.Tenant.ToString()
                dto.SessionId <- request.SessionId.ToString()
                dto.SubscriberToken <- request.SubscriberToken)
            :> obj
        elif message :? CrossNodeSubscriptions.CrossNodeEventBatch then
            let batch = message :?> CrossNodeSubscriptions.CrossNodeEventBatch

            let events =
                if isNull (box batch.Events) then
                    [||]
                else
                    batch.Events |> Seq.filter (fun evt -> not (isNull (box evt))) |> Array.ofSeq

            buildDto (fun (dto: EventBatchDto) ->
                dto.SessionId <- batch.SessionId.ToString()
                dto.Events <- events
                dto.NextCursor <- batch.NextCursor
                dto.EndOfStream <- batch.EndOfStream)
            :> obj
        elif message :? SessionEvent then
            let evt = message :?> SessionEvent

            if isNull (box evt) then
                raise (ArgumentNullException(nameof message))

            buildDto (fun (dto: SessionEventDto) -> dto.Event <- evt) :> obj
        else
            raise (
                InvalidOperationException(
                    $"The wire envelope maps no DTO for '{message.GetType().FullName}'. Only actor, router, and entity protocol messages cross a node boundary."
                )
            )

    /// Maps one wire DTO back onto its live actor-protocol message.
    /// CancellationToken values re-attach as None from the receiver's own
    /// scope (Ask timeouts and actor-local sources own cancellation
    /// cross-node); fault reasons rebuild as generic exceptions; the typed
    /// reply-mismatch error rebuilds from its carried ids; suspensions
    /// rebuild with no nested resume. Anything else raises.
    /// <param name="wire">The wire DTO. Must not be null.</param>
    /// <returns>The live message.</returns>
    let ofWire (wire: obj) : obj =
        if isNull (box wire) then
            raise (ArgumentNullException(nameof wire))

        if wire :? QueuePromptDto then
            QueuePrompt((wire :?> QueuePromptDto).Payload |> requirePayload, CancellationToken.None) :> obj
        elif wire :? InjectPromptDto then
            InjectPrompt((wire :?> InjectPromptDto).Payload |> requirePayload, CancellationToken.None) :> obj
        elif wire :? InterruptPromptDto then
            InterruptPrompt((wire :?> InterruptPromptDto).Payload |> requirePayload, CancellationToken.None) :> obj
        elif wire :? CloseSessionDto then
            CloseSession(CancellationToken.None) :> obj
        elif wire :? AbortSessionDto then
            let dto = wire :?> AbortSessionDto
            AbortSession(dto.Cause, dto.Reason, CancellationToken.None) :> obj
        elif wire :? CompactSessionDto then
            CompactSession(CancellationToken.None) :> obj
        elif wire :? GetSnapshotDto then
            GetSnapshot :> obj
        elif wire :? TurnSettledDto then
            let dto = wire :?> TurnSettledDto
            ArgumentNullException.ThrowIfNull(dto.Result)
            SessionTurnSettled(requireEntry dto.Entry, dto.Result) :> obj
        elif wire :? TurnFaultedDto then
            let dto = wire :?> TurnFaultedDto
            SessionTurnFaulted(requireEntry dto.Entry, faultOf dto.Reason) :> obj
        elif wire :? PromptAcceptedDto then
            PromptAccepted(requireEntry (wire :?> PromptAcceptedDto).Entry) :> obj
        elif wire :? PromptRejectedDto then
            PromptRejected((wire :?> PromptRejectedDto).State) :> obj
        elif wire :? CompactCompletedDto then
            let dto = wire :?> CompactCompletedDto
            CompactCompleted(dto.BeforeEstimate, dto.AfterEstimate) :> obj
        elif wire :? CompactNotNeededDto then
            CompactNotNeeded :> obj
        elif wire :? CompactDeferredDto then
            CompactDeferred :> obj
        elif wire :? CompactFencedDto then
            CompactFenced :> obj
        elif wire :? CompactRejectedDto then
            CompactRejected((wire :?> CompactRejectedDto).State) :> obj
        elif wire :? SnapshotDto then
            snapshotOfWire (wire :?> SnapshotDto) :> obj
        elif wire :? SessionClosedDto then
            requireSession (wire :?> SessionClosedDto).Session :> obj
        elif wire :? ResolveSessionDto then
            ResolveSession((wire :?> ResolveSessionDto).SessionId) :> obj
        elif wire :? SuspendableQueuePromptDto then
            SessionActor.SuspendableQueuePrompt(
                (wire :?> SuspendableQueuePromptDto).Payload |> requirePayload,
                CancellationToken.None
            )
            :> obj
        elif wire :? SuspendableInjectPromptDto then
            SessionActor.SuspendableInjectPrompt(
                (wire :?> SuspendableInjectPromptDto).Payload |> requirePayload,
                CancellationToken.None
            )
            :> obj
        elif wire :? SuspendableInterruptPromptDto then
            SessionActor.SuspendableInterruptPrompt(
                (wire :?> SuspendableInterruptPromptDto).Payload |> requirePayload,
                CancellationToken.None
            )
            :> obj
        elif wire :? SuspendableFinishedDto then
            let dto = wire :?> SuspendableFinishedDto

            let completion = completionOfWire dto.Result dto.HasPendingInjects dto.Suspension

            let allowed =
                if isNull (box dto.Allowed) then
                    HashSet<string>()
                else
                    HashSet<string>(dto.Allowed |> Array.filter (fun name -> not (isNull (box name))))

            SessionActor.SuspendableFinished(requireEntry dto.Entry, completion, dto.Attempt, allowed) :> obj
        elif wire :? SuspendableFaultedDto then
            let dto = wire :?> SuspendableFaultedDto
            SessionActor.SuspendableFaulted(requireEntry dto.Entry, faultOf dto.Reason, dto.Attempt) :> obj
        elif wire :? ReplyEntryDto then
            SessionActor.ReplyEntry(requireEntry (wire :?> ReplyEntryDto).Entry) :> obj
        elif wire :? SuspendableGetSnapshotDto then
            SessionActor.SuspendableGetSnapshot :> obj
        elif wire :? SuspendTimedOutDto then
            SessionActor.SuspendTimedOut((wire :?> SuspendTimedOutDto).RequestId) :> obj
        elif wire :? SuspendableCloseSessionDto then
            SessionActor.SuspendableCloseSession(CancellationToken.None) :> obj
        elif wire :? SuspendableAbortSessionDto then
            let dto = wire :?> SuspendableAbortSessionDto
            SessionActor.SuspendableAbortSession(dto.Cause, dto.Reason, CancellationToken.None) :> obj
        elif wire :? SuspendableCompactSessionDto then
            SessionActor.SuspendableCompactSession(CancellationToken.None) :> obj
        elif wire :? SuspendableCheckInboxDto then
            SessionActor.SuspendableCheckInbox :> obj
        elif wire :? SuspendableSetAgentDto then
            SessionActor.SuspendableSetAgent((wire :?> SuspendableSetAgentDto).AgentId, CancellationToken.None) :> obj
        elif wire :? ReplyAcceptedDto then
            SessionActor.ReplyAccepted(requireEntry (wire :?> ReplyAcceptedDto).Entry) :> obj
        elif wire :? ReplyRejectedDto then
            let dto = wire :?> ReplyRejectedDto
            let mutable parsed = Unchecked.defaultof<SessionId>

            if not (SessionId.TryParse(dto.SessionId, &parsed)) then
                raise (InvalidOperationException($"The reply rejection carries an invalid session id."))

            SessionActor.ReplyRejected(ReplyMismatchException(parsed, dto.RequestId, dto.Message)) :> obj
        elif wire :? SetAgentAppliedDto then
            SessionActor.SetAgentApplied(requireSession (wire :?> SetAgentAppliedDto).Session) :> obj
        elif wire :? SetAgentPendingDto then
            SessionActor.SetAgentPending(requireSession (wire :?> SetAgentPendingDto).Session) :> obj
        elif wire :? SetAgentRejectedDto then
            SessionActor.SetAgentRejected((wire :?> SetAgentRejectedDto).State) :> obj
        elif wire :? SubscribeDto then
            let dto = wire :?> SubscribeDto

            if String.IsNullOrWhiteSpace dto.Tenant then
                raise (InvalidOperationException("The subscribe request carries no tenant."))

            let mutable sessionId = Unchecked.defaultof<SessionId>

            if not (SessionId.TryParse(dto.SessionId, &sessionId)) then
                raise (InvalidOperationException("The subscribe request carries an invalid session id."))

            if String.IsNullOrWhiteSpace dto.SubscriberToken then
                raise (InvalidOperationException("The subscribe request carries no subscriber token."))

            ({
                Tenant = TenantId.Create(dto.Tenant)
                SessionId = sessionId
                FromSequence = dto.FromSequence
                SubscriberToken = dto.SubscriberToken
            }
            : CrossNodeSubscriptions.CrossNodeSubscribeRequest)
            :> obj
        elif wire :? UnsubscribeDto then
            let dto = wire :?> UnsubscribeDto

            if String.IsNullOrWhiteSpace dto.Tenant then
                raise (InvalidOperationException("The unsubscribe request carries no tenant."))

            let mutable sessionId = Unchecked.defaultof<SessionId>

            if not (SessionId.TryParse(dto.SessionId, &sessionId)) then
                raise (InvalidOperationException("The unsubscribe request carries an invalid session id."))

            if String.IsNullOrWhiteSpace dto.SubscriberToken then
                raise (InvalidOperationException("The unsubscribe request carries no subscriber token."))

            ({
                Tenant = TenantId.Create(dto.Tenant)
                SessionId = sessionId
                SubscriberToken = dto.SubscriberToken
            }
            : CrossNodeSubscriptions.CrossNodeUnsubscribe)
            :> obj
        elif wire :? EventBatchDto then
            let dto = wire :?> EventBatchDto

            let mutable sessionId = Unchecked.defaultof<SessionId>

            if not (SessionId.TryParse(dto.SessionId, &sessionId)) then
                raise (InvalidOperationException("The event batch carries an invalid session id."))

            let events =
                if isNull (box dto.Events) then
                    Array.Empty<SessionEvent>() :> IReadOnlyList<SessionEvent>
                else
                    dto.Events |> Array.filter (fun evt -> not (isNull (box evt))) :> IReadOnlyList<SessionEvent>

            ({
                SessionId = sessionId
                Events = events
                NextCursor = dto.NextCursor
                EndOfStream = dto.EndOfStream
            }
            : CrossNodeSubscriptions.CrossNodeEventBatch)
            :> obj
        elif wire :? SessionEventDto then
            let dto = wire :?> SessionEventDto

            if isNull (box dto.Event) then
                raise (InvalidOperationException("The session event envelope carries no event."))

            dto.Event :> obj
        else
            raise (
                InvalidOperationException(
                    $"The wire envelope maps no live message for '{wire.GetType().FullName}'. Only registered wire DTOs cross a node boundary."
                )
            )
