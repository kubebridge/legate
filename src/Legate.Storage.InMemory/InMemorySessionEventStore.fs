// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.InMemory

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Legate

/// The in-memory <see cref="T:Legate.ISessionEventStore" />: the ordered
/// per-session journal with claim-fenced appends, cursor replay, and the
/// leased cleanup that archives a journal away. Appends fence on the
/// session store's live claim token through the shared database, so the
/// loser of a takeover writes nothing. Sequences are per-session, 1-based,
/// and gap-free; a limit breach (the database options) throws before any
/// part of the batch lands.
type InMemorySessionEventStore(database: InMemoryDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let ok value = Task.FromResult value

    let jsonOptions = JsonSerializerOptions()

    // The estimated byte size of one event: the UTF-8 length of its
    // serialised JSON form, the only fair cross-implementation estimate
    // available inside the store.
    let eventBytes (event: SessionEvent) =
        float (JsonSerializer.Serialize(event, jsonOptions).Length)

    let journalRow (tenant: TenantId) (sessionId: SessionId) =
        match database.Journals.TryGetValue((tenant, sessionId)) with
        | true, row when not row.Archived -> Some row
        | _ -> None

    let ensureJournal (tenant: TenantId) (sessionId: SessionId) =
        match journalRow tenant sessionId with
        | Some row -> row
        | None ->
            let row = JournalRow(List<SessionEvent>(), false, 0L)
            database.Journals[(tenant, sessionId)] <- row
            row

    let claimFences (tenant: TenantId) (sessionId: SessionId) (claimToken: string) =
        match database.LiveClaims.TryGetValue((tenant, sessionId)) with
        | true, live -> String.Equals(live.Token, claimToken, StringComparison.Ordinal)
        | false, _ -> false

    interface ISessionEventStore with

        member _.Append(tenant, sessionId, claimToken, events, _) =
            if isNull (box events) then
                raise (ArgumentNullException(nameof events))

            if isNull (box claimToken) then
                raise (ArgumentNullException(nameof claimToken))

            if Seq.isEmpty events then
                raise (ArgumentException("The event batch must not be empty.", nameof events))

            for event in events do
                if isNull (box event) then
                    raise (ArgumentNullException(nameof events))

            lock database.Gate (fun () ->
                if not (database.Sessions.ContainsKey((tenant, sessionId))) then
                    raise (
                        SessionNotFoundException(
                            sessionId,
                            sprintf "No session %O exists in tenant %O." sessionId tenant
                        )
                    )

                // Limit checks run before any part of the batch lands.
                let batchSize = Seq.length events

                let options = database.Options

                if options.MaxAppendBatchSize > 0 && batchSize > options.MaxAppendBatchSize then
                    raise (
                        EventLimitExceededException(
                            "batchSize",
                            int64 options.MaxAppendBatchSize,
                            int64 batchSize,
                            sprintf
                                "An append of %d events exceeds the batch-size limit %d."
                                batchSize
                                options.MaxAppendBatchSize
                        )
                    )

                let sizes = events |> Seq.map eventBytes |> Seq.toList

                for (_: SessionEvent), size in Seq.zip events sizes do
                    if options.MaxEventBytes > 0L && size > float options.MaxEventBytes then
                        raise (
                            EventLimitExceededException(
                                "perEventBytes",
                                options.MaxEventBytes,
                                int64 size,
                                sprintf "An event exceeds the per-event byte limit %d." options.MaxEventBytes
                            )
                        )

                // The claim fence: a stale token rejects with zero writes.
                if not (claimFences tenant sessionId claimToken) then
                    EventAppendRejected(sessionId, "staleClaim") :> EventAppendOutcome
                else
                    let row = ensureJournal tenant sessionId

                    let countAfter = row.Events.Count + batchSize

                    if
                        options.MaxEventsPerSession > 0L
                        && int64 countAfter > options.MaxEventsPerSession
                    then
                        raise (
                            EventLimitExceededException(
                                "perSessionCount",
                                options.MaxEventsPerSession,
                                int64 countAfter,
                                sprintf
                                    "The journal would hold %d events, over the per-session limit %d."
                                    countAfter
                                    options.MaxEventsPerSession
                            )
                        )

                    let bytesAfter = row.TotalBytes + (sizes |> List.sum |> int64)

                    if
                        options.MaxJournalBytesPerSession > 0L
                        && bytesAfter > options.MaxJournalBytesPerSession
                    then
                        raise (
                            EventLimitExceededException(
                                "perSessionBytes",
                                options.MaxJournalBytesPerSession,
                                bytesAfter,
                                sprintf
                                    "The journal would hold %d bytes, over the per-session limit %d."
                                    bytesAfter
                                    options.MaxJournalBytesPerSession
                            )
                        )

                    // Stamp per-session gap-free sequences and store new
                    // instances; the input events are not mutated. Every
                    // kind constructor takes (sessionId, turnId,
                    // sequence, timestamp) then its kind fields.
                    let mutable sequence = int64 row.Events.Count

                    let stamped =
                        events
                        |> Seq.map (fun (event: SessionEvent) ->
                            sequence <- sequence + 1L
                            let sequence = Nullable sequence
                            let sessionId = event.SessionId
                            let turnId = event.TurnId
                            let timestamp = event.Timestamp

                            let restamped =
                                match event with
                                | :? TurnStartedEvent ->
                                    TurnStartedEvent(sessionId, turnId, sequence, timestamp) :> SessionEvent
                                | :? TextDeltaEvent as source ->
                                    TextDeltaEvent(sessionId, turnId, sequence, timestamp, source.Text)
                                    :> SessionEvent
                                | :? ReasoningDeltaEvent as source ->
                                    ReasoningDeltaEvent(sessionId, turnId, sequence, timestamp, source.Text)
                                    :> SessionEvent
                                | :? ToolCallStartedEvent as source ->
                                    ToolCallStartedEvent(
                                        sessionId,
                                        turnId,
                                        sequence,
                                        timestamp,
                                        source.ToolCallId,
                                        source.ToolName
                                    )
                                    :> SessionEvent
                                | :? ToolCallOutputEvent as source ->
                                    ToolCallOutputEvent(
                                        sessionId,
                                        turnId,
                                        sequence,
                                        timestamp,
                                        source.ToolCallId,
                                        source.Output
                                    )
                                    :> SessionEvent
                                | :? ToolCallCompletedEvent as source ->
                                    ToolCallCompletedEvent(
                                        sessionId,
                                        turnId,
                                        sequence,
                                        timestamp,
                                        source.ToolCallId,
                                        source.Error
                                    )
                                    :> SessionEvent
                                | :? PermissionRequestedEvent as source ->
                                    PermissionRequestedEvent(
                                        sessionId,
                                        turnId,
                                        sequence,
                                        timestamp,
                                        source.RequestId,
                                        source.ToolName
                                    )
                                    :> SessionEvent
                                | :? PermissionResolvedEvent as source ->
                                    PermissionResolvedEvent(
                                        sessionId,
                                        turnId,
                                        sequence,
                                        timestamp,
                                        source.RequestId,
                                        source.Decision
                                    )
                                    :> SessionEvent
                                | :? QuestionAskedEvent as source ->
                                    QuestionAskedEvent(
                                        sessionId,
                                        turnId,
                                        sequence,
                                        timestamp,
                                        source.QuestionId,
                                        source.Question
                                    )
                                    :> SessionEvent
                                | :? QuestionAnsweredEvent as source ->
                                    QuestionAnsweredEvent(
                                        sessionId,
                                        turnId,
                                        sequence,
                                        timestamp,
                                        source.QuestionId,
                                        source.Answer
                                    )
                                    :> SessionEvent
                                | :? UsageEvent as source ->
                                    UsageEvent(
                                        sessionId,
                                        turnId,
                                        sequence,
                                        timestamp,
                                        source.InputTokens,
                                        source.OutputTokens
                                    )
                                    :> SessionEvent
                                | :? CompactedEvent as source ->
                                    CompactedEvent(
                                        sessionId,
                                        turnId,
                                        sequence,
                                        timestamp,
                                        source.BeforeEstimate,
                                        source.AfterEstimate
                                    )
                                    :> SessionEvent
                                | :? TurnCompletedEvent ->
                                    TurnCompletedEvent(sessionId, turnId, sequence, timestamp) :> SessionEvent
                                | :? TurnAbortedEvent ->
                                    TurnAbortedEvent(sessionId, turnId, sequence, timestamp) :> SessionEvent
                                | :? TurnFailedEvent as source ->
                                    TurnFailedEvent(sessionId, turnId, sequence, timestamp, source.Reason)
                                    :> SessionEvent
                                | :? SessionClosedEvent ->
                                    SessionClosedEvent(sessionId, turnId, sequence, timestamp) :> SessionEvent
                                | _ ->
                                    raise (
                                        ArgumentException(
                                            "The event batch carries an unknown event kind.",
                                            nameof events
                                        )
                                    )

                            restamped)
                        |> Seq.toList

                    for event in stamped do
                        row.Events.Add event

                    row.TotalBytes <- row.TotalBytes + (sizes |> List.sum |> int64)

                    EventAppended(stamped :> IReadOnlyList<SessionEvent>) :> EventAppendOutcome)
            |> ok

        member _.Replay(tenant, sessionId, fromSequence, limit, _) =
            if limit <= 0 then
                raise (ArgumentOutOfRangeException(nameof limit, "The limit must be positive."))

            if fromSequence < 0L then
                raise (ArgumentOutOfRangeException(nameof fromSequence, "The cursor must not be negative."))

            lock database.Gate (fun () ->
                if not (database.Sessions.ContainsKey((tenant, sessionId))) then
                    EventReplayUnknownSession sessionId :> EventReplayOutcome
                else
                    match database.Journals.TryGetValue((tenant, sessionId)) with
                    | true, row when row.Archived -> EventReplayJournalExpired sessionId :> EventReplayOutcome
                    | true, row ->
                        let page =
                            row.Events
                            |> Seq.filter (fun event -> event.Sequence.HasValue && event.Sequence.Value > fromSequence)
                            |> Seq.truncate limit
                            |> Seq.toList

                        match page with
                        | [] -> EventReplayEndOfStream sessionId :> EventReplayOutcome
                        | _ ->
                            let next = page |> List.last |> (fun event -> event.Sequence.Value)

                            EventReplayPage(sessionId, page :> IReadOnlyList<SessionEvent>, Nullable next)
                            :> EventReplayOutcome
                    | false, _ -> EventReplayEndOfStream sessionId :> EventReplayOutcome)
            |> ok

        member _.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, _) =
            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            if leaseDuration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

            lock database.Gate (fun () ->
                if not (database.Sessions.ContainsKey((tenant, sessionId))) then
                    EventCleanupNotClaimable(sessionId, "unknownSession") :> EventCleanupState
                else
                    match journalRow tenant sessionId with
                    | None -> EventCleanupNotClaimable(sessionId, "journalGone") :> EventCleanupState
                    | Some row when row.Archived ->
                        EventCleanupNotClaimable(sessionId, "journalGone") :> EventCleanupState
                    | Some _ ->
                        match database.CleanupLeases.TryGetValue((tenant, sessionId)) with
                        | true, held when held.ExpiresAt > database.UtcNow ->
                            EventCleanupNotClaimable(sessionId, "leaseHeld") :> EventCleanupState
                        | _ ->
                            let claim =
                                {
                                    SessionId = sessionId
                                    Token = Ulid.NewUlid().ToString()
                                    Owner = owner
                                    ExpiresAt = database.UtcNow + leaseDuration
                                }

                            database.CleanupLeases[(tenant, sessionId)] <- claim
                            EventCleanupClaimed claim :> EventCleanupState)
            |> ok

        member _.CompleteCleanup(tenant, sessionId, claimToken, _) =
            if isNull (box claimToken) then
                raise (ArgumentNullException(nameof claimToken))

            lock database.Gate (fun () ->
                match database.CleanupLeases.TryGetValue((tenant, sessionId)) with
                | true, held when String.Equals(held.Token, claimToken, StringComparison.Ordinal) ->
                    database.CleanupLeases.Remove((tenant, sessionId)) |> ignore

                    match database.Journals.TryGetValue((tenant, sessionId)) with
                    | true, row ->
                        row.Archived <- true
                        row.Events.Clear()
                        row.TotalBytes <- 0L
                    | false, _ -> ()

                    EventCleanupApplied(sessionId, true) :> EventCleanupSettlement
                | true, held when held.ExpiresAt <= database.UtcNow ->
                    EventCleanupRejected(sessionId, "leaseExpired") :> EventCleanupSettlement
                | _ -> EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)
            |> ok

        member _.DeferCleanup(tenant, sessionId, claimToken, _) =
            if isNull (box claimToken) then
                raise (ArgumentNullException(nameof claimToken))

            lock database.Gate (fun () ->
                match database.CleanupLeases.TryGetValue((tenant, sessionId)) with
                | true, held when String.Equals(held.Token, claimToken, StringComparison.Ordinal) ->
                    database.CleanupLeases.Remove((tenant, sessionId)) |> ignore
                    EventCleanupApplied(sessionId, false) :> EventCleanupSettlement
                | true, held when held.ExpiresAt <= database.UtcNow ->
                    EventCleanupRejected(sessionId, "leaseExpired") :> EventCleanupSettlement
                | _ -> EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)
            |> ok
