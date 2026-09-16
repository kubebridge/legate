// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres

open System
open System.Collections.Generic
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Legate
open Npgsql
open PostgresSql

// The PostgreSQL ISessionEventStore: the ordered per-session journal with
// claim-fenced appends, cursor replay, and the leased cleanup that archives
// a journal away. Appends fence on the session store's live claim token in
// the turns table, so the loser of a takeover writes nothing. Sequences are
// per-session, 1-based, and gap-free: the session row locks first, so
// concurrent appends serialise. A limit breach throws before any part of
// the batch lands. Every query carries the tenant predicate: isolation is
// enforced here, not only in the host.
//
// Journal expiry rides a marker row in cleanup_claims (owner and token
// "__archived__"): completing cleanup deletes the live rows and leaves the
// marker, so replay reports the expired journal while unknown sessions and
// live-but-empty journals keep their own outcomes. Appending after a
// completion clears the marker and reopens the journal, matching the
// in-memory reference.
type PostgresSessionEventStore(options: PostgresOptions, timeProvider: TimeProvider) =

    do
        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        if isNull (box timeProvider) then
            raise (ArgumentNullException(nameof timeProvider))

    /// Constructs the store with the system clock.
    /// <param name="options">The PostgreSQL options. Must not be null.</param>
    new(options: PostgresOptions) = PostgresSessionEventStore(options, TimeProvider.System)

    /// The options the store was constructed with.
    member private _.Options = options

    /// The clock lease expiry reads.
    member private _.TimeProvider = timeProvider

    // ── Private helpers ──

    member private _.UtcNow = timeProvider.GetUtcNow()

    member private _.EnsureMigrated() = PostgresMigrations.ensure options

    member private _.SessionsTable = qualified options "sessions"
    member private _.TurnsTable = qualified options "turns"
    member private _.EventsTable = qualified options "events"
    member private _.CleanupTable = qualified options "cleanup_claims"

    member private _.OpenStatusFilter = "'Pending','Running','Suspended'"

    /// The marker owner and token CompleteCleanup leaves behind.
    member private _.ArchiveMarker = "__archived__"

    /// The estimated byte size of one event: the UTF-8 length of its
    /// serialised JSON form, the only fair cross-implementation estimate
    /// available inside the store.
    member private _.EventBytes(event: SessionEvent) =
        float (Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(event, JsonSerializerOptions())))

    /// The claim fence: the session's live claim token still matches and
    /// has not expired. The caller holds the session row lock.
    member private this.ClaimFences
        (
            connection: NpgsqlConnection,
            transaction: NpgsqlTransaction,
            tenant: TenantId,
            sessionId: SessionId,
            claimToken: string
        ) : bool =
        use cmd =
            command
                connection
                transaction
                $"SELECT claim_token, claim_expires_at FROM {this.TurnsTable} WHERE session_id = @sid AND tenant = @t AND claim_token IS NOT NULL AND status IN ({this.OpenStatusFilter}) LIMIT 1"

        textParam cmd "sid" (sessionId.ToString())
        textParam cmd "t" (tenant.ToString())

        use reader = cmd.ExecuteReader()

        let fences =
            reader.Read()
            && String.Equals(reader.GetString(0), claimToken, StringComparison.Ordinal)
            && (parseStamp (reader.GetString(1))) > this.UtcNow

        reader.Close()
        fences

    /// Stamps per-session gap-free sequences onto the batch, building new
    /// instances: the input events are not mutated. Every kind constructor
    /// takes (sessionId, turnId, sequence, timestamp) then its kind fields.
    member private _.Restamp(events: IReadOnlyList<SessionEvent>, firstSequence: int64) : SessionEvent list =
        let mutable sequence = firstSequence - 1L

        events
        |> Seq.map (fun (event: SessionEvent) ->
            sequence <- sequence + 1L
            let stamped = Nullable sequence
            let sessionId = event.SessionId
            let turnId = event.TurnId
            let timestamp = event.Timestamp

            match event with
            | :? TurnStartedEvent -> TurnStartedEvent(sessionId, turnId, stamped, timestamp) :> SessionEvent
            | :? TextDeltaEvent as source ->
                TextDeltaEvent(sessionId, turnId, stamped, timestamp, source.Text) :> SessionEvent
            | :? ReasoningDeltaEvent as source ->
                ReasoningDeltaEvent(sessionId, turnId, stamped, timestamp, source.Text) :> SessionEvent
            | :? ToolCallStartedEvent as source ->
                ToolCallStartedEvent(sessionId, turnId, stamped, timestamp, source.ToolCallId, source.ToolName)
                :> SessionEvent
            | :? ToolCallOutputEvent as source ->
                ToolCallOutputEvent(sessionId, turnId, stamped, timestamp, source.ToolCallId, source.Output)
                :> SessionEvent
            | :? ToolCallCompletedEvent as source ->
                ToolCallCompletedEvent(sessionId, turnId, stamped, timestamp, source.ToolCallId, source.Error)
                :> SessionEvent
            | :? PermissionRequestedEvent as source ->
                PermissionRequestedEvent(sessionId, turnId, stamped, timestamp, source.RequestId, source.ToolName)
                :> SessionEvent
            | :? PermissionResolvedEvent as source ->
                PermissionResolvedEvent(sessionId, turnId, stamped, timestamp, source.RequestId, source.Decision)
                :> SessionEvent
            | :? QuestionAskedEvent as source ->
                QuestionAskedEvent(sessionId, turnId, stamped, timestamp, source.QuestionId, source.Question)
                :> SessionEvent
            | :? QuestionAnsweredEvent as source ->
                QuestionAnsweredEvent(sessionId, turnId, stamped, timestamp, source.QuestionId, source.Answer)
                :> SessionEvent
            | :? UsageEvent as source ->
                UsageEvent(sessionId, turnId, stamped, timestamp, source.InputTokens, source.OutputTokens)
                :> SessionEvent
            | :? CompactedEvent as source ->
                CompactedEvent(sessionId, turnId, stamped, timestamp, source.BeforeEstimate, source.AfterEstimate)
                :> SessionEvent
            | :? CompactionFailedEvent as source ->
                CompactionFailedEvent(sessionId, turnId, stamped, timestamp, source.Reason) :> SessionEvent
            | :? TurnCompletedEvent -> TurnCompletedEvent(sessionId, turnId, stamped, timestamp) :> SessionEvent
            | :? TurnAbortedEvent as source ->
                TurnAbortedEvent(sessionId, turnId, stamped, timestamp, source.Cause, source.Reason) :> SessionEvent
            | :? TurnFailedEvent as source ->
                TurnFailedEvent(sessionId, turnId, stamped, timestamp, source.Reason) :> SessionEvent
            | :? SessionClosedEvent -> SessionClosedEvent(sessionId, turnId, stamped, timestamp) :> SessionEvent
            | :? UserMessageEvent as source ->
                UserMessageEvent(sessionId, turnId, stamped, timestamp, source.Message) :> SessionEvent
            | :? ContextPrunedEvent as source ->
                ContextPrunedEvent(
                    sessionId,
                    turnId,
                    stamped,
                    timestamp,
                    source.PrunedCount,
                    source.BeforeEstimate,
                    source.AfterEstimate
                )
                :> SessionEvent
            | :? SkillInvalidEvent as source ->
                SkillInvalidEvent(sessionId, turnId, stamped, timestamp, source.SkillName, source.Reason)
                :> SessionEvent
            | :? SkillLoadedEvent as source ->
                SkillLoadedEvent(sessionId, turnId, stamped, timestamp, source.SkillName, source.Companions)
                :> SessionEvent
            | :? AgentInvalidEvent as source ->
                AgentInvalidEvent(sessionId, turnId, stamped, timestamp, source.AgentName, source.Reason)
                :> SessionEvent
            | _ -> raise (ArgumentException("The event batch carries an unknown event kind.", "events")))
        |> Seq.toList

    interface ISessionEventStore with

        member this.Append(tenant, sessionId, claimToken, events, _) =
            if isNull (box events) then
                raise (ArgumentNullException(nameof events))

            if isNull (box claimToken) then
                raise (ArgumentNullException(nameof claimToken))

            if Seq.isEmpty events then
                raise (ArgumentException("The event batch must not be empty.", nameof events))

            for event in events do
                if isNull (box event) then
                    raise (ArgumentNullException(nameof events))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use lockCmd =
                    command
                        connection
                        transaction
                        $"SELECT id FROM {this.SessionsTable} WHERE id = @id AND tenant = @t FOR UPDATE"

                textParam lockCmd "id" (sessionId.ToString())
                textParam lockCmd "t" (tenant.ToString())

                use lockReader = lockCmd.ExecuteReader()
                let found = lockReader.Read()
                lockReader.Close()

                if not found then
                    raise (
                        SessionNotFoundException(
                            sessionId,
                            sprintf "No session %O exists in tenant %O." sessionId tenant
                        )
                    )

                // Limit checks run before any part of the batch lands.
                let batchSize = Seq.length events

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

                let sizes = events |> Seq.map this.EventBytes |> Seq.toList

                for size in sizes do
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
                if not (this.ClaimFences(connection, transaction, tenant, sessionId, claimToken)) then
                    EventAppendRejected(sessionId, "staleClaim") :> EventAppendOutcome
                else
                    use countCmd =
                        command
                            connection
                            transaction
                            $"SELECT COUNT(*), COALESCE(SUM(octet_length(payload_json)::bigint), 0), COALESCE(MAX(sequence), 0) FROM {this.EventsTable} WHERE session_id = @sid AND tenant = @t"

                    textParam countCmd "sid" (sessionId.ToString())
                    textParam countCmd "t" (tenant.ToString())

                    use countReader = countCmd.ExecuteReader()
                    countReader.Read() |> ignore
                    let count = countReader.GetInt64(0)
                    let totalBytes = countReader.GetInt64(1)
                    let maxSequence = countReader.GetInt64(2)
                    countReader.Close()

                    let countAfter = count + int64 batchSize

                    if options.MaxEventsPerSession > 0L && countAfter > options.MaxEventsPerSession then
                        raise (
                            EventLimitExceededException(
                                "perSessionCount",
                                options.MaxEventsPerSession,
                                countAfter,
                                sprintf
                                    "The journal would hold %d events, over the per-session limit %d."
                                    countAfter
                                    options.MaxEventsPerSession
                            )
                        )

                    let bytesAfter = totalBytes + (sizes |> List.sum |> int64)

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

                    let stamped = this.Restamp(events, maxSequence + 1L)

                    for event in stamped do
                        use insertCmd =
                            command
                                connection
                                transaction
                                $"INSERT INTO {this.EventsTable} (session_id, sequence, tenant, turn_id, event_type, payload_json, timestamp) VALUES (@sid, @seq, @t, @tid, @kind, @payload, @ts)"

                        textParam insertCmd "sid" (sessionId.ToString())
                        longParam insertCmd "seq" event.Sequence.Value
                        textParam insertCmd "t" (tenant.ToString())
                        textParam insertCmd "tid" (event.TurnId.ToString())
                        textParam insertCmd "kind" (event.GetType().Name)
                        textParam insertCmd "payload" (serialize<SessionEvent> event)
                        textParam insertCmd "ts" (stamp event.Timestamp)
                        insertCmd.ExecuteNonQuery() |> ignore

                    // Reopening: an append after a completed cleanup clears
                    // the archive marker, so the journal reads live again.
                    use reopenCmd =
                        command
                            connection
                            transaction
                            $"DELETE FROM {this.CleanupTable} WHERE session_id = @sid AND tenant = @t AND owner = @marker"

                    textParam reopenCmd "sid" (sessionId.ToString())
                    textParam reopenCmd "t" (tenant.ToString())
                    textParam reopenCmd "marker" this.ArchiveMarker
                    reopenCmd.ExecuteNonQuery() |> ignore

                    EventAppended(stamped :> IReadOnlyList<SessionEvent>) :> EventAppendOutcome)
            |> Task.FromResult

        member this.Replay(tenant, sessionId, fromSequence, limit, _) =
            if limit <= 0 then
                raise (ArgumentOutOfRangeException(nameof limit, "The limit must be positive."))

            if fromSequence < 0L then
                raise (ArgumentOutOfRangeException(nameof fromSequence, "The cursor must not be negative."))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use lockCmd =
                    command
                        connection
                        transaction
                        $"SELECT id FROM {this.SessionsTable} WHERE id = @id AND tenant = @t"

                textParam lockCmd "id" (sessionId.ToString())
                textParam lockCmd "t" (tenant.ToString())

                use lockReader = lockCmd.ExecuteReader()
                let found = lockReader.Read()
                lockReader.Close()

                if not found then
                    EventReplayUnknownSession sessionId :> EventReplayOutcome
                else
                    use markerCmd =
                        command
                            connection
                            transaction
                            $"SELECT 1 FROM {this.CleanupTable} WHERE session_id = @sid AND tenant = @t AND owner = @marker"

                    textParam markerCmd "sid" (sessionId.ToString())
                    textParam markerCmd "t" (tenant.ToString())
                    textParam markerCmd "marker" this.ArchiveMarker

                    use markerReader = markerCmd.ExecuteReader()
                    let archived = markerReader.Read()
                    markerReader.Close()

                    if archived then
                        EventReplayJournalExpired sessionId :> EventReplayOutcome
                    else
                        use pageCmd =
                            command
                                connection
                                transaction
                                $"SELECT payload_json, sequence FROM {this.EventsTable} WHERE session_id = @sid AND tenant = @t AND sequence > @cursor ORDER BY sequence LIMIT @take"

                        textParam pageCmd "sid" (sessionId.ToString())
                        textParam pageCmd "t" (tenant.ToString())
                        longParam pageCmd "cursor" fromSequence
                        intParam pageCmd "take" limit

                        use pageReader = pageCmd.ExecuteReader()

                        let page =
                            [
                                while pageReader.Read() do
                                    let journaled: SessionEvent =
                                        match JsonSerializer.Deserialize(pageReader.GetString(0), jsonOptions) with
                                        | null ->
                                            raise (InvalidOperationException("The stored journal event is null."))
                                        | decoded -> decoded

                                    (journaled, pageReader.GetInt64(1))
                            ]

                        pageReader.Close()

                        match page with
                        | [] -> EventReplayEndOfStream sessionId :> EventReplayOutcome
                        | _ ->
                            let next = page |> List.last |> snd

                            EventReplayPage(
                                sessionId,
                                (page |> List.map fst) :> IReadOnlyList<SessionEvent>,
                                Nullable next
                            )
                            :> EventReplayOutcome)
            |> Task.FromResult

        member this.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, _) =
            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            if leaseDuration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use lockCmd =
                    command
                        connection
                        transaction
                        $"SELECT id FROM {this.SessionsTable} WHERE id = @id AND tenant = @t"

                textParam lockCmd "id" (sessionId.ToString())
                textParam lockCmd "t" (tenant.ToString())

                use lockReader = lockCmd.ExecuteReader()
                let found = lockReader.Read()
                lockReader.Close()

                if not found then
                    EventCleanupNotClaimable(sessionId, "unknownSession") :> EventCleanupState
                else
                    use markerCmd =
                        command
                            connection
                            transaction
                            $"SELECT 1 FROM {this.CleanupTable} WHERE session_id = @sid AND tenant = @t AND owner = @marker"

                    textParam markerCmd "sid" (sessionId.ToString())
                    textParam markerCmd "t" (tenant.ToString())
                    textParam markerCmd "marker" this.ArchiveMarker

                    use markerReader = markerCmd.ExecuteReader()
                    let archived = markerReader.Read()
                    markerReader.Close()

                    if archived then
                        EventCleanupNotClaimable(sessionId, "journalGone") :> EventCleanupState
                    else
                        use journalCmd =
                            command
                                connection
                                transaction
                                $"SELECT 1 FROM {this.EventsTable} WHERE session_id = @sid AND tenant = @t LIMIT 1"

                        textParam journalCmd "sid" (sessionId.ToString())
                        textParam journalCmd "t" (tenant.ToString())

                        use journalReader = journalCmd.ExecuteReader()
                        let hasJournal = journalReader.Read()
                        journalReader.Close()

                        if not hasJournal then
                            EventCleanupNotClaimable(sessionId, "journalGone") :> EventCleanupState
                        else
                            use leaseCmd =
                                command
                                    connection
                                    transaction
                                    $"SELECT token, expires_at FROM {this.CleanupTable} WHERE session_id = @sid AND tenant = @t AND owner <> @marker FOR UPDATE"

                            textParam leaseCmd "sid" (sessionId.ToString())
                            textParam leaseCmd "t" (tenant.ToString())
                            textParam leaseCmd "marker" this.ArchiveMarker

                            use leaseReader = leaseCmd.ExecuteReader()

                            let held =
                                leaseReader.Read() && (parseStamp (leaseReader.GetString(1))) > this.UtcNow

                            leaseReader.Close()

                            if held then
                                EventCleanupNotClaimable(sessionId, "leaseHeld") :> EventCleanupState
                            else
                                let claim =
                                    {
                                        SessionId = sessionId
                                        Token = Ulid.NewUlid().ToString()
                                        Owner = owner
                                        ExpiresAt = this.UtcNow + leaseDuration
                                    }

                                use grantCmd =
                                    command
                                        connection
                                        transaction
                                        $"INSERT INTO {this.CleanupTable} (session_id, tenant, token, owner, expires_at) VALUES (@sid, @t, @tok, @owner, @exp) ON CONFLICT (session_id) DO UPDATE SET tenant = @t, token = @tok, owner = @owner, expires_at = @exp"

                                textParam grantCmd "sid" (sessionId.ToString())
                                textParam grantCmd "t" (tenant.ToString())
                                textParam grantCmd "tok" claim.Token
                                textParam grantCmd "owner" owner
                                textParam grantCmd "exp" (stamp claim.ExpiresAt)
                                grantCmd.ExecuteNonQuery() |> ignore

                                EventCleanupClaimed claim :> EventCleanupState)
            |> Task.FromResult

        member this.CompleteCleanup(tenant, sessionId, claimToken, _) =
            if isNull (box claimToken) then
                raise (ArgumentNullException(nameof claimToken))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use leaseCmd =
                    command
                        connection
                        transaction
                        $"SELECT token, expires_at FROM {this.CleanupTable} WHERE session_id = @sid AND tenant = @t AND owner <> @marker FOR UPDATE"

                textParam leaseCmd "sid" (sessionId.ToString())
                textParam leaseCmd "t" (tenant.ToString())
                textParam leaseCmd "marker" this.ArchiveMarker

                use leaseReader = leaseCmd.ExecuteReader()

                let lease: (string * DateTimeOffset) option =
                    if leaseReader.Read() then
                        Some(leaseReader.GetString(0), parseStamp (leaseReader.GetString(1)))
                    else
                        None

                leaseReader.Close()

                match lease with
                | None -> EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement
                | Some(_, expiresAt) when expiresAt <= this.UtcNow ->
                    EventCleanupRejected(sessionId, "leaseExpired") :> EventCleanupSettlement
                | Some(token, _) when not (String.Equals(token, claimToken, StringComparison.Ordinal)) ->
                    EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement
                | Some _ ->
                    use deleteLeaseCmd =
                        command
                            connection
                            transaction
                            $"DELETE FROM {this.CleanupTable} WHERE session_id = @sid AND tenant = @t"

                    textParam deleteLeaseCmd "sid" (sessionId.ToString())
                    textParam deleteLeaseCmd "t" (tenant.ToString())
                    deleteLeaseCmd.ExecuteNonQuery() |> ignore

                    use deleteEventsCmd =
                        command
                            connection
                            transaction
                            $"DELETE FROM {this.EventsTable} WHERE session_id = @sid AND tenant = @t"

                    textParam deleteEventsCmd "sid" (sessionId.ToString())
                    textParam deleteEventsCmd "t" (tenant.ToString())
                    deleteEventsCmd.ExecuteNonQuery() |> ignore

                    use markerCmd =
                        command
                            connection
                            transaction
                            $"INSERT INTO {this.CleanupTable} (session_id, tenant, token, owner, expires_at) VALUES (@sid, @t, @marker, @marker, @exp)"

                    textParam markerCmd "sid" (sessionId.ToString())
                    textParam markerCmd "t" (tenant.ToString())
                    textParam markerCmd "marker" this.ArchiveMarker
                    textParam markerCmd "exp" (stamp this.UtcNow)
                    markerCmd.ExecuteNonQuery() |> ignore

                    EventCleanupApplied(sessionId, true) :> EventCleanupSettlement)
            |> Task.FromResult

        member this.DeferCleanup(tenant, sessionId, claimToken, _) =
            if isNull (box claimToken) then
                raise (ArgumentNullException(nameof claimToken))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use leaseCmd =
                    command
                        connection
                        transaction
                        $"SELECT token, expires_at FROM {this.CleanupTable} WHERE session_id = @sid AND tenant = @t AND owner <> @marker FOR UPDATE"

                textParam leaseCmd "sid" (sessionId.ToString())
                textParam leaseCmd "t" (tenant.ToString())
                textParam leaseCmd "marker" this.ArchiveMarker

                use leaseReader = leaseCmd.ExecuteReader()

                let lease: (string * DateTimeOffset) option =
                    if leaseReader.Read() then
                        Some(leaseReader.GetString(0), parseStamp (leaseReader.GetString(1)))
                    else
                        None

                leaseReader.Close()

                match lease with
                | None -> EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement
                | Some(_, expiresAt) when expiresAt <= this.UtcNow ->
                    EventCleanupRejected(sessionId, "leaseExpired") :> EventCleanupSettlement
                | Some(token, _) when not (String.Equals(token, claimToken, StringComparison.Ordinal)) ->
                    EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement
                | Some _ ->
                    use deleteCmd =
                        command
                            connection
                            transaction
                            $"DELETE FROM {this.CleanupTable} WHERE session_id = @sid AND tenant = @t"

                    textParam deleteCmd "sid" (sessionId.ToString())
                    textParam deleteCmd "t" (tenant.ToString())
                    deleteCmd.ExecuteNonQuery() |> ignore

                    EventCleanupApplied(sessionId, false) :> EventCleanupSettlement)
            |> Task.FromResult
