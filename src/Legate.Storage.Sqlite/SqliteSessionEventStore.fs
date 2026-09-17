// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Sqlite

open System
open System.Collections.Generic
open System.Threading.Tasks
open Legate
open Microsoft.Data.Sqlite

// The SQLite ISessionEventStore: the ordered per-session journal with
// claim-fenced appends, cursor replay, and the leased cleanup that archives
// a journal away. Appends fence on the session store's live claim token
// through the shared database, so the loser of a takeover writes nothing.
// Sequences are per-session, 1-based, and gap-free. The baseline carries no
// journal-archive flag, so archival markers live in a local table beside
// the baseline, over the same file. Every SqliteException funnels through
// the SqliteErrors boundary.

/// <summary>
/// The SQLite <see cref="T:Legate.ISessionEventStore" /> over one shared
/// database file.
/// </summary>
/// <param name="database">The shared database every store uses. Must not be null.</param>
type SqliteSessionEventStore(database: SqliteDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let path = database.Path
    let mapSql (ex: SqliteException) : LegateException = SqliteErrors.ofSqliteException path ex
    let toIso = SqliteDatabase.ToIso
    let ofIso = SqliteDatabase.OfIso

    let eventsTable () = database.Table "events"
    let sessionsTable () = database.Table "sessions"
    let turnsTable () = database.Table "turns"
    let cleanupTable () = database.Table "cleanup_claims"
    let archiveTable () = database.Table "journal_archive"

    let sessionExists
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (sessionId: SessionId)
        : bool =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <- $"SELECT id FROM \"%s{sessionsTable ()}\" WHERE id = $id AND tenant = $tenant"

        command.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

        use reader = command.ExecuteReader()
        reader.Read()

    let claimFences
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (sessionId: SessionId)
        (claimToken: string)
        : bool =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT claim_token, claim_expires_at FROM \"%s{turnsTable ()}\" WHERE session_id = $session AND tenant = $tenant AND claim_token IS NOT NULL AND status IN ('Running','Suspended','Pending')"

        command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

        use reader = command.ExecuteReader()
        let mutable fences = false

        while reader.Read() do
            let stored = reader.GetString(0)
            let expiresAt = ofIso (reader.GetString(1))

            if
                String.Equals(stored, claimToken, StringComparison.Ordinal)
                && expiresAt > database.UtcNow
            then
                fences <- true

        fences

    let isArchived
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (sessionId: SessionId)
        : bool =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT session_id FROM \"%s{archiveTable ()}\" WHERE session_id = $session AND tenant = $tenant"

        command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

        use reader = command.ExecuteReader()
        reader.Read()

    let markArchived
        (connection: SqliteConnection)
        (transaction: SqliteTransaction)
        (tenant: TenantId)
        (sessionId: SessionId)
        (archiveLocation: string | null)
        =
        use command = connection.CreateCommand()
        command.Transaction <- transaction

        command.CommandText <-
            $"INSERT INTO \"%s{archiveTable ()}\" (session_id, tenant, archived_at, archive_path) VALUES ($session, $tenant, $at, $path) ON CONFLICT (session_id) DO UPDATE SET tenant = $tenant, archived_at = $at, archive_path = $path"

        command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
        command.Parameters.AddWithValue("$at", toIso database.UtcNow) |> ignore

        command.Parameters.AddWithValue(
            "$path",
            if isNull (box archiveLocation) then
                box DBNull.Value
            else
                box archiveLocation
        )
        |> ignore

        command.ExecuteNonQuery() |> ignore

    let readArchivePath
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (sessionId: SessionId)
        : string | null =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT archive_path FROM \"%s{archiveTable ()}\" WHERE session_id = $session AND tenant = $tenant"

        command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

        use reader = command.ExecuteReader()

        if reader.Read() && not (reader.IsDBNull(0)) then
            reader.GetString(0)
        else
            null

    let clearArchived
        (connection: SqliteConnection)
        (transaction: SqliteTransaction)
        (tenant: TenantId)
        (sessionId: SessionId)
        =
        use command = connection.CreateCommand()
        command.Transaction <- transaction

        command.CommandText <- $"DELETE FROM \"%s{archiveTable ()}\" WHERE session_id = $session AND tenant = $tenant"

        command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
        command.ExecuteNonQuery() |> ignore

    let eventTypeName (event: SessionEvent) : string = event.GetType().Name

    let restamp (event: SessionEvent) (sequence: int64) : SessionEvent =
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
            ToolCallOutputEvent(sessionId, turnId, stamped, timestamp, source.ToolCallId, source.Output) :> SessionEvent
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
            UsageEvent(sessionId, turnId, stamped, timestamp, source.InputTokens, source.OutputTokens) :> SessionEvent
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
            SkillInvalidEvent(sessionId, turnId, stamped, timestamp, source.SkillName, source.Reason) :> SessionEvent
        | :? SkillLoadedEvent as source ->
            SkillLoadedEvent(sessionId, turnId, stamped, timestamp, source.SkillName, source.Companions) :> SessionEvent
        | :? AgentInvalidEvent as source ->
            AgentInvalidEvent(sessionId, turnId, stamped, timestamp, source.AgentName, source.Reason) :> SessionEvent
        | _ -> raise (ArgumentException("The event batch carries an unknown event kind.", nameof event))

    interface ISessionEventStore with

        member _.Append(tenant, sessionId, claimToken, events, _) =
            task {
                if isNull (box events) then
                    raise (ArgumentNullException(nameof events))

                if isNull (box claimToken) then
                    raise (ArgumentNullException(nameof claimToken))

                if Seq.isEmpty events then
                    raise (ArgumentException("The event batch must not be empty.", nameof events))

                for event in events do
                    if isNull (box event) then
                        raise (ArgumentNullException(nameof events))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            if not (sessionExists connection transaction tenant sessionId) then
                                raise (
                                    SessionNotFoundException(
                                        sessionId,
                                        sprintf "No session %O exists in tenant %O." sessionId tenant
                                    )
                                )

                            if not (claimFences connection transaction tenant sessionId claimToken) then
                                transaction.Rollback()
                                EventAppendRejected(sessionId, "staleClaim") :> EventAppendOutcome
                            else
                                // A cleanup that archived the journal re-opens on the next
                                // append: the fresh batch starts a new journal.
                                clearArchived connection transaction tenant sessionId

                                use next = connection.CreateCommand()
                                next.Transaction <- transaction

                                next.CommandText <-
                                    $"SELECT COALESCE(MAX(sequence), 0) FROM \"%s{eventsTable ()}\" WHERE session_id = $session"

                                next.Parameters.AddWithValue("$session", sessionId.Value) |> ignore

                                let baseSequence = Convert.ToInt64(next.ExecuteScalar())
                                let mutable sequence = baseSequence
                                let stamped = List<SessionEvent>()

                                for event in events do
                                    sequence <- sequence + 1L
                                    stamped.Add(restamp event sequence)

                                for event in stamped do
                                    use insert = connection.CreateCommand()
                                    insert.Transaction <- transaction

                                    insert.CommandText <-
                                        $"INSERT INTO \"%s{eventsTable ()}\" (session_id, sequence, tenant, turn_id, event_type, payload_json, timestamp) VALUES ($session, $sequence, $tenant, $turn, $type, $payload, $at)"

                                    insert.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                    insert.Parameters.AddWithValue("$sequence", event.Sequence.Value) |> ignore
                                    insert.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                    insert.Parameters.AddWithValue("$turn", event.TurnId.Value) |> ignore
                                    insert.Parameters.AddWithValue("$type", eventTypeName event) |> ignore
                                    insert.Parameters.AddWithValue("$payload", SqliteJson.serialize event) |> ignore
                                    insert.Parameters.AddWithValue("$at", toIso event.Timestamp) |> ignore
                                    insert.ExecuteNonQuery() |> ignore

                                transaction.Commit()
                                EventAppended(stamped :> IReadOnlyList<SessionEvent>) :> EventAppendOutcome)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.Replay(tenant, sessionId, fromSequence, limit, _) =
            task {
                if limit <= 0 then
                    raise (ArgumentOutOfRangeException(nameof limit, "The limit must be positive."))

                if fromSequence < 0L then
                    raise (ArgumentOutOfRangeException(nameof fromSequence, "The cursor must not be negative."))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            if not (sessionExists connection null tenant sessionId) then
                                EventReplayUnknownSession sessionId :> EventReplayOutcome
                            elif isArchived connection null tenant sessionId then
                                let pointer = readArchivePath connection null tenant sessionId
                                EventReplayJournalExpired(sessionId, pointer) :> EventReplayOutcome
                            else
                                use command = connection.CreateCommand()

                                command.CommandText <-
                                    $"SELECT payload_json, sequence FROM \"%s{eventsTable ()}\" WHERE session_id = $session AND sequence > $cursor ORDER BY sequence LIMIT $limit"

                                command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                command.Parameters.AddWithValue("$cursor", fromSequence) |> ignore
                                command.Parameters.AddWithValue("$limit", limit) |> ignore

                                use reader = command.ExecuteReader()
                                let page = List<SessionEvent>()

                                while reader.Read() do
                                    page.Add(SqliteJson.deserialize<SessionEvent> (reader.GetString(0)))

                                match page.Count with
                                | 0 -> EventReplayEndOfStream sessionId :> EventReplayOutcome
                                | _ ->
                                    let next = page |> Seq.last |> (fun event -> event.Sequence.Value)

                                    EventReplayPage(sessionId, page :> IReadOnlyList<SessionEvent>, Nullable next)
                                    :> EventReplayOutcome)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, _) =
            task {
                if isNull (box owner) then
                    raise (ArgumentNullException(nameof owner))

                if leaseDuration <= TimeSpan.Zero then
                    raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            if not (sessionExists connection transaction tenant sessionId) then
                                transaction.Rollback()
                                EventCleanupNotClaimable(sessionId, "unknownSession") :> EventCleanupState
                            elif isArchived connection transaction tenant sessionId then
                                transaction.Rollback()
                                EventCleanupNotClaimable(sessionId, "journalGone") :> EventCleanupState
                            else
                                use count = connection.CreateCommand()
                                count.Transaction <- transaction

                                count.CommandText <-
                                    $"SELECT COUNT(*) FROM \"%s{eventsTable ()}\" WHERE session_id = $session"

                                count.Parameters.AddWithValue("$session", sessionId.Value) |> ignore

                                if Convert.ToInt64(count.ExecuteScalar()) = 0L then
                                    transaction.Rollback()
                                    EventCleanupNotClaimable(sessionId, "journalGone") :> EventCleanupState
                                else
                                    use lease = connection.CreateCommand()
                                    lease.Transaction <- transaction

                                    lease.CommandText <-
                                        $"SELECT token, owner, expires_at FROM \"%s{cleanupTable ()}\" WHERE session_id = $session"

                                    lease.Parameters.AddWithValue("$session", sessionId.Value) |> ignore

                                    use reader = lease.ExecuteReader()

                                    if reader.Read() && ofIso (reader.GetString(2)) > database.UtcNow then
                                        transaction.Rollback()
                                        EventCleanupNotClaimable(sessionId, "leaseHeld") :> EventCleanupState
                                    else
                                        reader.Close()

                                        let claim =
                                            {
                                                SessionId = sessionId
                                                Token = Ulid.NewUlid().ToString()
                                                Owner = owner
                                                ExpiresAt = database.UtcNow + leaseDuration
                                            }

                                        use grant = connection.CreateCommand()
                                        grant.Transaction <- transaction

                                        grant.CommandText <-
                                            $"INSERT INTO \"%s{cleanupTable ()}\" (session_id, tenant, token, owner, expires_at) VALUES ($session, $tenant, $token, $owner, $expires) ON CONFLICT (session_id) DO UPDATE SET tenant = $tenant, token = $token, owner = $owner, expires_at = $expires"

                                        grant.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                        grant.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                        grant.Parameters.AddWithValue("$token", claim.Token) |> ignore
                                        grant.Parameters.AddWithValue("$owner", owner) |> ignore
                                        grant.Parameters.AddWithValue("$expires", toIso claim.ExpiresAt) |> ignore
                                        grant.ExecuteNonQuery() |> ignore
                                        transaction.Commit()
                                        EventCleanupClaimed claim :> EventCleanupState)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.CompleteCleanup(tenant, sessionId, claimToken, archiveLocation, _) =
            task {
                if isNull (box claimToken) then
                    raise (ArgumentNullException(nameof claimToken))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            use lease = connection.CreateCommand()
                            lease.Transaction <- transaction

                            lease.CommandText <-
                                $"SELECT token, expires_at FROM \"%s{cleanupTable ()}\" WHERE session_id = $session AND tenant = $tenant"

                            lease.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                            lease.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use reader = lease.ExecuteReader()

                            if not (reader.Read()) then
                                transaction.Rollback()
                                EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement
                            else
                                let stored = reader.GetString(0)
                                let expiresAt = ofIso (reader.GetString(1))
                                reader.Close()

                                if expiresAt <= database.UtcNow then
                                    transaction.Rollback()
                                    EventCleanupRejected(sessionId, "leaseExpired") :> EventCleanupSettlement
                                elif not (String.Equals(stored, claimToken, StringComparison.Ordinal)) then
                                    transaction.Rollback()
                                    EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement
                                else
                                    use release = connection.CreateCommand()
                                    release.Transaction <- transaction

                                    release.CommandText <-
                                        $"DELETE FROM \"%s{cleanupTable ()}\" WHERE session_id = $session"

                                    release.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                    release.ExecuteNonQuery() |> ignore

                                    use clear = connection.CreateCommand()
                                    clear.Transaction <- transaction

                                    clear.CommandText <-
                                        $"DELETE FROM \"%s{eventsTable ()}\" WHERE session_id = $session"

                                    clear.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                    clear.ExecuteNonQuery() |> ignore
                                    markArchived connection transaction tenant sessionId archiveLocation
                                    transaction.Commit()
                                    EventCleanupApplied(sessionId, true) :> EventCleanupSettlement)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.DeferCleanup(tenant, sessionId, claimToken, _) =
            task {
                if isNull (box claimToken) then
                    raise (ArgumentNullException(nameof claimToken))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            use lease = connection.CreateCommand()
                            lease.Transaction <- transaction

                            lease.CommandText <-
                                $"SELECT token, expires_at FROM \"%s{cleanupTable ()}\" WHERE session_id = $session AND tenant = $tenant"

                            lease.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                            lease.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use reader = lease.ExecuteReader()

                            if not (reader.Read()) then
                                transaction.Rollback()
                                EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement
                            else
                                let stored = reader.GetString(0)
                                let expiresAt = ofIso (reader.GetString(1))
                                reader.Close()

                                if expiresAt <= database.UtcNow then
                                    transaction.Rollback()
                                    EventCleanupRejected(sessionId, "leaseExpired") :> EventCleanupSettlement
                                elif not (String.Equals(stored, claimToken, StringComparison.Ordinal)) then
                                    transaction.Rollback()
                                    EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement
                                else
                                    use release = connection.CreateCommand()
                                    release.Transaction <- transaction

                                    release.CommandText <-
                                        $"DELETE FROM \"%s{cleanupTable ()}\" WHERE session_id = $session"

                                    release.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                    release.ExecuteNonQuery() |> ignore
                                    transaction.Commit()
                                    EventCleanupApplied(sessionId, false) :> EventCleanupSettlement)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }
