// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Sqlite

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Data.Sqlite

/// The SQLite atomic terminal settlement capability (issue 363): validates captured claim
/// authority and commits terminal consumption, lifecycle and prime disposition, completion
/// deduplication and outbox, and settlement bookkeeping under one takeover-serializing
/// transaction. Shares the session database file, so the journal append rides the same
/// transaction with savepoint isolation. A separate VerifyClaim followed by unfenced writes
/// is never called atomic.
type SqliteSessionSettlementStore(database: SqliteDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let mapSql (ex: SqliteException) : LegateException =
        SqliteErrors.ofSqliteException database.Path ex

    let toIso = SqliteDatabase.ToIso
    let ofIso = SqliteDatabase.OfIso
    let sessionsTable () = database.Table "sessions"
    let inboxTable () = database.Table "inbox"
    let turnsTable () = database.Table "turns"
    let outboxTable () = database.Table "outbox"
    let settlementsTable () = database.Table "execution_settlements"
    let eventsTable () = database.Table "events"
    let stateName (state: SessionState) = state.ToString()
    let parseDelivery (text: string) : DeliveryMode = Enum.Parse<DeliveryMode>(text, false)
    let turnStatusName (status: TurnStatus) = status.ToString()

    let isTerminalStatus (status: TurnStatus) =
        status = TurnStatus.Completed
        || status = TurnStatus.Aborted
        || status = TurnStatus.Failed

    let fingerprintOf (request: SessionSettlementRequest) =
        sprintf
            "%O|%d|%O|%s|%d|%O|%O|%s"
            request.SessionId
            request.Position
            request.Claim.TurnId
            request.Claim.Token
            request.Claim.Attempt
            request.ExecutionId
            request.Result.Status
            request.CompletionKey

    interface ISessionSettlementStore with

        member _.SupportsSettlementJournal(eventStore) =
            match eventStore with
            | :? SqliteSessionEventStore as journal -> Object.ReferenceEquals(database, journal.Database)
            | _ -> false

        member _.AdmitExecution(tenant, sessionId, position, claim, _) =
            task {
                if isNull (box claim) then
                    raise (ArgumentNullException(nameof claim))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            use sessionCheck = connection.CreateCommand()
                            sessionCheck.Transaction <- transaction

                            sessionCheck.CommandText <-
                                $"SELECT state FROM \"%s{sessionsTable ()}\" WHERE id = $id AND tenant = $tenant"

                            sessionCheck.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
                            sessionCheck.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use sessionReader = sessionCheck.ExecuteReader()

                            if not (sessionReader.Read()) then
                                raise (
                                    SessionNotFoundException(
                                        sessionId,
                                        sprintf "No session %O exists in tenant %O." sessionId tenant
                                    )
                                )

                            let state = Enum.Parse<SessionState>(sessionReader.GetString(0), false)
                            sessionReader.Close()

                            if state = SessionState.Closed then
                                transaction.Rollback()
                                false
                            else
                                use pending = connection.CreateCommand()
                                pending.Transaction <- transaction

                                pending.CommandText <-
                                    $"SELECT position FROM \"%s{inboxTable ()}\" WHERE session_id = $session AND position = $pos"

                                pending.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                pending.Parameters.AddWithValue("$pos", position) |> ignore

                                use pendingReader = pending.ExecuteReader()

                                if not (pendingReader.Read()) then
                                    transaction.Rollback()
                                    false
                                else
                                    pendingReader.Close()
                                    use claimCheck = connection.CreateCommand()
                                    claimCheck.Transaction <- transaction

                                    claimCheck.CommandText <-
                                        $"SELECT session_id, claim_token, claim_expires_at, status FROM \"%s{turnsTable ()}\" WHERE turn_id = $turn AND tenant = $tenant"

                                    claimCheck.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                    claimCheck.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                                    use claimReader = claimCheck.ExecuteReader()

                                    if not (claimReader.Read()) then
                                        transaction.Rollback()
                                        false
                                    else
                                        let ownerSession = SessionId.Parse(claimReader.GetString(0))
                                        let tokenNull = claimReader.IsDBNull(1)
                                        let storedToken = if tokenNull then null else claimReader.GetString(1)
                                        let expiresAt = ofIso (claimReader.GetString(2))
                                        let status = Enum.Parse<TurnStatus>(claimReader.GetString(3), false)
                                        claimReader.Close()

                                        if ownerSession <> sessionId then
                                            transaction.Rollback()
                                            false
                                        elif tokenNull || storedToken <> claim.Token then
                                            transaction.Rollback()
                                            false
                                        elif expiresAt <= database.UtcNow then
                                            transaction.Rollback()
                                            false
                                        elif
                                            status = TurnStatus.Completed
                                            || status = TurnStatus.Aborted
                                            || status = TurnStatus.Failed
                                        then
                                            transaction.Rollback()
                                            false
                                        else
                                            use existing = connection.CreateCommand()
                                            existing.Transaction <- transaction

                                            existing.CommandText <-
                                                $"SELECT claim_token, claim_attempt FROM \"%s{settlementsTable ()}\" WHERE tenant = $tenant AND session_id = $session AND position = $pos"

                                            existing.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                            existing.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                            existing.Parameters.AddWithValue("$pos", position) |> ignore

                                            use existingReader = existing.ExecuteReader()

                                            if existingReader.Read() then
                                                let token = existingReader.GetString(0)
                                                let attempt = existingReader.GetInt32(1)
                                                transaction.Rollback()
                                                token = claim.Token && attempt = claim.Attempt
                                            else
                                                existingReader.Close()

                                                use insert = connection.CreateCommand()
                                                insert.Transaction <- transaction

                                                insert.CommandText <-
                                                    $"INSERT INTO \"%s{settlementsTable ()}\" (tenant, session_id, position, claim_turn_id, claim_token, claim_attempt, request_json, outcome_json) VALUES ($tenant, $session, $pos, $turn, $token, $attempt, NULL, NULL)"

                                                insert.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                                insert.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                                insert.Parameters.AddWithValue("$pos", position) |> ignore
                                                insert.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                                insert.Parameters.AddWithValue("$token", claim.Token) |> ignore
                                                insert.Parameters.AddWithValue("$attempt", claim.Attempt) |> ignore
                                                insert.ExecuteNonQuery() |> ignore
                                                transaction.Commit()
                                                true)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.SettleExecution(tenant, request, _) =
            task {
                if isNull (box request) then
                    raise (ArgumentNullException(nameof request))

                if isNull (box request.Claim) then
                    raise (ArgumentNullException(nameof request))

                if isNull (box request.Result) then
                    raise (ArgumentNullException(nameof request))

                if String.IsNullOrWhiteSpace request.CompletionKey then
                    raise (ArgumentException("The completion key must be a non-empty string.", nameof request))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            let sessionId = request.SessionId
                            use sessionCmd = connection.CreateCommand()
                            sessionCmd.Transaction <- transaction

                            sessionCmd.CommandText <-
                                $"SELECT state, options_json, permission_grants_json FROM \"%s{sessionsTable ()}\" WHERE id = $id AND tenant = $tenant"

                            sessionCmd.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
                            sessionCmd.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use sessionReader = sessionCmd.ExecuteReader()

                            if not (sessionReader.Read()) then
                                raise (
                                    SessionNotFoundException(
                                        sessionId,
                                        sprintf "No session %O exists in tenant %O." sessionId tenant
                                    )
                                )

                            let state = Enum.Parse<SessionState>(sessionReader.GetString(0), false)
                            let optionsJson = sessionReader.GetString(1)
                            sessionReader.Close()
                            let options = SessionOptionsPersistence.Deserialize optionsJson
                            let emptyEvents = Array.empty<SessionEvent> :> IReadOnlyList<SessionEvent>

                            let rejected () =
                                transaction.Rollback()

                                SessionSettlementOutcome(
                                    SessionSettlementStatus.Rejected,
                                    state,
                                    Unchecked.defaultof<TurnResult>,
                                    Unchecked.defaultof<SessionCompletion>,
                                    Unchecked.defaultof<InboxEntry>,
                                    emptyEvents,
                                    null
                                )

                            let fingerprint = fingerprintOf request
                            use receiptCheck = connection.CreateCommand()
                            receiptCheck.Transaction <- transaction

                            receiptCheck.CommandText <-
                                $"SELECT request_json, outcome_json FROM \"%s{settlementsTable ()}\" WHERE tenant = $tenant AND session_id = $session AND position = $pos"

                            receiptCheck.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            receiptCheck.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                            receiptCheck.Parameters.AddWithValue("$pos", request.Position) |> ignore

                            use receiptReader = receiptCheck.ExecuteReader()

                            if receiptReader.Read() && not (receiptReader.IsDBNull(1)) then
                                let prior =
                                    if receiptReader.IsDBNull(0) then
                                        null
                                    else
                                        receiptReader.GetString(0)

                                let outcomeJson = receiptReader.GetString(1)
                                receiptReader.Close()

                                if prior = fingerprint then
                                    let outcome = SqliteJson.deserialize<SessionSettlementOutcome> outcomeJson
                                    transaction.Rollback()

                                    SessionSettlementOutcome(
                                        SessionSettlementStatus.AlreadyApplied,
                                        outcome.State,
                                        outcome.Result,
                                        outcome.Completion,
                                        outcome.Following,
                                        emptyEvents,
                                        outcome.JournalReason
                                    )
                                else
                                    rejected ()
                            else
                                receiptReader.Close()

                                if state = SessionState.Closed then
                                    rejected ()
                                else
                                    let claim = request.Claim
                                    use admissionCheck = connection.CreateCommand()
                                    admissionCheck.Transaction <- transaction

                                    admissionCheck.CommandText <-
                                        $"SELECT claim_turn_id, claim_token, claim_attempt FROM \"%s{settlementsTable ()}\" WHERE tenant = $tenant AND session_id = $session AND position = $pos"

                                    admissionCheck.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                    admissionCheck.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                    admissionCheck.Parameters.AddWithValue("$pos", request.Position) |> ignore

                                    use admissionReader = admissionCheck.ExecuteReader()

                                    let admissionMatches =
                                        if admissionReader.Read() then
                                            admissionReader.GetString(0) = claim.TurnId.Value
                                            && admissionReader.GetString(1) = claim.Token
                                            && admissionReader.GetInt32(2) = claim.Attempt
                                        else
                                            false

                                    admissionReader.Close()
                                    use claimCheck = connection.CreateCommand()
                                    claimCheck.Transaction <- transaction

                                    claimCheck.CommandText <-
                                        $"SELECT session_id, claim_token, claim_expires_at, status FROM \"%s{turnsTable ()}\" WHERE turn_id = $turn AND tenant = $tenant"

                                    claimCheck.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                    claimCheck.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                                    use claimReader = claimCheck.ExecuteReader()

                                    let claimLive =
                                        if claimReader.Read() then
                                            let ownerSession = SessionId.Parse(claimReader.GetString(0))
                                            let tokenNull = claimReader.IsDBNull(1)
                                            let storedToken = if tokenNull then null else claimReader.GetString(1)
                                            let expiresAt = ofIso (claimReader.GetString(2))
                                            let status = Enum.Parse<TurnStatus>(claimReader.GetString(3), false)

                                            ownerSession = sessionId
                                            && not tokenNull
                                            && storedToken = claim.Token
                                            && expiresAt > database.UtcNow
                                            && status <> TurnStatus.Completed
                                            && status <> TurnStatus.Aborted
                                            && status <> TurnStatus.Failed
                                        else
                                            false

                                    claimReader.Close()

                                    if not (claimLive && admissionMatches) then
                                        rejected ()
                                    else
                                        let result = request.Result

                                        if not (isTerminalStatus result.Status) then
                                            raise (
                                                ArgumentException(
                                                    "Settlement requires a terminal result.",
                                                    nameof request
                                                )
                                            )

                                        use pendingCheck = connection.CreateCommand()
                                        pendingCheck.Transaction <- transaction

                                        pendingCheck.CommandText <-
                                            $"SELECT position FROM \"%s{inboxTable ()}\" WHERE session_id = $session AND position = $pos"

                                        pendingCheck.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                        pendingCheck.Parameters.AddWithValue("$pos", request.Position) |> ignore

                                        use pendingReader = pendingCheck.ExecuteReader()

                                        if not (pendingReader.Read()) then
                                            rejected ()
                                        else
                                            pendingReader.Close()
                                            let mutable events = emptyEvents
                                            let mutable journalReason: string | null = null

                                            match request.TerminalEvent with
                                            | null -> ()
                                            | terminalEvent ->
                                                use savepoint = connection.CreateCommand()
                                                savepoint.Transaction <- transaction
                                                savepoint.CommandText <- "SAVEPOINT sp_terminal"
                                                savepoint.ExecuteNonQuery() |> ignore

                                                try
                                                    use maxSeq = connection.CreateCommand()
                                                    maxSeq.Transaction <- transaction

                                                    maxSeq.CommandText <-
                                                        $"SELECT COALESCE(MAX(sequence), 0) FROM \"%s{eventsTable ()}\" WHERE session_id = $session"

                                                    maxSeq.Parameters.AddWithValue("$session", sessionId.Value)
                                                    |> ignore

                                                    let next = Convert.ToInt64(maxSeq.ExecuteScalar()) + 1L

                                                    let stamped: SessionEvent =
                                                        match terminalEvent with
                                                        | :? TurnCompletedEvent ->
                                                            TurnCompletedEvent(
                                                                terminalEvent.SessionId,
                                                                terminalEvent.TurnId,
                                                                Nullable next,
                                                                terminalEvent.Timestamp
                                                            )
                                                            :> SessionEvent
                                                        | :? TurnAbortedEvent as aborted ->
                                                            TurnAbortedEvent(
                                                                terminalEvent.SessionId,
                                                                terminalEvent.TurnId,
                                                                Nullable next,
                                                                terminalEvent.Timestamp,
                                                                aborted.Cause,
                                                                aborted.Reason
                                                            )
                                                            :> SessionEvent
                                                        | :? TurnFailedEvent as failed ->
                                                            TurnFailedEvent(
                                                                terminalEvent.SessionId,
                                                                terminalEvent.TurnId,
                                                                Nullable next,
                                                                terminalEvent.Timestamp,
                                                                failed.Reason
                                                            )
                                                            :> SessionEvent
                                                        | _ ->
                                                            raise (
                                                                ArgumentException(
                                                                    "Unsupported terminal event kind.",
                                                                    nameof request
                                                                )
                                                            )

                                                    use insertEvent = connection.CreateCommand()
                                                    insertEvent.Transaction <- transaction

                                                    insertEvent.CommandText <-
                                                        $"INSERT INTO \"%s{eventsTable ()}\" (session_id, sequence, tenant, turn_id, event_type, payload_json, timestamp) VALUES ($session, $seq, $tenant, $turn, $type, $payload, $at)"

                                                    insertEvent.Parameters.AddWithValue("$session", sessionId.Value)
                                                    |> ignore

                                                    insertEvent.Parameters.AddWithValue("$seq", next) |> ignore

                                                    insertEvent.Parameters.AddWithValue("$tenant", tenant.Value)
                                                    |> ignore

                                                    insertEvent.Parameters.AddWithValue("$turn", stamped.TurnId.Value)
                                                    |> ignore

                                                    insertEvent.Parameters.AddWithValue(
                                                        "$type",
                                                        stamped.GetType().Name
                                                    )
                                                    |> ignore

                                                    insertEvent.Parameters.AddWithValue(
                                                        "$payload",
                                                        SqliteJson.serialize stamped
                                                    )
                                                    |> ignore

                                                    insertEvent.Parameters.AddWithValue("$at", toIso stamped.Timestamp)
                                                    |> ignore

                                                    insertEvent.ExecuteNonQuery() |> ignore
                                                    use release = connection.CreateCommand()
                                                    release.Transaction <- transaction
                                                    release.CommandText <- "RELEASE sp_terminal"
                                                    release.ExecuteNonQuery() |> ignore
                                                    events <- [| stamped |] :> IReadOnlyList<SessionEvent>
                                                with _ ->
                                                    try
                                                        use rollback = connection.CreateCommand()
                                                        rollback.Transaction <- transaction

                                                        rollback.CommandText <-
                                                            "ROLLBACK TO sp_terminal; RELEASE sp_terminal"

                                                        rollback.ExecuteNonQuery() |> ignore
                                                    with _ ->
                                                        ()

                                                    if isNull journalReason then
                                                        journalReason <- "failed"

                                            use consume = connection.CreateCommand()
                                            consume.Transaction <- transaction

                                            consume.CommandText <-
                                                $"UPDATE \"%s{inboxTable ()}\" SET consumed = 1 WHERE session_id = $session AND position = $pos"

                                            consume.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                            consume.Parameters.AddWithValue("$pos", request.Position) |> ignore
                                            consume.ExecuteNonQuery() |> ignore
                                            use followingQuery = connection.CreateCommand()
                                            followingQuery.Transaction <- transaction

                                            followingQuery.CommandText <-
                                                $"SELECT position, payload_json, delivery_mode FROM \"%s{inboxTable ()}\" WHERE session_id = $session AND consumed = 0 ORDER BY position"

                                            followingQuery.Parameters.AddWithValue("$session", sessionId.Value)
                                            |> ignore

                                            use followingReader = followingQuery.ExecuteReader()
                                            let mutable interruptHead: int64 option = None
                                            let mutable queueHead: int64 option = None

                                            while followingReader.Read() do
                                                let position = followingReader.GetInt64(0)

                                                let payload =
                                                    SqliteJson.deserialize<InboxPayload> (followingReader.GetString(1))

                                                let delivery = parseDelivery (followingReader.GetString(2))

                                                match payload with
                                                | :? UserMessagePayload ->
                                                    match delivery with
                                                    | DeliveryMode.Interrupt when interruptHead.IsNone ->
                                                        interruptHead <- Some position
                                                    | DeliveryMode.Queue
                                                    | DeliveryMode.Inject when queueHead.IsNone ->
                                                        queueHead <- Some position
                                                    | _ -> ()
                                                | _ -> ()

                                            followingReader.Close()

                                            let readEntry (position: int64) : InboxEntry =
                                                use entryQuery = connection.CreateCommand()
                                                entryQuery.Transaction <- transaction

                                                entryQuery.CommandText <-
                                                    $"SELECT payload_json, delivery_mode, consumed, appended_at FROM \"%s{inboxTable ()}\" WHERE session_id = $session AND position = $pos"

                                                entryQuery.Parameters.AddWithValue("$session", sessionId.Value)
                                                |> ignore

                                                entryQuery.Parameters.AddWithValue("$pos", position) |> ignore

                                                use entryReader = entryQuery.ExecuteReader()
                                                entryReader.Read() |> ignore

                                                let payload =
                                                    SqliteJson.deserialize<InboxPayload> (entryReader.GetString(0))

                                                let delivery = parseDelivery (entryReader.GetString(1))
                                                let consumed = entryReader.GetInt64(2) <> 0L
                                                let appendedAt = ofIso (entryReader.GetString(3))

                                                {
                                                    SessionId = sessionId
                                                    Position = position
                                                    Payload = payload
                                                    Delivery = delivery
                                                    Consumed = consumed
                                                    AppendedAt = appendedAt
                                                }

                                            let following: InboxEntry | null =
                                                match interruptHead, queueHead with
                                                | Some position, _ -> readEntry position
                                                | None, Some position -> readEntry position
                                                | None, None -> Unchecked.defaultof<InboxEntry>

                                            let autoClose =
                                                result.Status = TurnStatus.Completed
                                                && not (isNull (box options))
                                                && options.AutoClose

                                            let nextState =
                                                if autoClose then SessionState.Closed
                                                elif not (isNull (box following)) then SessionState.Running
                                                else SessionState.Idle

                                            let completion: SessionCompletion | null =
                                                match options.CompletionDestinationId with
                                                | null -> Unchecked.defaultof<SessionCompletion>
                                                | destinationId when
                                                    not (CompletionDestinationRules.IsValid destinationId)
                                                    ->
                                                    Unchecked.defaultof<SessionCompletion>
                                                | destinationId ->
                                                    let payload =
                                                        {
                                                            SessionId = sessionId
                                                            TurnResult = result
                                                            Metadata = options.Metadata
                                                            IdempotencyKey = request.CompletionKey
                                                        }

                                                    use outboxCheck = connection.CreateCommand()
                                                    outboxCheck.Transaction <- transaction

                                                    outboxCheck.CommandText <-
                                                        $"SELECT completion_json FROM \"%s{outboxTable ()}\" WHERE idempotency_key = $key"

                                                    outboxCheck.Parameters.AddWithValue("$key", request.CompletionKey)
                                                    |> ignore

                                                    use outboxReader = outboxCheck.ExecuteReader()

                                                    if outboxReader.Read() then
                                                        SqliteJson.deserialize<SessionCompletion> (
                                                            outboxReader.GetString(0)
                                                        )
                                                    else
                                                        outboxReader.Close()
                                                        let now = database.UtcNow
                                                        let completionJson = SqliteJson.serialize payload
                                                        use outboxInsert = connection.CreateCommand()
                                                        outboxInsert.Transaction <- transaction

                                                        outboxInsert.CommandText <-
                                                            $"INSERT INTO \"%s{outboxTable ()}\" (idempotency_key, tenant, session_id, completion_json, created_at, delivered, delivered_at, lease_owner, lease_expires_at, destination_id) VALUES ($key, $tenant, $session, $payload, $at, 0, NULL, NULL, NULL, $dest)"

                                                        outboxInsert.Parameters.AddWithValue(
                                                            "$key",
                                                            request.CompletionKey
                                                        )
                                                        |> ignore

                                                        outboxInsert.Parameters.AddWithValue("$tenant", tenant.Value)
                                                        |> ignore

                                                        outboxInsert.Parameters.AddWithValue(
                                                            "$session",
                                                            sessionId.Value
                                                        )
                                                        |> ignore

                                                        outboxInsert.Parameters.AddWithValue(
                                                            "$payload",
                                                            completionJson
                                                        )
                                                        |> ignore

                                                        outboxInsert.Parameters.AddWithValue("$at", toIso now)
                                                        |> ignore

                                                        outboxInsert.Parameters.AddWithValue("$dest", destinationId)
                                                        |> ignore

                                                        outboxInsert.ExecuteNonQuery() |> ignore
                                                        payload

                                            use turnUpdate = connection.CreateCommand()
                                            turnUpdate.Transaction <- transaction

                                            turnUpdate.CommandText <-
                                                $"UPDATE \"%s{turnsTable ()}\" SET status = $status, outcome_json = $outcome, completed_at = $completed, claim_token = NULL, claim_owner = NULL, claim_expires_at = NULL WHERE turn_id = $turn"

                                            turnUpdate.Parameters.AddWithValue("$status", turnStatusName result.Status)
                                            |> ignore

                                            let outcomeValue =
                                                if isNull (box result.Outcome) then
                                                    box DBNull.Value
                                                else
                                                    box (SqliteJson.serialize result.Outcome)

                                            turnUpdate.Parameters.AddWithValue("$outcome", outcomeValue) |> ignore

                                            turnUpdate.Parameters.AddWithValue("$completed", toIso database.UtcNow)
                                            |> ignore

                                            turnUpdate.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                            turnUpdate.ExecuteNonQuery() |> ignore
                                            use clearCurrent = connection.CreateCommand()
                                            clearCurrent.Transaction <- transaction

                                            clearCurrent.CommandText <-
                                                $"UPDATE \"%s{sessionsTable ()}\" SET current_turn_id = NULL WHERE id = $id AND tenant = $tenant"

                                            clearCurrent.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
                                            clearCurrent.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                            clearCurrent.ExecuteNonQuery() |> ignore
                                            let now = database.UtcNow
                                            use sessionUpdate = connection.CreateCommand()
                                            sessionUpdate.Transaction <- transaction

                                            sessionUpdate.CommandText <-
                                                $"UPDATE \"%s{sessionsTable ()}\" SET state = $state, updated_at = $now, closed_at = $closed WHERE id = $id AND tenant = $tenant"

                                            sessionUpdate.Parameters.AddWithValue("$state", stateName nextState)
                                            |> ignore

                                            sessionUpdate.Parameters.AddWithValue("$now", toIso now) |> ignore

                                            sessionUpdate.Parameters.AddWithValue(
                                                "$closed",
                                                (if autoClose then box (toIso now) else box DBNull.Value)
                                            )
                                            |> ignore

                                            sessionUpdate.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
                                            sessionUpdate.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                            sessionUpdate.ExecuteNonQuery() |> ignore

                                            let outcome =
                                                SessionSettlementOutcome(
                                                    SessionSettlementStatus.Applied,
                                                    nextState,
                                                    result,
                                                    completion,
                                                    following,
                                                    events,
                                                    journalReason
                                                )

                                            let outcomeJson = SqliteJson.serialize outcome
                                            use receiptUpdate = connection.CreateCommand()
                                            receiptUpdate.Transaction <- transaction

                                            receiptUpdate.CommandText <-
                                                $"UPDATE \"%s{settlementsTable ()}\" SET request_json = $req, outcome_json = $out WHERE tenant = $tenant AND session_id = $session AND position = $pos"

                                            receiptUpdate.Parameters.AddWithValue("$req", fingerprint) |> ignore
                                            receiptUpdate.Parameters.AddWithValue("$out", outcomeJson) |> ignore
                                            receiptUpdate.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                                            receiptUpdate.Parameters.AddWithValue("$session", sessionId.Value)
                                            |> ignore

                                            receiptUpdate.Parameters.AddWithValue("$pos", request.Position) |> ignore
                                            receiptUpdate.ExecuteNonQuery() |> ignore
                                            transaction.Commit()
                                            outcome)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }
