// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage
open Npgsql
open PostgresSql

/// The PostgreSQL atomic terminal settlement capability (issue 363): validates captured claim
/// authority and commits terminal consumption, lifecycle and prime disposition, completion
/// deduplication and outbox, and settlement bookkeeping under one database-serialized
/// transaction with row locks, never only a process-local lock. The terminal journal append
/// rides the same transaction with savepoint isolation. A separate VerifyClaim followed by
/// unfenced writes is never called atomic.
type PostgresSessionSettlementStore(options: PostgresOptions, timeProvider: TimeProvider) =

    do
        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        if isNull (box timeProvider) then
            raise (ArgumentNullException(nameof timeProvider))

    /// Constructs the store with the system clock.
    /// <param name="options">The PostgreSQL options. Must not be null.</param>
    new(options: PostgresOptions) = PostgresSessionSettlementStore(options, TimeProvider.System)

    member private _.UtcNow = timeProvider.GetUtcNow()
    member private _.EnsureMigrated() = PostgresMigrations.ensure options
    member private _.SessionsTable = qualified options "sessions"
    member private _.InboxTable = qualified options "inbox"
    member private _.TurnsTable = qualified options "turns"
    member private _.OutboxTable = qualified options "outbox"
    member private _.EventsTable = qualified options "events"
    member private _.SettlementsTable = qualified options "execution_settlements"
    member private _.ControlTable = qualified options "session_control"

    member private _.Deserialize<'T>(json: string) : 'T =
        let boxed = JsonSerializer.Deserialize(json, typeof<'T>, jsonOptions)

        if isNull boxed then
            raise (JsonException "Storage JSON must not be null.")
        else
            unbox<'T> boxed

    /// Releases the prime turn row and current-turn tracking for a quiescent
    /// terminal settlement (issue 363): stamps the turn terminal and clears
    /// its claim plus the session's current turn. Queued chains and pending
    /// control retirements skip this and keep the prime: chains settle next
    /// under the same authority, and a pending control retirement keeps
    /// authority for the actor's retireControl with the existing prime settle
    /// releasing afterwards, exactly as before.
    member private this.ReleasePrime
        (
            connection: NpgsqlConnection,
            transaction: NpgsqlTransaction,
            claim: TurnClaim,
            result: TurnResult,
            tenant: TenantId,
            sessionId: SessionId
        ) =
        use turnUpdate =
            command
                connection
                transaction
                $"UPDATE {this.TurnsTable} SET status = @status, outcome_json = @outcome, completed_at = @completed, claim_token = NULL, claim_owner = NULL, claim_expires_at = NULL WHERE turn_id = @tid"

        textParam turnUpdate "status" (result.Status.ToString())

        let outcomeValue =
            if isNull (box result.Outcome) then
                null
            else
                serialize result.Outcome

        textParam turnUpdate "outcome" outcomeValue
        textParam turnUpdate "completed" (stamp this.UtcNow)
        textParam turnUpdate "tid" (claim.TurnId.ToString())
        turnUpdate.ExecuteNonQuery() |> ignore

        use clearCurrent =
            command
                connection
                transaction
                $"UPDATE {this.SessionsTable} SET current_turn_id = NULL WHERE id = @id AND tenant = @t"

        textParam clearCurrent "id" (sessionId.ToString())
        textParam clearCurrent "t" (tenant.ToString())
        clearCurrent.ExecuteNonQuery() |> ignore

    member private _.Fingerprint(request: SessionSettlementRequest) =
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

        member this.SupportsSettlementJournal(eventStore) =
            match eventStore with
            | :? PostgresSessionEventStore as journal ->
                let other = journal.SettlementOptions

                other.ConnectionString = options.ConnectionString
                && other.Schema = options.Schema
                && other.TablePrefix = options.TablePrefix
            | _ -> false

        member this.AdmitExecution(tenant, sessionId, position, claim, ct) =
            task {
                if isNull (box claim) then
                    raise (ArgumentNullException(nameof claim))

                this.EnsureMigrated()
                ct.ThrowIfCancellationRequested()

                return
                    transact options (fun connection transaction ->
                        ct.ThrowIfCancellationRequested()

                        use sessionCmd =
                            command
                                connection
                                transaction
                                $"SELECT state FROM {this.SessionsTable} WHERE id = @id AND tenant = @t FOR UPDATE"

                        textParam sessionCmd "id" (sessionId.ToString())
                        textParam sessionCmd "t" (tenant.ToString())

                        use sessionReader = sessionCmd.ExecuteReader()

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
                            false
                        else
                            use pending =
                                command
                                    connection
                                    transaction
                                    $"SELECT position FROM {this.InboxTable} WHERE session_id = @sid AND tenant = @t AND position = @pos FOR UPDATE"

                            textParam pending "sid" (sessionId.ToString())
                            textParam pending "t" (tenant.ToString())
                            longParam pending "pos" position

                            use pendingReader = pending.ExecuteReader()
                            let hasPending = pendingReader.Read()
                            pendingReader.Close()

                            if not hasPending then
                                false
                            else
                                use claimCmd =
                                    command
                                        connection
                                        transaction
                                        $"SELECT session_id, claim_token, claim_expires_at, status FROM {this.TurnsTable} WHERE turn_id = @tid AND tenant = @t FOR UPDATE"

                                textParam claimCmd "tid" (claim.TurnId.ToString())
                                textParam claimCmd "t" (tenant.ToString())

                                use claimReader = claimCmd.ExecuteReader()

                                if not (claimReader.Read()) then
                                    false
                                else
                                    let ownerSession = SessionId.Parse(claimReader.GetString(0))
                                    let tokenNull = claimReader.IsDBNull(1)
                                    let storedToken = getTextOrNull claimReader 1
                                    let expiresAt = parseStamp (claimReader.GetString(2))
                                    let status = Enum.Parse<TurnStatus>(claimReader.GetString(3), false)
                                    claimReader.Close()

                                    if
                                        ownerSession <> sessionId
                                        || tokenNull
                                        || storedToken <> claim.Token
                                        || expiresAt <= this.UtcNow
                                        || status = TurnStatus.Completed
                                        || status = TurnStatus.Aborted
                                        || status = TurnStatus.Failed
                                    then
                                        false
                                    else
                                        use existing =
                                            command
                                                connection
                                                transaction
                                                $"SELECT claim_token, claim_attempt FROM {this.SettlementsTable} WHERE tenant = @t AND session_id = @sid AND position = @pos FOR UPDATE"

                                        textParam existing "t" (tenant.ToString())
                                        textParam existing "sid" (sessionId.ToString())
                                        longParam existing "pos" position

                                        use existingReader = existing.ExecuteReader()

                                        if existingReader.Read() then
                                            let token = existingReader.GetString(0)
                                            let attempt = existingReader.GetInt32(1)
                                            token = claim.Token && attempt = claim.Attempt
                                        else
                                            existingReader.Close()

                                            use insert =
                                                command
                                                    connection
                                                    transaction
                                                    $"INSERT INTO {this.SettlementsTable} (tenant, session_id, position, claim_turn_id, claim_token, claim_attempt, request_json, outcome_json) VALUES (@t, @sid, @pos, @turn, @token, @attempt, NULL, NULL)"

                                            textParam insert "t" (tenant.ToString())
                                            textParam insert "sid" (sessionId.ToString())
                                            longParam insert "pos" position
                                            textParam insert "turn" (claim.TurnId.ToString())
                                            textParam insert "token" claim.Token
                                            intParam insert "attempt" claim.Attempt
                                            insert.ExecuteNonQuery() |> ignore
                                            true)
            }

        member this.SettleExecution(tenant, request, ct) =
            task {
                if isNull (box request) then
                    raise (ArgumentNullException(nameof request))

                if isNull (box request.Claim) then
                    raise (ArgumentNullException(nameof request))

                if isNull (box request.Result) then
                    raise (ArgumentNullException(nameof request))

                if String.IsNullOrWhiteSpace request.CompletionKey then
                    raise (ArgumentException("The completion key must be a non-empty string.", nameof request))

                this.EnsureMigrated()
                ct.ThrowIfCancellationRequested()

                return
                    transact options (fun connection transaction ->
                        ct.ThrowIfCancellationRequested()
                        let sessionId = request.SessionId

                        use sessionCmd =
                            command
                                connection
                                transaction
                                $"SELECT state, options_json FROM {this.SessionsTable} WHERE id = @id AND tenant = @t FOR UPDATE"

                        textParam sessionCmd "id" (sessionId.ToString())
                        textParam sessionCmd "t" (tenant.ToString())

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
                        let fingerprint = this.Fingerprint request

                        use receiptCheck =
                            command
                                connection
                                transaction
                                $"SELECT request_json, outcome_json FROM {this.SettlementsTable} WHERE tenant = @t AND session_id = @sid AND position = @pos FOR UPDATE"

                        textParam receiptCheck "t" (tenant.ToString())
                        textParam receiptCheck "sid" (sessionId.ToString())
                        longParam receiptCheck "pos" request.Position

                        use receiptReader = receiptCheck.ExecuteReader()

                        if receiptReader.Read() && not (receiptReader.IsDBNull(1)) then
                            let prior = getTextOrNull receiptReader 0
                            let outcomeJson = receiptReader.GetString(1)
                            receiptReader.Close()
                            let outcome = this.Deserialize<SessionSettlementOutcome>(outcomeJson)

                            if prior = fingerprint then
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
                                SessionSettlementOutcome(
                                    SessionSettlementStatus.Rejected,
                                    state,
                                    Unchecked.defaultof<TurnResult>,
                                    Unchecked.defaultof<SessionCompletion>,
                                    Unchecked.defaultof<InboxEntry>,
                                    emptyEvents,
                                    null
                                )
                        else
                            receiptReader.Close()

                            if state = SessionState.Closed then
                                SessionSettlementOutcome(
                                    SessionSettlementStatus.Rejected,
                                    state,
                                    Unchecked.defaultof<TurnResult>,
                                    Unchecked.defaultof<SessionCompletion>,
                                    Unchecked.defaultof<InboxEntry>,
                                    emptyEvents,
                                    null
                                )
                            else
                                let claim = request.Claim

                                use admissionCheck =
                                    command
                                        connection
                                        transaction
                                        $"SELECT claim_turn_id, claim_token, claim_attempt FROM {this.SettlementsTable} WHERE tenant = @t AND session_id = @sid AND position = @pos FOR UPDATE"

                                textParam admissionCheck "t" (tenant.ToString())
                                textParam admissionCheck "sid" (sessionId.ToString())
                                longParam admissionCheck "pos" request.Position

                                use admissionReader = admissionCheck.ExecuteReader()

                                let admissionMatches =
                                    if admissionReader.Read() then
                                        admissionReader.GetString(0) = claim.TurnId.ToString()
                                        && admissionReader.GetString(1) = claim.Token
                                        && admissionReader.GetInt32(2) = claim.Attempt
                                    else
                                        false

                                admissionReader.Close()

                                use claimCheck =
                                    command
                                        connection
                                        transaction
                                        $"SELECT session_id, claim_token, claim_expires_at, status FROM {this.TurnsTable} WHERE turn_id = @tid AND tenant = @t FOR UPDATE"

                                textParam claimCheck "tid" (claim.TurnId.ToString())
                                textParam claimCheck "t" (tenant.ToString())

                                use claimReader = claimCheck.ExecuteReader()

                                let claimLive =
                                    if claimReader.Read() then
                                        let ownerSession = SessionId.Parse(claimReader.GetString(0))
                                        let tokenNull = claimReader.IsDBNull(1)
                                        let storedToken = getTextOrNull claimReader 1
                                        let expiresAt = parseStamp (claimReader.GetString(2))
                                        let status = Enum.Parse<TurnStatus>(claimReader.GetString(3), false)

                                        ownerSession = sessionId
                                        && not tokenNull
                                        && storedToken = claim.Token
                                        && expiresAt > this.UtcNow
                                        && status <> TurnStatus.Completed
                                        && status <> TurnStatus.Aborted
                                        && status <> TurnStatus.Failed
                                    else
                                        false

                                claimReader.Close()

                                if not (claimLive && admissionMatches) then
                                    SessionSettlementOutcome(
                                        SessionSettlementStatus.Rejected,
                                        state,
                                        Unchecked.defaultof<TurnResult>,
                                        Unchecked.defaultof<SessionCompletion>,
                                        Unchecked.defaultof<InboxEntry>,
                                        emptyEvents,
                                        null
                                    )
                                else
                                    let result = request.Result

                                    if
                                        result.Status <> TurnStatus.Completed
                                        && result.Status <> TurnStatus.Aborted
                                        && result.Status <> TurnStatus.Failed
                                    then
                                        raise (
                                            ArgumentException("Settlement requires a terminal result.", nameof request)
                                        )

                                    use pendingCheck =
                                        command
                                            connection
                                            transaction
                                            $"SELECT position FROM {this.InboxTable} WHERE session_id = @sid AND tenant = @t AND position = @pos FOR UPDATE"

                                    textParam pendingCheck "sid" (sessionId.ToString())
                                    textParam pendingCheck "t" (tenant.ToString())
                                    longParam pendingCheck "pos" request.Position

                                    use pendingReader = pendingCheck.ExecuteReader()
                                    let hasPending = pendingReader.Read()
                                    pendingReader.Close()

                                    if not hasPending then
                                        SessionSettlementOutcome(
                                            SessionSettlementStatus.Rejected,
                                            state,
                                            Unchecked.defaultof<TurnResult>,
                                            Unchecked.defaultof<SessionCompletion>,
                                            Unchecked.defaultof<InboxEntry>,
                                            emptyEvents,
                                            null
                                        )
                                    else
                                        let mutable events = emptyEvents
                                        let mutable journalReason: string | null = null

                                        match request.TerminalEvent with
                                        | null -> ()
                                        | terminalEvent ->
                                            use savepoint = command connection transaction "SAVEPOINT sp_terminal"
                                            savepoint.ExecuteNonQuery() |> ignore

                                            try
                                                use maxSeq =
                                                    command
                                                        connection
                                                        transaction
                                                        $"SELECT COALESCE(MAX(sequence), 0) FROM {this.EventsTable} WHERE session_id = @sid AND tenant = @t"

                                                textParam maxSeq "sid" (sessionId.ToString())
                                                textParam maxSeq "t" (tenant.ToString())
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

                                                use insertEvent =
                                                    command
                                                        connection
                                                        transaction
                                                        $"INSERT INTO {this.EventsTable} (session_id, sequence, tenant, turn_id, event_type, payload_json, timestamp) VALUES (@sid, @seq, @t, @tid, @kind, @payload, @ts)"

                                                textParam insertEvent "sid" (sessionId.ToString())
                                                longParam insertEvent "seq" next
                                                textParam insertEvent "t" (tenant.ToString())
                                                textParam insertEvent "tid" (stamped.TurnId.ToString())
                                                textParam insertEvent "kind" (stamped.GetType().Name)
                                                textParam insertEvent "payload" (serialize stamped)
                                                textParam insertEvent "ts" (stamp stamped.Timestamp)
                                                insertEvent.ExecuteNonQuery() |> ignore
                                                use release = command connection transaction "RELEASE sp_terminal"
                                                release.ExecuteNonQuery() |> ignore
                                                events <- [| stamped |] :> IReadOnlyList<SessionEvent>
                                            with _ ->
                                                try
                                                    use rollback =
                                                        command
                                                            connection
                                                            transaction
                                                            "ROLLBACK TO sp_terminal; RELEASE sp_terminal"

                                                    rollback.ExecuteNonQuery() |> ignore
                                                with _ ->
                                                    ()

                                                if isNull journalReason then
                                                    journalReason <- "failed"

                                        use consume =
                                            command
                                                connection
                                                transaction
                                                $"UPDATE {this.InboxTable} SET consumed = TRUE WHERE session_id = @sid AND tenant = @t AND position = @pos"

                                        textParam consume "sid" (sessionId.ToString())
                                        textParam consume "t" (tenant.ToString())
                                        longParam consume "pos" request.Position
                                        consume.ExecuteNonQuery() |> ignore

                                        // A decided-but-unretired control verdict still owns
                                        // the entry (issue 363 plus #393): the actor's
                                        // retireControl must run before the prime is
                                        // released, so the quiescent release below defers
                                        // while this holds. Missing state reads as
                                        // nothing pending.
                                        let controlPending =
                                            try
                                                use controlCmd =
                                                    command
                                                        connection
                                                        transaction
                                                        $"SELECT control_json FROM {this.ControlTable} WHERE tenant = @t AND session_id = @sid"

                                                textParam controlCmd "t" (tenant.ToString())
                                                textParam controlCmd "sid" (sessionId.ToString())

                                                use controlReader = controlCmd.ExecuteReader()

                                                if controlReader.Read() then
                                                    let json = controlReader.GetString(0)
                                                    controlReader.Close()

                                                    try
                                                        let state = ControlTargetProtocol.decode sessionId json

                                                        ControlTargetProtocol.retirementPendingFor
                                                            state
                                                            request.Position
                                                    with _ ->
                                                        false
                                                else
                                                    controlReader.Close()
                                                    false
                                            with _ ->
                                                false

                                        use followingQuery =
                                            command
                                                connection
                                                transaction
                                                $"SELECT position, payload_json, delivery_mode FROM {this.InboxTable} WHERE session_id = @sid AND tenant = @t AND consumed = FALSE ORDER BY position"

                                        textParam followingQuery "sid" (sessionId.ToString())
                                        textParam followingQuery "t" (tenant.ToString())

                                        use followingReader = followingQuery.ExecuteReader()
                                        let mutable interruptHead: int64 option = None
                                        let mutable queueHead: int64 option = None

                                        while followingReader.Read() do
                                            let position = followingReader.GetInt64(0)
                                            let payload = this.Deserialize<InboxPayload>(followingReader.GetString(1))

                                            let delivery =
                                                Enum.Parse<DeliveryMode>(followingReader.GetString(2), false)

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
                                            use entryQuery =
                                                command
                                                    connection
                                                    transaction
                                                    $"SELECT payload_json, delivery_mode, consumed, appended_at, turn_id FROM {this.InboxTable} WHERE session_id = @sid AND tenant = @t AND position = @pos"

                                            textParam entryQuery "sid" (sessionId.ToString())
                                            textParam entryQuery "t" (tenant.ToString())
                                            longParam entryQuery "pos" position

                                            use entryReader = entryQuery.ExecuteReader()
                                            entryReader.Read() |> ignore
                                            let payload = this.Deserialize<InboxPayload>(entryReader.GetString(0))
                                            let delivery = Enum.Parse<DeliveryMode>(entryReader.GetString(1), false)
                                            let consumed = entryReader.GetBoolean(2)
                                            let appendedAt = parseStamp (entryReader.GetString(3))
                                            let turnText: string | null = getTextOrNull entryReader 4

                                            {
                                                SessionId = sessionId
                                                Position = position
                                                Payload = payload
                                                Delivery = delivery
                                                Consumed = consumed
                                                AppendedAt = appendedAt
                                                TurnId =
                                                    match turnText with
                                                    | null -> Unchecked.defaultof<TurnId>
                                                    | text when String.IsNullOrWhiteSpace(text) ->
                                                        Unchecked.defaultof<TurnId>
                                                    | text -> TurnId.Parse(text)
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
                                            | destinationId when not (CompletionDestinationRules.IsValid destinationId) ->
                                                Unchecked.defaultof<SessionCompletion>
                                            | destinationId ->
                                                let payload =
                                                    {
                                                        SessionId = sessionId
                                                        TurnResult = result
                                                        Metadata = options.Metadata
                                                        IdempotencyKey = request.CompletionKey
                                                    }

                                                use outboxCheck =
                                                    command
                                                        connection
                                                        transaction
                                                        $"SELECT completion_json FROM {this.OutboxTable} WHERE idempotency_key = @key"

                                                textParam outboxCheck "key" request.CompletionKey

                                                use outboxReader = outboxCheck.ExecuteReader()

                                                if outboxReader.Read() then
                                                    this.Deserialize<SessionCompletion>(outboxReader.GetString(0))
                                                else
                                                    outboxReader.Close()
                                                    let now = this.UtcNow
                                                    let completionJson = serialize payload

                                                    use outboxInsert =
                                                        command
                                                            connection
                                                            transaction
                                                            $"INSERT INTO {this.OutboxTable} (idempotency_key, tenant, session_id, completion_json, created_at, delivered, delivered_at, lease_owner, lease_expires_at, destination_id) VALUES (@key, @t, @sid, @payload, @at, FALSE, NULL, NULL, NULL, @dest)"

                                                    textParam outboxInsert "key" request.CompletionKey
                                                    textParam outboxInsert "t" (tenant.ToString())
                                                    textParam outboxInsert "sid" (sessionId.ToString())
                                                    textParam outboxInsert "payload" completionJson
                                                    textParam outboxInsert "at" (stamp now)
                                                    textParam outboxInsert "dest" destinationId
                                                    // Named parameters above use explicit names; Npgsql matches by name.
                                                    outboxInsert.ExecuteNonQuery() |> ignore
                                                    payload

                                        // Prime release (issue 363): the turn row is stamped
                                        // terminal and its claim plus the session's
                                        // current turn clear only at quiescence with no
                                        // control retirement pending. Queued chains keep
                                        // the prime for the next admit and settle; a
                                        // pending control retirement keeps the row
                                        // pristine for the actor's retireControl and the
                                        // existing prime settle, exactly as before. (A
                                        // stamped-terminal row resolves as Absent, so a
                                        // partial stamp would strand the prime.)
                                        if nextState <> SessionState.Running && not controlPending then
                                            this.ReleasePrime(
                                                connection,
                                                transaction,
                                                claim,
                                                result,
                                                tenant,
                                                sessionId
                                            )

                                        let now = this.UtcNow

                                        use sessionUpdate =
                                            command
                                                connection
                                                transaction
                                                $"UPDATE {this.SessionsTable} SET state = @state, updated_at = @now, closed_at = @closed WHERE id = @id AND tenant = @t"

                                        textParam sessionUpdate "state" (nextState.ToString())
                                        textParam sessionUpdate "now" (stamp now)

                                        if autoClose then
                                            textParam sessionUpdate "closed" (stamp now)
                                        else
                                            textParam sessionUpdate "closed" null

                                        textParam sessionUpdate "id" (sessionId.ToString())
                                        textParam sessionUpdate "t" (tenant.ToString())
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

                                        let outcomeJson = serialize outcome

                                        use receiptUpdate =
                                            command
                                                connection
                                                transaction
                                                $"UPDATE {this.SettlementsTable} SET request_json = @req, outcome_json = @out WHERE tenant = @t AND session_id = @sid AND position = @pos"

                                        textParam receiptUpdate "req" fingerprint
                                        textParam receiptUpdate "out" outcomeJson
                                        textParam receiptUpdate "t" (tenant.ToString())
                                        textParam receiptUpdate "sid" (sessionId.ToString())
                                        longParam receiptUpdate "pos" request.Position
                                        receiptUpdate.ExecuteNonQuery() |> ignore
                                        outcome)
            }
