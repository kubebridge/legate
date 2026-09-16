// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres

open System
open System.Collections.Generic
open System.Data.Common
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Legate
open Npgsql
open PostgresSql

// The PostgreSQL ISessionStore: session CRUD, the inbox, turn claims under
// a lease with the fencing semantics the contract pins, dispatch
// candidates, and the capacity counts. One live claim per session and one
// open turn per session, exactly like the in-memory reference: ClaimNextTurn
// consumes the head pending user message into a new turn, or the head
// pending reply into a resume of the open turn with the attempt incremented;
// a live unexpired claim makes every further claim the missing branch. Every
// call runs in one transaction with the session row locked first, so claims
// and settlements are atomic under concurrent callers and a takeover race
// leaves the loser with zero effects. Lease expiry reads the injected clock,
// passed as a statement parameter, so deterministic test clocks drive lease
// states. The stored row's current_turn_id is stamped on claim and cleared
// on settlement, the basis of the CurrentTurnId-based state rules. Every
// query carries the tenant predicate: isolation is enforced here, not only
// in the host.
type PostgresSessionStore(options: PostgresOptions, timeProvider: TimeProvider) =

    do
        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        if isNull (box timeProvider) then
            raise (ArgumentNullException(nameof timeProvider))

    /// Constructs the store with the system clock.
    /// <param name="options">The PostgreSQL options. Must not be null.</param>
    new(options: PostgresOptions) = PostgresSessionStore(options, TimeProvider.System)

    /// The options the store was constructed with.
    member private _.Options = options

    /// The clock lease expiry reads.
    member private _.TimeProvider = timeProvider

    // ── Private helpers ──

    member private _.UtcNow = timeProvider.GetUtcNow()

    member private _.EnsureMigrated() = PostgresMigrations.ensure options

    member private this.SessionsTable = qualified options "sessions"
    member private this.InboxTable = qualified options "inbox"
    member private _.TurnsTable = qualified options "turns"
    member private this.OutboxTable = qualified options "outbox"

    member private _.SessionColumns =
        "id, tenant, agent_id, title, state, current_turn_id, created_at, updated_at, closed_at, workspace_binding, options_json, permission_grants_json"

    member private _.OutboxColumns =
        "idempotency_key, tenant, session_id, completion_json, created_at, delivered, delivered_at, lease_owner, lease_expires_at"

    /// Statuses a turn is still said to be open under, as the quoted
    /// filter text for IN lists: anything not terminal.
    member private _.OpenStatusFilter = "'Pending','Running','Suspended'"

    /// The stored grant list, normalised: a null deserialised value reads
    /// as empty, and duplicates collapse, so every stored row carries a
    /// distinct, non-null grant set.
    member private _.StoredGrants(grants: IReadOnlyList<string>) : IReadOnlyList<string> =
        if isNull (box grants) then
            ResizeArray<string>() :> IReadOnlyList<string>
        else
            let distinct = ResizeArray<string>()

            for grant in grants do
                if not (isNull (box grant)) && not (distinct.Contains grant) then
                    distinct.Add grant

            distinct :> IReadOnlyList<string>

    /// Reads one session row in SessionColumns order. The reader is
    /// positioned on the row; values are materialised before it closes.
    member private this.ReadSession(reader: DbDataReader) : Session =
        let closedText: string | null = getTextOrNull reader 8
        let binding: string | null = getTextOrNull reader 9
        let optionsText = reader.GetString(10)
        let grantsText: string | null = getTextOrNull reader 11
        let currentText: string | null = getTextOrNull reader 5

        let grants: IReadOnlyList<string> =
            match grantsText with
            | null
            | "null" -> ResizeArray<string>() :> IReadOnlyList<string>
            | text ->
                let items: List<string> =
                    match JsonSerializer.Deserialize(text, jsonOptions) with
                    | null -> raise (InvalidOperationException("The stored permission grants are null."))
                    | decoded -> decoded

                this.StoredGrants(items :> IReadOnlyList<string>)

        let optionsValue: SessionOptions =
            if optionsText = "null" then
                // A host that persisted a null options snapshot reads back
                // as default options; the store itself always writes
                // non-null.
                SessionOptions()
            else
                match JsonSerializer.Deserialize(optionsText, jsonOptions) with
                | null -> raise (InvalidOperationException("The stored session options are null."))
                | decoded -> decoded

        {
            Id = SessionId.Parse(reader.GetString(0))
            Tenant = TenantId.Create(reader.GetString(1))
            AgentId = AgentId.Parse(reader.GetString(2))
            Title = reader.GetString(3)
            State = Enum.Parse<SessionState>(reader.GetString(4))
            CurrentTurnId =
                match currentText with
                | null -> Nullable()
                | text -> Nullable(TurnId.Parse(text))
            CreatedAt = parseStamp (reader.GetString(6))
            UpdatedAt = parseStamp (reader.GetString(7))
            ClosedAt =
                match closedText with
                | null -> Nullable()
                | text -> Nullable(parseStamp text)
            WorkspaceBinding = binding
            Options = optionsValue
            PermissionGrants = grants
        }

    /// Reads one outbox row in OutboxColumns order.
    member private _.ReadOutboxEntry(reader: DbDataReader) : CompletionOutboxEntry =
        let deliveredText: string | null = getTextOrNull reader 6
        let leaseOwner: string | null = getTextOrNull reader 7
        let leaseExpires: string | null = getTextOrNull reader 8

        let completion: SessionCompletion =
            match JsonSerializer.Deserialize(reader.GetString(3), jsonOptions) with
            | null -> raise (InvalidOperationException("The stored outbox completion is null."))
            | decoded -> decoded

        {
            Tenant = TenantId.Create(reader.GetString(1))
            SessionId = SessionId.Parse(reader.GetString(2))
            IdempotencyKey = reader.GetString(0)
            Completion = completion
            CreatedAt = parseStamp (reader.GetString(4))
            Delivered = reader.GetBoolean(5)
            DeliveredAt =
                match deliveredText with
                | null -> Nullable()
                | text -> Nullable(parseStamp text)
            LeaseOwner = leaseOwner
            LeaseExpiresAt =
                match leaseExpires with
                | null -> Nullable()
                | text -> Nullable(parseStamp text)
        }

    /// The stable ordering token ListSessions pages on: the (updatedAt, id)
    /// pair rendered exactly like the in-memory reference, so paging
    /// semantics match.
    member private _.PageToken(session: Session) =
        sprintf "%s|%O" (session.UpdatedAt.ToString "O") session.Id

    /// One open turn fence row as the resolve logic reads it.
    member private _.ReadTurnFence(reader: DbDataReader) =
        {|
            TurnId = TurnId.Parse(reader.GetString(0))
            SessionId = SessionId.Parse(reader.GetString(1))
            Attempt = reader.GetInt32(2)
            Status = Enum.Parse<TurnStatus>(reader.GetString(3))
            Token = getTextOrNull reader 4
            Owner = getTextOrNull reader 5
            ExpiresText = getTextOrNull reader 6
            CompletedText = getTextOrNull reader 7
        |}

    /// What a fenced call against the claim's turn resolved to: the live
    /// claim the token still matches with its session id, an open turn the
    /// token no longer owns (taken over), or nothing (settled or unknown).
    /// The caller runs inside the claim's transaction with the turn row
    /// locked; expiry is applied by the caller (renew, verify, and
    /// checkpoint reject a lapsed lease, settle and abort ignore it).
    member private this.ResolveClaim
        (
            connection: NpgsqlConnection,
            transaction: NpgsqlTransaction,
            tenant: TenantId,
            claim: TurnClaim,
            nowText: string
        ) : Choice<(SessionId * TurnClaim), unit, unit> =
        use cmd =
            command
                connection
                transaction
                $"SELECT turn_id, session_id, attempt, status, claim_token, claim_owner, claim_expires_at, completed_at FROM {this.TurnsTable} WHERE turn_id = @tid AND tenant = @t FOR UPDATE"

        textParam cmd "tid" (claim.TurnId.ToString())
        textParam cmd "t" (tenant.ToString())

        use reader = cmd.ExecuteReader()

        if not (reader.Read()) then
            reader.Close()
            Choice3Of3()
        else
            let row = this.ReadTurnFence(reader)
            reader.Close()

            match Option.ofObj row.Token, Option.ofObj row.Owner, Option.ofObj row.ExpiresText with
            | Some token, Some owner, Some expiresText when String.Equals(token, claim.Token, StringComparison.Ordinal) ->
                let live =
                    {
                        TurnId = row.TurnId
                        Token = token
                        Owner = owner
                        ExpiresAt = parseStamp expiresText
                        Attempt = row.Attempt
                    }

                Choice1Of3(row.SessionId, live)
            | _ ->
                use liveCmd =
                    command
                        connection
                        transaction
                        $"SELECT turn_id FROM {this.TurnsTable} WHERE session_id = @sid AND tenant = @t AND claim_token IS NOT NULL AND claim_expires_at > @now AND status IN ({this.OpenStatusFilter}) LIMIT 1"

                textParam liveCmd "sid" (row.SessionId.ToString())
                textParam liveCmd "t" (tenant.ToString())
                textParam liveCmd "now" nowText

                use liveReader = liveCmd.ExecuteReader()

                let takenOver =
                    liveReader.Read() && (TurnId.Parse(liveReader.GetString(0))).Equals claim.TurnId

                liveReader.Close()

                if takenOver then Choice2Of3() else Choice3Of3()

    /// Applies a terminal settlement to an open turn row: records the
    /// outcome shape, stamps completion, releases the fence, and clears the
    /// session's current turn. The caller holds the turn row lock.
    member private this.ApplySettlement
        (
            connection: NpgsqlConnection,
            transaction: NpgsqlTransaction,
            tenant: TenantId,
            sessionId: SessionId,
            claim: TurnClaim,
            status: TurnStatus,
            outcome: TurnOutcome | null,
            nowText: string
        ) =
        let (errorText: string | null, stopText: string | null) =
            match outcome with
            | null -> null, null
            | :? TurnFinished -> null, null
            | :? TurnPartiallyFinished as partial -> partial.Summary, null
            | :? TurnAborted as aborted -> aborted.Reason, aborted.Cause.ToString()
            | :? TurnFailed as failed -> failed.Reason, null
            | decoded -> failwith $"Unexpected turn outcome: %s{decoded.GetType().FullName}"

        let outcomeText: string | null =
            match outcome with
            | null -> null
            | decoded -> serialize<TurnOutcome> decoded

        use settleCmd =
            command
                connection
                transaction
                $"UPDATE {this.TurnsTable} SET status = @status, completed_at = @completed, error = @error, stop_cause = @stop, outcome_json = @outcome, claim_token = NULL, claim_owner = NULL, claim_expires_at = NULL WHERE turn_id = @tid AND tenant = @t"

        textParam settleCmd "status" (status.ToString())
        textParam settleCmd "completed" nowText
        textParam settleCmd "error" errorText
        textParam settleCmd "stop" stopText
        textParam settleCmd "outcome" outcomeText
        textParam settleCmd "tid" (claim.TurnId.ToString())
        textParam settleCmd "t" (tenant.ToString())
        settleCmd.ExecuteNonQuery() |> ignore

        use clearCmd =
            command
                connection
                transaction
                $"UPDATE {this.SessionsTable} SET current_turn_id = NULL, updated_at = @now WHERE id = @sid AND tenant = @t"

        textParam clearCmd "now" nowText
        textParam clearCmd "sid" (sessionId.ToString())
        textParam clearCmd "t" (tenant.ToString())
        clearCmd.ExecuteNonQuery() |> ignore

    /// Locks the session row, throwing when the id does not exist in the
    /// tenant. Serialises every multi-step transition on the session.
    member private this.RequireSession
        (connection: NpgsqlConnection, transaction: NpgsqlTransaction, tenant: TenantId, sessionId: SessionId)
        =
        use cmd =
            command
                connection
                transaction
                $"SELECT id FROM {this.SessionsTable} WHERE id = @id AND tenant = @t FOR UPDATE"

        textParam cmd "id" (sessionId.ToString())
        textParam cmd "t" (tenant.ToString())

        use reader = cmd.ExecuteReader()
        let found = reader.Read()
        reader.Close()

        if not found then
            raise (SessionNotFoundException(sessionId, sprintf "No session %O exists in tenant %O." sessionId tenant))

    /// Reads the stored session row after a write in the same transaction.
    member private this.ReadStoredSession
        (connection: NpgsqlConnection, transaction: NpgsqlTransaction, tenant: TenantId, sessionId: SessionId)
        : Session =
        use cmd =
            command
                connection
                transaction
                $"SELECT {this.SessionColumns} FROM {this.SessionsTable} WHERE id = @id AND tenant = @t"

        textParam cmd "id" (sessionId.ToString())
        textParam cmd "t" (tenant.ToString())

        use reader = cmd.ExecuteReader()
        reader.Read() |> ignore
        let session = this.ReadSession(reader)
        reader.Close()
        session

    /// Reads one outbox row by key in the same transaction.
    member private this.ReadOutboxByKey
        (connection: NpgsqlConnection, transaction: NpgsqlTransaction, tenantText: string, key: string)
        : CompletionOutboxEntry =
        use readCmd =
            command
                connection
                transaction
                $"SELECT {this.OutboxColumns} FROM {this.OutboxTable} WHERE tenant = @t AND idempotency_key = @key"

        textParam readCmd "t" tenantText
        textParam readCmd "key" key

        use reader = readCmd.ExecuteReader()
        reader.Read() |> ignore
        let entry = this.ReadOutboxEntry(reader)
        reader.Close()
        entry

    interface ISessionStore with

        member this.CreateSession(tenant, session, _) =
            if isNull (box session) then
                raise (ArgumentNullException(nameof session))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use existsCmd =
                    command
                        connection
                        transaction
                        $"SELECT id FROM {this.SessionsTable} WHERE id = @id AND tenant = @t"

                textParam existsCmd "id" (session.Id.ToString())
                textParam existsCmd "t" (tenant.ToString())

                use existsReader = existsCmd.ExecuteReader()
                let duplicate = existsReader.Read()
                existsReader.Close()

                if duplicate then
                    raise (
                        InvalidSessionStateException(
                            session.Id,
                            nameof SessionState,
                            sprintf "A session %O already exists in tenant %O." session.Id tenant
                        )
                    )

                let now = this.UtcNow
                let nowText = stamp now
                let grants = this.StoredGrants(session.PermissionGrants)

                let stored =
                    { session with
                        Tenant = tenant
                        State = SessionState.Idle
                        CurrentTurnId = Nullable()
                        CreatedAt = now
                        UpdatedAt = now
                        ClosedAt = Nullable()
                        PermissionGrants = grants
                    }

                use insertCmd =
                    command
                        connection
                        transaction
                        $"INSERT INTO {this.SessionsTable} (id, tenant, agent_id, title, state, current_turn_id, created_at, updated_at, closed_at, workspace_binding, options_json, permission_grants_json) VALUES (@id, @t, @agent, @title, @state, NULL, @created, @updated, NULL, @binding, @options, @grants)"

                textParam insertCmd "id" (stored.Id.ToString())
                textParam insertCmd "t" (tenant.ToString())
                textParam insertCmd "agent" (stored.AgentId.ToString())
                textParam insertCmd "title" stored.Title
                textParam insertCmd "state" (stored.State.ToString())
                textParam insertCmd "created" nowText
                textParam insertCmd "updated" nowText
                textParam insertCmd "binding" stored.WorkspaceBinding
                textParam insertCmd "options" (serialize<SessionOptions> stored.Options)
                textParam insertCmd "grants" (serialize<List<string>> (List<string>(grants)))
                insertCmd.ExecuteNonQuery() |> ignore

                stored)
            |> Task.FromResult

        member this.GetSession(tenant, sessionId, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.SessionColumns} FROM {this.SessionsTable} WHERE id = @id AND tenant = @t"

                textParam cmd "id" (sessionId.ToString())
                textParam cmd "t" (tenant.ToString())

                use reader = cmd.ExecuteReader()

                let row =
                    if reader.Read() then
                        Some(this.ReadSession(reader))
                    else
                        None

                reader.Close()
                row |> Option.toObj)
            |> Task.FromResult

        member this.ListSessions(tenant, state, pageSize, continuation, _) =
            if pageSize <= 0 then
                raise (ArgumentOutOfRangeException(nameof pageSize, "The page size must be positive."))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                let stateText: string | null =
                    if state.HasValue then state.Value.ToString() else null

                // A continuation the store never minted resolves to an
                // empty page, matching the in-memory reference.
                let (continuationClause: string,
                     continuationTs: string | null,
                     continuationId: string | null,
                     bogus: bool) =
                    match continuation with
                    | null -> "", null, null, false
                    | token ->
                        match token.Split('|') with
                        | [| ts; id |] ->
                            try
                                parseStamp ts |> ignore
                                SessionId.Parse(id) |> ignore
                                "AND (updated_at < @cts OR (updated_at = @cts AND id < @cid))", ts, id, false
                            with
                            | :? FormatException
                            | :? LegateIdentifierException -> "", null, null, true
                        | _ -> "", null, null, true

                if bogus then
                    {
                        Items = [] :> IReadOnlyList<Session>
                        Continuation = null
                    }
                else
                    use cmd =
                        command
                            connection
                            transaction
                            $"SELECT {this.SessionColumns} FROM {this.SessionsTable} WHERE tenant = @t AND (@state IS NULL OR state = @state) {continuationClause} ORDER BY updated_at DESC, id DESC LIMIT @take"

                    textParam cmd "t" (tenant.ToString())
                    textParam cmd "state" stateText

                    if continuationClause <> "" then
                        textParam cmd "cts" continuationTs
                        textParam cmd "cid" continuationId

                    intParam cmd "take" (pageSize + 1)

                    use reader = cmd.ExecuteReader()

                    let rows =
                        [
                            while reader.Read() do
                                this.ReadSession(reader)
                        ]

                    reader.Close()

                    let page = rows |> List.truncate pageSize
                    let hasMore = rows.Length > page.Length

                    {
                        Items = page :> IReadOnlyList<Session>
                        Continuation =
                            match hasMore, List.tryLast page with
                            | true, Some last -> this.PageToken(last)
                            | _ -> null
                    })
            |> Task.FromResult

        member this.UpdateSessionState(tenant, sessionId, state, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use lockCmd =
                    command
                        connection
                        transaction
                        $"SELECT state FROM {this.SessionsTable} WHERE id = @id AND tenant = @t FOR UPDATE"

                textParam lockCmd "id" (sessionId.ToString())
                textParam lockCmd "t" (tenant.ToString())

                use lockReader = lockCmd.ExecuteReader()

                let current: string | null =
                    if lockReader.Read() then lockReader.GetString(0) else null

                lockReader.Close()

                if isNull (box current) then
                    raise (
                        SessionNotFoundException(
                            sessionId,
                            sprintf "No session %O exists in tenant %O." sessionId tenant
                        )
                    )

                if current = SessionState.Closed.ToString() && state <> SessionState.Closed then
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            nameof SessionState,
                            "A closed session cannot leave the Closed state."
                        )
                    )

                let nowText = stamp this.UtcNow

                use updateCmd =
                    command
                        connection
                        transaction
                        $"UPDATE {this.SessionsTable} SET state = @state, updated_at = @now WHERE id = @id AND tenant = @t"

                textParam updateCmd "state" (state.ToString())
                textParam updateCmd "now" nowText
                textParam updateCmd "id" (sessionId.ToString())
                textParam updateCmd "t" (tenant.ToString())
                updateCmd.ExecuteNonQuery() |> ignore

                this.ReadStoredSession(connection, transaction, tenant, sessionId))
            |> Task.FromResult

        member this.CloseSession(tenant, sessionId, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use lockCmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.SessionColumns} FROM {this.SessionsTable} WHERE id = @id AND tenant = @t FOR UPDATE"

                textParam lockCmd "id" (sessionId.ToString())
                textParam lockCmd "t" (tenant.ToString())

                use lockReader = lockCmd.ExecuteReader()

                if not (lockReader.Read()) then
                    lockReader.Close()

                    raise (
                        SessionNotFoundException(
                            sessionId,
                            sprintf "No session %O exists in tenant %O." sessionId tenant
                        )
                    )

                let session = this.ReadSession(lockReader)
                lockReader.Close()

                if session.State = SessionState.Closed then
                    session
                else
                    let nowText = stamp this.UtcNow

                    use updateCmd =
                        command
                            connection
                            transaction
                            $"UPDATE {this.SessionsTable} SET state = @state, updated_at = @now, closed_at = @now, permission_grants_json = @grants WHERE id = @id AND tenant = @t"

                    textParam updateCmd "state" (SessionState.Closed.ToString())
                    textParam updateCmd "now" nowText
                    textParam updateCmd "grants" "[]"
                    textParam updateCmd "id" (sessionId.ToString())
                    textParam updateCmd "t" (tenant.ToString())
                    updateCmd.ExecuteNonQuery() |> ignore

                    this.ReadStoredSession(connection, transaction, tenant, sessionId))
            |> Task.FromResult

        member this.GrantSessionTool(tenant, sessionId, toolName, _) =
            if String.IsNullOrWhiteSpace toolName then
                raise (ArgumentException("The tool name must be a non-empty string.", nameof toolName))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use lockCmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.SessionColumns} FROM {this.SessionsTable} WHERE id = @id AND tenant = @t FOR UPDATE"

                textParam lockCmd "id" (sessionId.ToString())
                textParam lockCmd "t" (tenant.ToString())

                use lockReader = lockCmd.ExecuteReader()

                if not (lockReader.Read()) then
                    lockReader.Close()

                    raise (
                        SessionNotFoundException(
                            sessionId,
                            sprintf "No session %O exists in tenant %O." sessionId tenant
                        )
                    )

                let session = this.ReadSession(lockReader)
                lockReader.Close()

                if session.State = SessionState.Closed then
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            nameof SessionState,
                            "A closed session carries no grant memory."
                        )
                    )

                let grants = ResizeArray<string>(this.StoredGrants(session.PermissionGrants))

                if not (grants.Contains toolName) then
                    grants.Add toolName

                let nowText = stamp this.UtcNow

                use updateCmd =
                    command
                        connection
                        transaction
                        $"UPDATE {this.SessionsTable} SET permission_grants_json = @grants, updated_at = @now WHERE id = @id AND tenant = @t"

                textParam updateCmd "grants" (serialize<List<string>> (List<string>(grants :> IReadOnlyList<string>)))
                textParam updateCmd "now" nowText
                textParam updateCmd "id" (sessionId.ToString())
                textParam updateCmd "t" (tenant.ToString())
                updateCmd.ExecuteNonQuery() |> ignore

                { session with
                    PermissionGrants = grants :> IReadOnlyList<string>
                    UpdatedAt = parseStamp nowText
                })
            |> Task.FromResult

        member this.SetSessionAgent(tenant, sessionId, agentId, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use lockCmd =
                    command
                        connection
                        transaction
                        $"SELECT current_turn_id FROM {this.SessionsTable} WHERE id = @id AND tenant = @t FOR UPDATE"

                textParam lockCmd "id" (sessionId.ToString())
                textParam lockCmd "t" (tenant.ToString())

                use lockReader = lockCmd.ExecuteReader()

                if not (lockReader.Read()) then
                    lockReader.Close()

                    raise (
                        SessionNotFoundException(
                            sessionId,
                            sprintf "No session %O exists in tenant %O." sessionId tenant
                        )
                    )

                // The rule is CurrentTurnId-based: the store's claim
                // tracking decides, not the lifecycle state the host
                // maintains.
                let blocked = not (lockReader.IsDBNull(0))
                lockReader.Close()

                if blocked then
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            nameof SessionState,
                            "A turn is running or suspended in the session."
                        )
                    )

                let nowText = stamp this.UtcNow

                use updateCmd =
                    command
                        connection
                        transaction
                        $"UPDATE {this.SessionsTable} SET agent_id = @agent, updated_at = @now WHERE id = @id AND tenant = @t"

                textParam updateCmd "agent" (agentId.ToString())
                textParam updateCmd "now" nowText
                textParam updateCmd "id" (sessionId.ToString())
                textParam updateCmd "t" (tenant.ToString())
                updateCmd.ExecuteNonQuery() |> ignore

                this.ReadStoredSession(connection, transaction, tenant, sessionId))
            |> Task.FromResult

        member this.SetSessionTitle(tenant, sessionId, title, _) =
            if isNull (box title) then
                raise (ArgumentNullException(nameof title))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                this.RequireSession(connection, transaction, tenant, sessionId)

                let nowText = stamp this.UtcNow

                use updateCmd =
                    command
                        connection
                        transaction
                        $"UPDATE {this.SessionsTable} SET title = @title, updated_at = @now WHERE id = @id AND tenant = @t"

                textParam updateCmd "title" title
                textParam updateCmd "now" nowText
                textParam updateCmd "id" (sessionId.ToString())
                textParam updateCmd "t" (tenant.ToString())
                updateCmd.ExecuteNonQuery() |> ignore

                this.ReadStoredSession(connection, transaction, tenant, sessionId))
            |> Task.FromResult

        member this.AppendInboxMessage(tenant, sessionId, payload, delivery, _) =
            if isNull (box payload) then
                raise (ArgumentNullException(nameof payload))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                this.RequireSession(connection, transaction, tenant, sessionId)

                use maxCmd =
                    command
                        connection
                        transaction
                        $"SELECT COALESCE(MAX(position), 0) FROM {this.InboxTable} WHERE session_id = @sid AND tenant = @t"

                textParam maxCmd "sid" (sessionId.ToString())
                textParam maxCmd "t" (tenant.ToString())

                use maxReader = maxCmd.ExecuteReader()
                maxReader.Read() |> ignore
                let position = maxReader.GetInt64(0) + 1L
                maxReader.Close()

                let now = this.UtcNow

                use insertCmd =
                    command
                        connection
                        transaction
                        $"INSERT INTO {this.InboxTable} (session_id, position, tenant, payload_json, delivery_mode, consumed, appended_at) VALUES (@sid, @pos, @t, @payload, @delivery, FALSE, @appended)"

                textParam insertCmd "sid" (sessionId.ToString())
                longParam insertCmd "pos" position
                textParam insertCmd "t" (tenant.ToString())
                textParam insertCmd "payload" (serialize<InboxPayload> payload)
                textParam insertCmd "delivery" (delivery.ToString())
                textParam insertCmd "appended" (stamp now)
                insertCmd.ExecuteNonQuery() |> ignore

                {
                    SessionId = sessionId
                    Position = position
                    Payload = payload
                    Delivery = delivery
                    Consumed = false
                    AppendedAt = now
                })
            |> Task.FromResult

        member this.ReadPendingInbox(tenant, sessionId, _) =
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
                    raise (
                        SessionNotFoundException(
                            sessionId,
                            sprintf "No session %O exists in tenant %O." sessionId tenant
                        )
                    )

                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT position, payload_json, delivery_mode, appended_at FROM {this.InboxTable} WHERE session_id = @sid AND tenant = @t AND consumed = FALSE ORDER BY position"

                textParam cmd "sid" (sessionId.ToString())
                textParam cmd "t" (tenant.ToString())

                use reader = cmd.ExecuteReader()

                let entries =
                    [
                        while reader.Read() do
                            let payload: InboxPayload =
                                match JsonSerializer.Deserialize(reader.GetString(1), jsonOptions) with
                                | null -> raise (InvalidOperationException("The stored inbox payload is null."))
                                | decoded -> decoded

                            {
                                SessionId = sessionId
                                Position = reader.GetInt64(0)
                                Payload = payload
                                Delivery = Enum.Parse<DeliveryMode>(reader.GetString(2))
                                Consumed = false
                                AppendedAt = parseStamp (reader.GetString(3))
                            }
                    ]

                reader.Close()
                entries :> IReadOnlyList<InboxEntry>)
            |> Task.FromResult

        member this.MarkInboxConsumed(tenant, sessionId, positions, _) =
            if isNull (box positions) then
                raise (ArgumentNullException(nameof positions))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                this.RequireSession(connection, transaction, tenant, sessionId)

                // Only pending rows flip: consuming twice is a no-op and
                // reports zero further effects.
                use cmd =
                    command
                        connection
                        transaction
                        $"UPDATE {this.InboxTable} SET consumed = TRUE WHERE session_id = @sid AND tenant = @t AND consumed = FALSE AND position = ANY (@positions)"

                textParam cmd "sid" (sessionId.ToString())
                textParam cmd "t" (tenant.ToString())

                let arrayParam =
                    NpgsqlParameter("positions", NpgsqlTypes.NpgsqlDbType.Array ||| NpgsqlTypes.NpgsqlDbType.Bigint)

                arrayParam.Value <- box (Seq.toArray positions)
                cmd.Parameters.Add(arrayParam) |> ignore

                cmd.ExecuteNonQuery())
            |> Task.FromResult

        member this.ClaimNextTurn(tenant, sessionId, owner, leaseDuration, _) =
            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            if leaseDuration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                this.RequireSession(connection, transaction, tenant, sessionId)

                let now = this.UtcNow
                let nowText = stamp now
                let expiresText = stamp (now + leaseDuration)
                let mintToken () = Ulid.NewUlid().ToString()

                // One live claim per session: an unexpired claim makes
                // every further claim the missing branch, with no effects.
                use liveCmd =
                    command
                        connection
                        transaction
                        $"SELECT turn_id FROM {this.TurnsTable} WHERE session_id = @sid AND tenant = @t AND claim_token IS NOT NULL AND claim_expires_at > @now AND status IN ({this.OpenStatusFilter}) LIMIT 1 FOR UPDATE"

                textParam liveCmd "sid" (sessionId.ToString())
                textParam liveCmd "t" (tenant.ToString())
                textParam liveCmd "now" nowText

                use liveReader = liveCmd.ExecuteReader()
                let held = liveReader.Read()
                liveReader.Close()

                if held then
                    TurnLeaseMissing(TurnId.New()) :> TurnLeaseState
                else
                    use openCmd =
                        command
                            connection
                            transaction
                            $"SELECT turn_id, attempt FROM {this.TurnsTable} WHERE session_id = @sid AND tenant = @t AND status IN ({this.OpenStatusFilter}) LIMIT 1 FOR UPDATE"

                    textParam openCmd "sid" (sessionId.ToString())
                    textParam openCmd "t" (tenant.ToString())

                    use openReader = openCmd.ExecuteReader()

                    let openTurn: (TurnId * int) option =
                        if openReader.Read() then
                            Some(TurnId.Parse(openReader.GetString(0)), openReader.GetInt32(1))
                        else
                            None

                    openReader.Close()

                    use headCmd =
                        command
                            connection
                            transaction
                            $"SELECT position, payload_json FROM {this.InboxTable} WHERE session_id = @sid AND tenant = @t AND consumed = FALSE ORDER BY position LIMIT 1 FOR UPDATE"

                    textParam headCmd "sid" (sessionId.ToString())
                    textParam headCmd "t" (tenant.ToString())

                    use headReader = headCmd.ExecuteReader()

                    let head: (int64 * InboxPayload) option =
                        if headReader.Read() then
                            let payload: InboxPayload =
                                match JsonSerializer.Deserialize(headReader.GetString(1), jsonOptions) with
                                | null -> raise (InvalidOperationException("The stored inbox payload is null."))
                                | decoded -> decoded

                            Some(headReader.GetInt64(0), payload)
                        else
                            None

                    headReader.Close()

                    let consume position =
                        use consumeCmd =
                            command
                                connection
                                transaction
                                $"UPDATE {this.InboxTable} SET consumed = TRUE WHERE session_id = @sid AND tenant = @t AND position = @pos"

                        textParam consumeCmd "sid" (sessionId.ToString())
                        textParam consumeCmd "t" (tenant.ToString())
                        longParam consumeCmd "pos" position
                        consumeCmd.ExecuteNonQuery() |> ignore

                    let stampCurrent (turnIdText: string) =
                        use stampCmd =
                            command
                                connection
                                transaction
                                $"UPDATE {this.SessionsTable} SET current_turn_id = @tid, updated_at = @now WHERE id = @sid AND tenant = @t"

                        textParam stampCmd "tid" turnIdText
                        textParam stampCmd "now" nowText
                        textParam stampCmd "sid" (sessionId.ToString())
                        textParam stampCmd "t" (tenant.ToString())
                        stampCmd.ExecuteNonQuery() |> ignore

                    match head, openTurn with
                    | Some(position, :? ReplyPayload), Some(openTurnId, openAttempt) ->
                        // Resume: consume the reply and re-claim the same
                        // open turn under a fresh token, attempt + 1.
                        consume position

                        let attempt = openAttempt + 1

                        let claim =
                            {
                                TurnId = openTurnId
                                Token = mintToken ()
                                Owner = owner
                                ExpiresAt = now + leaseDuration
                                Attempt = attempt
                            }

                        use resumeCmd =
                            command
                                connection
                                transaction
                                $"UPDATE {this.TurnsTable} SET claim_token = @tok, claim_owner = @owner, claim_expires_at = @exp, attempt = @attempt WHERE turn_id = @tid AND tenant = @t"

                        textParam resumeCmd "tok" claim.Token
                        textParam resumeCmd "owner" owner
                        textParam resumeCmd "exp" expiresText
                        intParam resumeCmd "attempt" attempt
                        textParam resumeCmd "tid" (openTurnId.ToString())
                        textParam resumeCmd "t" (tenant.ToString())
                        resumeCmd.ExecuteNonQuery() |> ignore

                        stampCurrent (openTurnId.ToString())
                        TurnLeaseRenewed claim :> TurnLeaseState
                    | Some(position, :? UserMessagePayload), _ ->
                        // New turn: consume the message and mint a fresh
                        // turn; a stale open turn from a lapsed claim is
                        // retired to Aborted with its fence released and no
                        // settlement recorded, so a later settle on it
                        // rejects as a stale claim.
                        consume position

                        let turnId = TurnId.New()

                        let claim =
                            {
                                TurnId = turnId
                                Token = mintToken ()
                                Owner = owner
                                ExpiresAt = now + leaseDuration
                                Attempt = 1
                            }

                        use retireCmd =
                            command
                                connection
                                transaction
                                $"UPDATE {this.TurnsTable} SET status = 'Aborted', claim_token = NULL, claim_owner = NULL, claim_expires_at = NULL WHERE session_id = @sid AND tenant = @t AND status IN ({this.OpenStatusFilter}) AND turn_id <> @tid"

                        textParam retireCmd "sid" (sessionId.ToString())
                        textParam retireCmd "t" (tenant.ToString())
                        textParam retireCmd "tid" (turnId.ToString())
                        retireCmd.ExecuteNonQuery() |> ignore

                        use insertCmd =
                            command
                                connection
                                transaction
                                $"INSERT INTO {this.TurnsTable} (turn_id, session_id, tenant, attempt, status, claim_token, claim_owner, claim_expires_at, started_at, completed_at, iterations, input_tokens, output_tokens, error, stop_cause, outcome_json) VALUES (@tid, @sid, @t, 1, 'Running', @tok, @owner, @exp, @started, NULL, 0, 0, 0, NULL, NULL, NULL)"

                        textParam insertCmd "tid" (turnId.ToString())
                        textParam insertCmd "sid" (sessionId.ToString())
                        textParam insertCmd "t" (tenant.ToString())
                        textParam insertCmd "tok" claim.Token
                        textParam insertCmd "owner" owner
                        textParam insertCmd "exp" expiresText
                        textParam insertCmd "started" nowText
                        insertCmd.ExecuteNonQuery() |> ignore

                        stampCurrent (turnId.ToString())
                        TurnLeaseRenewed claim :> TurnLeaseState
                    | _ ->
                        // Nothing claimable: a reply with no open turn to
                        // resume, or an empty inbox.
                        TurnLeaseMissing(TurnId.New()) :> TurnLeaseState)
            |> Task.FromResult

        member this.RenewClaim(tenant, claim, leaseDuration, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            if leaseDuration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                let now = this.UtcNow
                let nowText = stamp now

                match this.ResolveClaim(connection, transaction, tenant, claim, nowText) with
                | Choice1Of3(_, live) ->
                    if live.ExpiresAt <= now then
                        TurnLeaseLost(claim.TurnId, "expired") :> TurnLeaseState
                    else
                        let renewed =
                            { live with
                                ExpiresAt = now + leaseDuration
                            }

                        use renewCmd =
                            command
                                connection
                                transaction
                                $"UPDATE {this.TurnsTable} SET claim_expires_at = @exp WHERE turn_id = @tid AND tenant = @t"

                        textParam renewCmd "exp" (stamp renewed.ExpiresAt)
                        textParam renewCmd "tid" (claim.TurnId.ToString())
                        textParam renewCmd "t" (tenant.ToString())
                        renewCmd.ExecuteNonQuery() |> ignore

                        TurnLeaseRenewed renewed :> TurnLeaseState
                | Choice2Of3() -> TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                | Choice3Of3() -> TurnLeaseMissing claim.TurnId :> TurnLeaseState)
            |> Task.FromResult

        member this.ObserveAndRenewClaim(tenant, claim, leaseDuration, cancellationToken) =
            // Plain atomic renew, matching the in-memory reference: no
            // cancellation-request carrier type exists in the contracts
            // yet, so the renew leg is the whole behaviour.
            (this :> ISessionStore).RenewClaim(tenant, claim, leaseDuration, cancellationToken)

        member this.VerifyClaim(tenant, claim, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                let now = this.UtcNow
                let nowText = stamp now

                match this.ResolveClaim(connection, transaction, tenant, claim, nowText) with
                | Choice1Of3(_, live) ->
                    if live.ExpiresAt <= now then
                        TurnLeaseLost(claim.TurnId, "expired") :> TurnLeaseState
                    else
                        TurnLeaseHeld live :> TurnLeaseState
                | Choice2Of3() -> TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                | Choice3Of3() -> TurnLeaseMissing claim.TurnId :> TurnLeaseState)
            |> Task.FromResult

        member this.CheckpointUsage(tenant, claim, usage, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                let now = this.UtcNow
                let nowText = stamp now

                match this.ResolveClaim(connection, transaction, tenant, claim, nowText) with
                | Choice1Of3(_, live) ->
                    if live.ExpiresAt <= now then
                        TurnLeaseLost(claim.TurnId, "expired") :> TurnLeaseState
                    else
                        use checkpointCmd =
                            command
                                connection
                                transaction
                                $"UPDATE {this.TurnsTable} SET input_tokens = @input, output_tokens = @output WHERE turn_id = @tid AND tenant = @t"

                        longParam checkpointCmd "input" usage.InputTokens
                        longParam checkpointCmd "output" usage.OutputTokens
                        textParam checkpointCmd "tid" (claim.TurnId.ToString())
                        textParam checkpointCmd "t" (tenant.ToString())
                        checkpointCmd.ExecuteNonQuery() |> ignore

                        TurnLeaseHeld live :> TurnLeaseState
                | Choice2Of3() -> TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                | Choice3Of3() -> TurnLeaseMissing claim.TurnId :> TurnLeaseState)
            |> Task.FromResult

        member this.SettleTurn(tenant, claim, status, outcome, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                let nowText = stamp this.UtcNow

                // The guard runs inside the transaction, so the typed
                // exception carries the session the settle targeted; a claim
                // that resolves nowhere falls through to the stale-claim
                // rejection below.
                use guardCmd =
                    command
                        connection
                        transaction
                        $"SELECT session_id FROM {this.TurnsTable} WHERE turn_id = @tid AND tenant = @t"

                textParam guardCmd "tid" (claim.TurnId.ToString())
                textParam guardCmd "t" (tenant.ToString())

                use guardReader = guardCmd.ExecuteReader()

                let guardSession: string | null =
                    if guardReader.Read() then
                        guardReader.GetString(0)
                    else
                        null

                guardReader.Close()

                match guardSession with
                | null -> ()
                | sessionText ->
                    match status with
                    | TurnStatus.Pending
                    | TurnStatus.Running
                    | TurnStatus.Suspended ->
                        raise (
                            InvalidSessionStateException(
                                SessionId.Parse(sessionText),
                                "nonTerminal",
                                "Only terminal statuses (Completed, Aborted, Failed) settle a turn."
                            )
                        )
                    | _ -> ()

                match this.ResolveClaim(connection, transaction, tenant, claim, nowText) with
                | Choice1Of3(sessionId, _) ->
                    // The token still fences: the first settle wins, and a
                    // lapsed-but-uncontested lease does not unseat it (a
                    // settle is terminal; nothing can take over a turn the
                    // owner is settling).
                    use settledCmd =
                        command
                            connection
                            transaction
                            $"SELECT status, completed_at FROM {this.TurnsTable} WHERE turn_id = @tid AND tenant = @t"

                    textParam settledCmd "tid" (claim.TurnId.ToString())
                    textParam settledCmd "t" (tenant.ToString())

                    use settledReader = settledCmd.ExecuteReader()
                    settledReader.Read() |> ignore
                    let appliedStatus = Enum.Parse<TurnStatus>(settledReader.GetString(0))
                    let completed = not (settledReader.IsDBNull(1))
                    settledReader.Close()

                    if completed then
                        if appliedStatus = status then
                            TurnAlreadySettled claim.TurnId :> TurnSettlement
                        else
                            TurnSettleRejected(claim.TurnId, "alreadySettledByOther") :> TurnSettlement
                    else
                        this.ApplySettlement(
                            connection,
                            transaction,
                            tenant,
                            sessionId,
                            claim,
                            status,
                            outcome,
                            nowText
                        )

                        TurnSettled(claim.TurnId, status) :> TurnSettlement
                | Choice2Of3() -> TurnSettleRejected(claim.TurnId, "staleClaim") :> TurnSettlement
                | Choice3Of3() ->
                    // No live claim for the token. When the turn settled
                    // already, a retry of the same outcome observes it and
                    // a different outcome is rejected; otherwise the claim
                    // is stale. Only a stamped completion counts as a
                    // settlement: a retired (abandoned) row carries a
                    // terminal status with no stamp and still rejects.
                    use settledCmd =
                        command
                            connection
                            transaction
                            $"SELECT status, completed_at FROM {this.TurnsTable} WHERE turn_id = @tid AND tenant = @t"

                    textParam settledCmd "tid" (claim.TurnId.ToString())
                    textParam settledCmd "t" (tenant.ToString())

                    use settledReader = settledCmd.ExecuteReader()

                    let outcome_ =
                        if settledReader.Read() && not (settledReader.IsDBNull(1)) then
                            let appliedStatus = Enum.Parse<TurnStatus>(settledReader.GetString(0))
                            settledReader.Close()

                            if appliedStatus = status then
                                TurnAlreadySettled claim.TurnId :> TurnSettlement
                            else
                                TurnSettleRejected(claim.TurnId, "alreadySettledByOther") :> TurnSettlement
                        else
                            settledReader.Close()
                            TurnSettleRejected(claim.TurnId, "staleClaim") :> TurnSettlement

                    outcome_)
            |> Task.FromResult

        member this.AbortTurn(tenant, claim, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                let nowText = stamp this.UtcNow

                match this.ResolveClaim(connection, transaction, tenant, claim, nowText) with
                | Choice1Of3(sessionId, live) ->
                    // Abort settles Aborted and releases the lease; a
                    // later settle observes the applied settlement.
                    this.ApplySettlement(
                        connection,
                        transaction,
                        tenant,
                        sessionId,
                        claim,
                        TurnStatus.Aborted,
                        null,
                        nowText
                    )

                    TurnLeaseHeld live :> TurnLeaseState
                | Choice2Of3() -> TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                | Choice3Of3() -> TurnLeaseMissing claim.TurnId :> TurnLeaseState)
            |> Task.FromResult

        member this.EnqueueCompletionOutbox(tenant, completion, _) =
            if isNull (box completion) then
                raise (ArgumentNullException(nameof completion))

            if String.IsNullOrWhiteSpace completion.IdempotencyKey then
                raise (
                    ArgumentException("The completion's idempotency key must be a non-empty string.", nameof completion)
                )

            this.EnsureMigrated()

            let readByKey
                (connection: NpgsqlConnection)
                (transaction: NpgsqlTransaction)
                : CompletionOutboxEntry option =
                use findCmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.OutboxColumns} FROM {this.OutboxTable} WHERE tenant = @t AND idempotency_key = @key"

                textParam findCmd "t" (tenant.ToString())
                textParam findCmd "key" completion.IdempotencyKey

                use findReader = findCmd.ExecuteReader()

                let row =
                    if findReader.Read() then
                        Some(this.ReadOutboxEntry(findReader))
                    else
                        None

                findReader.Close()
                row

            let insertAttempt () =
                transact options (fun connection transaction ->
                    this.RequireSession(connection, transaction, tenant, completion.SessionId)

                    match readByKey connection transaction with
                    | Some existing -> Choice1Of2 existing
                    | None ->
                        let nowText = stamp this.UtcNow

                        use insertCmd =
                            command
                                connection
                                transaction
                                $"INSERT INTO {this.OutboxTable} (idempotency_key, tenant, session_id, completion_json, created_at, delivered, delivered_at, lease_owner, lease_expires_at) VALUES (@key, @t, @sid, @completion, @created, FALSE, NULL, NULL, NULL)"

                        textParam insertCmd "key" completion.IdempotencyKey
                        textParam insertCmd "t" (tenant.ToString())
                        textParam insertCmd "sid" (completion.SessionId.ToString())
                        textParam insertCmd "completion" (serialize<SessionCompletion> completion)
                        textParam insertCmd "created" nowText
                        insertCmd.ExecuteNonQuery() |> ignore

                        Choice2Of2())

            try
                match insertAttempt () with
                | Choice1Of2 existing -> existing
                | Choice2Of2() ->
                    // Re-read the row just written; the insert and the stamp
                    // become visible together.
                    transact options (fun connection transaction ->
                        match readByKey connection transaction with
                        | Some row -> row
                        | None -> failwith "The outbox row just enqueued is missing.")
            with :? PostgresException as ex when ex.SqlState = PostgresErrorCodes.UniqueViolation ->
                // A concurrent enqueue of the same key won the race:
                // observe the first row with zero further effects.
                transact options (fun connection transaction ->
                    match readByKey connection transaction with
                    | Some row -> row
                    | None -> raise (InvalidOperationException("The conflicting outbox row is missing.")))
            |> Task.FromResult

        member this.ClaimCompletionOutbox(owner, maxBatch, leaseDuration, _) =
            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            if maxBatch <= 0 then
                raise (ArgumentOutOfRangeException(nameof maxBatch, "The batch size must be positive."))

            if leaseDuration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

            transact options (fun connection transaction ->
                let clock = timeProvider
                let nowText = stamp (clock.GetUtcNow())
                let leaseText = stamp (clock.GetUtcNow() + leaseDuration)

                use claimCmd =
                    command
                        connection
                        transaction
                        $"SELECT idempotency_key, tenant FROM {this.OutboxTable} WHERE delivered = FALSE AND (lease_owner IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= @now) ORDER BY created_at, idempotency_key LIMIT @take FOR UPDATE SKIP LOCKED"

                textParam claimCmd "now" nowText
                intParam claimCmd "take" maxBatch

                use claimReader = claimCmd.ExecuteReader()

                let victims =
                    [
                        while claimReader.Read() do
                            (claimReader.GetString(0), claimReader.GetString(1))
                    ]

                claimReader.Close()

                for key, tenantText in victims do
                    use leaseCmd =
                        command
                            connection
                            transaction
                            $"UPDATE {this.OutboxTable} SET lease_owner = @owner, lease_expires_at = @exp WHERE tenant = @t AND idempotency_key = @key"

                    textParam leaseCmd "owner" owner
                    textParam leaseCmd "exp" leaseText
                    textParam leaseCmd "t" tenantText
                    textParam leaseCmd "key" key
                    leaseCmd.ExecuteNonQuery() |> ignore

                let entries =
                    victims
                    |> List.map (fun (key, tenantText) ->
                        this.ReadOutboxByKey(connection, transaction, tenantText, key))

                entries :> IReadOnlyList<CompletionOutboxEntry>)
            |> Task.FromResult

        member this.VerifyCompletionClaim(tenant, idempotencyKey, owner, _) =
            if String.IsNullOrWhiteSpace idempotencyKey then
                raise (ArgumentException("The idempotency key must be a non-empty string.", nameof idempotencyKey))

            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT delivered, lease_owner, lease_expires_at FROM {this.OutboxTable} WHERE tenant = @t AND idempotency_key = @key"

                textParam cmd "t" (tenant.ToString())
                textParam cmd "key" idempotencyKey

                use reader = cmd.ExecuteReader()

                let live =
                    if not (reader.Read()) then
                        reader.Close()
                        false
                    else
                        let delivered = reader.GetBoolean(0)

                        let lease: (string | null) * (string | null) =
                            (getTextOrNull reader 1, getTextOrNull reader 2)

                        reader.Close()

                        match delivered, lease with
                        | false, (ownerText, expiresText) ->
                            match Option.ofObj ownerText, Option.ofObj expiresText with
                            | Some leaseOwner, Some leaseExpires ->
                                String.Equals(leaseOwner, owner, StringComparison.Ordinal)
                                && (parseStamp leaseExpires) > timeProvider.GetUtcNow()
                            | _ -> false
                        | _ -> false

                live)
            |> Task.FromResult

        member this.MarkCompletionDelivered(tenant, idempotencyKey, owner, _) =
            if String.IsNullOrWhiteSpace idempotencyKey then
                raise (ArgumentException("The idempotency key must be a non-empty string.", nameof idempotencyKey))

            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT delivered, lease_owner, lease_expires_at FROM {this.OutboxTable} WHERE tenant = @t AND idempotency_key = @key FOR UPDATE"

                textParam cmd "t" (tenant.ToString())
                textParam cmd "key" idempotencyKey

                use reader = cmd.ExecuteReader()

                if not (reader.Read()) then
                    reader.Close()
                    false
                elif reader.GetBoolean(0) then
                    reader.Close()
                    true
                else
                    let lease: (string | null) * (string | null) =
                        (getTextOrNull reader 1, getTextOrNull reader 2)

                    reader.Close()

                    match lease with
                    | ownerText, expiresText ->
                        match Option.ofObj ownerText, Option.ofObj expiresText with
                        | Some leaseOwner, Some leaseExpires when
                            String.Equals(leaseOwner, owner, StringComparison.Ordinal)
                            && (parseStamp leaseExpires) > timeProvider.GetUtcNow()
                            ->
                            let nowText = stamp (timeProvider.GetUtcNow())

                            use markCmd =
                                command
                                    connection
                                    transaction
                                    $"UPDATE {this.OutboxTable} SET delivered = TRUE, delivered_at = @now, lease_owner = NULL, lease_expires_at = NULL WHERE tenant = @t AND idempotency_key = @key"

                            textParam markCmd "now" nowText
                            textParam markCmd "t" (tenant.ToString())
                            textParam markCmd "key" idempotencyKey
                            markCmd.ExecuteNonQuery() |> ignore
                            true
                        | _ -> false)
            |> Task.FromResult

        member this.PurgeDeliveredCompletions(deliveredBefore, _) =
            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"DELETE FROM {this.OutboxTable} WHERE delivered = TRUE AND delivered_at <= @cutoff"

                textParam cmd "cutoff" (stamp deliveredBefore)
                cmd.ExecuteNonQuery())
            |> Task.FromResult

        member this.GetDispatchCandidates(tenant, maxBatch, _) =
            if maxBatch <= 0 then
                raise (ArgumentOutOfRangeException(nameof maxBatch, "The batch size must be positive."))

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT s.id FROM {this.SessionsTable} s WHERE s.tenant = @t AND EXISTS (SELECT 1 FROM {this.InboxTable} i WHERE i.session_id = s.id AND i.tenant = @t AND i.consumed = FALSE) ORDER BY s.id LIMIT @take"

                textParam cmd "t" (tenant.ToString())
                intParam cmd "take" (maxBatch + 1)

                use reader = cmd.ExecuteReader()

                let pending =
                    [
                        while reader.Read() do
                            SessionId.Parse(reader.GetString(0))
                    ]

                reader.Close()

                let batch = pending |> List.truncate maxBatch

                {
                    Sessions = batch :> IReadOnlyList<SessionId>
                    HasMore = pending.Length > batch.Length
                })
            |> Task.FromResult

        member this.CountSessionsByAgent(tenant, agentId, _) =
            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT COUNT(*) FROM {this.SessionsTable} WHERE tenant = @t AND agent_id = @agent"

                textParam cmd "t" (tenant.ToString())
                textParam cmd "agent" (agentId.ToString())

                use reader = cmd.ExecuteReader()
                reader.Read() |> ignore
                let count = int (reader.GetInt64(0))
                reader.Close()
                count)
            |> Task.FromResult

        member this.CountSessionsByTenant(tenant, _) =
            transact options (fun connection transaction ->
                use cmd =
                    command connection transaction $"SELECT COUNT(*) FROM {this.SessionsTable} WHERE tenant = @t"

                textParam cmd "t" (tenant.ToString())

                use reader = cmd.ExecuteReader()
                reader.Read() |> ignore
                let count = int (reader.GetInt64(0))
                reader.Close()
                count)
            |> Task.FromResult

        member this.CountRunningSessions(_) =
            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT COUNT(*) FROM {this.SessionsTable} WHERE current_turn_id IS NOT NULL"

                use reader = cmd.ExecuteReader()
                reader.Read() |> ignore
                let count = int (reader.GetInt64(0))
                reader.Close()
                count)
            |> Task.FromResult
