// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Sqlite

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Data.Sqlite

// The SQLite ISessionStore: session CRUD, the inbox, turn claims under a
// lease with the fencing semantics the contract pins, dispatch candidates,
// the completion outbox, and the capacity counts. One live claim per
// session and one open turn per session, mirroring the in-memory
// implementation: ClaimNextTurn consumes the head pending user message into
// a new turn, or the head pending reply into a resume of the open turn with
// the attempt incremented; a live unexpired claim makes every further claim
// the missing branch. Every transition that touches more than one row runs
// as a single transaction under the database gate, so claims and
// settlements are atomic and a takeover race leaves the loser with zero
// effects. Every SqliteException funnels through the SqliteErrors boundary.

/// What a fenced call against the claim's turn resolved to.
type private ClaimResolution =
    | Live of sessionId: SessionId * token: string * owner: string * expiresAt: DateTimeOffset * attempt: int
    | TakenOver
    | Absent

/// <summary>
/// The SQLite <see cref="T:Legate.ISessionStore" /> over one shared
/// database file.
/// </summary>
/// <param name="database">The shared database every store uses. Must not be null.</param>
type SqliteSessionStore(database: SqliteDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let path = database.Path

    let mintToken () = Ulid.NewUlid().ToString()

    let mapSql (ex: SqliteException) : LegateException = SqliteErrors.ofSqliteException path ex

    let toIso = SqliteDatabase.ToIso
    let ofIso = SqliteDatabase.OfIso

    let stateName (state: SessionState) = state.ToString()

    let parseState (text: string) : SessionState = Enum.Parse<SessionState>(text, false)

    let deliveryName (delivery: DeliveryMode) = delivery.ToString()

    let parseDelivery (text: string) : DeliveryMode = Enum.Parse<DeliveryMode>(text, false)

    let turnStatusName (status: TurnStatus) = status.ToString()

    let isTerminalStatus (status: TurnStatus) =
        status = TurnStatus.Completed
        || status = TurnStatus.Aborted
        || status = TurnStatus.Failed

    let nonTerminalFilter = "'Running','Suspended','Pending'"

    let sessionsTable () = database.Table "sessions"
    let inboxTable () = database.Table "inbox"
    let turnsTable () = database.Table "turns"
    let outboxTable () = database.Table "outbox"

    let grantsOf (session: Session) : List<string> =
        if isNull (box session.PermissionGrants) then
            List<string>()
        else
            let distinct = List<string>()

            for grant in session.PermissionGrants do
                if not (isNull (box grant)) && not (distinct.Contains grant) then
                    distinct.Add grant

            distinct

    let readSession (reader: SqliteDataReader) : Session =
        let id = SessionId.Parse(reader.GetString(0))
        let tenant = TenantId.Create(reader.GetString(1))
        let agentId = AgentId.Parse(reader.GetString(2))
        let title = reader.GetString(3)
        let state = parseState (reader.GetString(4))

        let currentTurnId =
            if reader.IsDBNull(5) then
                Nullable()
            else
                Nullable(TurnId.Parse(reader.GetString(5)))

        let createdAt = ofIso (reader.GetString(6))
        let updatedAt = ofIso (reader.GetString(7))

        let closedAt =
            if reader.IsDBNull(8) then
                Nullable()
            else
                Nullable(ofIso (reader.GetString(8)))

        let workspaceBinding = if reader.IsDBNull(9) then null else reader.GetString(9)

        let optionsJson = reader.GetString(10)
        let grantsJson = reader.GetString(11)
        let options = SqliteJson.deserialize<SessionOptions> optionsJson
        let grants = SqliteJson.deserialize<List<string>> grantsJson

        {
            Id = id
            Tenant = tenant
            AgentId = agentId
            Title = title
            State = state
            CurrentTurnId = currentTurnId
            CreatedAt = createdAt
            UpdatedAt = updatedAt
            ClosedAt = closedAt
            WorkspaceBinding = workspaceBinding
            Options = options
            PermissionGrants = grants :> IReadOnlyList<string>
        }

    let readInboxEntry (reader: SqliteDataReader) : InboxEntry =
        let sessionId = SessionId.Parse(reader.GetString(0))
        let position = reader.GetInt64(1)
        let payloadJson = reader.GetString(2)
        let delivery = parseDelivery (reader.GetString(3))
        let consumed = reader.GetInt64(4) <> 0L
        let appendedAt = ofIso (reader.GetString(5))
        let payload = SqliteJson.deserialize<InboxPayload> payloadJson

        {
            SessionId = sessionId
            Position = position
            Payload = payload
            Delivery = delivery
            Consumed = consumed
            AppendedAt = appendedAt
        }

    let readOutboxEntry (reader: SqliteDataReader) : CompletionOutboxEntry =
        let key = reader.GetString(0)
        let tenant = TenantId.Create(reader.GetString(1))
        let completionJson = reader.GetString(3)
        let createdAt = ofIso (reader.GetString(4))
        let delivered = reader.GetInt64(5) <> 0L

        let deliveredAt =
            if reader.IsDBNull(6) then
                Nullable()
            else
                Nullable(ofIso (reader.GetString(6)))

        let leaseOwner = if reader.IsDBNull(7) then null else reader.GetString(7)

        let leaseExpiresAt =
            if reader.IsDBNull(8) then
                Nullable()
            else
                Nullable(ofIso (reader.GetString(8)))

        let completion = SqliteJson.deserialize<SessionCompletion> completionJson

        {
            Tenant = tenant
            SessionId = completion.SessionId
            IdempotencyKey = key
            Completion = completion
            CreatedAt = createdAt
            Delivered = delivered
            DeliveredAt = deliveredAt
            LeaseOwner = leaseOwner
            LeaseExpiresAt = leaseExpiresAt
        }

    /// What a fenced call against the claim's turn resolved to.
    let resolveClaim
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (claim: TurnClaim)
        : ClaimResolution =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT session_id, claim_token, claim_owner, claim_expires_at, attempt, status FROM \"%s{turnsTable ()}\" WHERE turn_id = $turn AND tenant = $tenant"

        command.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

        use reader = command.ExecuteReader()

        if not (reader.Read()) then
            Absent
        else
            let status = Enum.Parse<TurnStatus>(reader.GetString(5), false)

            if isTerminalStatus status then
                Absent
            elif reader.IsDBNull(1) then
                TakenOver
            else
                let storedToken = reader.GetString(1)

                if not (String.Equals(storedToken, claim.Token, StringComparison.Ordinal)) then
                    TakenOver
                else
                    let expiresAt = ofIso (reader.GetString(3))
                    let sessionId = SessionId.Parse(reader.GetString(0))
                    let owner = reader.GetString(2)
                    let attempt = reader.GetInt32(4)

                    if expiresAt <= database.UtcNow then
                        TakenOver
                    else
                        Live(sessionId, storedToken, owner, expiresAt, attempt)

    let liveClaimHeld
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (sessionId: SessionId)
        : bool =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT claim_expires_at FROM \"%s{turnsTable ()}\" WHERE session_id = $session AND tenant = $tenant AND status IN (%s{nonTerminalFilter}) AND claim_token IS NOT NULL"

        command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

        use reader = command.ExecuteReader()
        let now = database.UtcNow
        let mutable held = false

        while reader.Read() do
            if not (reader.IsDBNull(0)) then
                let expiresAt = ofIso (reader.GetString(0))

                if expiresAt > now then
                    held <- true

        held

    let openTurnRow
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (sessionId: SessionId)
        : (TurnId * int) option =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT turn_id, attempt FROM \"%s{turnsTable ()}\" WHERE session_id = $session AND tenant = $tenant AND status IN (%s{nonTerminalFilter}) ORDER BY started_at LIMIT 1"

        command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

        use reader = command.ExecuteReader()

        if reader.Read() then
            Some(TurnId.Parse(reader.GetString(0)), reader.GetInt32(1))
        else
            None

    let headPending
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (sessionId: SessionId)
        : (int64 * InboxPayload) option =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT position, payload_json FROM \"%s{inboxTable ()}\" WHERE session_id = $session AND consumed = 0 ORDER BY position LIMIT 1"

        command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore

        use reader = command.ExecuteReader()

        if reader.Read() then
            let position = reader.GetInt64(0)
            let payload = SqliteJson.deserialize<InboxPayload> (reader.GetString(1))
            Some(position, payload)
        else
            None

    let consumePosition
        (connection: SqliteConnection)
        (transaction: SqliteTransaction)
        (sessionId: SessionId)
        (position: int64)
        =
        use command = connection.CreateCommand()
        command.Transaction <- transaction

        command.CommandText <-
            $"UPDATE \"%s{inboxTable ()}\" SET consumed = 1 WHERE session_id = $session AND position = $position"

        command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
        command.Parameters.AddWithValue("$position", position) |> ignore
        command.ExecuteNonQuery() |> ignore

    let stampCurrentTurn
        (connection: SqliteConnection)
        (transaction: SqliteTransaction)
        (tenant: TenantId)
        (sessionId: SessionId)
        (turnId: TurnId option)
        (now: DateTimeOffset)
        =
        use command = connection.CreateCommand()
        command.Transaction <- transaction

        match turnId with
        | None ->
            command.CommandText <-
                $"UPDATE \"%s{sessionsTable ()}\" SET current_turn_id = NULL, updated_at = $now WHERE id = $id AND tenant = $tenant"

            command.Parameters.AddWithValue("$now", toIso now) |> ignore
            command.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
            command.ExecuteNonQuery() |> ignore
        | Some turn ->
            command.CommandText <-
                $"UPDATE \"%s{sessionsTable ()}\" SET current_turn_id = $turn, updated_at = $now WHERE id = $id AND tenant = $tenant"

            command.Parameters.AddWithValue("$turn", turn.Value) |> ignore
            command.Parameters.AddWithValue("$now", toIso now) |> ignore
            command.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
            command.ExecuteNonQuery() |> ignore

    let requireSessionRow
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (sessionId: SessionId)
        : Session =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT id, tenant, agent_id, title, state, current_turn_id, created_at, updated_at, closed_at, workspace_binding, options_json, permission_grants_json FROM \"%s{sessionsTable ()}\" WHERE id = $id AND tenant = $tenant"

        command.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

        use reader = command.ExecuteReader()

        if reader.Read() then
            readSession reader
        else
            raise (SessionNotFoundException(sessionId, sprintf "No session %O exists in tenant %O." sessionId tenant))

    interface ISessionStore with

        member _.CreateSession(tenant, session, _) =
            task {
                if isNull (box session) then
                    raise (ArgumentNullException(nameof session))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            use check = connection.CreateCommand()
                            check.Transaction <- transaction

                            check.CommandText <-
                                $"SELECT id FROM \"%s{sessionsTable ()}\" WHERE id = $id AND tenant = $tenant"

                            check.Parameters.AddWithValue("$id", session.Id.Value) |> ignore
                            check.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use reader = check.ExecuteReader()

                            if reader.Read() then
                                raise (
                                    InvalidSessionStateException(
                                        session.Id,
                                        nameof SessionState,
                                        sprintf "A session %O already exists in tenant %O." session.Id tenant
                                    )
                                )

                            reader.Close()

                            let now = database.UtcNow
                            let storedGrants = grantsOf session
                            let optionsJson = SqliteJson.serialize session.Options
                            let grantsJson = SqliteJson.serialize storedGrants

                            use insert = connection.CreateCommand()
                            insert.Transaction <- transaction

                            insert.CommandText <-
                                $"INSERT INTO \"%s{sessionsTable ()}\" (id, tenant, agent_id, title, state, current_turn_id, created_at, updated_at, closed_at, workspace_binding, options_json, permission_grants_json) VALUES ($id, $tenant, $agent, $title, $state, NULL, $created, $updated, NULL, $binding, $options, $grants)"

                            insert.Parameters.AddWithValue("$id", session.Id.Value) |> ignore
                            insert.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            insert.Parameters.AddWithValue("$agent", session.AgentId.Value) |> ignore

                            insert.Parameters.AddWithValue(
                                "$title",
                                (if isNull (box session.Title) then "" else session.Title)
                            )
                            |> ignore

                            insert.Parameters.AddWithValue("$state", stateName SessionState.Idle) |> ignore
                            insert.Parameters.AddWithValue("$created", toIso now) |> ignore
                            insert.Parameters.AddWithValue("$updated", toIso now) |> ignore

                            insert.Parameters.AddWithValue(
                                "$binding",
                                (if isNull (box session.WorkspaceBinding) then
                                     box DBNull.Value
                                 else
                                     box session.WorkspaceBinding)
                            )
                            |> ignore

                            insert.Parameters.AddWithValue("$options", optionsJson) |> ignore
                            insert.Parameters.AddWithValue("$grants", grantsJson) |> ignore
                            insert.ExecuteNonQuery() |> ignore
                            transaction.Commit()

                            { session with
                                Tenant = tenant
                                State = SessionState.Idle
                                CurrentTurnId = Nullable()
                                CreatedAt = now
                                UpdatedAt = now
                                ClosedAt = Nullable()
                                PermissionGrants = storedGrants :> IReadOnlyList<string>
                            })
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.GetSession(tenant, sessionId, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT id, tenant, agent_id, title, state, current_turn_id, created_at, updated_at, closed_at, workspace_binding, options_json, permission_grants_json FROM \"%s{sessionsTable ()}\" WHERE id = $id AND tenant = $tenant"

                            command.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use reader = command.ExecuteReader()

                            if reader.Read() then
                                readSession reader
                            else
                                Unchecked.defaultof<Session>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.ListSessions(tenant, state, pageSize, continuation, _) =
            task {
                if pageSize <= 0 then
                    raise (ArgumentOutOfRangeException(nameof pageSize, "The page size must be positive."))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            if state.HasValue then
                                command.CommandText <-
                                    $"SELECT id, tenant, agent_id, title, state, current_turn_id, created_at, updated_at, closed_at, workspace_binding, options_json, permission_grants_json FROM \"%s{sessionsTable ()}\" WHERE tenant = $tenant AND state = $state ORDER BY updated_at DESC, id DESC"

                                command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                command.Parameters.AddWithValue("$state", stateName state.Value) |> ignore
                            else
                                command.CommandText <-
                                    $"SELECT id, tenant, agent_id, title, state, current_turn_id, created_at, updated_at, closed_at, workspace_binding, options_json, permission_grants_json FROM \"%s{sessionsTable ()}\" WHERE tenant = $tenant ORDER BY updated_at DESC, id DESC"

                                command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use reader = command.ExecuteReader()
                            let rows = List<Session>()

                            while reader.Read() do
                                rows.Add(readSession reader)

                            let tokenOf (session: Session) =
                                sprintf "%s|%O" (session.UpdatedAt.ToString "O") session.Id

                            let ordered = rows |> Seq.toList

                            let remaining =
                                match continuation with
                                | null -> ordered
                                | token ->
                                    match ordered |> List.tryFindIndex (fun session -> tokenOf session = token) with
                                    | Some index -> ordered |> List.skip (index + 1)
                                    | None -> []

                            let page = remaining |> List.truncate pageSize
                            let hasMore = remaining.Length > page.Length

                            {
                                Items = page :> IReadOnlyList<Session>
                                Continuation =
                                    (match hasMore, List.tryLast page with
                                     | true, Some last -> tokenOf last
                                     | _ -> null)
                            })
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.UpdateSessionState(tenant, sessionId, state, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            let session = requireSessionRow connection transaction tenant sessionId

                            if session.State = SessionState.Closed && state <> SessionState.Closed then
                                raise (
                                    InvalidSessionStateException(
                                        sessionId,
                                        nameof session.State,
                                        "A closed session cannot leave the Closed state."
                                    )
                                )

                            let now = database.UtcNow

                            use update = connection.CreateCommand()
                            update.Transaction <- transaction

                            update.CommandText <-
                                $"UPDATE \"%s{sessionsTable ()}\" SET state = $state, updated_at = $now WHERE id = $id AND tenant = $tenant"

                            update.Parameters.AddWithValue("$state", stateName state) |> ignore
                            update.Parameters.AddWithValue("$now", toIso now) |> ignore
                            update.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
                            update.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            update.ExecuteNonQuery() |> ignore
                            transaction.Commit()

                            { session with
                                State = state
                                UpdatedAt = now
                            })
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.CloseSession(tenant, sessionId, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            let session = requireSessionRow connection transaction tenant sessionId

                            if session.State = SessionState.Closed then
                                transaction.Rollback()
                                session
                            else
                                let now = database.UtcNow
                                let empty = SqliteJson.serialize (List<string>())

                                use update = connection.CreateCommand()
                                update.Transaction <- transaction

                                update.CommandText <-
                                    $"UPDATE \"%s{sessionsTable ()}\" SET state = $state, updated_at = $now, closed_at = $closed, permission_grants_json = $grants WHERE id = $id AND tenant = $tenant"

                                update.Parameters.AddWithValue("$state", stateName SessionState.Closed)
                                |> ignore

                                update.Parameters.AddWithValue("$now", toIso now) |> ignore
                                update.Parameters.AddWithValue("$closed", toIso now) |> ignore
                                update.Parameters.AddWithValue("$grants", empty) |> ignore
                                update.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
                                update.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                update.ExecuteNonQuery() |> ignore
                                transaction.Commit()

                                { session with
                                    State = SessionState.Closed
                                    UpdatedAt = now
                                    ClosedAt = Nullable now
                                    PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
                                })
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.GrantSessionTool(tenant, sessionId, toolName, _) =
            task {
                if String.IsNullOrWhiteSpace toolName then
                    raise (ArgumentException("The tool name must be a non-empty string.", nameof toolName))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            let session = requireSessionRow connection transaction tenant sessionId

                            if session.State = SessionState.Closed then
                                raise (
                                    InvalidSessionStateException(
                                        sessionId,
                                        nameof session.State,
                                        "A closed session carries no grant memory."
                                    )
                                )

                            let grants = grantsOf session

                            if not (grants.Contains toolName) then
                                grants.Add toolName

                            let now = database.UtcNow
                            let grantsJson = SqliteJson.serialize grants

                            use update = connection.CreateCommand()
                            update.Transaction <- transaction

                            update.CommandText <-
                                $"UPDATE \"%s{sessionsTable ()}\" SET permission_grants_json = $grants, updated_at = $now WHERE id = $id AND tenant = $tenant"

                            update.Parameters.AddWithValue("$grants", grantsJson) |> ignore
                            update.Parameters.AddWithValue("$now", toIso now) |> ignore
                            update.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
                            update.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            update.ExecuteNonQuery() |> ignore
                            transaction.Commit()

                            { session with
                                PermissionGrants = grants :> IReadOnlyList<string>
                                UpdatedAt = now
                            })
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.SetSessionAgent(tenant, sessionId, agentId, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            let session = requireSessionRow connection transaction tenant sessionId

                            if session.CurrentTurnId.HasValue then
                                raise (
                                    InvalidSessionStateException(
                                        sessionId,
                                        nameof session.State,
                                        "A turn is running or suspended in the session."
                                    )
                                )

                            let now = database.UtcNow

                            use update = connection.CreateCommand()
                            update.Transaction <- transaction

                            update.CommandText <-
                                $"UPDATE \"%s{sessionsTable ()}\" SET agent_id = $agent, updated_at = $now WHERE id = $id AND tenant = $tenant"

                            update.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            update.Parameters.AddWithValue("$now", toIso now) |> ignore
                            update.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
                            update.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            update.ExecuteNonQuery() |> ignore
                            transaction.Commit()

                            { session with
                                AgentId = agentId
                                UpdatedAt = now
                            })
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.SetSessionTitle(tenant, sessionId, title, _) =
            task {
                if isNull (box title) then
                    raise (ArgumentNullException(nameof title))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            let session = requireSessionRow connection transaction tenant sessionId
                            let now = database.UtcNow

                            use update = connection.CreateCommand()
                            update.Transaction <- transaction

                            update.CommandText <-
                                $"UPDATE \"%s{sessionsTable ()}\" SET title = $title, updated_at = $now WHERE id = $id AND tenant = $tenant"

                            update.Parameters.AddWithValue("$title", title) |> ignore
                            update.Parameters.AddWithValue("$now", toIso now) |> ignore
                            update.Parameters.AddWithValue("$id", sessionId.Value) |> ignore
                            update.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            update.ExecuteNonQuery() |> ignore
                            transaction.Commit()

                            { session with
                                Title = title
                                UpdatedAt = now
                            })
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.AppendInboxMessage(tenant, sessionId, payload, delivery, _) =
            task {
                if isNull (box payload) then
                    raise (ArgumentNullException(nameof payload))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            requireSessionRow connection transaction tenant sessionId |> ignore

                            use next = connection.CreateCommand()
                            next.Transaction <- transaction

                            next.CommandText <-
                                $"SELECT COALESCE(MAX(position), 0) FROM \"%s{inboxTable ()}\" WHERE session_id = $session"

                            next.Parameters.AddWithValue("$session", sessionId.Value) |> ignore

                            let position =
                                match next.ExecuteScalar() with
                                | null -> 1L
                                | :? int64 as max -> max + 1L
                                | :? int as max -> int64 max + 1L
                                | value -> Convert.ToInt64 value + 1L

                            let now = database.UtcNow
                            let payloadJson = SqliteJson.serialize payload

                            use insert = connection.CreateCommand()
                            insert.Transaction <- transaction

                            insert.CommandText <-
                                $"INSERT INTO \"%s{inboxTable ()}\" (session_id, position, tenant, payload_json, delivery_mode, consumed, appended_at) VALUES ($session, $position, $tenant, $payload, $delivery, 0, $at)"

                            insert.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                            insert.Parameters.AddWithValue("$position", position) |> ignore
                            insert.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            insert.Parameters.AddWithValue("$payload", payloadJson) |> ignore
                            insert.Parameters.AddWithValue("$delivery", deliveryName delivery) |> ignore
                            insert.Parameters.AddWithValue("$at", toIso now) |> ignore
                            insert.ExecuteNonQuery() |> ignore
                            transaction.Commit()

                            {
                                SessionId = sessionId
                                Position = position
                                Payload = payload
                                Delivery = delivery
                                Consumed = false
                                AppendedAt = now
                            })
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.ReadPendingInbox(tenant, sessionId, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            requireSessionRow connection null tenant sessionId |> ignore

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT session_id, position, payload_json, delivery_mode, consumed, appended_at FROM \"%s{inboxTable ()}\" WHERE session_id = $session AND consumed = 0 ORDER BY position"

                            command.Parameters.AddWithValue("$session", sessionId.Value) |> ignore

                            use reader = command.ExecuteReader()
                            let entries = List<InboxEntry>()

                            while reader.Read() do
                                entries.Add(readInboxEntry reader)

                            entries :> IReadOnlyList<InboxEntry>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.MarkInboxConsumed(tenant, sessionId, positions, _) =
            task {
                if isNull (box positions) then
                    raise (ArgumentNullException(nameof positions))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            requireSessionRow connection transaction tenant sessionId |> ignore
                            let mutable flipped = 0

                            for position in positions do
                                use update = connection.CreateCommand()
                                update.Transaction <- transaction

                                update.CommandText <-
                                    $"UPDATE \"%s{inboxTable ()}\" SET consumed = 1 WHERE session_id = $session AND position = $position AND consumed = 0"

                                update.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                update.Parameters.AddWithValue("$position", position) |> ignore
                                flipped <- flipped + update.ExecuteNonQuery()

                            transaction.Commit()
                            flipped)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.ClaimNextTurn(tenant, sessionId, owner, leaseDuration, _) =
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
                            requireSessionRow connection transaction tenant sessionId |> ignore
                            let now = database.UtcNow
                            let expiresAt = now + leaseDuration

                            if liveClaimHeld connection transaction tenant sessionId then
                                transaction.Rollback()
                                TurnLeaseMissing(TurnId.New()) :> TurnLeaseState
                            else
                                let pending = headPending connection transaction sessionId
                                let openTurn = openTurnRow connection transaction tenant sessionId

                                match pending, openTurn with
                                | Some(position, (:? ReplyPayload as _reply)), Some(turnId, attempt) ->
                                    consumePosition connection transaction sessionId position
                                    let nextAttempt = attempt + 1
                                    let token = mintToken ()

                                    let claim =
                                        {
                                            TurnId = turnId
                                            Token = token
                                            Owner = owner
                                            ExpiresAt = expiresAt
                                            Attempt = nextAttempt
                                        }

                                    use update = connection.CreateCommand()
                                    update.Transaction <- transaction

                                    update.CommandText <-
                                        $"UPDATE \"%s{turnsTable ()}\" SET attempt = $attempt, claim_token = $token, claim_owner = $owner, claim_expires_at = $expires WHERE turn_id = $turn"

                                    update.Parameters.AddWithValue("$attempt", nextAttempt) |> ignore
                                    update.Parameters.AddWithValue("$token", token) |> ignore
                                    update.Parameters.AddWithValue("$owner", owner) |> ignore
                                    update.Parameters.AddWithValue("$expires", toIso expiresAt) |> ignore
                                    update.Parameters.AddWithValue("$turn", turnId.Value) |> ignore
                                    update.ExecuteNonQuery() |> ignore
                                    stampCurrentTurn connection transaction tenant sessionId (Some turnId) now
                                    transaction.Commit()
                                    TurnLeaseRenewed claim :> TurnLeaseState
                                | Some(position, (:? UserMessagePayload as _message)), _ ->
                                    consumePosition connection transaction sessionId position

                                    // A stale open turn from a lapsed claim is abandoned.
                                    use clear = connection.CreateCommand()
                                    clear.Transaction <- transaction

                                    clear.CommandText <-
                                        $"DELETE FROM \"%s{turnsTable ()}\" WHERE session_id = $session AND tenant = $tenant AND status IN (%s{nonTerminalFilter})"

                                    clear.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                    clear.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                    clear.ExecuteNonQuery() |> ignore

                                    let turnId = TurnId.New()
                                    let token = mintToken ()

                                    let claim =
                                        {
                                            TurnId = turnId
                                            Token = token
                                            Owner = owner
                                            ExpiresAt = expiresAt
                                            Attempt = 1
                                        }

                                    use insert = connection.CreateCommand()
                                    insert.Transaction <- transaction

                                    insert.CommandText <-
                                        $"INSERT INTO \"%s{turnsTable ()}\" (turn_id, session_id, tenant, attempt, status, claim_token, claim_owner, claim_expires_at, started_at, completed_at, iterations, input_tokens, output_tokens, error, stop_cause, outcome_json) VALUES ($turn, $session, $tenant, 1, $status, $token, $owner, $expires, $started, NULL, 0, 0, 0, NULL, NULL, NULL)"

                                    insert.Parameters.AddWithValue("$turn", turnId.Value) |> ignore
                                    insert.Parameters.AddWithValue("$session", sessionId.Value) |> ignore
                                    insert.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                                    insert.Parameters.AddWithValue("$status", turnStatusName TurnStatus.Running)
                                    |> ignore

                                    insert.Parameters.AddWithValue("$token", token) |> ignore
                                    insert.Parameters.AddWithValue("$owner", owner) |> ignore
                                    insert.Parameters.AddWithValue("$expires", toIso expiresAt) |> ignore
                                    insert.Parameters.AddWithValue("$started", toIso now) |> ignore
                                    insert.ExecuteNonQuery() |> ignore
                                    stampCurrentTurn connection transaction tenant sessionId (Some turnId) now
                                    transaction.Commit()
                                    TurnLeaseRenewed claim :> TurnLeaseState
                                | _ ->
                                    transaction.Rollback()
                                    TurnLeaseMissing(TurnId.New()) :> TurnLeaseState)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.RenewClaim(tenant, claim, leaseDuration, _) =
            task {
                if isNull (box claim) then
                    raise (ArgumentNullException(nameof claim))

                if leaseDuration <= TimeSpan.Zero then
                    raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            match resolveClaim connection transaction tenant claim with
                            | Live(_, _, _, _, _) ->
                                let renewed =
                                    { claim with
                                        ExpiresAt = database.UtcNow + leaseDuration
                                    }

                                use update = connection.CreateCommand()
                                update.Transaction <- transaction

                                update.CommandText <-
                                    $"UPDATE \"%s{turnsTable ()}\" SET claim_expires_at = $expires WHERE turn_id = $turn"

                                update.Parameters.AddWithValue("$expires", toIso renewed.ExpiresAt) |> ignore
                                update.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                update.ExecuteNonQuery() |> ignore
                                transaction.Commit()
                                TurnLeaseRenewed renewed :> TurnLeaseState
                            | TakenOver ->
                                // Distinguish expiry from takeover by reading the live row.
                                use command = connection.CreateCommand()
                                command.Transaction <- transaction

                                command.CommandText <-
                                    $"SELECT claim_token, claim_expires_at FROM \"%s{turnsTable ()}\" WHERE turn_id = $turn AND tenant = $tenant"

                                command.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                                use reader = command.ExecuteReader()

                                let outcome =
                                    if reader.Read() && not (reader.IsDBNull(0)) then
                                        let stored = reader.GetString(0)

                                        if String.Equals(stored, claim.Token, StringComparison.Ordinal) then
                                            TurnLeaseLost(claim.TurnId, "expired") :> TurnLeaseState
                                        else
                                            TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                                    else
                                        TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState

                                transaction.Rollback()
                                outcome
                            | Absent ->
                                transaction.Rollback()
                                TurnLeaseMissing claim.TurnId :> TurnLeaseState)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member this.ObserveAndRenewClaim(tenant, claim, leaseDuration, cancellationToken) =
            (this :> ISessionStore).RenewClaim(tenant, claim, leaseDuration, cancellationToken)

        member _.VerifyClaim(tenant, claim, _) =
            task {
                if isNull (box claim) then
                    raise (ArgumentNullException(nameof claim))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            match resolveClaim connection null tenant claim with
                            | Live(_, _, _, expiresAt, _) ->
                                let live = { claim with ExpiresAt = expiresAt }

                                TurnLeaseHeld live :> TurnLeaseState
                            | TakenOver -> TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                            | Absent -> TurnLeaseMissing claim.TurnId :> TurnLeaseState)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.CheckpointUsage(tenant, claim, usage, _) =
            task {
                if isNull (box claim) then
                    raise (ArgumentNullException(nameof claim))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            match resolveClaim connection transaction tenant claim with
                            | Live(_, _, _, expiresAt, _) ->
                                use update = connection.CreateCommand()
                                update.Transaction <- transaction

                                update.CommandText <-
                                    $"UPDATE \"%s{turnsTable ()}\" SET input_tokens = $input, output_tokens = $output WHERE turn_id = $turn"

                                update.Parameters.AddWithValue("$input", usage.InputTokens) |> ignore
                                update.Parameters.AddWithValue("$output", usage.OutputTokens) |> ignore
                                update.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                update.ExecuteNonQuery() |> ignore
                                transaction.Commit()

                                let live = { claim with ExpiresAt = expiresAt }

                                TurnLeaseHeld live :> TurnLeaseState
                            | TakenOver ->
                                transaction.Rollback()
                                TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                            | Absent ->
                                transaction.Rollback()
                                TurnLeaseMissing claim.TurnId :> TurnLeaseState)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.SettleTurn(tenant, claim, status, outcome, _) =
            task {
                if isNull (box claim) then
                    raise (ArgumentNullException(nameof claim))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            if not (isTerminalStatus status) then
                                use locate = connection.CreateCommand()
                                locate.Transaction <- transaction

                                locate.CommandText <-
                                    $"SELECT session_id FROM \"%s{turnsTable ()}\" WHERE turn_id = $turn AND tenant = $tenant"

                                locate.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                locate.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                                use reader = locate.ExecuteReader()

                                let sessionId =
                                    if reader.Read() then
                                        SessionId.Parse(reader.GetString(0))
                                    else
                                        SessionId.New()

                                raise (
                                    InvalidSessionStateException(
                                        sessionId,
                                        "nonTerminal",
                                        "Only terminal statuses (Completed, Aborted, Failed) settle a turn."
                                    )
                                )

                            match resolveClaim connection transaction tenant claim with
                            | Live(sessionId, _, _, _, _) ->
                                let outcomeJson =
                                    if isNull (box outcome) then
                                        null
                                    else
                                        SqliteJson.serialize outcome

                                let now = database.UtcNow

                                use update = connection.CreateCommand()
                                update.Transaction <- transaction

                                update.CommandText <-
                                    $"UPDATE \"%s{turnsTable ()}\" SET status = $status, outcome_json = $outcome, completed_at = $completed, claim_token = NULL, claim_owner = NULL, claim_expires_at = NULL WHERE turn_id = $turn"

                                update.Parameters.AddWithValue("$status", turnStatusName status) |> ignore

                                update.Parameters.AddWithValue(
                                    "$outcome",
                                    (if isNull (box outcomeJson) then
                                         box DBNull.Value
                                     else
                                         box outcomeJson)
                                )
                                |> ignore

                                update.Parameters.AddWithValue("$completed", toIso now) |> ignore
                                update.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                update.ExecuteNonQuery() |> ignore
                                stampCurrentTurn connection transaction tenant sessionId None now
                                transaction.Commit()
                                TurnSettled(claim.TurnId, status) :> TurnSettlement
                            | TakenOver ->
                                transaction.Rollback()
                                TurnSettleRejected(claim.TurnId, "staleClaim") :> TurnSettlement
                            | Absent ->
                                use applied = connection.CreateCommand()
                                applied.Transaction <- transaction

                                applied.CommandText <-
                                    $"SELECT status FROM \"%s{turnsTable ()}\" WHERE turn_id = $turn AND tenant = $tenant AND status NOT IN (%s{nonTerminalFilter})"

                                applied.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                applied.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                                use reader = applied.ExecuteReader()

                                let outcome =
                                    if reader.Read() then
                                        let settled = Enum.Parse<TurnStatus>(reader.GetString(0), false)

                                        if settled = status then
                                            TurnAlreadySettled claim.TurnId :> TurnSettlement
                                        else
                                            TurnSettleRejected(claim.TurnId, "alreadySettledByOther") :> TurnSettlement
                                    else
                                        TurnSettleRejected(claim.TurnId, "staleClaim") :> TurnSettlement

                                transaction.Rollback()
                                outcome)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.AbortTurn(tenant, claim, _) =
            task {
                if isNull (box claim) then
                    raise (ArgumentNullException(nameof claim))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            match resolveClaim connection transaction tenant claim with
                            | Live(sessionId, _, _, expiresAt, _) ->
                                let now = database.UtcNow

                                use update = connection.CreateCommand()
                                update.Transaction <- transaction

                                update.CommandText <-
                                    $"UPDATE \"%s{turnsTable ()}\" SET status = $status, completed_at = $completed, claim_token = NULL, claim_owner = NULL, claim_expires_at = NULL WHERE turn_id = $turn"

                                update.Parameters.AddWithValue("$status", turnStatusName TurnStatus.Aborted)
                                |> ignore

                                update.Parameters.AddWithValue("$completed", toIso now) |> ignore
                                update.Parameters.AddWithValue("$turn", claim.TurnId.Value) |> ignore
                                update.ExecuteNonQuery() |> ignore
                                stampCurrentTurn connection transaction tenant sessionId None now
                                transaction.Commit()

                                let live = { claim with ExpiresAt = expiresAt }

                                TurnLeaseHeld live :> TurnLeaseState
                            | TakenOver ->
                                transaction.Rollback()
                                TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                            | Absent ->
                                transaction.Rollback()
                                TurnLeaseMissing claim.TurnId :> TurnLeaseState)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.EnqueueCompletionOutbox(tenant, completion, _) =
            task {
                if isNull (box completion) then
                    raise (ArgumentNullException(nameof completion))

                if String.IsNullOrWhiteSpace completion.IdempotencyKey then
                    raise (
                        ArgumentException(
                            "The completion's idempotency key must be a non-empty string.",
                            nameof completion
                        )
                    )

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            requireSessionRow connection transaction tenant completion.SessionId |> ignore

                            use check = connection.CreateCommand()
                            check.Transaction <- transaction

                            check.CommandText <-
                                $"SELECT idempotency_key, tenant, session_id, completion_json, created_at, delivered, delivered_at, lease_owner, lease_expires_at FROM \"%s{outboxTable ()}\" WHERE idempotency_key = $key"

                            check.Parameters.AddWithValue("$key", completion.IdempotencyKey) |> ignore

                            use reader = check.ExecuteReader()

                            if reader.Read() then
                                let entry = readOutboxEntry reader
                                transaction.Rollback()
                                entry
                            else
                                reader.Close()
                                let now = database.UtcNow
                                let completionJson = SqliteJson.serialize completion

                                use insert = connection.CreateCommand()
                                insert.Transaction <- transaction

                                insert.CommandText <-
                                    $"INSERT INTO \"%s{outboxTable ()}\" (idempotency_key, tenant, session_id, completion_json, created_at, delivered, delivered_at, lease_owner, lease_expires_at) VALUES ($key, $tenant, $session, $completion, $created, 0, NULL, NULL, NULL)"

                                insert.Parameters.AddWithValue("$key", completion.IdempotencyKey) |> ignore
                                insert.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                insert.Parameters.AddWithValue("$session", completion.SessionId.Value) |> ignore
                                insert.Parameters.AddWithValue("$completion", completionJson) |> ignore
                                insert.Parameters.AddWithValue("$created", toIso now) |> ignore
                                insert.ExecuteNonQuery() |> ignore
                                transaction.Commit()

                                {
                                    Tenant = tenant
                                    SessionId = completion.SessionId
                                    IdempotencyKey = completion.IdempotencyKey
                                    Completion = completion
                                    CreatedAt = now
                                    Delivered = false
                                    DeliveredAt = Nullable()
                                    LeaseOwner = null
                                    LeaseExpiresAt = Nullable()
                                })
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.ClaimCompletionOutbox(owner, maxBatch, leaseDuration, _) =
            task {
                if isNull (box owner) then
                    raise (ArgumentNullException(nameof owner))

                if maxBatch <= 0 then
                    raise (ArgumentOutOfRangeException(nameof maxBatch, "The batch size must be positive."))

                if leaseDuration <= TimeSpan.Zero then
                    raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            let now = database.UtcNow
                            let nowText = toIso now

                            use pick = connection.CreateCommand()
                            pick.Transaction <- transaction

                            pick.CommandText <-
                                $"SELECT idempotency_key FROM \"%s{outboxTable ()}\" WHERE delivered = 0 AND (lease_owner IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= $now) ORDER BY created_at LIMIT $limit"

                            pick.Parameters.AddWithValue("$now", nowText) |> ignore
                            pick.Parameters.AddWithValue("$limit", maxBatch) |> ignore

                            use reader = pick.ExecuteReader()
                            let keys = List<string>()

                            while reader.Read() do
                                keys.Add(reader.GetString(0))

                            reader.Close()

                            let expires = now + leaseDuration
                            let entries = List<CompletionOutboxEntry>()

                            for key in keys do
                                use lease = connection.CreateCommand()
                                lease.Transaction <- transaction

                                lease.CommandText <-
                                    $"UPDATE \"%s{outboxTable ()}\" SET lease_owner = $owner, lease_expires_at = $expires WHERE idempotency_key = $key"

                                lease.Parameters.AddWithValue("$owner", owner) |> ignore
                                lease.Parameters.AddWithValue("$expires", toIso expires) |> ignore
                                lease.Parameters.AddWithValue("$key", key) |> ignore
                                lease.ExecuteNonQuery() |> ignore

                                use fetch = connection.CreateCommand()
                                fetch.Transaction <- transaction

                                fetch.CommandText <-
                                    $"SELECT idempotency_key, tenant, session_id, completion_json, created_at, delivered, delivered_at, lease_owner, lease_expires_at FROM \"%s{outboxTable ()}\" WHERE idempotency_key = $key"

                                fetch.Parameters.AddWithValue("$key", key) |> ignore

                                use row = fetch.ExecuteReader()

                                if row.Read() then
                                    entries.Add(readOutboxEntry row)

                            transaction.Commit()
                            entries :> IReadOnlyList<CompletionOutboxEntry>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.VerifyCompletionClaim(tenant, idempotencyKey, owner, _) =
            task {
                if String.IsNullOrWhiteSpace idempotencyKey then
                    raise (ArgumentException("The idempotency key must be a non-empty string.", nameof idempotencyKey))

                if isNull (box owner) then
                    raise (ArgumentNullException(nameof owner))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT delivered, lease_owner, lease_expires_at FROM \"%s{outboxTable ()}\" WHERE idempotency_key = $key AND tenant = $tenant"

                            command.Parameters.AddWithValue("$key", idempotencyKey) |> ignore
                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use reader = command.ExecuteReader()

                            if not (reader.Read()) then
                                false
                            elif reader.GetInt64(0) <> 0L then
                                false
                            elif reader.IsDBNull(1) then
                                false
                            elif not (String.Equals(reader.GetString(1), owner, StringComparison.Ordinal)) then
                                false
                            elif reader.IsDBNull(2) then
                                false
                            else
                                ofIso (reader.GetString(2)) > database.UtcNow)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.MarkCompletionDelivered(tenant, idempotencyKey, owner, _) =
            task {
                if String.IsNullOrWhiteSpace idempotencyKey then
                    raise (ArgumentException("The idempotency key must be a non-empty string.", nameof idempotencyKey))

                if isNull (box owner) then
                    raise (ArgumentNullException(nameof owner))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            use command = connection.CreateCommand()
                            command.Transaction <- transaction

                            command.CommandText <-
                                $"SELECT delivered, lease_owner, lease_expires_at FROM \"%s{outboxTable ()}\" WHERE idempotency_key = $key AND tenant = $tenant"

                            command.Parameters.AddWithValue("$key", idempotencyKey) |> ignore
                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use reader = command.ExecuteReader()

                            if not (reader.Read()) then
                                transaction.Rollback()
                                false
                            elif reader.GetInt64(0) <> 0L then
                                transaction.Rollback()
                                true
                            elif
                                reader.IsDBNull(1)
                                || not (String.Equals(reader.GetString(1), owner, StringComparison.Ordinal))
                                || reader.IsDBNull(2)
                                || ofIso (reader.GetString(2)) <= database.UtcNow
                            then
                                transaction.Rollback()
                                false
                            else
                                reader.Close()
                                let now = database.UtcNow

                                use mark = connection.CreateCommand()
                                mark.Transaction <- transaction

                                mark.CommandText <-
                                    $"UPDATE \"%s{outboxTable ()}\" SET delivered = 1, delivered_at = $at, lease_owner = NULL, lease_expires_at = NULL WHERE idempotency_key = $key"

                                mark.Parameters.AddWithValue("$at", toIso now) |> ignore
                                mark.Parameters.AddWithValue("$key", idempotencyKey) |> ignore
                                mark.ExecuteNonQuery() |> ignore
                                transaction.Commit()
                                true)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.PurgeDeliveredCompletions(deliveredBefore, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"DELETE FROM \"%s{outboxTable ()}\" WHERE delivered = 1 AND delivered_at IS NOT NULL AND delivered_at <= $cutoff"

                            command.Parameters.AddWithValue("$cutoff", toIso deliveredBefore) |> ignore
                            command.ExecuteNonQuery())
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.GetDispatchCandidates(tenant, maxBatch, _) =
            task {
                if maxBatch <= 0 then
                    raise (ArgumentOutOfRangeException(nameof maxBatch, "The batch size must be positive."))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT DISTINCT session_id FROM \"%s{inboxTable ()}\" WHERE tenant = $tenant AND consumed = 0 ORDER BY session_id"

                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use reader = command.ExecuteReader()
                            let pending = List<SessionId>()

                            while reader.Read() do
                                pending.Add(SessionId.Parse(reader.GetString(0)))

                            let batch = pending |> Seq.truncate maxBatch |> Seq.toList

                            {
                                Sessions = batch :> IReadOnlyList<SessionId>
                                HasMore = pending.Count > batch.Length
                            })
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.CountSessionsByAgent(tenant, agentId, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT COUNT(*) FROM \"%s{sessionsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent"

                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            command.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            Convert.ToInt32(command.ExecuteScalar()))
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.CountSessionsByTenant(tenant, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT COUNT(*) FROM \"%s{sessionsTable ()}\" WHERE tenant = $tenant"

                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            Convert.ToInt32(command.ExecuteScalar()))
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.CountRunningSessions(_) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT COUNT(*) FROM \"%s{sessionsTable ()}\" WHERE current_turn_id IS NOT NULL"

                            Convert.ToInt32(command.ExecuteScalar()))
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }
