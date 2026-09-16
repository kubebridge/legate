// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Sqlite

open System
open System.Collections.Generic
open System.Threading.Tasks
open Legate
open Microsoft.Data.Sqlite

// The SQLite IAgentStore and IAgentCustomToolStore over one shared
// database: agent definitions with the checked upsert, the schedule query,
// and the name-keyed custom HTTP tools with the plain upsert. Custom-tool
// upserts resolve the agent through the shared agent rows and throw
// AgentNotFoundException on a missing agent, which is why both contracts
// live on one type over one database. Every SqliteException funnels through
// the SqliteErrors boundary.

/// <summary>
/// The SQLite <see cref="T:Legate.IAgentStore" /> and
/// <see cref="T:Legate.IAgentCustomToolStore" /> over one shared database
/// file.
/// </summary>
/// <param name="database">The shared database every store uses. Must not be null.</param>
type SqliteAgentStore(database: SqliteDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let path = database.Path
    let mapSql (ex: SqliteException) : LegateException = SqliteErrors.ofSqliteException path ex
    let toIso = SqliteDatabase.ToIso

    let agentsTable () = database.Table "agents"
    let toolsTable () = database.Table "custom_tools"

    let readAgent (reader: SqliteDataReader) : Agent =
        let json = reader.GetString(4)
        SqliteJson.deserialize<Agent> json

    let readTool (reader: SqliteDataReader) : AgentCustomTool =
        let json = reader.GetString(5)
        SqliteJson.deserialize<AgentCustomTool> json

    let agentExists
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (agentId: AgentId)
        : bool =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT agent_id FROM \"%s{agentsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent"

        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
        command.Parameters.AddWithValue("$agent", agentId.Value) |> ignore

        use reader = command.ExecuteReader()
        reader.Read()

    interface IAgentStore with

        member _.GetAgent(tenant, agentId, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT tenant, agent_id, name, row_version, definition_json FROM \"%s{agentsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent"

                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            command.Parameters.AddWithValue("$agent", agentId.Value) |> ignore

                            use reader = command.ExecuteReader()

                            if reader.Read() then
                                readAgent reader
                            else
                                Unchecked.defaultof<Agent>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.ListAgents(tenant, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT tenant, agent_id, name, row_version, definition_json FROM \"%s{agentsTable ()}\" WHERE tenant = $tenant ORDER BY name"

                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use reader = command.ExecuteReader()
                            let agents = List<Agent>()

                            while reader.Read() do
                                agents.Add(readAgent reader)

                            agents :> IReadOnlyList<Agent>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.UpdateIfUnchanged(tenant, agent, expectedRowVersion, _) =
            task {
                if isNull (box agent) then
                    raise (ArgumentNullException(nameof agent))

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            use check = connection.CreateCommand()
                            check.Transaction <- transaction

                            check.CommandText <-
                                $"SELECT definition_json FROM \"%s{agentsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent"

                            check.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            check.Parameters.AddWithValue("$agent", agent.Id.Value) |> ignore

                            use reader = check.ExecuteReader()
                            let hasRow = reader.Read()

                            let current =
                                if hasRow then
                                    Some(SqliteJson.deserialize<Agent> (reader.GetString(0)))
                                else
                                    None

                            reader.Close()

                            match current with
                            | None ->
                                if expectedRowVersion <> 0UL then
                                    raise (
                                        AgentNotFoundException(
                                            agent.Id,
                                            sprintf "No agent %O exists in tenant %O." agent.Id tenant
                                        )
                                    )

                                let now = database.UtcNow

                                let stored =
                                    { agent with
                                        Tenant = tenant
                                        RowVersion = 1UL
                                        CreatedAt = now
                                        UpdatedAt = now
                                    }

                                let json = SqliteJson.serialize stored

                                let enabled, cron, timezone =
                                    match stored.Schedule with
                                    | null -> 0L, box DBNull.Value, box DBNull.Value
                                    | schedule ->
                                        (if schedule.Enabled then 1L else 0L),
                                        (if isNull (box schedule.Cron) then
                                             box DBNull.Value
                                         else
                                             box schedule.Cron),
                                        (if isNull (box schedule.TimeZone) then
                                             box DBNull.Value
                                         else
                                             box schedule.TimeZone)

                                use insert = connection.CreateCommand()
                                insert.Transaction <- transaction

                                insert.CommandText <-
                                    $"INSERT INTO \"%s{agentsTable ()}\" (tenant, agent_id, name, row_version, definition_json, schedule_enabled, schedule_cron, schedule_timezone, created_at, updated_at) VALUES ($tenant, $agent, $name, 1, $json, $enabled, $cron, $timezone, $created, $updated)"

                                insert.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                insert.Parameters.AddWithValue("$agent", agent.Id.Value) |> ignore
                                insert.Parameters.AddWithValue("$name", agent.Name) |> ignore
                                insert.Parameters.AddWithValue("$json", json) |> ignore
                                insert.Parameters.AddWithValue("$enabled", enabled) |> ignore
                                insert.Parameters.AddWithValue("$cron", cron) |> ignore
                                insert.Parameters.AddWithValue("$timezone", timezone) |> ignore
                                insert.Parameters.AddWithValue("$created", toIso now) |> ignore
                                insert.Parameters.AddWithValue("$updated", toIso now) |> ignore
                                insert.ExecuteNonQuery() |> ignore
                                transaction.Commit()
                                AgentUpdated stored :> AgentUpdateOutcome
                            | Some current ->
                                if current.RowVersion <> expectedRowVersion then
                                    transaction.Rollback()
                                    AgentUpdateConflict current :> AgentUpdateOutcome
                                else
                                    let stored =
                                        { agent with
                                            Tenant = tenant
                                            RowVersion = current.RowVersion + 1UL
                                            CreatedAt = current.CreatedAt
                                            UpdatedAt = database.UtcNow
                                        }

                                    let json = SqliteJson.serialize stored

                                    let enabled, cron, timezone =
                                        match stored.Schedule with
                                        | null -> 0L, box DBNull.Value, box DBNull.Value
                                        | schedule ->
                                            (if schedule.Enabled then 1L else 0L),
                                            (if isNull (box schedule.Cron) then
                                                 box DBNull.Value
                                             else
                                                 box schedule.Cron),
                                            (if isNull (box schedule.TimeZone) then
                                                 box DBNull.Value
                                             else
                                                 box schedule.TimeZone)

                                    use update = connection.CreateCommand()
                                    update.Transaction <- transaction

                                    update.CommandText <-
                                        $"UPDATE \"%s{agentsTable ()}\" SET name = $name, row_version = $version, definition_json = $json, schedule_enabled = $enabled, schedule_cron = $cron, schedule_timezone = $timezone, updated_at = $updated WHERE tenant = $tenant AND agent_id = $agent"

                                    update.Parameters.AddWithValue("$name", stored.Name) |> ignore
                                    update.Parameters.AddWithValue("$version", int64 stored.RowVersion) |> ignore
                                    update.Parameters.AddWithValue("$json", json) |> ignore
                                    update.Parameters.AddWithValue("$enabled", enabled) |> ignore
                                    update.Parameters.AddWithValue("$cron", cron) |> ignore
                                    update.Parameters.AddWithValue("$timezone", timezone) |> ignore
                                    update.Parameters.AddWithValue("$updated", toIso stored.UpdatedAt) |> ignore
                                    update.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                    update.Parameters.AddWithValue("$agent", agent.Id.Value) |> ignore
                                    update.ExecuteNonQuery() |> ignore
                                    transaction.Commit()
                                    AgentUpdated stored :> AgentUpdateOutcome)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.DeleteAgent(tenant, agentId, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"DELETE FROM \"%s{agentsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent"

                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            command.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            command.ExecuteNonQuery() > 0)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.ListAgentsWithEnabledSchedules(tenant, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT tenant, agent_id, name, row_version, definition_json FROM \"%s{agentsTable ()}\" WHERE tenant = $tenant AND schedule_enabled = 1 ORDER BY name"

                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore

                            use reader = command.ExecuteReader()
                            let agents = List<Agent>()

                            while reader.Read() do
                                agents.Add(readAgent reader)

                            agents :> IReadOnlyList<Agent>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

    interface IAgentCustomToolStore with

        member _.ListCustomTools(tenant, agentId, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            if not (agentExists connection null tenant agentId) then
                                [] :> IReadOnlyList<AgentCustomTool>
                            else
                                use command = connection.CreateCommand()

                                command.CommandText <-
                                    $"SELECT tenant, agent_id, name, row_version, enabled, definition_json FROM \"%s{toolsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent AND enabled = 1 ORDER BY name"

                                command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                command.Parameters.AddWithValue("$agent", agentId.Value) |> ignore

                                use reader = command.ExecuteReader()
                                let tools = List<AgentCustomTool>()

                                while reader.Read() do
                                    tools.Add(readTool reader)

                                tools :> IReadOnlyList<AgentCustomTool>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.GetCustomTool(tenant, agentId, name, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT tenant, agent_id, name, row_version, enabled, definition_json FROM \"%s{toolsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent AND name = $name"

                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            command.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            command.Parameters.AddWithValue("$name", name) |> ignore

                            use reader = command.ExecuteReader()

                            if reader.Read() then
                                readTool reader
                            else
                                Unchecked.defaultof<AgentCustomTool>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.UpsertCustomTool(tenant, agentId, customTool, _) =
            task {
                if isNull (box customTool) then
                    raise (ArgumentNullException(nameof customTool))

                ToolNameRules.Validate customTool.Name |> ignore

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            if not (agentExists connection transaction tenant agentId) then
                                raise (
                                    AgentNotFoundException(
                                        agentId,
                                        sprintf "No agent %O exists in tenant %O." agentId tenant
                                    )
                                )

                            use check = connection.CreateCommand()
                            check.Transaction <- transaction

                            check.CommandText <-
                                $"SELECT definition_json FROM \"%s{toolsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent AND name = $name"

                            check.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            check.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            check.Parameters.AddWithValue("$name", customTool.Name) |> ignore

                            use reader = check.ExecuteReader()
                            let hasRow = reader.Read()

                            let current =
                                if hasRow then
                                    Some(SqliteJson.deserialize<AgentCustomTool> (reader.GetString(0)))
                                else
                                    None

                            reader.Close()
                            let now = database.UtcNow

                            let stored =
                                match current with
                                | Some existing ->
                                    { customTool with
                                        Tenant = tenant
                                        AgentId = agentId
                                        RowVersion = existing.RowVersion + 1UL
                                        CreatedAt = existing.CreatedAt
                                        UpdatedAt = now
                                    }
                                | None ->
                                    { customTool with
                                        Tenant = tenant
                                        AgentId = agentId
                                        RowVersion = 1UL
                                        CreatedAt = now
                                        UpdatedAt = now
                                    }

                            let json = SqliteJson.serialize stored

                            // The baseline unique key is an index, not a table
                            // constraint, so the upsert is delete-then-insert
                            // inside the transaction: portable across the
                            // SQLite versions the runner supports.
                            use delete = connection.CreateCommand()
                            delete.Transaction <- transaction

                            delete.CommandText <-
                                $"DELETE FROM \"%s{toolsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent AND name = $name"

                            delete.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            delete.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            delete.Parameters.AddWithValue("$name", customTool.Name) |> ignore
                            delete.ExecuteNonQuery() |> ignore

                            use insert = connection.CreateCommand()
                            insert.Transaction <- transaction

                            insert.CommandText <-
                                $"INSERT INTO \"%s{toolsTable ()}\" (tenant, agent_id, name, row_version, enabled, definition_json, created_at, updated_at) VALUES ($tenant, $agent, $name, $version, $enabled, $json, $created, $updated)"

                            insert.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            insert.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            insert.Parameters.AddWithValue("$name", customTool.Name) |> ignore
                            insert.Parameters.AddWithValue("$version", int64 stored.RowVersion) |> ignore

                            insert.Parameters.AddWithValue("$enabled", (if stored.Enabled then 1L else 0L))
                            |> ignore

                            insert.Parameters.AddWithValue("$json", json) |> ignore
                            insert.Parameters.AddWithValue("$created", toIso stored.CreatedAt) |> ignore
                            insert.Parameters.AddWithValue("$updated", toIso now) |> ignore
                            insert.ExecuteNonQuery() |> ignore
                            transaction.Commit()
                            stored)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.DeleteCustomTool(tenant, agentId, name, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"DELETE FROM \"%s{toolsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent AND name = $name"

                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            command.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            command.Parameters.AddWithValue("$name", name) |> ignore
                            command.ExecuteNonQuery() > 0)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }
