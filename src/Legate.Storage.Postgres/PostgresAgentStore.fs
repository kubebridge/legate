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

// The PostgreSQL IAgentStore and IAgentCustomToolStore: agent definitions
// with the checked upsert, the schedule query, and the name-keyed custom
// HTTP tools with the plain upsert, all tenant-scoped. The full record
// rides definition_json; the queried dimensions (name, schedule, enabled)
// ride columns kept in agreement at write time and authoritative at read
// time. Every query carries the tenant predicate: isolation is enforced
// here, not only in the host.
type PostgresAgentStore(options: PostgresOptions, timeProvider: TimeProvider) =

    do
        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        if isNull (box timeProvider) then
            raise (ArgumentNullException(nameof timeProvider))

    /// Constructs the store with the system clock.
    /// <param name="options">The PostgreSQL options. Must not be null.</param>
    new(options: PostgresOptions) = PostgresAgentStore(options, TimeProvider.System)

    /// The options the store was constructed with.
    member private _.Options = options

    /// The clock the store stamps with.
    member private _.TimeProvider = timeProvider

    // ── Private helpers ──

    member private _.UtcNow = timeProvider.GetUtcNow()

    member private _.EnsureMigrated() = PostgresMigrations.ensure options

    member private _.AgentsTable = qualified options "agents"
    member private _.ToolsTable = qualified options "custom_tools"

    member private _.AgentColumns =
        "tenant, agent_id, name, row_version, definition_json, schedule_enabled, schedule_cron, schedule_timezone, created_at, updated_at"

    member private _.ToolColumns =
        "tenant, agent_id, name, row_version, enabled, definition_json, created_at, updated_at"

    /// Reads one agent row in AgentColumns order. Key fields come from the
    /// columns; the remaining shape comes from the stored definition.
    member private _.ReadAgent(reader: DbDataReader) : Agent =
        let name = reader.GetString(2)
        let version = reader.GetInt64(3)

        let definition: Agent =
            match JsonSerializer.Deserialize(reader.GetString(4), jsonOptions) with
            | null -> raise (InvalidOperationException("The stored agent definition is null."))
            | decoded -> decoded

        let scheduleEnabled = reader.GetBoolean(5)
        let cron: string | null = getTextOrNull reader 6
        let timeZone: string | null = getTextOrNull reader 7

        let schedule: AgentSchedule | null =
            match cron with
            | null -> null
            | cronText ->
                let zone =
                    match timeZone with
                    | null -> ""
                    | zoneText -> zoneText

                let message =
                    match definition.Schedule with
                    | null -> ""
                    | scheduled -> scheduled.Message

                {
                    Cron = cronText
                    TimeZone = zone
                    Message = message
                    Enabled = scheduleEnabled
                }

        { definition with
            Tenant = TenantId.Create(reader.GetString(0))
            Id = AgentId.Parse(reader.GetString(1))
            Name = name
            Schedule = schedule
            RowVersion = uint64 version
            CreatedAt = parseStamp (reader.GetString(8))
            UpdatedAt = parseStamp (reader.GetString(9))
        }

    /// Reads one custom-tool row in ToolColumns order.
    member private _.ReadTool(reader: DbDataReader) : AgentCustomTool =
        let definition: AgentCustomTool =
            match JsonSerializer.Deserialize(reader.GetString(5), jsonOptions) with
            | null -> raise (InvalidOperationException("The stored custom-tool definition is null."))
            | decoded -> decoded

        { definition with
            Tenant = TenantId.Create(reader.GetString(0))
            AgentId = AgentId.Parse(reader.GetString(1))
            Name = reader.GetString(2)
            RowVersion = uint64 (reader.GetInt64(3))
            Enabled = reader.GetBoolean(4)
            CreatedAt = parseStamp (reader.GetString(6))
            UpdatedAt = parseStamp (reader.GetString(7))
        }

    /// The schedule columns for an agent definition: the enabled flag plus
    /// the nullable cron and time zone.
    member private _.ScheduleColumns(schedule: AgentSchedule | null) : bool * (string | null) * (string | null) =
        match schedule with
        | null -> false, null, null
        | scheduled -> scheduled.Enabled, scheduled.Cron, scheduled.TimeZone

    interface IAgentStore with

        member this.GetAgent(tenant, agentId, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.AgentColumns} FROM {this.AgentsTable} WHERE tenant = @t AND agent_id = @agent"

                textParam cmd "t" (tenant.ToString())
                textParam cmd "agent" (agentId.ToString())

                use reader = cmd.ExecuteReader()

                let row = if reader.Read() then Some(this.ReadAgent(reader)) else None

                reader.Close()
                row |> Option.toObj)
            |> Task.FromResult

        member this.ListAgents(tenant, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.AgentColumns} FROM {this.AgentsTable} WHERE tenant = @t ORDER BY name"

                textParam cmd "t" (tenant.ToString())

                use reader = cmd.ExecuteReader()

                let agents =
                    [
                        while reader.Read() do
                            this.ReadAgent(reader)
                    ]

                reader.Close()
                agents :> IReadOnlyList<Agent>)
            |> Task.FromResult

        member this.UpdateIfUnchanged(tenant, agent, expectedRowVersion, _) =
            if isNull (box agent) then
                raise (ArgumentNullException(nameof agent))

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use lockCmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.AgentColumns} FROM {this.AgentsTable} WHERE tenant = @t AND agent_id = @agent FOR UPDATE"

                textParam lockCmd "t" (tenant.ToString())
                textParam lockCmd "agent" (agent.Id.ToString())

                use lockReader = lockCmd.ExecuteReader()

                let current: Agent option =
                    if lockReader.Read() then
                        Some(this.ReadAgent(lockReader))
                    else
                        None

                lockReader.Close()

                match current with
                | None ->
                    if expectedRowVersion <> 0UL then
                        raise (
                            AgentNotFoundException(
                                agent.Id,
                                sprintf "No agent %O exists in tenant %O." agent.Id tenant
                            )
                        )

                    let now = this.UtcNow
                    let enabled, cron, timeZone = this.ScheduleColumns(agent.Schedule)

                    let stored =
                        { agent with
                            Tenant = tenant
                            RowVersion = 1UL
                            CreatedAt = now
                            UpdatedAt = now
                        }

                    use insertCmd =
                        command
                            connection
                            transaction
                            $"INSERT INTO {this.AgentsTable} (tenant, agent_id, name, row_version, definition_json, schedule_enabled, schedule_cron, schedule_timezone, created_at, updated_at) VALUES (@t, @agent, @name, 1, @definition, @enabled, @cron, @tz, @created, @updated)"

                    textParam insertCmd "t" (tenant.ToString())
                    textParam insertCmd "agent" (stored.Id.ToString())
                    textParam insertCmd "name" stored.Name
                    textParam insertCmd "definition" (serialize<Agent> stored)
                    boolParam insertCmd "enabled" enabled
                    textParam insertCmd "cron" cron
                    textParam insertCmd "tz" timeZone
                    textParam insertCmd "created" (stamp now)
                    textParam insertCmd "updated" (stamp now)
                    insertCmd.ExecuteNonQuery() |> ignore

                    AgentUpdated stored :> AgentUpdateOutcome
                | Some current ->
                    if current.RowVersion <> expectedRowVersion then
                        AgentUpdateConflict current :> AgentUpdateOutcome
                    else
                        let now = this.UtcNow
                        let enabled, cron, timeZone = this.ScheduleColumns(agent.Schedule)

                        let stored =
                            { agent with
                                Tenant = tenant
                                RowVersion = current.RowVersion + 1UL
                                CreatedAt = current.CreatedAt
                                UpdatedAt = now
                            }

                        use updateCmd =
                            command
                                connection
                                transaction
                                $"UPDATE {this.AgentsTable} SET name = @name, row_version = @version, definition_json = @definition, schedule_enabled = @enabled, schedule_cron = @cron, schedule_timezone = @tz, updated_at = @updated WHERE tenant = @t AND agent_id = @agent"

                        textParam updateCmd "name" stored.Name
                        longParam updateCmd "version" (int64 stored.RowVersion)
                        textParam updateCmd "definition" (serialize<Agent> stored)
                        boolParam updateCmd "enabled" enabled
                        textParam updateCmd "cron" cron
                        textParam updateCmd "tz" timeZone
                        textParam updateCmd "updated" (stamp now)
                        textParam updateCmd "t" (tenant.ToString())
                        textParam updateCmd "agent" (stored.Id.ToString())
                        updateCmd.ExecuteNonQuery() |> ignore

                        AgentUpdated stored :> AgentUpdateOutcome)
            |> Task.FromResult

        member this.DeleteAgent(tenant, agentId, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"DELETE FROM {this.AgentsTable} WHERE tenant = @t AND agent_id = @agent"

                textParam cmd "t" (tenant.ToString())
                textParam cmd "agent" (agentId.ToString())
                cmd.ExecuteNonQuery() > 0)
            |> Task.FromResult

        member this.ListAgentsWithEnabledSchedules(tenant, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.AgentColumns} FROM {this.AgentsTable} WHERE tenant = @t AND schedule_enabled = TRUE ORDER BY name"

                textParam cmd "t" (tenant.ToString())

                use reader = cmd.ExecuteReader()

                let agents =
                    [
                        while reader.Read() do
                            this.ReadAgent(reader)
                    ]

                reader.Close()
                agents :> IReadOnlyList<Agent>)
            |> Task.FromResult

    interface IAgentCustomToolStore with

        member this.ListCustomTools(tenant, agentId, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.ToolColumns} FROM {this.ToolsTable} WHERE tenant = @t AND agent_id = @agent AND enabled = TRUE ORDER BY name"

                textParam cmd "t" (tenant.ToString())
                textParam cmd "agent" (agentId.ToString())

                use reader = cmd.ExecuteReader()

                let tools =
                    [
                        while reader.Read() do
                            this.ReadTool(reader)
                    ]

                reader.Close()
                tools :> IReadOnlyList<AgentCustomTool>)
            |> Task.FromResult

        member this.GetCustomTool(tenant, agentId, name, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.ToolColumns} FROM {this.ToolsTable} WHERE tenant = @t AND agent_id = @agent AND name = @name"

                textParam cmd "t" (tenant.ToString())
                textParam cmd "agent" (agentId.ToString())
                textParam cmd "name" name

                use reader = cmd.ExecuteReader()

                let row = if reader.Read() then Some(this.ReadTool(reader)) else None

                reader.Close()
                row |> Option.toObj)
            |> Task.FromResult

        member this.UpsertCustomTool(tenant, agentId, customTool, _) =
            if isNull (box customTool) then
                raise (ArgumentNullException(nameof customTool))

            ToolNameRules.Validate customTool.Name |> ignore

            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use agentCmd =
                    command
                        connection
                        transaction
                        $"SELECT agent_id FROM {this.AgentsTable} WHERE tenant = @t AND agent_id = @agent"

                textParam agentCmd "t" (tenant.ToString())
                textParam agentCmd "agent" (agentId.ToString())

                use agentReader = agentCmd.ExecuteReader()
                let agentFound = agentReader.Read()
                agentReader.Close()

                if not agentFound then
                    raise (AgentNotFoundException(agentId, sprintf "No agent %O exists in tenant %O." agentId tenant))

                use lockCmd =
                    command
                        connection
                        transaction
                        $"SELECT {this.ToolColumns} FROM {this.ToolsTable} WHERE tenant = @t AND agent_id = @agent AND name = @name FOR UPDATE"

                textParam lockCmd "t" (tenant.ToString())
                textParam lockCmd "agent" (agentId.ToString())
                textParam lockCmd "name" customTool.Name

                use lockReader = lockCmd.ExecuteReader()

                let current: AgentCustomTool option =
                    if lockReader.Read() then
                        Some(this.ReadTool(lockReader))
                    else
                        None

                lockReader.Close()

                let now = this.UtcNow

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

                use upsertCmd =
                    command
                        connection
                        transaction
                        $"INSERT INTO {this.ToolsTable} (tenant, agent_id, name, row_version, enabled, definition_json, created_at, updated_at) VALUES (@t, @agent, @name, @version, @enabled, @definition, @created, @updated) ON CONFLICT (tenant, agent_id, name) DO UPDATE SET row_version = @version, enabled = @enabled, definition_json = @definition, updated_at = @updated"

                textParam upsertCmd "t" (tenant.ToString())
                textParam upsertCmd "agent" (agentId.ToString())
                textParam upsertCmd "name" stored.Name
                longParam upsertCmd "version" (int64 stored.RowVersion)
                boolParam upsertCmd "enabled" stored.Enabled
                textParam upsertCmd "definition" (serialize<AgentCustomTool> stored)
                textParam upsertCmd "created" (stamp stored.CreatedAt)
                textParam upsertCmd "updated" (stamp now)
                upsertCmd.ExecuteNonQuery() |> ignore

                stored)
            |> Task.FromResult

        member this.DeleteCustomTool(tenant, agentId, name, _) =
            this.EnsureMigrated()

            transact options (fun connection transaction ->
                use cmd =
                    command
                        connection
                        transaction
                        $"DELETE FROM {this.ToolsTable} WHERE tenant = @t AND agent_id = @agent AND name = @name"

                textParam cmd "t" (tenant.ToString())
                textParam cmd "agent" (agentId.ToString())
                textParam cmd "name" name
                cmd.ExecuteNonQuery() > 0)
            |> Task.FromResult
