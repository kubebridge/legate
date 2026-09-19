// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.InMemory

open System
open System.Collections.Generic
open System.Linq
open System.Threading
open System.Threading.Tasks
open Legate

/// The in-memory <see cref="T:Legate.IAgentStore" /> and
/// <see cref="T:Legate.IAgentCustomToolStore" />: agent definitions with
/// the checked upsert, the schedule query, and the name-keyed custom HTTP
/// tools with the plain upsert. Custom-tool upserts resolve the agent
/// through the shared database's agent rows and throw
/// <see cref="T:Legate.AgentNotFoundException" /> on a missing agent,
/// which is why both stores live over one database. All transitions run
/// under the database's gate lock.
type InMemoryAgentStore(database: InMemoryDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let ok value = Task.FromResult value

    let agentRow (tenant: TenantId) (agentId: AgentId) =
        match database.Agents.TryGetValue((tenant, agentId)) with
        | true, row -> Some row
        | false, _ -> None

    let toolsOf (tenant: TenantId) (agentId: AgentId) =
        match database.CustomTools.TryGetValue((tenant, agentId)) with
        | true, tools -> tools
        | false, _ ->
            let tools = Dictionary<string, AgentCustomTool>()
            database.CustomTools[(tenant, agentId)] <- tools
            tools

    interface IAgentStore with

        member _.GetAgent(tenant, agentId, _) =
            lock database.Gate (fun () -> agentRow tenant agentId) |> Option.toObj |> ok

        member _.ListAgents(tenant, _) =
            lock database.Gate (fun () ->
                database.Agents.Values
                |> Seq.filter (fun agent -> agent.Tenant.Equals tenant)
                |> Seq.sortBy (fun agent -> agent.Name)
                |> Seq.toList
                :> IReadOnlyList<Agent>)
            |> ok

        member _.UpdateIfUnchanged(tenant, agent, expectedRowVersion, _) =
            if isNull (box agent) then
                raise (ArgumentNullException(nameof agent))

            AgentScheduleRules.ValidateSchedule(agent.Schedule, agent.Id)

            lock database.Gate (fun () ->
                match agentRow tenant agent.Id with
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

                    database.Agents[(tenant, agent.Id)] <- stored
                    AgentUpdated stored :> AgentUpdateOutcome
                | Some current ->
                    if current.RowVersion <> expectedRowVersion then
                        AgentUpdateConflict current :> AgentUpdateOutcome
                    else
                        let stored =
                            { agent with
                                Tenant = tenant
                                RowVersion = current.RowVersion + 1UL
                                CreatedAt = current.CreatedAt
                                UpdatedAt = database.UtcNow
                            }

                        database.Agents[(tenant, agent.Id)] <- stored
                        AgentUpdated stored :> AgentUpdateOutcome)
            |> ok

        member _.DeleteAgent(tenant, agentId, _) =
            lock database.Gate (fun () -> database.Agents.Remove((tenant, agentId))) |> ok

        member _.ListAgentsWithEnabledSchedules(tenant, _) =
            lock database.Gate (fun () ->
                database.Agents.Values
                |> Seq.filter (fun agent -> agent.Tenant.Equals tenant)
                |> Seq.filter (fun agent ->
                    match agent.Schedule with
                    | null -> false
                    | schedule -> schedule.Enabled)
                |> Seq.sortBy (fun agent -> agent.Name)
                |> Seq.toList
                :> IReadOnlyList<Agent>)
            |> ok

        member _.TryConsumeScheduleOccurrence(tenant, agentId, occurrenceKey, occurrenceUtc, _) =
            if isNull (box occurrenceKey) then
                raise (ArgumentNullException(nameof occurrenceKey))

            if String.IsNullOrWhiteSpace occurrenceKey then
                raise (ArgumentException("The occurrence key must be a non-empty string.", nameof occurrenceKey))

            lock database.Gate (fun () ->
                match database.ScheduleOccurrences.TryGetValue((tenant, occurrenceKey)) with
                | true, _ -> ScheduleOccurrenceAlreadyConsumed occurrenceKey :> ScheduleOccurrenceOutcome
                | false, _ ->
                    database.ScheduleOccurrences[(tenant, occurrenceKey)] <-
                        ScheduleOccurrenceRow(tenant, agentId, occurrenceKey, occurrenceUtc, database.UtcNow)

                    ScheduleOccurrenceConsumed occurrenceKey :> ScheduleOccurrenceOutcome)
            |> ok

    interface IAgentCustomToolStore with

        member _.ListCustomTools(tenant, agentId, _) =
            lock database.Gate (fun () ->
                let listed =
                    match agentRow tenant agentId with
                    | None -> []
                    | Some _ ->
                        toolsOf tenant agentId
                        |> Seq.map (fun pair -> pair.Value)
                        |> Seq.filter (fun tool -> tool.Enabled)
                        |> Seq.sortBy (fun tool -> tool.Name)
                        |> Seq.toList

                listed :> IReadOnlyList<AgentCustomTool>)
            |> ok

        member _.GetCustomTool(tenant, agentId, name, _) =
            lock database.Gate (fun () ->
                match toolsOf tenant agentId |> fun tools -> tools.TryGetValue name with
                | true, tool -> tool
                | false, _ -> Unchecked.defaultof<AgentCustomTool>)
            |> ok

        member _.UpsertCustomTool(tenant, agentId, customTool, _) =
            if isNull (box customTool) then
                raise (ArgumentNullException(nameof customTool))

            ToolNameRules.Validate customTool.Name |> ignore

            lock database.Gate (fun () ->
                match agentRow tenant agentId with
                | None ->
                    raise (AgentNotFoundException(agentId, sprintf "No agent %O exists in tenant %O." agentId tenant))
                | Some _ ->
                    let tools = toolsOf tenant agentId
                    let now = database.UtcNow

                    let stored =
                        match tools.TryGetValue customTool.Name with
                        | true, current ->
                            { customTool with
                                Tenant = tenant
                                AgentId = agentId
                                RowVersion = current.RowVersion + 1UL
                                CreatedAt = current.CreatedAt
                                UpdatedAt = now
                            }
                        | false, _ ->
                            { customTool with
                                Tenant = tenant
                                AgentId = agentId
                                RowVersion = 1UL
                                CreatedAt = now
                                UpdatedAt = now
                            }

                    tools[customTool.Name] <- stored
                    stored)
            |> ok

        member _.DeleteCustomTool(tenant, agentId, name, _) =
            lock database.Gate (fun () -> toolsOf tenant agentId |> fun tools -> tools.Remove name)
            |> ok
