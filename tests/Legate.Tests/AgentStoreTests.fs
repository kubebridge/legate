// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.AgentStoreTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Xunit

// The generic Deserialize<'T> overload is annotated to return 'T | null,
// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

let nullString = Unchecked.defaultof<string>
let jsonOptions = JsonSerializerOptions()

let tenant = TenantId.Create "acme"
let otherTenant = TenantId.Create "other"

let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
let laterStamp = DateTimeOffset(2024, 6, 7, 8, 9, 10, TimeSpan.Zero)

/// Builds an agent with every field set, the common shape for the fake
/// store's rows.
let sampleAgent () =
    {
        Id = AgentId.New()
        Tenant = tenant
        Name = "checkout"
        Description = "Handles checkout questions"
        Model = ModelReference.Parse "anthropic/claude-sonnet"
        SystemPrompt = "You help with checkout."
        EnvironmentVariables = null
        PermissionDefaults = null
        ToolSelection = null
        PackageReference = null
        Enabled = true
        Schedule = null
        RowVersion = 0UL
        CreatedAt = stamp
        UpdatedAt = stamp
    }

/// Builds a custom tool with every field set, the common shape for the
/// fake store's rows.
let sampleCustomTool (agentId: AgentId) =
    {
        Tenant = tenant
        AgentId = agentId
        Name = "lookup_order"
        Description = "Looks up an order by id"
        Endpoint = Uri "https://orders.internal.example/api/orders"
        InputSchema = null
        Headers = null
        SigningSecret = [| 1uy; 2uy; 3uy; 4uy |]
        Enabled = true
        RowVersion = 0UL
        CreatedAt = stamp
        UpdatedAt = stamp
    }

/// An in-memory IAgentStore and IAgentCustomToolStore implemented entirely
/// from outside the assembly: the C#-friendly-surface proof, mirroring
/// SessionsStoreTests.fs's FakeSessionStore. It implements the documented
/// semantics the tests pin, so each test exercises the contract through
/// behaviour rather than reflection.
type FakeAgentStore() =
    let agents = Dictionary<string, Agent>()
    let tools = Dictionary<string, AgentCustomTool>()
    let consumed = HashSet<string>()

    let agentKey (tenantId: TenantId) (agentId: AgentId) =
        sprintf "%s|%s" tenantId.Value agentId.Value

    let toolKey (tenantId: TenantId) (agentId: AgentId) (name: string) =
        sprintf "%s|%s|%s" tenantId.Value agentId.Value name

    let tenantAgents (tenantId: TenantId) =
        agents.Values |> Seq.filter (fun a -> a.Tenant.Equals tenantId) |> Array.ofSeq

    interface IAgentStore with
        member _.GetAgent(t, agentId, _) =
            Task.FromResult(
                match agents.TryGetValue(agentKey t agentId) with
                | true, agent -> agent
                | false, _ -> Unchecked.defaultof<Agent>
            )

        member _.ListAgents(t, _) =
            let listed = ResizeArray<Agent>()
            listed.AddRange(tenantAgents t)
            Task.FromResult(listed :> IReadOnlyList<Agent>)

        member _.UpdateIfUnchanged(t, agent, expectedRowVersion, _) =
            if box agent |> isNull then
                raise (ArgumentNullException(nameof agent))

            let key = agentKey t agent.Id

            match agents.TryGetValue key with
            | false, _ ->
                if expectedRowVersion <> 0UL then
                    raise (AgentNotFoundException(agent.Id, "Agent not found."))

                let stored =
                    { agent with
                        Tenant = t
                        RowVersion = 1UL
                        UpdatedAt = laterStamp
                    }

                agents[key] <- stored
                Task.FromResult(AgentUpdated(stored) :> AgentUpdateOutcome)
            | true, current ->
                if current.RowVersion = expectedRowVersion then
                    let stored =
                        { agent with
                            Tenant = t
                            RowVersion = current.RowVersion + 1UL
                            UpdatedAt = laterStamp
                        }

                    agents[key] <- stored
                    Task.FromResult(AgentUpdated(stored) :> AgentUpdateOutcome)
                else
                    Task.FromResult(AgentUpdateConflict(current) :> AgentUpdateOutcome)

        member _.DeleteAgent(t, agentId, _) =
            Task.FromResult(agents.Remove(agentKey t agentId))

        member _.ListAgentsWithEnabledSchedules(t, _) =
            let listed =
                tenantAgents t
                |> Array.filter (fun a ->
                    match a.Schedule |> Option.ofObj with
                    | Some schedule -> schedule.Enabled
                    | None -> false)

            let result = ResizeArray<Agent>()
            result.AddRange listed
            Task.FromResult(result :> IReadOnlyList<Agent>)

        member _.TryConsumeScheduleOccurrence(t, _agentId, occurrenceKey, _occurrenceUtc, _) =
            if box occurrenceKey |> isNull then
                raise (ArgumentNullException(nameof occurrenceKey))

            let key = sprintf "%s|%s" t.Value occurrenceKey

            if consumed.Add key then
                Task.FromResult(ScheduleOccurrenceConsumed occurrenceKey :> ScheduleOccurrenceOutcome)
            else
                Task.FromResult(ScheduleOccurrenceAlreadyConsumed occurrenceKey :> ScheduleOccurrenceOutcome)

    interface IAgentCustomToolStore with
        member _.ListCustomTools(t, agentId, _) =
            let listed =
                tools.Values
                |> Seq.filter (fun tool -> tool.Tenant.Equals t && tool.AgentId.Equals agentId && tool.Enabled)
                |> ResizeArray

            Task.FromResult(listed :> IReadOnlyList<AgentCustomTool>)

        member _.GetCustomTool(t, agentId, name, _) =
            if isNull (box name) then
                raise (ArgumentNullException(nameof name))

            Task.FromResult(
                match tools.TryGetValue(toolKey t agentId name) with
                | true, tool -> tool
                | false, _ -> Unchecked.defaultof<AgentCustomTool>
            )

        member _.UpsertCustomTool(t, agentId, customTool, _) =
            if box customTool |> isNull then
                raise (ArgumentNullException(nameof customTool))

            let name = ToolNameRules.Validate customTool.Name

            if not (agents.ContainsKey(agentKey t agentId)) then
                raise (AgentNotFoundException(agentId, "Agent not found."))

            let key = toolKey t agentId name

            let stored =
                match tools.TryGetValue key with
                | true, previous ->
                    { customTool with
                        Tenant = t
                        AgentId = agentId
                        Name = name
                        RowVersion = previous.RowVersion + 1UL
                        CreatedAt = previous.CreatedAt
                        UpdatedAt = laterStamp
                    }
                | false, _ ->
                    { customTool with
                        Tenant = t
                        AgentId = agentId
                        Name = name
                        RowVersion = 1UL
                        UpdatedAt = laterStamp
                    }

            tools[key] <- stored
            Task.FromResult stored

        member _.DeleteCustomTool(t, agentId, name, _) =
            if isNull (box name) then
                raise (ArgumentNullException(nameof name))

            Task.FromResult(tools.Remove(toolKey t agentId name))

// ───────────────────────────────────────────────────────────────────────────
// AgentUpdateOutcome

[<Fact>]
let ``AgentUpdateOutcome serialises with stable type discriminators`` () =
    let agent = { sampleAgent () with RowVersion = 1UL }

    let updated = AgentUpdated(agent) :> AgentUpdateOutcome
    JsonSerializer.Serialize(updated, jsonOptions) |> JsonDocument.Parse |> ignore

    let document = JsonSerializer.SerializeToUtf8Bytes(updated, jsonOptions)
    document.Length |> should be (greaterThan 0)

    let conflictJson = """{"$type":"agentUpdateConflict","Agent":null}"""
    let conflict = deserialize<AgentUpdateConflict> conflictJson

    box conflict.Agent |> should equal null

// ───────────────────────────────────────────────────────────────────────────
// IAgentStore

[<Fact>]
let ``GetAgent returns null for an absent agent`` () =
    let store = FakeAgentStore() :> IAgentStore

    task {
        let! absent = store.GetAgent(tenant, AgentId.New(), CancellationToken.None)
        absent |> should equal null
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Store operations are tenant-scoped`` () =
    let store = FakeAgentStore() :> IAgentStore
    let agent = sampleAgent ()

    task {
        let! outcome = store.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        match outcome with
        | :? AgentUpdated as updated ->
            let stored = updated.Agent

            let! crossTenant = store.GetAgent(otherTenant, stored.Id, CancellationToken.None)
            crossTenant |> should equal null

            let! crossTenantList = store.ListAgents(otherTenant, CancellationToken.None)
            crossTenantList.Count |> should equal 0

            let! sameTenant = store.GetAgent(tenant, stored.Id, CancellationToken.None)

            match sameTenant with
            | null -> failwith "the created agent was not found"
            | found -> found.Id |> should equal stored.Id
        | _ -> failwith "expected AgentUpdated"
    }
    |> (fun t -> t.Wait())

/// Asserts the absent upsert inserted the agent at version 1. Pure so
/// the resumable test stays a straight-line await plus a return.
let private checkInsertedAsUpdated (outcome: AgentUpdateOutcome) =
    match outcome with
    | :? AgentUpdated as updated ->
        updated.Agent.RowVersion |> should equal 1UL
        updated.Agent.UpdatedAt |> should equal laterStamp
        updated.Agent.Tenant |> should equal tenant
    | _ -> failwith "expected AgentUpdated"

/// Asserts the inserted agent reads back at version 1. Pure so the
/// resumable test stays a straight-line await plus a return.
let private checkInsertedRead (read: Agent | null) =
    match read with
    | null -> failwith "the inserted agent was not found"
    | found -> found.RowVersion |> should equal 1UL

[<Fact>]
let ``UpdateIfUnchanged inserts when absent with expected version 0`` () =
    let store = FakeAgentStore() :> IAgentStore
    let agent = sampleAgent ()

    task {
        let! outcome = store.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        checkInsertedAsUpdated outcome

        let! read = store.GetAgent(tenant, agent.Id, CancellationToken.None)

        checkInsertedRead read
    }
    |> (fun t -> t.Wait())

/// Asserts the matching update renamed the agent at version 2. Pure so
/// the resumable test stays a straight-line await plus a return.
let private checkIncremented (second: AgentUpdateOutcome) =
    match second with
    | :? AgentUpdated as updated ->
        updated.Agent.Name |> should equal "checkout-v2"
        updated.Agent.RowVersion |> should equal 2UL
        updated.Agent.UpdatedAt |> should equal laterStamp
    | _ -> failwith "expected AgentUpdated"

[<Fact>]
let ``UpdateIfUnchanged increments the version on a matching update`` () =
    let store = FakeAgentStore() :> IAgentStore
    let agent = sampleAgent ()

    task {
        let! first = store.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)
        let inserted = (first :?> AgentUpdated).Agent

        let renamed = { inserted with Name = "checkout-v2" }
        let! second = store.UpdateIfUnchanged(tenant, renamed, inserted.RowVersion, CancellationToken.None)

        checkIncremented second
    }
    |> (fun t -> t.Wait())

/// Asserts the stale edit lost to the concurrent edit and carries the
/// current row. Pure so the resumable test stays a straight-line await
/// plus a return.
let private checkConflict (expectedVersion: uint64) (outcome: AgentUpdateOutcome) =
    match outcome with
    | :? AgentUpdateConflict as conflict ->
        let current = conflict.Agent |> Option.ofObj
        current.Value.Name |> should equal "concurrent-edit"
        current.Value.RowVersion |> should equal expectedVersion
    | _ -> failwith "expected AgentUpdateConflict"

[<Fact>]
let ``UpdateIfUnchanged returns the conflict branch with the current row`` () =
    let store = FakeAgentStore() :> IAgentStore
    let agent = sampleAgent ()

    task {
        let! first = store.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)
        let inserted = (first :?> AgentUpdated).Agent

        let staleEdit = { inserted with Name = "stale-edit" }

        let concurrentEdit =
            { inserted with
                Name = "concurrent-edit"
            }

        let! _ = store.UpdateIfUnchanged(tenant, concurrentEdit, inserted.RowVersion, CancellationToken.None)

        let! outcome = store.UpdateIfUnchanged(tenant, staleEdit, inserted.RowVersion, CancellationToken.None)

        let expectedVersion = inserted.RowVersion + 1UL
        checkConflict expectedVersion outcome
    }
    |> (fun t -> t.Wait())

/// Runs one store call and captures any exception instead of raising, so
/// the test's resumable body stays a straight-line await plus a return.
/// The call is deferred so synchronous throws are captured too.
let private captureCall (call: unit -> Task) : Task<exn option> =
    task {
        try
            do! call ()
            return None
        with ex ->
            return Some ex
    }

/// Asserts the captured outcome is the typed missing-agent failure.
/// Pure so the resumable test stays a straight-line await plus a return.
let private checkAgentNotFound (agentId: AgentId) (captured: exn option) =
    match captured with
    | Some(:? AgentNotFoundException as exn) ->
        exn.AgentId |> should equal agentId
        exn.Message |> should equal "Agent not found."
    | Some unexpected -> failwith $"expected AgentNotFoundException but got {unexpected.GetType().Name}"
    | None -> failwith "expected AgentNotFoundException"

[<Fact>]
let ``UpdateIfUnchanged throws AgentNotFoundException for a missing row with a nonzero expected version`` () =
    let store = FakeAgentStore() :> IAgentStore
    let agent = { sampleAgent () with RowVersion = 4UL }

    task {
        let! captured =
            captureCall (fun () -> store.UpdateIfUnchanged(tenant, agent, 4UL, CancellationToken.None) :> Task)

        checkAgentNotFound agent.Id captured
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``UpdateIfUnchanged rejects a null agent`` () =
    let store = FakeAgentStore() :> IAgentStore
    let nullAgent = Unchecked.defaultof<Agent>

    task {
        try
            let! _ = store.UpdateIfUnchanged(tenant, nullAgent, 0UL, CancellationToken.None)
            failwith "expected ArgumentNullException"
        with :? ArgumentNullException ->
            ()
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ListAgents returns every agent of the tenant including disabled ones`` () =
    let store = FakeAgentStore() :> IAgentStore

    task {
        let! first = store.UpdateIfUnchanged(tenant, sampleAgent (), 0UL, CancellationToken.None)
        let firstId = (first :?> AgentUpdated).Agent.Id

        let! second =
            store.UpdateIfUnchanged(tenant, { sampleAgent () with Enabled = false }, 0UL, CancellationToken.None)

        let secondId = (second :?> AgentUpdated).Agent.Id

        let listed =
            store.ListAgents(tenant, CancellationToken.None)
            |> Async.AwaitTask
            |> Async.RunSynchronously

        listed.Count |> should equal 2
        listed[0].Id |> should equal firstId
        listed[1].Id |> should equal secondId
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``DeleteAgent is idempotent and returns whether the agent existed`` () =
    let store = FakeAgentStore() :> IAgentStore
    let agent = sampleAgent ()

    task {
        let! _ = store.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        let! deleted = store.DeleteAgent(tenant, agent.Id, CancellationToken.None)
        deleted |> should equal true

        let! again = store.DeleteAgent(tenant, agent.Id, CancellationToken.None)
        again |> should equal false
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ListAgentsWithEnabledSchedules filters on non-null enabled schedules`` () =
    let store = FakeAgentStore() :> IAgentStore

    task {
        // Enabled schedule: listed.
        let! scheduled =
            store.UpdateIfUnchanged(
                tenant,
                { sampleAgent () with
                    Schedule =
                        {
                            Cron = "0 9 * * 1-5"
                            TimeZone = "Europe/Berlin"
                            Message = "Standup"
                            Enabled = true
                        }
                },
                0UL,
                CancellationToken.None
            )

        // Disabled schedule: not listed.
        let! _ =
            store.UpdateIfUnchanged(
                tenant,
                { sampleAgent () with
                    Schedule =
                        {
                            Cron = "0 9 * * 1-5"
                            TimeZone = "Europe/Berlin"
                            Message = "Standup"
                            Enabled = false
                        }
                },
                0UL,
                CancellationToken.None
            )

        // No schedule: not listed.
        let! _ = store.UpdateIfUnchanged(tenant, sampleAgent (), 0UL, CancellationToken.None)

        let! listed = store.ListAgentsWithEnabledSchedules(tenant, CancellationToken.None)

        listed.Count |> should equal 1
        listed[0].Id |> should equal (scheduled :?> AgentUpdated).Agent.Id

        let schedule = listed[0].Schedule |> Option.ofObj
        schedule.Value.Enabled |> should equal true
    }
    |> (fun t -> t.Wait())

// ───────────────────────────────────────────────────────────────────────────
// IAgentCustomToolStore

[<Fact>]
let ``UpsertCustomTool inserts with row version 1 and stamps timestamps`` () =
    let fake = FakeAgentStore()
    let store = fake :> IAgentCustomToolStore
    let agentStore = fake :> IAgentStore
    let agent = sampleAgent ()
    let agentId = agent.Id

    task {
        let! _ = agentStore.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        let! stored = store.UpsertCustomTool(tenant, agentId, sampleCustomTool agentId, CancellationToken.None)

        stored.RowVersion |> should equal 1UL
        stored.CreatedAt |> should equal stamp
        stored.UpdatedAt |> should equal laterStamp
        stored.Tenant |> should equal tenant
        stored.AgentId |> should equal agentId
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``UpsertCustomTool increments the row version and preserves CreatedAt`` () =
    let fake = FakeAgentStore()
    let store = fake :> IAgentCustomToolStore
    let agentStore = fake :> IAgentStore
    let agent = sampleAgent ()
    let agentId = agent.Id

    task {
        let! _ = agentStore.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        let! first = store.UpsertCustomTool(tenant, agentId, sampleCustomTool agentId, CancellationToken.None)

        let! second =
            store.UpsertCustomTool(tenant, agentId, { first with Description = "Updated" }, CancellationToken.None)

        second.RowVersion |> should equal 2UL
        second.CreatedAt |> should equal first.CreatedAt
        second.Description |> should equal "Updated"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``UpsertCustomTool rejects an invalid tool name`` () =
    let fake = FakeAgentStore()
    let store = fake :> IAgentCustomToolStore
    let agentStore = fake :> IAgentStore
    let agent = sampleAgent ()
    let agentId = agent.Id

    task {
        let! _ = agentStore.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        let invalid =
            { sampleCustomTool agentId with
                Name = "not a valid name"
            }

        try
            let! _ = store.UpsertCustomTool(tenant, agentId, invalid, CancellationToken.None)
            failwith "expected ArgumentException"
        with :? ArgumentException ->
            ()
    }
    |> (fun t -> t.Wait())

/// Asserts the captured custom-tool outcome is the typed missing-agent
/// failure. Pure so the resumable test stays a straight-line await plus a
/// return.
let private checkCustomToolAgentNotFound (agentId: AgentId) (captured: exn option) =
    match captured with
    | Some(:? AgentNotFoundException as exn) ->
        exn.AgentId |> should equal agentId
        exn.Message |> should equal "Agent not found."
    | Some unexpected -> failwith $"expected AgentNotFoundException but got {unexpected.GetType().Name}"
    | None -> failwith "expected AgentNotFoundException"

/// Asserts the captured outcome is the typed read-only refusal for the
/// given operation, checking the message when one is expected. Pure so
/// the resumable test stays a straight-line await plus a return.
let private checkReadOnlyRefusal (operation: string) (message: string option) (captured: exn option) =
    match captured with
    | Some(:? ReadOnlyAgentStoreException as exn) ->
        exn.Operation |> should equal operation

        match message with
        | Some expected -> exn.Message |> should equal expected
        | None -> ()
    | Some unexpected -> failwith $"expected ReadOnlyAgentStoreException but got {unexpected.GetType().Name}"
    | None -> failwith "expected ReadOnlyAgentStoreException"

[<Fact>]
let ``UpsertCustomTool throws AgentNotFoundException when the agent is missing`` () =
    let store = FakeAgentStore() :> IAgentCustomToolStore
    let agent = sampleAgent ()
    let agentId = agent.Id

    task {
        let! captured =
            captureCall (fun () ->
                store.UpsertCustomTool(tenant, agentId, sampleCustomTool agentId, CancellationToken.None) :> Task)

        checkCustomToolAgentNotFound agentId captured
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ListCustomTools returns only enabled tools of the agent`` () =
    let fake = FakeAgentStore()
    let store = fake :> IAgentCustomToolStore
    let agentStore = fake :> IAgentStore
    let agent = sampleAgent ()
    let agentId = agent.Id

    task {
        let! _ = agentStore.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        let! enabled = store.UpsertCustomTool(tenant, agentId, sampleCustomTool agentId, CancellationToken.None)

        let! _ =
            store.UpsertCustomTool(
                tenant,
                agentId,
                { sampleCustomTool agentId with
                    Name = "disabled_tool"
                    Enabled = false
                },
                CancellationToken.None
            )

        let! listed = store.ListCustomTools(tenant, agentId, CancellationToken.None)

        listed.Count |> should equal 1
        listed[0].Name |> should equal enabled.Name
        listed[0].Enabled |> should equal true
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Custom tool operations are tenant-scoped and agent-scoped`` () =
    let fake = FakeAgentStore()
    let store = fake :> IAgentCustomToolStore
    let agentStore = fake :> IAgentStore
    let agent = sampleAgent ()
    let agentId = agent.Id
    let otherAgentId = AgentId.New()

    task {
        let! _ = agentStore.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        let! _ = store.UpsertCustomTool(tenant, agentId, sampleCustomTool agentId, CancellationToken.None)

        let! crossTenant = store.ListCustomTools(otherTenant, agentId, CancellationToken.None)
        crossTenant.Count |> should equal 0

        let! crossAgent = store.ListCustomTools(tenant, otherAgentId, CancellationToken.None)
        crossAgent.Count |> should equal 0

        let! crossTenantGet = store.GetCustomTool(otherTenant, agentId, "lookup_order", CancellationToken.None)
        crossTenantGet |> should equal null
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``GetCustomTool returns any state by name and null when absent`` () =
    let fake = FakeAgentStore()
    let store = fake :> IAgentCustomToolStore
    let agentStore = fake :> IAgentStore
    let agent = sampleAgent ()
    let agentId = agent.Id

    task {
        let! _ = agentStore.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        let! stored =
            store.UpsertCustomTool(
                tenant,
                agentId,
                { sampleCustomTool agentId with
                    Enabled = false
                },
                CancellationToken.None
            )

        // Disabled state is still readable by name.
        let! found = store.GetCustomTool(tenant, agentId, "lookup_order", CancellationToken.None)
        found |> should equal stored

        let! absent = store.GetCustomTool(tenant, agentId, "no_such_tool", CancellationToken.None)
        absent |> should equal null
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``DeleteCustomTool is idempotent and returns whether the tool existed`` () =
    let fake = FakeAgentStore()
    let store = fake :> IAgentCustomToolStore
    let agentStore = fake :> IAgentStore
    let agent = sampleAgent ()
    let agentId = agent.Id

    task {
        let! _ = agentStore.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        let! _ = store.UpsertCustomTool(tenant, agentId, sampleCustomTool agentId, CancellationToken.None)

        let! deleted = store.DeleteCustomTool(tenant, agentId, "lookup_order", CancellationToken.None)
        deleted |> should equal true

        let! again = store.DeleteCustomTool(tenant, agentId, "lookup_order", CancellationToken.None)
        again |> should equal false
    }
    |> (fun t -> t.Wait())

// ───────────────────────────────────────────────────────────────────────────
// Read-only implementations

/// A read-only IAgentStore: serves every read, refuses every write with the
/// typed exception, the behaviour the file-based store pins.
type ReadOnlyAgentStore() =
    interface IAgentStore with
        member _.GetAgent(_, _, _) =
            Task.FromResult Unchecked.defaultof<Agent>

        member _.ListAgents(_, _) =
            Task.FromResult(ResizeArray<Agent>() :> IReadOnlyList<Agent>)

        member _.UpdateIfUnchanged(_, _, _, _) =
            raise (ReadOnlyAgentStoreException("UpdateIfUnchanged", "The agent store is read-only."))

        member _.DeleteAgent(_, _, _) =
            raise (ReadOnlyAgentStoreException("DeleteAgent", "The agent store is read-only."))

        member _.ListAgentsWithEnabledSchedules(_, _) =
            Task.FromResult(ResizeArray<Agent>() :> IReadOnlyList<Agent>)

        member _.TryConsumeScheduleOccurrence(_, _, _, _, _) =
            raise (ReadOnlyAgentStoreException("TryConsumeScheduleOccurrence", "The agent store is read-only."))

/// A read-only IAgentCustomToolStore: serves every read, refuses every
/// write with the typed exception.
type ReadOnlyAgentCustomToolStore() =
    interface IAgentCustomToolStore with
        member _.ListCustomTools(_, _, _) =
            Task.FromResult(ResizeArray<AgentCustomTool>() :> IReadOnlyList<AgentCustomTool>)

        member _.GetCustomTool(_, _, _, _) =
            Task.FromResult Unchecked.defaultof<AgentCustomTool>

        member _.UpsertCustomTool(_, _, _, _) =
            raise (ReadOnlyAgentStoreException("UpsertCustomTool", "The custom tool store is read-only."))

        member _.DeleteCustomTool(_, _, _, _) =
            raise (ReadOnlyAgentStoreException("DeleteCustomTool", "The custom tool store is read-only."))

[<Fact>]
let ``A read-only IAgentStore serves reads and throws the typed exception on writes`` () =
    let store = ReadOnlyAgentStore() :> IAgentStore

    task {
        let! absent = store.GetAgent(tenant, AgentId.New(), CancellationToken.None)
        absent |> should equal null

        let! listed = store.ListAgents(tenant, CancellationToken.None)
        listed.Count |> should equal 0

        let! updateOutcome =
            captureCall (fun () -> store.UpdateIfUnchanged(tenant, sampleAgent (), 0UL, CancellationToken.None) :> Task)

        checkReadOnlyRefusal "UpdateIfUnchanged" (Some "The agent store is read-only.") updateOutcome

        let! deleteOutcome =
            captureCall (fun () -> store.DeleteAgent(tenant, AgentId.New(), CancellationToken.None) :> Task)

        checkReadOnlyRefusal "DeleteAgent" None deleteOutcome

        let! consumeOutcome =
            captureCall (fun () ->
                store.TryConsumeScheduleOccurrence(
                    tenant,
                    AgentId.New(),
                    "agent:cron:ticks",
                    DateTimeOffset.UtcNow,
                    CancellationToken.None
                )
                :> Task)

        checkReadOnlyRefusal "TryConsumeScheduleOccurrence" None consumeOutcome
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``A read-only IAgentCustomToolStore serves reads and throws the typed exception on writes`` () =
    let store = ReadOnlyAgentCustomToolStore() :> IAgentCustomToolStore

    task {
        let! listed = store.ListCustomTools(tenant, AgentId.New(), CancellationToken.None)
        listed.Count |> should equal 0

        let! absent = store.GetCustomTool(tenant, AgentId.New(), "lookup_order", CancellationToken.None)
        absent |> should equal null

        let! upsertOutcome =
            captureCall (fun () ->
                store.UpsertCustomTool(tenant, AgentId.New(), sampleCustomTool (AgentId.New()), CancellationToken.None)
                :> Task)

        checkReadOnlyRefusal "UpsertCustomTool" (Some "The custom tool store is read-only.") upsertOutcome

        let! deleteOutcome =
            captureCall (fun () ->
                store.DeleteCustomTool(tenant, AgentId.New(), "lookup_order", CancellationToken.None) :> Task)

        checkReadOnlyRefusal "DeleteCustomTool" None deleteOutcome
    }
    |> (fun t -> t.Wait())
