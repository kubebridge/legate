// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.FileAgentStoreTests

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Agents
open Legate.Storage.InMemory
open Microsoft.Extensions.DependencyInjection
open Xunit

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// Creates an empty scratch directory for one test.
let private freshDir () : string =
    let dir =
        Path.Combine(Path.GetTempPath(), "legate-file-agent-tests", Guid.NewGuid().ToString("N"))

    Directory.CreateDirectory(dir) |> ignore
    dir

/// Writes one `*.md` agent file and returns its path.
let private writeAgent (dir: string) (fileName: string) (content: string) : string =
    let path = Path.Combine(dir, fileName)
    File.WriteAllText(path, content)
    path

/// Builds a backing-store agent row.
let private backingAgent (tenant: TenantId) (name: string) (prompt: string) : Agent =
    {
        Id = AgentId.New()
        Tenant = tenant
        Name = name
        Description = null
        Model = ModelReference.Parse "test/model"
        SystemPrompt = prompt
        EnvironmentVariables = null
        PermissionDefaults = null
        ToolSelection = null
        PackageReference = null
        Enabled = true
        Schedule = null
        RowVersion = 0UL
        CreatedAt = DateTimeOffset.UtcNow
        UpdatedAt = DateTimeOffset.UtcNow
    }

/// Inserts one agent into the backing store and returns the stored row.
let private insertBacking (store: IAgentStore) (tenant: TenantId) (name: string) (prompt: string) : Agent =
    let outcome =
        store
            .UpdateIfUnchanged(tenant, backingAgent tenant name prompt, 0UL, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    match outcome with
    | :? AgentUpdated as updated -> updated.Agent
    | :? AgentUpdateConflict -> failwith "The backing insert unexpectedly conflicted."
    | _ -> failwith "The backing insert returned an unknown outcome."

/// Builds a code-defined agent template the way AgentsBuilder.Add does.
let private codeAgent (name: string) (prompt: string) : Agent =
    {
        Id = AgentId.New()
        Tenant = TenantId.Default
        Name = name
        Description = null
        Model = ModelReference.Parse "test/model"
        SystemPrompt = prompt
        EnvironmentVariables = null
        PermissionDefaults = null
        ToolSelection = null
        PackageReference = null
        Enabled = true
        Schedule = null
        RowVersion = 0UL
        CreatedAt = DateTimeOffset.UtcNow
        UpdatedAt = DateTimeOffset.UtcNow
    }

/// Builds the composite store over one backing database.
let private composite (database: InMemoryDatabase) (directories: string list) (coded: Agent list) : IAgentStore =
    let backing = InMemoryStoreFactory.agentStore database

    FileAgentStore(backing, directories :> IReadOnlyList<string>, coded :> IReadOnlyList<Agent>) :> IAgentStore

/// Prompts keyed by agent name for one tenant.
let private promptsOf (store: IAgentStore) (tenant: TenantId) : Dictionary<string, string> =
    let agents =
        store.ListAgents(tenant, CancellationToken.None).GetAwaiter().GetResult()

    let table = Dictionary<string, string>(StringComparer.Ordinal)

    for agent in agents do
        table[agent.Name] <- agent.SystemPrompt

    table

// ──────────────────────────────────────────────────────────────────────────
// Reads

[<Fact>]
let ``Directory agents list with frontmatter and body prompts`` () =
    let dir = freshDir ()

    try
        writeAgent dir "b.md" "---\nname: beta\ndescription: Second\n---\nBeta prompt.\n"
        |> ignore

        writeAgent dir "a.md" "---\nname: alpha\nmodel: anthropic/claude-sonnet\nenabled: true\n---\nAlpha prompt.\n"
        |> ignore

        let store = composite (InMemoryDatabase()) [ dir ] []

        let agents =
            store.ListAgents(TenantId.Default, CancellationToken.None).GetAwaiter().GetResult()

        agents.Count |> should equal 2
        agents[0].Name |> should equal "alpha"
        agents[0].SystemPrompt |> should equal "Alpha prompt.\n"
        agents[0].Description |> should equal null
        agents[0].Model.Value |> should equal "anthropic/claude-sonnet"
        agents[1].Name |> should equal "beta"

        let found =
            store.GetAgent(TenantId.Default, agents[1].Id, CancellationToken.None).GetAwaiter().GetResult()

        match box found with
        | :? Agent as beta -> beta.SystemPrompt |> should equal "Beta prompt.\n"
        | _ -> failwith "Expected to find the beta agent."

        let missing =
            store.GetAgent(TenantId.Default, AgentId.New(), CancellationToken.None).GetAwaiter().GetResult()

        isNull (box missing) |> should equal true
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Missing directory reads as empty`` () =
    let missing = Path.Combine(freshDir (), "no-such-dir")
    let store = composite (InMemoryDatabase()) [ missing ] []

    let agents =
        store.ListAgents(TenantId.Default, CancellationToken.None).GetAwaiter().GetResult()

    agents.Count |> should equal 0

// ──────────────────────────────────────────────────────────────────────────
// Merge precedence

[<Fact>]
let ``Precedence is backing then directory then code`` () =
    let dir = freshDir ()

    try
        writeAgent dir "shared.md" "---\nname: shared\n---\nDirectory prompt.\n"
        |> ignore

        writeAgent dir "dir-only.md" "---\nname: dir-only\n---\nDir only.\n" |> ignore

        let database = InMemoryDatabase()
        let backing = InMemoryStoreFactory.agentStore database
        insertBacking backing TenantId.Default "shared" "Backing prompt." |> ignore
        insertBacking backing TenantId.Default "backing-only" "Backing only." |> ignore

        let store =
            composite
                database
                [ dir ]
                [
                    codeAgent "shared" "Code prompt."
                    codeAgent "code-only" "Code only."
                ]

        let prompts = promptsOf store TenantId.Default

        prompts["shared"] |> should equal "Code prompt."
        prompts["dir-only"] |> should equal "Dir only.\n"
        prompts["backing-only"] |> should equal "Backing only."
        prompts["code-only"] |> should equal "Code only."
        prompts.Count |> should equal 4
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Directory files win in sorted file order`` () =
    let dir = freshDir ()

    try
        writeAgent dir "b-second.md" "---\nname: shared\n---\nSecond file.\n" |> ignore
        writeAgent dir "a-first.md" "---\nname: shared\n---\nFirst file.\n" |> ignore

        let store = composite (InMemoryDatabase()) [ dir ] []
        let prompts = promptsOf store TenantId.Default

        prompts["shared"] |> should equal "Second file.\n"
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Code entries win in call order`` () =
    let store =
        composite
            (InMemoryDatabase())
            []
            [
                codeAgent "shared" "First call."
                codeAgent "shared" "Second call."
            ]

    let prompts = promptsOf store TenantId.Default

    prompts["shared"] |> should equal "Second call."

// ──────────────────────────────────────────────────────────────────────────
// Read-only writes

[<Fact>]
let ``UpdateIfUnchanged throws the typed read-only exception`` () : Task =
    task {
        let store = composite (InMemoryDatabase()) [] []

        let! ex =
            Assert.ThrowsAsync<ReadOnlyAgentStoreException>(fun () ->
                store.UpdateIfUnchanged(
                    TenantId.Default,
                    backingAgent TenantId.Default "agent" "Prompt.",
                    0UL,
                    CancellationToken.None
                ))

        ex.Operation |> should equal "UpdateIfUnchanged"
    }

[<Fact>]
let ``DeleteAgent throws the typed read-only exception`` () : Task =
    task {
        let store = composite (InMemoryDatabase()) [] []

        let! ex =
            Assert.ThrowsAsync<ReadOnlyAgentStoreException>(fun () ->
                store.DeleteAgent(TenantId.Default, AgentId.New(), CancellationToken.None))

        ex.Operation |> should equal "DeleteAgent"
    }

// ──────────────────────────────────────────────────────────────────────────
// Reload and tenancy

[<Fact>]
let ``Disk changes are visible on the next read with stable ids`` () =
    let dir = freshDir ()

    try
        let path = writeAgent dir "agent.md" "---\nname: Watched\n---\nVersion one.\n"
        let store = composite (InMemoryDatabase()) [ dir ] []

        let first =
            store.ListAgents(TenantId.Default, CancellationToken.None).GetAwaiter().GetResult()

        first.Count |> should equal 1
        first[0].SystemPrompt |> should equal "Version one.\n"

        File.WriteAllText(path, "---\nname: Watched\ndescription: Updated\n---\nVersion two.\n")

        let second =
            store.ListAgents(TenantId.Default, CancellationToken.None).GetAwaiter().GetResult()

        second.Count |> should equal 1
        second[0].SystemPrompt |> should equal "Version two.\n"
        second[0].Description |> should equal "Updated"
        second[0].Id |> should equal first[0].Id
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Other tenants see the backing store alone`` () =
    let dir = freshDir ()

    try
        writeAgent dir "file-agent.md" "---\nname: file-agent\n---\nFile prompt.\n"
        |> ignore

        let database = InMemoryDatabase()
        let backing = InMemoryStoreFactory.agentStore database

        let otherRow =
            insertBacking backing (TenantId.Create "other") "other-agent" "Other prompt."

        insertBacking backing TenantId.Default "backing-agent" "Backing prompt."
        |> ignore

        let store =
            composite
                database
                [ dir ]
                [
                    codeAgent "code-agent" "Code prompt."
                ]

        let other = promptsOf store (TenantId.Create "other")

        other.Count |> should equal 1
        other["other-agent"] |> should equal "Other prompt."

        let hidden =
            store.GetAgent(TenantId.Create "other", otherRow.Id, CancellationToken.None).GetAwaiter().GetResult()

        match box hidden with
        | :? Agent as otherAgent -> otherAgent.Name |> should equal "other-agent"
        | _ -> failwith "Expected to find the other tenant's agent."

        let visible =
            store.ListAgents(TenantId.Default, CancellationToken.None).GetAwaiter().GetResult()

        visible.Count |> should equal 3
    finally
        Directory.Delete(dir, true)

// ──────────────────────────────────────────────────────────────────────────
// Schedules and builder composition

[<Fact>]
let ``Schedule query lists only enabled schedules`` () =
    let database = InMemoryDatabase()
    let backing = InMemoryStoreFactory.agentStore database

    let scheduled =
        { backingAgent TenantId.Default "scheduled" "Prompt." with
            Schedule =
                {
                    Cron = "0 9 * * 1-5"
                    TimeZone = "Europe/Berlin"
                    Message = "Standup"
                    Enabled = true
                }
        }

    backing.UpdateIfUnchanged(TenantId.Default, scheduled, 0UL, CancellationToken.None).GetAwaiter().GetResult()
    |> ignore

    let disabledCode =
        { codeAgent "code-scheduled" "Code prompt." with
            Schedule =
                {
                    Cron = "0 9 * * 1-5"
                    TimeZone = "Europe/Berlin"
                    Message = "Standup"
                    Enabled = false
                }
        }

    let store = composite database [] [ disabledCode ]

    let listed =
        store.ListAgentsWithEnabledSchedules(TenantId.Default, CancellationToken.None).GetAwaiter().GetResult()

    listed.Count |> should equal 1
    listed[0].Name |> should equal "scheduled"

[<Fact>]
let ``AddLegate composes file layers over the backing store`` () =
    let dir = freshDir ()

    try
        writeAgent dir "file-agent.md" "---\nname: file-agent\n---\nFile prompt.\n"
        |> ignore

        let database = InMemoryDatabase()
        let backing = InMemoryStoreFactory.agentStore database

        insertBacking backing TenantId.Default "backing-agent" "Backing prompt."
        |> ignore

        let services = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(
            services,
            Action<LegateBuilder>(fun builder ->
                builder.Agents.UseStore(backing) |> ignore
                builder.Agents.AddFromDirectory(dir) |> ignore

                builder.Agents.Add(
                    "code-agent",
                    Func<Agent, Agent>(fun agent ->
                        { agent with
                            SystemPrompt = "Code prompt."
                        })
                )
                |> ignore)
        )
        |> ignore

        use provider = services.BuildServiceProvider()
        let store = provider.GetRequiredService<IAgentStore>()
        let prompts = promptsOf store TenantId.Default

        prompts["backing-agent"] |> should equal "Backing prompt."
        prompts["file-agent"] |> should equal "File prompt.\n"
        prompts["code-agent"] |> should equal "Code prompt."
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``AddLegate without file layers keeps the backing registration`` () =
    let database = InMemoryDatabase()
    let backing = InMemoryStoreFactory.agentStore database

    let services = ServiceCollection()

    LegateServiceCollectionExtensions.AddLegate(
        services,
        Action<LegateBuilder>(fun builder -> builder.Agents.UseStore(backing) |> ignore)
    )
    |> ignore

    use provider = services.BuildServiceProvider()
    let store = provider.GetRequiredService<IAgentStore>()

    store |> should equal backing

// ──────────────────────────────────────────────────────────────────────────
// Tools allowlist mapping

/// Reads the single agent in the directory store.
let private singleAgent (store: IAgentStore) : Agent =
    let agents =
        store.ListAgents(TenantId.Default, CancellationToken.None).GetAwaiter().GetResult()

    agents.Count |> should equal 1
    agents[0]

/// The built-in names of one agent's tool selection; empty reads as none
/// (the absent-tools null default and an empty list both surface here, so
/// null-default assertions use the selection itself).
let private builtInsOf (agent: Agent) : string list =
    match agent.ToolSelection with
    | null -> []
    | selection -> selection.BuiltIns |> Seq.toList

[<Fact>]
let ``Tools allowlist maps to ToolSelection BuiltIns`` () =
    let dir = freshDir ()

    try
        writeAgent dir "agent.md" "---\nname: helper\ntools:\n  - read_file\n  - glob\n---\nPrompt.\n"
        |> ignore

        let store = composite (InMemoryDatabase()) [ dir ] []
        let agent = singleAgent store

        builtInsOf agent |> should equal [ "read_file"; "glob" ]
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Absent tools map to the null runtime default`` () =
    let dir = freshDir ()

    try
        writeAgent dir "agent.md" "---\nname: helper\n---\nPrompt.\n" |> ignore

        let store = composite (InMemoryDatabase()) [ dir ] []
        let agent = singleAgent store

        isNull (box agent.ToolSelection) |> should equal true
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Later files win the tools allowlist`` () =
    let dir = freshDir ()

    try
        writeAgent dir "a-first.md" "---\nname: shared\ntools: [read_file]\n---\nFirst.\n"
        |> ignore

        writeAgent dir "b-second.md" "---\nname: shared\ntools:\n  - glob\n  - grep\n---\nSecond.\n"
        |> ignore

        let store = composite (InMemoryDatabase()) [ dir ] []
        let agent = singleAgent store

        agent.SystemPrompt |> should equal "Second.\n"
        builtInsOf agent |> should equal [ "glob"; "grep" ]
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Code entries win the tools allowlist`` () =
    let dir = freshDir ()

    try
        writeAgent dir "shared.md" "---\nname: shared\ntools: [read_file]\n---\nDirectory.\n"
        |> ignore

        let coded =
            { codeAgent "shared" "Code." with
                ToolSelection =
                    let selection = ToolSelection()
                    selection.BuiltIns <- ResizeArray<string>([| "grep" |]) :> IReadOnlyList<string>
                    selection.ToolSources <- ResizeArray<string>() :> IReadOnlyList<string>
                    selection
            }

        let store = composite (InMemoryDatabase()) [ dir ] [ coded ]
        let agent = singleAgent store

        agent.SystemPrompt |> should equal "Code."
        builtInsOf agent |> should equal [ "grep" ]
    finally
        Directory.Delete(dir, true)

// ──────────────────────────────────────────────────────────────────────────
// Diagnostics

[<Fact>]
let ``Valid definitions carry no diagnostics`` () =
    let dir = freshDir ()

    try
        writeAgent dir "agent.md" "---\nname: helper\ndescription: Helps out\ntools: [read_file]\n---\nPrompt.\n"
        |> ignore

        let store = composite (InMemoryDatabase()) [ dir ] []

        let fileStore = store :?> FileAgentStore

        let events =
            fileStore.ListDiagnostics(SessionId.New(), TurnId.New(), DateTimeOffset.UtcNow)

        events.Count |> should equal 0
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Missing description emits one diagnostic and keeps the agent`` () =
    let dir = freshDir ()

    try
        writeAgent dir "agent.md" "---\nname: helper\ntools: [read_file]\n---\nPrompt.\n"
        |> ignore

        let store = composite (InMemoryDatabase()) [ dir ] []

        let fileStore = store :?> FileAgentStore
        let sessionId = SessionId.New()
        let turnId = TurnId.New()
        let stamp = DateTimeOffset.UtcNow
        let events = fileStore.ListDiagnostics(sessionId, turnId, stamp)

        events.Count |> should equal 1

        let flagged = events[0] :?> AgentInvalidEvent
        flagged.AgentName |> should equal "helper"
        flagged.SessionId |> should equal sessionId
        flagged.TurnId |> should equal turnId
        flagged.Timestamp |> should equal stamp

        let agent = singleAgent store
        agent.Name |> should equal "helper"
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Unknown tools emit one diagnostic and keep the agent`` () =
    let dir = freshDir ()

    try
        writeAgent
            dir
            "agent.md"
            "---\nname: helper\ndescription: Helps out\ntools: [read_file, turbo_laser]\n---\nPrompt.\n"
        |> ignore

        let store = composite (InMemoryDatabase()) [ dir ] []

        let fileStore = store :?> FileAgentStore

        let events =
            fileStore.ListDiagnostics(SessionId.New(), TurnId.New(), DateTimeOffset.UtcNow)

        events.Count |> should equal 1

        let flagged = events[0] :?> AgentInvalidEvent
        flagged.AgentName |> should equal "helper"
        flagged.Reason.Contains("turbo_laser") |> should equal true

        let agent = singleAgent store
        builtInsOf agent |> should equal [ "read_file"; "turbo_laser" ]
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Diagnostics run over the merge winner only`` () =
    let dir = freshDir ()

    try
        writeAgent dir "a-first.md" "---\nname: shared\ntools: [turbo_laser]\n---\nFirst.\n"
        |> ignore

        writeAgent dir "b-second.md" "---\nname: shared\ndescription: Winner\ntools: [read_file]\n---\nSecond.\n"
        |> ignore

        let store = composite (InMemoryDatabase()) [ dir ] []

        let fileStore = store :?> FileAgentStore

        let events =
            fileStore.ListDiagnostics(SessionId.New(), TurnId.New(), DateTimeOffset.UtcNow)

        events.Count |> should equal 0
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Bad YAML stays fatal instead of a diagnostic`` () : Task =
    task {
        let dir = freshDir ()

        try
            writeAgent dir "broken.md" "---\nname: [unclosed\n---\nBody\n" |> ignore

            let store = composite (InMemoryDatabase()) [ dir ] []

            let! _ =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    store.ListAgents(TenantId.Default, CancellationToken.None))

            return ()
        finally
            Directory.Delete(dir, true)
    }
