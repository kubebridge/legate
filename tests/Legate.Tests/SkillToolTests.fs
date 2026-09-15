// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SkillToolTests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// Tests for the built-in skill tool (issue 68): the AIFunction definition
// loading SKILL.md from the active package version plus its wiring through
// the existing TurnLoop permission bypass and claim-fence path. Content
// load, unknown-name error, bound behaviour, the journaled event, and the
// takeover race live here; the discriminator round-trip lives in
// EventsTests.

// ───────────────────────────────────────────────────────────────────────────
// Doubles

let private entries (pairs: (string * string) list) : IAsyncEnumerable<AgentPackageEntry> =
    let prepared =
        pairs
        |> List.map (fun (path, text) -> AgentPackageEntry(path, new MemoryStream(Encoding.UTF8.GetBytes text)))
        |> List.toArray

    { new IAsyncEnumerable<AgentPackageEntry> with
        member _.GetAsyncEnumerator(_: CancellationToken) =
            let mutable index = -1

            { new IAsyncEnumerator<AgentPackageEntry> with
                member _.MoveNextAsync() =
                    index <- index + 1
                    ValueTask<bool>(index < prepared.Length)

                member _.Current: AgentPackageEntry = prepared[index]

                member _.DisposeAsync() : ValueTask = ValueTask()
            }
    }

/// An in-memory workspace recording every write: discovery staging lands
/// here at package-relative paths.
type private FakeWorkspace() =
    let files = Dictionary<string, byte[]>(StringComparer.Ordinal)

    member _.Text(path: string) =
        match files.TryGetValue path with
        | true, bytes -> Encoding.UTF8.GetString bytes
        | false, _ -> failwith (sprintf "the workspace has no file '%s'" path)

    interface IWorkspace with
        member _.Root = WorkspaceRoot("skill-tool-tests", null)

        member _.Exec(_, _, _, _) =
            Task.FromException<WorkspaceExecResult>(NotImplementedException("not used"))

        member _.Exists(path, _) = Task.FromResult(files.ContainsKey path)

        member _.ReadFile(path, _) =
            match files.TryGetValue path with
            | true, bytes -> Task.FromResult<Stream>(new MemoryStream(bytes, false) :> Stream)
            | false, _ -> Task.FromException<Stream>(FileNotFoundException path)

        member _.WriteFile(path, content, _) =
            task {
                files[path] <- Array.copy content
                return ()
            }

        member _.DeleteFile(path, _) = Task.FromResult(files.Remove path)

        member _.DisposeAsync() = ValueTask.CompletedTask

/// A package store decorator counting every read: the takeover race
/// asserts the loser performs zero reads.
type private CountingPackageStore(inner: IAgentPackageStore) =
    let mutable infos = 0
    let mutable reads = 0
    let mutable lists = 0

    interface IAgentPackageStore with
        member _.GetPackageInfo(tenant, agentId, cancellationToken) =
            infos <- infos + 1
            inner.GetPackageInfo(tenant, agentId, cancellationToken)

        member _.ReadFile(tenant, agentId, version, path, cancellationToken) =
            reads <- reads + 1
            inner.ReadFile(tenant, agentId, version, path, cancellationToken)

        member _.ListFiles(tenant, agentId, version, prefix, cancellationToken) =
            lists <- lists + 1
            inner.ListFiles(tenant, agentId, version, prefix, cancellationToken)

        member _.UploadPackage(tenant, agentId, version, source, entries, cancellationToken) =
            inner.UploadPackage(tenant, agentId, version, source, entries, cancellationToken)

        member _.ReplacePackageVersion(tenant, agentId, version, source, entries, cancellationToken) =
            inner.ReplacePackageVersion(tenant, agentId, version, source, entries, cancellationToken)

        member _.DeletePackageVersion(tenant, agentId, version, cancellationToken) =
            inner.DeletePackageVersion(tenant, agentId, version, cancellationToken)

        member _.ListVersions(tenant, agentId, cancellationToken) =
            inner.ListVersions(tenant, agentId, cancellationToken)

    member _.Reads = infos + reads + lists

/// An ILlmDelay that never elapses: the turn deadline stays pending so
/// wiring tests run without one.
type private NeverLlmDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

/// A permission policy denying every call while recording which tool names
/// it saw: the skill bypass test asserts it never sees the skill name.
type private RecordingDenyPolicy() =
    let seen = ResizeArray<string>()

    interface IPermissionPolicy with
        member _.Evaluate(request) =
            seen.Add(request.ToolName)
            PermissionVerdict.Deny("no tools in this session")

    member _.Seen: IReadOnlyList<string> = seen :> IReadOnlyList<string>

let private tenant = TenantId.Create "acme"

let private sessionId = SessionId.New()
let private turnId = TurnId.New()
let private stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)

let private startInstant = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

let private lease = TimeSpan.FromSeconds 120.0

/// Builds the tool bound to one package scope, recording every journal
/// callback event.
let private toolWith
    (store: IAgentPackageStore)
    (agentId: AgentId)
    (loaded: ResizeArray<SkillLoadedEvent>)
    : AIFunction =
    let onLoaded =
        Func<SkillLoadedEvent, Task>(fun event ->
            loaded.Add(event)
            Task.CompletedTask)

    SkillTool.Create(store, tenant, agentId, sessionId, turnId, onLoaded)

/// Converts a tool return value to text: null becomes empty, everything
/// else renders through ToString (identity for strings). Match-null
/// narrowing keeps every slot exact; no nullable-typed value ever flows
/// into a string slot.
let private resultText (value: obj | null) : string =
    match value with
    | null -> ""
    | live ->
        let text: string | null = live.ToString()

        match text with
        | null -> ""
        | present -> present

/// Invokes the tool function with the given arguments and returns the
/// result text.
let private invoke (fn: AIFunction) (args: (string * obj) list) : string =
    let table = Dictionary<string, obj>()

    for key, value in args do
        table[key] <- value

    (fn.InvokeAsync(AIFunctionArguments(table :> IDictionary<string, obj>), CancellationToken.None))
        .GetAwaiter()
        .GetResult()
    |> resultText

/// Uploads one package version carrying the given entries and returns the
/// agent it belongs to.
let private upload (store: IAgentPackageStore) (pairs: (string * string) list) : AgentId =
    let agentId = AgentId.New()

    store
        .UploadPackage(tenant, agentId, "1.0.0", "skill-tool-tests", entries pairs, CancellationToken.None)
        .GetAwaiter()
        .GetResult()
    |> ignore

    agentId

/// Loads one skill through the loader, bypassing the AIFunction surface.
let private load (store: IAgentPackageStore) (agentId: AgentId) (skillName: string) : SkillLoader.SkillLoadOutcome =
    SkillLoader.loadAsync
        {
            Store = store
            Tenant = tenant
            AgentId = agentId
            SessionId = sessionId
            TurnId = turnId
            Timestamp = stamp
            SkillName = skillName
        }
        CancellationToken.None
    |> fun task -> task.GetAwaiter().GetResult()

// ───────────────────────────────────────────────────────────────────────────
// Tool definition

[<Fact>]
let ``The factory serves the skill name and its schema`` () =
    let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
    let loaded = ResizeArray<SkillLoadedEvent>()
    let fn = toolWith store (AgentId.New()) loaded

    fn.Name |> should equal "skill"
    fn.Name |> should equal SkillTool.ToolName
    fn.Name |> should equal TurnLoop.SkillToolName
    fn.Description |> should equal SkillTool.Description
    String.IsNullOrWhiteSpace fn.Description |> should equal false

    fn.JsonSchema.GetProperty("required").EnumerateArray()
    |> Seq.map (fun element -> element.GetString())
    |> List.ofSeq
    |> should equal [ "name" ]

    fn.JsonSchema.GetProperty("properties").GetProperty("name").GetProperty("type").GetString()
    |> should equal "string"

    fn.JsonSchema.GetProperty("additionalProperties").GetBoolean()
    |> should equal false

[<Fact>]
let ``Direct invocation without a name fails validation`` () =
    let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
    let loaded = ResizeArray<SkillLoadedEvent>()
    let fn = toolWith store (AgentId.New()) loaded

    Assert.Throws<ToolException>(fun () ->
        fn.InvokeAsync(AIFunctionArguments(), CancellationToken.None).GetAwaiter().GetResult()
        |> ignore)
    |> ignore

    let blank = Dictionary<string, obj>()
    blank["name"] <- "  " :> obj

    Assert.Throws<ToolException>(fun () ->
        fn
            .InvokeAsync(AIFunctionArguments(blank :> IDictionary<string, obj>), CancellationToken.None)
            .GetAwaiter()
            .GetResult()
        |> ignore)
    |> ignore

    loaded.Count |> should equal 0

// ───────────────────────────────────────────────────────────────────────────
// Content load and unknown names

[<Fact>]
let ``Content load returns the full SKILL.md with its companion list and journals one event`` () =
    let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())

    let agentId =
        upload
            store
            [
                ".agent/skills/deploy/SKILL.md", "# Deploy\nDeploys things.\n"
                ".agent/skills/deploy/refs/api.md", "api notes"
                ".agent/skills/deploy/runbook.md", "runbook"
            ]

    let loaded = ResizeArray<SkillLoadedEvent>()
    let text = invoke (toolWith store agentId loaded) [ "name", "deploy" :> obj ]

    text.Contains("# Deploy\nDeploys things.") |> should equal true
    text.Contains("# Skill: deploy") |> should equal true
    text.Contains(".agent/skills/deploy/refs/api.md") |> should equal true
    text.Contains(".agent/skills/deploy/runbook.md") |> should equal true
    // SKILL.md itself is content, never a companion.
    text.Contains("- .agent/skills/deploy/SKILL.md") |> should equal false

    loaded.Count |> should equal 1
    let event = loaded[0]
    event.SkillName |> should equal "deploy"
    event.SessionId |> should equal sessionId
    event.TurnId |> should equal turnId

    event.Companions
    |> List.ofSeq
    |> should
        equal
        [
            ".agent/skills/deploy/refs/api.md"
            ".agent/skills/deploy/runbook.md"
        ]

[<Fact>]
let ``Content load with no companions reports none`` () =
    let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())

    let agentId =
        upload
            store
            [
                ".agent/skills/solo/SKILL.md", "# Solo\n"
            ]

    let loaded = ResizeArray<SkillLoadedEvent>()
    let text = invoke (toolWith store agentId loaded) [ "name", "solo" :> obj ]

    text.Contains("# Solo") |> should equal true
    text.Contains("(none)") |> should equal true

    loaded.Count |> should equal 1
    loaded[0].Companions.Count |> should equal 0

[<Fact>]
let ``Unknown names error listing the available skills ordinally with no event`` () =
    let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())

    let agentId =
        upload
            store
            [
                ".agent/skills/beta/SKILL.md", "---\nname: beta\n---\n# Beta\n"
                ".agent/skills/alpha/SKILL.md", "---\nname: alpha\n---\n# Alpha\n"
            ]

    let loaded = ResizeArray<SkillLoadedEvent>()
    let text = invoke (toolWith store agentId loaded) [ "name", "gamma" :> obj ]

    text
    |> should equal "Error: unknown skill 'gamma'. Available skills: alpha, beta."

    loaded.Count |> should equal 0

[<Fact>]
let ``Unknown names over an unpackaged agent list no skills`` () =
    let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
    let loaded = ResizeArray<SkillLoadedEvent>()

    let text =
        invoke (toolWith store (AgentId.New()) loaded) [ "name", "deploy" :> obj ]

    text |> should equal "Error: unknown skill 'deploy'. Available skills: none."
    loaded.Count |> should equal 0

// ───────────────────────────────────────────────────────────────────────────
// Bound behaviour (Task 2 validation: the 262144-byte literal plus marker)

[<Fact>]
let ``Over-bound content truncates at the discovery bound with the marker`` () =
    let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
    // The bound literal mirrored: content reads cut at 262144 bytes.
    let oversized = String('a', SkillDiscovery.MaxSkillFileBytes + 50)

    let agentId =
        upload
            store
            [
                ".agent/skills/big/SKILL.md", oversized
            ]

    SkillDiscovery.MaxSkillFileBytes |> should equal 262144

    let outcome = load store agentId "big"

    outcome.ResultText.Contains(TurnLoop.TruncationMarker) |> should equal true
    TurnLoop.TruncationMarker |> should equal "[truncated]"

    let header = "# Skill: big\n\n"
    let footer = "\n\n## Companions\n(none)"

    outcome.ResultText.StartsWith(header, StringComparison.Ordinal)
    |> should equal true

    outcome.ResultText.EndsWith(footer, StringComparison.Ordinal)
    |> should equal true

    let content =
        outcome.ResultText.Substring(header.Length, outcome.ResultText.Length - header.Length - footer.Length)

    content |> should equal (String('a', 262144) + "[truncated]")

    match outcome.LoadedEvent with
    | Some _ -> ()
    | None -> failwith "expected a journaled event for a successful bounded load"

[<Fact>]
let ``At-bound content passes through without the marker`` () =
    let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
    let exact = String('b', SkillDiscovery.MaxSkillFileBytes)

    let agentId =
        upload
            store
            [
                ".agent/skills/exact/SKILL.md", exact
            ]

    let outcome = load store agentId "exact"

    outcome.ResultText.Contains(TurnLoop.TruncationMarker) |> should equal false
    outcome.ResultText.Contains(exact) |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Discovery wiring (Task 4 validation: staged companions surface in result)

[<Fact>]
let ``The staged companion list surfaces in the loader result`` () =
    task {
        let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
        let agentId = AgentId.New()
        let workspace = FakeWorkspace()

        let! _ =
            store.UploadPackage(
                tenant,
                agentId,
                "1.0.0",
                "skill-tool-tests",
                entries
                    [
                        ".agent/skills/deploy/SKILL.md",
                        "---\nname: deploy\ndescription: Deploys things.\n---\n# Deploy\n"
                        ".agent/skills/deploy/refs/api.md", "api notes"
                        ".agent/skills/deploy/runbook.md", "runbook"
                    ],
                CancellationToken.None
            )

        let! discovered =
            SkillDiscovery.discoverAndStage
                {
                    Store = store
                    Tenant = tenant
                    AgentId = agentId
                    Workspace = workspace
                    SessionId = sessionId
                    TurnId = turnId
                    Timestamp = stamp
                }
                CancellationToken.None

        let outcome = load store agentId "deploy"

        // The loader lists live from the store: the same paths discovery
        // staged, in the same ordinal order.
        outcome.ResultText.Contains(".agent/skills/deploy/refs/api.md")
        |> should equal true

        outcome.ResultText.Contains(".agent/skills/deploy/runbook.md")
        |> should equal true

        match outcome.LoadedEvent with
        | None -> failwith "expected a journaled event for a staged skill"
        | Some loaded ->
            loaded.Companions
            |> List.ofSeq
            |> should
                equal
                (discovered.StagedCompanions
                 |> List.sortWith (fun l r -> String.CompareOrdinal(l, r)))
    }

// ───────────────────────────────────────────────────────────────────────────
// Journal and fence (Tasks 3 and 5 validation)

/// Fresh clock-backed stores; every fact owns its database.
let private createStores () =
    let clock = TestClock(startInstant)
    let database = InMemoryDatabase(clock)
    let sessions = InMemorySessionStore(database) :> ISessionStore
    let events = InMemorySessionEventStore(database) :> ISessionEventStore
    (clock, sessions, events)

/// A minimal Idle session row, mirroring the session actor suite sample.
let private sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "checkout"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private createSession (store: ISessionStore) : Session =
    store.CreateSession(tenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

let private appendUser (store: ISessionStore) (sessionId: SessionId) (text: string) : unit =
    let payload = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload

    store.AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()
    |> ignore

/// Claims the next turn, failing the test unless the store grants it.
let private claimTurn (store: ISessionStore) (sessionId: SessionId) (owner: string) : TurnClaim =
    match
        store.ClaimNextTurn(tenant, sessionId, owner, lease, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnLeaseRenewed as renewed -> renewed.Claim
    | state -> failwith $"Expected a granted claim, observed %s{state.GetType().Name}."

[<Fact>]
let ``A load journals its SkillLoadedEvent under the live turn claim`` () =
    let _clock, sessions, events = createStores ()
    let session = createSession sessions
    appendUser sessions session.Id "first"
    let claim = claimTurn sessions session.Id "owner-a"

    let packages = InMemoryStoreFactory.packageStore (InMemoryDatabase())

    let agentId =
        upload
            packages
            [
                ".agent/skills/deploy/SKILL.md", "# Deploy\n"
                ".agent/skills/deploy/refs/api.md", "api"
            ]

    let outcome =
        SkillLoader.loadAsync
            {
                Store = packages
                Tenant = tenant
                AgentId = agentId
                SessionId = session.Id
                TurnId = claim.TurnId
                Timestamp = stamp
                SkillName = "deploy"
            }
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let loaded =
        match outcome.LoadedEvent with
        | Some event -> event
        | None -> failwith "expected a journaled event"

    let written =
        JournalWriter.appendAsync
            sessions
            events
            tenant
            session.Id
            claim
            (ResizeArray<SessionEvent>([| loaded :> SessionEvent |]) :> IReadOnlyList<SessionEvent>)
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    match written with
    | JournalWriter.JournalAppended stamped ->
        stamped.Count |> should equal 1

        match stamped[0] with
        | :? SkillLoadedEvent as journaled ->
            journaled.SkillName |> should equal "deploy"

            journaled.Companions
            |> List.ofSeq
            |> should equal [ ".agent/skills/deploy/refs/api.md" ]
        | other -> failwith (sprintf "expected SkillLoadedEvent, journaled %s" (other.GetType().Name))
    | JournalWriter.JournalRejected reason -> failwith (sprintf "expected the append to land, rejected: %s" reason)
    | JournalWriter.JournalFailed reason -> failwith (sprintf "expected the append to land, failed: %s" reason)

[<Fact>]
let ``A takeover loser loads nothing: zero reads and no event`` () =
    let clock, sessions, _events = createStores ()
    let session = createSession sessions
    appendUser sessions session.Id "first"
    appendUser sessions session.Id "second"

    let packages =
        CountingPackageStore(InMemoryStoreFactory.packageStore (InMemoryDatabase()))

    let agentId =
        upload
            (packages :> IAgentPackageStore)
            [
                ".agent/skills/deploy/SKILL.md", "# Deploy\n"
                ".agent/skills/deploy/refs/api.md", "api"
            ]

    // Owner A holds the first turn; the clock lapses its lease and owner
    // B takes over on the second message.
    let loser = claimTurn sessions session.Id "owner-a"
    clock.Advance(TimeSpan.FromSeconds 121.0)
    let winner = claimTurn sessions session.Id "owner-b"

    winner.TurnId |> should not' (equal loser.TurnId)

    let journaled = ResizeArray<SkillLoadedEvent>()

    let effect () =
        task {
            let! outcome =
                SkillLoader.loadAsync
                    {
                        Store = packages :> IAgentPackageStore
                        Tenant = tenant
                        AgentId = agentId
                        SessionId = session.Id
                        TurnId = loser.TurnId
                        Timestamp = stamp
                        SkillName = "deploy"
                    }
                    CancellationToken.None

            match outcome.LoadedEvent with
            | Some event -> journaled.Add(event)
            | None -> ()

            return outcome.ResultText
        }

    // The loser attempts the fenced load: the gate skips without calling.
    let loserLoad =
        ClaimFence.gateAsync sessions tenant loser effect CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    loserLoad.IsNone |> should equal true
    // Zero reads: the fence skipped before the first package lookup.
    packages.Reads |> should equal 0
    journaled.Count |> should equal 0

    // The winner's load lands afterward, proving the setup serves reads:
    // one lookup, one file read, one companion listing, and one event.
    let winnerLoad =
        ClaimFence.gateAsync sessions tenant winner effect CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    winnerLoad.IsSome |> should equal true
    packages.Reads |> should equal 3
    journaled.Count |> should equal 1
    journaled[0].SkillName |> should equal "deploy"

// ───────────────────────────────────────────────────────────────────────────
// Turn wiring: the loader bypasses the permission gate under the claim fence

let private scriptedClient (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

[<Fact>]
let ``The skill call bypasses a denying policy and still verifies the claim`` () =
    let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())

    let agentId =
        upload
            store
            [
                ".agent/skills/deploy/SKILL.md", "# Deploy\nDeploys things.\n"
            ]

    let journaled = ResizeArray<SkillLoadedEvent>()

    let onLoaded =
        Func<SkillLoadedEvent, Task>(fun event ->
            journaled.Add(event)
            Task.CompletedTask)

    let fn = SkillTool.Create(store, tenant, agentId, sessionId, turnId, onLoaded)
    let tools = Dictionary<string, AITool>()
    tools[SkillTool.ToolName] <- fn :> AITool

    let args = Dictionary<string, obj>()
    args["name"] <- "deploy" :> obj

    let client =
        scriptedClient
            [
                ScriptStep.ToolCall("c1", SkillTool.ToolName, args)
                ScriptStep.Text("done")
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let policy = RecordingDenyPolicy()

    let completion =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (tools :> IReadOnlyDictionary<string, AITool>)
            TurnLoop.TurnLoopOptions.Default
            (NeverLlmDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            (policy :> IPermissionPolicy)
            sessionId
            turnId
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    completion.Result.Status |> should equal TurnStatus.Completed
    completion.Suspension.IsNone |> should equal true

    // The denying policy never saw the skill call: the loader bypasses the
    // gate (package metadata needs no decision).
    policy.Seen.Count |> should equal 0

    let results =
        TurnLoopTests.toolMessages history |> List.map TurnLoopTests.toolResultText

    results.Length |> should equal 1
    results[0].Contains("# Deploy") |> should equal true
    journaled.Count |> should equal 1
    journaled[0].SkillName |> should equal "deploy"

[<Fact>]
let ``A fenced-out claim never invokes the skill tool`` () =
    let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())

    let agentId =
        upload
            store
            [
                ".agent/skills/deploy/SKILL.md", "# Deploy\n"
            ]

    let journaled = ResizeArray<SkillLoadedEvent>()

    let onLoaded =
        Func<SkillLoadedEvent, Task>(fun event ->
            journaled.Add(event)
            Task.CompletedTask)

    let fn = SkillTool.Create(store, tenant, agentId, sessionId, turnId, onLoaded)
    let tools = Dictionary<string, AITool>()
    tools[SkillTool.ToolName] <- fn :> AITool

    let args = Dictionary<string, obj>()
    args["name"] <- "deploy" :> obj

    let client =
        scriptedClient
            [
                ScriptStep.ToolCall("c1", SkillTool.ToolName, args)
                ScriptStep.Text("never")
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let mutable fencedOut = false

    try
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (tools :> IReadOnlyDictionary<string, AITool>)
            { TurnLoop.TurnLoopOptions.Default with
                VerifyClaim = Some(fun () -> Task.FromResult(false))
            }
            (NeverLlmDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            (Unchecked.defaultof<IPermissionPolicy>)
            sessionId
            turnId
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()
        |> ignore
    with :? TurnLoop.TurnLeaseLostException ->
        fencedOut <- true

    fencedOut |> should equal true
    journaled.Count |> should equal 0
