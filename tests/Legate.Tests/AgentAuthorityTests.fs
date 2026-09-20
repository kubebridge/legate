// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.AgentAuthorityTests

open System
open System.Collections.Generic
open System.IO
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open FsUnit.Xunit
open Legate
open Legate.Agents
open Legate.Storage.InMemory
open Microsoft.Extensions.AI
open Xunit

// Agent refresh and execution authority (issue 128): the actor-side
// per-turn gate in the suspendable behavior re-reads IAgentStore.GetAgent
// at every fresh-turn start, adopts (RowVersion, UpdatedAt) changes for the
// next turn, lets in-flight turns finish on the old definition, and settles
// missing, disabled, or tenant-mismatched agents as Failed with the typed
// TurnAgentRejected outcome plus a TurnFailedEvent, without invoking the
// runner.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

let tenant = TenantId.Create "acme"
let otherTenant = TenantId.Create "other"

let jsonOptions = JsonSerializerOptions()

/// Builds an agent row template for the test tenant.
let private agentTemplate (name: string) (prompt: string) : Agent =
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

/// Saves the agent and returns the stored row.
let private saveAgent (agents: IAgentStore) (agent: Agent) : Agent =
    let outcome =
        agents.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None).GetAwaiter().GetResult()

    match outcome with
    | :? AgentUpdated as updated -> updated.Agent
    | _ -> failwith "The agent save should have applied."

/// Updates the agent against its stored version and returns the new row.
let private updateAgent (agents: IAgentStore) (stored: Agent) (next: Agent) : Agent =
    let outcome =
        agents.UpdateIfUnchanged(tenant, next, stored.RowVersion, CancellationToken.None).GetAwaiter().GetResult()

    match outcome with
    | :? AgentUpdated as updated -> updated.Agent
    | _ -> failwith "The agent update should have applied."

/// Builds an Idle session row conversing with the given agent.
let private sessionFor (agentId: AgentId) : Session =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = agentId
        Title = "authority"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// In-memory journal fake without claim fencing: records appends.
type private TestJournal() =
    let events = ResizeArray<SessionEvent>()

    /// Every appended event, in append order.
    member _.Appended = events :> IReadOnlyList<SessionEvent>

    interface ISessionEventStore with
        member _.Append(_, _, _, batch, _) =
            if isNull (box batch) then
                raise (ArgumentNullException(nameof batch))

            for event in batch do
                events.Add(event)

            let stamped = ResizeArray<SessionEvent>(events) :> IReadOnlyList<SessionEvent>
            Task.FromResult(EventAppended(stamped) :> EventAppendOutcome)

        member _.Replay(_, sessionId, fromSequence, _, _) =
            if fromSequence = 0L && events.Count > 0 then
                let page = ResizeArray<SessionEvent>(events) :> IReadOnlyList<SessionEvent>
                Task.FromResult(EventReplayPage(sessionId, page, Nullable<int64>()) :> EventReplayOutcome)
            else
                Task.FromResult(EventReplayEndOfStream(sessionId) :> EventReplayOutcome)

        member _.TryClaimCleanup(_, sessionId, _, _, _) =
            Task.FromResult(EventCleanupNotClaimable(sessionId, "notSupported") :> EventCleanupState)

        member _.CompleteCleanup(_, sessionId, _, _, _) =
            Task.FromResult(EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)

        member _.DeferCleanup(_, sessionId, _, _) =
            Task.FromResult(EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)

/// A settled completion with no suspension.
let private settledCompletion (text: string) : TurnLoop.TurnLoopCompletion =
    {
        Result =
            {
                AssistantText = text
                Status = TurnStatus.Completed
                Iterations = 1
                Usage = { InputTokens = 0L; OutputTokens = 0L }
                Outcome = null
            }
        HasPendingInjects = false
        Suspension = None
    }

/// Scripted suspendable runner: answers every attempt with the next
/// completion and records entries in call order.
type private ScriptRunner(completions: TurnLoop.TurnLoopCompletion list) =
    let gate = obj ()
    let mutable calls = 0
    let entries = ResizeArray<InboxEntry>()

    /// How many turns ran.
    member _.Calls = lock gate (fun () -> calls)

    /// The runner as the suspendable delegate.
    member _.Func: SessionActor.SuspendableRunner =
        fun entry _ _ _ _ _ _ ->
            lock gate (fun () ->
                calls <- calls + 1
                entries.Add(entry))

            let index = min (lock gate (fun () -> calls) - 1) (completions.Length - 1)
            Task.FromResult(completions[index])

/// Gated suspendable runner: the first turn waits for Release, later turns
/// complete at once. Proves an agent change lands mid-turn without disturbing
/// the running turn.
type private GatedRunner(second: TurnLoop.TurnLoopCompletion) =
    let gate = new TaskCompletionSource<TurnLoop.TurnLoopCompletion>()
    let lockObj = obj ()
    let mutable calls = 0

    /// How many turns ran.
    member _.Calls = lock lockObj (fun () -> calls)

    /// Releases the waiting first turn to complete.
    member _.Release() = gate.TrySetResult(second) |> ignore

    /// The runner as the suspendable delegate.
    member _.Func: SessionActor.SuspendableRunner =
        fun _ _ _ _ _ _ _ ->
            lock lockObj (fun () -> calls <- calls + 1)

            if lock lockObj (fun () -> calls) = 1 then
                gate.Task
            else
                Task.FromResult(second)

/// A fixed agent catalog returning one agent whose tenant differs from the
/// session tenant: proves the tenant-mismatch refusal even when the row
/// exists (tenant-scoped stores answer null instead).
type private MismatchedAgentStore(agent: Agent) =
    interface IAgentStore with
        member _.GetAgent(_, _, _) = Task.FromResult(agent)

        member _.ListAgents(_, _) =
            Task.FromResult(ResizeArray<Agent>([| agent |]) :> IReadOnlyList<Agent>)

        member _.UpdateIfUnchanged(_, _, _, _) =
            Task.FromException<AgentUpdateOutcome>(
                NotSupportedException("The mismatched agent store is read-only for this test.")
            )

        member _.DeleteAgent(_, _, _) =
            Task.FromException<bool>(NotSupportedException("The mismatched agent store is read-only for this test."))

        member _.ListAgentsWithEnabledSchedules(_, _) =
            Task.FromResult(ResizeArray<Agent>() :> IReadOnlyList<Agent>)

        member _.TryConsumeScheduleOccurrence(_, _, _, _, _) =
            Task.FromException<ScheduleOccurrenceOutcome>(
                NotSupportedException("The mismatched agent store is read-only for this test.")
            )

/// Spawns a suspendable session actor gated on the given agent catalog.
let private spawnGated
    (system: ActorSystem)
    (store: ISessionStore)
    (journal: TestJournal)
    (agents: IAgentStore | null)
    (sessionTenant: TenantId)
    (sessionId: SessionId)
    (runner: SessionActor.SuspendableRunner)
    (settled: ResizeArray<TurnResult>)
    : IActorRef =
    let baseProps: SessionActorProps =
        {
            Store = store
            Tenant = sessionTenant
            SessionId = sessionId
            RunTurn = (fun _ _ -> Task.FromResult(Unchecked.defaultof<TurnResult>))
            OnTurnSettled = Some(fun result -> lock settled (fun () -> settled.Add(result)))
            OnInjectJournaled = None
            Logger = null
            Compact = None
        }

    let deps: SessionActor.SuspendDeps =
        {
            EventStore = journal :> ISessionEventStore
            Delay = TurnLoopTests.NeverDelay() :> ILlmDelay
            AskTimeout = TimeSpan.FromMinutes 5.0
            JournalToken = "test-token"
            RunSuspendable = runner
            ReprimeJournal = None
            RefreshCompact = None
            AgentStore = agents
        }

    spawn system $"authority-{Guid.NewGuid():N}" (SessionActor.behaviorWithSuspend baseProps deps)

/// Starts a local actor system for one test.
let private createSystem () : ActorSystem = LocalActorSystem.createSystem ()

/// Terminates a test system, bounding the drain.
let private stopSystem (system: ActorSystem) : unit =
    system.Terminate() |> ignore
    system.WhenTerminated.Wait(TimeSpan.FromSeconds 10.0) |> ignore

/// Polls a condition until it holds or the timeout lapses.
let private waitFor (timeout: TimeSpan) (condition: unit -> bool) : bool =
    let deadline = DateTime.UtcNow + timeout
    let mutable holds = condition ()

    while not holds && DateTime.UtcNow < deadline do
        Thread.Sleep(25)
        holds <- condition ()

    holds

/// Prompts a suspendable actor, blocking for the ack.
let private promptGated (store: ISessionStore) (sessionTenant: TenantId) (sessionId: SessionId) (session: IActorRef) =
    SessionActor.promptSuspendableAsync
        store
        sessionTenant
        sessionId
        session
        (UserMessage.Text "run")
        CancellationToken.None
    |> fun task -> task.GetAwaiter().GetResult()

let private storedState (store: ISessionStore) (sessionTenant: TenantId) (sessionId: SessionId) : SessionState =
    match store.GetSession(sessionTenant, sessionId, CancellationToken.None).GetAwaiter().GetResult() with
    | null -> failwith "Expected the session row to exist."
    | session -> session.State

let private pendingCount (store: ISessionStore) (sessionTenant: TenantId) (sessionId: SessionId) : int =
    store.ReadPendingInbox(sessionTenant, sessionId, CancellationToken.None).GetAwaiter().GetResult().Count

let private failedEvents (journal: TestJournal) : TurnFailedEvent list =
    [
        for event in journal.Appended do
            match event with
            | :? TurnFailedEvent as failed when not (isNull (box failed)) -> yield failed
            | _ -> ()
    ]

// ──────────────────────────────────────────────────────────────────────────
// Typed outcome shape

[<Fact>]
let ``AgentAuthorityFailure has exactly the three documented members`` () =
    Enum.GetNames<AgentAuthorityFailure>()
    |> should
        equal
        [|
            "NotFound"
            "Disabled"
            "TenantMismatch"
        |]

    int AgentAuthorityFailure.NotFound |> should equal 0
    int AgentAuthorityFailure.Disabled |> should equal 1
    int AgentAuthorityFailure.TenantMismatch |> should equal 2

[<Fact>]
let ``TurnAgentRejected round-trips to the correct subtype via $type`` () =
    let outcome =
        TurnAgentRejected(AgentAuthorityFailure.Disabled, "Agent is disabled.") :> TurnOutcome

    let json = JsonSerializer.Serialize(outcome, jsonOptions)
    json.Contains("\"$type\":\"turnAgentRejected\"") |> should equal true

    match JsonSerializer.Deserialize<TurnOutcome>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? TurnAgentRejected) |> should equal true

        let rejected = restored :?> TurnAgentRejected
        rejected.Failure |> should equal AgentAuthorityFailure.Disabled
        rejected.Reason |> should equal "Agent is disabled."

// ──────────────────────────────────────────────────────────────────────────
// Refresh: next turn adopts the change, in-flight finishes on the old one

[<Fact>]
let ``Enabled agent authorizes the fresh turn`` () =
    use system = createSystem ()
    let database = InMemoryDatabase()
    let store = InMemorySessionStore(database) :> ISessionStore
    let agents = InMemoryStoreFactory.agentStore database
    let stored = saveAgent agents (agentTemplate "checkout" "You help with checkout.")

    let created =
        store.CreateSession(tenant, sessionFor stored.Id, CancellationToken.None).GetAwaiter().GetResult()

    let journal = TestJournal()
    let runner = ScriptRunner([ settledCompletion "finished" ])
    let settled = ResizeArray<TurnResult>()

    let session =
        spawnGated system store journal agents tenant created.Id runner.Func settled

    try
        promptGated store tenant created.Id session |> ignore

        let finished =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && storedState store tenant created.Id = SessionState.Idle)

        finished |> should equal true
        runner.Calls |> should equal 1
        settled[0].Status |> should equal TurnStatus.Completed
        failedEvents journal |> List.length |> should equal 0
        pendingCount store tenant created.Id |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Next turn adopts a definition change`` () =
    use system = createSystem ()
    let database = InMemoryDatabase()
    let store = InMemorySessionStore(database) :> ISessionStore
    let agents = InMemoryStoreFactory.agentStore database
    let first = saveAgent agents (agentTemplate "checkout" "First prompt.")

    let created =
        store.CreateSession(tenant, sessionFor first.Id, CancellationToken.None).GetAwaiter().GetResult()

    let journal = TestJournal()

    let runner =
        ScriptRunner(
            [
                settledCompletion "one"
                settledCompletion "two"
            ]
        )

    let settled = ResizeArray<TurnResult>()

    let session =
        spawnGated system store journal agents tenant created.Id runner.Func settled

    try
        promptGated store tenant created.Id session |> ignore

        let oneDone =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && storedState store tenant created.Id = SessionState.Idle)

        oneDone |> should equal true

        updateAgent
            agents
            first
            { first with
                SystemPrompt = "Second prompt."
            }
        |> ignore

        promptGated store tenant created.Id session |> ignore

        let twoDone =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 2 && storedState store tenant created.Id = SessionState.Idle)

        twoDone |> should equal true
        runner.Calls |> should equal 2

        settled
        |> Seq.map (fun result -> result.Status)
        |> List.ofSeq
        |> should
            equal
            [
                TurnStatus.Completed
                TurnStatus.Completed
            ]

        failedEvents journal |> List.length |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``In-flight turn finishes on the old definition`` () =
    use system = createSystem ()
    let database = InMemoryDatabase()
    let store = InMemorySessionStore(database) :> ISessionStore
    let agents = InMemoryStoreFactory.agentStore database
    let first = saveAgent agents (agentTemplate "checkout" "First prompt.")

    let created =
        store.CreateSession(tenant, sessionFor first.Id, CancellationToken.None).GetAwaiter().GetResult()

    let journal = TestJournal()
    let runner = GatedRunner(settledCompletion "finished")
    let settled = ResizeArray<TurnResult>()

    let session =
        spawnGated system store journal agents tenant created.Id runner.Func settled

    try
        promptGated store tenant created.Id session |> ignore

        let started = waitFor (TimeSpan.FromSeconds 10.0) (fun () -> runner.Calls = 1)
        started |> should equal true

        updateAgent
            agents
            first
            { first with
                SystemPrompt = "Changed mid-turn."
            }
        |> ignore

        runner.Release()

        let finished =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && storedState store tenant created.Id = SessionState.Idle)

        finished |> should equal true
        runner.Calls |> should equal 1
        settled[0].Status |> should equal TurnStatus.Completed
        settled[0].AssistantText |> should equal "finished"
        failedEvents journal |> List.length |> should equal 0
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Refusals: Failed plus the typed outcome plus one event plus a drained inbox

[<Fact>]
let ``Missing agent settles Failed with NotFound`` () =
    use system = createSystem ()
    let database = InMemoryDatabase()
    let store = InMemorySessionStore(database) :> ISessionStore
    let agents = InMemoryStoreFactory.agentStore database

    let created =
        store.CreateSession(tenant, sessionFor (AgentId.New()), CancellationToken.None).GetAwaiter().GetResult()

    let journal = TestJournal()
    let runner = ScriptRunner([ settledCompletion "never" ])
    let settled = ResizeArray<TurnResult>()

    let session =
        spawnGated system store journal agents tenant created.Id runner.Func settled

    try
        promptGated store tenant created.Id session |> ignore

        let finished =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && storedState store tenant created.Id = SessionState.Idle)

        finished |> should equal true
        runner.Calls |> should equal 0
        settled[0].Status |> should equal TurnStatus.Failed

        match settled[0].Outcome with
        | null -> failwith "Expected a TurnAgentRejected outcome."
        | outcome ->
            (outcome :? TurnAgentRejected) |> should equal true

            (outcome :?> TurnAgentRejected).Failure
            |> should equal AgentAuthorityFailure.NotFound

        failedEvents journal |> List.length |> should equal 1
        pendingCount store tenant created.Id |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Disabled agent settles Failed with Disabled`` () =
    use system = createSystem ()
    let database = InMemoryDatabase()
    let store = InMemorySessionStore(database) :> ISessionStore
    let agents = InMemoryStoreFactory.agentStore database
    let stored = saveAgent agents (agentTemplate "checkout" "You help with checkout.")
    updateAgent agents stored { stored with Enabled = false } |> ignore

    let created =
        store.CreateSession(tenant, sessionFor stored.Id, CancellationToken.None).GetAwaiter().GetResult()

    let journal = TestJournal()
    let runner = ScriptRunner([ settledCompletion "never" ])
    let settled = ResizeArray<TurnResult>()

    let session =
        spawnGated system store journal agents tenant created.Id runner.Func settled

    try
        promptGated store tenant created.Id session |> ignore

        let finished =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && storedState store tenant created.Id = SessionState.Idle)

        finished |> should equal true
        runner.Calls |> should equal 0
        settled[0].Status |> should equal TurnStatus.Failed

        match settled[0].Outcome with
        | null -> failwith "Expected a TurnAgentRejected outcome."
        | outcome ->
            (outcome :? TurnAgentRejected) |> should equal true

            (outcome :?> TurnAgentRejected).Failure
            |> should equal AgentAuthorityFailure.Disabled

        failedEvents journal |> List.length |> should equal 1
        pendingCount store tenant created.Id |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Tenant-mismatched agent settles Failed with TenantMismatch`` () =
    use system = createSystem ()
    let database = InMemoryDatabase()
    let store = InMemorySessionStore(database) :> ISessionStore

    let mismatched =
        { agentTemplate "checkout" "You help with checkout." with
            Tenant = otherTenant
        }

    let agents = MismatchedAgentStore(mismatched) :> IAgentStore

    let created =
        store.CreateSession(tenant, sessionFor mismatched.Id, CancellationToken.None).GetAwaiter().GetResult()

    let journal = TestJournal()
    let runner = ScriptRunner([ settledCompletion "never" ])
    let settled = ResizeArray<TurnResult>()

    let session =
        spawnGated system store journal agents tenant created.Id runner.Func settled

    try
        promptGated store tenant created.Id session |> ignore

        let finished =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && storedState store tenant created.Id = SessionState.Idle)

        finished |> should equal true
        runner.Calls |> should equal 0
        settled[0].Status |> should equal TurnStatus.Failed

        match settled[0].Outcome with
        | null -> failwith "Expected a TurnAgentRejected outcome."
        | outcome ->
            (outcome :? TurnAgentRejected) |> should equal true

            (outcome :?> TurnAgentRejected).Failure
            |> should equal AgentAuthorityFailure.TenantMismatch

        failedEvents journal |> List.length |> should equal 1
        pendingCount store tenant created.Id |> should equal 0
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// File store: RowVersion stays 0, so UpdatedAt drives the refresh

[<Fact>]
let ``File agent refreshes the next turn through UpdatedAt`` () =
    let dir =
        Path.Combine(Path.GetTempPath(), "legate-authority-tests", Guid.NewGuid().ToString("N"))

    Directory.CreateDirectory(dir) |> ignore

    try
        let path = Path.Combine(dir, "checkout.md")
        File.WriteAllText(path, "---\nname: checkout\nmodel: test/model\nenabled: true\n---\nFirst prompt.\n")
        File.SetLastWriteTimeUtc(path, DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc))

        let database = InMemoryDatabase()
        let backing = InMemoryStoreFactory.agentStore database

        let agents =
            FileAgentStore(backing, [| dir |] :> IReadOnlyList<string>, [||] :> IReadOnlyList<Agent>) :> IAgentStore

        let listed =
            agents.ListAgents(TenantId.Default, CancellationToken.None).GetAwaiter().GetResult()

        listed.Count |> should equal 1
        listed[0].RowVersion |> should equal 0UL

        use system = createSystem ()
        let store = InMemorySessionStore(database) :> ISessionStore

        let sessionRow: Session =
            {
                Id = SessionId.New()
                Tenant = TenantId.Default
                AgentId = listed[0].Id
                Title = "authority"
                State = SessionState.Idle
                CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
                CreatedAt = DateTimeOffset.MinValue
                UpdatedAt = DateTimeOffset.MinValue
                ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
                WorkspaceBinding = null
                Options = SessionOptions()
                PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
            }

        let created =
            store.CreateSession(TenantId.Default, sessionRow, CancellationToken.None).GetAwaiter().GetResult()

        let journal = TestJournal()

        let runner =
            ScriptRunner(
                [
                    settledCompletion "one"
                    settledCompletion "two"
                ]
            )

        let settled = ResizeArray<TurnResult>()

        let session =
            spawnGated system store journal agents TenantId.Default created.Id runner.Func settled

        try
            SessionActor.promptSuspendableAsync
                store
                TenantId.Default
                created.Id
                session
                (UserMessage.Text "run")
                CancellationToken.None
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            let oneDone =
                waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                    settled.Count = 1
                    && storedState store TenantId.Default created.Id = SessionState.Idle)

            oneDone |> should equal true

            File.WriteAllText(path, "---\nname: checkout\nmodel: test/model\nenabled: true\n---\nSecond prompt.\n")
            File.SetLastWriteTimeUtc(path, DateTime(2024, 6, 7, 8, 9, 10, DateTimeKind.Utc))

            SessionActor.promptSuspendableAsync
                store
                TenantId.Default
                created.Id
                session
                (UserMessage.Text "run")
                CancellationToken.None
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            let twoDone =
                waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                    settled.Count = 2
                    && storedState store TenantId.Default created.Id = SessionState.Idle)

            twoDone |> should equal true
            runner.Calls |> should equal 2
            failedEvents journal |> List.length |> should equal 0
        finally
            stopSystem system
    finally
        Directory.Delete(dir, true)
