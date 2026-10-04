// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.IdleAuthorityTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// Issue 373: idle session operations carry host authority, never fabricated
// executing turns. The additive host path (AppendHostEvents under the
// lifecycle fence, sentinel default TurnId attribution) journals idle
// compacts, rebinds, and fork-prefix copies; stale execution cannot borrow
// it to consume input, alter lifecycle/agent, or close the session. The
// ambient prime stays for execution until #400.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

let private tenant = TenantId.Create "acme"

let private waitBound = TimeSpan.FromSeconds 10.0

/// A wait-bound seam that never fires: these tests never rely on a delay.
type private NeverDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

let private noResolve (_: SessionId) (_: CancellationToken) : Task<IActorRef> =
    Task.FromException<IActorRef>(InvalidOperationException("resolve must not run without an actor system"))

let private makeDirectClient (store: ISessionStore) (journal: ISessionEventStore) : SessionClient =
    new SessionClient(
        store,
        tenant,
        noResolve,
        new SessionEventBus(journal),
        TimeSpan.FromMinutes 1.0,
        NeverDelay() :> ILlmDelay,
        None
    )

let private makeHarnessClient (harness: SessionHarness) : SessionClient =
    new SessionClient(
        harness.Store,
        harness.Tenant,
        (fun _ _ -> Task.FromResult(harness.Actor)),
        new SessionEventBus(harness.Journal),
        TimeSpan.FromSeconds 30.0,
        NeverDelay() :> ILlmDelay,
        None
    )

let private scripted (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

let private sourced (tools: AITool list) : StaticToolSource =
    new StaticToolSource(ResizeArray<AITool>(tools) :> IReadOnlyList<AITool>)

let private sampleSession (state: SessionState) =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "idle"
        State = state
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private createDirect (store: ISessionStore) (state: SessionState) : Session =
    store.CreateSession(tenant, sampleSession state, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()

/// Claims the next turn, failing the test unless the store grants it.
let private claimDirect (store: ISessionStore) (sessionId: SessionId) : TurnClaim =
    match
        store.ClaimNextTurn(tenant, sessionId, "idle-test", TimeSpan.FromMinutes 5.0, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnLeaseRenewed as renewed -> renewed.Claim
    | :? TurnLeaseHeld as held -> held.Claim
    | :? TurnLeaseExpiring as expiring -> expiring.Claim
    | _ -> failwith "Expected a granted claim."

/// Seeds one completed turn of text deltas through the claim fence,
/// returning the turn ids in journal order.
let private seedCompletedTurn
    (store: ISessionStore)
    (journal: ISessionEventStore)
    (sessionId: SessionId)
    : TurnId list =
    let payload = UserMessagePayload(UserMessage.Text("go")) :> InboxPayload

    store.AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()
    |> ignore

    let claim = claimDirect store sessionId

    let events =
        ResizeArray<SessionEvent>(
            [|
                TurnStartedEvent(sessionId, claim.TurnId, Nullable(), DateTimeOffset.UtcNow) :> SessionEvent
                TextDeltaEvent(sessionId, claim.TurnId, Nullable(), DateTimeOffset.UtcNow, "hello") :> SessionEvent
                TurnCompletedEvent(sessionId, claim.TurnId, Nullable(), DateTimeOffset.UtcNow) :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match
        journal.Append(tenant, sessionId, claim.Token, events, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? EventAppended -> ()
    | _ -> failwith "Expected the seeded turn to append."

    match
        store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnSettled -> ()
    | _ -> failwith "Expected the seeded turn to settle."

    [
        claim.TurnId
        claim.TurnId
        claim.TurnId
    ]

/// Reads the whole journal in sequence order.
let private readJournal (journal: ISessionEventStore) (sessionId: SessionId) : SessionEvent list =
    match
        journal.Replay(tenant, sessionId, 0L, 100, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? EventReplayPage as page when not (isNull (box page)) && not (isNull (box page.Events)) ->
        List.ofSeq page.Events
    | :? EventReplayEndOfStream -> []
    | _ -> failwith "Expected a replay page or end of stream."

let private pendingCount (store: ISessionStore) (owner: TenantId) (sessionId: SessionId) : int =
    store.ReadPendingInbox(owner, sessionId, CancellationToken.None).GetAwaiter().GetResult().Count

/// Reads the stored session row, failing the test when it is missing.
let private storedRow (store: ISessionStore) (sessionId: SessionId) : Task<Session> =
    task {
        let! found = store.GetSession(tenant, sessionId, CancellationToken.None)

        match found with
        | null -> return failwith "Expected the session row to exist."
        | session -> return session
    }

let private isSentinel (event: SessionEvent) : bool = box event.TurnId.Value |> isNull

// ──────────────────────────────────────────────────────────────────────────
// Open and read-only resolve manufacture nothing

[<Fact>]
let ``Open creates an Idle session with no turn inbox or journal`` () : Task =
    task {
        let database = InMemoryDatabase()

        let client =
            makeDirectClient (InMemoryStoreFactory.sessionStore database) (InMemoryStoreFactory.eventStore database)

        let! created = SessionClientOperations.OpenSessionAsync(client, AgentId.New(), null, CancellationToken.None)

        created.State |> should equal SessionState.Idle
        created.CurrentTurnId.HasValue |> should equal false

        let! stored = storedRow client.Store created.Id
        stored.State |> should equal SessionState.Idle
        stored.CurrentTurnId.HasValue |> should equal false

        readJournal client.EventBus.EventStore created.Id
        |> List.length
        |> should equal 0

        pendingCount client.Store client.Tenant created.Id |> should equal 0
    }

[<Fact>]
let ``Read-only resolve observes no outcome`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])

        let! session = harness.GetSessionAsync(CancellationToken.None)
        session.State |> should equal SessionState.Idle

        let! collected = harness.CollectEventsAsync(CancellationToken.None)
        collected.Count |> should equal 0
        harness.SettledResults.Count |> should equal 0
        pendingCount harness.Store harness.Tenant harness.SessionId |> should equal 0
    }

// ──────────────────────────────────────────────────────────────────────────
// Fork rides the host path with preserved TurnIds

[<Fact>]
let ``Fork copies the prefix preserving TurnIds with no claim inbox or outcome`` () : Task =
    task {
        let database = InMemoryDatabase()
        let store = InMemoryStoreFactory.sessionStore database
        let journal = InMemoryStoreFactory.eventStore database
        let client = makeDirectClient store journal

        let source = createDirect store SessionState.Idle
        let turnIds = seedCompletedTurn store journal source.Id

        let! forked = SessionClientOperations.ForkAsync(client, source.Id, 10L, CancellationToken.None)

        forked.State |> should equal SessionState.Idle
        forked.CurrentTurnId.HasValue |> should equal false
        forked.AgentId |> should equal source.AgentId

        match Option.ofObj forked.Options.Metadata with
        | None -> failwith "Expected the fork metadata."
        | Some metadata ->
            match metadata.TryGetValue("ForkedFrom") with
            | true, parent -> parent |> should equal (source.Id.ToString())
            | false, _ -> failwith "Expected the ForkedFrom metadata."

        // No claim or lease state and no inbox entries travel to the copy.
        pendingCount store tenant forked.Id |> should equal 0

        let copied = readJournal journal forked.Id
        copied.Length |> should equal 3

        copied |> List.map (fun event -> event.TurnId) |> should equal turnIds

        copied
        |> List.map (fun event -> event.Sequence.Value)
        |> should equal [ 1L; 2L; 3L ]

        copied[0] |> should be ofExactType<TurnStartedEvent>
        copied[1] |> should be ofExactType<TextDeltaEvent>
        copied[2] |> should be ofExactType<TurnCompletedEvent>

        // The source prefix is untouched: same events, same sequences.
        let origin = readJournal journal source.Id
        origin.Length |> should equal 3

        origin
        |> List.map (fun event -> event.Sequence.Value)
        |> should equal [ 1L; 2L; 3L ]
    }

[<Fact>]
let ``Fork of an empty transcript journals nothing and claims nothing`` () : Task =
    task {
        let database = InMemoryDatabase()
        let store = InMemoryStoreFactory.sessionStore database
        let journal = InMemoryStoreFactory.eventStore database
        let client = makeDirectClient store journal

        let source = createDirect store SessionState.Idle

        let! forked = SessionClientOperations.ForkAsync(client, source.Id, 10L, CancellationToken.None)

        forked.State |> should equal SessionState.Idle
        forked.CurrentTurnId.HasValue |> should equal false
        readJournal journal forked.Id |> List.length |> should equal 0
        pendingCount store tenant forked.Id |> should equal 0
    }

[<Fact>]
let ``Fork of a closed source still opens Idle through the host path`` () : Task =
    task {
        let database = InMemoryDatabase()
        let store = InMemoryStoreFactory.sessionStore database
        let journal = InMemoryStoreFactory.eventStore database
        let client = makeDirectClient store journal

        let source = createDirect store SessionState.Idle
        seedCompletedTurn store journal source.Id |> ignore

        let! _ = store.CloseSession(tenant, source.Id, CancellationToken.None)

        let! forked = SessionClientOperations.ForkAsync(client, source.Id, 10L, CancellationToken.None)

        forked.State |> should equal SessionState.Idle
        (readJournal journal forked.Id).Length |> should equal 3

        let! sourceRow = storedRow store source.Id
        sourceRow.State |> should equal SessionState.Closed
    }

[<Fact>]
let ``SQLite fork preserves history through the host path`` () : Task =
    task {
        let clock = TestClock()
        let database, path = SqliteTestFixture.openTestDatabase clock

        try
            let store = SqliteStoreFactory.sessionStore database
            let journal = SqliteStoreFactory.eventStore database
            let client = makeDirectClient store journal

            let source = createDirect store SessionState.Idle
            let turnIds = seedCompletedTurn store journal source.Id

            let! forked = SessionClientOperations.ForkAsync(client, source.Id, 10L, CancellationToken.None)

            forked.State |> should equal SessionState.Idle
            forked.CurrentTurnId.HasValue |> should equal false
            pendingCount store tenant forked.Id |> should equal 0

            let copied = readJournal journal forked.Id
            copied.Length |> should equal 3
            copied |> List.map (fun event -> event.TurnId) |> should equal turnIds
        finally
            (database :> IDisposable).Dispose()
            SqliteTestFixture.deleteDatabaseFiles path
    }

// ──────────────────────────────────────────────────────────────────────────
// Close Abort and expiry keep lifecycle restrictions with no journal

[<Fact>]
let ``Close journals no SessionClosedEvent and stays closed`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])

        let! closed = harness.CloseAsync(CancellationToken.None)
        closed.State |> should equal SessionState.Closed

        // Idempotent: closing twice still reports Closed.
        let! again = harness.CloseAsync(CancellationToken.None)
        again.State |> should equal SessionState.Closed

        // No SessionClosedEvent is invented on the host path.
        let! collected = harness.CollectEventsAsync(CancellationToken.None)
        collected.Count |> should equal 0
    }

[<Fact>]
let ``Prompt on Closed throws without reopening`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])

        let! _ = harness.CloseAsync(CancellationToken.None)

        let! refusal =
            Assert.ThrowsAsync<InvalidSessionStateException>(fun () ->
                harness.PromptAsync("late", CancellationToken.None) :> Task)

        (isNull (box refusal)) |> should equal false

        let! stored = harness.GetSessionAsync(CancellationToken.None)
        stored.State |> should equal SessionState.Closed

        let! collected = harness.CollectEventsAsync(CancellationToken.None)
        collected.Count |> should equal 0
    }

[<Fact>]
let ``Abort on Idle reports NoCurrentTurn and journals nothing`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])
        let facade = makeHarnessClient harness

        let! receipt =
            SessionClientOperations.AbortAsync(
                facade,
                harness.SessionId,
                TurnId.New(),
                StopCause.ExplicitAbort,
                "nothing runs",
                CancellationToken.None
            )

        receipt.Outcome |> should equal HostAbortOutcome.NoCurrentTurn

        let! stored = harness.GetSessionAsync(CancellationToken.None)
        stored.State |> should equal SessionState.Idle

        let! collected = harness.CollectEventsAsync(CancellationToken.None)
        collected.Count |> should equal 0
    }

[<Fact>]
let ``Expiry closes only Idle sessions past the bound`` () : Task =
    task {
        let clock = TestClock()
        let database = InMemoryDatabase(clock)
        let store = InMemoryStoreFactory.sessionStore database

        let sessions = SessionsOptions()
        sessions.Expiry <- Nullable(TimeSpan.FromHours 1.0)

        let oldIdle = createDirect store SessionState.Idle
        clock.Advance(TimeSpan.FromHours 2.0)

        let running = createDirect store SessionState.Idle

        let! _ = store.UpdateSessionState(tenant, running.Id, SessionState.Running, CancellationToken.None)
        let freshIdle = createDirect store SessionState.Idle

        let! closed = SessionExpiry.passOnceAsync store tenant sessions clock CancellationToken.None
        closed |> should equal 1

        let! oldRow = storedRow store oldIdle.Id
        oldRow.State |> should equal SessionState.Closed

        let! runningRow = storedRow store running.Id
        runningRow.State |> should equal SessionState.Running

        let! freshRow = storedRow store freshIdle.Id
        freshRow.State |> should equal SessionState.Idle
    }

// ──────────────────────────────────────────────────────────────────────────
// Idle rebind and compact carry the sentinel with no execution

[<Fact>]
let ``Idle rebind journals the sentinel switch and keeps quiescence`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])
        let facade = makeHarnessClient harness

        let! before = harness.GetSessionAsync(CancellationToken.None)
        let target = AgentId.New()

        let! rebound = SessionClientOperations.SetAgentAsync(facade, harness.SessionId, target, CancellationToken.None)

        rebound.AgentId |> should equal target

        let! collected = harness.CollectEventsAsync(CancellationToken.None)

        let switches =
            [ for event in collected -> event ]
            |> List.choose (fun event ->
                match event with
                | :? AgentSwitchedEvent as switched -> Some switched
                | _ -> None)

        switches.Length |> should equal 1
        switches[0].PreviousAgentId |> should equal before.AgentId
        switches[0].NewAgentId |> should equal target
        (isSentinel switches[0]) |> should equal true

        // Quiescence: no inbox entries and still Idle. The ambient spawn
        // prime (kept until #400) still holds its claim; it owns no turn
        // input and the switch rode the host path, not the token.
        pendingCount harness.Store harness.Tenant harness.SessionId |> should equal 0

        let! stored = harness.GetSessionAsync(CancellationToken.None)
        stored.State |> should equal SessionState.Idle
    }

[<Fact>]
let ``Repeated idle compaction stays turn-free`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])
        let facade = makeHarnessClient harness

        // No compaction wiring on the harness: both compacts no-op without
        // starting a turn, journaling, or touching the inbox.
        for _ in 1..2 do
            let! outcome = SessionClientOperations.CompactAsync(facade, harness.SessionId, CancellationToken.None)
            (outcome :? SessionCompactNotNeeded) |> should equal true

        let! collected = harness.CollectEventsAsync(CancellationToken.None)
        collected.Count |> should equal 0

        let! stored = harness.GetSessionAsync(CancellationToken.None)
        stored.State |> should equal SessionState.Idle
        pendingCount harness.Store harness.Tenant harness.SessionId |> should equal 0
    }

[<Fact>]
let ``Suspended compact no-ops without consuming the reply`` () : Task =
    task {
        let invocations = ref []

        let method =
            Func<string>(fun () ->
                invocations.Value <- invocations.Value @ [ "exec" ]
                "out")

        let exec =
            AIFunctionFactory.Create(
                method,
                "exec",
                Unchecked.defaultof<string>,
                Unchecked.defaultof<System.Text.Json.JsonSerializerOptions>
            )
            :> AITool

        let policy =
            { new IPermissionPolicy with
                member _.Evaluate(request) =
                    if request.ToolName = "exec" then
                        PermissionVerdict.Ask
                    else
                        PermissionVerdict.Allow
            }

        let options = SessionHarnessOptions()
        options.Policy <- policy

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "exec")
                    ScriptStep.Text "finished"
                ]

        use! harness = SessionHarness.CreateAsync(chat, sourced [ exec ], options)
        let facade = makeHarnessClient harness

        let! _ = harness.PromptAsync("run", CancellationToken.None)

        let! requestId = harness.WaitForSuspensionAsync(CancellationToken.None)
        (String.IsNullOrEmpty requestId) |> should equal false

        // Suspension is not idle authority: the compact no-ops and the
        // suspended turn still owns the history.
        let! outcome = SessionClientOperations.CompactAsync(facade, harness.SessionId, CancellationToken.None)
        (outcome :? SessionCompactNotNeeded) |> should equal true

        let! suspended = harness.GetSessionAsync(CancellationToken.None)
        suspended.State |> should equal SessionState.WaitingForInput

        let! _ =
            harness.ReplyAsync(PermissionDecision(requestId, PermissionDecisionKind.AllowOnce), CancellationToken.None)

        // The resumed turn still settles after the reply it waited for.
        let stateDeadline = DateTimeOffset.UtcNow.AddSeconds(10.0)

        while harness.SettledResults.Count < 1 && DateTimeOffset.UtcNow < stateDeadline do
            do! Task.Yield()

        harness.SettledResults.Count |> should equal 1
        harness.SettledResults[0].Status |> should equal TurnStatus.Completed

        // The idle compact journaled nothing.
        let! collected = harness.CollectEventsAsync(CancellationToken.None)

        collected
        |> Seq.exists (fun event -> event :? CompactedEvent)
        |> should equal false
    }

// ──────────────────────────────────────────────────────────────────────────
// Stale execution cannot borrow host-control authority

[<Fact>]
let ``Stale host stamp rejects after an idle rebind moves the version`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])
        let facade = makeHarnessClient harness

        let! before = harness.GetSessionAsync(CancellationToken.None)
        harness.Clock.Advance(TimeSpan.FromMinutes 1.0)

        let target = AgentId.New()

        let! _ = SessionClientOperations.SetAgentAsync(facade, harness.SessionId, target, CancellationToken.None)

        // The rebind bumped UpdatedAt past the stale stamp: the loser's
        // host write rejects with zero effects.
        let sentinel =
            CompactedEvent(harness.SessionId, Unchecked.defaultof<TurnId>, Nullable(), DateTimeOffset.UtcNow, 8L, 4L)
            :> SessionEvent

        let! rejected =
            harness.Journal.AppendHostEvents(
                harness.Tenant,
                harness.SessionId,
                before.UpdatedAt,
                (ResizeArray<SessionEvent>([| sentinel |]) :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )

        match rejected with
        | :? EventAppendRejected as refused -> refused.Reason |> should equal "staleLifecycle"
        | _ -> failwith "Expected the staleLifecycle rejection."

        // Only the winner's switch landed.
        let! collected = harness.CollectEventsAsync(CancellationToken.None)
        collected.Count |> should equal 1
        collected[0] |> should be ofExactType<AgentSwitchedEvent>
    }

[<Fact>]
let ``Host append to a closed session rejects and the session stays closed`` () : Task =
    task {
        let database = InMemoryDatabase()
        let store = InMemoryStoreFactory.sessionStore database
        let journal = InMemoryStoreFactory.eventStore database

        let created = createDirect store SessionState.Idle

        let! before = storedRow store created.Id
        let! _ = store.CloseSession(tenant, created.Id, CancellationToken.None)

        let sentinel =
            CompactedEvent(created.Id, Unchecked.defaultof<TurnId>, Nullable(), DateTimeOffset.UtcNow, 8L, 4L)
            :> SessionEvent

        let! rejected =
            journal.AppendHostEvents(
                tenant,
                created.Id,
                before.UpdatedAt,
                (ResizeArray<SessionEvent>([| sentinel |]) :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )

        match rejected with
        | :? EventAppendRejected as refused -> refused.Reason |> should equal "sessionClosed"
        | _ -> failwith "Expected the sessionClosed rejection."

        readJournal journal created.Id |> List.length |> should equal 0

        let! stored = storedRow store created.Id
        stored.State |> should equal SessionState.Closed
    }

// ──────────────────────────────────────────────────────────────────────────
// Races close without reopening overlapping or orphaning

[<Fact>]
let ``Concurrent prompts run without overlapping turns`` () : Task =
    task {
        let client =
            scripted
                [
                    ScriptStep.Text "first"
                    ScriptStep.Text "second"
                ]

        use! harness = SessionHarness.CreateAsync(client, sourced [])
        let facade = makeHarnessClient harness

        let first =
            SessionClientOperations.PromptAsync(
                facade,
                harness.SessionId,
                UserMessage.Text "one",
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let second =
            SessionClientOperations.PromptAsync(
                facade,
                harness.SessionId,
                UserMessage.Text "two",
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! _ = first
        let! _ = second

        let stateDeadline = DateTimeOffset.UtcNow.AddSeconds(10.0)

        while harness.SettledResults.Count < 2 && DateTimeOffset.UtcNow < stateDeadline do
            do! Task.Delay(25)

        harness.SettledResults.Count |> should equal 2

        let! collected = harness.CollectEventsAsync(CancellationToken.None)

        let terminals =
            collected |> Seq.filter (fun event -> event :? TurnCompletedEvent) |> Seq.length

        terminals |> should equal 2

        let! stored = harness.GetSessionAsync(CancellationToken.None)
        stored.State |> should equal SessionState.Idle
        pendingCount harness.Store harness.Tenant harness.SessionId |> should equal 0
    }

[<Fact>]
let ``Subsequent prompting works after idle operations`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "after" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])
        let facade = makeHarnessClient harness

        let target = AgentId.New()
        let! _ = SessionClientOperations.SetAgentAsync(facade, harness.SessionId, target, CancellationToken.None)

        let! outcome = SessionClientOperations.CompactAsync(facade, harness.SessionId, CancellationToken.None)
        (outcome :? SessionCompactNotNeeded) |> should equal true

        let! result = harness.PromptAndSettleAsync("hello", CancellationToken.None)
        result.Status |> should equal TurnStatus.Completed
        result.AssistantText |> should equal "after"

        let! stored = harness.GetSessionAsync(CancellationToken.None)
        stored.State |> should equal SessionState.Idle
        stored.AgentId |> should equal target
    }
