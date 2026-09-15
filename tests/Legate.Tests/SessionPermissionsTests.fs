// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionPermissionsTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Legate.Tests.TurnLoopTests
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Xunit

// Production permission pipeline (issue 62): the live path through
// SessionActor.spawnSuspendFactory plus SessionPermissions.createRunner
// bound to a DI-resolved IPermissionPolicy, with AllowForSession grants on
// the session store row evicted on close. Every fact below drives the real
// suspendable actor spawned by the production factory (never
// behaviorWithSuspend directly) over the real TurnLoop runner and the real
// fenced in-memory journal, so Deny, Ask, resume, restart, eviction, and
// the journaled events are all production-path evidence.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

let tenant = TenantId.Create "permissions-pipeline"

/// An empty store pair over one database: the session store and the fenced
/// journal share claim state, so the factory-primed token fences for real.
let private createStores (clock: TimeProvider) : ISessionStore * ISessionEventStore =
    let database = InMemoryDatabase(clock)
    InMemorySessionStore(database) :> ISessionStore, InMemorySessionEventStore(database) :> ISessionEventStore

/// A minimal Idle session row with empty grant memory.
let private sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "pipeline"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// Creates the session row and returns it.
let private createSession (store: ISessionStore) : Session =
    store.CreateSession(tenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

/// Reads the stored session row, blocking. Tests only read rows they
/// created, so a missing row is a test bug.
let private storedOf (store: ISessionStore) (sessionId: SessionId) : Session =
    match store.GetSession(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult() with
    | null -> failwith "Expected the session row to exist."
    | session -> session

/// Polls a condition until it holds or the timeout lapses. Sleeps are the
/// poll cadence only: every assertion below is eventual, never timing.
let private waitFor (timeout: TimeSpan) (condition: unit -> bool) : bool =
    let deadline = DateTime.UtcNow + timeout
    let mutable holds = condition ()

    while not holds && DateTime.UtcNow < deadline do
        Thread.Sleep(25)
        holds <- condition ()

    holds

/// Starts the local actor system with the production suspend factory wired
/// as the session child factory: resolving an id spawns the live
/// suspendable actor through spawnSuspendFactory.
let private startService
    (store: ISessionStore)
    (eventStore: ISessionEventStore)
    (runner: SessionActor.SuspendableRunner)
    (leaseDuration: TimeSpan)
    : LocalActorSystemService =
    let options = LegateOptions()

    let service =
        LocalActorSystemService(OptionsWrapper<LegateOptions>(options) :> IOptions<LegateOptions>, TimeProvider.System)

    service.SessionChildFactory <-
        Some(
            SessionActor.spawnSuspendFactory
                store
                tenant
                eventStore
                (NeverDelay() :> ILlmDelay)
                (TimeSpan.FromMinutes 5.0)
                "production"
                leaseDuration
                runner
        )

    (service :> IHostedService).StartAsync(CancellationToken.None).GetAwaiter().GetResult()
    service

/// Stops a started service, bounding the drain.
let private stopService (service: LocalActorSystemService) : unit =
    (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()

/// Resolves the live session child for a session id through the router.
let private resolveChild (service: LocalActorSystemService) (sessionId: SessionId) =
    service.ResolveSessionAsync(sessionId.Value, CancellationToken.None).GetAwaiter().GetResult()

/// Prompts the live suspendable actor, blocking for the ack.
let private promptLive (store: ISessionStore) (sessionId: SessionId) (child: Akka.Actor.IActorRef) (text: string) =
    let call =
        SessionActor.promptSuspendableAsync store tenant sessionId child (UserMessage.Text text) CancellationToken.None

    call.GetAwaiter().GetResult() |> ignore

/// Replies to the live suspendable actor, blocking for the ack.
let private replyLive (store: ISessionStore) (sessionId: SessionId) (child: Akka.Actor.IActorRef) (reply: Reply) =
    let call =
        SessionActor.replyAsync store tenant sessionId child reply CancellationToken.None

    call.GetAwaiter().GetResult() |> ignore

/// Reads the live actor's snapshot, blocking.
let private liveSnapshot (child: Akka.Actor.IActorRef) : SessionSnapshot =
    let call = SessionActor.getSuspendSnapshotAsync child CancellationToken.None
    call.GetAwaiter().GetResult()

/// Collects the session journal in sequence order, paging from the first
/// event.
let private collectJournal (journal: ISessionEventStore) (sessionId: SessionId) : IReadOnlyList<SessionEvent> =
    let call =
        task {
            let collected = ResizeArray<SessionEvent>()
            let mutable cursor = 0L
            let mutable go = true

            while go do
                let! outcome = journal.Replay(tenant, sessionId, cursor, 100, CancellationToken.None)

                match outcome with
                | :? EventReplayPage as page when not (isNull (box page)) ->
                    if not (isNull (box page.Events)) then
                        collected.AddRange(page.Events)

                    if page.NextCursor.HasValue then
                        cursor <- page.NextCursor.Value
                    else
                        go <- false
                | _ -> go <- false

            return collected :> IReadOnlyList<SessionEvent>
        }

    call.GetAwaiter().GetResult()

/// Builds the production runner over a scripted client and stub tools.
let private productionRunner
    (client: ScriptedChatClient)
    (tools: IReadOnlyDictionary<string, AITool>)
    (policy: IPermissionPolicy | null)
    : SessionActor.SuspendableRunner =
    SessionPermissions.createRunner
        (client :> IChatClient)
        tools
        TurnLoop.TurnLoopOptions.Default
        (NeverDelay() :> ILlmDelay)
        policy
        None

// ──────────────────────────────────────────────────────────────────────────
// Production spawn wiring: Ask suspends the live actor (Tasks 1+3)

[<Fact>]
let ``Ask suspends the live factory-spawned actor with the request journaled store-first`` () =
    let store, journal = createStores TimeProvider.System
    let created = createSession store
    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "never"
            ]

    let policy =
        ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ]) :> IPermissionPolicy

    let service =
        startService store journal (productionRunner client tools policy) (TimeSpan.FromHours 1.0)

    try
        let child = resolveChild service created.Id
        promptLive store created.Id child "run"

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        // The store moved first: the snapshot carries the pending id.
        let snapshot = liveSnapshot child
        snapshot.State |> should equal SessionState.WaitingForInput
        Assert.NotNull(snapshot.PendingRequestId)

        // The production runner invoked nothing: the Ask parked the call.
        invocations.Value.Length |> should equal 0
        client.Calls |> should equal 1

        // The suspend journaled from the live path, not the harness.
        let asked =
            collectJournal journal created.Id
            |> Seq.choose (fun event ->
                match event with
                | :? PermissionRequestedEvent as requested when not (isNull (box requested)) -> Some requested
                | _ -> None)
            |> List.ofSeq

        asked.Length |> should equal 1
        asked[0].RequestId |> should equal snapshot.PendingRequestId
        asked[0].ToolName |> should equal "exec"
    finally
        stopService service

// ──────────────────────────────────────────────────────────────────────────
// Production runner: Deny continues, AllowOnce resumes (Task 2)

[<Fact>]
let ``Deny on the live path skips the tool effect and continues the turn`` () =
    let store, journal = createStores TimeProvider.System
    let created = createSession store
    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "done"
            ]

    let policy =
        ScriptPolicy(
            Map.ofList
                [
                    "exec", PermissionVerdict.Deny("no writes")
                ]
        )
        :> IPermissionPolicy

    let service =
        startService store journal (productionRunner client tools policy) (TimeSpan.FromHours 1.0)

    try
        let child = resolveChild service created.Id
        promptLive store created.Id child "run"

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (storedOf store created.Id).State = SessionState.Idle)

        settled |> should equal true

        // The denied call never ran, but the turn continued to the next
        // provider step instead of failing.
        invocations.Value.Length |> should equal 0
        client.Calls |> should equal 2
        Assert.Null((liveSnapshot child).PendingRequestId)
    finally
        stopService service

[<Fact>]
let ``AllowOnce reply resumes the live turn and journals the resolve`` () =
    let store, journal = createStores TimeProvider.System
    let created = createSession store
    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "finished"
            ]

    let policy =
        ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ]) :> IPermissionPolicy

    let service =
        startService store journal (productionRunner client tools policy) (TimeSpan.FromHours 1.0)

    try
        let child = resolveChild service created.Id
        promptLive store created.Id child "run"

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        let requestId =
            match (liveSnapshot child).PendingRequestId with
            | null -> failwith "Expected a pending permission request."
            | id -> id

        replyLive store created.Id child (PermissionDecision(requestId, PermissionDecisionKind.AllowOnce))

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (storedOf store created.Id).State = SessionState.Idle)

        settled |> should equal true

        // The resumed turn executed exactly the parked call, once.
        invocations.Value |> should equal [ "exec" ]
        client.Calls |> should equal 2

        let resolved =
            collectJournal journal created.Id
            |> Seq.choose (fun event ->
                match event with
                | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) -> Some resolved
                | _ -> None)
            |> List.ofSeq

        resolved.Length |> should equal 1
        resolved[0].RequestId |> should equal requestId
        resolved[0].Decision |> should equal PermissionDecisionKind.AllowOnce
    finally
        stopService service

// ──────────────────────────────────────────────────────────────────────────
// Grants: restart survival through the session store (Task 4)

[<Fact>]
let ``AllowForSession grants survive restart through the session store`` () =
    let clock = TestClock()
    let store, journal = createStores clock
    let created = createSession store
    let policy = ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ])

    let invocations1 = ref []

    let client1 =
        scripted
            [
                callStep "c1" "exec"
                textStep "first-done"
            ]

    let service1 =
        startService
            store
            journal
            (productionRunner
                client1
                (makeTools
                    [
                        "exec", stubTool "exec" "out" invocations1
                    ])
                policy)
            (TimeSpan.FromMinutes 5.0)

    try
        let child1 = resolveChild service1 created.Id
        promptLive store created.Id child1 "run"

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        let requestId =
            match (liveSnapshot child1).PendingRequestId with
            | null -> failwith "Expected a pending permission request."
            | id -> id

        replyLive store created.Id child1 (PermissionDecision(requestId, PermissionDecisionKind.AllowForSession))

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (storedOf store created.Id).State = SessionState.Idle)

        settled |> should equal true
        invocations1.Value |> should equal [ "exec" ]

        // The grant landed on the session row, surviving the actor.
        (storedOf store created.Id).PermissionGrants
        |> List.ofSeq
        |> should equal [ "exec" ]
    finally
        stopService service1

    // Restart past the primed lease: the new child primes a fresh token
    // and reloads the persisted grants instead of re-asking the policy.
    clock.Advance(TimeSpan.FromMinutes 10.0)

    let invocations2 = ref []

    let client2 =
        scripted
            [
                callStep "c2" "exec"
                textStep "second-done"
            ]

    let service2 =
        startService
            store
            journal
            (productionRunner
                client2
                (makeTools
                    [
                        "exec", stubTool "exec" "out" invocations2
                    ])
                policy)
            (TimeSpan.FromMinutes 5.0)

    try
        let child2 = resolveChild service2 created.Id
        promptLive store created.Id child2 "again"

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (storedOf store created.Id).State = SessionState.Idle)

        settled |> should equal true

        // The remembered tool executed with no new policy evaluation.
        invocations2.Value |> should equal [ "exec" ]
        client2.Calls |> should equal 2
        policy.Evaluations |> should equal 1
    finally
        stopService service2

[<Fact>]
let ``Closing the live session evicts the grant memory`` () =
    let store, journal = createStores TimeProvider.System
    let created = createSession store
    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "done"
            ]

    let policy =
        ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ]) :> IPermissionPolicy

    let service =
        startService store journal (productionRunner client tools policy) (TimeSpan.FromHours 1.0)

    try
        let child = resolveChild service created.Id
        promptLive store created.Id child "run"

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        let requestId =
            match (liveSnapshot child).PendingRequestId with
            | null -> failwith "Expected a pending permission request."
            | id -> id

        replyLive store created.Id child (PermissionDecision(requestId, PermissionDecisionKind.AllowForSession))

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (storedOf store created.Id).State = SessionState.Idle)

        settled |> should equal true

        (storedOf store created.Id).PermissionGrants
        |> List.ofSeq
        |> should equal [ "exec" ]

        let closed =
            let call =
                SessionActor.closeSuspendableAsync store tenant created.Id child CancellationToken.None

            call.GetAwaiter().GetResult()

        closed.State |> should equal SessionState.Closed
        closed.PermissionGrants.Count |> should equal 0
        (storedOf store created.Id).PermissionGrants.Count |> should equal 0
    finally
        stopService service

// ──────────────────────────────────────────────────────────────────────────
// Events and cells from the live path (Task 5)

[<Fact>]
let ``Permission events from the live path derive system cells`` () =
    let store, journal = createStores TimeProvider.System
    let created = createSession store
    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "done"
            ]

    let policy =
        ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ]) :> IPermissionPolicy

    let service =
        startService store journal (productionRunner client tools policy) (TimeSpan.FromHours 1.0)

    try
        let child = resolveChild service created.Id
        promptLive store created.Id child "run"

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        // The host denies: the turn continues without the effect, and both
        // the request and the decision journal from the live path.
        let requestId =
            match (liveSnapshot child).PendingRequestId with
            | null -> failwith "Expected a pending permission request."
            | id -> id

        replyLive store created.Id child (PermissionDecision(requestId, PermissionDecisionKind.Deny))

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (storedOf store created.Id).State = SessionState.Idle)

        settled |> should equal true
        invocations.Value.Length |> should equal 0

        let events = collectJournal journal created.Id

        let requested =
            events
            |> Seq.choose (fun event ->
                match event with
                | :? PermissionRequestedEvent as asked when not (isNull (box asked)) -> Some asked
                | _ -> None)
            |> List.ofSeq

        let resolved =
            events
            |> Seq.choose (fun event ->
                match event with
                | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) -> Some resolved
                | _ -> None)
            |> List.ofSeq

        requested.Length |> should equal 1
        resolved.Length |> should equal 1
        resolved[0].Decision |> should equal PermissionDecisionKind.Deny

        // Each permission event folds to one system cell carrying the tool
        // name and the decision, with the request id in metadata.
        let requestCells =
            SessionCellDeriver.Fold(
                created.Id,
                requested[0].TurnId,
                null,
                requested[0].Timestamp,
                (ResizeArray<SessionEvent>([| requested[0] :> SessionEvent |]) :> IReadOnlyList<SessionEvent>)
            )

        requestCells.Count |> should equal 1
        requestCells[0].Kind |> should equal SessionCellKind.System
        requestCells[0].Content |> should equal "exec"

        match requestCells[0].Metadata with
        | null -> failwith "Expected request metadata on the permission cell."
        | metadata -> metadata["requestId"] |> should equal requestId

        let resolveCells =
            SessionCellDeriver.Fold(
                created.Id,
                resolved[0].TurnId,
                null,
                resolved[0].Timestamp,
                (ResizeArray<SessionEvent>([| resolved[0] :> SessionEvent |]) :> IReadOnlyList<SessionEvent>)
            )

        resolveCells.Count |> should equal 1
        resolveCells[0].Kind |> should equal SessionCellKind.System
        resolveCells[0].Content |> should equal "Deny"

        match resolveCells[0].Metadata with
        | null -> failwith "Expected decision metadata on the resolve cell."
        | metadata -> metadata["decision"] |> should equal "Deny"
    finally
        stopService service

// ──────────────────────────────────────────────────────────────────────────
// DI policy resolution and the no-gate fallback

[<Fact>]
let ``Null policy runs the live turn with no gate`` () =
    let store, journal = createStores TimeProvider.System
    let created = createSession store
    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "done"
            ]

    let service =
        startService
            store
            journal
            (productionRunner client tools Unchecked.defaultof<IPermissionPolicy>)
            (TimeSpan.FromHours 1.0)

    try
        let child = resolveChild service created.Id
        promptLive store created.Id child "run"

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (storedOf store created.Id).State = SessionState.Idle)

        settled |> should equal true
        invocations.Value |> should equal [ "exec" ]
        client.Calls |> should equal 2
    finally
        stopService service

[<Fact>]
let ``resolvePolicy resolves the container policy or null when none is registered`` () =
    let expected = ScriptPolicy(Map.empty) :> IPermissionPolicy
    let services = ServiceCollection()
    services.AddSingleton<IPermissionPolicy>(expected) |> ignore

    let resolved = SessionPermissions.resolvePolicy (services.BuildServiceProvider())
    Object.ReferenceEquals(resolved, expected) |> should equal true

    let empty = ServiceCollection().BuildServiceProvider()
    Assert.Null(SessionPermissions.resolvePolicy empty)

// ──────────────────────────────────────────────────────────────────────────
// Factory validation

[<Fact>]
let ``spawnSuspendFactory rejects invalid wiring`` () =
    let _, journal = createStores TimeProvider.System

    let runner: SessionActor.SuspendableRunner =
        fun _ _ _ _ _ _ -> Task.FromResult(Unchecked.defaultof<TurnLoop.TurnLoopCompletion>)

    let store = InMemorySessionStore(InMemoryDatabase()) :> ISessionStore

    (fun () ->
        SessionActor.spawnSuspendFactory
            Unchecked.defaultof<ISessionStore>
            tenant
            journal
            (NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            "production"
            (TimeSpan.FromHours 1.0)
            runner
        |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () ->
        SessionActor.spawnSuspendFactory
            store
            tenant
            journal
            (NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            "  "
            (TimeSpan.FromHours 1.0)
            runner
        |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () ->
        SessionActor.spawnSuspendFactory
            store
            tenant
            journal
            (NeverDelay() :> ILlmDelay)
            TimeSpan.Zero
            "production"
            (TimeSpan.FromHours 1.0)
            runner
        |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>
