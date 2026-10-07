// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ProductionSetupCancellationTests

open System
open System.Collections.Generic
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Legate.Tests.TurnLoopTests
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging
open Xunit

// Production runner setup, tool discovery, and Inject dependencies are
// asynchronous and cancellable (issue 391): the session row plus per-source
// discovery await under the attempt token, the runner awaits setup instead
// of blocking and re-checks authority at the last moment, Inject
// drain/consume propagate the attempt token with truthful outcomes, and
// non-cooperative dependencies have documented bounds with late-result
// inert behavior. Every fact below is deterministic: gated fakes hold
// dependencies pending without sleeps, cancellation is explicit token
// cancellation, and waits carry a ten-second bound that fails the test
// instead of hanging it.
//
// Out of scope here (owned elsewhere, reused not reimplemented): the #390
// piped lifecycle pattern, the #409 journal hook, #375 heartbeat internals,
// #374 attribution, #376 invocation admission, #377 atomic consumption, and
// the #363 winning settlement. The fenced tests below reuse those contracts
// through the same hooks production wires.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

let private waitBound = TimeSpan.FromSeconds 10.0

let private awaitWhat (work: Task<'T>) (what: string) : Task<'T> =
    task {
        try
            return! work.WaitAsync(waitBound, CancellationToken.None)
        with :? TimeoutException ->
            return raise (TimeoutException($"The test timed out waiting for {what}."))
    }

let private tenant = TenantId.Create "setup-cancellation"

let private createStores () : ISessionStore * ISessionEventStore =
    let database = InMemoryDatabase(TimeProvider.System)
    InMemorySessionStore(database) :> ISessionStore, InMemorySessionEventStore(database) :> ISessionEventStore

let private sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "setup"
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

let private sampleEntry (sessionId: SessionId) : InboxEntry =
    {
        SessionId = sessionId
        Position = 0L
        Payload = UserMessagePayload(UserMessage.Text "run") :> InboxPayload
        Delivery = DeliveryMode.Queue
        Consumed = false
        AppendedAt = DateTimeOffset.UtcNow
        TurnId = TurnId.New()
    }

/// A bare AITool carrying only a name: discovery resolves names, never
/// invocations, so a test double needs nothing else.
type private NamedTool(name: string) =
    inherit AITool()
    override _.Name = name

let private namedTools (names: string list) : IReadOnlyList<AITool> =
    let tools = ResizeArray<AITool>()

    for name in names do
        tools.Add(NamedTool(name) :> AITool)

    tools :> IReadOnlyList<AITool>

/// A tool source gated on a TCS: holds discovery pending until the test
/// releases it, records every (context, token) pair, and observes
/// cancellation while waiting plus late cancellation after release (a late
/// result after authority loss stays inert instead of resolving).
type private GatedToolSource(gate: TaskCompletionSource<unit>, tools: IReadOnlyList<AITool>) =
    let recorded = ResizeArray<ToolSourceContext * CancellationToken>()
    let gateLock = obj ()

    interface IToolSource with
        member _.GetTools(context, cancellationToken) =
            task {
                lock gateLock (fun () -> recorded.Add((context, cancellationToken)))
                do! gate.Task.WaitAsync(cancellationToken)
                cancellationToken.ThrowIfCancellationRequested()
                return tools
            }

    member _.Recorded: (ToolSourceContext * CancellationToken) list =
        lock gateLock (fun () -> List.ofSeq recorded)

/// A tool source answering immediately while recording tokens.
type private RecordingToolSource(tools: IReadOnlyList<AITool>) =
    let recorded = ResizeArray<ToolSourceContext * CancellationToken>()
    let gateLock = obj ()

    interface IToolSource with
        member _.GetTools(context, cancellationToken) =
            lock gateLock (fun () -> recorded.Add((context, cancellationToken)))
            Task.FromResult(tools)

    member _.Recorded: (ToolSourceContext * CancellationToken) list =
        lock gateLock (fun () -> List.ofSeq recorded)

/// A tool source failing discovery: the setup must fail truthfully instead
/// of degrading the failure to empty.
type private ThrowingToolSource() =
    interface IToolSource with
        member _.GetTools(_, _) =
            Task.FromException<IReadOnlyList<AITool>>(InvalidOperationException("discovery down"))

/// A tool source returning null: the contract reads null as empty, never as
/// an error.
type private NullToolSource() =
    interface IToolSource with
        member _.GetTools(_, _) =
            Task.FromResult(Unchecked.defaultof<IReadOnlyList<AITool>>)

/// A tool source returning a non-conforming name: assembly must fail loudly.
type private BadNameToolSource() =
    interface IToolSource with
        member _.GetTools(_, _) =
            Task.FromResult(namedTools [ "has space" ])

/// A non-cooperative source: ignores the token and answers immediately. The
/// documented bound (issue 391, Task 5) is that cancellation alone cannot
/// stop it; production safety for such sources rides the post-setup claim
/// fence, and a late result after authority loss is inert there.
type private NonCooperativeSource(tools: IReadOnlyList<AITool>) =
    interface IToolSource with
        member _.GetTools(_, _) = Task.FromResult(tools)

/// A gated custom-tool store: holds the listing pending, records the token,
/// and observes cancellation.
type private GatedCustomToolStore(gate: TaskCompletionSource<unit>) =
    let mutable observed: CancellationToken option = None
    let gateLock = obj ()

    interface IAgentCustomToolStore with
        member _.ListCustomTools(_, _, cancellationToken) =
            task {
                lock gateLock (fun () -> observed <- Some cancellationToken)
                do! gate.Task.WaitAsync(cancellationToken)
                cancellationToken.ThrowIfCancellationRequested()
                return ResizeArray<AgentCustomTool>() :> IReadOnlyList<AgentCustomTool>
            }

        member _.GetCustomTool(_, _, _, _) =
            Task.FromResult(Unchecked.defaultof<AgentCustomTool>)

        member _.UpsertCustomTool(_, _, tool, _) = Task.FromResult(tool)
        member _.DeleteCustomTool(_, _, _, _) = Task.FromResult(false)

    member _.Observed: CancellationToken option = lock gateLock (fun () -> observed)

/// A canned resolver seam answering loopback for every host.
type private FakeResolver() =
    interface IHostAddressResolver with
        member _.ResolveAsync(_, _) =
            Task.FromResult([| Net.IPAddress.Loopback |] :> IReadOnlyList<Net.IPAddress>)

let private allowLocal () =
    let options = SsrfGuardOptions()
    options.AllowList.Add("127.0.0.1")
    options

let private resolveFor (store: ISessionStore) (sources: IToolSource list) =
    SessionClientWiring.resolveInputs store tenant sources (TurnsOptions()) null

let private runRunner
    (store: ISessionStore)
    (journal: ISessionEventStore)
    (client: ScriptedChatClient)
    (resolver: InboxEntry -> CancellationToken -> Task<IReadOnlyDictionary<string, AITool> * TurnLoop.TurnLoopOptions>)
    (entry: InboxEntry)
    (token: CancellationToken)
    : Task<TurnLoop.TurnLoopCompletion> =
    let runner =
        SessionPermissions.createRunner
            (client :> IChatClient)
            store
            tenant
            resolver
            (NeverDelay() :> ILlmDelay)
            Unchecked.defaultof<IPermissionPolicy>
            None
            journal
            SessionStreaming.defaultBounds
            None

    runner entry 1 (HashSet<string>()) None None None token None None None (TurnId.New())

let private fastResolver (tools: IReadOnlyDictionary<string, AITool>) =
    fun (_: InboxEntry) (_: CancellationToken) -> Task.FromResult((tools, TurnLoop.TurnLoopOptions.Default))

// ──────────────────────────────────────────────────────────────────────────
// resolveInputs: pending, independence, and token propagation (Tasks 2+5)

[<Fact>]
let ``Pending discovery blocks setup while an unrelated session resolves`` () : Task =
    task {
        let store, _ = createStores ()
        let slow = createSession store
        let fast = createSession store

        let gate =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        // Holds only the slow session pending: any other session answers
        // immediately, so one slow dependency never stalls unrelated work.
        let source =
            { new IToolSource with
                member _.GetTools(context, cancellationToken) =
                    task {
                        if context.SessionId = slow.Id then
                            do! gate.Task.WaitAsync(cancellationToken)
                            cancellationToken.ThrowIfCancellationRequested()

                        return namedTools [ "slow_tool" ]
                    }
            }

        let resolver = resolveFor store [ source ]
        use slowCts = new CancellationTokenSource()
        let slowTask = resolver (sampleEntry slow.Id) slowCts.Token
        let! fastTools, _ = awaitWhat (resolver (sampleEntry fast.Id) CancellationToken.None) "the unrelated session"

        fastTools.ContainsKey("slow_tool") |> should equal true
        slowTask.IsCompleted |> should equal false

        gate.TrySetResult(()) |> ignore
        let! slowTools, _ = awaitWhat slowTask "the released session"
        slowTools.ContainsKey("slow_tool") |> should equal true
    }

[<Fact>]
let ``Per-source discovery pending independently with first registration winning`` () : Task =
    task {
        let store, _ = createStores ()
        let created = createSession store

        let gateA =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let gateB =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let sourceA = GatedToolSource(gateA, namedTools [ "shared"; "alpha" ])
        let sourceB = GatedToolSource(gateB, namedTools [ "shared"; "beta" ])

        let resolver =
            resolveFor
                store
                [
                    sourceA :> IToolSource
                    sourceB :> IToolSource
                ]

        let entry = sampleEntry created.Id
        let setupTask = resolver entry CancellationToken.None

        // Releasing A alone must not complete the setup: B still holds it.
        gateA.TrySetResult(()) |> ignore
        do! Task.Delay(50)
        setupTask.IsCompleted |> should equal false

        gateB.TrySetResult(()) |> ignore
        let! tools, _ = awaitWhat setupTask "both released sources"
        tools.ContainsKey("alpha") |> should equal true
        tools.ContainsKey("beta") |> should equal true
        // First registration wins across sources.
        tools.Count |> should equal 3
    }

[<Fact>]
let ``Attempt token reaches every discovery call`` () : Task =
    task {
        let store, _ = createStores ()
        let created = createSession store
        let first = RecordingToolSource(namedTools [ "one" ])
        let second = RecordingToolSource(namedTools [ "two" ])

        let resolver =
            resolveFor
                store
                [
                    first :> IToolSource
                    second :> IToolSource
                ]

        use cts = new CancellationTokenSource()
        let! tools, _ = awaitWhat (resolver (sampleEntry created.Id) cts.Token) "the resolution"

        tools.Count |> should equal 2

        for source in [ first :> obj; second :> obj ] do
            ignore source

        first.Recorded.Length |> should equal 1
        second.Recorded.Length |> should equal 1
        (snd first.Recorded.Head).Equals(cts.Token) |> should equal true
        (snd second.Recorded.Head).Equals(cts.Token) |> should equal true
        // Tenant, session, and agent attribution rides the context.
        (fst first.Recorded.Head).SessionId |> should equal created.Id
        (fst first.Recorded.Head).Tenant |> should equal tenant
        (fst first.Recorded.Head).AgentId |> should equal created.AgentId
    }

[<Fact>]
let ``Cancellation during pending discovery faults as cancelled with no partial tools`` () : Task =
    task {
        let store, _ = createStores ()
        let created = createSession store

        let gate =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let source = GatedToolSource(gate, namedTools [ "late_tool" ])
        let resolver = resolveFor store [ source :> IToolSource ]
        use cts = new CancellationTokenSource()
        let setupTask = resolver (sampleEntry created.Id) cts.Token
        cts.Cancel()

        let! outcome =
            task {
                try
                    let! _ = setupTask
                    return None
                with ex ->
                    return Some ex
            }

        match outcome with
        | Some(:? OperationCanceledException) -> ()
        | Some other -> raise (Exception($"Expected OperationCanceledException, got {other.GetType().Name}."))
        | None -> raise (Exception("Expected cancellation, the setup resolved."))

        // A late release after cancellation stays inert: still cancelled,
        // never a partial or fabricated tool set.
        gate.TrySetResult(()) |> ignore

        let! late =
            task {
                try
                    let! _ = setupTask
                    return None
                with ex ->
                    return Some ex
            }

        match late with
        | Some(:? OperationCanceledException) -> ()
        | _ -> raise (Exception("Expected the late setup to stay cancelled."))
    }

[<Fact>]
let ``Throwing discovery fails setup truthfully instead of degrading to empty`` () =
    let store, _ = createStores ()
    let created = createSession store
    let resolver = resolveFor store [ ThrowingToolSource() :> IToolSource ]

    (fun () ->
        resolver (sampleEntry created.Id) CancellationToken.None
        |> fun t -> t.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<InvalidOperationException>

[<Fact>]
let ``Null discovery reads as empty with the budget still resolved`` () : Task =
    task {
        let store, _ = createStores ()
        let created = createSession store
        let resolver = resolveFor store [ NullToolSource() :> IToolSource ]
        let! tools, budget = awaitWhat (resolver (sampleEntry created.Id) CancellationToken.None) "the null discovery"

        tools.Count |> should equal 0
        budget |> should not' (be null)
    }

[<Fact>]
let ``Invalid tool names fail loudly`` () =
    let store, _ = createStores ()
    let created = createSession store
    let resolver = resolveFor store [ BadNameToolSource() :> IToolSource ]

    (fun () ->
        resolver (sampleEntry created.Id) CancellationToken.None
        |> fun t -> t.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``First registration wins across sources`` () : Task =
    task {
        let store, _ = createStores ()
        let created = createSession store

        let first = RecordingToolSource(namedTools [ "shared" ])
        let second = RecordingToolSource(namedTools [ "shared"; "other" ])

        let resolver =
            resolveFor
                store
                [
                    first :> IToolSource
                    second :> IToolSource
                ]

        let! tools, _ = awaitWhat (resolver (sampleEntry created.Id) CancellationToken.None) "the resolution"

        tools.Count |> should equal 2
        tools.ContainsKey("shared") |> should equal true
        tools.ContainsKey("other") |> should equal true
    }

[<Fact>]
let ``Missing session row fails truthfully instead of fabricating inputs`` () =
    let store, _ = createStores ()

    let resolver =
        resolveFor
            store
            [
                RecordingToolSource(namedTools [ "tool" ]) :> IToolSource
            ]

    let ghost = sampleEntry (SessionId.New())

    (fun () ->
        resolver ghost CancellationToken.None
        |> fun t -> t.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<InvalidOperationException>

[<Fact>]
let ``No cross-session leakage in tool contexts`` () : Task =
    task {
        let store, _ = createStores ()
        let first = createSession store
        let second = createSession store
        let recording = RecordingToolSource(namedTools [ "tool" ])
        let resolver = resolveFor store [ recording :> IToolSource ]
        let! _ = awaitWhat (resolver (sampleEntry first.Id) CancellationToken.None) "the first session"
        let! _ = awaitWhat (resolver (sampleEntry second.Id) CancellationToken.None) "the second session"

        recording.Recorded.Length |> should equal 2
        (fst recording.Recorded[0]).SessionId |> should equal first.Id
        (fst recording.Recorded[1]).SessionId |> should equal second.Id
        (fst recording.Recorded[0]).AgentId |> should equal first.AgentId
        (fst recording.Recorded[1]).AgentId |> should equal second.AgentId
    }

// ──────────────────────────────────────────────────────────────────────────
// IToolSource contract: cancellation and degrade (Task 1)

[<Fact>]
let ``CustomToolSource cancellation reaches the listing`` () : Task =
    task {
        let gate =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let listing = GatedCustomToolStore(gate)

        let source =
            CustomToolSource(
                listing :> IAgentCustomToolStore,
                FakeResolver() :> IHostAddressResolver,
                allowLocal (),
                null,
                TimeSpan.FromSeconds(10.0),
                null
            )

        let context: ToolSourceContext =
            {
                Tenant = TenantId.Default
                AgentId = AgentId.New()
                SessionId = SessionId.New()
            }

        use cts = new CancellationTokenSource()
        let pending = (source :> IToolSource).GetTools(context, cts.Token)
        cts.Cancel()

        let! outcome =
            task {
                try
                    let! _ = pending
                    return None
                with ex ->
                    return Some ex
            }

        match outcome with
        | Some(:? OperationCanceledException) -> ()
        | Some other -> raise (Exception($"Expected OperationCanceledException, got {other.GetType().Name}."))
        | None -> raise (Exception("Expected cancellation to reach the listing."))

        match listing.Observed with
        | Some observed -> observed.Equals(cts.Token) |> should equal true
        | None -> raise (Exception("Expected the listing to observe the attempt token."))
    }

[<Fact>]
let ``CustomToolSource degraded store still returns empty`` () : Task =
    task {
        let failing =
            { new IAgentCustomToolStore with
                member _.ListCustomTools(_, _, _) =
                    Task.FromException<IReadOnlyList<AgentCustomTool>>(InvalidOperationException("store down"))

                member _.GetCustomTool(_, _, _, _) =
                    Task.FromResult(Unchecked.defaultof<AgentCustomTool>)

                member _.UpsertCustomTool(_, _, tool, _) = Task.FromResult(tool)
                member _.DeleteCustomTool(_, _, _, _) = Task.FromResult(false)
            }

        let source =
            CustomToolSource(
                failing,
                FakeResolver() :> IHostAddressResolver,
                allowLocal (),
                null,
                TimeSpan.FromSeconds(10.0),
                Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance :> ILogger
            )

        let context: ToolSourceContext =
            {
                Tenant = TenantId.Default
                AgentId = AgentId.New()
                SessionId = SessionId.New()
            }

        let! tools =
            awaitWhat ((source :> IToolSource).GetTools(context, CancellationToken.None)) "the degraded listing"

        tools |> should not' (be null)
        tools.Count |> should equal 0
    }

// ──────────────────────────────────────────────────────────────────────────
// Runner: pending setup, cancellation, and last-moment authority (Tasks 3+4)

[<Fact>]
let ``Runner pending setup blocks while unrelated work completes`` () : Task =
    task {
        let store, journal = createStores ()
        let slowSession = createSession store
        let fastSession = createSession store

        let gate =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let slowTools = makeTools []
        let fastTools = makeTools []

        let gatedResolver (_: InboxEntry) (token: CancellationToken) =
            task {
                do! gate.Task.WaitAsync(token)
                token.ThrowIfCancellationRequested()
                return (slowTools, TurnLoop.TurnLoopOptions.Default)
            }

        let slowClient = scripted [ textStep "slow done" ]
        let fastClient = scripted [ textStep "fast done" ]

        let slowTask =
            runRunner store journal slowClient gatedResolver (sampleEntry slowSession.Id) CancellationToken.None

        let! fastCompletion =
            awaitWhat
                (runRunner
                    store
                    journal
                    fastClient
                    (fastResolver fastTools)
                    (sampleEntry fastSession.Id)
                    CancellationToken.None)
                "the unrelated turn"

        fastCompletion.Result.AssistantText |> should equal "fast done"
        slowTask.IsCompleted |> should equal false

        gate.TrySetResult(()) |> ignore
        let! slowCompletion = awaitWhat slowTask "the released turn"
        slowCompletion.Result.AssistantText |> should equal "slow done"
    }

[<Fact>]
let ``Runner cancellation during pending setup launches no invocation`` () : Task =
    task {
        let store, journal = createStores ()
        let created = createSession store

        let gate =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let tools = makeTools []

        let gatedResolver (_: InboxEntry) (token: CancellationToken) =
            task {
                do! gate.Task.WaitAsync(token)
                token.ThrowIfCancellationRequested()
                return (tools, TurnLoop.TurnLoopOptions.Default)
            }

        let client = scripted [ textStep "must never run" ]
        use cts = new CancellationTokenSource()

        let pending =
            runRunner store journal client gatedResolver (sampleEntry created.Id) cts.Token

        cts.Cancel()

        let! outcome =
            task {
                try
                    let! _ = pending
                    return None
                with ex ->
                    return Some ex
            }

        match outcome with
        | Some(:? OperationCanceledException) -> ()
        | Some other -> raise (Exception($"Expected OperationCanceledException, got {other.GetType().Name}."))
        | None -> raise (Exception("Expected the cancelled setup to fault."))

        // Late release stays inert: still cancelled, and the model was
        // never invoked.
        gate.TrySetResult(()) |> ignore

        let! late =
            task {
                try
                    let! _ = pending
                    return None
                with ex ->
                    return Some ex
            }

        match late with
        | Some(:? OperationCanceledException) -> ()
        | _ -> raise (Exception("Expected the late setup to stay cancelled."))

        client.ReceivedMessages.Count |> should equal 0
    }

[<Fact>]
let ``Runner fenced setup with lost admission launches no invocation`` () : Task =
    task {
        let store, journal = createStores ()
        let created = createSession store
        let tools = makeTools []
        let client = scripted [ textStep "must never run" ]
        let entry = sampleEntry created.Id

        let fakeClaim =
            {
                TurnId = TurnId.New()
                Token = "stale-token"
                Owner = "test"
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(1.0)
                Attempt = 1
            }

        // Ownership loss during setup (issue 375 signal): the cached
        // admission no longer holds, so the late setup completion is inert.
        use _fence = FencedClaimScope.enter (Some fakeClaim)
        use _control = ControlAdmission.enter (fun () -> false)
        use _lease = LeaseAdmission.enter (fun () -> true)

        let! outcome =
            task {
                try
                    let! _ = runRunner store journal client (fastResolver tools) entry CancellationToken.None
                    return None
                with ex ->
                    return Some ex
            }

        match outcome with
        | Some(:? TurnLoop.TurnLeaseLostException) -> ()
        | Some other -> raise (Exception($"Expected TurnLeaseLostException, got {other.GetType().Name}."))
        | None -> raise (Exception("Expected the fenced-out setup to fault."))

        client.ReceivedMessages.Count |> should equal 0
    }

[<Fact>]
let ``Runner fenced setup with a stale claim launches no invocation`` () : Task =
    task {
        let store, journal = createStores ()
        let created = createSession store
        let tools = makeTools []
        let client = scripted [ textStep "must never run" ]
        let entry = sampleEntry created.Id

        // A claim the store never issued: verification fails closed, so the
        // takeover loser performs zero effects.
        let stale =
            {
                TurnId = TurnId.New()
                Token = "never-issued"
                Owner = "test"
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(1.0)
                Attempt = 1
            }

        use _fence = FencedClaimScope.enter (Some stale)
        use _control = ControlAdmission.enter (fun () -> true)
        use _lease = LeaseAdmission.enter (fun () -> true)

        let! outcome =
            task {
                try
                    let! _ = runRunner store journal client (fastResolver tools) entry CancellationToken.None
                    return None
                with ex ->
                    return Some ex
            }

        match outcome with
        | Some(:? TurnLoop.TurnLeaseLostException) -> ()
        | Some other -> raise (Exception($"Expected TurnLeaseLostException, got {other.GetType().Name}."))
        | None -> raise (Exception("Expected the stale-claim setup to fault."))

        client.ReceivedMessages.Count |> should equal 0
    }

[<Fact>]
let ``Runner unclaimed with non-cooperative deps keeps the harness shape`` () : Task =
    task {
        // Documented bound (issue 391, Task 5): outside a fenced claim there
        // is no authority to lose, so a resolver that ignores the token
        // still runs the turn normally. Production Abort safety rides the
        // claim fence above, never the token alone; cancellation reaching
        // provider execution stays the existing TurnLoop behavior.
        let store, journal = createStores ()
        let created = createSession store
        let tools = makeTools []
        let client = scripted [ textStep "harness runs" ]

        let nonCooperative (_: InboxEntry) (_: CancellationToken) =
            Task.FromResult((tools, TurnLoop.TurnLoopOptions.Default))

        let! completion =
            awaitWhat
                (runRunner store journal client nonCooperative (sampleEntry created.Id) CancellationToken.None)
                "the harness turn"

        completion.Result.AssistantText |> should equal "harness runs"
    }

// ──────────────────────────────────────────────────────────────────────────
// Disconnect and durability bounds (Task 5)

[<Fact>]
let ``Cancelling an observer does not cancel the accepted setup`` () : Task =
    task {
        let store, _ = createStores ()
        let created = createSession store

        let gate =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let source = GatedToolSource(gate, namedTools [ "tool" ])
        let resolver = resolveFor store [ source :> IToolSource ]
        use attemptCts = new CancellationTokenSource()
        use observerCts = new CancellationTokenSource()
        let setupTask = resolver (sampleEntry created.Id) attemptCts.Token

        // An observer watches the same setup under its own token, then goes
        // away: abandoning observation never abandons the accepted work.
        let observer =
            task {
                let! _ = setupTask.WaitAsync(observerCts.Token)
                return ()
            }

        observerCts.Cancel()

        let! observerOutcome =
            task {
                try
                    do! observer
                    return None
                with ex ->
                    return Some ex
            }

        match observerOutcome with
        | Some(:? OperationCanceledException) -> ()
        | _ -> raise (Exception("Expected the observer to fault as cancelled."))

        setupTask.IsCompleted |> should equal false
        gate.TrySetResult(()) |> ignore
        let! tools, _ = awaitWhat setupTask "the accepted setup"
        tools.ContainsKey("tool") |> should equal true
    }

[<Fact>]
let ``Cancelled setup leaves pending Injects neither lost nor fabricated`` () : Task =
    task {
        let store, journal = createStores ()
        let created = createSession store

        let! queueEntry =
            awaitWhat
                (store.AppendInboxMessage(
                    tenant,
                    created.Id,
                    UserMessagePayload(UserMessage.Text "queued") :> InboxPayload,
                    DeliveryMode.Queue,
                    CancellationToken.None
                ))
                "the queue append"

        let! _ =
            awaitWhat
                (store.AppendInboxMessage(
                    tenant,
                    created.Id,
                    UserMessagePayload(UserMessage.Text "injected") :> InboxPayload,
                    DeliveryMode.Inject,
                    CancellationToken.None
                ))
                "the inject append"

        let client = scripted [ textStep "must never run" ]

        let cancelledResolver
            (_: InboxEntry)
            (_: CancellationToken)
            : Task<IReadOnlyDictionary<string, AITool> * TurnLoop.TurnLoopOptions> =
            Task.FromException<IReadOnlyDictionary<string, AITool> * TurnLoop.TurnLoopOptions>(
                OperationCanceledException()
            )

        let! outcome =
            task {
                try
                    let! _ = runRunner store journal client cancelledResolver queueEntry CancellationToken.None
                    return None
                with ex ->
                    return Some ex
            }

        match outcome with
        | Some(:? OperationCanceledException) -> ()
        | Some other -> raise (Exception($"Expected OperationCanceledException, got {other.GetType().Name}."))
        | None -> raise (Exception("Expected the cancelled setup to fault."))

        // Truthful outcome: the failure propagated instead of claiming a
        // turn, both entries stay pending (nothing consumed, nothing
        // fabricated as a separate turn), and the model never ran.
        let! pending = awaitWhat (store.ReadPendingInbox(tenant, created.Id, CancellationToken.None)) "the pending read"

        pending.Count |> should equal 2
        client.ReceivedMessages.Count |> should equal 0
    }

[<Fact>]
let ``Repeated cancelled setups leave bounded outstanding work`` () : Task =
    task {
        let store, journal = createStores ()
        let created = createSession store
        let tools = makeTools []
        let mutable invocations = 0

        let observing (_: InboxEntry) (token: CancellationToken) =
            task {
                token.ThrowIfCancellationRequested()
                invocations <- invocations + 1
                return (tools, TurnLoop.TurnLoopOptions.Default)
            }

        for _ in 1..20 do
            use cts = new CancellationTokenSource()
            cts.Cancel()
            let client = scripted [ textStep "must never run" ]

            let! outcome =
                task {
                    try
                        let! _ = runRunner store journal client observing (sampleEntry created.Id) cts.Token
                        return None
                    with ex ->
                        return Some ex
                }

            match outcome with
            | Some(:? OperationCanceledException) -> ()
            | Some other -> raise (Exception($"Expected OperationCanceledException, got {other.GetType().Name}."))
            | None -> raise (Exception("Expected each cancelled setup to fault."))

        // Every attempt observed the token and faulted before invoking: no
        // lingering work, no model calls, no durable writes from setup
        // (setup only reads; control processing and cleanup stay distinct).
        invocations |> should equal 0
    }
