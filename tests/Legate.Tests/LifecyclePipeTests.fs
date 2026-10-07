// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LifecyclePipeTests

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Reflection
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Xunit

// Piped lifecycle store waits (issue 390): deterministic evidence that a
// delayed store dependency never occupies a dispatcher thread, that control
// is answered while a wait is outstanding, and that stale or late
// completions produce zero effects. Gated stores return chained tasks that
// complete when the test opens the gate, so no actor thread ever blocks;
// virtual clocks lapse deadlines without sleeps.

// ──────────────────────────────────────────────────────────────────────────
// Pipe message and probe helpers

/// Completion protocol the pipe unit tests pack pipe outcomes into.
type private TestPipeMessage =
    | PipeCompleted of opId: int64 * incarnation: Guid * outcome: obj
    | PipeTimeout of opId: int64 * incarnation: Guid

/// An ILlmDelay that never elapses: tests that do not exercise a deadline
/// run without one.
type private NeverDelay() =
    interface ILlmDelay with
        member _.Delay(_, _) =
            TaskCompletionSource<unit>().Task :> Task

/// Spawns an actor that records every message it receives, in arrival order.
let private spawnRecorder (system: ActorSystem) (name: string) (queue: ConcurrentQueue<obj>) =
    spawn system name (fun mailbox ->
        let rec loop () =
            actor {
                let! message = mailbox.Receive()
                queue.Enqueue(message)
                return! loop ()
            }

        loop ())

/// Polls a queue until an item arrives or the timeout lapses. The sleep is
/// the poll cadence only: cross-thread delivery is never timing-assumed.
let private tryDequeueWithin (queue: ConcurrentQueue<'T>) (timeout: TimeSpan) : 'T option =
    let deadline = DateTime.UtcNow + timeout
    let mutable found = None
    let mutable item = Unchecked.defaultof<'T>

    while found.IsNone && DateTime.UtcNow < deadline do
        if queue.TryDequeue(&item) then
            found <- Some item
        else
            Thread.Sleep(10)

    found

// ──────────────────────────────────────────────────────────────────────────
// Pipe contract unit tests

[<Fact>]
let ``an already completed wait resumes inline without suspending`` () =
    let outcomes = ResizeArray<Result<string, exn>>()
    let mutable suspended = false

    LifecyclePipe.start
        ActorRefs.Nobody
        TimeProvider.System
        (TimeSpan.FromSeconds 30.0)
        "test-op"
        (Task.FromResult("ready"))
        (fun _ _ outcome ->
            outcomes.Add(outcome)
            actor { return () })
        (fun (opId, incarnation, outcome) -> PipeCompleted(opId, incarnation, outcome))
        (fun (opId, incarnation) -> PipeTimeout(opId, incarnation))
        (fun _ ->
            suspended <- true
            actor { return () })
        ()
        (LifecyclePipe.empty<TestPipeMessage, unit> ())
    |> ignore

    Assert.False(suspended)
    Assert.Equal<Result<string, exn> list>([ Ok "ready" ], outcomes |> List.ofSeq)

[<Fact>]
let ``a faulted wait surfaces its failure inline`` () =
    let outcomes = ResizeArray<Result<string, exn>>()

    LifecyclePipe.start
        ActorRefs.Nobody
        TimeProvider.System
        (TimeSpan.FromSeconds 30.0)
        "test-op"
        (Task.FromException<string>(InvalidOperationException("boom")))
        (fun _ _ outcome ->
            outcomes.Add(outcome)
            actor { return () })
        (fun (opId, incarnation, outcome) -> PipeCompleted(opId, incarnation, outcome))
        (fun (opId, incarnation) -> PipeTimeout(opId, incarnation))
        (fun _ -> actor { return () })
        ()
        (LifecyclePipe.empty<TestPipeMessage, unit> ())
    |> ignore

    Assert.Equal(1, outcomes.Count)

    match outcomes[0] with
    | Ok _ -> failwith "Expected the faulted wait to resume with an error."
    | Error error ->
        Assert.IsType<InvalidOperationException>(error) |> ignore
        Assert.Equal("boom", error.Message)

[<Fact>]
let ``a duplicate completion settles a piped wait exactly once`` () =
    use system = ActorSystem.Create("pipe-duplicate-" + Guid.NewGuid().ToString("N"))
    let queue = ConcurrentQueue<obj>()
    let probe = spawnRecorder system "recorder" queue

    let gate =
        TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously)

    let outcomes = ResizeArray<Result<string, exn>>()
    let mutable suspended: LifecyclePipe.PipeState<TestPipeMessage, unit> option = None

    LifecyclePipe.start
        probe
        TimeProvider.System
        (TimeSpan.FromMinutes 5.0)
        "test-op"
        gate.Task
        (fun _ _ outcome ->
            outcomes.Add(outcome)
            actor { return () })
        (fun (opId, incarnation, outcome) -> PipeCompleted(opId, incarnation, outcome))
        (fun (opId, incarnation) -> PipeTimeout(opId, incarnation))
        (fun pipe ->
            suspended <- Some pipe
            actor { return () })
        ()
        (LifecyclePipe.empty<TestPipeMessage, unit> ())
    |> ignore

    Assert.True(suspended.IsSome)
    Assert.True(suspended.Value.Outstanding.IsSome)
    Assert.True(queue.IsEmpty)

    gate.SetResult("value") |> ignore

    let opId, incarnation, outcome =
        match tryDequeueWithin queue (TimeSpan.FromSeconds 10.0) with
        | Some(:? TestPipeMessage as PipeCompleted(opId, incarnation, outcome)) -> opId, incarnation, outcome
        | _ -> failwith "Expected the piped completion."

    match LifecyclePipe.tryComplete suspended.Value opId incarnation with
    | None -> failwith "Expected the completion to match the outstanding wait."
    | Some(outstanding, drained) ->
        outstanding.Resume () drained outcome |> ignore

        // The redelivered completion finds no outstanding wait and settles nothing.
        Assert.True((LifecyclePipe.tryComplete drained opId incarnation).IsNone)

    Assert.Equal(1, outcomes.Count)
    Assert.Equal<Result<string, exn>>(Ok "value", outcomes[0])

[<Fact>]
let ``a completion for a stale incarnation is discarded`` () =
    use system = ActorSystem.Create("pipe-incarnation-" + Guid.NewGuid().ToString("N"))
    let queue = ConcurrentQueue<obj>()
    let probe = spawnRecorder system "recorder" queue

    let gate =
        TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously)

    let outcomes = ResizeArray<Result<string, exn>>()
    let mutable suspended: LifecyclePipe.PipeState<TestPipeMessage, unit> option = None

    LifecyclePipe.start
        probe
        TimeProvider.System
        (TimeSpan.FromMinutes 5.0)
        "test-op"
        gate.Task
        (fun _ _ outcome ->
            outcomes.Add(outcome)
            actor { return () })
        (fun (opId, incarnation, outcome) -> PipeCompleted(opId, incarnation, outcome))
        (fun (opId, incarnation) -> PipeTimeout(opId, incarnation))
        (fun pipe ->
            suspended <- Some pipe
            actor { return () })
        ()
        (LifecyclePipe.empty<TestPipeMessage, unit> ())
    |> ignore

    gate.SetResult("value") |> ignore

    let opId, incarnation, outcome =
        match tryDequeueWithin queue (TimeSpan.FromSeconds 10.0) with
        | Some(:? TestPipeMessage as PipeCompleted(opId, incarnation, outcome)) -> opId, incarnation, outcome
        | _ -> failwith "Expected the piped completion."

    // A completion stamped with another incarnation leaves the wait untouched.
    Assert.True((LifecyclePipe.tryComplete suspended.Value opId (Guid.NewGuid())).IsNone)
    Assert.True(suspended.Value.Outstanding.IsSome)
    Assert.Equal(0, outcomes.Count)

    match LifecyclePipe.tryComplete suspended.Value opId incarnation with
    | None -> failwith "Expected the genuine completion to match."
    | Some(outstanding, drained) ->
        outstanding.Resume () drained outcome |> ignore
        Assert.True(drained.Outstanding.IsNone)

    Assert.Equal(1, outcomes.Count)

[<Fact>]
let ``deferred lifecycle messages stay bounded and drain head-first`` () =
    let mutable current = LifecyclePipe.empty<TestPipeMessage, unit> ()

    for i in 1 .. LifecyclePipe.MaxDeferredMessages do
        let updated, accepted =
            LifecyclePipe.defer current (PipeTimeout(int64 i, Guid.Empty)) ActorRefs.Nobody

        Assert.True(accepted)
        current <- updated

    Assert.Equal(LifecyclePipe.MaxDeferredMessages, current.Deferred.Length)

    let _, refused =
        LifecyclePipe.defer current (PipeTimeout(0L, Guid.Empty)) ActorRefs.Nobody

    Assert.False(refused)

    match LifecyclePipe.tryTakeDeferred current with
    | Some((message, _), _) -> Assert.Equal<TestPipeMessage>(PipeTimeout(1L, Guid.Empty), message)
    | None -> failwith "Expected the head deferred message."

[<Fact>]
let ``an overlong wait fails with a deadline and discards its late result`` () =
    use system = ActorSystem.Create("pipe-deadline-" + Guid.NewGuid().ToString("N"))
    let queue = ConcurrentQueue<obj>()
    let probe = spawnRecorder system "recorder" queue
    let clock = FakeClock()

    let gate =
        TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously)

    let outcomes = ResizeArray<Result<string, exn>>()
    let mutable suspended: LifecyclePipe.PipeState<TestPipeMessage, unit> option = None

    LifecyclePipe.start
        probe
        clock
        (TimeSpan.FromSeconds 30.0)
        "test-op"
        gate.Task
        (fun _ _ outcome ->
            outcomes.Add(outcome)
            actor { return () })
        (fun (opId, incarnation, outcome) -> PipeCompleted(opId, incarnation, outcome))
        (fun (opId, incarnation) -> PipeTimeout(opId, incarnation))
        (fun pipe ->
            suspended <- Some pipe
            actor { return () })
        ()
        (LifecyclePipe.empty<TestPipeMessage, unit> ())
    |> ignore

    Assert.Equal(1, clock.PendingTimerCount)
    Assert.True(queue.IsEmpty)

    clock.Advance(TimeSpan.FromSeconds 31.0)

    let opId, incarnation =
        match tryDequeueWithin queue (TimeSpan.FromSeconds 10.0) with
        | Some(:? TestPipeMessage as PipeTimeout(opId, incarnation)) -> opId, incarnation
        | _ -> failwith "Expected the piped timeout."

    match LifecyclePipe.tryComplete suspended.Value opId incarnation with
    | None -> failwith "Expected the timeout to match the outstanding wait."
    | Some(outstanding, drained) ->
        // The resumption observes the deadline failure, like a timed-out
        // GetAwaiter().GetResult() would surface it, and the timer is gone.
        outstanding.Resume () drained outstanding.TimeoutOutcome |> ignore
        Assert.True(drained.Outstanding.IsNone)
        Assert.Equal(0, clock.PendingTimerCount)

        Assert.Equal(1, outcomes.Count)

        match outcomes[0] with
        | Ok _ -> failwith "Expected the timed-out wait to resume with an error."
        | Error error -> Assert.IsType<DeadlineExceededException>(error) |> ignore

        // The dependency's late result arrives but settles nothing.
        gate.SetResult("late") |> ignore

        match tryDequeueWithin queue (TimeSpan.FromSeconds 10.0) with
        | Some(:? TestPipeMessage as PipeCompleted(lateOp, lateIncarnation, _)) ->
            Assert.True((LifecyclePipe.tryComplete drained lateOp lateIncarnation).IsNone)
        | _ -> failwith "Expected the late completion to arrive and be discarded."

    Assert.Equal(1, outcomes.Count)

// ──────────────────────────────────────────────────────────────────────────
// Gated store decorator

/// Barrier decorator over a real provider: gated methods return chained
/// tasks that complete when the test opens the gate, so the calling actor
/// thread never blocks. Reads and control/settlement capabilities delegate
/// to the inner store once the gate opens.
type GatedSessionStore() =
    inherit DispatchProxy()

    member val Inner: ISessionStore = Unchecked.defaultof<ISessionStore> with get, set
    member val Gates = ConcurrentDictionary<string, TaskCompletionSource<unit>>() with get
    member val Entered = ConcurrentQueue<string>() with get

    /// Opens a gate: calls with this key pend until the test sets the result.
    member this.Gate(key: string) : TaskCompletionSource<unit> =
        let gate =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        this.Gates[key] <- gate
        gate

    member private this.SessionKey(methodName: string, sessionId: SessionId) = $"{methodName}:{sessionId}"

    static member private Chain<'T>(gate: Task, invoke: unit -> obj) : Task<'T> =
        task {
            do! gate

            let! inner =
                task {
                    match invoke () with
                    | :? Task<'T> as running -> return running
                    | _ ->
                        return!
                            Task.FromException<Task<'T>>(
                                InvalidOperationException("The gated store call did not return its declared task.")
                            )
                }

            return! inner
        }

    member private this.Call<'T>(key: string, invoke: unit -> Task<'T>) : Task<'T> =
        match this.Gates.TryGetValue(key) with
        | true, gate ->
            this.Entered.Enqueue(key)
            GatedSessionStore.Chain<'T>(gate.Task, (fun () -> invoke () :> obj))
        | _ -> invoke ()

    override this.Invoke(methodInfo, args) =
        match methodInfo with
        | null -> raise (InvalidOperationException("No store method to invoke."))
        | methodInfo ->
            let arguments =
                match args with
                | null -> [||]
                | args -> args |> Array.map box

            let produce () =
                match methodInfo.Invoke(this.Inner, arguments) with
                | null -> raise (InvalidOperationException($"The store method {methodInfo.Name} returned null."))
                | produced -> produced

            let unwrap () =
                try
                    produce ()
                with :? TargetInvocationException as error ->
                    match error.InnerException with
                    | null -> raise error
                    | inner -> raise inner

            let key =
                if arguments.Length >= 2 then
                    match arguments[1] with
                    | :? SessionId as sessionId -> this.SessionKey(methodInfo.Name, sessionId)
                    | _ -> methodInfo.Name
                else
                    methodInfo.Name

            match this.Gates.TryGetValue(key) with
            | false, _ -> unwrap ()
            | true, gate ->
                this.Entered.Enqueue(key)
                let returnType = methodInfo.ReturnType

                if returnType = typeof<Task> then
                    GatedSessionStore.Chain<unit>(
                        gate.Task,
                        (fun () ->
                            task {
                                do! (unwrap () :?> Task)
                                return ()
                            }
                            :> obj)
                    )
                    :> obj
                elif
                    returnType.IsGenericType
                    && returnType.GetGenericTypeDefinition() = typedefof<Task<_>>
                then
                    let element = returnType.GetGenericArguments()[0]

                    let chain =
                        match
                            typeof<GatedSessionStore>.GetMethod("Chain", BindingFlags.NonPublic ||| BindingFlags.Static)
                        with
                        | null -> raise (InvalidOperationException("GatedSessionStore.Chain was not found."))
                        | found -> found

                    match chain.MakeGenericMethod(element).Invoke(null, [| gate.Task; (fun () -> unwrap ()) |]) with
                    | null ->
                        raise (InvalidOperationException($"The gated store method {methodInfo.Name} returned null."))
                    | running -> running
                else
                    raise (
                        InvalidOperationException($"The gated store method {methodInfo.Name} did not return a task.")
                    )

    interface ISessionAbortControlStore with
        member this.TryRecoverControlTarget(t, s, turn, owner, duration, ct) =
            this.Call<ControlTargetRecovery>(
                this.SessionKey("TryRecoverControlTarget", s),
                (fun () ->
                    (this.Inner :?> ISessionAbortControlStore).TryRecoverControlTarget(t, s, turn, owner, duration, ct))
            )

        member this.ReadAbortTarget(t, s, ct) =
            this.Call<AbortTarget | null>(
                this.SessionKey("ReadAbortTarget", s),
                (fun () -> (this.Inner :?> ISessionAbortControlStore).ReadAbortTarget(t, s, ct))
            )

        member this.RequestHostAbort(t, s, turn, cause, reason, ct) =
            this.Call<HostAbortReceipt>(
                this.SessionKey("RequestHostAbort", s),
                (fun () -> (this.Inner :?> ISessionAbortControlStore).RequestHostAbort(t, s, turn, cause, reason, ct))
            )

        member this.BindControlTarget(t, s, turn, position, claim, ct) =
            this.Call<ControlOperationResult>(
                this.SessionKey("BindControlTarget", s),
                (fun () ->
                    (this.Inner :?> ISessionAbortControlStore).BindControlTarget(t, s, turn, position, claim, ct))
            )

        member this.CheckControlTarget(t, s, turn, position, claim, ct) =
            this.Call<ControlOperationResult>(
                this.SessionKey("CheckControlTarget", s),
                (fun () ->
                    (this.Inner :?> ISessionAbortControlStore).CheckControlTarget(t, s, turn, position, claim, ct))
            )

        member this.TryDecideControlTarget(t, s, turn, position, claim, id, status, cause, reason, ct) =
            this.Call<ControlOperationResult>(
                this.SessionKey("TryDecideControlTarget", s),
                (fun () ->
                    (this.Inner :?> ISessionAbortControlStore)
                        .TryDecideControlTarget(t, s, turn, position, claim, id, status, cause, reason, ct))
            )

        member this.RetireControlTarget(t, s, turn, position, claim, id, ct) =
            this.Call<ControlOperationResult>(
                this.SessionKey("RetireControlTarget", s),
                (fun () ->
                    (this.Inner :?> ISessionAbortControlStore).RetireControlTarget(t, s, turn, position, claim, id, ct))
            )

    interface ISessionSettlementStore with
        member this.SupportsSettlementJournal(eventStore) =
            (this.Inner :?> ISessionSettlementStore).SupportsSettlementJournal(eventStore)

        member this.AdmitExecution(t, s, position, claim, ct) =
            this.Call<bool>(
                this.SessionKey("AdmitExecution", s),
                (fun () -> (this.Inner :?> ISessionSettlementStore).AdmitExecution(t, s, position, claim, ct))
            )

        member this.SettleExecution(t, request, ct) =
            this.Call<SessionSettlementOutcome>(
                this.SessionKey("SettleExecution", request.SessionId),
                (fun () -> (this.Inner :?> ISessionSettlementStore).SettleExecution(t, request, ct))
            )

        member this.TryReadCommitted(t, s, position, ct) =
            this.Call<SessionSettlementOutcome | null>(
                this.SessionKey("TryReadCommitted", s),
                (fun () -> (this.Inner :?> ISessionSettlementStore).TryReadCommitted(t, s, position, ct))
            )

        member this.TryReadEntry(t, s, position, ct) =
            this.Call<InboxEntry | null>(
                this.SessionKey("TryReadEntry", s),
                (fun () -> (this.Inner :?> ISessionSettlementStore).TryReadEntry(t, s, position, ct))
            )

// ──────────────────────────────────────────────────────────────────────────
// Actor integration setup

module LifecyclePipeActors =
    let ct = CancellationToken.None
    let tenant = TenantId.Create "lifecycle-pipe"
    let bound = TimeSpan.FromSeconds 15.0

    let completedTurn =
        {
            AssistantText = "done"
            Status = TurnStatus.Completed
            Iterations = 1
            Usage = { InputTokens = 0L; OutputTokens = 0L }
            Outcome = null
        }

    let sampleSession () =
        {
            Id = SessionId.New()
            Tenant = tenant
            AgentId = AgentId.New()
            Title = "lifecycle"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    /// Polls a condition until it holds or the timeout lapses. The sleep is
    /// the poll cadence only: every assertion below is eventual, never timing.
    let waitFor (timeout: TimeSpan) (condition: unit -> bool) : bool =
        let deadline = DateTime.UtcNow + timeout
        let mutable holds = condition ()

        while not holds && DateTime.UtcNow < deadline do
            Thread.Sleep(25)
            holds <- condition ()

        holds

    let storedState (store: ISessionStore) (sessionId: SessionId) =
        match store.GetSession(tenant, sessionId, ct).GetAwaiter().GetResult() with
        | null -> failwith "Expected the session row to exist."
        | session -> session.State

    let factoryActor (system: ActorSystem) factory (sessionId: SessionId) =
        let parent =
            spawn system ("parent-" + Guid.NewGuid().ToString("N")) (fun mailbox ->
                actor {
                    let! _ = mailbox.Receive()

                    try
                        let child = factory (sessionId.ToString()) mailbox.Context "session"
                        mailbox.Sender() <! child
                    with error ->
                        mailbox.Sender() <! Status.Failure(error)

                    let! _ = mailbox.Receive()
                    return ()
                })

        parent.Ask<IActorRef>("activate", bound)

    let spawnImmediateFactory
        (store: ISessionStore)
        (journal: ISessionEventStore)
        (entered: ConcurrentQueue<int64 * int>)
        =
        let runner: SessionActor.SuspendableRunner =
            fun work attempt _ _ _ _ _ _ _ _ turn ->
                entered.Enqueue((work.Position, attempt))

                Task.FromResult(
                    {
                        Result = completedTurn
                        TurnId = turn
                        HasPendingInjects = false
                        Suspension = None
                    }
                )

        SessionActor.spawnSuspendFactory
            store
            tenant
            journal
            (NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            "lifecycle"
            (TimeSpan.FromMinutes 5.0)
            runner
            (fun _ _ -> None)
            null
            (fun _ _ _ -> Task.FromResult(false))
            None

    let prompt (store: ISessionStore) (child: IActorRef) (sessionId: SessionId) (text: string) =
        SessionActor.promptSuspendableAsync store tenant sessionId child (UserMessage.Text text) ct
        |> fun call -> call.WaitAsync(bound)

    let settledJournal (journal: ISessionEventStore) (sessionId: SessionId) =
        match journal.Replay(tenant, sessionId, 0L, 100, ct).GetAwaiter().GetResult() with
        | :? EventReplayPage as page when not (isNull (box page)) && not (isNull (box page.Events)) ->
            page.Events |> List.ofSeq
        | _ -> []

// ──────────────────────────────────────────────────────────────────────────
// Actor integration tests

[<Fact>]
let ``a pending dependency never stalls an unrelated session`` () =
    task {
        let database = InMemoryDatabase(TestClock())
        let inner = InMemorySessionStore(database) :> ISessionStore
        let journal = InMemorySessionEventStore(database) :> ISessionEventStore
        let store = DispatchProxy.Create<ISessionStore, GatedSessionStore>()
        let proxy = store :?> GatedSessionStore
        proxy.Inner <- inner

        let! createdA =
            store.CreateSession(
                LifecyclePipeActors.tenant,
                LifecyclePipeActors.sampleSession (),
                LifecyclePipeActors.ct
            )

        let! createdB =
            store.CreateSession(
                LifecyclePipeActors.tenant,
                LifecyclePipeActors.sampleSession (),
                LifecyclePipeActors.ct
            )

        // A pinned single-thread dispatcher: with blocking waits, the held
        // dependency would occupy the only thread and stall session B too.
        let config =
            ConfigurationFactory.ParseString(
                "akka.actor.default-dispatcher.fork-join-executor.parallelism-min = 1
                 akka.actor.default-dispatcher.fork-join-executor.parallelism-max = 1"
            )

        use system =
            ActorSystem.Create("pipe-pinned-" + Guid.NewGuid().ToString("N"), config)

        let entered = ConcurrentQueue<int64 * int>()
        let factory = LifecyclePipeActors.spawnImmediateFactory store journal entered

        // Session A's first lifecycle read pends behind a closed gate.
        let readGate = proxy.Gate($"ReadPendingInbox:{createdA.Id}")
        let! childA = LifecyclePipeActors.factoryActor system factory createdA.Id
        let! childB = LifecyclePipeActors.factoryActor system factory createdB.Id

        let! entryB = LifecyclePipeActors.prompt store childB createdB.Id "unrelated work"

        let bSettled =
            LifecyclePipeActors.waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                LifecyclePipeActors.storedState store createdB.Id = SessionState.Idle)

        Assert.True(bSettled)
        Assert.Contains((entryB.Position, 1), entered)
        Assert.Contains("ReadPendingInbox", proxy.Entered |> Seq.map (fun key -> key.Split(':')[0]))

        // Session B settled while session A's dependency stayed pending:
        // the dispatcher thread was never occupied by the held wait.
        readGate.SetResult() |> ignore
        let! _ = LifecyclePipeActors.prompt store childA createdA.Id "held work"

        let aSettled =
            LifecyclePipeActors.waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                LifecyclePipeActors.storedState store createdA.Id = SessionState.Idle)

        Assert.True(aSettled)
    }

[<Fact>]
let ``an abort is answered while a settle wait is outstanding and the winner stands`` () =
    task {
        let database = InMemoryDatabase(TestClock())
        let inner = InMemorySessionStore(database) :> ISessionStore
        let journal = InMemorySessionEventStore(database) :> ISessionEventStore
        let store = DispatchProxy.Create<ISessionStore, GatedSessionStore>()
        let proxy = store :?> GatedSessionStore
        proxy.Inner <- inner

        let! created =
            store.CreateSession(
                LifecyclePipeActors.tenant,
                LifecyclePipeActors.sampleSession (),
                LifecyclePipeActors.ct
            )

        use system = ActorSystem.Create("pipe-abort-" + Guid.NewGuid().ToString("N"))
        let entered = ConcurrentQueue<int64 * int>()
        let factory = LifecyclePipeActors.spawnImmediateFactory store journal entered
        let! child = LifecyclePipeActors.factoryActor system factory created.Id

        let! entry = LifecyclePipeActors.prompt store child created.Id "settling work"

        // The turn finished; its settle admission pends behind a closed gate.
        let admitGate = proxy.Gate($"AdmitExecution:{created.Id}")

        let admitted =
            LifecyclePipeActors.waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                proxy.Entered |> Seq.exists (fun key -> key = $"AdmitExecution:{created.Id}"))

        Assert.True(admitted)

        // The abort is answered from memory while the settle wait stays
        // outstanding: the stop arrived too late to win, and the committed
        // winner still stands instead of being corrupted by it.
        let! snapshot =
            SessionActor.abortSuspendableAsync
                store
                LifecyclePipeActors.tenant
                created.Id
                child
                StopCause.ExplicitAbort
                "late stop"
                LifecyclePipeActors.ct
            |> fun call -> call.WaitAsync(LifecyclePipeActors.bound)

        Assert.Equal(SessionState.Running, snapshot.State)

        admitGate.SetResult() |> ignore

        let settled =
            LifecyclePipeActors.waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                LifecyclePipeActors.storedState store created.Id = SessionState.Idle)

        Assert.True(settled)

        let events = LifecyclePipeActors.settledJournal journal created.Id

        let terminals =
            events
            |> List.filter (fun event -> event :? TurnCompletedEvent || event :? TurnAbortedEvent)

        Assert.Equal(1, terminals.Length)
        let committed = Assert.IsType<TurnCompletedEvent>(terminals[0])
        Assert.Equal(entry.TurnId, committed.TurnId)
    }

[<Fact>]
let ``a close requested behind a settle wait lands once the pipe drains`` () =
    task {
        let database = InMemoryDatabase(TestClock())
        let inner = InMemorySessionStore(database) :> ISessionStore
        let journal = InMemorySessionEventStore(database) :> ISessionEventStore
        let store = DispatchProxy.Create<ISessionStore, GatedSessionStore>()
        let proxy = store :?> GatedSessionStore
        proxy.Inner <- inner

        let! created =
            store.CreateSession(
                LifecyclePipeActors.tenant,
                LifecyclePipeActors.sampleSession (),
                LifecyclePipeActors.ct
            )

        use system = ActorSystem.Create("pipe-close-" + Guid.NewGuid().ToString("N"))
        let entered = ConcurrentQueue<int64 * int>()
        let factory = LifecyclePipeActors.spawnImmediateFactory store journal entered
        let! child = LifecyclePipeActors.factoryActor system factory created.Id

        // The turn's settle admission pends behind a closed gate.
        let admitGate = proxy.Gate($"AdmitExecution:{created.Id}")
        let! entry = LifecyclePipeActors.prompt store child created.Id "settling work"

        let admitted =
            LifecyclePipeActors.waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                proxy.Entered |> Seq.exists (fun key -> key = $"AdmitExecution:{created.Id}"))

        Assert.True(admitted)

        // The close is recorded but cannot land while the settle wait stays
        // outstanding: the reply arrives only after the test opens the gate.
        let closeTask =
            SessionActor.closeSuspendableAsync store LifecyclePipeActors.tenant created.Id child LifecyclePipeActors.ct

        let answeredEarly =
            LifecyclePipeActors.waitFor (TimeSpan.FromSeconds 1.0) (fun () -> closeTask.IsCompleted)

        Assert.False(answeredEarly)

        admitGate.SetResult() |> ignore

        let! closed = closeTask.WaitAsync(LifecyclePipeActors.bound)
        Assert.Equal(SessionState.Closed, closed.State)
        Assert.Equal(SessionState.Closed, LifecyclePipeActors.storedState store created.Id)

        // The finished turn committed before the close landed, and nothing
        // ran after it.
        let events = LifecyclePipeActors.settledJournal journal created.Id

        let terminals =
            events
            |> List.filter (fun event -> event :? TurnCompletedEvent || event :? TurnAbortedEvent)

        Assert.Equal(1, terminals.Length)
        let committed = Assert.IsType<TurnCompletedEvent>(terminals[0])
        Assert.Equal(entry.TurnId, committed.TurnId)
        Assert.Equal(1, entered.Count)
    }
