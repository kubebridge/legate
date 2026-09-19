// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DispatcherTests

open System
open System.Collections.Generic
open System.Diagnostics
open System.Diagnostics.Metrics
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options
open Xunit

// Polling dispatcher with wake hints (issue 120): the authoritative poll
// pass over the bounded dispatch candidates with its conjunctive capacity
// gate, the coalesced in-process wake sink, the idempotent check-inbox actor
// message, and the hosted loop wiring. Store tests run over the in-memory
// implementation on a TestClock and resolve actors to Nobody: never sleeps.
// Actor tests spawn suspendable actors with scripted runners and prove the
// Idle drain, the non-Idle no-ops, and the duplicate-free race.

let tenant = TenantId.Create "acme"

// ──────────────────────────────────────────────────────────────────────────
// Store doubles

/// An Idle session row for the given agent; the store stamps the rest.
let private sampleSession (agentId: AgentId) =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = agentId
        Title = "dispatch"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// A store over a fresh database on a TestClock, plus the clock.
let private createStore () =
    let clock = TestClock()
    let store = InMemoryStoreFactory.sessionStore (InMemoryDatabase(clock))
    store, clock

/// Creates the session row, blocking.
let private createSession (store: ISessionStore) (agentId: AgentId) : Session =
    store.CreateSession(tenant, sampleSession agentId, CancellationToken.None).GetAwaiter().GetResult()

/// Appends one Queue user message straight to the store, blocking.
let private appendStored (store: ISessionStore) (sessionId: SessionId) (text: string) : InboxEntry =
    let payload = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload

    store
        .AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
        .GetAwaiter()
        .GetResult()

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

/// Records every resolved session id and answers Nobody: the pass-level
/// seam for gate and paging tests, where no actor runs.
type private RecordingResolve() =
    let gate = obj ()
    let resolved = ResizeArray<SessionId>()

    /// The resolved session ids, in resolve order.
    member _.Resolved: IReadOnlyList<SessionId> =
        lock gate (fun () -> ResizeArray<SessionId>(resolved) :> IReadOnlyList<SessionId>)

    /// The resolve function the pass drives.
    member _.Func: SessionId -> CancellationToken -> Task<IActorRef> =
        fun sessionId _ ->
            lock gate (fun () -> resolved.Add(sessionId))
            Task.FromResult(ActorRefs.Nobody :> IActorRef)

/// Runs emit under a MeterListener scoped to Meter("Legate") and returns
/// every double measurement the listener observed.
let private collectDoubles (emit: unit -> unit) : (string * float) list =
    let gate = obj ()
    let observed = ResizeArray<string * float>()

    use listener = new MeterListener()

    listener.InstrumentPublished <-
        Action<Instrument, MeterListener>(fun instrument _ ->
            if instrument.Meter.Name = Telemetry.MeterName then
                listener.EnableMeasurementEvents(instrument, null) |> ignore)

    listener.SetMeasurementEventCallback<double>(fun instrument measurement _ _ ->
        lock gate (fun () -> observed.Add(instrument.Name, measurement)))

    listener.Start()
    emit ()
    lock gate (fun () -> observed |> List.ofSeq)

// ──────────────────────────────────────────────────────────────────────────
// Actor doubles

/// A minimal journal: appends land in order, replays read from the start.
type private DispatchJournal() =
    let events = ResizeArray<SessionEvent>()

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

/// A completed TurnResult carrying assistant text.
let private completed text =
    {
        AssistantText = text
        Status = TurnStatus.Completed
        Iterations = 1
        Usage = { InputTokens = 0L; OutputTokens = 0L }
        Outcome = null
    }

/// A settled completion with no suspension.
let private settledCompletion text : TurnLoop.TurnLoopCompletion =
    {
        Result = completed text
        HasPendingInjects = false
        Suspension = None
    }

/// Scripted suspendable runner: answers every attempt with the next
/// completion and records entries in call order.
type private ScriptSuspendRunner(completions: TurnLoop.TurnLoopCompletion list) =
    let gate = obj ()
    let mutable calls = 0
    let entries = ResizeArray<InboxEntry>()

    /// How many turns ran.
    member _.Calls = lock gate (fun () -> calls)

    /// The entries turns executed, in execution order.
    member _.Entries: InboxEntry list = lock gate (fun () -> entries |> List.ofSeq)

    /// The runner as the suspendable delegate.
    member _.Func: SessionActor.SuspendableRunner =
        fun entry _ _ _ _ _ _ ->
            lock gate (fun () ->
                calls <- calls + 1
                entries.Add(entry))

            let index = min (lock gate (fun () -> calls) - 1) (completions.Length - 1)
            Task.FromResult(completions[index])

/// Gated suspendable runner: the first turn waits for Release, later turns
/// complete at once. Signals Started when the first turn begins.
type private GatedSuspendRunner(completion: TurnLoop.TurnLoopCompletion) =
    let started = new TaskCompletionSource<unit>()
    let release = new TaskCompletionSource<unit>()
    let gate = obj ()
    let mutable calls = 0
    let entries = ResizeArray<InboxEntry>()

    /// Fires when the first turn begins.
    member _.Started = started.Task

    /// Releases the waiting turns to complete.
    member _.Release() = release.TrySetResult() |> ignore

    /// How many turns ran.
    member _.Calls = lock gate (fun () -> calls)

    /// The entries turns executed, in execution order.
    member _.Entries: InboxEntry list = lock gate (fun () -> entries |> List.ofSeq)

    /// The runner as the suspendable delegate.
    member _.Func: SessionActor.SuspendableRunner =
        fun entry _ _ _ _ _ _ ->
            lock gate (fun () ->
                calls <- calls + 1
                entries.Add(entry))

            started.TrySetResult() |> ignore

            task {
                do! release.Task
                return completion
            }

/// Builds one suspend cursor for the scripted runner.
let private suspendCursor (requestId: string) (toolName: string) : TurnLoop.TurnLoopSuspension =
    let args = Dictionary<string, obj>() :> IDictionary<string, obj>
    let pendingCall = FunctionCallContent("call-1", toolName, args)

    {
        RequestId = requestId
        ToolName = toolName
        ToolCallId = "call-1"
        Kind = TurnLoop.PermissionSuspension
        QuestionText = ""
        QuestionOptions = []
        HistorySnapshot = ResizeArray<ChatMessage>() :> IList<ChatMessage>
        InputTokens = 3L
        OutputTokens = 5L
        Iterations = 1
        PendingCall = pendingCall
        Nested = None
    }

/// A Suspended completion parking on the given cursor.
let private suspendedCompletion (cursor: TurnLoop.TurnLoopSuspension) : TurnLoop.TurnLoopCompletion =
    {
        Result =
            {
                AssistantText = ""
                Status = TurnStatus.Suspended
                Iterations = cursor.Iterations
                Usage =
                    {
                        InputTokens = cursor.InputTokens
                        OutputTokens = cursor.OutputTokens
                    }
                Outcome = null
            }
        HasPendingInjects = false
        Suspension = Some cursor
    }

/// Starts a local actor system for one test.
let private createSystem () : ActorSystem = LocalActorSystem.createSystem ()

/// Terminates a test system, bounding the drain.
let private stopSystem (system: ActorSystem) : unit =
    system.Terminate() |> ignore
    system.WhenTerminated.Wait(TimeSpan.FromSeconds 10.0) |> ignore

/// Spawns a suspendable session actor on a test system.
let private spawnSuspendable
    (system: ActorSystem)
    (store: ISessionStore)
    (journal: DispatchJournal)
    (sessionId: SessionId)
    (runner: SessionActor.SuspendableRunner)
    (settled: ResizeArray<TurnResult>)
    : IActorRef =
    let baseProps: SessionActorProps =
        {
            Store = store
            Tenant = tenant
            SessionId = sessionId
            RunTurn = (fun _ _ -> Task.FromResult(completed "unused"))
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
        }

    spawn system $"dispatch-{Guid.NewGuid():N}" (SessionActor.behaviorWithSuspend baseProps deps)

/// Wakes a suspendable actor through the dispatcher boundary, blocking.
let private checkInbox (store: ISessionStore) (sessionId: SessionId) (session: IActorRef) : unit =
    SessionActor.checkInboxAsync store tenant sessionId session CancellationToken.None
    |> fun task -> task.GetAwaiter().GetResult()

/// Prompts a suspendable actor, blocking for the ack.
let private promptSuspendable (store: ISessionStore) (sessionId: SessionId) (session: IActorRef) (text: string) =
    SessionActor.promptSuspendableAsync store tenant sessionId session (UserMessage.Text text) CancellationToken.None
    |> fun task -> task.GetAwaiter().GetResult()

// ──────────────────────────────────────────────────────────────────────────
// Poll pass: authoritative sweep and conjunctive gate

[<Fact>]
let ``Authoritative poll wakes Idle sessions with stored work and no wake`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())

        let! _ =
            store.AppendInboxMessage(
                tenant,
                created.Id,
                UserMessagePayload(UserMessage.Text "hi") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync
                store
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        result.Started |> should equal 1
        result.Queued |> should equal 0
        resolve.Resolved.Count |> should equal 1
        resolve.Resolved[0] |> should equal created.Id
    }

[<Fact>]
let ``Tripped process limit keeps the session queued`` () =
    task {
        let store, clock = createStore ()
        let sessions = SessionsOptions(Capacity = 1)

        let! running = store.CreateSession(tenant, sampleSession (AgentId.New()), CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                running.Id,
                UserMessagePayload(UserMessage.Text "work") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! _ = store.ClaimNextTurn(tenant, running.Id, "owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)

        let! quiet = store.CreateSession(tenant, sampleSession (AgentId.New()), CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                quiet.Id,
                UserMessagePayload(UserMessage.Text "queued") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync
                store
                tenant
                sessions
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        result.Started |> should equal 0
        result.QueuedProcess |> should equal 1
        result.QueuedTenant |> should equal 0
        result.QueuedAgent |> should equal 0
        resolve.Resolved.Count |> should equal 0
    }

[<Fact>]
let ``Tripped tenant limit keeps the sessions queued`` () =
    task {
        let store, clock = createStore ()
        let sessions = SessionsOptions(MaxSessionsPerTenant = 1)
        let dispatcher = DispatcherOptions(MaxSessionsPerAgent = 100)

        for _ in 1..2 do
            let! created = store.CreateSession(tenant, sampleSession (AgentId.New()), CancellationToken.None)

            let! _ =
                store.AppendInboxMessage(
                    tenant,
                    created.Id,
                    UserMessagePayload(UserMessage.Text "queued") :> InboxPayload,
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            ()

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync store tenant sessions dispatcher resolve.Func clock CancellationToken.None

        result.Started |> should equal 0
        result.QueuedProcess |> should equal 0
        result.QueuedTenant |> should equal 2
        result.QueuedAgent |> should equal 0
        resolve.Resolved.Count |> should equal 0
    }

[<Fact>]
let ``Tripped agent limit keeps the sessions queued`` () =
    task {
        let store, clock = createStore ()
        let agent = AgentId.New()
        let dispatcher = DispatcherOptions(MaxSessionsPerAgent = 2)

        for _ in 1..2 do
            let! created = store.CreateSession(tenant, sampleSession agent, CancellationToken.None)

            let! _ =
                store.AppendInboxMessage(
                    tenant,
                    created.Id,
                    UserMessagePayload(UserMessage.Text "queued") :> InboxPayload,
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            ()

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync
                store
                tenant
                (SessionsOptions())
                dispatcher
                resolve.Func
                clock
                CancellationToken.None

        result.Started |> should equal 0
        result.QueuedProcess |> should equal 0
        result.QueuedTenant |> should equal 0
        result.QueuedAgent |> should equal 2
        resolve.Resolved.Count |> should equal 0
    }

[<Fact>]
let ``Gate order is labeling only: process wins over tenant over agent`` () =
    task {
        let store, clock = createStore ()
        // Every limit trips for the one pending session: capacity 1 with a
        // running turn, one tenant slot with two sessions, one agent slot
        // with two sessions of the same agent.
        let sessions = SessionsOptions(Capacity = 1, MaxSessionsPerTenant = 1)
        let dispatcher = DispatcherOptions(MaxSessionsPerAgent = 1)
        let agent = AgentId.New()

        let! running = store.CreateSession(tenant, sampleSession agent, CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                running.Id,
                UserMessagePayload(UserMessage.Text "work") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! _ = store.ClaimNextTurn(tenant, running.Id, "owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)

        let! queued = store.CreateSession(tenant, sampleSession agent, CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                queued.Id,
                UserMessagePayload(UserMessage.Text "queued") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync store tenant sessions dispatcher resolve.Func clock CancellationToken.None

        // The process limit labels the queued session even though the
        // tenant and agent limits trip too: no precedence, only labeling.
        result.Started |> should equal 0
        result.QueuedProcess |> should equal 1
        result.QueuedTenant |> should equal 0
        result.QueuedAgent |> should equal 0
    }

[<Fact>]
let ``Local in-flight gate bounds one pass under capacity`` () =
    task {
        let store, clock = createStore ()
        let sessions = SessionsOptions(Capacity = 2)

        for _ in 1..3 do
            let! created = store.CreateSession(tenant, sampleSession (AgentId.New()), CancellationToken.None)

            let! _ =
                store.AppendInboxMessage(
                    tenant,
                    created.Id,
                    UserMessagePayload(UserMessage.Text "work") :> InboxPayload,
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            ()

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync
                store
                tenant
                sessions
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        // The store snapshot reads zero running, so the first two wakes
        // admit and the local gate queues the third: no overshoot.
        result.Started |> should equal 2
        result.QueuedProcess |> should equal 1
        resolve.Resolved.Count |> should equal 2
    }

[<Fact>]
let ``Pass follows HasMore across bounded batches`` () =
    task {
        let store, clock = createStore ()
        let dispatcher = DispatcherOptions(MaxBatchSize = 1)

        let! first = store.CreateSession(tenant, sampleSession (AgentId.New()), CancellationToken.None)
        let firstEntry = appendStored store first.Id "first"

        let! second = store.CreateSession(tenant, sampleSession (AgentId.New()), CancellationToken.None)
        let secondEntry = appendStored store second.Id "second"

        // The resolve seam consumes the woken entry, simulating the
        // actor-side start-and-settle the steady state reaches: each batch
        // head shrinks, so the pass follows HasMore to the second session
        // instead of re-reading the same head.
        let resolve (sessionId: SessionId) (candidateToken: CancellationToken) : Task<IActorRef> =
            task {
                let! pending = store.ReadPendingInbox(tenant, sessionId, candidateToken)

                let positions =
                    if isNull (box pending) then
                        [||]
                    else
                        pending
                        |> Seq.filter (fun entry -> not (isNull (box entry)))
                        |> Seq.map (fun entry -> entry.Position)
                        |> Seq.toArray

                let! _ = store.MarkInboxConsumed(tenant, sessionId, positions :> IReadOnlyList<int64>, candidateToken)
                return ActorRefs.Nobody :> IActorRef
            }

        let! result =
            Dispatcher.passOnceAsync store tenant (SessionsOptions()) dispatcher resolve clock CancellationToken.None

        result.Started |> should equal 2
        result.Queued |> should equal 0

        // Positions are per-session inbox order: each session's head is its
        // first entry.
        firstEntry.Position |> should equal 1L
        secondEntry.Position |> should equal 1L
    }

[<Fact>]
let ``Started sessions record the inbox-age dispatch latency`` () =
    let store, clock = createStore ()
    let created = createSession store (AgentId.New())
    appendStored store created.Id "hi" |> ignore

    // Thirty seconds pass between the append and the sweep.
    clock.Advance(TimeSpan.FromSeconds 30.0)

    let resolve = RecordingResolve()

    let measurements =
        collectDoubles (fun () ->
            Dispatcher.passOnceAsync
                store
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None
            |> fun task -> task.GetAwaiter().GetResult() |> ignore)

    let latency =
        measurements
        |> List.filter (fun (instrument, _) -> instrument = Telemetry.DispatchLatencyName)
        |> List.map snd

    latency.Length |> should be (greaterThanOrEqualTo 1)

    latency
    |> List.iter (fun value -> value |> should be (greaterThanOrEqualTo 29000.0))

// ──────────────────────────────────────────────────────────────────────────
// Wake sink: coalesced, latency-only hints

[<Fact>]
let ``Wake sink coalesces to one wake per session per cycle`` () =
    let sink = DispatcherWakeSink()
    let first = SessionId.New()
    let second = SessionId.New()

    sink.RequestWake(first)
    sink.RequestWake(first)
    sink.RequestWake(first)
    sink.RequestWake(second)

    let drained = sink.TakePending()
    drained.Count |> should equal 2
    drained |> Seq.contains first |> should equal true
    drained |> Seq.contains second |> should equal true

    // The drain resets the cycle: nothing pending until the next request.
    sink.TakePending().Count |> should equal 0

    sink.RequestWake(first)
    sink.TakePending().Count |> should equal 1

[<Fact>]
let ``WaitAsync pulses on the next request after a drain`` () =
    task {
        let sink = DispatcherWakeSink()
        let sessionId = SessionId.New()

        sink.RequestWake(sessionId)
        sink.TakePending() |> ignore

        let waiting = sink.WaitAsync(CancellationToken.None)
        waiting.IsCompleted |> should equal false

        sink.RequestWake(sessionId)
        do! waiting
    }

[<Fact>]
let ``WaitAsync abandons on cancellation`` () =
    use cancelled = new CancellationTokenSource()
    cancelled.Cancel()

    let sink = DispatcherWakeSink()
    let waiting = sink.WaitAsync(cancelled.Token)

    let abandoned =
        try
            waiting.GetAwaiter().GetResult() |> ignore
            false
        with :? OperationCanceledException ->
            true

    abandoned |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Actor: idempotent check-inbox

/// Reads the store's pending inbox, blocking.
let private pendingOf (store: ISessionStore) (sessionId: SessionId) : IReadOnlyList<InboxEntry> =
    store.ReadPendingInbox(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult()

[<Fact>]
let ``Idle with pending starts the oldest drainable entry without appending`` () =
    use system = createSystem ()
    let store = InMemorySessionStore(InMemoryDatabase()) :> ISessionStore
    let created = createSession store (AgentId.New())
    let first = appendStored store created.Id "first"
    appendStored store created.Id "second" |> ignore

    let journal = DispatchJournal()
    let runner = GatedSuspendRunner(settledCompletion "done")
    let settled = ResizeArray<TurnResult>()
    let session = spawnSuspendable system store journal created.Id runner.Func settled

    try
        checkInbox store created.Id session

        let started = waitFor (TimeSpan.FromSeconds 10.0) (fun () -> runner.Calls = 1)
        started |> should equal true

        // The oldest drainable entry runs, and nothing was appended: both
        // original entries are still pending while the turn is gated.
        runner.Entries.Length |> should equal 1
        runner.Entries[0].Position |> should equal first.Position
        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 2

        runner.Release()

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                runner.Calls = 2 && (storedOf store created.Id).State = SessionState.Idle)

        drained |> should equal true

        // The settle drain starts the second entry in position order.
        runner.Entries.Length |> should equal 2
        runner.Entries[1].Position |> should be (greaterThan first.Position)
    finally
        stopSystem system

[<Fact>]
let ``Check-inbox on a Running session is a no-op`` () =
    use system = createSystem ()
    let store = InMemorySessionStore(InMemoryDatabase()) :> ISessionStore
    let created = createSession store (AgentId.New())

    let journal = DispatchJournal()
    let runner = GatedSuspendRunner(settledCompletion "done")
    let settled = ResizeArray<TurnResult>()
    let session = spawnSuspendable system store journal created.Id runner.Func settled

    try
        promptSuspendable store created.Id session "run" |> ignore

        let started = waitFor (TimeSpan.FromSeconds 10.0) (fun () -> runner.Calls = 1)
        started |> should equal true

        checkInbox store created.Id session
        runner.Release()

        let finished =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && (storedOf store created.Id).State = SessionState.Idle)

        finished |> should equal true
        runner.Calls |> should equal 1
    finally
        stopSystem system

[<Fact>]
let ``Check-inbox on a WaitingForInput session is a no-op`` () =
    use system = createSystem ()
    let store = InMemorySessionStore(InMemoryDatabase()) :> ISessionStore
    let created = createSession store (AgentId.New())

    let journal = DispatchJournal()

    let runner =
        ScriptSuspendRunner(
            [
                suspendedCompletion (suspendCursor "req-1" "exec")
            ]
        )

    let settled = ResizeArray<TurnResult>()
    let session = spawnSuspendable system store journal created.Id runner.Func settled

    try
        promptSuspendable store created.Id session "run" |> ignore

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        checkInbox store created.Id session
        Thread.Sleep(250)

        runner.Calls |> should equal 1
        (storedOf store created.Id).State |> should equal SessionState.WaitingForInput

        let snapshot =
            SessionActor.getSuspendSnapshotAsync session CancellationToken.None
            |> fun task -> task.GetAwaiter().GetResult()

        snapshot.State |> should equal SessionState.WaitingForInput
        snapshot.PendingRequestId |> should equal "req-1"
    finally
        stopSystem system

[<Fact>]
let ``Check-inbox on a Closed session is a no-op`` () =
    use system = createSystem ()
    let store = InMemorySessionStore(InMemoryDatabase()) :> ISessionStore
    let created = createSession store (AgentId.New())
    appendStored store created.Id "orphan" |> ignore

    store.CloseSession(tenant, created.Id, CancellationToken.None).GetAwaiter().GetResult()
    |> ignore

    let journal = DispatchJournal()
    let runner = ScriptSuspendRunner([ settledCompletion "done" ])
    let settled = ResizeArray<TurnResult>()
    let session = spawnSuspendable system store journal created.Id runner.Func settled

    try
        // The dispatcher boundary skips Closed sessions before touching
        // the actor: no throw, no start.
        checkInbox store created.Id session
        Thread.Sleep(250)

        runner.Calls |> should equal 0
        (storedOf store created.Id).State |> should equal SessionState.Closed
    finally
        stopSystem system

[<Fact>]
let ``Double wake while Idle starts exactly one turn`` () =
    use system = createSystem ()
    let store = InMemorySessionStore(InMemoryDatabase()) :> ISessionStore
    let created = createSession store (AgentId.New())
    appendStored store created.Id "wake" |> ignore

    let journal = DispatchJournal()
    let runner = GatedSuspendRunner(settledCompletion "done")
    let settled = ResizeArray<TurnResult>()
    let session = spawnSuspendable system store journal created.Id runner.Func settled

    try
        // Two wakes race on one Idle session with one pending entry: the
        // mailbox serializes them and the Idle re-check makes the second a
        // no-op, so the entry runs once.
        checkInbox store created.Id session
        checkInbox store created.Id session

        let started = waitFor (TimeSpan.FromSeconds 10.0) (fun () -> runner.Calls = 1)
        started |> should equal true

        runner.Release()

        let finished =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && (storedOf store created.Id).State = SessionState.Idle)

        finished |> should equal true
        runner.Calls |> should equal 1
        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Check-vs-prompt race is duplicate-free`` () =
    use system = createSystem ()
    let store = InMemorySessionStore(InMemoryDatabase()) :> ISessionStore
    let created = createSession store (AgentId.New())
    appendStored store created.Id "stored" |> ignore

    let journal = DispatchJournal()
    let runner = GatedSuspendRunner(settledCompletion "done")
    let settled = ResizeArray<TurnResult>()
    let session = spawnSuspendable system store journal created.Id runner.Func settled

    try
        let checkTask =
            SessionActor.checkInboxAsync store tenant created.Id session CancellationToken.None

        let promptTask =
            SessionActor.promptSuspendableAsync
                store
                tenant
                created.Id
                session
                (UserMessage.Text "prompted")
                CancellationToken.None
            :> Task

        Task.WhenAll([| checkTask; promptTask |]).GetAwaiter().GetResult() |> ignore

        // Whichever message the mailbox ordered first started the turn;
        // the other queued: exactly one turn runs before the release.
        let started = waitFor (TimeSpan.FromSeconds 10.0) (fun () -> runner.Calls = 1)
        started |> should equal true

        runner.Release()

        let finished =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 2 && (storedOf store created.Id).State = SessionState.Idle)

        finished |> should equal true

        // Both entries ran exactly once: distinct positions, nothing left.
        runner.Calls |> should equal 2

        runner.Entries
        |> List.map (fun entry -> entry.Position)
        |> List.distinct
        |> List.length
        |> should equal 2

        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 0
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Hosted loop wiring

/// Builds the container the dispatcher service runs on: the store, the
/// facade tenant, the options, the wake sink, and a client resolving to
/// the given actor.
let private buildServiceProvider
    (store: ISessionStore)
    (clock: TimeProvider)
    (resolve: SessionId -> CancellationToken -> Task<IActorRef>)
    =
    let journal = DispatchJournal()

    let bus =
        new SessionEventBus(journal :> ISessionEventStore, SessionSubscriptionOptions(), null)

    let client =
        new SessionClient(store, tenant, resolve, bus, TimeSpan.FromMinutes 1.0, RecordingDelay() :> ILlmDelay)

    let services = ServiceCollection()
    services.AddSingleton<ISessionStore>(store) |> ignore
    services.AddSingleton<SessionClient>(client) |> ignore
    services.AddSingleton<DispatcherWakeSink>(DispatcherWakeSink()) |> ignore

    services.AddSingleton<SessionClientOptions>(SessionClientOptions(Tenant = tenant))
    |> ignore

    services.Configure<LegateOptions>(Action<LegateOptions>(fun _ -> ())) |> ignore

    services.AddSingleton<TimeProvider>(clock) |> ignore
    services.AddSingleton<ILlmDelay>(RecordingDelay() :> ILlmDelay) |> ignore
    services.BuildServiceProvider()

[<Fact>]
let ``RunOnceAsync dispatches pending sessions through the client actors`` () =
    use system = createSystem ()
    let clock = TestClock()
    let store = InMemoryStoreFactory.sessionStore (InMemoryDatabase(clock))
    let created = createSession store (AgentId.New())
    appendStored store created.Id "hi" |> ignore

    let journal = DispatchJournal()
    let runner = ScriptSuspendRunner([ settledCompletion "done" ])
    let settled = ResizeArray<TurnResult>()
    let actor = spawnSuspendable system store journal created.Id runner.Func settled

    try
        use provider = buildServiceProvider store clock (fun _ _ -> Task.FromResult(actor))

        let options = provider.GetRequiredService<IOptions<LegateOptions>>()

        let service =
            DispatcherService(provider, options, clock, RecordingDelay() :> ILlmDelay)

        let sink = provider.GetRequiredService<DispatcherWakeSink>()
        sink.RequestWake(created.Id)

        let result = service.RunOnceAsync(CancellationToken.None).GetAwaiter().GetResult()

        result.Started |> should equal 1
        result.Queued |> should equal 0

        // The cycle drained the wake: the next cycle coalesces from empty.
        sink.TakePending().Count |> should equal 0

        let started = waitFor (TimeSpan.FromSeconds 10.0) (fun () -> runner.Calls = 1)
        started |> should equal true
    finally
        stopSystem system

[<Fact>]
let ``DispatcherService resolves its store lazily at loop start`` () =
    let clock = TestClock()
    let services = ServiceCollection()

    services.Configure<LegateOptions>(Action<LegateOptions>(fun _ -> ())) |> ignore

    services.AddSingleton<TimeProvider>(clock :> TimeProvider) |> ignore
    services.AddSingleton<ILlmDelay>(RecordingDelay() :> ILlmDelay) |> ignore

    use provider = services.BuildServiceProvider()
    let options = provider.GetRequiredService<IOptions<LegateOptions>>()

    // Construction touches nothing: the store, client, and sink resolve
    // when the loop starts, so an empty host fails there instead.
    let service =
        DispatcherService(provider, options, clock, RecordingDelay() :> ILlmDelay)

    Assert.Throws<InvalidOperationException>(fun () ->
        service.RunOnceAsync(CancellationToken.None).GetAwaiter().GetResult() |> ignore)
    |> ignore
