// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DispatcherTests

open System
open System.Collections.Generic
open System.Diagnostics
open System.Diagnostics.Metrics
open System.IO
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
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

[<Fact>]
let ``issue415 Running consumed target discovery wakes without replacing authority at full capacity`` () =
    task {
        let clock = TestClock()
        let database = InMemoryDatabase(clock)
        let store = InMemoryStoreFactory.sessionStore database
        let journal = InMemoryStoreFactory.eventStore database
        let session = createSession store (AgentId.New())
        let entry = appendStored store session.Id "original"

        let! lease =
            store.ClaimNextTurn(tenant, session.Id, "victim", TimeSpan.FromSeconds(60.0), CancellationToken.None)

        let claim = Assert.IsType<TurnLeaseRenewed>(lease).Claim
        let control = store :?> ISessionAbortControlStore

        let! bound =
            control.BindControlTarget(tenant, session.Id, entry.TurnId, entry.Position, claim, CancellationToken.None)

        Assert.Equal(ControlOperationOutcome.Applied, bound.Outcome)
        let! _ = store.UpdateSessionState(tenant, session.Id, SessionState.Running, CancellationToken.None)
        let! _ = store.MarkInboxConsumed(tenant, session.Id, [| entry.Position |], CancellationToken.None)
        clock.Advance(TimeSpan.FromSeconds(61.0))
        let resolve = RecordingResolve()
        let sessions = SessionsOptions(Capacity = 1)

        let! _ =
            Dispatcher.passOnceRoutedAsync
                (fun _ -> true)
                store
                journal
                (fun _ _ _ -> Task.FromResult(false))
                tenant
                sessions
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        Assert.Contains(session.Id, resolve.Resolved)
        let! target = control.ReadAbortTarget(tenant, session.Id, CancellationToken.None)

        match target with
        | null -> failwith "Lost original target"
        | target ->
            Assert.Equal(entry.TurnId, target.TurnId)
            Assert.Equal(entry.Position, target.InboxPosition)

        let! pending = store.ReadPendingInbox(tenant, session.Id, CancellationToken.None)
        Assert.Empty(pending)
        Assert.Equal(claim.TurnId, (storedOf store session.Id).CurrentTurnId.Value)
    }

[<Fact>]
let ``issue415 Running keyset advances past refused pages and retries finite sweeps`` () =
    task {
        let clock = TestClock()
        let database = InMemoryDatabase(clock)

        let store, journal =
            InMemoryStoreFactory.sessionStore database, InMemoryStoreFactory.eventStore database

        let ids = ResizeArray<SessionId>()

        for _ in 1..7 do
            let session = createSession store (AgentId.New())
            let entry = appendStored store session.Id "original"

            let! lease =
                store.ClaimNextTurn(tenant, session.Id, "owner", TimeSpan.FromMinutes(5.0), CancellationToken.None)

            let claim = Assert.IsType<TurnLeaseRenewed>(lease).Claim

            let! _ =
                (store :?> ISessionAbortControlStore)
                    .BindControlTarget(tenant, session.Id, entry.TurnId, entry.Position, claim, CancellationToken.None)

            let! _ = store.UpdateSessionState(tenant, session.Id, SessionState.Running, CancellationToken.None)
            let! _ = store.MarkInboxConsumed(tenant, session.Id, [| entry.Position |], CancellationToken.None)
            ids.Add(session.Id)

        let ordered = ids |> Seq.sortBy (fun id -> id.Value) |> Seq.toArray
        let refused = HashSet<SessionId>(ordered |> Seq.take 3)
        let resolve = RecordingResolve()
        let cursor = ref null

        let run () =
            Dispatcher.passOnceRoutedWithCursorAsync
                (fun session -> not (refused.Contains session.Id))
                store
                journal
                (fun _ _ _ -> Task.FromResult false)
                tenant
                (SessionsOptions(Capacity = 1))
                (DispatcherOptions(MaxBatchSize = 2))
                resolve.Func
                clock
                CancellationToken.None
                cursor

        let! _ = run ()
        Assert.Equal(2, resolve.Resolved.Count)
        let! _ = run ()
        Assert.Equal(4, resolve.Resolved.Count)
        Assert.Null(cursor.Value)
        let! _ = run ()
        Assert.Equal(6, resolve.Resolved.Count)

        for id in ordered |> Seq.skip 3 do
            Assert.Contains(id, resolve.Resolved)

        for id in refused do
            Assert.DoesNotContain(id, resolve.Resolved)
    }

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

        member _.AppendHostEvents(_, _, _, batch, _) =
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

/// A quiet era reader (issue 289): pre-era, so the orphan sweep stays off
/// unless a fact opts in with markedEra.
let private preEra: TenantId -> SessionId -> CancellationToken -> Task<bool> =
    fun _ _ _ -> Task.FromResult false

/// A marked era reader (issue 289): every session reads era-marked.
let private markedEra: TenantId -> SessionId -> CancellationToken -> Task<bool> =
    fun _ _ _ -> Task.FromResult true

/// Seeds one marker row for the turn id into the test journal, blocking.
let private seedMarker (journal: DispatchJournal) (sessionId: SessionId) (turnId: TurnId) : unit =
    let marker =
        TurnStartedEvent(sessionId, turnId, Nullable<int64>(), DateTimeOffset.UtcNow) :> SessionEvent

    let batch = ResizeArray<SessionEvent>([| marker |]) :> IReadOnlyList<SessionEvent>

    (journal :> ISessionEventStore).Append(tenant, sessionId, "test-token", batch, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult() |> ignore

/// Seeds one terminal completion row for the turn id into the test
/// journal, blocking.
let private seedTerminal (journal: DispatchJournal) (sessionId: SessionId) (turnId: TurnId) : unit =
    let terminal =
        TurnCompletedEvent(sessionId, turnId, Nullable<int64>(), DateTimeOffset.UtcNow) :> SessionEvent

    let batch = ResizeArray<SessionEvent>([| terminal |]) :> IReadOnlyList<SessionEvent>

    (journal :> ISessionEventStore).Append(tenant, sessionId, "test-token", batch, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult() |> ignore

/// Replays the test journal from the start, blocking.
let private journalEvents (journal: DispatchJournal) (sessionId: SessionId) : IReadOnlyList<SessionEvent> =
    match
        (journal :> ISessionEventStore).Replay(tenant, sessionId, 0L, 100, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? EventReplayPage as page when not (isNull (box page)) && not (isNull (box page.Events)) -> page.Events
    | _ -> ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>

/// Every TurnFailedEvent in the replayed journal for the turn id.
let private failedFor (journal: DispatchJournal) (sessionId: SessionId) (turnId: TurnId) : TurnFailedEvent list =
    journalEvents journal sessionId
    |> Seq.choose (fun event ->
        match event with
        | :? TurnFailedEvent as failed when failed.TurnId.Equals(turnId) -> Some failed
        | _ -> None)
    |> List.ofSeq

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
        TurnId = Unchecked.defaultof<TurnId>
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
        fun entry _ _ _ _ _ _ _ _ _ _ ->
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
        fun entry _ _ _ _ _ _ _ _ _ _ ->
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
        OriginTurnId = Unchecked.defaultof<TurnId>
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
        TurnId = cursor.OriginTurnId
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
            Settlement = None
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
            PrimeClaim = None
            Recovery = null
            RunSuspendable = runner
            ReprimeJournal = None
            RefreshCompact = None
            AgentStore = null
            EraMarked = preEra
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
                (DispatchJournal() :> ISessionEventStore)
                preEra
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
                (DispatchJournal() :> ISessionEventStore)
                preEra
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
            Dispatcher.passOnceAsync
                store
                (DispatchJournal() :> ISessionEventStore)
                preEra
                tenant
                sessions
                dispatcher
                resolve.Func
                clock
                CancellationToken.None

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
                (DispatchJournal() :> ISessionEventStore)
                preEra
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
            Dispatcher.passOnceAsync
                store
                (DispatchJournal() :> ISessionEventStore)
                preEra
                tenant
                sessions
                dispatcher
                resolve.Func
                clock
                CancellationToken.None

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
                (DispatchJournal() :> ISessionEventStore)
                preEra
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
            Dispatcher.passOnceAsync
                store
                (DispatchJournal() :> ISessionEventStore)
                preEra
                tenant
                (SessionsOptions())
                dispatcher
                resolve
                clock
                CancellationToken.None

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
                (DispatchJournal() :> ISessionEventStore)
                preEra
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

[<Theory>]
[<InlineData(0)>]
[<InlineData(1)>]
[<InlineData(2)>]
let ``control candidate refusal cannot starve later eligible sessions on repeated sweeps`` disposition : Task =
    task {
        let clock = TestClock()
        let database = InMemoryDatabase(clock)
        let store = InMemoryStoreFactory.sessionStore database
        let journal = InMemoryStoreFactory.eventStore database
        let control = store :?> ISessionAbortControlStore

        let first =
            { sampleSession (AgentId.New()) with
                Id = SessionId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FAV")
            }

        let! first = store.CreateSession(tenant, first, CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                first.Id,
                UserMessagePayload(UserMessage.Text "prime"),
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! lease = store.ClaimNextTurn(tenant, first.Id, "owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)

        let claim =
            match lease with
            | :? TurnLeaseRenewed as lease -> lease.Claim
            | _ -> failwith "No prime"

        let! current =
            store.AppendInboxMessage(
                tenant,
                first.Id,
                UserMessagePayload(UserMessage.Text "current"),
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let turn = TurnId.New()
        let! _ = control.BindControlTarget(tenant, first.Id, turn, current.Position, claim, CancellationToken.None)
        let! _ = store.UpdateSessionState(tenant, first.Id, SessionState.Running, CancellationToken.None)

        if disposition = 1 then
            let! _ =
                control.RequestHostAbort(
                    tenant,
                    first.Id,
                    turn,
                    StopCause.ExplicitAbort,
                    "stop",
                    CancellationToken.None
                )

            ()

        if disposition = 2 then
            let! _ =
                control.TryDecideControlTarget(
                    tenant,
                    first.Id,
                    turn,
                    current.Position,
                    claim,
                    "report",
                    TurnStatus.Completed,
                    Nullable(),
                    null,
                    CancellationToken.None
                )

            ()

        let! later = store.CreateSession(tenant, sampleSession (AgentId.New()), CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                later.Id,
                UserMessagePayload(UserMessage.Text "eligible"),
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let resolve = RecordingResolve()

        for _ in 1..3 do
            let! result =
                Dispatcher.passOnceAsync
                    store
                    journal
                    (fun _ _ _ -> Task.FromResult false)
                    tenant
                    (SessionsOptions())
                    (DispatcherOptions())
                    resolve.Func
                    clock
                    CancellationToken.None

            Assert.True(result.Started >= 1)

        Assert.Equal(3, resolve.Resolved |> Seq.filter ((=) later.Id) |> Seq.length)
        Assert.Equal((if disposition = 0 then 3 else 0), resolve.Resolved |> Seq.filter ((=) first.Id) |> Seq.length)
        let! unchanged = store.VerifyClaim(tenant, claim, CancellationToken.None)
        Assert.IsType<TurnLeaseHeld>(unchanged) |> ignore
        let! rows = store.ReadPendingInbox(tenant, first.Id, CancellationToken.None)
        Assert.Single(rows) |> ignore
        Assert.Equal(current.Position, rows[0].Position)
        let! target = control.ReadAbortTarget(tenant, first.Id, CancellationToken.None)

        match target with
        | null -> failwith "Control binding lost"
        | target -> Assert.Equal(turn, target.TurnId)
    }

/// A route validator with an empty registry: sinkless sessions pass,
/// configured destinations refuse. Mirrors SessionClient with no
/// AddCompletionDestination registrations.
let private sinklessRoute (session: Session) : bool =
    try
        session.Options.ValidatePersistence()
        isNull session.Options.CompletionDestinationId
    with :? CompletionRoutingException ->
        false

/// A session row carrying an unregistered destination: every admission
/// refuses it before any effect.
let private routedSession (agentId: AgentId) (destinationId: string) =
    let options = SessionOptions()
    options.CompletionDestinationId <- destinationId

    { sampleSession agentId with
        Options = options
    }

[<Fact>]
let ``route refused candidates cannot starve later eligible sessions on repeated sweeps`` () =
    task {
        let clock = TestClock()
        let database = InMemoryDatabase(clock)
        let store = InMemoryStoreFactory.sessionStore database
        let journal = InMemoryStoreFactory.eventStore database

        let! first =
            store.CreateSession(tenant, routedSession (AgentId.New()) "unknown-receiver", CancellationToken.None)

        let! refused =
            store.AppendInboxMessage(
                tenant,
                first.Id,
                UserMessagePayload(UserMessage.Text "refused"),
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! later = store.CreateSession(tenant, sampleSession (AgentId.New()), CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                later.Id,
                UserMessagePayload(UserMessage.Text "eligible"),
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let resolve = RecordingResolve()

        for _ in 1..3 do
            let! result =
                Dispatcher.passOnceRoutedAsync
                    sinklessRoute
                    store
                    journal
                    (fun _ _ _ -> Task.FromResult false)
                    tenant
                    (SessionsOptions())
                    (DispatcherOptions())
                    resolve.Func
                    clock
                    CancellationToken.None

            Assert.True(result.Started >= 1)

        Assert.Equal(3, resolve.Resolved |> Seq.filter ((=) later.Id) |> Seq.length)
        Assert.Equal(0, resolve.Resolved |> Seq.filter ((=) first.Id) |> Seq.length)

        // The refused candidate kept its inbox untouched: no consumption,
        // no bootstrap, no claim.
        let! pending = store.ReadPendingInbox(tenant, first.Id, CancellationToken.None)
        Assert.Single(pending) |> ignore
        Assert.Equal(refused.Position, pending[0].Position)
    }

[<Fact>]
let ``orphan sweep advances past entirely refused pages without effects`` () =
    task {
        let clock = TestClock()
        let database = InMemoryDatabase(clock)
        let store = InMemoryStoreFactory.sessionStore database
        let journal = InMemoryStoreFactory.eventStore database

        // Three refused orphans sort before the supported sinkless orphan:
        // every page of the sweep is a refusal until the last candidate.
        // IDs sort ordinally, so fixed prefixes control the page order.
        let refusedIds =
            [|
                "00000000000000000000000001"
                "00000000000000000000000002"
                "00000000000000000000000003"
            |]
            |> Array.map SessionId.Parse

        for id in refusedIds do
            let! created =
                store.CreateSession(
                    tenant,
                    { routedSession (AgentId.New()) "unknown-receiver" with
                        Id = id
                    },
                    CancellationToken.None
                )

            let! _ =
                store.AppendInboxMessage(
                    tenant,
                    created.Id,
                    UserMessagePayload(UserMessage.Text "prime"),
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let! _ = store.ClaimNextTurn(tenant, created.Id, "owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)
            let! _ = store.UpdateSessionState(tenant, created.Id, SessionState.Idle, CancellationToken.None)
            ()

        let! later =
            store.CreateSession(
                tenant,
                { sampleSession (AgentId.New()) with
                    Id = SessionId.Parse("00000000000000000000000004")
                },
                CancellationToken.None
            )

        let! _ =
            store.AppendInboxMessage(
                tenant,
                later.Id,
                UserMessagePayload(UserMessage.Text "prime"),
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! _ = store.ClaimNextTurn(tenant, later.Id, "owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)
        let! _ = store.UpdateSessionState(tenant, later.Id, SessionState.Idle, CancellationToken.None)

        let resolve = RecordingResolve()
        let options = DispatcherOptions()
        options.MaxBatchSize <- 2

        // Repeated finite sweeps terminate and never poke a refused
        // orphan: no bootstrap, no claim, no runner.
        for _ in 1..3 do
            let! _ =
                Dispatcher.passOnceRoutedAsync
                    sinklessRoute
                    store
                    journal
                    (fun _ _ _ -> Task.FromResult false)
                    tenant
                    (SessionsOptions())
                    options
                    resolve.Func
                    clock
                    CancellationToken.None

            ()

        for id in refusedIds do
            let! pending = store.ReadPendingInbox(tenant, id, CancellationToken.None)
            Assert.Empty(pending)
    }

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
        new SessionClient(store, tenant, resolve, bus, TimeSpan.FromMinutes 1.0, RecordingDelay() :> ILlmDelay, None)

    let services = ServiceCollection()
    services.AddSingleton<ISessionStore>(store) |> ignore

    services.AddSingleton<ISessionEventStore>(journal :> ISessionEventStore)
    |> ignore

    services.AddSingleton<SessionClient>(client) |> ignore

    let context =
        {
            Tenant = tenant
            Store = store
            EventStore = journal
            SubscriptionOptions = SessionSubscriptionOptions()
            SubscriptionLifetime = new SessionSubscriptionLifetime()
            WorkTracker = new ExecutionWorkTracker()
            Spawn = fun _ _ _ -> ActorRefs.Nobody :> IActorRef
            Client = lazy (client :> obj)
            Background =
                {
                    Sessions = SessionsOptions()
                    Dispatcher = DispatcherOptions()
                    Clock = clock
                    Delay = RecordingDelay() :> ILlmDelay
                    EraMarked = preEra
                    Agents = null
                    RecoveryCursor = ref null
                }
        }

    services.AddSingleton<ISessionHostContexts>(
        { new ISessionHostContexts with
            member _.InitializeAsync(_) = Task.CompletedTask
            member _.HasDeclaredBindings = false
            member _.DefaultTenant = tenant
            member _.All = [| context |]
            member _.Get(_) = context
            member _.OpenAdmission() = ()
            member _.CloseAdmission() = ()
            member _.DrainAsync(_, _, _) = Task.CompletedTask
        }
    )
    |> ignore

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

// ──────────────────────────────────────────────────────────────────────────
// Orphan sweep: lease-gated poke for live-turn orphans (issue 289)
//
// Idle sessions still carrying a live turn with a consumed inbox never
// appear in the pending sweep, so a second bounded sweep lease-gates
// them through an atomic priming claim: an expired or missing lease lets
// the poke win and resolve, while a live lease makes the poke lose
// silently and re-consume its bootstrap. The TestClock stays frozen
// unless a fact advances it past the prime lease, so wins and losses
// are deterministic without sleeps.

/// Claims the session's appended entry so the row reads Idle with a live
/// turn and a consumed inbox, and returns the winning claim.
let private claimLiveTurn (store: ISessionStore) (sessionId: SessionId) (lease: TimeSpan) : TurnClaim =
    match
        store.ClaimNextTurn(tenant, sessionId, "owner-a", lease, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnLeaseRenewed as renewed -> renewed.Claim
    | :? TurnLeaseHeld as held -> held.Claim
    | _ -> failwith "Expected the prime claim."

[<Fact>]
let ``Orphan sweep pokes Idle sessions with a live turn and an empty inbox`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())
        appendStored store created.Id "orphaned" |> ignore
        let prime = claimLiveTurn store created.Id (TimeSpan.FromMinutes 5.0)

        // The victim's lease lapses: the poke must win. The journal holds
        // the marker with no terminal row for the live turn on an
        // era-marked session.
        let journal = DispatchJournal()
        seedMarker journal created.Id prime.TurnId
        clock.Advance(TimeSpan.FromMinutes 6.0)

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync
                store
                (journal :> ISessionEventStore)
                markedEra
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        // The winner settles directly inside the sweep: no entity
        // involvement, so nothing resolves, but the pass counts the
        // settle as started.
        result.Started |> should equal 1
        result.Queued |> should equal 0
        resolve.Resolved.Count |> should equal 0

        // Exactly one fenced TurnFailedEvent carries the orphan id with
        // the crash reason.
        let failed = failedFor journal created.Id prime.TurnId
        failed.Length |> should equal 1
        failed[0].Reason |> should equal SessionActor.CrashFailReason

        // The winning claim consumed the poke bootstrap and the prime
        // settled quietly: nothing pends and no live turn stands.
        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 0

        let after = storedOf store created.Id
        after.CurrentTurnId.HasValue |> should equal false
    }

[<Fact>]
let ``Orphan sweep never pokes a healthy session holding a live lease`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())
        appendStored store created.Id "primed" |> ignore
        let prime = claimLiveTurn store created.Id (TimeSpan.FromHours 1.0)

        // Marker-only journal on an era-marked session, but the lease is
        // live: the lease gate still wins and the poke loses.
        let journal = DispatchJournal()
        seedMarker journal created.Id prime.TurnId

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync
                store
                (journal :> ISessionEventStore)
                markedEra
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        // The poke loses silently: nothing resolves, nothing starts, the
        // bootstrap is re-consumed, the live turn is untouched, and the
        // loser journals nothing.
        result.Started |> should equal 0
        result.Queued |> should equal 0
        resolve.Resolved.Count |> should equal 0
        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 0
        journalEvents journal created.Id |> fun events -> events.Count |> should equal 1

        let after = storedOf store created.Id
        after.CurrentTurnId.HasValue |> should equal true
        after.CurrentTurnId.Value |> should equal prime.TurnId
    }

[<Fact>]
let ``Orphan sweep skips settled sessions with no live turn`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())
        appendStored store created.Id "settled" |> ignore
        let prime = claimLiveTurn store created.Id (TimeSpan.FromMinutes 5.0)

        let! settled = store.SettleTurn(tenant, prime, TurnStatus.Completed, null, CancellationToken.None)
        settled |> should be ofExactType<TurnSettled>

        let resolve = RecordingResolve()
        let journal = DispatchJournal()

        let! result =
            Dispatcher.passOnceAsync
                store
                (journal :> ISessionEventStore)
                markedEra
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        result.Started |> should equal 0
        result.Queued |> should equal 0
        resolve.Resolved.Count |> should equal 0
    }

[<Fact>]
let ``Orphan sweep leaves sessions with pending inbox to the authoritative poll`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())
        appendStored store created.Id "queued" |> ignore

        let resolve = RecordingResolve()
        let journal = DispatchJournal()

        let! result =
            Dispatcher.passOnceAsync
                store
                (journal :> ISessionEventStore)
                markedEra
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        // The pending sweep wakes it exactly once: the orphan sweep sees
        // the pending inbox and stays out.
        result.Started |> should equal 1
        result.Queued |> should equal 0
        resolve.Resolved.Count |> should equal 1
        resolve.Resolved[0] |> should equal created.Id
        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 1
    }

[<Fact>]
let ``Takeover race settles on exactly one winner with loser-zero-effects`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())
        appendStored store created.Id "orphaned" |> ignore
        let prime = claimLiveTurn store created.Id (TimeSpan.FromMinutes 5.0)

        // Marker-only journal on an era-marked session; both passes race
        // after the victim's lease lapses.
        let journal = DispatchJournal()
        seedMarker journal created.Id prime.TurnId
        clock.Advance(TimeSpan.FromMinutes 6.0)

        let winner = RecordingResolve()
        let loser = RecordingResolve()

        let pass (resolve: RecordingResolve) =
            Dispatcher.passOnceAsync
                store
                (journal :> ISessionEventStore)
                markedEra
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        let! results = Task.WhenAll([| pass winner; pass loser |])

        // Exactly one poke wins and settles directly, whichever pass ran
        // first: the atomic priming claim leaves the loser with zero
        // effects, and no entity ever resolves.
        (winner.Resolved.Count + loser.Resolved.Count) |> should equal 0
        (results[0].Started + results[1].Started) |> should equal 1
        results[0].Queued |> should equal 0
        results[1].Queued |> should equal 0

        // Loser-zero-effects: both bootstraps are consumed (the winner's
        // by its claim, the loser's by its cleanup), one terminal lands
        // for the orphan id, no live turn stands, and the loser journals
        // nothing.
        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 0

        let failed = failedFor journal created.Id prime.TurnId
        failed.Length |> should equal 1
        failed[0].Reason |> should equal SessionActor.CrashFailReason
        journalEvents journal created.Id |> fun events -> events.Count |> should equal 2

        let after = storedOf store created.Id
        after.CurrentTurnId.HasValue |> should equal false
    }

[<Fact>]
let ``Orphan sweep pokes live-turn orphans over the SQLite store`` () =
    // Relational parity for the sweep's store interactions (ListSessions
    // Idle filter, CurrentTurnId surfacing, atomic priming claim): the
    // smoke runs Postgres, whose SQL mirrors the SQLite shape. Opens the
    // temp-file database inline: SqliteTestFixture compiles later in
    // this project, so the sweep module cannot see it.
    task {
        let clock = TestClock()

        let path =
            Path.Combine(Path.GetTempPath(), "legate-sqlite-" + Guid.NewGuid().ToString("N") + ".db")

        let database = SqliteDatabase.Open(path, clock)

        try
            let store = SqliteStoreFactory.sessionStore database
            let created = createSession store (AgentId.New())
            appendStored store created.Id "orphaned" |> ignore
            let prime = claimLiveTurn store created.Id (TimeSpan.FromMinutes 5.0)

            // Marker-only journal on an era-marked session; the victim's
            // lease lapses on the shared TestClock, so the poke must win.
            let journal = DispatchJournal()
            seedMarker journal created.Id prime.TurnId
            clock.Advance(TimeSpan.FromMinutes 6.0)

            let resolve = RecordingResolve()

            let! result =
                Dispatcher.passOnceAsync
                    store
                    (journal :> ISessionEventStore)
                    markedEra
                    tenant
                    (SessionsOptions())
                    (DispatcherOptions())
                    resolve.Func
                    clock
                    CancellationToken.None

            result.Started |> should equal 1
            result.Queued |> should equal 0
            resolve.Resolved.Count |> should equal 0
            pendingOf store created.Id |> fun pending -> pending.Count |> should equal 0

            // The winner settles directly: one fenced terminal for the
            // orphan id and no live turn left standing.
            let failed = failedFor journal created.Id prime.TurnId
            failed.Length |> should equal 1

            let after = storedOf store created.Id
            after.CurrentTurnId.HasValue |> should equal false
        finally
            (database :> IDisposable).Dispose()

            for suffix in [ ""; ".lock"; "-wal"; "-shm" ] do
                try
                    File.Delete(path + suffix)
                with _ ->
                    ()
    }

[<Fact>]
let ``Orphan sweep skips live turns with a terminal row`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())
        appendStored store created.Id "orphaned" |> ignore
        let prime = claimLiveTurn store created.Id (TimeSpan.FromMinutes 5.0)

        // Marker plus terminal row for the live turn on an era-marked
        // session: healthy restart shape, never an orphan.
        let journal = DispatchJournal()
        seedMarker journal created.Id prime.TurnId
        seedTerminal journal created.Id prime.TurnId
        clock.Advance(TimeSpan.FromMinutes 6.0)

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync
                store
                (journal :> ISessionEventStore)
                markedEra
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        result.Started |> should equal 0
        result.Queued |> should equal 0
        resolve.Resolved.Count |> should equal 0
        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 0

        let after = storedOf store created.Id
        after.CurrentTurnId.HasValue |> should equal true
        after.CurrentTurnId.Value |> should equal prime.TurnId
    }

[<Fact>]
let ``Orphan sweep skips pre-era live turns`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())
        appendStored store created.Id "orphaned" |> ignore
        let prime = claimLiveTurn store created.Id (TimeSpan.FromMinutes 5.0)

        // Marker-only journal, but the session predates the completion
        // era: pre-era quiet, never an orphan.
        let journal = DispatchJournal()
        seedMarker journal created.Id prime.TurnId
        clock.Advance(TimeSpan.FromMinutes 6.0)

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync
                store
                (journal :> ISessionEventStore)
                preEra
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        result.Started |> should equal 0
        result.Queued |> should equal 0
        resolve.Resolved.Count |> should equal 0
        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 0

        let after = storedOf store created.Id
        after.CurrentTurnId.HasValue |> should equal true
        after.CurrentTurnId.Value |> should equal prime.TurnId
    }

[<Fact>]
let ``Orphan sweep re-pokes after a prior poke claim expired through the fallback`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())
        appendStored store created.Id "orphaned" |> ignore
        let victim = claimLiveTurn store created.Id (TimeSpan.FromMinutes 5.0)

        // The orphan marker names the victim turn.
        let journal = DispatchJournal()
        seedMarker journal created.Id victim.TurnId

        // A prior sweep's poke won after the victim's lease lapsed, then
        // the process died before settling: the live turn now names the
        // marker-less poke prime, so only the journal-derived fallback
        // still names the orphan.
        clock.Advance(TimeSpan.FromMinutes 6.0)
        appendStored store created.Id "stale poke" |> ignore
        let stale = claimLiveTurn store created.Id (TimeSpan.FromMinutes 5.0)
        stale.TurnId |> should not' (equal victim.TurnId)
        clock.Advance(TimeSpan.FromMinutes 6.0)

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync
                store
                (journal :> ISessionEventStore)
                markedEra
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        // The fallback re-candidates the orphan: the winner settles
        // directly with no entity involvement.
        result.Started |> should equal 1
        result.Queued |> should equal 0
        resolve.Resolved.Count |> should equal 0

        let failed = failedFor journal created.Id victim.TurnId
        failed.Length |> should equal 1
        failed[0].Reason |> should equal SessionActor.CrashFailReason

        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 0

        let after = storedOf store created.Id
        after.CurrentTurnId.HasValue |> should equal false
    }

[<Fact>]
let ``Won poke settles exactly once with a single fenced terminal`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())
        appendStored store created.Id "orphaned" |> ignore
        let prime = claimLiveTurn store created.Id (TimeSpan.FromMinutes 5.0)

        let journal = DispatchJournal()
        seedMarker journal created.Id prime.TurnId
        clock.Advance(TimeSpan.FromMinutes 6.0)

        let resolve = RecordingResolve()

        let pass () =
            Dispatcher.passOnceAsync
                store
                (journal :> ISessionEventStore)
                markedEra
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        let! first = pass ()
        first.Started |> should equal 1

        // The orphan id is terminated now: a second pass stays quiet, so
        // the terminal lands exactly once.
        let! second = pass ()
        second.Started |> should equal 0
        second.Queued |> should equal 0
        resolve.Resolved.Count |> should equal 0

        journalEvents journal created.Id |> fun events -> events.Count |> should equal 2

        let failed = failedFor journal created.Id prime.TurnId
        failed.Length |> should equal 1
        failed[0].Reason |> should equal SessionActor.CrashFailReason
    }

[<Fact>]
let ``Losing poke journals nothing`` () =
    task {
        let store, clock = createStore ()
        let created = createSession store (AgentId.New())
        appendStored store created.Id "primed" |> ignore
        let prime = claimLiveTurn store created.Id (TimeSpan.FromHours 1.0)

        // Marker-only journal on an era-marked session, but the lease is
        // live: the poke loses the priming claim.
        let journal = DispatchJournal()
        seedMarker journal created.Id prime.TurnId

        let resolve = RecordingResolve()

        let! result =
            Dispatcher.passOnceAsync
                store
                (journal :> ISessionEventStore)
                markedEra
                tenant
                (SessionsOptions())
                (DispatcherOptions())
                resolve.Func
                clock
                CancellationToken.None

        result.Started |> should equal 0
        result.Queued |> should equal 0
        resolve.Resolved.Count |> should equal 0

        // Loser-zero-effects at the journal: only the seeded marker
        // stands, with no terminal for any turn.
        journalEvents journal created.Id |> fun events -> events.Count |> should equal 1

        failedFor journal created.Id prime.TurnId
        |> fun failed -> failed.Length |> should equal 0

        pendingOf store created.Id |> fun pending -> pending.Count |> should equal 0

        let after = storedOf store created.Id
        after.CurrentTurnId.HasValue |> should equal true
        after.CurrentTurnId.Value |> should equal prime.TurnId
    }
