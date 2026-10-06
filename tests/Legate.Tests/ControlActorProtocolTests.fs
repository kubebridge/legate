// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Collections.Concurrent
open System.Reflection
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
open Legate.Testing
open Legate.Tests.TurnLoopTests
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open Xunit

/// Fault/barrier decorator retaining the real provider's claim and control operations.
type ControlFaultStore() =
    inherit DispatchProxy()
    member val Inner: ISessionStore = Unchecked.defaultof<ISessionStore> with get, set
    member val Failure = "" with get, set
    member val DecisionEntered = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously) with get
    member val Failed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously) with get
    member val DecisionRelease: ManualResetEventSlim | null = null with get, set
    member val Decisions = ConcurrentQueue<string>() with get
    member val Effects = ConcurrentQueue<string>() with get

    override this.Invoke(methodInfo, args) =
        match methodInfo with
        | null -> raise (InvalidOperationException("No method"))
        | methodInfo ->
            if methodInfo.Name = "MarkInboxConsumed" && this.Failure = "consume" then
                this.Failed.TrySetResult() |> ignore
                raise (InvalidOperationException("injected consume failure"))

            this.Effects.Enqueue methodInfo.Name

            try
                methodInfo.Invoke(this.Inner, args)
            with :? TargetInvocationException as error ->
                match error.InnerException with
                | null -> raise error
                | inner -> raise inner

    interface ISessionAbortControlStore with
        member this.ReadAbortTarget(t, s, ct) =
            (this.Inner :?> ISessionAbortControlStore).ReadAbortTarget(t, s, ct)

        member this.RequestHostAbort(t, s, turn, cause, reason, ct) =
            (this.Inner :?> ISessionAbortControlStore).RequestHostAbort(t, s, turn, cause, reason, ct)

        member this.BindControlTarget(t, s, turn, pos, claim, ct) =
            (this.Inner :?> ISessionAbortControlStore).BindControlTarget(t, s, turn, pos, claim, ct)

        member this.CheckControlTarget(t, s, turn, pos, claim, ct) =
            (this.Inner :?> ISessionAbortControlStore).CheckControlTarget(t, s, turn, pos, claim, ct)

        member this.TryRecoverControlTarget(t, s, turn, owner, duration, ct) =
            (this.Inner :?> ISessionAbortControlStore).TryRecoverControlTarget(t, s, turn, owner, duration, ct)

        member this.TryDecideControlTarget(t, s, turn, pos, claim, id, status, cause, reason, ct) =
            this.Decisions.Enqueue id
            this.DecisionEntered.TrySetResult() |> ignore

            match this.DecisionRelease with
            | null -> ()
            | release -> release.Wait()

            if this.Failure = "decision" then
                this.Failed.TrySetResult() |> ignore
                raise (InvalidOperationException("injected decision failure"))

            let result =
                (this.Inner :?> ISessionAbortControlStore)
                    .TryDecideControlTarget(t, s, turn, pos, claim, id, status, cause, reason, ct)

            if this.Failure = "lost-response" || this.Failure = "crash-decision-response" then
                result.GetAwaiter().GetResult() |> ignore
                this.Failed.TrySetResult() |> ignore

                if this.Failure = "crash-decision-response" then
                    this.Failure <- ""

                raise (InvalidOperationException("injected lost committed decision response"))

            result

        member this.RetireControlTarget(t, s, turn, pos, claim, id, ct) =
            if this.Failure = "retire" then
                this.Failed.TrySetResult() |> ignore
                raise (InvalidOperationException("injected retirement failure"))

            (this.Inner :?> ISessionAbortControlStore).RetireControlTarget(t, s, turn, pos, claim, id, ct)

module ControlActorProtocolTests =
    let private ct = CancellationToken.None
    let private tenant = TenantId.Create "control-actors"
    let private bound = TimeSpan.FromSeconds 10.0

    let private completed =
        {
            AssistantText = "done"
            Status = TurnStatus.Completed
            Iterations = 1
            Usage = { InputTokens = 0L; OutputTokens = 0L }
            Outcome = null
        }

    let private session options =
        {
            Id = SessionId.New()
            Tenant = tenant
            AgentId = AgentId.New()
            Title = "control"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = options
            PermissionGrants = [||]
        }

    let private delay =
        { new ILlmDelay with
            member _.Delay(_, _) =
                TaskCompletionSource<unit>().Task :> Task
        }

    let private prime (store: ISessionStore) sid =
        task {
            let! _ =
                store.AppendInboxMessage(
                    tenant,
                    sid,
                    UserMessagePayload(UserMessage.Text "prime"),
                    DeliveryMode.Queue,
                    ct
                )

            let! lease = store.ClaimNextTurn(tenant, sid, "original-owner", TimeSpan.FromMinutes 5.0, ct)

            return
                match lease with
                | :? TurnLeaseRenewed as lease -> lease.Claim
                | _ -> failwith "No genuine prime"
        }

    let private factoryActor (system: ActorSystem) factory (sid: SessionId) =
        let parent =
            spawn system ("parent-" + Guid.NewGuid().ToString("N")) (fun mailbox ->
                actor {
                    let! _ = mailbox.Receive()

                    try
                        let child = factory (sid.ToString()) mailbox.Context "session"
                        mailbox.Sender() <! child
                    with error ->
                        mailbox.Sender() <! Status.Failure(error)

                    let! _ = mailbox.Receive()
                    return ()
                })

        parent.Ask<IActorRef>("activate", bound)

    [<Fact>]
    let ``issue415 explicitly registered settlement capability is used by production execution`` () =
        task {
            let database = InMemoryDatabase()
            let actual = InMemoryStoreFactory.sessionStore database
            let journal = InMemoryStoreFactory.eventStore database
            let proxy = DispatchProxy.Create<ISessionStore, ControlFaultStore>()
            (proxy :?> ControlFaultStore).Inner <- actual
            Assert.False(proxy :? ISessionSettlementStore)
            let atomic = actual :?> ISessionSettlementStore
            let mutable admits, settlements = 0, 0

            let selected =
                { new ISessionSettlementStore with
                    member _.SupportsSettlementJournal(events) =
                        atomic.SupportsSettlementJournal(events)

                    member _.AdmitExecution(t, s, p, claim, token) =
                        Interlocked.Increment(&admits) |> ignore
                        atomic.AdmitExecution(t, s, p, claim, token)

                    member _.SettleExecution(t, request, token) =
                        Interlocked.Increment(&settlements) |> ignore
                        atomic.SettleExecution(t, request, token)

                    member _.TryReadEntry(t, s, p, token) = atomic.TryReadEntry(t, s, p, token)

                    member _.TryReadCommitted(t, s, p, token) = atomic.TryReadCommitted(t, s, p, token)
                }

            let services = ServiceCollection()

            services.AddLegate(
                Action<LegateBuilder>(fun b ->
                    b.Llm.AddProvider(BuilderTests.StubLlmProvider()) |> ignore
                    b.Workspace.UseRuntime(BuilderTests.StubWorkspaceRuntime()) |> ignore
                    b.Storage.UseSessionStore(proxy) |> ignore)
            )
            |> ignore

            services.AddSingleton<ISessionSettlementStore>(selected) |> ignore
            services.AddSingleton<ISessionEventStore>(journal) |> ignore

            use chat =
                new ScriptedChatClient(
                    [|
                        ScriptStep.Text "explicit capability"
                    |]
                )

            services.AddSingleton<IChatClient>(chat) |> ignore
            use provider = services.BuildServiceProvider()

            let local =
                provider.GetServices<IHostedService>()
                |> Seq.pick (function
                    | :? LocalActorSystemService as local -> Some local
                    | _ -> None)

            do! (local :> IHostedService).StartAsync(ct)

            let client =
                provider.GetRequiredService<ISessionClientFactory>().GetClient(TenantId.Default)

            let! opened = client.OpenSessionAsync(AgentId.New())
            let! result = client.PromptAndWaitAsync(opened.Id, "hello")
            Assert.Equal(TurnStatus.Completed, result.Status)
            Assert.True(admits > 0)
            Assert.Equal(1, settlements)
            do! (local :> IHostedService).StopAsync(ct)
        }

    [<Theory>]
    [<InlineData(false, "success")>]
    [<InlineData(true, "success")>]
    [<InlineData(false, "stop-before")>]
    [<InlineData(true, "stop-before")>]
    [<InlineData(false, "stop-after")>]
    [<InlineData(true, "stop-after")>]
    [<InlineData(false, "lost-response")>]
    [<InlineData(true, "lost-response")>]
    [<InlineData(false, "admission-false")>]
    [<InlineData(true, "admission-false")>]
    [<InlineData(false, "admission-fault")>]
    [<InlineData(true, "admission-fault")>]
    [<InlineData(false, "settlement-fault")>]
    [<InlineData(true, "settlement-fault")>]
    [<InlineData(false, "takeover")>]
    [<InlineData(true, "takeover")>]
    [<InlineData(false, "lost-decision")>]
    [<InlineData(true, "lost-decision")>]
    [<InlineData(false, "retire-fault")>]
    [<InlineData(true, "retire-fault")>]
    let ``issue415 atomic crash failure publishes only committed winner and cleans genuine prime`` sqlite mode =
        task {
            let clock = TestClock()
            let path = SqliteTestFixture.tempDatabasePath ()

            let sql =
                if sqlite then
                    Some(SqliteDatabase.Open(path, clock))
                else
                    None

            let database = InMemoryDatabase(clock)

            try
                let store, journal, atomic =
                    match sql with
                    | Some db ->
                        SqliteStoreFactory.sessionStore db,
                        SqliteStoreFactory.eventStore db,
                        (SqliteSessionSettlementStore(db) :> ISessionSettlementStore)
                    | None ->
                        let store = InMemoryStoreFactory.sessionStore database
                        store, InMemoryStoreFactory.eventStore database, (store :?> ISessionSettlementStore)

                let actualStore = store
                let store = DispatchProxy.Create<ISessionStore, ControlFaultStore>()
                let proxy = store :?> ControlFaultStore
                proxy.Inner <- actualStore

                proxy.Failure <-
                    (if mode = "lost-decision" then "crash-decision-response"
                     elif mode = "retire-fault" then "retire"
                     else "")

                let options =
                    SessionOptions(OnCrashResume = OnCrashResume.FailAttempt, AutoClose = true)

                options.CompletionDestinationId <- "crash-receiver"
                let! created = store.CreateSession(tenant, session options, ct)
                let! old = prime store created.Id

                let! entry =
                    store.AppendInboxMessage(
                        tenant,
                        created.Id,
                        UserMessagePayload(UserMessage.Text "original"),
                        DeliveryMode.Queue,
                        ct
                    )

                let control = store :?> ISessionAbortControlStore
                let! _ = control.BindControlTarget(tenant, created.Id, entry.TurnId, entry.Position, old, ct)
                let! _ = store.UpdateSessionState(tenant, created.Id, SessionState.Running, ct)
                clock.Advance(TimeSpan.FromMinutes 6.0)

                let! recovered =
                    control.TryRecoverControlTarget(
                        tenant,
                        created.Id,
                        entry.TurnId,
                        "survivor",
                        TimeSpan.FromMinutes 5.0,
                        ct
                    )

                let recovery = unbox<ControlTargetRecovery> (box recovered)
                let claim = unbox<TurnClaim> (box recovery.Claim)
                let observed = ConcurrentQueue<TurnResult>()
                let requests = ConcurrentQueue<SessionSettlementRequest>()
                let mutable admits = 0
                let mutable lateStop: HostAbortReceipt option = None

                let selected =
                    { new ISessionSettlementStore with
                        member _.SupportsSettlementJournal(events) =
                            atomic.SupportsSettlementJournal(events)

                        member _.TryReadEntry(t, s, p, token) = atomic.TryReadEntry(t, s, p, token)
                        member _.TryReadCommitted(t, s, p, token) = atomic.TryReadCommitted(t, s, p, token)

                        member _.AdmitExecution(t, s, p, authority, token) =
                            task {
                                admits <- admits + 1

                                if mode = "admission-fault" then
                                    failwith "injected admission fault"

                                if mode = "admission-false" then
                                    return false
                                else
                                    let! admitted = atomic.AdmitExecution(t, s, p, authority, token)

                                    if mode = "stop-before" then
                                        let! stop =
                                            control.RequestHostAbort(
                                                t,
                                                s,
                                                entry.TurnId,
                                                StopCause.HostShutdown,
                                                "exact host reason",
                                                token
                                            )

                                        Assert.Equal(HostAbortOutcome.Accepted, stop.Outcome)
                                    elif mode = "takeover" then
                                        clock.Advance(TimeSpan.FromMinutes 6.0)

                                        let! winner =
                                            control.TryRecoverControlTarget(
                                                t,
                                                s,
                                                entry.TurnId,
                                                "winner",
                                                TimeSpan.FromMinutes 5.0,
                                                token
                                            )

                                        Assert.False(isNull (box winner))

                                    return admitted
                            }

                        member _.SettleExecution(t, request, token) =
                            task {
                                requests.Enqueue request

                                if mode = "settlement-fault" then
                                    failwith "injected settlement fault"

                                if mode = "stop-after" then
                                    let! stop =
                                        control.RequestHostAbort(
                                            t,
                                            created.Id,
                                            entry.TurnId,
                                            StopCause.HostShutdown,
                                            "too late",
                                            token
                                        )

                                    lateStop <- Some stop

                                let! result = atomic.SettleExecution(t, request, token)

                                if mode = "lost-response" && requests.Count = 1 then
                                    failwith "injected committed response loss"

                                return result
                            }
                    }

                let props: SessionActorProps =
                    {
                        Store = store
                        Settlement = Some selected
                        Tenant = tenant
                        SessionId = created.Id
                        RunTurn = fun _ _ -> Task.FromResult completed
                        OnTurnSettled = Some observed.Enqueue
                        OnInjectJournaled = None
                        Compact = None
                        Logger = null
                    }

                let suspend: SessionActor.SuspendDeps =
                    {
                        EventStore = journal
                        Delay = delay
                        AskTimeout = bound
                        JournalToken = claim.Token
                        PrimeClaim = Some claim
                        Recovery = recovery
                        RunSuspendable = fun _ _ _ _ _ _ _ _ _ _ _ -> failwith "FailAttempt never executes the original"
                        ReprimeJournal = None
                        RefreshCompact = None
                        AgentStore = null
                        EraMarked = fun _ _ _ -> Task.FromResult false
                    }

                use system = ActorSystem.Create("atomic-crash-" + Guid.NewGuid().ToString("N"))

                let initialized =
                    TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)

                let _ =
                    spawn system "session" (fun mailbox ->
                        try
                            let behavior =
                                SessionActor.behaviorWithSuspendRouted (fun () -> ()) props suspend clock None mailbox

                            initialized.SetResult true
                            behavior
                        with _ ->
                            initialized.SetResult false
                            actorOf (fun (_: SessionActor.SuspendableActorMessage) -> ()) mailbox)

                let! succeeded = initialized.Task.WaitAsync(bound)

                let refused =
                    mode.StartsWith("admission-") || mode = "settlement-fault" || mode = "takeover"

                Assert.Equal(not refused && mode <> "retire-fault", succeeded)
                Assert.Equal(1, admits)
                let! committed = atomic.TryReadCommitted(tenant, created.Id, entry.Position, ct)
                let! page = journal.Replay(tenant, created.Id, 0L, 100, ct)

                let events =
                    match page with
                    | :? EventReplayPage as page -> page.Events
                    | :? EventReplayEndOfStream -> [||] :> System.Collections.Generic.IReadOnlyList<SessionEvent>
                    | _ -> failwith "Unexpected replay refusal"

                let! stored = store.GetSession(tenant, created.Id, ct)
                let stored = unbox<Session> (box stored)
                let! outbox = store.ClaimCompletionOutbox("evidence", 100, TimeSpan.FromMinutes 1.0, ct)

                if refused then
                    Assert.Empty(outbox)
                    Assert.Null(committed)
                    Assert.Empty(observed)
                    Assert.Empty(events)
                    Assert.Equal(SessionState.Running, stored.State)
                    let! pending = store.ReadPendingInbox(tenant, created.Id, ct)
                    Assert.Contains(pending, fun row -> row.Position = entry.Position)
                else
                    Assert.Single(outbox) |> ignore
                    let committed = unbox<SessionSettlementOutcome> (box committed)
                    let winner = unbox<TurnResult> (box committed.Result)

                    Assert.Equal(
                        (if mode = "stop-before" then
                             TurnStatus.Aborted
                         else
                             TurnStatus.Failed),
                        winner.Status
                    )

                    Assert.Equal(SessionState.Idle, stored.State)
                    Assert.Equal((mode = "retire-fault"), stored.CurrentTurnId.HasValue)
                    Assert.Single(events) |> ignore
                    Assert.Equal(entry.TurnId, events[0].TurnId)
                    Assert.Equal(clock.GetUtcNow(), events[0].Timestamp)
                    Assert.NotNull(committed.Completion)

                    if mode = "stop-before" then
                        let stop = Assert.IsType<TurnAborted>(winner.Outcome)
                        Assert.Equal(StopCause.HostShutdown, stop.Cause)
                        Assert.Equal("exact host reason", stop.Reason)
                        Assert.IsType<TurnAbortedEvent>(events[0]) |> ignore
                    else
                        Assert.IsType<TurnFailedEvent>(events[0]) |> ignore

                    if mode = "stop-after" then
                        Assert.Equal(HostAbortOutcome.AlreadyTerminal, lateStop.Value.Outcome)

                    if mode = "lost-response" then
                        Assert.Empty(observed)
                        let calls = requests.ToArray()
                        Assert.Equal(2, calls.Length)
                        Assert.Same(calls[0], calls[1])
                    else
                        Assert.Single(observed) |> ignore

                    if mode = "lost-decision" then
                        let decisions = proxy.Decisions.ToArray()
                        Assert.Equal(2, decisions.Length)
                        Assert.Equal(decisions[0], decisions[1])

                    let! late =
                        control.RequestHostAbort(tenant, created.Id, entry.TurnId, StopCause.ExplicitAbort, "late", ct)

                    Assert.Equal(
                        (if mode = "stop-before" then
                             HostAbortOutcome.AlreadyAccepted
                         else
                             HostAbortOutcome.AlreadyTerminal),
                        late.Outcome
                    )

                    let! stale =
                        journal.Append(
                            tenant,
                            created.Id,
                            old.Token,
                            [|
                                TextDeltaEvent(created.Id, entry.TurnId, Nullable(), clock.GetUtcNow(), "loser")
                                :> SessionEvent
                            |],
                            ct
                        )

                    Assert.IsType<EventAppendRejected>(stale) |> ignore
            finally
                match sql with
                | Some db ->
                    (db :> IDisposable).Dispose()
                    SqliteTestFixture.deleteDatabaseFiles path
                | None -> ()
        }

    [<Theory>]
    [<InlineData(false, OnCrashResume.ResumeAttempt, false)>]
    [<InlineData(false, OnCrashResume.ResumeAttempt, true)>]
    [<InlineData(false, OnCrashResume.FailAttempt, false)>]
    [<InlineData(false, OnCrashResume.FailAttempt, true)>]
    [<InlineData(true, OnCrashResume.ResumeAttempt, false)>]
    [<InlineData(true, OnCrashResume.ResumeAttempt, true)>]
    [<InlineData(true, OnCrashResume.FailAttempt, false)>]
    [<InlineData(true, OnCrashResume.FailAttempt, true)>]
    let ``issue415 Running sweep recovers genuine original target with InMemory SQLite policy parity``
        sqlite
        policy
        consumed
        =
        task {
            let clock = TestClock()
            let path = SqliteTestFixture.tempDatabasePath ()
            let database = InMemoryDatabase(clock)

            let sql =
                if sqlite then
                    Some(SqliteDatabase.Open(path, clock))
                else
                    None

            try
                let store, journal =
                    match sql with
                    | Some db -> SqliteStoreFactory.sessionStore db, SqliteStoreFactory.eventStore db
                    | None -> InMemoryStoreFactory.sessionStore database, InMemoryStoreFactory.eventStore database

                let settlement =
                    match sql with
                    | Some db -> SqliteSessionSettlementStore(db) :> ISessionSettlementStore
                    | None -> store :?> ISessionSettlementStore

                let! created = store.CreateSession(tenant, session (SessionOptions(OnCrashResume = policy)), ct)
                let! oldClaim = prime store created.Id

                let! entry =
                    store.AppendInboxMessage(
                        tenant,
                        created.Id,
                        UserMessagePayload(UserMessage.Text "original"),
                        DeliveryMode.Queue,
                        ct
                    )

                let control = store :?> ISessionAbortControlStore
                let! _ = control.BindControlTarget(tenant, created.Id, entry.TurnId, entry.Position, oldClaim, ct)
                let! _ = store.UpdateSessionState(tenant, created.Id, SessionState.Running, ct)

                if consumed then
                    let! _ = store.MarkInboxConsumed(tenant, created.Id, [| entry.Position |], ct)
                    ()

                use system = ActorSystem.Create("sweep-" + Guid.NewGuid().ToString("N"))
                use bus = new SessionEventBus(journal, SessionSubscriptionOptions(), null)
                let entered = ConcurrentQueue<InboxEntry * TurnId * int>()

                let finish =
                    TaskCompletionSource<TurnLoop.TurnLoopCompletion>(
                        TaskCreationOptions.RunContinuationsAsynchronously
                    )

                let runner: SessionActor.SuspendableRunner =
                    fun work attempt _ _ _ _ _ _ _ _ turn ->
                        entered.Enqueue(work, turn, attempt)
                        finish.Task

                let factory =
                    SessionActor.spawnSuspendFactory
                        store
                        tenant
                        journal
                        delay
                        (TimeSpan.FromMinutes 1.0)
                        "survivor"
                        (TimeSpan.FromMinutes 5.0)
                        runner
                        (fun _ _ -> None)
                        null
                        (fun _ _ _ -> Task.FromResult(false))
                        (Some settlement)

                let resolve id _ =
                    factoryActor system factory id |> Async.StartAsTask

                let sweep () =
                    Dispatcher.passOnceRoutedAsync
                        (fun _ -> true)
                        store
                        journal
                        (fun _ _ _ -> Task.FromResult(false))
                        tenant
                        (SessionsOptions(Capacity = 1))
                        (DispatcherOptions())
                        resolve
                        clock
                        ct

                let! _ = sweep ()
                Assert.Empty(entered)
                let! stillHeld = store.VerifyClaim(tenant, oldClaim, ct)
                Assert.IsType<TurnLeaseHeld>(stillHeld) |> ignore
                clock.Advance(TimeSpan.FromMinutes 6.0)
                use timeout = new CancellationTokenSource(bound)

                let stream =
                    bus.Subscribe(tenant, created.Id, 0L, timeout.Token).GetAsyncEnumerator(timeout.Token)

                let terminal =
                    task {
                        let mutable found: SessionEvent option = None

                        while found.IsNone do
                            let! has = stream.MoveNextAsync().AsTask()

                            if not has then
                                failwith "No recovered terminal"

                            match stream.Current with
                            | :? TurnCompletedEvent
                            | :? TurnFailedEvent
                            | :? TurnAbortedEvent -> found <- Some stream.Current
                            | _ -> ()

                        return found.Value
                    }

                let! _ = Task.WhenAll(sweep (), sweep ())

                if policy = OnCrashResume.ResumeAttempt then
                    finish.SetResult(
                        {
                            Result = completed
                            TurnId = entry.TurnId
                            HasPendingInjects = false
                            Suspension = None
                        }
                    )

                let! final = terminal.WaitAsync(bound)
                do! stream.DisposeAsync().AsTask()
                Assert.Equal(entry.TurnId, final.TurnId)

                // The recovered original operation must also be observable by
                // late, independent receipt waiters, not just journal readers.
                let observer = new SessionClient(store, tenant, resolve, bus, bound, delay, None)
                observer.SettlementStore <- Some settlement
                let receipt = SessionClientExtensions.ToReceipt(entry)

                let! observations =
                    Task
                        .WhenAll(
                            observer.WaitForOperationAsync(receipt, bound, ct),
                            observer.WaitForOperationAsync(receipt, bound, ct)
                        )
                        .WaitAsync(bound)

                for observed in observations do
                    Assert.Equal(OperationStatus.Terminal, observed.Status)
                    Assert.Equal(entry.Position, observed.Position)
                    Assert.Equal(entry.TurnId, observed.TurnId.Value)

                    Assert.Equal(
                        (if policy = OnCrashResume.ResumeAttempt then
                             TurnStatus.Completed
                         else
                             TurnStatus.Failed),
                        (unbox<TurnResult> (box observed.Result)).Status
                    )

                if policy = OnCrashResume.ResumeAttempt then
                    let original, turn, attempt = Assert.Single(entered)
                    Assert.Equal(entry.Position, original.Position)
                    Assert.Equal(consumed, original.Consumed)
                    Assert.Equal(entry.TurnId, turn)
                    Assert.Equal(2, attempt)
                    Assert.IsType<TurnCompletedEvent>(final) |> ignore
                else
                    Assert.Empty(entered)
                    Assert.IsType<TurnFailedEvent>(final) |> ignore

                let! lost = store.VerifyClaim(tenant, oldClaim, ct)
                Assert.True(lost :? TurnLeaseLost || lost :? TurnLeaseMissing)

                let stale =
                    TextDeltaEvent(created.Id, entry.TurnId, Nullable(), clock.GetUtcNow(), "loser") :> SessionEvent

                let! rejected = journal.Append(tenant, created.Id, oldClaim.Token, [| stale |], ct)
                Assert.IsType<EventAppendRejected>(rejected) |> ignore
                let! page = journal.Replay(tenant, created.Id, 0L, 100, ct)
                let events = Assert.IsType<EventReplayPage>(page).Events

                Assert.Single(events |> Seq.filter (fun e -> e :? TurnCompletedEvent || e :? TurnFailedEvent))
                |> ignore

                Assert.Equal<int64 array>(
                    [| 1L .. int64 events.Count |],
                    events |> Seq.map (fun e -> e.Sequence.Value) |> Seq.toArray
                )
            finally
                match sql with
                | Some db ->
                    (db :> IDisposable).Dispose()
                    SqliteTestFixture.deleteDatabaseFiles path
                | None -> ()
        }

    [<Theory>]
    [<InlineData(OnCrashResume.ResumeAttempt, false)>]
    [<InlineData(OnCrashResume.FailAttempt, false)>]
    [<InlineData(OnCrashResume.ResumeAttempt, true)>]
    [<InlineData(OnCrashResume.FailAttempt, true)>]
    let ``fresh SQLite factory recovers only original unstopped work under both policies`` policy consumed =
        task {
            let clock = TestClock()
            let path = SqliteTestFixture.tempDatabasePath ()

            try
                let db = SqliteDatabase.Open(path, clock)
                let store = SqliteStoreFactory.sessionStore db
                let options = SessionOptions(OnCrashResume = policy)
                let! created = store.CreateSession(tenant, session options, ct)
                let! oldClaim = prime store created.Id

                let! entry =
                    store.AppendInboxMessage(
                        tenant,
                        created.Id,
                        UserMessagePayload(UserMessage.Text "original"),
                        DeliveryMode.Queue,
                        ct
                    )

                let control = store :?> ISessionAbortControlStore
                let turn = entry.TurnId
                let! _ = control.BindControlTarget(tenant, created.Id, turn, entry.Position, oldClaim, ct)
                let! _ = store.UpdateSessionState(tenant, created.Id, SessionState.Running, ct)

                if consumed then
                    let! _ = store.MarkInboxConsumed(tenant, created.Id, [| entry.Position |], ct)
                    ()

                let! queued =
                    store.AppendInboxMessage(
                        tenant,
                        created.Id,
                        UserMessagePayload(UserMessage.Text "unrelated"),
                        DeliveryMode.Interrupt,
                        ct
                    )

                (db :> IDisposable).Dispose()
                clock.Advance(TimeSpan.FromMinutes 6.0)
                use fresh = SqliteDatabase.Open(path, clock)
                let freshStore = SqliteStoreFactory.sessionStore fresh
                let journal = SqliteStoreFactory.eventStore fresh
                use system = ActorSystem.Create("recover-" + Guid.NewGuid().ToString("N"))

                let entered =
                    TaskCompletionSource<InboxEntry * TurnId * int>(TaskCreationOptions.RunContinuationsAsynchronously)

                let runner: SessionActor.SuspendableRunner =
                    fun entry attempt _ _ _ _ _ _ _ _ target ->
                        entered.TrySetResult(entry, target, attempt) |> ignore
                        TaskCompletionSource<TurnLoop.TurnLoopCompletion>().Task

                let factory =
                    SessionActor.spawnSuspendFactory
                        freshStore
                        tenant
                        journal
                        delay
                        (TimeSpan.FromMinutes 1.0)
                        "new owner"
                        (TimeSpan.FromMinutes 5.0)
                        runner
                        (fun _ _ -> None)
                        null
                        (fun _ _ _ -> Task.FromResult false)
                        (Some(SqliteSessionSettlementStore(fresh) :> ISessionSettlementStore))

                let! child = factoryActor system factory created.Id
                let! _ = SessionActor.getSuspendSnapshotAsync child ct
                let! rows = freshStore.ReadPendingInbox(tenant, created.Id, ct)
                Assert.Contains(rows, fun row -> row.Position = queued.Position)

                if policy = OnCrashResume.ResumeAttempt then
                    let! original, actualTurn, attempt = entered.Task.WaitAsync bound
                    Assert.Equal(entry.Position, original.Position)
                    Assert.Equal(turn, actualTurn)
                    Assert.Equal(2, attempt)
                    Assert.Equal(consumed, original.Consumed)
                else
                    let! following, actualTurn, attempt = entered.Task.WaitAsync bound
                    Assert.Equal(queued.Position, following.Position)
                    Assert.Equal(queued.TurnId, actualTurn)
                    Assert.Equal(1, attempt)
                    Assert.Single(rows) |> ignore
                    let! stored = freshStore.GetSession(tenant, created.Id, ct)

                    match stored with
                    | null -> failwith "Lost session"
                    | stored -> Assert.Equal(SessionState.Running, stored.State)

                    let! late =
                        (freshStore :?> ISessionAbortControlStore)
                            .RequestHostAbort(tenant, created.Id, turn, StopCause.ExplicitAbort, "late", ct)

                    Assert.Equal(HostAbortOutcome.AlreadyTerminal, late.Outcome)
                    Assert.Equal(Nullable TurnStatus.Failed, late.TerminalStatus)

                let! lost = freshStore.VerifyClaim(tenant, oldClaim, ct)
                Assert.IsType<TurnLeaseLost>(lost) |> ignore
            finally
                SqliteTestFixture.deleteDatabaseFiles path
        }

    [<Fact>]
    let ``fresh SQLite suspended recovery replies to original target not queued head`` () =
        task {
            let clock = TestClock()
            let path = SqliteTestFixture.tempDatabasePath ()

            try
                let db = SqliteDatabase.Open(path, clock)
                let store = SqliteStoreFactory.sessionStore db
                let! created = store.CreateSession(tenant, session (SessionOptions()), ct)
                let! oldClaim = prime store created.Id

                let! entry =
                    store.AppendInboxMessage(
                        tenant,
                        created.Id,
                        UserMessagePayload(UserMessage.Text "original"),
                        DeliveryMode.Queue,
                        ct
                    )

                let turn = TurnId.New()

                let! _ =
                    (store :?> ISessionAbortControlStore)
                        .BindControlTarget(tenant, created.Id, turn, entry.Position, oldClaim, ct)

                let! _ = store.UpdateSessionState(tenant, created.Id, SessionState.WaitingForInput, ct)
                let journal = SqliteStoreFactory.eventStore db

                let marker =
                    PermissionRequestedEvent(created.Id, turn, Nullable(), clock.GetUtcNow(), "request", "tool")
                    :> SessionEvent

                let! _ = journal.Append(tenant, created.Id, oldClaim.Token, [| marker |], ct)
                let! _ = store.MarkInboxConsumed(tenant, created.Id, [| entry.Position |], ct)

                let! queued =
                    store.AppendInboxMessage(
                        tenant,
                        created.Id,
                        UserMessagePayload(UserMessage.Text "unrelated"),
                        DeliveryMode.Interrupt,
                        ct
                    )

                (db :> IDisposable).Dispose()
                clock.Advance(TimeSpan.FromMinutes 6.0)
                use fresh = SqliteDatabase.Open(path, clock)
                let store = SqliteStoreFactory.sessionStore fresh
                use system = ActorSystem.Create("reply-" + Guid.NewGuid().ToString("N"))

                let entered =
                    TaskCompletionSource<InboxEntry * TurnId>(TaskCreationOptions.RunContinuationsAsynchronously)

                let runner: SessionActor.SuspendableRunner =
                    fun entry _ _ _ _ _ _ _ _ _ target ->
                        entered.TrySetResult(entry, target) |> ignore
                        TaskCompletionSource<TurnLoop.TurnLoopCompletion>().Task

                let factory =
                    SessionActor.spawnSuspendFactory
                        store
                        tenant
                        (SqliteStoreFactory.eventStore fresh)
                        delay
                        (TimeSpan.FromMinutes 1.0)
                        "new owner"
                        (TimeSpan.FromMinutes 5.0)
                        runner
                        (fun _ _ -> None)
                        null
                        (fun _ _ _ -> Task.FromResult false)
                        None

                let! child = factoryActor system factory created.Id
                let! snapshot = SessionActor.getSuspendSnapshotAsync child ct
                Assert.Equal(SessionState.WaitingForInput, snapshot.State)
                Assert.False(entered.Task.IsCompleted)

                let! _ =
                    SessionActor.replyAsync
                        store
                        tenant
                        created.Id
                        child
                        (PermissionDecision("request", PermissionDecisionKind.AllowOnce))
                        ct

                let! original, actualTurn = entered.Task.WaitAsync bound
                Assert.Equal(entry.Position, original.Position)
                Assert.Equal(turn, actualTurn)
                let! rows = store.ReadPendingInbox(tenant, created.Id, ct)
                Assert.Single(rows) |> ignore
                Assert.Equal(queued.Position, rows[0].Position)
            finally
                SqliteTestFixture.deleteDatabaseFiles path
        }

    [<Fact>]
    let ``factory refuses unknown routes before priming and still serves inspection`` () =
        task {
            let clock = TestClock()
            let database = InMemoryDatabase(clock)
            let store = InMemorySessionStore(database) :> ISessionStore
            let journal = InMemorySessionEventStore(database) :> ISessionEventStore
            // No destination is registered on this receiving host.
            let routes = CompletionDestinations(ServiceCollection().BuildServiceProvider())

            let routed options =
                {
                    Id = SessionId.New()
                    Tenant = tenant
                    AgentId = AgentId.New()
                    Title = "routed"
                    State = SessionState.Idle
                    CurrentTurnId = Nullable()
                    CreatedAt = DateTimeOffset.MinValue
                    UpdatedAt = DateTimeOffset.MinValue
                    ClosedAt = Nullable()
                    WorkspaceBinding = null
                    Options = options
                    PermissionGrants = [||]
                }

            let refusedOptions = SessionOptions()
            refusedOptions.CompletionDestinationId <- "unknown-receiver"
            let! refused = store.CreateSession(tenant, routed refusedOptions, ct)

            let runner: SessionActor.SuspendableRunner =
                fun _ _ _ _ _ _ _ _ _ _ _ ->
                    Task.FromException<TurnLoop.TurnLoopCompletion>(InvalidOperationException("no run"))

            let factory =
                SessionActor.spawnSuspendFactoryRouted
                    (Some routes)
                    store
                    tenant
                    journal
                    delay
                    (TimeSpan.FromMinutes 1.0)
                    "route-probe"
                    (TimeSpan.FromMinutes 5.0)
                    runner
                    (fun _ _ -> None)
                    null
                    (fun _ _ _ -> Task.FromResult false)
                    System.TimeProvider.System
                    None
                    None

            use system = ActorSystem.Create("route-" + Guid.NewGuid().ToString("N"))

            try
                let! child = factoryActor system factory refused.Id

                // Inspection stays route-independent and nonactivating.
                let! snapshot = child.Ask<obj>(SessionActor.SuspendableGetSnapshot, bound)
                Assert.IsNotType<CompletionRoutingRefused>(snapshot)

                // Work is refused before any prime, bootstrap, or claim.
                let! refusal =
                    child.Ask<obj>(
                        SessionActor.SuspendableQueuePrompt(
                            UserMessagePayload(UserMessage.Text "work") :> InboxPayload,
                            ct
                        ),
                        bound
                    )

                match refusal with
                | :? CompletionRoutingRefused as routed ->
                    Assert.Equal(tenant, routed.Tenant)
                    Assert.Equal(refused.Id, routed.SessionId)
                    Assert.Equal("unknown-receiver", routed.DestinationId)
                    Assert.Equal(CompletionRoutingReason.Unknown, routed.Reason)
                | other -> failwith $"Expected a routing refusal but got '{other.GetType().Name}'."

                let! pending = store.ReadPendingInbox(tenant, refused.Id, ct)
                Assert.Empty(pending)
            finally
                system.Terminate().GetAwaiter().GetResult() |> ignore
        }

    type private Rig(autoClose: bool) =
        let clock = TestClock()
        let db = InMemoryDatabase(clock)
        let inner = InMemorySessionStore(db) :> ISessionStore
        let store = DispatchProxy.Create<ISessionStore, ControlFaultStore>()
        let proxy = store :?> ControlFaultStore
        let journal = InMemorySessionEventStore(db) :> ISessionEventStore
        let observed = ConcurrentQueue<TurnResult>()
        let sinks = ConcurrentQueue<SessionCompletion>()

        let mutable routes: CompletionDestinations =
            Unchecked.defaultof<CompletionDestinations>

        let runs =
            ConcurrentQueue<InboxEntry * TurnId * TaskCompletionSource<TurnLoop.TurnLoopCompletion>>()

        let settled =
            TaskCompletionSource<TurnResult>(TaskCreationOptions.RunContinuationsAsynchronously)

        let options = SessionOptions(AutoClose = autoClose)

        do
            options.CompletionDestinationId <- "control-receiver"
            proxy.Inner <- inner

            let sink =
                { new ISessionCompletionSink with
                    member _.NotifyAsync(completion, _) =
                        sinks.Enqueue completion
                        Task.CompletedTask
                }

            let services = ServiceCollection()

            services.AddKeyedSingleton<ISessionCompletionSink>(box (tenant, "control-receiver"), sink)
            |> ignore

            routes <- CompletionDestinations(services.BuildServiceProvider())

        let created =
            inner.CreateSession(tenant, session options, ct).GetAwaiter().GetResult()

        let claim = (prime inner created.Id).GetAwaiter().GetResult()
        let system = ActorSystem.Create("control-" + Guid.NewGuid().ToString("N"))

        let runner: SessionActor.SuspendableRunner =
            fun entry _ _ _ _ _ _ _ _ _ turn ->
                let result =
                    TaskCompletionSource<TurnLoop.TurnLoopCompletion>(
                        TaskCreationOptions.RunContinuationsAsynchronously
                    )

                runs.Enqueue(entry, turn, result)
                result.Task

        let props: SessionActorProps =
            {
                Store = store
                Settlement = None
                Tenant = tenant
                SessionId = created.Id
                RunTurn = (fun _ _ -> Task.FromResult completed)
                OnTurnSettled =
                    Some(fun result ->
                        observed.Enqueue result
                        settled.TrySetResult result |> ignore)
                OnInjectJournaled = None
                Compact = None
                Logger = null
            }

        let deps: SessionActor.SuspendDeps =
            {
                EventStore = journal
                Delay = delay
                AskTimeout = TimeSpan.FromMinutes 1.0
                JournalToken = claim.Token
                PrimeClaim = Some claim
                Recovery = null
                RunSuspendable = runner
                ReprimeJournal = None
                RefreshCompact = None
                AgentStore = null
                EraMarked = (fun _ _ _ -> Task.FromResult false)
            }

        let actor = spawn system "session" (SessionActor.behaviorWithSuspend props deps)
        member _.Proxy = proxy
        member _.Store = inner
        member _.Control = inner :?> ISessionAbortControlStore
        member _.Session = created.Id
        member _.Claim = claim
        member _.Clock = clock
        member _.Runs = runs
        member _.Observed = observed
        member _.Sinks = sinks
        member _.Settled = settled.Task

        /// Runs one durable redrive pass over the rig's store: the sole
        /// delivery path for the enqueued route snapshot.
        member _.Deliver() =
            CompletionRedriver.passOnceAsync
                store
                routes
                "rig-redriver"
                (CompletionOptions())
                clock
                (TurnLoopTests.NeverDelay() :> ILlmDelay)
                (NullLogger.Instance :> ILogger)
                ct

        member _.Actor = actor
        member _.Journal = journal

        member _.Prompt text =
            SessionActor.promptSuspendableAsync store tenant created.Id actor (UserMessage.Text text) ct

        member _.Finish(index, result) =
            let _, turn, completion = runs.ToArray()[index]

            completion.TrySetResult
                {
                    Result = result
                    TurnId = turn
                    HasPendingInjects = false
                    Suspension = None
                }
            |> ignore

        interface IDisposable with
            member _.Dispose() =
                system.Terminate().GetAwaiter().GetResult()

    [<Theory>]
    [<InlineData("decision")>]
    [<InlineData("consume")>]
    [<InlineData("retire")>]
    [<InlineData("lost-response")>]
    let ``genuine actor mandatory failures retain barrier and never drain`` failure =
        task {
            use rig = new Rig(true)
            let! entry = rig.Prompt "current"
            let! queued = rig.Prompt "unrelated"
            rig.Proxy.Failure <- failure
            rig.Proxy.Effects.Clear()
            rig.Finish(0, completed)
            do! rig.Proxy.Failed.Task.WaitAsync bound
            let! target = rig.Control.ReadAbortTarget(tenant, rig.Session, ct)

            match target with
            | null -> failwith "Lost barrier"
            | target ->
                Assert.Equal(
                    (if failure = "decision" then
                         ControlTargetState.Active
                     else
                         ControlTargetState.TerminalPendingRetirement),
                    target.State
                )

            let! pending = rig.Store.ReadPendingInbox(tenant, rig.Session, ct)
            Assert.Contains(pending, fun row -> row.Position = queued.Position)

            if failure <> "retire" then
                Assert.Contains(pending, fun row -> row.Position = entry.Position)
                Assert.Empty rig.Observed
                Assert.Empty rig.Sinks
                Assert.DoesNotContain("EnqueueCompletionOutbox", rig.Proxy.Effects)
                let! replay = rig.Journal.Replay(tenant, rig.Session, 0L, 100, ct)

                match replay with
                | :? EventReplayPage as page -> Assert.Empty(page.Events)
                | _ -> ()
            else
                Assert.Single(rig.Observed) |> ignore
                do! rig.Deliver()
                Assert.Single(rig.Sinks) |> ignore

            Assert.Single(rig.Runs) |> ignore
            Assert.DoesNotContain("CloseSession", rig.Proxy.Effects)
            Assert.DoesNotContain("SettleTurn", rig.Proxy.Effects)
            let! stored = rig.Store.GetSession(tenant, rig.Session, ct)

            match stored with
            | null -> failwith "No session"
            | stored -> Assert.Equal(SessionState.Running, stored.State)
        }

    [<Fact>]
    let ``genuine report decision barrier chooses first durable stop before effects`` () =
        task {
            use rig = new Rig(true)
            use release = new ManualResetEventSlim(false)
            rig.Proxy.DecisionRelease <- release
            let! _ = rig.Prompt "current"
            rig.Finish(0, completed)
            do! rig.Proxy.DecisionEntered.Task.WaitAsync bound
            Assert.Empty rig.Observed
            Assert.Empty rig.Sinks
            let! current = rig.Control.ReadAbortTarget(tenant, rig.Session, ct)

            let target =
                match current with
                | null -> failwith "No target"
                | target -> target

            let! _ =
                rig.Control.RequestHostAbort(tenant, rig.Session, target.TurnId, StopCause.HostShutdown, "first", ct)

            let! _ =
                rig.Control.RequestHostAbort(tenant, rig.Session, target.TurnId, StopCause.ExplicitAbort, "late", ct)

            release.Set()
            let! result = rig.Settled.WaitAsync bound
            Assert.Equal(TurnStatus.Aborted, result.Status)

            match result.Outcome with
            | :? TurnAborted as stop -> Assert.Equal("first", stop.Reason)
            | _ -> failwith "No canonical stop"

            let! _ = SessionActor.getSuspendSnapshotAsync rig.Actor ct
            Assert.DoesNotContain("CloseSession", rig.Proxy.Effects)
        }

    [<Fact>]
    let ``genuine completion first keeps verdict when abort arrives later`` () =
        task {
            use rig = new Rig(false)
            let! _ = rig.Prompt "current"
            let _, turn, _ = rig.Runs.ToArray()[0]
            rig.Finish(0, completed)
            let! result = rig.Settled.WaitAsync bound
            Assert.Equal(TurnStatus.Completed, result.Status)
            let! _ = SessionActor.getSuspendSnapshotAsync rig.Actor ct
            let! late = rig.Control.RequestHostAbort(tenant, rig.Session, turn, StopCause.HostShutdown, "too late", ct)
            Assert.Equal(HostAbortOutcome.AlreadyTerminal, late.Outcome)
            Assert.Equal(Nullable TurnStatus.Completed, late.TerminalStatus)
            Assert.Single(rig.Observed) |> ignore
            do! rig.Deliver()
            Assert.Single(rig.Sinks) |> ignore
        }

    [<Fact>]
    let ``genuine two entry chain keeps shared prime and ignores late old report`` () =
        task {
            use rig = new Rig(false)
            let! first = rig.Prompt "first"
            let! second = rig.Prompt "second"
            let _, oldTurn, _ = rig.Runs.ToArray()[0]
            rig.Finish(0, completed)
            let! _ = rig.Settled.WaitAsync bound
            let! _ = SessionActor.getSuspendSnapshotAsync rig.Actor ct
            Assert.Equal(2, rig.Runs.Count)
            let _, newTurn, _ = rig.Runs.ToArray()[1]
            Assert.NotEqual(oldTurn, newTurn)
            let! held = rig.Store.VerifyClaim(tenant, rig.Claim, ct)
            Assert.IsType<TurnLeaseHeld>(held) |> ignore

            rig.Actor.Tell(
                SessionActor.SuspendableFinished(
                    first,
                    {
                        Result = completed
                        TurnId = oldTurn
                        HasPendingInjects = false
                        Suspension = None
                    },
                    1,
                    System.Collections.Generic.HashSet<string>()
                )
            )

            let! _ = SessionActor.getSuspendSnapshotAsync rig.Actor ct
            Assert.Single(rig.Observed) |> ignore
            Assert.Equal(2, rig.Runs.Count)
            let! pending = rig.Store.ReadPendingInbox(tenant, rig.Session, ct)
            Assert.Single(pending) |> ignore
            Assert.Equal(second.Position, pending[0].Position)
            let! current = rig.Control.ReadAbortTarget(tenant, rig.Session, ct)

            match current with
            | null -> failwith "New target lost"
            | target -> Assert.Equal(newTurn, target.TurnId)
        }

    [<Fact>]
    let ``genuine takeover rejects old actor report with zero terminal effects`` () =
        task {
            use rig = new Rig(true)
            let! entry = rig.Prompt "original"
            let! queued = rig.Prompt "unrelated"
            let _, turn, _ = rig.Runs.ToArray()[0]
            rig.Clock.Advance(TimeSpan.FromMinutes 6.0)

            let! winner =
                rig.Control.TryRecoverControlTarget(tenant, rig.Session, turn, "winner", TimeSpan.FromMinutes 5.0, ct)

            Assert.Equal(ControlOperationOutcome.Applied, winner.Outcome)
            rig.Finish(0, completed)
            do! rig.Proxy.DecisionEntered.Task.WaitAsync bound
            let! pending = rig.Store.ReadPendingInbox(tenant, rig.Session, ct)
            Assert.Contains(pending, fun row -> row.Position = entry.Position)
            Assert.Contains(pending, fun row -> row.Position = queued.Position)
            Assert.Empty rig.Observed
            Assert.Empty rig.Sinks
            Assert.Single(rig.Runs) |> ignore

            match winner.Claim with
            | null -> failwith "No winner"
            | claim ->
                let! held = rig.Store.VerifyClaim(tenant, claim, ct)
                Assert.IsType<TurnLeaseHeld>(held) |> ignore
        }
