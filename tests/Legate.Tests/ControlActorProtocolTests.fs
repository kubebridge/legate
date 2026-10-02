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

            if this.Failure = "lost-response" then
                result.GetAwaiter().GetResult() |> ignore
                this.Failed.TrySetResult() |> ignore
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
                let turn = TurnId.New()
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
                    Assert.False(entered.Task.IsCompleted)
                    Assert.Single(rows) |> ignore
                    let! stored = freshStore.GetSession(tenant, created.Id, ct)

                    match stored with
                    | null -> failwith "Lost session"
                    | stored -> Assert.Equal(SessionState.Idle, stored.State)

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

    type private Rig(autoClose: bool) =
        let clock = TestClock()
        let db = InMemoryDatabase(clock)
        let inner = InMemorySessionStore(db) :> ISessionStore
        let store = DispatchProxy.Create<ISessionStore, ControlFaultStore>()
        let proxy = store :?> ControlFaultStore
        let journal = InMemorySessionEventStore(db) :> ISessionEventStore
        let observed = ConcurrentQueue<TurnResult>()
        let sinks = ConcurrentQueue<SessionCompletion>()

        let runs =
            ConcurrentQueue<InboxEntry * TurnId * TaskCompletionSource<TurnLoop.TurnLoopCompletion>>()

        let settled =
            TaskCompletionSource<TurnResult>(TaskCreationOptions.RunContinuationsAsynchronously)

        let options = SessionOptions(AutoClose = autoClose)

        do
            proxy.Inner <- inner

            options.CompletionSink <-
                { new ISessionCompletionSink with
                    member _.Notify completion = sinks.Enqueue completion
                }

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
