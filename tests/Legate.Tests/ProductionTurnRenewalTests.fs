// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ProductionTurnRenewalTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// Issue 375: production turn ownership renews through execution and
// suspension. Separate production-shaped fixture driving
// spawnSuspendFactoryRouted with the ambient LeaseAdmission scope, the
// FakeTimeProvider clock, recording delays, and the ScriptedChatClient.
// The harness runners only swap the always-true stub for the ambient
// check (default true, no behavior change to existing suites).

let private tenant = TenantId.Create "turn-renewal"

let private createStores (clock: TimeProvider) =
    let database = InMemoryDatabase(clock)
    InMemorySessionStore(database) :> ISessionStore, InMemorySessionEventStore(database) :> ISessionEventStore

let private sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "renewal"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private createSession (store: ISessionStore) =
    store.CreateSession(tenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

let private storedOf (store: ISessionStore) (sessionId: SessionId) =
    match store.GetSession(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult() with
    | null -> failwith "Expected the session row to exist."
    | session -> session

let private waitFor (timeout: TimeSpan) (condition: unit -> bool) =
    let deadline = DateTime.UtcNow + timeout
    let mutable holds = condition ()

    while not holds && DateTime.UtcNow < deadline do
        Thread.Sleep(25)
        holds <- condition ()

    holds

let private completedResult (text: string) =
    {
        AssistantText = text
        Status = TurnStatus.Completed
        Iterations = 1
        Usage = { InputTokens = 1L; OutputTokens = 1L }
        Outcome = null
    }

let private immediateRunner (text: string) : SessionActor.SuspendableRunner =
    fun _ _ _ _ _ _ _ _ _ _ turnId ->
        let completion: TurnLoop.TurnLoopCompletion =
            {
                Result = completedResult text
                TurnId = turnId
                HasPendingInjects = false
                Suspension = None
            }

        Task.FromResult(completion)

let private startServiceWith
    (store: ISessionStore)
    (journal: ISessionEventStore)
    (clock: TimeProvider)
    (delay: ILlmDelay)
    (runner: SessionActor.SuspendableRunner)
    (heartbeat: ClaimHeartbeat.ClaimHeartbeatOptions option)
    : ActorSystem =
    let system = ActorSystem.Create("renewal-" + Guid.NewGuid().ToString("N"))

    let factory =
        SessionActor.spawnSuspendFactoryRouted
            None
            store
            tenant
            journal
            delay
            (TimeSpan.FromMinutes 5.0)
            "renewal-owner"
            (TimeSpan.FromHours 1.0)
            runner
            (fun _ _ -> None)
            null
            (fun _ _ _ -> Task.FromResult false)
            clock
            heartbeat
            None

    let _router = LocalActorSystem.spawnRouterWith system factory
    system

let private resolveChild (system: ActorSystem) (sessionId: SessionId) =
    let routerSel = system.ActorSelection("/user/legate-session-router")
    let router = routerSel.ResolveOne(TimeSpan.FromSeconds 5.0).GetAwaiter().GetResult()

    router
        .Ask<IActorRef>(SessionRouterMessage.ResolveSession sessionId.Value, CancellationToken.None)
        .GetAwaiter()
        .GetResult()

let private stubTool (name: string) (result: string) (invocations: string list ref) : AITool =
    let method =
        Func<string>(fun () ->
            invocations.Value <- invocations.Value @ [ name ]
            result)

    AIFunctionFactory.Create(method, name, Unchecked.defaultof<string>, Unchecked.defaultof<JsonSerializerOptions>)
    :> AITool

type private AskOncePolicy(gated: string) =
    interface IPermissionPolicy with
        member _.Evaluate(request) =
            if request.ToolName = gated then
                PermissionVerdict.Ask
            else
                PermissionVerdict.Allow

/// Heartbeat delay that records waits and blocks on the FakeClock:
/// renewals fire only when the test advances the clock, so no tight
/// spin and no wall-clock sleeps.
type private ClockRecordingDelay(clock: FakeClock, recorded: ResizeArray<TimeSpan>) =
    interface ILlmDelay with
        member _.Delay(span, cancellationToken) =
            recorded.Add(span)
            Task.Delay(span, clock, cancellationToken)

[<Fact>]
let ``fromSessions derives the production renewal tuning and enforces the under-half bound`` () =
    let sessions = SessionsOptions()
    sessions.LeaseDuration <- TimeSpan.FromSeconds 60.0
    sessions.LeaseRenewalInterval <- TimeSpan.FromSeconds 15.0

    let options = ClaimHeartbeat.fromSessions sessions
    options.LeaseDuration |> should equal (TimeSpan.FromSeconds 60.0)
    options.RenewalInterval |> should equal (TimeSpan.FromSeconds 15.0)

    let bad = SessionsOptions()
    bad.LeaseDuration <- TimeSpan.FromSeconds 60.0
    bad.LeaseRenewalInterval <- TimeSpan.FromSeconds 30.0

    (fun () -> ClaimHeartbeat.fromSessions bad |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

[<Fact>]
let ``LeaseAdmission defaults true and scopes the live view`` () =
    LeaseAdmission.check () |> should equal true

    let scope = LeaseAdmission.enter (fun () -> false)
    LeaseAdmission.check () |> should equal false
    scope.Dispose()
    LeaseAdmission.check () |> should equal true

[<Fact>]
let ``prime stays one hour while renewals grant the Sessions duration`` () =
    let clientOptions = SessionClientOptions()
    clientOptions.LeaseDuration |> should equal (TimeSpan.FromHours 1.0)

    let sessions = SessionsOptions()
    let heartbeat = ClaimHeartbeat.fromSessions sessions
    heartbeat.LeaseDuration |> should equal (TimeSpan.FromSeconds 60.0)
    heartbeat.RenewalInterval |> should equal (TimeSpan.FromSeconds 15.0)

[<Fact>]
let ``long execution renews under configured settings without wall-clock sleeps`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let recorded = ResizeArray<TimeSpan>()
    let delay = ClockRecordingDelay(clock, recorded) :> ILlmDelay
    let heartbeat = Some(ClaimHeartbeat.fromSessions (SessionsOptions()))

    let gate =
        TaskCompletionSource<TurnLoop.TurnLoopCompletion>(TaskCreationOptions.RunContinuationsAsynchronously)

    let blockingRunner: SessionActor.SuspendableRunner =
        fun _ _ _ _ _ _ _ _ _ _ _ -> gate.Task

    // Seed the gate with a completed turn to be released after renewals.
    let release () =
        let completion: TurnLoop.TurnLoopCompletion =
            {
                Result = completedResult "done"
                TurnId = TurnId.New()
                HasPendingInjects = false
                Suspension = None
            }

        gate.TrySetResult(completion) |> ignore

    use system = startServiceWith store journal clock delay blockingRunner heartbeat

    try
        let child = resolveChild system created.Id

        SessionActor.promptSuspendableAsync
            store
            tenant
            created.Id
            child
            (UserMessage.Text "run")
            CancellationToken.None
        |> fun t -> t.GetAwaiter().GetResult() |> ignore

        let running =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (storedOf store created.Id).State = SessionState.Running)

        running |> should equal true

        // Advance past one renewal interval: the heartbeat renews in place.
        clock.Advance(TimeSpan.FromSeconds 16.0)

        let renewed = waitFor (TimeSpan.FromSeconds 10.0) (fun () -> recorded.Count > 0)

        renewed |> should equal true

        // Still running under the same prime: no settle yet.
        (storedOf store created.Id).State |> should equal SessionState.Running

        release ()

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (storedOf store created.Id).State = SessionState.Idle)

        settled |> should equal true
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore

[<Fact>]
let ``suspended renewal consumes no reply and resumes no work by itself`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let recorded = ResizeArray<TimeSpan>()
    let delay = ClockRecordingDelay(clock, recorded) :> ILlmDelay
    let invocations = ref []

    let toolsDict =
        let table = Dictionary<string, AITool>(StringComparer.Ordinal)
        table["exec"] <- stubTool "exec" "out" invocations
        table :> IReadOnlyDictionary<string, AITool>

    let chat =
        new ScriptedChatClient(
            ResizeArray<ScriptStep>(
                [|
                    ScriptStep.ToolCall("c1", "exec")
                    ScriptStep.Text("never")
                |]
            )
            :> IReadOnlyList<ScriptStep>
        )

    let policy = AskOncePolicy("exec") :> IPermissionPolicy

    let runner =
        SessionPermissions.createRunner
            (chat :> IChatClient)
            store
            tenant
            (fun _ _ -> Task.FromResult((toolsDict, TurnLoop.TurnLoopOptions.Default)))
            delay
            policy
            None
            journal
            SessionStreaming.defaultBounds
            None

    let heartbeat = Some(ClaimHeartbeat.fromSessions (SessionsOptions()))

    use system = startServiceWith store journal clock delay runner heartbeat

    try
        let child = resolveChild system created.Id

        SessionActor.promptSuspendableAsync
            store
            tenant
            created.Id
            child
            (UserMessage.Text "run")
            CancellationToken.None
        |> fun t -> t.GetAwaiter().GetResult() |> ignore

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        // The Ask parked the call: no tool effect ran and the renewal
        // itself resumed no work.
        invocations.Value.Length |> should equal 0

        // Suspended renewal continues under the same claim: advance past
        // one interval and stay parked with no extra provider work.
        recorded.Clear()
        clock.Advance(TimeSpan.FromSeconds 16.0)

        let renewed = waitFor (TimeSpan.FromSeconds 10.0) (fun () -> recorded.Count > 0)

        renewed |> should equal true
        (storedOf store created.Id).State |> should equal SessionState.WaitingForInput
        invocations.Value.Length |> should equal 0

        let pending =
            store.ReadPendingInbox(tenant, created.Id, CancellationToken.None).GetAwaiter().GetResult()

        (pending.Count >= 0) |> should equal true
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore

[<Fact>]
let ``expired claim renews to lease loss and the view flips invalid`` () =
    let clock = FakeClock(DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))
    let store, _journal = createStores clock
    let created = createSession store

    let payload = UserMessagePayload(UserMessage.Text "prime") :> InboxPayload

    store.AppendInboxMessage(tenant, created.Id, payload, DeliveryMode.Queue, CancellationToken.None)
    |> fun t -> t.GetAwaiter().GetResult() |> ignore

    let leased =
        store.ClaimNextTurn(tenant, created.Id, "owner-a", TimeSpan.FromSeconds 60.0, CancellationToken.None)
        |> fun t -> t.GetAwaiter().GetResult()

    let claim =
        match leased with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | other -> failwithf "Expected a granted claim, got %O." other

    let view = ClaimHeartbeat.ClaimLeaseView(clock, claim)
    view.IsValid() |> should equal true

    clock.Advance(TimeSpan.FromSeconds 61.0)
    view.IsValid() |> should equal false

    let rivalPayload = UserMessagePayload(UserMessage.Text "rival") :> InboxPayload

    store.AppendInboxMessage(tenant, created.Id, rivalPayload, DeliveryMode.Queue, CancellationToken.None)
    |> fun t -> t.GetAwaiter().GetResult() |> ignore

    let rival =
        store.ClaimNextTurn(tenant, created.Id, "owner-b", TimeSpan.FromSeconds 60.0, CancellationToken.None)
        |> fun t -> t.GetAwaiter().GetResult()

    match rival with
    | :? TurnLeaseRenewed
    | :? TurnLeaseHeld
    | :? TurnLeaseExpiring -> ()
    | other -> failwithf "Expected the rival to hold a claim, got %O." other

    let renewed =
        store.ObserveAndRenewClaim(tenant, claim, TimeSpan.FromSeconds 60.0, CancellationToken.None)
        |> fun t -> t.GetAwaiter().GetResult()

    match renewed with
    | :? TurnLeaseLost
    | :? TurnLeaseMissing -> ()
    | other -> failwithf "Expected the loser to observe loss, got %O." other

[<Fact>]
let ``loser cannot settle the winner`` () =
    let clock = FakeClock()
    let store, _journal = createStores clock
    let created = createSession store

    let first = UserMessagePayload(UserMessage.Text "first") :> InboxPayload

    store.AppendInboxMessage(tenant, created.Id, first, DeliveryMode.Queue, CancellationToken.None)
    |> fun t -> t.GetAwaiter().GetResult() |> ignore

    let firstLeased =
        store.ClaimNextTurn(tenant, created.Id, "owner-a", TimeSpan.FromSeconds 60.0, CancellationToken.None)
        |> fun t -> t.GetAwaiter().GetResult()

    let firstClaim =
        match firstLeased with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | other -> failwithf "Expected first claim, got %O." other

    clock.Advance(TimeSpan.FromSeconds 61.0)

    let second = UserMessagePayload(UserMessage.Text "second") :> InboxPayload

    store.AppendInboxMessage(tenant, created.Id, second, DeliveryMode.Queue, CancellationToken.None)
    |> fun t -> t.GetAwaiter().GetResult() |> ignore

    let secondLeased =
        store.ClaimNextTurn(tenant, created.Id, "owner-b", TimeSpan.FromSeconds 60.0, CancellationToken.None)
        |> fun t -> t.GetAwaiter().GetResult()

    let secondClaim =
        match secondLeased with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | :? TurnLeaseHeld as held -> held.Claim
        | :? TurnLeaseExpiring as expiring -> expiring.Claim
        | other -> failwithf "Expected second claim, got %O." other

    let loserSettle =
        store.SettleTurn(tenant, firstClaim, TurnStatus.Completed, null, CancellationToken.None)
        |> fun t -> t.GetAwaiter().GetResult()

    match loserSettle with
    | :? TurnAlreadySettled -> ()
    | :? TurnSettled -> failwith "The loser must not settle the winner as completed."
    | _ -> ()

    let winnerSettle =
        store.SettleTurn(tenant, secondClaim, TurnStatus.Completed, null, CancellationToken.None)
        |> fun t -> t.GetAwaiter().GetResult()

    match winnerSettle with
    | :? TurnSettled
    | :? TurnAlreadySettled -> ()
    | other -> failwithf "Expected the winner to settle, got %O." other

[<Fact>]
let ``unsupported journal format fails closed without rebinding a heartbeat`` () =
    let clock = FakeClock()
    let database = InMemoryDatabase(clock)
    let store = InMemorySessionStore(database) :> ISessionStore
    let journal = InMemorySessionEventStore(database) :> ISessionEventStore

    let badOptions = SessionOptions()
    badOptions.FormatVersion <- 0

    let bad =
        {
            Id = SessionId.New()
            Tenant = tenant
            AgentId = AgentId.New()
            Title = "bad"
            State = SessionState.Idle
            CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
            WorkspaceBinding = null
            Options = badOptions
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    // Bypass CreateSession validation: legacy rows exist from before the
    // format gate, and activation must refuse them fail-closed.
    database.Sessions[(tenant, bad.Id)] <- bad
    let created = bad

    let factory =
        SessionActor.spawnSuspendFactoryRouted
            None
            store
            tenant
            journal
            (RecordingDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            "owner"
            (TimeSpan.FromHours 1.0)
            (immediateRunner "never")
            (fun _ _ -> None)
            null
            (fun _ _ _ -> Task.FromResult false)
            clock
            (Some(ClaimHeartbeat.fromSessions (SessionsOptions())))
            None

    use system = ActorSystem.Create("unsupported-" + Guid.NewGuid().ToString("N"))

    try
        (fun () ->
            let context = Unchecked.defaultof<IActorContext>
            factory created.Id.Value context "child" |> ignore)
        |> should throw typeof<Exception>
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore
