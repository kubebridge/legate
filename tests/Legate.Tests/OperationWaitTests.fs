// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.OperationWaitTests

open System
open System.Collections.Generic
open System.Reflection
open System.Runtime.CompilerServices
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Xunit

// Receipt-bound operation waits (issue 383): every wait follows its own
// accepted-operation receipt to the single authoritative
// execution_settlements row, with the live hint as a fast path only. Late,
// concurrent, reconnected, restarted, and separate-process observers read
// the same durable truth; cancel and deadline abandon only the observation.
// Deterministic and event-driven: waits carry generous bounds while the
// scripted turns settle in milliseconds, and deadline tests hold the turn
// on a blocking tool or fire the seam immediately, never sleeps.

let private waitBound = TimeSpan.FromSeconds 30.0

let private awaitWhat (work: Task<'T>) (what: string) : Task<'T> =
    task {
        try
            return! work.WaitAsync(TimeSpan.FromSeconds 10.0, CancellationToken.None)
        with :? TimeoutException ->
            return raise (TimeoutException($"The test timed out waiting for {what}."))
    }

let private scripted (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

let private sourced (tools: AITool list) : StaticToolSource =
    new StaticToolSource(ResizeArray<AITool>(tools) :> IReadOnlyList<AITool>)

/// A tool that signals entry, then blocks until released: keeps a turn
/// Running so waits meet it mid-flight.
let private blockingTool
    (name: string)
    (entered: ManualResetEventSlim)
    (release: TaskCompletionSource<string>)
    : AITool =
    let method =
        Func<Task<string>>(fun () ->
            entered.Set() |> ignore
            release.Task)

    AIFunctionFactory.Create(method, name, Unchecked.defaultof<string>, Unchecked.defaultof<JsonSerializerOptions>)
    :> AITool

let private createServicesOn
    (database: InMemoryDatabase)
    (chatClient: ScriptedChatClient)
    (tools: StaticToolSource)
    : IServiceCollection =
    let services = ServiceCollection() :> IServiceCollection

    LegateServiceCollectionExtensions.AddLegate(
        services,
        ?configure =
            Some(fun (builder: LegateBuilder) ->
                builder.Llm.AddProvider(BuilderTests.StubLlmProvider()) |> ignore
                builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore
                builder.Workspace.UseRuntime(BuilderTests.StubWorkspaceRuntime()) |> ignore
                builder.Tools.AddSource(tools) |> ignore)
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    services.AddSingleton<IChatClient>(chatClient) |> ignore
    services

let private createServices (chatClient: ScriptedChatClient) (tools: StaticToolSource) : IServiceCollection =
    createServicesOn (InMemoryDatabase()) chatClient tools

let private actorServiceOf (provider: IServiceProvider) : LocalActorSystemService =
    provider.GetServices<IHostedService>()
    |> Seq.pick (fun service ->
        match service with
        | :? LocalActorSystemService as local -> Some local
        | _ -> None)

let private stopQuietly (service: LocalActorSystemService) : Task =
    task {
        try
            do! (service :> IHostedService).StopAsync(CancellationToken.None)
        with _ ->
            ()
    }

let private withClient (provider: IServiceProvider) (work: SessionClient -> Task<'T>) : Task<'T> =
    task {
        let service = actorServiceOf provider
        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let client = provider.GetRequiredService<SessionClient>()

        try
            let! outcome = work client
            do! stopQuietly service
            return outcome
        with ex ->
            do! stopQuietly service
            return raise ex
    }

let private openSession (client: SessionClient) : Task<Session> =
    SessionClientOperations.OpenSessionAsync(client, AgentId.New(), null, CancellationToken.None)

let private prompt (client: SessionClient) (sessionId: SessionId) (text: string) (delivery: DeliveryMode) =
    SessionClientOperations.PromptAsync(client, sessionId, UserMessage.Text text, delivery, CancellationToken.None)

let private waitOp (client: SessionClient) (receipt: AcceptedOperation) (bound: TimeSpan) (ct: CancellationToken) =
    SessionClientOperations.WaitForOperationAsync(client, receipt, bound, ct)

/// Spins without sleeping until the committed winner lands, then returns
/// it. The scripted turns settle in milliseconds; the deadline only fires
/// on a test bug.
let private awaitTerminal (client: SessionClient) (receipt: AcceptedOperation) : Task<OperationResult> =
    task {
        let deadline = DateTimeOffset.UtcNow.AddSeconds(10.0)
        let mutable found: OperationResult | null = null

        while isNull (box found) && DateTimeOffset.UtcNow < deadline do
            let! observed = SessionClientOperations.GetOperationResultAsync(client, receipt, CancellationToken.None)

            if observed.Status = OperationStatus.Terminal then
                found <- observed
            else
                do! Task.Yield()

        Assert.False(isNull (box found), "The operation settled before the bound.")
        return unbox<OperationResult> (box found)
    }

let private presentResult (value: TurnResult | null) : TurnResult =
    Assert.False(isNull (box value))
    unbox<TurnResult> (box value)

/// Permission policy asking once for the gated tool, allowing the rest.
type private AskWaitPolicy(gated: string) =
    interface IPermissionPolicy with
        member _.Evaluate(request) =
            if request.ToolName = gated then
                PermissionVerdict.Ask
            else
                PermissionVerdict.Allow

/// A settlement store whose reads always fail: storage is unavailable, and
/// observations must read Unavailable, never terminal.
type private FailingSettlementStore() =
    interface ISessionSettlementStore with
        member _.SupportsSettlementJournal _ = false

        member _.AdmitExecution(_, _, _, _, _) = Task.FromResult(false)

        member _.SettleExecution(_, _, _) =
            task { return raise (InvalidOperationException("The settlement store is unavailable.")) }

        member _.TryReadCommitted(_, _, _, _) =
            task { return raise (InvalidOperationException("The settlement store is unavailable.")) }

        member _.TryReadEntry(_, _, _, _) =
            task { return raise (InvalidOperationException("The settlement store is unavailable.")) }

/// A wait-bound seam that never fires unless its token cancels: the turn,
/// never the bound, decides these tests.
type private NeverWaitDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

// ──────────────────────────────────────────────────────────────
// Completion before, during, and racing subscription
// ──────────────────────────────────────────────────────────────

[<Fact>]
let ``completion before subscription resolves without pre-registration`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "already there" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let! receipt = awaitWhat (prompt client created.Id "hi" DeliveryMode.Queue) "the prompt to land"
                    let! winner = awaitTerminal client receipt
                    let winnerResult = presentResult winner.Result
                    Assert.Equal("already there", winnerResult.AssistantText)

                    // A late observer that never registered still reads the
                    // same committed winner on its first durable read.
                    let! late = awaitWhat (waitOp client receipt waitBound CancellationToken.None) "the late wait"

                    Assert.Equal(OperationStatus.Terminal, late.Status)
                    Assert.Equal(receipt.Position, late.Position)
                    Assert.Equal(receipt.Kind, late.Kind)
                    let lateResult = presentResult late.Result
                    Assert.Equal("already there", lateResult.AssistantText)

                    // Concurrent late observers share the one winner.
                    let first = waitOp client receipt waitBound CancellationToken.None
                    let second = waitOp client receipt waitBound CancellationToken.None
                    let! both = awaitWhat (Task.WhenAll(first, second)) "both late waits"
                    Assert.Equal("already there", (presentResult both[0].Result).AssistantText)
                    Assert.Equal("already there", (presentResult both[1].Result).AssistantText)
                })
    }

[<Fact>]
let ``completion racing subscription converges on the same winner`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "racing" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let! receipt = awaitWhat (prompt client created.Id "hi" DeliveryMode.Queue) "the prompt to land"

                    // No settle barrier first: the wait subscribes while the
                    // turn is still running and converges via hint plus
                    // re-read.
                    let! observed =
                        awaitWhat (waitOp client receipt waitBound CancellationToken.None) "the racing wait"

                    Assert.Equal(OperationStatus.Terminal, observed.Status)
                    let observedResult = presentResult observed.Result
                    Assert.Equal(TurnStatus.Completed, observedResult.Status)
                    Assert.Equal("racing", observedResult.AssistantText)
                })
    }

// ──────────────────────────────────────────────────────────────
// Cancel and deadline abandon only the observation
// ──────────────────────────────────────────────────────────────

[<Fact>]
let ``a lapsed bound abandons only that wait while the turn keeps running`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "done"
                ]

        use provider =
            (createServices
                chat
                (sourced
                    [
                        blockingTool "blocker" entered release
                    ]))
                .BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client

                        let! receipt =
                            awaitWhat (prompt client created.Id "start" DeliveryMode.Queue) "the prompt to land"

                        Assert.True(entered.Wait(TimeSpan.FromSeconds 10.0))

                        // A holds a short bound while the turn is parked:
                        // only A's observation lapses, never the turn.
                        let! lapsed =
                            Assert.ThrowsAsync<DeadlineExceededException>(fun () ->
                                waitOp client receipt (TimeSpan.FromMilliseconds 250.0) CancellationToken.None)

                        Assert.Equal("WaitForOperation", lapsed.OperationName)

                        // B waits on the same receipt and observes the
                        // winner; the lapsed wait consumed nothing.
                        let waiting = waitOp client receipt waitBound CancellationToken.None
                        release.TrySetResult("unblocked") |> ignore
                        let! winner = awaitWhat waiting "the survivor wait"

                        Assert.Equal(OperationStatus.Terminal, winner.Status)
                        let winnerResult = presentResult winner.Result
                        Assert.Equal("done", winnerResult.AssistantText)

                        // A later wait for the same operation remains valid.
                        let! again =
                            awaitWhat (waitOp client receipt waitBound CancellationToken.None) "the later wait"

                        Assert.Equal(OperationStatus.Terminal, again.Status)
                        let againResult = presentResult again.Result
                        Assert.Equal("done", againResult.AssistantText)
                    })
        finally
            entered.Dispose()
    }

[<Fact>]
let ``one cancelled observer does not disturb the survivor`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "shared"
                ]

        use provider =
            (createServices
                chat
                (sourced
                    [
                        blockingTool "blocker" entered release
                    ]))
                .BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client

                        let! receipt =
                            awaitWhat (prompt client created.Id "hi" DeliveryMode.Queue) "the prompt to land"

                        Assert.True(entered.Wait(TimeSpan.FromSeconds 10.0))

                        use abandoned = new CancellationTokenSource()

                        let leaving = waitOp client receipt waitBound abandoned.Token
                        let staying = waitOp client receipt waitBound CancellationToken.None

                        // The turn is parked in the tool, so the cancel
                        // lands mid-flight: only this observation ends.
                        abandoned.Cancel()

                        let abandon () : Task = leaving :> Task
                        let! _ = Assert.ThrowsAsync<OperationCanceledException>(abandon)

                        release.TrySetResult("unblocked") |> ignore
                        let! survivor = awaitWhat staying "the survivor wait"

                        Assert.Equal(OperationStatus.Terminal, survivor.Status)
                        let survivorResult = presentResult survivor.Result
                        Assert.Equal("shared", survivorResult.AssistantText)
                    })
        finally
            entered.Dispose()
    }

// ──────────────────────────────────────────────────────────────
// Failure, abort, and duplicate reports
// ──────────────────────────────────────────────────────────────

[<Fact>]
let ``failed turns report the committed failure`` () : Task =
    task {
        let failure = InvalidOperationException("boom")

        use provider =
            (createServices (scripted [ ScriptStep.Failure(failure) ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let! receipt = awaitWhat (prompt client created.Id "hi" DeliveryMode.Queue) "the prompt to land"

                    let! observed =
                        awaitWhat (waitOp client receipt waitBound CancellationToken.None) "the failure wait"

                    Assert.Equal(OperationStatus.Terminal, observed.Status)
                    let observedResult = presentResult observed.Result
                    Assert.Equal(TurnStatus.Failed, observedResult.Status)

                    match box observedResult.Outcome with
                    | :? TurnFailed -> ()
                    | _ -> Assert.Fail("The failed turn carries no TurnFailed outcome.")
                })
    }

[<Fact>]
let ``abort settles the committed abort verdict`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "done"
                ]

        use provider =
            (createServices
                chat
                (sourced
                    [
                        blockingTool "blocker" entered release
                    ]))
                .BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client

                        let! receipt =
                            awaitWhat (prompt client created.Id "start" DeliveryMode.Queue) "the prompt to land"

                        Assert.True(entered.Wait(TimeSpan.FromSeconds 10.0))

                        let! current =
                            SessionClientOperations.ReadAbortTargetAsync(client, created.Id, CancellationToken.None)

                        let target =
                            match current with
                            | null -> failwith "No current control target."
                            | target -> target

                        let! abortReceipt =
                            SessionClientOperations.AbortAsync(
                                client,
                                created.Id,
                                target.TurnId,
                                StopCause.ExplicitAbort,
                                "test-abort",
                                CancellationToken.None
                            )

                        Assert.Equal(HostAbortOutcome.Accepted, abortReceipt.Outcome)

                        let waiting = waitOp client receipt waitBound CancellationToken.None
                        release.TrySetResult("unblocked") |> ignore
                        let! observed = awaitWhat waiting "the abort wait"

                        Assert.Equal(OperationStatus.Terminal, observed.Status)
                        let observedResult = presentResult observed.Result
                        Assert.Equal(TurnStatus.Aborted, observedResult.Status)

                        match box observedResult.Outcome with
                        | :? TurnAborted -> ()
                        | _ -> Assert.Fail("The aborted turn carries no TurnAborted outcome.")
                    })
        finally
            entered.Dispose()
    }

// ──────────────────────────────────────────────────────────────
// Delivery-mode associations
// ──────────────────────────────────────────────────────────────

[<Fact>]
let ``interrupt and displaced turns stay unconfusable under waits`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "tail"
                    ScriptStep.Text "second"
                ]

        use provider =
            (createServices
                chat
                (sourced
                    [
                        blockingTool "blocker" entered release
                    ]))
                .BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client

                        let! first =
                            awaitWhat (prompt client created.Id "start" DeliveryMode.Queue) "the first prompt to land"

                        Assert.True(entered.Wait(TimeSpan.FromSeconds 10.0))

                        let! interrupted =
                            awaitWhat
                                (prompt client created.Id "stop that" DeliveryMode.Interrupt)
                                "the interrupt to land"

                        Assert.Equal(OperationKind.Interrupt, interrupted.Kind)
                        Assert.NotEqual(first.Position, interrupted.Position)
                        Assert.NotEqual(first.OperationId, interrupted.OperationId)

                        let firstWait = waitOp client first waitBound CancellationToken.None
                        let secondWait = waitOp client interrupted waitBound CancellationToken.None
                        release.TrySetResult("unblocked") |> ignore

                        let! displaced = awaitWhat firstWait "the displaced wait"
                        let! winner = awaitWhat secondWait "the interrupt wait"

                        Assert.Equal(OperationStatus.Terminal, displaced.Status)
                        let displacedResult = presentResult displaced.Result
                        Assert.Equal(TurnStatus.Aborted, displacedResult.Status)

                        Assert.Equal(OperationStatus.Terminal, winner.Status)
                        let winnerResult = presentResult winner.Result
                        Assert.Equal(TurnStatus.Completed, winnerResult.Status)
                        Assert.Equal("second", winnerResult.AssistantText)
                    })
        finally
            entered.Dispose()
    }

[<Fact>]
let ``inject and reply receipts keep their associations without independent turns`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "steered"
                ]

        use provider =
            (createServices
                chat
                (sourced
                    [
                        blockingTool "blocker" entered release
                    ]))
                .BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client

                        let! queued =
                            awaitWhat (prompt client created.Id "start" DeliveryMode.Queue) "the prompt to land"

                        Assert.True(entered.Wait(TimeSpan.FromSeconds 10.0))

                        let! injected =
                            awaitWhat (prompt client created.Id "steer this" DeliveryMode.Inject) "the inject to land"

                        Assert.Equal(OperationKind.Inject, injected.Kind)

                        release.TrySetResult("unblocked") |> ignore

                        let! winner =
                            awaitWhat (waitOp client queued waitBound CancellationToken.None) "the queue wait"

                        Assert.Equal(OperationStatus.Terminal, winner.Status)

                        // The inject receipt never promises an independent
                        // turn, but it stays observable under its own kind.
                        let! injectedObserved =
                            SessionClientOperations.GetOperationResultAsync(client, injected, CancellationToken.None)

                        Assert.Equal(OperationKind.Inject, injectedObserved.Kind)

                        Assert.True(
                            injectedObserved.Status = OperationStatus.Pending
                            || injectedObserved.Status = OperationStatus.Terminal
                        )
                    })
        finally
            entered.Dispose()
    }

[<Fact>]
let ``reply receipts resume without an independent turn`` () : Task =
    task {
        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "gated")
                    ScriptStep.Text "resumed"
                ]

        let invocations = ref []

        let gatedMethod =
            Func<string>(fun () ->
                invocations.Value <- invocations.Value @ [ "gated" ]
                "gated-result")

        let gated =
            AIFunctionFactory.Create(
                gatedMethod,
                "gated",
                Unchecked.defaultof<string>,
                Unchecked.defaultof<JsonSerializerOptions>
            )
            :> AITool

        let services = createServices chat (sourced [ gated ])
        services.AddSingleton<IPermissionPolicy>(AskWaitPolicy("gated")) |> ignore
        use provider = services.BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client

                    let! queued = awaitWhat (prompt client created.Id "run it" DeliveryMode.Queue) "the prompt to land"

                    let mutable requestId = ""
                    let deadline = DateTimeOffset.UtcNow.AddSeconds(10.0)

                    while String.IsNullOrEmpty(requestId) && DateTimeOffset.UtcNow < deadline do
                        let! events =
                            SessionClientOperations.ReadEventsAsync(
                                client,
                                created.Id,
                                0L,
                                100,
                                CancellationToken.None
                            )

                        for event in events do
                            match event with
                            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
                                requestId <- asked.RequestId
                            | _ -> ()

                        if String.IsNullOrEmpty(requestId) then
                            do! Task.Yield()

                    Assert.False(String.IsNullOrEmpty(requestId))

                    let! receipt =
                        SessionClientOperations.ReplyAsync(
                            client,
                            created.Id,
                            PermissionDecision(requestId, PermissionDecisionKind.AllowOnce),
                            CancellationToken.None
                        )

                    Assert.Equal(OperationKind.Reply, receipt.Kind)

                    let! resumed = awaitWhat (waitOp client queued waitBound CancellationToken.None) "the resumed wait"

                    Assert.Equal(OperationStatus.Terminal, resumed.Status)
                    let resumedResult = presentResult resumed.Result
                    Assert.Equal("resumed", resumedResult.AssistantText)

                    let! replyObserved =
                        SessionClientOperations.GetOperationResultAsync(client, receipt, CancellationToken.None)

                    Assert.Equal(OperationKind.Reply, replyObserved.Kind)

                    Assert.True(
                        replyObserved.Status = OperationStatus.Pending
                        || replyObserved.Status = OperationStatus.Terminal
                    )
                })
    }

// ──────────────────────────────────────────────────────────────
// Tenant isolation and authorization
// ──────────────────────────────────────────────────────────────

[<Fact>]
let ``other-tenant observers read as unknown session`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "hi" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let! receipt = awaitWhat (prompt client created.Id "hi" DeliveryMode.Queue) "the prompt to land"

                    let otherTenant = TenantId.Create "op-wait-other"

                    let foreign =
                        new SessionClient(
                            client.Store,
                            otherTenant,
                            (fun _ _ ->
                                Task.FromException<IActorRef>(
                                    InvalidOperationException("No actor resolution across tenants.")
                                )),
                            client.EventBus,
                            waitBound,
                            client.WaitDelay,
                            None
                        )

                    let! missingGet =
                        Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                            SessionClientOperations.GetOperationResultAsync(foreign, receipt, CancellationToken.None))

                    Assert.False(isNull (box missingGet))

                    // Possession of the receipt grants nothing: the wait
                    // fails fast instead of lapsing the bound.
                    let! missingWait =
                        Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                            waitOp foreign receipt waitBound CancellationToken.None)

                    Assert.False(isNull (box missingWait))
                })
    }

// ──────────────────────────────────────────────────────────────
// Reconnect, restart, and separate processes
// ──────────────────────────────────────────────────────────────

[<Fact>]
let ``a second client over the same store observes the winner`` () : Task =
    task {
        let database = InMemoryDatabase()
        let chat = scripted [ ScriptStep.Text "shared" ]

        let buildProvider () =
            (createServicesOn database chat (sourced [])).BuildServiceProvider()

        use firstProvider = buildProvider ()
        let firstService = actorServiceOf firstProvider
        do! (firstService :> IHostedService).StartAsync(CancellationToken.None)

        let mutable receiptOpt: AcceptedOperation option = None

        let! receipt =
            task {
                try
                    let first = firstProvider.GetRequiredService<SessionClient>()
                    let! created = openSession first
                    let! accepted = awaitWhat (prompt first created.Id "hi" DeliveryMode.Queue) "the prompt to land"
                    let! _ = awaitTerminal first accepted
                    return accepted
                with ex ->
                    do! stopQuietly firstService
                    return raise ex
            }

        do! stopQuietly firstService
        receiptOpt <- Some receipt

        match receiptOpt with
        | None -> failwith "The first client accepted no operation."
        | Some accepted ->
            use secondProvider = buildProvider ()
            let secondService = actorServiceOf secondProvider
            do! (secondService :> IHostedService).StartAsync(CancellationToken.None)

            let! observed =
                task {
                    try
                        let second = secondProvider.GetRequiredService<SessionClient>()

                        // No live hint fires in this client: the poll converges on
                        // the same durable row.
                        return! awaitWhat (waitOp second accepted waitBound CancellationToken.None) "the reconnect wait"
                    with ex ->
                        do! stopQuietly secondService
                        return raise ex
                }

            do! stopQuietly secondService

            Assert.Equal(OperationStatus.Terminal, observed.Status)
            let observedResult = presentResult observed.Result
            Assert.Equal("shared", observedResult.AssistantText)
    }

[<Fact>]
let ``unavailable storage never reports terminal`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "hi" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let! receipt = awaitWhat (prompt client created.Id "hi" DeliveryMode.Queue) "the prompt to land"

                    let blind =
                        new SessionClient(
                            client.Store,
                            client.Tenant,
                            (fun _ _ ->
                                Task.FromException<IActorRef>(InvalidOperationException("No actor resolution."))),
                            client.EventBus,
                            waitBound,
                            RecordingDelay() :> ILlmDelay,
                            None
                        )

                    blind.SettlementStore <- Some(FailingSettlementStore() :> ISessionSettlementStore)

                    let! unavailable =
                        SessionClientOperations.GetOperationResultAsync(blind, receipt, CancellationToken.None)

                    Assert.Equal(OperationStatus.Unavailable, unavailable.Status)
                    Assert.True(isNull (box unavailable.Result))

                    // The seam fires at once, so the bound lapses while the
                    // store stays unavailable: the wait reports the lapse,
                    // never a fabricated terminal verdict.
                    let! lapsed =
                        Assert.ThrowsAsync<DeadlineExceededException>(fun () ->
                            waitOp blind receipt (TimeSpan.FromSeconds 5.0) CancellationToken.None)

                    Assert.Equal("WaitForOperation", lapsed.OperationName)
                })
    }

// ──────────────────────────────────────────────────────────────
// PromptAndWait over receipts
// ──────────────────────────────────────────────────────────────

[<Fact>]
let ``PromptAndWait observes its own receipt when another waiter lapses`` () : Task =
    task {
        let makeHarnessClient
            (harness: SessionHarness)
            (waitDelay: ILlmDelay)
            (defaultBound: TimeSpan)
            : SessionClient =
            new SessionClient(
                harness.Store,
                harness.Tenant,
                (fun _ _ -> Task.FromResult(harness.Actor)),
                new SessionEventBus(harness.Journal),
                defaultBound,
                waitDelay,
                None
            )

        let questionArgs (question: string) : IDictionary<string, obj> =
            let args = Dictionary<string, obj>()
            args["question"] <- question :> obj
            args :> IDictionary<string, obj>

        let client =
            scripted
                [
                    ScriptStep.ToolCall("q1", "ask_user", questionArgs "Which region?")
                    ScriptStep.Text "one"
                    ScriptStep.Text "two"
                ]

        use! harness = SessionHarness.CreateAsync(client, sourced [ AskUserTool.Create() ])

        let lapsed =
            makeHarnessClient harness (RecordingDelay() :> ILlmDelay) (TimeSpan.FromSeconds 5.0)

        let waiter = makeHarnessClient harness (NeverWaitDelay() :> ILlmDelay) waitBound

        // A lapses on the immediate seam while its turn is parked on the
        // question: only A's observation ends, never the turn.
        let invokeA () : Task =
            lapsed.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "a", CancellationToken.None) :> Task

        let! expired = Assert.ThrowsAsync<DeadlineExceededException>(invokeA)
        Assert.Equal("PromptAndWait", expired.OperationName)

        // B observes only its own receipt: the second step, never A's.
        let waitingB =
            waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "b", CancellationToken.None)

        let! requestId = harness.WaitForSuspensionAsync(CancellationToken.None)

        let! answer = harness.ReplyAndSettleAsync(QuestionAnswer(requestId, "east"), CancellationToken.None)
        Assert.Equal(TurnStatus.Completed, answer.Status)
        Assert.Equal("one", answer.AssistantText)

        let! resultB = awaitWhat waitingB "B's wait"
        Assert.Equal(TurnStatus.Completed, resultB.Status)
        Assert.Equal("two", resultB.AssistantText)

        // A's abandoned turn settled normally with the first step, and B's
        // queued prompt settled with the second: nothing was stolen or lost.
        Assert.Equal(2, harness.SettledResults.Count)
        Assert.Equal("one", harness.SettledResults[0].AssistantText)
        Assert.Equal("two", harness.SettledResults[1].AssistantText)
    }

[<Fact>]
let ``WaitForOperationAsync stays BCL-only`` () =
    let method =
        typeof<SessionClientOperations>.GetMethod("WaitForOperationAsync", BindingFlags.Public ||| BindingFlags.Static)
        |> Option.ofObj
        |> Option.defaultWith (fun () -> raise (InvalidOperationException("WaitForOperationAsync is missing.")))

    let extension = method.GetCustomAttribute<ExtensionAttribute>() |> Option.ofObj

    Assert.True(extension.IsSome, "WaitForOperationAsync must carry ExtensionAttribute.")
    Assert.Equal(typeof<Task<OperationResult>>, method.ReturnType)

    let parameters =
        method.GetParameters()
        |> Seq.map (fun parameter -> parameter.ParameterType)
        |> List.ofSeq

    Assert.Equal<Type list>(
        [
            typeof<SessionClient>
            typeof<AcceptedOperation>
            typeof<TimeSpan>
            typeof<CancellationToken>
        ],
        parameters
    )

    let fsharpCore = typeof<list<int>>.Assembly

    for exposed in method.ReturnType :: parameters do
        Assert.False(
            exposed.Assembly.Equals(fsharpCore),
            sprintf "The public surface leaks the F# core type %s." exposed.FullName
        )

// ──────────────────────────────────────────────────────────────
// Transient cleanup convergence (issue 384)
// ──────────────────────────────────────────────────────────────

[<Fact>]
let ``late observer after transient cleanup reads the durable receipt`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "kept" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let! receipt = awaitWhat (prompt client created.Id "hi" DeliveryMode.Queue) "the prompt to land"
                    let! winner = awaitTerminal client receipt
                    Assert.Equal("kept", (presentResult winner.Result).AssistantText)

                    // Drop every transient entry for the session: the hub
                    // (true: the settle created one) plus the client sync
                    // state (false: PromptAsync never takes the gate).
                    Assert.True(PromptWaitHubs.ReleaseSession client.Tenant created.Id)
                    Assert.False(client.ReleaseSession(created.Id))

                    // A late observer that never registered still reads the
                    // same committed winner from the durable row.
                    let! late = awaitWhat (waitOp client receipt waitBound CancellationToken.None) "the late wait"

                    Assert.Equal(OperationStatus.Terminal, late.Status)
                    Assert.Equal("kept", (presentResult late.Result).AssistantText)

                    let! read =
                        SessionClientOperations.GetOperationResultAsync(client, receipt, CancellationToken.None)

                    Assert.Equal(OperationStatus.Terminal, read.Status)
                    Assert.Equal("kept", (presentResult read.Result).AssistantText)
                })
    }

[<Fact>]
let ``simultaneous live observers during cleanup each get the committed winner`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        use provider =
            (createServices
                (scripted
                    [
                        ScriptStep.ToolCall("c1", "blocker")
                        ScriptStep.Text "shared"
                    ])
                (sourced
                    [
                        blockingTool "blocker" entered release
                    ]))
                .BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client

                        let! receipt =
                            awaitWhat (prompt client created.Id "go" DeliveryMode.Queue) "the prompt to land"

                        Assert.True(entered.Wait(TimeSpan.FromSeconds 10.0))

                        let first = waitOp client receipt waitBound CancellationToken.None
                        let second = waitOp client receipt waitBound CancellationToken.None

                        // Best-effort barrier: both waits subscribe within
                        // milliseconds, so the release below races live
                        // observers. Either way both must converge.
                        let deadline = DateTimeOffset.UtcNow.AddSeconds(5.0)

                        while PromptWaitHubs.HintObserverCount() < 2 && DateTimeOffset.UtcNow < deadline do
                            do! Task.Yield()

                        PromptWaitHubs.ReleaseSession client.Tenant created.Id |> ignore

                        release.TrySetResult("unblocked") |> ignore

                        let! both = awaitWhat (Task.WhenAll(first, second)) "both survivor waits"

                        Assert.Equal(OperationStatus.Terminal, both[0].Status)
                        Assert.Equal(OperationStatus.Terminal, both[1].Status)
                        Assert.Equal("shared", (presentResult both[0].Result).AssistantText)
                        Assert.Equal("shared", (presentResult both[1].Result).AssistantText)
                    })
        finally
            release.TrySetResult("unblocked") |> ignore
    }

[<Fact>]
let ``session synchronization reuse after release is safe`` () : Task =
    task {
        use provider =
            (createServices
                (scripted
                    [
                        ScriptStep.Text "one"
                        ScriptStep.Text "two"
                    ])
                (sourced []))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client

                    let! first =
                        awaitWhat
                            (client.PromptAndWaitAsync(created.Id, UserMessage.Text "hi", CancellationToken.None))
                            "the first wait"

                    Assert.Equal("one", first.AssistantText)
                    Assert.Equal(1, client.TrackedSyncCount)

                    Assert.True(client.ReleaseSession(created.Id))
                    Assert.Equal(0, client.TrackedSyncCount)

                    let! second =
                        awaitWhat
                            (client.PromptAndWaitAsync(created.Id, UserMessage.Text "again", CancellationToken.None))
                            "the second wait"

                    Assert.Equal("two", second.AssistantText)
                    Assert.Equal(1, client.TrackedSyncCount)
                })
    }
