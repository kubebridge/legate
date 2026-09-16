// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionClientFacadeTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Xunit

// Session client facade (issue 96): the DI-registered SessionClient over
// the suspendable router wiring, driven through the public
// SessionClientOperations. Every test owns its container, stores, actor
// system, and scripted transports: no live keys, no external services.
// Waits are event-driven with a ten-second bound, never sleeps.

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

let private scripted (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

let private sourced (tools: AITool list) : StaticToolSource =
    new StaticToolSource(ResizeArray<AITool>(tools) :> IReadOnlyList<AITool>)

let private tool (name: string) (result: string) (invocations: string list ref) : AITool =
    let method =
        Func<string>(fun () ->
            invocations.Value <- invocations.Value @ [ name ]
            result)

    AIFunctionFactory.Create(method, name, Unchecked.defaultof<string>, Unchecked.defaultof<JsonSerializerOptions>)
    :> AITool

/// A tool that signals entry, then blocks until released: keeps a turn
/// Running so Inject/Abort/Compact meet it mid-flight. Async-gated, so the
/// actor thread stays free to answer while the turn parks: a sync block
/// would pin the actor thread through the synchronous turn invocation.
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

/// Permission policy asking once for the gated tool, allowing the rest.
type private AskPolicy(gated: string) =
    interface IPermissionPolicy with
        member _.Evaluate(request) =
            if request.ToolName = gated then
                PermissionVerdict.Ask
            else
                PermissionVerdict.Allow

/// Builds a container with the facade registered: in-memory stores over
/// one database, the scripted chat client opted in, and the static tool
/// source. The caller registers anything else (policies, options) on the
/// returned collection before building the provider.
let private createServices (chatClient: ScriptedChatClient) (tools: StaticToolSource) : IServiceCollection =
    let database = InMemoryDatabase()

    let services = ServiceCollection() :> IServiceCollection

    LegateServiceCollectionExtensions.AddLegate(
        services,
        ?configure =
            Some(fun (builder: LegateBuilder) ->
                builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore
                builder.Tools.AddSource(tools) |> ignore)
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    services.AddSingleton<IChatClient>(chatClient) |> ignore
    services

let private actorServiceOf (provider: IServiceProvider) : LocalActorSystemService =
    provider.GetServices<IHostedService>()
    |> Seq.pick (fun service ->
        match service with
        | :? LocalActorSystemService as local -> Some local
        | _ -> None)

/// Stops the local actor system, swallowing teardown noise.
let private stopQuietly (service: LocalActorSystemService) : Task =
    task {
        try
            do! (service :> IHostedService).StopAsync(CancellationToken.None)
        with _ ->
            ()
    }

/// Resolves the client (triggering the router wiring), starts the local
/// actor system, runs the work, then stops the system.
let private withClient (provider: IServiceProvider) (work: SessionClient -> Task<'T>) : Task<'T> =
    task {
        let client = provider.GetRequiredService<SessionClient>()
        let service = actorServiceOf provider
        do! (service :> IHostedService).StartAsync(CancellationToken.None)

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

let private settleWaiter (sessionId: SessionId) : TaskCompletionSource<TurnResult> =
    PromptWaitHubs.GetOrAdd(sessionId).EnqueueSettle()

let private settledOf (sessionId: SessionId) : IReadOnlyList<TurnResult> =
    PromptWaitHubs.GetOrAdd(sessionId).Settled

/// Reads the stored session row. Tests only read rows they created, so a
/// missing row is a test bug.
let private storedOf (client: SessionClient) (sessionId: SessionId) : Task<Session> =
    task {
        let! found = client.Store.GetSession(client.Tenant, sessionId, CancellationToken.None)

        match found with
        | null -> return raise (InvalidOperationException("Expected the session row to exist."))
        | session -> return session
    }

let private collectStream
    (client: SessionClient)
    (sessionId: SessionId)
    (fromSequence: int64)
    (take: int)
    : Task<SessionEvent list> =
    task {
        let collected = ResizeArray<SessionEvent>()

        let stream =
            SessionClientOperations.Subscribe(client, sessionId, fromSequence, CancellationToken.None)

        let enumerator = stream.GetAsyncEnumerator(CancellationToken.None)

        try
            let mutable remaining = take

            while remaining > 0 do
                let! has = awaitWhat (enumerator.MoveNextAsync().AsTask()) "the subscribed event"
                Assert.True(has)
                collected.Add(enumerator.Current)
                remaining <- remaining - 1

            return List.ofSeq collected
        finally
            enumerator.DisposeAsync().AsTask() |> ignore
    }

// ──────────────────────────────────────────────────────────────────────────
// Open

[<Fact>]
let ``Open creates an Idle session row with the agent and title`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let agent = AgentId.New()
                    let options = SessionOptions()
                    options.Title <- "cli"

                    let! created =
                        SessionClientOperations.OpenSessionAsync(client, agent, options, CancellationToken.None)

                    created.State |> should equal SessionState.Idle
                    created.AgentId |> should equal agent
                    created.Title |> should equal "cli"
                    created.Tenant |> should equal TenantId.Default

                    let! stored = storedOf client created.Id
                    stored.Id |> should equal created.Id
                })
    }

[<Fact>]
let ``Open without options takes interactive defaults`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    created.State |> should equal SessionState.Idle
                    created.Title |> should equal ""
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Prompt

[<Fact>]
let ``Prompt queues and settles through the DI runner`` () : Task =
    task {
        let chat = scripted [ ScriptStep.Text "hello back" ]

        use provider = (createServices chat (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! entry =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "hello",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    entry.Delivery |> should equal DeliveryMode.Queue
                    entry.SessionId |> should equal created.Id

                    let! result = awaitWhat waiter.Task "the turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                    result.AssistantText |> should equal "hello back"
                })
    }

[<Fact>]
let ``Prompt with Inject folds into the running turn`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "done"
                ]

        let tools =
            sourced
                [
                    blockingTool "blocker" entered release
                ]

        use provider = (createServices chat tools).BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client
                        let waiter = settleWaiter created.Id

                        let prompt =
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "start",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            )

                        let! _ = awaitWhat prompt "the prompt to land"
                        Assert.True(entered.Wait(waitBound))

                        let! injected =
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "steer this",
                                DeliveryMode.Inject,
                                CancellationToken.None
                            )

                        injected.Delivery |> should equal DeliveryMode.Inject

                        release.TrySetResult("unblocked") |> ignore
                        let! result = awaitWhat waiter.Task "the turn to settle"
                        result.Status |> should equal TurnStatus.Completed

                        // The fold lands in the second provider call, never
                        // the first: the first history predates the inject.
                        let received = chat.ReceivedMessages
                        Assert.True(received.Count >= 2)
                        (received[0].Text.Contains("steer this")) |> should equal false

                        received
                        |> Seq.exists (fun message ->
                            message.Role = ChatRole.User && message.Text.Contains("steer this"))
                        |> should equal true

                        let! pending = client.Store.ReadPendingInbox(client.Tenant, created.Id, CancellationToken.None)

                        pending
                        |> Seq.exists (fun entry -> entry.Position = injected.Position)
                        |> should equal false
                    })
        finally
            entered.Dispose()
    }

[<Fact>]
let ``Prompt with Interrupt pre-empts and drains first`` () : Task =
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

        let tools =
            sourced
                [
                    blockingTool "blocker" entered release
                ]

        use provider = (createServices chat tools).BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client
                        let waiter = settleWaiter created.Id

                        let prompt =
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "start",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            )

                        let! _ = awaitWhat prompt "the prompt to land"
                        Assert.True(entered.Wait(waitBound))

                        let! interrupted =
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "stop that",
                                DeliveryMode.Interrupt,
                                CancellationToken.None
                            )

                        interrupted.Delivery |> should equal DeliveryMode.Interrupt

                        // Queue the second waiter before releasing the blocker:
                        // the hub is FIFO with no replay, so a waiter enqueued
                        // after the interrupt turn settles would hang to its
                        // bound (see WaitForSettleAsync docs).
                        let secondWaiter = settleWaiter created.Id
                        release.TrySetResult("unblocked") |> ignore
                        let! first = awaitWhat waiter.Task "the pre-empted turn to settle"
                        first.Status |> should equal TurnStatus.Aborted

                        match first.Outcome with
                        | :? TurnAborted as aborted ->
                            aborted.Cause |> should equal StopCause.ExplicitAbort
                            aborted.Reason |> should equal SessionActor.InterruptReason
                        | _ -> failwith "Expected the pre-empted turn to settle Aborted."

                        let! second = awaitWhat secondWaiter.Task "the interrupt turn to settle"
                        second.Status |> should equal TurnStatus.Completed
                        second.AssistantText |> should equal "second"

                        (settledOf created.Id).Count |> should equal 2
                    })
        finally
            entered.Dispose()
    }

[<Fact>]
let ``Prompt on unknown and closed sessions throws`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! unknown =
                        Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                            SessionClientOperations.PromptAsync(
                                client,
                                SessionId.New(),
                                UserMessage.Text "hello",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))

                    (isNull (box unknown)) |> should equal false

                    let! created = openSession client

                    let! _ = client.Store.CloseSession(client.Tenant, created.Id, CancellationToken.None)

                    let! closed =
                        Assert.ThrowsAsync<InvalidSessionStateException>(fun () ->
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "hello",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))

                    (isNull (box closed)) |> should equal false
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Reply and Subscribe

[<Fact>]
let ``Reply resumes a suspended turn and Subscribe streams the lifecycle`` () : Task =
    task {
        let invocations = ref []

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "gated")
                    ScriptStep.Text "resumed"
                ]

        let tools = sourced [ tool "gated" "ok" invocations ]

        let services = createServices chat tools
        services.AddSingleton<IPermissionPolicy>(AskPolicy("gated")) |> ignore

        use provider = services.BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id
                    let stream = collectStream client created.Id 0L 1

                    let prompt =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "run it",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! _ = awaitWhat prompt "the prompt to land"
                    let! events = awaitWhat stream "the suspend lifecycle"

                    let asked =
                        events
                        |> List.pick (fun event ->
                            match event with
                            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) -> Some asked
                            | _ -> None)

                    asked.ToolName |> should equal "gated"

                    let! replyEntry =
                        SessionClientOperations.ReplyAsync(
                            client,
                            created.Id,
                            PermissionDecision(asked.RequestId, PermissionDecisionKind.AllowOnce),
                            CancellationToken.None
                        )

                    (isNull (box replyEntry)) |> should equal false

                    let! result = awaitWhat waiter.Task "the resumed turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                    result.AssistantText |> should equal "resumed"
                    invocations.Value |> should equal [ "gated" ]

                    // The resolve streams live after the replay cursor.
                    let! live = collectStream client created.Id asked.Sequence.Value 1

                    live
                    |> List.exists (fun event -> event :? PermissionResolvedEvent)
                    |> should equal true
                })
    }

[<Fact>]
let ``Reply with an unknown request id throws ReplyMismatch`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client

                    let! error =
                        Assert.ThrowsAsync<ReplyMismatchException>(fun () ->
                            SessionClientOperations.ReplyAsync(
                                client,
                                created.Id,
                                PermissionDecision("no-such-request", PermissionDecisionKind.AllowOnce),
                                CancellationToken.None
                            ))

                    (isNull (box error)) |> should equal false
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Abort

[<Fact>]
let ``Abort on Idle is a no-op`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client

                    do!
                        SessionClientOperations.AbortAsync(
                            client,
                            created.Id,
                            StopCause.ExplicitAbort,
                            "nothing runs",
                            CancellationToken.None
                        )

                    let! stored = storedOf client created.Id
                    stored.State |> should equal SessionState.Idle
                    (settledOf created.Id).Count |> should equal 0
                })
    }

[<Fact>]
let ``Abort while Running settles Aborted with zero loser effects`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "done"
                ]

        let tools =
            sourced
                [
                    blockingTool "blocker" entered release
                ]

        use provider = (createServices chat tools).BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client
                        let waiter = settleWaiter created.Id

                        let prompt =
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "start",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            )

                        let! _ = awaitWhat prompt "the prompt to land"
                        Assert.True(entered.Wait(waitBound))

                        do!
                            SessionClientOperations.AbortAsync(
                                client,
                                created.Id,
                                StopCause.ExplicitAbort,
                                "test-abort",
                                CancellationToken.None
                            )

                        // The loser reports back after the release, and the
                        // recorded stop maps its finish to Aborted.
                        release.TrySetResult("unblocked") |> ignore

                        let! aborted = awaitWhat waiter.Task "the aborted turn to settle"
                        aborted.Status |> should equal TurnStatus.Aborted

                        match aborted.Outcome with
                        | :? TurnAborted as outcome ->
                            outcome.Cause |> should equal StopCause.ExplicitAbort
                            outcome.Reason |> should equal "test-abort"
                        | _ -> failwith "Expected the aborted turn to carry the stop cause."

                        // The settle already won: the loser's late report
                        // takes zero further effects.
                        let! actor = awaitWhat (client.Resolve(created.Id, CancellationToken.None)) "the resolve"

                        let! snapshot = SessionActor.getSuspendSnapshotAsync actor CancellationToken.None

                        snapshot.State |> should equal SessionState.Idle
                        (settledOf created.Id).Count |> should equal 1

                        let! pending = client.Store.ReadPendingInbox(client.Tenant, created.Id, CancellationToken.None)

                        pending.Count |> should equal 0
                    })
        finally
            entered.Dispose()
    }

[<Fact>]
let ``Abort while WaitingForInput is a no-op and Reply still resumes`` () : Task =
    task {
        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "gated")
                    ScriptStep.Text "resumed"
                ]

        let tools = sourced [ tool "gated" "ok" (ref []) ]

        let services = createServices chat tools
        services.AddSingleton<IPermissionPolicy>(AskPolicy("gated")) |> ignore

        use provider = services.BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let prompt =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "run it",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! _ = awaitWhat prompt "the prompt to land"
                    let! events = awaitWhat (collectStream client created.Id 0L 1) "the suspension"

                    let asked =
                        events
                        |> List.pick (fun event ->
                            match event with
                            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) -> Some asked
                            | _ -> None)

                    do!
                        SessionClientOperations.AbortAsync(
                            client,
                            created.Id,
                            StopCause.ExplicitAbort,
                            "suspended",
                            CancellationToken.None
                        )

                    let! stored = storedOf client created.Id
                    stored.State |> should equal SessionState.WaitingForInput

                    let! _ =
                        SessionClientOperations.ReplyAsync(
                            client,
                            created.Id,
                            PermissionDecision(asked.RequestId, PermissionDecisionKind.AllowOnce),
                            CancellationToken.None
                        )

                    let! result = awaitWhat waiter.Task "the resumed turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                })
    }

[<Fact>]
let ``Abort validates the session and the cause`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! missing =
                        Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                            SessionClientOperations.AbortAsync(
                                client,
                                SessionId.New(),
                                StopCause.ExplicitAbort,
                                "gone",
                                CancellationToken.None
                            ))

                    (isNull (box missing)) |> should equal false

                    let! created = openSession client

                    let! cause =
                        Assert.ThrowsAsync<ArgumentOutOfRangeException>(fun () ->
                            SessionClientOperations.AbortAsync(
                                client,
                                created.Id,
                                StopCause.Deadline,
                                "not abort-family",
                                CancellationToken.None
                            ))

                    (isNull (box cause)) |> should equal false
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Compact

[<Fact>]
let ``Compact on Idle answers NotNeeded under threshold`` () : Task =
    task {
        let chat = scripted [ ScriptStep.Text "done" ]

        use provider = (createServices chat (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "hello",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! _ = awaitWhat waiter.Task "the turn to settle"

                    let! outcome = SessionClientOperations.CompactAsync(client, created.Id, CancellationToken.None)

                    (outcome :? SessionCompactNotNeeded) |> should equal true
                })
    }

[<Fact>]
let ``Compact while Running defers`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "done"
                ]

        let tools =
            sourced
                [
                    blockingTool "blocker" entered release
                ]

        use provider = (createServices chat tools).BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client

                        let prompt =
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "start",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            )

                        let! _ = awaitWhat prompt "the prompt to land"
                        Assert.True(entered.Wait(waitBound))

                        let! outcome = SessionClientOperations.CompactAsync(client, created.Id, CancellationToken.None)

                        (outcome :? SessionCompactDeferred) |> should equal true

                        release.TrySetResult("unblocked") |> ignore
                    })
        finally
            entered.Dispose()
    }

[<Fact>]
let ``Compact on unknown and closed sessions throws`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! missing =
                        Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                            SessionClientOperations.CompactAsync(client, SessionId.New(), CancellationToken.None))

                    (isNull (box missing)) |> should equal false

                    let! created = openSession client

                    let! _ = client.Store.CloseSession(client.Tenant, created.Id, CancellationToken.None)

                    let! closed =
                        Assert.ThrowsAsync<InvalidSessionStateException>(fun () ->
                            SessionClientOperations.CompactAsync(client, created.Id, CancellationToken.None))

                    (isNull (box closed)) |> should equal false
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// WaitForSettle

[<Fact>]
let ``WaitForSettle returns the settled result when queued before the prompt`` () : Task =
    task {
        let chat = scripted [ ScriptStep.Text "waited" ]

        use provider = (createServices chat (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client

                    let wait =
                        SessionClientOperations.WaitForSettleAsync(
                            client,
                            created.Id,
                            TimeSpan.FromSeconds 30.0,
                            CancellationToken.None
                        )

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "hello",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! result = awaitWhat wait "the settle wait"
                    result.Status |> should equal TurnStatus.Completed
                    result.AssistantText |> should equal "waited"
                })
    }

[<Fact>]
let ``WaitForSettle throws past its bound with the turn left running`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "done"
                ]

        let tools =
            sourced
                [
                    blockingTool "blocker" entered release
                ]

        use provider = (createServices chat tools).BuildServiceProvider()

        try
            return!
                withClient provider (fun client ->
                    task {
                        let! created = openSession client

                        let wait =
                            SessionClientOperations.WaitForSettleAsync(
                                client,
                                created.Id,
                                TimeSpan.FromMilliseconds 250.0,
                                CancellationToken.None
                            )

                        let prompt =
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "start",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            )

                        let! _ = awaitWhat prompt "the prompt to land"
                        Assert.True(entered.Wait(waitBound))

                        // A second waiter queued before the release observes
                        // the turn the bound left running.
                        let waiter = settleWaiter created.Id

                        let! bound = Assert.ThrowsAsync<DeadlineExceededException>(fun () -> wait)

                        (isNull (box bound)) |> should equal false
                        (settledOf created.Id).Count |> should equal 0

                        release.TrySetResult("unblocked") |> ignore

                        let! result = awaitWhat waiter.Task "the turn to settle after the bound"
                        result.Status |> should equal TurnStatus.Completed
                    })
        finally
            entered.Dispose()
    }

// ──────────────────────────────────────────────────────────────────────────
// Reads

[<Fact>]
let ``ReadEvents pages by cursor and limit`` () : Task =
    task {
        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "gated")
                    ScriptStep.Text "resumed"
                ]

        let tools = sourced [ tool "gated" "ok" (ref []) ]

        let services = createServices chat tools
        services.AddSingleton<IPermissionPolicy>(AskPolicy("gated")) |> ignore

        use provider = services.BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "run it",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! events = awaitWhat (collectStream client created.Id 0L 1) "the suspension"

                    let asked =
                        events
                        |> List.pick (fun event ->
                            match event with
                            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) -> Some asked
                            | _ -> None)

                    let! _ =
                        SessionClientOperations.ReplyAsync(
                            client,
                            created.Id,
                            PermissionDecision(asked.RequestId, PermissionDecisionKind.AllowOnce),
                            CancellationToken.None
                        )

                    let! _ = awaitWhat waiter.Task "the resumed turn to settle"

                    let! first =
                        SessionClientOperations.ReadEventsAsync(client, created.Id, 0L, 1, CancellationToken.None)

                    first.Count |> should equal 1

                    let! rest =
                        SessionClientOperations.ReadEventsAsync(
                            client,
                            created.Id,
                            first[0].Sequence.Value,
                            100,
                            CancellationToken.None
                        )

                    (rest.Count >= 1) |> should equal true

                    rest
                    |> Seq.forall (fun event -> event.Sequence.Value > first[0].Sequence.Value)
                    |> should equal true

                    let! missing =
                        Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                            SessionClientOperations.ReadEventsAsync(
                                client,
                                SessionId.New(),
                                0L,
                                10,
                                CancellationToken.None
                            ))

                    (isNull (box missing)) |> should equal false
                })
    }

[<Fact>]
let ``ReadTranscript returns the session cells`` () : Task =
    task {
        let chat = scripted [ ScriptStep.Text "done" ]

        use provider = (createServices chat (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client

                    let! empty =
                        SessionClientOperations.ReadTranscriptAsync(client, created.Id, CancellationToken.None)

                    (isNull (box empty)) |> should equal false
                    empty.Count |> should equal 0

                    let waiter = settleWaiter created.Id

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "hello",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! _ = awaitWhat waiter.Task "the turn to settle"

                    let! cells =
                        SessionClientOperations.ReadTranscriptAsync(client, created.Id, CancellationToken.None)

                    (isNull (box cells)) |> should equal false
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// DI registration

[<Fact>]
let ``DI registers the client bus and suspend wiring`` () =
    let chat = scripted [ ScriptStep.Text "done" ]
    use provider = (createServices chat (sourced [])).BuildServiceProvider()

    let client = provider.GetRequiredService<SessionClient>()
    (isNull (box client)) |> should equal false

    let bus = provider.GetRequiredService<SessionEventBus>()
    (isNull (box bus)) |> should equal false
    (bus.EventStore :? InMemorySessionEventStore) |> should equal true

    let service = actorServiceOf provider
    (service.SessionChildFactory.IsSome) |> should equal true

[<Fact>]
let ``Without a chat client the router keeps identity children`` () =
    let database = InMemoryDatabase()
    let services = ServiceCollection() :> IServiceCollection

    LegateServiceCollectionExtensions.AddLegate(
        services,
        ?configure =
            Some(fun (builder: LegateBuilder) ->
                builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore)
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    use provider = services.BuildServiceProvider()

    let client = provider.GetRequiredService<SessionClient>()
    (isNull (box client)) |> should equal false

    let service = actorServiceOf provider
    (service.SessionChildFactory.IsNone) |> should equal true

[<Fact>]
let ``Invalid facade options fail client resolution`` () =
    let services = createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])

    let options = SessionClientOptions()
    options.ClaimOwner <- "  "
    services.AddSingleton<SessionClientOptions>(options) |> ignore

    use provider = services.BuildServiceProvider()

    (fun () -> provider.GetRequiredService<SessionClient>() |> ignore)
    |> should throw typeof<InvalidOperationException>
