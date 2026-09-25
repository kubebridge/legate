// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionClientFacadeTests

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
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
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

/// Asserts the pre-empted turn settled Aborted for the interrupt.
/// Pure so the resumable test stays a straight-line await plus a return.
let private checkPreemptedOutcome (outcome: TurnOutcome | null) =
    match outcome with
    | :? TurnAborted as aborted ->
        aborted.Cause |> should equal StopCause.ExplicitAbort
        aborted.Reason |> should equal SessionActor.InterruptReason
    | _ -> failwith "Expected the pre-empted turn to settle Aborted."

/// Asserts the aborted turn carries the explicit-abort stop cause.
/// Pure so the resumable test stays a straight-line await plus a return.
let private checkAbortedOutcome (outcome: TurnOutcome | null) =
    match outcome with
    | :? TurnAborted as outcome ->
        outcome.Cause |> should equal StopCause.ExplicitAbort
        outcome.Reason |> should equal "test-abort"
    | _ -> failwith "Expected the aborted turn to carry the stop cause."

/// Asserts the fork carried the source metadata and options over. Pure
/// so the resumable test stays a straight-line await plus a return.
let private checkForkedMetadata (options: SessionOptions) (sourceId: SessionId) =
    match Option.ofObj options.Metadata with
    | None -> failwith "Expected the fork to carry metadata."
    | Some forkedMetadata ->
        options.MaxIterations |> should equal 7
        forkedMetadata["ForkedFrom"] |> should equal (sourceId.ToString())
        forkedMetadata["lane"] |> should equal "evening"

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

                        checkPreemptedOutcome first.Outcome

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

                        checkAbortedOutcome aborted.Outcome

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

// ──────────────────────────────────────────────────────────────────────────
// Fork and SetAgent (issue 123)

/// Builds a container with the facade registered plus the agent catalog:
/// the agent store SetAgent validates the rebound agent against.
let private createServicesWithAgents
    (chatClient: ScriptedChatClient)
    (tools: StaticToolSource)
    (agents: IAgentStore)
    : IServiceCollection =
    let database = InMemoryDatabase()

    let services = ServiceCollection() :> IServiceCollection

    LegateServiceCollectionExtensions.AddLegate(
        services,
        ?configure =
            Some(fun (builder: LegateBuilder) ->
                builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore
                builder.Tools.AddSource(tools) |> ignore
                builder.Agents.UseStore(agents) |> ignore)
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    services.AddSingleton<IChatClient>(chatClient) |> ignore
    services

/// An agent definition for the catalog, enabled or not.
let private agentDefinition (id: AgentId) (enabled: bool) : Agent =
    {
        Id = id
        Tenant = TenantId.Default
        Name = "rebindable"
        Description = "Handles rebinding questions"
        Model = ModelReference.Parse "anthropic/claude-sonnet"
        SystemPrompt = "You help with rebinding."
        EnvironmentVariables = null
        PermissionDefaults = null
        ToolSelection = null
        PackageReference = null
        Enabled = enabled
        Schedule = null
        RowVersion = 0UL
        CreatedAt = DateTimeOffset.UtcNow
        UpdatedAt = DateTimeOffset.UtcNow
    }

/// Inserts the agent into the catalog, failing the test on conflict.
let private insertAgent (agents: IAgentStore) (agent: Agent) : Task =
    task {
        let! outcome = agents.UpdateIfUnchanged(TenantId.Default, agent, 0UL, CancellationToken.None)

        match outcome with
        | :? AgentUpdated -> ()
        | _ -> failwith "Expected the agent insert to apply."
    }

/// Reads the whole journal in sequence order through the facade.
let private readAllEvents (client: SessionClient) (sessionId: SessionId) : Task<SessionEvent list> =
    task {
        let collected = ResizeArray<SessionEvent>()
        let mutable cursor = 0L
        let mutable paging = true

        while paging do
            let! page = SessionClientOperations.ReadEventsAsync(client, sessionId, cursor, 100, CancellationToken.None)

            if isNull (box page) || page.Count = 0 then
                paging <- false
            else
                for event in page do
                    if not (isNull (box event)) then
                        collected.Add(event)

                        if event.Sequence.HasValue && event.Sequence.Value > cursor then
                            cursor <- event.Sequence.Value

                if page.Count < 100 then
                    paging <- false

        return List.ofSeq collected
    }

/// The agent-switch events in a journal, in sequence order.
let private switchEventsOf (events: SessionEvent list) : AgentSwitchedEvent list =
    [
        for event in events do
            match event with
            | :? AgentSwitchedEvent as switched when not (isNull (box switched)) -> yield switched
            | _ -> ()
    ]

[<Fact>]
let ``SetAgent on Idle rebinds at once through the DI actor system`` () : Task =
    task {
        // No agent catalog registered: validation is skipped and any id
        // rebinds, so this also pins the catalog-less path.
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let target = AgentId.New()

                    let! rebound =
                        SessionClientOperations.SetAgentAsync(client, created.Id, target, CancellationToken.None)

                    rebound.AgentId |> should equal target

                    let! stored = storedOf client created.Id
                    stored.AgentId |> should equal target

                    let! events = readAllEvents client created.Id
                    let switches = switchEventsOf events

                    switches.Length |> should equal 1
                    switches[0].PreviousAgentId |> should equal created.AgentId
                    switches[0].NewAgentId |> should equal target

                    let! pending = client.Store.ReadPendingInbox(client.Tenant, created.Id, CancellationToken.None)

                    pending.Count |> should equal 0
                })
    }

[<Fact>]
let ``SetAgent with an unknown agent throws AgentNotFoundException`` () : Task =
    task {
        let agents = InMemoryStoreFactory.agentStore (InMemoryDatabase())

        use provider =
            (createServicesWithAgents (scripted [ ScriptStep.Text "done" ]) (sourced []) agents).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let missing = AgentId.New()

                    let! rejected =
                        Assert.ThrowsAsync<AgentNotFoundException>(fun () ->
                            SessionClientOperations.SetAgentAsync(client, created.Id, missing, CancellationToken.None))

                    rejected.AgentId |> should equal missing

                    // Nothing rebound: the row still converses with the
                    // previous agent.
                    let! stored = storedOf client created.Id
                    stored.AgentId |> should equal created.AgentId
                })
    }

[<Fact>]
let ``SetAgent with a disabled agent throws AgentDisabledException`` () : Task =
    task {
        let agents = InMemoryStoreFactory.agentStore (InMemoryDatabase())
        let target = AgentId.New()
        do! insertAgent agents (agentDefinition target false)

        use provider =
            (createServicesWithAgents (scripted [ ScriptStep.Text "done" ]) (sourced []) agents).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client

                    let! rejected =
                        Assert.ThrowsAsync<AgentDisabledException>(fun () ->
                            SessionClientOperations.SetAgentAsync(client, created.Id, target, CancellationToken.None))

                    rejected.AgentId |> should equal target

                    let! stored = storedOf client created.Id
                    stored.AgentId |> should equal created.AgentId
                })
    }

[<Fact>]
let ``SetAgent with an enabled registered agent rebinds`` () : Task =
    task {
        let agents = InMemoryStoreFactory.agentStore (InMemoryDatabase())
        let target = AgentId.New()
        do! insertAgent agents (agentDefinition target true)

        use provider =
            (createServicesWithAgents (scripted [ ScriptStep.Text "done" ]) (sourced []) agents).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client

                    let! rebound =
                        SessionClientOperations.SetAgentAsync(client, created.Id, target, CancellationToken.None)

                    rebound.AgentId |> should equal target

                    let! stored = storedOf client created.Id
                    stored.AgentId |> should equal target
                })
    }

[<Fact>]
let ``SetAgent on closed and missing sessions throws`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! missing =
                        Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                            SessionClientOperations.SetAgentAsync(
                                client,
                                SessionId.New(),
                                AgentId.New(),
                                CancellationToken.None
                            ))

                    (isNull (box missing)) |> should equal false

                    let! created = openSession client
                    let! _ = client.Store.CloseSession(client.Tenant, created.Id, CancellationToken.None)

                    let! closed =
                        Assert.ThrowsAsync<InvalidSessionStateException>(fun () ->
                            SessionClientOperations.SetAgentAsync(
                                client,
                                created.Id,
                                AgentId.New(),
                                CancellationToken.None
                            ))

                    closed.SessionId |> should equal created.Id
                })
    }

[<Fact>]
let ``SetAgent while Running defers to the quiescent boundary without stealing`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = new TaskCompletionSource<string>()

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "gated")
                    ScriptStep.Text "first"
                    ScriptStep.Text "second"
                ]

        let tools = sourced [ blockingTool "gated" entered release ]

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

                        // The rebind records as pending: the row still
                        // converses with the previous agent while the turn
                        // runs.
                        let target = AgentId.New()

                        let! recorded =
                            SessionClientOperations.SetAgentAsync(client, created.Id, target, CancellationToken.None)

                        recorded.AgentId |> should equal created.AgentId

                        let! _ =
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "follow-up",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            )

                        let secondWaiter = settleWaiter created.Id
                        release.TrySetResult("unblocked") |> ignore

                        let! first = awaitWhat waiter.Task "the running turn to settle"
                        first.Status |> should equal TurnStatus.Completed
                        first.AssistantText |> should equal "first"

                        let! second = awaitWhat secondWaiter.Task "the queued turn to settle"
                        second.Status |> should equal TurnStatus.Completed
                        second.AssistantText |> should equal "second"

                        // Snapshot barrier: the snapshot answers after the
                        // finish handling (apply included) completed, making
                        // the following row and journal reads exact without
                        // polling.
                        let! actor = client.Resolve(created.Id, CancellationToken.None)
                        let! _ = SessionActor.getSuspendSnapshotAsync actor CancellationToken.None

                        // Both prompts ran, in order, and the empty inbox
                        // applied the recorded rebind at the boundary.
                        (settledOf created.Id).Count |> should equal 2

                        let! stored = storedOf client created.Id
                        stored.AgentId |> should equal target

                        let! events = readAllEvents client created.Id
                        let switches = switchEventsOf events

                        switches.Length |> should equal 1
                        switches[0].PreviousAgentId |> should equal created.AgentId
                        switches[0].NewAgentId |> should equal target

                        let! pending = client.Store.ReadPendingInbox(client.Tenant, created.Id, CancellationToken.None)

                        pending.Count |> should equal 0
                    })
        finally
            entered.Dispose()
    }

[<Fact>]
let ``Fork copies the prefix and references the source`` () : Task =
    task {
        // The source turn suspends on the Ask verdict and resumes: the
        // suspendable flow journals suspend and resolve events only, so the
        // suspend/resume cycle is what makes the prefix non-empty (a bare
        // text turn journals nothing).
        let invocations = ref []

        let services =
            createServices
                (scripted
                    [
                        ScriptStep.ToolCall("c1", "gated")
                        ScriptStep.Text "src"
                        ScriptStep.Text "forked"
                    ])
                (sourced [ tool "gated" "ok" invocations ])

        services.AddSingleton<IPermissionPolicy>(AskPolicy("gated")) |> ignore

        use provider = services.BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let agent = AgentId.New()
                    let options = SessionOptions()
                    options.Title <- "checkout"
                    options.MaxIterations <- 7

                    let metadata = Dictionary<string, string>(StringComparer.Ordinal)
                    metadata["lane"] <- "evening"
                    options.Metadata <- metadata :> IReadOnlyDictionary<string, string>

                    let! created =
                        SessionClientOperations.OpenSessionAsync(client, agent, options, CancellationToken.None)

                    let waiter = settleWaiter created.Id
                    let stream = collectStream client created.Id 0L 1

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "start",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! suspended = awaitWhat stream "the suspend lifecycle"

                    let asked =
                        suspended
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

                    let! first = awaitWhat waiter.Task "the source turn to settle"
                    first.Status |> should equal TurnStatus.Completed
                    first.AssistantText |> should equal "src"

                    let! sourceEvents = readAllEvents client created.Id
                    Assert.True(sourceEvents.Length > 0)
                    let prefixLength = int64 sourceEvents.Length

                    let! forked =
                        SessionClientOperations.ForkAsync(client, created.Id, prefixLength, CancellationToken.None)

                    forked.Id |> should not' (equal created.Id)
                    forked.AgentId |> should equal agent
                    forked.Title |> should equal "checkout"
                    forked.State |> should equal SessionState.Idle
                    forked.CurrentTurnId.HasValue |> should equal false

                    checkForkedMetadata forked.Options created.Id

                    let! forkedEvents = readAllEvents client forked.Id
                    forkedEvents.Length |> should equal sourceEvents.Length

                    (forkedEvents |> List.map (fun event -> event.GetType().Name))
                    |> should equal (sourceEvents |> List.map (fun event -> event.GetType().Name))

                    for event in forkedEvents do
                        event.SessionId |> should equal forked.Id

                    let! forkedPending =
                        client.Store.ReadPendingInbox(client.Tenant, forked.Id, CancellationToken.None)

                    forkedPending.Count |> should equal 0

                    // The fork is live: it prompts and settles on its own
                    // actor with the copied agent.
                    let forkWaiter = settleWaiter forked.Id

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            forked.Id,
                            UserMessage.Text "hello",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! forkedResult = awaitWhat forkWaiter.Task "the forked turn to settle"
                    forkedResult.Status |> should equal TurnStatus.Completed
                    forkedResult.AssistantText |> should equal "forked"
                })
    }

[<Fact>]
let ``Fork clamps beyond-tail and allows closed and empty prefixes`` () : Task =
    task {
        let invocations = ref []

        let services =
            createServices
                (scripted
                    [
                        ScriptStep.ToolCall("c1", "gated")
                        ScriptStep.Text "src"
                    ])
                (sourced [ tool "gated" "ok" invocations ])

        services.AddSingleton<IPermissionPolicy>(AskPolicy("gated")) |> ignore

        use provider = services.BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id
                    let stream = collectStream client created.Id 0L 1

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "start",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! suspended = awaitWhat stream "the suspend lifecycle"

                    let asked =
                        suspended
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

                    let! _ = awaitWhat waiter.Task "the source turn to settle"

                    let! sourceEvents = readAllEvents client created.Id
                    Assert.True(sourceEvents.Length > 0)

                    let! _ = client.Store.CloseSession(client.Tenant, created.Id, CancellationToken.None)

                    // A closed source forks fine, clamping past the tail to
                    // the full journal.
                    let! clamped =
                        SessionClientOperations.ForkAsync(client, created.Id, Int64.MaxValue, CancellationToken.None)

                    clamped.AgentId |> should equal created.AgentId
                    clamped.Title |> should equal created.Title

                    let! clampedEvents = readAllEvents client clamped.Id
                    clampedEvents.Length |> should equal sourceEvents.Length

                    // A cursor below the first sequence forks an empty
                    // transcript that still references its parent.
                    let! empty = SessionClientOperations.ForkAsync(client, created.Id, 0L, CancellationToken.None)

                    match Option.ofObj empty.Options.Metadata with
                    | None -> failwith "Expected the empty fork to carry metadata."
                    | Some emptyMetadata -> emptyMetadata["ForkedFrom"] |> should equal (created.Id.ToString())

                    let! emptyEvents = readAllEvents client empty.Id
                    emptyEvents.Length |> should equal 0
                })
    }

[<Fact>]
let ``Fork of a missing session throws SessionNotFoundException`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! missing =
                        Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                            SessionClientOperations.ForkAsync(client, SessionId.New(), 1L, CancellationToken.None))

                    (isNull (box missing)) |> should equal false
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// ListSessions and automatic titles (issue 124)

/// An ILogger capturing formatted lines for the no-prompt-content proof.
type private RecordingLogger() =
    let entries = ResizeArray<string * string>()

    /// The captured (level, line) pairs in call order.
    member _.Entries: IReadOnlyList<string * string> =
        entries :> IReadOnlyList<string * string>

    interface ILogger with
        member _.BeginScope<'TState when 'TState: not null>(_state: 'TState) : IDisposable =
            Unchecked.defaultof<IDisposable>

        member _.IsEnabled(_) = true

        member _.Log<'TState>
            (logLevel: LogLevel, _eventId: EventId, state: 'TState, ex: exn, formatter: Func<'TState, exn, string>)
            : unit =
            entries.Add(logLevel.ToString(), formatter.Invoke(state, ex))

/// Builds auto-title deps over the given title client for direct-client
/// and override tests.
let private titleDeps (enabled: bool) (chat: IChatClient | null) (logger: ILogger | null) : AutoTitleDeps =
    {
        Enabled = enabled
        TitleModel = null
        CompactionModel = null
        FacadeDefaultModel = null
        LlmDefaultModel = null
        FallbackModel = ModelReference.Parse "legate/default"
        ChatClient = chat
        Logger = logger
    }

/// Builds a session row with the given agent and title for direct-store
/// setup (the store stamps the tenant, state, and timestamps).
let private storedSession (agent: AgentId) (title: string) : Session =
    {
        Id = SessionId.New()
        Tenant = TenantId.Default
        AgentId = agent
        Title = title
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.UtcNow
        UpdatedAt = DateTimeOffset.UtcNow
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// Builds a SessionClient over a fresh in-memory store and journal for
/// direct auto-title tests: no actor system, since titling touches only
/// the store and the chat client.
let private directTitleSetup () : SessionClient * ISessionStore * TenantId =
    let database = InMemoryDatabase()
    let store = InMemorySessionStore(database) :> ISessionStore
    let journal = InMemorySessionEventStore(database) :> ISessionEventStore
    let bus = new SessionEventBus(journal, SessionSubscriptionOptions(), null)

    let client =
        new SessionClient(
            store,
            TenantId.Default,
            (fun (_: SessionId) (_: CancellationToken) -> Task.FromResult(Unchecked.defaultof<IActorRef>)),
            bus,
            TimeSpan.FromMinutes 1.0,
            SystemLlmDelay(TimeProvider.System) :> ILlmDelay,
            None
        )

    client, store, TenantId.Default

/// Requires the session row or fails the test: GetSession returns null
/// when the id does not exist in the tenant.
let private require (session: Session | null) =
    match session with
    | null -> failwith "The session row is missing."
    | live -> live

/// Polls the stored title until it lands or the ten-second bound lapses:
/// the fire-and-forget title write races the assertion.
let private awaitTitle (client: SessionClient) (sessionId: SessionId) : Task<string> =
    task {
        let deadline = DateTimeOffset.UtcNow.AddSeconds 10.0
        let mutable title = ""

        while title = "" && DateTimeOffset.UtcNow < deadline do
            let! found = client.Store.GetSession(client.Tenant, sessionId, CancellationToken.None)

            match found with
            | null -> do! Task.Delay(50, CancellationToken.None)
            | live when not (String.IsNullOrWhiteSpace live.Title) -> title <- live.Title
            | _ -> do! Task.Delay(50, CancellationToken.None)

        return title
    }

[<Fact>]
let ``resolveTitleModel falls back through title compaction facade default and agent default`` () =
    let chat = scripted [ ScriptStep.Text "x" ]

    let depsWith (title: string | null) (compaction: string | null) (facade: string | null) (llm: string | null) =
        { titleDeps false (chat :> IChatClient) null with
            TitleModel = title
            CompactionModel = compaction
            FacadeDefaultModel = facade
            LlmDefaultModel = llm
        }

    SessionAutoTitle.resolveTitleModel (depsWith "a/m1" "b/m2" "c/m3" "d/m4")
    |> should equal (ModelReference.Parse "a/m1")

    SessionAutoTitle.resolveTitleModel (depsWith null "b/m2" "c/m3" "d/m4")
    |> should equal (ModelReference.Parse "b/m2")

    SessionAutoTitle.resolveTitleModel (depsWith null null "c/m3" "d/m4")
    |> should equal (ModelReference.Parse "c/m3")

    SessionAutoTitle.resolveTitleModel (depsWith null null null "d/m4")
    |> should equal (ModelReference.Parse "d/m4")

    SessionAutoTitle.resolveTitleModel (depsWith null null null null)
    |> should equal (ModelReference.Parse "legate/default")

[<Fact>]
let ``normalizeTitle takes the first trimmed line within bounds`` () =
    SessionAutoTitle.normalizeTitle "  Harvest Moon  \nsecond line"
    |> should equal "Harvest Moon"

    SessionAutoTitle.normalizeTitle "\"Quoted\"" |> should equal "Quoted"
    SessionAutoTitle.normalizeTitle "   " |> should equal null
    SessionAutoTitle.normalizeTitle null |> should equal null

    let long = String.replicate 100 "a"

    match SessionAutoTitle.normalizeTitle long with
    | null -> failwith "Expected the long title to bound, not vanish."
    | bounded -> bounded.Length |> should equal SessionAutoTitle.MaxTitleLength

[<Fact>]
let ``promptTextOf joins text parts with newlines`` () =
    SessionAutoTitle.promptTextOf (UserMessage.Text "hello") |> should equal "hello"

    SessionAutoTitle.promptTextOf (Unchecked.defaultof<UserMessage>)
    |> should equal ""

    let parts =
        ResizeArray<AIContent>(
            [|
                TextContent("first") :> AIContent
                TextContent("second") :> AIContent
            |]
        )
        :> IReadOnlyList<AIContent>

    SessionAutoTitle.promptTextOf (UserMessage(parts, null))
    |> should equal ("first\nsecond")

[<Fact>]
let ``titleAsync on a disabled client makes no chat call`` () : Task =
    task {
        let client, store, tenant = directTitleSetup ()
        let chat = scripted [ ScriptStep.Text "Unused" ]
        client.AutoTitle <- Some(titleDeps false (chat :> IChatClient) null)

        let! created = store.CreateSession(tenant, storedSession (AgentId.New()) "", CancellationToken.None)

        do! SessionAutoTitle.titleAsync client created.Id (UserMessage.Text "hello") CancellationToken.None

        chat.Calls |> should equal 0

        let! stored = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require stored).Title |> should equal ""
    }

[<Fact>]
let ``titleAsync without a chat client no-ops`` () : Task =
    task {
        let client, store, tenant = directTitleSetup ()
        client.AutoTitle <- Some(titleDeps true null null)

        let! created = store.CreateSession(tenant, storedSession (AgentId.New()) "", CancellationToken.None)

        do! SessionAutoTitle.titleAsync client created.Id (UserMessage.Text "hello") CancellationToken.None

        let! stored = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require stored).Title |> should equal ""
    }

[<Fact>]
let ``titleAsync on a titled session makes no chat call`` () : Task =
    task {
        let client, store, tenant = directTitleSetup ()
        let chat = scripted [ ScriptStep.Text "Unused" ]
        client.AutoTitle <- Some(titleDeps true (chat :> IChatClient) null)

        let! created = store.CreateSession(tenant, storedSession (AgentId.New()) "Kept", CancellationToken.None)

        do! SessionAutoTitle.titleAsync client created.Id (UserMessage.Text "hello") CancellationToken.None

        chat.Calls |> should equal 0

        let! stored = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require stored).Title |> should equal "Kept"
    }

[<Fact>]
let ``titleAsync on textless and missing sessions makes no chat call`` () : Task =
    task {
        let client, store, tenant = directTitleSetup ()
        let chat = scripted [ ScriptStep.Text "Unused" ]
        client.AutoTitle <- Some(titleDeps true (chat :> IChatClient) null)

        let! created = store.CreateSession(tenant, storedSession (AgentId.New()) "", CancellationToken.None)

        let fileParts =
            ResizeArray<AIContent>(
                [|
                    UserMessage.WithFile(ReadOnlyMemory [| 1uy |], "application/pdf").Parts[0]
                |]
            )
            :> IReadOnlyList<AIContent>

        do! SessionAutoTitle.titleAsync client created.Id (UserMessage(fileParts, null)) CancellationToken.None

        do! SessionAutoTitle.titleAsync client (SessionId.New()) (UserMessage.Text "hello") CancellationToken.None

        do! SessionAutoTitle.titleAsync client created.Id (Unchecked.defaultof<UserMessage>) CancellationToken.None

        chat.Calls |> should equal 0
    }

[<Fact>]
let ``titleAsync writes the normalized title through the store`` () : Task =
    task {
        let client, store, tenant = directTitleSetup ()

        let chat =
            scripted
                [
                    ScriptStep.Text "  Harvest Moon  \nignored second line"
                ]

        client.AutoTitle <- Some(titleDeps true (chat :> IChatClient) null)

        let! created = store.CreateSession(tenant, storedSession (AgentId.New()) "", CancellationToken.None)

        do!
            SessionAutoTitle.titleAsync
                client
                created.Id
                (UserMessage.Text "a quiet farming game")
                CancellationToken.None

        chat.Calls |> should equal 1

        // The title call carries the instruction plus the first prompt.
        chat.ReceivedMessages.Count |> should equal 2
        chat.ReceivedMessages[0].Role |> should equal ChatRole.System
        chat.ReceivedMessages[1].Role |> should equal ChatRole.User

        chat.ReceivedMessages[1].Text.Contains("a quiet farming game", StringComparison.Ordinal)
        |> should equal true

        let! stored = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require stored).Title |> should equal "Harvest Moon"
    }

[<Fact>]
let ``titleAsync failure leaves the title empty and retries on the next prompt`` () : Task =
    task {
        let client, store, tenant = directTitleSetup ()

        let chat =
            scripted
                [
                    ScriptStep.Failure(Exception "boom")
                    ScriptStep.Text "Second Try"
                ]

        client.AutoTitle <- Some(titleDeps true (chat :> IChatClient) null)

        let! created = store.CreateSession(tenant, storedSession (AgentId.New()) "", CancellationToken.None)

        // A provider failure never throws and writes nothing.
        do! SessionAutoTitle.titleAsync client created.Id (UserMessage.Text "hello") CancellationToken.None

        let! afterFailure = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require afterFailure).Title |> should equal ""

        // The failed flight clears, so the next prompt retries and lands.
        do! SessionAutoTitle.titleAsync client created.Id (UserMessage.Text "hello") CancellationToken.None

        let! stored = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require stored).Title |> should equal "Second Try"
    }

[<Fact>]
let ``titleAsync fires once under concurrent prompts`` () : Task =
    task {
        let client, store, tenant = directTitleSetup ()
        let chat = scripted [ ScriptStep.Text "Only Once" ]
        client.AutoTitle <- Some(titleDeps true (chat :> IChatClient) null)

        let! created = store.CreateSession(tenant, storedSession (AgentId.New()) "", CancellationToken.None)

        let! _ =
            Task.WhenAll(
                [|
                    SessionAutoTitle.titleAsync client created.Id (UserMessage.Text "hello") CancellationToken.None
                    SessionAutoTitle.titleAsync client created.Id (UserMessage.Text "hello") CancellationToken.None
                |]
            )

        chat.Calls |> should equal 1

        let! stored = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require stored).Title |> should equal "Only Once"
    }

[<Fact>]
let ``titleAsync never logs prompt content`` () : Task =
    task {
        let client, store, tenant = directTitleSetup ()
        let chat = scripted [ ScriptStep.Text "Logged Title" ]
        let logger = RecordingLogger()
        client.AutoTitle <- Some(titleDeps true (chat :> IChatClient) (logger :> ILogger))

        let! created = store.CreateSession(tenant, storedSession (AgentId.New()) "", CancellationToken.None)

        do!
            SessionAutoTitle.titleAsync
                client
                created.Id
                (UserMessage.Text "secret phrase alpha bravo 12345")
                CancellationToken.None

        let! stored = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require stored).Title |> should equal "Logged Title"

        (logger.Entries.Count = 0) |> should equal false

        for _, line in logger.Entries do
            line.Contains("secret phrase alpha bravo 12345", StringComparison.Ordinal)
            |> should equal false
    }

[<Fact>]
let ``ListSessionsAsync pages the default size newest walk`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    for _ in 1..55 do
                        let! _ =
                            client.Store.CreateSession(
                                client.Tenant,
                                storedSession (AgentId.New()) "",
                                CancellationToken.None
                            )

                        ()

                    // Null options read as the defaults: 50 per page.
                    let! first = SessionClientListingOperations.ListSessionsAsync(client, null, CancellationToken.None)

                    first.Items.Count |> should equal 50
                    (isNull (box first.Continuation)) |> should equal false

                    let seen = ResizeArray<SessionId>()
                    let mutable continuation: string | null = first.Continuation
                    let mutable more = true

                    for item in first.Items do
                        seen.Add(item.Id)

                    while more do
                        let options = SessionListOptions()
                        options.Continuation <- continuation

                        let! page =
                            SessionClientListingOperations.ListSessionsAsync(client, options, CancellationToken.None)

                        for item in page.Items do
                            seen.Add(item.Id)

                        continuation <- page.Continuation
                        more <- not (isNull (box page.Continuation))

                    seen.Count |> should equal 55
                    seen |> Seq.distinct |> Seq.length |> should equal 55
                })
    }

[<Fact>]
let ``ListSessionsAsync clamps oversized pages to 200`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    for _ in 1..205 do
                        let! _ =
                            client.Store.CreateSession(
                                client.Tenant,
                                storedSession (AgentId.New()) "",
                                CancellationToken.None
                            )

                        ()

                    let options = SessionListOptions()
                    options.PageSize <- 500

                    let! first =
                        SessionClientListingOperations.ListSessionsAsync(client, options, CancellationToken.None)

                    first.Items.Count |> should equal 200
                    (isNull (box first.Continuation)) |> should equal false

                    let seen = ResizeArray<SessionId>()

                    for item in first.Items do
                        seen.Add(item.Id)

                    let follow = SessionListOptions()
                    follow.Continuation <- first.Continuation

                    let! rest =
                        SessionClientListingOperations.ListSessionsAsync(client, follow, CancellationToken.None)

                    for item in rest.Items do
                        seen.Add(item.Id)

                    seen.Count |> should equal 205
                    seen |> Seq.distinct |> Seq.length |> should equal 205
                    (isNull (box rest.Continuation)) |> should equal true
                })
    }

[<Fact>]
let ``ListSessionsAsync honors state agent and created filters`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let target = AgentId.New()
                    let now = DateTimeOffset.UtcNow

                    let! tagged =
                        client.Store.CreateSession(client.Tenant, storedSession target "", CancellationToken.None)

                    let! _ =
                        client.Store.CreateSession(
                            client.Tenant,
                            storedSession (AgentId.New()) "",
                            CancellationToken.None
                        )

                    let! _ =
                        client.Store.UpdateSessionState(
                            client.Tenant,
                            tagged.Id,
                            SessionState.Running,
                            CancellationToken.None
                        )

                    let byState = SessionListOptions()
                    byState.State <- Nullable SessionState.Running

                    let! running =
                        SessionClientListingOperations.ListSessionsAsync(client, byState, CancellationToken.None)

                    running.Items.Count |> should equal 1
                    running.Items[0].Id |> should equal tagged.Id

                    let byAgent = SessionListOptions()
                    byAgent.AgentId <- Nullable target

                    let! agentOnly =
                        SessionClientListingOperations.ListSessionsAsync(client, byAgent, CancellationToken.None)

                    agentOnly.Items.Count |> should equal 1
                    agentOnly.Items[0].Id |> should equal tagged.Id

                    let future = SessionListOptions()
                    future.CreatedFrom <- Nullable(now.AddHours 1.0)

                    let! noneFuture =
                        SessionClientListingOperations.ListSessionsAsync(client, future, CancellationToken.None)

                    noneFuture.Items.Count |> should equal 0

                    let past = SessionListOptions()
                    past.CreatedTo <- Nullable(now.AddHours -1.0)

                    let! nonePast =
                        SessionClientListingOperations.ListSessionsAsync(client, past, CancellationToken.None)

                    nonePast.Items.Count |> should equal 0

                    let wide = SessionListOptions()
                    wide.CreatedFrom <- Nullable(now.AddHours -1.0)
                    wide.CreatedTo <- Nullable(now.AddHours 1.0)

                    let! both = SessionClientListingOperations.ListSessionsAsync(client, wide, CancellationToken.None)

                    both.Items.Count |> should equal 2
                })
    }

[<Fact>]
let ``ListSessionsAsync rejects a non-positive page size`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let options = SessionListOptions()
                    options.PageSize <- 0

                    let! _ =
                        Assert.ThrowsAsync<ArgumentOutOfRangeException>(fun () ->
                            SessionClientListingOperations.ListSessionsAsync(client, options, CancellationToken.None))

                    return ()
                })
    }

[<Fact>]
let ``buildClient wires auto-title from Sessions options`` () =
    let services = createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])

    services.Configure<LegateOptions>(
        Action<LegateOptions>(fun options ->
            options.Sessions.AutoTitle <- true
            options.Sessions.AutoTitleModel <- "anthropic/claude-sonnet")
    )
    |> ignore

    use provider = services.BuildServiceProvider()
    let client = provider.GetRequiredService<SessionClient>()

    match client.AutoTitle with
    | None -> failwith "Expected the container to wire auto-title deps."
    | Some deps ->
        deps.Enabled |> should equal true

        match deps.TitleModel with
        | null -> failwith "Expected the title model to carry through."
        | model -> model |> should equal "anthropic/claude-sonnet"

        SessionAutoTitle.resolveTitleModel deps
        |> should equal (ModelReference.Parse "anthropic/claude-sonnet")

        (isNull (box deps.ChatClient)) |> should equal false

[<Fact>]
let ``PromptAsync titles the untitled session without failing the prompt`` () : Task =
    task {
        let titleClient = scripted [ ScriptStep.Text "Pumpkin Soup" ]

        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    client.AutoTitle <- Some(titleDeps true (titleClient :> IChatClient) null)

                    let! created = openSession client

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "tell me a bedtime story",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! title = awaitTitle client created.Id
                    title |> should equal "Pumpkin Soup"
                })
    }

[<Fact>]
let ``PromptAndWaitAsync titles the untitled session and still settles`` () : Task =
    task {
        let titleClient = scripted [ ScriptStep.Text "Pumpkin Soup" ]

        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    client.AutoTitle <- Some(titleDeps true (titleClient :> IChatClient) null)

                    let! created = openSession client

                    let! result =
                        awaitWhat
                            (SessionClientExtensions.PromptAndWaitAsync(
                                client,
                                created.Id,
                                UserMessage.Text "tell me a bedtime story",
                                CancellationToken.None
                            ))
                            "the PromptAndWait settle"

                    result.Status |> should equal TurnStatus.Completed

                    let! title = awaitTitle client created.Id
                    title |> should equal "Pumpkin Soup"
                })
    }

[<Fact>]
let ``PromptAsync on a missing session throws and fires no title`` () : Task =
    task {
        let titleClient = scripted [ ScriptStep.Text "Unused" ]

        use provider =
            (createServices (scripted [ ScriptStep.Text "done" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    client.AutoTitle <- Some(titleDeps true (titleClient :> IChatClient) null)

                    try
                        let! _ =
                            SessionClientOperations.PromptAsync(
                                client,
                                SessionId.New(),
                                UserMessage.Text "hi",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            )

                        failwith "expected SessionNotFoundException"
                    with :? SessionNotFoundException ->
                        ()

                    titleClient.Calls |> should equal 0
                })
    }
