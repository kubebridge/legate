// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ProductionCompactionTests

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.IO
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
open Microsoft.Extensions.Options
open Xunit

// Live automatic and requested compaction on production model input (issue
// 386): the force-aware compaction boundary wired into the DI facade path,
// so the actual next model call consumes compacted context. Every fact
// below drives the public SessionClientOperations facade and asserts actual
// model requests (ScriptedChatClient.ReceivedMessages), authoritative
// journal mutations, and turn outcomes, never reply success alone.
//
// Threshold math: the small test catalog entry (600 context window, 100
// reserved output) with ReservedBufferTokens 50 yields threshold 450. A
// 1000-char text message estimates to 254 tokens (4 framing plus
// ceiling(1000/4)); a 376-char text estimates to 98. Deltas coalesce into
// one assistant message per turn through ConversationRecovery, so per-turn
// histories stay countable.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

let private waitBound = TimeSpan.FromSeconds 30.0

let private sessionModelRef = ModelReference.Parse "test/session-model"

let private compactModelText = "test/compact-model"

let private smokeAgentId = AgentId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FBV")

let private awaitWhat (work: Task<'T>) (what: string) : Task<'T> =
    task {
        try
            return! work.WaitAsync(waitBound, CancellationToken.None)
        with :? TimeoutException ->
            return raise (TimeoutException($"The test timed out waiting for {what}."))
    }

let private big (filler: char) (length: int) : string = String(filler, length)

/// The small catalog entry every compaction test thresholds against:
/// 600 window minus 100 reserved output minus the configured 50 buffer
/// leaves threshold 450.
let private smallEntry () : ModelCatalogEntry =
    {
        Model = sessionModelRef
        ContextWindowTokens = 600
        ReservedOutputTokens = 100
        MaxOutputTokens = 100
        Capabilities =
            {
                Streaming = true
                Reasoning = false
                ToolCalling = true
            }
    }

let private smallThreshold () : int =
    ContextPruning.PruneThreshold(smallEntry (), 50)

type private FakeCatalog(entry: ModelCatalogEntry | null) =
    interface ILlmModelCatalog with
        member _.GetEntry _ = entry
        member _.HasEntry _ = not (isNull (box entry))

/// Records every tenant/provider/model authorization in order.
type private RecordingPolicy(decide: string -> string -> ModelPolicyDecision) =
    let seen = ConcurrentBag<string * string>()

    interface IModelPolicy with
        member _.Authorize(_, provider, model) =
            seen.Add((provider, model))
            decide provider model

    member _.Seen = seen |> Seq.toList

/// Records every usage checkpoint and settlement the runtime reports.
type private RecordingObserver() =
    let checkpoints = ConcurrentBag<UsageCheckpoint>()
    let settlements = ConcurrentBag<UsageSettlement>()

    interface IUsageObserver with
        member _.OnCheckpoint usage = checkpoints.Add usage
        member _.OnSettled usage = settlements.Add usage

    member _.Checkpoints = checkpoints |> Seq.toList

let private allowAll (_provider: string) (_model: string) : ModelPolicyDecision = ModelPolicyDecision.Allow

/// Builds the production facade container: in-memory stores, the scripted
/// chat client the facade opts into suspendable children with, the smoke
/// agent on the session model, the small compaction catalog, and the
/// caller-supplied options/policy/observer overrides.
let private createServices
    (database: InMemoryDatabase)
    (chat: IChatClient)
    (tools: AITool list)
    (configure: Action<LegateOptions>)
    (catalog: ILlmModelCatalog | null)
    (policy: IModelPolicy | null)
    (observer: IUsageObserver | null)
    : IServiceCollection =
    let services = ServiceCollection() :> IServiceCollection

    let clientOptions = SessionClientOptions()
    clientOptions.Tenant <- TenantId.Default
    services.AddSingleton(clientOptions) |> ignore

    LegateServiceCollectionExtensions.AddLegate(
        services,
        ?configure =
            Some(fun (builder: LegateBuilder) ->
                builder.Llm.AddProvider(BuilderTests.StubLlmProvider()) |> ignore
                builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore
                builder.Workspace.UseRuntime(BuilderTests.StubWorkspaceRuntime()) |> ignore

                builder.Tools.AddSource(StaticToolSource(ResizeArray<AITool>(tools) :> IReadOnlyList<AITool>))
                |> ignore

                builder.Agents.Add(
                    "smoke",
                    Func<Agent, Agent>(fun template ->
                        { template with
                            Id = smokeAgentId
                            Description = "Production compaction agent."
                            Model = sessionModelRef
                            SystemPrompt = "Answer briefly."
                            ToolSelection = ToolSelection()
                        })
                )
                |> ignore)
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    services.AddSingleton<IChatClient>(chat) |> ignore
    services.Configure<LegateOptions>(configure) |> ignore

    match Option.ofObj catalog with
    | Some live -> services.AddSingleton<ILlmModelCatalog>(live) |> ignore
    | None -> ()

    match Option.ofObj policy with
    | Some live -> services.AddSingleton<IModelPolicy>(live) |> ignore
    | None -> ()

    match Option.ofObj observer with
    | Some live -> services.AddSingleton<IUsageObserver>(live) |> ignore
    | None -> ()

    services

/// The base Legate options every compaction test starts from: the session
/// model default, two retained messages, and the 50-token buffer the
/// threshold math above assumes.
let private baseOptions (extra: LegateOptions -> unit) : Action<LegateOptions> =
    Action<LegateOptions>(fun options ->
        options.Llm.DefaultModel <- "test/session-model"
        options.Llm.CompactionKeepMessages <- 2
        options.Pruning.ReservedBufferTokens <- 50
        extra options)

let private actorServiceOf (provider: IServiceProvider) : LocalActorSystemService =
    provider.GetServices<IHostedService>()
    |> Seq.pick (fun service ->
        match service with
        | :? LocalActorSystemService as local -> Some local
        | _ -> None)

let private withClient (provider: IServiceProvider) (work: SessionClient -> Task<'T>) : Task<'T> =
    task {
        let service = actorServiceOf provider
        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let client = provider.GetRequiredService<SessionClient>()

        try
            let! outcome = work client
            do! (service :> IHostedService).StopAsync(CancellationToken.None)
            return outcome
        with ex ->
            try
                (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
            with _ ->
                ()

            return raise ex
    }

let private openSmoke (client: SessionClient) : Task<Session> =
    task {
        match client.Agents with
        | None -> return raise (InvalidOperationException("Expected the test container to carry an agent catalog."))
        | Some catalog ->
            let! smoke = catalog.GetAgent(client.Tenant, smokeAgentId, CancellationToken.None)

            match smoke with
            | null -> return raise (InvalidOperationException("Expected the code-defined smoke agent to resolve."))
            | found -> return! SessionClientOperations.OpenSessionAsync(client, found.Id, null, CancellationToken.None)
    }

let private promptWait (client: SessionClient) (sessionId: SessionId) (text: string) : Task<TurnResult> =
    SessionClientExtensions.PromptAndWaitAsync(client, sessionId, UserMessage.Text text, CancellationToken.None)

let private replayAll (client: SessionClient) (sessionId: SessionId) : Task<SessionEvent list> =
    task {
        let collected = ResizeArray<SessionEvent>()
        let mutable cursor = 0L
        let mutable more = true

        while more do
            let! outcome =
                client.EventBus.EventStore.Replay(client.Tenant, sessionId, cursor, 100, CancellationToken.None)

            match outcome with
            | :? EventReplayPage as page when not (isNull (box page)) ->
                if not (isNull (box page.Events)) then
                    collected.AddRange page.Events

                if page.NextCursor.HasValue then
                    cursor <- page.NextCursor.Value
                else
                    more <- false
            | _ -> more <- false

        return List.ofSeq collected
    }

let private compactedOf (events: SessionEvent list) : CompactedEvent list =
    events
    |> List.choose (function
        | :? CompactedEvent as compacted when not (isNull (box compacted)) -> Some compacted
        | _ -> None)

let private failedOf (events: SessionEvent list) : CompactionFailedEvent list =
    events
    |> List.choose (function
        | :? CompactionFailedEvent as failed when not (isNull (box failed)) -> Some failed
        | _ -> None)

/// Concatenates one received message's tool-result payloads.
let private resultText (message: ChatMessage) : string =
    if isNull (box message) || isNull (box message.Contents) then
        ""
    else
        message.Contents
        |> Seq.choose (function
            | :? FunctionResultContent as result when not (isNull (box result)) ->
                match result.Result with
                | :? string as text -> if String.IsNullOrEmpty text then None else Some text
                | _ -> None
            | _ -> None)
        |> String.concat "\n"

/// Concatenates one received message's text parts.
let private messageText (message: ChatMessage) : string =
    if isNull (box message) || isNull (box message.Contents) then
        ""
    else
        message.Contents
        |> Seq.choose (function
            | :? TextContent as text when not (isNull (box text)) && not (isNull (box text.Text)) -> Some text.Text
            | _ -> None)
        |> String.concat "\n"

let private slice (chat: ScriptedChatClient) (skip: int) : ChatMessage list =
    chat.ReceivedMessages |> Seq.skip skip |> List.ofSeq

/// The last inputs the final provider call received.
let private lastInputs (chat: ScriptedChatClient) (take: int) : ChatMessage list =
    slice chat (chat.ReceivedMessages.Count - take)

/// Collects the non-empty function-call ids in one received message.
let private callIds (message: ChatMessage) : string list =
    if isNull (box message) || isNull (box message.Contents) then
        []
    else
        [
            for content in message.Contents do
                match content with
                | :? FunctionCallContent as call when not (isNull (box call)) && not (String.IsNullOrEmpty call.CallId) ->
                    yield call.CallId
                | _ -> ()
        ]

/// Collects the non-empty function-result ids in one received message.
let private resultIds (message: ChatMessage) : string list =
    if isNull (box message) || isNull (box message.Contents) then
        []
    else
        [
            for content in message.Contents do
                match content with
                | :? FunctionResultContent as result when
                    not (isNull (box result)) && not (String.IsNullOrEmpty result.CallId)
                    ->
                    yield result.CallId
                | _ -> ()
        ]

/// Asserts every result id in the messages answers a call id in the same
/// messages: the next model input carries complete pairings, never
/// orphaned retained calls or results.
let private checkPaired (messages: ChatMessage list) =
    let calls = messages |> List.collect callIds |> Set.ofList
    let results = messages |> List.collect resultIds

    for id in results do
        calls.Contains id |> should equal true

let private tool (name: string) (result: string) : AITool =
    let method = Func<string>(fun () -> result)

    AIFunctionFactory.Create(method, name, Unchecked.defaultof<string>, Unchecked.defaultof<JsonSerializerOptions>)
    :> AITool

/// A tool that signals entry, then blocks until released: keeps a turn
/// Running so Compact and Abort meet it mid-flight without pinning the
/// actor thread.
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

let private settleWaiter (sessionId: SessionId) : TaskCompletionSource<TurnResult> =
    PromptWaitHubs.GetOrAdd(sessionId).EnqueueSettle()

// ──────────────────────────────────────────────────────────────────────────
// Task 1: production automatic compaction wiring

[<Fact>]
let ``Threshold math pins the test catalog at 450 tokens`` () = smallThreshold () |> should equal 450

[<Fact>]
let ``Below threshold runs no summarizer call and journals no compaction event`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.Text "ans-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                chat
                []
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! first = awaitWhat (promptWait client created.Id "hello") "the first turn to settle"
                    first.Status |> should equal TurnStatus.Completed

                    let! second = awaitWhat (promptWait client created.Id "again") "the second turn to settle"
                    second.Status |> should equal TurnStatus.Completed
                    second.AssistantText |> should equal "ans-2"

                    // Two turns, two provider calls: no summarizer ran.
                    chat.Calls |> should equal 2

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 0
                    (failedOf events).Length |> should equal 0
                })
    }

[<Fact>]
let ``At threshold runs no summarizer call`` () : Task =
    task {
        // 254 (1000 chars) + 98 (376 chars) + 98 (376 chars) = 450, exactly
        // the threshold: the strictly-exceeds check stays quiet.
        let answer1 = big 'y' 376
        let prompt2 = big 'z' 376

        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text answer1
                        ScriptStep.Text "ans-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                chat
                []
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"
                    let! second = awaitWhat (promptWait client created.Id prompt2) "the second turn to settle"
                    second.Status |> should equal TurnStatus.Completed

                    chat.Calls |> should equal 2

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 0
                })
    }

[<Fact>]
let ``Above threshold compacts before the next model call`` () : Task =
    task {
        // Prefix estimates 254 + 6 + 254 = 514 over the 450 threshold, so
        // the second turn's first boundary summarises before its provider
        // call: three takes for two turns.
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.Text "the gist"
                        ScriptStep.Text "ans-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                chat
                []
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"
                    let! second = awaitWhat (promptWait client created.Id (big 'b' 1000)) "the second turn to settle"
                    second.Status |> should equal TurnStatus.Completed
                    second.AssistantText |> should equal "ans-2"

                    chat.Calls |> should equal 3

                    // Turn one recorded its single user message; the
                    // summarizer recorded the working history plus the
                    // instruction; the provider recorded the compacted
                    // history.
                    let afterFirst = slice chat 1
                    afterFirst.Length |> should equal 7

                    let summarizerInput = afterFirst |> List.take 4
                    let compactedInput = afterFirst |> List.skip 4

                    (messageText summarizerInput[3] = Compaction.SummarizeInstruction)
                    |> should equal true

                    (messageText compactedInput[0]).StartsWith(Compaction.SummaryMarker, StringComparison.Ordinal)
                    |> should equal true

                    (messageText compactedInput[0]).Contains("the gist") |> should equal true
                    compactedInput.Length |> should equal 3

                    // The superseded first prompt never reaches the model
                    // again; the retained tail does.
                    compactedInput
                    |> List.exists (fun message -> (messageText message).Contains(big 'a' 100))
                    |> should equal false

                    compactedInput
                    |> List.exists (fun message -> (messageText message).Contains(big 'b' 100))
                    |> should equal true

                    let! events = replayAll client created.Id
                    let compacted = compactedOf events
                    compacted.Length |> should equal 1
                    (compacted[0].BeforeEstimate > compacted[0].AfterEstimate) |> should equal true
                })
    }

[<Fact>]
let ``Nothing eligible compacts nothing and claims no success`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(ResizeArray<ScriptStep>([| ScriptStep.Text "ans-1" |]))

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                chat
                []
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client

                    // A single-message history keeps nothing replaceable
                    // under keep two: no summarizer call, no journal write,
                    // and the turn still completes on its own answer.
                    let! first = awaitWhat (promptWait client created.Id "hello") "the turn to settle"
                    first.Status |> should equal TurnStatus.Completed
                    first.AssistantText |> should equal "ans-1"

                    chat.Calls |> should equal 1

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 0
                    (failedOf events).Length |> should equal 0
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 2: on-demand Compact on running turns

[<Fact>]
let ``On-demand Compact during a running turn compacts at the next boundary`` () : Task =
    task {
        use entered = new ManualResetEventSlim(false)
        let release = TaskCompletionSource<string>()

        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.ToolCall("c1", "blocker")
                        ScriptStep.Text "the gist"
                        ScriptStep.Text "done-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                chat
                [
                    blockingTool "blocker" entered release
                ]
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        let service = actorServiceOf provider
        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let client = provider.GetRequiredService<SessionClient>()

        try
            let! created = openSmoke client
            let! _ = awaitWhat (promptWait client created.Id "first") "the first turn to settle"
            let waiter = settleWaiter created.Id

            let prompt =
                SessionClientOperations.PromptAsync(
                    client,
                    created.Id,
                    UserMessage.Text "second",
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let! _ = awaitWhat prompt "the prompt to land"
            Assert.True(entered.Wait(waitBound))

            // Small history, far under threshold: only the force bypass
            // compacts here.
            let! outcome = SessionClientOperations.CompactAsync(client, created.Id, CancellationToken.None)
            (outcome :? SessionCompactDeferred) |> should equal true

            release.TrySetResult("unblocked") |> ignore
            let! result = awaitWhat waiter.Task "the turn to settle"
            result.Status |> should equal TurnStatus.Completed
            result.AssistantText |> should equal "done-2"

            // First turn, the tool-call response, the forced summary, and
            // the post-compaction answer: the force fired exactly once.
            chat.Calls |> should equal 4

            let! events = replayAll client created.Id
            (compactedOf events).Length |> should equal 1
        finally
            release.TrySetResult("unblocked") |> ignore
            entered.Dispose()

            try
                (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
            with _ ->
                ()
    }

[<Fact>]
let ``Repeated on-demand Compacts coalesce into one summarizer call`` () : Task =
    task {
        use entered = new ManualResetEventSlim(false)
        let release = TaskCompletionSource<string>()

        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.ToolCall("c1", "blocker")
                        ScriptStep.Text "the gist"
                        ScriptStep.Text "done-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                chat
                [
                    blockingTool "blocker" entered release
                ]
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        let service = actorServiceOf provider
        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let client = provider.GetRequiredService<SessionClient>()

        try
            let! created = openSmoke client
            let! _ = awaitWhat (promptWait client created.Id "first") "the first turn to settle"
            let waiter = settleWaiter created.Id

            let prompt =
                SessionClientOperations.PromptAsync(
                    client,
                    created.Id,
                    UserMessage.Text "second",
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let! _ = awaitWhat prompt "the prompt to land"
            Assert.True(entered.Wait(waitBound))

            let! first = SessionClientOperations.CompactAsync(client, created.Id, CancellationToken.None)
            (first :? SessionCompactDeferred) |> should equal true

            let! second = SessionClientOperations.CompactAsync(client, created.Id, CancellationToken.None)
            (second :? SessionCompactDeferred) |> should equal true

            release.TrySetResult("unblocked") |> ignore
            let! result = awaitWhat waiter.Task "the turn to settle"
            result.Status |> should equal TurnStatus.Completed

            // Two deferred requests, one one-shot take: a single summary.
            chat.Calls |> should equal 4

            let! events = replayAll client created.Id
            (compactedOf events).Length |> should equal 1
        finally
            release.TrySetResult("unblocked") |> ignore
            entered.Dispose()

            try
                (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
            with _ ->
                ()
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 3: instruction, retention, and provider validity

[<Fact>]
let ``Compaction preserves instructions and keeps the summary out of authority`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.Text "the gist"
                        ScriptStep.Text "ans-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                chat
                []
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        let path =
            Path.Combine(Path.GetTempPath(), "legate-compact-" + Guid.NewGuid().ToString("N") + ".md")

        File.WriteAllText(path, "Follow the brief.")

        try
            return!
                withClient provider (fun client ->
                    task {
                        let options = SessionOptions()
                        options.HostInstructionFiles <- ResizeArray<string>([| path |]) :> IReadOnlyList<string>

                        let! created =
                            task {
                                match client.Agents with
                                | None -> return raise (InvalidOperationException("Expected an agent catalog."))
                                | Some catalog ->
                                    let! smoke = catalog.GetAgent(client.Tenant, smokeAgentId, CancellationToken.None)

                                    match smoke with
                                    | null ->
                                        return
                                            raise (
                                                InvalidOperationException(
                                                    "Expected the code-defined smoke agent to resolve."
                                                )
                                            )
                                    | found ->
                                        return!
                                            SessionClientOperations.OpenSessionAsync(
                                                client,
                                                found.Id,
                                                options,
                                                CancellationToken.None
                                            )
                            }

                        let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"
                        let! _ = awaitWhat (promptWait client created.Id (big 'b' 1000)) "the second turn to settle"

                        // System, first-turn pair, second user prompt, then
                        // the summarizer request: the compacted provider
                        // input is the final four.
                        let compactedInput = lastInputs chat 4
                        compactedInput.Length |> should equal 4

                        compactedInput[0].Role |> should equal ChatRole.System

                        (messageText compactedInput[0]).Contains("Follow the brief.")
                        |> should equal true

                        compactedInput[1].Role |> should equal ChatRole.User

                        (messageText compactedInput[1]).StartsWith(Compaction.SummaryMarker, StringComparison.Ordinal)
                        |> should equal true

                        // The summary is user content, never a new system
                        // instruction.
                        compactedInput
                        |> List.exists (fun message -> message.Role = ChatRole.System)
                        |> should equal true

                        let systems =
                            compactedInput |> List.filter (fun message -> message.Role = ChatRole.System)

                        systems.Length |> should equal 1

                        let! events = replayAll client created.Id
                        (compactedOf events).Length |> should equal 1
                    })
        finally
            try
                File.Delete(path)
            with _ ->
                ()
    }

[<Fact>]
let ``Boundary-crossing tool exchange retains its complete pairing`` () : Task =
    task {
        // keep one with a five-message history ending in a call/result
        // pair: the positional tail would keep only the result, so the
        // pairing-safe cut must extend back over the call.
        let args = Dictionary<string, obj>()
        args["city"] <- "Oslo" :> obj

        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ready"
                        ScriptStep.ToolCall("k9", "lookup", args)
                        ScriptStep.Text "the gist"
                        ScriptStep.Text "done"
                    |]
                )
            )

        let database = InMemoryDatabase()

        let configure =
            baseOptions (fun options ->
                options.Llm.CompactionKeepMessages <- 1
                options.Llm.DefaultModel <- "test/session-model")

        use provider =
            (createServices
                database
                chat
                [ tool "lookup" (big 'R' 1200) ]
                configure
                (FakeCatalog(
                    {
                        Model = sessionModelRef
                        ContextWindowTokens = 400
                        ReservedOutputTokens = 50
                        MaxOutputTokens = 50
                        Capabilities =
                            {
                                Streaming = true
                                Reasoning = false
                                ToolCalling = true
                            }
                    }
                )
                :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id "go") "the first turn to settle"

                    // The tool result tips the history over the 300-token
                    // threshold at the post-execution boundary.
                    let! result = awaitWhat (promptWait client created.Id "again") "the second turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                    result.AssistantText |> should equal "done"

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 1

                    // The provider input after the summarizer request keeps
                    // the call with its actual id, name, and arguments plus
                    // the result with its text: no orphan, no invention.
                    let compactedInput = lastInputs chat 3
                    compactedInput.Length |> should equal 3
                    checkPaired compactedInput

                    let calls = compactedInput |> List.collect callIds
                    calls |> should equal [ "k9" ]

                    let results = compactedInput |> List.collect resultIds
                    results |> should equal [ "k9" ]

                    compactedInput
                    |> List.exists (fun message -> (resultText message).Contains(big 'R' 100))
                    |> should equal true

                    let calls = compactedInput |> List.collect callIds
                    calls |> should equal [ "k9" ]

                    let results = compactedInput |> List.collect resultIds
                    results |> should equal [ "k9" ]

                    compactedInput
                    |> List.exists (fun message -> (resultText message).Contains(big 'R' 100))
                    |> should equal true
                })
    }

[<Fact>]
let ``Multi-call exchange retains every pairing with actual arguments`` () : Task =
    task {
        let firstArgs = Dictionary<string, obj>()
        firstArgs["city"] <- "Oslo" :> obj
        let secondArgs = Dictionary<string, obj>()
        secondArgs["city"] <- "Bergen" :> obj

        let calls =
            ResizeArray<ScriptToolCall>(
                [|
                    ScriptToolCall("k1", "lookup", firstArgs)
                    ScriptToolCall("k2", "lookup", secondArgs)
                |]
            )
            :> IReadOnlyList<ScriptToolCall>

        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ready"
                        ScriptStep.ToolCalls(calls)
                        ScriptStep.Text "the gist"
                        ScriptStep.Text "done"
                    |]
                )
            )

        let database = InMemoryDatabase()

        let configure =
            baseOptions (fun options ->
                options.Llm.CompactionKeepMessages <- 1
                options.Llm.DefaultModel <- "test/session-model")

        use provider =
            (createServices
                database
                chat
                [ tool "lookup" (big 'R' 700) ]
                configure
                (FakeCatalog(
                    {
                        Model = sessionModelRef
                        ContextWindowTokens = 400
                        ReservedOutputTokens = 50
                        MaxOutputTokens = 50
                        Capabilities =
                            {
                                Streaming = true
                                Reasoning = false
                                ToolCalling = true
                            }
                    }
                )
                :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id "go") "the first turn to settle"

                    let! result = awaitWhat (promptWait client created.Id "again") "the second turn to settle"
                    result.Status |> should equal TurnStatus.Completed

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 1

                    let compactedInput = lastInputs chat 4
                    checkPaired compactedInput

                    (compactedInput |> List.collect callIds |> List.sort)
                    |> should equal [ "k1"; "k2" ]

                    (compactedInput |> List.collect resultIds |> List.sort)
                    |> should equal [ "k1"; "k2" ]
                })
    }

[<Fact>]
let ``Retained non-text content survives compaction`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.Text "the gist"
                        ScriptStep.Text "ans-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                chat
                []
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"

                    let parts =
                        ResizeArray<AIContent>(
                            [|
                                TextContent(big 'b' 1000) :> AIContent
                                DataContent(ReadOnlyMemory([| 1uy; 2uy; 3uy |]), "image/png") :> AIContent
                            |]
                        )
                        :> IReadOnlyList<AIContent>

                    let! _ =
                        awaitWhat
                            (SessionClientExtensions.PromptAndWaitAsync(
                                client,
                                created.Id,
                                UserMessage(parts, null),
                                CancellationToken.None
                            ))
                            "the second turn to settle"

                    let compactedInput = slice chat 5
                    compactedInput.Length |> should equal 3

                    let retained = compactedInput[2]

                    let images =
                        retained.Contents
                        |> Seq.choose (function
                            | :? DataContent as data when not (isNull (box data)) -> Some data
                            | _ -> None)
                        |> List.ofSeq

                    images.Length |> should equal 1
                    images[0].MediaType |> should equal "image/png"
                    images[0].Data.ToArray() |> should equal [| 1uy; 2uy; 3uy |]

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 1
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 4: summarizer model selection and policy

[<Fact>]
let ``Configured compaction model is selected and authorized`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text("ans-1", 10L, 5L)
                        ScriptStep.Text("the gist", 30L, 12L)
                        ScriptStep.Text("ans-2", 7L, 3L)
                    |]
                )
            )

        let database = InMemoryDatabase()
        let policy = RecordingPolicy(allowAll)
        let observer = RecordingObserver()

        let configure =
            baseOptions (fun options ->
                options.Llm.DefaultModel <- "test/session-model"
                options.Llm.Compaction <- compactModelText)

        use provider =
            (createServices
                database
                chat
                []
                configure
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                (policy :> IModelPolicy)
                (observer :> IUsageObserver))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"
                    let! _ = awaitWhat (promptWait client created.Id (big 'b' 1000)) "the second turn to settle"

                    // The summarizer authorization names the configured
                    // provider and model, not merely a usage label.
                    policy.Seen |> List.contains ("test", "compact-model") |> should equal true

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 1
                })
    }

[<Fact>]
let ``Session-model fallback authorizes the session model`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.Text "the gist"
                        ScriptStep.Text "ans-2"
                    |]
                )
            )

        let database = InMemoryDatabase()
        let policy = RecordingPolicy(allowAll)

        use provider =
            (createServices
                database
                chat
                []
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                (policy :> IModelPolicy)
                null)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"
                    let! _ = awaitWhat (promptWait client created.Id (big 'b' 1000)) "the second turn to settle"

                    // No override configured: every authorization names the
                    // session model, so nothing bypasses policy or silently
                    // substitutes another model.
                    policy.Seen.IsEmpty |> should equal false

                    for (_, model) in policy.Seen do
                        model |> should equal "session-model"

                    for (provider, _) in policy.Seen do
                        provider |> should equal "test"

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 1
                })
    }

[<Fact>]
let ``Denied compaction model never bypasses policy`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.Text "ans-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        let decide (provider: string) (model: string) : ModelPolicyDecision =
            if provider = "test" && model = "compact-model" then
                ModelPolicyDecision.Deny("quota spent")
            else
                ModelPolicyDecision.Allow

        let policy = RecordingPolicy(decide)

        let configure =
            baseOptions (fun options ->
                options.Llm.DefaultModel <- "test/session-model"
                options.Llm.Compaction <- compactModelText)

        use provider =
            (createServices
                database
                chat
                []
                configure
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                (policy :> IModelPolicy)
                null)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"

                    let! second = awaitWhat (promptWait client created.Id (big 'b' 1000)) "the second turn to settle"
                    second.Status |> should equal TurnStatus.Completed
                    second.AssistantText |> should equal "ans-2"

                    // The denied summarizer never runs: two turns, two
                    // provider calls, no silent substitute on another model.
                    chat.Calls |> should equal 2

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 0

                    let failed = failedOf events
                    failed.Length |> should equal 1
                    failed[0].Reason |> should equal "quota spent"
                })
    }

[<Fact>]
let ``Unknown-model fallback thresholds without a catalog`` () : Task =
    task {
        // No catalog registered: the threshold falls back to the catalog
        // defaults, and a forced request still compacts replaceable
        // context without one.
        use entered = new ManualResetEventSlim(false)
        let release = TaskCompletionSource<string>()

        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.ToolCall("c1", "blocker")
                        ScriptStep.Text "the gist"
                        ScriptStep.Text "done-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                chat
                [
                    blockingTool "blocker" entered release
                ]
                (baseOptions ignore)
                null
                null
                null)
                .BuildServiceProvider()

        let service = actorServiceOf provider
        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let client = provider.GetRequiredService<SessionClient>()

        try
            let! created = openSmoke client
            let! _ = awaitWhat (promptWait client created.Id "first") "the first turn to settle"
            let waiter = settleWaiter created.Id

            let prompt =
                SessionClientOperations.PromptAsync(
                    client,
                    created.Id,
                    UserMessage.Text "second",
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let! _ = awaitWhat prompt "the prompt to land"
            Assert.True(entered.Wait(waitBound))

            let! outcome = SessionClientOperations.CompactAsync(client, created.Id, CancellationToken.None)
            (outcome :? SessionCompactDeferred) |> should equal true

            release.TrySetResult("unblocked") |> ignore
            let! result = awaitWhat waiter.Task "the turn to settle"
            result.Status |> should equal TurnStatus.Completed

            let! events = replayAll client created.Id
            (compactedOf events).Length |> should equal 1
        finally
            release.TrySetResult("unblocked") |> ignore
            entered.Dispose()

            try
                (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
            with _ ->
                ()
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 5: bounded summarizer failure

[<Fact>]
let ``Provider error yields an explicit failure and continues bounded`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.Failure(InvalidOperationException("boom"))
                        ScriptStep.Text "ans-2"
                    |]
                )
            )

        let database = InMemoryDatabase()
        let observer = RecordingObserver()

        use provider =
            (createServices
                database
                chat
                []
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                (observer :> IUsageObserver))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"

                    // The failed summarizer journals once and the turn
                    // continues on the original working context: exactly
                    // one summarizer attempt, no retry loop.
                    let! second = awaitWhat (promptWait client created.Id (big 'b' 1000)) "the second turn to settle"
                    second.Status |> should equal TurnStatus.Completed
                    second.AssistantText |> should equal "ans-2"

                    chat.Calls |> should equal 3

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 0

                    let failed = failedOf events
                    failed.Length |> should equal 1
                    failed[0].Reason.Contains("boom") |> should equal true

                    // Missing usage is never fabricated: the failed attempt
                    // reports nothing to the observer.
                    observer.Checkpoints.Length |> should equal 0
                })
    }

[<Fact>]
let ``Empty summary yields an explicit failure without retry`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.Text ""
                        ScriptStep.Text "ans-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                chat
                []
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"

                    let! second = awaitWhat (promptWait client created.Id (big 'b' 1000)) "the second turn to settle"
                    second.Status |> should equal TurnStatus.Completed

                    chat.Calls |> should equal 3

                    let! events = replayAll client created.Id
                    (compactedOf events).Length |> should equal 0

                    let failed = failedOf events
                    failed.Length |> should equal 1
                    failed[0].Reason.Contains("empty") |> should equal true
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 6: cancellation, ownership, and suspension semantics

/// Permission policy asking once for the gated tool, allowing the rest.
type private AskPolicy(gated: string) =
    interface IPermissionPolicy with
        member _.Evaluate request =
            if request.ToolName = gated then
                PermissionVerdict.Ask
            else
                PermissionVerdict.Allow

[<Fact>]
let ``Compact while suspended no-ops without consuming the reply`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.ToolCall("c1", "gated")
                        ScriptStep.Text "resumed"
                    |]
                )
            )

        let database = InMemoryDatabase()

        let services =
            createServices database chat [ tool "gated" "ok" ] (baseOptions ignore) null null null

        services.AddSingleton<IPermissionPolicy>(AskPolicy("gated")) |> ignore

        use provider = services.BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
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

                    let collect =
                        task {
                            let collected = ResizeArray<SessionEvent>()

                            let stream =
                                SessionClientOperations.Subscribe(client, created.Id, 0L, CancellationToken.None)

                            let enumerator = stream.GetAsyncEnumerator(CancellationToken.None)

                            try
                                let mutable remaining = 3

                                while remaining > 0 do
                                    let! has = awaitWhat (enumerator.MoveNextAsync().AsTask()) "the suspension"
                                    Assert.True(has)
                                    collected.Add(enumerator.Current)
                                    remaining <- remaining - 1

                                return List.ofSeq collected
                            finally
                                enumerator.DisposeAsync().AsTask() |> ignore
                        }

                    let! events = awaitWhat collect "the suspension"

                    let asked =
                        events
                        |> List.pick (fun event ->
                            match event with
                            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) -> Some asked
                            | _ -> None)

                    // A suspended turn owns the history: compaction no-ops
                    // instead of deferring into the parked turn.
                    let! outcome = SessionClientOperations.CompactAsync(client, created.Id, CancellationToken.None)
                    (outcome :? SessionCompactNotNeeded) |> should equal true

                    let! _ =
                        SessionClientOperations.ReplyAsync(
                            client,
                            created.Id,
                            PermissionDecision(asked.RequestId, PermissionDecisionKind.AllowOnce),
                            CancellationToken.None
                        )

                    let! result = awaitWhat waiter.Task "the resumed turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                    result.AssistantText |> should equal "resumed"

                    let! journaled = replayAll client created.Id
                    (compactedOf journaled).Length |> should equal 0
                })
    }

[<Fact>]
let ``Suspended turn resumes and settles after compaction`` () : Task =
    task {
        // The over-threshold prefix compacts at the first boundary, before
        // the gated tool suspends; the resume then settles normally with a
        // single compaction behind it.
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.Text "the gist"
                        ScriptStep.ToolCall("c2", "gated")
                        ScriptStep.Text "resumed"
                    |]
                )
            )

        let database = InMemoryDatabase()

        let services =
            createServices
                database
                chat
                [ tool "gated" "ok" ]
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null

        services.AddSingleton<IPermissionPolicy>(AskPolicy("gated")) |> ignore

        use provider = services.BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"
                    let waiter = settleWaiter created.Id

                    let prompt =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text(big 'b' 1000),
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! _ = awaitWhat prompt "the prompt to land"

                    let collect =
                        task {
                            let collected = ResizeArray<SessionEvent>()

                            let stream =
                                SessionClientOperations.Subscribe(client, created.Id, 0L, CancellationToken.None)

                            let enumerator = stream.GetAsyncEnumerator(CancellationToken.None)

                            try
                                let mutable seen = false

                                while not seen do
                                    let! has = awaitWhat (enumerator.MoveNextAsync().AsTask()) "the suspension"
                                    Assert.True(has)
                                    collected.Add(enumerator.Current)

                                    match enumerator.Current with
                                    | :? PermissionRequestedEvent -> seen <- true
                                    | _ -> ()

                                return List.ofSeq collected
                            finally
                                enumerator.DisposeAsync().AsTask() |> ignore
                        }

                    let! _ = awaitWhat collect "the suspension"

                    let! journaled = replayAll client created.Id

                    let asked =
                        journaled
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

                    let! result = awaitWhat waiter.Task "the resumed turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                    result.AssistantText |> should equal "resumed"

                    let! settled = replayAll client created.Id
                    (compactedOf settled).Length |> should equal 1
                })
    }

[<Fact>]
let ``Configured deadline fails the turn with no compaction commit`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)

        let hanging =
            { new IChatClient with
                member _.GetResponseAsync(history, _, cancellationToken) =
                    task {
                        entered.Set() |> ignore
                        do! Task.Delay(TimeSpan.FromSeconds(5.0), cancellationToken)

                        return
                            ChatResponse(
                                ResizeArray<ChatMessage>(
                                    [|
                                        ChatMessage(ChatRole.Assistant, "late")
                                    |]
                                )
                            )
                    }

                member _.GetStreamingResponseAsync(_, _, _) =
                    raise (
                        NotSupportedException(
                            "The hanging client is non-streaming: the caller falls back to GetResponseAsync."
                        )
                    )

                member _.GetService(_, _) = null
                member _.Dispose() = ()
            }

        let database = InMemoryDatabase()

        use provider =
            (createServices database hanging [] (baseOptions ignore) null null null).BuildServiceProvider()

        let service = actorServiceOf provider
        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let client = provider.GetRequiredService<SessionClient>()

        try
            let options = SessionOptions()
            options.Timeout <- Nullable(TimeSpan.FromMilliseconds(200.0))

            let! session =
                task {
                    match client.Agents with
                    | None -> return raise (InvalidOperationException("Expected an agent catalog."))
                    | Some catalog ->
                        let! smoke = catalog.GetAgent(client.Tenant, smokeAgentId, CancellationToken.None)

                        match smoke with
                        | null -> return raise (InvalidOperationException("Expected the smoke agent to resolve."))
                        | found ->
                            return!
                                SessionClientOperations.OpenSessionAsync(
                                    client,
                                    found.Id,
                                    options,
                                    CancellationToken.None
                                )
                }

            let prompt =
                SessionClientOperations.PromptAsync(
                    client,
                    session.Id,
                    UserMessage.Text "start",
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            // Queue the waiter before prompting: a settle with no waiter
            // only records.
            let waiter = settleWaiter session.Id

            let! _ = awaitWhat prompt "the prompt to land"
            Assert.True(entered.Wait(waitBound))

            // Arm the force while the provider call hangs: the 200 ms turn
            // deadline fires first, so no boundary ever consumes it and
            // nothing commits.
            let! deferred = SessionClientOperations.CompactAsync(client, session.Id, CancellationToken.None)
            (deferred :? SessionCompactDeferred) |> should equal true

            let! result = awaitWhat waiter.Task "the turn to settle"
            result.Status |> should equal TurnStatus.Failed

            match result.Outcome with
            | :? TurnFailed as failed ->
                failed.Reason.Contains("timeout", StringComparison.OrdinalIgnoreCase)
                |> should equal true
            | _ -> failwith "Expected the timed-out turn to carry a TurnFailed outcome."

            let! events = replayAll client session.Id
            (compactedOf events).Length |> should equal 0
            (failedOf events).Length |> should equal 0
        finally
            entered.Dispose()

            try
                (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
            with _ ->
                ()
    }

[<Fact>]
let ``Takeover loser admits no summarizer work and commits nothing`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = TaskCompletionSource<string>()
        let clock = FakeClock()

        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.ToolCall("c1", "blocker")
                        ScriptStep.Text "the gist"
                        ScriptStep.Text "done-2"
                    |]
                )
            )

        let database = InMemoryDatabase()

        let services =
            createServices
                database
                chat
                [
                    blockingTool "blocker" entered release
                ]
                (baseOptions ignore)
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                null

        services.AddSingleton<TimeProvider>(clock :> TimeProvider) |> ignore

        use provider = services.BuildServiceProvider()

        let service = actorServiceOf provider
        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let client = provider.GetRequiredService<SessionClient>()

        try
            let! created = openSmoke client
            let! _ = awaitWhat (promptWait client created.Id "first") "the first turn to settle"
            let waiter = settleWaiter created.Id

            let prompt =
                SessionClientOperations.PromptAsync(
                    client,
                    created.Id,
                    UserMessage.Text "second",
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let! _ = awaitWhat prompt "the prompt to land"
            Assert.True(entered.Wait(waitBound))

            // Arm the force while blocked, then expire the heartbeat view
            // past every live lease: the post-release boundary meets a dead
            // lease before the hook, so no summarizer invocation runs and
            // nothing commits.
            let! outcome = SessionClientOperations.CompactAsync(client, created.Id, CancellationToken.None)
            (outcome :? SessionCompactDeferred) |> should equal true

            clock.Advance(TimeSpan.FromHours(2.0))
            release.TrySetResult("unblocked") |> ignore

            let! result = awaitWhat waiter.Task "the loser turn to settle"
            result.Status |> should equal TurnStatus.Failed

            // First answer plus the tool-call response only: the armed
            // force never fired after authority was lost.
            chat.Calls |> should equal 2

            let! events = replayAll client created.Id
            (compactedOf events).Length |> should equal 0
            (failedOf events).Length |> should equal 0
        finally
            release.TrySetResult("unblocked") |> ignore
            entered.Dispose()

            try
                (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
            with _ ->
                ()
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 7: usage attribution and the facade gate

[<Fact>]
let ``Summarizer usage is attributed once to the selected model`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text("ans-1", 10L, 5L)
                        ScriptStep.Text("the gist", 30L, 12L)
                        ScriptStep.Text("ans-2", 7L, 3L)
                    |]
                )
            )

        let database = InMemoryDatabase()
        let observer = RecordingObserver()

        let configure =
            baseOptions (fun options ->
                options.Llm.DefaultModel <- "test/session-model"
                options.Llm.Compaction <- compactModelText)

        use provider =
            (createServices
                database
                chat
                []
                configure
                (FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
                null
                (observer :> IUsageObserver))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let! _ = awaitWhat (promptWait client created.Id (big 'a' 1000)) "the first turn to settle"

                    let! second = awaitWhat (promptWait client created.Id (big 'b' 1000)) "the second turn to settle"
                    second.Status |> should equal TurnStatus.Completed

                    // The summarizer checkpoint names the selected
                    // provider/model with the scripted usage, reported
                    // once: estimates are never billed.
                    observer.Checkpoints.Length |> should equal 1

                    let checkpoint = observer.Checkpoints[0]
                    checkpoint.Provider |> should equal "test"
                    checkpoint.Model |> should equal "compact-model"
                    checkpoint.InputTokens |> should equal 30L
                    checkpoint.OutputTokens |> should equal 12L
                    checkpoint.Attempt |> should equal 1
                    checkpoint.SessionId |> should equal created.Id

                    // The turn totals fold the summarizer spend in exactly
                    // once: 30 + 7 in, 12 + 3 out.
                    second.Usage.InputTokens |> should equal 37L
                    second.Usage.OutputTokens |> should equal 15L
                })
    }
