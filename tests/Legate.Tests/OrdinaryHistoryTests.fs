// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.OrdinaryHistoryTests

open System
open System.Collections.Generic
open System.IO
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Legate.Tests.TurnLoopTests
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Xunit

// Ordinary production history (issue 366): the model input across ordinary
// turns restores prior conversation context from the journal through the
// read-only #380 recovery builder, with the executing entry appended in
// order and the composed system prompt exactly once. Every fact below
// drives the production facade path (agent authority gate plus
// SessionPermissions.createRunner over fenced in-memory stores) and asserts
// actual model input via ScriptedChatClient.ReceivedMessages, never reply
// success alone. Runner-level facts pin the fenced once-only evidence
// sinks, the real-arguments tool pairing, and the takeover-loser
// zero-effects fence.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

let private waitBound = TimeSpan.FromSeconds 30.0

let private tenant = TenantId.Default

let private runnerTenant = TenantId.Create "ordinary-history-runner"

let private smokeAgentId = AgentId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FBV")

let private nullString = Unchecked.defaultof<string>

let private noSequence = Unchecked.defaultof<Nullable<int64>>

/// Builds the compose-faithful container over the given database and
/// tenant: the facade options, in-memory stores, the static tool source,
/// the optional permission policy, and the chat client the facade opts
/// into suspendable children with. Code-defined agents register on the
/// default tenant only, so agent-driven tests run on the default tenant
/// with withAgent, while the isolation test runs agentless on each tenant.
let private createServices
    (database: InMemoryDatabase)
    (owner: TenantId)
    (chatClient: IChatClient)
    (tools: AITool list)
    (policy: IPermissionPolicy | null)
    (leaseDuration: TimeSpan)
    (withAgent: bool)
    : IServiceCollection =
    let services = ServiceCollection() :> IServiceCollection

    let clientOptions = SessionClientOptions()
    clientOptions.LeaseDuration <- leaseDuration
    clientOptions.Tenant <- owner
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

                if withAgent then
                    builder.Agents.Add(
                        "smoke",
                        Func<Agent, Agent>(fun template ->
                            { template with
                                Id = smokeAgentId
                                Description = "Stable history agent mirroring the compose smoke agent."
                                Model = ModelReference.Parse("scripted/scripted")
                                SystemPrompt = "Answer briefly."
                                ToolSelection = ToolSelection()
                            })
                    )
                    |> ignore

                match Option.ofObj policy with
                | Some gate -> builder.Services.AddSingleton<IPermissionPolicy>(gate) |> ignore
                | None -> ())
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

/// Starts the local actor system, resolves the initialized client, runs the
/// work, then stops the system.
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
                do! (service :> IHostedService).StopAsync(CancellationToken.None)
            with _ ->
                ()

            return raise ex
    }

let private awaitWhat (work: Task<'T>) (what: string) : Task<'T> =
    task {
        try
            return! work.WaitAsync(waitBound, CancellationToken.None)
        with :? TimeoutException ->
            return raise (TimeoutException($"The test timed out waiting for {what}."))
    }

/// Polls a condition until it holds or the timeout lapses. Sleeps are the
/// poll cadence only: every assertion below is eventual, never timing.
let private waitFor (timeout: TimeSpan) (condition: unit -> bool) : bool =
    let deadline = DateTime.UtcNow + timeout
    let mutable holds = condition ()

    while not holds && DateTime.UtcNow < deadline do
        Thread.Sleep(25)
        holds <- condition ()

    holds

/// Opens a session against the resolving smoke agent with the given
/// options. A missing smoke row is a test bug.
let private openSmokeWith (client: SessionClient) (options: SessionOptions | null) : Task<Session> =
    task {
        let agents = client.Agents

        match agents with
        | None -> return raise (InvalidOperationException("Expected the test container to carry an agent catalog."))
        | Some catalog ->
            let! smoke = catalog.GetAgent(client.Tenant, smokeAgentId, CancellationToken.None)

            match smoke with
            | null -> return raise (InvalidOperationException("Expected the code-defined smoke agent to resolve."))
            | found ->
                return! SessionClientOperations.OpenSessionAsync(client, found.Id, options, CancellationToken.None)
    }

/// Opens a session against the resolving smoke agent with default options.
let private openSmoke (client: SessionClient) : Task<Session> = openSmokeWith client null

/// Opens a smoke session with one host instruction file: the facade
/// composes host files into the turn system prompt, so this is the
/// system-once driver.
let private openSmokeWithFiles (client: SessionClient) (path: string) : Task<Session> =
    task {
        let options = SessionOptions()
        options.HostInstructionFiles <- ResizeArray<string>([| path |]) :> IReadOnlyList<string>
        return! openSmokeWith client options
    }

/// Runs the work with a fresh host instruction file carrying a marker
/// prompt, deleting the file afterwards.
let private withInstructionFile (work: string -> Task<'T>) : Task<'T> =
    task {
        let path =
            Path.Combine(Path.GetTempPath(), "legate-history-" + Guid.NewGuid().ToString("N") + ".md")

        File.WriteAllText(path, "Follow the brief.")

        try
            return! work path
        finally
            try
                File.Delete(path)
            with _ ->
                ()
    }

/// Opens a session on a fresh agent id: agentless containers skip catalog
/// validation, so any id opens without a registered agent.
let private openBare (client: SessionClient) : Task<Session> =
    SessionClientOperations.OpenSessionAsync(client, AgentId.New(), null, CancellationToken.None)

/// Reads the session journal in sequence order, paging from the first event.
let private collectJournal (client: SessionClient) (sessionId: SessionId) : Task<SessionEvent list> =
    task {
        let collected = ResizeArray<SessionEvent>()
        let mutable cursor = 0L
        let mutable paging = true

        while paging do
            let! page = SessionClientOperations.ReadEventsAsync(client, sessionId, cursor, 100, CancellationToken.None)

            if page.Count = 0 then
                paging <- false
            else
                for event in page do
                    collected.Add(event)

                    if event.Sequence.HasValue && event.Sequence.Value > cursor then
                        cursor <- event.Sequence.Value

        return List.ofSeq collected
    }

/// Prompts with Queue delivery and waits for the settle, returning the
/// settled result.
let private promptAndSettle (client: SessionClient) (sessionId: SessionId) (text: string) : Task<TurnResult> =
    task {
        let wait =
            SessionClientOperations.WaitForSettleAsync(client, sessionId, waitBound, CancellationToken.None)

        let! _ =
            SessionClientOperations.PromptAsync(
                client,
                sessionId,
                UserMessage.Text text,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        return! awaitWhat wait "the turn to settle"
    }

/// Renders one tool result: strings pass through, anything else reads
/// as empty. Test doubles only produce strings; replayed results arrive
/// as strings through the text evidence.
let private resultTextOf (result: FunctionResultContent) : string =
    match result.Result with
    | null -> ""
    | :? string as text -> text
    | _ -> ""

/// Renders one provider-call input compactly: roles, texts, call
/// identities with canonical arguments, and paired results.
let private renderInput (messages: ChatMessage seq) : string =
    messages
    |> Seq.map (fun message ->
        if isNull (box message) then
            "<null>"
        else
            let role = message.Role.ToString()
            let text = message.Text
            let safeText = if isNull (box text) then "" else text

            let calls =
                if isNull (box message.Contents) then
                    ""
                else
                    message.Contents
                    |> Seq.choose (fun content ->
                        match content with
                        | :? FunctionCallContent as call when not (isNull (box call)) ->
                            let args =
                                if isNull (box call.Arguments) then
                                    "{}"
                                else
                                    JsonSerializer.Serialize(call.Arguments)

                            Some($"{call.Name}#{call.CallId}#{args}")
                        | _ -> None)
                    |> String.concat ","

            let results =
                if isNull (box message.Contents) then
                    ""
                else
                    message.Contents
                    |> Seq.choose (fun content ->
                        match content with
                        | :? FunctionResultContent as result when not (isNull (box result)) ->
                            Some($"{result.CallId}={resultTextOf result}")
                        | _ -> None)
                    |> String.concat ","

            $"{role}|{safeText}|{calls}|{results}")
    |> String.concat "\n"

/// Reads one call argument tolerantly: plain values pass through,
/// replayed JsonElement values unwrap, anything else renders. A missing
/// table or key reads as empty.
let private argText (args: IDictionary<string, obj | null> | null) (key: string) : string =
    match args with
    | null -> ""
    | table ->
        let found, boxed = table.TryGetValue(key)

        if not found then
            ""
        else
            match box boxed with
            | null -> ""
            | live ->
                match live with
                | :? JsonElement as element when element.ValueKind = JsonValueKind.String ->
                    match element.GetString() with
                    | null -> ""
                    | text -> text
                | _ ->
                    match live.ToString() with
                    | null -> ""
                    | rendered -> rendered

/// Permission policy asking for the gated tool, allowing the rest.
type private AskPolicy(gated: string) =
    interface IPermissionPolicy with
        member _.Evaluate(request) =
            if request.ToolName = gated then
                PermissionVerdict.Ask
            else
                PermissionVerdict.Allow

/// A weather tool recording the city it was called with.
let private weatherTool (seen: string ref) : AIFunction =
    let method =
        Func<string, string>(fun city ->
            seen.Value <- if isNull (box city) then "" else city
            $"sunny in {seen.Value}")

    AIFunctionFactory.Create(
        method,
        "get_weather",
        Unchecked.defaultof<string>,
        Unchecked.defaultof<JsonSerializerOptions>
    )

// ──────────────────────────────────────────────────────────────────────────
// Task 6: ordinary history across turns

[<Fact>]
let ``Three ordinary turns carry prior user and assistant content in order with system once`` () : Task =
    task {
        let chat =
            scripted
                [
                    textStep "reply-one"
                    textStep "reply-two"
                    textStep "reply-three"
                ]

        let database = InMemoryDatabase()

        use provider =
            (createServices database tenant (chat :> IChatClient) [] null (TimeSpan.FromHours 1.0) true)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client

                    let! first = promptAndSettle client created.Id "first"
                    first.Status |> should equal TurnStatus.Completed
                    let afterFirst = chat.ReceivedMessages.Count

                    let! second = promptAndSettle client created.Id "second"
                    second.Status |> should equal TurnStatus.Completed
                    let afterSecond = chat.ReceivedMessages.Count

                    let! third = promptAndSettle client created.Id "third"
                    third.Status |> should equal TurnStatus.Completed
                    let afterThird = chat.ReceivedMessages.Count

                    let at index = chat.ReceivedMessages[index]

                    let slice fromCount toCount =
                        [
                            for index in fromCount .. toCount - 1 -> at index
                        ]

                    // Turn one sees only its own input: no system prompt
                    // is invented when none is configured.
                    let firstInput = slice 0 afterFirst
                    firstInput.Length |> should equal 1
                    firstInput[0].Role |> should equal ChatRole.User
                    firstInput[0].Text |> should equal "first"

                    // Turn two restores turn one's user and committed
                    // assistant content ahead of its own input, in order,
                    // with no system message invented.
                    let secondInput = slice afterFirst afterSecond
                    secondInput.Length |> should equal 3
                    secondInput[0].Role |> should equal ChatRole.User
                    secondInput[0].Text |> should equal "first"
                    secondInput[1].Role |> should equal ChatRole.Assistant
                    secondInput[1].Text |> should equal "reply-one"
                    secondInput[2].Role |> should equal ChatRole.User
                    secondInput[2].Text |> should equal "second"

                    secondInput
                    |> List.filter (fun message -> message.Role = ChatRole.System)
                    |> should haveLength 0

                    // Turn three extends the same ordered prefix: no
                    // duplicate logical messages, no resumption note.
                    let thirdInput = slice afterSecond afterThird
                    thirdInput.Length |> should equal 5
                    thirdInput[0].Text |> should equal "first"
                    thirdInput[1].Text |> should equal "reply-one"
                    thirdInput[2].Text |> should equal "second"
                    thirdInput[3].Text |> should equal "reply-two"
                    thirdInput[4].Text |> should equal "third"

                    thirdInput
                    |> List.filter (fun message -> message.Role = ChatRole.System)
                    |> should haveLength 0

                    // Ordinary turns never carry the crash resumption note.
                    for message in thirdInput do
                        message.Text.Contains(SessionActor.CrashResumptionNote) |> should equal false
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 7: fidelity and equivalence

[<Fact>]
let ``Composed system prompt leads exactly once across turns`` () : Task =
    task {
        let chat =
            scripted
                [
                    textStep "reply-one"
                    textStep "reply-two"
                ]

        let database = InMemoryDatabase()

        use provider =
            (createServices database tenant (chat :> IChatClient) [] null (TimeSpan.FromHours 1.0) true)
                .BuildServiceProvider()

        return!
            withInstructionFile (fun path ->
                withClient provider (fun client ->
                    task {
                        let! created = openSmokeWithFiles client path

                        let! first = promptAndSettle client created.Id "first"
                        first.Status |> should equal TurnStatus.Completed
                        let afterFirst = chat.ReceivedMessages.Count

                        let! second = promptAndSettle client created.Id "second"
                        second.Status |> should equal TurnStatus.Completed

                        let at index = chat.ReceivedMessages[index]

                        let firstInput =
                            [
                                for index in 0 .. afterFirst - 1 -> at index
                            ]

                        // The composed host prompt leads the first turn,
                        // exactly once, verbatim.
                        firstInput.Length |> should equal 2
                        firstInput[0].Role |> should equal ChatRole.System
                        firstInput[0].Text |> should equal "Follow the brief."
                        firstInput[1].Text |> should equal "first"

                        let secondInput =
                            [
                                for index in afterFirst .. chat.ReceivedMessages.Count - 1 -> at index
                            ]

                        // The second turn restores the prefix behind the
                        // same single leading system message: history,
                        // recovery, and continuation never duplicate it.
                        secondInput.Length |> should equal 4
                        secondInput[0].Role |> should equal ChatRole.System
                        secondInput[0].Text |> should equal "Follow the brief."
                        secondInput[1].Text |> should equal "first"
                        secondInput[2].Text |> should equal "reply-one"
                        secondInput[3].Text |> should equal "second"

                        secondInput
                        |> List.filter (fun message -> message.Role = ChatRole.System)
                        |> should haveLength 1
                    }))
    }

[<Fact>]
let ``Ordinary tool pairing retains actual ids names arguments and paired results`` () : Task =
    task {
        let seen = ref ""

        let args = Dictionary<string, obj>()
        args["city"] <- "Oslo" :> obj

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "get_weather", args :> IDictionary<string, obj>)
                    ScriptStep.Text "done-one"
                    ScriptStep.Text "done-two"
                ]

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                tenant
                (chat :> IChatClient)
                [ weatherTool seen :> AITool ]
                null
                (TimeSpan.FromHours 1.0)
                true)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client

                    let! first = promptAndSettle client created.Id "weather in Oslo"
                    first.Status |> should equal TurnStatus.Completed
                    seen.Value |> should equal "Oslo"
                    let afterFirst = chat.ReceivedMessages.Count

                    // The tool turn spent two provider calls: the call,
                    // then the continuation with the paired result.
                    afterFirst |> should equal 4

                    let at index = chat.ReceivedMessages[index]

                    let continuation =
                        [
                            for index in 1 .. afterFirst - 1 -> at index
                        ]

                    continuation.Length |> should equal 3

                    let callMessage = continuation[1]

                    let call =
                        callMessage.Contents
                        |> Seq.pick (fun content ->
                            match content with
                            | :? FunctionCallContent as call when not (isNull (box call)) -> Some call
                            | _ -> None)

                    call.CallId |> should equal "c1"
                    call.Name |> should equal "get_weather"
                    argText call.Arguments "city" |> should equal "Oslo"

                    let resultMessage = continuation[2]

                    let paired =
                        resultMessage.Contents
                        |> Seq.pick (fun content ->
                            match content with
                            | :? FunctionResultContent as result when not (isNull (box result)) -> Some result
                            | _ -> None)

                    paired.CallId |> should equal "c1"

                    match paired.Result with
                    | :? string as text -> text |> should equal "sunny in Oslo"
                    | other -> failwith $"Expected a string tool result but rebuilt '{other}'."

                    // Turn two replays the paired exchange ahead of its own
                    // input: the call keeps its actual id, name, and
                    // arguments, and the result keeps its success content.
                    let! second = promptAndSettle client created.Id "and tomorrow?"
                    second.Status |> should equal TurnStatus.Completed

                    let replayed =
                        [
                            for index in afterFirst .. chat.ReceivedMessages.Count - 1 -> at index
                        ]

                    replayed.Length |> should equal 5

                    let replayedCall =
                        replayed[1].Contents
                        |> Seq.pick (fun content ->
                            match content with
                            | :? FunctionCallContent as call when not (isNull (box call)) -> Some call
                            | _ -> None)

                    replayedCall.CallId |> should equal "c1"
                    replayedCall.Name |> should equal "get_weather"
                    argText replayedCall.Arguments "city" |> should equal "Oslo"

                    let replayedResults =
                        replayed
                        |> List.collect (fun message ->
                            if isNull (box message) || isNull (box message.Contents) then
                                []
                            else
                                [
                                    for content in message.Contents do
                                        match content with
                                        | :? FunctionResultContent as result when not (isNull (box result)) ->
                                            yield result
                                        | _ -> ()
                                ])

                    replayedResults.Length |> should equal 1
                    replayedResults.Head.CallId |> should equal "c1"

                    // The journal carries the real arguments JSON (never a
                    // placeholder) with the paired success content.
                    let! journal = collectJournal client created.Id

                    let started =
                        journal
                        |> List.pick (fun event ->
                            match event with
                            | :? ToolCallStartedEvent as started when not (isNull (box started)) -> Some started
                            | _ -> None)

                    started.ToolCallId |> should equal "c1"
                    started.ToolName |> should equal "get_weather"
                    started.ArgumentsJson |> should not' (equal "{}")

                    match started.ArgumentsJson with
                    | null -> failwith "Expected tool arguments JSON in the journal."
                    | argsJson -> argsJson.Contains("Oslo") |> should equal true

                    let completed =
                        journal
                        |> List.pick (fun event ->
                            match event with
                            | :? ToolCallCompletedEvent as completed when not (isNull (box completed)) ->
                                Some completed
                            | _ -> None)

                    completed.ToolCallId |> should equal "c1"
                    completed.Error |> should equal nullString
                    completed.ResultText |> should equal "sunny in Oslo"
                })
    }

[<Fact>]
let ``Non-text parts survive across turns by reference`` () : Task =
    task {
        let chat = scripted [ textStep "seen"; textStep "again" ]
        let database = InMemoryDatabase()

        use provider =
            (createServices database tenant (chat :> IChatClient) [] null (TimeSpan.FromHours 1.0) true)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client
                    let payload = [| 1uy; 2uy; 3uy |]

                    let parts =
                        ResizeArray<AIContent>(
                            [|
                                TextContent("look at this") :> AIContent
                                DataContent(ReadOnlyMemory<byte>(payload), "application/pdf") :> AIContent
                            |]
                        )
                        :> IReadOnlyList<AIContent>

                    let wait =
                        SessionClientOperations.WaitForSettleAsync(
                            client,
                            created.Id,
                            waitBound,
                            CancellationToken.None
                        )

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage(parts, null),
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! first = awaitWhat wait "the file turn to settle"
                    first.Status |> should equal TurnStatus.Completed
                    let afterFirst = chat.ReceivedMessages.Count

                    let! second = promptAndSettle client created.Id "what did you see?"
                    second.Status |> should equal TurnStatus.Completed

                    let replayed =
                        [
                            for index in afterFirst .. chat.ReceivedMessages.Count - 1 -> chat.ReceivedMessages[index]
                        ]

                    // The prior user message keeps its text and its file
                    // part with the same bytes and media type.
                    let prior = replayed[0]

                    prior.Role |> should equal ChatRole.User

                    let file =
                        prior.Contents
                        |> Seq.pick (fun content ->
                            match content with
                            | :? DataContent as file when not (isNull (box file)) -> Some file
                            | _ -> None)

                    file.MediaType |> should equal "application/pdf"
                    (file.Data.ToArray()) |> should equal payload

                    prior.Text.Contains("look at this") |> should equal true
                })
    }

[<Fact>]
let ``Suspension resume keeps ordering pairing and non-duplication`` () : Task =
    task {
        let seen = ref ""

        let args = Dictionary<string, obj>()
        args["city"] <- "Oslo" :> obj

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "get_weather", args :> IDictionary<string, obj>)
                    ScriptStep.Text "resumed-done"
                    ScriptStep.Text "second-done"
                ]

        let database = InMemoryDatabase()

        use provider =
            (createServices
                database
                tenant
                (chat :> IChatClient)
                [ weatherTool seen :> AITool ]
                (AskPolicy("get_weather") :> IPermissionPolicy)
                (TimeSpan.FromHours 1.0)
                true)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client

                    let waiter =
                        SessionClientOperations.WaitForSettleAsync(
                            client,
                            created.Id,
                            waitBound,
                            CancellationToken.None
                        )

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "weather in Oslo",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let suspended =
                        waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                            let stored =
                                client.Store
                                    .GetSession(client.Tenant, created.Id, CancellationToken.None)
                                    .GetAwaiter()
                                    .GetResult()

                            match stored with
                            | null -> false
                            | live -> live.State = SessionState.WaitingForInput)

                    Assert.True(suspended, "The turn should suspend on the permission Ask.")

                    let! journal = collectJournal client created.Id

                    let request =
                        journal
                        |> List.pick (fun event ->
                            match event with
                            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) -> Some asked
                            | _ -> None)

                    let! _ =
                        SessionClientOperations.ReplyAsync(
                            client,
                            created.Id,
                            PermissionDecision(request.RequestId, PermissionDecisionKind.AllowOnce) :> Reply,
                            CancellationToken.None
                        )

                    let! first = awaitWhat waiter "the resumed turn to settle"
                    first.Status |> should equal TurnStatus.Completed
                    seen.Value |> should equal "Oslo"
                    let afterFirst = chat.ReceivedMessages.Count

                    let! second = promptAndSettle client created.Id "and tomorrow?"
                    second.Status |> should equal TurnStatus.Completed

                    let replayed =
                        [
                            for index in afterFirst .. chat.ReceivedMessages.Count - 1 -> chat.ReceivedMessages[index]
                        ]

                    // The suspended-then-resumed exchange replays exactly
                    // once: one call, one paired result, in order, between
                    // the two user inputs.
                    let calls =
                        replayed
                        |> List.collect (fun message ->
                            if isNull (box message) || isNull (box message.Contents) then
                                []
                            else
                                [
                                    for content in message.Contents do
                                        match content with
                                        | :? FunctionCallContent as call when not (isNull (box call)) -> yield call
                                        | _ -> ()
                                ])

                    calls.Length |> should equal 1
                    calls.Head.CallId |> should equal "c1"
                    calls.Head.Name |> should equal "get_weather"
                    argText calls.Head.Arguments "city" |> should equal "Oslo"

                    let results =
                        replayed
                        |> List.collect (fun message ->
                            if isNull (box message) || isNull (box message.Contents) then
                                []
                            else
                                [
                                    for content in message.Contents do
                                        match content with
                                        | :? FunctionResultContent as result when not (isNull (box result)) ->
                                            yield result
                                        | _ -> ()
                                ])

                    results.Length |> should equal 1
                    results.Head.CallId |> should equal "c1"

                    let callIndex =
                        replayed
                        |> List.findIndex (fun message ->
                            message.Contents |> Seq.exists (fun content -> content :? FunctionCallContent))

                    let resultIndex =
                        replayed
                        |> List.findIndex (fun message ->
                            message.Contents |> Seq.exists (fun content -> content :? FunctionResultContent))

                    (callIndex < resultIndex) |> should equal true

                    let users =
                        replayed
                        |> List.filter (fun message -> not (isNull (box message)) && message.Role = ChatRole.User)

                    users.Length |> should equal 2
                    users[0].Text |> should equal "weather in Oslo"
                    users[1].Text |> should equal "and tomorrow?"

                    // Each accepted prompt evidenced exactly once, even
                    // across the suspend/resume boundary: no duplicates.
                    let! settled = collectJournal client created.Id

                    let evidenced =
                        settled
                        |> List.choose (fun event ->
                            match event with
                            | :? UserMessageEvent as user when not (isNull (box user)) -> Some user
                            | _ -> None)

                    evidenced.Length |> should equal 2

                    let markers =
                        settled
                        |> List.choose (fun event ->
                            match event with
                            | :? TurnStartedEvent as marker when not (isNull (box marker)) -> Some marker
                            | _ -> None)

                    markers.Length |> should equal 2

                    (evidenced[0].TurnId = markers[0].TurnId) |> should equal true
                    (evidenced[1].TurnId = markers[1].TurnId) |> should equal true
                })
    }

[<Fact>]
let ``Reopen produces equivalent provider input`` () : Task =
    task {
        let prompts = [ "first"; "second"; "third" ]

        // Continuous run: three turns straight on one provider.
        let chatFull =
            scripted
                [
                    textStep "reply-one"
                    textStep "reply-two"
                    textStep "reply-three"
                ]

        let databaseFull = InMemoryDatabase()

        use providerFull =
            (createServices databaseFull tenant (chatFull :> IChatClient) [] null (TimeSpan.FromHours 1.0) true)
                .BuildServiceProvider()

        let! continuous =
            withClient providerFull (fun client ->
                task {
                    let! created = openSmoke client

                    for prompt in prompts |> List.take 2 do
                        let! settled = promptAndSettle client created.Id prompt
                        settled.Status |> should equal TurnStatus.Completed

                    let before = chatFull.ReceivedMessages.Count
                    let! last = promptAndSettle client created.Id (prompts |> List.item 2)
                    last.Status |> should equal TurnStatus.Completed

                    return
                        [
                            for index in before .. chatFull.ReceivedMessages.Count - 1 ->
                                chatFull.ReceivedMessages[index]
                        ]
                })

        // Reopened run: two turns, then a fresh provider over the same
        // database (no shared actor state, journal only) runs turn three
        // on the same session.
        let database = InMemoryDatabase()

        let chatFirst =
            scripted
                [
                    textStep "reply-one"
                    textStep "reply-two"
                ]

        use providerFirst =
            (createServices database tenant (chatFirst :> IChatClient) [] null (TimeSpan.FromHours 1.0) true)
                .BuildServiceProvider()

        let! sessionId =
            withClient providerFirst (fun client ->
                task {
                    let! created = openSmoke client

                    for prompt in prompts |> List.take 2 do
                        let! settled = promptAndSettle client created.Id prompt
                        settled.Status |> should equal TurnStatus.Completed

                    return created.Id
                })

        let chatThird = scripted [ textStep "reply-three" ]

        use providerReopened =
            (createServices database tenant (chatThird :> IChatClient) [] null (TimeSpan.FromHours 1.0) true)
                .BuildServiceProvider()

        let! reopened =
            withClient providerReopened (fun client ->
                task {
                    let before = chatThird.ReceivedMessages.Count
                    let! last = promptAndSettle client sessionId (prompts |> List.item 2)
                    last.Status |> should equal TurnStatus.Completed

                    return
                        [
                            for index in before .. chatThird.ReceivedMessages.Count - 1 ->
                                chatThird.ReceivedMessages[index]
                        ]
                })

        renderInput reopened |> should equal (renderInput continuous)
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 8: isolation and failure

[<Fact>]
let ``Sessions and tenants stay isolated across queue inject and reopen`` () : Task =
    task {
        let database = InMemoryDatabase()

        let chatA =
            scripted
                [
                    textStep "a-one"
                    textStep "a-steer"
                    textStep "a-two"
                ]

        let chatB = scripted [ textStep "b-one" ]

        use providerA =
            (createServices
                database
                (TenantId.Create "iso-a")
                (chatA :> IChatClient)
                []
                null
                (TimeSpan.FromHours 1.0)
                false)
                .BuildServiceProvider()

        use providerB =
            (createServices
                database
                (TenantId.Create "iso-b")
                (chatB :> IChatClient)
                []
                null
                (TimeSpan.FromHours 1.0)
                false)
                .BuildServiceProvider()

        let! inputA3 =
            withClient providerA (fun client ->
                task {
                    let! created = openBare client

                    let! one = promptAndSettle client created.Id "alpha-one"
                    one.Status |> should equal TurnStatus.Completed

                    // Inject while idle starts its own turn: the steered
                    // input journals once as executed user content.
                    let steerWait =
                        SessionClientOperations.WaitForSettleAsync(
                            client,
                            created.Id,
                            waitBound,
                            CancellationToken.None
                        )

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "alpha-steer",
                            DeliveryMode.Inject,
                            CancellationToken.None
                        )

                    let! steered = awaitWhat steerWait "the steered turn to settle"
                    steered.Status |> should equal TurnStatus.Completed

                    let before = chatA.ReceivedMessages.Count
                    let! two = promptAndSettle client created.Id "alpha-two"
                    two.Status |> should equal TurnStatus.Completed

                    return
                        [
                            for index in before .. chatA.ReceivedMessages.Count - 1 -> chatA.ReceivedMessages[index]
                        ]
                })

        let! inputB1 =
            withClient providerB (fun client ->
                task {
                    let! created = openBare client
                    let before = chatB.ReceivedMessages.Count
                    let! one = promptAndSettle client created.Id "beta-one"
                    one.Status |> should equal TurnStatus.Completed

                    return
                        [
                            for index in before .. chatB.ReceivedMessages.Count - 1 -> chatB.ReceivedMessages[index]
                        ]
                })

        // Tenant A's third turn carries its Queue and Inject inputs exactly
        // once each, plus its own replies, and nothing from tenant B.
        let textsA =
            inputA3
            |> List.map (fun message -> if isNull (box message.Text) then "" else message.Text)

        (textsA |> List.filter (fun text -> text = "alpha-one")).Length
        |> should equal 1

        (textsA |> List.filter (fun text -> text = "alpha-steer")).Length
        |> should equal 1

        (textsA |> List.filter (fun text -> text = "alpha-two")).Length
        |> should equal 1

        (textsA |> List.exists (fun text -> text.Contains("beta")))
        |> should equal false

        // Tenant B sees only its own input.
        let textsB =
            inputB1
            |> List.map (fun message -> if isNull (box message.Text) then "" else message.Text)

        textsB.Length |> should equal 1

        (textsB |> List.exists (fun text -> text.Contains("alpha")))
        |> should equal false

        // A same-tenant second session carries none of the first session's
        // content.
        let chatA2 = scripted [ textStep "a2-one" ]

        use providerA2 =
            (createServices
                database
                (TenantId.Create "iso-a")
                (chatA2 :> IChatClient)
                []
                null
                (TimeSpan.FromHours 1.0)
                false)
                .BuildServiceProvider()

        let! inputA2 =
            withClient providerA2 (fun client ->
                task {
                    let! created = openBare client
                    let before = chatA2.ReceivedMessages.Count
                    let! one = promptAndSettle client created.Id "alpha2-one"
                    one.Status |> should equal TurnStatus.Completed

                    return
                        [
                            for index in before .. chatA2.ReceivedMessages.Count - 1 -> chatA2.ReceivedMessages[index]
                        ]
                })

        let textsA2 =
            inputA2
            |> List.map (fun message -> if isNull (box message.Text) then "" else message.Text)

        (textsA2 |> List.exists (fun text -> text.Contains("alpha-one")))
        |> should equal false

        (textsA2 |> List.exists (fun text -> text.Contains("beta")))
        |> should equal false
    }

[<Fact>]
let ``Unpaired history fails before any provider call`` () : Task =
    task {
        let chat =
            scripted
                [
                    textStep "first-done"
                    textStep "never"
                ]

        let database = InMemoryDatabase()

        use provider =
            (createServices database tenant (chat :> IChatClient) [] null (TimeSpan.FromHours 1.0) true)
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client

                    let! first = promptAndSettle client created.Id "first"
                    first.Status |> should equal TurnStatus.Completed
                    let callsAfterFirst = chat.Calls

                    // Craft an unpaired completion directly in the journal:
                    // a success result with no call start to pair it with.
                    let eventStore = provider.GetRequiredService<ISessionEventStore>()

                    let! storedOpt = client.Store.GetSession(client.Tenant, created.Id, CancellationToken.None)

                    let stored =
                        match storedOpt with
                        | null -> failwith "Expected the session row to exist."
                        | row -> row

                    let orphan =
                        ToolCallCompletedEvent(
                            created.Id,
                            TurnId.New(),
                            noSequence,
                            DateTimeOffset.UtcNow,
                            "c-orphan",
                            nullString,
                            "orphan result"
                        )
                        :> SessionEvent

                    let! outcome =
                        eventStore.AppendHostEvents(
                            client.Tenant,
                            created.Id,
                            stored.UpdatedAt,
                            ResizeArray<SessionEvent>([| orphan |]) :> IReadOnlyList<SessionEvent>,
                            CancellationToken.None
                        )

                    match outcome with
                    | :? EventAppended -> ()
                    | other -> failwith $"Expected the orphan append to land but got '{other}'."

                    let! second = promptAndSettle client created.Id "second"
                    second.Status |> should equal TurnStatus.Failed

                    match second.Outcome with
                    | :? TurnFailed as failed -> failed.Reason.Contains("without its call") |> should equal true
                    | _ -> failwith "Expected a TurnFailed outcome naming the unpaired completion."

                    // No provider call ran for the faulted turn, and the
                    // fault journaled nothing new for it: no marker, no
                    // user evidence.
                    chat.Calls |> should equal callsAfterFirst

                    let! journal = collectJournal client created.Id

                    (journal |> List.filter (fun event -> event :? TurnStartedEvent)).Length
                    |> should equal 1

                    (journal |> List.filter (fun event -> event :? UserMessageEvent)).Length
                    |> should equal 1
                })
    }

[<Fact>]
let ``Unsupported old persistence rejects with a clean-start requirement`` () =
    let refusal =
        try
            SessionOptionsPersistence.Deserialize """{"FormatVersion":0,"Outcome":0,"OnCrashResume":0}"""
            |> ignore

            failwith "Expected CompletionRoutingException with UnsupportedFormat."
        with :? CompletionRoutingException as refusal ->
            refusal

    refusal.Reason |> should equal CompletionRoutingReason.UnsupportedFormat
    refusal.Message.Contains("clean-start") |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Tasks 3 and 9: runner-level fencing and fidelity

let private runnerLease = TimeSpan.FromMinutes 5.0

/// An empty store pair over one database: the session store and the fenced
/// journal share claim state, so primed tokens fence for real.
let private createRunnerStores (clock: TimeProvider) : ISessionStore * ISessionEventStore =
    let database = InMemoryDatabase(clock)
    InMemorySessionStore(database) :> ISessionStore, InMemorySessionEventStore(database) :> ISessionEventStore

/// A minimal Idle session row.
let private runnerSession () =
    {
        Id = SessionId.New()
        Tenant = runnerTenant
        AgentId = AgentId.New()
        Title = "runner"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private createRunnerSession (store: ISessionStore) : Session =
    store.CreateSession(runnerTenant, runnerSession (), CancellationToken.None).GetAwaiter().GetResult()

let private appendRunnerQueue (store: ISessionStore) (sessionId: SessionId) (text: string) : InboxEntry =
    let payload = UserMessagePayload(UserMessage.Text text) :> InboxPayload

    store
        .AppendInboxMessage(runnerTenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
        .GetAwaiter()
        .GetResult()

let private claimOfState (state: TurnLeaseState) : TurnClaim =
    match state with
    | :? TurnLeaseRenewed as renewed when not (isNull (box renewed)) -> renewed.Claim
    | :? TurnLeaseHeld as held when not (isNull (box held)) -> held.Claim
    | :? TurnLeaseExpiring as expiring when not (isNull (box expiring)) -> expiring.Claim
    | other -> failwithf "Expected a granted claim, got %O." other

let private claimRunnerLive (store: ISessionStore) (sessionId: SessionId) (owner: string) : TurnClaim =
    store.ClaimNextTurn(runnerTenant, sessionId, owner, runnerLease, CancellationToken.None).GetAwaiter().GetResult()
    |> claimOfState

/// Builds the production runner over a scripted client and tool map.
let private productionHistoryRunner
    (store: ISessionStore)
    (journal: ISessionEventStore)
    (client: ScriptedChatClient)
    (tools: IReadOnlyDictionary<string, AITool>)
    : SessionActor.SuspendableRunner =
    SessionPermissions.createRunner
        (client :> IChatClient)
        store
        runnerTenant
        (fun _ -> (tools, TurnLoop.TurnLoopOptions.Default))
        (NeverDelay() :> ILlmDelay)
        null
        None
        journal
        SessionStreaming.defaultBounds
        None

let private runRunnerFresh (runner: SessionActor.SuspendableRunner) (entry: InboxEntry) : TurnLoop.TurnLoopCompletion =
    runner entry 1 (HashSet<string>()) None None None CancellationToken.None None None None (TurnId.New())
    |> fun task -> task.GetAwaiter().GetResult()

[<Fact>]
let ``Stale loser journals zero evidence and performs zero provider effects`` () =
    let clock = FakeClock()
    let store, journal = createRunnerStores clock
    let created = createRunnerSession store
    let entry = appendRunnerQueue store created.Id "first"
    let loser = claimRunnerLive store created.Id "owner-a"

    clock.Advance(TimeSpan.FromMinutes 10.0)
    appendRunnerQueue store created.Id "second" |> ignore
    claimRunnerLive store created.Id "owner-b" |> ignore

    let client = scripted [ textStep "loser-text" ]
    let tools = Dictionary<string, AITool>() :> IReadOnlyDictionary<string, AITool>
    let runner = productionHistoryRunner store journal client tools

    let attempt () =
        use _c = ControlAdmission.enter (fun () -> true)
        use _l = LeaseAdmission.enter (fun () -> true)
        use _f = FencedClaimScope.enter (Some loser)

        runRunnerFresh runner entry |> ignore

    (fun () -> attempt ()) |> should throw typeof<TurnLoop.TurnLeaseLostException>
    client.Calls |> should equal 0

    let outcome =
        journal.Replay(runnerTenant, created.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult()

    match outcome with
    | :? EventReplayPage as page when not (isNull (box page)) ->
        (isNull (box page.Events) || page.Events.Count = 0) |> should equal true
    | :? EventReplayEndOfStream -> ()
    | other -> failwithf "Expected an empty replay, got %O." other

[<Fact>]
let ``Tool sink journals real arguments with paired results`` () =
    let clock = FakeClock()
    let store, journal = createRunnerStores clock
    let created = createRunnerSession store
    let entry = appendRunnerQueue store created.Id "weather in Oslo"
    let claim = claimRunnerLive store created.Id "owner-a"
    let seen = ref ""

    let args = Dictionary<string, obj>()
    args["city"] <- "Oslo" :> obj

    let client =
        scripted
            [
                ScriptStep.ToolCall("c9", "get_weather", args :> IDictionary<string, obj>)
                textStep "done"
            ]

    let tools = makeTools [ "get_weather", weatherTool seen ]
    let runner = productionHistoryRunner store journal client tools

    use _c = ControlAdmission.enter (fun () -> true)
    use _l = LeaseAdmission.enter (fun () -> true)
    use _f = FencedClaimScope.enter (Some claim)

    let completion = runRunnerFresh runner entry
    completion.Result.Status |> should equal TurnStatus.Completed
    seen.Value |> should equal "Oslo"

    // One user evidence, the atomic Started/Output/Completed triple, and
    // the non-streaming text delta: unclaimed shells sink nothing, and the
    // loser path above proves a fenced-out claim sinks nothing either.
    let outcome =
        journal.Replay(runnerTenant, created.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult()

    let events =
        match outcome with
        | :? EventReplayPage as page when not (isNull (box page)) && not (isNull (box page.Events)) ->
            List.ofSeq page.Events
        | other -> failwithf "Expected a journal page, got %O." other

    events.Length |> should equal 5
    events[0] |> should be ofExactType<UserMessageEvent>
    events[1] |> should be ofExactType<ToolCallStartedEvent>
    events[2] |> should be ofExactType<ToolCallOutputEvent>
    events[3] |> should be ofExactType<ToolCallCompletedEvent>
    events[4] |> should be ofExactType<TextDeltaEvent>

    let started = events[1] :?> ToolCallStartedEvent
    started.ToolCallId |> should equal "c9"
    started.ToolName |> should equal "get_weather"
    started.ArgumentsJson |> should not' (equal "{}")

    match started.ArgumentsJson with
    | null -> failwith "Expected tool arguments JSON in the journal."
    | argsJson -> argsJson.Contains("Oslo") |> should equal true

    let completed = events[3] :?> ToolCallCompletedEvent
    completed.ToolCallId |> should equal "c9"
    completed.Error |> should equal nullString
    completed.ResultText |> should equal "sunny in Oslo"

    // The continuation input pairs the same call with its result.
    client.Calls |> should equal 2

    let secondInput =
        [
            for index in 1 .. client.ReceivedMessages.Count - 1 -> client.ReceivedMessages[index]
        ]

    secondInput.Length |> should equal 3
