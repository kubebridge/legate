// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TurnInCallMarkerTests

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

// In-call marker (issue 284): a fenced TurnStartedEvent journaled at
// TurnLoop provider-call entry, so subscribers observe an executing turn
// in-call. Every fact below drives the production facade path (agent
// authority gate plus SessionPermissions runner over a code-defined smoke
// agent, mirroring the MinimalHost compose shape) over fenced in-memory
// stores: no live keys, no external services. Waits are event-driven with
// a thirty-second bound, never sleeps, except the short takeover lapse
// and the absence poll, which assert steady state after it.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

let private waitBound = TimeSpan.FromSeconds 30.0

let private smokeAgentId = AgentId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FAV")

/// Non-streaming echo client with a fixed reply delay, mirroring
/// MinimalHost's EchoChatClient (declines streaming so the loop falls back
/// to GetResponseAsync). Counts provider calls, so the fencing fact proves
/// a takeover loser performs zero effects, not just zero journal writes.
type private DelayEchoClient(delay: TimeSpan, calls: int ref) =
    interface IChatClient with
        member _.GetResponseAsync(_, _, cancellationToken) =
            task {
                calls.Value <- calls.Value + 1
                cancellationToken.ThrowIfCancellationRequested()
                do! Task.Delay(delay, cancellationToken)
                return ChatResponse(ChatMessage(ChatRole.Assistant, "marker scripted reply"))
            }

        member _.GetStreamingResponseAsync(_, _, _) =
            raise (NotSupportedException("The marker client is non-streaming."))

        member _.GetService(_, _) = null
        member _.Dispose() = ()

/// Permission policy asking for the gated tool, allowing the rest.
type private AskPolicy(gated: string) =
    interface IPermissionPolicy with
        member _.Evaluate(request) =
            if request.ToolName = gated then
                PermissionVerdict.Ask
            else
                PermissionVerdict.Allow

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

/// Builds the compose-faithful container: the facade options (host
/// registrations win over the facade default), in-memory stores over one
/// database, the code-defined smoke agent on the default tenant, the
/// static tool source, the optional permission policy, and the chat
/// client the facade opts into suspendable children with.
let private createServices
    (chatClient: IChatClient)
    (tools: AITool list)
    (policy: IPermissionPolicy | null)
    (leaseDuration: TimeSpan)
    : IServiceCollection =
    let database = InMemoryDatabase()
    let services = ServiceCollection() :> IServiceCollection

    let clientOptions = SessionClientOptions()
    clientOptions.LeaseDuration <- leaseDuration
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
                            Description = "Stable marker agent mirroring the compose smoke agent."
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

/// Opens a session against the resolving smoke agent. A missing smoke
/// row is a test bug: the container registers it code-defined.
let private openSmoke (client: SessionClient) : Task<Session> =
    task {
        let agents = client.Agents

        match agents with
        | None -> return raise (InvalidOperationException("Expected the test container to carry an agent catalog."))
        | Some catalog ->
            let! smoke = catalog.GetAgent(TenantId.Default, smokeAgentId, CancellationToken.None)

            match smoke with
            | null -> return raise (InvalidOperationException("Expected the code-defined smoke agent to resolve."))
            | found -> return! SessionClientOperations.OpenSessionAsync(client, found.Id, null, CancellationToken.None)
    }

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

/// Reads the stored session row. Tests only read rows they created, so a
/// missing row is a test bug.
let private storedOf (client: SessionClient) (sessionId: SessionId) : Task<Session> =
    task {
        let! found = client.Store.GetSession(client.Tenant, sessionId, CancellationToken.None)

        match found with
        | null -> return raise (InvalidOperationException("Expected the session row to exist."))
        | session -> return session
    }

let private scripted (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

let private tool (name: string) (result: string) : AITool =
    let method = Func<string>(fun () -> result)

    AIFunctionFactory.Create(method, name, Unchecked.defaultof<string>, Unchecked.defaultof<JsonSerializerOptions>)
    :> AITool

/// Reads the text parts of an accepted user message in order.
let private userTextOf (message: UserMessage) : string =
    if isNull (box message) || isNull (box message.Parts) then
        ""
    else
        message.Parts
        |> Seq.choose (fun part ->
            match part with
            | :? TextContent as text when not (isNull (box text)) ->
                Some(if isNull (box text.Text) then "" else text.Text)
            | _ -> None)
        |> String.concat ""

// ──────────────────────────────────────────────────────────────────────────
// First write (tasks 2+3)

[<Fact>]
let ``First journal writes are user evidence then the in-call marker and the turn settles promptly`` () : Task =
    task {
        let calls = ref 0

        use provider =
            (createServices
                (new DelayEchoClient(TimeSpan.FromMilliseconds 500.0, calls) :> IChatClient)
                []
                null
                (TimeSpan.FromHours 1.0))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client

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
                            UserMessage.Text "hello",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! result = awaitWhat wait "the turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                    result.AssistantText |> should equal "marker scripted reply"

                    let! journal = collectJournal client created.Id
                    journal.Length |> should be (greaterThanOrEqualTo 2)

                    // User evidence journals once before the first provider
                    // call (issue 366); the in-call marker follows it.
                    journal.Head |> should be ofExactType<UserMessageEvent>

                    let evidence = journal.Head :?> UserMessageEvent
                    evidence.SessionId |> should equal created.Id
                    userTextOf evidence.Message |> should equal "hello"

                    journal.Tail.Head |> should be ofExactType<TurnStartedEvent>

                    let marker = journal.Tail.Head :?> TurnStartedEvent
                    marker.SessionId |> should equal created.Id

                    // The settle observation lands before the Idle row write
                    // (notify precedes UpdateSessionState), so poll for the
                    // terminal row state rather than reading it once. The
                    // poll never blocks the actor thread: waiter
                    // continuations run asynchronously (RunContinuationsAsynchronously
                    // on the hub), so the settle it waits for always lands.
                    let idled =
                        waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                            let stored =
                                client.Store
                                    .GetSession(client.Tenant, created.Id, CancellationToken.None)
                                    .GetAwaiter()
                                    .GetResult()

                            match stored with
                            | null -> false
                            | live -> live.State = SessionState.Idle)

                    Assert.True(idled, "The session should return to Idle after the settle.")

                    let! pending = client.Store.ReadPendingInbox(client.Tenant, created.Id, CancellationToken.None)

                    pending.Count |> should equal 0
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Fencing: a takeover loser journals nothing and calls nothing

[<Fact>]
let ``Takeover loser journals nothing and never reaches the provider`` () : Task =
    task {
        let calls = ref 0

        use provider =
            (createServices
                (new DelayEchoClient(TimeSpan.FromMilliseconds 100.0, calls) :> IChatClient)
                []
                null
                (TimeSpan.FromMilliseconds 100.0))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client

                    // Lapse the spawn-primed claim, then take over from the
                    // side: the actor cell still holds the primed token, so
                    // the next turn runs as the loser. The dummy entry is
                    // consumed by the takeover claim and never runs as a turn.
                    do! Task.Delay(TimeSpan.FromMilliseconds 500.0)

                    let! _ =
                        client.Store.AppendInboxMessage(
                            client.Tenant,
                            created.Id,
                            UserMessagePayload(UserMessage.Text "takeover consumed") :> InboxPayload,
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! _ =
                        client.Store.ClaimNextTurn(
                            client.Tenant,
                            created.Id,
                            "takeover",
                            TimeSpan.FromHours 1.0,
                            CancellationToken.None
                        )

                    let! refused =
                        Assert.ThrowsAsync<InvalidSessionStateException>(fun () ->
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "loser turn",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))

                    Assert.Equal("executionAuthorityUnavailable", refused.CurrentState)

                    let settled =
                        waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                            let stored =
                                client.Store
                                    .GetSession(client.Tenant, created.Id, CancellationToken.None)
                                    .GetAwaiter()
                                    .GetResult()

                            match stored with
                            | null -> false
                            | live -> live.State = SessionState.Idle)

                    Assert.True(settled, "The loser turn should settle back to Idle.")

                    // The fence held: no marker row, no provider call, no
                    // observed settle. The faulted settle consumes and idles
                    // without observing, so the hub stays empty.
                    let! journal = collectJournal client created.Id
                    journal |> should be Empty

                    calls.Value |> should equal 0

                    (PromptWaitHubs.GetOrAddScoped client.Tenant created.Id).Settled.Count
                    |> should equal 0

                    let! pending = client.Store.ReadPendingInbox(client.Tenant, created.Id, CancellationToken.None)

                    pending.Count |> should equal 1
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Observability: the subscriber sees the marker pre-settle

[<Fact>]
let ``Subscriber observes the in-call marker while the turn is still running`` () : Task =
    task {
        let calls = ref 0

        use provider =
            (createServices
                (new DelayEchoClient(TimeSpan.FromSeconds 3.0, calls) :> IChatClient)
                []
                null
                (TimeSpan.FromHours 1.0))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client

                    let stream =
                        SessionClientOperations.Subscribe(client, created.Id, 0L, CancellationToken.None)

                    let enumerator = stream.GetAsyncEnumerator(CancellationToken.None)

                    try
                        let! _ =
                            SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "hello",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            )

                        let! has = awaitWhat (enumerator.MoveNextAsync().AsTask()) "the user evidence"
                        Assert.True(has)
                        enumerator.Current |> should be ofExactType<UserMessageEvent>

                        let! marked = awaitWhat (enumerator.MoveNextAsync().AsTask()) "the in-call marker"
                        Assert.True(marked)
                        enumerator.Current |> should be ofExactType<TurnStartedEvent>

                        // Pre-settle: the turn is still inside the delayed
                        // provider call, so the session reads Running.
                        let! running = storedOf client created.Id
                        running.State |> should equal SessionState.Running

                        let! result =
                            awaitWhat
                                (SessionClientOperations.WaitForSettleAsync(
                                    client,
                                    created.Id,
                                    waitBound,
                                    CancellationToken.None
                                ))
                                "the turn to settle after the marker"

                        result.Status |> should equal TurnStatus.Completed
                    finally
                        enumerator.DisposeAsync().AsTask() |> ignore

                    // The evidence opens the journal: sequences run gap-free
                    // from 1 with no duplicates across the handoff.
                    let! journal = collectJournal client created.Id
                    journal.Length |> should be (greaterThanOrEqualTo 2)
                    journal.Head |> should be ofExactType<UserMessageEvent>
                    journal.Tail.Head |> should be ofExactType<TurnStartedEvent>

                    journal
                    |> List.mapi (fun index event -> index, event)
                    |> List.iter (fun (index, event) ->
                        Assert.True(event.Sequence.HasValue)
                        event.Sequence.Value |> should equal (int64 index + 1L))
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Exactly once per turn: iterations and resumes never double-mark

[<Fact>]
let ``Multi-iteration turn marks exactly once`` () : Task =
    task {
        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "echo")
                    ScriptStep.Text "done"
                ]

        use provider =
            (createServices (chat :> IChatClient) [ tool "echo" "ok" ] null (TimeSpan.FromHours 1.0))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client

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
                            UserMessage.Text "run the tool",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! result = awaitWhat wait "the tool turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                    result.Iterations |> should equal 2

                    // Two provider calls, one marker: later iterations never
                    // refire the hook. The settled turn also journals its
                    // terminal completion row (issue 289), coexisting with
                    // the single marker, plus the progressive text delta
                    // (issue 379), the once-per-turn user evidence, and the
                    // settled tool's Started/Output/Completed markers
                    // (issue 366).
                    let! journal = collectJournal client created.Id
                    journal.Length |> should equal 7
                    journal[0] |> should be ofExactType<UserMessageEvent>
                    journal[1] |> should be ofExactType<TurnStartedEvent>
                    journal[2] |> should be ofExactType<ToolCallStartedEvent>
                    journal[3] |> should be ofExactType<ToolCallOutputEvent>
                    journal[4] |> should be ofExactType<ToolCallCompletedEvent>
                    journal[5] |> should be ofExactType<TextDeltaEvent>
                    journal[6] |> should be ofExactType<TurnCompletedEvent>

                    // The empty-arguments scripted call journals the empty
                    // object: empty stays empty, never a placeholder.
                    let started = journal[2] :?> ToolCallStartedEvent
                    started.ToolCallId |> should equal "c1"
                    started.ToolName |> should equal "echo"
                    started.ArgumentsJson |> should equal "{}"

                    let completed = journal[4] :?> ToolCallCompletedEvent
                    completed.ToolCallId |> should equal "c1"
                    completed.ResultText |> should equal "ok"

                    journal
                    |> List.filter (fun event -> event :? TurnStartedEvent)
                    |> should haveLength 1
                })
    }

[<Fact>]
let ``Ask resume marks exactly once`` () : Task =
    task {
        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "gated")
                    ScriptStep.Text "done"
                ]

        use provider =
            (createServices
                (chat :> IChatClient)
                [ tool "gated" "allowed result" ]
                (AskPolicy("gated") :> IPermissionPolicy)
                (TimeSpan.FromHours 1.0))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSmoke client

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
                            UserMessage.Text "run the gated tool",
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
                    journal.Length |> should equal 3
                    journal[0] |> should be ofExactType<UserMessageEvent>
                    journal[1] |> should be ofExactType<TurnStartedEvent>
                    journal[2] |> should be ofExactType<PermissionRequestedEvent>

                    let request = journal[2] :?> PermissionRequestedEvent

                    let! _ =
                        SessionClientOperations.ReplyAsync(
                            client,
                            created.Id,
                            PermissionDecision(request.RequestId, PermissionDecisionKind.AllowOnce) :> Reply,
                            CancellationToken.None
                        )

                    let! result = awaitWhat wait "the resumed turn to settle"
                    result.Status |> should equal TurnStatus.Completed

                    // The resume continuation runs stripped: still exactly
                    // one marker, plus the resolve event, the allowed tool's
                    // Started/Output/Completed markers (issue 366), the
                    // post-resume text delta (issue 379), and the terminal
                    // completion row (issue 289), behind the once-per-turn
                    // user evidence.
                    let! settled = collectJournal client created.Id
                    settled.Length |> should equal 9

                    settled
                    |> List.filter (fun event -> event :? TurnStartedEvent)
                    |> should haveLength 1

                    settled[0] |> should be ofExactType<UserMessageEvent>
                    settled[1] |> should be ofExactType<TurnStartedEvent>
                    settled[2] |> should be ofExactType<PermissionRequestedEvent>
                    settled[3] |> should be ofExactType<PermissionResolvedEvent>
                    settled[4] |> should be ofExactType<ToolCallStartedEvent>
                    settled[5] |> should be ofExactType<ToolCallOutputEvent>
                    settled[6] |> should be ofExactType<ToolCallCompletedEvent>
                    settled[7] |> should be ofExactType<TextDeltaEvent>
                    settled[8] |> should be ofExactType<TurnCompletedEvent>
                })
    }
