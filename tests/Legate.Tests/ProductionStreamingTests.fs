// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ProductionStreamingTests

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

// Production assistant streaming with bounded durable journaling (issue
// 379): through public DI registration and the normal production execution
// path, Subscribe receives assistant text and provider-surfaced reasoning
// before settlement while a streaming provider remains open. Every test
// owns its container, stores, actor system, and scripted transports: no
// live keys, no external services. Waits are event-driven with a
// ten-second bound, never sleeps (polling uses the same short-delay
// precedent as the harness suites).

// ──────────────────────────────────────────────────────────────────────────
// Shared helpers

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

/// Permission policy asking once for the gated tool, allowing the rest.
type private AskPolicy(gated: string) =
    interface IPermissionPolicy with
        member _.Evaluate(request) =
            if request.ToolName = gated then
                PermissionVerdict.Ask
            else
                PermissionVerdict.Allow

/// An ILlmDelay that never elapses unless its token fires.
type private NeverDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

let private textChunk (text: string) : AIContent = TextContent(text) :> AIContent

let private reasoningChunk (text: string) : AIContent = TextReasoningContent(text) :> AIContent

let private streamStep (contents: AIContent list) : ScriptStep =
    ScriptStep.Stream(ResizeArray<AIContent>(contents) :> IReadOnlyList<AIContent>)

/// Builds a container with the facade registered: the session store over
/// the given database, the given journal (built over the same database by
/// the caller so claim tokens fence across both stores), the scripted chat
/// client, and the static tool source. The caller tunes TurnsOptions
/// through tuneTurns before the provider builds.
let private createServices
    (database: InMemoryDatabase)
    (chatClient: IChatClient)
    (tools: StaticToolSource)
    (journal: ISessionEventStore)
    (tuneTurns: (TurnsOptions -> unit) option)
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

    services.AddSingleton<ISessionEventStore>(journal) |> ignore
    services.AddSingleton<IChatClient>(chatClient) |> ignore

    match tuneTurns with
    | Some tune ->
        services.Configure<LegateOptions>(
            Action<LegateOptions>(fun target ->
                if isNull (box target.Turns) then
                    target.Turns <- TurnsOptions()

                tune target.Turns)
        )
        |> ignore
    | None -> ()

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

/// Starts the local actor system (which initializes the immutable host
/// context), resolves the client, runs the work, then stops the system.
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

let private settleWaiter (sessionId: SessionId) : TaskCompletionSource<TurnResult> =
    PromptWaitHubs.GetOrAdd(sessionId).EnqueueSettle()

let private settledOf (sessionId: SessionId) : IReadOnlyList<TurnResult> =
    PromptWaitHubs.GetOrAdd(sessionId).Settled

/// Replays the whole journal for a session in sequence order.
let private replayAll
    (journal: ISessionEventStore)
    (tenant: TenantId)
    (sessionId: SessionId)
    : Task<SessionEvent list> =
    task {
        let collected = ResizeArray<SessionEvent>()
        let mutable cursor = 0L
        let mutable go = true

        while go do
            let! outcome =
                awaitWhat (journal.Replay(tenant, sessionId, cursor, 100, CancellationToken.None)) "the replay page"

            match outcome with
            | :? EventReplayPage as page when not (isNull (box page)) ->
                if not (isNull (box page.Events)) then
                    collected.AddRange(page.Events)

                if page.NextCursor.HasValue then
                    cursor <- page.NextCursor.Value
                else
                    go <- false
            | _ -> go <- false

        return List.ofSeq collected
    }

/// Polls the journal until at least one event matches, or the bound fires.
let private waitForJournal
    (journal: ISessionEventStore)
    (tenant: TenantId)
    (sessionId: SessionId)
    (matches: SessionEvent -> bool)
    (what: string)
    : Task<SessionEvent list> =
    task {
        let deadline = DateTime.UtcNow + waitBound
        let mutable matching: SessionEvent list = []
        let mutable expired = false

        while matching.IsEmpty && not expired do
            let! events = replayAll journal tenant sessionId
            matching <- events |> List.filter matches

            if matching.IsEmpty then
                if DateTime.UtcNow >= deadline then
                    expired <- true
                else
                    do! Task.Delay(25)

        if expired then
            return raise (TimeoutException($"The test timed out waiting for {what}."))
        else
            return matching
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

let private textOf (event: SessionEvent) : string =
    match event with
    | :? TextDeltaEvent as delta when not (isNull (box delta)) -> if isNull (box delta.Text) then "" else delta.Text
    | _ -> failwith "Expected a TextDeltaEvent."

let private reasoningOf (event: SessionEvent) : string =
    match event with
    | :? ReasoningDeltaEvent as delta when not (isNull (box delta)) ->
        if isNull (box delta.Text) then "" else delta.Text
    | _ -> failwith "Expected a ReasoningDeltaEvent."

let private isTextDelta (event: SessionEvent) : bool = event :? TextDeltaEvent

let private isReasoningDelta (event: SessionEvent) : bool = event :? ReasoningDeltaEvent

let private isTerminal (event: SessionEvent) : bool =
    event :? TurnCompletedEvent
    || event :? TurnFailedEvent
    || event :? TurnAbortedEvent

// ──────────────────────────────────────────────────────────────────────────
// Event-store decorators

/// Counts Append calls and journaled delta events, forwarding everything.
type private CountingEventStore(inner: ISessionEventStore) =
    do ArgumentNullException.ThrowIfNull(inner)

    let gate = obj ()
    let mutable appends = 0
    let mutable events = 0

    /// The Append calls observed so far.
    member _.Appends = lock gate (fun () -> appends)

    /// The events carried by those calls.
    member _.Events = lock gate (fun () -> events)

    interface ISessionEventStore with
        member _.Append(tenant, sessionId, claimToken, batch, cancellationToken) =
            task {
                lock gate (fun () ->
                    appends <- appends + 1

                    if not (isNull (box batch)) then
                        events <- events + batch.Count)

                return! inner.Append(tenant, sessionId, claimToken, batch, cancellationToken)
            }

        member _.AppendHostEvents(tenant, sessionId, expectedUpdatedAt, batch, cancellationToken) =
            inner.AppendHostEvents(tenant, sessionId, expectedUpdatedAt, batch, cancellationToken)

        member _.Replay(tenant, sessionId, fromSequence, limit, cancellationToken) =
            inner.Replay(tenant, sessionId, fromSequence, limit, cancellationToken)

        member _.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, cancellationToken) =
            inner.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, cancellationToken)

        member _.CompleteCleanup(tenant, sessionId, claimToken, archiveLocation, cancellationToken) =
            inner.CompleteCleanup(tenant, sessionId, claimToken, archiveLocation, cancellationToken)

        member _.DeferCleanup(tenant, sessionId, claimToken, cancellationToken) =
            inner.DeferCleanup(tenant, sessionId, claimToken, cancellationToken)

/// Lets the first passThrough Append calls land, then blocks the rest on
/// the gate: delayed storage the turn backpressures on. Counts entered and
/// completed calls so tests observe the stall.
type private GatedEventStore(inner: ISessionEventStore, passThrough: int, gate: TaskCompletionSource<unit>) =
    do ArgumentNullException.ThrowIfNull(inner)
    do ArgumentNullException.ThrowIfNull(gate)

    let monitor = obj ()
    let mutable entered = 0
    let mutable completed = 0

    /// The Append calls that arrived so far.
    member _.Entered = lock monitor (fun () -> entered)

    /// The Append calls that returned so far.
    member _.Completed = lock monitor (fun () -> completed)

    interface ISessionEventStore with
        member _.Append(tenant, sessionId, claimToken, batch, cancellationToken) =
            task {
                let arrival =
                    lock monitor (fun () ->
                        entered <- entered + 1
                        entered)

                if arrival > passThrough then
                    do! gate.Task

                let! outcome = inner.Append(tenant, sessionId, claimToken, batch, cancellationToken)
                lock monitor (fun () -> completed <- completed + 1)
                return outcome
            }

        member _.AppendHostEvents(tenant, sessionId, expectedUpdatedAt, batch, cancellationToken) =
            inner.AppendHostEvents(tenant, sessionId, expectedUpdatedAt, batch, cancellationToken)

        member _.Replay(tenant, sessionId, fromSequence, limit, cancellationToken) =
            inner.Replay(tenant, sessionId, fromSequence, limit, cancellationToken)

        member _.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, cancellationToken) =
            inner.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, cancellationToken)

        member _.CompleteCleanup(tenant, sessionId, claimToken, archiveLocation, cancellationToken) =
            inner.CompleteCleanup(tenant, sessionId, claimToken, archiveLocation, cancellationToken)

        member _.DeferCleanup(tenant, sessionId, claimToken, cancellationToken) =
            inner.DeferCleanup(tenant, sessionId, claimToken, cancellationToken)

/// Fails every Append with a transient fault: the writer exhausts its
/// bounded retries and reports the typed failure.
type private FailingEventStore(inner: ISessionEventStore) =
    do ArgumentNullException.ThrowIfNull(inner)

    interface ISessionEventStore with
        member _.Append(_, _, _, _, _) =
            Task.FromException<EventAppendOutcome>(InvalidOperationException("boom-append"))

        member _.AppendHostEvents(tenant, sessionId, expectedUpdatedAt, batch, cancellationToken) =
            inner.AppendHostEvents(tenant, sessionId, expectedUpdatedAt, batch, cancellationToken)

        member _.Replay(tenant, sessionId, fromSequence, limit, cancellationToken) =
            inner.Replay(tenant, sessionId, fromSequence, limit, cancellationToken)

        member _.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, cancellationToken) =
            inner.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, cancellationToken)

        member _.CompleteCleanup(tenant, sessionId, claimToken, archiveLocation, cancellationToken) =
            inner.CompleteCleanup(tenant, sessionId, claimToken, archiveLocation, cancellationToken)

        member _.DeferCleanup(tenant, sessionId, claimToken, cancellationToken) =
            inner.DeferCleanup(tenant, sessionId, claimToken, cancellationToken)

// ──────────────────────────────────────────────────────────────────────────
// Held-open streaming provider

/// A streaming provider that yields the first chunk sequence, signals, waits
/// for the test's release, then yields the remainder: the turn stays
/// inside one provider call while the test observes the journal.
type private HeldOpenStream
    (
        before: IReadOnlyList<AIContent>,
        after: IReadOnlyList<AIContent>,
        started: TaskCompletionSource<unit>,
        release: TaskCompletionSource<unit>
    ) =
    do ArgumentNullException.ThrowIfNull(before)
    do ArgumentNullException.ThrowIfNull(after)
    do ArgumentNullException.ThrowIfNull(started)
    do ArgumentNullException.ThrowIfNull(release)

    let updateOf (content: AIContent) : ChatResponseUpdate =
        ChatResponseUpdate(Nullable ChatRole.Assistant, ResizeArray<AIContent>([| content |]) :> IList<AIContent>)

    interface IChatClient with
        member _.GetResponseAsync(_, _, _) =
            Task.FromException<ChatResponse>(
                InvalidOperationException("Streaming is supported: the loop must stream, never fall back.")
            )

        member _.GetStreamingResponseAsync(_, _, cancellationToken) =
            { new IAsyncEnumerable<ChatResponseUpdate> with
                member _.GetAsyncEnumerator(_) =
                    let mutable first = List.ofSeq before
                    let mutable rest = List.ofSeq after
                    let mutable held = false
                    let mutable current = Unchecked.defaultof<ChatResponseUpdate>

                    { new IAsyncEnumerator<ChatResponseUpdate> with
                        member _.Current = current

                        member _.MoveNextAsync() =
                            ValueTask<bool>(
                                task {
                                    cancellationToken.ThrowIfCancellationRequested()

                                    // The hold fires only once the loop
                                    // asks past the last pre-hold chunk:
                                    // every pre-hold chunk is delivered
                                    // (and journaled through the trip
                                    // caps) before the test observes.
                                    match first with
                                    | head :: tail ->
                                        first <- tail
                                        current <- updateOf head
                                        return true
                                    | [] ->
                                        if not held then
                                            held <- true
                                            started.TrySetResult() |> ignore
                                            do! release.Task

                                        match rest with
                                        | head :: tail ->
                                            rest <- tail
                                            current <- updateOf head
                                            return true
                                        | [] -> return false
                                }
                            )

                        member _.DisposeAsync() = ValueTask()
                    }
            }

        member _.GetService(_, _) = null
        member _.Dispose() = ()

// ──────────────────────────────────────────────────────────────────────────
// Direct journaler fixtures

let private journalTenant = TenantId.Create "production-streaming"

let private createJournalStores () : ISessionStore * ISessionEventStore =
    let database = InMemoryDatabase()
    InMemorySessionStore(database) :> ISessionStore, InMemorySessionEventStore(database) :> ISessionEventStore

let private sampleSession () : Session =
    {
        Id = SessionId.New()
        Tenant = journalTenant
        AgentId = AgentId.New()
        Title = "streaming"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private createJournalSession (store: ISessionStore) : Session =
    store.CreateSession(journalTenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

let private claimOf (state: TurnLeaseState) : TurnClaim =
    match state with
    | :? TurnLeaseRenewed as renewed when not (isNull (box renewed)) -> renewed.Claim
    | :? TurnLeaseHeld as held when not (isNull (box held)) -> held.Claim
    | :? TurnLeaseExpiring as expiring when not (isNull (box expiring)) -> expiring.Claim
    | other -> failwithf "Expected a granted claim, got %O." other

/// Primes genuine journal authority the way the spawn factory does:
/// appends a bootstrap entry, then claims the turn.
let private primeClaim (store: ISessionStore) (sessionId: SessionId) (owner: string) : TurnClaim =
    let bootstrap =
        UserMessagePayload(UserMessage.Text "legate journal prime") :> InboxPayload

    store
        .AppendInboxMessage(journalTenant, sessionId, bootstrap, DeliveryMode.Queue, CancellationToken.None)
        .GetAwaiter()
        .GetResult()
    |> ignore

    store
        .ClaimNextTurn(journalTenant, sessionId, owner, TimeSpan.FromHours 1.0, CancellationToken.None)
        .GetAwaiter()
        .GetResult()
    |> claimOf

/// Counts live publications for one session. JournalWriter.Published is
/// process-wide, so every observation filters on the session id.
let private observePublished (sessionId: SessionId) (published: ResizeArray<SessionEvent>) : IDisposable =
    Observable.subscribe
        (fun (tenant: TenantId, sid: SessionId, stamped: IReadOnlyList<SessionEvent>) ->
            if tenant = journalTenant && sid = sessionId && not (isNull (box stamped)) then
                lock published (fun () -> published.AddRange(stamped)))
        JournalWriter.Published

// ──────────────────────────────────────────────────────────────────────────
// Task 8: bounds configuration and validation

[<Fact>]
let ``streaming defaults mirror TurnsOptions`` () =
    let turns = TurnsOptions()

    turns.MaxStreamingPendingEvents
    |> should equal SessionStreaming.defaultBounds.MaxPendingEvents

    turns.MaxStreamingPendingBytes
    |> should equal SessionStreaming.defaultBounds.MaxPendingBytes

    turns.MaxStreamingAppendBatchEvents
    |> should equal SessionStreaming.defaultBounds.MaxBatchEvents

    turns.MaxStreamingAppendBatchChars
    |> should equal SessionStreaming.defaultBounds.MaxBatchChars

    let resolved = SessionStreaming.boundsFromTurns turns
    resolved |> should equal SessionStreaming.defaultBounds

    let nulled = SessionStreaming.boundsFromTurns Unchecked.defaultof<TurnsOptions>
    nulled |> should equal SessionStreaming.defaultBounds

    (isNull (box (turns.Validate()))) |> should equal true

[<Fact>]
let ``TurnsOptions rejects non-positive streaming bounds`` () =
    let invalid (tune: TurnsOptions -> unit) (message: string) =
        let turns = TurnsOptions()
        tune turns
        turns.Validate() |> should equal message

    invalid (fun turns -> turns.MaxStreamingPendingEvents <- 0) "MaxStreamingPendingEvents must be at least 1."
    invalid (fun turns -> turns.MaxStreamingPendingBytes <- 0) "MaxStreamingPendingBytes must be at least 1."
    invalid (fun turns -> turns.MaxStreamingAppendBatchEvents <- 0) "MaxStreamingAppendBatchEvents must be at least 1."
    invalid (fun turns -> turns.MaxStreamingAppendBatchChars <- 0) "MaxStreamingAppendBatchChars must be at least 1."

[<Fact>]
let ``boundsFromTurns rejects non-positive bounds at use`` () =
    let turns = TurnsOptions()
    turns.MaxStreamingPendingEvents <- 0

    (fun () -> SessionStreaming.boundsFromTurns turns |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

    let chars = TurnsOptions()
    chars.MaxStreamingAppendBatchChars <- -1

    (fun () -> SessionStreaming.boundsFromTurns chars |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

// ──────────────────────────────────────────────────────────────────────────
// Task 1: TurnLoop delta hooks

[<Fact>]
let ``runSuspendableAsync fans streaming chunks to the delta hooks in order`` () =
    let texts = ResizeArray<string>()
    let reasonings = ResizeArray<string>()

    let client =
        scripted
            [
                streamStep
                    [
                        textChunk "a"
                        reasoningChunk "r"
                        textChunk "b"
                    ]
            ]

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            OnTextDelta = Some(fun text -> texts.Add(text))
            OnReasoningDelta = Some(fun text -> reasonings.Add(text))
        }

    let history =
        ResizeArray<ChatMessage>([| ChatMessage(ChatRole.User, "hi") |]) :> IList<ChatMessage>

    let completion =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (Dictionary<string, AITool>() :> IReadOnlyDictionary<string, AITool>)
            options
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            Unchecked.defaultof<IPermissionPolicy>
            (SessionId.New())
            (TurnId.New())
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    completion.Result.Status |> should equal TurnStatus.Completed
    completion.Result.AssistantText |> should equal "ab"
    List.ofSeq texts |> should equal [ "a"; "b" ]
    List.ofSeq reasonings |> should equal [ "r" ]

[<Fact>]
let ``runSuspendableAsync falls back to a single delta per kind`` () =
    let texts = ResizeArray<string>()
    let reasonings = ResizeArray<string>()

    let client = scripted [ ScriptStep.Text "hello" ]

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            OnTextDelta = Some(fun text -> texts.Add(text))
            OnReasoningDelta = Some(fun text -> reasonings.Add(text))
        }

    let history =
        ResizeArray<ChatMessage>([| ChatMessage(ChatRole.User, "hi") |]) :> IList<ChatMessage>

    let completion =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (Dictionary<string, AITool>() :> IReadOnlyDictionary<string, AITool>)
            options
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            Unchecked.defaultof<IPermissionPolicy>
            (SessionId.New())
            (TurnId.New())
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    completion.Result.Status |> should equal TurnStatus.Completed
    List.ofSeq texts |> should equal [ "hello" ]
    reasonings.Count |> should equal 0

// ──────────────────────────────────────────────────────────────────────────
// Task 2: bounded streaming journaler

[<Fact>]
let ``coalesced short stream lands one bounded append`` () =
    let store, journal = createJournalStores ()
    let created = createJournalSession store
    let claim = primeClaim store created.Id "owner-a"
    let turnId = TurnId.New()

    let streamer =
        SessionStreaming.StreamingJournaler(
            journal,
            journalTenant,
            created.Id,
            turnId,
            claim.Token,
            SessionStreaming.defaultBounds
        )

    streamer.AppendText("hel")
    streamer.AppendText("lo")
    streamer.AppendReasoning("hmm")
    streamer.AppendText(Unchecked.defaultof<string>)
    streamer.AppendText("")
    streamer.FlushAsync().GetAwaiter().GetResult()

    streamer.TotalAppends |> should equal 1
    streamer.TotalEvents |> should equal 2

    let events =
        journal.Replay(journalTenant, created.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult()

    match events with
    | :? EventReplayPage as page when not (isNull (box page)) ->
        let listed = List.ofSeq page.Events
        listed.Length |> should equal 2

        let text = listed[0] :?> TextDeltaEvent
        text.Text |> should equal "hello"
        text.TurnId |> should equal turnId
        text.Sequence.HasValue |> should equal true

        let reasoning = listed[1] :?> ReasoningDeltaEvent
        reasoning.Text |> should equal "hmm"
        reasoning.TurnId |> should equal turnId

        reasoning.Sequence.Value |> should equal (text.Sequence.Value + 1L)
    | other -> failwithf "Expected a replay page, got %O." other

[<Fact>]
let ``large buffer splits at batch chars across bounded appends`` () =
    let store, journal = createJournalStores ()
    let created = createJournalSession store
    let claim = primeClaim store created.Id "owner-a"

    let bounds: SessionStreaming.StreamingBounds =
        {
            MaxPendingEvents = 1000
            MaxPendingBytes = 1000000
            MaxBatchEvents = 2
            MaxBatchChars = 8
        }

    let streamer =
        SessionStreaming.StreamingJournaler(journal, journalTenant, created.Id, TurnId.New(), claim.Token, bounds)

    streamer.AppendText("0123456789ABCDEFGHIJ")
    streamer.FlushAsync().GetAwaiter().GetResult()

    // 20 chars split into 8 + 8 + 4 across ceil(3 / 2) = 2 appends.
    streamer.TotalAppends |> should equal 2
    streamer.TotalEvents |> should equal 3

    let events =
        journal.Replay(journalTenant, created.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult()

    match events with
    | :? EventReplayPage as page when not (isNull (box page)) ->
        let listed = List.ofSeq page.Events
        listed.Length |> should equal 3

        for event in listed do
            (event :?> TextDeltaEvent).Text.Length |> should be (lessThanOrEqualTo 8)

        listed
        |> List.map textOf
        |> String.concat ""
        |> should equal "0123456789ABCDEFGHIJ"

        let sequences = listed |> List.map (fun event -> event.Sequence.Value)
        sequences |> should equal [ 1L; 2L; 3L ]
    | other -> failwithf "Expected a replay page, got %O." other

[<Fact>]
let ``empty chunks and empty flushes cost zero storage ops`` () =
    let database = InMemoryDatabase()
    let inner = InMemorySessionEventStore(database) :> ISessionEventStore
    let counting = CountingEventStore(inner)
    let store = InMemorySessionStore(database) :> ISessionStore
    let created = createJournalSession store
    let claim = primeClaim store created.Id "owner-a"

    let streamer =
        SessionStreaming.StreamingJournaler(
            counting,
            journalTenant,
            created.Id,
            TurnId.New(),
            claim.Token,
            SessionStreaming.defaultBounds
        )

    streamer.AppendText(Unchecked.defaultof<string>)
    streamer.AppendText("")
    streamer.AppendReasoning(Unchecked.defaultof<string>)
    streamer.AppendReasoning("")
    streamer.FlushAsync().GetAwaiter().GetResult()

    counting.Appends |> should equal 0
    streamer.TotalAppends |> should equal 0

[<Fact>]
let ``pending caps force mid-stream flushes under sustained production`` () =
    let store, journal = createJournalStores ()
    let created = createJournalSession store
    let claim = primeClaim store created.Id "owner-a"

    let bounds: SessionStreaming.StreamingBounds =
        {
            MaxPendingEvents = 1000
            MaxPendingBytes = 16
            MaxBatchEvents = 8
            MaxBatchChars = 64
        }

    let streamer =
        SessionStreaming.StreamingJournaler(journal, journalTenant, created.Id, TurnId.New(), claim.Token, bounds)

    // Ten 8-byte chunks against a 16-byte pending cap: the 3rd, 5th, 7th,
    // and 9th arrivals trip a flush first, plus the final remainder.
    for _ in 1..10 do
        streamer.AppendText("abcdefgh")

    streamer.FlushAsync().GetAwaiter().GetResult()

    streamer.TotalAppends |> should be (greaterThanOrEqualTo 3)
    streamer.TotalAppends |> should be (lessThan 10)
    streamer.MaxPendingBytes |> should be (lessThanOrEqualTo 24)

    let events =
        journal.Replay(journalTenant, created.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult()

    match events with
    | :? EventReplayPage as page when not (isNull (box page)) ->
        let listed = List.ofSeq page.Events

        listed
        |> List.map textOf
        |> String.concat ""
        |> should equal (String.concat "" (List.init 10 (fun _ -> "abcdefgh")))
    | other -> failwithf "Expected a replay page, got %O." other

[<Fact>]
let ``content safety redacts secrets before the journal`` () =
    let store, journal = createJournalStores ()
    let created = createJournalSession store
    let claim = primeClaim store created.Id "owner-a"

    let streamer =
        SessionStreaming.StreamingJournaler(
            journal,
            journalTenant,
            created.Id,
            TurnId.New(),
            claim.Token,
            SessionStreaming.defaultBounds
        )

    streamer.AppendText("key sk-ant-12345678 end")
    streamer.FlushAsync().GetAwaiter().GetResult()

    let events =
        journal.Replay(journalTenant, created.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult()

    match events with
    | :? EventReplayPage as page when not (isNull (box page)) ->
        let listed = List.ofSeq page.Events
        listed.Length |> should equal 1
        let text = textOf listed[0]
        text.Contains("sk-ant-", StringComparison.Ordinal) |> should equal false

        text.Contains(JournalWriter.RedactedText, StringComparison.Ordinal)
        |> should equal true
    | other -> failwithf "Expected a replay page, got %O." other

// ──────────────────────────────────────────────────────────────────────────
// Tasks 4 + 6: fenced appends and stale-ownership takeover

[<Fact>]
let ``stale token rejects with zero writes and zero publication`` () =
    let clock = FakeClock()
    let database = InMemoryDatabase(clock)
    let store = InMemorySessionStore(database) :> ISessionStore
    let journal = InMemorySessionEventStore(database) :> ISessionEventStore
    let created = createJournalSession store
    let published = ResizeArray<SessionEvent>()

    use _subscription = observePublished created.Id published

    let prime owner lease =
        let payload =
            UserMessagePayload(UserMessage.Text "legate journal prime") :> InboxPayload

        store
            .AppendInboxMessage(journalTenant, created.Id, payload, DeliveryMode.Queue, CancellationToken.None)
            .GetAwaiter()
            .GetResult()
        |> ignore

        store.ClaimNextTurn(journalTenant, created.Id, owner, lease, CancellationToken.None).GetAwaiter().GetResult()
        |> claimOf

    let loser = prime "owner-a" (TimeSpan.FromMinutes 1.0)

    // Preliminary verification passes while the loser still owns the turn.
    let live =
        ClaimFence.checkBeforeCallAsync store journalTenant loser CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    live |> should equal true

    let loserJournaler =
        SessionStreaming.StreamingJournaler(
            journal,
            journalTenant,
            created.Id,
            TurnId.New(),
            loser.Token,
            SessionStreaming.defaultBounds
        )

    loserJournaler.AppendText("buffered")

    // Takeover past the lease between verification and the flush: the
    // winner primes under a fresh token while the loser goes stale.
    clock.Advance(TimeSpan.FromMinutes 10.0)
    prime "owner-b" (TimeSpan.FromMinutes 1.0) |> ignore

    (fun () -> loserJournaler.FlushAsync().GetAwaiter().GetResult())
    |> should throw typeof<TurnLoop.TurnLeaseLostException>

    // Zero journal writes and zero live publication from the loser.
    let events =
        journal.Replay(journalTenant, created.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult()

    match events with
    | :? EventReplayPage as page when not (isNull (box page)) ->
        (isNull (box page.Events) || page.Events.Count = 0) |> should equal true
    | :? EventReplayEndOfStream -> ()
    | other -> failwithf "Expected an empty replay, got %O." other

    lock published (fun () -> published.Count) |> should equal 0

    // Prior authorized commits stay truthful evidence: the winner lands
    // after its own prime once the previous claim lapses.
    clock.Advance(TimeSpan.FromMinutes 10.0)
    let winner = prime "owner-c" (TimeSpan.FromMinutes 1.0)

    let winnerJournaler =
        SessionStreaming.StreamingJournaler(
            journal,
            journalTenant,
            created.Id,
            TurnId.New(),
            winner.Token,
            SessionStreaming.defaultBounds
        )

    winnerJournaler.AppendText("prior")
    winnerJournaler.FlushAsync().GetAwaiter().GetResult()

    let after =
        journal.Replay(journalTenant, created.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult()

    match after with
    | :? EventReplayPage as page when not (isNull (box page)) ->
        let listed = List.ofSeq page.Events
        listed.Length |> should equal 1
        textOf listed[0] |> should equal "prior"
    | other -> failwithf "Expected a replay page, got %O." other

    lock published (fun () -> published.Count) |> should equal 1

[<Fact>]
let ``exhausted retries raise typed failure with zero publication`` () =
    let database = InMemoryDatabase()
    let inner = InMemorySessionEventStore(database) :> ISessionEventStore
    let failing = FailingEventStore(inner) :> ISessionEventStore
    let store = InMemorySessionStore(database) :> ISessionStore
    let created = createJournalSession store
    let claim = primeClaim store created.Id "owner-a"
    let published = ResizeArray<SessionEvent>()

    use _subscription = observePublished created.Id published

    let streamer =
        SessionStreaming.StreamingJournaler(
            failing,
            journalTenant,
            created.Id,
            TurnId.New(),
            claim.Token,
            SessionStreaming.defaultBounds
        )

    streamer.AppendText("doomed")

    let message =
        try
            streamer.FlushAsync().GetAwaiter().GetResult()
            failwith "Expected InvalidOperationException."
        with :? InvalidOperationException as failed ->
            failed.Message

    message.Contains("attempts", StringComparison.OrdinalIgnoreCase)
    |> should equal true

    let events =
        inner.Replay(journalTenant, created.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult()

    match events with
    | :? EventReplayPage as page when not (isNull (box page)) ->
        (isNull (box page.Events) || page.Events.Count = 0) |> should equal true
    | :? EventReplayEndOfStream -> ()
    | other -> failwithf "Expected an empty replay, got %O." other

    lock published (fun () -> published.Count) |> should equal 0

// ──────────────────────────────────────────────────────────────────────────
// Task 7: public facade end-to-end coverage

[<Fact>]
let ``non-streaming fallback journals a single text delta`` () : Task =
    task {
        let database = InMemoryDatabase()
        let inner = InMemorySessionEventStore(database) :> ISessionEventStore
        let counting = CountingEventStore(inner)
        let chat = scripted [ ScriptStep.Text "hello" ]

        use provider =
            (createServices database chat (sourced []) counting None).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! _ =
                        awaitWhat
                            (SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "hi",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))
                            "the prompt to land"

                    let! result = awaitWhat waiter.Task "the turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                    result.AssistantText |> should equal "hello"

                    // Settle first, then count: the terminal event lands
                    // through the same journal after the waiter fires.
                    let! _ = waitForJournal counting client.Tenant created.Id isTerminal "the terminal event"
                    let! events = replayAll counting client.Tenant created.Id

                    let kinds = events |> List.map (fun event -> event.GetType().Name)

                    kinds
                    |> should
                        equal
                        [
                            nameof TurnStartedEvent
                            nameof TextDeltaEvent
                            nameof TurnCompletedEvent
                        ]

                    textOf events[1] |> should equal "hello"

                    // Marker plus one coalesced flush plus the terminal:
                    // no synchronous storage operation per token.
                    counting.Appends |> should equal 3
                })
    }

[<Fact>]
let ``streaming chunks coalesce into ordered deltas before the terminal event`` () : Task =
    task {
        let database = InMemoryDatabase()
        let inner = InMemorySessionEventStore(database) :> ISessionEventStore
        let counting = CountingEventStore(inner)

        let chat =
            scripted
                [
                    streamStep
                        [
                            textChunk "hel"
                            textChunk "lo "
                            reasoningChunk "hmm"
                            textChunk "world"
                        ]
                ]

        use provider =
            (createServices database chat (sourced []) counting None).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! _ =
                        awaitWhat
                            (SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "hi",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))
                            "the prompt to land"

                    let! result = awaitWhat waiter.Task "the turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                    result.AssistantText |> should equal "hello world"

                    let! _ = waitForJournal counting client.Tenant created.Id isTerminal "the terminal event"
                    let! events = replayAll counting client.Tenant created.Id

                    let kinds = events |> List.map (fun event -> event.GetType().Name)

                    kinds
                    |> should
                        equal
                        [
                            nameof TurnStartedEvent
                            nameof TextDeltaEvent
                            nameof ReasoningDeltaEvent
                            nameof TurnCompletedEvent
                        ]

                    textOf events[1] |> should equal "hello world"
                    reasoningOf events[2] |> should equal "hmm"

                    let turn = events[0].TurnId
                    events |> List.iter (fun event -> event.TurnId |> should equal turn)

                    // One marker, one streaming flush, and the terminal for
                    // four chunks: no synchronous storage operation per token.
                    counting.Appends |> should equal 3
                })
    }

[<Fact>]
let ``held-open provider observes deltas before settlement`` () : Task =
    task {
        let database = InMemoryDatabase()
        let inner = InMemorySessionEventStore(database) :> ISessionEventStore
        let counting = CountingEventStore(inner)

        let started =
            new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let release =
            new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let before =
            [
                textChunk "01234567"
                textChunk "89ABCDEF"
                textChunk "GHIJKLMN"
                reasoningChunk "why-tho"
                textChunk "OPQRSTUV"
                textChunk "WXYZ1234"
            ]

        let chat =
            new HeldOpenStream(
                ResizeArray<AIContent>(before) :> IReadOnlyList<AIContent>,
                ResizeArray<AIContent>(
                    [
                        textChunk "56789ab"
                        textChunk "cdefghi"
                    ]
                )
                :> IReadOnlyList<AIContent>,
                started,
                release
            )
            :> IChatClient

        // A 32-byte pending cap trips mid-stream: the first three text
        // chunks (24 bytes) plus the reasoning chunk land while the
        // provider stays open.
        let tune (turns: TurnsOptions) =
            turns.MaxStreamingPendingBytes <- 32
            turns.MaxStreamingAppendBatchChars <- 64
            turns.MaxStreamingAppendBatchEvents <- 64

        use provider =
            (createServices database chat (sourced []) counting (Some tune)).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let prompt =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "hi",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! _ = awaitWhat prompt "the prompt to land"
                    let! _ = awaitWhat started.Task "the provider to stream"

                    // The provider is still open: nothing released yet.
                    release.Task.IsCompleted |> should equal false

                    // Progressive durable output is already observable.
                    let! landed =
                        waitForJournal counting client.Tenant created.Id isTextDelta "the progressive text delta"

                    release.Task.IsCompleted |> should equal false
                    (settledOf created.Id).Count |> should equal 0

                    // Subscribe streams the committed deltas live.
                    let! observed = collectStream client created.Id 0L 3

                    (observed |> List.exists isTextDelta) |> should equal true
                    (observed |> List.exists isReasoningDelta) |> should equal true
                    (settledOf created.Id).Count |> should equal 0

                    release.TrySetResult() |> ignore

                    let! result = awaitWhat waiter.Task "the turn to settle"
                    result.Status |> should equal TurnStatus.Completed

                    let expectedText =
                        "01234567"
                        + "89ABCDEF"
                        + "GHIJKLMN"
                        + "OPQRSTUV"
                        + "WXYZ1234"
                        + "56789ab"
                        + "cdefghi"

                    result.AssistantText |> should equal expectedText

                    let! _ = waitForJournal counting client.Tenant created.Id isTerminal "the terminal event"
                    let! events = replayAll counting client.Tenant created.Id

                    let texts = events |> List.filter isTextDelta |> List.map textOf |> String.concat ""

                    texts |> should equal expectedText

                    let last = List.last events
                    last :? TurnCompletedEvent |> should equal true

                    // Seven provider chunks journaled in a handful of
                    // bounded appends, never one per token.
                    counting.Appends |> should be (lessThan 7)
                    landed.Length |> should be (greaterThanOrEqualTo 1)
                })
    }

[<Fact>]
let ``partial committed output survives provider failure`` () : Task =
    task {
        let database = InMemoryDatabase()
        let inner = InMemorySessionEventStore(database) :> ISessionEventStore
        let counting = CountingEventStore(inner)
        let invocations = ref []

        // An 8-byte pending cap commits the first chunk mid-stream; the
        // fault-path flush commits the remainder under the still-live
        // claim before the terminal failure lands.
        let chat =
            scripted
                [
                    streamStep
                        [
                            textChunk "12345678"
                            textChunk "more"
                            FunctionCallContent("c1", "exec", Dictionary<string, obj>() :> IDictionary<string, obj>)
                            :> AIContent
                        ]
                    ScriptStep.Failure(InvalidOperationException("boom"))
                ]

        let tune (turns: TurnsOptions) =
            turns.MaxStreamingPendingBytes <- 8
            turns.MaxStreamingAppendBatchChars <- 64

        use provider =
            (createServices database chat (sourced [ tool "exec" "ok" invocations ]) counting (Some tune))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! _ =
                        awaitWhat
                            (SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "hi",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))
                            "the prompt to land"

                    let! result = awaitWhat waiter.Task "the failed turn to settle"
                    result.Status |> should equal TurnStatus.Failed
                    invocations.Value |> should equal [ "exec" ]

                    let! _ = waitForJournal counting client.Tenant created.Id isTerminal "the terminal event"
                    let! events = replayAll counting client.Tenant created.Id

                    let kinds = events |> List.map (fun event -> event.GetType().Name)

                    kinds
                    |> should
                        equal
                        [
                            nameof TurnStartedEvent
                            nameof TextDeltaEvent
                            nameof TextDeltaEvent
                            nameof TurnFailedEvent
                        ]

                    // Every surfaced chunk survives the failure in order:
                    // the mid-stream commit plus the fault-path remainder.
                    events
                    |> List.choose (fun event ->
                        match event with
                        | :? TextDeltaEvent as delta when not (isNull (box delta)) -> Some delta.Text
                        | _ -> None)
                    |> String.concat ""
                    |> should equal "12345678more"
                })
    }

[<Fact>]
let ``suspended then resumed streams share the origin turn`` () : Task =
    task {
        let database = InMemoryDatabase()
        let inner = InMemorySessionEventStore(database) :> ISessionEventStore
        let counting = CountingEventStore(inner)
        let invocations = ref []

        let chat =
            scripted
                [
                    streamStep
                        [
                            textChunk "before"
                            FunctionCallContent("c1", "gated", Dictionary<string, obj>() :> IDictionary<string, obj>)
                            :> AIContent
                        ]
                    ScriptStep.Text "after"
                ]

        let services =
            createServices database chat (sourced [ tool "gated" "ok" invocations ]) counting None

        services.AddSingleton<IPermissionPolicy>(AskPolicy("gated")) |> ignore

        use provider = services.BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id
                    let stream = collectStream client created.Id 0L 3

                    let! _ =
                        awaitWhat
                            (SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "run it",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))
                            "the prompt to land"

                    let! events = awaitWhat stream "the suspend lifecycle"

                    let asked =
                        events
                        |> List.pick (fun event ->
                            match event with
                            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) -> Some asked
                            | _ -> None)

                    let! _ =
                        awaitWhat
                            (SessionClientOperations.ReplyAsync(
                                client,
                                created.Id,
                                PermissionDecision(asked.RequestId, PermissionDecisionKind.AllowOnce),
                                CancellationToken.None
                            ))
                            "the reply to land"

                    let! result = awaitWhat waiter.Task "the resumed turn to settle"
                    result.Status |> should equal TurnStatus.Completed
                    invocations.Value |> should equal [ "gated" ]

                    let! _ = waitForJournal counting client.Tenant created.Id isTerminal "the terminal event"
                    let! journaled = replayAll counting client.Tenant created.Id

                    let deltas = journaled |> List.filter isTextDelta
                    deltas.Length |> should equal 2
                    deltas |> List.map textOf |> should equal [ "before"; "after" ]

                    // Both phases journal under the origin turn the marker
                    // carried: fresh plus resumed continuations covered.
                    let origin = (journaled[0] :?> TurnStartedEvent).TurnId
                    deltas |> List.iter (fun event -> event.TurnId |> should equal origin)

                    let beforeSeq = deltas[0].Sequence.Value
                    let askSeq = asked.Sequence.Value
                    let afterSeq = deltas[1].Sequence.Value
                    let completedSeq = (List.last journaled).Sequence.Value

                    (beforeSeq < askSeq && askSeq < afterSeq && afterSeq < completedSeq)
                    |> should equal true
                })
    }

[<Fact>]
let ``delayed storage backpressures without per-token ops`` () : Task =
    task {
        let database = InMemoryDatabase()
        let inner = InMemorySessionEventStore(database) :> ISessionEventStore

        let gate =
            new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        // The TurnStarted marker lands at once; the streaming flush stalls.
        let gated = GatedEventStore(inner, 1, gate)

        let chunks = List.init 20 (fun index -> textChunk (sprintf "%02d" index))

        let chat = scripted [ streamStep chunks ]

        use provider =
            (createServices database chat (sourced []) gated None).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let prompt =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "hi",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! _ = awaitWhat prompt "the prompt to land"

                    // The streaming flush arrived and stalled on storage:
                    // entered twice (marker plus flush), only the marker
                    // completed, and the turn has not settled.
                    let deadline = DateTime.UtcNow + waitBound
                    let mutable stalled = false

                    while not stalled do
                        if DateTime.UtcNow >= deadline then
                            return raise (TimeoutException("The test timed out waiting for the storage stall."))

                        if gated.Entered >= 2 && gated.Completed = 1 then
                            stalled <- true
                        else
                            do! Task.Delay(25)

                    (settledOf created.Id).Count |> should equal 0

                    let! mid = replayAll gated client.Tenant created.Id
                    (mid |> List.exists isTextDelta) |> should equal false

                    gate.TrySetResult() |> ignore

                    let! result = awaitWhat waiter.Task "the turn to settle"
                    result.Status |> should equal TurnStatus.Completed

                    let! _ = waitForJournal gated client.Tenant created.Id isTerminal "the terminal event"
                    let! events = replayAll gated client.Tenant created.Id
                    let texts = events |> List.filter isTextDelta |> List.map textOf |> String.concat ""

                    texts
                    |> should equal (String.concat "" (List.init 20 (fun index -> sprintf "%02d" index)))

                    // Marker, one coalesced streaming flush, and the
                    // terminal for twenty chunks: never one op per token.
                    gated.Entered |> should equal 3
                })
    }

[<Fact>]
let ``slow subscriber never stalls the turn`` () : Task =
    task {
        let database = InMemoryDatabase()
        let inner = InMemorySessionEventStore(database) :> ISessionEventStore
        let counting = CountingEventStore(inner)
        let chat = scripted [ ScriptStep.Text "done" ]

        use provider =
            (createServices database chat (sourced []) counting None).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client

                    // Attach a lagging subscriber that reads one event and
                    // then stops draining while the turn runs.
                    let stream =
                        SessionClientOperations.Subscribe(client, created.Id, 0L, CancellationToken.None)

                    let enumerator = stream.GetAsyncEnumerator(CancellationToken.None)

                    let waiter = settleWaiter created.Id

                    let! _ =
                        awaitWhat
                            (SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "hi",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))
                            "the prompt to land"

                    let! first = awaitWhat (enumerator.MoveNextAsync().AsTask()) "the first live event"
                    Assert.True(first)

                    // The turn settles promptly even though nobody drains
                    // the subscriber: storage backpressure is distinct from
                    // subscriber lag.
                    let! result = awaitWhat waiter.Task "the turn to settle"
                    result.Status |> should equal TurnStatus.Completed

                    try
                        enumerator.DisposeAsync().AsTask() |> ignore
                    finally
                        ()

                    let! events = replayAll counting client.Tenant created.Id
                    (events |> List.exists isTextDelta) |> should equal true
                })
    }

[<Fact>]
let ``abort preserves committed partials before the abort event`` () : Task =
    task {
        let database = InMemoryDatabase()
        let inner = InMemorySessionEventStore(database) :> ISessionEventStore
        let counting = CountingEventStore(inner)

        let started =
            new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let release =
            new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let before =
            [
                textChunk "AAAAAAAA"
                textChunk "BBBBBBBB"
                textChunk "CCCCCCCC"
                textChunk "DDDDDDDD"
                textChunk "EEEEEEEE"
            ]

        let chat =
            new HeldOpenStream(
                ResizeArray<AIContent>(before) :> IReadOnlyList<AIContent>,
                ResizeArray<AIContent>([ textChunk "FFFFFFFF" ]) :> IReadOnlyList<AIContent>,
                started,
                release
            )
            :> IChatClient

        // A 32-byte pending cap commits the first four chunks while the
        // provider stays open; the abort lands before the release.
        let tune (turns: TurnsOptions) =
            turns.MaxStreamingPendingBytes <- 32
            turns.MaxStreamingAppendBatchChars <- 64

        use provider =
            (createServices database chat (sourced []) counting (Some tune)).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let prompt =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "hi",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! _ = awaitWhat prompt "the prompt to land"
                    let! _ = awaitWhat started.Task "the provider to stream"

                    let! _ = waitForJournal counting client.Tenant created.Id isTextDelta "the committed partial"

                    let! target =
                        awaitWhat
                            (SessionClientOperations.ReadAbortTargetAsync(client, created.Id, CancellationToken.None))
                            "the abort target"

                    let abortTurn =
                        match target with
                        | null -> failwith "Expected an abort target."
                        | live -> live.TurnId

                    let! receipt =
                        awaitWhat
                            (SessionClientOperations.AbortAsync(
                                client,
                                created.Id,
                                abortTurn,
                                StopCause.ExplicitAbort,
                                "test-abort",
                                CancellationToken.None
                            ))
                            "the abort to land"

                    receipt.Outcome |> should equal HostAbortOutcome.Accepted

                    release.TrySetResult() |> ignore

                    let! aborted = awaitWhat waiter.Task "the aborted turn to settle"
                    aborted.Status |> should equal TurnStatus.Aborted

                    let! _ = waitForJournal counting client.Tenant created.Id isTerminal "the terminal event"
                    let! events = replayAll counting client.Tenant created.Id

                    let abortedEvent =
                        events
                        |> List.pick (fun event ->
                            match event with
                            | :? TurnAbortedEvent as aborted when not (isNull (box aborted)) -> Some aborted
                            | _ -> None)

                    let deltas = events |> List.filter isTextDelta
                    deltas.Length |> should be (greaterThanOrEqualTo 1)

                    // Committed partials stay ordered before the abort.
                    for delta in deltas do
                        (delta.Sequence.Value < abortedEvent.Sequence.Value) |> should equal true
                })
    }

[<Fact>]
let ``tiny configured bounds stay bounded end to end`` () : Task =
    task {
        let database = InMemoryDatabase()
        let inner = InMemorySessionEventStore(database) :> ISessionEventStore
        let counting = CountingEventStore(inner)

        let chunks = List.init 10 (fun _ -> textChunk "abcd")
        let chat = scripted [ streamStep chunks ]

        // Two pending events and two events per append over 4-char
        // events: ten chunks become ten events in five appends.
        let tune (turns: TurnsOptions) =
            turns.MaxStreamingPendingEvents <- 2
            turns.MaxStreamingPendingBytes <- 1000000
            turns.MaxStreamingAppendBatchEvents <- 2
            turns.MaxStreamingAppendBatchChars <- 4

        use provider =
            (createServices database chat (sourced []) counting (Some tune)).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! _ =
                        awaitWhat
                            (SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "hi",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))
                            "the prompt to land"

                    let! result = awaitWhat waiter.Task "the turn to settle"
                    result.Status |> should equal TurnStatus.Completed

                    let! _ = waitForJournal counting client.Tenant created.Id isTerminal "the terminal event"
                    let! events = replayAll counting client.Tenant created.Id
                    let deltas = events |> List.filter isTextDelta
                    deltas.Length |> should equal 10

                    for delta in deltas do
                        textOf delta |> should equal "abcd"

                    deltas
                    |> List.map (fun event -> event.Sequence.Value)
                    |> should
                        equal
                        [
                            2L
                            3L
                            4L
                            5L
                            6L
                            7L
                            8L
                            9L
                            10L
                            11L
                        ]

                    // Five bounded streaming appends plus the marker plus
                    // the terminal: ten chunks never cost ten storage ops.
                    counting.Appends |> should equal 7
                })
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 6: runner-level stale ownership

[<Fact>]
let ``runner losing the claim journals zero streaming writes`` () =
    let clock = FakeClock()
    let database = InMemoryDatabase(clock)
    let store = InMemorySessionStore(database) :> ISessionStore
    let journal = InMemorySessionEventStore(database) :> ISessionEventStore

    let session =
        store.CreateSession(journalTenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

    let bootstrap (text: string) =
        let payload = UserMessagePayload(UserMessage.Text text) :> InboxPayload

        store
            .AppendInboxMessage(journalTenant, session.Id, payload, DeliveryMode.Queue, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    let entry = bootstrap "run"

    let loser =
        store
            .ClaimNextTurn(journalTenant, session.Id, "owner-a", TimeSpan.FromMinutes 1.0, CancellationToken.None)
            .GetAwaiter()
            .GetResult()
        |> claimOf

    // Preliminary verification passes while the loser still owns the turn.
    let live =
        ClaimFence.checkBeforeCallAsync store journalTenant loser CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    live |> should equal true

    // Takeover past the lease: the winner primes under a fresh token.
    clock.Advance(TimeSpan.FromMinutes 10.0)
    bootstrap "winner" |> ignore

    store
        .ClaimNextTurn(journalTenant, session.Id, "owner-b", TimeSpan.FromMinutes 1.0, CancellationToken.None)
        .GetAwaiter()
        .GetResult()
    |> ignore

    let client = scripted [ ScriptStep.Text "loser-text" ]
    let tools = Dictionary<string, AITool>() :> IReadOnlyDictionary<string, AITool>

    let runner =
        SessionPermissions.createRunner
            (client :> IChatClient)
            store
            journalTenant
            (fun _ -> (tools, TurnLoop.TurnLoopOptions.Default))
            (NeverDelay() :> ILlmDelay)
            Unchecked.defaultof<IPermissionPolicy>
            None
            journal
            SessionStreaming.defaultBounds

    let attempt () =
        use _lease = LeaseAdmission.enter (fun () -> true)
        use _claim = FencedClaimScope.enter (Some loser)

        runner entry 1 (HashSet<string>()) None None None CancellationToken.None None None None (TurnId.New())
        |> fun task -> task.GetAwaiter().GetResult()
        |> ignore

    (fun () -> attempt ()) |> should throw typeof<TurnLoop.TurnLeaseLostException>

    // Zero journal writes from the rejected attempt.
    let events =
        journal.Replay(journalTenant, session.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult()

    match events with
    | :? EventReplayPage as page when not (isNull (box page)) ->
        (isNull (box page.Events) || page.Events.Count = 0) |> should equal true
    | :? EventReplayEndOfStream -> ()
    | other -> failwithf "Expected an empty replay, got %O." other
