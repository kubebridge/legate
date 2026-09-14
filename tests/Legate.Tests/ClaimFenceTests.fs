// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ClaimFenceTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// Last-moment claim fence (issue 33): every post-claim surface verifies
// the token before acting and skips with zero effects on a lost or missing
// claim. Live claims land; expired, taken-over, and missing claims fence
// out. The takeover race test proves the loser produces zero effects
// across checkpoints, journal appends, tool calls, sink notification, and
// settlement, while the winner's effects land afterward.

// ──────────────────────────────────────────────────────────────────────────
// Fixtures

let tenant = TenantId.Create "acme"

let private startInstant = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

let private lease = TimeSpan.FromSeconds 120.0

/// Fresh clock-backed stores; every fact owns its database.
let private createStores () =
    let clock = TestClock(startInstant)
    let database = InMemoryDatabase(clock)
    let sessions = InMemorySessionStore(database) :> ISessionStore
    let events = InMemorySessionEventStore(database) :> ISessionEventStore
    (clock, sessions, events)

/// A minimal Idle session row, mirroring the session actor suite sample.
let private sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "checkout"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
    }

let private createSession (store: ISessionStore) : Session =
    store.CreateSession(tenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

let private appendUser (store: ISessionStore) (sessionId: SessionId) (text: string) : unit =
    let payload = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload

    store.AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()
    |> ignore

/// Claims the next turn, failing the test unless the store grants it.
let private claimTurn (store: ISessionStore) (sessionId: SessionId) (owner: string) : TurnClaim =
    match
        store.ClaimNextTurn(tenant, sessionId, owner, lease, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnLeaseRenewed as renewed -> renewed.Claim
    | state -> failwith $"Expected a granted claim, observed %s{state.GetType().Name}."

let private sampleUsage () : UsageSummary =
    { InputTokens = 10L; OutputTokens = 5L }

let private noSequence = Nullable<int64>()

let private startedEvent (sessionId: SessionId) (turnId: TurnId) : SessionEvent =
    TurnStartedEvent(sessionId, turnId, noSequence, startInstant) :> SessionEvent

/// A sink recording every completion it receives.
type RecordingSink() =
    let calls = ResizeArray<SessionCompletion>()

    interface ISessionCompletionSink with
        member _.Notify(completion) = calls.Add(completion)

    member _.Calls: IReadOnlyList<SessionCompletion> =
        calls :> IReadOnlyList<SessionCompletion>

let private sampleResult () : TurnResult =
    {
        AssistantText = "done"
        Status = TurnStatus.Completed
        Iterations = 1
        Usage = sampleUsage ()
        Outcome = null
    }

let private sampleCompletion (sessionId: SessionId) (key: string) : SessionCompletion =
    {
        SessionId = sessionId
        TurnResult = sampleResult ()
        Metadata = null
        IdempotencyKey = key
    }

let private sampleTurn (sessionId: SessionId) (turnId: TurnId) : Turn =
    {
        Id = turnId
        SessionId = sessionId
        Attempt = 1
        Status = TurnStatus.Running
        StartedAt = startInstant
        CompletedAt = Nullable<DateTimeOffset>()
        Iterations = 0
        Usage = sampleUsage ()
        Error = null
    }

// ──────────────────────────────────────────────────────────────────────────
// Live claims land on every surface

[<Fact>]
let ``Live claim checkpoints usage`` () =
    let _, store, _ = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"

    let landed =
        ClaimFence.checkpointUsageAsync store tenant claim (sampleUsage ()) CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    landed |> should equal true

[<Fact>]
let ``Live claim appends journal events with stamped sequences`` () =
    let _, store, events = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"

    let outcome =
        ClaimFence.appendEventsAsync
            store
            events
            tenant
            session.Id
            claim
            (ResizeArray([ startedEvent session.Id claim.TurnId ]) :> IReadOnlyList<SessionEvent>)
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    match outcome with
    | Some(:? EventAppended as applied) ->
        applied.Events.Count |> should equal 1
        applied.Events[0].Sequence.Value |> should equal 1L
    | _ -> failwith "Expected the append to land."

[<Fact>]
let ``Live claim settles the turn`` () =
    let _, store, _ = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"

    let settlement =
        ClaimFence.settleTurnAsync store tenant claim TurnStatus.Completed null CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    match settlement with
    | Some(:? TurnSettled as settled) -> settled.Status |> should equal TurnStatus.Completed
    | _ -> failwith "Expected the settlement to land."

[<Fact>]
let ``Live claim notifies the sink`` () =
    let _, store, _ = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"
    let sink = RecordingSink()

    let notified =
        ClaimFence.notifyIfLiveAsync
            store
            tenant
            claim
            (sink :> ISessionCompletionSink)
            (sampleCompletion session.Id "key-1")
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    notified |> should equal true
    sink.Calls.Count |> should equal 1
    sink.Calls[0].IdempotencyKey |> should equal "key-1"

[<Fact>]
let ``Live claim passes the tool-call check`` () =
    let _, store, _ = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"

    let live =
        ClaimFence.checkBeforeCallAsync store tenant claim CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    live |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Fenced-out claims skip with zero effects

/// An already-expired claim with no takeover.
let private expiredClaim () =
    let clock, store, events = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"
    clock.Advance(TimeSpan.FromSeconds 121.0)
    (store, events, session, claim)

[<Fact>]
let ``Expired claim checkpoints nothing`` () =
    let store, _, _, claim = expiredClaim ()

    let landed =
        ClaimFence.checkpointUsageAsync store tenant claim (sampleUsage ()) CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    landed |> should equal false

[<Fact>]
let ``Expired claim appends nothing`` () =
    let store, events, session, claim = expiredClaim ()

    let outcome =
        ClaimFence.appendEventsAsync
            store
            events
            tenant
            session.Id
            claim
            (ResizeArray([ startedEvent session.Id claim.TurnId ]) :> IReadOnlyList<SessionEvent>)
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    outcome |> should equal None

    let replay =
        events.Replay(tenant, session.Id, 0L, 10, CancellationToken.None).GetAwaiter().GetResult()

    match replay with
    | :? EventReplayEndOfStream -> ()
    | :? EventReplayPage as page when page.Events.Count = 0 -> ()
    | _ -> failwith "Expected an empty journal after the fenced-out append."

[<Fact>]
let ``Expired claim settles nothing`` () =
    let store, _, _, claim = expiredClaim ()

    let settlement =
        ClaimFence.settleTurnAsync store tenant claim TurnStatus.Completed null CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    settlement |> should equal None

[<Fact>]
let ``Expired claim notifies nothing`` () =
    let store, _, session, claim = expiredClaim ()
    let sink = RecordingSink()

    let notified =
        ClaimFence.notifyIfLiveAsync
            store
            tenant
            claim
            (sink :> ISessionCompletionSink)
            (sampleCompletion session.Id "key-1")
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    notified |> should equal false
    sink.Calls.Count |> should equal 0

[<Fact>]
let ``Expired claim fails the tool-call check`` () =
    let store, _, _, claim = expiredClaim ()

    let live =
        ClaimFence.checkBeforeCallAsync store tenant claim CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    live |> should equal false

[<Fact>]
let ``Missing claim fences every surface out`` () =
    let _, store, events = createStores ()
    let session = createSession store

    let missing =
        {
            TurnId = TurnId.New()
            Token = "never-minted"
            Owner = "owner-a"
            ExpiresAt = startInstant.Add(lease)
            Attempt = 1
        }

    let checkpointed =
        ClaimFence.checkpointUsageAsync store tenant missing (sampleUsage ()) CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let appended =
        ClaimFence.appendEventsAsync
            store
            events
            tenant
            session.Id
            missing
            (ResizeArray(
                [
                    startedEvent session.Id missing.TurnId
                ]
            )
            :> IReadOnlyList<SessionEvent>)
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let settled =
        ClaimFence.settleTurnAsync store tenant missing TurnStatus.Completed null CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let sink = RecordingSink()

    let notified =
        ClaimFence.notifyIfLiveAsync
            store
            tenant
            missing
            (sink :> ISessionCompletionSink)
            (sampleCompletion session.Id "key-1")
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let toolLive =
        ClaimFence.checkBeforeCallAsync store tenant missing CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    checkpointed |> should equal false
    appended |> should equal None
    settled |> should equal None
    notified |> should equal false
    toolLive |> should equal false
    sink.Calls.Count |> should equal 0

[<Fact>]
let ``Null sink notifies nothing without touching the store`` () =
    let _, store, _ = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"

    let notified =
        ClaimFence.notifyIfLiveAsync
            store
            tenant
            claim
            (Unchecked.defaultof<ISessionCompletionSink>)
            (sampleCompletion session.Id "key-1")
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    notified |> should equal false

// ──────────────────────────────────────────────────────────────────────────
// Takeover race: the loser produces zero effects

[<Fact>]
let ``Takeover loser produces zero effects across all five fenced surfaces`` () =
    let clock, store, events = createStores ()
    let session = createSession store
    appendUser store session.Id "first"
    appendUser store session.Id "second"

    // Owner A holds the first turn; the clock lapses its lease and owner
    // B takes over on the second message.
    let loser = claimTurn store session.Id "owner-a"
    clock.Advance(TimeSpan.FromSeconds 121.0)
    let winner = claimTurn store session.Id "owner-b"

    winner.TurnId |> should not' (equal loser.TurnId)

    // The loser attempts every fenced surface: each skips.
    let loserCheckpoint =
        ClaimFence.checkpointUsageAsync store tenant loser (sampleUsage ()) CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let loserAppend =
        ClaimFence.appendEventsAsync
            store
            events
            tenant
            session.Id
            loser
            (ResizeArray([ startedEvent session.Id loser.TurnId ]) :> IReadOnlyList<SessionEvent>)
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let loserToolLive =
        ClaimFence.checkBeforeCallAsync store tenant loser CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let toolInvocations = ref []

    if loserToolLive then
        toolInvocations.Value <- toolInvocations.Value @ [ "loser-tool" ]

    let sink = RecordingSink()

    let loserNotified =
        ClaimFence.notifyIfLiveAsync
            store
            tenant
            loser
            (sink :> ISessionCompletionSink)
            (sampleCompletion session.Id "loser-key")
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let loserSettle =
        ClaimFence.settleTurnAsync store tenant loser TurnStatus.Completed null CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    loserCheckpoint |> should equal false
    loserAppend |> should equal None
    loserToolLive |> should equal false
    // Counts rather than `should equal []`: the matcher boxes a generic
    // empty list, which does not compare equal to a typed empty list.
    toolInvocations.Value.Length |> should equal 0
    loserNotified |> should equal false
    loserSettle |> should equal None

    // Zero journal effects: the journal holds nothing from the loser.
    let replay =
        events.Replay(tenant, session.Id, 0L, 10, CancellationToken.None).GetAwaiter().GetResult()

    match replay with
    | :? EventReplayEndOfStream -> ()
    | :? EventReplayPage as page when page.Events.Count = 0 -> ()
    | _ -> failwith "Expected an empty journal after the loser's append."

    // The winner's effects land afterward, proving the loser settled and
    // wrote nothing: the first sequence is still 1 and the turn settles
    // fresh instead of observing a prior settlement.
    let winnerCheckpoint =
        ClaimFence.checkpointUsageAsync store tenant winner (sampleUsage ()) CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let winnerAppend =
        ClaimFence.appendEventsAsync
            store
            events
            tenant
            session.Id
            winner
            (ResizeArray(
                [
                    startedEvent session.Id winner.TurnId
                ]
            )
            :> IReadOnlyList<SessionEvent>)
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let winnerNotified =
        ClaimFence.notifyIfLiveAsync
            store
            tenant
            winner
            (sink :> ISessionCompletionSink)
            (sampleCompletion session.Id "winner-key")
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let winnerSettle =
        ClaimFence.settleTurnAsync store tenant winner TurnStatus.Completed null CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    winnerCheckpoint |> should equal true

    match winnerAppend with
    | Some(:? EventAppended as applied) -> applied.Events[0].Sequence.Value |> should equal 1L
    | _ -> failwith "Expected the winner's append to land first."

    winnerNotified |> should equal true
    sink.Calls.Count |> should equal 1
    sink.Calls[0].IdempotencyKey |> should equal "winner-key"

    match winnerSettle with
    | Some(:? TurnSettled as settled) -> settled.Status |> should equal TurnStatus.Completed
    | _ -> failwith "Expected the winner's settlement to land fresh."

// ──────────────────────────────────────────────────────────────────────────
// Attempt stamping across resume re-claim

[<Fact>]
let ``Resume re-claim surfaces attempt plus one stamped onto the turn`` () =
    let clock, store, _ = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let first = claimTurn store session.Id "owner-a"
    first.Attempt |> should equal 1

    // A reply arrives for the open turn; past the lease, the same owner
    // re-claims (crash recovery) and the store resumes the turn id at
    // attempt 2.
    let reply = ReplyPayload(QuestionAnswer("q1", "go")) :> InboxPayload

    store.AppendInboxMessage(tenant, session.Id, reply, DeliveryMode.Queue, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()
    |> ignore

    clock.Advance(TimeSpan.FromSeconds 121.0)
    let second = claimTurn store session.Id "owner-a"

    second.TurnId |> should equal first.TurnId
    second.Attempt |> should equal 2

    let stamped = ClaimFence.stampAttempt second (sampleTurn session.Id second.TurnId)
    stamped.Attempt |> should equal 2
    stamped.Id |> should equal second.TurnId

[<Fact>]
let ``First claim stamps attempt one`` () =
    let _, store, _ = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"

    let stamped = ClaimFence.stampAttempt claim (sampleTurn session.Id claim.TurnId)
    stamped.Attempt |> should equal 1

// ──────────────────────────────────────────────────────────────────────────
// Seam wiring: the loop fence and the actor runner

/// Runs the loop with scripted provider answers and the given per-tool
/// fence, blocking for the settled result.
let private runFenced
    (responses: ChatResponse list)
    (tools: IReadOnlyDictionary<string, AITool>)
    (verifyClaim: (unit -> Task<bool>) option)
    : TurnResult =
    let client = new TurnLoopTests.ScriptedChatClient(responses) :> IChatClient
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            VerifyClaim = verifyClaim
        }

    TurnLoop.runAsync client history tools options CancellationToken.None TurnLoopTests.alwaysLeased
    |> fun task -> task.GetAwaiter().GetResult()

/// One assistant turn calling the named tools in order.
let private callResponse (calls: (string * string) list) : ChatResponse =
    new ChatResponse(ResizeArray<ChatMessage>([| TurnLoopTests.callMessage calls |]))

[<Fact>]
let ``Fenced-out tool call never invokes the tool and loses the lease`` () =
    let invocations = ref []
    let fn = TurnLoopTests.stubTool "lookup" "row-1" invocations
    let tools = TurnLoopTests.makeTools [ "lookup", fn ]

    let responses =
        [
            callResponse [ "c1", "lookup" ]
            TurnLoopTests.textResponse "finished"
        ]

    let outcome =
        try
            runFenced responses tools (Some(fun () -> Task.FromResult false)) |> Choice1Of2
        with ex ->
            Choice2Of2 ex

    match outcome with
    | Choice2Of2(:? TurnLoop.TurnLeaseLostException) -> ()
    | _ -> failwith "Expected the fenced-out tool call to lose the lease."

    // Counts rather than `should equal []`: the matcher boxes a generic
    // empty list, which does not compare equal to a typed empty list.
    invocations.Value.Length |> should equal 0

[<Fact>]
let ``Live fence lets the tool call through`` () =
    let invocations = ref []
    let fn = TurnLoopTests.stubTool "lookup" "row-1" invocations
    let tools = TurnLoopTests.makeTools [ "lookup", fn ]

    let responses =
        [
            callResponse [ "c1", "lookup" ]
            TurnLoopTests.textResponse "finished"
        ]

    let result = runFenced responses tools (Some(fun () -> Task.FromResult true))

    result.AssistantText |> should equal "finished"
    invocations.Value |> should equal [ "lookup" ]

[<Fact>]
let ``Actor runner with a dead lease faults without calling the provider`` () =
    let client =
        new TurnLoopTests.ScriptedChatClient([ TurnLoopTests.textResponse "done" ])

    let tools = TurnLoopTests.makeTools []

    let runner =
        SessionActor.createClaimedTurnRunner
            (client :> IChatClient)
            tools
            TurnLoop.TurnLoopOptions.Default
            (fun () -> false)
            None

    let payload = UserMessagePayload(UserMessage.Text("hi")) :> InboxPayload

    let entry =
        {
            SessionId = SessionId.New()
            Position = 1L
            Payload = payload
            Delivery = DeliveryMode.Queue
            Consumed = false
            AppendedAt = startInstant
        }

    let callsBefore = client.Calls

    let outcome =
        try
            runner entry CancellationToken.None
            |> fun task -> task.GetAwaiter().GetResult() |> Choice1Of2
        with ex ->
            Choice2Of2 ex

    match outcome with
    | Choice2Of2(:? TurnLoop.TurnLeaseLostException) -> ()
    | _ -> failwith "Expected the dead-lease runner to fault with lease loss."

    client.Calls |> should equal callsBefore

[<Fact>]
let ``Actor runner keeps its always-live default`` () =
    let client =
        new TurnLoopTests.ScriptedChatClient([ TurnLoopTests.textResponse "done" ])

    let tools = TurnLoopTests.makeTools []

    let runner =
        SessionActor.createTurnRunner (client :> IChatClient) tools TurnLoop.TurnLoopOptions.Default

    let payload = UserMessagePayload(UserMessage.Text("hi")) :> InboxPayload

    let entry =
        {
            SessionId = SessionId.New()
            Position = 1L
            Payload = payload
            Delivery = DeliveryMode.Queue
            Consumed = false
            AppendedAt = startInstant
        }

    let result =
        runner entry CancellationToken.None |> fun task -> task.GetAwaiter().GetResult()

    result.AssistantText |> should equal "done"
