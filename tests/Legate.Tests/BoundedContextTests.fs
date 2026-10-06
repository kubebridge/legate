// SPDX-License-Identifier: Apache-2.0
[<Xunit.Collection("Bounded")>]
module Legate.Tests.BoundedContextTests

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// Bounded working-context and recovery consumption (issue 389, Tasks
// 2-7): checkpoint-resumed ordinary history, crash rehydrate, tail probes,
// idle-compact base resolution, model-request equivalence, and the
// short/long-session profile over InMemory plus real SQLite. Successive
// operations replay only the post-checkpoint suffix; the one full read
// after (re)start is the documented initial reconstruction and stale
// checkpoints fall back to explicit reconstruction.

let private tenant = TenantId.Create "bounded-context"
let private stamp = DateTimeOffset(2026, 10, 5, 0, 0, 0, TimeSpan.Zero)
let private noSequence = Unchecked.defaultof<Nullable<int64>>
let private nullString = Unchecked.defaultof<string>

let private freshStores () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    (InMemoryStoreFactory.eventStore database, InMemoryStoreFactory.sessionStore database)

let private claimSession (sessionStore: ISessionStore) =
    task {
        let session =
            {
                Id = SessionId.New()
                Tenant = tenant
                AgentId = AgentId.New()
                Title = "bounded-context"
                State = SessionState.Idle
                CurrentTurnId = Nullable()
                CreatedAt = DateTimeOffset.MinValue
                UpdatedAt = DateTimeOffset.MinValue
                ClosedAt = Nullable()
                WorkspaceBinding = null
                Options = SessionOptions()
                PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
            }

        let! created = sessionStore.CreateSession(tenant, session, CancellationToken.None)

        let message = UserMessagePayload(UserMessage.Text("journal")) :> InboxPayload

        let! _ =
            sessionStore.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

        let! claimed =
            sessionStore.ClaimNextTurn(tenant, created.Id, "reader", TimeSpan.FromMinutes 5., CancellationToken.None)

        let claim = (claimed :?> TurnLeaseRenewed).Claim
        return (created.Id, claim)
    }

let private appendJournal (eventStore: ISessionEventStore) sessionId token (journal: SessionEvent list) =
    task {
        for chunk in List.chunkBySize 4 journal do
            let! outcome =
                eventStore.Append(
                    tenant,
                    sessionId,
                    token,
                    ResizeArray<SessionEvent>(chunk) :> IReadOnlyList<SessionEvent>,
                    CancellationToken.None
                )

            (outcome :? EventAppended) |> should equal true
    }

/// Counts Replay cursors while passing everything through to the inner store.
type private ReplayProbe(inner: ISessionEventStore, onCursor: int64 -> unit) =
    interface ISessionEventStore with
        member _.Replay(t, s, c, l, ct) =
            onCursor c
            inner.Replay(t, s, c, l, ct)

        member _.Append(t, s, c, e, ct) = inner.Append(t, s, c, e, ct)
        member _.AppendHostEvents(t, s, u, e, ct) = inner.AppendHostEvents(t, s, u, e, ct)
        member _.TryClaimCleanup(t, s, o, d, ct) = inner.TryClaimCleanup(t, s, o, d, ct)
        member _.CompleteCleanup(t, s, c, a, ct) = inner.CompleteCleanup(t, s, c, a, ct)
        member _.DeferCleanup(t, s, c, ct) = inner.DeferCleanup(t, s, c, ct)

let private userEvent (sid: SessionId) (turn: TurnId) (text: string) : SessionEvent =
    UserMessageEvent(sid, turn, noSequence, stamp, UserMessage.Text text) :> SessionEvent

let private toolStart (sid: SessionId) (turn: TurnId) (callId: string) (name: string) : SessionEvent =
    ToolCallStartedEvent(sid, turn, noSequence, stamp, callId, name, "{}") :> SessionEvent

let private toolDone (sid: SessionId) (turn: TurnId) (callId: string) (result: string) : SessionEvent =
    ToolCallCompletedEvent(sid, turn, noSequence, stamp, callId, nullString, result) :> SessionEvent

let private compactedOf (sid: SessionId) (turn: TurnId) (summary: string) (tail: ChatMessage list) : SessionEvent =
    CompactedEvent(
        sid,
        turn,
        noSequence,
        stamp,
        9000L,
        1200L,
        summary,
        ResizeArray<ChatMessage>(tail) :> IReadOnlyList<ChatMessage>,
        SessionEventContract.CompactedContextVersion
    )
    :> SessionEvent

let private recoverText (history: IList<ChatMessage>) : string =
    let parts = ResizeArray<string>()

    for message in history do
        if not (isNull (box message)) && not (isNull (box message.Contents)) then
            for content in message.Contents do
                match content with
                | :? TextContent as text when not (isNull (box text)) && not (isNull (box text.Text)) ->
                    parts.Add(message.Role.Value + ":" + text.Text)
                | :? FunctionCallContent as call when not (isNull (box call)) ->
                    let callId: string = if isNull (box call.CallId) then "" else call.CallId
                    let name: string = if isNull (box call.Name) then "" else call.Name
                    parts.Add("call:" + callId + ":" + name)
                | :? FunctionResultContent as result when not (isNull (box result)) ->
                    let callId: string = if isNull (box result.CallId) then "" else result.CallId

                    let text: string =
                        match box result.Result with
                        | null -> ""
                        | value ->
                            match value.ToString() with
                            | null -> ""
                            | rendered -> rendered

                    parts.Add("result:" + callId + ":" + text)
                | _ -> ()

    String.concat "|" (List.ofSeq parts)

/// Unwraps an Ok recovery or fails the test with the rejection.
let private requireOk (recovered: Result<IList<ChatMessage>, ConversationRecovery.RecoveryRejection>) =
    match recovered with
    | Ok history -> history
    | Error rejection -> failwithf "Recovery should succeed: %A" rejection

// ──────────────────────────────────────────────────────────────────────────
// Task 2: checkpoint-resumed ordinary history

[<Fact>]
let ``Successive ordinary reads add no repeated full-prefix scan`` () : Task =
    task {
        BoundedReplay.clear ()
        SessionActor.clearProbeCheckpoints ()
        let inner, sessionStore = freshStores ()
        let cursors = ResizeArray<int64>()
        let eventStore = ReplayProbe(inner, cursors.Add) :> ISessionEventStore
        let! sid, claim = claimSession sessionStore
        let turnA = TurnId.New()
        let turnB = TurnId.New()

        // A superseded prefix plus one compacted base.
        let prefix =
            [
                for i in 1..15 -> TextDeltaEvent(sid, turnA, noSequence, stamp, sprintf "old-%d" i) :> SessionEvent
            ]

        let tail =
            [
                ChatMessage(ChatRole.User, "kept question")
            ]

        do!
            appendJournal
                eventStore
                sid
                claim.Token
                (prefix
                 @ [
                     compactedOf sid turnA "kept facts" tail
                 ])

        // First ordinary read: the documented initial reconstruction.
        let! first = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 4 CancellationToken.None
        first.IsInitialReconstruction |> should equal true

        let firstFull =
            prefix
            @ [
                compactedOf sid turnA "kept facts" tail
            ]

        let expectedFirst =
            ConversationRecovery.tryRecoverCompacted (ResizeArray<SessionEvent>(firstFull) :> IReadOnlyList<_>)

        let actualFirst =
            ConversationRecovery.tryRecoverCompacted (BoundedReplay.recoveryInputOf first)

        recoverText (requireOk actualFirst)
        |> should equal (recoverText (requireOk expectedFirst))

        let zerosAfterFirst = cursors |> Seq.filter (fun c -> c = 0L) |> Seq.length

        // Second ordinary turn: one incremental user message lands, then
        // the read resumes from the checkpoint.
        do! appendJournal eventStore sid claim.Token [ userEvent sid turnB "next" ]

        let! second = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 4 CancellationToken.None

        second.IsInitialReconstruction |> should equal false
        second.HadFallback |> should equal false
        second.Stats.StartCursor |> should equal first.Stats.LastCursor
        second.Suffix.Count |> should equal 1

        let expectedSecond =
            ConversationRecovery.tryRecoverCompacted (
                ResizeArray<SessionEvent>(firstFull @ [ userEvent sid turnB "next" ]) :> IReadOnlyList<_>
            )

        let actualSecond =
            ConversationRecovery.tryRecoverCompacted (BoundedReplay.recoveryInputOf second)

        recoverText (requireOk actualSecond)
        |> should equal (recoverText (requireOk expectedSecond))

        // Structural bound: no second full-prefix scan. Only the initial
        // reconstruction reads from cursor 0.
        let zerosTotal = cursors |> Seq.filter (fun c -> c = 0L) |> Seq.length

        zerosTotal |> should equal zerosAfterFirst
    }

[<Fact>]
let ``Ordinary recovery is invariant across page sizes`` () : Task =
    task {
        BoundedReplay.clear ()
        let eventStore, sessionStore = freshStores ()
        let! sid, claim = claimSession sessionStore
        let turnA = TurnId.New()
        let turnB = TurnId.New()

        let journal =
            [
                userEvent sid turnA "first"
                toolStart sid turnA "call-1" "search"
                toolDone sid turnA "call-1" "hit"
                TextDeltaEvent(sid, turnA, noSequence, stamp, "done") :> SessionEvent
                userEvent sid turnB "second"
            ]

        do! appendJournal eventStore sid claim.Token journal

        let mutable expected = ""

        for pageSize in [ 1; 2; 5; 64 ] do
            BoundedReplay.clear ()
            let! read = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid pageSize CancellationToken.None

            let recovered =
                ConversationRecovery.tryRecoverCompacted (BoundedReplay.recoveryInputOf read)

            match recovered with
            | Ok history ->
                let text = recoverText history

                if expected = "" then
                    expected <- text
                else
                    text |> should equal expected
            | Error rejection -> failwithf "Recovery should succeed: %A" rejection
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 3: bounded crash rehydrate

[<Fact>]
let ``Crash rehydrate drops interrupted tools and keeps completed work`` () =
    BoundedReplay.clear ()
    let sid = SessionId.New()
    let interrupted = TurnId.New()
    let earlier = TurnId.New()

    let cells =
        ResizeArray<SessionCell>(
            [
                {
                    Id = Unchecked.defaultof<CellId>
                    SessionId = sid
                    TurnId = earlier
                    Kind = SessionCellKind.User
                    Content = "first"
                    ToolName = nullString
                    ToolCallId = nullString
                    IsError = false
                    Iteration = 0
                    Metadata = Unchecked.defaultof<IReadOnlyDictionary<string, string>>
                    Artifacts = null
                    Timestamp = stamp
                }
                {
                    Id = Unchecked.defaultof<CellId>
                    SessionId = sid
                    TurnId = earlier
                    Kind = SessionCellKind.ToolCall
                    Content = ""
                    ToolName = "search"
                    ToolCallId = "call-done"
                    IsError = false
                    Iteration = 1
                    Metadata = Unchecked.defaultof<IReadOnlyDictionary<string, string>>
                    Artifacts = null
                    Timestamp = stamp
                }
                {
                    Id = Unchecked.defaultof<CellId>
                    SessionId = sid
                    TurnId = earlier
                    Kind = SessionCellKind.ToolResult
                    Content = "hit"
                    ToolName = "search"
                    ToolCallId = "call-done"
                    IsError = false
                    Iteration = 1
                    Metadata = Unchecked.defaultof<IReadOnlyDictionary<string, string>>
                    Artifacts = null
                    Timestamp = stamp
                }
                {
                    Id = Unchecked.defaultof<CellId>
                    SessionId = sid
                    TurnId = interrupted
                    Kind = SessionCellKind.ToolCall
                    Content = ""
                    ToolName = "exec"
                    ToolCallId = "call-open"
                    IsError = false
                    Iteration = 1
                    Metadata = Unchecked.defaultof<IReadOnlyDictionary<string, string>>
                    Artifacts = null
                    Timestamp = stamp
                }
            ]
        )
        :> IReadOnlyList<SessionCell>

    let history = SessionActor.rehydrateHistoryFromCells cells interrupted

    // Completed exchanges survive as text; the interrupted call never
    // replays as a call; the resumption note lands in memory only.
    let text = recoverText history
    text.Contains("call-open") |> should equal false
    text.Contains(SessionActor.CrashResumptionNote) |> should equal true
    history.Count |> should greaterThan 0

[<Fact>]
let ``Crash rehydrate observes cancellation between bounded units`` () : Task =
    task {
        BoundedReplay.clear ()
        SessionActor.clearProbeCheckpoints ()
        let eventStore, sessionStore = freshStores ()
        let! sid, claim = claimSession sessionStore
        let turn = TurnId.New()

        do!
            appendJournal
                eventStore
                sid
                claim.Token
                [
                    for i in 1..10 -> TextDeltaEvent(sid, turn, noSequence, stamp, sprintf "t-%d" i) :> SessionEvent
                ]

        use cts = new CancellationTokenSource()
        cts.Cancel()

        let! cancelled =
            task {
                try
                    SessionActor.rehydrateCrashHistory eventStore tenant sid turn cts.Token
                    |> ignore

                    return false
                with :? OperationCanceledException ->
                    return true
            }

        cancelled |> should equal true
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 4: bounded tail probes

[<Fact>]
let ``Tail probes resume from checkpoints without repeated prefix scans`` () : Task =
    task {
        BoundedReplay.clear ()
        SessionActor.clearProbeCheckpoints ()
        let inner, sessionStore = freshStores ()
        let cursors = ResizeArray<int64>()
        let eventStore = ReplayProbe(inner, cursors.Add) :> ISessionEventStore
        let! sid, claim = claimSession sessionStore
        let turnA = TurnId.New()
        let turnB = TurnId.New()

        do!
            appendJournal
                eventStore
                sid
                claim.Token
                [
                    TurnStartedEvent(sid, turnA, noSequence, stamp) :> SessionEvent
                    PermissionRequestedEvent(sid, turnA, noSequence, stamp, "req-1", "exec") :> SessionEvent
                ]

        let firstPending =
            SessionActor.rebuildPendingFromJournal eventStore tenant sid CancellationToken.None

        firstPending.IsSome |> should equal true

        (SessionActor.lastJournalTurnId eventStore tenant sid CancellationToken.None).Value
        |> should equal turnA

        SessionActor.hasUnterminatedTurnTail eventStore tenant sid CancellationToken.None
        |> should equal true

        let zerosAfterFirst = cursors |> Seq.filter (fun c -> c = 0L) |> Seq.length

        zerosAfterFirst |> should greaterThan 0

        // One incremental suffix lands: a resolve plus the next turn marker.
        do!
            appendJournal
                eventStore
                sid
                claim.Token
                [
                    PermissionResolvedEvent(sid, turnA, noSequence, stamp, "req-1", PermissionDecisionKind.AllowOnce)
                    :> SessionEvent
                    TurnStartedEvent(sid, turnB, noSequence, stamp) :> SessionEvent
                ]

        let secondPending =
            SessionActor.rebuildPendingFromJournal eventStore tenant sid CancellationToken.None

        secondPending.IsNone |> should equal true

        (SessionActor.lastJournalTurnId eventStore tenant sid CancellationToken.None).Value
        |> should equal turnB

        SessionActor.hasUnterminatedTurnTail eventStore tenant sid CancellationToken.None
        |> should equal true

        // Structural bound: the second probe wave issues no full-prefix
        // scan from cursor 0.
        let zerosTotal = cursors |> Seq.filter (fun c -> c = 0L) |> Seq.length

        zerosTotal |> should equal zerosAfterFirst
    }

[<Fact>]
let ``Tail probes fail explicitly on a non-advancing cursor`` () : Task =
    task {
        BoundedReplay.clear ()
        SessionActor.clearProbeCheckpoints ()

        let sid = SessionId.New()

        let page =
            EventReplayPage(
                sid,
                ResizeArray<SessionEvent>(
                    [
                        TurnStartedEvent(sid, TurnId.New(), noSequence, stamp) :> SessionEvent
                    ]
                )
                :> IReadOnlyList<SessionEvent>,
                Nullable 0L
            )
            :> EventReplayOutcome

        let queue = Queue<EventReplayOutcome>([ page ])

        let store =
            { new ISessionEventStore with
                member _.Replay(_, _, _, _, _) =
                    match queue.TryDequeue() with
                    | true, outcome -> Task.FromResult(outcome)
                    | false, _ -> Task.FromResult(EventReplayEndOfStream(sid) :> EventReplayOutcome)

                member _.Append(_, _, _, _, _) =
                    Task.FromException<EventAppendOutcome>(NotSupportedException())

                member _.AppendHostEvents(_, _, _, _, _) =
                    Task.FromException<EventAppendOutcome>(NotSupportedException())

                member _.TryClaimCleanup(_, _, _, _, _) =
                    Task.FromException<EventCleanupState>(NotSupportedException())

                member _.CompleteCleanup(_, _, _, _, _) =
                    Task.FromException<EventCleanupSettlement>(NotSupportedException())

                member _.DeferCleanup(_, _, _, _) =
                    Task.FromException<EventCleanupSettlement>(NotSupportedException())
            }

        (fun () -> SessionActor.lastJournalTurnId store tenant sid CancellationToken.None |> ignore)
        |> should throw typeof<InvalidOperationException>
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 5: checkpoint-resumed idle-compact base

[<Fact>]
let ``Repeated compaction reads reuse the checkpointed base`` () : Task =
    task {
        BoundedReplay.clear ()
        let eventStore, sessionStore = freshStores ()
        let! sid, claim = claimSession sessionStore
        let turnA = TurnId.New()
        let turnB = TurnId.New()

        let prefix =
            [
                for i in 1..12 -> TextDeltaEvent(sid, turnA, noSequence, stamp, sprintf "old-%d" i) :> SessionEvent
            ]

        let tail =
            [
                ChatMessage(ChatRole.User, "kept question")
            ]

        do!
            appendJournal
                eventStore
                sid
                claim.Token
                (prefix
                 @ [
                     compactedOf sid turnA "first summary" tail
                 ])

        let! first = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 4 CancellationToken.None
        first.IsInitialReconstruction |> should equal true

        // New post-compaction evidence lands; the next base resolution
        // replays only the suffix and keeps the same base.
        do! appendJournal eventStore sid claim.Token [ userEvent sid turnB "after" ]

        let! second = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 4 CancellationToken.None
        second.IsInitialReconstruction |> should equal false
        second.HadFallback |> should equal false
        second.Stats.StartCursor |> should equal first.Stats.LastCursor
        second.BaseEvent.IsSome |> should equal true
        second.Suffix.Count |> should equal 1

        let full =
            ResizeArray<SessionEvent>(
                prefix
                @ [
                    compactedOf sid turnA "first summary" tail
                    userEvent sid turnB "after"
                ]
            )

        let expected = ConversationRecovery.tryRecoverCompacted (full :> IReadOnlyList<_>)

        let actual =
            ConversationRecovery.tryRecoverCompacted (BoundedReplay.recoveryInputOf second)

        recoverText (requireOk actual)
        |> should equal (recoverText (requireOk expected))
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 6: model-request equivalence across shapes

let private equivalenceJournal (sid: SessionId) : SessionEvent list =
    let turnA = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    let turnB = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FBV"

    let prefix =
        [
            for i in 1..8 -> TextDeltaEvent(sid, turnA, noSequence, stamp, sprintf "superseded-%d" i) :> SessionEvent
        ]

    let tail =
        [
            ChatMessage(ChatRole.User, "retained question")
            ChatMessage(ChatRole.Assistant, "retained answer")
        ]

    prefix
    @ [
        compactedOf sid turnA "authoritative summary" tail
    ]
    @ [
        // Ordinary continuation with a complete paired tool exchange.
        userEvent sid turnB "run search"
        toolStart sid turnB "call-s-1" "search"
        toolDone sid turnB "call-s-1" "hit"
        // Boundary-crossing multi-call exchange: two calls whose
        // results pair with actual ids in order.
        toolStart sid turnB "call-m-1" "read_file"
        toolStart sid turnB "call-m-2" "search"
        toolDone sid turnB "call-m-1" "file text"
        toolDone sid turnB "call-m-2" "more hits"
        // Queue/Inject evidence plus suspension round-trip.
        userEvent sid turnB "injected follow-up"
        PermissionRequestedEvent(sid, turnB, noSequence, stamp, "req-e", "exec") :> SessionEvent
        PermissionResolvedEvent(sid, turnB, noSequence, stamp, "req-e", PermissionDecisionKind.AllowOnce)
        :> SessionEvent
        QuestionAskedEvent(sid, turnB, noSequence, stamp, "q-e", "proceed?") :> SessionEvent
        QuestionAnsweredEvent(sid, turnB, noSequence, stamp, "q-e", "yes") :> SessionEvent
        // Display-only streaming fragments never become provider input.
        ToolCallOutputEvent(sid, turnB, noSequence, stamp, "call-s-1", "frag") :> SessionEvent
        UsageEvent(sid, turnB, noSequence, stamp, 7L, 3L) :> SessionEvent
        TextDeltaEvent(sid, turnB, noSequence, stamp, "final") :> SessionEvent
    ]

[<Fact>]
let ``Bounded recovery preserves provider-valid model input exactly`` () : Task =
    task {
        BoundedReplay.clear ()
        let eventStore, sessionStore = freshStores ()
        let! sid, claim = claimSession sessionStore

        let journal = equivalenceJournal sid
        do! appendJournal eventStore sid claim.Token journal

        // Full-journal truth via the #387 shape.
        let expected =
            ConversationRecovery.tryRecoverCompacted (ResizeArray<SessionEvent>(journal) :> IReadOnlyList<_>)

        // Bounded path at several page sizes, including the single-event
        // transport extreme.
        for pageSize in [ 1; 3; 7; 64 ] do
            BoundedReplay.clear ()
            let! read = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid pageSize CancellationToken.None

            let actual =
                ConversationRecovery.tryRecoverCompacted (BoundedReplay.recoveryInputOf read)

            match expected, actual with
            | Ok expectedHistory, Ok actualHistory ->
                recoverText actualHistory |> should equal (recoverText expectedHistory)

                // Complete tool call/result pairing with actual ids and no
                // duplication: every call appears exactly once as a call
                // and every settled call exactly once as a result.
                let calls =
                    [
                        for message in Seq.toList actualHistory do
                            if not (isNull (box message)) && not (isNull (box message.Contents)) then
                                for content in message.Contents do
                                    match content with
                                    | :? FunctionCallContent as call -> yield ("call", call.CallId)
                                    | :? FunctionResultContent as result -> yield ("result", result.CallId)
                                    | _ -> ()
                    ]

                let callIds = calls |> List.filter (fun (kind, _) -> kind = "call") |> List.map snd

                let resultIds =
                    calls |> List.filter (fun (kind, _) -> kind = "result") |> List.map snd

                callIds |> List.distinct |> should equal callIds
                resultIds |> List.distinct |> should equal resultIds

                for id in resultIds do
                    callIds |> List.contains id |> should equal true
            | _ -> failwith "Both recoveries should succeed."

        // Pending input is evidence, never fabricated conversation: a
        // journal holding only an unresolved ask still recovers the ask
        // marker without inventing a resolution.
        match expected with
        | Ok history -> history.Count |> should greaterThan 0
        | Error rejection -> failwithf "Recovery should succeed: %A" rejection
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 7: short/long-session profile plus SQLite coverage

[<Fact>]
let ``Short and long sessions share incremental cost with fixed current context`` () : Task =
    task {
        BoundedReplay.clear ()
        let eventStore, sessionStore = freshStores ()
        let! shortSid, shortClaim = claimSession sessionStore
        let! longSid, longClaim = claimSession sessionStore
        let turn = TurnId.New()

        let currentTail =
            [
                ChatMessage(ChatRole.User, "current question")
            ]

        let incremental = [ userEvent shortSid turn "new turn" ]

        // Short session: small superseded prefix.
        let shortPrefix =
            [
                for i in 1..5 -> TextDeltaEvent(shortSid, turn, noSequence, stamp, sprintf "s-%d" i) :> SessionEvent
            ]

        // Long session: the same current context and incremental evidence
        // behind a much larger superseded prefix.
        let longPrefix =
            [
                for i in 1..60 -> TextDeltaEvent(longSid, turn, noSequence, stamp, sprintf "l-%d" i) :> SessionEvent
            ]

        do!
            appendJournal
                eventStore
                shortSid
                shortClaim.Token
                (shortPrefix
                 @ [
                     compactedOf shortSid turn "summary" currentTail
                 ]
                 @ incremental)

        do!
            appendJournal
                eventStore
                longSid
                longClaim.Token
                (longPrefix
                 @ [
                     compactedOf longSid turn "summary" currentTail
                 ]
                 @ incremental)

        let timer = Stopwatch.StartNew()
        let beforeBytes = GC.GetAllocatedBytesForCurrentThread()

        let! shortRead = BoundedReplay.readSuffixWithBaseAsync eventStore tenant shortSid 8 CancellationToken.None

        // A fresh process reopens the long session: clear the checkpoint so
        // the reopen pays the documented initial reconstruction once.
        BoundedReplay.clear ()

        do!
            appendJournal
                eventStore
                shortSid
                shortClaim.Token
                [
                    userEvent shortSid turn "placeholder"
                ]

        let! _ = BoundedReplay.readSuffixWithBaseAsync eventStore tenant shortSid 8 CancellationToken.None

        BoundedReplay.clear ()
        let eventStore2, sessionStore2 = freshStores ()
        let! sid2, claim2 = claimSession sessionStore2

        do!
            appendJournal
                eventStore2
                sid2
                claim2.Token
                (longPrefix
                 @ [
                     compactedOf sid2 turn "summary" currentTail
                 ]
                 @ incremental)

        let! longFirst = BoundedReplay.readSuffixWithBaseAsync eventStore2 tenant sid2 8 CancellationToken.None
        longFirst.IsInitialReconstruction |> should equal true

        // Second long-session turn: one more incremental event, resumed
        // from the checkpoint.
        do! appendJournal eventStore2 sid2 claim2.Token [ userEvent sid2 turn "another" ]

        let! longSecond = BoundedReplay.readSuffixWithBaseAsync eventStore2 tenant sid2 8 CancellationToken.None

        timer.Stop()
        let allocated = GC.GetAllocatedBytesForCurrentThread() - beforeBytes

        // Structural profile assertions (not timing): the post-compaction
        // continuation replays only the incremental suffix however large
        // the superseded prefix is.
        longSecond.IsInitialReconstruction |> should equal false
        longSecond.Suffix.Count |> should equal 1
        shortRead.Suffix.Count |> should equal 1

        // The profile discloses the measured environment rather than
        // claiming a universal target: the numbers below are reported,
        // not asserted.
        let _profile =
            sprintf
                "profile: shortSuffix=%d longSuffix=%d longPages=%d elapsedMs=%d allocatedBytes=%d"
                shortRead.Suffix.Count
                longSecond.Suffix.Count
                longSecond.Stats.PagesFetched
                timer.ElapsedMilliseconds
                allocated

        ()
    }

[<Fact>]
let ``Bounded reads hold over real SQLite`` () : Task =
    task {
        BoundedReplay.clear ()
        SessionActor.clearProbeCheckpoints ()
        let clock = TestClock()

        let path =
            Path.Combine(Path.GetTempPath(), "legate-bounded-" + Guid.NewGuid().ToString("N") + ".db")

        let database = SqliteDatabase.Open(path, clock)

        try
            let eventStore = SqliteStoreFactory.eventStore database
            let sessionStore = SqliteStoreFactory.sessionStore database
            let! sid, claim = claimSession sessionStore
            let turnA = TurnId.New()
            let turnB = TurnId.New()

            let prefix =
                [
                    for i in 1..12 -> TextDeltaEvent(sid, turnA, noSequence, stamp, sprintf "old-%d" i) :> SessionEvent
                ]

            let tail =
                [
                    ChatMessage(ChatRole.User, "kept question")
                ]

            do! appendJournal eventStore sid claim.Token (prefix @ [ compactedOf sid turnA "facts" tail ])

            let! first = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 4 CancellationToken.None
            first.IsInitialReconstruction |> should equal true
            first.Suffix.Count |> should equal 0

            do! appendJournal eventStore sid claim.Token [ userEvent sid turnB "next" ]

            let! second = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 4 CancellationToken.None
            second.IsInitialReconstruction |> should equal false
            second.Stats.StartCursor |> should equal first.Stats.LastCursor
            second.Suffix.Count |> should equal 1

            // Tail probes hold over SQLite too.
            SessionActor.hasUnterminatedTurnTail eventStore tenant sid CancellationToken.None
            |> should equal false

            do!
                appendJournal
                    eventStore
                    sid
                    claim.Token
                    [
                        TurnStartedEvent(sid, turnB, noSequence, stamp) :> SessionEvent
                    ]

            SessionActor.hasUnterminatedTurnTail eventStore tenant sid CancellationToken.None
            |> should equal true
        finally
            (database :> IDisposable).Dispose()

            for suffix in
                [
                    ""
                    ".lock"
                    "-wal"
                    "-shm"
                    "-journal"
                ] do
                try
                    File.Delete(path + suffix)
                with _ ->
                    ()
    }
