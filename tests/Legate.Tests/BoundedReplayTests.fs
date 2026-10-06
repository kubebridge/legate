// SPDX-License-Identifier: Apache-2.0
[<Xunit.Collection("Bounded")>]
module Legate.Tests.BoundedReplayTests

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

[<Xunit.CollectionDefinition("Bounded", DisableParallelization = true)>]
type BoundedCollection() = class end

// Shared hardened consume-pages helper plus the checkpoint-resumed suffix
// read (issue 389, Task 1): cursor/cancellation/empty-page hardening with
// fake-store unit tests, then the base-plus-suffix checkpoint mechanics
// over the real in-memory store.

let private tenant = TenantId.Create "bounded-replay"
let private sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let private turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FBV"
let private stamp = DateTimeOffset(2026, 10, 5, 0, 0, 0, TimeSpan.Zero)
let private noSequence = Unchecked.defaultof<Nullable<int64>>

let private textEvent (text: string) : SessionEvent =
    TextDeltaEvent(sessionId, turnId, noSequence, stamp, text) :> SessionEvent

let private userEvent (sid: SessionId) (turn: TurnId) (text: string) : SessionEvent =
    UserMessageEvent(sid, turn, noSequence, stamp, UserMessage.Text text) :> SessionEvent

let private stampedText (sequence: int64) (text: string) : SessionEvent =
    TextDeltaEvent(sessionId, turnId, Nullable sequence, stamp, text) :> SessionEvent

let private stampedBase (sequence: int64) (summary: string) : SessionEvent =
    CompactedEvent(
        sessionId,
        turnId,
        Nullable sequence,
        stamp,
        9000L,
        1200L,
        summary,
        ResizeArray<ChatMessage>() :> IReadOnlyList<ChatMessage>,
        SessionEventContract.CompactedContextVersion
    )
    :> SessionEvent

// ──────────────────────────────────────────────────────────────────────────
// Scripted store

type private ScriptedEventStore(handler: int64 -> Task<EventReplayOutcome>) =
    interface ISessionEventStore with
        member _.Replay(_, _, fromSequence, _, _) = handler fromSequence

        member _.Append(_, _, _, _, _) : Task<EventAppendOutcome> =
            raise (NotSupportedException("ScriptedEventStore supports Replay only."))

        member _.AppendHostEvents(_, _, _, _, _) : Task<EventAppendOutcome> =
            raise (NotSupportedException("ScriptedEventStore supports Replay only."))

        member _.TryClaimCleanup(_, _, _, _, _) : Task<EventCleanupState> =
            raise (NotSupportedException("ScriptedEventStore supports Replay only."))

        member _.CompleteCleanup(_, _, _, _, _) : Task<EventCleanupSettlement> =
            raise (NotSupportedException("ScriptedEventStore supports Replay only."))

        member _.DeferCleanup(_, _, _, _) : Task<EventCleanupSettlement> =
            raise (NotSupportedException("ScriptedEventStore supports Replay only."))

type private MysteryOutcome() =
    inherit EventReplayOutcome()

let private pageOf (events: SessionEvent list) (next: Nullable<int64>) =
    EventReplayPage(sessionId, ResizeArray<SessionEvent>(events) :> IReadOnlyList<SessionEvent>, next)
    :> EventReplayOutcome

let private scripted (outcomes: EventReplayOutcome list) =
    let queue = Queue<EventReplayOutcome>(outcomes)
    let cursors = ResizeArray<int64>()

    let store =
        ScriptedEventStore(fun cursor ->
            cursors.Add(cursor)

            match queue.TryDequeue() with
            | true, outcome -> Task.FromResult(outcome)
            | false, _ -> Task.FromResult(EventReplayEndOfStream(sessionId) :> EventReplayOutcome))

    store, cursors

let private consume (store: ScriptedEventStore) (start: int64) (pageSize: int) (token: CancellationToken) =
    let seen = ResizeArray<SessionEvent>()

    BoundedReplay.consumePagesAsync (store :> ISessionEventStore) tenant sessionId start pageSize token seen.Add

// ──────────────────────────────────────────────────────────────────────────
// Hardening

[<Fact>]
let ``An empty page with continuation settles without looping`` () : Task =
    task {
        BoundedReplay.clear ()

        let store, cursors =
            scripted
                [
                    pageOf [ textEvent "one" ] (Nullable 1L)
                    pageOf [] (Nullable 1L)
                ]

        let seen = ResizeArray<SessionEvent>()

        let! stats =
            BoundedReplay.consumePagesAsync
                (store :> ISessionEventStore)
                tenant
                sessionId
                0L
                5
                CancellationToken.None
                seen.Add

        seen.Count |> should equal 1
        stats.EventsSeen |> should equal 1
        stats.EndReason |> should equal "settledEmptyContinuation"
        cursors.Count |> should equal 2
    }

[<Fact>]
let ``A nonempty page with a repeated cursor fails explicitly`` () : Task =
    task {
        BoundedReplay.clear ()

        let store, cursors =
            scripted
                [
                    pageOf [ textEvent "one" ] (Nullable 0L)
                ]

        let! failed =
            task {
                try
                    let! _ = consume store 0L 5 CancellationToken.None
                    return false
                with :? InvalidOperationException ->
                    return true
            }

        failed |> should equal true
        cursors.Count |> should equal 1
    }

[<Fact>]
let ``A regressing cursor fails explicitly`` () : Task =
    task {
        BoundedReplay.clear ()

        let store, cursors =
            scripted
                [
                    pageOf [ textEvent "one" ] (Nullable 5L)
                    pageOf [ textEvent "two" ] (Nullable 3L)
                ]

        let! failed =
            task {
                try
                    let! _ = consume store 0L 5 CancellationToken.None
                    return false
                with :? InvalidOperationException ->
                    return true
            }

        failed |> should equal true
        cursors.Count |> should equal 2
    }

[<Fact>]
let ``Null and unknown replay outcomes fail explicitly`` () : Task =
    task {
        BoundedReplay.clear ()

        let nullStore, _ =
            scripted
                [
                    Unchecked.defaultof<EventReplayOutcome>
                ]

        let! nullFailed =
            task {
                try
                    let! _ = consume nullStore 0L 5 CancellationToken.None
                    return false
                with :? InvalidOperationException ->
                    return true
            }

        nullFailed |> should equal true

        let mysteryStore, _ =
            scripted
                [
                    MysteryOutcome() :> EventReplayOutcome
                ]

        let! mysteryFailed =
            task {
                try
                    let! _ = consume mysteryStore 0L 5 CancellationToken.None
                    return false
                with :? InvalidOperationException ->
                    return true
            }

        mysteryFailed |> should equal true
    }

[<Fact>]
let ``Settled tails return what was seen without throwing`` () : Task =
    task {
        BoundedReplay.clear ()

        for tail in
            [
                EventReplayEndOfStream(sessionId) :> EventReplayOutcome
                EventReplayUnknownSession(sessionId) :> EventReplayOutcome
                EventReplayJournalExpired(sessionId, null) :> EventReplayOutcome
            ] do
            let store, _ = scripted [ tail ]
            let! stats = consume store 0L 5 CancellationToken.None
            stats.EventsSeen |> should equal 0
    }

[<Fact>]
let ``A pre-cancelled consume abandons before the first fetch`` () : Task =
    task {
        BoundedReplay.clear ()

        let store, cursors =
            scripted
                [
                    EventReplayEndOfStream(sessionId) :> EventReplayOutcome
                ]

        use cts = new CancellationTokenSource()
        cts.Cancel()

        let! cancelled =
            task {
                try
                    let! _ = consume store 0L 5 cts.Token
                    return false
                with :? OperationCanceledException ->
                    return true
            }

        cancelled |> should equal true
        cursors.Count |> should equal 0
    }

[<Fact>]
let ``A cancel after the last fetch never reads back as complete`` () : Task =
    task {
        BoundedReplay.clear ()
        use cts = new CancellationTokenSource()

        let queue =
            Queue<EventReplayOutcome>(
                [
                    pageOf [ textEvent "one" ] (Nullable 1L)
                    EventReplayEndOfStream(sessionId) :> EventReplayOutcome
                ]
            )

        let handler (_cursor: int64) =
            let outcome =
                match queue.TryDequeue() with
                | true, next -> next
                | false, _ -> EventReplayEndOfStream(sessionId) :> EventReplayOutcome

            match outcome with
            | :? EventReplayEndOfStream as tail ->
                cts.Cancel()
                Task.FromResult(tail :> EventReplayOutcome)
            | _ -> Task.FromResult(outcome)

        let store = ScriptedEventStore(handler)

        let! cancelled =
            task {
                try
                    let! _ = consume store 0L 5 cts.Token
                    return false
                with :? OperationCanceledException ->
                    return true
            }

        cancelled |> should equal true
    }

[<Fact>]
let ``Consume rejects a null store and a non-positive page size`` () =
    BoundedReplay.clear ()
    let session = SessionId.New()

    (fun () ->
        BoundedReplay.consumePagesAsync
            Unchecked.defaultof<ISessionEventStore>
            tenant
            session
            0L
            5
            CancellationToken.None
            (fun _ -> ())
        |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () ->
        BoundedReplay.consumePagesAsync
            (scripted [] |> fst :> ISessionEventStore)
            tenant
            session
            0L
            0
            CancellationToken.None
            (fun _ -> ())
        |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

// ──────────────────────────────────────────────────────────────────────────
// Checkpoint mechanics over the real store

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
                Title = "bounded-replay"
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
        for chunk in List.chunkBySize 3 journal do
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

let private compactedBase (sid: SessionId) (turn: TurnId) (summary: string) (tail: ChatMessage list) : SessionEvent =
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

[<Fact>]
let ``Initial reconstruction retains only the base plus the post-base suffix`` () : Task =
    task {
        BoundedReplay.clear ()
        let eventStore, sessionStore = freshStores ()
        let! sid, claim = claimSession sessionStore
        let turnA = TurnId.New()
        let turnB = TurnId.New()

        // A large superseded prefix, one compacted base, and a two-event suffix.
        let prefix =
            [
                for i in 1..20 -> TextDeltaEvent(sid, turnA, noSequence, stamp, sprintf "old-%d" i) :> SessionEvent
            ]

        let tail =
            [
                ChatMessage(ChatRole.User, "kept question")
            ]

        let suffix =
            [
                UserMessageEvent(sid, turnB, noSequence, stamp, UserMessage.Text "new") :> SessionEvent
            ]

        do!
            appendJournal
                eventStore
                sid
                claim.Token
                (prefix
                 @ [
                     compactedBase sid turnA "kept facts" tail
                 ]
                 @ suffix)

        let! read = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 5 CancellationToken.None

        read.IsInitialReconstruction |> should equal true
        read.HadFallback |> should equal false
        read.BaseEvent.IsSome |> should equal true
        // Bounded retention: the suffix holds only the post-base events,
        // never the 20-event superseded prefix.
        read.Suffix.Count |> should equal 1
        read.Stats.EventsSeen |> should equal 22

        // The recovery input is the base plus the suffix only.
        let input = BoundedReplay.recoveryInputOf read
        input.Count |> should equal 2
    }

[<Fact>]
let ``Reads without a compacted base scan from cursor zero with complete history`` () : Task =
    task {
        BoundedReplay.clear ()
        let eventStore, sessionStore = freshStores ()
        let! sid, claim = claimSession sessionStore
        let turnA = TurnId.New()

        do!
            appendJournal
                eventStore
                sid
                claim.Token
                [
                    UserMessageEvent(sid, turnA, noSequence, stamp, UserMessage.Text "first") :> SessionEvent
                ]

        let! first = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 5 CancellationToken.None
        first.IsInitialReconstruction |> should equal true
        first.BaseEvent.IsNone |> should equal true

        let turnB = TurnId.New()

        do!
            appendJournal
                eventStore
                sid
                claim.Token
                [
                    UserMessageEvent(sid, turnB, noSequence, stamp, UserMessage.Text "second") :> SessionEvent
                ]

        let! second = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 5 CancellationToken.None

        // Without a compacted base nothing is superseded, so the whole
        // prefix stays applicable context: the read scans from cursor 0
        // (never a suffix-only shortcut that would drop history) and the
        // recovery stays complete.
        second.IsInitialReconstruction |> should equal false
        second.HadFallback |> should equal false
        second.BaseEvent.IsNone |> should equal true
        second.Stats.StartCursor |> should equal 0L
        second.Suffix.Count |> should equal 2

        match ConversationRecovery.tryRecoverCompacted (BoundedReplay.recoveryInputOf second) with
        | Ok history -> history.Count |> should equal 2
        | Error rejection -> failwithf "Recovery should succeed: %A" rejection
    }

[<Fact>]
let ``A second read with a cached base resumes from the checkpoint`` () : Task =
    task {
        BoundedReplay.clear ()
        let eventStore, sessionStore = freshStores ()
        let! sid, claim = claimSession sessionStore
        let turnA = TurnId.New()
        let turnB = TurnId.New()

        let prefix =
            [
                for i in 1..10 -> TextDeltaEvent(sid, turnA, noSequence, stamp, sprintf "old-%d" i) :> SessionEvent
            ]

        let tail =
            [
                ChatMessage(ChatRole.User, "kept question")
            ]

        do! appendJournal eventStore sid claim.Token (prefix @ [ compactedBase sid turnA "facts" tail ])

        let! first = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 4 CancellationToken.None
        first.IsInitialReconstruction |> should equal true
        first.Suffix.Count |> should equal 0

        do! appendJournal eventStore sid claim.Token [ userEvent sid turnB "next" ]

        let! second = BoundedReplay.readSuffixWithBaseAsync eventStore tenant sid 4 CancellationToken.None

        second.IsInitialReconstruction |> should equal false
        second.HadFallback |> should equal false
        // No repeated full-prefix scan: the second read starts where the
        // first settled and replays only the one incremental event.
        second.Stats.StartCursor |> should equal first.Stats.LastCursor
        second.Stats.StartCursor |> should greaterThan 0L
        second.Suffix.Count |> should equal 1
    }

[<Fact>]
let ``A stale checkpoint falls back to explicit reconstruction`` () : Task =
    task {
        BoundedReplay.clear ()

        // First read establishes the base plus cursor 10 on one store.
        let firstStore, _ =
            scripted
                [
                    pageOf
                        [
                            stampedBase 5L "facts"
                            stampedText 6L "e-6"
                        ]
                        (Nullable 6L)
                    pageOf [ stampedText 10L "e-10" ] (Nullable 10L)
                ]

        let! first =
            BoundedReplay.readSuffixWithBaseAsync
                (firstStore :> ISessionEventStore)
                tenant
                sessionId
                5
                CancellationToken.None

        first.IsInitialReconstruction |> should equal true
        first.BaseEvent.IsSome |> should equal true
        first.Stats.LastCursor |> should equal 10L

        // A second store for the same session whose journal skips ahead:
        // the first suffix sequence (20) does not continue the checkpoint
        // (10), so the read falls back to explicit reconstruction instead
        // of truncating silently. The handler serves the suffix attempt
        // from the checkpoint and the full journal from cursor 0.
        let gapEvents =
            [
                for seq in 20L .. 21L -> stampedText seq (sprintf "g-%d" seq)
            ]

        let fullEvents = [ stampedBase 5L "facts" ] @ gapEvents
        let gapCursors = ResizeArray<int64>()

        let gapStore =
            ScriptedEventStore(fun cursor ->
                gapCursors.Add(cursor)

                if cursor >= 21L then
                    Task.FromResult(EventReplayEndOfStream(sessionId) :> EventReplayOutcome)
                elif cursor = 0L then
                    Task.FromResult(pageOf fullEvents (Nullable 21L))
                else
                    Task.FromResult(pageOf gapEvents (Nullable 21L)))

        let! second =
            BoundedReplay.readSuffixWithBaseAsync
                (gapStore :> ISessionEventStore)
                tenant
                sessionId
                5
                CancellationToken.None

        second.IsInitialReconstruction |> should equal false
        second.HadFallback |> should equal true
        second.BaseEvent.IsSome |> should equal true
        second.Suffix.Count |> should equal 2
        // The stale suffix attempt plus the explicit reconstruction both ran.
        List.ofSeq gapCursors |> should equal [ 10L; 21L; 0L; 21L ]
    }

[<Fact>]
let ``Base-plus-suffix recovery equals the full-journal recovery`` () =
    BoundedReplay.clear ()
    let sid = SessionId.New()
    let turnA = TurnId.New()
    let turnB = TurnId.New()

    let prefix =
        [
            for i in 1..10 -> TextDeltaEvent(sid, turnA, noSequence, stamp, sprintf "old-%d" i) :> SessionEvent
        ]

    let tail =
        [
            ChatMessage(ChatRole.User, "kept question")
            ChatMessage(ChatRole.Assistant, "kept answer")
        ]

    let suffixEvents =
        [
            UserMessageEvent(sid, turnB, noSequence, stamp, UserMessage.Text "next") :> SessionEvent
            TextDeltaEvent(sid, turnB, noSequence, stamp, "reply") :> SessionEvent
        ]

    let full =
        ResizeArray<SessionEvent>(
            prefix
            @ [
                compactedBase sid turnA "kept facts" tail
            ]
            @ suffixEvents
        )

    let read: BoundedReplay.SuffixRead =
        {
            BaseEvent =
                match full[10] with
                | :? CompactedEvent as baseEvent -> Some baseEvent
                | _ -> None
            Suffix = ResizeArray<SessionEvent>(suffixEvents) :> IReadOnlyList<SessionEvent>
            Stats =
                {
                    StartCursor = 11L
                    PagesFetched = 1
                    EventsSeen = 2
                    LastCursor = 13L
                    EndReason = "endOfStream"
                }
            IsInitialReconstruction = false
            HadFallback = false
        }

    let expected =
        ConversationRecovery.tryRecoverCompacted (full :> IReadOnlyList<SessionEvent>)

    let actual =
        ConversationRecovery.tryRecoverCompacted (BoundedReplay.recoveryInputOf read)

    match expected, actual with
    | Ok expectedHistory, Ok actualHistory ->
        actualHistory.Count |> should equal expectedHistory.Count

        for index in 0 .. expectedHistory.Count - 1 do
            actualHistory[index].Role |> should equal expectedHistory[index].Role
    | _ -> failwith "Both recoveries should succeed."
