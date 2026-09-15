// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TranscriptsTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Xunit

// Fixtures follow the CellsTests precedent: one session, fixed turns, an
// empty (in-flight) sequence, and one timestamp every event reuses.
let tenant = TenantId.Create "acme"
let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let parentTurn = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let subTurn = TurnId.Parse "01D7CB31YQKCJPY9FDTN2WTAFF"
let secondTurn = TurnId.Parse "01F8MECHZX3TBDSZ7XRADM79XV"
let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
let noSequence = Unchecked.defaultof<Nullable<int64>>
let nullString = Unchecked.defaultof<string>
let nullEvent = Unchecked.defaultof<SessionEvent>
let nullOptions = Unchecked.defaultof<ReadTranscriptOptions>
let nullEventStore = Unchecked.defaultof<ISessionEventStore>

let includeSubAgents = ReadTranscriptOptions()
let excludeSubAgents = ReadTranscriptOptions(IncludeSubAgentCells = false)

let read (events: SessionEvent list) (options: ReadTranscriptOptions) =
    TranscriptReader.Read(ResizeArray<SessionEvent>(events) :> IReadOnlyList<SessionEvent>, options)

let document cells = SessionCellJson.Document(cells)

// The issue-50 journal: a parent turn that spawns a sub-agent turn
// through the task tool, plus a second top-level turn with its own tool
// call. The sub-agent turn's tool events carry the parent call's id (the
// documented event-layer parent linkage); every other id starts in its
// own turn.
let parentCallId = "call-task-1"
let ownCallId = "call-search-1"
let subCallId = "call-read-9"

let issueJournal: SessionEvent list =
    [
        ToolCallStartedEvent(sessionId, parentTurn, noSequence, stamp, parentCallId, "task") :> SessionEvent
        TextDeltaEvent(sessionId, parentTurn, noSequence, stamp, "working") :> SessionEvent
        // Sub-agent turn, interleaved in sequence order.
        ToolCallStartedEvent(sessionId, subTurn, noSequence, stamp, parentCallId, "task") :> SessionEvent
        TextDeltaEvent(sessionId, subTurn, noSequence, stamp, "sub result") :> SessionEvent
        ToolCallOutputEvent(sessionId, subTurn, noSequence, stamp, parentCallId, "out") :> SessionEvent
        ToolCallCompletedEvent(sessionId, subTurn, noSequence, stamp, parentCallId, nullString) :> SessionEvent
        ToolCallOutputEvent(sessionId, parentTurn, noSequence, stamp, parentCallId, "done") :> SessionEvent
        ToolCallCompletedEvent(sessionId, parentTurn, noSequence, stamp, parentCallId, nullString) :> SessionEvent
        // Second top-level turn with its own call.
        TextDeltaEvent(sessionId, secondTurn, noSequence, stamp, "again") :> SessionEvent
        ToolCallStartedEvent(sessionId, secondTurn, noSequence, stamp, ownCallId, "search") :> SessionEvent
        ToolCallOutputEvent(sessionId, secondTurn, noSequence, stamp, ownCallId, "hit") :> SessionEvent
        ToolCallCompletedEvent(sessionId, secondTurn, noSequence, stamp, ownCallId, nullString) :> SessionEvent
    ]

// ───────────────────────────────────────────────────────────────────────────
// Pure read

[<Fact>]
let ``ReadTranscriptOptions defaults to including sub-agent cells`` () =
    ReadTranscriptOptions().IncludeSubAgentCells |> should equal true

[<Fact>]
let ``An empty journal reads no cells`` () =
    (read [] includeSubAgents).Count |> should equal 0
    (read [] excludeSubAgents).Count |> should equal 0

[<Fact>]
let ``Turns concatenate in first-seen order`` () =
    let events =
        [
            TextDeltaEvent(sessionId, parentTurn, noSequence, stamp, "one") :> SessionEvent
            TextDeltaEvent(sessionId, secondTurn, noSequence, stamp, "two") :> SessionEvent
        ]

    let cells = read events includeSubAgents

    cells.Count |> should equal 2
    cells[0].Content |> should equal "one"
    cells[0].TurnId |> should equal parentTurn
    cells[1].Content |> should equal "two"
    cells[1].TurnId |> should equal secondTurn

[<Fact>]
let ``Including sub-agent cells keeps the linked turn with its parent id`` () =
    let cells = read issueJournal includeSubAgents

    // Parent turn: call, assistant text, result; sub-agent turn: the
    // same three; second turn: assistant text, call, result.
    cells.Count |> should equal 9

    let subCells =
        cells |> Seq.filter (fun cell -> cell.TurnId = subTurn) |> Array.ofSeq

    subCells.Length |> should equal 3
    subCells[0].Kind |> should equal SessionCellKind.ToolCall
    subCells[0].ToolCallId |> should equal parentCallId
    subCells[1].Kind |> should equal SessionCellKind.Assistant
    subCells[1].Content |> should equal "sub result"
    subCells[2].Kind |> should equal SessionCellKind.ToolResult
    subCells[2].ToolCallId |> should equal parentCallId

[<Fact>]
let ``Excluding sub-agent cells drops the linked turn but keeps the parent call`` () =
    let cells = read issueJournal excludeSubAgents

    cells.Count |> should equal 6

    // No cell derives from the sub-agent turn.
    cells |> Seq.exists (fun cell -> cell.TurnId = subTurn) |> should equal false

    // The parent turn keeps its own task call: the id's first start is
    // the parent turn itself, so the parent never links to itself.
    let parentCall =
        cells
        |> Seq.filter (fun cell -> cell.TurnId = parentTurn && cell.Kind = SessionCellKind.ToolCall)
        |> Array.ofSeq

    parentCall.Length |> should equal 1
    parentCall[0].ToolCallId |> should equal parentCallId
    parentCall[0].ToolName |> should equal "task"

    // The second top-level turn is untouched.
    let second =
        cells |> Seq.filter (fun cell -> cell.TurnId = secondTurn) |> Array.ofSeq

    second.Length |> should equal 3

[<Fact>]
let ``Tool calls started in their own turn are never gated`` () =
    let events =
        [
            ToolCallStartedEvent(sessionId, parentTurn, noSequence, stamp, ownCallId, "search") :> SessionEvent
            ToolCallCompletedEvent(sessionId, parentTurn, noSequence, stamp, ownCallId, nullString) :> SessionEvent
            ToolCallStartedEvent(sessionId, secondTurn, noSequence, stamp, subCallId, "read_file") :> SessionEvent
            ToolCallCompletedEvent(sessionId, secondTurn, noSequence, stamp, subCallId, nullString) :> SessionEvent
        ]

    let cells = read events excludeSubAgents

    cells.Count |> should equal 4
    cells[0].ToolCallId |> should equal ownCallId
    cells[2].ToolCallId |> should equal subCallId

[<Fact>]
let ``Reader output matches the per-turn folds exactly`` () =
    // The reader groups and gates; it never re-derives. Pin the
    // composition against direct Fold calls over the same journal.
    let ofTurn turnId =
        let group = issueJournal |> List.filter (fun event -> event.TurnId = turnId)

        SessionCellDeriver.Fold(
            sessionId,
            turnId,
            null,
            stamp,
            ResizeArray<SessionEvent>(group) :> IReadOnlyList<SessionEvent>
        )

    let expected =
        [
            yield! ofTurn parentTurn
            yield! ofTurn subTurn
            yield! ofTurn secondTurn
        ]

    document (read issueJournal includeSubAgents)
    |> should equal (document (ResizeArray<SessionCell>(expected) :> IReadOnlyList<SessionCell>))

[<Fact>]
let ``Null events, null elements, and null options fail`` () =
    (fun () ->
        TranscriptReader.Read(Unchecked.defaultof<IReadOnlyList<SessionEvent>>, includeSubAgents)
        |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () ->
        TranscriptReader.Read(
            ResizeArray<SessionEvent>([ nullEvent ]) :> IReadOnlyList<SessionEvent>,
            includeSubAgents
        )
        |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () ->
        TranscriptReader.Read(ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>, nullOptions)
        |> ignore)
    |> should throw typeof<ArgumentNullException>

// ───────────────────────────────────────────────────────────────────────────
// Paged read over the in-memory store

let freshStores () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    (InMemoryStoreFactory.eventStore database, InMemoryStoreFactory.sessionStore database)

let claimSession (sessionStore: ISessionStore) =
    task {
        let session =
            {
                Id = SessionId.New()
                Tenant = tenant
                AgentId = AgentId.New()
                Title = "transcript"
                State = SessionState.Idle
                CurrentTurnId = Nullable()
                CreatedAt = DateTimeOffset.MinValue
                UpdatedAt = DateTimeOffset.MinValue
                ClosedAt = Nullable()
                WorkspaceBinding = null
                Options = SessionOptions()
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

let appendJournal (eventStore: ISessionEventStore) sessionId token (journal: SessionEvent list) =
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

[<Fact>]
let ``Paged read rejects a null store, null options, and a non-positive page size`` () =
    let sessionId = SessionId.New()

    (fun () ->
        Transcripts.readTranscript nullEventStore tenant sessionId includeSubAgents 10 CancellationToken.None
        |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () ->
        Transcripts.readTranscript (fst (freshStores ())) tenant sessionId nullOptions 10 CancellationToken.None
        |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () ->
        Transcripts.readTranscript (fst (freshStores ())) tenant sessionId includeSubAgents 0 CancellationToken.None
        |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

[<Fact>]
let ``Paged read returns empty cells for an empty journal`` () =
    task {
        let eventStore, sessionStore = freshStores ()
        let! sessionId, _ = claimSession sessionStore

        let! cells = Transcripts.readTranscript eventStore tenant sessionId includeSubAgents 5 CancellationToken.None

        cells.Count |> should equal 0
    }

[<Fact>]
let ``Paged read returns empty cells for an unknown session`` () =
    task {
        let eventStore, _ = freshStores ()

        // Unknown sessions are a settled tail like end of stream: the
        // read returns what it saw (nothing) instead of throwing, per
        // the replay contract's racing callers.
        let! cells =
            Transcripts.readTranscript eventStore tenant (SessionId.New()) includeSubAgents 5 CancellationToken.None

        cells.Count |> should equal 0
    }

// ───────────────────────────────────────────────────────────────────────────
// Chunking invariance (issue 50, Task 3)

[<Fact>]
let ``Every page size yields identical transcripts`` () =
    task {
        let eventStore, sessionStore = freshStores ()
        let! storedId, claim = claimSession sessionStore

        // Re-key the shared journal onto the stored session: content is
        // identical, only the session id differs.
        let journal =
            issueJournal
            |> List.map (fun event ->
                match event with
                | :? ToolCallStartedEvent as started ->
                    ToolCallStartedEvent(
                        storedId,
                        started.TurnId,
                        noSequence,
                        started.Timestamp,
                        started.ToolCallId,
                        started.ToolName
                    )
                    :> SessionEvent
                | :? ToolCallOutputEvent as output ->
                    ToolCallOutputEvent(
                        storedId,
                        output.TurnId,
                        noSequence,
                        output.Timestamp,
                        output.ToolCallId,
                        output.Output
                    )
                    :> SessionEvent
                | :? ToolCallCompletedEvent as completed ->
                    ToolCallCompletedEvent(
                        storedId,
                        completed.TurnId,
                        noSequence,
                        completed.Timestamp,
                        completed.ToolCallId,
                        completed.Error
                    )
                    :> SessionEvent
                | :? TextDeltaEvent as delta ->
                    TextDeltaEvent(storedId, delta.TurnId, noSequence, delta.Timestamp, delta.Text) :> SessionEvent
                | other -> other)

        do! appendJournal eventStore storedId claim.Token journal

        let expectedIncluded = document (read journal includeSubAgents)
        let expectedExcluded = document (read journal excludeSubAgents)

        // The included transcript keeps the sub-agent turn; the excluded
        // one drops it. Both are page-size independent below.
        (read journal includeSubAgents).Count |> should equal 9
        (read journal excludeSubAgents).Count |> should equal 6

        for pageSize in [ 1; 2; 3; 4; 5; 8; 16; 64 ] do
            let! included =
                Transcripts.readTranscript eventStore tenant storedId includeSubAgents pageSize CancellationToken.None

            document included |> should equal expectedIncluded

            let! excluded =
                Transcripts.readTranscript eventStore tenant storedId excludeSubAgents pageSize CancellationToken.None

            document excluded |> should equal expectedExcluded
    }

[<Fact>]
let ``Folding each page independently breaks the transcript`` () =
    task {
        let eventStore, sessionStore = freshStores ()
        let! storedId, claim = claimSession sessionStore

        let journal =
            issueJournal
            |> List.map (fun event ->
                match event with
                | :? ToolCallStartedEvent as started ->
                    ToolCallStartedEvent(
                        storedId,
                        started.TurnId,
                        noSequence,
                        started.Timestamp,
                        started.ToolCallId,
                        started.ToolName
                    )
                    :> SessionEvent
                | :? ToolCallOutputEvent as output ->
                    ToolCallOutputEvent(
                        storedId,
                        output.TurnId,
                        noSequence,
                        output.Timestamp,
                        output.ToolCallId,
                        output.Output
                    )
                    :> SessionEvent
                | :? ToolCallCompletedEvent as completed ->
                    ToolCallCompletedEvent(
                        storedId,
                        completed.TurnId,
                        noSequence,
                        completed.Timestamp,
                        completed.ToolCallId,
                        completed.Error
                    )
                    :> SessionEvent
                | :? TextDeltaEvent as delta ->
                    TextDeltaEvent(storedId, delta.TurnId, noSequence, delta.Timestamp, delta.Text) :> SessionEvent
                | other -> other)

        do! appendJournal eventStore storedId claim.Token journal

        // Collect the actual single-event transport pages.
        let pages = ResizeArray<SessionEvent list>()
        let mutable cursor = 0L
        let mutable paging = true

        while paging do
            let! outcome = eventStore.Replay(tenant, storedId, cursor, 1, CancellationToken.None)

            match outcome with
            | :? EventReplayPage as page ->
                pages.Add(List.ofSeq page.Events)

                if page.NextCursor.HasValue then
                    cursor <- page.NextCursor.Value
                else
                    paging <- false
            | _ -> paging <- false

        pages.Count |> should equal journal.Length

        // The rejected shape: regroup turns per page and fold each
        // chunk on its own. Single-event chunks split every run and
        // pairing, so this must differ from the invariant read: the
        // property test discriminates chunk-sensitive implementations.
        let strawman = ResizeArray<SessionCell>()

        for chunk in pages do
            let order = ResizeArray<TurnId>()
            let groups = Dictionary<TurnId, ResizeArray<SessionEvent>>()

            for event in chunk do
                match groups.TryGetValue(event.TurnId) with
                | true, group -> group.Add(event)
                | false, _ ->
                    order.Add(event.TurnId)

                    let group = ResizeArray<SessionEvent>()
                    group.Add(event)
                    groups[event.TurnId] <- group

            for turnId in order do
                strawman.AddRange(
                    SessionCellDeriver.Fold(
                        storedId,
                        turnId,
                        null,
                        stamp,
                        groups[turnId] :> IReadOnlyList<SessionEvent>
                    )
                )

        document strawman
        |> should not' (equal (document (read journal includeSubAgents)))
    }
