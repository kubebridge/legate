// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TranscriptsTests

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

/// The pure read plus the end enrichment the served path applies: the
/// driver enriches once at the end, so driver output compares against
/// this, not against the bare fold.
let readEnriched (events: SessionEvent list) (options: ReadTranscriptOptions) =
    SessionArtifactEnrichment.enrichCells (read events options)

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
        ToolCallStartedEvent(sessionId, parentTurn, noSequence, stamp, parentCallId, "task", "{}") :> SessionEvent
        TextDeltaEvent(sessionId, parentTurn, noSequence, stamp, "working") :> SessionEvent
        // Sub-agent turn, interleaved in sequence order.
        ToolCallStartedEvent(sessionId, subTurn, noSequence, stamp, parentCallId, "task", "{}") :> SessionEvent
        TextDeltaEvent(sessionId, subTurn, noSequence, stamp, "sub result") :> SessionEvent
        ToolCallOutputEvent(sessionId, subTurn, noSequence, stamp, parentCallId, "out") :> SessionEvent
        ToolCallCompletedEvent(sessionId, subTurn, noSequence, stamp, parentCallId, nullString, "result")
        :> SessionEvent
        ToolCallOutputEvent(sessionId, parentTurn, noSequence, stamp, parentCallId, "done") :> SessionEvent
        ToolCallCompletedEvent(sessionId, parentTurn, noSequence, stamp, parentCallId, nullString, "result")
        :> SessionEvent
        // Second top-level turn with its own call.
        TextDeltaEvent(sessionId, secondTurn, noSequence, stamp, "again") :> SessionEvent
        ToolCallStartedEvent(sessionId, secondTurn, noSequence, stamp, ownCallId, "search", "{}") :> SessionEvent
        ToolCallOutputEvent(sessionId, secondTurn, noSequence, stamp, ownCallId, "hit") :> SessionEvent
        ToolCallCompletedEvent(sessionId, secondTurn, noSequence, stamp, ownCallId, nullString, "result")
        :> SessionEvent
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
            ToolCallStartedEvent(sessionId, parentTurn, noSequence, stamp, ownCallId, "search", "{}") :> SessionEvent
            ToolCallCompletedEvent(sessionId, parentTurn, noSequence, stamp, ownCallId, nullString, "result")
            :> SessionEvent
            ToolCallStartedEvent(sessionId, secondTurn, noSequence, stamp, subCallId, "read_file", "{}") :> SessionEvent
            ToolCallCompletedEvent(sessionId, secondTurn, noSequence, stamp, subCallId, nullString, "result")
            :> SessionEvent
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
                        started.ToolName,
                        "{}"
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
                        completed.Error,
                        "result"
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
                        started.ToolName,
                        "{}"
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
                        completed.Error,
                        "result"
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

// ───────────────────────────────────────────────────────────────────────────
// Issue 388: incremental derivation
//
// The feeder below mirrors SessionCellDeriver.Fold without retaining the
// journal: these tests pin equivalence with the pure read across chunkings,
// cursor/cancellation hardening, artifact and usage semantics, isolation,
// structural single-pass bounds, and the repeatable short/long profile.

let thirdTurn = TurnId.Parse "01F8MECHZX3TBDSZ7XRADM79XW"
let otherTenant = TenantId.Create "other-tenant"

let feedIncremental (sid: SessionId) (events: SessionEvent list) (options: ReadTranscriptOptions) =
    let derivation = Transcripts.IncrementalReader(sid)

    for ev in events do
        derivation.Feed(ev)

    derivation.Finish(options), derivation.EventsFed, derivation.TurnCount

// The rich journal: interleaved turns, a sub-agent pair, partial assistant
// output split across events, parallel tool calls with artifact references
// split across fragments, usage and progress markers between deltas, a
// permission exchange, an injected message, and an orphan completion with
// no observed start (which must derive nothing: no fabrication).
let artifactA =
    ArtifactReference.Format("report-a.pdf", "application/pdf", 12L, 0, 0)

let artifactAPart1 = artifactA.Substring(0, 20)
let artifactAPart2 = artifactA.Substring(20)
let artifactB = ArtifactReference.Format("notes-b.txt", "text/plain", 9L, 0, 0)

let richJournalFor (sid: SessionId) : SessionEvent list =
    [
        TextDeltaEvent(sid, parentTurn, noSequence, stamp, "par") :> SessionEvent
        UsageEvent(sid, parentTurn, noSequence, stamp, 10L, 2L) :> SessionEvent
        TextDeltaEvent(sid, parentTurn, noSequence, stamp, "tial") :> SessionEvent
        ToolCallStartedEvent(sid, parentTurn, noSequence, stamp, ownCallId, "search", "{}") :> SessionEvent
        ToolCallStartedEvent(sid, parentTurn, noSequence, stamp, subCallId, "read_file", "{}") :> SessionEvent
        ToolCallOutputEvent(sid, parentTurn, noSequence, stamp, ownCallId, "frag-a1 ") :> SessionEvent
        ToolCallOutputEvent(sid, parentTurn, noSequence, stamp, subCallId, "frag-b1 ") :> SessionEvent
        ToolCallOutputEvent(sid, parentTurn, noSequence, stamp, ownCallId, artifactAPart1) :> SessionEvent
        ToolCallOutputEvent(sid, parentTurn, noSequence, stamp, subCallId, artifactB) :> SessionEvent
        ToolCallOutputEvent(sid, parentTurn, noSequence, stamp, ownCallId, artifactAPart2) :> SessionEvent
        ToolCallCompletedEvent(sid, parentTurn, noSequence, stamp, ownCallId, nullString, "result") :> SessionEvent
        ToolCallCompletedEvent(sid, parentTurn, noSequence, stamp, subCallId, nullString, "result") :> SessionEvent
        TextDeltaEvent(sid, parentTurn, noSequence, stamp, "done") :> SessionEvent
        ToolCallStartedEvent(sid, parentTurn, noSequence, stamp, parentCallId, "task", "{}") :> SessionEvent
        ToolCallOutputEvent(sid, subTurn, noSequence, stamp, parentCallId, "sub out") :> SessionEvent
        ToolCallCompletedEvent(sid, subTurn, noSequence, stamp, parentCallId, nullString, "result") :> SessionEvent
        ToolCallOutputEvent(sid, parentTurn, noSequence, stamp, parentCallId, "parent out") :> SessionEvent
        ToolCallCompletedEvent(sid, parentTurn, noSequence, stamp, parentCallId, nullString, "result") :> SessionEvent
        PermissionRequestedEvent(sid, parentTurn, noSequence, stamp, "req-9", "search") :> SessionEvent
        PermissionResolvedEvent(sid, parentTurn, noSequence, stamp, "req-9", PermissionDecisionKind.AllowOnce)
        :> SessionEvent
        // Resumed orphan: output and completion with no observed start
        // derive nothing.
        ToolCallOutputEvent(sid, secondTurn, noSequence, stamp, "call-ghost-1", "lost") :> SessionEvent
        ToolCallCompletedEvent(sid, secondTurn, noSequence, stamp, "call-ghost-1", nullString, "result") :> SessionEvent
        TextDeltaEvent(sid, secondTurn, noSequence, stamp, "tail") :> SessionEvent
        UserMessageEvent(sid, thirdTurn, noSequence, stamp, UserMessage.Text "injected") :> SessionEvent
        TextDeltaEvent(sid, thirdTurn, noSequence, stamp, "third") :> SessionEvent
        TurnStartedEvent(sid, thirdTurn, noSequence, stamp) :> SessionEvent
        TurnCompletedEvent(sid, thirdTurn, noSequence, stamp) :> SessionEvent
    ]

let distinctTurns (journal: SessionEvent list) =
    journal |> List.map (fun ev -> ev.TurnId) |> List.distinct

/// Unwraps a cell's artifact references, failing the test when absent.
let artifactsOfCell (cell: SessionCell) : string list =
    match cell.Artifacts |> Option.ofObj with
    | Some artifacts -> artifacts |> Seq.toList
    | None -> failwith "Expected artifact references."

// Isolated SQLite databases for the transcript tests below. The shared
// SqliteTestFixture compiles after this file, so the two helpers it would
// lend (a fresh temp path plus best-effort sidecar cleanup) are repeated
// here in miniature instead of reordering the compile list.
let transcriptTempPath () : string =
    Path.Combine(Path.GetTempPath(), "legate-transcript-" + Guid.NewGuid().ToString("N") + ".db")

let deleteTranscriptTempFiles (path: string) : unit =
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

// ───────────────────────────────────────────────────────────────────────────
// Incremental parity

[<Fact>]
let ``Incremental feed matches the pure read across chunkings`` () =
    for chunk in [ 1; 2; 3; 5; 7; 64 ] do
        for options in [ includeSubAgents; excludeSubAgents ] do
            let expected = document (read issueJournal options)

            let derivation = Transcripts.IncrementalReader(sessionId)

            for events in List.chunkBySize chunk issueJournal do
                for ev in events do
                    derivation.Feed(ev)

            let actual = SessionCellJson.Document(derivation.Finish(options))
            actual |> should equal expected
            derivation.EventsFed |> should equal issueJournal.Length

[<Fact>]
let ``Incremental feed matches the per-turn folds exactly`` () =
    let ofTurn turnId =
        let group = issueJournal |> List.filter (fun ev -> ev.TurnId = turnId)

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

    let actual, fed, turns = feedIncremental sessionId issueJournal includeSubAgents

    document actual
    |> should equal (document (ResizeArray<SessionCell>(expected) :> IReadOnlyList<SessionCell>))

    fed |> should equal issueJournal.Length
    turns |> should equal 3

[<Fact>]
let ``Incremental feed rejects null events and null options`` () =
    let derivation = Transcripts.IncrementalReader(sessionId)

    (fun () -> derivation.Feed(Unchecked.defaultof<SessionEvent>) |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> derivation.Finish(Unchecked.defaultof<ReadTranscriptOptions>) |> ignore)
    |> should throw typeof<ArgumentNullException>

// ───────────────────────────────────────────────────────────────────────────
// Rich journal equivalence over the real stores

let appendRichJournal (eventStore: ISessionEventStore) sessionId token (journal: SessionEvent list) =
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
let ``Rich journal reads identically at every page size`` () =
    task {
        let eventStore, sessionStore = freshStores ()
        let! storedId, claim = claimSession sessionStore
        let journal = richJournalFor storedId
        do! appendRichJournal eventStore storedId claim.Token journal

        let expectedIncluded = document (readEnriched journal includeSubAgents)
        let expectedExcluded = document (readEnriched journal excludeSubAgents)

        for pageSize in [ 1; 2; 3; 4; 5; 8; 16; 64 ] do
            let! included =
                Transcripts.readTranscript eventStore tenant storedId includeSubAgents pageSize CancellationToken.None

            document included |> should equal expectedIncluded

            let! excluded =
                Transcripts.readTranscript eventStore tenant storedId excludeSubAgents pageSize CancellationToken.None

            document excluded |> should equal expectedExcluded

        // Repeated reads are stable.
        let! again = Transcripts.readTranscript eventStore tenant storedId includeSubAgents 3 CancellationToken.None

        document again |> should equal expectedIncluded
    }

[<Fact>]
let ``Split artifact references stay with the correct tool result`` () =
    task {
        let eventStore, sessionStore = freshStores ()
        let! storedId, claim = claimSession sessionStore
        let journal = richJournalFor storedId
        do! appendRichJournal eventStore storedId claim.Token journal

        let! cells = Transcripts.readTranscript eventStore tenant storedId includeSubAgents 1 CancellationToken.None

        let results =
            cells
            |> Seq.filter (fun cell -> cell.Kind = SessionCellKind.ToolResult)
            |> Array.ofSeq

        let byCall callId =
            results
            |> Array.filter (fun cell -> cell.ToolCallId = callId)
            |> Array.exactlyOne

        let first = byCall ownCallId
        first |> artifactsOfCell |> should equal [ "report-a.pdf" ]

        let second = byCall subCallId
        second |> artifactsOfCell |> should equal [ "notes-b.txt" ]

        // The orphan completion derives no result cell: required content
        // is not fabricated.
        results
        |> Array.exists (fun cell -> cell.ToolCallId = "call-ghost-1")
        |> should equal false
    }

[<Fact>]
let ``Usage markers add no cells and break no assistant runs`` () =
    task {
        let eventStore, sessionStore = freshStores ()
        let! storedId, claim = claimSession sessionStore
        let journal = richJournalFor storedId
        do! appendRichJournal eventStore storedId claim.Token journal

        let! cells = Transcripts.readTranscript eventStore tenant storedId includeSubAgents 1 CancellationToken.None

        let parentAssistant =
            cells
            |> Seq.filter (fun cell -> cell.TurnId = parentTurn && cell.Kind = SessionCellKind.Assistant)
            |> Array.ofSeq

        // "par" + "tial" stay one run despite the UsageEvent between them,
        // even when every event pages alone.
        parentAssistant.Length |> should equal 2
        parentAssistant[0].Content |> should equal "partial"
        parentAssistant[1].Content |> should equal "done"

        // Every output cell id was observed in the journal: nothing is
        // invented.
        let journalIds =
            journal
            |> List.choose (fun ev ->
                match ev with
                | :? ToolCallStartedEvent as started -> Some started.ToolCallId
                | :? ToolCallCompletedEvent as completed -> Some completed.ToolCallId
                | _ -> None)
            |> Set.ofList

        for cell in cells do
            match cell.ToolCallId with
            | null -> ()
            | id -> journalIds.Contains(id) |> should equal true
    }

[<Fact>]
let ``Rich journal reads identically over real SQLite`` () =
    task {
        let clock = TestClock()
        let path = transcriptTempPath ()
        let database = SqliteDatabase.Open(path, clock)

        try
            let eventStore = SqliteStoreFactory.eventStore database
            let sessionStore = SqliteStoreFactory.sessionStore database
            let! storedId, claim = claimSession sessionStore
            let journal = richJournalFor storedId
            do! appendRichJournal eventStore storedId claim.Token journal

            let expectedIncluded = document (readEnriched journal includeSubAgents)
            let expectedExcluded = document (readEnriched journal excludeSubAgents)

            for pageSize in [ 1; 2; 3; 4; 5; 8; 16; 64 ] do
                let! included =
                    Transcripts.readTranscript
                        eventStore
                        tenant
                        storedId
                        includeSubAgents
                        pageSize
                        CancellationToken.None

                document included |> should equal expectedIncluded

                let! excluded =
                    Transcripts.readTranscript
                        eventStore
                        tenant
                        storedId
                        excludeSubAgents
                        pageSize
                        CancellationToken.None

                document excluded |> should equal expectedExcluded
        finally
            (database :> IDisposable).Dispose()
            deleteTranscriptTempFiles path
    }

// ───────────────────────────────────────────────────────────────────────────
// Scripted replay fixtures: cursors, outcomes, cancellation

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

/// A delegating replay probe: counts or reacts to replays while every
/// other member passes through to the inner store.
type private ReplayProbe(inner: ISessionEventStore, onOutcome: EventReplayOutcome -> unit) =
    interface ISessionEventStore with
        member _.Replay(t, s, c, l, ct) =
            task {
                let! outcome = inner.Replay(t, s, c, l, ct)
                onOutcome outcome
                return outcome
            }

        member _.Append(t, s, c, e, ct) = inner.Append(t, s, c, e, ct)
        member _.AppendHostEvents(t, s, u, e, ct) = inner.AppendHostEvents(t, s, u, e, ct)

        member _.TryClaimCleanup(t, s, o, d, ct) = inner.TryClaimCleanup(t, s, o, d, ct)
        member _.CompleteCleanup(t, s, c, a, ct) = inner.CompleteCleanup(t, s, c, a, ct)
        member _.DeferCleanup(t, s, c, ct) = inner.DeferCleanup(t, s, c, ct)

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

[<Fact>]
let ``An empty page with continuation settles without looping`` () : Task =
    task {
        let first =
            [
                TextDeltaEvent(sessionId, parentTurn, noSequence, stamp, "one") :> SessionEvent
            ]

        let store, cursors =
            scripted
                [
                    pageOf first (Nullable 1L)
                    pageOf [] (Nullable 1L)
                ]

        let! cells =
            Transcripts.readTranscript
                (store :> ISessionEventStore)
                tenant
                sessionId
                includeSubAgents
                5
                CancellationToken.None

        cells.Count |> should equal 1
        cells[0].Content |> should equal "one"
        cursors.Count |> should equal 2
    }

[<Fact>]
let ``A nonempty page with a repeated cursor fails explicitly`` () : Task =
    task {
        let first =
            [
                TextDeltaEvent(sessionId, parentTurn, noSequence, stamp, "one") :> SessionEvent
            ]

        let store, cursors = scripted [ pageOf first (Nullable 0L) ]

        let! failed =
            task {
                try
                    let! _ =
                        Transcripts.readTranscript
                            (store :> ISessionEventStore)
                            tenant
                            sessionId
                            includeSubAgents
                            5
                            CancellationToken.None

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
        let first =
            [
                TextDeltaEvent(sessionId, parentTurn, noSequence, stamp, "one") :> SessionEvent
            ]

        let second =
            [
                TextDeltaEvent(sessionId, parentTurn, noSequence, stamp, "two") :> SessionEvent
            ]

        let store, cursors =
            scripted
                [
                    pageOf first (Nullable 5L)
                    pageOf second (Nullable 3L)
                ]

        let! failed =
            task {
                try
                    let! _ =
                        Transcripts.readTranscript
                            (store :> ISessionEventStore)
                            tenant
                            sessionId
                            includeSubAgents
                            5
                            CancellationToken.None

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
        let nullStore, _ =
            scripted
                [
                    Unchecked.defaultof<EventReplayOutcome>
                ]

        let! nullFailed =
            task {
                try
                    let! _ =
                        Transcripts.readTranscript
                            (nullStore :> ISessionEventStore)
                            tenant
                            sessionId
                            includeSubAgents
                            5
                            CancellationToken.None

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
                    let! _ =
                        Transcripts.readTranscript
                            (mysteryStore :> ISessionEventStore)
                            tenant
                            sessionId
                            includeSubAgents
                            5
                            CancellationToken.None

                    return false
                with :? InvalidOperationException ->
                    return true
            }

        mysteryFailed |> should equal true
    }

[<Fact>]
let ``Settled tails return what was seen without throwing`` () : Task =
    task {
        for tail in
            [
                EventReplayEndOfStream(sessionId) :> EventReplayOutcome
                EventReplayUnknownSession(sessionId) :> EventReplayOutcome
                EventReplayJournalExpired(sessionId, null) :> EventReplayOutcome
            ] do
            let store, _ = scripted [ tail ]

            let! cells =
                Transcripts.readTranscript
                    (store :> ISessionEventStore)
                    tenant
                    sessionId
                    includeSubAgents
                    5
                    CancellationToken.None

            cells.Count |> should equal 0
    }

[<Fact>]
let ``An expired journal reads empty after cleanup`` () : Task =
    task {
        let eventStore, sessionStore = freshStores ()
        let! storedId, claim = claimSession sessionStore
        let journal = richJournalFor storedId
        do! appendRichJournal eventStore storedId claim.Token journal

        let! before = Transcripts.readTranscript eventStore tenant storedId includeSubAgents 5 CancellationToken.None

        before.Count |> should greaterThan 0

        let! granted =
            eventStore.TryClaimCleanup(
                tenant,
                storedId,
                "cleanup-worker",
                TimeSpan.FromMinutes 5.,
                CancellationToken.None
            )

        let lease = (granted :?> EventCleanupClaimed).Claim

        let! completed = eventStore.CompleteCleanup(tenant, storedId, lease.Token, null, CancellationToken.None)

        (completed :? EventCleanupApplied) |> should equal true

        let! cells = Transcripts.readTranscript eventStore tenant storedId includeSubAgents 5 CancellationToken.None

        cells.Count |> should equal 0
    }

[<Fact>]
let ``A pre-cancelled read abandons before the first fetch`` () : Task =
    task {
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
                    let! _ =
                        Transcripts.readTranscript
                            (store :> ISessionEventStore)
                            tenant
                            sessionId
                            includeSubAgents
                            5
                            cts.Token

                    return false
                with :? OperationCanceledException ->
                    return true
            }

        cancelled |> should equal true
        cursors.Count |> should equal 0
    }

[<Fact>]
let ``A cancel between pages abandons without complete output`` () : Task =
    task {
        let eventStore, sessionStore = freshStores ()
        let! storedId, claim = claimSession sessionStore
        let journal = richJournalFor storedId
        do! appendRichJournal eventStore storedId claim.Token journal

        use cts = new CancellationTokenSource()

        let mutable calls = 0

        let cancelling =
            ReplayProbe(
                eventStore,
                fun outcome ->
                    calls <- calls + 1

                    match outcome with
                    | :? EventReplayPage -> cts.Cancel()
                    | _ -> ()
            )
            :> ISessionEventStore

        let! cancelled =
            task {
                try
                    let! _ = Transcripts.readTranscript cancelling tenant storedId includeSubAgents 2 cts.Token

                    return false
                with :? OperationCanceledException ->
                    return true
            }

        cancelled |> should equal true
        calls |> should equal 1
    }

[<Fact>]
let ``A cancel after the last fetch never reads back as complete`` () : Task =
    task {
        let first =
            [
                TextDeltaEvent(sessionId, parentTurn, noSequence, stamp, "one") :> SessionEvent
            ]

        use cts = new CancellationTokenSource()

        let recorded = ResizeArray<int64>()

        let queue =
            Queue<EventReplayOutcome>(
                [
                    pageOf first (Nullable 1L)
                    EventReplayEndOfStream(sessionId) :> EventReplayOutcome
                ]
            )

        let handler (cursor: int64) =
            recorded.Add(cursor)

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
                    let! _ =
                        Transcripts.readTranscript
                            (store :> ISessionEventStore)
                            tenant
                            sessionId
                            includeSubAgents
                            5
                            cts.Token

                    return false
                with :? OperationCanceledException ->
                    return true
            }

        cancelled |> should equal true
        recorded.Count |> should equal 2
    }

[<Fact>]
let ``Cross-tenant reads stay empty and repeated reads are stable`` () : Task =
    task {
        let eventStore, sessionStore = freshStores ()
        let! storedId, claim = claimSession sessionStore
        let journal = richJournalFor storedId
        do! appendRichJournal eventStore storedId claim.Token journal

        let! foreign =
            Transcripts.readTranscript eventStore otherTenant storedId includeSubAgents 5 CancellationToken.None

        foreign.Count |> should equal 0

        let! first = Transcripts.readTranscript eventStore tenant storedId includeSubAgents 3 CancellationToken.None
        let! second = Transcripts.readTranscript eventStore tenant storedId includeSubAgents 7 CancellationToken.None

        document first |> should equal (document second)
    }

// ───────────────────────────────────────────────────────────────────────────
// Structural bounds: single pass, no second journal, no prefix refolding

[<Fact>]
let ``The driver fetches every event exactly once`` () : Task =
    task {
        let eventStore, sessionStore = freshStores ()
        let! storedId, claim = claimSession sessionStore
        let journal = richJournalFor storedId
        do! appendRichJournal eventStore storedId claim.Token journal

        let mutable replays = 0
        let mutable delivered = 0

        let counting =
            ReplayProbe(
                eventStore,
                fun outcome ->
                    replays <- replays + 1

                    match outcome with
                    | :? EventReplayPage as page when not (isNull (box page)) && not (isNull (box page.Events)) ->
                        delivered <- delivered + page.Events.Count
                    | _ -> ()
            )
            :> ISessionEventStore

        let! cells = Transcripts.readTranscript counting tenant storedId includeSubAgents 4 CancellationToken.None

        // Every journaled event crosses the transport exactly once: the
        // read never refetches a page and never replays a prefix.
        delivered |> should equal journal.Length

        // Full pages over the journal plus the terminal call: a second
        // full pass would add another round of replays.
        replays |> should equal ((journal.Length + 4 - 1) / 4 + 1)

        document cells
        |> should equal (document (readEnriched journal includeSubAgents))

        // The incremental derivation agrees: one feed per event, one
        // entry per distinct turn, identical content once enriched.
        let incremental, fed, turns = feedIncremental storedId journal includeSubAgents
        fed |> should equal journal.Length
        turns |> should equal (distinctTurns journal).Length

        document (SessionArtifactEnrichment.enrichCells incremental)
        |> should equal (document cells)
    }

// ───────────────────────────────────────────────────────────────────────────
// Repeatable profile: history volume varies independently from output

type TranscriptProfile =
    {
        Fixture: string
        Events: int
        EventBytes: int64
        PageSizes: int list
        OutputCells: int
        OutputBytes: int
        EventsFed: int
        Turns: int
        Replays: int
        AllocatedBytes: int64
        ElapsedMs: int64
    }

let private eventBytes (journal: SessionEvent list) =
    journal
    |> List.sumBy (fun ev -> int64 (JsonSerializer.Serialize(ev, ev.GetType()).Length))

let private runProfile (name: string) (build: SessionId -> SessionEvent list) (pageSizes: int list) =
    task {
        let eventStore, sessionStore = freshStores ()
        let! storedId, claim = claimSession sessionStore
        let stored = build storedId
        do! appendRichJournal eventStore storedId claim.Token stored

        let expected = document (readEnriched stored includeSubAgents)
        let expectedCount = (readEnriched stored includeSubAgents).Count

        let mutable replays = 0

        let counting =
            ReplayProbe(eventStore, fun _ -> replays <- replays + 1) :> ISessionEventStore

        GC.Collect()
        GC.WaitForPendingFinalizers()
        let allocatedBefore = GC.GetAllocatedBytesForCurrentThread()
        let timer = Stopwatch.StartNew()

        for pageSize in pageSizes do
            let! cells =
                Transcripts.readTranscript counting tenant storedId includeSubAgents pageSize CancellationToken.None

            document cells |> should equal expected

        timer.Stop()
        let allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore

        let _, fed, turns = feedIncremental storedId stored includeSubAgents

        let profile =
            {
                Fixture = name
                Events = stored.Length
                EventBytes = eventBytes stored
                PageSizes = pageSizes
                OutputCells = expectedCount
                OutputBytes = expected.Length
                EventsFed = fed
                Turns = turns
                Replays = replays
                AllocatedBytes = allocated
                ElapsedMs = timer.ElapsedMilliseconds
            }

        printfn
            $"transcript-profile {profile.Fixture} events={profile.Events} eventBytes={profile.EventBytes} outputCells={profile.OutputCells} outputBytes={profile.OutputBytes} fed={profile.EventsFed} turns={profile.Turns} replays={profile.Replays} allocated={profile.AllocatedBytes} elapsedMs={profile.ElapsedMs}"

        return profile
    }

[<Fact>]
let ``Short and long sessions profile history independently from output`` () : Task =
    task {
        let historyOnly (sid: SessionId) =
            [
                // History-only volume: usage checkpoints and progress
                // markers derive no cells, so the long fixture fetches far
                // more events for the same transcript.
                for index in 1..200 do
                    UsageEvent(sid, parentTurn, noSequence, stamp, int64 index, 1L) :> SessionEvent
                    TurnStartedEvent(sid, parentTurn, noSequence, stamp) :> SessionEvent
                    TurnCompletedEvent(sid, parentTurn, noSequence, stamp) :> SessionEvent
            ]

        let shortBuild (sid: SessionId) = richJournalFor sid

        let longBuild (sid: SessionId) =
            [
                yield! richJournalFor sid
                yield! historyOnly sid
            ]

        let pageSizes = [ 1; 4; 16; 64 ]

        let! shortProfile = runProfile "short" shortBuild pageSizes
        let! longProfile = runProfile "long" longBuild pageSizes

        // Same output from far more history: output-proportional cost is
        // reported separately from history volume.
        longProfile.OutputCells |> should equal shortProfile.OutputCells
        longProfile.OutputBytes |> should equal shortProfile.OutputBytes
        longProfile.Events |> should greaterThan (shortProfile.Events * 3)

        // Structural bounds reproduce on both fixtures: every event is
        // fed exactly once and every page-size run agrees.
        shortProfile.EventsFed |> should equal shortProfile.Events
        longProfile.EventsFed |> should equal longProfile.Events
        shortProfile.Turns |> should equal (distinctTurns (shortBuild sessionId)).Length
        longProfile.Turns |> should equal (distinctTurns (longBuild sessionId)).Length

        // The profile reproduces: the same fixture reports the same
        // structural shape on a second run.
        let! rerun = runProfile "short-rerun" shortBuild pageSizes
        rerun.Events |> should equal shortProfile.Events
        rerun.OutputCells |> should equal shortProfile.OutputCells
        rerun.EventsFed |> should equal shortProfile.EventsFed
    }
