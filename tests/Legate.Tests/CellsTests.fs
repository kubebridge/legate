// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.CellsTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Text.Json.Serialization
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Xunit

// The generic Deserialize<'T> overload is annotated to return 'T | null,
// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

let nullString = Unchecked.defaultof<string>

let jsonOptions = JsonSerializerOptions()

// The shared fold fixture: one session, one turn, an empty (in-flight)
// sequence, and one timestamp every event reuses, following the
// EventsTests precedent.
let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
let noSequence = Unchecked.defaultof<Nullable<int64>>

// ───────────────────────────────────────────────────────────────────────────
// Shape

[<Fact>]
let ``SessionCellKind has exactly the documented members and values`` () =
    let kinds =
        [
            SessionCellKind.User, 0
            SessionCellKind.Assistant, 1
            SessionCellKind.ToolCall, 2
            SessionCellKind.ToolResult, 3
            SessionCellKind.System, 4
        ]

    for kind, value in kinds do
        int kind |> should equal value

    // The enum is closed: five members, values 0..4, no extras.
    Enum.GetValues(typeof<SessionCellKind>).Length |> should equal 5

// ───────────────────────────────────────────────────────────────────────────
// JSON and JSONL

[<Fact>]
let ``A fully populated cell round-trips through JSON`` () =
    let metadata = Dictionary<string, string>()
    metadata["source"] <- "cli"

    let cell =
        {
            Id = CellId.New()
            SessionId = sessionId
            TurnId = turnId
            Kind = SessionCellKind.ToolResult
            Content = "line one"
            ToolName = "read_file"
            ToolCallId = "call-1"
            IsError = true
            Iteration = 2
            Metadata = metadata :> IReadOnlyDictionary<string, string>
            Artifacts = null
            Timestamp = stamp
        }

    let json = SessionCellJson.Line cell
    let restored = SessionCellJson.Parse json

    restored.Id |> should equal cell.Id
    restored.SessionId |> should equal cell.SessionId
    restored.TurnId |> should equal cell.TurnId
    restored.Kind |> should equal cell.Kind
    restored.Content |> should equal cell.Content
    restored.ToolName |> should equal cell.ToolName
    restored.ToolCallId |> should equal cell.ToolCallId
    restored.IsError |> should equal cell.IsError
    restored.Iteration |> should equal cell.Iteration

    match restored.Metadata with
    | null -> failwith "metadata was lost in the round-trip"
    | meta -> meta["source"] |> should equal "cli"

    restored.Timestamp |> should equal cell.Timestamp

[<Fact>]
let ``Nullable string fields round-trip as JSON null`` () =
    let cell =
        {
            Id = CellId.New()
            SessionId = sessionId
            TurnId = turnId
            Kind = SessionCellKind.Assistant
            Content = "hello"
            ToolName = nullString
            ToolCallId = nullString
            IsError = false
            Iteration = 1
            Metadata = null
            Artifacts = null
            Timestamp = stamp
        }

    let json = SessionCellJson.Line cell
    json.Contains("\"ToolName\":null") |> should equal true
    json.Contains("\"ToolCallId\":null") |> should equal true

    let restored = SessionCellJson.Parse json
    restored.ToolName |> should equal null
    restored.ToolCallId |> should equal null
    restored.Metadata |> should equal null

[<Fact>]
let ``Line serialises with PascalCase properties and a numeric enum`` () =
    let cell =
        {
            Id = CellId.New()
            SessionId = sessionId
            TurnId = turnId
            Kind = SessionCellKind.Assistant
            Content = "hello"
            ToolName = nullString
            ToolCallId = nullString
            IsError = false
            Iteration = 1
            Metadata = null
            Artifacts = null
            Timestamp = stamp
        }

    let json = SessionCellJson.Line cell

    json.Contains("\"Kind\":1") |> should equal true
    json.Contains("\"SessionId\":\"" + sessionId.Value + "\"") |> should equal true
    json.Contains("\"Content\":\"hello\"") |> should equal true

[<Fact>]
let ``Line keeps multiline content on one JSON line`` () =
    // STJ escapes newlines inside string values, so a cell never spans
    // lines: the pin for the JSONL export contract.
    let cell =
        {
            Id = CellId.New()
            SessionId = sessionId
            TurnId = turnId
            Kind = SessionCellKind.User
            Content = "first\nsecond\r\nthird"
            ToolName = nullString
            ToolCallId = nullString
            IsError = false
            Iteration = 0
            Metadata = null
            Artifacts = null
            Timestamp = stamp
        }

    let json = SessionCellJson.Line cell

    json.Contains('\n') |> should equal false
    json.Contains('\r') |> should equal false

    let restored = SessionCellJson.Parse json
    restored.Content |> should equal cell.Content

[<Fact>]
let ``Document emits one line per cell in order and parses back`` () =
    let first =
        {
            Id = CellId.New()
            SessionId = sessionId
            TurnId = turnId
            Kind = SessionCellKind.User
            Content = "hello"
            ToolName = nullString
            ToolCallId = nullString
            IsError = false
            Iteration = 0
            Metadata = null
            Artifacts = null
            Timestamp = stamp
        }

    let second =
        {
            Id = CellId.New()
            SessionId = sessionId
            TurnId = turnId
            Kind = SessionCellKind.Assistant
            Content = "line one\nline two"
            ToolName = nullString
            ToolCallId = nullString
            IsError = false
            Iteration = 1
            Metadata = null
            Artifacts = null
            Timestamp = stamp
        }

    let document = SessionCellJson.Document([| first; second |])

    // Two cells, two lines; no cell spans a line despite the embedded
    // newline in the second cell's content.
    document.Split('\n').Length |> should equal 2

    let restored = SessionCellJson.ParseDocument document

    restored.Count |> should equal 2
    restored[0].Id |> should equal first.Id
    restored[0].Content |> should equal "hello"
    restored[1].Id |> should equal second.Id
    restored[1].Content |> should equal "line one\nline two"

[<Fact>]
let ``Document of an empty transcript is empty and parses back empty`` () =
    let document = SessionCellJson.Document(Array.empty<SessionCell>)
    document |> should equal ""
    SessionCellJson.ParseDocument document |> Seq.length |> should equal 0

[<Fact>]
let ``Document of one cell is a single line without a trailing newline`` () =
    let cell =
        {
            Id = CellId.New()
            SessionId = sessionId
            TurnId = turnId
            Kind = SessionCellKind.System
            Content = "allowed once"
            ToolName = nullString
            ToolCallId = nullString
            IsError = false
            Iteration = 1
            Metadata = null
            Artifacts = null
            Timestamp = stamp
        }

    let document = SessionCellJson.Document([| cell |])

    document.EndsWith('\n') |> should equal false
    document.Split('\n').Length |> should equal 1

// ───────────────────────────────────────────────────────────────────────────
// Derivation fixtures

let userMessage text = UserMessage.Text text

let foldEvents events =
    SessionCellDeriver.Fold(sessionId, turnId, null, stamp, events)

let foldWithMessage message events =
    SessionCellDeriver.Fold(sessionId, turnId, message, stamp, events)

let kindOf (cells: IReadOnlyList<SessionCell>) index = cells[index].Kind

[<Fact>]
let ``A user message folds into one user cell with joined text parts`` () =
    let parts =
        [
            TextContent("first") :> AIContent
            TextContent("second") :> AIContent
        ]

    let message = UserMessage(parts, null)
    let cells = foldWithMessage message []

    cells.Count |> should equal 1
    cells[0].Kind |> should equal SessionCellKind.User
    cells[0].Content |> should equal "first\nsecond"
    cells[0].Iteration |> should equal 0
    cells[0].Timestamp |> should equal stamp
    cells[0].IsError |> should equal false
    cells[0].SessionId |> should equal sessionId
    cells[0].TurnId |> should equal turnId
    cells[0].ToolName |> should equal null
    cells[0].ToolCallId |> should equal null
    cells[0].Metadata |> should equal null
    cells[0].Artifacts |> should equal null

[<Fact>]
let ``The user cell carries the message metadata verbatim`` () =
    let metadata = Dictionary<string, string>()
    metadata["source"] <- "webhook"

    let message = UserMessage([ TextContent("hi") :> AIContent ], metadata)
    let cells = foldWithMessage message []

    cells.Count |> should equal 1

    match cells[0].Metadata with
    | null -> failwith "message metadata was lost"
    | meta -> meta["source"] |> should equal "webhook"

[<Fact>]
let ``Contiguous text deltas fold into one assistant cell`` () =
    let events =
        [
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "Hel") :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp.AddMilliseconds 10.0, "lo ") :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp.AddMilliseconds 20.0, "world") :> SessionEvent
        ]

    let cells = foldEvents events

    cells.Count |> should equal 1
    cells[0].Kind |> should equal SessionCellKind.Assistant
    cells[0].Content |> should equal "Hello world"
    // The cell's timestamp is the first delta's.
    cells[0].Timestamp |> should equal stamp
    cells[0].Iteration |> should equal 1

[<Fact>]
let ``Reasoning deltas produce no cells`` () =
    let events =
        [
            ReasoningDeltaEvent(sessionId, turnId, noSequence, stamp, "thinking") :> SessionEvent
            ReasoningDeltaEvent(sessionId, turnId, noSequence, stamp, " more") :> SessionEvent
        ]

    let cells = foldEvents events
    cells.Count |> should equal 0

[<Fact>]
let ``A tool call derives a tool call cell immediately and a result cell on completion`` () =
    let events =
        [
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file") :> SessionEvent
            ToolCallOutputEvent(sessionId, turnId, noSequence, stamp.AddMilliseconds 5.0, "call-1", "line one")
            :> SessionEvent
            ToolCallOutputEvent(sessionId, turnId, noSequence, stamp.AddMilliseconds 6.0, "call-1", "\nline two")
            :> SessionEvent
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp.AddMilliseconds 7.0, "call-1", nullString)
            :> SessionEvent
        ]

    let cells = foldEvents events

    cells.Count |> should equal 2

    cells[0].Kind |> should equal SessionCellKind.ToolCall
    cells[0].ToolName |> should equal "read_file"
    cells[0].ToolCallId |> should equal "call-1"
    cells[0].Content |> should equal ""
    cells[0].Iteration |> should equal 1
    cells[0].Timestamp |> should equal stamp
    cells[0].IsError |> should equal false

    cells[1].Kind |> should equal SessionCellKind.ToolResult
    cells[1].ToolName |> should equal "read_file"
    cells[1].ToolCallId |> should equal "call-1"
    cells[1].Content |> should equal "line one\nline two"
    cells[1].Iteration |> should equal 1
    cells[1].Timestamp |> should equal (stamp.AddMilliseconds 7.0)
    cells[1].IsError |> should equal false

[<Fact>]
let ``A failed tool call with output keeps the output and sets is error`` () =
    let events =
        [
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file") :> SessionEvent
            ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, "call-1", "partial output") :> SessionEvent
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", "exit code 1") :> SessionEvent
        ]

    let cells = foldEvents events

    cells.Count |> should equal 2
    cells[1].Kind |> should equal SessionCellKind.ToolResult
    cells[1].Content |> should equal "partial output"
    cells[1].IsError |> should equal true

[<Fact>]
let ``A failed tool call with no output falls back to the error reason`` () =
    let events =
        [
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file") :> SessionEvent
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", "exit code 1") :> SessionEvent
        ]

    let cells = foldEvents events

    cells.Count |> should equal 2
    cells[1].Kind |> should equal SessionCellKind.ToolResult
    cells[1].Content |> should equal "exit code 1"
    cells[1].IsError |> should equal true

[<Fact>]
let ``A call started but never completed yields no result cell`` () =
    // The abort case: the journal ends mid-call, so no ToolResult cell
    // exists for the call.
    let events =
        [
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file") :> SessionEvent
            ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, "call-1", "partial") :> SessionEvent
        ]

    let cells = foldEvents events

    cells.Count |> should equal 1
    cells[0].Kind |> should equal SessionCellKind.ToolCall

[<Fact>]
let ``Permission events fold into system cells with request metadata`` () =
    let events =
        [
            PermissionRequestedEvent(sessionId, turnId, noSequence, stamp, "req-1", "write_file") :> SessionEvent
            PermissionResolvedEvent(sessionId, turnId, noSequence, stamp, "req-1", PermissionDecisionKind.AllowOnce)
            :> SessionEvent
        ]

    let cells = foldEvents events

    cells.Count |> should equal 2

    cells[0].Kind |> should equal SessionCellKind.System
    cells[0].Content |> should equal "write_file"
    cells[0].IsError |> should equal false

    match cells[0].Metadata with
    | null -> failwith "request metadata was lost"
    | meta ->
        meta["requestId"] |> should equal "req-1"
        meta["event"] |> should equal "permissionRequested"

    cells[1].Kind |> should equal SessionCellKind.System
    cells[1].Content |> should equal "AllowOnce"

    match cells[1].Metadata with
    | null -> failwith "request metadata was lost"
    | meta ->
        meta["requestId"] |> should equal "req-1"
        meta["event"] |> should equal "permissionResolved"
        meta["decision"] |> should equal "AllowOnce"

[<Fact>]
let ``Question events fold into system cells with question metadata`` () =
    let events =
        [
            QuestionAskedEvent(sessionId, turnId, noSequence, stamp, "q-1", "which colour?") :> SessionEvent
            QuestionAnsweredEvent(sessionId, turnId, noSequence, stamp, "q-1", "blue") :> SessionEvent
        ]

    let cells = foldEvents events

    cells.Count |> should equal 2

    cells[0].Kind |> should equal SessionCellKind.System
    cells[0].Content |> should equal "which colour?"

    match cells[0].Metadata with
    | null -> failwith "question metadata was lost"
    | meta ->
        meta["questionId"] |> should equal "q-1"
        meta["event"] |> should equal "questionAsked"

    cells[1].Kind |> should equal SessionCellKind.System
    cells[1].Content |> should equal "blue"

    match cells[1].Metadata with
    | null -> failwith "question metadata was lost"
    | meta ->
        meta["questionId"] |> should equal "q-1"
        meta["event"] |> should equal "questionAnswered"

[<Fact>]
let ``A failed turn folds into one system cell with is error`` () =
    let events =
        [
            TurnFailedEvent(sessionId, turnId, noSequence, stamp, "provider returned 500") :> SessionEvent
        ]

    let cells = foldEvents events

    cells.Count |> should equal 1
    cells[0].Kind |> should equal SessionCellKind.System
    cells[0].Content |> should equal "provider returned 500"
    cells[0].IsError |> should equal true

    match cells[0].Metadata with
    | null -> failwith "failure metadata was lost"
    | meta -> meta["event"] |> should equal "turnFailed"

[<Fact>]
let ``Progress markers produce no cells and do not break a delta run`` () =
    let events =
        [
            TurnStartedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "Hel") :> SessionEvent
            UsageEvent(sessionId, turnId, noSequence, stamp, 10L, 2L) :> SessionEvent
            CompactedEvent(sessionId, turnId, noSequence, stamp, 100L, 20L) :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "lo") :> SessionEvent
            TurnCompletedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
        ]

    let cells = foldEvents events

    // One assistant cell: the markers did not break the delta run and
    // added no cells of their own.
    cells.Count |> should equal 1
    cells[0].Kind |> should equal SessionCellKind.Assistant
    cells[0].Content |> should equal "Hello"

[<Fact>]
let ``A golden full turn folds into the exact documented cell sequence`` () =
    let message = userMessage "list the files"

    let events =
        [
            TurnStartedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "I will ") :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "check.") :> SessionEvent
            ReasoningDeltaEvent(sessionId, turnId, noSequence, stamp, "hmm") :> SessionEvent
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "list_dir") :> SessionEvent
            ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, "call-1", "a.txt") :> SessionEvent
            ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, "call-1", "\nb.txt") :> SessionEvent
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString) :> SessionEvent
            PermissionRequestedEvent(sessionId, turnId, noSequence, stamp, "req-1", "rm") :> SessionEvent
            PermissionResolvedEvent(sessionId, turnId, noSequence, stamp, "req-1", PermissionDecisionKind.Deny)
            :> SessionEvent
            QuestionAskedEvent(sessionId, turnId, noSequence, stamp, "q-1", "proceed without rm?") :> SessionEvent
            QuestionAnsweredEvent(sessionId, turnId, noSequence, stamp, "q-1", "yes") :> SessionEvent
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-2", "list_dir") :> SessionEvent
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-2", nullString) :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "Done.") :> SessionEvent
            UsageEvent(sessionId, turnId, noSequence, stamp, 100L, 20L) :> SessionEvent
            TurnCompletedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
        ]

    let cells = foldWithMessage message events

    cells.Count |> should equal 11

    cells[0].Kind |> should equal SessionCellKind.User
    cells[0].Content |> should equal "list the files"
    cells[0].Iteration |> should equal 0

    cells[1].Kind |> should equal SessionCellKind.Assistant
    cells[1].Content |> should equal "I will check."
    cells[1].Iteration |> should equal 1

    cells[2].Kind |> should equal SessionCellKind.ToolCall
    cells[2].ToolCallId |> should equal "call-1"
    cells[2].ToolName |> should equal "list_dir"
    cells[2].Iteration |> should equal 1

    cells[3].Kind |> should equal SessionCellKind.ToolResult
    cells[3].ToolCallId |> should equal "call-1"
    cells[3].Content |> should equal "a.txt\nb.txt"
    cells[3].Iteration |> should equal 1

    cells[4].Kind |> should equal SessionCellKind.System
    cells[4].Content |> should equal "rm"
    cells[4].Iteration |> should equal 1

    match cells[4].Metadata with
    | null -> failwith "request metadata was lost"
    | meta -> meta["requestId"] |> should equal "req-1"

    cells[5].Kind |> should equal SessionCellKind.System
    cells[5].Content |> should equal "Deny"
    cells[5].Iteration |> should equal 1

    cells[6].Kind |> should equal SessionCellKind.System
    cells[6].Content |> should equal "proceed without rm?"
    cells[6].Iteration |> should equal 1

    cells[7].Kind |> should equal SessionCellKind.System
    cells[7].Content |> should equal "yes"
    cells[7].Iteration |> should equal 1

    cells[8].Kind |> should equal SessionCellKind.ToolCall
    cells[8].ToolCallId |> should equal "call-2"
    // The second call starts after call-1 completed: a new tool-call
    // start is model activity, so the boundary advances the iteration.
    cells[8].Iteration |> should equal 2

    cells[9].Kind |> should equal SessionCellKind.ToolResult
    cells[9].ToolCallId |> should equal "call-2"
    cells[9].Iteration |> should equal 2

    // The trailing delta run flushes at the end of the fold window; it
    // is model activity after call-2 completed, so it opens the next
    // iteration.
    cells[10].Kind |> should equal SessionCellKind.Assistant
    cells[10].Content |> should equal "Done."
    cells[10].Iteration |> should equal 3

[<Fact>]
let ``Two tool calls in one iteration share its number`` () =
    // Parallel calls: both start before either completes, so no
    // completed call precedes the second start and the iteration does
    // not advance between them.
    let events =
        [
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "checking both") :> SessionEvent
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "list_dir") :> SessionEvent
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-2", "read_file") :> SessionEvent
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString) :> SessionEvent
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-2", nullString) :> SessionEvent
            ReasoningDeltaEvent(sessionId, turnId, noSequence, stamp, "done thinking") :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "Both done") :> SessionEvent
        ]

    let cells = foldEvents events

    cells.Count |> should equal 6

    cells[0].Kind |> should equal SessionCellKind.Assistant
    cells[0].Iteration |> should equal 1

    cells[1].Kind |> should equal SessionCellKind.ToolCall
    cells[1].ToolCallId |> should equal "call-1"
    cells[1].Iteration |> should equal 1

    cells[2].Kind |> should equal SessionCellKind.ToolCall
    cells[2].ToolCallId |> should equal "call-2"
    cells[2].Iteration |> should equal 1

    cells[3].Kind |> should equal SessionCellKind.ToolResult
    cells[3].ToolCallId |> should equal "call-1"
    cells[3].ToolName |> should equal "list_dir"
    cells[3].Iteration |> should equal 1

    cells[4].Kind |> should equal SessionCellKind.ToolResult
    cells[4].ToolCallId |> should equal "call-2"
    cells[4].ToolName |> should equal "read_file"
    cells[4].Iteration |> should equal 1

    // Post-result model activity is the boundary: the new cell is in
    // the next iteration.
    cells[5].Kind |> should equal SessionCellKind.Assistant
    cells[5].Content |> should equal "Both done"
    cells[5].Iteration |> should equal 2

[<Fact>]
let ``Model activity after a completed call advances the iteration`` () =
    let events =
        [
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "list_dir") :> SessionEvent
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString) :> SessionEvent
            ReasoningDeltaEvent(sessionId, turnId, noSequence, stamp, "thinking") :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "Found it") :> SessionEvent
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-2", "read_file") :> SessionEvent
            ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, "call-2", "body") :> SessionEvent
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-2", nullString) :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "Here it is") :> SessionEvent
        ]

    let cells = foldEvents events

    cells.Count |> should equal 6

    cells[0].Kind |> should equal SessionCellKind.ToolCall
    cells[0].Iteration |> should equal 1
    cells[1].Kind |> should equal SessionCellKind.ToolResult
    cells[1].Iteration |> should equal 1

    // The reasoning delta after the completed call is the boundary.
    cells[2].Kind |> should equal SessionCellKind.Assistant
    cells[2].Content |> should equal "Found it"
    cells[2].Iteration |> should equal 2

    cells[3].Kind |> should equal SessionCellKind.ToolCall
    cells[3].Iteration |> should equal 2
    cells[4].Kind |> should equal SessionCellKind.ToolResult
    cells[4].Iteration |> should equal 2

    // The trailing delta run flushes at the end of the fold window; it
    // is model activity after call-2 completed, so it opens the next
    // iteration.
    cells[5].Kind |> should equal SessionCellKind.Assistant
    cells[5].Content |> should equal "Here it is"
    cells[5].Iteration |> should equal 3

[<Fact>]
let ``Events from other turns are ignored`` () =
    let otherTurn = TurnId.Parse "01D7CB31YQKCJPY9FDTN2WTAFF"

    let events =
        [
            TextDeltaEvent(sessionId, otherTurn, noSequence, stamp, "sub-agent text") :> SessionEvent
            ToolCallStartedEvent(sessionId, otherTurn, noSequence, stamp, "call-1", "list_dir") :> SessionEvent
            ToolCallCompletedEvent(sessionId, otherTurn, noSequence, stamp, "call-1", nullString) :> SessionEvent
        ]

    let cells = foldEvents events
    cells.Count |> should equal 0

[<Fact>]
let ``Derived cells serialise and parse back through the JSONL helper`` () =
    let message = userMessage "hello"

    let events =
        [
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "Hi ") :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "there") :> SessionEvent
        ]

    let cells = foldWithMessage message events

    let document = SessionCellJson.Document(cells)
    let restored = SessionCellJson.ParseDocument document

    restored.Count |> should equal cells.Count
    restored[0].Kind |> should equal SessionCellKind.User
    restored[0].Content |> should equal "hello"
    restored[1].Kind |> should equal SessionCellKind.Assistant
    restored[1].Content |> should equal "Hi there"

// ───────────────────────────────────────────────────────────────────────────
// Event-kind pin test
//
// Maps every registered SessionEvent discriminator to its documented
// derivation rule. A future event kind (a new JsonDerivedType attribute)
// fails this suite until it is mapped here, so the fold's match can never
// silently misclassify a kind.

/// For each registered discriminator, the rule the fold applies: the
/// number of cells it derives from a bare event of that kind, plus the
/// kind of the first derived cell, or the User sentinel (never derived
/// for an event) when the event derives no cell.
let expectedRules: (string * int * SessionCellKind) list =
    [
        "turnStarted", 0, SessionCellKind.User // progress marker: no cells
        "textDelta", 1, SessionCellKind.Assistant
        "reasoningDelta", 0, SessionCellKind.User // transient: no cells
        "toolCallStarted", 1, SessionCellKind.ToolCall
        "toolCallOutput", 0, SessionCellKind.User // accumulates only
        "toolCallCompleted", 0, SessionCellKind.User // needs a started call
        "permissionRequested", 1, SessionCellKind.System
        "permissionResolved", 1, SessionCellKind.System
        "questionAsked", 1, SessionCellKind.System
        "questionAnswered", 1, SessionCellKind.System
        "usage", 0, SessionCellKind.User // progress marker: no cells
        "compacted", 0, SessionCellKind.User // progress marker: no cells
        "compactionFailed", 1, SessionCellKind.System // one System failure cell per failed compaction
        "turnCompleted", 0, SessionCellKind.User // progress marker: no cells
        "turnAborted", 0, SessionCellKind.User // progress marker: no cells
        "turnFailed", 1, SessionCellKind.System
        "sessionClosed", 0, SessionCellKind.User // progress marker: no cells
        "userMessage", 1, SessionCellKind.User // one User cell per folded Inject message
        "contextPruned", 1, SessionCellKind.System // one System audit cell per prune event
    ]

[<Fact>]
let ``Every registered event discriminator maps to its documented rule`` () =
    let declared =
        Attribute.GetCustomAttributes(typeof<SessionEvent>, typeof<JsonDerivedTypeAttribute>)
        |> Seq.cast<JsonDerivedTypeAttribute>
        |> Seq.map (fun attribute -> string attribute.TypeDiscriminator)
        |> Array.ofSeq

    // Nothing extra is declared beyond the mapped table, and nothing in
    // the table is unregistered: adding a kind without a mapping fails
    // here.
    declared.Length |> should equal expectedRules.Length

    for discriminator, _expectedCount, _expectedKind in expectedRules do
        declared |> Array.contains discriminator |> should equal true

[<Fact>]
let ``The fold classifies a bare event of every kind per the mapped rule`` () =
    for discriminator, expectedCount, expectedKind in expectedRules do
        // The upcast is written per branch rather than once after the
        // match: F# type inference pins the match's result type to the
        // first branch's concrete type otherwise.
        let buildEvent: unit -> SessionEvent =
            match discriminator with
            | "turnStarted" -> fun () -> TurnStartedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
            | "textDelta" -> fun () -> TextDeltaEvent(sessionId, turnId, noSequence, stamp, "x") :> SessionEvent
            | "reasoningDelta" ->
                fun () -> ReasoningDeltaEvent(sessionId, turnId, noSequence, stamp, "x") :> SessionEvent
            | "toolCallStarted" ->
                fun () -> ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call", "tool") :> SessionEvent
            | "toolCallOutput" ->
                fun () -> ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, "call", "out") :> SessionEvent
            | "toolCallCompleted" ->
                fun () ->
                    ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call", nullString) :> SessionEvent
            | "permissionRequested" ->
                fun () -> PermissionRequestedEvent(sessionId, turnId, noSequence, stamp, "req", "tool") :> SessionEvent
            | "permissionResolved" ->
                fun () ->
                    PermissionResolvedEvent(
                        sessionId,
                        turnId,
                        noSequence,
                        stamp,
                        "req",
                        PermissionDecisionKind.AllowOnce
                    )
                    :> SessionEvent
            | "questionAsked" ->
                fun () -> QuestionAskedEvent(sessionId, turnId, noSequence, stamp, "q", "why?") :> SessionEvent
            | "questionAnswered" ->
                fun () -> QuestionAnsweredEvent(sessionId, turnId, noSequence, stamp, "q", "because") :> SessionEvent
            | "usage" -> fun () -> UsageEvent(sessionId, turnId, noSequence, stamp, 1L, 2L) :> SessionEvent
            | "compacted" -> fun () -> CompactedEvent(sessionId, turnId, noSequence, stamp, 10L, 2L) :> SessionEvent
            | "compactionFailed" ->
                fun () -> CompactionFailedEvent(sessionId, turnId, noSequence, stamp, "model denied") :> SessionEvent
            | "turnCompleted" -> fun () -> TurnCompletedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
            | "turnAborted" ->
                fun () ->
                    TurnAbortedEvent(sessionId, turnId, noSequence, stamp, StopCause.ExplicitAbort, "host stop")
                    :> SessionEvent
            | "turnFailed" -> fun () -> TurnFailedEvent(sessionId, turnId, noSequence, stamp, "reason") :> SessionEvent
            | "sessionClosed" -> fun () -> SessionClosedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
            | "userMessage" ->
                fun () -> UserMessageEvent(sessionId, turnId, noSequence, stamp, userMessage "steer") :> SessionEvent
            | "contextPruned" ->
                fun () -> ContextPrunedEvent(sessionId, turnId, noSequence, stamp, 2, 10L, 4L) :> SessionEvent
            | _ -> failwith (sprintf "unmapped discriminator '%s' in the pin table" discriminator)

        let cells = foldEvents [ buildEvent () ]

        cells.Count |> should equal expectedCount

        if expectedCount > 0 then
            cells[0].Kind |> should equal expectedKind

[<Fact>]
let ``A prune event derives one System cell carrying the audit metadata`` () =
    let event =
        ContextPrunedEvent(sessionId, turnId, noSequence, stamp, 2, 10L, 4L) :> SessionEvent

    let cells = foldEvents [ event ]

    cells.Count |> should equal 1
    cells[0].Kind |> should equal SessionCellKind.System
    cells[0].IsError |> should equal false
    cells[0].Timestamp |> should equal stamp

    match cells[0].Metadata with
    | null -> failwith "prune cell lost its audit metadata"
    | meta ->
        meta["event"] |> should equal "contextPruned"
        meta["prunedCount"] |> should equal "2"
        meta["beforeEstimate"] |> should equal "10"
        meta["afterEstimate"] |> should equal "4"

    match cells[0].Content with
    | null -> failwith "prune cell lost its summary content"
    | content -> content.Contains("2") |> should equal true

[<Fact>]
let ``A compaction failure derives one System error cell carrying the reason`` () =
    let event =
        CompactionFailedEvent(sessionId, turnId, noSequence, stamp, "model denied") :> SessionEvent

    let cells = foldEvents [ event ]

    cells.Count |> should equal 1
    cells[0].Kind |> should equal SessionCellKind.System
    cells[0].IsError |> should equal true
    cells[0].Timestamp |> should equal stamp
    cells[0].Content |> should equal "model denied"

    match cells[0].Metadata with
    | null -> failwith "compaction failure cell lost its metadata"
    | meta -> meta["event"] |> should equal "compactionFailed"
