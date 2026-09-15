// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ContextPruningTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Xunit

let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
let nullString = Unchecked.defaultof<string>

let nullEntry = Unchecked.defaultof<ModelCatalogEntry>
let nullCells = Unchecked.defaultof<IReadOnlyList<SessionCell>>
let nullOptions = Unchecked.defaultof<ContextPruningOptions>

let private nullMetadata = Unchecked.defaultof<IReadOnlyDictionary<string, string>>
let private nullArtifacts = Unchecked.defaultof<IReadOnlyList<string>>

// One transcript cell of the given kind. Content, tool name, and call id
// take nullString where the kind carries none, following the CellsTests
// precedent for nullable fields.
let cell kind content toolName toolCallId =
    {
        Id = CellId.New()
        SessionId = sessionId
        TurnId = turnId
        Kind = kind
        Content = content
        ToolName = toolName
        ToolCallId = toolCallId
        IsError = false
        Iteration = 1
        Metadata = nullMetadata
        Artifacts = nullArtifacts
        Timestamp = stamp
    }

let textCell text =
    cell SessionCellKind.Assistant text nullString nullString

let toolPair callId name result =
    [
        cell SessionCellKind.ToolCall "" name callId
        cell SessionCellKind.ToolResult result name callId
    ]

// Two assistant turns with one tool pair each: user, assistant, call,
// result, assistant, call, result. The first result is old, the second is
// in the last assistant turn.
let twoTurnTranscript resultLength =
    let result = String('x', resultLength)

    ([
        cell SessionCellKind.User "do the thing" nullString nullString
        textCell "on it"
     ]
     @ toolPair "call-1" "grep" result
     @ [ textCell "more" ]
     @ toolPair "call-2" "read" result)
    |> ResizeArray<SessionCell>
    :> IReadOnlyList<SessionCell>

let optionsWithKeep keep =
    let options = ContextPruningOptions()
    options.KeepLastAssistantTurns <- keep
    options

// ──────────────────────────────────────────────────────────────────────────
// Estimator

[<Fact>]
let ``Text cell estimates ceiling chars over 4 plus overhead`` () =
    ContextPruning.EstimateCell(textCell "") |> should equal 4L
    ContextPruning.EstimateCell(textCell "abcd") |> should equal 5L
    ContextPruning.EstimateCell(textCell "abcde") |> should equal 6L

    ContextPruning.EstimateCell(cell SessionCellKind.Assistant nullString nullString nullString)
    |> should equal 4L

[<Fact>]
let ``ToolCall cell estimates from tool name plus content`` () =
    // read_file is 9 chars: 3 name tokens, no content, 4 overhead.
    ContextPruning.EstimateCell(cell SessionCellKind.ToolCall "" "read_file" "call-1")
    |> should equal 7L

[<Fact>]
let ``ToolResult cell estimates from result content`` () =
    ContextPruning.EstimateCell(cell SessionCellKind.ToolResult "12345678" "grep" "call-1")
    |> should equal (4L + 2L + 1L)

[<Fact>]
let ``Estimate sums cells and is zero for an empty transcript`` () =
    let cells = [| textCell "abcd"; textCell "abcde" |] :> IReadOnlyList<SessionCell>

    ContextPruning.Estimate cells |> should equal 11L

    ContextPruning.Estimate(ResizeArray<SessionCell>() :> IReadOnlyList<SessionCell>)
    |> should equal 0L

[<Fact>]
let ``Estimate grows monotonically with content length`` () =
    // Property pin for the heuristic contract: a longer content never
    // estimates smaller, so the threshold comparison cannot flip as
    // transcripts grow.
    let mutable previous = 0L

    for length in 0..200 do
        let estimate = ContextPruning.EstimateCell(textCell (String('x', length)))
        (estimate >= previous) |> should equal true
        previous <- estimate

[<Fact>]
let ``Estimate never shrinks when cells are appended`` () =
    let first = [| textCell "abcd" |] :> IReadOnlyList<SessionCell>

    let both = [| textCell "abcd"; textCell "abcde" |] :> IReadOnlyList<SessionCell>

    (ContextPruning.Estimate both >= ContextPruning.Estimate first)
    |> should equal true

[<Fact>]
let ``Estimate rejects a null cell list`` () =
    (fun () -> ContextPruning.Estimate nullCells |> ignore)
    |> should throw typeof<ArgumentNullException>

// ──────────────────────────────────────────────────────────────────────────
// Threshold

[<Fact>]
let ``Threshold derives from the catalog entry minus reserves`` () =
    let catalog = DefaultModelCatalog()
    let entry = catalog.GetEntry(ModelReference.Parse "anthropic/claude-sonnet")

    // claude-sonnet: 200,000 context with a 20,000 reserve.
    ContextPruning.PruneThreshold(entry, 10_000) |> should equal 170_000

[<Fact>]
let ``Unknown model falls back to the 128k catalog defaults`` () =
    let catalog = DefaultModelCatalog()
    let entry = catalog.GetEntry(ModelReference.Parse "acme/unknown-model")

    (entry |> box |> isNull) |> should equal true
    ContextPruning.PruneThreshold(entry, 10_000) |> should equal 98_000
    ContextPruning.PruneThreshold(nullEntry, 10_000) |> should equal 98_000

[<Fact>]
let ``Threshold clamps at zero instead of going negative`` () =
    ContextPruning.PruneThreshold(nullEntry, 200_000) |> should equal 0

[<Fact>]
let ``Threshold rejects a negative buffer`` () =
    (fun () -> ContextPruning.PruneThreshold(nullEntry, -1) |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

// ──────────────────────────────────────────────────────────────────────────
// Prune

[<Fact>]
let ``Prune replaces the old tool result with the marker`` () =
    let cells = twoTurnTranscript 400
    let before = ContextPruning.Estimate cells

    let result = ContextPruning.Prune(cells, optionsWithKeep 1)

    result.PrunedCount |> should equal 1
    result.BeforeEstimate |> should equal before
    (result.AfterEstimate < before) |> should equal true

    // Index 3 is the old result (before the last assistant turn at
    // index 4); index 6 is the recent result and keeps its content.
    result.PrunedCells[3].Content
    |> should equal ContextPruningOptions.DefaultPrunedMarker

    result.PrunedCells[6].Content |> should equal (String('x', 400))

    // Non-tool cells are never touched: same kind and content throughout.
    for index in [ 0; 1; 2; 4; 5 ] do
        result.PrunedCells[index].Kind |> should equal cells[index].Kind
        result.PrunedCells[index].Content |> should equal cells[index].Content

[<Fact>]
let ``Prune never touches the last N assistant turns`` () =
    let cells = twoTurnTranscript 400
    let result = ContextPruning.Prune(cells, optionsWithKeep 2)

    // Both assistant turns are protected: nothing is pruned.
    result.PrunedCount |> should equal 0

    for index in 0 .. cells.Count - 1 do
        result.PrunedCells[index].Content |> should equal cells[index].Content

[<Fact>]
let ``Prune with keep zero replaces every tool result`` () =
    let cells = twoTurnTranscript 400
    let result = ContextPruning.Prune(cells, optionsWithKeep 0)

    result.PrunedCount |> should equal 2

    result.PrunedCells[3].Content
    |> should equal ContextPruningOptions.DefaultPrunedMarker

    result.PrunedCells[6].Content
    |> should equal ContextPruningOptions.DefaultPrunedMarker

[<Fact>]
let ``Prune with no assistant cells prunes every tool result`` () =
    let cells =
        toolPair "call-1" "grep" (String('x', 100)) |> ResizeArray<SessionCell> :> IReadOnlyList<SessionCell>

    let result = ContextPruning.Prune(cells, optionsWithKeep 2)

    // No assistant turn means no recent turn to protect.
    result.PrunedCount |> should equal 1

    result.PrunedCells[1].Content
    |> should equal ContextPruningOptions.DefaultPrunedMarker

[<Fact>]
let ``Prune of an empty transcript is empty`` () =
    let cells = ResizeArray<SessionCell>() :> IReadOnlyList<SessionCell>
    let result = ContextPruning.Prune(cells, optionsWithKeep 2)

    result.PrunedCount |> should equal 0
    result.BeforeEstimate |> should equal 0L
    result.AfterEstimate |> should equal 0L
    result.PrunedCells.Count |> should equal 0

[<Fact>]
let ``Prune is idempotent on cells already carrying the marker`` () =
    let cells = twoTurnTranscript 400
    let first = ContextPruning.Prune(cells, optionsWithKeep 1)
    let second = ContextPruning.Prune(first.PrunedCells, optionsWithKeep 1)

    // The marker audit pin: the second pass finds nothing new, and the
    // estimates stop moving.
    second.PrunedCount |> should equal 0
    second.BeforeEstimate |> should equal first.AfterEstimate
    second.AfterEstimate |> should equal first.AfterEstimate

[<Fact>]
let ``Prune audit estimates match recomputation`` () =
    let cells = twoTurnTranscript 400
    let result = ContextPruning.Prune(cells, optionsWithKeep 1)

    result.BeforeEstimate |> should equal (ContextPruning.Estimate cells)

    result.AfterEstimate
    |> should equal (ContextPruning.Estimate result.PrunedCells)

[<Fact>]
let ``Prune rejects nulls and invalid options`` () =
    let cells = twoTurnTranscript 10

    (fun () -> ContextPruning.Prune(nullCells, optionsWithKeep 1) |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> ContextPruning.Prune(cells, nullOptions) |> ignore)
    |> should throw typeof<ArgumentNullException>

    let badBuffer = ContextPruningOptions()
    badBuffer.ReservedBufferTokens <- -1

    (fun () -> ContextPruning.Prune(cells, badBuffer) |> ignore)
    |> should throw typeof<ArgumentException>
