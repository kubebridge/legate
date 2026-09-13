// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TurnTests

open System
open System.Text.Json
open FsUnit.Xunit
open Legate
open Xunit

// The generic Deserialize<'T> overload is annotated to return 'T | null,
// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

// Unchecked.defaultof<string> rather than a bare null literal: under
// Nullable=enable the literal trips F# nullness checking on string-typed
// parameters. Deliberate: this is the null string value for rejection cases.
let nullString = Unchecked.defaultof<string>

// Empty Nullable<'T> spellings; Nullable() inline reads ambiguously.
let noStamp = Unchecked.defaultof<Nullable<DateTimeOffset>>

// The null TurnOutcome value; Unchecked.defaultof<string> only spells the
// null string, so an object-typed null needs its own spelling.
let noOutcome = Unchecked.defaultof<TurnOutcome>

let jsonOptions = JsonSerializerOptions()

/// Builds a settled turn with every field set, the common shape for
/// construction and serialisation-shape tests.
let sampleTurn () =
    {
        Id = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        SessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        Attempt = 2
        Status = TurnStatus.Completed
        StartedAt = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
        CompletedAt = Nullable(DateTimeOffset(2024, 1, 2, 3, 5, 6, TimeSpan.Zero))
        Iterations = 7
        Usage =
            {
                InputTokens = 1200L
                OutputTokens = 340L
            }
        Error = nullString
    }

/// Builds a turn still in flight: no completion stamp, no error.
let runningTurn () =
    { sampleTurn () with
        Status = TurnStatus.Running
        CompletedAt = noStamp
    }

/// Builds the flat result payload a host receives when it waits on a turn.
let sampleResult () =
    {
        AssistantText = "done"
        Status = TurnStatus.Completed
        Iterations = 7
        Usage =
            {
                InputTokens = 1200L
                OutputTokens = 340L
            }
        Outcome = TurnFinished("renamed the widget") :> TurnOutcome
    }

// ───────────────────────────────────────────────────────────────────────────
// TurnStatus

[<Fact>]
let ``TurnStatus has exactly the six documented members`` () =
    Enum.GetNames<TurnStatus>()
    |> should
        equal
        [|
            "Pending"
            "Running"
            "Suspended"
            "Completed"
            "Aborted"
            "Failed"
        |]

    int TurnStatus.Pending |> should equal 0
    int TurnStatus.Running |> should equal 1
    int TurnStatus.Suspended |> should equal 2
    int TurnStatus.Completed |> should equal 3
    int TurnStatus.Aborted |> should equal 4
    int TurnStatus.Failed |> should equal 5

// ───────────────────────────────────────────────────────────────────────────
// UsageSummary

[<Fact>]
let ``UsageSummary round-trips both token counts`` () =
    let usage = { InputTokens = 42L; OutputTokens = 7L }

    let json = JsonSerializer.Serialize usage
    let restored = deserialize<UsageSummary> json

    restored.InputTokens |> should equal 42L
    restored.OutputTokens |> should equal 7L

[<Fact>]
let ``UsageSummary has exactly two properties pinning the no-cost rule`` () =
    let properties = typeof<UsageSummary>.GetProperties() |> Array.map (fun p -> p.Name)
    properties |> Array.sort |> should equal [| "InputTokens"; "OutputTokens" |]

// ───────────────────────────────────────────────────────────────────────────
// Turn construction and JSON

[<Fact>]
let ``Turn record constructs with F# syntax and compares structurally`` () =
    let turn = sampleTurn ()

    let changed = { turn with Status = TurnStatus.Failed }

    changed |> should not' (equal turn)

    let copy =
        { turn with
            Status = TurnStatus.Completed
        }

    copy |> should equal turn

[<Fact>]
let ``Turn is CLIMutable: parameterless constructor and settable properties`` () =
    // The C#-friendly construction contract: [<CLIMutable>] generates the
    // default constructor and property setters C# object initialisers and
    // System.Text.Json bind through. F# syntax cannot reach the generated
    // setters (the immutable record field shadows them), so this pins the
    // contract by reflection.
    let turnType = typeof<Turn>

    turnType.GetConstructor([||]) |> should not' (equal null)

    for property in turnType.GetProperties() do
        property.CanWrite |> should equal true

[<Fact>]
let ``Turn CLIMutable record holds the sample values F# construction set`` () =
    let turn = sampleTurn ()

    turn.Attempt |> should equal 2
    turn.Status |> should equal TurnStatus.Completed
    turn.CompletedAt.HasValue |> should equal true

    turn.CompletedAt.Value
    |> should equal (DateTimeOffset(2024, 1, 2, 3, 5, 6, TimeSpan.Zero))

    turn.Iterations |> should equal 7
    turn.Usage.OutputTokens |> should equal 340L
    turn.Error |> should equal null

[<Fact>]
let ``Turn JSON round-trip preserves every field with default options`` () =
    let turn = sampleTurn ()

    let json = JsonSerializer.Serialize turn
    let restored = deserialize<Turn> json

    restored.Id |> should equal turn.Id
    restored.SessionId |> should equal turn.SessionId
    restored.Attempt |> should equal turn.Attempt
    restored.Status |> should equal turn.Status
    restored.StartedAt |> should equal turn.StartedAt
    restored.CompletedAt.Value |> should equal turn.CompletedAt.Value
    restored.Iterations |> should equal turn.Iterations
    restored.Usage.InputTokens |> should equal 1200L
    restored.Usage.OutputTokens |> should equal 340L
    restored.Error |> should equal null

[<Fact>]
let ``Turn JSON round-trips an in-flight turn with every null in place`` () =
    let turn = runningTurn ()

    let json = JsonSerializer.Serialize turn
    let restored = deserialize<Turn> json

    restored.Status |> should equal TurnStatus.Running
    restored.CompletedAt.HasValue |> should equal false
    restored.Error |> should equal null

[<Fact>]
let ``Turn JSON round-trips a failed turn with an error message`` () =
    let failed =
        { sampleTurn () with
            Status = TurnStatus.Failed
            Error = "provider returned 500"
        }

    let json = JsonSerializer.Serialize failed
    let restored = deserialize<Turn> json

    restored.Status |> should equal TurnStatus.Failed
    restored.Error |> should equal "provider returned 500"

[<Fact>]
let ``Turn JSON serialises identifiers as plain strings`` () =
    let document = JsonSerializer.Serialize(sampleTurn ()) |> JsonDocument.Parse

    document.RootElement.GetProperty("Id").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    document.RootElement.GetProperty("SessionId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

[<Fact>]
let ``Turn JSON deserialises a payload without the nullable fields`` () =
    let json =
        """{"Id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","SessionId":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Attempt":1,"Status":1,"StartedAt":"2024-01-02T03:04:05+00:00","Iterations":3}"""

    let turn = deserialize<Turn> json

    turn.Status |> should equal TurnStatus.Running
    turn.CompletedAt.HasValue |> should equal false
    turn.Error |> should equal null
    // Same shape SessionOptions deserialises to when absent: a property
    // missing from the payload leaves the reference-typed record field
    // null (System.Text.Json never calls the CLIMutable constructor).
    turn.Usage |> should equal null

// ───────────────────────────────────────────────────────────────────────────
// TurnOutcome polymorphism

[<Fact>]
let ``TurnFinished round-trips to the correct subtype via $type`` () =
    let outcome = TurnFinished("renamed the widget") :> TurnOutcome

    let json = JsonSerializer.Serialize(outcome, jsonOptions)
    json.Contains("\"$type\":\"turnFinished\"") |> should equal true

    match JsonSerializer.Deserialize<TurnOutcome>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? TurnFinished) |> should equal true
        (restored :?> TurnFinished).Summary |> should equal "renamed the widget"

[<Fact>]
let ``TurnPartiallyFinished round-trips to the correct subtype via $type`` () =
    let outcome = TurnPartiallyFinished("12 of 20 items") :> TurnOutcome

    let json = JsonSerializer.Serialize(outcome, jsonOptions)
    json.Contains("\"$type\":\"turnPartiallyFinished\"") |> should equal true

    match JsonSerializer.Deserialize<TurnOutcome>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? TurnPartiallyFinished) |> should equal true
        (restored :?> TurnPartiallyFinished).Summary |> should equal "12 of 20 items"

[<Fact>]
let ``TurnFailed round-trips to the correct subtype via $type`` () =
    let outcome = TurnFailed("tool call timed out") :> TurnOutcome

    let json = JsonSerializer.Serialize(outcome, jsonOptions)
    json.Contains("\"$type\":\"turnFailed\"") |> should equal true

    match JsonSerializer.Deserialize<TurnOutcome>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? TurnFailed) |> should equal true
        (restored :?> TurnFailed).Reason |> should equal "tool call timed out"

// ───────────────────────────────────────────────────────────────────────────
// TurnResult

[<Fact>]
let ``TurnResult record constructs with F# syntax and compares structurally`` () =
    let result = sampleResult ()

    let changed =
        { result with
            Status = TurnStatus.Failed
        }

    changed |> should not' (equal result)

    let copy =
        { result with
            Status = TurnStatus.Completed
        }

    copy |> should equal result

[<Fact>]
let ``TurnResult is CLIMutable: parameterless constructor and settable properties`` () =
    // Same reflection pin as Turn: the setters C# and System.Text.Json bind
    // through exist even though F# syntax cannot reach them.
    let resultType = typeof<TurnResult>

    resultType.GetConstructor([||]) |> should not' (equal null)

    for property in resultType.GetProperties() do
        property.CanWrite |> should equal true

[<Fact>]
let ``TurnResult JSON round-trip preserves every field with default options`` () =
    let result = sampleResult ()

    let json = JsonSerializer.Serialize result
    let restored = deserialize<TurnResult> json

    restored.AssistantText |> should equal "done"
    restored.Status |> should equal TurnStatus.Completed
    restored.Iterations |> should equal 7
    restored.Usage.InputTokens |> should equal 1200L
    restored.Usage.OutputTokens |> should equal 340L

    match restored.Outcome with
    | null -> failwith "outcome was lost in the round-trip"
    | outcome ->
        (outcome :? TurnFinished) |> should equal true
        (outcome :?> TurnFinished).Summary |> should equal "renamed the widget"

[<Fact>]
let ``TurnResult JSON round-trips with a null outcome for interactive sessions`` () =
    let result =
        { sampleResult () with
            Outcome = noOutcome
        }

    let json = JsonSerializer.Serialize result
    let restored = deserialize<TurnResult> json

    restored.Outcome |> should equal null

[<Fact>]
let ``TurnResult AssistantText may be empty without ever being null`` () =
    let result =
        { sampleResult () with
            AssistantText = ""
        }

    let json = JsonSerializer.Serialize result
    let restored = deserialize<TurnResult> json

    restored.AssistantText |> should equal ""

[<Fact>]
let ``TurnResult carries the turn's terminal status and usage as-is`` () =
    let failed =
        { sampleResult () with
            Status = TurnStatus.Aborted
            Outcome = noOutcome
        }

    let json = JsonSerializer.Serialize failed
    let restored = deserialize<TurnResult> json

    restored.Status |> should equal TurnStatus.Aborted
    restored.Outcome |> should equal null
