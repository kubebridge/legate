// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.PermissionTests

open System
open System.Text.Json
open FsUnit.Xunit
open Legate
open Xunit

let jsonOptions = JsonSerializerOptions()

// The generic Deserialize<'T> overload is annotated to return 'T | null,
// which trips FS3261 for value-bearing records; the Type-based overload
// avoids it. Same helper as SessionTests.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

// Unchecked.defaultof<string> rather than a bare null literal: under
// Nullable=enable the literal trips F# nullness checking on string-typed
// record fields. Deliberate: this is the null string value for the
// optional request fields.
let nullString = Unchecked.defaultof<string>

let sampleRequest () =
    {
        SessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        TurnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        ToolName = "lookup_order"
        ToolSourceId = "mcp-warehouse"
        ArgumentPreview = "{\"orderId\":12345}"
        RequestId = "req-1"
    }

/// A permission policy that records the last request it saw and returns
/// the verdict it was configured with.
type FakePermissionPolicy(verdict: PermissionVerdict) =
    let mutable lastRequest: PermissionRequest option = None

    interface IPermissionPolicy with
        member _.Evaluate(request: PermissionRequest) =
            lastRequest <- Some request
            verdict

    /// The most recent request Evaluate received, if any.
    member _.LastRequest = lastRequest

// ───────────────────────────────────────────────────────────────────────────
// Verdict discriminators

[<Fact>]
let ``Allow round-trips to AllowVerdict via $type`` () =
    let json = JsonSerializer.Serialize(PermissionVerdict.Allow, jsonOptions)

    json.Contains("\"$type\":\"allow\"") |> should equal true

    match JsonSerializer.Deserialize<PermissionVerdict>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored -> (restored :? AllowVerdict) |> should equal true

[<Fact>]
let ``Deny round-trips to DenyVerdict carrying its reason via $type`` () =
    let json =
        JsonSerializer.Serialize(PermissionVerdict.Deny("tool is not on the allow-list"), jsonOptions)

    json.Contains("\"$type\":\"deny\"") |> should equal true

    match JsonSerializer.Deserialize<PermissionVerdict>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? DenyVerdict) |> should equal true

        (restored :?> DenyVerdict).Reason
        |> should equal "tool is not on the allow-list"

[<Fact>]
let ``Ask round-trips to AskVerdict via $type`` () =
    let json = JsonSerializer.Serialize(PermissionVerdict.Ask, jsonOptions)

    json.Contains("\"$type\":\"ask\"") |> should equal true

    match JsonSerializer.Deserialize<PermissionVerdict>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored -> (restored :? AskVerdict) |> should equal true

[<Fact>]
let ``Deny carries the reason it was built with`` () =
    (DenyVerdict("read-only workspace") :> PermissionVerdict) :?> DenyVerdict
    |> fun deny -> deny.Reason |> should equal "read-only workspace"

// ───────────────────────────────────────────────────────────────────────────
// PermissionRequest JSON

[<Fact>]
let ``PermissionRequest JSON round-trip preserves every field`` () =
    let request = sampleRequest ()

    let json = JsonSerializer.Serialize(request, jsonOptions)
    let restored = deserialize<PermissionRequest> json

    restored.SessionId |> should equal request.SessionId
    restored.TurnId |> should equal request.TurnId
    restored.ToolName |> should equal "lookup_order"
    restored.ToolSourceId |> should equal "mcp-warehouse"
    restored.ArgumentPreview |> should equal "{\"orderId\":12345}"
    restored.RequestId |> should equal "req-1"

[<Fact>]
let ``PermissionRequest JSON serialises identifiers as plain strings`` () =
    let document =
        JsonSerializer.Serialize(sampleRequest (), jsonOptions) |> JsonDocument.Parse

    document.RootElement.GetProperty("SessionId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    document.RootElement.GetProperty("TurnId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

[<Fact>]
let ``PermissionRequest JSON round-trips null source and preview`` () =
    let request =
        { sampleRequest () with
            ToolSourceId = nullString
            ArgumentPreview = nullString
        }

    let json = JsonSerializer.Serialize(request, jsonOptions)
    let restored = deserialize<PermissionRequest> json

    restored.ToolSourceId |> should equal null
    restored.ArgumentPreview |> should equal null

// ───────────────────────────────────────────────────────────────────────────
// IPermissionPolicy evaluation

[<Fact>]
let ``A policy's Evaluate returns the verdict it was configured with`` () =
    let request = sampleRequest ()
    let fake = FakePermissionPolicy(PermissionVerdict.Ask)

    (fake :> IPermissionPolicy).Evaluate(request) :? AskVerdict |> should equal true

    fake.LastRequest |> should equal (Some request)

[<Fact>]
let ``A denying policy surfaces its reason through Evaluate`` () =
    let policy =
        FakePermissionPolicy(PermissionVerdict.Deny("not allowed")) :> IPermissionPolicy

    let verdict = policy.Evaluate(sampleRequest ())
    (verdict :?> DenyVerdict).Reason |> should equal "not allowed"
