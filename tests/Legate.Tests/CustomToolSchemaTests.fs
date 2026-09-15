// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.CustomToolSchemaTests

open System.Text.Json
open FsUnit.Xunit
open Legate
open Xunit

// Tests for custom tool input-schema resolution (issue 74, task 2): the
// parse-once rules with the permissive fallback, the model-visible
// diagnostic suffix, and the host log reason carrying no schema text.

// ───────────────────────────────────────────────────────────────────────────
// Helpers

/// Reads the served schema's JSON text in canonical form.
let private schemaText (resolution: SchemaResolution) : string =
    resolution.Schema.RootElement.GetRawText()

// ───────────────────────────────────────────────────────────────────────────
// Resolution

[<Fact>]
let ``Null schema takes the permissive fallback silently`` () =
    let resolution = CustomToolSchema.resolve null

    schemaText resolution |> should equal CustomToolSchema.PermissiveJson
    resolution.Diagnostic |> should equal ""
    resolution.ParseReason |> should equal None

[<Fact>]
let ``Blank schema takes the permissive fallback silently`` () =
    for blank in [ ""; "   " ] do
        let resolution = CustomToolSchema.resolve blank
        schemaText resolution |> should equal CustomToolSchema.PermissiveJson
        resolution.Diagnostic |> should equal ""
        resolution.ParseReason |> should equal None

[<Fact>]
let ``Valid object schema serves verbatim with no diagnostic`` () =
    let json =
        """{"type":"object","properties":{"q":{"type":"string"}},"required":["q"],"additionalProperties":false}"""

    let resolution = CustomToolSchema.resolve json

    let served = JsonDocument.Parse(schemaText resolution)
    served.RootElement.GetProperty("type").GetString() |> should equal "object"
    served.RootElement.GetProperty("required").[0].GetString() |> should equal "q"
    resolution.Diagnostic |> should equal ""
    resolution.ParseReason |> should equal None

[<Fact>]
let ``Valid boolean schema serves verbatim with no diagnostic`` () =
    let resolution = CustomToolSchema.resolve "true"

    schemaText resolution |> should equal "true"
    resolution.Diagnostic |> should equal ""
    resolution.ParseReason |> should equal None

[<Fact>]
let ``Invalid JSON falls back with the diagnostic and a reason`` () =
    let resolution = CustomToolSchema.resolve """{"type":"object","properties":{"""

    schemaText resolution |> should equal CustomToolSchema.PermissiveJson

    resolution.Diagnostic.Contains(CustomToolSchema.FallbackDiagnostic)
    |> should equal true

    match resolution.ParseReason with
    | None -> failwith "Expected a parse reason for the host log."
    | Some _ -> ()

[<Fact>]
let ``Non-object JSON falls back with the diagnostic and a reason`` () =
    let resolution = CustomToolSchema.resolve """["not","a","schema"]"""

    schemaText resolution |> should equal CustomToolSchema.PermissiveJson

    resolution.Diagnostic.Contains(CustomToolSchema.FallbackDiagnostic)
    |> should equal true

    resolution.ParseReason.IsSome |> should equal true

[<Fact>]
let ``The parse reason never carries the schema text`` () =
    // The schema text itself may look sensitive; the host log carries the
    // parse reason only, so a marker embedded in the schema must not
    // travel into the reason.
    let marker = "schema-secret-marker-9f3c"
    let resolution = CustomToolSchema.resolve ("{invalid " + marker)

    match resolution.ParseReason with
    | None -> failwith "Expected a parse reason for the host log."
    | Some reason -> reason.Contains(marker) |> should equal false
