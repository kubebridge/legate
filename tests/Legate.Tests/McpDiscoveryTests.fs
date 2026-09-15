// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpDiscoveryTests

open System.Text.Json
open FsUnit.Xunit
open Legate.Mcp
open Xunit

[<Fact>]
let ``Default input schema is the minimal object schema`` () =
    McpDiscovery.DefaultInputSchema.GetRawText()
    |> should equal """{"type":"object"}"""

[<Fact>]
let ``Parse keeps a JSON object schema`` () =
    let parsed =
        McpDiscovery.parseInputSchema """{"type":"object","properties":{"q":{"type":"string"}}}"""

    parsed.GetProperty("type").GetString() |> should equal "object"

    parsed.GetProperty("properties").GetProperty("q").GetProperty("type").GetString()
    |> should equal "string"

[<Fact>]
let ``Parse falls back for missing and malformed schemas`` () =
    (McpDiscovery.parseInputSchema Unchecked.defaultof<string>).GetRawText()
    |> should equal """{"type":"object"}"""

    (McpDiscovery.parseInputSchema "").GetRawText()
    |> should equal """{"type":"object"}"""

    (McpDiscovery.parseInputSchema "   ").GetRawText()
    |> should equal """{"type":"object"}"""

    (McpDiscovery.parseInputSchema "[1,2]").GetRawText()
    |> should equal """{"type":"object"}"""

    (McpDiscovery.parseInputSchema "{oops").GetRawText()
    |> should equal """{"type":"object"}"""

[<Fact>]
let ``Discovered tools carry only SDK-free values`` () =
    let discovered: McpDiscovery.McpDiscoveredTool =
        {
            ServerName = "alpha"
            ToolName = "read"
            Title = null
            Description = "Reads."
            InputSchema = McpDiscovery.DefaultInputSchema
            OutputSchema = None
            DestructiveHint = false
            ReadOnlyHint = Some true
            IdempotentHint = None
            OpenWorldHint = None
            AnnotationTitle = null
        }

    discovered.ServerName |> should equal "alpha"
    discovered.DestructiveHint |> should equal false
    discovered.ReadOnlyHint |> should equal (Some true)
