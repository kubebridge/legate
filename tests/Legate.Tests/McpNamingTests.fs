// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpNamingTests

open System
open FsUnit.Xunit
open Legate
open Legate.Mcp
open Xunit

[<Fact>]
let ``Sanitize keeps the name rule characters`` () =
    McpNaming.sanitize "alpha_read-file_9" |> should equal "alpha_read-file_9"

[<Fact>]
let ``Sanitize maps anything outside the rule to underscores`` () =
    McpNaming.sanitize "a b.c/d:e" |> should equal "a_b_c_d_e"
    McpNaming.sanitize "caf\u00E9" |> should equal "caf_"

[<Fact>]
let ``Sanitize never yields an empty name`` () =
    McpNaming.sanitize "" |> should equal "_"
    McpNaming.sanitize Unchecked.defaultof<string> |> should equal "_"

[<Fact>]
let ``Sanitize truncates to 128 characters`` () =
    McpNaming.sanitize (String.replicate 200 "a")
    |> should equal (String.replicate 128 "a")

    (McpNaming.sanitize (String.replicate 200 "a")).Length |> should equal 128

[<Fact>]
let ``Server tool names join server and tool with an underscore`` () =
    McpNaming.buildServerToolName "alpha" "read_file"
    |> should equal "alpha_read_file"

[<Fact>]
let ``Server tool names sanitize both halves`` () =
    McpNaming.buildServerToolName "my server" "read.file"
    |> should equal "my_server_read_file"

[<Fact>]
let ``Built names always satisfy the shared name rule`` () =
    for server, tool in
        [
            "alpha", "read"
            "a b", "c/d"
            String.replicate 100 "s", String.replicate 100 "t"
        ] do
        McpNaming.buildServerToolName server tool
        |> ToolNameRules.TryValidate
        |> should equal true

[<Fact>]
let ``Fail keeps distinct names in order`` () =
    McpNaming.resolve McpNameCollisionPolicy.Fail [ "a"; "b"; "c" ]
    |> should equal (Ok [ "a"; "b"; "c" ]: Result<string list, string>)

[<Fact>]
let ``Fail reports the first duplicate`` () =
    match McpNaming.resolve McpNameCollisionPolicy.Fail [ "a"; "b"; "a" ] with
    | Ok _ -> Assert.Fail("Fail must report duplicates.") |> ignore
    | Error message -> message.Contains("'a'") |> should equal true

[<Fact>]
let ``Suffix renames deterministically with _2 and _3`` () =
    McpNaming.resolve McpNameCollisionPolicy.Suffix [ "x"; "x"; "x"; "y" ]
    |> should equal (Ok [ "x"; "x_2"; "x_3"; "y" ]: Result<string list, string>)

[<Fact>]
let ``Suffix keeps an existing suffixed name and skips past it`` () =
    McpNaming.resolve McpNameCollisionPolicy.Suffix [ "x_2"; "x"; "x" ]
    |> should equal (Ok [ "x_2"; "x"; "x_3" ]: Result<string list, string>)

[<Fact>]
let ``Suffix truncates back to 128 and re-validates`` () =
    let long = String.replicate 128 "a"

    match McpNaming.resolve McpNameCollisionPolicy.Suffix [ long; long ] with
    | Error message -> Assert.Fail($"Suffix must rename, not fail: {message}") |> ignore
    | Ok assigned ->
        assigned.Length |> should equal 2
        assigned[0] |> should equal long
        assigned[1].Length |> should equal 128
        assigned[1] |> should equal (String.replicate 126 "a" + "_2")
        assigned |> List.forall ToolNameRules.TryValidate |> should equal true

[<Fact>]
let ``Suffix fails when the suffix space is exhausted`` () =
    let taken =
        [
            for suffix in 2..9999 -> $"x_{suffix}"
        ]

    let names = "x" :: taken @ [ "x" ]

    match McpNaming.resolve McpNameCollisionPolicy.Suffix names with
    | Ok _ -> Assert.Fail("Exhausted suffix space must fail.") |> ignore
    | Error message -> message.Contains("suffix space exhausted") |> should equal true

[<Fact>]
let ``Unknown policy fails closed`` () =
    match McpNaming.resolve (enum<McpNameCollisionPolicy> 99) [ "a" ] with
    | Ok _ -> Assert.Fail("Unknown policies must fail.") |> ignore
    | Error _ -> ()
