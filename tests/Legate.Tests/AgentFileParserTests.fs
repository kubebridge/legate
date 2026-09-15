// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.AgentFileParserTests

open System
open System.IO
open FsUnit.Xunit
open Legate.Agents
open Xunit

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// Creates an empty scratch directory for one test.
let private freshDir () : string =
    let dir =
        Path.Combine(Path.GetTempPath(), "legate-agent-parser-tests", Guid.NewGuid().ToString("N"))

    Directory.CreateDirectory(dir) |> ignore
    dir

/// Writes one agent file and returns its path.
let private writeFile (dir: string) (fileName: string) (content: string) : string =
    let path = Path.Combine(dir, fileName)
    File.WriteAllText(path, content)
    path

/// Parses content written to a scratch `agent.md` file.
let private parseContent (content: string) : AgentFileParser.AgentFileDefinition =
    let dir = freshDir ()

    try
        AgentFileParser.parseFile (writeFile dir "agent.md" content)
    finally
        Directory.Delete(dir, true)

// ──────────────────────────────────────────────────────────────────────────
// Schema

[<Fact>]
let ``Full frontmatter parses with the body as the system prompt`` () =
    let definition =
        parseContent
            "---\nname: helper\ndescription: Helps out\nmodel: anthropic/claude-sonnet\nenabled: false\n---\nYou help out.\nSecond line.\n"

    definition.Name |> should equal "helper"
    definition.Description |> should equal "Helps out"
    definition.Model.Value |> should equal "anthropic/claude-sonnet"
    definition.Enabled |> should equal false
    definition.SystemPrompt |> should equal "You help out.\nSecond line.\n"

[<Fact>]
let ``Missing keys fall back to the documented defaults`` () =
    let definition = parseContent "---\nname: helper\n---\nPrompt.\n"

    definition.Description |> should equal null
    definition.Model |> should equal AgentFileParser.defaultModel
    definition.Model.Value |> should equal "legate/default"
    definition.Enabled |> should equal true

[<Fact>]
let ``Multi-line and quoted values parse`` () =
    let definition =
        parseContent
            "---\nname: multi\ndescription: |\n  line one\n  line two\nmodel: \"anthropic/claude-sonnet\"\n---\nPrompt.\n"

    definition.Description |> should equal "line one\nline two\n"
    definition.Model.Value |> should equal "anthropic/claude-sonnet"

[<Fact>]
let ``Unknown keys are ignored`` () =
    let definition =
        parseContent "---\nname: helper\ntools: [read, write]\nallowed-tools: read\n---\nPrompt.\n"

    definition.Name |> should equal "helper"
    definition.SystemPrompt |> should equal "Prompt.\n"

[<Fact>]
let ``String enabled parses case-insensitively`` () =
    let definition =
        parseContent "---\nname: helper\nenabled: \"False\"\n---\nPrompt.\n"

    definition.Enabled |> should equal false

// ──────────────────────────────────────────────────────────────────────────
// Fail-fast

[<Fact>]
let ``Invalid YAML fails naming the file`` () =
    let dir = freshDir ()

    try
        let path = writeFile dir "broken.md" "---\nname: [unclosed\n---\nBody\n"

        let ex =
            Assert.Throws<InvalidOperationException>(fun () -> AgentFileParser.parseFile path |> ignore)

        ex.Message.Contains("broken.md") |> should equal true
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Missing name fails naming the file`` () =
    let dir = freshDir ()

    try
        let path = writeFile dir "nameless.md" "---\ndescription: No name\n---\nBody\n"

        let ex =
            Assert.Throws<InvalidOperationException>(fun () -> AgentFileParser.parseFile path |> ignore)

        ex.Message.Contains("nameless.md") |> should equal true
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Missing frontmatter fails naming the file`` () =
    let dir = freshDir ()

    try
        let path = writeFile dir "bare.md" "Just a body, no frontmatter.\n"

        let ex =
            Assert.Throws<InvalidOperationException>(fun () -> AgentFileParser.parseFile path |> ignore)

        ex.Message.Contains("bare.md") |> should equal true
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Missing closing marker fails naming the file`` () =
    let dir = freshDir ()

    try
        let path =
            writeFile dir "open.md" "---\nname: helper\nBody without a closing marker.\n"

        let ex =
            Assert.Throws<InvalidOperationException>(fun () -> AgentFileParser.parseFile path |> ignore)

        ex.Message.Contains("open.md") |> should equal true
    finally
        Directory.Delete(dir, true)

[<Fact>]
let ``Invalid model fails naming the file`` () =
    let dir = freshDir ()

    try
        let path =
            writeFile dir "badmodel.md" "---\nname: helper\nmodel: \"not a model ref\"\n---\nBody\n"

        let ex =
            Assert.Throws<InvalidOperationException>(fun () -> AgentFileParser.parseFile path |> ignore)

        ex.Message.Contains("badmodel.md") |> should equal true
    finally
        Directory.Delete(dir, true)
