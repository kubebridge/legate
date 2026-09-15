// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SearchToolsTests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Workspace.Process
open Microsoft.Extensions.AI
open Xunit

let private bindWorkspace () : IWorkspace =
    let root =
        Path.Combine(Path.GetTempPath(), "legate-search-tools-tests", Ulid.NewUlid().ToString())

    Directory.CreateDirectory root |> ignore

    let runtime =
        ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = root), null, null) :> IWorkspaceRuntime

    let session =
        {
            Id = SessionId.New()
            Tenant = TenantId.Create "search-tools-tests"
            AgentId = AgentId.New()
            Title = "search-tools"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    runtime.Bind(session, null, CancellationToken.None).GetAwaiter().GetResult()

let private invokeAsync (tool: AITool) (args: (string * (obj | null)) list) : Task<string> =
    task {
        let fn = tool :?> AIFunction
        let! result = fn.InvokeAsync(AIFunctionArguments(dict args), CancellationToken.None)

        // MEAI JSON-round-trips tool results: a string return arrives as a
        // JsonElement whose GetString is the original text (the same value
        // TurnLoop's converter passes to the model).
        match result with
        | :? System.Text.Json.JsonElement as element ->
            match Option.ofObj (element.GetString()) with
            | Some value -> return value
            | None -> return raise (InvalidOperationException("The tool returned no text result."))
        | _ -> return raise (InvalidOperationException("The tool returned an unexpected result shape."))
    }

let private writeText (workspace: IWorkspace) (path: string) (text: string) : Task =
    workspace.WriteFile(path, Encoding.UTF8.GetBytes text, CancellationToken.None)

/// A workspace stub whose root exposes no path, proving glob and grep fail
/// loudly on null-Path runtimes instead of falling back.
type private NullPathWorkspace() =
    interface IWorkspace with
        member _.Root = WorkspaceRoot("stub", null)

        member _.Exec(_, _, _, _) =
            Task.FromResult(WorkspaceExecResult(0, "", "", false))

        member _.Exists(_, _) = Task.FromResult(false)

        member _.ReadFile(_, _) =
            raise (FileNotFoundException("No file exists at this workspace path.", "stub"))

        member _.WriteFile(_, _, _) = Task.CompletedTask
        member _.DeleteFile(_, _) = Task.FromResult(false)
        member _.DisposeAsync() = ValueTask.CompletedTask

// ────────────────────────────────────────────────────────────────────
// glob

[<Fact>]
let ``glob matches file names at any depth and returns relative slash paths`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! writeText workspace "a/app.fs" "module A"
        do! writeText workspace "a/readme.md" "# docs"
        do! writeText workspace "nested/deep/util.fs" "module U"

        let tool = BuiltinSearchTools.CreateGlobTool(workspace)
        let! result = invokeAsync tool [ "pattern", box "*.fs" ]
        let text = result

        Assert.Contains("a/app.fs", text)
        Assert.Contains("nested/deep/util.fs", text)
        Assert.DoesNotContain("readme.md", text)
        Assert.DoesNotContain("\\", text)
    }

[<Fact>]
let ``glob with a slash matches the whole relative path`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! writeText workspace "a/app.fs" "module A"
        do! writeText workspace "nested/deep/util.fs" "module U"

        let tool = BuiltinSearchTools.CreateGlobTool(workspace)
        let! result = invokeAsync tool [ "pattern", box "a/*.fs" ]
        let text = result

        Assert.Contains("a/app.fs", text)
        Assert.DoesNotContain("nested/deep/util.fs", text)
    }

[<Fact>]
let ``glob caps at the default 100 with truncation and total`` () : Task =
    task {
        use workspace = bindWorkspace ()

        for i in 0..149 do
            do! writeText workspace (sprintf "bulk/file%03d.txt" i) "bulk"

        let tool = BuiltinSearchTools.CreateGlobTool(workspace)
        let! result = invokeAsync tool [ "pattern", box "*" ]
        let text = result

        Assert.Contains("truncated: true", text)
        Assert.Contains("total: 150", text)
        Assert.Contains("returned: 100", text)
        Assert.Equal(101, text.Split('\n').Length)
    }

[<Fact>]
let ``glob honours an explicit max_results`` () : Task =
    task {
        use workspace = bindWorkspace ()

        for i in 0..149 do
            do! writeText workspace (sprintf "bulk/file%03d.txt" i) "bulk"

        let tool = BuiltinSearchTools.CreateGlobTool(workspace)

        let! result =
            invokeAsync
                tool
                [
                    "pattern", box "*"
                    "max_results", box 10
                ]

        let text = result

        Assert.Contains("truncated: true", text)
        Assert.Contains("total: 150", text)
        Assert.Contains("returned: 10", text)
    }

[<Fact>]
let ``glob returns everything when the cap covers the total`` () : Task =
    task {
        use workspace = bindWorkspace ()

        for i in 0..9 do
            do! writeText workspace (sprintf "bulk/file%03d.txt" i) "bulk"

        let tool = BuiltinSearchTools.CreateGlobTool(workspace)

        let! result =
            invokeAsync
                tool
                [
                    "pattern", box "*"
                    "max_results", box 1000
                ]

        let text = result

        Assert.Contains("truncated: false", text)
        Assert.Contains("total: 10", text)
        Assert.Contains("returned: 10", text)
    }

[<Fact>]
let ``glob rejects max_results outside 1 to 1000`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! writeText workspace "a.txt" "a"

        let tool = BuiltinSearchTools.CreateGlobTool(workspace)

        let! low =
            Assert.ThrowsAsync<ToolException>(fun () ->
                invokeAsync
                    tool
                    [
                        "pattern", box "*"
                        "max_results", box 0
                    ]
                :> Task)

        Assert.Contains("max_results", low.Message)

        let! high =
            Assert.ThrowsAsync<ToolException>(fun () ->
                invokeAsync
                    tool
                    [
                        "pattern", box "*"
                        "max_results", box 1001
                    ]
                :> Task)

        Assert.Contains("max_results", high.Message)
    }

[<Fact>]
let ``glob respects gitignore star dir and negation rules when present`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! writeText workspace ".gitignore" "*.log\n# a comment\n\nbuild/\n!keep.log\n"
        do! writeText workspace "app.log" "log"
        do! writeText workspace "keep.log" "kept"
        do! writeText workspace "notes.txt" "notes"
        do! writeText workspace "build/out.txt" "out"

        let tool = BuiltinSearchTools.CreateGlobTool(workspace)
        let! result = invokeAsync tool [ "pattern", box "*" ]
        let text = result

        Assert.Contains("keep.log", text)
        Assert.Contains("notes.txt", text)
        Assert.DoesNotContain("app.log", text)
        Assert.DoesNotContain("build/out.txt", text)
        Assert.Contains("total: 3", text)
    }

[<Fact>]
let ``glob filters nothing when no gitignore file is present`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! writeText workspace "app.log" "log"
        do! writeText workspace "keep.log" "kept"
        do! writeText workspace "notes.txt" "notes"
        do! writeText workspace "build/out.txt" "out"

        let tool = BuiltinSearchTools.CreateGlobTool(workspace)
        let! result = invokeAsync tool [ "pattern", box "*" ]
        let text = result

        Assert.Contains("app.log", text)
        Assert.Contains("build/out.txt", text)
        Assert.Contains("total: 4", text)
    }

[<Fact>]
let ``glob respects gitignore question-mark and double-star rules`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! writeText workspace ".gitignore" "file?.txt\n**/skip.txt\n"
        do! writeText workspace "file1.txt" "one"
        do! writeText workspace "file10.txt" "ten"
        do! writeText workspace "deep/nested/skip.txt" "skip"
        do! writeText workspace "deep/keep.txt" "keep"

        let tool = BuiltinSearchTools.CreateGlobTool(workspace)
        let! result = invokeAsync tool [ "pattern", box "*" ]
        let text = result

        Assert.Contains("file10.txt", text)
        Assert.Contains("deep/keep.txt", text)
        Assert.DoesNotContain("file1.txt", text)
        Assert.DoesNotContain("skip.txt", text)
        Assert.Contains("total: 3", text)
    }

// ────────────────────────────────────────────────────────────────────
// grep

[<Fact>]
let ``grep returns path line text matches and honours the file filter`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! writeText workspace "a.txt" "hello world\nsecond line\n"
        do! writeText workspace "docs/b.md" "say hello here\n"
        do! writeText workspace "c.txt" "nothing\n"

        let tool = BuiltinSearchTools.CreateGrepTool(workspace)

        let! all = invokeAsync tool [ "pattern", box "hello" ]
        let allText = all
        Assert.Contains("a.txt:1: hello world", allText)
        Assert.Contains("docs/b.md:1: say hello here", allText)
        Assert.Contains("total: 2", allText)

        let! filtered =
            invokeAsync
                tool
                [
                    "pattern", box "hello"
                    "file_filter", box "*.md"
                ]

        let filteredText = filtered
        Assert.Contains("docs/b.md:1: say hello here", filteredText)
        Assert.DoesNotContain("a.txt", filteredText)
    }

[<Fact>]
let ``grep respects gitignore when present`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! writeText workspace ".gitignore" "*.log\n"
        do! writeText workspace "app.log" "hello log\n"
        do! writeText workspace "notes.txt" "hello notes\n"

        let tool = BuiltinSearchTools.CreateGrepTool(workspace)
        let! result = invokeAsync tool [ "pattern", box "hello" ]
        let text = result

        Assert.Contains("notes.txt:1: hello notes", text)
        Assert.DoesNotContain("app.log", text)
    }

[<Fact>]
let ``grep caps matches with truncation and total`` () : Task =
    task {
        use workspace = bindWorkspace ()

        let lines =
            [
                for i in 1..120 -> sprintf "hit %d" i
            ]
            |> String.concat "\n"

        do! writeText workspace "big.txt" lines

        let tool = BuiltinSearchTools.CreateGrepTool(workspace)

        let! result =
            invokeAsync
                tool
                [
                    "pattern", box "hit"
                    "max_results", box 50
                ]

        let text = result

        Assert.Contains("truncated: true", text)
        Assert.Contains("total: 120", text)
        Assert.Contains("returned: 50", text)
    }

[<Fact>]
let ``grep rejects an invalid regular expression`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! writeText workspace "a.txt" "a"

        let tool = BuiltinSearchTools.CreateGrepTool(workspace)

        let! error = Assert.ThrowsAsync<ToolException>(fun () -> invokeAsync tool [ "pattern", box "(" ] :> Task)

        Assert.Contains("regular expression", error.Message)
    }

// ────────────────────────────────────────────────────────────────────
// shared contract surface

[<Fact>]
let ``search tools fail loudly on null-Path runtimes with no exec fallback`` () : Task =
    task {
        use workspace = NullPathWorkspace() :> IWorkspace

        let glob = BuiltinSearchTools.CreateGlobTool(workspace)

        let! globError = Assert.ThrowsAsync<ToolException>(fun () -> invokeAsync glob [ "pattern", box "*" ] :> Task)

        Assert.Contains("root path", globError.Message)

        let grep = BuiltinSearchTools.CreateGrepTool(workspace)

        let! grepError = Assert.ThrowsAsync<ToolException>(fun () -> invokeAsync grep [ "pattern", box "x" ] :> Task)

        Assert.Contains("root path", grepError.Message)
    }

[<Fact>]
let ``search tools carry their names schemas and descriptions`` () : Task =
    task {
        use workspace = bindWorkspace ()

        Assert.Equal(100, BuiltinSearchTools.DefaultMaxResults)
        Assert.Equal(1000, BuiltinSearchTools.MaxMaxResults)

        let glob = BuiltinSearchTools.CreateGlobTool(workspace)
        Assert.Equal("glob", glob.Name)
        Assert.Equal("glob", BuiltinSearchTools.GlobToolName)
        Assert.Contains(".gitignore", glob.Description)

        let globFn = glob :?> AIFunction
        Assert.Contains("max_results", globFn.JsonSchema.GetRawText())
        Assert.Contains("\"default\":100", globFn.JsonSchema.GetRawText())

        let grep = BuiltinSearchTools.CreateGrepTool(workspace)
        Assert.Equal("grep", grep.Name)
        Assert.Equal("grep", BuiltinSearchTools.GrepToolName)
        Assert.Contains("file_filter", grep.Description)

        let grepFn = grep :?> AIFunction
        Assert.Contains("file_filter", grepFn.JsonSchema.GetRawText())
    }

[<Fact>]
let ``search tools reject a null workspace`` () =
    Assert.Throws<ArgumentNullException>(fun () ->
        BuiltinSearchTools.CreateGlobTool(Unchecked.defaultof<IWorkspace>) |> ignore)
    |> ignore

    Assert.Throws<ArgumentNullException>(fun () ->
        BuiltinSearchTools.CreateGrepTool(Unchecked.defaultof<IWorkspace>) |> ignore)
    |> ignore
