// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.EditToolsTests

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
        Path.Combine(Path.GetTempPath(), "legate-edit-tools-tests", Ulid.NewUlid().ToString())

    Directory.CreateDirectory root |> ignore

    let runtime =
        ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = root), null, null) :> IWorkspaceRuntime

    let session =
        {
            Id = SessionId.New()
            Tenant = TenantId.Create "edit-tools-tests"
            AgentId = AgentId.New()
            Title = "edit-tools"
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

let private readText (workspace: IWorkspace) (path: string) : Task<string> =
    task {
        use! stream = workspace.ReadFile(path, CancellationToken.None)
        use reader = new StreamReader(stream, Encoding.UTF8)
        return! reader.ReadToEndAsync()
    }

[<Fact>]
let ``edit_file fails when the old string is missing`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! workspace.WriteFile("notes.txt", Encoding.UTF8.GetBytes "hello world", CancellationToken.None)

        let tool = BuiltinEditTools.CreateEditFileTool(workspace)

        let! error =
            Assert.ThrowsAsync<ToolException>(fun () ->
                invokeAsync
                    tool
                    [
                        "path", box "notes.txt"
                        "old_string", box "goodbye"
                        "new_string", box "hi"
                        "replace_all", box false
                    ]
                :> Task)

        Assert.Contains("not found", error.Message)
    }

[<Fact>]
let ``edit_file fails when the old string is ambiguous without replace_all`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! workspace.WriteFile("repeat.txt", Encoding.UTF8.GetBytes "a a a", CancellationToken.None)

        let tool = BuiltinEditTools.CreateEditFileTool(workspace)

        let! error =
            Assert.ThrowsAsync<ToolException>(fun () ->
                invokeAsync
                    tool
                    [
                        "path", box "repeat.txt"
                        "old_string", box "a"
                        "new_string", box "b"
                        "replace_all", box false
                    ]
                :> Task)

        Assert.Contains("replace_all", error.Message)
    }

[<Fact>]
let ``edit_file with replace_all replaces every occurrence and reports the count`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! workspace.WriteFile("repeat.txt", Encoding.UTF8.GetBytes "a a a", CancellationToken.None)

        let tool = BuiltinEditTools.CreateEditFileTool(workspace)

        let! result =
            invokeAsync
                tool
                [
                    "path", box "repeat.txt"
                    "old_string", box "a"
                    "new_string", box "b"
                    "replace_all", box true
                ]

        let report = result
        Assert.Contains("3", report)
        Assert.Contains("repeat.txt", report)

        let! text = readText workspace "repeat.txt"
        Assert.Equal("b b b", text)
    }

[<Fact>]
let ``edit_file replaces a single occurrence and reports the change`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! workspace.WriteFile("notes.txt", Encoding.UTF8.GetBytes "hello world", CancellationToken.None)

        let tool = BuiltinEditTools.CreateEditFileTool(workspace)

        let! result =
            invokeAsync
                tool
                [
                    "path", box "notes.txt"
                    "old_string", box "world"
                    "new_string", box "there"
                    "replace_all", box false
                ]

        let report = result
        Assert.Contains("1", report)
        Assert.Contains("notes.txt", report)

        let! text = readText workspace "notes.txt"
        Assert.Equal("hello there", text)
    }

[<Fact>]
let ``edit_file defaults replace_all to false when omitted`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! workspace.WriteFile("notes.txt", Encoding.UTF8.GetBytes "hello world", CancellationToken.None)

        let tool = BuiltinEditTools.CreateEditFileTool(workspace)

        let! report =
            invokeAsync
                tool
                [
                    "path", box "notes.txt"
                    "old_string", box "world"
                    "new_string", box "there"
                ]

        Assert.Contains("1", report)

        let! text = readText workspace "notes.txt"
        Assert.Equal("hello there", text)
    }

[<Fact>]
let ``edit_file rejects an empty old string`` () : Task =
    task {
        use workspace = bindWorkspace ()
        do! workspace.WriteFile("notes.txt", Encoding.UTF8.GetBytes "hello", CancellationToken.None)

        let tool = BuiltinEditTools.CreateEditFileTool(workspace)

        let! error =
            Assert.ThrowsAsync<ToolException>(fun () ->
                invokeAsync
                    tool
                    [
                        "path", box "notes.txt"
                        "old_string", box ""
                        "new_string", box "hi"
                        "replace_all", box false
                    ]
                :> Task)

        Assert.Contains("empty", error.Message)
    }

[<Fact>]
let ``edit_file surfaces a missing file`` () : Task =
    task {
        use workspace = bindWorkspace ()

        let tool = BuiltinEditTools.CreateEditFileTool(workspace)

        do!
            Assert.ThrowsAsync<FileNotFoundException>(fun () ->
                invokeAsync
                    tool
                    [
                        "path", box "missing.txt"
                        "old_string", box "a"
                        "new_string", box "b"
                        "replace_all", box false
                    ]
                :> Task)
            :> Task
    }

[<Fact>]
let ``edit_file carries its name schema and description`` () : Task =
    task {
        use workspace = bindWorkspace ()
        let tool = BuiltinEditTools.CreateEditFileTool(workspace)

        Assert.Equal("edit_file", tool.Name)
        Assert.Equal("edit_file", BuiltinEditTools.EditFileToolName)
        Assert.False(String.IsNullOrWhiteSpace(tool.Description))
        Assert.Contains("replace_all", tool.Description)

        let fn = tool :?> AIFunction
        let schema = fn.JsonSchema.GetRawText()
        Assert.Contains("old_string", schema)
        Assert.Contains("new_string", schema)
        Assert.Contains("replace_all", schema)
        Assert.Contains("\"default\":false", schema)
    }

[<Fact>]
let ``edit_file rejects a null workspace`` () =
    Assert.Throws<ArgumentNullException>(fun () ->
        BuiltinEditTools.CreateEditFileTool(Unchecked.defaultof<IWorkspace>) |> ignore)
    |> ignore
