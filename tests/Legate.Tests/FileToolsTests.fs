// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Tools
open Legate.Workspace.Process
open Microsoft.Extensions.AI
open Xunit

module FileToolsTests =

    let private bindTempWorkspace () : IWorkspace =
        let root =
            Path.Combine(Path.GetTempPath(), "legate-filetools-tests", Ulid.NewUlid().ToString())

        Directory.CreateDirectory root |> ignore

        let runtime =
            ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = root), null, null) :> IWorkspaceRuntime

        let session =
            {
                Id = SessionId.New()
                Tenant = TenantId.Create "filetools-tests"
                AgentId = AgentId.New()
                Title = "filetools"
                State = SessionState.Idle
                CurrentTurnId = Nullable()
                CreatedAt = DateTimeOffset.MinValue
                UpdatedAt = DateTimeOffset.MinValue
                ClosedAt = Nullable()
                WorkspaceBinding = null
                Options = SessionOptions()
            }

        runtime.Bind(session, null, CancellationToken.None).GetAwaiter().GetResult()

    let private rootOf (workspace: IWorkspace) : string =
        match workspace.Root.Path with
        | null -> failwith "The process workspace must expose a root path."
        | root -> root

    let private arguments (pairs: (string * (obj | null)) list) =
        let table = Dictionary<string, obj | null>()

        for key, value in pairs do
            table[key] <- value

        AIFunctionArguments(table)

    let private invoke (tool: AIFunction) (pairs: (string * (obj | null)) list) : ValueTask<obj | null> =
        tool.InvokeAsync(arguments pairs, CancellationToken.None)

    let private invokeText (tool: AIFunction) (pairs: (string * (obj | null)) list) : Task<string> =
        task {
            let! result = invoke tool pairs
            let text: string | null = unbox result

            match text with
            | null -> return failwith "The tool returned null."
            | value -> return value
        }

    let private invokeFails (tool: AIFunction) (pairs: (string * (obj | null)) list) : Task<ToolException> =
        task {
            let! ex = Assert.ThrowsAsync<ToolException>(fun () -> (invoke tool pairs).AsTask() :> Task)

            return ex
        }

    let private nullRootWorkspace () : IWorkspace =
        { new IWorkspace with
            member _.Root = WorkspaceRoot("file-tools-tests", null)

            member _.Exec(_, _, _, _) =
                Task.FromException<WorkspaceExecResult>(NotImplementedException("not used"))

            member _.Exists(_, _) =
                Task.FromException<bool>(NotImplementedException("not used"))

            member _.ReadFile(_, _) =
                Task.FromException<Stream>(NotImplementedException("not used"))

            member _.WriteFile(_, _, _) =
                Task.FromException(NotImplementedException("not used"))

            member _.DeleteFile(_, _) =
                Task.FromException<bool>(NotImplementedException("not used"))

            member _.DisposeAsync() = ValueTask.CompletedTask
        }

    // ── read_file ───────────────────────────────────────────────

    [<Fact>]
    let ``ReadFile returns line-numbered content`` () =
        task {
            use workspace = bindTempWorkspace ()
            do! workspace.WriteFile("notes/a.txt", Encoding.UTF8.GetBytes "hello\nworld", CancellationToken.None)

            let! text = invokeText (FileTools.createReadFileTool workspace) [ "path", box "notes/a.txt" ]

            Assert.Equal("1: hello\n2: world", text)
        }

    [<Fact>]
    let ``ReadFile honours a 1-based line range`` () =
        task {
            use workspace = bindTempWorkspace ()
            do! workspace.WriteFile("a.txt", Encoding.UTF8.GetBytes "one\ntwo\nthree\nfour", CancellationToken.None)

            let! text =
                invokeText
                    (FileTools.createReadFileTool workspace)
                    [
                        "path", box "a.txt"
                        "startLine", box 2
                        "endLine", box 3
                    ]

            Assert.Equal("2: two\n3: three", text)
        }

    [<Fact>]
    let ``ReadFile normalises CRLF line endings`` () =
        task {
            use workspace = bindTempWorkspace ()
            do! workspace.WriteFile("a.txt", Encoding.UTF8.GetBytes "one\r\ntwo\r\n", CancellationToken.None)

            let! text = invokeText (FileTools.createReadFileTool workspace) [ "path", box "a.txt" ]

            Assert.Equal("1: one\n2: two", text)
        }

    [<Fact>]
    let ``ReadFile marks output cut at the byte cap`` () =
        task {
            use workspace = bindTempWorkspace ()
            do! workspace.WriteFile("a.txt", Encoding.UTF8.GetBytes "alpha\nbeta\ngamma", CancellationToken.None)

            let! text =
                invokeText
                    (FileTools.createReadFileTool workspace)
                    [
                        "path", box "a.txt"
                        "maxBytes", box 8
                    ]

            Assert.StartsWith("1: alpha", text)
            Assert.Contains("truncated", text)
            Assert.Contains("8 bytes", text)
        }

    [<Fact>]
    let ``ReadFile rejects an invalid line range`` () =
        task {
            use workspace = bindTempWorkspace ()
            do! workspace.WriteFile("a.txt", Encoding.UTF8.GetBytes "one\ntwo", CancellationToken.None)

            let! backward =
                invokeFails
                    (FileTools.createReadFileTool workspace)
                    [
                        "path", box "a.txt"
                        "startLine", box 3
                        "endLine", box 2
                    ]

            Assert.Equal(FileTools.readFileName, backward.ToolName)

            let! zero =
                invokeFails
                    (FileTools.createReadFileTool workspace)
                    [
                        "path", box "a.txt"
                        "startLine", box 0
                    ]

            Assert.Equal(FileTools.readFileName, zero.ToolName)
        }

    [<Fact>]
    let ``ReadFile reports a missing file as a tool failure`` () =
        task {
            use workspace = bindTempWorkspace ()

            let! ex = invokeFails (FileTools.createReadFileTool workspace) [ "path", box "missing.txt" ]

            Assert.Equal(FileTools.readFileName, ex.ToolName)
            Assert.Contains("does not exist", ex.Message)
        }

    [<Fact>]
    let ``ReadFile rejects paths outside the workspace`` () =
        task {
            use workspace = bindTempWorkspace ()
            let tool = FileTools.createReadFileTool workspace

            for bad in
                [
                    "../outside.txt"
                    "/absolute.txt"
                    "C:/absolute.txt"
                    "a\\b.txt"
                    ""
                    "a//b.txt"
                ] do
                let! ex = invokeFails tool [ "path", box bad ]
                Assert.Equal(FileTools.readFileName, ex.ToolName)
        }

    [<Fact>]
    let ``ReadFile rejects a symlink escaping the root`` () =
        task {
            use workspace = bindTempWorkspace ()
            let root = rootOf workspace

            let outsideDir =
                Path.Combine(Path.GetTempPath(), "legate-filetools-outside", Ulid.NewUlid().ToString())

            Directory.CreateDirectory outsideDir |> ignore
            let outsideFile = Path.Combine(outsideDir, "secret.txt")
            File.WriteAllText(outsideFile, "outside")

            try
                File.CreateSymbolicLink(Path.Combine(root, "escape.txt"), outsideFile) |> ignore

                let! ex = invokeFails (FileTools.createReadFileTool workspace) [ "path", box "escape.txt" ]

                Assert.Equal(FileTools.readFileName, ex.ToolName)
            with :? UnauthorizedAccessException ->
                // The environment forbids symlinks: nothing to assert.
                ()
        }

    [<Fact>]
    let ``ReadFile rejects a symlinked directory escaping the root`` () =
        task {
            use workspace = bindTempWorkspace ()
            let root = rootOf workspace

            let outsideDir =
                Path.Combine(Path.GetTempPath(), "legate-filetools-outside", Ulid.NewUlid().ToString())

            Directory.CreateDirectory outsideDir |> ignore
            File.WriteAllText(Path.Combine(outsideDir, "secret.txt"), "outside")

            try
                Directory.CreateSymbolicLink(Path.Combine(root, "escape-dir"), outsideDir)
                |> ignore

                let! ex = invokeFails (FileTools.createReadFileTool workspace) [ "path", box "escape-dir/secret.txt" ]

                Assert.Equal(FileTools.readFileName, ex.ToolName)
            with :? UnauthorizedAccessException ->
                // The environment forbids symlinks: nothing to assert.
                ()
        }

    // ── write_file ──────────────────────────────────────────────

    [<Fact>]
    let ``WriteFile creates parent directories and round-trips`` () =
        task {
            use workspace = bindTempWorkspace ()

            let! confirmation =
                invokeText
                    (FileTools.createWriteFileTool workspace)
                    [
                        "path", box "a/b/c.txt"
                        "content", box "hi"
                    ]

            Assert.Equal("Wrote 2 bytes to a/b/c.txt.", confirmation)

            use! stream = workspace.ReadFile("a/b/c.txt", CancellationToken.None)
            use reader = new StreamReader(stream)
            let! text = reader.ReadToEndAsync()
            Assert.Equal("hi", text)
        }

    [<Fact>]
    let ``WriteFile refuses the read-only input area with a hint`` () =
        task {
            use workspace = bindTempWorkspace ()
            let tool = FileTools.createWriteFileTool workspace

            for bad in [ "input/notes.txt"; "input" ] do
                let! ex = invokeFails tool [ "path", box bad; "content", box "hi" ]
                Assert.Equal(FileTools.writeFileName, ex.ToolName)
                Assert.Contains("read-only", ex.Message)
                Assert.Contains("input", ex.Message)
        }

    [<Fact>]
    let ``WriteFile rejects traversal paths`` () =
        task {
            use workspace = bindTempWorkspace ()

            let! ex =
                invokeFails
                    (FileTools.createWriteFileTool workspace)
                    [
                        "path", box "../outside.txt"
                        "content", box "hi"
                    ]

            Assert.Equal(FileTools.writeFileName, ex.ToolName)
        }

    [<Fact>]
    let ``ReadFile serves the read-only input area`` () =
        task {
            use workspace = bindTempWorkspace ()
            do! workspace.WriteFile("input/data.txt", Encoding.UTF8.GetBytes "seed", CancellationToken.None)

            let! text = invokeText (FileTools.createReadFileTool workspace) [ "path", box "input/data.txt" ]

            Assert.Equal("1: seed", text)
        }

    // ── list_files ──────────────────────────────────────────────

    [<Fact>]
    let ``ListFiles lists one level with directory markers`` () =
        task {
            use workspace = bindTempWorkspace ()
            do! workspace.WriteFile("b.txt", Encoding.UTF8.GetBytes "b", CancellationToken.None)
            do! workspace.WriteFile("a/x.txt", Encoding.UTF8.GetBytes "x", CancellationToken.None)
            Directory.CreateDirectory(Path.Combine(rootOf workspace, "adir")) |> ignore

            let! listing = invokeText (FileTools.createListFilesTool workspace) []

            // The process runtime seeds input/, output/, and scratch/ on
            // bind, so the fresh root already holds them.
            Assert.Equal("a/\nadir/\nb.txt\ninput/\noutput/\nscratch/", listing)
        }

    [<Fact>]
    let ``ListFiles lists a subdirectory`` () =
        task {
            use workspace = bindTempWorkspace ()
            do! workspace.WriteFile("a/x.txt", Encoding.UTF8.GetBytes "x", CancellationToken.None)
            do! workspace.WriteFile("a/y.txt", Encoding.UTF8.GetBytes "y", CancellationToken.None)

            let! listing = invokeText (FileTools.createListFilesTool workspace) [ "directory", box "a" ]

            // Entries stay workspace-relative (directly usable as tool
            // paths), not relative to the listed directory.
            Assert.Equal("a/x.txt\na/y.txt", listing)
        }

    [<Fact>]
    let ``ListFiles rejects bad paths and missing directories`` () =
        task {
            use workspace = bindTempWorkspace ()
            let tool = FileTools.createListFilesTool workspace

            let! traversal = invokeFails tool [ "directory", box ".." ]
            Assert.Equal(FileTools.listFilesName, traversal.ToolName)

            let! missing = invokeFails tool [ "directory", box "nope" ]
            Assert.Equal(FileTools.listFilesName, missing.ToolName)

            do! workspace.WriteFile("f.txt", Encoding.UTF8.GetBytes "f", CancellationToken.None)
            let! fileTarget = invokeFails tool [ "directory", box "f.txt" ]
            Assert.Equal(FileTools.listFilesName, fileTarget.ToolName)
        }

    [<Fact>]
    let ``ListFiles fails without a workspace filesystem path`` () =
        task {
            use workspace = nullRootWorkspace ()

            let! ex = invokeFails (FileTools.createListFilesTool workspace) []

            Assert.Equal(FileTools.listFilesName, ex.ToolName)
        }

    // ── binary pair ─────────────────────────────────────────────

    [<Fact>]
    let ``Binary tools round-trip bytes under the cap`` () =
        task {
            use workspace = bindTempWorkspace ()
            let payload = [| 0uy; 1uy; 2uy; 250uy; 255uy |]
            let encoded = Convert.ToBase64String payload

            let! writeConfirmation =
                invokeText
                    (FileTools.createWriteBinaryTool workspace)
                    [
                        "path", box "blob.bin"
                        "base64Content", box encoded
                    ]

            Assert.Equal(sprintf "Wrote %d bytes to blob.bin." payload.Length, writeConfirmation)

            use! stream = workspace.ReadFile("blob.bin", CancellationToken.None)
            use memory = new MemoryStream()
            do! stream.CopyToAsync memory
            Assert.Equal<byte>(payload, memory.ToArray())

            do! workspace.WriteFile("raw.bin", payload, CancellationToken.None)

            let! readBack = invokeText (FileTools.createReadBinaryTool workspace) [ "path", box "raw.bin" ]

            Assert.Equal(encoded, readBack)
        }

    [<Fact>]
    let ``ReadBinary rejects files over the 10 MiB cap`` () =
        task {
            use workspace = bindTempWorkspace ()
            let oversized = Array.zeroCreate<byte> (11 * 1024 * 1024)
            do! workspace.WriteFile("big.bin", oversized, CancellationToken.None)

            let! ex = invokeFails (FileTools.createReadBinaryTool workspace) [ "path", box "big.bin" ]

            Assert.Equal(FileTools.readBinaryName, ex.ToolName)
            Assert.Contains("exceeds", ex.Message)
        }

    [<Fact>]
    let ``WriteBinary rejects payloads over the 10 MiB cap`` () =
        task {
            use workspace = bindTempWorkspace ()
            let oversized = Convert.ToBase64String(Array.zeroCreate<byte> (11 * 1024 * 1024))

            let! ex =
                invokeFails
                    (FileTools.createWriteBinaryTool workspace)
                    [
                        "path", box "big.bin"
                        "base64Content", box oversized
                    ]

            Assert.Equal(FileTools.writeBinaryName, ex.ToolName)
            Assert.Contains("exceeds", ex.Message)
        }

    [<Fact>]
    let ``WriteBinary rejects invalid base64 and the input area`` () =
        task {
            use workspace = bindTempWorkspace ()
            let tool = FileTools.createWriteBinaryTool workspace

            let! badEncoding =
                invokeFails
                    tool
                    [
                        "path", box "blob.bin"
                        "base64Content", box "not base64!!"
                    ]

            Assert.Equal(FileTools.writeBinaryName, badEncoding.ToolName)
            Assert.Contains("base64", badEncoding.Message)

            let! refused =
                invokeFails
                    tool
                    [
                        "path", box "input/blob.bin"
                        "base64Content", box "aGk="
                    ]

            Assert.Equal(FileTools.writeBinaryName, refused.ToolName)
            Assert.Contains("read-only", refused.Message)
        }

    // ── tool shape ──────────────────────────────────────────────

    [<Fact>]
    let ``Factories reject a null workspace`` () =
        let missing = Unchecked.defaultof<IWorkspace>

        for create in
            [
                FileTools.createReadFileTool
                FileTools.createWriteFileTool
                FileTools.createListFilesTool
                FileTools.createReadBinaryTool
                FileTools.createWriteBinaryTool
            ] do
            Assert.Throws<ArgumentNullException>(fun () -> create missing |> ignore)
            |> ignore

    [<Fact>]
    let ``Every tool exposes its name, description, and JSON schema`` () =
        task {
            use workspace = bindTempWorkspace ()

            let tools =
                [
                    FileTools.createReadFileTool workspace,
                    "read_file",
                    [ "path" ],
                    [ "startLine"; "endLine"; "maxBytes" ]
                    FileTools.createWriteFileTool workspace, "write_file", [ "path"; "content" ], []
                    FileTools.createListFilesTool workspace, "list_files", [], [ "directory" ]
                    FileTools.createReadBinaryTool workspace, "read_binary_base64", [ "path" ], [ "maxBytes" ]
                    FileTools.createWriteBinaryTool workspace,
                    "write_binary_base64",
                    [ "path"; "base64Content" ],
                    [ "maxBytes" ]
                ]

            for tool, name, required, optional in tools do
                Assert.Equal(name, tool.Name)
                Assert.False(String.IsNullOrEmpty tool.Description)

                let schema = tool.JsonSchema
                Assert.Equal(System.Text.Json.JsonValueKind.Object, schema.ValueKind)

                let properties = schema.GetProperty("properties")

                let mutable probe = Unchecked.defaultof<System.Text.Json.JsonElement>

                for property in required @ optional do
                    Assert.True(properties.TryGetProperty(property, &probe))

                // No "required" section means nothing is required
                // (list_files takes only the optional directory).
                let mutable requiredSection = Unchecked.defaultof<System.Text.Json.JsonElement>

                let requiredNames =
                    if schema.TryGetProperty("required", &requiredSection) then
                        [
                            for entry in requiredSection.EnumerateArray() ->
                                match entry.GetString() with
                                | null -> ""
                                | name -> name
                        ]
                    else
                        []

                for property in required do
                    Assert.Contains(property, requiredNames)

                for property in optional do
                    Assert.DoesNotContain(property, requiredNames)
        }
