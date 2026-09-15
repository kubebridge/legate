// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Workspace.Process
open Xunit

module WorkspaceLayoutTests =

    // Unchecked.defaultof rather than a bare null literal: under
    // Nullable=enable the literal does not satisfy a non-nullable
    // parameter, but the runtime value is still null.
    let nullString = Unchecked.defaultof<string>

    [<Fact>]
    let ``Roots resolve under input output and scratch`` () =
        Assert.Equal("input/data/orders.csv", WorkspaceLayout.InputPath "data/orders.csv")
        Assert.Equal("output/turn-03/summary.md", WorkspaceLayout.OutputPath "turn-03/summary.md")
        Assert.Equal("scratch/working/notes.txt", WorkspaceLayout.ScratchPath "working/notes.txt")

    [<Fact>]
    let ``Resolve matches the convenience paths`` () =
        Assert.Equal(
            WorkspaceLayout.InputPath "data/orders.csv",
            WorkspaceLayout.Resolve(WorkspaceLayoutRoot.Input, "data/orders.csv")
        )

        Assert.Equal(
            WorkspaceLayout.OutputPath "turn-03/summary.md",
            WorkspaceLayout.Resolve(WorkspaceLayoutRoot.Output, "turn-03/summary.md")
        )

        Assert.Equal(
            WorkspaceLayout.ScratchPath "working/notes.txt",
            WorkspaceLayout.Resolve(WorkspaceLayoutRoot.Scratch, "working/notes.txt")
        )

    [<Fact>]
    let ``RootName names the three roots`` () =
        Assert.Equal("input", WorkspaceLayout.RootName WorkspaceLayoutRoot.Input)
        Assert.Equal("output", WorkspaceLayout.RootName WorkspaceLayoutRoot.Output)
        Assert.Equal("scratch", WorkspaceLayout.RootName WorkspaceLayoutRoot.Scratch)

    [<Fact>]
    let ``Null and empty relative paths are rejected`` () =
        Assert.Throws<ArgumentNullException>(fun () -> WorkspaceLayout.InputPath nullString |> ignore)
        |> ignore

        Assert.Throws<ArgumentException>(fun () -> WorkspaceLayout.OutputPath "" |> ignore)
        |> ignore

    [<Fact>]
    let ``An undefined root value is rejected`` () =
        let undefined = enum<WorkspaceLayoutRoot> 99

        Assert.Throws<ArgumentOutOfRangeException>(fun () ->
            WorkspaceLayout.Resolve(undefined, "data/orders.csv") |> ignore)
        |> ignore

    let private freshRoot () =
        let root =
            Path.Combine(Path.GetTempPath(), "legate-layout-tests", Ulid.NewUlid().ToString())

        Directory.CreateDirectory root |> ignore
        root

    let private sessionFor () =
        {
            Id = SessionId.New()
            Tenant = TenantId.Create "layout-tests"
            AgentId = AgentId.New()
            Title = "workspace layout"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
        }

    let private readText (workspace: IWorkspace) (path: string) : Task<string> =
        task {
            use! stream = workspace.ReadFile(path, CancellationToken.None)
            use reader = new StreamReader(stream, Encoding.UTF8)
            return! reader.ReadToEndAsync()
        }

    [<Fact>]
    let ``Helper paths address the process runtime layout across re-binds`` () =
        task {
            let root = freshRoot ()

            let runtime =
                ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = root), null, null)

            let session = sessionFor ()

            let! workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

            let inputPath = WorkspaceLayout.InputPath "data/orders.csv"
            let outputPath = WorkspaceLayout.OutputPath "turn-03/summary.md"
            let scratchPath = WorkspaceLayout.ScratchPath "working/notes.txt"

            do! workspace.WriteFile(inputPath, Encoding.UTF8.GetBytes "orders", CancellationToken.None)

            do! workspace.WriteFile(outputPath, Encoding.UTF8.GetBytes "summary", CancellationToken.None)

            do! workspace.WriteFile(scratchPath, Encoding.UTF8.GetBytes "notes", CancellationToken.None)

            let! inputExists = workspace.Exists(inputPath, CancellationToken.None)
            let! outputExists = workspace.Exists(outputPath, CancellationToken.None)
            let! scratchExists = workspace.Exists(scratchPath, CancellationToken.None)
            Assert.True(inputExists)
            Assert.True(outputExists)
            Assert.True(scratchExists)

            do! workspace.DisposeAsync()

            // Re-binding over the same session preserves the state the
            // helper paths addressed: the layout is stable across binds.
            let! rebound = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

            let! orders = readText rebound inputPath
            let! summary = readText rebound outputPath
            let! notes = readText rebound scratchPath

            Assert.Equal("orders", orders)
            Assert.Equal("summary", summary)
            Assert.Equal("notes", notes)

            let directory =
                (match rebound.Root.Path with
                 | null -> failwith "the process runtime exposes its root path"
                 | path -> path)

            Assert.True(Directory.Exists(Path.Combine(directory, "input")))
            Assert.True(Directory.Exists(Path.Combine(directory, "output")))
            Assert.True(Directory.Exists(Path.Combine(directory, "scratch")))
        }
