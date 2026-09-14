// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Workspace.Process
open Xunit

module ProcessWorkspaceRuntimeTests =

    let private freshRoot () =
        let root =
            Path.Combine(Path.GetTempPath(), "legate-wsrt-tests", Ulid.NewUlid().ToString())

        Directory.CreateDirectory root |> ignore
        root

    let private optionsFor root =
        ProcessWorkspaceRuntimeOptions(Root = root)

    let private sessionFor () =
        {
            Id = SessionId.New()
            Tenant = TenantId.Create "runtime-tests"
            AgentId = AgentId.New()
            Title = "workspace runtime"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
        }

    [<Fact>]
    let ``Bind creates the input output scratch layout`` () =
        task {
            let root = freshRoot ()
            let runtime = ProcessWorkspaceRuntime(optionsFor root, null, null)

            let session = sessionFor ()

            let! workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

            let directory =
                (match workspace.Root.Path with
                 | null -> failwith "the process runtime exposes its root path"
                 | path -> path)

            Assert.True(Directory.Exists(Path.Combine(directory, "input")))
            Assert.True(Directory.Exists(Path.Combine(directory, "output")))
            Assert.True(Directory.Exists(Path.Combine(directory, "scratch")))
        }

    [<Fact>]
    let ``Re-binding honours the session's WorkspaceBinding`` () =
        task {
            let root = freshRoot ()
            let runtime = ProcessWorkspaceRuntime(optionsFor root, null, null)

            let firstSession =
                { sessionFor () with
                    WorkspaceBinding = "rebound-session"
                }

            let! firstWorkspace = (runtime :> IWorkspaceRuntime).Bind(firstSession, null, CancellationToken.None)

            do!
                firstWorkspace.WriteFile(
                    "scratch/state.txt",
                    System.Text.Encoding.UTF8.GetBytes "survives",
                    CancellationToken.None
                )

            do! firstWorkspace.DisposeAsync()

            let! rebound = (runtime :> IWorkspaceRuntime).Bind(firstSession, null, CancellationToken.None)

            let! exists = rebound.Exists("scratch/state.txt", CancellationToken.None)
            Assert.True(exists)
        }

    [<Fact>]
    let ``A binding that escapes the root throws and binds nothing`` () =
        task {
            let root = freshRoot ()

            let runtime =
                ProcessWorkspaceRuntime(optionsFor root, null, null) :> IWorkspaceRuntime

            let escaping =
                { sessionFor () with
                    WorkspaceBinding = "../escape"
                }

            Assert.Throws<WorkspaceException>(fun () ->
                runtime.Bind(escaping, null, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore

            Assert.False(Directory.Exists(Path.Combine(Path.GetFullPath root, "escape")))
        }

    [<Fact>]
    let ``CheckReadiness reports an unready result on an unwritable root`` () =
        task {
            let root = freshRoot ()

            // A FILE occupies the runtime root: the readiness probe cannot
            // create its temp file under it, and the host reports why.
            let blocked = Path.Combine(root, "occupied")
            do! File.WriteAllTextAsync(blocked, "not a directory")

            let runtime =
                ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = blocked), null, null) :> IWorkspaceRuntime

            let! readiness = runtime.CheckReadiness(CancellationToken.None)

            Assert.False(readiness.IsReady)
            Assert.NotNull(readiness.Reason)
        }

    [<Fact>]
    let ``CheckReadiness reports ready on a writable root`` () =
        task {
            let root = freshRoot ()

            let runtime =
                ProcessWorkspaceRuntime(optionsFor root, null, null) :> IWorkspaceRuntime

            let! readiness = runtime.CheckReadiness(CancellationToken.None)

            Assert.True(readiness.IsReady)
            Assert.Null(readiness.Reason)
        }

    [<Fact>]
    let ``Bind throws WorkspaceException on an uncreatable root with no fallback`` () =
        task {
            let root = freshRoot ()

            // A FILE occupies the runtime root: the bind cannot create the
            // session's directory under it, and the failure surfaces
            // instead of falling back.
            let blocked = Path.Combine(root, "blocked")
            do! File.WriteAllTextAsync(blocked, "not a directory")

            let runtime =
                ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = blocked), null, null) :> IWorkspaceRuntime

            Assert.Throws<WorkspaceException>(fun () ->
                runtime.Bind(sessionFor (), null, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }
