// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.IO
open System.Text
open System.Threading
open System.Collections.Generic
open System.Threading.Tasks
open Legate
open Legate.Workspace.Process
open Xunit

module ProcessWorkspaceTests =

    let private workspace () =
        let root =
            Path.Combine(Path.GetTempPath(), "legate-ws-tests", Ulid.NewUlid().ToString())

        Directory.CreateDirectory root |> ignore

        let runtime =
            ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = root), null, null) :> IWorkspaceRuntime

        let session =
            {
                Id = SessionId.New()
                Tenant = TenantId.Create "workspace-tests"
                AgentId = AgentId.New()
                Title = "workspace"
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

    [<Fact>]
    let ``Exists and ReadFile round-trip a written file`` () =
        task {
            use workspace = workspace ()

            let bytes = Encoding.UTF8.GetBytes "hello workspace"
            do! workspace.WriteFile("output/hello.txt", bytes, CancellationToken.None)

            let! exists = workspace.Exists("output/hello.txt", CancellationToken.None)
            Assert.True(exists)

            use! stream = workspace.ReadFile("output/hello.txt", CancellationToken.None)
            use memory = new MemoryStream()
            do! stream.CopyToAsync memory

            Assert.Equal<byte>(bytes, memory.ToArray())
        }

    [<Fact>]
    let ``WriteFile atomically replaces existing content`` () =
        task {
            use workspace = workspace ()

            do! workspace.WriteFile("scratch/a.txt", Encoding.UTF8.GetBytes "first", CancellationToken.None)
            do! workspace.WriteFile("scratch/a.txt", Encoding.UTF8.GetBytes "second", CancellationToken.None)

            use! stream = workspace.ReadFile("scratch/a.txt", CancellationToken.None)
            use reader = new StreamReader(stream)

            let! text = reader.ReadToEndAsync()
            Assert.Equal("second", text)
        }

    [<Fact>]
    let ``ReadFile throws FileNotFound for a missing file`` () =
        task {
            use workspace = workspace ()

            Assert.Throws<FileNotFoundException>(fun () ->
                workspace.ReadFile("scratch/missing.txt", CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

    [<Fact>]
    let ``DeleteFile reports existence`` () =
        task {
            use workspace = workspace ()

            do! workspace.WriteFile("scratch/doomed.txt", [||], CancellationToken.None)

            let! deleted = workspace.DeleteFile("scratch/doomed.txt", CancellationToken.None)
            Assert.True(deleted)

            let! again = workspace.DeleteFile("scratch/doomed.txt", CancellationToken.None)
            Assert.False(again)
        }

    [<Fact>]
    let ``Path violations never escape the workspace`` () =
        task {
            use workspace = workspace ()

            let doomed () =
                workspace.Exists("../escape.txt", CancellationToken.None).GetAwaiter().GetResult()
                |> ignore

            Assert.Throws<WorkspaceException>(fun () -> doomed ()) |> ignore

            let rooted () =
                workspace.WriteFile("C:/outside.txt", [||], CancellationToken.None).GetAwaiter().GetResult()

            Assert.Throws<WorkspaceException>(fun () -> rooted ()) |> ignore

            let backslashed () =
                workspace.ReadFile("output\\file.txt", CancellationToken.None).GetAwaiter().GetResult()
                |> ignore

            Assert.Throws<WorkspaceException>(fun () -> backslashed ()) |> ignore
        }

    [<Fact>]
    let ``Exec captures output and the exit code`` () =
        task {
            use workspace = workspace ()

            let! result = workspace.Exec("echo legate-echo", Nullable(), null, CancellationToken.None)

            Assert.False(result.TimedOut)
            Assert.Equal(0, result.ExitCode)
            Assert.Contains("legate-echo", result.StandardOutput)
        }

    [<Fact>]
    let ``Exec injects environment variables`` () =
        task {
            use workspace = workspace ()

            let env =
                Dictionary<string, string>(dict [ "LEGATE_TEST_VAR", "injected-value" ])
                :> IReadOnlyDictionary<string, string>

            let command =
                if OperatingSystem.IsWindows() then
                    "echo %LEGATE_TEST_VAR%"
                else
                    "echo $LEGATE_TEST_VAR"

            let! result = workspace.Exec(command, Nullable(), env, CancellationToken.None)

            Assert.Contains("injected-value", result.StandardOutput)
        }

    [<Fact>]
    let ``Exec reports non-zero exits without throwing`` () =
        task {
            use workspace = workspace ()

            let command =
                if OperatingSystem.IsWindows() then
                    "cmd /c exit 7"
                else
                    "exit 7"

            let! result = workspace.Exec(command, Nullable(), null, CancellationToken.None)

            Assert.Equal(7, result.ExitCode)
        }

    [<Fact>]
    let ``Exec kills the tree on timeout and reports TimedOut`` () =
        task {
            use workspace = workspace ()

            let command =
                if OperatingSystem.IsWindows() then
                    "ping -n 30 127.0.0.1 > nul"
                else
                    "sleep 30"

            let! result = workspace.Exec(command, Nullable(TimeSpan.FromSeconds 2.), null, CancellationToken.None)

            Assert.True(result.TimedOut)
        }

    [<Fact>]
    let ``Dispose is idempotent and members throw after it`` () =
        task {
            let workspace = workspace ()

            do! workspace.DisposeAsync()

            do! workspace.DisposeAsync()

            Assert.Throws<ObjectDisposedException>(fun () ->
                workspace.Exists("any.txt", CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

    [<Fact>]
    let ``Cancellation abandons the exec and kills the process`` () =
        task {
            use workspace = workspace ()

            use cts = new CancellationTokenSource()

            let command =
                if OperatingSystem.IsWindows() then
                    "ping -n 30 127.0.0.1 > nul"
                else
                    "sleep 30"

            let run = workspace.Exec(command, Nullable(), null, cts.Token)

            // Let the shell start, then cancel: the result carries the
            // killed process's OS exit value and TimedOut stays false
            // (the caller, not the timeout, did the cancelling).
            do! Task.Delay(300)
            cts.Cancel()

            let! result = run

            Assert.False(result.TimedOut)
        }

    [<Fact>]
    let ``The runtime default timeout kills unbounded execs`` () =
        task {
            let root =
                Path.Combine(Path.GetTempPath(), "legate-ws-tests", Ulid.NewUlid().ToString())

            Directory.CreateDirectory root |> ignore

            let runtime =
                ProcessWorkspaceRuntime(
                    ProcessWorkspaceRuntimeOptions(Root = root, DefaultExecTimeout = Nullable(TimeSpan.FromSeconds 2.)),
                    null,
                    null
                )
                :> IWorkspaceRuntime

            let session =
                {
                    Id = SessionId.New()
                    Tenant = TenantId.Create "workspace-tests"
                    AgentId = AgentId.New()
                    Title = "workspace"
                    State = SessionState.Idle
                    CurrentTurnId = Nullable()
                    CreatedAt = DateTimeOffset.MinValue
                    UpdatedAt = DateTimeOffset.MinValue
                    ClosedAt = Nullable()
                    WorkspaceBinding = null
                    Options = SessionOptions()
                    PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
                }

            let! bound = runtime.Bind(session, null, CancellationToken.None)
            use workspace = bound

            let command =
                if OperatingSystem.IsWindows() then
                    "ping -n 30 127.0.0.1 > nul"
                else
                    "sleep 30"

            let! result = workspace.Exec(command, Nullable(), null, CancellationToken.None)

            Assert.True(result.TimedOut)
        }
