// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.IO
open System.Text
open System.Threading
open System.Collections.Generic
open System.Threading.Tasks
open Legate
open Legate.Workspace.HostDirectory
open Xunit

module HostDirectoryWorkspaceTests =

    let private freshRoot (prefix: string) =
        let root = Path.Combine(Path.GetTempPath(), prefix, Ulid.NewUlid().ToString())

        Directory.CreateDirectory root |> ignore
        root

    let private sessionFor () =
        {
            Id = SessionId.New()
            Tenant = TenantId.Create "host-directory-tests"
            AgentId = AgentId.New()
            Title = "host directory"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    let private bind (root: string) (allowOutsideRoot: bool) =
        let options =
            HostDirectoryWorkspaceRuntimeOptions(Root = root, AllowOutsideRoot = allowOutsideRoot)

        let runtime =
            HostDirectoryWorkspaceRuntime(options, null, null) :> IWorkspaceRuntime

        runtime.Bind(sessionFor (), null, CancellationToken.None).GetAwaiter().GetResult()

    let private workspace () =
        bind (freshRoot "legate-hd-tests") false

    let private rootOf (workspace: IWorkspace) =
        match workspace.Root.Path with
        | null -> failwith "the host directory runtime exposes its root path"
        | path -> path

    let private assertWorkspaceThrows (action: unit -> 'T) =
        Assert.Throws<WorkspaceException>(fun () -> action () |> ignore) |> ignore

    [<Fact>]
    let ``Exists and ReadFile round-trip a written file`` () =
        task {
            use workspace = workspace ()

            let bytes = Encoding.UTF8.GetBytes "hello host directory"
            do! workspace.WriteFile("notes/hello.txt", bytes, CancellationToken.None)

            let! exists = workspace.Exists("notes/hello.txt", CancellationToken.None)
            Assert.True(exists)

            use! stream = workspace.ReadFile("notes/hello.txt", CancellationToken.None)
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

            for path in
                [
                    "../escape.txt"
                    "/absolute.txt"
                    "C:/outside.txt"
                    "output\\file.txt"
                ] do
                assertWorkspaceThrows (fun () ->
                    workspace.Exists(path, CancellationToken.None).GetAwaiter().GetResult())

                assertWorkspaceThrows (fun () ->
                    workspace.ReadFile(path, CancellationToken.None).GetAwaiter().GetResult())

                assertWorkspaceThrows (fun () ->
                    workspace.WriteFile(path, [||], CancellationToken.None).GetAwaiter().GetResult())

                assertWorkspaceThrows (fun () ->
                    workspace.DeleteFile(path, CancellationToken.None).GetAwaiter().GetResult())
        }

    [<Fact>]
    let ``A file symlink pointing outside is rejected on every file op`` () =
        task {
            use workspace = workspace ()
            let root = rootOf workspace

            let outside = freshRoot "legate-hd-outside"
            let secret = Path.Combine(outside, "secret.txt")
            do! File.WriteAllTextAsync(secret, "outside")

            File.CreateSymbolicLink(Path.Combine(root, "escape-link.txt"), secret) |> ignore

            assertWorkspaceThrows (fun () ->
                workspace.Exists("escape-link.txt", CancellationToken.None).GetAwaiter().GetResult())

            assertWorkspaceThrows (fun () ->
                workspace.ReadFile("escape-link.txt", CancellationToken.None).GetAwaiter().GetResult())

            assertWorkspaceThrows (fun () ->
                workspace.WriteFile("escape-link.txt", [||], CancellationToken.None).GetAwaiter().GetResult())

            assertWorkspaceThrows (fun () ->
                workspace.DeleteFile("escape-link.txt", CancellationToken.None).GetAwaiter().GetResult())

            // The rejected write left the outside file untouched.
            let! text = File.ReadAllTextAsync secret
            Assert.Equal("outside", text)
        }

    [<Fact>]
    let ``A directory symlink pointing outside is rejected through nested paths`` () =
        task {
            use workspace = workspace ()
            let root = rootOf workspace

            let outside = freshRoot "legate-hd-outside"
            do! File.WriteAllTextAsync(Path.Combine(outside, "inner.txt"), "outside")

            Directory.CreateSymbolicLink(Path.Combine(root, "escape-dir"), outside)
            |> ignore

            assertWorkspaceThrows (fun () ->
                workspace.Exists("escape-dir/inner.txt", CancellationToken.None).GetAwaiter().GetResult())

            assertWorkspaceThrows (fun () ->
                workspace.ReadFile("escape-dir/inner.txt", CancellationToken.None).GetAwaiter().GetResult())

            assertWorkspaceThrows (fun () ->
                workspace.WriteFile("escape-dir/planted.txt", [||], CancellationToken.None).GetAwaiter().GetResult())

            Assert.False(File.Exists(Path.Combine(outside, "planted.txt")))
        }

    [<Fact>]
    let ``A dangling link inside the root behaves like a missing file`` () =
        task {
            use workspace = workspace ()
            let root = rootOf workspace

            File.CreateSymbolicLink(Path.Combine(root, "dangling.txt"), Path.Combine(root, "never", "missing.txt"))
            |> ignore

            let! exists = workspace.Exists("dangling.txt", CancellationToken.None)
            Assert.False(exists)

            Assert.Throws<FileNotFoundException>(fun () ->
                workspace.ReadFile("dangling.txt", CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

    [<Fact>]
    let ``A dangling link pointing outside is rejected`` () =
        task {
            use workspace = workspace ()
            let root = rootOf workspace

            let outside = freshRoot "legate-hd-outside"

            File.CreateSymbolicLink(Path.Combine(root, "dangling-out.txt"), Path.Combine(outside, "missing.txt"))
            |> ignore

            assertWorkspaceThrows (fun () ->
                workspace.Exists("dangling-out.txt", CancellationToken.None).GetAwaiter().GetResult())
        }

    [<Fact>]
    let ``A symlink loop is rejected`` () =
        task {
            use workspace = workspace ()
            let root = rootOf workspace

            let a = Path.Combine(root, "loop-a")
            let b = Path.Combine(root, "loop-b")
            Directory.CreateSymbolicLink(a, b) |> ignore
            Directory.CreateSymbolicLink(b, a) |> ignore

            assertWorkspaceThrows (fun () ->
                workspace.Exists("loop-a", CancellationToken.None).GetAwaiter().GetResult())

            assertWorkspaceThrows (fun () ->
                workspace.ReadFile("loop-a/file.txt", CancellationToken.None).GetAwaiter().GetResult())
        }

    [<Fact>]
    let ``AllowOutsideRoot permits link-through reads and writes but keeps blob-key validation`` () =
        task {
            let root = freshRoot "legate-hd-tests"
            use workspace = bind root true

            let outside = freshRoot "legate-hd-outside"
            let secret = Path.Combine(outside, "secret.txt")
            do! File.WriteAllTextAsync(secret, "outside")

            File.CreateSymbolicLink(Path.Combine(root, "outside-link.txt"), secret)
            |> ignore

            do!
                workspace.WriteFile(
                    "outside-link.txt",
                    Encoding.UTF8.GetBytes "through the link",
                    CancellationToken.None
                )

            let! text = File.ReadAllTextAsync secret
            Assert.Equal("through the link", text)

            use! stream = workspace.ReadFile("outside-link.txt", CancellationToken.None)
            use reader = new StreamReader(stream)
            let! roundTripped = reader.ReadToEndAsync()
            Assert.Equal("through the link", roundTripped)

            // The opt-in lifts containment only: '..' is still not a path.
            assertWorkspaceThrows (fun () ->
                workspace.Exists("../escape.txt", CancellationToken.None).GetAwaiter().GetResult())
        }

    [<Fact>]
    let ``Exec captures output and the exit code`` () =
        task {
            use workspace = workspace ()

            let! result = workspace.Exec("echo legate-echo", Nullable(), null, CancellationToken.None)

            Assert.False(result.TimedOut)
            Assert.Equal(0, result.ExitCode)
            Assert.Contains("legate-echo", result.StandardOutput)
            Assert.NotNull(result.StandardError)
        }

    [<Fact>]
    let ``Exec runs in the bound directory`` () =
        task {
            use workspace = workspace ()

            let root = rootOf workspace

            let dirName =
                match Path.GetFileName(root.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)) with
                | null -> failwith "the bound root has a directory name"
                | name -> name

            let command = if OperatingSystem.IsWindows() then "cd" else "pwd"

            let! result = workspace.Exec(command, Nullable(), null, CancellationToken.None)

            Assert.Equal(0, result.ExitCode)
            Assert.Contains(dirName, result.StandardOutput)
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
    let ``Exec rejects a non-positive timeout`` () =
        task {
            use workspace = workspace ()

            Assert.Throws<ArgumentOutOfRangeException>(fun () ->
                workspace
                    .Exec("echo hi", Nullable(TimeSpan.Zero), null, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore
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
    let ``Dispose is idempotent, keeps files, and members throw after it`` () =
        task {
            let root = freshRoot "legate-hd-tests"
            let workspace = bind root false

            do! workspace.WriteFile("kept.txt", Encoding.UTF8.GetBytes "survives", CancellationToken.None)
            do! workspace.DisposeAsync()
            do! workspace.DisposeAsync()

            // Dispose never deletes the workspace's files.
            Assert.True(File.Exists(Path.Combine(root, "kept.txt")))

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
            let root = freshRoot "legate-hd-tests"

            let options =
                HostDirectoryWorkspaceRuntimeOptions(
                    Root = root,
                    DefaultExecTimeout = Nullable(TimeSpan.FromSeconds 2.)
                )

            let runtime =
                HostDirectoryWorkspaceRuntime(options, null, null) :> IWorkspaceRuntime

            let! bound = runtime.Bind(sessionFor (), null, CancellationToken.None)
            use workspace = bound

            let command =
                if OperatingSystem.IsWindows() then
                    "ping -n 30 127.0.0.1 > nul"
                else
                    "sleep 30"

            let! result = workspace.Exec(command, Nullable(), null, CancellationToken.None)

            Assert.True(result.TimedOut)
        }
