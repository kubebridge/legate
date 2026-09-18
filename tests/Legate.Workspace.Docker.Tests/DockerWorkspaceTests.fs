// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker.Tests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open Legate
open Legate.Workspace.Docker
open Xunit
open DockerTestHelpers

// Workspace tests over the fake runner: exec rides docker exec, file
// operations act through the mounted host directory, and dispose is
// idempotent and destroys only the vehicle. No daemon.
module DockerWorkspaceTests =

    /// Answers container-absent for inspect and success otherwise, so
    /// binds proceed to create.
    let private absent (args: IReadOnlyList<string>) : DockerCliResult =
        if args.Count >= 1 && args[0] = "inspect" then
            {
                ExitCode = 1
                Stdout = ""
                Stderr = "Error: No such container"
                TimedOut = false
            }
        else
            {
                ExitCode = 0
                Stdout = ""
                Stderr = ""
                TimedOut = false
            }

    let private bindAsync (runtime: DockerWorkspaceRuntime) (session: Session) =
        (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

    [<Fact>]
    let ``Exec runs through docker exec with the container and shell`` () =
        task {
            let mutable seen: IReadOnlyList<string> | null = null

            let echo (args: IReadOnlyList<string>) : DockerCliResult =
                if args.Count > 0 && args[0] = "exec" then
                    seen <- args

                    {
                        ExitCode = 0
                        Stdout = "hello\n"
                        Stderr = ""
                        TimedOut = false
                    }
                else
                    absent args

            let fake = FakeDockerCommandRunner(echo)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake
            let session = sessionFor ()

            use! workspace = bindAsync runtime session

            let env = readOnlyDict [ "STAGE", "test" ]

            let! outcome = workspace.Exec("echo hello", Nullable(), env, CancellationToken.None)

            Assert.Equal(0, outcome.ExitCode)
            Assert.Equal("hello\n", outcome.StandardOutput)
            Assert.False(outcome.TimedOut)
            Assert.NotNull(box seen)

            let values =
                match seen with
                | null -> failwith "expected the exec arg vector"
                | captured -> captured |> Seq.toList

            Assert.Equal("exec", values[0])
            Assert.Contains("-e", values)
            Assert.Contains("STAGE=test", values)
            Assert.Contains(runtime.ContainerNameOf session, values)
            Assert.Contains("/bin/sh", values)
            Assert.Contains("-c", values)
            Assert.Contains("echo hello", values)
        }

    [<Fact>]
    let ``Exec maps the command result including timeout`` () =
        task {
            let slow (args: IReadOnlyList<string>) : DockerCliResult =
                if args.Count > 0 && args[0] = "exec" then
                    {
                        ExitCode = 124
                        Stdout = ""
                        Stderr = ""
                        TimedOut = true
                    }
                else
                    absent args

            let fake = FakeDockerCommandRunner(slow)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake

            use! workspace = bindAsync runtime (sessionFor ())

            let! outcome = workspace.Exec("sleep 60", Nullable(TimeSpan.FromSeconds 1.0), null, CancellationToken.None)

            Assert.True(outcome.TimedOut)
        }

    [<Fact>]
    let ``Exec on a missing container throws WorkspaceException`` () =
        task {
            let gone (args: IReadOnlyList<string>) : DockerCliResult =
                if args.Count > 0 && args[0] = "exec" then
                    {
                        ExitCode = 1
                        Stdout = ""
                        Stderr = "Error: No such container: legate-gone"
                        TimedOut = false
                    }
                else
                    absent args

            let fake = FakeDockerCommandRunner(gone)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake

            use! workspace = bindAsync runtime (sessionFor ())

            let! failure =
                Assert.ThrowsAsync<WorkspaceException>(fun () ->
                    workspace.Exec("echo hello", Nullable(), null, CancellationToken.None) :> Threading.Tasks.Task)

            Assert.NotNull(failure)
        }

    [<Fact>]
    let ``File operations round-trip through the mounted host directory`` () =
        task {
            let fake = FakeDockerCommandRunner(absent)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake

            use! workspace = bindAsync runtime (sessionFor ())

            let! missing = workspace.Exists("notes/today.txt", CancellationToken.None)
            Assert.False(missing)

            do! workspace.WriteFile("notes/today.txt", Encoding.UTF8.GetBytes "hello", CancellationToken.None)

            let! present = workspace.Exists("notes/today.txt", CancellationToken.None)
            Assert.True(present)

            let! text =
                task {
                    use! stream = workspace.ReadFile("notes/today.txt", CancellationToken.None)
                    use reader = new StreamReader(stream, Encoding.UTF8)
                    return! reader.ReadToEndAsync()
                }

            Assert.Equal("hello", text)

            let! deleted = workspace.DeleteFile("notes/today.txt", CancellationToken.None)
            Assert.True(deleted)

            let! gone = workspace.Exists("notes/today.txt", CancellationToken.None)
            Assert.False(gone)
        }

    [<Fact>]
    let ``WriteFile leaves the file readable by the container user`` () =
        task {
            let fake = FakeDockerCommandRunner(absent)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake
            let session = sessionFor ()

            use! workspace = bindAsync runtime session

            do! workspace.WriteFile("hello.txt", Encoding.UTF8.GetBytes "round-trip", CancellationToken.None)

            let fullPath = Path.Combine(runtime.DirectoryOf session, "hello.txt")
            Assert.True(File.Exists fullPath)

            if not (OperatingSystem.IsWindows()) then
                let mode = File.GetUnixFileMode fullPath
                Assert.True(mode.HasFlag UnixFileMode.UserRead, "the file stays owner-readable")
                Assert.True(mode.HasFlag UnixFileMode.GroupRead, "the file is group-readable")
                Assert.True(mode.HasFlag UnixFileMode.OtherRead, "the file is other-readable")
        }

    [<Fact>]
    let ``WriteFile leaves created parent directories traversable`` () =
        task {
            let fake = FakeDockerCommandRunner(absent)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake
            let session = sessionFor ()

            use! workspace = bindAsync runtime session

            do! workspace.WriteFile("notes/today.txt", Encoding.UTF8.GetBytes "hello", CancellationToken.None)

            let parent = Path.Combine(runtime.DirectoryOf session, "notes")
            Assert.True(Directory.Exists parent)

            if not (OperatingSystem.IsWindows()) then
                let mode = File.GetUnixFileMode parent

                Assert.True(
                    mode.HasFlag UnixFileMode.OtherExecute,
                    "the created parent is search-traversable for the container user"
                )

        // No absence assertion on OtherRead: the runner umask may already grant o+r,
        // and stripping read bits off ancestors up to / would be unsafe.
        }

    [<Fact>]
    let ``File operations reject paths escaping the workspace`` () =
        task {
            let fake = FakeDockerCommandRunner(absent)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake

            use! workspace = bindAsync runtime (sessionFor ())

            Assert.Throws<WorkspaceException>(fun () ->
                workspace.WriteFile("../escape.txt", [| 1uy |], CancellationToken.None)
                |> ignore)
            |> ignore
        }

    [<Fact>]
    let ``Dispose is idempotent, stops and removes only the vehicle, and keeps the files`` () =
        task {
            let root = freshRoot ()
            let fake = FakeDockerCommandRunner(absent)
            let runtime = runtimeWith (optionsFor root) fake
            let session = sessionFor ()

            let! workspace = bindAsync runtime session

            do! workspace.WriteFile("kept.txt", Encoding.UTF8.GetBytes "kept", CancellationToken.None)

            do! workspace.DisposeAsync()
            do! workspace.DisposeAsync()

            // The vehicle teardown ran exactly once per verb.
            let stops =
                fake.Calls
                |> Seq.filter (fun args -> args.Count > 0 && args[0] = "stop")
                |> Seq.length

            let rms =
                fake.Calls
                |> Seq.filter (fun args -> args.Count > 0 && args[0] = "rm")
                |> Seq.length

            Assert.Equal(1, stops)
            Assert.Equal(1, rms)

            // The workspace's files survive dispose.
            Assert.True(File.Exists(Path.Combine(runtime.DirectoryOf session, "kept.txt")))

            // Every member throws once disposed.
            let! disposed =
                Assert.ThrowsAsync<ObjectDisposedException>(fun () ->
                    workspace.Exec("echo hello", Nullable(), null, CancellationToken.None) :> Threading.Tasks.Task)

            Assert.NotNull(disposed)
        }
