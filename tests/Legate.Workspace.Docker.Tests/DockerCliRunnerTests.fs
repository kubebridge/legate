// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker.Tests

open System
open System.Collections.Generic
open System.IO
open System.Threading
open Legate
open Legate.Workspace.Docker
open Xunit
open DockerTestHelpers

// CLI-seam tests: the run vector carries the image, network, CPU, memory,
// user, mount, workdir, and env file; failures map to WorkspaceException
// with bounded stop/rm cleanup and never a host fallback. No daemon.
module DockerCliRunnerTests =

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

    let private runVector (fake: FakeDockerCommandRunner) : IReadOnlyList<string> =
        fake.Calls |> Seq.find (fun args -> args.Count > 0 && args[0] = "run")

    let private hasFlag (vector: IReadOnlyList<string>) (flag: string) (value: string) =
        let values = vector |> Seq.toList

        let rec scan rest =
            match rest with
            | head :: next :: _ when head = flag && next = value -> true
            | _ :: tail -> scan tail
            | [] -> false

        scan values

    [<Fact>]
    let ``Create applies the image, network, CPU, memory, user, mount, and env file`` () =
        task {
            let root = freshRoot ()

            let options = DockerWorkspaceOptions(Root = root, Image = "alpine:3.20")
            options.Network <- "none"
            options.CpuLimit <- Nullable 1.5
            options.MemoryLimit <- "512m"
            options.User <- "1000:1000"

            let fake = FakeDockerCommandRunner(absent)
            let runtime = runtimeWith options fake
            let session = sessionFor ()

            use! _workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

            let vector = runVector fake
            let values = vector |> Seq.toList

            // Detached, session-scoped name.
            Assert.Equal("run", values[0])
            Assert.Equal("-d", values[1])
            Assert.Equal("--name", values[2])
            Assert.Equal(runtime.ContainerNameOf session, values[3])

            Assert.True(hasFlag vector "--network" "none", "the network limit is applied")
            Assert.True(hasFlag vector "--cpus" "1.5", "the CPU limit is applied")
            Assert.True(hasFlag vector "--memory" "512m", "the memory limit is applied")
            Assert.True(hasFlag vector "--user" "1000:1000", "the user is applied")

            Assert.True(
                hasFlag vector "-v" (sprintf "%s:/workspace" (Path.GetFullPath root)),
                "the host dir is mounted"
            )

            Assert.True(hasFlag vector "-w" "/workspace", "the container workdir is set")
            Assert.True(hasFlag vector "--env-file" (values[values.Length - 4]), "the env file is passed")

            // Image then the keep-alive.
            Assert.Equal("alpine:3.20", values[values.Length - 3])
            Assert.Equal("sleep", values[values.Length - 2])
            Assert.Equal("infinity", values[values.Length - 1])

            assertNoHostFallback fake
        }

    [<Fact>]
    let ``Unset limits leave their flags off the run vector`` () =
        task {
            let fake = FakeDockerCommandRunner(absent)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake

            use! _workspace = (runtime :> IWorkspaceRuntime).Bind(sessionFor (), null, CancellationToken.None)

            let vector = runVector fake |> Seq.toList
            Assert.DoesNotContain("--network", vector)
            Assert.DoesNotContain("--cpus", vector)
            Assert.DoesNotContain("--memory", vector)
            Assert.DoesNotContain("--user", vector)
        }

    [<Fact>]
    let ``The env file is owner-only while the container is created`` () =
        task {
            let mutable observed = false
            let mutable ownerOnly = false

            let inspecting (args: IReadOnlyList<string>) : DockerCliResult =
                if args.Count > 0 && args[0] = "run" then
                    let values = args |> Seq.toList
                    let flagAt = values |> List.findIndex (fun value -> value = "--env-file")
                    let path = values[flagAt + 1]
                    observed <- File.Exists path

                    if not (OperatingSystem.IsWindows()) then
                        let mode = File.GetUnixFileMode path
                        let expected = UnixFileMode.UserRead ||| UnixFileMode.UserWrite
                        ownerOnly <- (mode = expected)

                absent args

            let root = freshRoot ()
            let fake = FakeDockerCommandRunner(inspecting)
            let runtime = runtimeWith (optionsFor root) fake

            use! _workspace = (runtime :> IWorkspaceRuntime).Bind(sessionFor (), null, CancellationToken.None)

            Assert.True(observed, "the env file exists while the container is created")

            if not (OperatingSystem.IsWindows()) then
                Assert.True(ownerOnly, "the env file is 0600")
        }

    [<Fact>]
    let ``A failed create throws WorkspaceException and stops and removes the vehicle`` () =
        task {
            let failingCreate (args: IReadOnlyList<string>) : DockerCliResult =
                if args.Count > 0 && args[0] = "run" then
                    {
                        ExitCode = 125
                        Stdout = ""
                        Stderr = "docker: invalid reference format."
                        TimedOut = false
                    }
                else
                    absent args

            let fake = FakeDockerCommandRunner(failingCreate)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake

            let! failure =
                Assert.ThrowsAsync<WorkspaceException>(fun () ->
                    (runtime :> IWorkspaceRuntime).Bind(sessionFor (), null, CancellationToken.None)
                    :> Threading.Tasks.Task)

            Assert.NotNull(failure)

            // Bounded best-effort cleanup ran even on the failure path.
            assertRan fake "stop"
            assertRan fake "rm"
            assertNoHostFallback fake
        }

    [<Fact>]
    let ``A CLI start failure throws WorkspaceException, never a host fallback`` () =
        task {
            let failing (_: IReadOnlyList<string>) : DockerCliResult =
                raise (WorkspaceException("docker", "The workspace could not start the docker CLI."))

            let fake = FakeDockerCommandRunner(failing)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake

            let! failure =
                Assert.ThrowsAsync<WorkspaceException>(fun () ->
                    (runtime :> IWorkspaceRuntime).Bind(sessionFor (), null, CancellationToken.None)
                    :> Threading.Tasks.Task)

            Assert.NotNull(failure)
        }
