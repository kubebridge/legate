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

// Runtime seam tests over the fake runner: container naming, directory
// binding, readiness, and the no-fallback failure contract. No daemon.
module DockerWorkspaceRuntimeTests =

    /// Answers container-absent for inspect and success otherwise.
    let private handlerForCreate (_: IReadOnlyList<string>) : DockerCliResult =
        {
            ExitCode = 0
            Stdout = ""
            Stderr = ""
            TimedOut = false
        }

    let private absentThenSucceed (args: IReadOnlyList<string>) : DockerCliResult =
        if args.Count >= 1 && args[0] = "inspect" then
            {
                ExitCode = 1
                Stdout = ""
                Stderr = "Error: No such container"
                TimedOut = false
            }
        else
            handlerForCreate args

    [<Fact>]
    let ``ContainerNameOf is session-scoped, stable, and sanitised`` () =
        let runtime =
            runtimeWith (optionsFor (freshRoot ())) (FakeDockerCommandRunner(absentThenSucceed))

        let session = sessionFor ()
        let first = runtime.ContainerNameOf session
        let second = runtime.ContainerNameOf session
        Assert.Equal(first, second)
        Assert.StartsWith("legate-", first)
        Assert.DoesNotContain("/", first)
        Assert.Equal(first, first.ToLowerInvariant())
        Assert.True(first.Length <= 63)

        let other = runtime.ContainerNameOf(sessionFor ())
        Assert.NotEqual<string>(first, other)

    [<Fact>]
    let ``DirectoryOf binds the root itself without a binding`` () =
        let root = freshRoot ()

        let runtime =
            runtimeWith (optionsFor root) (FakeDockerCommandRunner(absentThenSucceed))

        Assert.Equal(Path.GetFullPath root, runtime.DirectoryOf(sessionFor ()))

    [<Fact>]
    let ``DirectoryOf honours a nested binding as a confined sub-path`` () =
        let root = freshRoot ()

        let runtime =
            runtimeWith (optionsFor root) (FakeDockerCommandRunner(absentThenSucceed))

        let bound =
            { sessionFor () with
                WorkspaceBinding = "sessions/alpha"
            }

        Assert.Equal(Path.GetFullPath(Path.Combine(root, "sessions", "alpha")), runtime.DirectoryOf bound)

    [<Fact>]
    let ``A binding that escapes the root throws and starts nothing`` () =
        let fake = FakeDockerCommandRunner(absentThenSucceed)
        let runtime = runtimeWith (optionsFor (freshRoot ())) fake

        let escaping =
            { sessionFor () with
                WorkspaceBinding = "../escape"
            }

        Assert.Throws<WorkspaceException>(fun () -> runtime.DirectoryOf escaping |> ignore)
        |> ignore

        Assert.Empty(fake.Calls)

    [<Fact>]
    let ``The constructor rejects invalid options`` () =
        let bad = DockerWorkspaceOptions(Root = freshRoot ())

        Assert.Throws<ArgumentException>(fun () ->
            DockerWorkspaceRuntime(bad, FakeDockerCommandRunner(absentThenSucceed) :> IDockerCommandRunner, null, null)
            |> ignore)
        |> ignore

    [<Fact>]
    let ``CheckReadiness reports ready when the daemon answers`` () =
        task {
            let runtime =
                runtimeWith (optionsFor (freshRoot ())) (FakeDockerCommandRunner(absentThenSucceed))

            let! readiness = (runtime :> IWorkspaceRuntime).CheckReadiness(CancellationToken.None)
            Assert.True(readiness.IsReady)
            Assert.Null(readiness.Reason)
        }

    [<Fact>]
    let ``CheckReadiness reports unready instead of throwing when the daemon refuses`` () =
        task {
            let refusing (args: IReadOnlyList<string>) : DockerCliResult =
                if args.Count >= 1 && args[0] = "info" then
                    {
                        ExitCode = 1
                        Stdout = ""
                        Stderr = "Cannot connect to the Docker daemon"
                        TimedOut = false
                    }
                else
                    absentThenSucceed args

            let runtime =
                runtimeWith (optionsFor (freshRoot ())) (FakeDockerCommandRunner(refusing))

            let! readiness = (runtime :> IWorkspaceRuntime).CheckReadiness(CancellationToken.None)
            Assert.False(readiness.IsReady)
            Assert.NotNull(readiness.Reason)
        }

    [<Fact>]
    let ``CheckReadiness reports unready when the CLI will not start`` () =
        task {
            let failing (_: IReadOnlyList<string>) : DockerCliResult =
                raise (WorkspaceException("docker", "The workspace could not start the docker CLI."))

            let runtime =
                runtimeWith (optionsFor (freshRoot ())) (FakeDockerCommandRunner(failing))

            let! readiness = (runtime :> IWorkspaceRuntime).CheckReadiness(CancellationToken.None)
            Assert.False(readiness.IsReady)
        }

    [<Fact>]
    let ``Bind reuses a running container without creating one`` () =
        task {
            let running (args: IReadOnlyList<string>) : DockerCliResult =
                if args.Count >= 1 && args[0] = "inspect" && args.Count = 2 then
                    {
                        ExitCode = 0
                        Stdout = "[]"
                        Stderr = ""
                        TimedOut = false
                    }
                elif args.Count >= 2 && args[0] = "inspect" && args[1] = "-f" then
                    {
                        ExitCode = 0
                        Stdout = "true\n"
                        Stderr = ""
                        TimedOut = false
                    }
                else
                    {
                        ExitCode = 0
                        Stdout = ""
                        Stderr = ""
                        TimedOut = false
                    }

            let fake = FakeDockerCommandRunner(running)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake

            use! _workspace = (runtime :> IWorkspaceRuntime).Bind(sessionFor (), null, CancellationToken.None)

            // No run: the running container was reused.
            let ranRun =
                fake.Calls |> Seq.exists (fun args -> args.Count > 0 && args[0] = "run")

            Assert.False(ranRun)
        }

    [<Fact>]
    let ``Bind starts a stopped container`` () =
        task {
            let stopped (args: IReadOnlyList<string>) : DockerCliResult =
                if args.Count >= 1 && args[0] = "inspect" && args.Count = 2 then
                    {
                        ExitCode = 0
                        Stdout = "[]"
                        Stderr = ""
                        TimedOut = false
                    }
                elif args.Count >= 2 && args[0] = "inspect" && args[1] = "-f" then
                    {
                        ExitCode = 0
                        Stdout = "false\n"
                        Stderr = ""
                        TimedOut = false
                    }
                else
                    {
                        ExitCode = 0
                        Stdout = ""
                        Stderr = ""
                        TimedOut = false
                    }

            let fake = FakeDockerCommandRunner(stopped)
            let runtime = runtimeWith (optionsFor (freshRoot ())) fake

            use! _workspace = (runtime :> IWorkspaceRuntime).Bind(sessionFor (), null, CancellationToken.None)

            assertRan fake "start"
        }

    [<Fact>]
    let ``Bind leaves the session directory traversable but not listable by the container user`` () =
        task {
            let root = freshRoot ()
            let fake = FakeDockerCommandRunner(absentThenSucceed)
            let runtime = runtimeWith (optionsFor root) fake
            let session = sessionFor ()

            use! _workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

            let directory = runtime.DirectoryOf session
            Assert.True(Directory.Exists directory)

            if not (OperatingSystem.IsWindows()) then
                let mode = File.GetUnixFileMode directory

                Assert.True(
                    mode.HasFlag UnixFileMode.OtherExecute,
                    "the session directory is search-traversable for the container user"
                )

                Assert.False(mode.HasFlag UnixFileMode.OtherRead, "the session directory stays non-listable")
        }

    [<Fact>]
    let ``The env file is gone after a successful bind`` () =
        task {
            let root = freshRoot ()
            let fake = FakeDockerCommandRunner(absentThenSucceed)
            let runtime = runtimeWith (optionsFor root) fake

            let session = sessionFor ()

            use! _workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

            let leftovers = Directory.GetFiles(runtime.DirectoryOf session, ".legate-env-*")
            Assert.Empty(leftovers)
        }
