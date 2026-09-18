// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker.Tests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Text.Json
open System.Threading
open Legate
open Legate.Workspace.Docker
open Xunit
open DockerTestHelpers

// Daemon-backed suites: the applied image, network, CPU, memory, and user
// are asserted through docker inspect, exec round-trips through docker
// exec, and readiness probes the daemon.
//
// This file compiles on non-Windows only (see the fsproj conditions): the
// Windows CI leg never attempts a container. Where the daemon is
// unavailable the gate fails loudly naming the remedy, never passing
// vacuously.
module DockerTestDaemon =

    let private gate = obj ()

    let mutable private ready = false
    let mutable private failed: string | null = null

    /// The image the suites start every container from.
    let TestImage = "alpine:3.20"

    /// Probes the daemon once: fails loudly with the remedy where Docker
    /// is unavailable instead of passing vacuously.
    let ensureReady () =
        lock gate (fun () ->
            if not (isNull (box failed)) then
                raise (InvalidOperationException(failed))

            if not ready then
                let runner = DockerCommandRunner() :> IDockerCommandRunner

                try
                    let outcome =
                        runner
                            .RunAsync(
                                [| "info" |] :> IReadOnlyList<string>,
                                Nullable(TimeSpan.FromSeconds 30.0),
                                CancellationToken.None
                            )
                            .GetAwaiter()
                            .GetResult()

                    if outcome.ExitCode <> 0 then
                        failed <-
                            sprintf
                                "Docker integration suites need a running Docker daemon; start Docker and re-run. The probe reported: %s"
                                (outcome.Stderr.Trim())

                        raise (InvalidOperationException(failed))
                    else
                        ready <- true
                with
                | :? InvalidOperationException -> reraise ()
                | error ->
                    failed <-
                        sprintf
                            "Docker integration suites need a running Docker daemon; start Docker and re-run. Cause: %s"
                            error.Message

                    raise (InvalidOperationException(failed)))

module DockerIntegrationTests =

    let private optionsForIntegration root =
        let options =
            DockerWorkspaceOptions(Root = root, Image = DockerTestDaemon.TestImage)

        options.Network <- "none"
        options.CpuLimit <- Nullable 0.5
        options.MemoryLimit <- "128m"
        options.User <- "65534:65534"
        options

    let private inspectJson (containerName: string) : JsonDocument =
        let runner = DockerCommandRunner() :> IDockerCommandRunner

        let outcome =
            runner
                .RunAsync(
                    [| "inspect"; containerName |] :> IReadOnlyList<string>,
                    Nullable(TimeSpan.FromSeconds 30.0),
                    CancellationToken.None
                )
                .GetAwaiter()
                .GetResult()

        if outcome.ExitCode <> 0 then
            raise (InvalidOperationException(sprintf "docker inspect failed: %s" (outcome.Stderr.Trim())))

        JsonDocument.Parse outcome.Stdout

    [<Fact>]
    let ``Inspect shows the applied image, network, CPU, memory, and user`` () =
        task {
            DockerTestDaemon.ensureReady ()

            let root = DockerTestHelpers.freshRoot ()
            let runtime = DockerWorkspaceRuntime(optionsForIntegration root, null, null)
            let session = DockerTestHelpers.sessionFor ()

            use! workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

            let containerName = runtime.ContainerNameOf session
            use inspection = inspectJson containerName
            let root = inspection.RootElement.EnumerateArray() |> Seq.head

            Assert.Equal(DockerTestDaemon.TestImage, root.GetProperty("Config").GetProperty("Image").GetString())
            Assert.Equal("none", root.GetProperty("HostConfig").GetProperty("NetworkMode").GetString())
            Assert.Equal(500000000L, root.GetProperty("HostConfig").GetProperty("NanoCpus").GetInt64())
            Assert.Equal(134217728L, root.GetProperty("HostConfig").GetProperty("Memory").GetInt64())
            Assert.Equal("65534:65534", root.GetProperty("Config").GetProperty("User").GetString())

            let mounts = root.GetProperty("Mounts").EnumerateArray() |> Seq.toList

            Assert.Contains(
                mounts,
                (fun (mount: JsonElement) -> mount.GetProperty("Destination").GetString() = "/workspace")
            )

            Assert.Equal("workspace.docker", workspace.Root.RuntimeId)

            do! workspace.DisposeAsync()
        }

    [<Fact>]
    let ``Exec round-trips through docker exec with environment`` () =
        task {
            DockerTestDaemon.ensureReady ()

            let root = DockerTestHelpers.freshRoot ()
            let runtime = DockerWorkspaceRuntime(optionsForIntegration root, null, null)

            use! workspace =
                (runtime :> IWorkspaceRuntime).Bind(DockerTestHelpers.sessionFor (), null, CancellationToken.None)

            do! workspace.WriteFile("hello.txt", Encoding.UTF8.GetBytes "round-trip", CancellationToken.None)

            let! cat = workspace.Exec("cat /workspace/hello.txt", Nullable(), null, CancellationToken.None)

            // The exact OS error decides traversal (search on the mount
            // chain) versus content (read on the file): surface stderr
            // permanently, never just the exit code.
            Assert.True(
                cat.ExitCode = 0,
                sprintf "cat /workspace/hello.txt failed with exit %d; stderr: %s" cat.ExitCode cat.StandardError
            )

            Assert.Equal("round-trip", cat.StandardOutput.Trim())
            Assert.False(cat.TimedOut)

            let env = readOnlyDict [ "LEGATE_PROBE", "probe-value" ]

            let! echoed = workspace.Exec("echo $LEGATE_PROBE", Nullable(), env, CancellationToken.None)
            Assert.Equal(0, echoed.ExitCode)
            Assert.Equal("probe-value", echoed.StandardOutput.Trim())

            do! workspace.DisposeAsync()
        }

    [<Fact>]
    let ``Readiness probes the daemon`` () =
        task {
            DockerTestDaemon.ensureReady ()

            let runtime =
                DockerWorkspaceRuntime(optionsForIntegration (DockerTestHelpers.freshRoot ()), null, null)

            let! readiness = (runtime :> IWorkspaceRuntime).CheckReadiness(CancellationToken.None)
            Assert.True(readiness.IsReady)
        }
