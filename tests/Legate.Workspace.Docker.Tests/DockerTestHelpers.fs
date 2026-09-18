// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker.Tests

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Workspace.Docker
open Xunit

// Shared Docker-free helpers: a session factory mirroring the
// HostDirectory suite shape, a fake CLI runner recording every arg vector
// so the suites assert shapes with no daemon, and a daemon gate that fails
// loudly with the remedy where Docker is unavailable.
//
// xUnit 2.9 has no dynamic-skip mechanism (verified against the 2.9.3
// runner assemblies), so the daemon-backed suite is compiled out on
// Windows (see the fsproj conditions) and the Windows CI leg never
// attempts a container. The Linux CI leg runs everything for real against
// the daemon.

/// A fake docker CLI runner: records every arg vector and answers from
/// the handler. The handler may raise to simulate a CLI start failure.
type internal FakeDockerCommandRunner(handler: IReadOnlyList<string> -> DockerCliResult) =

    let calls = ResizeArray<IReadOnlyList<string>>()

    /// Every arg vector the runner received, in order.
    member _.Calls: IReadOnlyList<IReadOnlyList<string>> =
        calls :> IReadOnlyList<IReadOnlyList<string>>

    /// Answers success with empty streams.
    static member succeed() : FakeDockerCommandRunner =
        FakeDockerCommandRunner(fun _ ->
            {
                ExitCode = 0
                Stdout = ""
                Stderr = ""
                TimedOut = false
            })

    interface IDockerCommandRunner with
        member _.RunAsync(args, _, cancellationToken) =
            cancellationToken.ThrowIfCancellationRequested()
            calls.Add args
            Task.FromResult(handler args)

module internal DockerTestHelpers =

    /// A fresh host root per test, under the system temp directory.
    let freshRoot () =
        let root =
            Path.Combine(Path.GetTempPath(), "legate-docker-tests", Ulid.NewUlid().ToString())

        Directory.CreateDirectory root |> ignore
        root

    /// Options over a fresh root with the test image and no limits.
    let optionsFor root =
        DockerWorkspaceOptions(Root = root, Image = "alpine:3.20")

    /// A session with no workspace binding.
    let sessionFor () =
        {
            Id = SessionId.New()
            Tenant = TenantId.Create "docker-tests"
            AgentId = AgentId.New()
            Title = "docker tests"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    /// Builds a runtime over the fake runner.
    let runtimeWith (options: DockerWorkspaceOptions) (fake: FakeDockerCommandRunner) =
        DockerWorkspaceRuntime(options, fake :> IDockerCommandRunner, null, null)

    /// Asserts the recorded calls contain an invocation whose first
    /// argument is the expected subcommand.
    let assertRan (fake: FakeDockerCommandRunner) (subcommand: string) =
        let found =
            fake.Calls |> Seq.exists (fun args -> args.Count > 0 && args[0] = subcommand)

        Assert.True(found, sprintf "expected a docker %s invocation" subcommand)

    /// Asserts no recorded invocation carries a host fallback: every call
    /// is a docker-native subcommand, never a host shell.
    let assertNoHostFallback (fake: FakeDockerCommandRunner) =
        let allowed =
            Set.ofList
                [
                    "run"
                    "start"
                    "stop"
                    "rm"
                    "inspect"
                    "exec"
                    "info"
                ]

        for args in fake.Calls do
            Assert.True(args.Count > 0, "every docker invocation carries a subcommand")
            Assert.Contains(args[0], allowed)
