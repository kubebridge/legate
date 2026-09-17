// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker.Tests

open System
open Legate.Workspace.Docker
open Xunit
open DockerTestHelpers

// DockerWorkspaceOptions validation: Docker-free, compiles everywhere.
module DockerWorkspaceOptionsTests =

    [<Fact>]
    let ``Defaults carry no image so they do not validate until the host supplies one`` () =
        let options = DockerWorkspaceOptions(Root = "/tmp/legate-docker")
        Assert.NotNull(options.Validate())

        options.Image <- "alpine:3.20"
        Assert.Null(options.Validate())

    [<Fact>]
    let ``Validate rejects a missing root`` () =
        let options = DockerWorkspaceOptions(Image = "alpine:3.20")
        Assert.NotNull(options.Validate())

    [<Fact>]
    let ``Validate rejects a blank network, user, and memory limit while null stays unset`` () =
        let network =
            DockerWorkspaceOptions(Root = "/tmp/legate-docker", Image = "alpine:3.20")

        network.Network <- " "
        Assert.NotNull(network.Validate())

        let user =
            DockerWorkspaceOptions(Root = "/tmp/legate-docker", Image = "alpine:3.20")

        user.User <- ""
        Assert.NotNull(user.Validate())

        let memory =
            DockerWorkspaceOptions(Root = "/tmp/legate-docker", Image = "alpine:3.20")

        memory.MemoryLimit <- "  "
        Assert.NotNull(memory.Validate())

        let unset =
            DockerWorkspaceOptions(Root = "/tmp/legate-docker", Image = "alpine:3.20")

        Assert.Null(unset.Validate())

    [<Fact>]
    let ``Validate rejects a non-positive CPU limit`` () =
        let zero =
            DockerWorkspaceOptions(Root = "/tmp/legate-docker", Image = "alpine:3.20")

        zero.CpuLimit <- Nullable 0.0
        Assert.NotNull(zero.Validate())

        let negative =
            DockerWorkspaceOptions(Root = "/tmp/legate-docker", Image = "alpine:3.20")

        negative.CpuLimit <- Nullable -1.5
        Assert.NotNull(negative.Validate())

        let positive =
            DockerWorkspaceOptions(Root = "/tmp/legate-docker", Image = "alpine:3.20")

        positive.CpuLimit <- Nullable 1.5
        Assert.Null(positive.Validate())

    [<Fact>]
    let ``Validate rejects a non-positive default exec timeout`` () =
        let dead =
            DockerWorkspaceOptions(Root = "/tmp/legate-docker", Image = "alpine:3.20")

        dead.DefaultExecTimeout <- Nullable TimeSpan.Zero
        Assert.NotNull(dead.Validate())
