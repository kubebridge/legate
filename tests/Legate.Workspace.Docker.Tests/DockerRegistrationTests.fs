// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker.Tests

open System
open System.Collections.Generic
open Legate
open Legate.Workspace.Docker
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options
open Xunit

// UseDocker wiring: the Legate:Workspace:Docker section binds onto
// DockerWorkspaceOptions and the runtime resolves from the container. No
// Docker is needed here: registration never contacts the daemon.
module DockerRegistrationTests =

    /// Builds the application configuration from in-memory pairs.
    let private buildConfiguration (pairs: (string * string) seq) : IConfiguration =
        let keyValues =
            pairs |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))

        ConfigurationBuilder().AddInMemoryCollection(keyValues).Build()

    [<Fact>]
    let ``UseDocker binds the section and registers the runtime`` () =
        let configuration =
            buildConfiguration
                [
                    "Legate:Workspace:Docker:Root", "/tmp/legate-docker"
                    "Legate:Workspace:Docker:Image", "alpine:3.20"
                    "Legate:Workspace:Docker:Network", "none"
                    "Legate:Workspace:Docker:CpuLimit", "1.5"
                    "Legate:Workspace:Docker:MemoryLimit", "512m"
                    "Legate:Workspace:Docker:User", "1000:1000"
                ]

        let services = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(
            services,
            ?configure = Some(fun (builder: LegateBuilder) -> builder.UseDocker(configuration) |> ignore)
        )
        |> ignore

        use provider = services.BuildServiceProvider()

        let options = provider.GetRequiredService<IOptions<DockerWorkspaceOptions>>().Value
        Assert.Equal("/tmp/legate-docker", options.Root)
        Assert.Equal("alpine:3.20", options.Image)
        Assert.Equal("none", options.Network)
        Assert.Equal(Nullable 1.5, options.CpuLimit)
        Assert.Equal("512m", options.MemoryLimit)
        Assert.Equal("1000:1000", options.User)

        Assert.IsType<DockerWorkspaceRuntime>(provider.GetRequiredService<IWorkspaceRuntime>())
        |> ignore

    [<Fact>]
    let ``UseDocker rejects a missing image before registering`` () =
        let configuration =
            buildConfiguration
                [
                    "Legate:Workspace:Docker:Root", "/tmp/legate-docker"
                ]

        let services = ServiceCollection()

        Assert.Throws<InvalidOperationException>(fun () ->
            LegateServiceCollectionExtensions.AddLegate(
                services,
                ?configure = Some(fun (builder: LegateBuilder) -> builder.UseDocker(configuration) |> ignore)
            )
            |> ignore)
        |> ignore

    [<Fact>]
    let ``UseDocker applies the configure callback over the section`` () =
        let configuration =
            buildConfiguration
                [
                    "Legate:Workspace:Docker:Root", "/tmp/legate-docker"
                    "Legate:Workspace:Docker:Image", "alpine:3.20"
                ]

        let services = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(
            services,
            ?configure =
                Some(fun (builder: LegateBuilder) ->
                    builder.UseDocker(
                        configuration,
                        Action<DockerWorkspaceOptions>(fun options -> options.Network <- "none")
                    )
                    |> ignore)
        )
        |> ignore

        use provider = services.BuildServiceProvider()

        let options = provider.GetRequiredService<IOptions<DockerWorkspaceOptions>>().Value
        Assert.Equal("none", options.Network)
