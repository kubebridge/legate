// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ActorSystemColdStartTests

open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Xunit

// Cold-start budget and session-router identity for the local actor system
// (issue 30). The children carry identity only: no turn logic lives here,
// so these tests assert same-id/same-child and distinct-id/distinct-child
// plus a bounded stop, and print the measured cold-start milliseconds the
// README records. Akka stays for single-node on these numbers alone; no
// replace-Akka work happens here regardless of outcome.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// Builds default options, optionally tuned before the service sees them.
let private buildOptions (configure: LegateOptions -> unit) : IOptions<LegateOptions> =
    let options = LegateOptions()
    configure options
    OptionsWrapper<LegateOptions>(options) :> IOptions<LegateOptions>

/// Starts a service over default options and returns it running.
let private startService (options: IOptions<LegateOptions>) : LocalActorSystemService =
    let service = LocalActorSystemService(options, TimeProvider.System)
    (service :> IHostedService).StartAsync(CancellationToken.None).GetAwaiter().GetResult()
    service

/// Stops a service, waiting for coordinated shutdown to drain.
let private stopService (service: LocalActorSystemService) : unit =
    (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()

/// Runs one resolve and blocks for the router's reply.
let private resolve (service: LocalActorSystemService) (sessionId: string) =
    service.ResolveSessionAsync(sessionId, CancellationToken.None).GetAwaiter().GetResult()

/// Configures the builder the F# way, mirroring BuilderTests.
let private addLegateFSharp (services: IServiceCollection) (configure: LegateBuilder -> unit) =
    LegateServiceCollectionExtensions.AddLegate(services, ?configure = Some configure)
    |> ignore

/// Registers the three required registrations through the sub-builders,
/// reusing the BuilderTests stubs.
let private registerRequired (builder: LegateBuilder) : unit =
    builder.Llm.AddProvider(BuilderTests.StubLlmProvider()) |> ignore

    builder.Storage.UseSessionStore(InMemorySessionStore(InMemoryDatabase()))
    |> ignore

    builder.Workspace.UseRuntime(BuilderTests.StubWorkspaceRuntime()) |> ignore

/// Builds the Legate configuration section from in-memory pairs.
let private buildSection (pairs: (string * string) seq) : IConfigurationSection =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build().GetSection("Legate")

/// Starts every registered hosted service, the way a host start runs
/// startup validation and options validation.
let private startHosted (provider: IServiceProvider) : unit =
    for service in provider.GetServices<IHostedService>() do
        service.StartAsync(CancellationToken.None).GetAwaiter().GetResult()

/// Stops every registered hosted service, the way a host shutdown drains.
let private stopHosted (provider: IServiceProvider) : unit =
    for service in provider.GetServices<IHostedService>() do
        service.StopAsync(CancellationToken.None).GetAwaiter().GetResult()

/// Picks the local actor system service out of the hosted services.
let private findActorService (provider: IServiceProvider) : LocalActorSystemService =
    provider.GetServices<IHostedService>()
    |> Seq.pick (fun service ->
        match service with
        | :? LocalActorSystemService as actorService -> Some actorService
        | _ -> None)

// ──────────────────────────────────────────────────────────────────────────
// Cold start

[<Fact>]
let ``Cold start creates a local system and prints elapsed milliseconds`` () =
    let stopwatch = Stopwatch.StartNew()
    let service = startService (buildOptions ignore)
    stopwatch.Stop()

    try
        isNull (box service.System) |> should equal false
        isNull (box service.Router) |> should equal false

        printfn
            "legate-local-cold-start-ms: %.1f (hook: %.1f)"
            stopwatch.Elapsed.TotalMilliseconds
            service.LastColdStart.TotalMilliseconds

        // Sanity bound only: the README target is a few hundred
        // milliseconds, but CI scheduling must never flake this gate.
        (service.LastColdStart < TimeSpan.FromSeconds 10.0) |> should equal true
    finally
        stopService service

// ──────────────────────────────────────────────────────────────────────────
// Router identity

[<Fact>]
let ``Router resolves the same child for the same session id`` () =
    let service = startService (buildOptions ignore)

    try
        let first = resolve service "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        let second = resolve service "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        Assert.Same(first, second)
    finally
        stopService service

[<Fact>]
let ``Router resolves distinct children for distinct session ids`` () =
    let service = startService (buildOptions ignore)

    try
        let first = resolve service "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        let second = resolve service "01ARZ3NDEKTSV4RRFFQ69G5FAW"
        Assert.NotSame(first, second)
    finally
        stopService service

[<Fact>]
let ``Resolve rejects a blank session id without touching the router`` () =
    let service = startService (buildOptions ignore)

    try
        let ex = Assert.Throws<ArgumentException>(fun () -> resolve service "  " |> ignore)

        ex.ParamName |> should equal "sessionId"
    finally
        stopService service

// ──────────────────────────────────────────────────────────────────────────
// Shutdown bound

[<Fact>]
let ``Stop completes within ShutdownGraceSeconds`` () =
    let options =
        buildOptions (fun root -> root.Cluster.ShutdownGraceSeconds <- TimeSpan.FromSeconds 5.0)

    let service = startService options
    let stopwatch = Stopwatch.StartNew()
    stopService service
    stopwatch.Stop()

    isNull (box service.System) |> should equal true
    isNull (box service.Router) |> should equal true

    printfn "legate-local-stop-ms: %.1f" stopwatch.Elapsed.TotalMilliseconds

    // The stop path swallows the timeout, so this only fails on a wedged
    // shutdown; the slack over the 5 s grace is CI scheduling headroom.
    (stopwatch.Elapsed < TimeSpan.FromSeconds 20.0) |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// AddLegate wiring

[<Fact>]
let ``AddLegate registers the local actor system hosted service`` () =
    let services = ServiceCollection()
    addLegateFSharp services (fun _ -> ())

    services
    |> Seq.exists (fun descriptor ->
        descriptor.ServiceType = typeof<IHostedService>
        && descriptor.ImplementationType = typeof<LocalActorSystemService>)
    |> should equal true

[<Fact>]
let ``Local mode starts the system through AddLegate`` () =
    let services = ServiceCollection()
    addLegateFSharp services registerRequired
    use provider = services.BuildServiceProvider()

    try
        startHosted provider

        let service = findActorService provider
        isNull (box service.System) |> should equal false
        isNull (box service.Router) |> should equal false
    finally
        stopHosted provider

[<Fact>]
let ``StaticSeeds mode skips the local system through AddLegate`` () =
    let section =
        buildSection
            [
                "Legate:Cluster:Mode", "StaticSeeds"
                "Legate:Cluster:SeedNodes:0", "127.0.0.1:5115"
            ]

    let services = ServiceCollection()

    addLegateFSharp services (fun builder ->
        registerRequired builder
        builder.UseConfiguration(section) |> ignore)

    use provider = services.BuildServiceProvider()

    try
        startHosted provider

        let service = findActorService provider
        isNull (box service.System) |> should equal true
        isNull (box service.Router) |> should equal true
    finally
        stopHosted provider
