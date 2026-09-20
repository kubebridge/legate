// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ClusterActorSystemTests

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Collections.Immutable
open System.Diagnostics
open System.Net
open System.Net.Sockets
open System.Reflection
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.Cluster
open Akka.Configuration
open Akka.FSharp
open Akka.Util
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Diagnostics.HealthChecks
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Microsoft.Extensions.Time.Testing
open Xunit

// Cluster modes and sharding (issue 126): the HOCON builder, the version
// guard, and the hosted service with its shard region and resolver seam.
// The slow runtime tests own the dedicated non-parallel collection:
// localhost remoting plus shard allocation needs generous bounds, and
// the waits below are event-driven with explicit timeouts, never sleeps.

[<CollectionDefinition("LegateCluster", DisableParallelization = true)>]
type LegateClusterCollection() = class end

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// Builds default options, optionally tuned before the service sees them.
let private buildOptions (configure: LegateOptions -> unit) : IOptions<LegateOptions> =
    let options = LegateOptions()
    configure options
    OptionsWrapper<LegateOptions>(options) :> IOptions<LegateOptions>

/// Builds the Legate configuration section from in-memory pairs.
let private buildSection (pairs: (string * string) seq) : IConfigurationSection =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build().GetSection("Legate")

/// Allocates a free loopback port through the OS: closing the listener
/// races in theory, but loopback ports recycle slowly enough for tests.
let private freePort () : int =
    use listener = new TcpListener(IPAddress.Loopback, 0)
    listener.Start()
    (listener.LocalEndpoint :?> IPEndPoint).Port

/// Reads the running system or fails the test with a clear message.
let private requireSystem (service: ClusterActorSystemService) : ActorSystem =
    match box service.System with
    | :? ActorSystem as system -> system
    | _ -> raise (InvalidOperationException("The cluster service has no running system."))

/// Awaits this node's own MemberUp, proving the cluster formed.
let private awaitUp (system: ActorSystem) (bound: TimeSpan) : Task =
    task {
        let reached = TaskCompletionSource<Address>()

        Cluster
            .Get(system)
            .RegisterOnMemberUp(Action(fun () -> reached.TrySetResult(Cluster.Get(system).SelfAddress) |> ignore))

        let! _ = reached.Task.WaitAsync(bound, CancellationToken.None)
        ()
    }

/// Awaits work with a named timeout: slow cluster steps fail loudly
/// instead of hanging the suite.
let private awaitWhat (work: Task<'T>) (bound: TimeSpan) (what: string) : Task<'T> =
    task {
        try
            return! work.WaitAsync(bound, CancellationToken.None)
        with :? TimeoutException ->
            return raise (TimeoutException($"The test timed out waiting for {what}."))
    }

/// Starts a cluster service on the port over tuned options and returns
/// it running. Port 0 binds an ephemeral port.
let private startClusterOn (port: int) (configure: LegateOptions -> unit) : ClusterActorSystemService =
    let service = ClusterActorSystemService(buildOptions configure, TimeProvider.System)
    service.RemotingPort <- port
    (service :> IHostedService).StartAsync(CancellationToken.None).GetAwaiter().GetResult()
    service

/// Stops a cluster service, swallowing teardown noise.
let private stopQuietly (service: ClusterActorSystemService) : unit =
    try
        (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
    with _ ->
        ()

/// Resolves one proxy and blocks for the map's reply.
let private resolve (service: ClusterActorSystemService) (sessionId: string) =
    (service :> ISessionResolver).ResolveSessionAsync(sessionId, CancellationToken.None).GetAwaiter().GetResult()

/// Builds a readiness check over a manually started service: the provider
/// exposes the service as the hosted service the check resolves, and the
/// options select the cluster mode the check evaluates.
let private checkFor (provider: IServiceProvider) (configure: LegateOptions -> unit) : IHealthCheck =
    let options = LegateOptions()
    configure options

    ClusterHealthCheck(OptionsWrapper<LegateOptions>(options) :> IOptions<LegateOptions>, provider) :> IHealthCheck

/// Exposes one started service as the hosted service a readiness check
/// resolves. The caller owns the provider.
let private providerFor (service: ClusterActorSystemService) : ServiceProvider =
    let services = ServiceCollection() :> IServiceCollection
    services.AddSingleton<IHostedService>(service :> IHostedService) |> ignore
    services.BuildServiceProvider()

/// Builds a MemberUp for the stamp through the internal Member
/// constructor: the constructor is internal to Akka, so the guard test
/// reaches it by reflection and fails loudly if its shape ever changes.
let private memberUp (stamp: string) : obj =
    let address = Address.Parse("akka.tcp://legate@127.0.0.1:2551")
    let unique = UniqueAddress(address, 7)
    let appVersion = AppVersion.Create(stamp)

    let flags = BindingFlags.NonPublic ||| BindingFlags.Instance

    let constructor =
        typeof<Member>
            .GetConstructor(
                flags,
                null,
                [|
                    typeof<UniqueAddress>
                    typeof<int>
                    typeof<MemberStatus>
                    typeof<ImmutableHashSet<string>>
                    typeof<AppVersion>
                |],
                null
            )

    if isNull (box constructor) then
        raise (InvalidOperationException("Akka Member constructor shape changed: update the guard test."))

    let roles = ImmutableHashSet.Create<string>()

    let clusterMember =
        match box constructor with
        | :? ConstructorInfo as exact ->
            exact.Invoke(
                [|
                    unique :> obj
                    7 :> obj
                    MemberStatus.Up :> obj
                    roles :> obj
                    appVersion :> obj
                |]
            )
            :?> Member
        | _ -> raise (InvalidOperationException("Akka Member constructor shape changed: update the guard test."))

    ClusterEvent.MemberUp(clusterMember) :> obj

/// Spawns a capture probe: every message lands in the queue.
let private spawnProbe (system: ActorSystem) (name: string) : IActorRef * BlockingCollection<obj> =
    let inbox = new BlockingCollection<obj>()

    let probe =
        spawn system name (fun mailbox ->
            let rec loop () =
                actor {
                    let! message = mailbox.Receive()
                    inbox.Add(message)
                    return! loop ()
                }

            loop ())

    probe, inbox

/// Takes one captured message of the expected type, failing on timeout
/// or on a message of another type.
let private expectMsg<'T> (inbox: BlockingCollection<obj>) (bound: TimeSpan) : 'T =
    let mutable message: obj = Unchecked.defaultof<obj>

    if inbox.TryTake(&message, bound) then
        match message with
        | :? 'T as typed -> typed
        | _ -> raise (InvalidOperationException($"Expected {typeof<'T>.Name} but captured {message.GetType().Name}."))
    else
        raise (TimeoutException($"Timed out waiting for {typeof<'T>.Name}."))

/// Asserts nothing arrives within the window.
let private expectNoMsg (inbox: BlockingCollection<obj>) (window: TimeSpan) : unit =
    let mutable message: obj = Unchecked.defaultof<obj>

    if inbox.TryTake(&message, window) then
        raise (InvalidOperationException($"Expected silence but captured {message.GetType().Name}."))

// ──────────────────────────────────────────────────────────────────────────
// HOCON

[<Fact>]
let ``HOCON publishes provider seeds roles and stamp for StaticSeeds`` () =
    let options = ClusterOptions(Mode = ClusterMode.StaticSeeds)
    options.SeedNodes.Add("127.0.0.1:5115") |> ignore
    options.SeedNodes.Add("127.0.0.1:5116") |> ignore
    options.Roles.Add("session") |> ignore

    let config =
        ConfigurationFactory.ParseString(ClusterActorSystem.buildClusterHocon options 0)

    config.GetString("akka.actor.provider") |> should equal "cluster"

    config.GetString("akka.remote.dot-netty.tcp.hostname")
    |> should equal "127.0.0.1"

    config.GetInt("akka.remote.dot-netty.tcp.port") |> should equal 0

    config.GetStringList("akka.cluster.seed-nodes")
    |> List.ofSeq
    |> should
        equal
        [
            "akka.tcp://legate@127.0.0.1:5115"
            "akka.tcp://legate@127.0.0.1:5116"
        ]

    config.GetStringList("akka.cluster.roles")
    |> List.ofSeq
    |> should equal [ "session" ]

    config.GetString("akka.cluster.app-version") |> should equal "1.1.128"

[<Fact>]
let ``HOCON lists no seeds for Kubernetes and honors the port`` () =
    let options = ClusterOptions(Mode = ClusterMode.Kubernetes)
    options.SeedNodes.Add("127.0.0.1:5115") |> ignore
    options.Roles.Add("session") |> ignore
    options.Roles.Add("api") |> ignore
    options.ShardCount <- 32
    options.ShardHashVersion <- 2

    let config =
        ConfigurationFactory.ParseString(ClusterActorSystem.buildClusterHocon options 1234)

    config.GetString("akka.actor.provider") |> should equal "cluster"
    config.GetInt("akka.remote.dot-netty.tcp.port") |> should equal 1234

    let seeds = config.GetStringList("akka.cluster.seed-nodes") |> List.ofSeq
    seeds.Length |> should equal 0
    seeds |> should be Empty

    config.GetStringList("akka.cluster.roles")
    |> List.ofSeq
    |> should equal [ "session"; "api" ]

    config.GetString("akka.cluster.app-version") |> should equal "1.2.32"

// ──────────────────────────────────────────────────────────────────────────
// SBR HOCON (issue 127)

[<Fact>]
let ``HOCON emits the keep-majority SBR block from options`` () =
    let options = ClusterOptions(Mode = ClusterMode.StaticSeeds)
    options.SeedNodes.Add("127.0.0.1:5115") |> ignore
    options.Roles.Add("session") |> ignore
    options.StableAfter <- TimeSpan.FromSeconds 30.0
    options.DownRemovalMargin <- TimeSpan.FromSeconds 10.0
    options.DownAllWhenUnstable <- Nullable(TimeSpan.FromSeconds 5.0)
    options.JoinTimeout <- TimeSpan.FromSeconds 7.0
    options.HostExitDeadline <- TimeSpan.FromSeconds 90.0

    let raw = ClusterActorSystem.buildClusterHocon options 0
    let config = ConfigurationFactory.ParseString(raw)

    config.GetString("akka.cluster.split-brain-resolver.active-strategy")
    |> should equal "keep-majority"

    config.GetString("akka.cluster.split-brain-resolver.stable-after")
    |> should equal "30s"

    config.GetString("akka.cluster.down-removal-margin") |> should equal "10s"

    config.GetString("akka.cluster.split-brain-resolver.down-all-when-unstable")
    |> should equal "5s"

    config.GetString("akka.cluster.seed-node-timeout") |> should equal "7s"

[<Fact>]
let ``HOCON renders SBR defaults with on and off conventions`` () =
    let options = ClusterOptions(Mode = ClusterMode.StaticSeeds)
    options.SeedNodes.Add("127.0.0.1:5115") |> ignore
    options.Roles.Add("session") |> ignore

    let raw = ClusterActorSystem.buildClusterHocon options 0
    let config = ConfigurationFactory.ParseString(raw)

    config.GetString("akka.cluster.split-brain-resolver.active-strategy")
    |> should equal "keep-majority"

    config.GetString("akka.cluster.split-brain-resolver.stable-after")
    |> should equal "20s"

    config.GetString("akka.cluster.seed-node-timeout") |> should equal "5s"

    // on and off are HOCON booleans, so they are asserted on the raw text.
    raw.Contains("down-removal-margin = off") |> should equal true
    raw.Contains("down-all-when-unstable = on") |> should equal true

[<Fact>]
let ``HOCON maps null and Zero down-all settings`` () =
    let onOptions = ClusterOptions()
    let onRaw = ClusterActorSystem.buildClusterHocon onOptions 0
    onRaw.Contains("down-all-when-unstable = on") |> should equal true

    let offOptions = ClusterOptions()
    offOptions.DownAllWhenUnstable <- Nullable TimeSpan.Zero
    let offRaw = ClusterActorSystem.buildClusterHocon offOptions 0
    offRaw.Contains("down-all-when-unstable = off") |> should equal true

    let durationOptions = ClusterOptions()
    durationOptions.DownAllWhenUnstable <- Nullable(TimeSpan.FromSeconds 12.0)
    let durationRaw = ClusterActorSystem.buildClusterHocon durationOptions 0
    durationRaw.Contains("down-all-when-unstable = 12s") |> should equal true

    let removalOff = ClusterActorSystem.buildClusterHocon (ClusterOptions()) 0
    removalOff.Contains("down-removal-margin = off") |> should equal true

    let removalOn = ClusterOptions(DownRemovalMargin = TimeSpan.FromSeconds 15.0)
    let removalRaw = ClusterActorSystem.buildClusterHocon removalOn 0
    removalRaw.Contains("down-removal-margin = 15s") |> should equal true

[<Fact>]
let ``HOCON never renders the Legate-level exit deadline`` () =
    let options = ClusterOptions(Mode = ClusterMode.StaticSeeds)
    options.SeedNodes.Add("127.0.0.1:5115") |> ignore
    options.HostExitDeadline <- TimeSpan.FromSeconds 90.0

    let raw = ClusterActorSystem.buildClusterHocon options 0

    // Unknown HOCON keys are silently ignored by Akka, so a guessed
    // host-exit key would be a silent no-op: assert it is never emitted.
    raw.Contains("HostExitDeadline") |> should equal false
    raw.ToLowerInvariant().Contains("host-exit") |> should equal false
    raw.ToLowerInvariant().Contains("hostexit") |> should equal false

// ──────────────────────────────────────────────────────────────────────────
// Drain (issue 127)

[<Fact>]
let ``BeginDrain flips the readiness flag once`` () =
    let service = ClusterActorSystemService(buildOptions ignore, TimeProvider.System)
    service.IsDraining |> should equal false
    service.BeginDrain()
    service.IsDraining |> should equal true
    service.BeginDrain()
    service.IsDraining |> should equal true

[<Fact>]
let ``StopAsync fails readiness even with no system`` () : Task =
    task {
        let service = ClusterActorSystemService(buildOptions ignore, TimeProvider.System)
        service.IsDraining |> should equal false
        do! (service :> IHostedService).StopAsync(CancellationToken.None)
        service.IsDraining |> should equal true
    }

[<Fact>]
let ``Drain wait settles immediately when no turns run`` () =
    let fake = FakeTimeProvider()
    let start = fake.GetTimestamp()

    let work =
        ClusterActorSystem.waitForDrainAsync
            (fun _ -> Task.FromResult 0)
            (TimeSpan.FromSeconds 30.0)
            (TimeSpan.FromSeconds 60.0)
            fake
            start
            CancellationToken.None

    work.Wait(TimeSpan.FromSeconds 10.0) |> should equal true

[<Fact>]
let ``Drain wait never exceeds the grace period`` () =
    let fake = FakeTimeProvider()
    let start = fake.GetTimestamp()

    let work =
        ClusterActorSystem.waitForDrainAsync
            (fun _ -> Task.FromResult 1)
            (TimeSpan.FromSeconds 30.0)
            (TimeSpan.FromSeconds 60.0)
            fake
            start
            CancellationToken.None

    work.IsCompleted |> should equal false
    fake.Advance(TimeSpan.FromSeconds 30.0)
    work.Wait(TimeSpan.FromSeconds 10.0) |> should equal true

[<Fact>]
let ``Drain wait never exceeds the host exit deadline`` () =
    let fake = FakeTimeProvider()
    let start = fake.GetTimestamp()

    // The grace is longer than the deadline: the deadline still caps the wait.
    let work =
        ClusterActorSystem.waitForDrainAsync
            (fun _ -> Task.FromResult 1)
            (TimeSpan.FromSeconds 60.0)
            (TimeSpan.FromSeconds 10.0)
            fake
            start
            CancellationToken.None

    work.IsCompleted |> should equal false
    fake.Advance(TimeSpan.FromSeconds 10.0)
    work.Wait(TimeSpan.FromSeconds 10.0) |> should equal true

[<Fact>]
let ``Drain wait wakes early when turns settle`` () =
    let mutable calls = 0

    let count (_: CancellationToken) =
        calls <- calls + 1
        Task.FromResult(if calls < 3 then 1 else 0)

    let fake = FakeTimeProvider()
    let start = fake.GetTimestamp()

    let work =
        ClusterActorSystem.waitForDrainAsync
            count
            (TimeSpan.FromSeconds 30.0)
            (TimeSpan.FromSeconds 60.0)
            fake
            start
            CancellationToken.None

    // Step the virtual clock poll by poll: each advance fires the next
    // due timer, and the bounded wait yields for the loop's continuation
    // to observe the settled count. Never Thread.Sleep.
    for _ in 1..5 do
        fake.Advance(TimeSpan.FromMilliseconds 100.0)
        work.Wait(TimeSpan.FromMilliseconds 500.0) |> ignore

    work.Wait(TimeSpan.FromSeconds 10.0) |> should equal true
    (calls >= 3) |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Slow runtime (dedicated collection)

[<Collection("LegateCluster")>]
type ClusterRuntimeTests() =

    [<Fact>]
    member _.``Local mode starts no cluster system``() : Task =
        task {
            let service = startClusterOn 0 ignore

            try
                isNull (box service.System) |> should equal true
                isNull (box service.Region) |> should equal true

                (fun () -> resolve service "01ARZ3NDEKTSV4RRFFQ69G5FAV" |> ignore)
                |> should throw typeof<InvalidOperationException>
            finally
                stopQuietly service
        }

    [<Fact>]
    member _.``Guard keeps the node on a matching stamp``() : Task =
        task {
            use system =
                ActorSystem.Create(
                    "legate-guard-match",
                    ConfigurationFactory.ParseString("""akka { loglevel = "WARNING" }""")
                )

            try
                let listener, inbox = spawnProbe system "listener"
                let gate = obj ()
                let mutable left = 0

                let guard =
                    spawn
                        system
                        "guard"
                        (ClusterActorSystem.shardingVersionGuard
                            "1.1.128"
                            (fun () -> lock gate (fun () -> left <- left + 1))
                            listener)

                guard.Tell(memberUp "1.1.128")
                expectNoMsg inbox (TimeSpan.FromSeconds(2.0))
                left |> should equal 0
            finally
                system.Terminate().GetAwaiter().GetResult() |> ignore
        }

    [<Fact>]
    member _.``Guard leaves and reports on a stamp mismatch``() : Task =
        task {
            use system =
                ActorSystem.Create(
                    "legate-guard-mismatch",
                    ConfigurationFactory.ParseString("""akka { loglevel = "WARNING" }""")
                )

            try
                let listener, inbox = spawnProbe system "listener"
                let gate = obj ()
                let mutable left = 0

                let guard =
                    spawn
                        system
                        "guard"
                        (ClusterActorSystem.shardingVersionGuard
                            "1.1.128"
                            (fun () -> lock gate (fun () -> left <- left + 1))
                            listener)

                guard.Tell(memberUp "1.2.128")

                let report = expectMsg<ShardingVersionReport> inbox (TimeSpan.FromSeconds(10.0))

                report |> should equal (VersionStampMismatch "1.2.128")
                left |> should equal 1
            finally
                system.Terminate().GetAwaiter().GetResult() |> ignore
        }

    [<Fact>]
    member _.``StaticSeeds self-seeded node resolves through the region``() : Task =
        task {
            let port = freePort ()
            let stopwatch = Stopwatch.StartNew()

            let service =
                startClusterOn port (fun root ->
                    root.Cluster.Mode <- ClusterMode.StaticSeeds
                    root.Cluster.SeedNodes.Add($"127.0.0.1:%d{port}") |> ignore
                    root.Cluster.Roles.Add("session") |> ignore)

            stopwatch.Stop()

            try
                do! awaitUp (requireSystem service) (TimeSpan.FromSeconds(30.0))

                printfn
                    "legate-cluster-cold-start-ms: %.1f (hook: %.1f)"
                    stopwatch.Elapsed.TotalMilliseconds
                    service.LastColdStart.TotalMilliseconds

                isNull (box service.System) |> should equal false
                isNull (box service.Region) |> should equal false

                let first = resolve service "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                let second = resolve service "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                Assert.Same(first, second)

                let other = resolve service "01ARZ3NDEKTSV4RRFFQ69G5FAW"
                Assert.NotSame(first, other)
            finally
                stopQuietly service
        }

    [<Fact>]
    member _.``Kubernetes ephemeral singleton resolves without seeds``() : Task =
        task {
            let service =
                startClusterOn 0 (fun root ->
                    root.Cluster.Mode <- ClusterMode.Kubernetes
                    root.Cluster.Roles.Add("session") |> ignore)

            try
                do! awaitUp (requireSystem service) (TimeSpan.FromSeconds(30.0))

                isNull (box service.System) |> should equal false
                isNull (box service.Region) |> should equal false

                let first = resolve service "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                let second = resolve service "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                Assert.Same(first, second)
            finally
                stopQuietly service
        }

    [<Fact>]
    member _.``Single-node prompt runs the same actor code as Local``() : Task =
        task {
            let port = freePort ()

            let chat =
                new ScriptedChatClient(
                    ResizeArray<ScriptStep>([ ScriptStep.Text "hello back" ]) :> IReadOnlyList<ScriptStep>
                )

            let database = InMemoryDatabase()
            let services = ServiceCollection() :> IServiceCollection

            let section =
                buildSection
                    [
                        "Legate:Cluster:Mode", "StaticSeeds"
                        "Legate:Cluster:SeedNodes:0", $"127.0.0.1:%d{port}"
                        "Legate:Cluster:Roles:0", "session"
                    ]

            LegateServiceCollectionExtensions.AddLegate(
                services,
                ?configure =
                    Some(fun (builder: LegateBuilder) ->
                        builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore

                        builder.Tools.AddSource(new StaticToolSource(ResizeArray<AITool>([]) :> IReadOnlyList<AITool>))
                        |> ignore

                        builder.UseConfiguration(section) |> ignore)
            )
            |> ignore

            services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
            |> ignore

            services.AddSingleton<IChatClient>(chat) |> ignore

            use provider = services.BuildServiceProvider()

            let cluster =
                provider.GetServices<IHostedService>()
                |> Seq.pick (fun service ->
                    match service with
                    | :? ClusterActorSystemService as clustered -> Some clustered
                    | _ -> None)

            cluster.RemotingPort <- port

            let client = provider.GetRequiredService<SessionClient>()

            do! (cluster :> IHostedService).StartAsync(CancellationToken.None)

            try
                do! awaitUp (requireSystem cluster) (TimeSpan.FromSeconds(30.0))

                let! created =
                    SessionClientOperations.OpenSessionAsync(client, AgentId.New(), null, CancellationToken.None)

                let waiter = PromptWaitHubs.GetOrAdd(created.Id).EnqueueSettle()

                let! entry =
                    SessionClientOperations.PromptAsync(
                        client,
                        created.Id,
                        UserMessage.Text "hello",
                        DeliveryMode.Queue,
                        CancellationToken.None
                    )

                entry.Delivery |> should equal DeliveryMode.Queue
                entry.SessionId |> should equal created.Id

                let! result = awaitWhat waiter.Task (TimeSpan.FromSeconds(90.0)) "the turn to settle"
                result.Status |> should equal TurnStatus.Completed
                result.AssistantText |> should equal "hello back"
            finally
                stopQuietly cluster
        }

    [<Fact>]
    member _.``Two nodes route from api to session``() : Task =
        task {
            let portA = freePort ()
            let portB = freePort ()

            let nodeA =
                startClusterOn portA (fun root ->
                    root.Cluster.Mode <- ClusterMode.StaticSeeds
                    root.Cluster.SeedNodes.Add($"127.0.0.1:%d{portA}") |> ignore
                    root.Cluster.Roles.Add("session") |> ignore)

            // Records where each entity actually spawns.
            let placed = ConcurrentDictionary<string, Address>()

            // The entities spawn lazily on first contact, after the
            // factory lands here: no prompt has flowed yet.
            nodeA.SessionEntityFactory <-
                Some(fun sessionId context name ->
                    placed[sessionId] <- Cluster.Get(context.System).SelfAddress
                    LocalActorSystem.identitySpawn sessionId context name)

            let nodeB =
                startClusterOn portB (fun root ->
                    root.Cluster.Mode <- ClusterMode.StaticSeeds
                    root.Cluster.SeedNodes.Add($"127.0.0.1:%d{portA}") |> ignore
                    root.Cluster.Roles.Add("api") |> ignore)

            try
                do! awaitUp (requireSystem nodeA) (TimeSpan.FromSeconds(30.0))
                do! awaitUp (requireSystem nodeB) (TimeSpan.FromSeconds(30.0))

                let sessionId = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                let proxy = resolve nodeB sessionId

                use askCts = new CancellationTokenSource(TimeSpan.FromSeconds(60.0))

                // The wire-safe resolve marker: a bare string crosses
                // remoting without a custom serializer and answers the
                // entity's child.
                let! child =
                    awaitWhat
                        (proxy.Ask<IActorRef>(sessionId, askCts.Token))
                        (TimeSpan.FromSeconds(60.0))
                        "the cross-node resolve"

                let expected = Cluster.Get(requireSystem nodeA).SelfAddress.ToString()

                child.Path.Address.ToString() |> should equal expected
                placed.ContainsKey(sessionId) |> should equal true
                placed[sessionId].ToString() |> should equal expected
            finally
                stopQuietly nodeB
                stopQuietly nodeA
        }

    [<Fact>]
    member _.``Facade wires the entity factory on the mode-active service``() =
        let chat =
            new ScriptedChatClient(ResizeArray<ScriptStep>([ ScriptStep.Text "done" ]) :> IReadOnlyList<ScriptStep>)

        let createServices (section: IConfigurationSection) : IServiceCollection =
            let database = InMemoryDatabase()
            let services = ServiceCollection() :> IServiceCollection

            LegateServiceCollectionExtensions.AddLegate(
                services,
                ?configure =
                    Some(fun (builder: LegateBuilder) ->
                        builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore

                        builder.Tools.AddSource(new StaticToolSource(ResizeArray<AITool>([]) :> IReadOnlyList<AITool>))
                        |> ignore

                        builder.UseConfiguration(section) |> ignore)
            )
            |> ignore

            services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
            |> ignore

            services.AddSingleton<IChatClient>(chat) |> ignore
            services

        let pickLocal (provider: IServiceProvider) : LocalActorSystemService =
            provider.GetServices<IHostedService>()
            |> Seq.pick (fun service ->
                match service with
                | :? LocalActorSystemService as local -> Some local
                | _ -> None)

        let pickCluster (provider: IServiceProvider) : ClusterActorSystemService =
            provider.GetServices<IHostedService>()
            |> Seq.pick (fun service ->
                match service with
                | :? ClusterActorSystemService as clustered -> Some clustered
                | _ -> None)

        use localProvider = (createServices (buildSection [])).BuildServiceProvider()
        localProvider.GetRequiredService<SessionClient>() |> ignore
        (pickLocal localProvider).SessionChildFactory.IsSome |> should equal true
        (pickCluster localProvider).SessionEntityFactory.IsNone |> should equal true

        use clusterProvider =
            (createServices (
                buildSection
                    [
                        "Legate:Cluster:Mode", "StaticSeeds"
                        "Legate:Cluster:SeedNodes:0", "127.0.0.1:5115"
                        "Legate:Cluster:Roles:0", "session"
                    ]
            ))
                .BuildServiceProvider()

        clusterProvider.GetRequiredService<SessionClient>() |> ignore
        (pickLocal clusterProvider).SessionChildFactory.IsNone |> should equal true
        (pickCluster clusterProvider).SessionEntityFactory.IsSome |> should equal true

    [<Fact>]
    member _.``AddLegate registers the cluster actor system hosted service``() =
        let services = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(services, ?configure = Some(fun _ -> ()))
        |> ignore

        services
        |> Seq.exists (fun descriptor ->
            descriptor.ServiceType = typeof<IHostedService>
            && descriptor.ImplementationType = typeof<ClusterActorSystemService>)
        |> should equal true

    [<Fact>]
    member _.``Single-node Up is Healthy``() : Task =
        task {
            let port = freePort ()

            let service =
                startClusterOn port (fun root ->
                    root.Cluster.Mode <- ClusterMode.StaticSeeds
                    root.Cluster.SeedNodes.Add($"127.0.0.1:%d{port}") |> ignore
                    root.Cluster.Roles.Add("session") |> ignore)

            try
                do! awaitUp (requireSystem service) (TimeSpan.FromSeconds(30.0))

                use provider = providerFor service

                let check =
                    checkFor provider (fun root -> root.Cluster.Mode <- ClusterMode.StaticSeeds)

                let! result =
                    awaitWhat
                        (check.CheckHealthAsync(HealthCheckContext(), CancellationToken.None))
                        (TimeSpan.FromSeconds(30.0))
                        "single-node readiness"

                result.Status |> should equal HealthStatus.Healthy
            finally
                stopQuietly service
        }

    [<Fact>]
    member _.``Two-node join is Healthy on both``() : Task =
        task {
            let portA = freePort ()
            let portB = freePort ()

            let nodeA =
                startClusterOn portA (fun root ->
                    root.Cluster.Mode <- ClusterMode.StaticSeeds
                    root.Cluster.SeedNodes.Add($"127.0.0.1:%d{portA}") |> ignore
                    root.Cluster.Roles.Add("session") |> ignore)

            let nodeB =
                startClusterOn portB (fun root ->
                    root.Cluster.Mode <- ClusterMode.StaticSeeds
                    root.Cluster.SeedNodes.Add($"127.0.0.1:%d{portA}") |> ignore
                    root.Cluster.Roles.Add("session") |> ignore)

            try
                do! awaitUp (requireSystem nodeA) (TimeSpan.FromSeconds(30.0))
                do! awaitUp (requireSystem nodeB) (TimeSpan.FromSeconds(30.0))

                use providerA = providerFor nodeA
                use providerB = providerFor nodeB

                let checkA =
                    checkFor providerA (fun root -> root.Cluster.Mode <- ClusterMode.StaticSeeds)

                let checkB =
                    checkFor providerB (fun root -> root.Cluster.Mode <- ClusterMode.StaticSeeds)

                let! resultA =
                    awaitWhat
                        (checkA.CheckHealthAsync(HealthCheckContext(), CancellationToken.None))
                        (TimeSpan.FromSeconds(30.0))
                        "node A readiness after join"

                let! resultB =
                    awaitWhat
                        (checkB.CheckHealthAsync(HealthCheckContext(), CancellationToken.None))
                        (TimeSpan.FromSeconds(30.0))
                        "node B readiness after join"

                resultA.Status |> should equal HealthStatus.Healthy
                resultB.Status |> should equal HealthStatus.Healthy
            finally
                stopQuietly nodeB
                stopQuietly nodeA
        }

    [<Fact>]
    member _.``Drain flips a live node Unhealthy``() : Task =
        task {
            let port = freePort ()

            let service =
                startClusterOn port (fun root ->
                    root.Cluster.Mode <- ClusterMode.StaticSeeds
                    root.Cluster.SeedNodes.Add($"127.0.0.1:%d{port}") |> ignore
                    root.Cluster.Roles.Add("session") |> ignore)

            try
                do! awaitUp (requireSystem service) (TimeSpan.FromSeconds(30.0))

                use provider = providerFor service

                let check =
                    checkFor provider (fun root -> root.Cluster.Mode <- ClusterMode.StaticSeeds)

                let! before =
                    awaitWhat
                        (check.CheckHealthAsync(HealthCheckContext(), CancellationToken.None))
                        (TimeSpan.FromSeconds(30.0))
                        "readiness before drain"

                before.Status |> should equal HealthStatus.Healthy

                // The first drain level fails readiness before any wait
                // starts: no restart, no gossip timing involved.
                service.BeginDrain()

                let! after =
                    awaitWhat
                        (check.CheckHealthAsync(HealthCheckContext(), CancellationToken.None))
                        (TimeSpan.FromSeconds(30.0))
                        "readiness after drain"

                after.Status |> should equal HealthStatus.Unhealthy
            finally
                stopQuietly service
        }

    [<Fact>]
    member _.``Drain then stop completes within the exit deadline``() : Task =
        task {
            let port = freePort ()

            let service =
                startClusterOn port (fun root ->
                    root.Cluster.Mode <- ClusterMode.StaticSeeds
                    root.Cluster.SeedNodes.Add($"127.0.0.1:%d{port}") |> ignore
                    root.Cluster.Roles.Add("session") |> ignore
                    root.Cluster.ShutdownGraceSeconds <- TimeSpan.FromSeconds 2.0
                    root.Cluster.HostExitDeadline <- TimeSpan.FromSeconds 15.0)

            try
                do! awaitUp (requireSystem service) (TimeSpan.FromSeconds(30.0))

                service.BeginDrain()
                service.IsDraining |> should equal true

                let stopwatch = Stopwatch.StartNew()

                let stop =
                    task {
                        do! (service :> IHostedService).StopAsync(CancellationToken.None)
                        return ()
                    }

                do! awaitWhat stop (TimeSpan.FromSeconds(15.0)) "drain then stop"

                stopwatch.Stop()
                (stopwatch.Elapsed < TimeSpan.FromSeconds 15.0) |> should equal true
            finally
                stopQuietly service
        }
