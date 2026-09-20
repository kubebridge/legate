// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Immutable
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.Cluster
open Akka.Cluster.Sharding
open Akka.Configuration
open Akka.Event
open Akka.FSharp
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options

// Clustered actor systems for StaticSeeds and Kubernetes modes. A
// singleton hosted service creates one Akka.NET ActorSystem with
// remoting+cluster HOCON, joins (seed nodes, or itself as a
// singleton until issue 138 ships the Akka.Management bootstrap),
// starts one session shard region restricted to the session role, and
// resolves sessions through per-id proxies that forward into the
// region preserving the sender, so Ask round-trips to the entity and
// back. Entities are thin delegators spawning the same SessionActor
// through the facade-wired factory, so the actor code is shared with
// Local mode. A version guard fails closed on a peer stamp mismatch.
// Local mode is untouched: this service starts nothing there.
// Kubernetes joins nothing until issue 138: SeedNodes is ignored and
// the node runs as a singleton until the bootstrap lands.

// ──────────────────────────────────────────────────────────────────────────
// Guard protocol

/// What the sharding version guard reports. A match stays silent; a
/// mismatch leaves the cluster first and then reports the peer stamp.
type internal ShardingVersionReport =

    /// A peer member came Up with a different shard stamp. The node has
    /// already left; this only carries the offending stamp.
    | VersionStampMismatch of peerStamp: string

// ──────────────────────────────────────────────────────────────────────────
// Construction

/// Builds the clustered actor system: HOCON, guard, entities, proxies.
/// Internal so no Akka type ever crosses the public API.
module internal ClusterActorSystem =

    /// The actor system name, stable so logs and paths read the same in
    /// every host and mode.
    let systemName = "legate"

    /// Proxy actor name prefix; the counter suffix keeps names unique.
    /// Names never embed the session id; identity lives in the map.
    let proxyPrefix = "legate-cluster-proxy-"

    /// The single sharding version guard under the system guardian.
    let guardName = "legate-sharding-guard"

    /// The guard's mismatch log listener under the system guardian.
    let listenerName = "legate-sharding-guard-listener"

    /// How long a first resolve waits for the entity to answer its
    /// warm-up marker before the resolve fails. Shard allocation (the
    /// coordinator singleton plus shard handoff) dominates a cold
    /// region, so this is cluster-sized; later resolves answer from the
    /// map. The caller's token still cancels earlier.
    let resolveTimeout = TimeSpan.FromSeconds 60.0

    /// The remoting bind hostname. Loopback keeps the default path and
    /// the containerless tests hermetic; stable cross-host bind knobs
    /// arrive with the production bootstrap work.
    let remotingHostname = "127.0.0.1"

    /// Formats a positive TimeSpan as HOCON duration: whole seconds as
    /// Ns, anything smaller as Nms. Validation keeps SBR knobs in range;
    /// this only renders what Validate already accepted.
    /// <param name="value">The duration to render.</param>
    /// <returns>The HOCON duration.</returns>
    let formatHoconDuration (value: TimeSpan) : string =
        if value.Ticks % TimeSpan.TicksPerSecond = 0L then
            $"%d{int64 value.TotalSeconds}s"
        else
            $"%d{int64 value.TotalMilliseconds}ms"

    /// Renders DownRemovalMargin: Zero emits off, anything else emits the
    /// duration.
    /// <param name="value">The removal margin.</param>
    /// <returns>The HOCON value.</returns>
    let downRemovalMarginHocon (value: TimeSpan) : string =
        if value = TimeSpan.Zero then
            "off"
        else
            formatHoconDuration value

    /// Renders DownAllWhenUnstable: null emits on, Zero emits off, anything
    /// else emits the duration.
    /// <param name="value">The down-all setting, or null for on.</param>
    /// <returns>The HOCON value.</returns>
    let downAllWhenUnstableHocon (value: Nullable<TimeSpan>) : string =
        if not value.HasValue then "on"
        elif value.Value = TimeSpan.Zero then "off"
        else formatHoconDuration value.Value

    /// Builds remoting+cluster HOCON from the cluster options. StaticSeeds
    /// lists SeedNodes as seed-nodes; Kubernetes lists none (SeedNodes is
    /// ignored) and runs as a singleton until issue 138. Both publish
    /// the version stamp as the member app-version and the node roles. The
    /// versioned envelope wiring (issue 130) rides along: the DTO
    /// serializer, its bindings for the actor, router, and entity protocol
    /// messages, and the global wire maximum from MaxWirePayloadBytes.
    /// The keep-majority split-brain resolver block is bound from options:
    /// StableAfter maps to split-brain-resolver.stable-after,
    /// DownRemovalMargin (Zero means off) maps to down-removal-margin,
    /// DownAllWhenUnstable (null means on, Zero means off) maps to
    /// split-brain-resolver.down-all-when-unstable, and JoinTimeout maps
    /// to seed-node-timeout. HostExitDeadline is a Legate-level StopAsync
    /// cap and is never emitted as HOCON.
    /// <param name="options">The cluster options. Must not be null.</param>
    /// <param name="port">The remoting port, or 0 for an ephemeral port.</param>
    /// <returns>The cluster HOCON.</returns>
    let buildClusterHocon (options: ClusterOptions) (port: int) : string =
        ArgumentNullException.ThrowIfNull(options)

        let seeds =
            match options.Mode with
            | ClusterMode.StaticSeeds when not (isNull (box options.SeedNodes)) ->
                options.SeedNodes
                |> Seq.map (fun raw -> $"\"akka.tcp://%s{systemName}@%s{raw.Trim()}\"")
                |> String.concat ", "
            | _ -> ""

        let roles =
            if isNull (box options.Roles) then
                ""
            else
                options.Roles
                |> Seq.map (fun raw -> $"\"%s{raw.Trim()}\"")
                |> String.concat ", "

        let stamp = SessionSharding.versionStamp options.ShardHashVersion options.ShardCount
        let wire = WireSerialization.hoconFragment (max 1 options.MaxWirePayloadBytes)
        let stableAfter = formatHoconDuration options.StableAfter
        let removalMargin = downRemovalMarginHocon options.DownRemovalMargin
        let downAll = downAllWhenUnstableHocon options.DownAllWhenUnstable
        let joinTimeout = formatHoconDuration options.JoinTimeout

        $"""akka {{
  actor {{
    provider = "cluster"
  }}
  loglevel = "WARNING"
  stdout-loglevel = "WARNING"
  remote.dot-netty.tcp {{
    hostname = "%s{remotingHostname}"
    port = %d{port}
  }}
  cluster {{
    seed-nodes = [%s{seeds}]
    roles = [%s{roles}]
    app-version = "%s{stamp}"
    seed-node-timeout = %s{joinTimeout}
    down-removal-margin = %s{removalMargin}
    split-brain-resolver {{
      active-strategy = keep-majority
      stable-after = %s{stableAfter}
      down-all-when-unstable = %s{downAll}
    }}
  }}
  coordinated-shutdown {{
    run-by-actor-system-terminate = on
    run-by-clr-shutdown-hook = off
  }}
}}
{wire}"""

    /// Parses SeedNodes entries in host:port form into cluster addresses.
    /// <param name="options">The cluster options. Must not be null.</param>
    /// <returns>The seed addresses.</returns>
    let seedAddresses (options: ClusterOptions) : Address list =
        ArgumentNullException.ThrowIfNull(options)

        let seeds =
            if isNull (box options.SeedNodes) then
                Seq.empty
            else
                options.SeedNodes :> string seq

        seeds
        |> Seq.mapi (fun index raw ->
            let text = if isNull (box raw) then "" else raw.Trim()
            let parts = text.Split(':')

            if parts.Length <> 2 then
                raise (
                    InvalidOperationException($"SeedNodes[%d{index}] must be a host:port value, but was '%s{text}'.")
                )

            let mutable port = 0

            if not (Int32.TryParse(parts[1], &port)) then
                raise (
                    InvalidOperationException($"SeedNodes[%d{index}] must be a host:port value, but was '%s{text}'.")
                )

            Address("akka.tcp", systemName, parts[0], Nullable<int>(port)))
        |> List.ofSeq

    /// The sharding version guard: watches MemberUp and fails closed on
    /// a peer stamp mismatch (leave first so a misconfigured node never
    /// serves traffic, then report the stamp). A match stays silent.
    /// <param name="expectedStamp">This node's shard stamp.</param>
    /// <param name="leaveSelf">Leaves the cluster (the fail-closed effect).</param>
    /// <param name="listener">Receives the mismatch report.</param>
    /// <param name="mailbox">The guard mailbox.</param>
    /// <returns>The guard actor computation.</returns>
    let shardingVersionGuard
        (expectedStamp: string)
        (leaveSelf: unit -> unit)
        (listener: IActorRef)
        (mailbox: Actor<obj>)
        =
        ArgumentNullException.ThrowIfNull(leaveSelf)
        ArgumentNullException.ThrowIfNull(listener)

        if String.IsNullOrWhiteSpace expectedStamp then
            raise (ArgumentException("Expected stamp must be a non-empty string.", nameof expectedStamp))

        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | :? ClusterEvent.MemberUp as up ->
                    let peerStamp = up.Member.AppVersion.ToString()

                    if not (String.Equals(peerStamp, expectedStamp, StringComparison.Ordinal)) then
                        leaveSelf ()
                        listener <! VersionStampMismatch peerStamp

                    return! loop ()
                | _ ->
                    mailbox.Unhandled(message)
                    return! loop ()
            }

        loop ()

    /// Logs guard mismatch reports. The leave already failed the node
    /// closed; this only keeps the stamp visible in the logs.
    /// <param name="mailbox">The listener mailbox.</param>
    /// <returns>The listener actor computation.</returns>
    let versionMismatchLog (mailbox: Actor<obj>) =
        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | :? ShardingVersionReport as report ->
                    match report with
                    | VersionStampMismatch peerStamp ->
                        mailbox.Log.Value.Warning(
                            "Legate sharding version mismatch: peer stamp {0} does not match this node; leaving the cluster.",
                            peerStamp
                        )
                | _ -> mailbox.Unhandled(message)

                return! loop ()
            }

        loop ()

    /// The shard entity: a thin delegator spawning one session child for
    /// its entity id and forwarding everything to it with the sender
    /// preserved. A bare string equal to the entity id is the wire-safe
    /// resolve marker (strings cross remoting without a custom
    /// serializer): it answers the child to the sender. Every other
    /// message is session traffic (the suspendable protocol the client
    /// speaks, today and for future messages) and forwards to the
    /// child, so the actor code stays shared with Local mode.
    /// <param name="entityId">The session id this entity hosts.</param>
    /// <param name="spawnSession">Spawns the session child.</param>
    /// <param name="mailbox">The entity mailbox.</param>
    /// <returns>The entity actor computation.</returns>
    let private sessionEntityBehavior
        (entityId: string)
        (spawnSession: string -> IActorContext -> string -> IActorRef)
        (mailbox: Actor<obj>)
        =
        let ensure (child: IActorRef option) : IActorRef =
            match child with
            | Some live -> live
            | None -> spawnSession entityId mailbox.Context "session"

        let rec loop (child: IActorRef option) =
            actor {
                let! message = mailbox.Receive()

                match message with
                | :? string as marker when marker = entityId ->
                    let live = ensure child
                    mailbox.Sender() <! live
                    return! loop (Some live)
                | _ ->
                    let live = ensure child
                    live.Tell(message, mailbox.Sender())
                    return! loop (Some live)
            }

        loop None

    /// Lifts the F# entity behavior into Props. Sharding owns entity
    /// lifecycles through Props, so the computation is lifted exactly
    /// like Akka.FSharp.spawnOpt lifts it; the behavior itself stays in
    /// actor computation style and shared SessionActor code.
    /// <param name="entityId">The session id the entity hosts.</param>
    /// <param name="spawnSession">Spawns the session child.</param>
    /// <returns>The entity Props.</returns>
    let entityProps (entityId: string) (spawnSession: string -> IActorContext -> string -> IActorRef) : Props =
        ArgumentNullException.ThrowIfNull(spawnSession)

        if String.IsNullOrWhiteSpace entityId then
            raise (ArgumentException("Entity id must be a non-empty string.", nameof entityId))

        Props.Create(
            Linq.Expression.ToExpression(fun () -> new FunActor<obj, unit>(sessionEntityBehavior entityId spawnSession))
        )

    /// The drain poll cadence: how often the stop path re-reads the
    /// running-turn count while waiting. Short enough that a settled
    /// drain returns promptly, long enough to avoid hot-polling the
    /// store; the grace and deadline caps bound the total wait either
    /// way.
    let drainPollInterval = TimeSpan.FromMilliseconds 100.0

    /// Waits for running turns to settle: polls the count until it reads
    /// zero, the grace elapses, or the deadline measured from the stop
    /// start elapses, whichever comes first. Cancellation abandons the
    /// wait so shutdown proceeds. Store failures propagate to the
    /// caller; only cancellation is absorbed here.
    /// <param name="countRunning">Reads the running-turn count.</param>
    /// <param name="grace">The running-turn wait bound.</param>
    /// <param name="deadline">The total stop bound from the stop start.</param>
    /// <param name="timeProvider">The clock waits and bounds run on.</param>
    /// <param name="stopStartTimestamp">The stop-start timestamp.</param>
    /// <param name="cancellationToken">Abandons the wait.</param>
    /// <returns>The drain wait task.</returns>
    let waitForDrainAsync
        (countRunning: CancellationToken -> Task<int>)
        (grace: TimeSpan)
        (deadline: TimeSpan)
        (timeProvider: TimeProvider)
        (stopStartTimestamp: int64)
        (cancellationToken: CancellationToken)
        : Task =
        ArgumentNullException.ThrowIfNull(countRunning)
        ArgumentNullException.ThrowIfNull(timeProvider)

        task {
            let graceStart = timeProvider.GetTimestamp()
            let mutable settled = false

            while not settled do
                let graceElapsed = timeProvider.GetElapsedTime graceStart
                let totalElapsed = timeProvider.GetElapsedTime stopStartTimestamp

                if totalElapsed >= deadline then
                    settled <- true
                elif graceElapsed >= grace then
                    settled <- true
                else
                    let! running = countRunning cancellationToken

                    if running <= 0 then
                        settled <- true
                    else
                        let remainingGrace = grace - graceElapsed
                        let remainingDeadline = deadline - totalElapsed

                        let remaining =
                            if remainingGrace < remainingDeadline then
                                remainingGrace
                            else
                                remainingDeadline

                        let wait =
                            if remaining < drainPollInterval then
                                remaining
                            else
                                drainPollInterval

                        if wait <= TimeSpan.Zero then
                            settled <- true
                        else
                            try
                                do! Task.Delay(wait, timeProvider, cancellationToken)
                            with :? OperationCanceledException ->
                                settled <- true
        }

    /// A per-session proxy: forwards everything into the region wrapped
    /// in the session envelope, preserving the sender so Ask round-trips
    /// through the entity and back to the asker.
    /// <param name="sessionId">The session id to envelope with.</param>
    /// <param name="region">The shard region (or proxy) actor.</param>
    /// <param name="mailbox">The proxy mailbox.</param>
    /// <returns>The proxy actor computation.</returns>
    let clusterProxy (sessionId: string) (region: IActorRef) (mailbox: Actor<obj>) =
        let rec loop () =
            actor {
                let! message = mailbox.Receive()
                region.Tell(ShardingEnvelope(sessionId, message), mailbox.Sender())
                return! loop ()
            }

        loop ()

// ──────────────────────────────────────────────────────────────────────────
// Hosted service

/// Singleton hosted service owning the clustered actor system. Starts
/// only in StaticSeeds and Kubernetes cluster modes; Local mode
/// resolves no system and no region. Stop fails readiness first through
/// BeginDrain, waits for running turns up to
/// <c>Cluster:ShutdownGraceSeconds</c>, then runs coordinated shutdown
/// with the whole stop hard-capped by <c>Cluster:HostExitDeadline</c>.
type internal ClusterActorSystemService(options: IOptions<LegateOptions>, timeProvider: TimeProvider) as this =

    do ArgumentNullException.ThrowIfNull(options)
    do ArgumentNullException.ThrowIfNull(timeProvider)

    let mutable systemOpt: Akka.Actor.ActorSystem option = None
    let mutable regionOpt: IActorRef option = None
    let mutable lastColdStart: TimeSpan = TimeSpan.Zero
    let gate = obj ()
    let mutable proxies: Map<string, Task<IActorRef>> = Map.empty
    let mutable nextProxy = 0
    let mutable draining = false

    /// The running actor system, or null when the host runs Local mode
    /// or has not started.
    member _.System: Akka.Actor.ActorSystem | null =
        match systemOpt with
        | Some created -> created
        | None -> null

    /// The session shard region (or proxy on role-less nodes), or null
    /// when the host runs Local mode or has not started.
    member _.Region: IActorRef | null =
        match regionOpt with
        | Some regionRef -> regionRef
        | None -> null

    /// How long the last cluster-mode StartAsync system creation took,
    /// measured on the injected clock; TimeSpan.Zero when never started.
    member _.LastColdStart: TimeSpan = lastColdStart

    /// The remoting bind port, or 0 for an ephemeral port. Seeded tests
    /// set a free loopback port before StartAsync so seed entries can
    /// name it; hosts bind ephemeral ports until a stable-port knob
    /// lands. Must not be negative.
    member val RemotingPort: int = 0 with get, set

    /// The session store polled for running turns during the drain wait,
    /// or None when no store is wired (the drain wait then observes zero
    /// running turns). The session client facade wires this alongside the
    /// entity factory; tests set it directly. Set before StartAsync.
    member val SessionStore: ISessionStore option = None with get, set

    /// Marks the node as draining. Readiness flips Unhealthy the moment
    /// this runs, before any wait starts, so the orchestrator stops
    /// routing to the node while running turns settle. Idempotent.
    member this.BeginDrain() : unit = lock gate (fun () -> draining <- true)

    /// Whether BeginDrain has run. The readiness health check reads this.
    member _.IsDraining: bool = lock gate (fun () -> draining)

    /// Spawns the session actor child for a newly hosted entity id.
    /// None keeps the legacy identity-only children; Some wires the
    /// SessionActor state machine (see
    /// <see cref="M:Legate.SessionActor.spawnFactory" />). The session
    /// client facade owns setting this once it can supply the store and
    /// turn runner. Read per entity instantiation, so setting it before
    /// the first prompt is enough. Set before StartAsync.
    member val SessionEntityFactory: (string -> IActorContext -> string -> IActorRef) option = None with get, set

    /// Reads the current entity spawn: the facade-wired factory, or the
    /// legacy identity spawn until the facade configures one.
    /// <returns>The spawn for newly hosted entities.</returns>
    member private this.currentSpawn() : (string -> IActorContext -> string -> IActorRef) =
        match this.SessionEntityFactory with
        | Some factory -> factory
        | None -> LocalActorSystem.identitySpawn

    /// Resolves the session proxy for a session id: the same id returns
    /// the same proxy, distinct ids return distinct proxies. The first
    /// resolve for an id warms the entity through the region and awaits
    /// its child, so the returned proxy fronts a live actor like Local
    /// mode (shard allocation happens here, bounded by the resolve
    /// timeout, not against the prompt's reply timeout); later resolves
    /// answer from the map. A failed warm-up evicts the entry so the
    /// next resolve retries.
    /// <param name="sessionId">The session whose proxy to resolve.</param>
    /// <param name="cancellationToken">Cancels the resolve.</param>
    /// <returns>The session proxy actor.</returns>
    member private this.resolveInner (sessionId: string) (cancellationToken: CancellationToken) : Task<IActorRef> =
        if String.IsNullOrWhiteSpace sessionId then
            raise (ArgumentException("Session id must be a non-empty string.", nameof sessionId))

        let warmed =
            lock gate (fun () ->
                match systemOpt, regionOpt with
                | Some created, Some regionRef ->
                    match Map.tryFind sessionId proxies with
                    | Some pending -> pending
                    | None ->
                        let proxy =
                            spawn
                                created
                                $"%s{ClusterActorSystem.proxyPrefix}%d{nextProxy}"
                                (ClusterActorSystem.clusterProxy sessionId regionRef)

                        nextProxy <- nextProxy + 1

                        let pending =
                            task {
                                try
                                    // Ask sparingly with a timeout, per the
                                    // actor rules; the timeout bounds the
                                    // shard-allocation wait while the token
                                    // honours the caller. The sender is the
                                    // ask's temporary actor, which the proxy
                                    // preserves into the region, so the
                                    // entity's child reply lands here.
                                    use timeoutCts = new CancellationTokenSource(ClusterActorSystem.resolveTimeout)

                                    use linkedCts =
                                        CancellationTokenSource.CreateLinkedTokenSource(
                                            cancellationToken,
                                            timeoutCts.Token
                                        )

                                    let! _ = proxy.Ask<IActorRef>(sessionId, linkedCts.Token)
                                    return proxy
                                with ex ->
                                    lock gate (fun () -> proxies <- Map.remove sessionId proxies)
                                    ExceptionDispatchInfo.Capture(ex).Throw()
                                    return proxy
                            }

                        proxies <- Map.add sessionId pending proxies
                        pending
                | _ ->
                    Task.FromException<IActorRef>(
                        InvalidOperationException(
                            "The Legate cluster actor system is not running: start the host in StaticSeeds or Kubernetes cluster mode first."
                        )
                    ))

        warmed

    /// Resolves the session proxy for a session id: the same id returns
    /// the same proxy, distinct ids return distinct proxies. The first
    /// resolve warms the entity; later resolves answer from the map.
    /// <param name="sessionId">The session whose proxy to resolve.</param>
    /// <param name="cancellationToken">Cancels the resolve.</param>
    /// <returns>The session proxy actor.</returns>
    member this.ResolveSessionAsync(sessionId: string, cancellationToken: CancellationToken) : Task<IActorRef> =
        this.resolveInner sessionId cancellationToken

    interface ISessionResolver with
        member this.ResolveSessionAsync(sessionId, cancellationToken) =
            this.resolveInner sessionId cancellationToken

    interface IHostedService with
        member _.StartAsync(_cancellationToken: CancellationToken) =
            let remotingPort = this.RemotingPort
            let spawnNow () = this.currentSpawn ()

            task {
                match options.Value.Cluster.Mode with
                | ClusterMode.StaticSeeds
                | ClusterMode.Kubernetes as mode ->
                    if remotingPort < 0 then
                        raise (
                            ArgumentOutOfRangeException(
                                "RemotingPort",
                                "The remoting port must be 0 (ephemeral) or a positive port."
                            )
                        )

                    let startTimestamp = timeProvider.GetTimestamp()
                    let clusterOptions = options.Value.Cluster

                    let stamp =
                        SessionSharding.versionStamp clusterOptions.ShardHashVersion clusterOptions.ShardCount

                    let config =
                        ConfigurationFactory.ParseString(
                            ClusterActorSystem.buildClusterHocon clusterOptions remotingPort
                        )

                    let created = Akka.FSharp.System.create ClusterActorSystem.systemName config

                    try
                        let cluster = Cluster.Get(created)

                        match mode with
                        | ClusterMode.StaticSeeds ->
                            cluster.JoinSeedNodes(
                                ImmutableList.CreateRange(ClusterActorSystem.seedAddresses clusterOptions)
                            )
                        | _ ->
                            // Kubernetes joins nothing until issue 138 ships the
                            // Akka.Management bootstrap: singleton-until-bootstrap.
                            cluster.Join(cluster.SelfAddress)

                        // Touching the extension first injects the sharding
                        // reference.conf fallback; reading settings
                        // straight off the system would find no
                        // akka.cluster.sharding node.
                        let sharding = ClusterSharding.Get(created)

                        let settings = sharding.Settings.WithRole(clusterOptions.SessionRole)

                        let extractor =
                            SessionSharding.SessionMessageExtractor(clusterOptions.ShardCount) :> IMessageExtractor

                        let entityPropsFactory =
                            System.Func<string, Props>(fun entityId ->
                                ClusterActorSystem.entityProps entityId (spawnNow ()))

                        let regionRef =
                            sharding.Start(SessionSharding.shardTypeName, entityPropsFactory, settings, extractor)

                        let listener =
                            spawn created ClusterActorSystem.listenerName ClusterActorSystem.versionMismatchLog

                        let guard =
                            spawn
                                created
                                ClusterActorSystem.guardName
                                (ClusterActorSystem.shardingVersionGuard
                                    stamp
                                    (fun () -> cluster.Leave(cluster.SelfAddress))
                                    listener)

                        cluster.Subscribe(guard, [| typeof<ClusterEvent.MemberUp> |])

                        systemOpt <- Some created
                        regionOpt <- Some regionRef
                        lastColdStart <- timeProvider.GetElapsedTime startTimestamp
                    with ex ->
                        created.Terminate().GetAwaiter().GetResult() |> ignore
                        ExceptionDispatchInfo.Capture(ex).Throw()
                        return ()
                | ClusterMode.Local ->
                    // Local mode is untouched: the local service owns it.
                    ()
                | _ ->
                    raise (
                        InvalidOperationException(
                            $"Unknown Legate cluster mode '%O{options.Value.Cluster.Mode}'. Expected one of: Local, StaticSeeds, Kubernetes."
                        )
                    )
            }
            :> Task

        member _.StopAsync(cancellationToken: CancellationToken) =
            task {
                // Fail readiness FIRST so the orchestrator stops routing
                // before the running-turn wait starts.
                this.BeginDrain()
                let stopStart = timeProvider.GetTimestamp()

                match systemOpt with
                | None -> ()
                | Some created ->
                    let clusterOptions = options.Value.Cluster

                    let countRunning =
                        match this.SessionStore with
                        | None -> fun (_: CancellationToken) -> Task.FromResult 0
                        | Some store -> fun (ct: CancellationToken) -> store.CountRunningSessions(ct)

                    try
                        do!
                            ClusterActorSystem.waitForDrainAsync
                                countRunning
                                clusterOptions.ShutdownGraceSeconds
                                clusterOptions.HostExitDeadline
                                timeProvider
                                stopStart
                                cancellationToken
                    with :? OperationCanceledException ->
                        ()

                    let elapsed = timeProvider.GetElapsedTime stopStart
                    let remainingDeadline = clusterOptions.HostExitDeadline - elapsed

                    // The shutdown wait stays inside the remaining deadline
                    // so the whole stop never exceeds HostExitDeadline; when
                    // the drain already spent it, the wait bounds to zero and
                    // termination proceeds in the background.
                    let bound =
                        if remainingDeadline <= TimeSpan.Zero then
                            TimeSpan.Zero
                        elif clusterOptions.ShutdownGraceSeconds < remainingDeadline then
                            clusterOptions.ShutdownGraceSeconds
                        else
                            remainingDeadline

                    try
                        // Termination drives coordinated shutdown (the
                        // cluster HOCON keeps run-by-actor-system-terminate
                        // on, which leaves the cluster first). A null
                        // from-phase runs every shutdown phase.
                        let shutdown =
                            CoordinatedShutdown.Get(created).Run(CoordinatedShutdown.ClrExitReason.Instance, null)

                        let! _ = shutdown.WaitAsync(bound, cancellationToken)
                        ()
                    with
                    | :? TimeoutException -> ()
                    | :? OperationCanceledException -> ()

                    systemOpt <- None
                    regionOpt <- None

                    lock gate (fun () -> proxies <- Map.empty)
            }
            :> Task

// ──────────────────────────────────────────────────────────────────────────
// Registration

/// Registers the cluster actor system hosted service.
module internal ClusterActorSystemRegistration =

    /// Registers ClusterActorSystemService as a singleton hosted service,
    /// only when the host has not already supplied its own registration
    /// for the same implementation.
    /// <param name="services">The container to add the service to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, ClusterActorSystemService>())
        |> ignore
