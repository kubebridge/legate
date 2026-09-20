// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options

// Local-only actor system for single-node hosts. A singleton hosted service
// creates one Akka.NET ActorSystem with minimal inline HOCON (no remoting,
// cluster, or sharding) when Cluster:Mode is Local, fronts it with a session
// router that spawns one child per session id, and stops through coordinated
// shutdown bounded by Cluster:ShutdownGraceSeconds. The cluster modes
// (StaticSeeds, Kubernetes) are untouched: this service starts nothing and
// stops nothing there. Each child
// is the SessionActor state machine when a child factory is configured, or
// the legacy identity-only child otherwise (no store or turn runner is
// available until the session client facade configures one): resolving an
// id stays observable either way.

// ──────────────────────────────────────────────────────────────────────────
// Router protocol

/// The session router protocol. Resolving a session id returns the same
/// child for the same id and a distinct child per id, whether the child is
/// a session actor or the legacy identity-only placeholder.
type internal SessionRouterMessage = ResolveSession of sessionId: string

// ──────────────────────────────────────────────────────────────────────────
// Construction

/// Builds the local actor system and its session router. Internal so no
/// Akka type ever crosses the public API.
module internal LocalActorSystem =

    /// The actor system name, stable so logs and paths read the same in
    /// every host.
    let systemName = "legate"

    /// The single session router actor under the system guardian.
    let routerName = "legate-session-router"

    /// How long a session resolve waits for the router's reply before the
    /// resolve fails. The router answers from memory, so this only fires
    /// when the system is wedged.
    let resolveTimeout = TimeSpan.FromSeconds 5.0

    /// Minimal inline HOCON: local provider only, quiet logging, and
    /// coordinated shutdown driven by actor-system termination (owned by
    /// StopAsync) rather than the CLR shutdown hook.
    let localHocon =
        """
        akka {
          actor {
            provider = "local"
          }
          loglevel = "WARNING"
          stdout-loglevel = "WARNING"
          coordinated-shutdown {
            run-by-actor-system-terminate = on
            run-by-clr-shutdown-hook = off
          }
        }
        """

    /// Creates the local actor system from the minimal inline HOCON.
    /// <returns>The running local actor system.</returns>
    let createSystem () : Akka.Actor.ActorSystem =
        let config = ConfigurationFactory.ParseString(localHocon)
        Akka.FSharp.System.create systemName config

    /// The legacy identity-only child spawn: accepts any message and does
    /// nothing, so resolving an id is observable without running session
    /// logic. Used when no child factory is configured, and for router ids
    /// that do not parse as session ids. Inlined callers would hit the value
    /// restriction on the generic continuation, so this stays a function.
    /// <param name="sessionId">The session id being resolved (unused).</param>
    /// <param name="context">The parent context spawning the child.</param>
    /// <param name="name">The child actor name.</param>
    /// <returns>The identity-only child actor.</returns>
    let identitySpawn (_sessionId: string) (context: IActorContext) (name: string) : IActorRef =
        spawn context name (actorOf (fun (_: obj) -> ()))

    /// The router loop: a Map from session id to child plus a counter for
    /// child actor names (names stay valid because they never embed the
    /// session id; identity lives in the Map). The child factory maps each
    /// new session id to its spawn: the session actor factory once the
    /// session client facade configures one, the identity spawn otherwise.
    /// <param name="spawnSession">Spawns the child for a new session id.</param>
    /// <param name="mailbox">The router mailbox.</param>
    /// <returns>The router actor computation.</returns>
    let private sessionRouter
        (spawnSession: string -> IActorContext -> string -> IActorRef)
        (mailbox: Actor<SessionRouterMessage>)
        =
        let rec loop (children: Map<string, IActorRef>) (nextId: int) =
            actor {
                let! message = mailbox.Receive()

                match message with
                | ResolveSession sessionId ->
                    match Map.tryFind sessionId children with
                    | Some child ->
                        mailbox.Sender() <! child
                        return! loop children nextId
                    | None ->
                        let child = spawnSession sessionId mailbox.Context $"session-{nextId}"

                        mailbox.Sender() <! child
                        return! loop (Map.add sessionId child children) (nextId + 1)
            }

        loop Map.empty 0

    /// Spawns the single session router under the system guardian with the
    /// legacy identity-only children.
    /// <param name="system">The local actor system hosting the router.</param>
    /// <returns>The session router actor.</returns>
    let spawnRouter (system: Akka.Actor.ActorSystem) : IActorRef =
        ArgumentNullException.ThrowIfNull(system)
        spawn system routerName (sessionRouter identitySpawn)

    /// Spawns the single session router under the system guardian with
    /// session actor children spawned through the factory (see
    /// <see cref="M:Legate.SessionActor.spawnFactory" />).
    /// <param name="system">The local actor system hosting the router.</param>
    /// <param name="spawnSession">Spawns the session actor for a new session id.</param>
    /// <returns>The session router actor.</returns>
    let spawnRouterWith
        (system: Akka.Actor.ActorSystem)
        (spawnSession: string -> IActorContext -> string -> IActorRef)
        : IActorRef =
        ArgumentNullException.ThrowIfNull(system)

        if isNull (box spawnSession) then
            raise (ArgumentNullException(nameof spawnSession))

        spawn system routerName (sessionRouter spawnSession)

// ──────────────────────────────────────────────────────────────────────────
// Hosted service

/// Singleton hosted service owning the local actor system. Starts only in
/// Local cluster mode; the cluster modes (StaticSeeds, Kubernetes)
/// resolve no system and no router here.
/// Stop bounds coordinated shutdown by
/// <c>Cluster:ShutdownGraceSeconds</c>.
type internal LocalActorSystemService(options: IOptions<LegateOptions>, timeProvider: TimeProvider) as this =

    do ArgumentNullException.ThrowIfNull(options)
    do ArgumentNullException.ThrowIfNull(timeProvider)

    let mutable system: Akka.Actor.ActorSystem | null = null
    let mutable router: IActorRef | null = null
    let mutable lastColdStart: TimeSpan = TimeSpan.Zero

    /// The running actor system, or null when the host runs clustered or
    /// has not started.
    member _.System: Akka.Actor.ActorSystem | null = system

    /// The session router, or null when the host runs clustered or has not
    /// started.
    member _.Router: IActorRef | null = router

    /// How long the last Local-mode StartAsync system creation took,
    /// measured on the injected clock; TimeSpan.Zero when never started.
    /// This is the cold-start hook the timing test reads.
    member _.LastColdStart: TimeSpan = lastColdStart

    /// Spawns the session actor child for a newly resolved session id.
    /// None keeps the legacy identity-only children; Some wires the
    /// SessionActor state machine (see
    /// <see cref="M:Legate.SessionActor.spawnFactory" />). The session
    /// client facade owns setting this once it can supply the store and
    /// turn runner; until then resolving an id stays observable either
    /// way. Set before StartAsync.
    member val SessionChildFactory: (string -> IActorContext -> string -> IActorRef) option = None with get, set

    /// Resolves the session child for a session id: the same id returns
    /// the same actor, distinct ids return distinct actors.
    /// <param name="sessionId">The session whose child to resolve.</param>
    /// <param name="cancellationToken">Cancels the resolve.</param>
    /// <returns>The session child actor.</returns>
    member private _.resolveInner (sessionId: string) (cancellationToken: CancellationToken) : Task<IActorRef> =
        if String.IsNullOrWhiteSpace sessionId then
            raise (ArgumentException("Session id must be a non-empty string.", nameof sessionId))

        match router with
        | null ->
            raise (
                InvalidOperationException(
                    "The Legate local actor system is not running: start the host in Local cluster mode first."
                )
            )
        | routerRef ->
            task {
                // Ask sparingly with a timeout, per the actor rules; the
                // timeout bounds the wait while the token honours the caller.
                use timeoutCts = new CancellationTokenSource(LocalActorSystem.resolveTimeout)

                use linkedCts =
                    CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)

                let! child = routerRef.Ask<IActorRef>(ResolveSession sessionId, linkedCts.Token)
                return child
            }

    /// Resolves the session child for a session id: the same id returns
    /// the same actor, distinct ids return distinct actors.
    /// <param name="sessionId">The session whose child to resolve.</param>
    /// <param name="cancellationToken">Cancels the resolve.</param>
    /// <returns>The session child actor.</returns>
    member this.ResolveSessionAsync(sessionId: string, cancellationToken: CancellationToken) : Task<IActorRef> =
        this.resolveInner sessionId cancellationToken

    interface ISessionResolver with
        member this.ResolveSessionAsync(sessionId, cancellationToken) =
            this.resolveInner sessionId cancellationToken

    interface IHostedService with
        member _.StartAsync(_cancellationToken: CancellationToken) =
            task {
                if options.Value.Cluster.Mode = ClusterMode.Local then
                    let startTimestamp = timeProvider.GetTimestamp()
                    let created = LocalActorSystem.createSystem ()

                    let routerRef =
                        match this.SessionChildFactory with
                        | Some spawnSession -> LocalActorSystem.spawnRouterWith created spawnSession
                        | None -> LocalActorSystem.spawnRouter created

                    system <- created
                    router <- routerRef
                    lastColdStart <- timeProvider.GetElapsedTime startTimestamp
                else
                    // The cluster modes are untouched: sharding bootstraps elsewhere.
                    ()
            }
            :> Task

        member _.StopAsync(cancellationToken: CancellationToken) =
            task {
                match system with
                | null -> ()
                | created ->
                    let gracePeriod = options.Value.Cluster.ShutdownGraceSeconds

                    try
                        // Termination drives coordinated shutdown (the local
                        // HOCON keeps run-by-actor-system-terminate on); the
                        // wait bounds the drain by the configured grace. A
                        // null from-phase runs every shutdown phase.
                        let shutdown =
                            CoordinatedShutdown.Get(created).Run(CoordinatedShutdown.ClrExitReason.Instance, null)

                        let! _ = shutdown.WaitAsync(gracePeriod, cancellationToken)
                        ()
                    with
                    | :? TimeoutException -> ()
                    | :? OperationCanceledException -> ()

                    system <- null
                    router <- null
            }
            :> Task

// ──────────────────────────────────────────────────────────────────────────
// Registration

/// Registers the local actor system hosted service.
module internal LocalActorSystemRegistration =

    /// Registers LocalActorSystemService as a singleton hosted service,
    /// only when the host has not already supplied its own registration
    /// for the same implementation.
    /// <param name="services">The container to add the service to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, LocalActorSystemService>())
        |> ignore
