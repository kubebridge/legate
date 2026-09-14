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
// router that spawns one identity-only child per session id, and stops
// through coordinated shutdown bounded by Cluster:ShutdownGraceSeconds.
// Clustered mode is untouched: this service starts nothing and stops nothing
// there. The children carry identity only (no inbox, turn, or lease logic):
// the session state machine belongs to later issues.

// ──────────────────────────────────────────────────────────────────────────
// Router protocol

/// The session router protocol. Identity only: resolving a session id
/// returns the same child for the same id and a distinct child per id.
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

    /// A session child carries identity only: it accepts any message and
    /// does nothing, so resolving an id is observable without running turn
    /// logic. Inlined at the spawn site: a shared actorOf value would hit
    /// the value restriction on its generic continuation.
    /// The router loop: a Map from session id to child plus a counter for
    /// child actor names (names stay valid because they never embed the
    /// session id; identity lives in the Map).
    let private sessionRouter (mailbox: Actor<SessionRouterMessage>) =
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
                        let child = spawn mailbox.Context $"session-{nextId}" (actorOf (fun (_: obj) -> ()))

                        mailbox.Sender() <! child
                        return! loop (Map.add sessionId child children) (nextId + 1)
            }

        loop Map.empty 0

    /// Spawns the single session router under the system guardian.
    /// <param name="system">The local actor system hosting the router.</param>
    /// <returns>The session router actor.</returns>
    let spawnRouter (system: Akka.Actor.ActorSystem) : IActorRef =
        ArgumentNullException.ThrowIfNull(system)
        spawn system routerName sessionRouter

// ──────────────────────────────────────────────────────────────────────────
// Hosted service

/// Singleton hosted service owning the local actor system. Starts only in
/// Local cluster mode; Clustered mode resolves no system and no router.
/// Stop bounds coordinated shutdown by
/// <c>Cluster:ShutdownGraceSeconds</c>.
type internal LocalActorSystemService(options: IOptions<LegateOptions>, timeProvider: TimeProvider) =

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

    /// Resolves the identity-only session child for a session id: the same
    /// id returns the same actor, distinct ids return distinct actors.
    /// <param name="sessionId">The session whose child to resolve.</param>
    /// <param name="cancellationToken">Cancels the resolve.</param>
    /// <returns>The session child actor.</returns>
    member _.ResolveSessionAsync(sessionId: string, cancellationToken: CancellationToken) : Task<IActorRef> =
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

    interface IHostedService with
        member _.StartAsync(_cancellationToken: CancellationToken) =
            task {
                if options.Value.Cluster.Mode = ClusterMode.Local then
                    let startTimestamp = timeProvider.GetTimestamp()
                    let created = LocalActorSystem.createSystem ()
                    let routerRef = LocalActorSystem.spawnRouter created
                    system <- created
                    router <- routerRef
                    lastColdStart <- timeProvider.GetElapsedTime startTimestamp
                else
                    // Clustered mode is untouched: sharding bootstraps elsewhere.
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
