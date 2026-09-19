// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open Microsoft.Extensions.Options

// Polling dispatcher with wake hints (issue 120). The poll stays
// authoritative: every cycle sweeps the facade tenant's dispatch candidates
// through the bounded GetDispatchCandidates batch (following HasMore),
// admits sessions through the conjunctive capacity gate (process, tenant,
// agent: a session stays queued while ANY limit trips, checked in that
// order for observability labeling only), and wakes each admitted session
// through the idempotent check-inbox actor message, which starts a turn on
// the oldest drainable entry when Idle and no-ops otherwise. The
// in-process wake sink only shortens the wait for the next cycle. No
// dispatcher-side ClaimNextTurn: claim ownership and fencing stay with the
// actor's start path. No external services: only ISessionStore, the
// TimeProvider clock, and the ILlmDelay seam, so Legate still builds and
// tests with no Redis, Docker, Postgres, or Kubernetes.

// ──────────────────────────────────────────────────────────────────────────
// One pass

/// One dispatch pass over pending sessions. Internal so no store or actor
/// type ever crosses the public API; tests drive <c>passOnceAsync</c>
/// directly under virtual time.
module internal Dispatcher =

    /// What one poll pass did: how many sessions it woke and how many stayed
    /// queued behind each conjunctive limit.
    type DispatchPassResult =
        {
            /// How many sessions the pass woke through check-inbox.
            Started: int
            /// How many stayed queued behind the per-process limit.
            QueuedProcess: int
            /// How many stayed queued behind the per-tenant limit.
            QueuedTenant: int
            /// How many stayed queued behind the per-agent limit.
            QueuedAgent: int
        }

        /// How many sessions stayed queued behind any limit.
        member this.Queued: int = this.QueuedProcess + this.QueuedTenant + this.QueuedAgent

    /// An empty pass: nothing woke, nothing queued.
    let emptyResult: DispatchPassResult =
        {
            Started = 0
            QueuedProcess = 0
            QueuedTenant = 0
            QueuedAgent = 0
        }

    /// Runs one full pass: pages the tenant's dispatch candidates in bounded
    /// batches following HasMore and wakes every admitted session. The
    /// capacity snapshots read once per pass plus the local in-flight gate
    /// bound the overshoot: every wake in the pass counts against the
    /// limits it was admitted under. A batch with no unseen sessions ends
    /// the pass, so gate-blocked sessions that stay pending never spin it.
    /// A vanished session (null row mid-pass) or a per-session wake failure
    /// skips that session for this pass; the next interval retries.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant whose pending sessions to sweep.</param>
    /// <param name="sessions">The session knobs carrying the process and per-tenant limits.</param>
    /// <param name="dispatcher">The dispatcher knobs carrying the batch size and the per-agent limit.</param>
    /// <param name="resolve">Resolves the session actor to wake. Never null.</param>
    /// <param name="clock">The clock dispatch latency reads.</param>
    /// <param name="cancellationToken">Abandons the pass.</param>
    /// <returns>What the pass woke and what stayed queued behind each limit.</returns>
    let passOnceAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessions: SessionsOptions)
        (dispatcher: DispatcherOptions)
        (resolve: SessionId -> CancellationToken -> Task<IActorRef>)
        (clock: TimeProvider)
        (cancellationToken: CancellationToken)
        : Task<DispatchPassResult> =
        task {
            ArgumentNullException.ThrowIfNull(store)
            ArgumentNullException.ThrowIfNull(sessions)
            ArgumentNullException.ThrowIfNull(dispatcher)
            ArgumentNullException.ThrowIfNull(clock)

            if isNull (box resolve) then
                raise (ArgumentNullException(nameof resolve))

            let maxBatch =
                if dispatcher.MaxBatchSize > 0 then
                    dispatcher.MaxBatchSize
                else
                    50

            let! runningSnapshot = store.CountRunningSessions(cancellationToken)
            let! tenantSnapshot = store.CountSessionsByTenant(tenant, cancellationToken)

            let agentSnapshots = Dictionary<AgentId, int>()
            let agentLocal = Dictionary<AgentId, int>()
            let visited = HashSet<SessionId>()

            let mutable runningLocal = 0
            let mutable tenantLocal = 0
            let mutable started = 0
            let mutable queuedProcess = 0
            let mutable queuedTenant = 0
            let mutable queuedAgent = 0
            let mutable more = true

            while more do
                let! batch = store.GetDispatchCandidates(tenant, maxBatch, cancellationToken)

                if isNull (box batch) || isNull (box batch.Sessions) then
                    more <- false
                else
                    let unseen =
                        batch.Sessions
                        |> Seq.filter (fun sessionId -> visited.Add(sessionId))
                        |> Seq.toList

                    if unseen.IsEmpty then
                        more <- false
                    else
                        for sessionId in unseen do
                            let! session = store.GetSession(tenant, sessionId, cancellationToken)

                            match session with
                            | null -> () // Vanished mid-pass: stays out of this pass.
                            | live ->
                                let! agentCount =
                                    match agentSnapshots.TryGetValue(live.AgentId) with
                                    | true, count -> task { return count }
                                    | false, _ ->
                                        task {
                                            let! count =
                                                store.CountSessionsByAgent(tenant, live.AgentId, cancellationToken)

                                            agentSnapshots[live.AgentId] <- count
                                            return count
                                        }

                                let agentStarted =
                                    match agentLocal.TryGetValue(live.AgentId) with
                                    | true, count -> count
                                    | false, _ -> 0

                                // Conjunctive gate, checked process ->
                                // tenant -> agent for observability labeling
                                // only: any tripped limit keeps the session
                                // queued, with no precedence between limits.
                                if runningSnapshot + runningLocal >= sessions.Capacity then
                                    queuedProcess <- queuedProcess + 1
                                elif tenantSnapshot + tenantLocal >= sessions.MaxSessionsPerTenant then
                                    queuedTenant <- queuedTenant + 1
                                elif agentCount + agentStarted >= dispatcher.MaxSessionsPerAgent then
                                    queuedAgent <- queuedAgent + 1
                                else
                                    try
                                        let! pending = store.ReadPendingInbox(tenant, sessionId, cancellationToken)

                                        let oldest =
                                            if isNull (box pending) then
                                                None
                                            else
                                                pending
                                                |> Seq.filter (fun entry -> not (isNull (box entry)))
                                                |> Seq.sortBy (fun entry -> entry.Position)
                                                |> Seq.tryHead

                                        match oldest with
                                        | Some entry ->
                                            Telemetry.recordDispatchLatency (
                                                (clock.GetUtcNow() - entry.AppendedAt).TotalMilliseconds
                                            )
                                        | None -> ()

                                        let! actor = resolve sessionId cancellationToken
                                        do! SessionActor.checkInboxAsync store tenant sessionId actor cancellationToken

                                        runningLocal <- runningLocal + 1
                                        tenantLocal <- tenantLocal + 1
                                        agentLocal[live.AgentId] <- agentStarted + 1
                                        started <- started + 1
                                    with
                                    | :? OperationCanceledException as cancelled -> raise cancelled
                                    | _ -> ()

                        more <- batch.HasMore

            return
                {
                    Started = started
                    QueuedProcess = queuedProcess
                    QueuedTenant = queuedTenant
                    QueuedAgent = queuedAgent
                }
        }

// ──────────────────────────────────────────────────────────────────────────
// Hosted service

/// Singleton hosted service owning the dispatch loop: one poll pass every
/// tick through <see cref="M:Legate.Dispatcher.passOnceAsync*" /> over the
/// injected clock and delay seams, so virtual-time tests advance it without
/// sleeping. A wake pulse only shortens the wait for the next pass; the
/// poll stays authoritative. A pass failure is retried next interval; host
/// stop cancels the wait and the in-flight pass. The session store, the
/// session client, and the wake sink resolve lazily from the provider at
/// loop start (never at construction), so an empty host still fails startup
/// with the required-registration message instead of a resolution error.
/// Operational visibility rides the runtime's own logs: like
/// LocalActorSystemService this service takes no logger, so it stays
/// constructible on containers without logging.
type internal DispatcherService
    (serviceProvider: IServiceProvider, options: IOptions<LegateOptions>, timeProvider: TimeProvider, delay: ILlmDelay)
    =

    do
        ArgumentNullException.ThrowIfNull(serviceProvider)
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(timeProvider)
        ArgumentNullException.ThrowIfNull(delay)

    let log: ILogger = NullLogger.Instance :> ILogger
    let lifetime = new CancellationTokenSource()
    let mutable loop: Task | null = null

    /// Resolves the locally scoped sweep tenant: the facade client's tenant
    /// when one is registered, else the default single-tenant id. The sweep
    /// stays tenant-scoped with no new store API by covering exactly the
    /// tenant this node serves.
    /// <returns>The tenant whose sessions to sweep.</returns>
    member private _.SweepTenant() : TenantId =
        match serviceProvider.GetService<SessionClientOptions>() with
        | null -> TenantId.Default
        | clientOptions -> clientOptions.Tenant

    /// The dispatcher knobs, falling back to the defaults when the options
    /// root or section is missing.
    /// <returns>The dispatcher knobs the loop sweeps under.</returns>
    member private _.DispatcherKnobs() : DispatcherOptions =
        let legateOptions = options.Value

        if isNull (box legateOptions) || isNull (box legateOptions.Dispatcher) then
            DispatcherOptions()
        else
            legateOptions.Dispatcher

    /// The session knobs, falling back to the defaults when the options
    /// root or section is missing.
    /// <returns>The session knobs the gate enforces.</returns>
    member private _.SessionKnobs() : SessionsOptions =
        let legateOptions = options.Value

        if isNull (box legateOptions) || isNull (box legateOptions.Sessions) then
            SessionsOptions()
        else
            legateOptions.Sessions

    /// Runs one full cycle: drains the coalesced wakes (the sweep that
    /// follows covers them authoritatively) and runs the poll pass.
    /// <param name="cancellationToken">Abandons the cycle.</param>
    /// <returns>What the pass woke and what stayed queued.</returns>
    member internal this.RunOnceAsync(cancellationToken: CancellationToken) : Task<Dispatcher.DispatchPassResult> =
        task {
            let store = serviceProvider.GetRequiredService<ISessionStore>()
            let client = serviceProvider.GetRequiredService<SessionClient>()
            let sink = serviceProvider.GetRequiredService<DispatcherWakeSink>()
            let tenant = this.SweepTenant()

            sink.TakePending() |> ignore

            let resolve (sessionId: SessionId) (candidateToken: CancellationToken) : Task<IActorRef> =
                client.Resolve(sessionId, candidateToken)

            return!
                Dispatcher.passOnceAsync
                    store
                    tenant
                    (this.SessionKnobs())
                    (this.DispatcherKnobs())
                    resolve
                    timeProvider
                    cancellationToken
        }

    /// The tick interval for the loop: the configured poll interval when it
    /// is a positive bound, else five seconds, so a misconfigured host can
    /// never busy-loop; configuration validation rejects such values at
    /// startup.
    /// <returns>How long the loop waits between passes without a wake.</returns>
    member private this.TickInterval() : TimeSpan =
        let candidate = (this.DispatcherKnobs()).PollInterval

        if candidate > TimeSpan.Zero then
            candidate
        else
            TimeSpan.FromSeconds 5.0

    /// Runs the poll loop until host stop: one cycle, then the shorter of
    /// the poll interval and the next wake pulse.
    /// <returns>A task that completes once the loop exits.</returns>
    member private this.RunAsync() : Task =
        task {
            let sink = serviceProvider.GetRequiredService<DispatcherWakeSink>()
            let mutable running = true

            while running && not lifetime.Token.IsCancellationRequested do
                try
                    let! _ = this.RunOnceAsync(lifetime.Token)
                    ()
                with
                | :? OperationCanceledException -> running <- false
                | failed ->
                    log.LogWarning("Dispatch pass failed and will retry next interval: {Reason}", failed.Message)

                if running && not lifetime.Token.IsCancellationRequested then
                    try
                        let wait = delay.Delay(this.TickInterval(), lifetime.Token)
                        let pulse = sink.WaitAsync(lifetime.Token)
                        let! _ = Task.WhenAny(wait, pulse)
                        ()
                    with :? OperationCanceledException ->
                        running <- false
        }

    interface IHostedService with
        member this.StartAsync(_cancellationToken: CancellationToken) =
            loop <- Task.Run(Func<Task>(fun () -> this.RunAsync()), lifetime.Token)
            Task.CompletedTask

        member _.StopAsync(cancellationToken: CancellationToken) =
            task {
                lifetime.Cancel()

                match loop with
                | null -> ()
                | running ->
                    try
                        do! running.WaitAsync(cancellationToken)
                    with
                    | :? OperationCanceledException -> ()
                    | :? TimeoutException -> ()
            }
            :> Task

// ──────────────────────────────────────────────────────────────────────────
// Registration

/// Registers the dispatcher wake sink and hosted service.
module internal DispatcherRegistration =

    /// Registers DispatcherWakeSink as a singleton and DispatcherService as
    /// a singleton hosted service, each only when the host has not already
    /// supplied its own registration for the same implementation.
    /// <param name="services">The container to add the dispatcher to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddSingleton<DispatcherWakeSink>() |> ignore

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, DispatcherService>())
        |> ignore
