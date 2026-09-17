// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open Microsoft.Extensions.Options

// Session expiry sweeper (issue 110). Idle sessions (no state change for
// Sessions:Expiry) close through the idempotent ISessionStore.CloseSession,
// which evicts the session's permission grants; the close carries the
// expiry reason on the close path and in logs while the existing
// SessionClosedEvent shape stays untouched (no new wire field). Only Idle
// sessions expire: Running turns stay open until they settle and
// WaitingForInput stays open under its AskTimeout, so the sweeper never
// kills active work. Enumeration stays locally scoped with no new store
// API: the service sweeps the facade tenant's sessions through the
// existing ListSessions paging. No external services: only ISessionStore,
// the TimeProvider clock, and the ILlmDelay seam, so Legate still builds
// and tests with no Redis, Docker, Postgres, or Kubernetes.

// ──────────────────────────────────────────────────────────────────────────
// One pass

/// One expiry pass over idle sessions. Internal so no store type ever
/// crosses the public API; tests drive <c>passOnceAsync</c> directly under
/// virtual time.
module internal SessionExpiry =

    /// How many sessions one ListSessions page holds at most: bounded pages
    /// keep a large idle backlog from growing the pass without bound.
    [<Literal>]
    let MaxBatchSize = 50

    /// The reason an expiry close carries on the close path and in logs.
    /// Client-safe: names only the idle bound, never secrets or tool
    /// arguments.
    [<Literal>]
    let ExpiryReason = "The session sat idle beyond Sessions:Expiry and was closed."

    /// Whether the session is due for expiry on the clock: open, Idle, and
    /// its last state change at least <paramref name="expiry" /> ago. Null
    /// sessions never expire (a store quirk skips instead of failing the
    /// pass).
    /// <param name="session">The session to test.</param>
    /// <param name="expiry">How long a session may sit idle.</param>
    /// <param name="now">The instant to measure idleness against.</param>
    /// <returns>True when the session is Idle and idle beyond the bound.</returns>
    let isExpired (session: Session) (expiry: TimeSpan) (now: DateTimeOffset) : bool =
        if isNull (box session) then false
        elif session.State <> SessionState.Idle then false
        else now - session.UpdatedAt >= expiry

    /// Closes one expired session through the idempotent store close:
    /// closing an already-closed session is a no-op returning the stored
    /// shape, and the store evicts the session's permission grants.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to close.</param>
    /// <param name="cancellationToken">Abandons the close.</param>
    /// <returns>The stored session after the close.</returns>
    let closeExpiredAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (cancellationToken: CancellationToken)
        : Task<Session> =
        ArgumentNullException.ThrowIfNull(store)
        store.CloseSession(tenant, sessionId, cancellationToken)

    /// Runs one full pass: pages the tenant's Idle sessions in bounded
    /// batches and closes every session idle beyond the configured expiry.
    /// Disabled (null) or non-positive expiry closes nothing: configuration
    /// validation rejects the latter at startup, so the pass treats it as
    /// disabled rather than failing the loop.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant whose sessions to sweep.</param>
    /// <param name="sessions">The session knobs carrying the expiry bound.</param>
    /// <param name="clock">The clock idleness reads.</param>
    /// <param name="cancellationToken">Abandons the pass.</param>
    /// <returns>How many sessions were closed.</returns>
    let passOnceAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessions: SessionsOptions)
        (clock: TimeProvider)
        (cancellationToken: CancellationToken)
        : Task<int> =
        task {
            ArgumentNullException.ThrowIfNull(store)
            ArgumentNullException.ThrowIfNull(sessions)
            ArgumentNullException.ThrowIfNull(clock)

            if not sessions.Expiry.HasValue || sessions.Expiry.Value <= TimeSpan.Zero then
                return 0
            else
                let expiry = sessions.Expiry.Value
                let mutable closed = 0
                let mutable continuation: string | null = null
                let mutable more = true

                while more do
                    let! page =
                        store.ListSessions(
                            tenant,
                            Nullable(SessionState.Idle),
                            MaxBatchSize,
                            continuation,
                            cancellationToken
                        )

                    if isNull (box page) || isNull (box page.Items) then
                        more <- false
                    else
                        let now = clock.GetUtcNow()

                        for session in page.Items do
                            if not (isNull (box session)) && isExpired session expiry now then
                                let! _ = closeExpiredAsync store tenant session.Id cancellationToken
                                closed <- closed + 1

                        continuation <- page.Continuation
                        more <- not (isNull (box page.Continuation))

                return closed
        }

// ──────────────────────────────────────────────────────────────────────────
// Hosted service

/// Singleton hosted service owning the expiry loop: one session-expiry pass
/// plus one workspace-teardown pass every tick through
/// <see cref="M:Legate.SessionExpiry.passOnceAsync*" /> over the injected
/// clock and delay seams, so virtual-time tests advance it without
/// sleeping. The tick interval is the configured expiry when set, else the
/// workspace idle teardown bound. A pass failure is retried next interval;
/// host stop cancels the wait and the in-flight pass. The session store and
/// the facade tenant resolve lazily from the provider at loop start (never
/// at construction), so an empty host still fails startup with the
/// required-registration message instead of a resolution error. The expiry
/// reason travels on the close path and in logs; the SessionClosedEvent
/// shape is untouched. Operational visibility rides the runtime's own logs:
/// like LocalActorSystemService this service takes no logger, so it stays
/// constructible on containers without logging.
type internal SessionExpiryService
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
    let tracker = SessionWorkspaceTracker(timeProvider)

    /// The workspace tracker this service tears down: hosts and tests Track
    /// bound workspaces on it; the service owns disposal of idle entries.
    member _.Tracker: SessionWorkspaceTracker = tracker

    /// Resolves the locally scoped sweep tenant: the facade client's tenant
    /// when one is registered, else the default single-tenant id. The sweep
    /// stays tenant-scoped with no new store API by covering exactly the
    /// tenant this node serves.
    /// <returns>The tenant whose sessions to sweep.</returns>
    member private _.SweepTenant() : TenantId =
        match serviceProvider.GetService<SessionClientOptions>() with
        | null -> TenantId.Default
        | clientOptions -> clientOptions.Tenant

    /// The tick interval for the loop: the configured expiry when it is a
    /// positive bound, else the workspace idle teardown bound. A
    /// non-positive result falls back to ten minutes so a misconfigured
    /// host can never busy-loop; configuration validation rejects such
    /// values at startup.
    /// <returns>How long the loop waits between passes.</returns>
    member private _.TickInterval() : TimeSpan =
        let legateOptions = options.Value
        let fallback = TimeSpan.FromMinutes 10.0

        let expiry =
            if isNull (box legateOptions) || isNull (box legateOptions.Sessions) then
                Nullable<TimeSpan>()
            else
                legateOptions.Sessions.Expiry

        let idleAfter =
            if isNull (box legateOptions) || isNull (box legateOptions.Workspace) then
                fallback
            else
                legateOptions.Workspace.IdleTeardownAfter

        let candidate =
            if expiry.HasValue && expiry.Value > TimeSpan.Zero then
                expiry.Value
            else
                idleAfter

        if candidate > TimeSpan.Zero then candidate else fallback

    /// Runs one full tick: the session-expiry pass, then the workspace
    /// idle-teardown pass over tracked bindings.
    /// <param name="cancellationToken">Abandons the tick.</param>
    /// <returns>How many sessions were closed plus how many workspaces were torn down.</returns>
    member internal this.RunOnceAsync(cancellationToken: CancellationToken) : Task<int> =
        task {
            let store = serviceProvider.GetRequiredService<ISessionStore>()
            let legateOptions = options.Value
            let tenant = this.SweepTenant()

            let sessions =
                if isNull (box legateOptions) || isNull (box legateOptions.Sessions) then
                    SessionsOptions()
                else
                    legateOptions.Sessions

            let! closed = SessionExpiry.passOnceAsync store tenant sessions timeProvider cancellationToken

            let idleAfter =
                if isNull (box legateOptions) || isNull (box legateOptions.Workspace) then
                    TimeSpan.FromMinutes 10.0
                else
                    legateOptions.Workspace.IdleTeardownAfter

            let! tornDown = tracker.TeardownIdleAsync(idleAfter, cancellationToken)

            if closed > 0 then
                log.LogInformation(
                    "Session expiry closed {Closed} idle sessions: {Reason}",
                    closed,
                    SessionExpiry.ExpiryReason
                )

            return closed + tornDown
        }

    /// Runs the tick loop until host stop.
    /// <returns>A task that completes once the loop exits.</returns>
    member private this.RunAsync() : Task =
        task {
            let mutable running = true

            while running && not lifetime.Token.IsCancellationRequested do
                try
                    let! _ = this.RunOnceAsync(lifetime.Token)
                    ()
                with
                | :? OperationCanceledException -> running <- false
                | failed ->
                    log.LogWarning("Session expiry pass failed and will retry next interval: {Reason}", failed.Message)

                if running && not lifetime.Token.IsCancellationRequested then
                    try
                        do! delay.Delay(this.TickInterval(), lifetime.Token)
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

/// Registers the session expiry hosted service.
module internal SessionExpiryRegistration =

    /// Registers SessionExpiryService as a singleton hosted service, only
    /// when the host has not already supplied its own registration for the
    /// same implementation.
    /// <param name="services">The container to add the service to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SessionExpiryService>())
        |> ignore
