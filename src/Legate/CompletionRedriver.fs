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

// Completion re-drive service (issue 84). The session actor enqueues one
// outbox row per settlement and notifies the sink inline with the same
// stable idempotency key; this background service lease-claims pending rows
// and redelivers at-least-once through the session's stored sink, so a
// delivery survives a restart between the settlement and the inline Notify.
// Delivered rows are retained for CompletionOptions.DeliveredRetention for
// idempotency, then purged. No external services: only ISessionStore, the
// TimeProvider clock, and the ILlmDelay seam, so Legate still builds and
// tests with no Redis, Docker, Postgres, or Kubernetes.

// ──────────────────────────────────────────────────────────────────────────
// One pass

/// One re-drive pass over the completion outbox. Internal so no store type
/// ever crosses the public API; tests drive <c>passOnceAsync</c> directly
/// under virtual time.
module internal CompletionRedriver =

    /// How many rows one pass claims at most: bounded batches keep a
    /// sink-down backlog from growing the pass without bound.
    [<Literal>]
    let MaxBatchSize = 50

    /// Delivers one claimed row when its lease still holds: resolves the
    /// sink from the stored session snapshot, verifies the lease owner at
    /// the last moment, notifies, then marks delivered under the same
    /// owner. A stale owner notifies nothing and marks nothing (zero
    /// effects); a throwing sink keeps the row pending for the next pass.
    /// <param name="store">The durable store.</param>
    /// <param name="owner">This re-driver's delivery owner identity.</param>
    /// <param name="entry">The claimed row to deliver.</param>
    /// <param name="cancellationToken">Abandons the delivery.</param>
    /// <returns>True when the row was delivered and marked.</returns>
    let private deliverOneAsync
        (store: ISessionStore)
        (owner: string)
        (entry: CompletionOutboxEntry)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        task {
            let! session = store.GetSession(entry.Tenant, entry.SessionId, cancellationToken)

            match session with
            | null -> return false
            | live when isNull (box live.Options) -> return false
            | live ->
                match live.Options.CompletionSink with
                | null -> return false
                | sink ->
                    let! liveLease =
                        store.VerifyCompletionClaim(entry.Tenant, entry.IdempotencyKey, owner, cancellationToken)

                    if not liveLease then
                        return false
                    else
                        let notified =
                            try
                                sink.Notify(entry.Completion)
                                true
                            with _ ->
                                false

                        if not notified then
                            return false
                        else
                            let! marked =
                                store.MarkCompletionDelivered(
                                    entry.Tenant,
                                    entry.IdempotencyKey,
                                    owner,
                                    cancellationToken
                                )

                            return marked
        }

    /// Claims and delivers one bounded batch of pending rows under this
    /// re-driver's lease.
    /// <param name="store">The durable store.</param>
    /// <param name="owner">This re-driver's delivery owner identity.</param>
    /// <param name="leaseDuration">How long each claimed lease lasts.</param>
    /// <param name="cancellationToken">Abandons the batch.</param>
    /// <returns>How many rows were delivered and marked.</returns>
    let deliverPendingAsync
        (store: ISessionStore)
        (owner: string)
        (leaseDuration: TimeSpan)
        (cancellationToken: CancellationToken)
        : Task<int> =
        task {
            let! claimed = store.ClaimCompletionOutbox(owner, MaxBatchSize, leaseDuration, cancellationToken)
            let mutable delivered = 0

            if not (isNull (box claimed)) then
                for entry in claimed do
                    if not (isNull (box entry)) then
                        let! ok = deliverOneAsync store owner entry cancellationToken

                        if ok then
                            delivered <- delivered + 1

            return delivered
        }

    /// Purges delivered rows older than the retention window; pending rows
    /// are never removed, however old.
    /// <param name="store">The durable store.</param>
    /// <param name="retention">How long delivered rows are retained.</param>
    /// <param name="clock">The clock the cutoff reads.</param>
    /// <param name="cancellationToken">Abandons the purge.</param>
    /// <returns>How many rows were removed.</returns>
    let purgeDeliveredAsync
        (store: ISessionStore)
        (retention: TimeSpan)
        (clock: TimeProvider)
        (cancellationToken: CancellationToken)
        : Task<int> =
        task {
            let cutoff = clock.GetUtcNow() - retention
            return! store.PurgeDeliveredCompletions(cutoff, cancellationToken)
        }

    /// Runs one full pass: delivers the bounded pending batch, then purges
    /// delivered rows past the retention window.
    /// <param name="store">The durable store.</param>
    /// <param name="owner">This re-driver's delivery owner identity.</param>
    /// <param name="completion">The completion redrive knobs.</param>
    /// <param name="clock">The clock the purge cutoff reads.</param>
    /// <param name="cancellationToken">Abandons the pass.</param>
    /// <returns>A task that completes once the pass has run.</returns>
    let passOnceAsync
        (store: ISessionStore)
        (owner: string)
        (completion: CompletionOptions)
        (clock: TimeProvider)
        (cancellationToken: CancellationToken)
        : Task =
        task {
            ArgumentNullException.ThrowIfNull(store)
            ArgumentNullException.ThrowIfNull(owner)
            ArgumentNullException.ThrowIfNull(completion)
            ArgumentNullException.ThrowIfNull(clock)

            let! _ = deliverPendingAsync store owner completion.ClaimLeaseDuration cancellationToken
            let! _ = purgeDeliveredAsync store completion.DeliveredRetention clock cancellationToken
            return ()
        }

// ──────────────────────────────────────────────────────────────────────────
// Hosted service

/// Singleton hosted service owning the completion re-drive loop. Polls the
/// outbox every <c>Completion:RedriveInterval</c> through
/// <see cref="M:Legate.CompletionRedriver.passOnceAsync*" /> over the
/// injected delay seam, so virtual-time tests advance it without sleeping.
/// A pass failure is retried next interval; host stop cancels the wait and
/// the in-flight pass. The session store resolves lazily from the provider
/// at loop start (never at construction), so an empty host still fails
/// startup with the required-registration message instead of a resolution
/// error. Operational visibility rides the sinks' own logs: like
/// LocalActorSystemService this service takes no logger, so it stays
/// constructible on containers without logging; undelivered rows stay
/// observable in the store.
type internal CompletionRedriverService
    (serviceProvider: IServiceProvider, options: IOptions<LegateOptions>, timeProvider: TimeProvider, delay: ILlmDelay)
    =

    do
        ArgumentNullException.ThrowIfNull(serviceProvider)
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(timeProvider)
        ArgumentNullException.ThrowIfNull(delay)

    let log: ILogger = NullLogger.Instance :> ILogger

    let owner = Guid.NewGuid().ToString("N")
    let lifetime = new CancellationTokenSource()
    let mutable loop: Task | null = null

    /// This service's delivery owner identity: what its claims stamp.
    member _.Owner: string = owner

    /// Runs the poll loop until host stop.
    /// <returns>A task that completes once the loop exits.</returns>
    member private this.RunAsync() : Task =
        task {
            let store = serviceProvider.GetRequiredService<ISessionStore>()
            let mutable running = true

            while running && not lifetime.Token.IsCancellationRequested do
                try
                    let completion = options.Value.Completion

                    do! CompletionRedriver.passOnceAsync store owner completion timeProvider lifetime.Token
                with
                | :? OperationCanceledException -> running <- false
                | failed ->
                    log.LogWarning(
                        "Completion re-drive pass failed and will retry next interval: {Reason}",
                        failed.Message
                    )

                if running && not lifetime.Token.IsCancellationRequested then
                    try
                        do! delay.Delay(options.Value.Completion.RedriveInterval, lifetime.Token)
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

/// Registers the completion re-drive hosted service.
module internal CompletionRedriverRegistration =

    /// Registers CompletionRedriverService as a singleton hosted service,
    /// only when the host has not already supplied its own registration
    /// for the same implementation.
    /// <param name="services">The container to add the service to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, CompletionRedriverService>())
        |> ignore
