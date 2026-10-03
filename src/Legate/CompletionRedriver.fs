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

/// Sole external completion delivery path. Rows, not current sessions, determine routing.
module internal CompletionRedriver =
    [<Literal>]
    let MaxBatchSize = 50

    let private deliverOneAsync
        (store: ISessionStore)
        (routes: CompletionDestinations)
        (owner: string)
        (entry: CompletionOutboxEntry)
        (settings: CompletionOptions)
        (clock: TimeProvider)
        (delay: ILlmDelay)
        (log: ILogger)
        (ct: CancellationToken)
        : Task<bool> =
        task {
            try
                let destination =
                    match entry.DestinationId with
                    | null ->
                        raise (
                            CompletionRoutingException(
                                Nullable entry.Tenant,
                                Nullable entry.SessionId,
                                null,
                                CompletionRoutingReason.UnsupportedFormat
                            )
                        )
                    | id -> id

                let sink = routes.Resolve(entry.Tenant, Nullable entry.SessionId, destination)

                if String.IsNullOrWhiteSpace entry.IdempotencyKey then
                    raise (InvalidOperationException("Unsupported delivery identity."))

                use deadline = new CancellationTokenSource(settings.AttemptTimeout, clock)
                use attempt = CancellationTokenSource.CreateLinkedTokenSource(ct, deadline.Token)
                let! live = store.VerifyCompletionClaim(entry.Tenant, entry.IdempotencyKey, owner, attempt.Token)

                if not live then
                    return false
                else
                    let mutable lost = false

                    let heartbeat: Task =
                        task {
                            try
                                while not attempt.IsCancellationRequested do
                                    do!
                                        delay.Delay(
                                            TimeSpan.FromTicks(max 1L (settings.ClaimLeaseDuration.Ticks / 3L)),
                                            attempt.Token
                                        )

                                    let! renewed =
                                        store.RenewCompletionClaim(
                                            entry.Tenant,
                                            entry.IdempotencyKey,
                                            owner,
                                            settings.ClaimLeaseDuration,
                                            attempt.Token
                                        )

                                    if not renewed then
                                        lost <- true
                                        attempt.Cancel()
                            with
                            | :? OperationCanceledException -> ()
                            | _ ->
                                lost <- true
                                attempt.Cancel()
                        }

                    try
                        // The provider receives the same token that bounds our wait.
                        do! sink.NotifyAsync(entry.Completion, attempt.Token).WaitAsync(attempt.Token)
                        attempt.Token.ThrowIfCancellationRequested()

                        if lost then
                            return false
                        else
                            return!
                                store.MarkCompletionDelivered(entry.Tenant, entry.IdempotencyKey, owner, attempt.Token)
                    finally
                        attempt.Cancel()
                        // Observe completion without blocking a broken delay seam or sink.
                        heartbeat.ContinueWith(
                            (fun (completed: Task) ->
                                if completed.IsFaulted then
                                    completed.Exception |> ignore),
                            TaskScheduler.Default
                        )
                        |> ignore
            with
            | :? CompletionRoutingException as refusal ->
                log.LogWarning(
                    "Completion pending for tenant {Tenant}, session {SessionId}: routing {Reason}.",
                    entry.Tenant,
                    entry.SessionId,
                    refusal.Reason
                )

                return false
            | :? OperationCanceledException -> return false
            | _ ->
                log.LogWarning(
                    "Completion pending for tenant {Tenant}, session {SessionId}: delivery not acknowledged.",
                    entry.Tenant,
                    entry.SessionId
                )

                return false
        }

    let deliverPendingAsync store routes owner (settings: CompletionOptions) clock delay log ct : Task<int> =
        task {
            let! claimed =
                (store: ISessionStore).ClaimCompletionOutbox(owner, MaxBatchSize, settings.ClaimLeaseDuration, ct)

            let mutable delivered = 0

            for entry in claimed do
                ct.ThrowIfCancellationRequested()
                let! marked = deliverOneAsync store routes owner entry settings clock delay log ct

                if marked then
                    delivered <- delivered + 1

            return delivered
        }

    let purgeDeliveredAsync (store: ISessionStore) retention (clock: TimeProvider) ct =
        store.PurgeDeliveredCompletions(clock.GetUtcNow() - retention, ct)

    let passOnceAsync store routes owner (settings: CompletionOptions) clock delay log ct : Task =
        task {
            let! _ = deliverPendingAsync store routes owner settings clock delay log ct
            let! _ = purgeDeliveredAsync store settings.DeliveredRetention clock ct
            return ()
        }

type internal CompletionRedriverService
    (provider: IServiceProvider, options: IOptions<LegateOptions>, clock: TimeProvider, delay: ILlmDelay) =
    let lifetime = new CancellationTokenSource()
    let mutable loop: Task | null = null
    let owner = Guid.NewGuid().ToString("N")

    let log: ILogger =
        match provider.GetService<ILogger<CompletionRedriverService>>() with
        | null -> NullLogger.Instance
        | logger -> logger

    member _.Owner = owner

    member private _.RunAsync() : Task =
        task {
            let store = provider.GetRequiredService<ISessionStore>()
            let routes = provider.GetRequiredService<CompletionDestinations>()

            while not lifetime.IsCancellationRequested do
                try
                    // A new owner for each batch prevents stale in-flight attempts from reusing authority.
                    do!
                        CompletionRedriver.passOnceAsync
                            store
                            routes
                            (owner + Guid.NewGuid().ToString("N"))
                            options.Value.Completion
                            clock
                            delay
                            log
                            lifetime.Token
                with
                | :? OperationCanceledException -> ()
                | _ -> log.LogWarning("Completion redrive pass failed; pending rows will retry.")

                if not lifetime.IsCancellationRequested then
                    try
                        do! delay.Delay(options.Value.Completion.RedriveInterval, lifetime.Token)
                    with :? OperationCanceledException ->
                        ()
        }

    interface IHostedService with
        member this.StartAsync(_) =
            loop <- Task.Run(Func<Task>(fun () -> this.RunAsync()), lifetime.Token)
            Task.CompletedTask

        member _.StopAsync(ct) : Task =
            task {
                lifetime.Cancel()

                match loop with
                | null -> ()
                | running ->
                    try
                        do! running.WaitAsync(ct)
                    with :? OperationCanceledException ->
                        ()
            }

    interface IDisposable with
        member _.Dispose() =
            lifetime.Cancel()
            lifetime.Dispose()

module internal CompletionRedriverRegistration =
    let register (services: IServiceCollection) =
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, CompletionRedriverService>())
        |> ignore
