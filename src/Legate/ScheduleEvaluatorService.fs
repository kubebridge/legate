// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks
open Cronos
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open Microsoft.Extensions.Options

// Agent schedule evaluator (issue 121). Each tick lists the swept tenant's
// agents with enabled schedules through the existing
// ListAgentsWithEnabledSchedules query, computes each agent's latest due
// occurrence from its 5-field cron plus time zone, atomically consumes that
// occurrence key through the store, and on a win opens a headless session
// bound to the agent and prompts it once with the schedule message (Queue
// delivery). Missed occurrences are never backfilled: only the latest due
// occurrence per agent fires, while every older due occurrence in the
// sweep window is consumed without firing so a second instance cannot fire
// it. Consume-first-then-fire gives at-most-once firing with exactly-once
// consumption: a crash between the consume and the prompt loses one firing,
// never duplicates one. No external services: only IAgentStore, the
// TimeProvider clock, and the ILlmDelay seam, so Legate still builds and
// tests with no Redis, Docker, Postgres, or Kubernetes.

// ──────────────────────────────────────────────────────────────────────────
// One pass

/// One evaluator pass over agents with enabled schedules. Internal so no
/// store type ever crosses the public API; tests drive
/// <c>passOnceAsync</c> directly under virtual time with a recording fire
/// callback.
module internal ScheduleEvaluator =

    /// The lookback windows one pass sweeps for due occurrences, narrowest
    /// first: the first window holding any due occurrence wins, so its
    /// latest is the overall latest and every older occurrence in it is
    /// consumed without firing. Narrow windows keep high-frequency crons
    /// cheap; the widest covers yearly crons after long downtimes.
    let lookbackWindows: TimeSpan list =
        [
            TimeSpan.FromMinutes 10.0
            TimeSpan.FromHours 2.0
            TimeSpan.FromHours 48.0
            TimeSpan.FromDays 32.0
            TimeSpan.FromDays 370.0
        ]

    /// How many occurrences one window collects at most: bounds a
    /// high-frequency cron swept over a wide window after a long downtime.
    [<Literal>]
    let MaxOccurrencesPerWindow = 10000

    /// Shapes the occurrence key the store consumes exactly once:
    /// <c>{agentId}:{cron}:{occurrenceTicks}</c> with the occurrence
    /// instant in UTC, so every instance derives the same key.
    /// <param name="agentId">The agent the occurrence fired for.</param>
    /// <param name="cron">The cron expression the occurrence was derived from.</param>
    /// <param name="occurrenceUtc">The occurrence instant in UTC.</param>
    /// <returns>The deterministic occurrence key.</returns>
    let occurrenceKey (agentId: AgentId) (cron: string) (occurrenceUtc: DateTimeOffset) : string =
        sprintf "%O:%s:%d" agentId cron occurrenceUtc.UtcTicks

    /// Collects the occurrence instants in (from, now] by walking Cronos
    /// forward from the window start, capped at
    /// <see cref="F:Legate.ScheduleEvaluator.MaxOccurrencesPerWindow" />.
    /// <param name="expression">The parsed 5-field cron expression.</param>
    /// <param name="zone">The time zone the schedule fires in.</param>
    /// <param name="from">The exclusive window start.</param>
    /// <param name="now">The inclusive window end.</param>
    /// <returns>The due occurrences in ascending order.</returns>
    let collectDue
        (expression: CronExpression)
        (zone: TimeZoneInfo)
        (from: DateTimeOffset)
        (now: DateTimeOffset)
        : DateTimeOffset list =
        ArgumentNullException.ThrowIfNull(expression)
        ArgumentNullException.ThrowIfNull(zone)

        let collected = ResizeArray<DateTimeOffset>()
        let mutable cursor = from
        let mutable go = true
        let mutable guard = 0

        while go && guard < MaxOccurrencesPerWindow do
            guard <- guard + 1
            let next: Nullable<DateTimeOffset> = expression.GetNextOccurrence(cursor, zone)

            if not next.HasValue || next.Value > now then
                go <- false
            else
                collected.Add(next.Value)
                cursor <- next.Value.AddTicks(1L)

        collected |> Seq.toList

    /// Finds the due occurrences for one agent at the clock: the first
    /// narrowest lookback window holding any occurrence wins.
    /// <param name="expression">The parsed 5-field cron expression.</param>
    /// <param name="zone">The time zone the schedule fires in.</param>
    /// <param name="now">The instant to measure due-ness against.</param>
    /// <returns>The winning window's due occurrences in ascending order; empty when nothing is due.</returns>
    let dueOccurrences (expression: CronExpression) (zone: TimeZoneInfo) (now: DateTimeOffset) : DateTimeOffset list =
        ArgumentNullException.ThrowIfNull(expression)
        ArgumentNullException.ThrowIfNull(zone)

        let rec sweep windows =
            match windows with
            | [] -> []
            | window :: rest ->
                match collectDue expression zone (now - window) now with
                | [] -> sweep rest
                | due -> due

        sweep lookbackWindows

    /// Sweeps one agent: consumes every due occurrence in the winning
    /// window and fires the latest on a consume win. Older missed
    /// occurrences are consumed without firing; an already-consumed latest
    /// produces zero effects. A per-agent failure skips the agent for this
    /// pass (the next interval retries); it never starves later agents.
    /// <param name="agentStore">The durable agent store.</param>
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agent">The agent to sweep.</param>
    /// <param name="clock">The clock due-ness reads.</param>
    /// <param name="fireAsync">Opens a session and prompts it once for a won occurrence.</param>
    /// <param name="cancellationToken">Abandons the sweep.</param>
    /// <returns>1 when the agent fired; otherwise 0.</returns>
    let sweepAgentAsync
        (agentStore: IAgentStore)
        (tenant: TenantId)
        (agent: Agent)
        (clock: TimeProvider)
        (fireAsync: Agent -> AgentSchedule -> DateTimeOffset -> CancellationToken -> Task)
        (cancellationToken: CancellationToken)
        : Task<int> =
        task {
            ArgumentNullException.ThrowIfNull(agentStore)
            ArgumentNullException.ThrowIfNull(clock)

            if isNull (box fireAsync) then
                raise (ArgumentNullException(nameof fireAsync))

            cancellationToken.ThrowIfCancellationRequested()

            try
                match agent.Schedule with
                | null -> return 0
                | schedule when not schedule.Enabled -> return 0
                | schedule ->
                    if not (AgentScheduleRules.TryValidateCron schedule.Cron) then
                        return 0
                    elif not (AgentScheduleRules.TryValidateTimeZone schedule.TimeZone) then
                        return 0
                    else
                        let zone = AgentScheduleRules.ResolveTimeZone schedule.TimeZone
                        let expression = CronExpression.Parse(schedule.Cron, CronFormat.Standard)
                        let now = clock.GetUtcNow()

                        match dueOccurrences expression zone now with
                        | [] -> return 0
                        | due ->
                            let latest = due |> List.max
                            let latestUtc = latest.ToUniversalTime()

                            for occurrence in due do
                                if occurrence < latest then
                                    let key = occurrenceKey agent.Id schedule.Cron (occurrence.ToUniversalTime())

                                    let! _ =
                                        agentStore.TryConsumeScheduleOccurrence(
                                            tenant,
                                            agent.Id,
                                            key,
                                            occurrence.ToUniversalTime(),
                                            cancellationToken
                                        )

                                    ()

                            let latestKey = occurrenceKey agent.Id schedule.Cron latestUtc

                            let! outcome =
                                agentStore.TryConsumeScheduleOccurrence(
                                    tenant,
                                    agent.Id,
                                    latestKey,
                                    latestUtc,
                                    cancellationToken
                                )

                            match outcome with
                            | :? ScheduleOccurrenceConsumed ->
                                do! fireAsync agent schedule latestUtc cancellationToken
                                return 1
                            | _ -> return 0
            with
            | :? OperationCanceledException as cancelled -> return raise cancelled
            | _ -> return 0
        }

    /// Runs one full pass: sweeps every agent with an enabled schedule and
    /// fires each agent's latest due occurrence at most once.
    /// <param name="agentStore">The durable agent store.</param>
    /// <param name="tenant">The tenant whose scheduled agents to sweep.</param>
    /// <param name="clock">The clock due-ness reads.</param>
    /// <param name="fireAsync">Opens a session and prompts it once for a won occurrence.</param>
    /// <param name="cancellationToken">Abandons the pass.</param>
    /// <returns>How many agents fired.</returns>
    let passOnceAsync
        (agentStore: IAgentStore)
        (tenant: TenantId)
        (clock: TimeProvider)
        (fireAsync: Agent -> AgentSchedule -> DateTimeOffset -> CancellationToken -> Task)
        (cancellationToken: CancellationToken)
        : Task<int> =
        task {
            ArgumentNullException.ThrowIfNull(agentStore)
            ArgumentNullException.ThrowIfNull(clock)

            if isNull (box fireAsync) then
                raise (ArgumentNullException(nameof fireAsync))

            let! agents = agentStore.ListAgentsWithEnabledSchedules(tenant, cancellationToken)
            let mutable fired = 0

            if not (isNull (box agents)) then
                for agent in agents do
                    if not (isNull (box agent)) then
                        let! count = sweepAgentAsync agentStore tenant agent clock fireAsync cancellationToken
                        fired <- fired + count

            return fired
        }

// ──────────────────────────────────────────────────────────────────────────
// Production fire

/// Opens a headless session for a won schedule occurrence and prompts it
/// once with the schedule message. Internal: the service wires the resolved
/// session client; tests inject a recording callback instead.
module internal ScheduleFire =

    /// Opens the headless session (auto-close, allow-all permissions) and
    /// prompts it once over Queue delivery with the schedule message.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="agent">The agent the session converses with.</param>
    /// <param name="schedule">The schedule whose message the session is prompted with.</param>
    /// <param name="occurrenceUtc">The won occurrence instant in UTC, carried for observability.</param>
    /// <param name="cancellationToken">Cancels the open or the prompt.</param>
    /// <returns>A task that completes once the prompt lands.</returns>
    let fireAsync
        (client: SessionClient)
        (agent: Agent)
        (schedule: AgentSchedule)
        (occurrenceUtc: DateTimeOffset)
        (cancellationToken: CancellationToken)
        : Task =
        task {
            ArgumentNullException.ThrowIfNull(client)
            ArgumentNullException.ThrowIfNull(agent)
            ArgumentNullException.ThrowIfNull(schedule)

            let sessionOptions = SessionOptions()
            sessionOptions.AutoClose <- true
            sessionOptions.Title <- sprintf "Scheduled run of agent %s at %O" agent.Name occurrenceUtc

            sessionOptions.Permissions <- AllowAllPermissionPolicy() :> IPermissionPolicy

            let! session = client.OpenSessionAsync(agent.Id, sessionOptions, cancellationToken)

            let! _ =
                client.PromptAsync(
                    session.Id,
                    UserMessage.Text(schedule.Message),
                    DeliveryMode.Queue,
                    cancellationToken
                )

            ()
        }

// ──────────────────────────────────────────────────────────────────────────
// Hosted service

/// Singleton hosted service owning the schedule evaluator loop: one
/// evaluator pass every <c>Schedules:PollInterval</c> through
/// <see cref="M:Legate.ScheduleEvaluator.passOnceAsync*" /> over the
/// injected clock and delay seams, so virtual-time tests advance it without
/// sleeping. A pass failure is retried next interval; host stop cancels the
/// wait and the in-flight pass. The agent store, the session client, and
/// the facade tenant resolve lazily from the provider at loop start (never
/// at construction), so an empty host still fails startup with the
/// required-registration message instead of a resolution error.
/// Operational visibility rides the runtime's own logs: like
/// LocalActorSystemService this service takes no logger, so it stays
/// constructible on containers without logging.
type internal ScheduleEvaluatorService
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
    /// <returns>The tenant whose scheduled agents to sweep.</returns>
    member private _.SweepTenant() : TenantId =
        match serviceProvider.GetService<SessionClientOptions>() with
        | null -> TenantId.Default
        | clientOptions -> clientOptions.Tenant

    /// The tick interval for the loop: the configured poll interval when it
    /// is a positive bound, else the 60 s default. A non-positive result
    /// falls back to the default so a misconfigured host can never
    /// busy-loop; configuration validation rejects such values at startup.
    /// <returns>How long the loop waits between passes.</returns>
    member private _.TickInterval() : TimeSpan =
        let legateOptions = options.Value
        let fallback = TimeSpan.FromSeconds 60.0

        let candidate =
            if isNull (box legateOptions) || isNull (box legateOptions.Schedules) then
                fallback
            else
                legateOptions.Schedules.PollInterval

        if candidate > TimeSpan.Zero then candidate else fallback

    /// Runs one full tick: one evaluator pass over the swept tenant's
    /// agents with enabled schedules, firing each agent's latest due
    /// occurrence at most once.
    /// <param name="cancellationToken">Abandons the tick.</param>
    /// <returns>How many agents fired.</returns>
    member internal this.RunOnceAsync(cancellationToken: CancellationToken) : Task<int> =
        task {
            let agentStore = serviceProvider.GetRequiredService<IAgentStore>()
            let client = serviceProvider.GetRequiredService<SessionClient>()
            let tenant = this.SweepTenant()

            let! fired =
                ScheduleEvaluator.passOnceAsync
                    agentStore
                    tenant
                    timeProvider
                    (fun agent schedule occurrenceUtc ct ->
                        ScheduleFire.fireAsync client agent schedule occurrenceUtc ct)
                    cancellationToken

            if fired > 0 then
                log.LogInformation("Schedule evaluator fired {Fired} due occurrences.", fired)

            return fired
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
                    log.LogWarning(
                        "Schedule evaluator pass failed and will retry next interval: {Reason}",
                        failed.Message
                    )

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

/// Registers the schedule evaluator hosted service.
module internal ScheduleEvaluatorRegistration =

    /// Registers ScheduleEvaluatorService as a singleton hosted service,
    /// only when the host has not already supplied its own registration for
    /// the same implementation.
    /// <param name="services">The container to add the service to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, ScheduleEvaluatorService>())
        |> ignore
