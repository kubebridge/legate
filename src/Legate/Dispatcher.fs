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
// dispatcher-side ClaimNextTurn on the pending path: claim ownership and
// fencing stay with the actor's start path. A second internal sweep (issue
// 289) covers live-turn orphans: Idle sessions still carrying a live turn
// with a consumed inbox, whose journal holds an unterminated marker — for
// the live turn id itself, or for the orphan id a won prime replaced (the
// marker is the only surviving record of it) — on an era-marked session,
// which the pending sweep above can never list.
// Each orphan candidate is lease-gated through an atomic priming claim (a
// live owner wins, the poke loses silently and re-consumes its bootstrap)
// and a winner settles directly inside the sweep: it journals the fenced
// TurnFailedEvent for the orphan id under the won poke token and settles
// the prime quietly, with no entity involvement. Every settle effect stays
// fenced under the fresh primed claim token. No external services: only
// ISessionStore, the TimeProvider clock, and the ILlmDelay seam, so Legate
// still builds and tests with no Redis, Docker, Postgres, or Kubernetes.

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

    /// The claim owner identity the orphan poke primes under: distinct
    /// from the facade prime owner, so forensics tell a poke-won claim
    /// from an entity-start prime.
    let private orphanPokeOwner = "legate-dispatcher-orphan-poke"

    /// The bootstrap text the orphan poke appends before its priming
    /// claim: always consumed by a winning claim or re-consumed on a
    /// loss, so it never drains as a turn.
    let private orphanPokeText = "legate orphan poke"

    /// Reads the journal state for one turn id (issue 289): whether the
    /// journal holds the TurnStartedEvent marker for the id, whether
    /// any terminal row (TurnCompleted, TurnFailed, TurnAborted,
    /// SessionClosed) carries it, and the latest marker id with no
    /// terminal row (the journal-derived orphan, mirroring the
    /// entity-side journalOrphanState fallback). Read-only: never
    /// appends. A failed replay reads as no marker, so the candidate
    /// stays skipped.
    ///
    /// The fallback exists because a winning prime replaces the row's
    /// turn id: after a poke-claim wins post-expiry, CurrentTurnId names
    /// the prime, never the orphan, so marker-for-live-id alone would
    /// stay quiet forever. The orphan id survives only in the journal.
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to inspect.</param>
    /// <param name="liveId">The live turn to inspect.</param>
    /// <param name="cancellationToken">Abandons the replay.</param>
    /// <returns>The marker flag and the terminal flag for the turn id, plus the latest unterminated marker id.</returns>
    let private turnStateAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (liveId: TurnId)
        (cancellationToken: CancellationToken)
        : Task<bool * bool * TurnId option> =
        task {
            let mutable marker = false
            let mutable terminal = false
            let markers = ResizeArray<TurnId>()
            let terminals = HashSet<TurnId>()
            let mutable cursor = 0L
            let mutable paging = true

            while paging do
                try
                    let! outcome = eventStore.Replay(tenant, sessionId, cursor, 100, cancellationToken)

                    match outcome with
                    | :? EventReplayPage as page when not (isNull (box page)) ->
                        if not (isNull (box page.Events)) then
                            for event in page.Events do
                                if not (isNull (box event)) then
                                    if event.TurnId.Equals(liveId) then
                                        match event with
                                        | :? TurnStartedEvent -> marker <- true
                                        | :? TurnCompletedEvent
                                        | :? TurnFailedEvent
                                        | :? TurnAbortedEvent
                                        | :? SessionClosedEvent -> terminal <- true
                                        | _ -> ()

                                    match event with
                                    | :? TurnStartedEvent -> markers.Add(event.TurnId)
                                    | :? TurnCompletedEvent
                                    | :? TurnFailedEvent
                                    | :? TurnAbortedEvent
                                    | :? SessionClosedEvent -> terminals.Add(event.TurnId) |> ignore
                                    | _ -> ()

                        if page.NextCursor.HasValue then
                            cursor <- page.NextCursor.Value
                        else
                            paging <- false
                    | _ -> paging <- false
                with
                | :? OperationCanceledException as cancelled -> raise cancelled
                | _ -> paging <- false

            let fallback =
                markers
                |> Seq.filter (fun candidate -> not (terminals.Contains(candidate)))
                |> Seq.tryLast

            return marker, terminal, fallback
        }

    /// Runs one full pass: pages the tenant's dispatch candidates in bounded
    /// batches following HasMore and wakes every admitted session. The
    /// capacity snapshots read once per pass plus the local in-flight gate
    /// bound the overshoot: every wake in the pass counts against the
    /// limits it was admitted under. A batch with no unseen sessions ends
    /// the pass, so gate-blocked sessions that stay pending never spin it.
    /// A vanished session (null row mid-pass) or a per-session wake failure
    /// skips that session for this pass; the next interval retries.
    /// A second bounded sweep then settles live-turn orphans (issue 289):
    /// Idle sessions still carrying a live turn with an empty inbox page
    /// through ListSessions; each candidate must also hold an unterminated
    /// marker — for the live turn id itself, or for the orphan id a won
    /// prime replaced (the journal-derived fallback, mirroring the
    /// entity-side probe) — on an era-marked session before it lease-gates
    /// through an atomic priming claim, so a live owner always wins and
    /// the poke loses silently while an expired or missing lease lets the
    /// poke win and settle directly inside the sweep. Orphan pokes check
    /// the capacity gate but never consume it and never count as queued.
    /// <param name="store">The durable store.</param>
    /// <param name="eventStore">The journal orphan candidates read their terminal rows from.</param>
    /// <param name="eraMarked">Reads whether a session opened in the completion era.</param>
    /// <param name="tenant">The tenant whose pending sessions to sweep.</param>
    /// <param name="sessions">The session knobs carrying the process and per-tenant limits.</param>
    /// <param name="dispatcher">The dispatcher knobs carrying the batch size and the per-agent limit.</param>
    /// <param name="resolve">Resolves the session actor to wake. Never null.</param>
    /// <param name="clock">The clock dispatch latency reads.</param>
    /// <param name="cancellationToken">Abandons the pass.</param>
    /// <returns>What the pass woke and what stayed queued behind each limit.</returns>
    let passOnceAsync
        (store: ISessionStore)
        (eventStore: ISessionEventStore)
        (eraMarked: CompletionEra.CompletionEraReader)
        (tenant: TenantId)
        (sessions: SessionsOptions)
        (dispatcher: DispatcherOptions)
        (resolve: SessionId -> CancellationToken -> Task<IActorRef>)
        (clock: TimeProvider)
        (cancellationToken: CancellationToken)
        : Task<DispatchPassResult> =
        task {
            ArgumentNullException.ThrowIfNull(store)
            ArgumentNullException.ThrowIfNull(eventStore)
            ArgumentNullException.ThrowIfNull(sessions)
            ArgumentNullException.ThrowIfNull(dispatcher)
            ArgumentNullException.ThrowIfNull(clock)

            if isNull (box resolve) then
                raise (ArgumentNullException(nameof resolve))

            if isNull (box eraMarked) then
                raise (ArgumentNullException(nameof eraMarked))

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

            // The cached per-agent snapshot plus this pass's local
            // in-flight count for one agent: shared by the pending sweep
            // and the orphan sweep below, so both gate on the same
            // snapshots.
            let agentCountsAsync (agentId: AgentId) : Task<int * int> =
                task {
                    let! snapshot =
                        match agentSnapshots.TryGetValue(agentId) with
                        | true, count -> task { return count }
                        | false, _ ->
                            task {
                                let! count = store.CountSessionsByAgent(tenant, agentId, cancellationToken)

                                agentSnapshots[agentId] <- count
                                return count
                            }

                    let startedLocal =
                        match agentLocal.TryGetValue(agentId) with
                        | true, count -> count
                        | false, _ -> 0

                    return snapshot, startedLocal
                }

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
                            match store with
                            | :? ISessionAbortControlStore as control ->
                                let! target = control.ReadAbortTarget(tenant, sessionId, cancellationToken)

                                if not (isNull (box target)) then
                                    raise (
                                        InvalidSessionStateException(
                                            sessionId,
                                            "controlPending",
                                            "Dispatcher cannot activate persisted control work or select unrelated inbox entries."
                                        )
                                    )
                            | _ -> ()

                            let! session = store.GetSession(tenant, sessionId, cancellationToken)

                            match session with
                            | null -> () // Vanished mid-pass: stays out of this pass.
                            | live ->
                                let! agentCount, agentStarted = agentCountsAsync live.AgentId

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

            // Orphan sweep (issue 289): Idle sessions still carrying a
            // live turn with a fully consumed inbox never appear in the
            // pending sweep above, yet their journal may hold an
            // unterminated marker — for the live turn id itself, or for
            // the orphan id a won prime replaced (the marker is the only
            // surviving record of it) — on an era-marked session. Each
            // candidate lease-gates through an atomic priming claim: a
            // live owner makes the claim the missing branch, so the poke
            // loses silently and re-consumes its bootstrap, while an
            // expired or missing lease lets the poke win and settle
            // directly inside the sweep, so a later poke re-finds the
            // orphan through the fallback instead of going single-shot.
            // Sessions with pending work stay with the authoritative
            // sweep, and a tripped capacity gate skips the poke silently:
            // orphans are recovery probes, not dispatch demand, so they
            // never consume the gate and never count as queued. Bounded
            // to MaxBatchSize primes per pass, following the ListSessions
            // continuation like the HasMore paging above.
            let mutable orphanBudget = maxBatch
            let mutable orphanContinuation: string | null = null
            let mutable orphanPaging = true

            while orphanPaging && orphanBudget > 0 do
                let! page =
                    store.ListSessions(
                        tenant,
                        Nullable(SessionState.Idle),
                        Nullable<AgentId>(),
                        Nullable<DateTimeOffset>(),
                        Nullable<DateTimeOffset>(),
                        maxBatch,
                        orphanContinuation,
                        cancellationToken
                    )

                if isNull (box page) || isNull (box page.Items) then
                    orphanPaging <- false
                else
                    for row in page.Items do
                        if orphanBudget > 0 && not (isNull (box row)) && row.CurrentTurnId.HasValue then
                            try
                                match store with
                                | :? ISessionAbortControlStore as control ->
                                    let! target = control.ReadAbortTarget(tenant, row.Id, cancellationToken)

                                    if not (isNull (box target)) then
                                        raise (
                                            InvalidSessionStateException(
                                                row.Id,
                                                "controlPending",
                                                "Dispatcher orphan prime is forbidden for unresolved control work."
                                            )
                                        )
                                | _ -> ()

                                let! fresh = store.GetSession(tenant, row.Id, cancellationToken)

                                match fresh with
                                | null -> ()
                                | current when current.State = SessionState.Closed -> ()
                                | current ->
                                    let! agentCount, agentStarted = agentCountsAsync current.AgentId

                                    if runningSnapshot + runningLocal >= sessions.Capacity then
                                        ()
                                    elif tenantSnapshot + tenantLocal >= sessions.MaxSessionsPerTenant then
                                        ()
                                    elif agentCount + agentStarted >= dispatcher.MaxSessionsPerAgent then
                                        ()
                                    else
                                        let! pending = store.ReadPendingInbox(tenant, current.Id, cancellationToken)

                                        let hasPending =
                                            not (isNull (box pending))
                                            && pending |> Seq.exists (fun entry -> not (isNull (box entry)))

                                        if not hasPending then
                                            // Completion-row-aware plus era-gated candidate (issue 289):
                                            // the settling id is the live turn id when its marker
                                            // stands unterminated, else the journal-derived orphan
                                            // the prime replaced, on an era-marked session.
                                            // Terminated tails, settled rows, empty tails, and
                                            // pre-era sessions never consume the orphan budget.
                                            let liveOpt =
                                                if current.CurrentTurnId.HasValue then
                                                    Some current.CurrentTurnId.Value
                                                else
                                                    None

                                            let! settlingOpt =
                                                task {
                                                    match liveOpt with
                                                    | None -> return None
                                                    | Some liveId ->
                                                        let! marker, terminal, fallback =
                                                            turnStateAsync
                                                                eventStore
                                                                tenant
                                                                current.Id
                                                                liveId
                                                                cancellationToken

                                                        let settling =
                                                            if marker && not terminal then Some liveId else fallback

                                                        match settling with
                                                        | None -> return None
                                                        | Some _ ->
                                                            try
                                                                let! marked =
                                                                    eraMarked tenant current.Id cancellationToken

                                                                if marked then return settling else return None
                                                            with _ ->
                                                                return None
                                                }

                                            match settlingOpt with
                                            | None -> ()
                                            | Some settlingId ->
                                                orphanBudget <- orphanBudget - 1

                                                let bootstrap =
                                                    UserMessagePayload(UserMessage.Text(orphanPokeText)) :> InboxPayload

                                                let! appended =
                                                    store.AppendInboxMessage(
                                                        tenant,
                                                        current.Id,
                                                        bootstrap,
                                                        DeliveryMode.Queue,
                                                        cancellationToken
                                                    )

                                                let cleanupPoke () : Task<unit> =
                                                    task {
                                                        try
                                                            let! _ =
                                                                store.MarkInboxConsumed(
                                                                    tenant,
                                                                    current.Id,
                                                                    [| appended.Position |] :> IReadOnlyList<int64>,
                                                                    cancellationToken
                                                                )

                                                            ()
                                                        with _ ->
                                                            ()
                                                    }

                                                let settleDirect (claim: TurnClaim) : Task<unit> =
                                                    task {
                                                        try
                                                            let failedEvent =
                                                                TurnFailedEvent(
                                                                    current.Id,
                                                                    settlingId,
                                                                    Nullable<int64>(),
                                                                    clock.GetUtcNow(),
                                                                    SessionActor.CrashFailReason
                                                                )
                                                                :> SessionEvent

                                                            let events =
                                                                ResizeArray<SessionEvent>([| failedEvent |])
                                                                :> IReadOnlyList<SessionEvent>

                                                            let! _ =
                                                                JournalWriter.appendWithTokenAsync
                                                                    eventStore
                                                                    tenant
                                                                    current.Id
                                                                    claim.Token
                                                                    events
                                                                    cancellationToken

                                                            ()
                                                        with _ ->
                                                            ()

                                                        try
                                                            let! _ =
                                                                store.SettleTurn(
                                                                    tenant,
                                                                    claim,
                                                                    TurnStatus.Completed,
                                                                    null,
                                                                    cancellationToken
                                                                )

                                                            ()
                                                        with _ ->
                                                            ()

                                                        do! cleanupPoke ()
                                                        started <- started + 1
                                                    }

                                                try
                                                    let! lease =
                                                        store.ClaimNextTurn(
                                                            tenant,
                                                            current.Id,
                                                            orphanPokeOwner,
                                                            sessions.LeaseDuration,
                                                            cancellationToken
                                                        )

                                                    match lease with
                                                    | :? TurnLeaseRenewed as renewed -> do! settleDirect renewed.Claim
                                                    | :? TurnLeaseHeld as held -> do! settleDirect held.Claim
                                                    | :? TurnLeaseExpiring as expiring ->
                                                        do! settleDirect expiring.Claim
                                                    | _ -> do! cleanupPoke ()
                                                with
                                                | :? OperationCanceledException as cancelled ->
                                                    do! cleanupPoke ()
                                                    raise cancelled
                                                | _ -> do! cleanupPoke ()
                            with
                            | :? OperationCanceledException as cancelled -> raise cancelled
                            | _ -> ()

                    orphanContinuation <- page.Continuation
                    orphanPaging <- not (isNull (box page.Continuation))

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
            let eventStore = serviceProvider.GetRequiredService<ISessionEventStore>()
            let client = serviceProvider.GetRequiredService<SessionClient>()
            let sink = serviceProvider.GetRequiredService<DispatcherWakeSink>()
            let tenant = this.SweepTenant()

            // Completion era (issue 289): the gate the orphan sweep reads;
            // an absent (or reader-less) registration reads pre-era quiet.
            let eraMarked: CompletionEra.CompletionEraReader =
                match serviceProvider.GetService<CompletionEra.CompletionEraGate>() with
                | null -> CompletionEra.preEraGate.Reader
                | gate when isNull (box gate.Reader) -> CompletionEra.preEraGate.Reader
                | gate -> gate.Reader

            sink.TakePending() |> ignore

            let resolve (sessionId: SessionId) (candidateToken: CancellationToken) : Task<IActorRef> =
                client.Resolve(sessionId, candidateToken)

            return!
                Dispatcher.passOnceAsync
                    store
                    eventStore
                    eraMarked
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
