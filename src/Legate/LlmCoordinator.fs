// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.IO
open System.Net.Http
open System.Net.Sockets
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging

// Local per-identity LLM coordinator (issue 54). One coordinator serves
// every provider and credential scope in the process: each coordinated call
// resolves its model reference through the registry, derives the identity
// key providerId/scope (the literal "static" when the provider's configured
// ApiKey is set, otherwise the session tenant whose key comes from the
// registered IApiKeyProvider), and runs under that identity's admission
// state. Admission enforces the LlmCoordinationOptions knobs: a concurrency
// gate with a FIFO queue capped by MaxQueuedRequests (overflow raises
// AdmissionRejectedException), RPM and TPM sliding windows over the last
// minute, a shared cooldown after HTTP 429, jittered retry on transient
// failures only (408, 429, 5xx, and network errors; other 4xx surface
// immediately), and the turn deadline from RequestTimeoutSeconds (expiry
// raises DeadlineExceededException). TPM accounting reserves the caller's
// token estimate plus EstimatedOutputTokens at admission and reconciles the
// reservation with the provider-reported UsageSummary on success. Raw API
// keys never enter identity keys, dictionaries, logs, or diagnostics; when
// neither key source has a key the call fails fast with ProviderException
// before anything queues. When LlmOptions.DistributedCoordination is true
// with Mode Redis and a registered admission client, the call admits
// through the distributed seam first (the seam owns the combined
// concurrency limit, FIFO queueing, and shared cooldown) while the local
// RPM/TPM sliding windows still apply per process; the seam lease TTLs
// derive from the turn deadline so no renewal runs on this path.
// Everything time-shaped runs off the
// injected TimeProvider (the deadline timer), ILlmDelay (rate, cooldown,
// and backoff waits), and ILlmRandom (retry jitter) seams: no Task.Delay,
// no DateTime.UtcNow, no Random. Rejected designs: a coordinator actor per
// identity (heavier lifecycle for a few counters and queues) and new
// exception types (the existing AdmissionRejected, DeadlineExceeded,
// Provider, and ProviderNotRegistered shapes already carry every failure).
module internal LlmCoordination =

    /// The credential scope shared by every tenant when the provider's
    /// configured <see cref="P:Legate.LlmProviderOptions.ApiKey" /> is set:
    /// one bucket for all tenants, and the static key wins when both key
    /// sources exist.
    [<Literal>]
    let StaticScope = "static"

    /// The <see cref="T:Legate.AdmissionRejectedException" /> reason carried
    /// when the FIFO queue already holds MaxQueuedRequests waiters.
    [<Literal>]
    let QueueFullReason = "queueFull"

    /// The <see cref="T:Legate.DeadlineExceededException" /> operation name
    /// carried when the turn deadline fires.
    [<Literal>]
    let ExecuteOperationName = "Execute"

    /// The <see cref="T:Legate.AdmissionRejectedException" /> reason carried
    /// when fail-closed rejects because the distributed seam is unavailable.
    [<Literal>]
    let DistributedUnavailableReason = "distributedUnavailable"

    /// The emergency fail-open concurrency bound: one call per identity per
    /// process, strictly below the normal default, so a Redis outage admits
    /// progress without a stampede.
    [<Literal>]
    let EmergencyMaxConcurrency = 1

    /// The emergency fail-open FIFO queue cap per identity per process.
    [<Literal>]
    let EmergencyMaxQueuedRequests = 16

    /// The sliding window RPM and TPM accounting covers: the last minute.
    let private rateWindow = TimeSpan.FromMinutes 1.0

    /// The distributed-admit poll cadence while the seam reports Queued.
    let private distributedPollInterval = TimeSpan.FromSeconds 1.0

    /// Releases one owner's distributed lease, best-effort: a release
    /// failure never fails the call. Callers resolve non-null arguments
    /// first: the distributed path always holds a lease owner.
    /// <param name="admission">The seam that holds the lease.</param>
    /// <param name="identity">The coordination identity.</param>
    /// <param name="ownerId">The owner releasing.</param>
    let private releaseLeaseGuarded (admission: IDistributedLlmAdmission) (identity: string) (ownerId: string) : Task =
        task {
            try
                let! _ = admission.ReleaseAsync(identity, ownerId, CancellationToken.None)
                ()
            with _ ->
                ()
        }

    /// Starts a shared cooldown on the seam, best-effort: a propagation
    /// failure never fails the retry. Skips non-positive pauses (the seam
    /// requires a positive TTL).
    /// <param name="admission">The seam owning the shared cooldown.</param>
    /// <param name="identity">The coordination identity.</param>
    /// <param name="pause">How long new work is refused.</param>
    let private startCooldownGuarded (admission: IDistributedLlmAdmission) (identity: string) (pause: TimeSpan) : Task =
        task {
            if pause > TimeSpan.Zero then
                try
                    do! admission.StartCooldownAsync(identity, pause, CancellationToken.None)
                with _ ->
                    ()
        }

    /// Reports whether an HTTP status is transient: 408, 429, or 5xx. Every
    /// other status, including the remaining 4xx, surfaces immediately.
    /// <param name="status">The HTTP status to classify.</param>
    /// <returns>true for 408, 429, and 500-599; otherwise false.</returns>
    let private isTransientStatus (status: int) : bool =
        status = 408 || status = 429 || (status >= 500 && status <= 599)

    /// Wraps a network-shaped failure (no HTTP status) in a
    /// <see cref="T:Legate.ProviderException" /> naming the provider and the
    /// failure kind. The raw message is never embedded: it may carry
    /// secrets or tool arguments.
    /// <param name="providerId">The provider the call targeted.</param>
    /// <param name="failure">The network failure to wrap.</param>
    /// <returns>The provider exception the retry loop classifies.</returns>
    let private wrapProviderError (providerId: string) (failure: exn) : ProviderException =
        ProviderException(
            providerId,
            Nullable<int>(),
            Nullable<TimeSpan>(),
            sprintf "The '%s' provider call failed with %s." providerId (failure.GetType().Name)
        )

    /// Draws the jittered backoff for one retry: uniform in
    /// [minBackoff, maxBackoff) from the injected random seam, so a seeded
    /// random makes retry timings reproducible. A zero-width range returns
    /// the bound itself without touching the seam.
    /// <param name="random">The injected jitter seam.</param>
    /// <param name="minBackoff">The minimum backoff between retries.</param>
    /// <param name="maxBackoff">The maximum backoff between retries.</param>
    /// <returns>The backoff to wait before the next attempt.</returns>
    let private jitteredBackoff (random: ILlmRandom) (minBackoff: TimeSpan) (maxBackoff: TimeSpan) : TimeSpan =
        let floor = minBackoff.TotalMilliseconds
        let width = maxBackoff.TotalMilliseconds - floor

        if width <= 0.0 then
            minBackoff
        else
            minBackoff + TimeSpan.FromMilliseconds(random.NextDouble() * width)

    /// The value one coordinated provider call produced, with the usage the
    /// provider reported for it. The coordinator reconciles its TPM
    /// reservation against <see cref="T:Legate.UsageSummary" />
    /// (a null usage counts as zero, mirroring the turn loop's null
    /// tolerance) and hands the value back to the caller.
    /// <typeparam name="T">The value the provider call produced.</typeparam>
    type LlmCallOutcome<'T> =
        {
            /// The value the provider call produced.
            Value: 'T
            /// The usage the provider reported, or null when it reported none.
            Usage: UsageSummary
        }

    /// In-memory registry resolving model references to the providers that
    /// serve them. Hosts register provider instances; resolution matches the
    /// reference's canonical lowercase provider segment and misses with
    /// <see cref="T:Legate.ProviderNotRegisteredException" /> listing the
    /// registered ids (never secrets). Thread-safe; a repeated registration
    /// under the same id replaces the previous one.
    [<Sealed>]
    type LlmProviderRegistry() =
        let gate = obj ()
        let providers = Dictionary<string, ILlmProvider>(StringComparer.Ordinal)

        /// Registers a provider under its canonical lowercase id, replacing
        /// any previous registration under that id.
        /// <param name="provider">The provider to serve its provider id.</param>
        /// <exception cref="T:System.ArgumentNullException">The provider is null.</exception>
        /// <exception cref="T:System.ArgumentException">The provider id is blank.</exception>
        member _.Register(provider: ILlmProvider) : unit =
            ArgumentNullException.ThrowIfNull(provider)

            if String.IsNullOrWhiteSpace provider.Id then
                raise (ArgumentException("The provider id must be a non-empty string.", nameof provider))

            lock gate (fun () -> providers[provider.Id.ToLowerInvariant()] <- provider)

        interface ILlmProviderRegistry with
            member _.Resolve(reference: ModelReference) =
                let found, provider =
                    lock gate (fun () ->
                        if providers.ContainsKey reference.Provider then
                            true, providers[reference.Provider]
                        else
                            false, Unchecked.defaultof<ILlmProvider>)

                if found && not (isNull (box provider)) then
                    provider
                else
                    let registered = lock gate (fun () -> providers.Keys |> Seq.sort |> List.ofSeq)

                    if registered.IsEmpty then
                        raise (
                            ProviderNotRegisteredException(
                                reference.Provider,
                                (registered :> IReadOnlyList<string>),
                                "No LLM providers are registered."
                            )
                        )
                    else
                        raise (
                            ProviderNotRegisteredException(
                                reference.Provider,
                                (registered :> IReadOnlyList<string>),
                                sprintf
                                    "No LLM provider is registered under '%s'. Registered providers: %s."
                                    reference.Provider
                                    (String.Join(", ", registered))
                            )
                        )

            member _.RegisteredProviders =
                lock gate (fun () -> providers.Keys |> Seq.sort |> List.ofSeq :> IReadOnlyList<string>)

    /// Why an admission attempt did not run yet.
    type AdmissionStep =
        /// The call holds a concurrency slot and a TPM reservation: run it.
        | Admitted
        /// The FIFO queue already holds MaxQueuedRequests waiters: reject.
        | Rejected
        /// Blocked on the concurrency gate or behind earlier waiters: wait
        /// for the admission pulse, which every state change sends.
        | AwaitSignal
        /// Blocked on the shared cooldown or a rate window: wait for the
        /// pulse or the given hint, whichever comes first.
        | AwaitTimed of TimeSpan

    /// What one guarded distributed acquire produced: the seam outcome to
    /// classify, or the fail-open fallback to emergency local admission.
    type private DistributedAcquireStep =
        | SeamOutcome of DistributedAdmissionOutcome
        | SeamFellBack

    /// One TPM ledger entry: the admission instant plus the reserved token
    /// count, reconciled with the actual usage when the call settles.
    type TokenEntry =
        {
            /// When the call was admitted.
            Instant: DateTimeOffset
            /// The reserved estimate, then the actual usage after settling.
            mutable Tokens: int64
        }

    /// An async condition variable: waiters hold the current task and every
    /// admission mutation replaces and completes it. All access happens
    /// under the coordinator gate.
    type Pulse() =
        let mutable current =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        /// The task a waiter holds until the next admission mutation.
        member _.Current: Task = current.Task :> Task

        /// Completes the waiters holding the current task and installs a
        /// fresh one for later waiters.
        member _.Signal() =
            let previous = current
            current <- TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            previous.TrySetResult() |> ignore

    /// The admission state for one identity key. All members mutate only
    /// under the coordinator gate.
    type IdentityState() =
        /// The calls currently holding concurrency slots.
        member val Active = 0 with get, set
        /// The queued waiters in FIFO order; only the head may admit.
        member val Waiters = ResizeArray<TaskCompletionSource<unit>>() with get
        /// The admission instants inside the RPM window.
        member val RequestTimes = ResizeArray<DateTimeOffset>() with get
        /// The reserved-or-actual token entries inside the TPM window.
        member val TokenLedger = ResizeArray<TokenEntry>() with get
        /// When the shared 429 cooldown lifts; MinValue means no cooldown.
        member val CooldownUntil = DateTimeOffset.MinValue with get, set
        /// The pulse every admission mutation sends.
        member val Signal = Pulse() with get

    /// The per-call parameters the retry loop threads through attempts.
    /// A record (rather than sixteen closure parameters) so the loop lives
    /// at module level, where generic recursion is routine.
    /// <typeparam name="T">The value the provider call produces.</typeparam>
    type AttemptContext<'T> =
        {
            /// The chat client the provider built for the reference.
            Client: IChatClient
            /// The provider call, receiving the client and the deadline-linked token.
            Invoke: Func<IChatClient, CancellationToken, Task<LlmCallOutcome<'T>>>
            /// The caller token linked with the deadline scope.
            LinkedToken: CancellationToken
            /// The caller's own token, distinguishing aborts from deadlines.
            CallerToken: CancellationToken
            /// True only when the deadline scope fired on its own.
            IsDeadlineFired: unit -> bool
            /// The instant the turn deadline expires.
            Deadline: DateTimeOffset
            /// Builds the deadline failure for this identity.
            DeadlineEx: unit -> DeadlineExceededException
            /// The provider the call targets, for failure messages.
            ProviderId: string
            /// The retry attempts per failed call.
            RetryCount: int
            /// The minimum jittered backoff between retries.
            MinBackoff: TimeSpan
            /// The maximum jittered backoff between retries.
            MaxBackoff: TimeSpan
            /// The pause after an HTTP 429 before requests resume.
            Cooldown: TimeSpan
            /// The injected clock.
            Clock: TimeProvider
            /// The injected wait seam.
            Delay: ILlmDelay
            /// The injected jitter seam.
            Random: ILlmRandom
            /// The coordinator gate guarding the admission state.
            Gate: obj
            /// This call's identity state.
            State: IdentityState
            /// This call's TPM ledger entry.
            Entry: TokenEntry
            /// The resolved logger the retry loop reports through.
            Log: ILogger
            /// The six-key scope every coordinator log line carries.
            LogScope: IReadOnlyList<KeyValuePair<string, obj>>
            /// True on the distributed path: settle reconciles the local
            /// token ledger without touching the concurrency gate and
            /// releases the seam lease; HTTP 429s also propagate to the
            /// seam cooldown. False on the local and emergency paths.
            Distributed: bool
            /// The seam the distributed path admits through, or null on the
            /// local and emergency paths.
            Admission: IDistributedLlmAdmission | null
            /// The identity the call admitted under (local and distributed).
            IdentityKey: string
            /// The owner holding the distributed lease, or null on the
            /// local and emergency paths.
            OwnerId: string | null
        }

    /// Reconciles the reservation with the actual usage and releases the
    /// concurrency slot, pulsing the waiters. On the distributed path the
    /// gate is untouched (the seam owns concurrency) and the seam lease is
    /// released best-effort instead.
    /// <param name="context">The per-call parameters.</param>
    /// <param name="actualTokens">The input plus output tokens the provider reported.</param>
    let private settleSuccess<'T> (context: AttemptContext<'T>) (actualTokens: int64) : Task =
        task {
            lock context.Gate (fun () ->
                context.Entry.Tokens <- actualTokens

                if not context.Distributed then
                    context.State.Active <- context.State.Active - 1
                    context.State.Signal.Signal())

            if context.Distributed then
                match box context.Admission, box context.OwnerId with
                | (:? IDistributedLlmAdmission as live), (:? string as owner) ->
                    do! releaseLeaseGuarded live context.IdentityKey owner
                | _ -> ()
        }

    /// Releases the reservation and the concurrency slot without
    /// reconciling (no usage was reported), pulsing the waiters. On the
    /// distributed path the gate is untouched and the seam lease is
    /// released best-effort instead.
    /// <param name="context">The per-call parameters.</param>
    let private settleFailure<'T> (context: AttemptContext<'T>) : Task =
        task {
            lock context.Gate (fun () ->
                context.State.TokenLedger.Remove(context.Entry) |> ignore

                if not context.Distributed then
                    context.State.Active <- context.State.Active - 1
                    context.State.Signal.Signal())

            if context.Distributed then
                match box context.Admission, box context.OwnerId with
                | (:? IDistributedLlmAdmission as live), (:? string as owner) ->
                    do! releaseLeaseGuarded live context.IdentityKey owner
                | _ -> ()
        }

    /// Authorises one tenant-provider-model call before the runtime
    /// resolves the provider: a deny throws
    /// <see cref="T:Legate.ModelDeniedException" /> carrying the policy's
    /// client-safe message, so deny outranks
    /// <see cref="T:Legate.ProviderNotRegisteredException" /> and the
    /// missing-key <see cref="T:Legate.ProviderException" />, and no key
    /// derivation or client build happens on deny. A null policy allows;
    /// a null decision or an unknown decision shape is a host bug and
    /// throws InvalidOperationException.
    /// <param name="policy">The host model policy, or null for no gate.</param>
    /// <param name="tenant">The tenant making the call.</param>
    /// <param name="providerId">The provider segment of the model reference.</param>
    /// <param name="model">The model segment of the model reference.</param>
    let private authorizeFirst
        (policy: IModelPolicy | null)
        (tenant: TenantId)
        (providerId: string)
        (model: string)
        : unit =
        match box policy with
        | null -> ()
        | :? IModelPolicy as live ->
            let decision = live.Authorize(tenant, providerId, model)

            if isNull (box decision) then
                raise (InvalidOperationException("The model policy returned null instead of a decision."))
            else
                match decision with
                | :? ModelAllowed -> ()
                | :? ModelDenied as denied ->
                    let message =
                        if isNull (box denied.Message) then
                            "The model call was denied by the model policy."
                        else
                            denied.Message

                    raise (ModelDeniedException(providerId, model, message))
                | _ ->
                    raise (
                        InvalidOperationException(
                            sprintf "The model policy returned an unknown decision: %s." (decision.GetType().FullName)
                        )
                    )
        | _ -> raise (InvalidOperationException("The model policy has an unknown shape."))

    /// Reports one successful call as a single usage checkpoint carrying
    /// the terminal outcome usage. Guarded: a throwing observer never
    /// kills the call (the Compaction.reportCheckpoint precedent).
    /// <param name="observer">The host usage observer, or null for no observation.</param>
    /// <param name="tenant">The tenant the call belongs to.</param>
    /// <param name="sessionId">The session the call runs in.</param>
    /// <param name="turnId">The turn the call runs under.</param>
    /// <param name="attempt">The 1-based attempt the call runs under.</param>
    /// <param name="reference">The model reference the call targeted.</param>
    /// <param name="inputTokens">The outcome input tokens, already normalised (null usage counts as zero).</param>
    /// <param name="outputTokens">The outcome output tokens, already normalised.</param>
    let private reportSuccessCheckpoint
        (observer: IUsageObserver | null)
        (tenant: TenantId)
        (sessionId: SessionId)
        (turnId: TurnId)
        (attempt: int)
        (reference: ModelReference)
        (inputTokens: int64)
        (outputTokens: int64)
        : unit =
        if not (isNull (box observer)) then
            match box observer with
            | :? IUsageObserver as live ->
                try
                    live.OnCheckpoint
                        {
                            Tenant = tenant
                            SessionId = sessionId
                            TurnId = turnId
                            Attempt = attempt
                            Provider = reference.Provider
                            Model = reference.Model
                            InputTokens = max 0L inputTokens
                            OutputTokens = max 0L outputTokens
                            IdempotencyKey = Guid.NewGuid().ToString("N")
                        }
                with _ ->
                    ()
            | _ -> ()

    /// Reports one terminal post-send failure as an abandoned zero-token
    /// settlement with a fresh idempotency key. The host correlates the
    /// settlement via the propagated exception. Guarded: a throwing
    /// observer never masks the original failure.
    /// <param name="observer">The host usage observer, or null for no observation.</param>
    /// <param name="tenant">The tenant the call belongs to.</param>
    /// <param name="sessionId">The session the call runs in.</param>
    /// <param name="turnId">The turn the call runs under.</param>
    /// <param name="attempt">The 1-based attempt the call runs under.</param>
    /// <param name="reference">The model reference the call targeted.</param>
    let private reportAbandonedSettlement
        (observer: IUsageObserver | null)
        (tenant: TenantId)
        (sessionId: SessionId)
        (turnId: TurnId)
        (attempt: int)
        (reference: ModelReference)
        : unit =
        if not (isNull (box observer)) then
            match box observer with
            | :? IUsageObserver as live ->
                try
                    live.OnSettled
                        {
                            Tenant = tenant
                            SessionId = sessionId
                            TurnId = turnId
                            Attempt = attempt
                            Provider = reference.Provider
                            Model = reference.Model
                            InputTokens = 0L
                            OutputTokens = 0L
                            IdempotencyKey = Guid.NewGuid().ToString("N")
                        }
                with _ ->
                    ()
            | _ -> ()

    /// Runs one attempt and, on transient failure, hands off to
    /// <c>retryOrRaise</c>. Transient failures are 408, 429, 5xx, and
    /// network errors; an OperationCanceledException the deadline scope
    /// fired becomes DeadlineExceededException, the caller's own abort
    /// propagates, and a provider timeout on its own watch (neither token
    /// fired) retries like any network failure. Anything else is terminal
    /// as-is: a non-transient ProviderException keeps its status and
    /// retry-after, and a host bug propagates unwrapped so it stays
    /// visible.
    /// Handles one transient failure: records the shared 429 cooldown
    /// (honouring the provider's Retry-After when it outlasts the
    /// configured pause), then either raises the mapped failure with the
    /// budget spent or waits the longer of the jittered backoff and the
    /// remaining cooldown before delegating to <paramref name="recurse" />
    /// for the next attempt. A wait that would cross the deadline fails
    /// fast instead of sleeping past it.
    /// <param name="recurse">Runs the next attempt: the attempt loop partially applied to this call.</param>
    /// <param name="context">The per-call parameters.</param>
    /// <param name="attempt">The zero-based attempt number that just failed.</param>
    /// <param name="mapped">The transient failure, already mapped to ProviderException.</param>
    /// <returns>The terminal outcome the retried call produced.</returns>
    let retryOrRaise<'T>
        (recurse: int -> Task<LlmCallOutcome<'T>>)
        (context: AttemptContext<'T>)
        (attempt: int)
        (mapped: ProviderException)
        : Task<LlmCallOutcome<'T>> =
        task {
            let now = context.Clock.GetUtcNow()

            if mapped.Status.HasValue && mapped.Status.Value = 429 then
                let retryAfter =
                    if mapped.RetryAfter.HasValue then
                        max TimeSpan.Zero mapped.RetryAfter.Value
                    else
                        TimeSpan.Zero

                let pause = max context.Cooldown retryAfter

                lock context.Gate (fun () ->
                    if now + pause > context.State.CooldownUntil then
                        context.State.CooldownUntil <- now + pause

                    context.State.Signal.Signal())

                // The distributed path shares the pause with peer
                // processes; a propagation failure never fails the retry.
                if context.Distributed then
                    match box context.Admission with
                    | :? IDistributedLlmAdmission as live -> do! startCooldownGuarded live context.IdentityKey pause
                    | _ -> ()

            if attempt >= context.RetryCount then
                do! settleFailure context
                return! Task.FromException<LlmCallOutcome<'T>>(mapped)
            else
                let backoff = jitteredBackoff context.Random context.MinBackoff context.MaxBackoff

                let cooldownWait =
                    lock context.Gate (fun () ->
                        if now < context.State.CooldownUntil then
                            context.State.CooldownUntil - now
                        else
                            TimeSpan.Zero)

                let wait = max backoff cooldownWait

                if context.Clock.GetUtcNow() + wait >= context.Deadline then
                    do! settleFailure context
                    return! Task.FromException<LlmCallOutcome<'T>>(context.DeadlineEx())
                else
                    try
                        do! context.Delay.Delay(wait, context.LinkedToken)
                    with :? OperationCanceledException as waitCanceled ->
                        if context.IsDeadlineFired() then
                            do! settleFailure context
                            raise (context.DeadlineEx())
                        else
                            do! settleFailure context
                            ExceptionDispatchInfo.Capture(waitCanceled).Throw()

                    use _scope = LoggingScopes.beginScope context.Log context.LogScope

                    context.Log.LogInformation(
                        "The coordinator retries the '{ProviderId}' provider call after a transient failure.",
                        LoggingScopes.redactForLog context.ProviderId
                    )

                    return! recurse (attempt + 1)
        }

    /// <param name="context">The per-call parameters.</param>
    /// <param name="attempt">The zero-based attempt number.</param>
    /// <returns>The terminal outcome the provider call produced, with null usage normalised to zero.</returns>
    let rec attemptLoop<'T> (context: AttemptContext<'T>) (attempt: int) : Task<LlmCallOutcome<'T>> =
        task {
            if context.Clock.GetUtcNow() >= context.Deadline then
                do! settleFailure context
                raise (context.DeadlineEx())

            try
                let! outcome = context.Invoke.Invoke(context.Client, context.LinkedToken)

                if isNull (box outcome) then
                    do! settleFailure context

                    return!
                        Task.FromException<LlmCallOutcome<'T>>(
                            InvalidOperationException(
                                sprintf "The '%s' provider call returned null instead of a result." context.ProviderId
                            )
                        )
                else
                    let usage: UsageSummary =
                        if isNull (box outcome.Usage) then
                            { InputTokens = 0L; OutputTokens = 0L }
                        else
                            outcome.Usage

                    do! settleSuccess context (max 0L (usage.InputTokens + usage.OutputTokens))

                    return { Value = outcome.Value; Usage = usage }
            with
            | :? ProviderException as providerFailure when
                providerFailure.Status.HasValue
                && isTransientStatus providerFailure.Status.Value
                ->
                return! retryOrRaise (attemptLoop context) context attempt providerFailure
            | :? HttpRequestException as networkFailure ->
                return!
                    retryOrRaise
                        (attemptLoop context)
                        context
                        attempt
                        (wrapProviderError context.ProviderId (networkFailure :> exn))
            | :? IOException as networkFailure ->
                return!
                    retryOrRaise
                        (attemptLoop context)
                        context
                        attempt
                        (wrapProviderError context.ProviderId (networkFailure :> exn))
            | :? SocketException as networkFailure ->
                return!
                    retryOrRaise
                        (attemptLoop context)
                        context
                        attempt
                        (wrapProviderError context.ProviderId (networkFailure :> exn))
            | :? TimeoutException as networkFailure ->
                return!
                    retryOrRaise
                        (attemptLoop context)
                        context
                        attempt
                        (wrapProviderError context.ProviderId (networkFailure :> exn))
            | :? OperationCanceledException as canceled ->
                if context.IsDeadlineFired() then
                    do! settleFailure context
                    return! Task.FromException<LlmCallOutcome<'T>>(context.DeadlineEx())
                elif context.CallerToken.IsCancellationRequested then
                    do! settleFailure context
                    return! Task.FromException<LlmCallOutcome<'T>>(canceled)
                else
                    return!
                        retryOrRaise
                            (attemptLoop context)
                            context
                            attempt
                            (wrapProviderError context.ProviderId (canceled :> exn))
            | ex ->
                do! settleFailure context
                return! Task.FromException<LlmCallOutcome<'T>>(ex)
        }

    /// Local per-identity coordinator: concurrency gate, RPM/TPM windows,
    /// capped FIFO queue, shared 429 cooldown, jittered transient retry,
    /// and the turn deadline, all off the injected seams. When
    /// <see cref="P:Legate.LlmOptions.DistributedCoordination" /> is true
    /// with <see cref="P:Legate.DistributedCoordinationOptions.Mode" />
    /// Redis and a registered admission client, calls admit through the
    /// distributed seam first (fail-closed rejects, fail-open admits under
    /// emergency limits). One instance
    /// serves every identity; state is keyed by providerId/scope and the
    /// instance is thread-safe. Construct directly (the session-actor
    /// wiring that owns the singleton arrives with the turn-loop
    /// integration); AddLegate registration is untouched by this issue.
    /// <param name="options">The LLM section: coordination knobs, per-provider settings, and the distributed flag. Read per call, so key rotation applies without rebuilding.</param>
    /// <param name="registry">The registry the coordinator resolves providers through.</param>
    /// <param name="keyProvider">The per-tenant key source, or null when the host keeps keys only in provider options.</param>
    /// <param name="clock">The injected clock: admission instants, windows, and the deadline timer.</param>
    /// <param name="delay">The injected wait seam: rate, cooldown, and backoff waits.</param>
    /// <param name="random">The injected jitter seam: retry backoff.</param>
    /// <param name="logger">The logger the coordinator reports admit/retry/reject points to, or null for no logging.</param>
    /// <param name="admission">The distributed admission seam, or null for local-only coordination.</param>
    /// <param name="distributedOptions">The distributed coordination options, or null for local-only coordination.</param>
    [<Sealed>]
    type LlmCoordinator
        (
            options: LlmOptions,
            registry: ILlmProviderRegistry,
            keyProvider: IApiKeyProvider | null,
            clock: TimeProvider,
            delay: ILlmDelay,
            random: ILlmRandom,
            logger: ILogger | null,
            admission: IDistributedLlmAdmission | null,
            distributedOptions: DistributedCoordinationOptions | null
        ) =

        do
            ArgumentNullException.ThrowIfNull(options)
            ArgumentNullException.ThrowIfNull(registry)
            ArgumentNullException.ThrowIfNull(clock)
            ArgumentNullException.ThrowIfNull(delay)
            ArgumentNullException.ThrowIfNull(random)

        let gate = obj ()
        let states = Dictionary<string, IdentityState>(StringComparer.Ordinal)
        let log = LoggingScopes.resolveLogger logger
        let distributedSeam = admission
        let distributedSettings = distributedOptions

        /// Builds the coordinator with no logger.
        /// <param name="options">The LLM section: coordination knobs, per-provider settings, and the distributed flag.</param>
        /// <param name="registry">The registry the coordinator resolves providers through.</param>
        /// <param name="keyProvider">The per-tenant key source, or null when the host keeps keys only in provider options.</param>
        /// <param name="clock">The injected clock.</param>
        /// <param name="delay">The injected wait seam.</param>
        /// <param name="random">The injected jitter seam.</param>
        new
            (
                options: LlmOptions,
                registry: ILlmProviderRegistry,
                keyProvider: IApiKeyProvider | null,
                clock: TimeProvider,
                delay: ILlmDelay,
                random: ILlmRandom
            ) =
            LlmCoordinator(options, registry, keyProvider, clock, delay, random, null, null, null)

        /// Builds the coordinator with a logger and local-only coordination.
        /// <param name="options">The LLM section: coordination knobs, per-provider settings, and the distributed flag.</param>
        /// <param name="registry">The registry the coordinator resolves providers through.</param>
        /// <param name="keyProvider">The per-tenant key source, or null when the host keeps keys only in provider options.</param>
        /// <param name="clock">The injected clock.</param>
        /// <param name="delay">The injected wait seam.</param>
        /// <param name="random">The injected jitter seam.</param>
        /// <param name="logger">The logger the coordinator reports admit/retry/reject points to, or null for no logging.</param>
        new
            (
                options: LlmOptions,
                registry: ILlmProviderRegistry,
                keyProvider: IApiKeyProvider | null,
                clock: TimeProvider,
                delay: ILlmDelay,
                random: ILlmRandom,
                logger: ILogger | null
            ) =
            LlmCoordinator(options, registry, keyProvider, clock, delay, random, logger, null, null)

        /// Runs one provider call under the reference's identity: authorises
        /// the tenant-provider-model through the policy first (a deny
        /// throws <see cref="T:Legate.ModelDeniedException" /> before the
        /// registry resolve, key lookup, or client build, so deny outranks
        /// <see cref="T:Legate.ProviderNotRegisteredException" /> and the
        /// missing-key <see cref="T:Legate.ProviderException" />), resolves
        /// the provider, derives providerId/scope (failing fast with
        /// <see cref="T:Legate.ProviderException" /> when neither key source
        /// has a key, before anything queues), builds the chat client with
        /// the resolved key, admits the call through the concurrency gate,
        /// rate windows, and shared cooldown, then invokes with jittered
        /// retry inside the turn deadline. When distributed coordination is
        /// on with Mode Redis and a registered admission client, the
        /// concurrency gate, FIFO queueing, and shared cooldown come from
        /// the seam (the local RPM/TPM windows still apply): Queued polls,
        /// CooldownActive waits out the pause, fail-closed rejects without
        /// contacting the provider, and fail-open admits locally under
        /// emergency limits (concurrency 1, queue 16) with one metric point
        /// per admitted call. Transient failures (408, 429,
        /// 5xx, network errors) retry up to
        /// <see cref="P:Legate.LlmCoordinationOptions.RetryCount" /> times;
        /// other 4xx surface immediately; every provider failure surfaces as
        /// ProviderException with status and retry-after. The TPM
        /// reservation is <paramref name="estimatedInputTokens" /> (the
        /// caller's <see cref="M:Legate.ContextPruning.Estimate(System.Collections.Generic.IReadOnlyList{Legate.SessionCell})" />
        /// of the request) plus EstimatedOutputTokens, reconciled with the
        /// reported usage on success and released on failure. A reservation
        /// larger than the whole TPM budget admits immediately: waiting
        /// could never free enough room. Waits that would cross the deadline
        /// raise <see cref="T:Legate.DeadlineExceededException" /> instead of
        /// sleeping past it. Usage is reported once per call at this
        /// boundary, never inside the retry loop: one
        /// <see cref="M:Legate.IUsageObserver.OnCheckpoint(Legate.UsageCheckpoint)" />
        /// checkpoint carrying the terminal outcome usage on success (null
        /// usage counts as zero), one zero-token
        /// <see cref="M:Legate.IUsageObserver.OnSettled(Legate.UsageSettlement)" />
        /// settlement with a fresh key on terminal post-send failure (the
        /// host correlates it via the propagated exception); deny and
        /// pre-send failures report nothing. Observer calls are guarded: a
        /// throwing observer never fails the call.
        /// <param name="reference">The model to call, resolved through the registry.</param>
        /// <param name="tenant">The session tenant: the credential scope when no static key is set.</param>
        /// <param name="estimatedInputTokens">The caller's deterministic estimate of the request tokens. Must not be negative.</param>
        /// <param name="invoke">The provider call, receiving the bound chat client and the deadline-linked token.</param>
        /// <param name="cancellationToken">Abandons the call: queued waits reject and in-flight work observes it.</param>
        /// <param name="policy">Authorises the tenant-provider-model call before the registry resolve, or null to allow every call.</param>
        /// <param name="observer">Receives the single terminal usage delivery, or null for no observation.</param>
        /// <param name="sessionId">The session the call runs in.</param>
        /// <param name="turnId">The turn the call runs under.</param>
        /// <param name="attempt">The 1-based attempt the call runs under.</param>
        /// <returns>The value the provider call produced.</returns>
        /// <exception cref="T:System.ArgumentException">The coordination knobs do not validate.</exception>
        /// <exception cref="T:System.InvalidOperationException">Distributed coordination is on without Mode Redis and a registered client, the provider returned null instead of a client or result, or the policy returned null or an unknown decision shape.</exception>
        /// <exception cref="T:Legate.ModelDeniedException">The model policy denied the call.</exception>
        /// <exception cref="T:Legate.ProviderNotRegisteredException">No provider is registered under the reference's provider segment.</exception>
        /// <exception cref="T:Legate.ProviderException">Neither key source has a key, or the provider call failed.</exception>
        /// <exception cref="T:Legate.AdmissionRejectedException">The FIFO queue is full, or the distributed seam is unavailable under FailClosed.</exception>
        /// <exception cref="T:Legate.DeadlineExceededException">The turn deadline fired.</exception>
        member _.ExecuteAsync<'T>
            (
                reference: ModelReference,
                tenant: TenantId,
                estimatedInputTokens: int64,
                invoke: Func<IChatClient, CancellationToken, Task<LlmCallOutcome<'T>>>,
                cancellationToken: CancellationToken,
                policy: IModelPolicy | null,
                observer: IUsageObserver | null,
                sessionId: SessionId,
                turnId: TurnId,
                attempt: int
            ) : Task<'T> =
            task {
                // ── Gate: knobs valid, local admission only.
                let coordination =
                    if isNull (box options.Coordination) then
                        raise (ArgumentException("Coordination must not be null.", nameof options))
                    else
                        options.Coordination

                match coordination.Validate() with
                | null -> ()
                | violation -> raise (ArgumentException(violation, nameof options))

                // ── Enabled rule: distributed admission is active when
                // LlmOptions.DistributedCoordination is true with Mode Redis
                // and a registered client. True with anything else fails
                // fast naming the missing piece, never silently local;
                // false always coordinates locally.
                let mutable useDistributed = false
                let mutable failClosed = true

                if options.DistributedCoordination then
                    match box distributedSettings with
                    | null ->
                        raise (
                            InvalidOperationException(
                                "LlmOptions.DistributedCoordination is true, but no DistributedCoordinationOptions was supplied: pass the options the coordination package binds from Legate:Llm:DistributedCoordination."
                            )
                        )
                    | :? DistributedCoordinationOptions as settings ->
                        if settings.Mode <> DistributedCoordinationMode.Redis then
                            raise (
                                InvalidOperationException(
                                    sprintf
                                        "LlmOptions.DistributedCoordination is true, but DistributedCoordinationOptions.Mode is %O: set Mode to Redis or coordinate locally."
                                        settings.Mode
                                )
                            )
                        elif isNull (box distributedSeam) then
                            raise (
                                InvalidOperationException(
                                    "LlmOptions.DistributedCoordination is true with Mode Redis, but no IDistributedLlmAdmission client was supplied: register the Redis coordination package."
                                )
                            )
                        else
                            useDistributed <- true
                            failClosed <- settings.FailClosed
                    | _ ->
                        raise (
                            InvalidOperationException(
                                "LlmOptions.DistributedCoordination is true, but the distributed coordination options have an unknown shape."
                            )
                        )

                if estimatedInputTokens < 0L then
                    raise (
                        ArgumentOutOfRangeException(
                            nameof estimatedInputTokens,
                            "The estimated input token count must not be negative."
                        )
                    )

                ArgumentNullException.ThrowIfNull(invoke)

                // ── Authorize first: deny throws before the registry
                // resolve, key lookup, or client build.
                authorizeFirst policy tenant reference.Provider reference.Model

                let maxConcurrency = coordination.MaxConcurrentRequests
                let rpm = coordination.RequestsPerMinute
                let tpm = coordination.TokensPerMinute
                let estimatedOutput = int64 coordination.EstimatedOutputTokens
                let maxQueued = coordination.MaxQueuedRequests
                let timeoutSeconds = coordination.RequestTimeoutSeconds
                let timeout = TimeSpan.FromSeconds(float timeoutSeconds)
                let retryCount = coordination.RetryCount
                let minBackoff = coordination.MinRetryBackoff
                let maxBackoff = coordination.MaxRetryBackoff
                let cooldown = coordination.RateLimitCooldown
                let reservation = estimatedInputTokens + estimatedOutput

                // ── Resolve and derive the identity before anything queues.
                let provider = registry.Resolve(reference)
                let providerId = reference.Provider

                let mutable configured = Unchecked.defaultof<LlmProviderOptions>

                let foundExact =
                    if isNull (box options.Providers) then
                        false
                    else
                        options.Providers.TryGetValue(providerId, &configured)

                let providerOptions =
                    if foundExact then
                        configured
                    elif isNull (box options.Providers) then
                        Unchecked.defaultof<LlmProviderOptions>
                    else
                        match
                            options.Providers
                            |> Seq.tryFind (fun entry ->
                                String.Equals(entry.Key, providerId, StringComparison.OrdinalIgnoreCase))
                        with
                        | Some entry -> entry.Value
                        | None -> Unchecked.defaultof<LlmProviderOptions>

                let configuredKey: string | null =
                    if isNull (box providerOptions) then
                        null
                    else
                        providerOptions.ApiKey

                let tenantKey: string | null =
                    match keyProvider with
                    | null -> null
                    | provider -> provider.GetApiKey(providerId, tenant)

                let missingKeyExn () =
                    ProviderException(
                        providerId,
                        Nullable<int>(),
                        Nullable<TimeSpan>(),
                        sprintf
                            "No API key is configured for provider '%s': set LlmProviderOptions.ApiKey or register an IApiKeyProvider."
                            providerId
                    )

                let tenantScope () : string * string =
                    match tenantKey with
                    | null -> raise (missingKeyExn ())
                    | key ->
                        if String.IsNullOrWhiteSpace key then
                            raise (missingKeyExn ())
                        else
                            tenant.Value, key

                let scope, apiKey =
                    match configuredKey with
                    | null -> tenantScope ()
                    | key ->
                        if String.IsNullOrWhiteSpace key then
                            tenantScope ()
                        else
                            StaticScope, key

                let identityKey = providerId + "/" + scope

                // The per-call lease owner: the turn plus the 1-based
                // attempt (correlatable) plus a fresh guid (unique,
                // secret-free). Retries hold the same lease under the same
                // owner: no re-acquire between attempts.
                let ownerId: string | null =
                    if useDistributed then
                        turnId.Value + "/attempt-" + string attempt + "/" + Guid.NewGuid().ToString("N")
                    else
                        null

                let clientOptions = LlmProviderOptions()
                clientOptions.ApiKey <- apiKey
                let client = provider.CreateChatClient(reference, clientOptions)

                if isNull (box client) then
                    raise (
                        InvalidOperationException(
                            sprintf "The '%s' provider returned null instead of a chat client." providerId
                        )
                    )

                // ── The turn deadline: a one-shot off the injected clock
                // (virtual time under a fake clock), plus clock fail-fast
                // checks before every wait. The timer keeps RecordingDelay
                // usable: the deadline never fires unless the clock moves.
                let deadline = clock.GetUtcNow() + timeout

                use deadlineCts = new CancellationTokenSource()

                use _deadlineTimer =
                    clock.CreateTimer(
                        TimerCallback(fun _ ->
                            try
                                deadlineCts.Cancel()
                            with :? ObjectDisposedException ->
                                ()),
                        null,
                        timeout,
                        Timeout.InfiniteTimeSpan
                    )

                use linkedCts =
                    CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadlineCts.Token)

                let linkedToken = linkedCts.Token

                let isDeadlineFired () =
                    deadlineCts.IsCancellationRequested
                    && not cancellationToken.IsCancellationRequested

                let deadlineEx () =
                    DeadlineExceededException(
                        ExecuteOperationName,
                        sprintf "The coordinated '%s' call exceeded its %d-second deadline." identityKey timeoutSeconds
                    )

                let queueFullEx (capQueued: int) () =
                    AdmissionRejectedException(
                        QueueFullReason,
                        sprintf
                            "The '%s' coordinator queue is full (%d waiting); the request was rejected without contacting the provider."
                            identityKey
                            capQueued
                    )

                // The queued waiter owned by this call, or null while the
                // call has not queued. Mutated only under the gate.
                let mutable waiter = Unchecked.defaultof<TaskCompletionSource<unit>>
                let mutable ticketState = Unchecked.defaultof<IdentityState>
                let mutable ticketEntry = Unchecked.defaultof<TokenEntry>

                let removeWaiter () =
                    if not (isNull (box waiter)) then
                        let owned = waiter
                        waiter <- Unchecked.defaultof<TaskCompletionSource<unit>>

                        lock gate (fun () ->
                            if states.ContainsKey(identityKey) then
                                let state = states[identityKey]

                                if state.Waiters.Remove(owned) then
                                    state.Signal.Signal())

                // Decides one admission step under the gate, enqueuing this
                // call when it must wait and capturing the pulse to wait on.
                // Only the queue head (or a new arrival on an empty queue)
                // may admit, so waiters leave strictly in FIFO order. The
                // caps are the coordination knobs on the local path and the
                // emergency bounds on the fail-open path.
                let decide (now: DateTimeOffset) (capConcurrency: int) (capQueued: int) : AdmissionStep * Task =
                    lock gate (fun () ->
                        let state =
                            if states.ContainsKey(identityKey) then
                                states[identityKey]
                            else
                                let fresh = IdentityState()
                                states[identityKey] <- fresh
                                fresh

                        let cutoff = now - rateWindow
                        state.RequestTimes.RemoveAll(fun instant -> instant <= cutoff) |> ignore
                        state.TokenLedger.RemoveAll(fun entry -> entry.Instant <= cutoff) |> ignore

                        let enqueued = not (isNull (box waiter))

                        let mayAdmit =
                            if enqueued then
                                state.Waiters.Count > 0
                                && Object.ReferenceEquals(box state.Waiters[0], box waiter)
                            else
                                state.Waiters.Count = 0

                        let cooldownOk = now >= state.CooldownUntil
                        let rpmOk = state.RequestTimes.Count < rpm

                        let tpmOk =
                            if not tpm.HasValue then
                                true
                            elif state.TokenLedger.Count = 0 then
                                // A reservation larger than the whole budget
                                // admits immediately: waiting could never
                                // free enough room.
                                true
                            else
                                let spent = state.TokenLedger |> Seq.sumBy (fun entry -> entry.Tokens)
                                spent + reservation <= int64 tpm.Value

                        if mayAdmit && state.Active < capConcurrency && cooldownOk && rpmOk && tpmOk then
                            if enqueued then
                                state.Waiters.Remove(waiter) |> ignore

                            state.Active <- state.Active + 1
                            state.RequestTimes.Add(now)

                            let entry = { Instant = now; Tokens = reservation }

                            state.TokenLedger.Add(entry)
                            ticketState <- state
                            ticketEntry <- entry
                            state.Signal.Signal()
                            Admitted, Task.CompletedTask
                        else if not enqueued && state.Waiters.Count >= capQueued then
                            Rejected, Task.CompletedTask
                        else
                            if not enqueued then
                                waiter <-
                                    TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

                                state.Waiters.Add(waiter)

                            let signal = state.Signal.Current

                            if mayAdmit && (not cooldownOk || not rpmOk || not tpmOk) then
                                let mutable hint = TimeSpan.MaxValue

                                if not cooldownOk then
                                    hint <- min hint (state.CooldownUntil - now)

                                if not rpmOk then
                                    let oldest = state.RequestTimes |> Seq.min
                                    hint <- min hint (oldest + rateWindow - now)

                                if not tpmOk then
                                    let oldest = state.TokenLedger |> Seq.minBy (fun entry -> entry.Instant)
                                    hint <- min hint (oldest.Instant + rateWindow - now)

                                AwaitTimed hint, signal
                            else
                                AwaitSignal, signal)

                // Raises the deadline failure for a cancelled wait: the
                // caller's own abort propagates, the coordinator deadline
                // becomes DeadlineExceededException. Never returns
                // normally, so the result type stays generic for every
                // waiter shape. A cancelled wait with neither token fired
                // is a broken seam: fail loudly rather than hang.
                let raiseForCancelledWait () : 'a =
                    removeWaiter ()

                    if isDeadlineFired () then
                        raise (deadlineEx ())
                    else
                        cancellationToken.ThrowIfCancellationRequested()

                        raise (OperationCanceledException(linkedToken))

                let callScope =
                    LoggingScopes.createScope
                        (tenant.ToString())
                        (sessionId.ToString())
                        (turnId.ToString())
                        null
                        attempt
                        null

                let logCall (message: string) : unit =
                    use _scope = LoggingScopes.beginScope log callScope
                    log.LogInformation("{Message}", LoggingScopes.redactForLog message)

                // Falls back to emergency local admission when the seam is
                // unavailable in fail-open mode. Set by the distributed
                // admit loop; the local loop below reads it for its caps.
                let mutable emergencyLocal = false

                // Re-validates the local RPM/TPM windows after the seam
                // granted a lease (closing the check-then-act race while
                // the seam still caps concurrency): records the reservation
                // and captures the ticket on success, otherwise hands back
                // the rate hint to wait on. The gate count, the waiters,
                // and the local cooldown stay untouched on this path.
                let checkDistributedRates (now: DateTimeOffset) : bool * TimeSpan =
                    lock gate (fun () ->
                        let state =
                            if states.ContainsKey(identityKey) then
                                states[identityKey]
                            else
                                let fresh = IdentityState()
                                states[identityKey] <- fresh
                                fresh

                        let cutoff = now - rateWindow
                        state.RequestTimes.RemoveAll(fun instant -> instant <= cutoff) |> ignore
                        state.TokenLedger.RemoveAll(fun entry -> entry.Instant <= cutoff) |> ignore

                        let rpmOk = state.RequestTimes.Count < rpm

                        let tpmOk =
                            if not tpm.HasValue then
                                true
                            elif state.TokenLedger.Count = 0 then
                                // A reservation larger than the whole
                                // budget admits immediately: waiting could
                                // never free enough room.
                                true
                            else
                                let spent = state.TokenLedger |> Seq.sumBy (fun entry -> entry.Tokens)
                                spent + reservation <= int64 tpm.Value

                        if rpmOk && tpmOk then
                            state.RequestTimes.Add(now)

                            let entry = { Instant = now; Tokens = reservation }

                            state.TokenLedger.Add(entry)
                            ticketState <- state
                            ticketEntry <- entry
                            true, TimeSpan.Zero
                        else
                            let mutable hint = TimeSpan.MaxValue

                            if not rpmOk then
                                let oldest = state.RequestTimes |> Seq.min
                                hint <- min hint (oldest + rateWindow - now)

                            if not tpmOk then
                                let oldest = state.TokenLedger |> Seq.minBy (fun entry -> entry.Instant)
                                hint <- min hint (oldest.Instant + rateWindow - now)

                            false, hint)

                // Admits through the distributed seam inside the turn
                // deadline: Queued polls every second, CooldownActive waits
                // out the shorter of the configured pause and the remaining
                // deadline (the seam stays the cooldown authority: the local
                // CooldownUntil is never written here), and an Acquired
                // lease re-validates the local rate windows before it
                // counts. Non-cancellation seam failures run the fail
                // policy: fail-closed rejects, fail-open falls back to
                // emergency local admission. Caller cancellation maps to
                // the deadline or the abort, never to the fail policy.
                // Retries hold the one lease under the one owner; every
                // give-up path releases it best-effort. Returns true when
                // the seam admitted the call.
                let admitDistributed () : Task<bool> =
                    task {
                        // The enabled rule proved both non-null; resolve
                        // once so every seam call below is null-clean.
                        let seam, owner =
                            match box distributedSeam, box ownerId with
                            | (:? IDistributedLlmAdmission as live), (:? string as owned) -> live, owned
                            | _ ->
                                raise (
                                    InvalidOperationException(
                                        "LlmOptions.DistributedCoordination is true with Mode Redis, but no IDistributedLlmAdmission client was supplied: register the Redis coordination package."
                                    )
                                )

                        // Waits out a distributed-admit pause off the delay
                        // seam: a cancelled wait releases the lease
                        // best-effort and maps to the deadline or the
                        // caller's abort; a faulted wait releases and
                        // rethrows the seam failure.
                        let waitDistributed (hint: TimeSpan) : Task =
                            task {
                                try
                                    do! delay.Delay(hint, linkedToken)
                                with
                                | :? OperationCanceledException ->
                                    do! releaseLeaseGuarded seam identityKey owner

                                    if isDeadlineFired () then
                                        raise (deadlineEx ())
                                    else
                                        cancellationToken.ThrowIfCancellationRequested()
                                        raise (OperationCanceledException(linkedToken))
                                | ex ->
                                    do! releaseLeaseGuarded seam identityKey owner
                                    ExceptionDispatchInfo.Capture(ex).Throw()
                            }

                        let ttl = TimeSpan.FromSeconds(float timeoutSeconds)
                        let mutable admitted = false
                        let mutable fellBack = false

                        while not admitted && not fellBack do
                            try
                                cancellationToken.ThrowIfCancellationRequested()
                            with ex ->
                                // A queued waiter entry belongs to this
                                // owner: release it best-effort before the
                                // abort surfaces.
                                do! releaseLeaseGuarded seam identityKey owner
                                ExceptionDispatchInfo.Capture(ex).Throw()

                            let now = clock.GetUtcNow()

                            if now >= deadline then
                                do! releaseLeaseGuarded seam identityKey owner
                                logCall "The coordinator deadline fired before distributed admission."
                                raise (deadlineEx ())

                            let! step =
                                task {
                                    try
                                        let! acquired =
                                            seam.AcquireAsync(identityKey, owner, maxConcurrency, ttl, ttl, linkedToken)

                                        return SeamOutcome acquired
                                    with
                                    | :? OperationCanceledException as canceled ->
                                        do! releaseLeaseGuarded seam identityKey owner

                                        if isDeadlineFired () then
                                            return! Task.FromException<DistributedAcquireStep>(deadlineEx ())
                                        else
                                            cancellationToken.ThrowIfCancellationRequested()
                                            return! Task.FromException<DistributedAcquireStep>(canceled)
                                    | _ ->
                                        do! releaseLeaseGuarded seam identityKey owner

                                        if failClosed then
                                            logCall
                                                "The coordinator rejected the call: the distributed seam is unavailable and FailClosed is set."

                                            return!
                                                Task.FromException<DistributedAcquireStep>(
                                                    AdmissionRejectedException(
                                                        DistributedUnavailableReason,
                                                        sprintf
                                                            "The '%s' distributed admission seam is unavailable and FailClosed rejects new work without contacting the provider."
                                                            identityKey
                                                    )
                                                )
                                        else
                                            logCall
                                                "The distributed seam is unavailable: the coordinator admits locally under emergency limits."

                                            fellBack <- true
                                            return SeamFellBack
                                }

                            match step with
                            | SeamFellBack -> ()
                            | SeamOutcome outcome ->
                                match outcome.Kind with
                                | DistributedAdmissionDecision.Acquired ->
                                    let rated = clock.GetUtcNow()
                                    let windowsOk, hint = checkDistributedRates rated

                                    if windowsOk then
                                        admitted <- true
                                    else
                                        do! releaseLeaseGuarded seam identityKey owner

                                        if rated + hint >= deadline then
                                            logCall
                                                "The coordinator deadline fired while waiting on the local rate window."

                                            raise (deadlineEx ())
                                        else
                                            do! waitDistributed hint
                                | DistributedAdmissionDecision.Queued ->
                                    if now + distributedPollInterval >= deadline then
                                        do! releaseLeaseGuarded seam identityKey owner
                                        logCall "The coordinator deadline fired while queued on the distributed seam."
                                        raise (deadlineEx ())
                                    else
                                        do! waitDistributed distributedPollInterval
                                | _ ->
                                    // CooldownActive (or an unknown future
                                    // decision): wait out the shorter of the
                                    // configured pause and the remaining
                                    // deadline, then re-acquire.
                                    let remaining = deadline - clock.GetUtcNow()
                                    let wait = min cooldown remaining

                                    if remaining <= TimeSpan.Zero then
                                        logCall "The coordinator deadline fired while the distributed seam cooled down."
                                        raise (deadlineEx ())
                                    elif wait > TimeSpan.Zero then
                                        do! waitDistributed wait
                                    else
                                        ()

                        emergencyLocal <- fellBack
                        return admitted
                    }

                let mutable seamAdmitted = false

                if useDistributed then
                    let! admittedViaSeam = admitDistributed ()
                    seamAdmitted <- admittedViaSeam

                // The local loop caps: the coordination knobs, or the
                // emergency bounds after a fail-open fallback.
                let capConcurrency =
                    if emergencyLocal then
                        EmergencyMaxConcurrency
                    else
                        maxConcurrency

                let capQueued =
                    if emergencyLocal then
                        EmergencyMaxQueuedRequests
                    else
                        maxQueued

                try
                    let mutable admitted = seamAdmitted

                    while not admitted do
                        cancellationToken.ThrowIfCancellationRequested()
                        let now = clock.GetUtcNow()

                        if now >= deadline then
                            removeWaiter ()
                            logCall "The coordinator deadline fired before admission."
                            raise (deadlineEx ())

                        let step, signal = decide now capConcurrency capQueued

                        match step with
                        | Admitted -> admitted <- true
                        | Rejected ->
                            removeWaiter ()
                            logCall "The coordinator rejected the call: the queue is full."
                            raise (queueFullEx capQueued ())
                        | AwaitSignal ->
                            try
                                do! signal.WaitAsync(linkedToken)
                            with :? OperationCanceledException ->
                                raiseForCancelledWait ()
                        | AwaitTimed hint when hint <= TimeSpan.Zero -> ()
                        | AwaitTimed hint ->
                            if now + hint >= deadline then
                                removeWaiter ()
                                raise (deadlineEx ())

                            try
                                let delayTask = delay.Delay(hint, linkedToken)
                                let! winner = Task.WhenAny(signal, delayTask)

                                if Object.ReferenceEquals(box winner, box delayTask) then
                                    if delayTask.IsCanceled then
                                        raiseForCancelledWait ()
                                    elif delayTask.IsFaulted then
                                        removeWaiter ()
                                        do! delayTask
                                        ()
                                    else
                                        ()
                                else
                                    ()
                            with :? OperationCanceledException ->
                                raiseForCancelledWait ()
                finally
                    if isNull (box ticketEntry) then
                        removeWaiter ()

                // One metric point per call the emergency path admitted
                // while the seam was unavailable in fail-open mode.
                if emergencyLocal then
                    Telemetry.recordFailOpenAdmission providerId

                let context: AttemptContext<'T> =
                    {
                        Client = client
                        Invoke = invoke
                        LinkedToken = linkedToken
                        CallerToken = cancellationToken
                        IsDeadlineFired = isDeadlineFired
                        Deadline = deadline
                        DeadlineEx = deadlineEx
                        ProviderId = providerId
                        RetryCount = retryCount
                        MinBackoff = minBackoff
                        MaxBackoff = maxBackoff
                        Cooldown = cooldown
                        Clock = clock
                        Delay = delay
                        Random = random
                        Gate = gate
                        State = ticketState
                        Entry = ticketEntry
                        Log = log
                        LogScope = callScope
                        Distributed = useDistributed && not emergencyLocal
                        Admission =
                            (if useDistributed && not emergencyLocal then
                                 distributedSeam
                             else
                                 null)
                        IdentityKey = identityKey
                        OwnerId = ownerId
                    }

                logCall "The coordinator admitted the call."

                let providerStart = Telemetry.timestamp ()

                use _providerScope =
                    Telemetry.startProviderScope
                        providerId
                        reference.Model
                        (Telemetry.idTagsFor
                            (sessionId.ToString())
                            (turnId.ToString())
                            null
                            (tenant.ToString())
                            (string attempt)
                            null)

                try
                    let! terminal = attemptLoop context 0

                    let inputTokens =
                        if isNull (box terminal.Usage) then
                            0L
                        else
                            terminal.Usage.InputTokens

                    let outputTokens =
                        if isNull (box terminal.Usage) then
                            0L
                        else
                            terminal.Usage.OutputTokens

                    reportSuccessCheckpoint observer tenant sessionId turnId attempt reference inputTokens outputTokens

                    Telemetry.recordProviderCall providerId reference.Model Telemetry.StatusOk

                    Telemetry.recordProviderLatency
                        (Telemetry.elapsedMilliseconds providerStart)
                        providerId
                        reference.Model

                    return terminal.Value
                with ex ->
                    Telemetry.recordProviderCall providerId reference.Model Telemetry.StatusError

                    Telemetry.recordProviderLatency
                        (Telemetry.elapsedMilliseconds providerStart)
                        providerId
                        reference.Model

                    reportAbandonedSettlement observer tenant sessionId turnId attempt reference
                    return! Task.FromException<'T>(ex)
            }

        /// Runs one provider call with no model policy and no usage
        /// observation: every call is allowed and nothing is reported. The
        /// policy-aware overload is the contract for hosted turns; this
        /// overload keeps single-call hosts free of session context.
        /// <param name="reference">The model to call, resolved through the registry.</param>
        /// <param name="tenant">The session tenant: the credential scope when no static key is set.</param>
        /// <param name="estimatedInputTokens">The caller's deterministic estimate of the request tokens. Must not be negative.</param>
        /// <param name="invoke">The provider call, receiving the bound chat client and the deadline-linked token.</param>
        /// <param name="cancellationToken">Abandons the call: queued waits reject and in-flight work observes it.</param>
        /// <returns>The value the provider call produced.</returns>
        /// <exception cref="T:System.ArgumentException">The coordination knobs do not validate.</exception>
        /// <exception cref="T:System.InvalidOperationException">Distributed coordination is on without Mode Redis and a registered client, or the provider returned null instead of a client or result.</exception>
        /// <exception cref="T:Legate.ProviderNotRegisteredException">No provider is registered under the reference's provider segment.</exception>
        /// <exception cref="T:Legate.ProviderException">Neither key source has a key, or the provider call failed.</exception>
        /// <exception cref="T:Legate.AdmissionRejectedException">The FIFO queue is full, or the distributed seam is unavailable under FailClosed.</exception>
        /// <exception cref="T:Legate.DeadlineExceededException">The turn deadline fired.</exception>
        member this.ExecuteAsync<'T>
            (
                reference: ModelReference,
                tenant: TenantId,
                estimatedInputTokens: int64,
                invoke: Func<IChatClient, CancellationToken, Task<LlmCallOutcome<'T>>>,
                cancellationToken: CancellationToken
            ) : Task<'T> =
            this.ExecuteAsync(
                reference,
                tenant,
                estimatedInputTokens,
                invoke,
                cancellationToken,
                null,
                null,
                SessionId.New(),
                TurnId.New(),
                1
            )
