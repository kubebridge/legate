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
// before anything queues. DistributedCoordination=true fails fast: there is
// no distributed admission here. Everything time-shaped runs off the
// injected TimeProvider (the deadline timer), ILlmDelay (rate, cooldown,
// and backoff waits), and ILlmRandom (retry jitter) seams: no Task.Delay,
// no DateTime.UtcNow, no Random. Rejected designs: a coordinator actor per
// identity (heavier lifecycle for a few counters and queues), new exception
// types (the existing AdmissionRejected, DeadlineExceeded, Provider, and
// ProviderNotRegistered shapes already carry every failure), and
// distributed admission (out of scope).
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

    /// The sliding window RPM and TPM accounting covers: the last minute.
    let private rateWindow = TimeSpan.FromMinutes 1.0

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
        }

    /// Reconciles the reservation with the actual usage and releases the
    /// concurrency slot, pulsing the waiters.
    /// <param name="gate">The coordinator gate.</param>
    /// <param name="state">The call's identity state.</param>
    /// <param name="entry">The call's TPM ledger entry.</param>
    /// <param name="actualTokens">The input plus output tokens the provider reported.</param>
    let private settleSuccess (gate: obj) (state: IdentityState) (entry: TokenEntry) (actualTokens: int64) : unit =
        lock gate (fun () ->
            entry.Tokens <- actualTokens
            state.Active <- state.Active - 1
            state.Signal.Signal())

    /// Releases the reservation and the concurrency slot without
    /// reconciling (no usage was reported), pulsing the waiters.
    /// <param name="gate">The coordinator gate.</param>
    /// <param name="state">The call's identity state.</param>
    /// <param name="entry">The call's TPM ledger entry.</param>
    let private settleFailure (gate: obj) (state: IdentityState) (entry: TokenEntry) : unit =
        lock gate (fun () ->
            state.TokenLedger.Remove(entry) |> ignore
            state.Active <- state.Active - 1
            state.Signal.Signal())

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
    /// <returns>The value the retried call produced.</returns>
    let retryOrRaise<'T>
        (recurse: int -> Task<'T>)
        (context: AttemptContext<'T>)
        (attempt: int)
        (mapped: ProviderException)
        : Task<'T> =
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

            if attempt >= context.RetryCount then
                settleFailure context.Gate context.State context.Entry
                return! Task.FromException<'T>(mapped)
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
                    settleFailure context.Gate context.State context.Entry
                    return! Task.FromException<'T>(context.DeadlineEx())
                else
                    try
                        do! context.Delay.Delay(wait, context.LinkedToken)
                    with :? OperationCanceledException as waitCanceled ->
                        if context.IsDeadlineFired() then
                            settleFailure context.Gate context.State context.Entry
                            raise (context.DeadlineEx())
                        else
                            settleFailure context.Gate context.State context.Entry
                            ExceptionDispatchInfo.Capture(waitCanceled).Throw()

                    return! recurse (attempt + 1)
        }

    /// <param name="context">The per-call parameters.</param>
    /// <param name="attempt">The zero-based attempt number.</param>
    /// <returns>The value the provider call produced.</returns>
    let rec attemptLoop<'T> (context: AttemptContext<'T>) (attempt: int) : Task<'T> =
        task {
            if context.Clock.GetUtcNow() >= context.Deadline then
                settleFailure context.Gate context.State context.Entry
                raise (context.DeadlineEx())

            try
                let! outcome = context.Invoke.Invoke(context.Client, context.LinkedToken)

                if isNull (box outcome) then
                    settleFailure context.Gate context.State context.Entry

                    return!
                        Task.FromException<'T>(
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

                    settleSuccess
                        context.Gate
                        context.State
                        context.Entry
                        (max 0L (usage.InputTokens + usage.OutputTokens))

                    return outcome.Value
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
                    settleFailure context.Gate context.State context.Entry
                    return! Task.FromException<'T>(context.DeadlineEx())
                elif context.CallerToken.IsCancellationRequested then
                    settleFailure context.Gate context.State context.Entry
                    return! Task.FromException<'T>(canceled)
                else
                    return!
                        retryOrRaise
                            (attemptLoop context)
                            context
                            attempt
                            (wrapProviderError context.ProviderId (canceled :> exn))
            | ex ->
                settleFailure context.Gate context.State context.Entry
                return! Task.FromException<'T>(ex)
        }

    /// Local per-identity coordinator: concurrency gate, RPM/TPM windows,
    /// capped FIFO queue, shared 429 cooldown, jittered transient retry,
    /// and the turn deadline, all off the injected seams. One instance
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
    [<Sealed>]
    type LlmCoordinator
        (
            options: LlmOptions,
            registry: ILlmProviderRegistry,
            keyProvider: IApiKeyProvider | null,
            clock: TimeProvider,
            delay: ILlmDelay,
            random: ILlmRandom
        ) =

        do
            ArgumentNullException.ThrowIfNull(options)
            ArgumentNullException.ThrowIfNull(registry)
            ArgumentNullException.ThrowIfNull(clock)
            ArgumentNullException.ThrowIfNull(delay)
            ArgumentNullException.ThrowIfNull(random)

        let gate = obj ()
        let states = Dictionary<string, IdentityState>(StringComparer.Ordinal)

        /// Runs one provider call under the reference's identity: resolves
        /// the provider, derives providerId/scope (failing fast with
        /// <see cref="T:Legate.ProviderException" /> when neither key source
        /// has a key, before anything queues), builds the chat client with
        /// the resolved key, admits the call through the concurrency gate,
        /// rate windows, and shared cooldown, then invokes with jittered
        /// retry inside the turn deadline. Transient failures (408, 429,
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
        /// sleeping past it.
        /// <param name="reference">The model to call, resolved through the registry.</param>
        /// <param name="tenant">The session tenant: the credential scope when no static key is set.</param>
        /// <param name="estimatedInputTokens">The caller's deterministic estimate of the request tokens. Must not be negative.</param>
        /// <param name="invoke">The provider call, receiving the bound chat client and the deadline-linked token.</param>
        /// <param name="cancellationToken">Abandons the call: queued waits reject and in-flight work observes it.</param>
        /// <returns>The value the provider call produced.</returns>
        /// <exception cref="T:System.ArgumentException">The coordination knobs do not validate.</exception>
        /// <exception cref="T:System.InvalidOperationException">Distributed coordination is on, or the provider returned null instead of a client or result.</exception>
        /// <exception cref="T:Legate.ProviderNotRegisteredException">No provider is registered under the reference's provider segment.</exception>
        /// <exception cref="T:Legate.ProviderException">Neither key source has a key, or the provider call failed.</exception>
        /// <exception cref="T:Legate.AdmissionRejectedException">The FIFO queue is full.</exception>
        /// <exception cref="T:Legate.DeadlineExceededException">The turn deadline fired.</exception>
        member _.ExecuteAsync<'T>
            (
                reference: ModelReference,
                tenant: TenantId,
                estimatedInputTokens: int64,
                invoke: Func<IChatClient, CancellationToken, Task<LlmCallOutcome<'T>>>,
                cancellationToken: CancellationToken
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

                if options.DistributedCoordination then
                    raise (
                        InvalidOperationException(
                            "LlmOptions.DistributedCoordination is true, but this coordinator admits locally: distributed admission is out of scope."
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

                let queueFullEx () =
                    AdmissionRejectedException(
                        QueueFullReason,
                        sprintf
                            "The '%s' coordinator queue is full (%d waiting); the request was rejected without contacting the provider."
                            identityKey
                            maxQueued
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
                // may admit, so waiters leave strictly in FIFO order.
                let decide (now: DateTimeOffset) : AdmissionStep * Task =
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

                        if mayAdmit && state.Active < maxConcurrency && cooldownOk && rpmOk && tpmOk then
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
                        else if not enqueued && state.Waiters.Count >= maxQueued then
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

                try
                    let mutable admitted = false

                    while not admitted do
                        cancellationToken.ThrowIfCancellationRequested()
                        let now = clock.GetUtcNow()

                        if now >= deadline then
                            removeWaiter ()
                            raise (deadlineEx ())

                        let step, signal = decide now

                        match step with
                        | Admitted -> admitted <- true
                        | Rejected ->
                            removeWaiter ()
                            raise (queueFullEx ())
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
                    }

                return! attemptLoop context 0
            }
