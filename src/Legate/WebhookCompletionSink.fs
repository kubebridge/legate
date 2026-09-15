// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Net.Http
open System.Security.Cryptography
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions

// Default webhook completion sink (issue 83). The host constructs one sink
// per webhook endpoint with the signing secret it resolved from the
// agent-definition store at construction time (agent-definition-only in
// v1: no per-session override), and registers it as the session's
// ISessionCompletionSink. Notify stays synchronous and non-blocking per
// the HostHooks contract: it maps the completion to the documented wire
// payload and queues a bounded background delivery (timeout per attempt,
// exponential backoff over ILlmDelay, at-most-N attempts), then returns.
// Permanent failures are logged with the attempt count; secrets travel
// only in the HMAC, never in logs, errors, or metadata. Every POST goes
// through SsrfGuard.checkAsync with a per-call pinned handler, reusing
// the issue 74/75 precedent.

// ────────────────────
// Wire payload field sources (task 1)
//
// sessionId      <- completion.SessionId.
// turnId         <- empty in v1: SessionCompletion and TurnResult carry no
//                   turn id. The field stays so the wire shape is stable
//                   when the contract gains one.
// status         <- TurnResult.Status enum name (for example "Completed").
// outcome        <- TurnResult.Outcome verbatim, with its stable $type
//                   discriminator; null when the session did not run with
//                   the structured outcome mode.
// summary        <- the outcome's summary or reason when an outcome is
//                   present, else the final assistant text.
// outputFiles    <- empty in v1: the completion contract carries no
//                   artifact manifest. Reserved for a future source.
// iterations     <- TurnResult.Iterations.
// usage          <- TurnResult.Usage verbatim (token counts, never cost).
// startedAt      <- null in v1: the contract carries no turn timestamps.
// completedAt    <- the sink-observed enqueue time from the injected
//                   TimeProvider: when Notify mapped the delivery, not
//                   when the turn settled.
// idempotencyKey <- completion.IdempotencyKey, echoed back as the
//                   Idempotency-Key header so the receiver deduplicates.

/// The documented JSON wire payload one webhook delivery POSTs: the
/// public wire contract hosts code against when receiving deliveries.
/// Serialises camelCase with System.Text.Json; the outcome keeps its
/// stable <c>$type</c> discriminator. Fields with no v1 source (turn id,
/// output files, start time) are pinned to their documented empties in
/// the field-source table above, so the shape stays stable when the
/// contract gains them. Constructible from C# through property setters.
[<CLIMutable; NoComparison>]
type WebhookCompletionPayload =
    {
        /// The session that completed, as a string.
        SessionId: string
        /// Empty in v1: the completion contract carries no turn id.
        TurnId: string
        /// The turn result's status enum name.
        Status: string
        /// The structured outcome, or null outside structured mode.
        Outcome: TurnOutcome | null
        /// The outcome summary or reason, else the final assistant text.
        Summary: string
        /// Empty in v1: the contract carries no artifact manifest.
        OutputFiles: IReadOnlyList<string>
        /// The model iterations the turn spent.
        Iterations: int
        /// The token usage the turn spent.
        Usage: UsageSummary
        /// Empty in v1: the contract carries no turn timestamps.
        StartedAt: Nullable<DateTimeOffset>
        /// When Notify mapped the delivery (sink-observed, not settled).
        CompletedAt: DateTimeOffset
        /// The stable key the receiver deduplicates on.
        IdempotencyKey: string
    }

/// How the default webhook sink delivers a headless session's structured
/// completion. Bound from configuration or built by the host in code; the
/// host resolves the signing secret from the agent-definition store when
/// it constructs the sink, so the secret is fixed for the sink's
/// lifetime (agent-definition-only in v1, no per-session override). A
/// reference type with mutable properties so absent JSON properties keep
/// the defaults and C# object initialisers work.
type WebhookCompletionSinkOptions() =

    /// The absolute http or https URL every delivery POSTs to. Must be
    /// set; deliveries never go anywhere else.
    member val Endpoint: Uri | null = null with get, set

    /// The opaque signing secret each delivery authenticates with:
    /// non-empty bytes the host has already protected. Required unless
    /// <see cref="P:Legate.WebhookCompletionSinkOptions.AllowUnsignedDelivery" />
    /// is true. Treated as sensitive; never logged, never embedded in
    /// exception messages.
    member val SigningSecret: byte[] | null = null with get, set

    /// Whether deliveries without a signing secret are sent (with no
    /// signature header). Defaults to false: a missing or empty secret is
    /// misconfiguration and never POSTs. Explicit opt-in only.
    member val AllowUnsignedDelivery: bool = false with get, set

    /// The bound on one delivery attempt. Defaults to 30 seconds (the
    /// <c>CustomToolFunction.DefaultTimeout</c> precedent), linked with
    /// the sink's lifetime token.
    member val Timeout: TimeSpan = TimeSpan.FromSeconds(30.0) with get, set

    /// The delivery attempts a completion gets. 0 means the
    /// <see cref="P:Legate.CompletionOptions.MaxDeliveryAttempts" />
    /// default (3). Attempts are bounded: delivery is at-most-N.
    member val MaxDeliveryAttempts: int = 0 with get, set

    /// The backoff before the first retry; doubles every retry. Defaults
    /// to 2 seconds. The 30 second <c>CompletionOptions.RetryDelay</c> is
    /// deliberately not reused verbatim: it is a re-drive cadence, far
    /// too slow for a per-delivery backoff base.
    member val BaseRetryDelay: TimeSpan = TimeSpan.FromSeconds(2.0) with get, set

    /// The cap every backoff is clamped to. Defaults to 30 seconds.
    member val MaxRetryDelay: TimeSpan = TimeSpan.FromSeconds(30.0) with get, set

    /// The default per-attempt bound: 30 seconds.
    static member DefaultTimeout: TimeSpan = TimeSpan.FromSeconds(30.0)

    /// The default backoff before the first retry: 2 seconds.
    static member DefaultBaseRetryDelay: TimeSpan = TimeSpan.FromSeconds(2.0)

    /// The default backoff cap: 30 seconds.
    static member DefaultMaxRetryDelay: TimeSpan = TimeSpan.FromSeconds(30.0)

    /// Returns null when every knob is in range, otherwise a message for
    /// the first violation. A missing or empty secret is a violation
    /// unless unsigned delivery is explicitly allowed.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let secretMissing =
            match Option.ofObj this.SigningSecret with
            | None -> true
            | Some live -> live.Length = 0

        let endpointSet, endpointUsable =
            match Option.ofObj this.Endpoint with
            | None -> false, false
            | Some live ->
                let usable =
                    live.IsAbsoluteUri
                    && (String.Equals(live.Scheme, "http", StringComparison.OrdinalIgnoreCase)
                        || String.Equals(live.Scheme, "https", StringComparison.OrdinalIgnoreCase))

                true, usable

        let violations =
            [|
                if not endpointSet then
                    "Endpoint must be set to the absolute webhook URL."
                elif not endpointUsable then
                    "Endpoint must be an absolute http or https URL."
                if secretMissing && not this.AllowUnsignedDelivery then
                    "SigningSecret is required unless AllowUnsignedDelivery is true."
                if this.Timeout <= TimeSpan.Zero then
                    "Timeout must be positive."
                if this.MaxDeliveryAttempts < 0 then
                    "MaxDeliveryAttempts must not be negative: 0 falls back to CompletionOptions.MaxDeliveryAttempts."
                if this.BaseRetryDelay < TimeSpan.Zero then
                    "BaseRetryDelay must not be negative."
                if this.MaxRetryDelay < TimeSpan.Zero then
                    "MaxRetryDelay must not be negative."
                elif this.MaxRetryDelay < this.BaseRetryDelay then
                    "MaxRetryDelay must not be less than BaseRetryDelay."
            |]

        if violations.Length = 0 then
            null
        else
            Array.head violations

/// Maps one completion to its wire payload. Internal: Notify maps inline
/// (cheap and synchronous) so every retry of the background delivery
/// sends identical bytes.
module internal WebhookCompletionPayloadMapping =

    /// The empty turn id v1 maps: the contract carries no turn id.
    [<Literal>]
    let UnknownTurnId = ""

    /// Serialises one payload to the exact bytes the sink signs and
    /// sends: camelCase, with the outcome's stable $type discriminator.
    /// <param name="payload">The payload to serialise.</param>
    /// <returns>The UTF-8 JSON bytes.</returns>
    let serialize (payload: WebhookCompletionPayload) : byte[] =
        let options = JsonSerializerOptions()
        options.PropertyNamingPolicy <- JsonNamingPolicy.CamelCase
        JsonSerializer.SerializeToUtf8Bytes(payload, options)

    /// Reads one possibly-null string as empty: the runtime-facing
    /// summary sources are typed non-null but a hostile caller can still
    /// hand a null across the boundary.
    /// <param name="text">The text, possibly null at runtime.</param>
    /// <returns>The text, or empty when null.</returns>
    let safeText (text: string) : string = if isNull (box text) then "" else text

    /// Reads the human summary out of the outcome: the finished and
    /// partially-finished summaries, the abort and failure reasons, else
    /// the final assistant text. Null-safe throughout: nulls read as
    /// empty, never throw.
    /// <param name="outcome">The structured outcome, or null.</param>
    /// <param name="assistantText">The final assistant text, or null.</param>
    /// <returns>The delivery summary.</returns>
    let summarize (outcome: TurnOutcome | null) (assistantText: string | null) : string =
        let fallback =
            match Option.ofObj assistantText with
            | Some text -> text
            | None -> ""

        match Option.ofObj outcome with
        | None -> fallback
        | Some(:? TurnFinished as finished) -> safeText finished.Summary
        | Some(:? TurnPartiallyFinished as partial) -> safeText partial.Summary
        | Some(:? TurnAborted as aborted) -> safeText aborted.Reason
        | Some(:? TurnFailed as failed) -> safeText failed.Reason
        | Some _ -> fallback

    /// Maps one completion to its payload at Notify time.
    /// <param name="completion">The completion to map. Must not be null.</param>
    /// <param name="completedAt">The sink-observed enqueue time.</param>
    /// <returns>The wire payload.</returns>
    let map (completion: SessionCompletion) (completedAt: DateTimeOffset) : WebhookCompletionPayload =
        ArgumentNullException.ThrowIfNull(completion)

        let result = completion.TurnResult

        let (assistantText: string | null,
             status: TurnStatus,
             iterations: int,
             usage: UsageSummary,
             outcome: TurnOutcome | null) =
            if isNull (box result) then
                null, TurnStatus.Completed, 0, { InputTokens = 0L; OutputTokens = 0L }, null
            else
                result.AssistantText, result.Status, result.Iterations, result.Usage, result.Outcome

        let usage =
            if isNull (box usage) then
                { InputTokens = 0L; OutputTokens = 0L }
            else
                usage

        let idempotencyKey =
            let key = completion.IdempotencyKey

            if isNull (box key) || String.IsNullOrWhiteSpace key then
                Guid.NewGuid().ToString("N")
            else
                key

        {
            SessionId = completion.SessionId.ToString()
            TurnId = UnknownTurnId
            Status = status.ToString()
            Outcome = outcome
            Summary = summarize outcome assistantText
            OutputFiles = ResizeArray<string>() :> IReadOnlyList<string>
            Iterations = iterations
            Usage = usage
            StartedAt = Nullable<DateTimeOffset>()
            CompletedAt = completedAt
            IdempotencyKey = idempotencyKey
        }

/// The signed POST transport: the only code that touches the SSRF guard
/// besides the issue 74 invoker. Each attempt checks the endpoint through
/// SsrfGuard.checkAsync at call time and sends over a per-call pinned
/// handler (the guard's ConnectCallback captures the pinned endpoint),
/// so a post-check DNS change cannot rebind the connection.
module internal WebhookDeliveryTransport =

    /// The signature header carrying the HMAC hex: sha256=&lt;hex&gt;.
    [<Literal>]
    let SignatureHeader = "X-Legate-Signature"

    /// The idempotency header echoing the payload's key.
    [<Literal>]
    let IdempotencyHeader = "Idempotency-Key"

    /// The cap on mapped exception text: longer messages truncate.
    /// Secrets never reach these paths (the secret only enters the HMAC),
    /// so truncation is the only treatment needed.
    [<Literal>]
    let MaxErrorChars = 500

    /// Renders a timeout bound for error text: whole seconds at or above
    /// one second, milliseconds below (the CustomToolTransport
    /// precedent).
    /// <param name="timeout">The bound that fired.</param>
    /// <returns>The bound's duration text.</returns>
    let formatTimeout (timeout: TimeSpan) : string =
        if timeout.TotalSeconds >= 1.0 then
            sprintf "%d seconds" (int timeout.TotalSeconds)
        else
            sprintf "%d ms" (int timeout.TotalMilliseconds)

    /// Bounds exception text: null reads as empty, longer messages
    /// truncate.
    /// <param name="text">The message to bound.</param>
    /// <returns>The bounded text.</returns>
    let truncate (text: string) : string =
        let safe = if isNull (box text) then "" else text

        if safe.Length > MaxErrorChars then
            safe.Substring(0, MaxErrorChars)
        else
            safe

    /// Why one attempt did not deliver. Internal: the deliverer retries
    /// the transient cases and logs the permanent ones with the attempt
    /// count. Carries no secrets, headers, bodies, or addresses.
    type internal DeliveryFailure =
        /// The endpoint answered 408, 429, or 5xx: retryable.
        | TransientStatus of int
        /// Any other non-success status: permanent.
        | PermanentStatus of int
        /// The per-attempt bound fired: retryable.
        | TimedOut of TimeSpan
        /// The sink's lifetime token fired (host stop or dispose):
        /// permanent, never retried.
        | Canceled
        /// A network or protocol failure carrying a bounded message:
        /// retryable.
        | TransportError of string
        /// The guard denied the endpoint: permanent, nothing was sent.
        | GuardDenied of SsrfDenyReason

        /// Maps the failure to its bounded log string, free of secrets,
        /// headers, bodies, and addresses.
        /// <returns>The bounded reason for this failure.</returns>
        member this.ToBoundedString() : string =
            match this with
            | TransientStatus status -> sprintf "the webhook endpoint returned HTTP status %d." status
            | PermanentStatus status -> sprintf "the webhook endpoint returned HTTP status %d." status
            | TimedOut timeout -> sprintf "the delivery timed out after %s." (formatTimeout timeout)
            | Canceled -> "the delivery was canceled."
            | TransportError message -> "the delivery failed: " + truncate message
            | GuardDenied reason -> reason.ToBoundedString()

        /// Whether another attempt may help.
        /// <returns>True for transient failures, false for permanent ones.</returns>
        member this.IsTransient: bool =
            match this with
            | TransientStatus _ -> true
            | TimedOut _ -> true
            | TransportError _ -> true
            | PermanentStatus _ -> false
            | Canceled -> false
            | GuardDenied _ -> false

    /// Computes the backoff before the given retry: the base doubled per
    /// retry, clamped to the cap. Retry 1 waits the base, retry 2 twice
    /// the base, and so on: min(base * 2^(retry-1), cap).
    /// <param name="baseDelay">The backoff before the first retry.</param>
    /// <param name="cap">The cap every backoff is clamped to.</param>
    /// <param name="retry">The 1-based retry number.</param>
    /// <returns>The bounded backoff.</returns>
    let backoffFor (baseDelay: TimeSpan) (cap: TimeSpan) (retry: int) : TimeSpan =
        let mutable delay = baseDelay

        for _ in 2..retry do
            delay <-
                if delay >= cap then cap
                elif delay.Ticks > Int64.MaxValue / 2L then cap
                else min (TimeSpan.FromTicks(delay.Ticks * 2L)) cap

        min delay cap

    /// Signs the exact body bytes: HMAC-SHA256 hex with the
    /// <c>sha256=&lt;hex&gt;</c> prefix (the issue Notes contract; never
    /// raw hex).
    /// <param name="secret">The signing secret bytes. Must not be null or empty.</param>
    /// <param name="body">The exact request bytes.</param>
    /// <returns>The signature header value.</returns>
    let sign (secret: byte[]) (body: byte[]) : string =
        use hmac = new HMACSHA256(secret)
        "sha256=" + Convert.ToHexString(hmac.ComputeHash(body)).ToLowerInvariant()

    /// Sends one attempt: checks the endpoint through the call-time SSRF
    /// guard, then POSTs the pre-signed body over the pinned endpoint's
    /// default handler with the per-attempt bound linked to the sink's
    /// lifetime token. Returns Ok on any 2xx; every other outcome is a
    /// bounded failure, never a throw (cancellation of the sink's own
    /// token reads as Canceled).
    /// <param name="resolver">The injected address resolver seam.</param>
    /// <param name="guardOptions">The host-level SSRF allow/deny lists.</param>
    /// <param name="endpoint">The absolute webhook URL.</param>
    /// <param name="body">The exact request bytes, signed and sent.</param>
    /// <param name="signature">The signature header value, or null for unsigned delivery.</param>
    /// <param name="idempotencyKey">The idempotency key header value.</param>
    /// <param name="timeout">The per-attempt bound.</param>
    /// <param name="lifetimeToken">The sink's lifetime token.</param>
    /// <returns>Ok on 2xx, else the bounded failure.</returns>
    let postOnceAsync
        (resolver: IHostAddressResolver)
        (guardOptions: SsrfGuardOptions)
        (endpoint: Uri)
        (body: byte[])
        (signature: string | null)
        (idempotencyKey: string)
        (timeout: TimeSpan)
        (lifetimeToken: CancellationToken)
        : Task<Result<unit, DeliveryFailure>> =
        task {
            try
                try
                    use linked = CancellationTokenSource.CreateLinkedTokenSource(lifetimeToken)
                    linked.CancelAfter(timeout)

                    let! guard = SsrfGuard.checkAsync resolver guardOptions endpoint linked.Token

                    match guard with
                    | Error reason -> return Error(GuardDenied reason)
                    | Ok pinned ->
                        use handler = pinned.CreateDefaultHandler()
                        use client = new HttpClient(handler, true)
                        client.Timeout <- Timeout.InfiniteTimeSpan

                        use request = new HttpRequestMessage(HttpMethod.Post, pinned.OriginalUri)
                        use content = new ByteArrayContent(body)
                        content.Headers.ContentType <- Headers.MediaTypeHeaderValue("application/json")
                        request.Content <- content
                        request.Headers.Add(IdempotencyHeader, idempotencyKey)

                        match Option.ofObj signature with
                        | Some value -> request.Headers.Add(SignatureHeader, value)
                        | None -> ()

                        use! response =
                            client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, linked.Token)

                        let status = int response.StatusCode

                        if status >= 200 && status <= 299 then
                            return Ok()
                        elif status = 408 || status = 429 || status >= 500 then
                            return Error(TransientStatus status)
                        else
                            return Error(PermanentStatus status)
                with :? OperationCanceledException ->
                    if lifetimeToken.IsCancellationRequested then
                        return Error Canceled
                    else
                        return Error(TimedOut timeout)
            with failed ->
                return Error(TransportError(truncate failed.Message))
        }

/// The default <see cref="T:Legate.ISessionCompletionSink" />: POSTs the
/// documented JSON wire payload to the configured webhook URL with the
/// <c>Idempotency-Key</c> and <c>X-Legate-Signature: sha256=&lt;hex&gt;</c>
/// headers (HMAC-SHA256 over the exact bytes; no signature header when
/// unsigned delivery was explicitly allowed), a bounded per-attempt
/// timeout, and exponential-backoff retries over the injected
/// <see cref="T:Legate.ILlmDelay" /> seam. Notify is synchronous and
/// non-blocking: it maps the payload and queues a bounded background
/// delivery (at most <c>MaxDeliveryAttempts</c>, defaulting to
/// <c>CompletionOptions.MaxDeliveryAttempts</c>), then returns. Delivery
/// is at-least-once per Notify call: receivers deduplicate on the
/// idempotency key. Disposing cancels in-flight work; deliveries already
/// handed to the endpoint are not recalled. Sealed so the default cannot
/// drift; hosts needing other transports implement
/// <see cref="T:Legate.ISessionCompletionSink" /> directly.
/// <param name="options">The webhook endpoint and delivery knobs. Must be valid.</param>
/// <param name="resolver">The address resolver seam. Must not be null.</param>
/// <param name="guardOptions">The host-level SSRF allow/deny lists. Must not be null.</param>
/// <param name="delay">The backoff seam. Must not be null.</param>
/// <param name="timeProvider">The clock stamping deliveries. Must not be null.</param>
/// <param name="logger">The logger, or null for no logging.</param>
[<Sealed>]
type WebhookCompletionSink
    (
        options: WebhookCompletionSinkOptions,
        resolver: IHostAddressResolver,
        guardOptions: SsrfGuardOptions,
        delay: ILlmDelay,
        timeProvider: TimeProvider,
        logger: ILogger | null
    ) =

    do
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(resolver)
        ArgumentNullException.ThrowIfNull(guardOptions)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(timeProvider)

        match Option.ofObj (options.Validate()) with
        | Some violation -> raise (ArgumentException(violation, nameof options))
        | None -> ()

    let log: ILogger =
        match Option.ofObj logger with
        | Some live -> live
        | None -> NullLogger.Instance :> ILogger

    let clock = timeProvider
    let lifetime = new CancellationTokenSource()
    let mutable disposed = false

    /// Builds the sink with the system delay seam, the system clock, and
    /// no logger.
    /// <param name="options">The webhook endpoint and delivery knobs. Must be valid.</param>
    /// <param name="resolver">The address resolver seam. Must not be null.</param>
    /// <param name="guardOptions">The host-level SSRF allow/deny lists. Must not be null.</param>
    new(options: WebhookCompletionSinkOptions, resolver: IHostAddressResolver, guardOptions: SsrfGuardOptions) =
        new WebhookCompletionSink(
            options,
            resolver,
            guardOptions,
            SystemLlmDelay(TimeProvider.System) :> ILlmDelay,
            TimeProvider.System,
            null
        )

    /// How many attempts this delivery gets: the configured count, or
    /// the <c>CompletionOptions.MaxDeliveryAttempts</c> default when
    /// unset (0).
    member private _.AttemptBudget: int =
        if options.MaxDeliveryAttempts > 0 then
            options.MaxDeliveryAttempts
        else
            CompletionOptions().MaxDeliveryAttempts

    /// Whether a signing secret is configured: null or empty reads as
    /// missing, matching the options validation.
    member private _.HasSecret: bool =
        match Option.ofObj options.SigningSecret with
        | None -> false
        | Some live -> live.Length > 0

    /// The signature for these bytes, or null when unsigned delivery was
    /// explicitly allowed and no secret is configured.
    member private this.SignatureFor(body: byte[]) : string | null =
        match Option.ofObj options.SigningSecret with
        | None -> null
        | Some live when live.Length = 0 -> null
        | Some live -> WebhookDeliveryTransport.sign live body

    /// Delivers one queued completion in the background: up to the
    /// attempt budget, backing off over the delay seam between attempts.
    /// Permanent failures stop immediately; every terminal outcome is
    /// logged with the attempt count, and nothing here ever throws or
    /// carries secrets.
    member private this.DeliverAsync(completion: SessionCompletion, body: byte[]) : Task =
        task {
            try
                let sessionId = completion.SessionId.ToString()

                let key = completion.IdempotencyKey

                let idempotencyKey =
                    if isNull (box key) || String.IsNullOrWhiteSpace key then
                        Guid.NewGuid().ToString("N")
                    else
                        key

                if not this.HasSecret && not options.AllowUnsignedDelivery then
                    log.LogWarning(
                        "Webhook completion delivery skipped for session {SessionId}: no signing secret and unsigned delivery is not allowed.",
                        sessionId
                    )
                else
                    match Option.ofObj options.Endpoint with
                    | None ->
                        log.LogWarning(
                            "Webhook completion delivery skipped for session {SessionId}: no webhook endpoint is configured.",
                            sessionId
                        )
                    | Some endpoint ->
                        let budget = this.AttemptBudget
                        let signature = this.SignatureFor(body)
                        let mutable attempt = 0
                        let mutable finished = false

                        while not finished do
                            attempt <- attempt + 1

                            let! outcome =
                                WebhookDeliveryTransport.postOnceAsync
                                    resolver
                                    guardOptions
                                    endpoint
                                    body
                                    signature
                                    idempotencyKey
                                    options.Timeout
                                    lifetime.Token

                            match outcome with
                            | Ok _ ->
                                log.LogDebug(
                                    "Webhook completion delivered for session {SessionId} on attempt {Attempt}.",
                                    sessionId,
                                    attempt
                                )

                                finished <- true
                            | Error failure when failure.IsTransient && attempt < budget ->
                                log.LogWarning(
                                    "Webhook completion delivery for session {SessionId} failed on attempt {Attempt} of {Budget}, retrying: {Reason}",
                                    sessionId,
                                    attempt,
                                    budget,
                                    failure.ToBoundedString()
                                )

                                let backoff =
                                    WebhookDeliveryTransport.backoffFor
                                        options.BaseRetryDelay
                                        options.MaxRetryDelay
                                        attempt

                                try
                                    do! delay.Delay(backoff, lifetime.Token)
                                with :? OperationCanceledException ->
                                    log.LogError(
                                        "Webhook completion delivery for session {SessionId} canceled on attempt {Attempt} of {Budget}.",
                                        sessionId,
                                        attempt,
                                        budget
                                    )

                                    finished <- true
                            | Error failure ->
                                log.LogError(
                                    "Webhook completion delivery for session {SessionId} failed permanently after {Attempt} of {Budget} attempts (idempotency key {IdempotencyKey}): {Reason}",
                                    sessionId,
                                    attempt,
                                    budget,
                                    idempotencyKey,
                                    failure.ToBoundedString()
                                )

                                finished <- true
            with failed ->
                log.LogError(
                    "Webhook completion delivery failed with an unexpected error: {Reason}",
                    WebhookDeliveryTransport.truncate failed.Message
                )
        }

    /// Receives one completion delivery: maps the documented wire payload
    /// and queues a bounded background delivery, then returns without
    /// blocking. At-least-once per call: receivers deduplicate on the
    /// idempotency key.
    /// <param name="completion">The session's structured completion. Must not be null.</param>
    interface ISessionCompletionSink with
        member this.Notify(completion: SessionCompletion) =
            ArgumentNullException.ThrowIfNull(completion)

            if disposed then
                raise (ObjectDisposedException(nameof WebhookCompletionSink))

            let payload = WebhookCompletionPayloadMapping.map completion (clock.GetUtcNow())

            let body = WebhookCompletionPayloadMapping.serialize payload

            try
                Task.Run(Func<Task>(fun () -> this.DeliverAsync(completion, body))) |> ignore
            with failed ->
                log.LogError(
                    "Webhook completion delivery for session {SessionId} could not start: {Reason}",
                    completion.SessionId.ToString(),
                    WebhookDeliveryTransport.truncate failed.Message
                )

    /// Cancels in-flight delivery work. Deliveries already handed to the
    /// endpoint are not recalled.
    interface IDisposable with
        member _.Dispose() =
            if not disposed then
                disposed <- true
                lifetime.Cancel()
                lifetime.Dispose()
