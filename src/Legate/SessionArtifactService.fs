// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open Microsoft.Extensions.Options

// Session artifact service (issue 117). Stages session artifacts over the
// primitive IBlobStore with IArtifactQuota reserve/commit/release
// accounting: reserve first, store through issue 118's validated-put for
// images and videos (as-is for every other media type), commit on success,
// and release plus delete partials on every failure path. Quota exhaustion
// deletes partials and returns the exhausted outcome (nothing was granted,
// so there is nothing to release); validation rejections release and
// return the rejected outcome (the validated-put writes zero blobs on
// rejection); host quota and store faults release, delete partials, and
// return the failed outcome. Host quota faults are caught at the service
// boundary and mapped to the typed failure: exhaustion is a typed outcome,
// never an exception, and quota exceptions are never control flow.
// Cancellation propagates instead of settling into an outcome.
//
// The scoped store below derives every key through BlobKeys, so no call
// can cross tenants or sessions, and DeletePrefix("") clears exactly the
// session's artifact scope. Outstanding granted reservations are tracked
// against the TimeProvider clock; the reclamation pass releases entries
// the clock aged past ReservationReclaimAfter without a commit or release,
// so a crash between reserve and settle leaks at most one bound plus one
// pass interval. Commit and release stay idempotent per the quota protocol,
// so reclaiming an already-settled reservation is a no-op.

// ──────────────────────────────────────────────────────────────────────────
// Scoped artifact store over the primitive blob store

/// Artifact-scoped blob storage for one tenant and session over the
/// primitive store: every name derives through BlobKeys, so a call can
/// never cross tenants or sessions. Internal: the service builds one per
/// call, so the scope never escapes into crosswords.
type internal ScopedArtifactStore(inner: IBlobStore, tenant: TenantId, sessionId: SessionId) =

    do ArgumentNullException.ThrowIfNull(inner)

    /// The full key prefix every name in this scope derives under.
    member _.ScopePrefix: string = BlobKeys.ArtifactPrefix(tenant, sessionId)

    /// Composes a scope-relative prefix into the full store prefix. The
    /// empty relative prefix reads as the scope itself, whose trailing
    /// slash the primitive validation rejects: trim it, mirroring the
    /// workspace re-bind scope listing. Exact: tenant segments carry no
    /// slashes and session ids are fixed-length ULIDs, so no sibling
    /// scope extends the trimmed prefix.
    member private this.FullPrefix(prefix: string) : string =
        (this.ScopePrefix + BlobKeys.ValidatePrefix prefix).TrimEnd('/')

    /// Maps full store keys back to scope-relative names.
    member private this.Relative(key: string) : string = key.Substring(this.ScopePrefix.Length)

    interface IArtifactBlobStore with
        member this.Get(name, cancellationToken) =
            inner.Get(BlobKeys.ForArtifact(tenant, sessionId, name), cancellationToken)

        member this.Put(name, content, cancellationToken) =
            inner.Put(BlobKeys.ForArtifact(tenant, sessionId, name), content, cancellationToken)

        member this.CompareExchange(name, content, expectedEtag, cancellationToken) =
            inner.CompareExchange(
                BlobKeys.ForArtifact(tenant, sessionId, name),
                content,
                expectedEtag,
                cancellationToken
            )

        member this.OpenRead(name, cancellationToken) =
            inner.OpenRead(BlobKeys.ForArtifact(tenant, sessionId, name), cancellationToken)

        member this.OpenWrite(name, contentType, cancellationToken) =
            inner.OpenWrite(BlobKeys.ForArtifact(tenant, sessionId, name), contentType, cancellationToken)

        member this.List(prefix, cancellationToken) =
            let scope = this.ScopePrefix
            let keys = inner.List(this.FullPrefix prefix, cancellationToken)

            { new IAsyncEnumerable<string> with
                member _.GetAsyncEnumerator(cancellationToken: CancellationToken) =
                    let enumerator = keys.GetAsyncEnumerator(cancellationToken)

                    { new IAsyncEnumerator<string> with
                        member _.Current: string = enumerator.Current.Substring(scope.Length)

                        member _.MoveNextAsync() : ValueTask<bool> = enumerator.MoveNextAsync()

                        member _.DisposeAsync() : ValueTask = enumerator.DisposeAsync()
                    }
            }

        member this.DeletePrefix(prefix, cancellationToken) =
            inner.DeletePrefix(this.FullPrefix prefix, cancellationToken)

        member this.GetMetadata(name, cancellationToken) =
            inner.GetMetadata(BlobKeys.ForArtifact(tenant, sessionId, name), cancellationToken)

        member this.TryGetPresignedUrl(name, expiry, cancellationToken) =
            inner.TryGetPresignedUrl(BlobKeys.ForArtifact(tenant, sessionId, name), expiry, cancellationToken)

// ──────────────────────────────────────────────────────────────────────────
// Outstanding reservation registry

/// Granted reservations the clock has seen but no commit or release has
/// settled yet, keyed by reservation id with the instant the grant landed.
/// Commit and release remove the entry; the reclamation pass releases
/// entries the clock aged past the bound.
module internal SessionArtifactReservations =

    /// Tracks one granted reservation under the clock's now.
    /// <param name="outstanding">The registry.</param>
    /// <param name="reservationId">The granted reservation id.</param>
    /// <param name="clock">The clock now reads.</param>
    let track
        (outstanding: ConcurrentDictionary<string, DateTimeOffset>)
        (reservationId: string)
        (clock: TimeProvider)
        =
        ArgumentNullException.ThrowIfNull(outstanding)
        ArgumentNullException.ThrowIfNull(reservationId)
        ArgumentNullException.ThrowIfNull(clock)
        outstanding[reservationId] <- clock.GetUtcNow()

    /// Forgets one settled reservation. Idempotent: unknown ids are a
    /// no-op.
    /// <param name="outstanding">The registry.</param>
    /// <param name="reservationId">The settled reservation id.</param>
    let settle (outstanding: ConcurrentDictionary<string, DateTimeOffset>) (reservationId: string) =
        ArgumentNullException.ThrowIfNull(outstanding)
        ArgumentNullException.ThrowIfNull(reservationId)
        outstanding.TryRemove(reservationId) |> ignore

    /// Releases every entry the clock aged past the bound without a commit
    /// or release and returns how many were reclaimed. Release faults are
    /// swallowed: compensation must never fail the pass, and the entry is
    /// already forgotten. Cancellation propagates with the abandoned entry
    /// re-tracked under its original instant.
    /// <param name="outstanding">The registry.</param>
    /// <param name="quota">The quota releases go to.</param>
    /// <param name="clock">The clock ages read.</param>
    /// <param name="bound">How long a grant may sit unsettled.</param>
    /// <param name="cancellationToken">Abandons the pass.</param>
    /// <returns>How many reservations were reclaimed.</returns>
    let reclaimPassAsync
        (outstanding: ConcurrentDictionary<string, DateTimeOffset>)
        (quota: IArtifactQuota)
        (clock: TimeProvider)
        (bound: TimeSpan)
        (cancellationToken: CancellationToken)
        : Task<int> =
        ArgumentNullException.ThrowIfNull(outstanding)
        ArgumentNullException.ThrowIfNull(quota)
        ArgumentNullException.ThrowIfNull(clock)

        task {
            let now = clock.GetUtcNow()
            let mutable reclaimed = 0

            for entry in outstanding.ToArray() do
                cancellationToken.ThrowIfCancellationRequested()

                if now - entry.Value >= bound then
                    match outstanding.TryRemove(KeyValuePair(entry.Key, entry.Value)) with
                    | false -> ()
                    | true ->
                        try
                            do! quota.Release(entry.Key, cancellationToken)
                            reclaimed <- reclaimed + 1
                        with
                        | :? OperationCanceledException as canceled ->
                            outstanding[entry.Key] <- entry.Value
                            raise canceled
                        | _ -> reclaimed <- reclaimed + 1

            return reclaimed
        }

// ──────────────────────────────────────────────────────────────────────────
// The service

/// Carries an internal validation rejection out of the store step to the
/// outcome mapping in StageAsync. Internal: constructed and caught inside
/// the service only, never escapes to hosts.
/// <param name="reason">The bounded rejection reason.</param>
/// <param name="limit">The breached limit, or -1.</param>
/// <param name="observed">The observed value, or -1.</param>
type internal ArtifactRejectedException(reason: string, limit: int64, observed: int64) =
    inherit Exception(reason)
    /// The bounded rejection reason.
    member _.Reason = reason
    /// The breached limit, or -1.
    member _.Limit = limit
    /// The observed value, or -1.
    member _.Observed = observed

/// Quota-accounted session artifact staging over the primitive blob store.
/// Internal so no store type ever crosses the public API; hosts resolve
/// <see cref="T:Legate.ISessionArtifactService" />.
type internal SessionArtifactService
    (serviceProvider: IServiceProvider, options: IOptions<ArtifactOptions>, quota: IArtifactQuota, clock: TimeProvider)
    =
    do
        ArgumentNullException.ThrowIfNull(serviceProvider)
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(quota)
        ArgumentNullException.ThrowIfNull(clock)

    let outstanding =
        ConcurrentDictionary<string, DateTimeOffset>(StringComparer.Ordinal)

    /// The registry tests drive under virtual time.
    member internal _.Outstanding: ConcurrentDictionary<string, DateTimeOffset> =
        outstanding

    /// The effective options: the bound value, or defaults when the
    /// container carries none.
    member private _.EffectiveOptions() : ArtifactOptions =
        match box options.Value with
        | null -> ArtifactOptions()
        | :? ArtifactOptions as live -> live
        | _ -> ArtifactOptions()

    /// Resolves the primitive blob store, failing with the registration
    /// naming the missing store instead of a resolution error.
    member private _.BlobStore() : IBlobStore =
        match serviceProvider.GetService<IBlobStore>() with
        | null ->
            raise (
                InvalidOperationException(
                    "No IBlobStore is registered: session artifacts need a blob store. "
                    + "Register one (the in-memory store for tests, a file-system, SQLite, or S3 store for hosts)."
                )
            )
        | store -> store

    /// Normalises a content type for allow-list matching and storage,
    /// mirroring the validated-put rules: trims, lowercases, strips
    /// parameters, and falls back to octet-stream when blank.
    static member private NormalizeMediaType(contentType: string | null) : string =
        match box contentType with
        | null -> "application/octet-stream"
        | :? string as raw when String.IsNullOrWhiteSpace raw -> "application/octet-stream"
        | :? string as raw ->
            let cleaned = raw.Trim().ToLowerInvariant()
            let semi = cleaned.IndexOf(';')

            if semi < 0 then
                cleaned
            else
                cleaned.Substring(0, semi).Trim()
        | _ -> "application/octet-stream"

    /// Reports whether the normalised media type is in the allow-list. A
    /// null list reads as empty.
    static member private IsAllowed(allowed: List<string> | null, mediaType: string) : bool =
        match box allowed with
        | null -> false
        | _ ->
            (unbox<List<string>> allowed)
            |> Seq.exists (fun entry ->
                not (isNull (box entry))
                && String.Equals(SessionArtifactService.NormalizeMediaType entry, mediaType, StringComparison.Ordinal))

    /// Maps an internal validation error onto the bounded rejected-outcome
    /// shape: the fixed reason set plus the limit numbers when the breach
    /// names one.
    static member private RejectionOf(error: Artifacts.ArtifactValidationError) : string * int64 * int64 =
        match error with
        | Artifacts.ArtifactValidationError.LimitExceeded(limitKind, limit, observed) -> limitKind, limit, observed
        | Artifacts.ArtifactValidationError.UnsupportedMediaType _ -> "unsupportedMediaType", -1L, -1L
        | Artifacts.ArtifactValidationError.Undecodable reason when reason = "emptyPayload" -> "emptyPayload", -1L, -1L
        | Artifacts.ArtifactValidationError.Undecodable _ -> "undecodable", -1L, -1L

    /// Best-effort release: compensation must never fail the stage, so
    /// release faults are swallowed. Cancellation propagates.
    static member private releaseQuietly
        (quota: IArtifactQuota)
        (reservationId: string)
        (cancellationToken: CancellationToken)
        : Task =
        task {
            try
                do! quota.Release(reservationId, cancellationToken)
            with
            | :? OperationCanceledException as canceled -> return! Task.FromException(canceled)
            | _ -> ()
        }

    /// Deletes exactly the names given and nothing else: lists the scope,
    /// keeps the victims that are present, and deletes a victim only when
    /// no surviving sibling extends it (an adversarial prefix-colliding
    /// name is left in place and documented rather than risking a
    /// neighbour). Never throws past the caller.
    static member private deleteNamesQuietly
        (store: IArtifactBlobStore)
        (names: string list)
        (cancellationToken: CancellationToken)
        : Task =
        task {
            try
                let listed = ResizeArray<string>()

                let enumerator =
                    store.List("", cancellationToken).GetAsyncEnumerator(cancellationToken)

                let mutable keepGoing = true

                while keepGoing do
                    try
                        let! advanced = enumerator.MoveNextAsync().AsTask()

                        if advanced then
                            if not (isNull (box enumerator.Current)) then
                                listed.Add(enumerator.Current)
                        else
                            keepGoing <- false

                            try
                                do! enumerator.DisposeAsync().AsTask()
                            with _ ->
                                ()
                    with _ ->
                        keepGoing <- false

                let present = HashSet<string>(listed, StringComparer.Ordinal)
                let victims = names |> List.filter present.Contains

                for victim in victims do
                    let shadowed =
                        listed
                        |> Seq.exists (fun other ->
                            other <> victim && other.StartsWith(victim, StringComparison.Ordinal))

                    if not shadowed then
                        try
                            let! _ = store.DeletePrefix(victim, cancellationToken)
                            ()
                        with _ ->
                            ()
            with _ ->
                ()
        }

    /// The deterministic names one stage may leave behind: the artifact
    /// itself plus its preview.
    static member private StageNames(name: string) : string list =
        [ name; name + Artifacts.PreviewSuffix ]

    /// Builds the stage descriptor from a validated-put result.
    static member private DescribeStored
        (name: string)
        (mediaType: string)
        (stored: Artifacts.StoredArtifact)
        : ArtifactDescriptor =
        {
            Name = name
            MediaType = mediaType
            SizeBytes = stored.SizeBytes
            Width = stored.Width
            Height = stored.Height
            Duration =
                match stored.Duration with
                | Some span -> Nullable span
                | None -> Nullable()
            PreviewName =
                match stored.PreviewName with
                | Some preview -> preview
                | None -> null
            DownloadUrl = null
            Reference = ArtifactReference.Format(name, mediaType, stored.SizeBytes, stored.Width, stored.Height)
        }

    /// Stores the payload: validated-put for allow-listed images and
    /// videos, as-is for every other media type. Validation rejections
    /// carry the bounded reason; store faults throw.
    member private _.StorePayloadAsync
        (artifactOptions: ArtifactOptions)
        (store: IArtifactBlobStore)
        (name: string)
        (mediaType: string)
        (payload: byte[])
        (cancellationToken: CancellationToken)
        : Task<ArtifactDescriptor> =
        task {
            if
                SessionArtifactService.IsAllowed(artifactOptions.AllowedImageMediaTypes, mediaType)
                || SessionArtifactService.IsAllowed(artifactOptions.AllowedVideoMediaTypes, mediaType)
            then
                let! validated =
                    Artifacts.putValidated
                        artifactOptions
                        store
                        name
                        (BlobContent(payload, mediaType))
                        cancellationToken

                match validated with
                | Ok stored -> return SessionArtifactService.DescribeStored name mediaType stored
                | Error validation ->
                    let reason, limit, observed = SessionArtifactService.RejectionOf validation
                    return! Task.FromException<ArtifactDescriptor>(ArtifactRejectedException(reason, limit, observed))
            else
                let! _ = store.Put(name, BlobContent(payload, mediaType), cancellationToken)

                return
                    {
                        Name = name
                        MediaType = mediaType
                        SizeBytes = int64 payload.Length
                        Width = 0
                        Height = 0
                        Duration = Nullable()
                        PreviewName = null
                        DownloadUrl = null
                        Reference = ArtifactReference.Format(name, mediaType, int64 payload.Length, 0, 0)
                    }
        }

    interface ISessionArtifactService with
        member this.StageAsync(tenant, sessionId, name, content, cancellationToken) =
            if isNull (box name) then
                raise (ArgumentNullException(nameof name))

            // Invalid names fail before any reservation: BlobKeys pins the
            // key rules once for every backend.
            BlobKeys.Validate name |> ignore

            task {
                cancellationToken.ThrowIfCancellationRequested()

                let inner = this.BlobStore()
                let artifactOptions = this.EffectiveOptions()

                match artifactOptions.Validate() with
                | null -> ()
                | violation -> raise (InvalidOperationException($"Invalid ArtifactOptions: %s{violation}"))

                let payload =
                    if isNull (box content.Bytes) then
                        Array.empty<byte>
                    else
                        content.Bytes

                let mediaType = SessionArtifactService.NormalizeMediaType content.ContentType
                let store = ScopedArtifactStore(inner, tenant, sessionId) :> IArtifactBlobStore

                let request: ArtifactQuotaRequest =
                    {
                        Tenant = tenant
                        SessionId = sessionId
                        RequestedBytes = int64 payload.Length
                    }

                let! decision =
                    task {
                        try
                            let! granted = quota.Reserve(request, cancellationToken)
                            return Some granted
                        with
                        | :? OperationCanceledException as canceled ->
                            return! Task.FromException<ArtifactQuotaDecision option>(canceled)
                        | _ -> return None
                    }

                match decision with
                | None -> return ArtifactStageFailed("quotaFault") :> ArtifactStageOutcome
                | Some(:? QuotaExhausted as exhausted) ->
                    // Nothing was granted, so there is nothing to release;
                    // sweep stale bytes under the target names so a retry
                    // starts clean.
                    do!
                        SessionArtifactService.deleteNamesQuietly
                            store
                            (SessionArtifactService.StageNames name)
                            cancellationToken

                    return
                        ArtifactQuotaExhausted(exhausted.RequestedBytes, exhausted.AllowedBytes) :> ArtifactStageOutcome
                | Some(:? QuotaGranted as granted) when String.IsNullOrWhiteSpace granted.ReservationId ->
                    return ArtifactStageFailed("quotaFault") :> ArtifactStageOutcome
                | Some(:? QuotaGranted as granted) ->
                    SessionArtifactReservations.track outstanding granted.ReservationId clock

                    try
                        let! descriptor =
                            this.StorePayloadAsync artifactOptions store name mediaType payload cancellationToken

                        let! committed =
                            task {
                                try
                                    do! quota.Commit(granted.ReservationId, cancellationToken)
                                    return true
                                with
                                | :? OperationCanceledException as canceled ->
                                    return! Task.FromException<bool>(canceled)
                                | _ -> return false
                            }

                        if not committed then
                            // The store holds an uncommitted payload: release
                            // the reservation and sweep the staged names so
                            // no orphan survives and no quota stays debited.
                            do! SessionArtifactService.releaseQuietly quota granted.ReservationId cancellationToken

                            do!
                                SessionArtifactService.deleteNamesQuietly
                                    store
                                    (SessionArtifactService.StageNames name)
                                    cancellationToken

                            SessionArtifactReservations.settle outstanding granted.ReservationId
                            return ArtifactStageFailed("quotaFault") :> ArtifactStageOutcome
                        else
                            SessionArtifactReservations.settle outstanding granted.ReservationId
                            return ArtifactStored(descriptor) :> ArtifactStageOutcome
                    with
                    | :? OperationCanceledException as canceled ->
                        // Abandoned mid-stage: compensate with an
                        // unlinked token (the caller's is gone), forget the
                        // reservation, and propagate.
                        do! SessionArtifactService.releaseQuietly quota granted.ReservationId CancellationToken.None

                        do!
                            SessionArtifactService.deleteNamesQuietly
                                store
                                (SessionArtifactService.StageNames name)
                                CancellationToken.None

                        SessionArtifactReservations.settle outstanding granted.ReservationId
                        return! Task.FromException<ArtifactStageOutcome>(canceled)
                    | :? ArtifactRejectedException as rejected ->
                        // The validated-put wrote zero blobs on rejection:
                        // release and report, nothing to delete.
                        do! SessionArtifactService.releaseQuietly quota granted.ReservationId cancellationToken
                        SessionArtifactReservations.settle outstanding granted.ReservationId

                        return
                            ArtifactRejected(rejected.Reason, rejected.Limit, rejected.Observed) :> ArtifactStageOutcome
                    | _ ->
                        do! SessionArtifactService.releaseQuietly quota granted.ReservationId cancellationToken

                        do!
                            SessionArtifactService.deleteNamesQuietly
                                store
                                (SessionArtifactService.StageNames name)
                                cancellationToken

                        SessionArtifactReservations.settle outstanding granted.ReservationId
                        return ArtifactStageFailed("storeFault") :> ArtifactStageOutcome
                | Some _ -> return ArtifactStageFailed("quotaFault") :> ArtifactStageOutcome
            }

        member this.DescribeAsync(tenant, sessionId, name, cancellationToken) =
            if isNull (box name) then
                raise (ArgumentNullException(nameof name))

            BlobKeys.Validate name |> ignore

            task {
                cancellationToken.ThrowIfCancellationRequested()

                let inner = this.BlobStore()
                let artifactOptions = this.EffectiveOptions()

                match artifactOptions.Validate() with
                | null -> ()
                | violation -> raise (InvalidOperationException($"Invalid ArtifactOptions: %s{violation}"))

                let store = ScopedArtifactStore(inner, tenant, sessionId) :> IArtifactBlobStore
                let! metadata = store.GetMetadata(name, cancellationToken)

                match box metadata with
                | null -> return ArtifactMissing(name) :> ArtifactDescribeOutcome
                | :? BlobMetadata as found ->
                    let! preview = store.GetMetadata(name + Artifacts.PreviewSuffix, cancellationToken)

                    let! url = store.TryGetPresignedUrl(name, artifactOptions.PresignedExpiry, cancellationToken)

                    let descriptor: ArtifactDescriptor =
                        {
                            Name = name
                            MediaType = found.ContentType
                            SizeBytes = found.SizeBytes
                            Width = 0
                            Height = 0
                            Duration = Nullable()
                            PreviewName =
                                match box preview with
                                | null -> null
                                | _ -> name + Artifacts.PreviewSuffix
                            DownloadUrl = url
                            Reference = ArtifactReference.Format(name, found.ContentType, found.SizeBytes, 0, 0)
                        }

                    return ArtifactFound(descriptor) :> ArtifactDescribeOutcome
                | _ -> return ArtifactMissing(name) :> ArtifactDescribeOutcome
            }

        member this.ClearSessionAsync(tenant, sessionId, cancellationToken) =
            task {
                cancellationToken.ThrowIfCancellationRequested()

                let inner = this.BlobStore()
                let store = ScopedArtifactStore(inner, tenant, sessionId) :> IArtifactBlobStore
                return! store.DeletePrefix("", cancellationToken)
            }

        member _.ReclaimStaleReservationsAsync(cancellationToken) =
            task {
                cancellationToken.ThrowIfCancellationRequested()

                let artifactOptions =
                    match box options.Value with
                    | null -> ArtifactOptions()
                    | :? ArtifactOptions as live -> live
                    | _ -> ArtifactOptions()

                match artifactOptions.Validate() with
                | null -> ()
                | violation -> raise (InvalidOperationException($"Invalid ArtifactOptions: %s{violation}"))

                return!
                    SessionArtifactReservations.reclaimPassAsync
                        outstanding
                        quota
                        clock
                        artifactOptions.ReservationReclaimAfter
                        cancellationToken
            }

// ──────────────────────────────────────────────────────────────────────────
// Reclamation hosted service

/// Singleton hosted service owning the reservation-reclamation loop: one
/// <see cref="M:Legate.ISessionArtifactService.ReclaimStaleReservationsAsync*" />
/// pass every tick through the injected clock seam, so virtual-time tests
/// advance the pass without sleeping. The tick interval is the configured
/// reclamation bound. A pass failure is retried next interval; host stop
/// cancels the wait and the in-flight pass. The service resolves lazily
/// from the provider at loop start, so an empty host still fails startup
/// with the required-registration message instead of a resolution error.
type internal SessionArtifactReclamationService
    (serviceProvider: IServiceProvider, options: IOptions<ArtifactOptions>, clock: TimeProvider, delay: ILlmDelay) =

    do
        ArgumentNullException.ThrowIfNull(serviceProvider)
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(clock)
        ArgumentNullException.ThrowIfNull(delay)

    let log: ILogger = NullLogger.Instance :> ILogger
    let lifetime = new CancellationTokenSource()
    let mutable loop: Task | null = null

    /// The tick interval for the loop: the configured reclamation bound. A
    /// non-positive result falls back to five minutes so a misconfigured
    /// host can never busy-loop; options validation rejects such values at
    /// startup.
    /// <returns>How long the loop waits between passes.</returns>
    member private _.TickInterval() : TimeSpan =
        let bound =
            match box options.Value with
            | null -> TimeSpan.Zero
            | :? ArtifactOptions as live -> live.ReservationReclaimAfter
            | _ -> TimeSpan.Zero

        if bound > TimeSpan.Zero then
            bound
        else
            TimeSpan.FromMinutes 5.0

    /// Runs one reclamation pass.
    /// <param name="cancellationToken">Abandons the pass.</param>
    /// <returns>How many reservations were reclaimed.</returns>
    member internal _.RunOnceAsync(cancellationToken: CancellationToken) : Task<int> =
        task {
            let service = serviceProvider.GetRequiredService<ISessionArtifactService>()
            return! service.ReclaimStaleReservationsAsync(cancellationToken)
        }

    /// Runs the pass loop until host stop.
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
                        "Artifact reservation reclamation pass failed and will retry next interval: {Reason}",
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

/// Registers the artifact service, its options, and the reclamation loop.
module internal SessionArtifactRegistration =

    /// Registers ISessionArtifactService as a singleton (host replacements
    /// win via TryAdd ordering), the ArtifactOptions pipeline with
    /// defaults, and the reclamation hosted service.
    /// <param name="services">The container to add the service to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddSingleton<ISessionArtifactService, SessionArtifactService>()
        |> ignore

        services.AddOptions<ArtifactOptions>() |> ignore

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SessionArtifactReclamationService>())
        |> ignore
