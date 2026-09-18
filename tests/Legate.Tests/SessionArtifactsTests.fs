// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionArtifactsTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options
open Xunit

// Session artifact service (issue 117): quota-accounted staging over the
// in-memory blob store. Reserve/upload/commit ordering with release on
// every failure path; exhaustion deletes partials and returns the typed
// outcome; validation rejections release with zero writes; host quota
// faults are caught at the boundary into the typed failure; Describe
// presigns under ArtifactOptions.PresignedExpiry with null-URL shapes for
// backends that cannot presign; ClearSession clears the scope; the
// reclamation pass releases stale reservations under virtual time (never
// sleeps); and the end-to-end fact proves the #116 acceptance (a staged
// image yields an event reference plus a downloadable URL).

let tenant = TenantId.Create "acme"

/// A real 1x1 PNG: decodable by the validated-put (header-only fixtures
/// are not).
let png1x1 () : byte[] =
    Convert.FromBase64String(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
    )

let pdfBytes () : byte[] =
    [|
        0x25uy
        0x50uy
        0x44uy
        0x46uy
        0x2Duy
    |]

let awaitTask (pending: Task<'T>) : 'T = pending.GetAwaiter().GetResult()

let singleField (items: 'T list) : 'T =
    match items with
    | [ one ] -> one
    | _ -> failwith $"Expected exactly one item, observed %d{items.Length}."

/// Unwraps possibly-missing bytes, failing the test with the label when
/// the blob is absent.
let requireBytes (label: string) (bytes: byte[] | null) : byte[] =
    match bytes |> Option.ofObj with
    | Some found -> found
    | None -> failwith $"Expected bytes for %s{label}."

/// Unwraps a possibly-missing cell text, failing the test when absent.
let contentOf (cell: SessionCell) : string =
    match cell.Content |> Option.ofObj with
    | Some text -> text
    | None -> failwith "Expected cell content."

/// Unwraps a cell's artifact references, failing the test when absent.
let artifactsOf (cell: SessionCell) : string list =
    match cell.Artifacts |> Option.ofObj with
    | Some artifacts -> artifacts |> Seq.toList
    | None -> failwith "Expected artifact references."

// ──────────────────────────────────────────────────────────────────────────
// Quota fakes

/// A counting quota with a fixed byte allowance: grants while the
/// allowance covers the request, exhausts otherwise.
type CountingQuota(allowance: int64) =
    let mutable remaining = allowance
    let committed = ResizeArray<string>()
    let released = ResizeArray<string>()
    let mutable nextId = 0

    interface IArtifactQuota with
        member _.Reserve(request, _) =
            if request.RequestedBytes <= remaining then
                nextId <- nextId + 1
                remaining <- remaining - request.RequestedBytes

                Task.FromResult(ArtifactQuotaDecision.Grant $"r-{nextId}")
            else
                Task.FromResult(ArtifactQuotaDecision.Exhausted(request.RequestedBytes, remaining))

        member _.Commit(reservationId, _) =
            committed.Add(reservationId)
            Task.CompletedTask

        member _.Release(reservationId, _) =
            released.Add(reservationId)
            Task.CompletedTask

    /// The committed reservation ids, in order.
    member _.Committed: string list = committed |> Seq.toList

    /// The released reservation ids, in order.
    member _.Released: string list = released |> Seq.toList

/// A quota whose reserve always throws: host faults become typed
/// failures, never propagates.
type ThrowingQuota() =

    interface IArtifactQuota with
        member _.Reserve(_, _) : Task<ArtifactQuotaDecision> =
            raise (InvalidOperationException("quota unavailable"))

        member _.Commit(_, _) = Task.CompletedTask
        member _.Release(_, _) = Task.CompletedTask

/// A quota answering every reserve with the fixed decision.
type ScriptedQuota(decision: ArtifactQuotaDecision) =

    interface IArtifactQuota with
        member _.Reserve(_, _) = Task.FromResult(decision)
        member _.Commit(_, _) = Task.CompletedTask
        member _.Release(_, _) = Task.CompletedTask

/// A quota decision the contract does not define: the service maps it to
/// the typed failure rather than throwing.
type MysteryDecision() =
    inherit ArtifactQuotaDecision()

// ──────────────────────────────────────────────────────────────────────────
// Store fakes

/// A blob store delegating everything to the inner store.
type DelegatingStore(inner: IBlobStore) =

    interface IBlobStore with
        member _.Get(key, cancellationToken) = inner.Get(key, cancellationToken)

        member _.Put(key, content, cancellationToken) =
            inner.Put(key, content, cancellationToken)

        member _.CompareExchange(key, content, expectedEtag, cancellationToken) =
            inner.CompareExchange(key, content, expectedEtag, cancellationToken)

        member _.OpenRead(key, cancellationToken) = inner.OpenRead(key, cancellationToken)

        member _.OpenWrite(key, contentType, cancellationToken) =
            inner.OpenWrite(key, contentType, cancellationToken)

        member _.List(prefix, cancellationToken) = inner.List(prefix, cancellationToken)

        member _.DeletePrefix(prefix, cancellationToken) =
            inner.DeletePrefix(prefix, cancellationToken)

        member _.GetMetadata(key, cancellationToken) =
            inner.GetMetadata(key, cancellationToken)

        member _.TryGetPresignedUrl(key, expiry, cancellationToken) =
            inner.TryGetPresignedUrl(key, expiry, cancellationToken)

/// A blob store whose puts always fail: mid-stage store faults.
type FailingPutStore(inner: IBlobStore) =
    inherit DelegatingStore(inner)

    interface IBlobStore with
        member _.Put(_, _, _) : Task<BlobMetadata> =
            raise (InvalidOperationException("store unavailable"))

/// A blob store presigning every key under a fake https URL and capturing
/// the expiry it was asked for.
type PresigningStore(inner: IBlobStore) =
    inherit DelegatingStore(inner)
    let mutable lastExpiry = Nullable<TimeSpan>()

    interface IBlobStore with
        member _.TryGetPresignedUrl(key, expiry, _) =
            lastExpiry <- Nullable expiry

            let url: Uri | null = Uri($"https://artifacts.example/{Uri.EscapeDataString key}")

            Task.FromResult url

    /// The expiry the last presign was asked for.
    member _.LastExpiry: Nullable<TimeSpan> = lastExpiry

// ──────────────────────────────────────────────────────────────────────────
// Service construction

/// Builds the service over the store and quota with the clock and options.
let serviceWith
    (store: IBlobStore)
    (quota: IArtifactQuota)
    (clock: TimeProvider)
    (configure: Action<ArtifactOptions>)
    : ISessionArtifactService =
    let services = ServiceCollection()
    services.AddSingleton<IBlobStore>(store) |> ignore
    services.AddOptions<ArtifactOptions>().Configure(configure) |> ignore
    let provider = services.BuildServiceProvider()

    SessionArtifactService(provider, provider.GetRequiredService<IOptions<ArtifactOptions>>(), quota, clock)
    :> ISessionArtifactService

let defaultService (store: IBlobStore) (quota: IArtifactQuota) (clock: TimeProvider) : ISessionArtifactService =
    serviceWith store quota clock (Action<ArtifactOptions>(fun _ -> ()))

let stage
    (service: ISessionArtifactService)
    (sessionId: SessionId)
    (name: string)
    (bytes: byte[])
    (mediaType: string)
    : ArtifactStageOutcome =
    service.StageAsync(tenant, sessionId, name, BlobContent(bytes, mediaType), CancellationToken.None)
    |> awaitTask

let describe (service: ISessionArtifactService) (sessionId: SessionId) (name: string) : ArtifactDescribeOutcome =
    service.DescribeAsync(tenant, sessionId, name, CancellationToken.None)
    |> awaitTask

let storedOf (outcome: ArtifactStageOutcome) : ArtifactDescriptor =
    match outcome with
    | :? ArtifactStored as stored -> stored.Descriptor
    | _ -> failwith $"Expected ArtifactStored, observed %s{outcome.GetType().Name}."

let foundOf (outcome: ArtifactDescribeOutcome) : ArtifactDescriptor =
    match outcome with
    | :? ArtifactFound as found -> found.Descriptor
    | _ -> failwith $"Expected ArtifactFound, observed %s{outcome.GetType().Name}."

// ──────────────────────────────────────────────────────────────────────────
// Stage

[<Fact>]
let ``Stage stores an image with quota accounting`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let quota = CountingQuota(1_000_000L)
    let service = defaultService inner quota (TestClock())
    let sessionId = SessionId.New()
    let payload = png1x1 ()

    let descriptor = stage service sessionId "shot.png" payload "image/png" |> storedOf

    descriptor.Name |> should equal "shot.png"
    descriptor.MediaType |> should equal "image/png"
    descriptor.SizeBytes |> should equal (int64 payload.Length)
    descriptor.Width |> should equal 1
    descriptor.Height |> should equal 1
    descriptor.PreviewName |> should equal null
    descriptor.DownloadUrl |> should equal null
    descriptor.Reference.Contains("shot.png") |> should equal true
    descriptor.Reference.Contains("dimensions=\"1x1\"") |> should equal true
    descriptor.Duration.HasValue |> should equal false
    quota.Committed |> should equal [ "r-1" ]
    quota.Released.Length |> should equal 0
    (service :?> SessionArtifactService).Outstanding.Count |> should equal 0

    let stored =
        inner.Get(BlobKeys.ForArtifact(tenant, sessionId, "shot.png"), CancellationToken.None)
        |> awaitTask
        |> requireBytes "shot.png"

    stored.Length |> should equal payload.Length

[<Fact>]
let ``Stage stores non-image payloads as-is with quota accounting`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let quota = CountingQuota(1_000_000L)
    let service = defaultService inner quota (TestClock())
    let sessionId = SessionId.New()
    let payload = pdfBytes ()

    let descriptor =
        stage service sessionId "report.pdf" payload "application/pdf" |> storedOf

    descriptor.MediaType |> should equal "application/pdf"
    descriptor.SizeBytes |> should equal (int64 payload.Length)
    descriptor.Width |> should equal 0
    descriptor.Height |> should equal 0
    descriptor.Reference.Contains("report.pdf") |> should equal true
    quota.Committed.Length |> should equal 1

[<Fact>]
let ``Exhaustion deletes partials and returns the typed outcome`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let quota = CountingQuota(0L)
    let service = defaultService inner quota (TestClock())
    let sessionId = SessionId.New()
    let payload = png1x1 ()

    // A stale partial from an earlier attempt sits under the target name.
    inner.Put(
        BlobKeys.ForArtifact(tenant, sessionId, "shot.png"),
        BlobContent(payload, "image/png"),
        CancellationToken.None
    )
    |> awaitTask
    |> ignore

    match stage service sessionId "shot.png" payload "image/png" with
    | :? ArtifactQuotaExhausted as exhausted ->
        exhausted.RequestedBytes |> should equal (int64 payload.Length)
        exhausted.AllowedBytes |> should equal 0L
    | outcome -> failwith $"Expected ArtifactQuotaExhausted, observed %s{outcome.GetType().Name}."

    quota.Committed.Length |> should equal 0
    quota.Released.Length |> should equal 0
    (service :?> SessionArtifactService).Outstanding.Count |> should equal 0

    match describe service sessionId "shot.png" with
    | :? ArtifactMissing -> ()
    | outcome -> failwith $"Expected ArtifactMissing after partial cleanup, observed %s{outcome.GetType().Name}."

[<Fact>]
let ``Reserve fault returns the typed failure and stores nothing`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let service = defaultService inner (ThrowingQuota()) (TestClock())
    let sessionId = SessionId.New()

    match stage service sessionId "shot.png" (png1x1 ()) "image/png" with
    | :? ArtifactStageFailed as failed -> failed.Reason |> should equal "quotaFault"
    | outcome -> failwith $"Expected ArtifactStageFailed, observed %s{outcome.GetType().Name}."

    match describe service sessionId "shot.png" with
    | :? ArtifactMissing -> ()
    | outcome -> failwith $"Expected ArtifactMissing, observed %s{outcome.GetType().Name}."

[<Fact>]
let ``Upload fault releases the reservation and deletes partials`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let quota = CountingQuota(1_000_000L)

    let service =
        defaultService (FailingPutStore(inner) :> IBlobStore) quota (TestClock())

    let sessionId = SessionId.New()
    let payload = pdfBytes ()

    inner.Put(
        BlobKeys.ForArtifact(tenant, sessionId, "report.pdf"),
        BlobContent(payload, "application/pdf"),
        CancellationToken.None
    )
    |> awaitTask
    |> ignore

    match stage service sessionId "report.pdf" payload "application/pdf" with
    | :? ArtifactStageFailed as failed -> failed.Reason |> should equal "storeFault"
    | outcome -> failwith $"Expected ArtifactStageFailed, observed %s{outcome.GetType().Name}."

    quota.Committed.Length |> should equal 0
    quota.Released |> should equal [ "r-1" ]
    (service :?> SessionArtifactService).Outstanding.Count |> should equal 0

    let leftover =
        inner.Get(BlobKeys.ForArtifact(tenant, sessionId, "report.pdf"), CancellationToken.None)
        |> awaitTask

    (leftover |> Option.ofObj).IsNone |> should equal true

[<Fact>]
let ``Validation rejection releases and stores nothing`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let quota = CountingQuota(1_000_000L)
    let service = defaultService inner quota (TestClock())
    let sessionId = SessionId.New()

    match stage service sessionId "shot.png" [| 1uy; 2uy; 3uy |] "image/png" with
    | :? ArtifactRejected as rejected ->
        rejected.Reason |> should equal "undecodable"
        rejected.Limit |> should equal -1L
        rejected.Observed |> should equal -1L
    | outcome -> failwith $"Expected ArtifactRejected, observed %s{outcome.GetType().Name}."

    quota.Committed.Length |> should equal 0
    quota.Released |> should equal [ "r-1" ]

    match describe service sessionId "shot.png" with
    | :? ArtifactMissing -> ()
    | outcome -> failwith $"Expected ArtifactMissing, observed %s{outcome.GetType().Name}."

[<Fact>]
let ``Oversized image maps to the bounded rejected outcome`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let quota = CountingQuota(1_000_000L)

    let service =
        serviceWith inner quota (TestClock()) (Action<ArtifactOptions>(fun options -> options.MaxEncodedBytes <- 4L))

    match stage service (SessionId.New()) "shot.png" (png1x1 ()) "image/png" with
    | :? ArtifactRejected as rejected ->
        rejected.Reason |> should equal "encodedBytes"
        rejected.Limit |> should equal 4L
        rejected.Observed |> should equal (int64 (png1x1 ()).Length)
    | outcome -> failwith $"Expected ArtifactRejected, observed %s{outcome.GetType().Name}."

[<Fact>]
let ``Unknown quota decision shape returns the typed failure`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore

    let service =
        defaultService inner (ScriptedQuota(MysteryDecision() :> ArtifactQuotaDecision)) (TestClock())

    let sessionId = SessionId.New()

    match stage service sessionId "shot.png" (png1x1 ()) "image/png" with
    | :? ArtifactStageFailed as failed -> failed.Reason |> should equal "quotaFault"
    | outcome -> failwith $"Expected ArtifactStageFailed, observed %s{outcome.GetType().Name}."

[<Fact>]
let ``Null reservation id returns the typed failure`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore

    let service =
        defaultService inner (ScriptedQuota(ArtifactQuotaDecision.Grant(Unchecked.defaultof<string>))) (TestClock())

    let sessionId = SessionId.New()

    match stage service sessionId "shot.png" (png1x1 ()) "image/png" with
    | :? ArtifactStageFailed as failed -> failed.Reason |> should equal "quotaFault"
    | outcome -> failwith $"Expected ArtifactStageFailed, observed %s{outcome.GetType().Name}."

[<Fact>]
let ``Cancellation propagates instead of settling into an outcome`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let service = defaultService inner (CountingQuota(1_000_000L)) (TestClock())

    use cancelled = new CancellationTokenSource()
    cancelled.Cancel()

    (fun () ->
        service.StageAsync(tenant, SessionId.New(), "shot.png", BlobContent(png1x1 (), "image/png"), cancelled.Token)
        |> awaitTask
        |> ignore)
    |> should throw typeof<OperationCanceledException>

// ──────────────────────────────────────────────────────────────────────────
// Describe and clear

[<Fact>]
let ``Describe returns the URL plus metadata and reference`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let presigning = PresigningStore(inner)

    let service =
        defaultService (presigning :> IBlobStore) (CountingQuota(1_000_000L)) (TestClock())

    let sessionId = SessionId.New()
    let payload = png1x1 ()

    stage service sessionId "shot.png" payload "image/png" |> ignore

    let descriptor = describe service sessionId "shot.png" |> foundOf

    descriptor.Name |> should equal "shot.png"
    descriptor.MediaType |> should equal "image/png"
    descriptor.SizeBytes |> should equal (int64 payload.Length)
    descriptor.Reference.Contains("shot.png") |> should equal true
    (isNull (box descriptor.DownloadUrl)) |> should equal false

    match descriptor.DownloadUrl |> Option.ofObj with
    | Some url -> url.ToString().Contains("shot.png") |> should equal true
    | None -> failwith "Expected a presigned URL."

    presigning.LastExpiry.HasValue |> should equal true
    presigning.LastExpiry.Value |> should equal (ArtifactOptions().PresignedExpiry)

[<Fact>]
let ``Describe honors a configured presign expiry`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let presigning = PresigningStore(inner)

    let service =
        serviceWith
            (presigning :> IBlobStore)
            (CountingQuota(1_000_000L))
            (TestClock())
            (Action<ArtifactOptions>(fun options -> options.PresignedExpiry <- TimeSpan.FromHours 1.0))

    let sessionId = SessionId.New()
    stage service sessionId "shot.png" (png1x1 ()) "image/png" |> ignore
    describe service sessionId "shot.png" |> ignore
    presigning.LastExpiry.Value |> should equal (TimeSpan.FromHours 1.0)

[<Fact>]
let ``Describe on a backend without presign returns a null URL with metadata and reference`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let service = defaultService inner (CountingQuota(1_000_000L)) (TestClock())
    let sessionId = SessionId.New()
    let payload = png1x1 ()

    stage service sessionId "shot.png" payload "image/png" |> ignore

    let descriptor = describe service sessionId "shot.png" |> foundOf

    descriptor.DownloadUrl |> should equal null
    descriptor.MediaType |> should equal "image/png"
    descriptor.SizeBytes |> should equal (int64 payload.Length)
    descriptor.Reference.Contains("shot.png") |> should equal true

[<Fact>]
let ``Describe of a missing artifact returns the missing outcome`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let service = defaultService inner (CountingQuota(1_000_000L)) (TestClock())

    match describe service (SessionId.New()) "nope.png" with
    | :? ArtifactMissing as missing -> missing.Name |> should equal "nope.png"
    | outcome -> failwith $"Expected ArtifactMissing, observed %s{outcome.GetType().Name}."

[<Fact>]
let ``ClearSession deletes the session scope and nothing else`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let service = defaultService inner (CountingQuota(1_000_000L)) (TestClock())
    let first = SessionId.New()
    let second = SessionId.New()

    stage service first "shot.png" (png1x1 ()) "image/png" |> ignore
    stage service first "report.pdf" (pdfBytes ()) "application/pdf" |> ignore
    stage service second "shot.png" (png1x1 ()) "image/png" |> ignore

    let deleted =
        service.ClearSessionAsync(tenant, first, CancellationToken.None) |> awaitTask

    deleted |> should equal 2

    match describe service first "shot.png" with
    | :? ArtifactMissing -> ()
    | outcome -> failwith $"Expected ArtifactMissing, observed %s{outcome.GetType().Name}."

    match describe service second "shot.png" with
    | :? ArtifactFound -> ()
    | outcome -> failwith $"Expected the sibling session to keep its artifact, observed %s{outcome.GetType().Name}."

// ──────────────────────────────────────────────────────────────────────────
// Reclamation

[<Fact>]
let ``Reclamation pass releases stale reservations under virtual time`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let quota = CountingQuota(1_000_000L)
    let clock = TestClock()
    let service = defaultService inner quota clock
    let concrete = service :?> SessionArtifactService

    SessionArtifactReservations.track concrete.Outstanding "r-stale" clock
    clock.Advance(TimeSpan.FromMinutes 6.0)
    SessionArtifactReservations.track concrete.Outstanding "r-fresh" clock

    let reclaimed =
        service.ReclaimStaleReservationsAsync(CancellationToken.None) |> awaitTask

    reclaimed |> should equal 1
    quota.Released |> should equal [ "r-stale" ]
    concrete.Outstanding.ContainsKey("r-fresh") |> should equal true
    concrete.Outstanding.ContainsKey("r-stale") |> should equal false

    let again =
        service.ReclaimStaleReservationsAsync(CancellationToken.None) |> awaitTask

    again |> should equal 0

[<Fact>]
let ``Reclamation honors a configured bound`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let quota = CountingQuota(1_000_000L)
    let clock = TestClock()

    let service =
        serviceWith
            inner
            quota
            clock
            (Action<ArtifactOptions>(fun options -> options.ReservationReclaimAfter <- TimeSpan.FromHours 1.0))

    let concrete = service :?> SessionArtifactService
    SessionArtifactReservations.track concrete.Outstanding "r-early" clock
    clock.Advance(TimeSpan.FromMinutes 30.0)

    let early =
        service.ReclaimStaleReservationsAsync(CancellationToken.None) |> awaitTask

    early |> should equal 0
    clock.Advance(TimeSpan.FromMinutes 31.0)

    let late =
        service.ReclaimStaleReservationsAsync(CancellationToken.None) |> awaitTask

    late |> should equal 1
    quota.Released |> should equal [ "r-early" ]

// ──────────────────────────────────────────────────────────────────────────
// References and enrichment

[<Fact>]
let ``ArtifactReference formats and parses round-trip`` () =
    let reference = ArtifactReference.Format("shot-abc123.png", "image/png", 70L, 1, 1)
    reference.Contains("dimensions=\"1x1\"") |> should equal true

    match ArtifactReference.TryParseNames reference |> Seq.toList with
    | [ name ] -> name |> should equal "shot-abc123.png"
    | names -> failwith $"Expected one parsed name, observed %d{names.Length}."

    let withoutDims =
        ArtifactReference.Format("report.pdf", "application/pdf", 5L, 0, 0)

    withoutDims.Contains("dimensions=") |> should equal false

    match ArtifactReference.TryParseNames withoutDims |> Seq.toList with
    | [ name ] -> name |> should equal "report.pdf"
    | names -> failwith $"Expected one parsed name, observed %d{names.Length}."

    ArtifactReference.TryParseNames("[artifact rejected: mime=\"image/png\" size=\"5 bytes\" reason=\"x\"]").Count
    |> should equal 0

    ArtifactReference.TryParseNames(null).Count |> should equal 0

let cellOf (kind: SessionCellKind) (content: string | null) : SessionCell =
    {
        Id = Unchecked.defaultof<CellId>
        SessionId = SessionId.New()
        TurnId = TurnId.New()
        Kind = kind
        Content = content
        ToolName = "shot"
        ToolCallId = "call-1"
        IsError = false
        Iteration = 1
        Metadata = null
        Artifacts = null
        Timestamp = DateTimeOffset.UtcNow
    }

[<Fact>]
let ``Enrichment attaches referenced names to ToolResult cells only`` () =
    let reference = ArtifactReference.Format("a.png", "image/png", 70L, 1, 1)
    let second = ArtifactReference.Format("b.pdf", "application/pdf", 5L, 0, 0)
    let result = cellOf SessionCellKind.ToolResult $"first {reference} then {second}"
    let bare = cellOf SessionCellKind.ToolResult "plain output"
    let assistant = cellOf SessionCellKind.Assistant $"echo {reference}"

    let enriched =
        SessionArtifactEnrichment.enrichCells (
            ResizeArray<SessionCell>([| result; bare; assistant |]) :> IReadOnlyList<SessionCell>
        )

    enriched[0] |> artifactsOf |> should equal [ "a.png"; "b.pdf" ]
    enriched[1].Artifacts |> should equal null
    enriched[2].Artifacts |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Wiring

[<Fact>]
let ``Service resolves from DI with the unlimited quota by default`` () =
    let services = ServiceCollection()

    services.AddSingleton<IBlobStore>(InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore)
    |> ignore

    LegateServiceCollectionExtensions.AddLegate(services) |> ignore
    use provider = services.BuildServiceProvider()

    provider.GetRequiredService<ISessionArtifactService>() |> ignore

    match box (provider.GetRequiredService<IArtifactQuota>()) with
    | :? UnlimitedArtifactQuota -> ()
    | quota -> failwith $"Expected UnlimitedArtifactQuota, observed %s{quota.GetType().Name}."

    let bound = provider.GetRequiredService<IOptions<ArtifactOptions>>().Value
    bound.PresignedExpiry |> should equal (TimeSpan.FromDays 7.0)
    bound.ReservationReclaimAfter |> should equal (TimeSpan.FromMinutes 5.0)

[<Fact>]
let ``UseConfiguration binds Legate Artifacts`` () =
    let configuration =
        ConfigurationBuilder()
            .AddInMemoryCollection(
                [|
                    KeyValuePair<string, string>("Legate:Artifacts:PresignedExpiry", "2.00:00:00")
                    KeyValuePair<string, string>("Legate:Artifacts:ReservationReclaimAfter", "00:10:00")
                |]
            )
            .Build()

    let services = ServiceCollection()

    LegateBuilder(services).UseConfiguration(configuration.GetSection("Legate"))
    |> ignore

    use provider = services.BuildServiceProvider()
    let bound = provider.GetRequiredService<IOptions<ArtifactOptions>>().Value
    bound.PresignedExpiry |> should equal (TimeSpan.FromDays 2.0)
    bound.ReservationReclaimAfter |> should equal (TimeSpan.FromMinutes 10.0)

[<Fact>]
let ``UseConfiguration rejects an invalid Artifacts section`` () =
    let configuration =
        ConfigurationBuilder()
            .AddInMemoryCollection(
                [|
                    KeyValuePair<string, string>("Legate:Artifacts:PresignedExpiry", "00:00:00")
                |]
            )
            .Build()

    let services = ServiceCollection()

    (fun () ->
        LegateBuilder(services).UseConfiguration(configuration.GetSection("Legate"))
        |> ignore)
    |> should throw typeof<InvalidOperationException>

// ──────────────────────────────────────────────────────────────────────────
// End to end: tool image yields an event reference plus a downloadable URL

/// A minimal Idle session row for the journal seeding below.
let sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "artifacts"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

[<Fact>]
let ``Tool image stages to an event reference and a downloadable URL`` () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    let presigning = PresigningStore(inner)

    let service =
        defaultService (presigning :> IBlobStore) (CountingQuota(1_000_000L)) (TestClock())

    let payload = png1x1 ()

    // The journal session owns the staged artifact.
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    let sessions = InMemorySessionStore(database) :> ISessionStore
    let events = InMemorySessionEventStore(database) :> ISessionEventStore

    let created =
        sessions.CreateSession(tenant, sampleSession (), CancellationToken.None)
        |> awaitTask

    let sessionId = created.Id

    // The tool sink stages the returned image through the service.
    let descriptor =
        stage service sessionId "tool-shot.png" payload "image/png" |> storedOf

    // The substituted output lands in the journal like any tool result.
    sessions.AppendInboxMessage(
        tenant,
        sessionId,
        UserMessagePayload(UserMessage.Text("go")) :> InboxPayload,
        DeliveryMode.Queue,
        CancellationToken.None
    )
    |> awaitTask
    |> ignore

    let claimed =
        sessions.ClaimNextTurn(tenant, sessionId, "owner", TimeSpan.FromSeconds 120.0, CancellationToken.None)
        |> awaitTask

    let claim =
        match claimed with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | state -> failwith $"Expected a granted claim, observed %s{state.GetType().Name}."

    let stamp = DateTimeOffset.UtcNow
    let noSequence = Unchecked.defaultof<Nullable<int64>>
    let callId = "call-shot-1"
    let outputText = $"snapshot {descriptor.Reference}"

    let batch =
        ResizeArray<SessionEvent>(
            [|
                ToolCallStartedEvent(sessionId, claim.TurnId, noSequence, stamp, callId, "camera") :> SessionEvent
                ToolCallOutputEvent(sessionId, claim.TurnId, noSequence, stamp, callId, outputText) :> SessionEvent
                ToolCallCompletedEvent(sessionId, claim.TurnId, noSequence, stamp, callId, null) :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match
        events.Append(tenant, sessionId, claim.Token, batch, CancellationToken.None)
        |> awaitTask
    with
    | :? EventAppended -> ()
    | outcome -> failwith $"Expected the journal append to land, observed %s{outcome.GetType().Name}."

    // The served transcript enriches the ToolResult cell with the name.
    let cells =
        Transcripts.readTranscript events tenant sessionId (ReadTranscriptOptions()) 100 CancellationToken.None
        |> awaitTask

    let result =
        cells
        |> Seq.filter (fun cell -> cell.Kind = SessionCellKind.ToolResult)
        |> Seq.toList
        |> singleField

    (contentOf result).Contains(descriptor.Reference) |> should equal true
    result |> artifactsOf |> should equal [ "tool-shot.png" ]

    // Describe delivers the downloadable URL for the referenced bytes.
    let downloaded = describe service sessionId "tool-shot.png" |> foundOf
    (isNull (box downloaded.DownloadUrl)) |> should equal false

    let bytes =
        inner.Get(BlobKeys.ForArtifact(tenant, sessionId, "tool-shot.png"), CancellationToken.None)
        |> awaitTask
        |> requireBytes "tool-shot.png"

    bytes.Length |> should equal payload.Length
