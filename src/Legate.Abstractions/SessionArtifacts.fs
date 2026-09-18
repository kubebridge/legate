// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text.Json.Serialization
open System.Text.RegularExpressions
open System.Threading
open System.Threading.Tasks

// Session artifact contracts (issue 117). ISessionArtifactService is the
// runtime's quota-accounted staging path over an IBlobStore: reserve bytes
// against IArtifactQuota, store through issue 118's validated-put (images
// and videos) or as-is (every other media type), commit on success, and
// release plus delete partials on every failure path. Outcomes are typed
// result hierarchies with stable $type discriminators like
// AgentUpdateOutcome: quota exhaustion, validation rejections, and host
// quota faults are expected branches, never exceptions. ArtifactReference
// is the one shared helper formatting and parsing the [artifact: ...]
// reference text the MCP sink substitutes into tool output and the cell
// enrichment reads back, so the sink, the service, and enrichment cannot
// drift. Public signatures stay BCL-only (no option, list, or DU).

/// What the artifact service stored for one staged artifact: the blob
/// identity, the stored media shape, and how hosts reach it. Width and
/// Height are 0 when the descriptor came from Describe, which reads blob
/// metadata only; Stage fills them from the validated put. DownloadUrl is
/// null when the backend cannot presign (the local directory store without
/// a host base URL, for instance): metadata and Reference still travel.
/// Serialises with System.Text.Json.
/// <param name="Name">The artifact-relative name the blob is stored under.</param>
/// <param name="MediaType">The normalised media type the blob was stored with.</param>
/// <param name="SizeBytes">The stored payload size in bytes.</param>
/// <param name="Width">The image or video width in pixels, or 0 when unknown.</param>
/// <param name="Height">The image or video height in pixels, or 0 when unknown.</param>
/// <param name="Duration">The video duration, or empty when the artifact is not a video or the duration is unknown.</param>
/// <param name="PreviewName">The deterministic preview artifact name, or null when the stage wrote no preview.</param>
/// <param name="DownloadUrl">The presigned download URL, or null when the backend cannot presign.</param>
/// <param name="Reference">The [artifact: ...] reference text naming this artifact in tool output and cells.</param>
[<CLIMutable; NoComparison>]
type ArtifactDescriptor =
    {
        /// The artifact-relative name the blob is stored under.
        Name: string
        /// The normalised media type the blob was stored with.
        MediaType: string
        /// The stored payload size in bytes.
        SizeBytes: int64
        /// The image or video width in pixels, or 0 when unknown.
        Width: int
        /// The image or video height in pixels, or 0 when unknown.
        Height: int
        /// The video duration, or empty when the artifact is not a video
        /// or the duration is unknown.
        Duration: Nullable<TimeSpan>
        /// The deterministic preview artifact name, or null when the stage
        /// wrote no preview.
        PreviewName: string | null
        /// The presigned download URL, or null when the backend cannot
        /// presign.
        DownloadUrl: Uri | null
        /// The [artifact: ...] reference text naming this artifact in tool
        /// output and cells.
        Reference: string
    }

/// What <see cref="M:Legate.ISessionArtifactService.StageAsync*" />
/// decided about one stage request: the caller must branch on the outcome.
/// A result object, never an exception: quota exhaustion, validation
/// rejections, and host quota faults are expected branches. Serialises
/// polymorphically: every concrete outcome carries a stable <c>$type</c>
/// discriminator on the wire, mirroring
/// <see cref="T:Legate.AgentUpdateOutcome" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<ArtifactStored>, "artifactStored")>]
[<JsonDerivedType(typeof<ArtifactQuotaExhausted>, "quotaExhausted")>]
[<JsonDerivedType(typeof<ArtifactRejected>, "artifactRejected")>]
[<JsonDerivedType(typeof<ArtifactStageFailed>, "stageFailed")>]
type ArtifactStageOutcome() = class end

/// The stage landed: the bytes are reserved and committed, and the
/// descriptor carries the stored metadata plus the reference text.
/// <param name="descriptor">The stored artifact and how hosts reach it.</param>
and [<Sealed>] ArtifactStored(descriptor: ArtifactDescriptor) =
    inherit ArtifactStageOutcome()

    /// The stored artifact and how hosts reach it.
    member _.Descriptor = descriptor

/// The quota had fewer bytes free than requested: nothing was reserved and
/// nothing was stored. The service deletes any partial blob under the
/// target name so a retry starts clean. The sizes travel back so the caller
/// can report how much it could have stored.
/// <param name="requestedBytes">The number of bytes the stage asked to reserve.</param>
/// <param name="allowedBytes">The number of bytes the quota could still grant at decision time.</param>
and [<Sealed>] ArtifactQuotaExhausted(requestedBytes: int64, allowedBytes: int64) =
    inherit ArtifactStageOutcome()

    /// The number of bytes the stage asked to reserve.
    member _.RequestedBytes = requestedBytes

    /// The number of bytes the quota could still grant at decision time.
    member _.AllowedBytes = allowedBytes

/// The validated-put refused the payload: the reservation was released and
/// nothing was stored. The reason is a bounded fixed set ("encodedBytes",
/// "decodedBytes", "width", "height", "pixels", "unsupportedMediaType",
/// "undecodable", "emptyPayload"); hosts never parse the message.
/// <param name="reason">Why the payload was rejected, from the fixed set above.</param>
/// <param name="limit">The configured limit that was breached, or -1 when the rejection names no limit.</param>
/// <param name="observed">The observed size, dimension, or count that breached the limit, or -1 when there is none.</param>
and [<Sealed>] ArtifactRejected(reason: string, limit: int64, observed: int64) =
    inherit ArtifactStageOutcome()

    /// Why the payload was rejected: "encodedBytes", "decodedBytes",
    /// "width", "height", "pixels", "unsupportedMediaType", "undecodable",
    /// or "emptyPayload". Never contains secrets or tool arguments.
    member _.Reason = reason

    /// The configured limit that was breached, or -1 when the rejection
    /// names no limit.
    member _.Limit = limit

    /// The observed size, dimension, or count that breached the limit, or
    /// -1 when there is none.
    member _.Observed = observed

/// The stage did not land: the host quota threw, or the store failed
/// mid-stage. The reservation was released and partial blobs were deleted;
/// nothing durable is left behind. The reason is a bounded fixed
/// vocabulary ("quotaFault", "storeFault"); hosts never parse it for
/// detail and it never contains secrets or tool arguments.
/// <param name="reason">Why the stage failed, from the fixed set above.</param>
and [<Sealed>] ArtifactStageFailed(reason: string) =
    inherit ArtifactStageOutcome()

    /// Why the stage failed: "quotaFault" when the host quota threw,
    /// "storeFault" when the blob store failed mid-stage. Never contains
    /// secrets or tool arguments.
    member _.Reason = reason

/// What <see cref="M:Legate.ISessionArtifactService.DescribeAsync*" />
/// found for one artifact name: the caller must branch on the outcome.
/// A result object, never an exception: a missing artifact is an expected
/// branch. Serialises polymorphically like
/// <see cref="T:Legate.ArtifactStageOutcome" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<ArtifactFound>, "artifactFound")>]
[<JsonDerivedType(typeof<ArtifactMissing>, "artifactMissing")>]
type ArtifactDescribeOutcome() = class end

/// The artifact exists: the descriptor carries its metadata, the presigned
/// URL when the backend can presign (null otherwise), and the reference
/// text.
/// <param name="descriptor">The artifact metadata, URL, and reference.</param>
and [<Sealed>] ArtifactFound(descriptor: ArtifactDescriptor) =
    inherit ArtifactDescribeOutcome()

    /// The artifact metadata, URL, and reference.
    member _.Descriptor = descriptor

/// No artifact exists under the name in this session's artifact scope.
/// <param name="name">The artifact-relative name that was described, exactly as passed.</param>
and [<Sealed>] ArtifactMissing(name: string) =
    inherit ArtifactDescribeOutcome()

    /// The artifact-relative name that was described, exactly as passed.
    member _.Name = name

// The pattern const and its compiled matcher, shared by Format and
// TryParseNames, live in an internal module so the single regex instance is
// built once and the reference shape is defined in one place.
module internal ArtifactReferenceInternals =

    /// The longest a formatted name or media type may run before
    /// truncation: references stay bounded no matter what the tool
    /// returned.
    [<Literal>]
    let MaxFieldChars = 128

    /// The compiled reference matcher: an <c>[artifact:</c> marker whose
    /// first field is <c>name="..."</c>. Rejection texts
    /// (<c>[artifact rejected: ...]</c>) carry no name and never match.
    let ReferencePattern =
        Regex(@"\[artifact:\s+name=""([^""]{1,128})""", RegexOptions.Compiled ||| RegexOptions.CultureInvariant)

/// The single shared helper formatting and parsing the
/// <c>[artifact: ...]</c> reference text that names a stored artifact in
/// tool output and transcript cells. One helper, tested once: the MCP sink,
/// the artifact service, and the cell enrichment all format and parse
/// through it, so the reference shape cannot drift between writers and
/// readers.
type ArtifactReference() =

    /// Truncates an untrusted field to the bounded reference length.
    /// <param name="value">The field value, or null.</param>
    /// <returns>The value, truncated and never null.</returns>
    static member private Truncate(value: string) : string =
        if isNull (box value) then
            ""
        elif value.Length <= ArtifactReferenceInternals.MaxFieldChars then
            value
        else
            value.Substring(0, ArtifactReferenceInternals.MaxFieldChars)

    /// Formats the stored-artifact reference: the storage name, mime, size,
    /// and dimensions when known (both positive). Dimensions read as absent
    /// when either side is not positive, so Describe shapes without decoded
    /// metadata format the same prefix every other writer formats.
    /// <param name="name">The storage name.</param>
    /// <param name="mediaType">The payload media type.</param>
    /// <param name="sizeBytes">The payload size in bytes.</param>
    /// <param name="width">The image or video width in pixels, or 0 when unknown.</param>
    /// <param name="height">The image or video height in pixels, or 0 when unknown.</param>
    /// <returns>The reference text.</returns>
    static member Format(name: string, mediaType: string, sizeBytes: int64, width: int, height: int) : string =
        let dims =
            if width > 0 && height > 0 then
                $" dimensions=\"{width}x{height}\""
            else
                ""

        $"[artifact: name=\"{ArtifactReference.Truncate name}\" mime=\"{ArtifactReference.Truncate mediaType}\" size=\"{sizeBytes} bytes\"{dims}]"

    /// Parses every artifact name referenced in free text, in order.
    /// <param name="text">The text to scan, or null.</param>
    /// <returns>The referenced names, in order; empty when the text carries none.</returns>
    static member TryParseNames(text: string | null) : IReadOnlyList<string> =
        match box text with
        | null -> ResizeArray<string>() :> IReadOnlyList<string>
        | :? string as value ->
            let names = ResizeArray<string>()

            for found in ArtifactReferenceInternals.ReferencePattern.Matches(value) do
                names.Add(found.Groups[1].Value)

            names :> IReadOnlyList<string>
        | _ -> ResizeArray<string>() :> IReadOnlyList<string>

/// How the runtime stages session artifacts with quota accounting: hosts
/// implement <see cref="T:Legate.IArtifactQuota" /> and the runtime
/// branches on the typed outcomes. The contract pins the lifecycle:
/// <list type="bullet">
/// <item><description><b>Stage</b> reserves the payload bytes, stores the
/// artifact (validated for images and videos, as-is for every other media
/// type), commits the reservation, and returns the stored outcome with the
/// descriptor. Exhaustion deletes partials and returns the exhausted
/// outcome; validation rejections release and return the rejected outcome;
/// host quota and store faults release, delete partials, and return the
/// failed outcome. Cancellation propagates instead of settling into an
/// outcome.</description></item>
/// <item><description><b>Describe</b> reads the artifact metadata and
/// presigns a download URL under the configured expiry. A backend that
/// cannot presign still returns the found outcome with a null URL alongside
/// the metadata and reference. A missing artifact returns the missing
/// outcome, never an exception.</description></item>
/// <item><description><b>ClearSession</b> deletes every artifact in the
/// session's artifact scope and returns how many were deleted.</description></item>
/// <item><description><b>ReclaimStaleReservations</b> releases granted
/// reservations the clock has aged past the configured bound without a
/// commit or release (a crash between reserve and settle, for instance)
/// and returns how many were reclaimed. Commit and release stay idempotent
/// per the quota protocol, so reclaiming a reservation the host already
/// settled is a no-op.</description></item>
/// </list>
type ISessionArtifactService =

    /// Stages one artifact with quota accounting: reserve, store, commit.
    /// <param name="tenant">The tenant the artifact belongs to.</param>
    /// <param name="sessionId">The session the artifact belongs to.</param>
    /// <param name="name">The artifact-relative name to store under.</param>
    /// <param name="content">The payload and content type to store.</param>
    /// <param name="cancellationToken">Token that abandons the stage.</param>
    /// <returns>The stored, exhausted, rejected, or failed outcome.</returns>
    abstract StageAsync:
        tenant: TenantId *
        sessionId: SessionId *
        name: string *
        content: BlobContent *
        cancellationToken: CancellationToken ->
            Task<ArtifactStageOutcome>

    /// Describes one staged artifact: metadata plus a presigned download
    /// URL under the configured expiry, or the missing outcome.
    /// <param name="tenant">The tenant the artifact belongs to.</param>
    /// <param name="sessionId">The session the artifact belongs to.</param>
    /// <param name="name">The artifact-relative name to describe.</param>
    /// <param name="cancellationToken">Token that abandons the describe.</param>
    /// <returns>The found outcome with metadata, URL, and reference, or the missing outcome.</returns>
    abstract DescribeAsync:
        tenant: TenantId * sessionId: SessionId * name: string * cancellationToken: CancellationToken ->
            Task<ArtifactDescribeOutcome>

    /// Deletes every artifact in the session's artifact scope.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose artifacts to clear.</param>
    /// <param name="cancellationToken">Token that abandons the clear.</param>
    /// <returns>How many artifacts were deleted.</returns>
    abstract ClearSessionAsync:
        tenant: TenantId * sessionId: SessionId * cancellationToken: CancellationToken -> Task<int>

    /// Releases granted reservations the clock aged past the configured
    /// bound without a commit or release.
    /// <param name="cancellationToken">Token that abandons the pass.</param>
    /// <returns>How many reservations were reclaimed.</returns>
    abstract ReclaimStaleReservationsAsync: cancellationToken: CancellationToken -> Task<int>
