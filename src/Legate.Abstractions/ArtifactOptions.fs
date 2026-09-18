// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic

// Image and video artifact caps (issue 118). ArtifactOptions is the plain
// mutable class bound standalone from the Legate:Artifacts configuration
// section (hosts override with Legate__Artifacts__* env vars); the internal
// validated-put in Legate enforces every knob and fails closed on
// undecodable payloads. No quota orchestration lives here: that is issue
// 117's, consuming the putValidated seam and the metadata it returns.

/// Image and video artifact caps: encoded and decoded size bounds, image
/// dimension and pixel-count bounds, JPEG preview shaping, the allowed
/// media types per kind, the presigned-URL expiry, and the reservation
/// reclamation bound the artifact service (issue 117) consumes. Bound
/// standalone from the <c>Legate:Artifacts</c> configuration section;
/// mutable so hosts can set properties before registering. Defaults accept
/// 10 MiB encoded and 128 MiB decoded at up to 8192 pixels per side and
/// 16.7M pixels total, preview images above 1 MiB down to 1024 on the long
/// edge, presign for 7 days, and reclaim stale reservations after
/// 5 minutes.
[<Sealed>]
type ArtifactOptions() =

    /// The configuration section these options bind from.
    static member ConfigurationSectionPath = "Legate:Artifacts"

    /// The maximum encoded payload size in bytes an artifact may carry.
    /// Default 10 MiB.
    member val MaxEncodedBytes: int64 = 10485760L with get, set

    /// The maximum decoded image size in bytes (width times height times 4
    /// RGBA bytes, computed in int64) an image may decode to. Default
    /// 128 MiB. The validator gates this from the Identify header before
    /// full decode, so decompression bombs never allocate.
    member val MaxDecodedBytes: int64 = 134217728L with get, set

    /// The maximum image width in pixels. Default 8192.
    member val MaxWidth: int = 8192 with get, set

    /// The maximum image height in pixels. Default 8192.
    member val MaxHeight: int = 8192 with get, set

    /// The maximum image pixel count (width times height, computed in
    /// int64). Default 16777216 (16.7M).
    member val MaxPixels: int64 = 16777216L with get, set

    /// The encoded size in bytes above which images gain a JPEG preview
    /// under the deterministic <c>{name}.preview.jpg</c> name. Default
    /// 1 MiB.
    member val PreviewThresholdBytes: int64 = 1048576L with get, set

    /// The longest edge in pixels of a generated JPEG preview; smaller
    /// images keep their size (previews never upscale). Default 1024.
    member val PreviewMaxDimension: int = 1024 with get, set

    /// The image media types the validated-put accepts, matched
    /// case-insensitively after stripping parameters. Defaults to PNG,
    /// JPEG (both spellings), GIF, and WebP.
    member val AllowedImageMediaTypes: List<string> =
        List<string>(
            [|
                "image/png"
                "image/jpeg"
                "image/jpg"
                "image/gif"
                "image/webp"
            |]
        ) with get, set

    /// The video media types the validated-put accepts, matched
    /// case-insensitively after stripping parameters. Defaults to MP4,
    /// QuickTime, WebM, and Matroska.
    member val AllowedVideoMediaTypes: List<string> =
        List<string>(
            [|
                "video/mp4"
                "video/quicktime"
                "video/webm"
                "video/x-matroska"
            |]
        ) with get, set

    /// How long presigned artifact URLs stay valid. Consumed by the
    /// artifact service (issue 117), not by validation. Default 7 days.
    member val PresignedExpiry: TimeSpan = TimeSpan.FromDays 7.0 with get, set

    /// How long a granted quota reservation may sit without a commit or
    /// release before the artifact service reclaims it. Consumed by the
    /// artifact service reclamation pass (issue 117): a crash between
    /// reserve and settle leaks at most this bound plus one pass interval.
    /// Default 5 minutes.
    member val ReservationReclaimAfter: TimeSpan = TimeSpan.FromMinutes 5.0 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        if this.MaxEncodedBytes < 1L then
            "MaxEncodedBytes must be at least 1."
        elif this.MaxDecodedBytes < 1L then
            "MaxDecodedBytes must be at least 1."
        elif this.MaxWidth < 1 then
            "MaxWidth must be at least 1."
        elif this.MaxHeight < 1 then
            "MaxHeight must be at least 1."
        elif this.MaxPixels < 1L then
            "MaxPixels must be at least 1."
        elif this.PreviewThresholdBytes < 0L then
            "PreviewThresholdBytes must be at least 0."
        elif this.PreviewMaxDimension < 1 then
            "PreviewMaxDimension must be at least 1."
        elif isNull (box this.AllowedImageMediaTypes) then
            "AllowedImageMediaTypes must not be null."
        elif isNull (box this.AllowedVideoMediaTypes) then
            "AllowedVideoMediaTypes must not be null."
        elif this.PresignedExpiry <= TimeSpan.Zero then
            "PresignedExpiry must be positive."
        elif this.ReservationReclaimAfter <= TimeSpan.Zero then
            "ReservationReclaimAfter must be positive."
        else
            let mutable violation: string | null = null
            let mutable index = 0

            while isNull (box violation) && index < this.AllowedImageMediaTypes.Count do
                if String.IsNullOrWhiteSpace this.AllowedImageMediaTypes[index] then
                    violation <- $"AllowedImageMediaTypes[%d{index}] must be a non-empty media type."

                index <- index + 1

            index <- 0

            while isNull (box violation) && index < this.AllowedVideoMediaTypes.Count do
                if String.IsNullOrWhiteSpace this.AllowedVideoMediaTypes[index] then
                    violation <- $"AllowedVideoMediaTypes[%d{index}] must be a non-empty media type."

                index <- index + 1

            violation
