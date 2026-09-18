// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open SixLabors.ImageSharp
open SixLabors.ImageSharp.Formats.Jpeg
open SixLabors.ImageSharp.PixelFormats
open SixLabors.ImageSharp.Processing

// Validated image and video artifact storage (issue 118). The internal
// validated-put enforces ArtifactOptions caps over an IArtifactBlobStore:
// images decode through ImageSharp with Identify-first dimension gating
// (decompression bombs never allocate) plus deterministic JPEG previews,
// videos store as-is with hand-rolled BMFF/EBML header sniffing, and
// anything undecodable or unrecognised fails closed with a bounded error.
// No quota orchestration, no sidecar files, no transcoding: the metadata
// returns to the caller for issue 117 to describe.

/// Validated image and video artifact storage over an artifact blob
/// scope. Internal so no store type ever crosses the public API; tests
/// drive <c>putValidated</c> directly against the in-memory blob store.
module internal Artifacts =

    // ──────────────────────────────────────────────────────────────────
    // Shapes

    /// Bounded validation failure for an artifact put. Internal: issue 117
    /// maps these onto its service outcome; hosts never see this type.
    type ArtifactValidationError =
        | LimitExceeded of limitKind: string * limit: int64 * observed: int64
        | UnsupportedMediaType of mediaType: string
        | Undecodable of reason: string

    /// Stored artifact metadata returned to the caller for issue 117 to
    /// describe. Internal: same-assembly consumers read these fields.
    type StoredArtifact =
        {
            Name: string
            MediaType: string
            SizeBytes: int64
            Width: int
            Height: int
            Duration: TimeSpan option
            PreviewName: string option
        }

    /// Header-sniffed video metadata. Internal.
    type private VideoMetadata =
        {
            Width: int
            Height: int
            Duration: TimeSpan option
        }

    // ──────────────────────────────────────────────────────────────────
    // Media types and bounds

    /// The content type stored with generated JPEG previews.
    [<Literal>]
    let private PreviewContentType = "image/jpeg"

    /// The suffix appended to the artifact name for its deterministic preview.
    [<Literal>]
    let private PreviewSuffix = ".preview.jpg"

    /// Normalises a content type for allow-list matching: trims,
    /// lowercases, and strips parameters. Null reads as empty.
    let private normalizeMediaType (mediaType: string) =
        if isNull (box mediaType) then
            ""
        else
            let raw = mediaType.Trim().ToLowerInvariant()
            let semi = raw.IndexOf(';')

            if semi < 0 then raw else raw.Substring(0, semi).Trim()

    /// Reports whether the normalised media type is in the allow-list.
    /// A null list reads as empty (the options Validate rejects it first).
    let private isAllowed (allowed: Collections.Generic.List<string>) (mediaType: string) =
        if isNull (box allowed) then
            false
        else
            allowed
            |> Seq.exists (fun entry ->
                not (isNull (box entry))
                && String.Equals(normalizeMediaType entry, mediaType, StringComparison.Ordinal))

    /// Checks image dimensions against the options, decoded size derived
    /// in int64 so hostile headers cannot overflow the math. Returns the
    /// first violation, or None when the dimensions fit.
    let private checkImageBounds
        (options: ArtifactOptions)
        (width: int)
        (height: int)
        : ArtifactValidationError option =
        if width > options.MaxWidth then
            Some(ArtifactValidationError.LimitExceeded("width", int64 options.MaxWidth, int64 width))
        elif height > options.MaxHeight then
            Some(ArtifactValidationError.LimitExceeded("height", int64 options.MaxHeight, int64 height))
        elif int64 width * int64 height > options.MaxPixels then
            Some(ArtifactValidationError.LimitExceeded("pixels", options.MaxPixels, int64 width * int64 height))
        else
            let decoded = int64 width * int64 height * 4L

            if decoded > options.MaxDecodedBytes then
                Some(ArtifactValidationError.LimitExceeded("decodedBytes", options.MaxDecodedBytes, decoded))
            else
                None

    // ──────────────────────────────────────────────────────────────────
    // Image previews

    /// Builds the deterministic JPEG preview for a decoded image:
    /// downscales the long edge to PreviewMaxDimension (never upscales)
    /// and encodes at a fixed quality, so the same input always yields
    /// byte-identical output.
    let private buildPreview (options: ArtifactOptions) (image: Image<Rgba32>) : byte[] =
        let longEdge = max image.Width image.Height

        if longEdge > options.PreviewMaxDimension then
            let scale = float options.PreviewMaxDimension / float longEdge
            let width = max 1 (int (float image.Width * scale))
            let height = max 1 (int (float image.Height * scale))
            image.Mutate(fun context -> context.Resize(width, height) |> ignore)

        use stream = new MemoryStream()
        let encoder = JpegEncoder(Quality = Nullable 80)
        image.SaveAsJpeg(stream, encoder)
        stream.ToArray()

    // ──────────────────────────────────────────────────────────────────
    // BMFF (mp4/mov) header sniffing

    /// Brands a top-level ftyp may carry while still counting as mp4/mov.
    /// Anything else fails closed.
    let private compatibleBrands =
        set
            [
                "isom"
                "iso2"
                "iso3"
                "iso4"
                "iso5"
                "iso6"
                "mp41"
                "mp42"
                "avc1"
                "dash"
                "mmp4"
                "M4V "
                "M4A "
                "qt  "
            ]

    let private u32BE (bytes: byte[]) (pos: int) : uint32 =
        (uint32 bytes[pos] <<< 24)
        ||| (uint32 bytes[pos + 1] <<< 16)
        ||| (uint32 bytes[pos + 2] <<< 8)
        ||| uint32 bytes[pos + 3]

    let private u64BE (bytes: byte[]) (pos: int) : uint64 =
        (uint64 (u32BE bytes pos) <<< 32) ||| uint64 (u32BE bytes (pos + 4))

    /// Lists the boxes in [start, finish): (boxType, contentStart,
    /// contentEnd). Returns None on any malformed header. Handles 64-bit
    /// largesize and zero-size (to end of parent); anything else fails
    /// closed.
    let private listBoxes (bytes: byte[]) (start: int) (finish: int) : (string * int * int) list option =
        let mutable pos = start
        let mutable boxes = []
        let mutable valid = pos <= finish

        while valid && pos + 8 <= finish do
            let size32 = u32BE bytes pos
            let boxType = Encoding.ASCII.GetString(bytes, pos + 4, 4)

            let headerLength, boxEnd =
                if size32 = 1u then
                    if pos + 16 > finish then
                        16, -1L
                    else
                        16, int64 pos + int64 (u64BE bytes (pos + 8))
                elif size32 = 0u then
                    8, int64 finish
                else
                    8, int64 pos + int64 size32

            let contentStart = int64 pos + int64 headerLength

            if boxEnd < contentStart || boxEnd > int64 finish then
                valid <- false
            else
                boxes <- (boxType, int contentStart, int boxEnd) :: boxes
                pos <- int boxEnd

        if valid then Some(List.rev boxes) else None

    /// Parses an mvhd box for the track duration. Returns None when the
    /// box is malformed or its timescale is zero; a zero duration reads
    /// as zero.
    let private parseMvhd (bytes: byte[]) (contentStart: int) (contentEnd: int) : TimeSpan option =
        let length = contentEnd - contentStart

        if length < 4 then
            None
        else
            let version = bytes[contentStart]

            if version = 0uy && length >= 20 then
                let timescale = u32BE bytes (contentStart + 12)
                let duration = u32BE bytes (contentStart + 16)

                if timescale = 0u then
                    None
                else
                    Some(TimeSpan.FromSeconds(float duration / float timescale))
            elif version = 1uy && length >= 32 then
                let timescale = u32BE bytes (contentStart + 20)
                let duration = u64BE bytes (contentStart + 24)

                if timescale = 0u then
                    None
                else
                    Some(TimeSpan.FromSeconds(float duration / float timescale))
            else
                None

    /// Parses a tkhd box for 16.16 fixed-point dimensions. Returns None
    /// unless both sides land in 1..65536.
    let private parseTkhd (bytes: byte[]) (contentStart: int) (contentEnd: int) : (int * int) option =
        let length = contentEnd - contentStart

        let fixedAt offset =
            if offset + 4 <= length then
                Some(u32BE bytes (contentStart + offset))
            else
                None

        let dims offsetWidth offsetHeight =
            match fixedAt offsetWidth, fixedAt offsetHeight with
            | Some widthFixed, Some heightFixed ->
                let width = int (widthFixed >>> 16)
                let height = int (heightFixed >>> 16)

                if width >= 1 && width <= 65536 && height >= 1 && height <= 65536 then
                    Some(width, height)
                else
                    None
            | _ -> None

        if length < 4 then
            None
        elif bytes[contentStart] = 0uy && length >= 84 then
            dims 76 80
        elif bytes[contentStart] = 1uy && length >= 96 then
            dims 88 92
        else
            None

    /// Sniffs a BMFF (mp4/mov) payload: optional ftyp brand check, then
    /// moov for mvhd duration plus the first dimensioned tkhd. Returns
    /// None when the payload is not a recognised BMFF file.
    let private tryBmff (bytes: byte[]) : VideoMetadata option =
        match listBoxes bytes 0 bytes.Length with
        | None -> None
        | Some top ->
            let brandOk =
                match top |> List.tryFind (fun (boxType, _, _) -> boxType = "ftyp") with
                | None -> true
                | Some(_, contentStart, contentEnd) ->
                    contentEnd - contentStart >= 8
                    && compatibleBrands.Contains(Encoding.ASCII.GetString(bytes, contentStart, 4))

            if not brandOk then
                None
            else
                match top |> List.tryFind (fun (boxType, _, _) -> boxType = "moov") with
                | None -> None
                | Some(_, moovStart, moovEnd) ->
                    match listBoxes bytes moovStart moovEnd with
                    | None -> None
                    | Some moov ->
                        let duration =
                            moov
                            |> List.tryPick (fun (boxType, contentStart, contentEnd) ->
                                if boxType = "mvhd" then
                                    parseMvhd bytes contentStart contentEnd
                                else
                                    None)

                        let dims =
                            moov
                            |> List.tryPick (fun (boxType, contentStart, contentEnd) ->
                                if boxType <> "trak" then
                                    None
                                else
                                    match listBoxes bytes contentStart contentEnd with
                                    | None -> None
                                    | Some trak ->
                                        trak
                                        |> List.tryPick (fun (innerType, innerStart, innerEnd) ->
                                            if innerType = "tkhd" then
                                                parseTkhd bytes innerStart innerEnd
                                            else
                                                None))

                        match dims with
                        | None -> None
                        | Some(width, height) ->
                            Some(
                                {
                                    Width = width
                                    Height = height
                                    Duration = duration
                                }
                            )

    // ──────────────────────────────────────────────────────────────────
    // EBML (webm/mkv) header sniffing

    /// Length in bytes of a variable-length integer from its first byte.
    /// None when the byte carries no marker (all zeros).
    let private vintLength (first: byte) : int option =
        let value = int first
        let mutable mask = 0x80
        let mutable length = 1
        let mutable found = false

        while not found && length <= 8 do
            if value &&& mask <> 0 then
                found <- true
            else
                mask <- mask >>> 1
                length <- length + 1

        if found then Some length else None

    /// Big-endian unsigned integer over [pos, pos + length).
    let private uintBE (bytes: byte[]) (pos: int) (length: int) : uint64 =
        let mutable value = 0UL

        for i = 0 to length - 1 do
            value <- (value <<< 8) ||| uint64 bytes[pos + i]

        value

    /// Big-endian IEEE float over 4 or 8 bytes. None for other lengths.
    let private floatBE (bytes: byte[]) (pos: int) (length: int) : float option =
        if length = 4 then
            let raw =
                [|
                    bytes[pos + 3]
                    bytes[pos + 2]
                    bytes[pos + 1]
                    bytes[pos]
                |]

            Some(float (BitConverter.ToSingle(raw, 0)))
        elif length = 8 then
            let raw =
                [|
                    bytes[pos + 7]
                    bytes[pos + 6]
                    bytes[pos + 5]
                    bytes[pos + 4]
                    bytes[pos + 3]
                    bytes[pos + 2]
                    bytes[pos + 1]
                    bytes[pos]
                |]

            Some(BitConverter.ToDouble(raw, 0))
        else
            None

    /// Reads one EBML element header at pos within [pos, parentEnd):
    /// (id, dataSize, unknownSize, contentStart, contentEnd). dataSize is
    /// valid only when unknownSize is false.
    let private readElement (bytes: byte[]) (pos: int) (parentEnd: int) : (uint64 * uint64 * bool * int * int) option =
        if pos >= parentEnd then
            None
        else
            match vintLength bytes[pos] with
            | None -> None
            | Some idLength when pos + idLength > parentEnd -> None
            | Some idLength ->
                let id = uintBE bytes pos idLength
                let sizePos = pos + idLength

                if sizePos >= parentEnd then
                    None
                else
                    match vintLength bytes[sizePos] with
                    | None -> None
                    | Some sizeLength when sizePos + sizeLength > parentEnd -> None
                    | Some sizeLength ->
                        let rawSize = uintBE bytes sizePos sizeLength
                        let dataBits = 7 * sizeLength
                        let mask = (1UL <<< dataBits) - 1UL
                        let dataSize = rawSize &&& mask
                        let unknown = dataSize = mask
                        let contentStart = sizePos + sizeLength

                        let contentEnd =
                            if unknown then
                                int64 parentEnd
                            else
                                int64 contentStart + int64 dataSize

                        if
                            int64 contentStart > int64 parentEnd
                            || contentEnd < int64 contentStart
                            || contentEnd > int64 parentEnd
                        then
                            None
                        else
                            Some(id, dataSize, unknown, contentStart, int contentEnd)

    /// Reads an unsigned integer element payload. None unless the payload
    /// is 1..8 bytes.
    let private readUIntPayload (bytes: byte[]) (contentStart: int) (contentEnd: int) : uint64 option =
        let length = contentEnd - contentStart

        if length >= 1 && length <= 8 then
            Some(uintBE bytes contentStart length)
        else
            None

    /// Parses an Info element for timecode scale (default 1,000,000) and
    /// duration seconds. Malformed children fail the whole Info.
    let private parseInfo (bytes: byte[]) (contentStart: int) (contentEnd: int) : (uint64 * float option) option =
        let mutable pos = contentStart
        let mutable scale = 1000000UL
        let mutable duration = None
        let mutable valid = true

        while valid && pos < contentEnd do
            match readElement bytes pos contentEnd with
            | None -> valid <- false
            | Some(id, _, _, childStart, childEnd) ->
                if id = 0x2AD7B1UL then
                    match readUIntPayload bytes childStart childEnd with
                    | None -> valid <- false
                    | Some value -> scale <- value
                elif id = 0x4489UL then
                    match floatBE bytes childStart (childEnd - childStart) with
                    | None -> valid <- false
                    | Some value -> duration <- Some value

                pos <- childEnd

        if valid then Some(scale, duration) else None

    /// Parses a Video element for pixel width and height. Both must be
    /// present and positive; otherwise None.
    let private parseVideoDims (bytes: byte[]) (contentStart: int) (contentEnd: int) : (int * int) option =
        let mutable pos = contentStart
        let mutable width = 0UL
        let mutable height = 0UL
        let mutable valid = true

        while valid && pos < contentEnd do
            match readElement bytes pos contentEnd with
            | None -> valid <- false
            | Some(id, _, _, childStart, childEnd) ->
                if id = 0xB0UL then
                    match readUIntPayload bytes childStart childEnd with
                    | None -> valid <- false
                    | Some value -> width <- value
                elif id = 0xBAUL then
                    match readUIntPayload bytes childStart childEnd with
                    | None -> valid <- false
                    | Some value -> height <- value

                pos <- childEnd

        if valid && width >= 1UL && width <= 65536UL && height >= 1UL && height <= 65536UL then
            Some(int width, int height)
        else
            None

    /// Parses a TrackEntry for its Video dimensions. None when no
    /// dimensioned Video child exists.
    let private parseTrackEntry (bytes: byte[]) (contentStart: int) (contentEnd: int) : (int * int) option =
        let mutable pos = contentStart
        let mutable dims = None
        let mutable valid = true

        while valid && pos < contentEnd do
            match readElement bytes pos contentEnd with
            | None -> valid <- false
            | Some(id, _, _, childStart, childEnd) ->
                if id = 0xE0UL && dims.IsNone then
                    dims <- parseVideoDims bytes childStart childEnd

                pos <- childEnd

        if valid then dims else None

    /// Parses a Tracks element for the first dimensioned video track.
    let private parseTracks (bytes: byte[]) (contentStart: int) (contentEnd: int) : (int * int) option =
        let mutable pos = contentStart
        let mutable dims = None
        let mutable valid = true

        while valid && pos < contentEnd do
            match readElement bytes pos contentEnd with
            | None -> valid <- false
            | Some(id, _, _, childStart, childEnd) ->
                if id = 0xAEUL && dims.IsNone then
                    dims <- parseTrackEntry bytes childStart childEnd

                pos <- childEnd

        if valid then dims else None

    /// Sniffs an EBML (webm/mkv) payload: the EBML header, then the
    /// Segment for Info duration plus the first video track's dimensions.
    /// Returns None when the payload is not a recognised EBML file.
    let private tryEbml (bytes: byte[]) : VideoMetadata option =
        match readElement bytes 0 bytes.Length with
        | Some(id, _, _, headerEnd, _) when id = 0x1A45DFA3UL ->
            let mutable pos = headerEnd
            let mutable segment = None

            while segment.IsNone && pos < bytes.Length do
                match readElement bytes pos bytes.Length with
                | None -> pos <- bytes.Length
                | Some(elementId, _, _, contentStart, contentEnd) ->
                    if elementId = 0x18538067UL then
                        segment <- Some(contentStart, contentEnd)

                    pos <- contentEnd

            match segment with
            | None -> None
            | Some(segmentStart, segmentEnd) ->
                let mutable pos = segmentStart
                let mutable duration = None
                let mutable dims = None
                let mutable valid = true

                while valid && pos < segmentEnd do
                    match readElement bytes pos segmentEnd with
                    | None -> valid <- false
                    | Some(elementId, _, _, contentStart, contentEnd) ->
                        if elementId = 0x1549A966UL && duration.IsNone then
                            match parseInfo bytes contentStart contentEnd with
                            | None -> valid <- false
                            | Some(scale, seconds) ->
                                if scale = 0UL then
                                    valid <- false
                                else
                                    duration <-
                                        seconds
                                        |> Option.map (fun value -> value * float scale / 1e9)
                                        |> Option.filter (fun value ->
                                            not (Double.IsNaN value) && not (Double.IsInfinity value) && value >= 0.0)
                        elif elementId = 0x1654AE6BUL && dims.IsNone then
                            dims <- parseTracks bytes contentStart contentEnd

                        pos <- contentEnd

                if not valid then
                    None
                else
                    match dims with
                    | None -> None
                    | Some(width, height) ->
                        Some(
                            {
                                Width = width
                                Height = height
                                Duration = duration |> Option.map TimeSpan.FromSeconds
                            }
                        )
        | _ -> None

    /// Sniffs a video payload with the BMFF then EBML parsers. Anything
    /// else (including parser exceptions) reads as unrecognised: fail
    /// closed.
    let private sniffVideo (bytes: byte[]) : VideoMetadata option =
        try
            match tryBmff bytes with
            | Some metadata -> Some metadata
            | None -> tryEbml bytes
        with _ ->
            None

    // ──────────────────────────────────────────────────────────────────
    // Validated put

    /// Validates and stores an image: Identify-first dimension gating,
    /// full decode with a second gate, the original put, and a
    /// deterministic JPEG preview when the encoded size exceeds the
    /// threshold.
    let private putImageAsync
        (options: ArtifactOptions)
        (store: IArtifactBlobStore)
        (name: string)
        (mediaType: string)
        (payload: byte[])
        (cancellationToken: CancellationToken)
        : Task<Result<StoredArtifact, ArtifactValidationError>> =
        task {
            try
                let info = Image.Identify(payload)

                if isNull (box info) || info.Width <= 0 || info.Height <= 0 then
                    return Error(ArtifactValidationError.Undecodable "undecodableImage")
                else
                    match checkImageBounds options info.Width info.Height with
                    | Some violation -> return Error violation
                    | None ->
                        try
                            use image = Image.Load<Rgba32>(payload)

                            match checkImageBounds options image.Width image.Height with
                            | Some violation -> return Error violation
                            | None ->
                                let! _ = store.Put(name, BlobContent(payload, mediaType), cancellationToken)

                                let stored =
                                    {
                                        Name = name
                                        MediaType = mediaType
                                        SizeBytes = int64 payload.Length
                                        Width = image.Width
                                        Height = image.Height
                                        Duration = None
                                        PreviewName = None
                                    }

                                if int64 payload.Length > options.PreviewThresholdBytes then
                                    let previewBytes = buildPreview options image
                                    let previewName = name + PreviewSuffix

                                    let! _ =
                                        store.Put(
                                            previewName,
                                            BlobContent(previewBytes, PreviewContentType),
                                            cancellationToken
                                        )

                                    return
                                        Ok
                                            { stored with
                                                PreviewName = Some previewName
                                            }
                                else
                                    return Ok stored
                        with _ ->
                            return Error(ArtifactValidationError.Undecodable "undecodableImage")
            with _ ->
                return Error(ArtifactValidationError.Undecodable "undecodableImage")
        }

    /// Validates and stores a video as-is: header sniffing for metadata,
    /// then the single put. Unrecognised containers fail closed before
    /// any write.
    let private putVideoAsync
        (store: IArtifactBlobStore)
        (name: string)
        (mediaType: string)
        (payload: byte[])
        (cancellationToken: CancellationToken)
        : Task<Result<StoredArtifact, ArtifactValidationError>> =
        task {
            match sniffVideo payload with
            | None -> return Error(ArtifactValidationError.Undecodable "unrecognizedVideoContainer")
            | Some metadata ->
                let! _ = store.Put(name, BlobContent(payload, mediaType), cancellationToken)

                return
                    Ok
                        {
                            Name = name
                            MediaType = mediaType
                            SizeBytes = int64 payload.Length
                            Width = metadata.Width
                            Height = metadata.Height
                            Duration = metadata.Duration
                            PreviewName = None
                        }
        }

    /// Validates an image or video payload against the options and stores
    /// it in the artifact blob scope: the original always, plus a
    /// deterministic <c>{name}.preview.jpg</c> JPEG for images above the
    /// preview threshold. Videos store as-is (never transcoded) with
    /// sniffed metadata. Every rejection is a bounded
    /// <see cref="T:Legate.ArtifactValidationError" />; the store reports
    /// nothing partial: a rejected put writes zero blobs. Host
    /// misconfiguration (null arguments, invalid options) throws instead.
    /// <param name="options">The artifact caps. Must be valid per Validate().</param>
    /// <param name="store">The artifact blob scope to write through.</param>
    /// <param name="name">The artifact-relative name to store under.</param>
    /// <param name="content">The payload and content type to validate.</param>
    /// <param name="cancellationToken">Abandons the put.</param>
    /// <returns>Ok with the stored metadata, or Error with the bounded rejection.</returns>
    let putValidated
        (options: ArtifactOptions)
        (store: IArtifactBlobStore)
        (name: string)
        (content: BlobContent)
        (cancellationToken: CancellationToken)
        : Task<Result<StoredArtifact, ArtifactValidationError>> =
        task {
            ArgumentNullException.ThrowIfNull(options)
            ArgumentNullException.ThrowIfNull(store)
            ArgumentNullException.ThrowIfNull(name)

            match options.Validate() with
            | null -> ()
            | violation -> raise (InvalidOperationException($"Invalid ArtifactOptions: %s{violation}"))

            let payload =
                if isNull (box content.Bytes) then
                    Array.empty<byte>
                else
                    content.Bytes

            if payload.Length = 0 then
                return Error(ArtifactValidationError.Undecodable "emptyPayload")
            else
                let mediaType = normalizeMediaType content.ContentType

                if int64 payload.Length > options.MaxEncodedBytes then
                    return
                        Error(
                            ArtifactValidationError.LimitExceeded(
                                "encodedBytes",
                                options.MaxEncodedBytes,
                                int64 payload.Length
                            )
                        )
                elif isAllowed options.AllowedImageMediaTypes mediaType then
                    return! putImageAsync options store name mediaType payload cancellationToken
                elif isAllowed options.AllowedVideoMediaTypes mediaType then
                    return! putVideoAsync store name mediaType payload cancellationToken
                else
                    return Error(ArtifactValidationError.UnsupportedMediaType mediaType)
        }
