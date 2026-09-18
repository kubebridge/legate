// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ArtifactsTests

open System
open System.IO
open System.IO.Compression
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Xunit

// Validation suite for issue 118: ArtifactOptions defaults and every
// Validate violation, cap enforcement on generated images, fail-closed
// undecodable inputs, deterministic JPEG previews, hand-rolled BMFF/EBML
// sniffing, the decompression-bomb fixture, and putValidated round-trips
// through an artifact-scoped wrapper over InMemoryBlobStore (the
// BlobsTests.fs FakeBlobStore pattern). Image fixtures are hand-encoded
// PNGs (CRC32 + zlib wrapped by hand) so the suite stays dependency-free
// and deterministic.

// ──────────────────────────────────────────────────────────────────────────
// PNG encoding helpers

let private crcTable: uint32[] =
    Array.init 256 (fun n ->
        let mutable c = uint32 n

        for _ = 1 to 8 do
            c <-
                if c &&& 1u <> 0u then
                    0xEDB88320u ^^^ (c >>> 1)
                else
                    c >>> 1

        c)

let private crc32 (bytes: byte[]) (offset: int) (length: int) =
    let mutable crc = 0xFFFFFFFFu

    for i = offset to offset + length - 1 do
        crc <- crcTable[int ((crc ^^^ uint32 bytes[i]) &&& 0xFFu)] ^^^ (crc >>> 8)

    crc ^^^ 0xFFFFFFFFu

let private be32 (value: uint32) =
    [|
        byte (value >>> 24)
        byte ((value >>> 16) &&& 0xFFu)
        byte ((value >>> 8) &&& 0xFFu)
        byte (value &&& 0xFFu)
    |]

let private pngChunk (typ: string) (data: byte[]) =
    let typeBytes = Encoding.ASCII.GetBytes typ

    let crc =
        crc32 (Array.concat [ typeBytes; data ]) 0 (typeBytes.Length + data.Length)

    Array.concat
        [
            be32 (uint32 data.Length)
            typeBytes
            data
            be32 crc
        ]

let private adler32 (bytes: byte[]) =
    let mutable a = 1u
    let mutable b = 0u

    for value in bytes do
        a <- (a + uint32 value) % 65521u
        b <- (b + a) % 65521u

    (b <<< 16) ||| a

/// Encodes a width-by-height 8-bit RGB PNG. Solid picks one colour;
/// noisy varies every pixel so the encoded form stays large.
let private makePng (width: int) (height: int) (pixel: int -> int -> byte * byte * byte) : byte[] =
    let stride = 1 + width * 3
    let raw = Array.zeroCreate (stride * height)

    for y = 0 to height - 1 do
        raw[y * stride] <- 0uy

        for x = 0 to width - 1 do
            let r, g, b = pixel x y
            raw[y * stride + 1 + x * 3] <- r
            raw[y * stride + 1 + x * 3 + 1] <- g
            raw[y * stride + 1 + x * 3 + 2] <- b

    let stream = new MemoryStream()
    let deflater = new DeflateStream(stream, CompressionLevel.Optimal, true)
    deflater.Write(raw, 0, raw.Length)
    deflater.Close()
    let deflated = stream.ToArray()

    let zlib =
        Array.concat
            [
                [| 0x78uy; 0x9Cuy |]
                deflated
                be32 (adler32 raw)
            ]

    let ihdr =
        Array.concat
            [
                be32 (uint32 width)
                be32 (uint32 height)
                [| 8uy; 2uy; 0uy; 0uy; 0uy |]
            ]

    Array.concat
        [
            [|
                137uy
                80uy
                78uy
                71uy
                13uy
                10uy
                26uy
                10uy
            |]
            pngChunk "IHDR" ihdr
            pngChunk "IDAT" zlib
            pngChunk "IEND" [||]
        ]

let private solidPng width height =
    makePng width height (fun _ _ -> 200uy, 30uy, 90uy)

let private noisyPng width height =
    makePng width height (fun x y ->
        byte ((x * 31 + y * 17) % 251), byte ((x * 13 + y * 41) % 251), byte ((x * y) % 251))

/// Rewrites the IHDR width/height (fixing the CRC): a hostile header
/// claiming far more pixels than its IDAT carries.
let private withPatchedDims (width: int) (height: int) (png: byte[]) =
    let patched = Array.copy png
    Array.blit (be32 (uint32 width)) 0 patched 16 4
    Array.blit (be32 (uint32 height)) 0 patched 20 4
    Array.blit (be32 (crc32 patched 12 17)) 0 patched 29 4
    patched

// ──────────────────────────────────────────────────────────────────────────
// Video fixture builders

let private box (typ: string) (content: byte[]) =
    Array.concat
        [
            be32 (uint32 (8 + content.Length))
            Encoding.ASCII.GetBytes typ
            content
        ]

/// Minimal mp4: ftyp(isom) + moov(mvhd timescale 1000 duration 5000 +
/// trak(tkhd 640x480)). Five seconds, 640 by 480.
let private bmffFixture () =
    let ftyp =
        box
            "ftyp"
            (Array.concat
                [
                    Encoding.ASCII.GetBytes "isom"
                    be32 0u
                    Encoding.ASCII.GetBytes "isom"
                ])

    let mvhd =
        box
            "mvhd"
            (Array.concat
                [
                    [| 0uy; 0uy; 0uy; 0uy |]
                    be32 0u
                    be32 0u
                    be32 1000u
                    be32 5000u
                    be32 0x00010000u
                    [| 1uy; 0uy; 0uy; 0uy |]
                    Array.zeroCreate 8
                    Array.zeroCreate 36
                    Array.zeroCreate 24
                    be32 2u
                ])

    let tkhd =
        box
            "tkhd"
            (Array.concat
                [
                    [| 0uy; 0uy; 0uy; 0uy |]
                    be32 0u
                    be32 0u
                    be32 1u
                    be32 0u
                    be32 5000u
                    Array.zeroCreate 8
                    [| 0uy; 0uy |]
                    [| 0uy; 0uy |]
                    [| 0uy; 0uy |]
                    [| 0uy; 0uy |]
                    Array.zeroCreate 36
                    be32 (uint32 (640 <<< 16))
                    be32 (uint32 (480 <<< 16))
                ])

    let moov = box "moov" (Array.concat [ mvhd; box "trak" tkhd ])
    Array.concat [ ftyp; moov ]

/// Same boxes under an unrecognised ftyp brand: must fail closed.
let private bmffUnknownBrand () =
    let ftyp =
        box
            "ftyp"
            (Array.concat
                [
                    Encoding.ASCII.GetBytes "xxxx"
                    be32 0u
                    Encoding.ASCII.GetBytes "xxxx"
                ])

    let fixture = bmffFixture ()
    Array.concat [ ftyp; fixture[20..] ]

let private vint (value: int) = [| byte (0x80 ||| value) |]

let private beBytes (value: uint64) (length: int) =
    Array.init length (fun i -> byte ((value >>> (8 * (length - 1 - i))) &&& 0xFFUL))

let private ebmlElement (id: byte[]) (content: byte[]) =
    Array.concat [ id; vint content.Length; content ]

/// Minimal WebM: EBML header + Segment(unknown size) carrying Info
/// (timecode scale 1,000,000, duration 2.5) and one video track
/// (320x240). Two and a half seconds, 320 by 240.
let private webmFixture () =
    let header =
        ebmlElement
            [| 0x1Auy; 0x45uy; 0xDFuy; 0xA3uy |]
            (ebmlElement [| 0x42uy; 0x82uy |] (Encoding.ASCII.GetBytes "webm"))

    let scale = ebmlElement [| 0x2Auy; 0xD7uy; 0xB1uy |] (beBytes 1000000UL 3)

    // Duration counts timecodes (1 ms each at this scale): 2500 timecodes
    // read as two and a half seconds.
    let durationBits = uint64 (BitConverter.DoubleToInt64Bits 2500.0)
    let duration = ebmlElement [| 0x44uy; 0x89uy |] (beBytes durationBits 8)

    let info =
        ebmlElement [| 0x15uy; 0x49uy; 0xA9uy; 0x66uy |] (Array.concat [ scale; duration ])

    let video =
        ebmlElement
            [| 0xE0uy |]
            (Array.concat
                [
                    ebmlElement [| 0xB0uy |] (beBytes 320UL 2)
                    ebmlElement [| 0xBAuy |] (beBytes 240UL 1)
                ])

    let trackEntry =
        ebmlElement
            [| 0xAEuy |]
            (Array.concat
                [
                    ebmlElement [| 0xD7uy |] (beBytes 1UL 1)
                    ebmlElement [| 0x83uy |] (beBytes 1UL 1)
                    video
                ])

    let tracks = ebmlElement [| 0x16uy; 0x54uy; 0xAEuy; 0x6Buy |] trackEntry

    let segment =
        Array.concat
            [
                [| 0x18uy; 0x53uy; 0x80uy; 0x67uy |]
                [| 0xFFuy |]
                info
                tracks
            ]

    Array.concat [ header; segment ]

// ──────────────────────────────────────────────────────────────────────────
// Store scope and put helper

let private tenant = TenantId.Create "acme"

/// Artifact-scoped wrapper over the in-memory blob store, deriving every
/// key through BlobKeys like the BlobsTests FakeBlobStore does.
type private ScopedArtifacts(inner: IBlobStore, sessionId: SessionId) =
    interface IArtifactBlobStore with
        member _.Get(name, ct) =
            inner.Get(BlobKeys.ForArtifact(tenant, sessionId, name), ct)

        member _.Put(name, content, ct) =
            inner.Put(BlobKeys.ForArtifact(tenant, sessionId, name), content, ct)

        member _.CompareExchange(_, _, _, _) : Task<BlobMetadata | null> =
            raise (NotSupportedException("The test scope implements Get, Put, metadata, and listing only."))

        member _.OpenRead(_, _) : Task<Stream> =
            raise (NotSupportedException("The test scope implements Get, Put, metadata, and listing only."))

        member _.OpenWrite(_, _, _) : Task<Stream> =
            raise (NotSupportedException("The test scope implements Get, Put, metadata, and listing only."))

        member _.List(prefix, ct) =
            inner.List(BlobKeys.ArtifactPrefix(tenant, sessionId) + prefix, ct)

        member _.DeletePrefix(prefix, ct) =
            inner.DeletePrefix(BlobKeys.ArtifactPrefix(tenant, sessionId) + prefix, ct)

        member _.GetMetadata(name, ct) =
            inner.GetMetadata(BlobKeys.ForArtifact(tenant, sessionId, name), ct)

        member _.TryGetPresignedUrl(_, _, _) : Task<Uri | null> = Task.FromResult null

let private scopedStore () =
    let inner = InMemoryBlobStore(InMemoryDatabase()) :> IBlobStore
    ScopedArtifacts(inner, SessionId.New()) :> IArtifactBlobStore

let private put
    (options: ArtifactOptions)
    (store: IArtifactBlobStore)
    (name: string)
    (bytes: byte[])
    (mediaType: string)
    =
    Artifacts.putValidated options store name (BlobContent(bytes, mediaType)) CancellationToken.None
    |> fun pending -> pending.GetAwaiter().GetResult()

let private get (store: IArtifactBlobStore) (name: string) : byte[] | null =
    store.Get(name, CancellationToken.None).GetAwaiter().GetResult()

/// Unwraps possibly-missing bytes, failing the test with the label when
/// the blob is absent.
let private requireBytes (label: string) (bytes: byte[] | null) : byte[] =
    match bytes |> Option.ofObj with
    | Some found -> found
    | None -> failwith $"Expected bytes for %s{label}."

let private tightOptions () =
    let options = ArtifactOptions()
    options.MaxEncodedBytes <- 65536L
    options.MaxDecodedBytes <- 1000000L
    options.MaxWidth <- 256
    options.MaxHeight <- 256
    options.MaxPixels <- 1000000L
    options.PreviewThresholdBytes <- 1048576L
    options.PreviewMaxDimension <- 32
    options

// ──────────────────────────────────────────────────────────────────────────
// ArtifactOptions

[<Fact>]
let ``ArtifactOptions carry the documented defaults`` () =
    let options = ArtifactOptions()
    options.MaxEncodedBytes |> should equal 10485760L
    options.MaxDecodedBytes |> should equal 134217728L
    options.MaxWidth |> should equal 8192
    options.MaxHeight |> should equal 8192
    options.MaxPixels |> should equal 16777216L
    options.PreviewThresholdBytes |> should equal 1048576L
    options.PreviewMaxDimension |> should equal 1024

    options.AllowedImageMediaTypes
    |> Seq.toList
    |> should
        equal
        [
            "image/png"
            "image/jpeg"
            "image/jpg"
            "image/gif"
            "image/webp"
        ]

    options.AllowedVideoMediaTypes
    |> Seq.toList
    |> should
        equal
        [
            "video/mp4"
            "video/quicktime"
            "video/webm"
            "video/x-matroska"
        ]

    options.PresignedExpiry |> should equal (TimeSpan.FromDays 7.0)
    options.Validate() |> should equal null
    ArtifactOptions.ConfigurationSectionPath |> should equal "Legate:Artifacts"

[<Fact>]
let ``ArtifactOptions Validate rejects a non-positive encoded cap`` () =
    let options = ArtifactOptions()
    options.MaxEncodedBytes <- 0L
    options.Validate() |> should equal "MaxEncodedBytes must be at least 1."

[<Fact>]
let ``ArtifactOptions Validate rejects a non-positive decoded cap`` () =
    let options = ArtifactOptions()
    options.MaxDecodedBytes <- 0L
    options.Validate() |> should equal "MaxDecodedBytes must be at least 1."

[<Fact>]
let ``ArtifactOptions Validate rejects a non-positive width cap`` () =
    let options = ArtifactOptions()
    options.MaxWidth <- 0
    options.Validate() |> should equal "MaxWidth must be at least 1."

[<Fact>]
let ``ArtifactOptions Validate rejects a non-positive height cap`` () =
    let options = ArtifactOptions()
    options.MaxHeight <- 0
    options.Validate() |> should equal "MaxHeight must be at least 1."

[<Fact>]
let ``ArtifactOptions Validate rejects a non-positive pixel cap`` () =
    let options = ArtifactOptions()
    options.MaxPixels <- 0L
    options.Validate() |> should equal "MaxPixels must be at least 1."

[<Fact>]
let ``ArtifactOptions Validate rejects a negative preview threshold`` () =
    let options = ArtifactOptions()
    options.PreviewThresholdBytes <- -1L
    options.Validate() |> should equal "PreviewThresholdBytes must be at least 0."

[<Fact>]
let ``ArtifactOptions Validate rejects a non-positive preview dimension`` () =
    let options = ArtifactOptions()
    options.PreviewMaxDimension <- 0
    options.Validate() |> should equal "PreviewMaxDimension must be at least 1."

[<Fact>]
let ``ArtifactOptions Validate rejects null allow-lists`` () =
    let images = ArtifactOptions()
    images.AllowedImageMediaTypes <- Unchecked.defaultof<_>
    images.Validate() |> should equal "AllowedImageMediaTypes must not be null."

    let videos = ArtifactOptions()
    videos.AllowedVideoMediaTypes <- Unchecked.defaultof<_>
    videos.Validate() |> should equal "AllowedVideoMediaTypes must not be null."

[<Fact>]
let ``ArtifactOptions Validate rejects blank allow-list entries`` () =
    let options = ArtifactOptions()
    options.AllowedImageMediaTypes <- System.Collections.Generic.List<string>([| "image/png"; " " |])

    options.Validate()
    |> should equal "AllowedImageMediaTypes[1] must be a non-empty media type."

    let videos = ArtifactOptions()
    videos.AllowedVideoMediaTypes <- System.Collections.Generic.List<string>([| "" |])

    videos.Validate()
    |> should equal "AllowedVideoMediaTypes[0] must be a non-empty media type."

[<Fact>]
let ``ArtifactOptions Validate rejects a non-positive presigned expiry`` () =
    let options = ArtifactOptions()
    options.PresignedExpiry <- TimeSpan.Zero
    options.Validate() |> should equal "PresignedExpiry must be positive."

// ──────────────────────────────────────────────────────────────────────────
// Bounded typed errors

[<Fact>]
let ``Artifact exceptions derive from LegateException and carry structured context`` () =
    typeof<ArtifactLimitExceededException>.IsSubclassOf(typeof<LegateException>)
    |> should equal true

    typeof<ArtifactDecodeException>.IsSubclassOf(typeof<LegateException>)
    |> should equal true

    let limit = ArtifactLimitExceededException("pixels", 100L, 200L, "Too many pixels.")
    limit.LimitKind |> should equal "pixels"
    limit.Limit |> should equal 100L
    limit.Observed |> should equal 200L
    limit.Message |> should equal "Too many pixels."

    let decode =
        ArtifactDecodeException("undecodableImage", "The image could not be decoded.")

    decode.Reason |> should equal "undecodableImage"
    decode.Message |> should equal "The image could not be decoded."

    (fun () ->
        try
            raise (ArtifactDecodeException("emptyPayload", "The payload is empty."))
        with :? LegateException ->
            ())
    |> should not' (throw typeof<LegateException>)

// ──────────────────────────────────────────────────────────────────────────
// Image caps on generated images

[<Fact>]
let ``Encoded size violations reject before any write`` () =
    let options = tightOptions ()
    options.MaxEncodedBytes <- 64L
    let store = scopedStore ()
    let bytes = solidPng 64 64

    match put options store "big.png" bytes "image/png" with
    | Ok _ -> failwith "Expected an encoded-size rejection."
    | Error(Artifacts.LimitExceeded(kind, limit, observed)) ->
        kind |> should equal "encodedBytes"
        limit |> should equal 64L
        observed |> should equal (int64 bytes.Length)
    | Error other -> failwith $"Expected LimitExceeded, got %A{other}"

    get store "big.png" |> should equal null

[<Fact>]
let ``Width violations reject with the width kind`` () =
    let options = tightOptions ()
    options.MaxWidth <- 32
    let store = scopedStore ()

    match put options store "wide.png" (solidPng 64 32) "image/png" with
    | Ok _ -> failwith "Expected a width rejection."
    | Error(Artifacts.LimitExceeded(kind, limit, observed)) ->
        kind |> should equal "width"
        limit |> should equal 32L
        observed |> should equal 64L
    | Error other -> failwith $"Expected LimitExceeded, got %A{other}"

    get store "wide.png" |> should equal null

[<Fact>]
let ``Height violations reject with the height kind`` () =
    let options = tightOptions ()
    options.MaxHeight <- 32
    let store = scopedStore ()

    match put options store "tall.png" (solidPng 32 64) "image/png" with
    | Ok _ -> failwith "Expected a height rejection."
    | Error(Artifacts.LimitExceeded(kind, limit, observed)) ->
        kind |> should equal "height"
        limit |> should equal 32L
        observed |> should equal 64L
    | Error other -> failwith $"Expected LimitExceeded, got %A{other}"

    get store "tall.png" |> should equal null

[<Fact>]
let ``Pixel count violations reject with the pixels kind`` () =
    let options = tightOptions ()
    options.MaxWidth <- 512
    options.MaxHeight <- 512
    options.MaxPixels <- 1000L
    options.MaxDecodedBytes <- 100000000L
    let store = scopedStore ()

    match put options store "many.png" (solidPng 64 64) "image/png" with
    | Ok _ -> failwith "Expected a pixel-count rejection."
    | Error(Artifacts.LimitExceeded(kind, limit, observed)) ->
        kind |> should equal "pixels"
        limit |> should equal 1000L
        observed |> should equal 4096L
    | Error other -> failwith $"Expected LimitExceeded, got %A{other}"

    get store "many.png" |> should equal null

[<Fact>]
let ``Decoded size violations reject with the decodedBytes kind`` () =
    let options = tightOptions ()
    options.MaxWidth <- 512
    options.MaxHeight <- 512
    options.MaxPixels <- 100000000L
    options.MaxDecodedBytes <- 1000L
    let store = scopedStore ()

    match put options store "deep.png" (solidPng 64 64) "image/png" with
    | Ok _ -> failwith "Expected a decoded-size rejection."
    | Error(Artifacts.LimitExceeded(kind, limit, observed)) ->
        kind |> should equal "decodedBytes"
        limit |> should equal 1000L
        observed |> should equal 16384L
    | Error other -> failwith $"Expected LimitExceeded, got %A{other}"

    get store "deep.png" |> should equal null

[<Fact>]
let ``Random bytes fail closed with a bounded undecodable error`` () =
    let store = scopedStore ()
    let bytes = Array.init 64 (fun i -> byte ((i * 7 + 3) % 251))

    match put (tightOptions ()) store "random.png" bytes "image/png" with
    | Ok _ -> failwith "Expected an undecodable rejection."
    | Error(Artifacts.Undecodable reason) -> reason |> should equal "undecodableImage"
    | Error other -> failwith $"Expected Undecodable, got %A{other}"

    get store "random.png" |> should equal null

[<Fact>]
let ``Truncated PNGs fail closed with a bounded undecodable error`` () =
    let store = scopedStore ()
    let bytes = solidPng 32 32
    let truncated = bytes[0 .. bytes.Length / 2]

    match put (tightOptions ()) store "cut.png" truncated "image/png" with
    | Ok _ -> failwith "Expected an undecodable rejection."
    | Error(Artifacts.Undecodable reason) -> reason |> should equal "undecodableImage"
    | Error other -> failwith $"Expected Undecodable, got %A{other}"

    get store "cut.png" |> should equal null

[<Fact>]
let ``Unsupported media types fail closed before any write`` () =
    let store = scopedStore ()

    match put (tightOptions ()) store "photo.tiff" (solidPng 16 16) "image/tiff" with
    | Ok _ -> failwith "Expected an unsupported-type rejection."
    | Error(Artifacts.UnsupportedMediaType mediaType) -> mediaType |> should equal "image/tiff"
    | Error other -> failwith $"Expected UnsupportedMediaType, got %A{other}"

    get store "photo.tiff" |> should equal null

[<Fact>]
let ``Media types match case-insensitively with parameters stripped`` () =
    let store = scopedStore ()

    match put (tightOptions ()) store "upper.png" (solidPng 16 16) "IMAGE/PNG; charset=binary" with
    | Ok stored -> stored.MediaType |> should equal "image/png"
    | Error other -> failwith $"Expected success, got %A{other}"

[<Fact>]
let ``Decompression bomb headers are rejected from Identify without allocating`` () =
    let options = ArtifactOptions()
    options.MaxWidth <- 40000
    options.MaxHeight <- 40000
    options.MaxPixels <- 10000000000L
    options.MaxDecodedBytes <- 1048576L
    let store = scopedStore ()
    let bomb = solidPng 16 16 |> withPatchedDims 30000 30000

    match put options store "bomb.png" bomb "image/png" with
    | Ok _ -> failwith "Expected a decoded-size rejection."
    | Error(Artifacts.LimitExceeded(kind, limit, observed)) ->
        kind |> should equal "decodedBytes"
        limit |> should equal 1048576L
        observed |> should equal 3600000000L
    | Error other -> failwith $"Expected LimitExceeded, got %A{other}"

    get store "bomb.png" |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Previews

[<Fact>]
let ``Images above the threshold gain a deterministic repeat-run identical preview`` () =
    let options = ArtifactOptions()
    options.PreviewThresholdBytes <- 256L
    options.PreviewMaxDimension <- 16
    let bytes = noisyPng 48 48

    if int64 bytes.Length <= 256L then
        failwith $"The noisy fixture must exceed the threshold, got %d{bytes.Length} bytes."

    let firstStore = scopedStore ()

    let first =
        match put options firstStore "photo.png" bytes "image/png" with
        | Ok stored -> stored
        | Error other -> failwith $"Expected success, got %A{other}"

    first.Name |> should equal "photo.png"
    first.MediaType |> should equal "image/png"
    first.SizeBytes |> should equal (int64 bytes.Length)
    first.Width |> should equal 48
    first.Height |> should equal 48
    first.Duration |> should equal None
    first.PreviewName |> should equal (Some "photo.png.preview.jpg")

    let previewName =
        match first.PreviewName with
        | Some name -> name
        | None -> failwith "Expected a preview name."

    let firstPreview = get firstStore previewName |> requireBytes previewName

    firstPreview[0] |> should equal 0xFFuy
    firstPreview[1] |> should equal 0xD8uy
    get firstStore "photo.png" |> should equal bytes

    let secondStore = scopedStore ()

    let second =
        match put options secondStore "photo.png" bytes "image/png" with
        | Ok stored -> stored
        | Error other -> failwith $"Expected success, got %A{other}"

    second.PreviewName |> should equal first.PreviewName

    let secondPreview = get secondStore previewName |> requireBytes previewName
    secondPreview |> should equal firstPreview

[<Fact>]
let ``Images below the threshold store without a preview`` () =
    let options = ArtifactOptions()
    options.PreviewThresholdBytes <- 1048576L
    let store = scopedStore ()
    let bytes = solidPng 16 16

    match put options store "tiny.png" bytes "image/png" with
    | Ok stored ->
        stored.PreviewName |> should equal None
        stored.Width |> should equal 16
        stored.Height |> should equal 16
    | Error other -> failwith $"Expected success, got %A{other}"

    get store "tiny.png" |> should equal bytes
    get store "tiny.png.preview.jpg" |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Videos stored as-is with sniffed metadata

[<Fact>]
let ``Minimal BMFF fixtures sniff width height and duration`` () =
    let store = scopedStore ()
    let bytes = bmffFixture ()

    match put (tightOptions ()) store "clip.mp4" bytes "video/mp4" with
    | Ok stored ->
        stored.Width |> should equal 640
        stored.Height |> should equal 480
        stored.Duration |> should equal (Some(TimeSpan.FromSeconds 5.0))
        stored.MediaType |> should equal "video/mp4"
        stored.SizeBytes |> should equal (int64 bytes.Length)
        stored.PreviewName |> should equal None
    | Error other -> failwith $"Expected success, got %A{other}"

    get store "clip.mp4" |> should equal bytes

[<Fact>]
let ``Minimal WebM fixtures sniff width height and duration`` () =
    let store = scopedStore ()
    let bytes = webmFixture ()

    match put (tightOptions ()) store "clip.webm" bytes "video/webm" with
    | Ok stored ->
        stored.Width |> should equal 320
        stored.Height |> should equal 240
        stored.Duration |> should equal (Some(TimeSpan.FromSeconds 2.5))
        stored.MediaType |> should equal "video/webm"
        stored.PreviewName |> should equal None
    | Error other -> failwith $"Expected success, got %A{other}"

    get store "clip.webm" |> should equal bytes

[<Fact>]
let ``Unknown video magic is rejected before any write`` () =
    let store = scopedStore ()
    let bytes = Encoding.ASCII.GetBytes "NOTAVIDEO-NOTAVIDEO-NOTAVIDEO!"

    match put (tightOptions ()) store "clip.mp4" bytes "video/mp4" with
    | Ok _ -> failwith "Expected an unrecognised-container rejection."
    | Error(Artifacts.Undecodable reason) -> reason |> should equal "unrecognizedVideoContainer"
    | Error other -> failwith $"Expected Undecodable, got %A{other}"

    get store "clip.mp4" |> should equal null

[<Fact>]
let ``BMFF with an unknown brand is rejected`` () =
    let store = scopedStore ()

    match put (tightOptions ()) store "clip.mp4" (bmffUnknownBrand ()) "video/mp4" with
    | Ok _ -> failwith "Expected an unrecognised-container rejection."
    | Error(Artifacts.Undecodable reason) -> reason |> should equal "unrecognizedVideoContainer"
    | Error other -> failwith $"Expected Undecodable, got %A{other}"

    get store "clip.mp4" |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Round-trips

[<Fact>]
let ``putValidated round-trips the original bytes through the artifact scope`` () =
    let store = scopedStore ()
    let bytes = solidPng 24 24

    let stored =
        match put (tightOptions ()) store "turn-01/result.png" bytes "image/png" with
        | Ok stored -> stored
        | Error other -> failwith $"Expected success, got %A{other}"

    stored.Name |> should equal "turn-01/result.png"

    let metadata =
        match
            store.GetMetadata("turn-01/result.png", CancellationToken.None).GetAwaiter().GetResult()
            |> Option.ofObj
        with
        | Some found -> found
        | None -> failwith "Expected metadata for turn-01/result.png."

    metadata.SizeBytes |> should equal (int64 bytes.Length)
    metadata.ContentType |> should equal "image/png"
