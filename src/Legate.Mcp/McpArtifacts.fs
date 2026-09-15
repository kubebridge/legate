// SPDX-License-Identifier: Apache-2.0
module internal Legate.Mcp.McpArtifacts

open System
open System.Collections.Generic
open System.Text

// Binary artifact validation and reference text for MCP tool results.
// Images are validated header-only (dimensions, pixel count, byte size)
// with no image-decoding dependency, so the Legate build keeps its
// no-external-services rule: PNG, JPEG, GIF, and WebP dimensions come
// from bounded header parsing over the raw bytes. Every rejection is a
// short bounded text carrying mime, size, and a fixed-set reason, never
// the bytes and never an exception into the turn.

// ──────────────────────────
// Caps

/// Bounds for binary artifact payloads. Internal: the per-call path
/// checks these before storing anything.
/// <param name="MaxBytes">Maximum stored payload size in bytes.</param>
/// <param name="MaxDimension">Maximum image width or height in pixels.</param>
/// <param name="MaxPixels">Maximum image pixel count (width times height).</param>
type internal McpArtifactCaps =
    {
        /// Maximum stored payload size in bytes.
        MaxBytes: int
        /// Maximum image width or height in pixels.
        MaxDimension: int
        /// Maximum image pixel count (width times height).
        MaxPixels: int64
    }

    /// Default bounds: 5 MiB per payload, 4096 pixels per side, and just
    /// over 16 megapixels total.
    static member Default: McpArtifactCaps =
        {
            MaxBytes = 5242880
            MaxDimension = 4096
            MaxPixels = 16777216L
        }

// ──────────────────────────
// Dimensions

/// Header-parsed image dimensions in pixels. Internal.
type internal McpImageDimensions =
    {
        /// The image width in pixels, always positive.
        Width: int
        /// The image height in pixels, always positive.
        Height: int
    }

/// Reports whether the mime type is an image type.
/// <param name="mimeType">The mime type, or null.</param>
/// <returns>True for image/* types; otherwise false.</returns>
let isImageMime (mimeType: string) : bool =
    not (isNull (box mimeType))
    && mimeType.Trim().StartsWith("image/", StringComparison.OrdinalIgnoreCase)

/// Parses PNG dimensions from the signature plus the IHDR header.
/// <param name="bytes">The raw payload.</param>
/// <returns>The dimensions, or None when the header is absent or malformed.</returns>
let private tryPng (bytes: byte[]) : McpImageDimensions option =
    try
        if bytes.Length < 24 then
            None
        elif
            bytes[0] <> 0x89uy
            || bytes[1] <> 0x50uy
            || bytes[2] <> 0x4Euy
            || bytes[3] <> 0x47uy
            || bytes[4] <> 0x0Duy
            || bytes[5] <> 0x0Auy
            || bytes[6] <> 0x1Auy
            || bytes[7] <> 0x0Auy
        then
            None
        elif
            bytes[12] <> 0x49uy
            || bytes[13] <> 0x48uy
            || bytes[14] <> 0x44uy
            || bytes[15] <> 0x52uy
        then
            None
        else
            let width =
                (int bytes[16] <<< 24)
                ||| (int bytes[17] <<< 16)
                ||| (int bytes[18] <<< 8)
                ||| int bytes[19]

            let height =
                (int bytes[20] <<< 24)
                ||| (int bytes[21] <<< 16)
                ||| (int bytes[22] <<< 8)
                ||| int bytes[23]

            if width <= 0 || height <= 0 then
                None
            else
                Some { Width = width; Height = height }
    with _ ->
        None

/// Parses GIF dimensions from the screen descriptor.
/// <param name="bytes">The raw payload.</param>
/// <returns>The dimensions, or None when the header is absent or malformed.</returns>
let private tryGif (bytes: byte[]) : McpImageDimensions option =
    try
        if bytes.Length < 10 then
            None
        elif
            bytes[0] <> 0x47uy
            || bytes[1] <> 0x49uy
            || bytes[2] <> 0x46uy
            || bytes[3] <> 0x38uy
            || (bytes[4] <> 0x37uy && bytes[4] <> 0x39uy)
            || bytes[5] <> 0x61uy
        then
            None
        else
            let width = int bytes[6] ||| (int bytes[7] <<< 8)
            let height = int bytes[8] ||| (int bytes[9] <<< 8)

            if width <= 0 || height <= 0 then
                None
            else
                Some { Width = width; Height = height }
    with _ ->
        None

/// Parses JPEG dimensions by scanning markers for the first start-of-frame
/// segment. Every read is bounds-checked; unknown or truncated input
/// yields None.
/// <param name="bytes">The raw payload.</param>
/// <returns>The dimensions, or None when no valid frame header is found.</returns>
let private tryJpeg (bytes: byte[]) : McpImageDimensions option =
    try
        if bytes.Length < 4 then
            None
        elif bytes[0] <> 0xFFuy || bytes[1] <> 0xD8uy then
            None
        else
            let isStartOfFrame marker =
                (marker >= 0xC0 && marker <= 0xC3)
                || (marker >= 0xC5 && marker <= 0xC7)
                || (marker >= 0xC9 && marker <= 0xCB)
                || (marker >= 0xCD && marker <= 0xCF)

            let mutable pos = 2
            let mutable found: McpImageDimensions option = None

            while found.IsNone && pos + 1 < bytes.Length do
                if bytes[pos] <> 0xFFuy then
                    pos <- bytes.Length
                else
                    let mutable marker = int bytes[pos + 1]
                    pos <- pos + 2

                    while marker = 0xFF && pos < bytes.Length do
                        marker <- int bytes[pos]
                        pos <- pos + 1

                    if marker = 0x00 || marker = 0xD8 || (marker >= 0xD0 && marker <= 0xD9) then
                        ()
                    elif marker = 0x01 then
                        ()
                    else if pos + 1 >= bytes.Length then
                        pos <- bytes.Length
                    else
                        let length = (int bytes[pos] <<< 8) ||| int bytes[pos + 1]

                        if length < 2 || pos + length > bytes.Length then
                            pos <- bytes.Length
                        elif isStartOfFrame marker then
                            if length >= 7 then
                                let height = (int bytes[pos + 3] <<< 8) ||| int bytes[pos + 4]
                                let width = (int bytes[pos + 5] <<< 8) ||| int bytes[pos + 6]

                                if width > 0 && height > 0 then
                                    found <- Some { Width = width; Height = height }

                            pos <- bytes.Length
                        else
                            pos <- pos + length

            found
    with _ ->
        None

/// Parses WebP dimensions for the VP8, VP8L, and VP8X chunks.
/// <param name="bytes">The raw payload.</param>
/// <returns>The dimensions, or None when the header is absent or malformed.</returns>
let private tryWebP (bytes: byte[]) : McpImageDimensions option =
    try
        if bytes.Length < 12 then
            None
        elif
            bytes[0] <> 0x52uy
            || bytes[1] <> 0x49uy
            || bytes[2] <> 0x46uy
            || bytes[3] <> 0x46uy
            || bytes[8] <> 0x57uy
            || bytes[9] <> 0x45uy
            || bytes[10] <> 0x42uy
            || bytes[11] <> 0x50uy
        then
            None
        elif bytes.Length < 20 then
            None
        else
            let dataStart = 20
            let dataLength = bytes.Length - dataStart

            if
                bytes[12] = 0x56uy
                && bytes[13] = 0x50uy
                && bytes[14] = 0x38uy
                && bytes[15] = 0x58uy
            then
                if dataLength < 10 then
                    None
                else
                    let width =
                        (int bytes[dataStart + 4]
                         ||| (int bytes[dataStart + 5] <<< 8)
                         ||| (int bytes[dataStart + 6] <<< 16))
                        + 1

                    let height =
                        (int bytes[dataStart + 7]
                         ||| (int bytes[dataStart + 8] <<< 8)
                         ||| (int bytes[dataStart + 9] <<< 16))
                        + 1

                    if width <= 0 || height <= 0 then
                        None
                    else
                        Some { Width = width; Height = height }
            elif
                bytes[12] = 0x56uy
                && bytes[13] = 0x50uy
                && bytes[14] = 0x38uy
                && bytes[15] = 0x4Cuy
            then
                if dataLength < 5 || bytes[dataStart] <> 0x2Fuy then
                    None
                else
                    let word =
                        int bytes[dataStart + 1]
                        ||| (int bytes[dataStart + 2] <<< 8)
                        ||| (int bytes[dataStart + 3] <<< 16)
                        ||| (int bytes[dataStart + 4] <<< 24)

                    let width = (word &&& 0x3FFF) + 1
                    let height = ((word >>> 14) &&& 0x3FFF) + 1

                    if width <= 0 || height <= 0 then
                        None
                    else
                        Some { Width = width; Height = height }
            elif
                bytes[12] = 0x56uy
                && bytes[13] = 0x50uy
                && bytes[14] = 0x38uy
                && bytes[15] = 0x20uy
            then
                if dataLength < 10 then
                    None
                elif
                    bytes[dataStart + 3] <> 0x9Duy
                    || bytes[dataStart + 4] <> 0x01uy
                    || bytes[dataStart + 5] <> 0x2Auy
                then
                    None
                else
                    let width =
                        (int bytes[dataStart + 6] ||| (int bytes[dataStart + 7] <<< 8)) &&& 0x3FFF

                    let height =
                        (int bytes[dataStart + 8] ||| (int bytes[dataStart + 9] <<< 8)) &&& 0x3FFF

                    if width <= 0 || height <= 0 then
                        None
                    else
                        Some { Width = width; Height = height }
            else
                None
    with _ ->
        None

/// Parses image dimensions from the payload header for the supported
/// image mime types (PNG, JPEG, GIF, WebP). Anything else reads as None.
/// <param name="mimeType">The payload mime type.</param>
/// <param name="bytes">The raw payload.</param>
/// <returns>The dimensions, or None when unsupported or malformed.</returns>
let tryGetDimensions (mimeType: string) (bytes: byte[]) : McpImageDimensions option =
    try
        if isNull (box bytes) || bytes.Length = 0 then
            None
        else
            let raw =
                if isNull (box mimeType) then
                    ""
                else
                    mimeType.Trim().ToLowerInvariant()

            let semi = raw.IndexOf(';')
            let clean = if semi < 0 then raw else raw.Substring(0, semi).Trim()

            match clean with
            | "image/png" -> tryPng bytes
            | "image/jpeg"
            | "image/jpg" -> tryJpeg bytes
            | "image/gif" -> tryGif bytes
            | "image/webp" -> tryWebP bytes
            | _ -> None
    with _ ->
        None

// ──────────────────────────
// Validation

/// Checks one binary payload against the caps, header-only: byte size
/// first, then dimensions and pixel count for image types. Every failure
/// is a short fixed-set reason; null means the payload may be stored.
/// <param name="mimeType">The payload mime type.</param>
/// <param name="bytes">The decoded payload bytes, or null when undecodable.</param>
/// <param name="caps">The bounds to check against.</param>
/// <returns>Null when valid; otherwise the bounded rejection reason.</returns>
let checkCaps (mimeType: string) (bytes: byte[] | null) (caps: McpArtifactCaps) : string | null =
    match box bytes with
    | null -> "undecodable payload"
    | :? array<byte> as payload when payload.Length = 0 -> "empty payload"
    | :? array<byte> as payload when payload.Length > caps.MaxBytes -> $"exceeds {caps.MaxBytes}-byte cap"
    | :? array<byte> as payload when isImageMime mimeType ->
        match tryGetDimensions mimeType payload with
        | None -> "undecodable image payload"
        | Some dims when dims.Width > caps.MaxDimension || dims.Height > caps.MaxDimension ->
            $"exceeds {caps.MaxDimension}-pixel dimension cap"
        | Some dims when int64 dims.Width * int64 dims.Height > caps.MaxPixels ->
            $"exceeds {caps.MaxPixels}-pixel count cap"
        | Some _ -> null
    | _ -> null

// ──────────────────────────
// Reference text

/// Truncates untrusted text to a bounded length.
/// <param name="value">The text, or null.</param>
/// <param name="maxLength">The maximum length.</param>
/// <returns>The text, truncated and never null.</returns>
let private truncate (value: string) (maxLength: int) : string =
    if isNull (box value) then ""
    elif value.Length <= maxLength then value
    else value.Substring(0, maxLength)

/// Sanitizes a resource name hint into one storage-key-safe segment:
/// ASCII letters, digits, dashes, and underscores only, at most 32
/// characters. Blank hints read as <c>artifact</c>.
/// <param name="hint">The resource name hint, or null.</param>
/// <returns>The safe segment.</returns>
let sanitizeHint (hint: string | null) : string =
    match box hint with
    | null -> "artifact"
    | :? string as text when String.IsNullOrWhiteSpace text -> "artifact"
    | :? string as text ->
        let builder = StringBuilder()

        for c in text.Trim() do
            if Char.IsAsciiLetterOrDigit c || c = '-' || c = '_' then
                builder.Append(c) |> ignore
            elif c = '.' then
                builder.Append('-') |> ignore
            elif builder.Length > 0 && builder[builder.Length - 1] <> '-' then
                builder.Append('-') |> ignore

        let cleaned = builder.ToString().Trim('-')

        let cut =
            if cleaned.Length > 32 then
                cleaned.Substring(0, 32).TrimEnd('-')
            else
                cleaned

        if String.IsNullOrEmpty cut then "artifact" else cut
    | _ -> "artifact"

/// Picks the storage file extension for a mime type.
/// <param name="mimeType">The payload mime type.</param>
/// <returns>The extension, including the dot.</returns>
let extensionFor (mimeType: string) : string =
    let raw =
        if isNull (box mimeType) then
            ""
        else
            mimeType.Trim().ToLowerInvariant()

    let semi = raw.IndexOf(';')
    let clean = if semi < 0 then raw else raw.Substring(0, semi).Trim()

    match clean with
    | "image/png" -> ".png"
    | "image/jpeg"
    | "image/jpg" -> ".jpg"
    | "image/gif" -> ".gif"
    | "image/webp" -> ".webp"
    | "audio/wav"
    | "audio/x-wav" -> ".wav"
    | "audio/mpeg" -> ".mp3"
    | "audio/ogg" -> ".ogg"
    | "audio/webm" -> ".weba"
    | "audio/mp4" -> ".m4a"
    | "application/pdf" -> ".pdf"
    | _ -> ".bin"

/// Builds a unique storage name from a resource hint and mime type: a
/// sanitized hint, a fresh GUID, and a mime-derived extension. The name
/// is a valid relative blob key; the caller-supplied sink scopes it to
/// the session, so this layer never derives tenant keys.
/// <param name="hint">The resource name hint, or null.</param>
/// <param name="mimeType">The payload mime type.</param>
/// <returns>The unique storage name.</returns>
let buildStorageName (hint: string | null) (mimeType: string) : string =
    $"{sanitizeHint hint}-{Guid.NewGuid():N}{extensionFor mimeType}"

/// Formats the stored-artifact reference substituting the placeholder:
/// the storage name, mime, size, and dimensions when known.
/// <param name="name">The storage name.</param>
/// <param name="mimeType">The payload mime type.</param>
/// <param name="sizeBytes">The payload size in bytes.</param>
/// <param name="dimensions">The parsed dimensions, or None for non-images.</param>
/// <returns>The reference text.</returns>
let formatReference
    (name: string)
    (mimeType: string)
    (sizeBytes: int)
    (dimensions: McpImageDimensions option)
    : string =
    let dims =
        match dimensions with
        | Some parsed -> $" dimensions=\"{parsed.Width}x{parsed.Height}\""
        | None -> ""

    $"[artifact: name=\"{truncate name 128}\" mime=\"{truncate mimeType 128}\" size=\"{sizeBytes} bytes\"{dims}]"

/// Formats the bounded rejection replacing the placeholder: mime, size,
/// and a fixed-set reason, never the bytes.
/// <param name="mimeType">The payload mime type.</param>
/// <param name="sizeBytes">The payload size in bytes.</param>
/// <param name="reason">The fixed-set rejection reason.</param>
/// <returns>The rejection text.</returns>
let formatRejection (mimeType: string) (sizeBytes: int) (reason: string) : string =
    $"[artifact rejected: mime=\"{truncate mimeType 128}\" size=\"{sizeBytes} bytes\" reason=\"{truncate reason 128}\"]"

/// Substitutes each binary's placeholder in the rendered text with its
/// replacement, in block order: the search advances past each
/// replacement, so duplicate placeholders resolve to the matching binary
/// and non-binary placeholders stay untouched.
/// <param name="text">The rendered text.</param>
/// <param name="binaries">The binaries in block order.</param>
/// <param name="replacements">The replacement per binary.</param>
/// <returns>The substituted text.</returns>
let substitute (text: string) (binaries: IReadOnlyList<McpBinaryPart>) (replacements: IReadOnlyList<string>) : string =
    ArgumentNullException.ThrowIfNull(binaries)
    ArgumentNullException.ThrowIfNull(replacements)

    if binaries.Count = 0 then
        if isNull (box text) then "" else text
    else
        let mutable current = if isNull (box text) then "" else text
        let mutable pos = 0
        let count = min binaries.Count replacements.Count

        for i = 0 to count - 1 do
            let placeholder = binaries.[i].Placeholder

            let replacement =
                if isNull (box replacements.[i]) then
                    ""
                else
                    replacements.[i]

            if not (String.IsNullOrEmpty placeholder) then
                let found = current.IndexOf(placeholder, pos, StringComparison.Ordinal)

                if found >= 0 then
                    current <-
                        current.Substring(0, found)
                        + replacement
                        + current.Substring(found + placeholder.Length)

                    pos <- found + replacement.Length

        current
