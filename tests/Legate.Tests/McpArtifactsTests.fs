// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpArtifactsTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Legate.Mcp
open Legate.Mcp.McpArtifacts
open Xunit

// Scripted byte payloads only: no live servers, no image libraries. The
// payloads below are minimal hand-built headers exercising the
// header-only parsers.

// ──────────────────────────
// Scripted payloads

/// A minimal 1x1 PNG: signature plus IHDR carrying width and height 1.
let private png1x1 () : byte[] =
    [|
        0x89uy
        0x50uy
        0x4Euy
        0x47uy
        0x0Duy
        0x0Auy
        0x1Auy
        0x0Auy
        0x00uy
        0x00uy
        0x00uy
        0x0Duy
        0x49uy
        0x48uy
        0x44uy
        0x52uy
        0x00uy
        0x00uy
        0x00uy
        0x01uy
        0x00uy
        0x00uy
        0x00uy
        0x01uy
        0x08uy
        0x02uy
        0x00uy
        0x00uy
        0x00uy
        0x90uy
        0x77uy
        0x53uy
        0xDEuy
    |]

/// A minimal 2x2 PNG: the 1x1 header with width and height 2.
let private png2x2 () : byte[] =
    let bytes = png1x1 ()
    bytes[19] <- 0x02uy
    bytes[23] <- 0x02uy
    bytes

/// A minimal 1x1 JPEG: SOI, one SOF0 segment, EOI.
let private jpeg1x1 () : byte[] =
    [|
        0xFFuy
        0xD8uy
        0xFFuy
        0xC0uy
        0x00uy
        0x0Buy
        0x08uy
        0x00uy
        0x01uy
        0x00uy
        0x01uy
        0x01uy
        0x01uy
        0x11uy
        0x00uy
        0xFFuy
        0xD9uy
    |]

/// A minimal 1x1 GIF89a screen descriptor.
let private gif1x1 () : byte[] =
    [|
        0x47uy
        0x49uy
        0x46uy
        0x38uy
        0x39uy
        0x61uy
        0x01uy
        0x00uy
        0x01uy
        0x00uy
    |]

/// A minimal 2x2 WebP: RIFF, WEBP, one VP8X chunk.
let private webpVpx2x2 () : byte[] =
    [|
        0x52uy
        0x49uy
        0x46uy
        0x46uy
        0x16uy
        0x00uy
        0x00uy
        0x00uy
        0x57uy
        0x45uy
        0x42uy
        0x50uy
        0x56uy
        0x50uy
        0x38uy
        0x58uy
        0x0Auy
        0x00uy
        0x00uy
        0x00uy
        0x00uy
        0x00uy
        0x00uy
        0x00uy
        0x01uy
        0x00uy
        0x00uy
        0x01uy
        0x00uy
        0x00uy
    |]

/// A minimal 1x1 WebP: RIFF, WEBP, one VP8L chunk.
let private webpVp8l1x1 () : byte[] =
    [|
        0x52uy
        0x49uy
        0x46uy
        0x46uy
        0x11uy
        0x00uy
        0x00uy
        0x00uy
        0x57uy
        0x45uy
        0x42uy
        0x50uy
        0x56uy
        0x50uy
        0x38uy
        0x4Cuy
        0x05uy
        0x00uy
        0x00uy
        0x00uy
        0x2Fuy
        0x00uy
        0x00uy
        0x00uy
        0x00uy
    |]

/// Bytes with no image header at all.
let private randomBytes () : byte[] = [| 0x00uy; 0x01uy; 0x02uy; 0x03uy |]

/// Tight caps failing a 33-byte payload on size first.
let private tinyByteCaps () : McpArtifactCaps =
    {
        MaxBytes = 10
        MaxDimension = 4096
        MaxPixels = 16777216L
    }

/// One binary part over the given placeholder.
let private part (mime: string) (bytes: byte[]) (placeholder: string) : McpBinaryPart =
    {
        MimeType = mime
        Bytes = bytes
        Name = null
        Placeholder = placeholder
    }

// ──────────────────────────
// Header parsing

[<Fact>]
let ``PNG header parses dimensions`` () =
    McpArtifacts.tryGetDimensions "image/png" (png1x1 ())
    |> should equal (Some { Width = 1; Height = 1 })

[<Fact>]
let ``JPEG header parses dimensions`` () =
    McpArtifacts.tryGetDimensions "image/jpeg" (jpeg1x1 ())
    |> should equal (Some { Width = 1; Height = 1 })

[<Fact>]
let ``GIF header parses dimensions`` () =
    McpArtifacts.tryGetDimensions "image/gif" (gif1x1 ())
    |> should equal (Some { Width = 1; Height = 1 })

[<Fact>]
let ``WebP VP8X header parses dimensions`` () =
    McpArtifacts.tryGetDimensions "image/webp" (webpVpx2x2 ())
    |> should equal (Some { Width = 2; Height = 2 })

[<Fact>]
let ``WebP VP8L header parses dimensions`` () =
    McpArtifacts.tryGetDimensions "image/webp" (webpVp8l1x1 ())
    |> should equal (Some { Width = 1; Height = 1 })

[<Fact>]
let ``Unknown bytes parse to no dimensions`` () =
    McpArtifacts.tryGetDimensions "image/png" (randomBytes ()) |> should equal None

// ──────────────────────────
// Caps

[<Fact>]
let ``Oversize payload yields the bounded byte-cap reason`` () =
    McpArtifacts.checkCaps "image/png" (png1x1 ()) (tinyByteCaps ())
    |> should equal "exceeds 10-byte cap"

[<Fact>]
let ``Over-dimension image yields the bounded dimension reason`` () =
    let caps =
        {
            MaxBytes = 1000
            MaxDimension = 1
            MaxPixels = 100L
        }

    McpArtifacts.checkCaps "image/png" (png2x2 ()) caps
    |> should equal "exceeds 1-pixel dimension cap"

[<Fact>]
let ``Over-pixel-count image yields the bounded pixel reason`` () =
    let caps =
        {
            MaxBytes = 1000
            MaxDimension = 4096
            MaxPixels = 3L
        }

    McpArtifacts.checkCaps "image/png" (png2x2 ()) caps
    |> should equal "exceeds 3-pixel count cap"

[<Fact>]
let ``Undecodable image yields a bounded reason`` () =
    McpArtifacts.checkCaps "image/png" (randomBytes ()) McpArtifactCaps.Default
    |> should equal "undecodable image payload"

[<Fact>]
let ``Null payload is undecodable and empty payload is empty`` () =
    McpArtifacts.checkCaps "image/png" null McpArtifactCaps.Default
    |> should equal "undecodable payload"

    McpArtifacts.checkCaps "image/png" [||] McpArtifactCaps.Default
    |> should equal "empty payload"

[<Fact>]
let ``Non-image binary within caps is valid`` () =
    McpArtifacts.checkCaps "audio/mpeg" [| 0x01uy; 0x02uy; 0x03uy |] McpArtifactCaps.Default
    |> should equal null

// ──────────────────────────
// Reference text

[<Fact>]
let ``Reference carries mime size dimensions and name`` () =
    let reference =
        McpArtifacts.formatReference "shot-abc123.png" "image/png" 33 (Some { Width = 1; Height = 1 })

    reference.Contains("name=\"shot-abc123.png\"") |> should equal true
    reference.Contains("mime=\"image/png\"") |> should equal true
    reference.Contains("33 bytes") |> should equal true
    reference.Contains("dimensions=\"1x1\"") |> should equal true

[<Fact>]
let ``Rejection is bounded and carries mime size and reason`` () =
    let rejection =
        McpArtifacts.formatRejection "image/png" 6000000 "exceeds 5242880-byte cap"

    rejection.Contains("mime=\"image/png\"") |> should equal true
    rejection.Contains("6000000 bytes") |> should equal true
    rejection.Contains("exceeds 5242880-byte cap") |> should equal true
    (rejection.Length < 300) |> should equal true

[<Fact>]
let ``Storage names are unique valid keys with mime extensions`` () =
    let first = McpArtifacts.buildStorageName null "image/png"
    let second = McpArtifacts.buildStorageName null "image/png"

    first |> should not' (equal second)
    first.EndsWith(".png", StringComparison.Ordinal) |> should equal true
    BlobKeys.Validate first |> ignore
    BlobKeys.Validate second |> ignore

    let report = McpArtifacts.buildStorageName "../../etc/passwd" "application/pdf"
    report.Contains("/") |> should equal false
    report.Contains("..") |> should equal false
    report.EndsWith(".pdf", StringComparison.Ordinal) |> should equal true
    BlobKeys.Validate report |> ignore

[<Fact>]
let ``Substitute replaces placeholders in order and keeps the rest`` () =
    let binaries =
        ResizeArray<McpBinaryPart>(
            [
                part "image/png" [| 0x01uy |] "[non-text content: image]"
                part "image/png" [| 0x02uy |] "[non-text content: image]"
            ]
        )
        :> IReadOnlyList<McpBinaryPart>

    let replacements =
        ResizeArray<string>([ "[artifact: a]"; "[artifact: b]" ]) :> IReadOnlyList<string>

    let text =
        "look\n[non-text content: image]\n[non-text content: resource_link]\n[non-text content: image]"

    McpArtifacts.substitute text binaries replacements
    |> should equal "look\n[artifact: a]\n[non-text content: resource_link]\n[artifact: b]"
