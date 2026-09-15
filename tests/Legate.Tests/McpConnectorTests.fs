// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpConnectorTests

open System
open System.Collections.Generic
open System.Threading
open FsUnit.Xunit
open Legate.Mcp
open ModelContextProtocol.Client
open ModelContextProtocol.Protocol
open Xunit

/// One stdio server with arguments and environment.
let private stdioServer () : McpServerOptions =
    let server = McpServerOptions(Name = "alpha", Command = "npx")
    server.Arguments.Add("--yes")
    server.Arguments.Add("run")
    server.EnvironmentVariables["TOKEN"] <- "test-token"
    server

/// One HTTP server with headers.
let private httpServer () : McpServerOptions =
    let server = McpServerOptions(Name = "beta", Url = "http://127.0.0.1:8080/mcp")
    server.Headers["Authorization"] <- "Bearer test"
    server

[<Fact>]
let ``Stdio options carry command arguments environment and name`` () =
    let options = SdkTransportOptions.buildStdioOptions (stdioServer ())
    unbox<string> (box options.Command) |> should equal "npx"

    unbox<IList<string>> (box options.Arguments)
    |> Seq.toList
    |> should equal [ "--yes"; "run" ]

    (unbox<IDictionary<string, string>> (box options.EnvironmentVariables))["TOKEN"]
    |> should equal "test-token"

    unbox<string> (box options.Name) |> should equal "alpha"

[<Fact>]
let ``Stdio options copy arguments and environment`` () =
    let server = stdioServer ()
    let options = SdkTransportOptions.buildStdioOptions server
    (unbox<IList<string>> (box options.Arguments)).Add("late")
    (unbox<IDictionary<string, string>> (box options.EnvironmentVariables))["LATE"] <- "late"
    server.Arguments.Count |> should equal 2
    server.EnvironmentVariables.ContainsKey("LATE") |> should equal false

[<Fact>]
let ``Stdio options reject servers without exactly one transport`` () =
    (fun () ->
        SdkTransportOptions.buildStdioOptions (McpServerOptions(Name = "neither"))
        |> ignore)
    |> should throw typeof<InvalidOperationException>

    (fun () ->
        SdkTransportOptions.buildStdioOptions (
            McpServerOptions(Name = "both", Command = "npx", Url = "http://127.0.0.1:8080/mcp")
        )
        |> ignore)
    |> should throw typeof<InvalidOperationException>

[<Fact>]
let ``HTTP options pin streamable HTTP with endpoint headers and name`` () =
    let options = SdkTransportOptions.buildHttpOptions (httpServer ())

    options.Endpoint
    |> should equal (Uri("http://127.0.0.1:8080/mcp", UriKind.Absolute))

    options.TransportMode |> should equal HttpTransportMode.StreamableHttp
    options.TransportMode |> should not' (equal HttpTransportMode.Sse)
    options.TransportMode |> should not' (equal HttpTransportMode.AutoDetect)

    (unbox<IDictionary<string, string>> (box options.AdditionalHeaders))["Authorization"]
    |> should equal "Bearer test"

    unbox<string> (box options.Name) |> should equal "beta"

[<Fact>]
let ``HTTP options copy headers`` () =
    let server = httpServer ()
    let options = SdkTransportOptions.buildHttpOptions server
    (unbox<IDictionary<string, string>> (box options.AdditionalHeaders))["LATE"] <- "late"
    server.Headers.ContainsKey("LATE") |> should equal false

[<Fact>]
let ``HTTP options reject stdio servers and bad urls`` () =
    (fun () -> SdkTransportOptions.buildHttpOptions (stdioServer ()) |> ignore)
    |> should throw typeof<InvalidOperationException>

    (fun () ->
        SdkTransportOptions.buildHttpOptions (McpServerOptions(Name = "bad", Url = "not-a-uri"))
        |> ignore)
    |> should throw typeof<InvalidOperationException>

// ──────────────────────────
// Binary extraction: scripted blocks only, no live servers.

// A minimal 1x1 PNG payload.
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

/// Builds a call result over the given blocks.
let private callResult (blocks: ContentBlock list) : CallToolResult =
    CallToolResult(Content = ResizeArray<ContentBlock>(blocks))

/// One text block.
let private textBlock (text: string) : ContentBlock =
    TextContentBlock(Text = text) :> ContentBlock

/// One image block over the given bytes.
let private imageBlock (bytes: byte[]) (mime: string) : ContentBlock =
    ImageContentBlock.FromBytes(ReadOnlyMemory bytes, mime) :> ContentBlock

/// One embedded blob resource block.
let private blobResourceBlock (bytes: byte[]) (uri: string) (mime: string) : ContentBlock =
    let contents = BlobResourceContents.FromBytes(ReadOnlyMemory bytes, uri, mime)
    EmbeddedResourceBlock(Resource = contents) :> ContentBlock

[<Fact>]
let ``Split keeps text byte-identical and extracts image binaries`` () =
    let result =
        callResult
            [
                textBlock "hello"
                imageBlock (png1x1 ()) "image/png"
            ]

    let text, binaries = McpProtocolMapping.splitCallResult result

    text |> should equal (McpProtocolMapping.renderCallResult result)
    text |> should equal "hello\n[non-text content: image]"
    binaries.Count |> should equal 1
    binaries.[0].MimeType |> should equal "image/png"
    binaries.[0].Bytes |> should equal (png1x1 ())
    binaries.[0].Name |> should equal null

[<Fact>]
let ``Embedded blob resources yield mime bytes and URI names`` () =
    let pdf = [| 0x25uy; 0x50uy; 0x44uy; 0x46uy |]

    let result =
        callResult
            [
                blobResourceBlock pdf "files/report.pdf" "application/pdf"
            ]

    let text, binaries = McpProtocolMapping.splitCallResult result

    text |> should equal (McpProtocolMapping.renderCallResult result)

    text.StartsWith("[non-text content:", StringComparison.Ordinal)
    |> should equal true

    binaries.Count |> should equal 1
    binaries.[0].MimeType |> should equal "application/pdf"
    binaries.[0].Bytes |> should equal pdf
    binaries.[0].Name |> should equal "report.pdf"

[<Fact>]
let ``Text-only results carry no binaries`` () =
    let result = callResult [ textBlock "hello" ]
    let text, binaries = McpProtocolMapping.splitCallResult result

    text |> should equal "hello"
    binaries.Count |> should equal 0

[<Fact>]
let ``Null results split to empty text with no binaries`` () =
    let text, binaries = McpProtocolMapping.splitCallResult null

    text |> should equal ""
    binaries.Count |> should equal 0

[<Fact>]
let ``Client options decline every elicitation request`` () =
    let options = McpElicitation.buildClientOptions ()
    options |> should not' (be null)
    options.Handlers |> should not' (be null)
    options.Handlers.ElicitationHandler |> should not' (be null)

    let result =
        options.Handlers.ElicitationHandler
            .Invoke(ElicitRequestParams(Message = "confirm"), CancellationToken.None)
            .Result

    unbox<string> (box result.Action) |> should equal McpElicitation.DeclineAction
    unbox<string> (box result.Action) |> should equal "decline"
    result.IsAccepted |> should equal false
