// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpInvocationTests

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Mcp
open Legate.Mcp.McpArtifacts
open ModelContextProtocol
open ModelContextProtocol.Protocol
open Xunit

// Scripted sessions only: no subprocess, no socket. The scripted session
// answers from memory, records the token it received, and optionally
// raises, so error mapping, cancellation passthrough, and observation
// stamping are verified without live servers.

// ──────────────────────────
// Scripted session

/// Empty binary list for results carrying no binary blocks.
let private noBinaries () : IReadOnlyList<McpBinaryPart> =
    ResizeArray<McpBinaryPart>() :> IReadOnlyList<McpBinaryPart>

/// One scripted session: answers every call with the scripted result,
/// records the token it received, and raises the scripted failure instead
/// when one is set.
type internal ScriptedInvokeSession(answer: McpCallResult, failure: exn | null) =

    let mutable seen: CancellationToken option = None

    interface IMcpServerSession with
        member _.ServerName: string = "alpha"

        member _.ListToolsAsync
            (_cancellationToken: CancellationToken)
            : Task<IReadOnlyList<McpDiscovery.McpDiscoveredTool>> =
            task {
                return ResizeArray<McpDiscovery.McpDiscoveredTool>() :> IReadOnlyList<McpDiscovery.McpDiscoveredTool>
            }

        member _.CallToolAsync
            (_toolName: string, _arguments: IReadOnlyDictionary<string, obj>, cancellationToken: CancellationToken)
            : Task<McpCallResult> =
            task {
                seen <- Some cancellationToken

                match box failure with
                | :? exn as toRaise -> return! Task.FromException<McpCallResult>(toRaise)
                | _ ->
                    // A cancelled turn abandons the call: wait until the
                    // token fires so cancellation propagates instead of the
                    // answer returning.
                    do! Task.Delay(Timeout.Infinite, cancellationToken)
                    return answer
            }

    interface IAsyncDisposable with
        member _.DisposeAsync() : ValueTask = ValueTask.CompletedTask

    /// The token the session received on its latest call, when called.
    member _.Seen: CancellationToken option = seen

/// A session answering immediately without waiting on the token.
type internal ImmediateSession(answer: McpCallResult, failure: exn | null) =

    let mutable seen: CancellationToken option = None

    interface IMcpServerSession with
        member _.ServerName: string = "alpha"

        member _.ListToolsAsync
            (_cancellationToken: CancellationToken)
            : Task<IReadOnlyList<McpDiscovery.McpDiscoveredTool>> =
            task {
                return ResizeArray<McpDiscovery.McpDiscoveredTool>() :> IReadOnlyList<McpDiscovery.McpDiscoveredTool>
            }

        member _.CallToolAsync
            (_toolName: string, _arguments: IReadOnlyDictionary<string, obj>, cancellationToken: CancellationToken)
            : Task<McpCallResult> =
            task {
                seen <- Some cancellationToken

                match box failure with
                | :? exn as toRaise -> return! Task.FromException<McpCallResult>(toRaise)
                | _ -> return answer
            }

    interface IAsyncDisposable with
        member _.DisposeAsync() : ValueTask = ValueTask.CompletedTask

    /// The token the session received on its latest call, when called.
    member _.Seen: CancellationToken option = seen

// ──────────────────────────
// Helpers

/// The empty call arguments.
let private noArguments () : IReadOnlyDictionary<string, obj> =
    Dictionary<string, obj>(StringComparer.Ordinal) :> IReadOnlyDictionary<string, obj>

/// Invokes through the scripted session, capturing observations.
let private invoke
    (session: IMcpServerSession)
    (observations: ResizeArray<McpInvocation.McpCallObservation>)
    (cancellationToken: CancellationToken)
    : string =
    let sink =
        Action<McpInvocation.McpCallObservation>(fun observation -> observations.Add(observation))

    let pending =
        McpInvocation.invokeAsync session "alpha_read" "read" (noArguments ()) cancellationToken sink

    pending.GetAwaiter().GetResult()

/// The single stamped observation, failing when the count differs.
let private single (observations: ResizeArray<McpInvocation.McpCallObservation>) : McpInvocation.McpCallObservation =
    observations.Count |> should equal 1
    observations[0]

// ──────────────────────────
// Mapping and observations

[<Fact>]
let ``Success returns the text and stamps a clean observation`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()

    let session =
        ImmediateSession(
            {
                Text = "ok"
                IsError = false
                Binaries = noBinaries ()
            },
            null
        )

    invoke (session :> IMcpServerSession) observations CancellationToken.None
    |> should equal "ok"

    let stamped = single observations
    stamped.ToolName |> should equal "alpha_read"
    stamped.Text |> should equal "ok"
    stamped.IsError |> should equal false
    (stamped.Duration >= TimeSpan.Zero) |> should equal true

[<Fact>]
let ``IsError maps to Error from carrying the tool name`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()

    let session =
        ImmediateSession(
            {
                Text = "denied by policy"
                IsError = true
                Binaries = noBinaries ()
            },
            null
        )

    invoke (session :> IMcpServerSession) observations CancellationToken.None
    |> should equal "Error from alpha_read: denied by policy"

    let stamped = single observations
    stamped.ToolName |> should equal "alpha_read"
    stamped.Text |> should equal "Error from alpha_read: denied by policy"
    stamped.IsError |> should equal true
    (stamped.Duration >= TimeSpan.Zero) |> should equal true

[<Fact>]
let ``Transport failure maps to Error calling with the exception shape`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()

    let session =
        ImmediateSession(
            {
                Text = ""
                IsError = false
                Binaries = noBinaries ()
            },
            InvalidOperationException("connection reset")
        )

    invoke (session :> IMcpServerSession) observations CancellationToken.None
    |> should equal "Error calling alpha_read: InvalidOperationException: connection reset"

    let stamped = single observations
    stamped.IsError |> should equal true

    stamped.Text
    |> should equal "Error calling alpha_read: InvalidOperationException: connection reset"

    (stamped.Duration >= TimeSpan.Zero) |> should equal true

[<Fact>]
let ``Elicitation decline maps to Error calling text and the turn continues`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()

    // The SDK surfaces a declined elicitation as a call failure: the fake
    // raises the SDK exception type with the decline message, and invoke
    // returns the mapped text instead of throwing, so the turn continues.
    let session =
        ImmediateSession(
            {
                Text = ""
                IsError = false
                Binaries = noBinaries ()
            },
            McpException("The elicitation request was declined.")
        )

    let text = invoke (session :> IMcpServerSession) observations CancellationToken.None

    text.StartsWith("Error calling alpha_read: McpException:", StringComparison.Ordinal)
    |> should equal true

    text.Contains("declined") |> should equal true

    let stamped = single observations
    stamped.IsError |> should equal true
    stamped.Text |> should equal text

[<Fact>]
let ``Null sink observes nothing`` () =
    let session =
        ImmediateSession(
            {
                Text = "ok"
                IsError = false
                Binaries = noBinaries ()
            },
            null
        )

    let pending =
        McpInvocation.invokeAsync
            (session :> IMcpServerSession)
            "alpha_read"
            "read"
            (noArguments ())
            CancellationToken.None
            null

    pending.GetAwaiter().GetResult() |> should equal "ok"

// ──────────────────────────
// Cancellation

[<Fact>]
let ``Cancel aborts the call and stamps nothing`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()

    let session =
        ScriptedInvokeSession(
            {
                Text = "ok"
                IsError = false
                Binaries = noBinaries ()
            },
            null
        )

    use cts = new CancellationTokenSource()

    let pending =
        McpInvocation.invokeAsync
            (session :> IMcpServerSession)
            "alpha_read"
            "read"
            (noArguments ())
            cts.Token
            (Action<McpInvocation.McpCallObservation>(fun observation -> observations.Add(observation)))

    cts.Cancel()

    (fun () -> pending.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<OperationCanceledException>

    observations.Count |> should equal 0

[<Fact>]
let ``Turn token reaches CallToolAsync`` () =
    let session =
        ImmediateSession(
            {
                Text = "ok"
                IsError = false
                Binaries = noBinaries ()
            },
            null
        )

    use cts = new CancellationTokenSource()

    let pending =
        McpInvocation.invokeAsync (session :> IMcpServerSession) "alpha_read" "read" (noArguments ()) cts.Token null

    pending.GetAwaiter().GetResult() |> should equal "ok"

    match session.Seen with
    | Some seen -> seen |> should equal cts.Token
    | None -> Assert.Fail("The session never received the call.") |> ignore

// ──────────────────────────
// Artifact handling: scripted sessions and scoped in-memory sinks only.

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

/// A recording artifact sink: every stored name is recorded, Put honors
/// cancellation first, and Put optionally fails.
type internal ScopedArtifactSink(failStore: bool) =
    let backing = Dictionary<string, byte[] * string>(StringComparer.Ordinal)
    let names = ResizeArray<string>()

    interface IArtifactSink with
        member _.StoreAsync(name, content, cancellationToken) =
            cancellationToken.ThrowIfCancellationRequested()
            names.Add(name)

            if failStore then
                raise (InvalidOperationException("store unavailable"))

            backing[name] <- content.Bytes, content.ContentType

            let dimensions =
                if McpArtifacts.isImageMime content.ContentType then
                    McpArtifacts.tryGetDimensions content.ContentType content.Bytes
                else
                    None

            Task.FromResult(
                StoredArtifact(
                    McpArtifacts.formatReference name content.ContentType content.Bytes.Length dimensions,
                    name
                )
            )

    /// The names stored so far.
    member _.StoredNames: string list = backing.Keys |> Seq.toList

    /// The names handed to Store, in order.
    member _.PutNames: string list = names |> Seq.toList

/// One SDK image answer: text plus a 1x1 PNG, split exactly as the
/// connector splits them so placeholders match.
let private imageAnswer (bytes: byte[]) (mime: string) (text: string) : McpCallResult =
    let blocks = ResizeArray<ContentBlock>()
    blocks.Add(TextContentBlock(Text = text) :> ContentBlock)
    blocks.Add(ImageContentBlock.FromBytes(ReadOnlyMemory bytes, mime) :> ContentBlock)

    let result = CallToolResult(Content = blocks)

    let rendered, binaries = McpProtocolMapping.splitCallResult result

    {
        Text = rendered
        IsError = false
        Binaries = binaries
    }

/// One SDK blob-resource answer over an embedded PDF.
let private blobAnswer () : McpCallResult =
    let pdf = [| 0x25uy; 0x50uy; 0x44uy; 0x46uy |]

    let contents =
        BlobResourceContents.FromBytes(ReadOnlyMemory pdf, "files/report.pdf", "application/pdf")

    let blocks = ResizeArray<ContentBlock>()
    blocks.Add(EmbeddedResourceBlock(Resource = contents) :> ContentBlock)

    let result = CallToolResult(Content = blocks)

    let rendered, binaries = McpProtocolMapping.splitCallResult result

    {
        Text = rendered
        IsError = false
        Binaries = binaries
    }

/// Invokes through the artifact-aware path with the given sink and caps.
let private invokeArtifacts
    (session: IMcpServerSession)
    (sink: IArtifactSink | null)
    (caps: McpArtifacts.McpArtifactCaps)
    (observations: ResizeArray<McpInvocation.McpCallObservation>)
    (cancellationToken: CancellationToken)
    : string =
    let observe =
        Action<McpInvocation.McpCallObservation>(fun observation -> observations.Add(observation))

    McpInvocation.invokeWithArtifactsAsync
        session
        "alpha_read"
        "read"
        (noArguments ())
        cancellationToken
        observe
        sink
        caps
    |> fun pending -> pending.GetAwaiter().GetResult()

[<Fact>]
let ``Image block is stored and substituted with a text reference`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()
    let sink = ScopedArtifactSink(false)
    let answer = imageAnswer (png1x1 ()) "image/png" "snapshot"
    let session = ImmediateSession(answer, null)

    let result =
        invokeArtifacts
            (session :> IMcpServerSession)
            (sink :> IArtifactSink)
            McpArtifacts.McpArtifactCaps.Default
            observations
            CancellationToken.None

    result.Contains("[artifact:") |> should equal true
    result.Contains("mime=\"image/png\"") |> should equal true
    result.Contains("dimensions=\"1x1\"") |> should equal true
    result.Contains($"{(png1x1 ()).Length} bytes") |> should equal true
    result.Contains("[non-text content:") |> should equal false
    sink.StoredNames.Length |> should equal 1

    let stamped = single observations
    stamped.Text |> should equal result
    stamped.IsError |> should equal false

[<Fact>]
let ``Binary resource is stored with its URI name and no dimensions`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()
    let sink = ScopedArtifactSink(false)
    let session = ImmediateSession(blobAnswer (), null)

    let result =
        invokeArtifacts
            (session :> IMcpServerSession)
            (sink :> IArtifactSink)
            McpArtifacts.McpArtifactCaps.Default
            observations
            CancellationToken.None

    result.Contains("[artifact:") |> should equal true
    result.Contains("mime=\"application/pdf\"") |> should equal true
    result.Contains("report-pdf") |> should equal true
    result.Contains("dimensions=") |> should equal false
    sink.StoredNames.Length |> should equal 1

    let stamped = single observations
    stamped.Text |> should equal result
    stamped.IsError |> should equal false

[<Fact>]
let ``Oversized image yields a bounded rejection and the turn continues`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()
    let sink = ScopedArtifactSink(false)
    let answer = imageAnswer (png1x1 ()) "image/png" "snapshot"
    let session = ImmediateSession(answer, null)

    let caps =
        {
            MaxBytes = 10
            MaxDimension = 4096
            MaxPixels = 16777216L
        }

    let result =
        invokeArtifacts (session :> IMcpServerSession) (sink :> IArtifactSink) caps observations CancellationToken.None

    result.Contains("[artifact rejected:") |> should equal true
    result.Contains("exceeds 10-byte cap") |> should equal true
    sink.StoredNames.Length |> should equal 0

    let stamped = single observations
    stamped.Text |> should equal result
    stamped.IsError |> should equal false

[<Fact>]
let ``Storage failure returns the original text and the turn continues`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()
    let sink = ScopedArtifactSink(true)
    let answer = imageAnswer (png1x1 ()) "image/png" "snapshot"
    let session = ImmediateSession(answer, null)

    let result =
        invokeArtifacts
            (session :> IMcpServerSession)
            (sink :> IArtifactSink)
            McpArtifacts.McpArtifactCaps.Default
            observations
            CancellationToken.None

    result |> should equal answer.Text

    let stamped = single observations
    stamped.Text |> should equal answer.Text
    stamped.IsError |> should equal false

[<Fact>]
let ``Null sink keeps placeholder output`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()
    let answer = imageAnswer (png1x1 ()) "image/png" "snapshot"
    let session = ImmediateSession(answer, null)

    let result =
        invokeArtifacts
            (session :> IMcpServerSession)
            null
            McpArtifacts.McpArtifactCaps.Default
            observations
            CancellationToken.None

    result |> should equal answer.Text

    let stamped = single observations
    stamped.Text |> should equal answer.Text
    stamped.IsError |> should equal false

[<Fact>]
let ``Tenancy: sinks receive only pre-scoped names and never leak across tenants`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()
    let sinkA = ScopedArtifactSink(false)
    let sinkB = ScopedArtifactSink(false)
    let answer = imageAnswer (png1x1 ()) "image/png" "snapshot"
    let session = ImmediateSession(answer, null)

    invokeArtifacts
        (session :> IMcpServerSession)
        (sinkA :> IArtifactSink)
        McpArtifacts.McpArtifactCaps.Default
        observations
        CancellationToken.None
    |> ignore

    sinkA.StoredNames.Length |> should equal 1
    sinkB.StoredNames.Length |> should equal 0

    for name in sinkA.PutNames do
        name.Contains("tenant") |> should equal false
        BlobKeys.Validate name |> ignore

    invokeArtifacts
        (session :> IMcpServerSession)
        (sinkB :> IArtifactSink)
        McpArtifacts.McpArtifactCaps.Default
        observations
        CancellationToken.None
    |> ignore

    sinkB.StoredNames.Length |> should equal 1
    sinkA.StoredNames.Length |> should equal 1

[<Fact>]
let ``Cancelled store propagates with nothing stamped`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()
    let sink = ScopedArtifactSink(false)
    let answer = imageAnswer (png1x1 ()) "image/png" "snapshot"
    let session = ImmediateSession(answer, null)

    use cts = new CancellationTokenSource()
    cts.Cancel()

    let observe =
        Action<McpInvocation.McpCallObservation>(fun observation -> observations.Add(observation))

    let pending =
        McpInvocation.invokeWithArtifactsAsync
            (session :> IMcpServerSession)
            "alpha_read"
            "read"
            (noArguments ())
            cts.Token
            observe
            (sink :> IArtifactSink)
            McpArtifacts.McpArtifactCaps.Default

    (fun () -> pending.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<OperationCanceledException>

    observations.Count |> should equal 0
    sink.StoredNames.Length |> should equal 0
