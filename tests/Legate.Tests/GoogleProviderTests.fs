// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.GoogleProviderTests

open System
open System.Collections.Generic
open System.Net
open System.Net.Http
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Llm
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// A provider options instance carrying only an API key.
let private keyedOptions (apiKey: string) : LlmProviderOptions =
    let options = LlmProviderOptions()
    options.ApiKey <- apiKey
    options

/// A Google provider whose configured options carry the key.
let private providerWithKey (apiKey: string) : GoogleProvider =
    let configured = GoogleLlmOptions()
    configured.ApiKey <- apiKey
    GoogleProvider(configured)

/// An in-process HTTP transport answering every request from a script with
/// no sockets and no network: the SDK client built over it never leaves the
/// process. The API key is a dummy that is captured, never sent.
type private ScriptedHttpHandler(respond: HttpRequestMessage -> HttpResponseMessage) =
    inherit HttpMessageHandler()

    let mutable lastBody = ""

    /// The body of the most recent outgoing request.
    member _.LastRequestBody = lastBody

    override _.SendAsync
        (request: HttpRequestMessage, cancellationToken: CancellationToken)
        : Task<HttpResponseMessage> =
        task {
            let! body =
                match request.Content with
                | null -> Task.FromResult("")
                | content -> content.ReadAsStringAsync(cancellationToken)

            lastBody <- body
            return respond request
        }

/// Builds a real SDK client over the scripted transport.
let private sdkClientOver (handler: HttpMessageHandler) : Google.GenAI.Client =
    let factory = Func<HttpClient>(fun () -> new HttpClient(handler))
    let clientOptions = Google.GenAI.Types.ClientOptions(HttpClientFactory = factory)

    new Google.GenAI.Client(
        apiKey = "test-key",
        httpOptions = Google.GenAI.Types.HttpOptions(),
        clientOptions = clientOptions
    )

/// Wraps a real SDK adapter (over the scripted transport) in the provider's
/// mapping wrapper: the exact production path minus the network.
let private wrappedOver (handler: ScriptedHttpHandler) : GoogleChatClient * Google.GenAI.Client =
    let sdk = sdkClientOver handler
    let inner = sdk.AsIChatClient("gemini-3.8-flash")
    new GoogleChatClient(inner, sdk), sdk

/// A JSON success envelope for one assistant message.
let private jsonResponse (body: string) : HttpResponseMessage =
    let response = new HttpResponseMessage(HttpStatusCode.OK)
    response.Content <- new StringContent(body, Encoding.UTF8, "application/json")
    response

/// A server-sent-events envelope for one streaming chunk.
let private sseResponse (body: string) : HttpResponseMessage =
    let response = new HttpResponseMessage(HttpStatusCode.OK)
    response.Content <- new StringContent(body, Encoding.UTF8, "text/event-stream")
    response

[<Literal>]
let private thoughtSignatureBase64 = "AQID+v8="

[<Literal>]
let private thoughtPayload =
    """{"candidates":[{"content":{"role":"model","parts":[{"text":"let me think","thought":true,"thoughtSignature":"AQID+v8="}]},"finishReason":"STOP"}],"modelVersion":"gemini-3.8-flash","responseId":"r1","usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":7,"totalTokenCount":12}}"""

[<Literal>]
let private thoughtChunk =
    """data: {"candidates":[{"content":{"role":"model","parts":[{"text":"stream thought","thought":true,"thoughtSignature":"AQID+v8="}]}}],"responseId":"s1"}"""
    + "\n\n"

let private userHistory () : IList<ChatMessage> =
    ResizeArray<ChatMessage>(
        [|
            ChatMessage(ChatRole.User, "hello")
        |]
    )
    :> IList<ChatMessage>

let private getResponse (client: IChatClient) (history: IList<ChatMessage>) : ChatResponse =
    client.GetResponseAsync(history, ChatOptions(), CancellationToken.None).GetAwaiter().GetResult()

// ──────────────────────────────────────────────────────────────────────────
// Identity and capabilities

[<Fact>]
let ``Provider id capabilities and default model`` () =
    let provider = providerWithKey "test-key"
    let contracted = provider :> ILlmProvider

    contracted.Id |> should equal "google"
    contracted.Capabilities.Streaming |> should equal true
    contracted.Capabilities.Reasoning |> should equal true
    contracted.Capabilities.ToolCalling |> should equal true
    contracted.DefaultModel |> should equal GoogleLlmOptions.DefaultModel

[<Fact>]
let ``Configured model overrides the default`` () =
    let configured = GoogleLlmOptions()
    configured.ApiKey <- "test-key"
    configured.Model <- "gemini-3.8-flash"

    GoogleProvider(configured).DefaultModel |> should equal "gemini-3.8-flash"

[<Fact>]
let ``Blank configured model falls back to the default`` () =
    let configured = GoogleLlmOptions()
    configured.ApiKey <- "test-key"
    configured.Model <- "   "

    GoogleProvider(configured).DefaultModel
    |> should equal GoogleLlmOptions.DefaultModel

// ──────────────────────────────────────────────────────────────────────────
// Client construction

[<Fact>]
let ``CreateChatClient rejects references naming another provider`` () =
    let provider = providerWithKey "test-key"

    let ex =
        Assert.Throws<ArgumentException>(fun () ->
            provider.CreateChatClient(ModelReference.Parse "anthropic/claude-sonnet", keyedOptions "test-key")
            |> ignore)

    ex.Message.Contains("google") |> should equal true

[<Fact>]
let ``CreateChatClient requires an API key and never echoes one`` () =
    let provider = GoogleProvider(GoogleLlmOptions())

    let ex =
        Assert.Throws<ArgumentException>(fun () ->
            provider.CreateChatClient(ModelReference.Parse "google/gemini-3.8-flash", LlmProviderOptions())
            |> ignore)

    ex.Message.Contains("GoogleLlmOptions.ApiKey") |> should equal true

[<Fact>]
let ``CreateChatClient accepts the configured key`` () =
    let provider = providerWithKey "test-key"

    use client =
        provider.CreateChatClient(ModelReference.Parse "google/gemini-3.8-flash", null)

    Assert.False(isNull (box client))

[<Fact>]
let ``CreateChatClient rejects a non-positive timeout`` () =
    let configured = GoogleLlmOptions()
    configured.ApiKey <- "test-key"
    configured.Timeout <- TimeSpan.Zero
    let provider = GoogleProvider(configured)

    Assert.Throws<ArgumentOutOfRangeException>(fun () ->
        provider.CreateChatClient(ModelReference.Parse "google/gemini-3.8-flash", null)
        |> ignore)
    |> ignore

// ──────────────────────────────────────────────────────────────────────────
// Error mapping with constructed SDK exceptions (no network)

[<Fact>]
let ``Quota maps to a 429 ProviderException without RetryAfter`` () =
    // The SDK drops response headers on its errors, so no Retry-After ever
    // reaches the mapping; the coordinator still honors the 429 through its
    // configured cooldown.
    let failure =
        Google.GenAI.ClientError("Quota exceeded for gemini-3.8-flash.", 429, "RESOURCE_EXHAUSTED")

    let mapped = GoogleErrors.mapError failure :?> ProviderException

    mapped.ProviderId |> should equal "google"
    mapped.Status.HasValue |> should equal true
    mapped.Status.Value |> should equal 429
    mapped.RetryAfter.HasValue |> should equal false
    mapped.Message.Contains("429") |> should equal true
    mapped.Message.Contains("RESOURCE_EXHAUSTED") |> should equal true

[<Fact>]
let ``Mapping never embeds the backend message`` () =
    let failure =
        Google.GenAI.ClientError("prompt 's3cr3t-tool-args' rejected", 400, "INVALID_ARGUMENT")

    let mapped = GoogleErrors.mapError failure :?> ProviderException

    mapped.Status.Value |> should equal 400
    mapped.Message.Contains("s3cr3t-tool-args") |> should equal false
    mapped.Message.Contains("INVALID_ARGUMENT") |> should equal true

[<Fact>]
let ``Server failures map with their status`` () =
    let failure = Google.GenAI.ServerError("backend unavailable", 503, "UNAVAILABLE")
    let mapped = GoogleErrors.mapError failure :?> ProviderException

    mapped.ProviderId |> should equal "google"
    mapped.Status.Value |> should equal 503
    mapped.RetryAfter.HasValue |> should equal false

[<Fact>]
let ``Network failures and cancellations pass through for the coordinator`` () =
    let network = HttpRequestException("conn reset")
    Assert.Same(network, GoogleErrors.mapError network)

    let socket = Net.Sockets.SocketException()
    Assert.Same(socket, GoogleErrors.mapError socket)

    let canceled = OperationCanceledException()
    Assert.Same(canceled, GoogleErrors.mapError canceled)

    let already =
        ProviderException("google", Nullable 429, Nullable<TimeSpan>(), "already shaped")

    Assert.Same(already, GoogleErrors.mapError already)

// ──────────────────────────────────────────────────────────────────────────
// Wrapper mapping over a scripted inner client

[<Fact>]
let ``Wrapper maps inner failures on the unary path`` () =
    let failure = Google.GenAI.ClientError("Quota exceeded.", 429, "RESOURCE_EXHAUSTED")

    use inner =
        new ScriptedChatClient(ResizeArray<ScriptStep>([| ScriptStep.Failure failure |]) :> IReadOnlyList<ScriptStep>)

    let sdk =
        sdkClientOver (new ScriptedHttpHandler(fun _ -> jsonResponse thoughtPayload))

    use client = new GoogleChatClient(inner :> IChatClient, sdk)

    let ex =
        Assert.Throws<ProviderException>(fun () -> getResponse client (userHistory ()) |> ignore)

    ex.ProviderId |> should equal "google"
    ex.Status.Value |> should equal 429

[<Fact>]
let ``Wrapper maps inner failures on the streaming path`` () =
    let failure = Google.GenAI.ServerError("backend unavailable", 503, "UNAVAILABLE")

    use inner =
        new ScriptedChatClient(ResizeArray<ScriptStep>([| ScriptStep.Failure failure |]) :> IReadOnlyList<ScriptStep>)

    let sdk =
        sdkClientOver (new ScriptedHttpHandler(fun _ -> jsonResponse thoughtPayload))

    use client = new GoogleChatClient(inner :> IChatClient, sdk)

    let ex =
        Assert.Throws<ProviderException>(fun () ->
            LlmStreaming.streamResponseAsync
                (client :> IChatClient)
                (userHistory ())
                (ChatOptions())
                CancellationToken.None
                (fun _ -> ())
                (fun _ -> ())
            |> fun task -> task.GetAwaiter().GetResult() |> ignore)

    ex.Status.Value |> should equal 503

// ──────────────────────────────────────────────────────────────────────────
// Thought-signature round-trip over the scripted transport

[<Fact>]
let ``Thought parts surface as reasoning with the signature marker`` () =
    use handler = new ScriptedHttpHandler(fun _ -> jsonResponse thoughtPayload)
    let client, _ = wrappedOver handler
    use client = client :> IChatClient

    let response = getResponse client (userHistory ())

    response.Messages.Count |> should equal 1
    response.Messages[0].Contents.Count |> should equal 2

    let thought = response.Messages[0].Contents[0] :?> TextReasoningContent
    thought.Text |> should equal "let me think"
    thought.RawRepresentation :? Google.GenAI.Types.Part |> should equal true

    let marker = response.Messages[0].Contents[1] :?> TextReasoningContent
    marker.ProtectedData |> should equal thoughtSignatureBase64

[<Fact>]
let ``Thought signatures round-trip verbatim into follow-up requests`` () =
    // Adapter-first proof: the history carries the exact raw blocks by
    // reference, so the SDK adapter reattaches the verbatim signature bytes
    // and no thin adapter is needed.
    use handler = new ScriptedHttpHandler(fun _ -> jsonResponse thoughtPayload)
    let client, _ = wrappedOver handler
    use client = client :> IChatClient

    let history = userHistory ()
    let response = getResponse client history
    history.Add(response.Messages[0])
    getResponse client history |> ignore

    use document = JsonDocument.Parse(handler.LastRequestBody)
    let contents = document.RootElement.GetProperty("contents")
    contents.GetArrayLength() |> should equal 2

    let parts = contents[1].GetProperty("parts")
    parts[0].GetProperty("thought").GetBoolean() |> should equal true
    // JsonDocument unescapes the transport encoding: the exact bytes survive.
    parts[0].GetProperty("thoughtSignature").GetString()
    |> should equal thoughtSignatureBase64

[<Fact>]
let ``Streaming thought chunks fan out to reasoning deltas`` () =
    use handler = new ScriptedHttpHandler(fun _ -> sseResponse thoughtChunk)
    let client, _ = wrappedOver handler
    use client = client :> IChatClient

    let texts = ResizeArray<string>()
    let reasonings = ResizeArray<string>()

    let response =
        LlmStreaming.streamResponseAsync
            client
            (userHistory ())
            (ChatOptions())
            CancellationToken.None
            (fun text -> texts.Add(text))
            (fun text -> reasonings.Add(text))
        |> fun task -> task.GetAwaiter().GetResult()

    reasonings.Count |> should equal 1
    reasonings[0] |> should equal "stream thought"
    texts.Count |> should equal 0

    let markers =
        response.Messages[0].Contents
        |> Seq.choose (fun content ->
            match content with
            | :? TextReasoningContent as reasoning when reasoning.ProtectedData = thoughtSignatureBase64 ->
                Some reasoning
            | _ -> None)
        |> List.ofSeq

    markers.Length |> should equal 1

[<Fact>]
let ``Transport-level quota maps to a 429 ProviderException`` () =
    use handler =
        new ScriptedHttpHandler(fun _ ->
            let response = new HttpResponseMessage(enum<HttpStatusCode> 429)

            response.Content <-
                new StringContent(
                    """{"error":{"code":429,"message":"Quota exceeded.","status":"RESOURCE_EXHAUSTED"}}""",
                    Encoding.UTF8,
                    "application/json"
                )

            // Advertised but dropped by the SDK: the mapping must not claim it.
            response.Headers.TryAddWithoutValidation("Retry-After", "17") |> ignore
            response)

    let client, _ = wrappedOver handler
    use client = client :> IChatClient

    let ex =
        Assert.Throws<ProviderException>(fun () -> getResponse client (userHistory ()) |> ignore)

    ex.ProviderId |> should equal "google"
    ex.Status.Value |> should equal 429
    ex.RetryAfter.HasValue |> should equal false
    ex.Message.Contains("Quota exceeded.") |> should equal false
