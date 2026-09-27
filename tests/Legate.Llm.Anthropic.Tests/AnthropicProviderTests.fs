// SPDX-License-Identifier: Apache-2.0
module Legate.Llm.Anthropic.Tests.AnthropicProviderTests

open System
open System.Collections.Generic
open System.Net
open System.Net.Http
open System.Text
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

/// Native options carrying the canned key.
let private optionsWithKey (apiKey: string) : AnthropicLlmOptions =
    let options = AnthropicLlmOptions()
    options.ApiKey <- apiKey
    options

/// A native provider whose configured options carry the key.
let private providerWithKey (apiKey: string) : AnthropicProvider =
    AnthropicProvider(optionsWithKey apiKey)

/// An in-process HTTP transport answering every request from a script with
/// no sockets and no network: the SDK client built over it never leaves the
/// process. API keys are dummies that are captured, never sent.
type private ScriptedHttpHandler(respond: HttpRequestMessage -> HttpResponseMessage) =
    inherit HttpMessageHandler()

    let mutable lastApiKey = "<missing>"
    let mutable lastBody = ""

    /// The x-api-key the most recent outgoing request carried.
    member _.LastApiKey = lastApiKey

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

            let mutable values = Unchecked.defaultof<IEnumerable<string>>

            if request.Headers.TryGetValues("x-api-key", &values) then
                lastApiKey <- values |> Seq.exactlyOne

            lastBody <- body
            return respond request
        }

/// Builds the provider under test over the scripted transport and hands
/// out a client bound to the model: the exact production path minus the
/// network.
let private clientOver (handler: ScriptedHttpHandler) (model: string) : IChatClient =
    let provider = AnthropicProvider.CreateForTests(optionsWithKey "test-key", handler)
    provider.CreateChatClient(ModelReference.Parse($"anthropic/%s{model}"), keyedOptions "test-key")

/// A JSON success envelope shaped like a Messages API response.
let private jsonResponse (body: string) : HttpResponseMessage =
    let response = new HttpResponseMessage(HttpStatusCode.OK)
    response.Content <- new StringContent(body, Encoding.UTF8, "application/json")
    response

/// A server-sent-events envelope for streaming chunks.
let private sseResponse (body: string) : HttpResponseMessage =
    let response = new HttpResponseMessage(HttpStatusCode.OK)
    response.Content <- new StringContent(body, Encoding.UTF8, "text/event-stream")
    response

[<Literal>]
let private thinkingSignature = "Ev sagt9vLx7f thought sig"

[<Literal>]
let private thinkingPayload =
    """{"id":"msg_test","type":"message","role":"assistant","model":"claude-sonnet-5","content":[{"type":"thinking","thinking":"let me think","signature":"Ev sagt9vLx7f thought sig"},{"type":"text","text":"hello"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":7}}"""

let private userHistory () : IList<ChatMessage> =
    ResizeArray<ChatMessage>(
        [|
            ChatMessage(ChatRole.User, "hello")
        |]
    )
    :> IList<ChatMessage>

let private getResponse (client: IChatClient) (history: IList<ChatMessage>) : ChatResponse =
    client.GetResponseAsync(history, ChatOptions(), CancellationToken.None).GetAwaiter().GetResult()

/// Enumerates the streaming path directly and collects the reasoning texts
/// the updates carry: the runtime fans TextReasoningContent out to
/// ReasoningDelta, so asserting the content shape here covers the contract
/// without touching the runtime internals.
let private collectReasoning (client: IChatClient) (history: IList<ChatMessage>) : string list =
    let updates =
        client.GetStreamingResponseAsync(history, ChatOptions(), CancellationToken.None)

    let texts = ResizeArray<string>()
    let enumerator = updates.GetAsyncEnumerator(CancellationToken.None)

    try
        let mutable more = true

        while more do
            if enumerator.MoveNextAsync().GetAwaiter().GetResult() then
                for content in enumerator.Current.Contents do
                    match content with
                    | :? TextReasoningContent as reasoning when not (String.IsNullOrEmpty reasoning.Text) ->
                        texts.Add(reasoning.Text)
                    | _ -> ()
            else
                more <- false
    finally
        enumerator.DisposeAsync().GetAwaiter().GetResult()

    texts |> List.ofSeq

// ──────────────────────────────────────────────────────────────────────────
// Identity and capabilities

[<Fact>]
let ``Provider id capabilities and default model`` () =
    let provider = providerWithKey "test-key"
    let contracted = provider :> ILlmProvider

    contracted.Id |> should equal "anthropic"
    contracted.Capabilities.Streaming |> should equal true
    contracted.Capabilities.Reasoning |> should equal true
    contracted.Capabilities.ToolCalling |> should equal true
    contracted.DefaultModel |> should equal AnthropicLlmOptions.DefaultModel

[<Fact>]
let ``Configured model overrides the default`` () =
    let options = optionsWithKey "test-key"
    options.Model <- "claude-sonnet-4-5-20250929"

    AnthropicProvider(options).DefaultModel
    |> should equal "claude-sonnet-4-5-20250929"

[<Fact>]
let ``Blank configured model fails construction`` () =
    let options = optionsWithKey "test-key"
    options.Model <- "   "

    Assert.Throws<InvalidOperationException>(fun () -> AnthropicProvider(options) |> ignore)
    |> ignore

// ──────────────────────────────────────────────────────────────────────────
// Client construction

[<Fact>]
let ``CreateChatClient rejects references naming another provider`` () =
    let provider = providerWithKey "test-key"

    let ex =
        Assert.Throws<ArgumentException>(fun () ->
            provider.CreateChatClient(ModelReference.Parse "openai/gpt-4o-mini", keyedOptions "test-key")
            |> ignore)

    ex.Message.Contains("anthropic") |> should equal true

[<Fact>]
let ``CreateChatClient requires an API key and never echoes one`` () =
    let provider = AnthropicProvider(AnthropicLlmOptions())

    let ex =
        Assert.Throws<ProviderException>(fun () ->
            provider.CreateChatClient(ModelReference.Parse "anthropic/claude-sonnet-5", LlmProviderOptions())
            |> ignore)

    ex.ProviderId |> should equal "anthropic"
    ex.Status.HasValue |> should equal false

    ex.Message.Contains("Legate:Llm:Providers:anthropic:ApiKey")
    |> should equal true

[<Fact>]
let ``CreateChatClient accepts the configured key`` () =
    let provider = providerWithKey "test-key"

    use client =
        provider.CreateChatClient(ModelReference.Parse "anthropic/claude-sonnet-5", null)

    Assert.False(isNull (box client))

[<Fact>]
let ``Per-call key wins over the configured key on the wire`` () =
    use handler = new ScriptedHttpHandler(fun _ -> jsonResponse thinkingPayload)

    let provider =
        AnthropicProvider.CreateForTests(optionsWithKey "configured-key", handler)

    use client =
        provider.CreateChatClient(ModelReference.Parse "anthropic/claude-sonnet-5", keyedOptions "call-key")

    getResponse client (userHistory ()) |> ignore

    handler.LastApiKey |> should equal "call-key"

[<Fact>]
let ``Configured key applies when the call carries none`` () =
    use handler = new ScriptedHttpHandler(fun _ -> jsonResponse thinkingPayload)

    let provider =
        AnthropicProvider.CreateForTests(optionsWithKey "configured-key", handler)

    use client =
        provider.CreateChatClient(ModelReference.Parse "anthropic/claude-sonnet-5", null)

    getResponse client (userHistory ()) |> ignore

    handler.LastApiKey |> should equal "configured-key"

[<Fact>]
let ``CreateChatClient rejects a non-positive timeout`` () =
    let options = optionsWithKey "test-key"
    options.Timeout <- TimeSpan.Zero

    Assert.Throws<InvalidOperationException>(fun () -> AnthropicProvider(options) |> ignore)
    |> ignore

// ──────────────────────────────────────────────────────────────────────────
// Error mapping with constructed SDK exceptions (no network)

let private rateLimited () : Anthropic.Exceptions.AnthropicRateLimitException =
    Anthropic.Exceptions.AnthropicRateLimitException(
        HttpRequestException("too many requests"),
        StatusCode = HttpStatusCode.TooManyRequests,
        ResponseBody =
            """{"type":"error","error":{"type":"rate_limit_error","message":"prompt 's3cr3t-tool-args' rejected"}}"""
    )

[<Fact>]
let ``Rate limiting maps to a 429 ProviderException without RetryAfter`` () =
    // The SDK exception carries no response headers, so no Retry-After ever
    // reaches the mapping; the coordinator still honors the 429 through its
    // configured cooldown.
    let mapped = AnthropicErrors.mapError (rateLimited ()) :?> ProviderException

    mapped.ProviderId |> should equal "anthropic"
    mapped.Status.HasValue |> should equal true
    mapped.Status.Value |> should equal 429
    mapped.RetryAfter.HasValue |> should equal false
    mapped.Message.Contains("429") |> should equal true

[<Fact>]
let ``Mapping never embeds the backend body`` () =
    let mapped = AnthropicErrors.mapError (rateLimited ()) :?> ProviderException

    mapped.Message.Contains("s3cr3t-tool-args") |> should equal false
    mapped.Message.Contains("rate_limit_error") |> should equal false

[<Fact>]
let ``Server failures map with their status`` () =
    let failure =
        Anthropic.Exceptions.Anthropic5xxException(
            HttpRequestException("backend unavailable"),
            StatusCode = HttpStatusCode.ServiceUnavailable
        )

    let mapped = AnthropicErrors.mapError failure :?> ProviderException

    mapped.ProviderId |> should equal "anthropic"
    mapped.Status.Value |> should equal 503
    mapped.RetryAfter.HasValue |> should equal false

[<Fact>]
let ``Network failures and cancellations pass through for the coordinator`` () =
    let network = HttpRequestException("conn reset")
    Assert.Same(network, AnthropicErrors.mapError network)

    let ioFailure =
        Anthropic.Exceptions.AnthropicIOException("transport failed", HttpRequestException("conn reset"))

    Assert.Same(ioFailure, AnthropicErrors.mapError ioFailure)

    let canceled = OperationCanceledException()
    Assert.Same(canceled, AnthropicErrors.mapError canceled)

    let already =
        ProviderException("anthropic", Nullable 429, Nullable<TimeSpan>(), "already shaped")

    Assert.Same(already, AnthropicErrors.mapError already)

// ──────────────────────────────────────────────────────────────────────────
// Wrapper mapping over a scripted inner client

[<Fact>]
let ``Wrapper maps inner failures on the unary path`` () =
    use inner =
        new ScriptedChatClient(
            ResizeArray<ScriptStep>([| ScriptStep.Failure(rateLimited ()) |]) :> IReadOnlyList<ScriptStep>
        )

    use sdk =
        new Anthropic.AnthropicClient(Anthropic.Core.ClientOptions(ApiKey = "test-key"))

    use client = new AnthropicChatClient(inner :> IChatClient, sdk)

    let ex =
        Assert.Throws<ProviderException>(fun () -> getResponse client (userHistory ()) |> ignore)

    ex.ProviderId |> should equal "anthropic"
    ex.Status.Value |> should equal 429

[<Fact>]
let ``Wrapper maps inner failures on the streaming path`` () =
    let failure =
        Anthropic.Exceptions.Anthropic5xxException(
            HttpRequestException("backend unavailable"),
            StatusCode = HttpStatusCode.ServiceUnavailable
        )

    use inner =
        new ScriptedChatClient(ResizeArray<ScriptStep>([| ScriptStep.Failure failure |]) :> IReadOnlyList<ScriptStep>)

    use sdk =
        new Anthropic.AnthropicClient(Anthropic.Core.ClientOptions(ApiKey = "test-key"))

    use client = new AnthropicChatClient(inner :> IChatClient, sdk)

    let ex =
        Assert.Throws<ProviderException>(fun () -> collectReasoning client (userHistory ()) |> ignore)

    ex.Status.Value |> should equal 503

// ──────────────────────────────────────────────────────────────────────────
// Thinking round-trip over the scripted transport

[<Fact>]
let ``Thinking blocks surface as reasoning content`` () =
    use handler = new ScriptedHttpHandler(fun _ -> jsonResponse thinkingPayload)
    use client = clientOver handler "claude-sonnet-5"

    let response = getResponse client (userHistory ())

    response.Messages.Count |> should equal 1

    let reasonings =
        response.Messages[0].Contents
        |> Seq.choose (fun content ->
            match content with
            | :? TextReasoningContent as reasoning -> Some reasoning
            | _ -> None)
        |> List.ofSeq

    reasonings.Length |> should equal 1
    reasonings[0].Text |> should equal "let me think"

    let texts =
        response.Messages[0].Contents
        |> Seq.choose (fun content ->
            match content with
            | :? TextContent as text -> Some text
            | _ -> None)
        |> List.ofSeq

    texts.Length |> should equal 1
    texts[0].Text |> should equal "hello"

[<Fact>]
let ``Thinking signatures round-trip verbatim into follow-up requests`` () =
    use handler = new ScriptedHttpHandler(fun _ -> jsonResponse thinkingPayload)
    use client = clientOver handler "claude-sonnet-5"

    let history = userHistory ()
    let response = getResponse client history
    history.Add(response.Messages[0])
    getResponse client history |> ignore

    handler.LastRequestBody.Contains(thinkingSignature) |> should equal true

[<Literal>]
let private thoughtStream =
    """event: message_start
data: {"type":"message_start","message":{"id":"msg_stream","type":"message","role":"assistant","model":"claude-sonnet-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"stream thought"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"hi"}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":7}}

event: message_stop
data: {"type":"message_stop"}

"""

[<Fact>]
let ``Streaming thought chunks surface as reasoning content`` () =
    use handler = new ScriptedHttpHandler(fun _ -> sseResponse thoughtStream)
    use client = clientOver handler "claude-sonnet-5"

    collectReasoning client (userHistory ()) |> should equal [ "stream thought" ]

[<Fact>]
let ``Caller-applied cache breakpoints pass through to the wire`` () =
    use handler = new ScriptedHttpHandler(fun _ -> jsonResponse thinkingPayload)
    use client = clientOver handler "claude-sonnet-5"

    let content =
        AIContentCacheExtensions.WithCacheControl(
            TextContent("hello"),
            Anthropic.Models.Messages.CacheControlEphemeral()
        )

    let history =
        let message = ChatMessage(ChatRole.User, "")
        message.Contents.Add(content)
        ResizeArray<ChatMessage>([| message |]) :> IList<ChatMessage>

    getResponse client history |> ignore

    handler.LastRequestBody.Contains("cache_control") |> should equal true

[<Fact>]
let ``Transport-level rate limiting maps to a 429 ProviderException`` () =
    use handler =
        new ScriptedHttpHandler(fun _ ->
            let response = new HttpResponseMessage(enum<HttpStatusCode> 429)

            response.Content <-
                new StringContent(
                    """{"type":"error","error":{"type":"rate_limit_error","message":"prompt 's3cr3t-tool-args' rejected"}}""",
                    Encoding.UTF8,
                    "application/json"
                )

            response.Headers.TryAddWithoutValidation("Retry-After", "17") |> ignore
            response)

    use client = clientOver handler "claude-sonnet-5"

    let ex =
        Assert.Throws<ProviderException>(fun () -> getResponse client (userHistory ()) |> ignore)

    ex.ProviderId |> should equal "anthropic"
    ex.Status.Value |> should equal 429
    ex.RetryAfter.HasValue |> should equal false
    ex.Message.Contains("s3cr3t-tool-args") |> should equal false
