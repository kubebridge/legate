// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.OpenAICompatibleProviderTests

open System
open System.Collections.Generic
open System.Net
open System.Net.Http
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Llm.OpenAI
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// Scripted-transport coverage for every preset: the real OpenAI SDK pipeline
// (endpoint, auth, timeout, disabled retries) answers canned synthetic
// payloads through a scripted HttpMessageHandler. No keys in the repo (every
// test uses a canned key), no live network calls.

// ──────────────────────────────────────────────────────────────────────────
// Canned wire payloads

/// A canned chat-completions text response in OpenAI v1 shape.
let private textPayload (model: string) (content: string) =
    $"""{{
  "id": "chatcmpl-canned",
  "object": "chat.completion",
  "created": 1700000000,
  "model": "{model}",
  "choices": [
    {{
      "index": 0,
      "message": {{ "role": "assistant", "content": "{content}" }},
      "finish_reason": "stop"
    }}
  ],
  "usage": {{ "prompt_tokens": 3, "completion_tokens": 5, "total_tokens": 8 }}
}}"""

/// A canned chat-completions tool-call response in OpenAI v1 shape.
let private toolCallPayload (model: string) =
    $"""{{
  "id": "chatcmpl-canned",
  "object": "chat.completion",
  "created": 1700000000,
  "model": "{model}",
  "choices": [
    {{
      "index": 0,
      "message": {{
        "role": "assistant",
        "content": null,
        "tool_calls": [
          {{
            "id": "call-1",
            "type": "function",
            "function": {{ "name": "get_weather", "arguments": "{{\"city\":\"Oslo\"}}" }}
          }}
        ]
      }},
      "finish_reason": "tool_calls"
    }}
  ],
  "usage": {{ "prompt_tokens": 3, "completion_tokens": 5, "total_tokens": 8 }}
}}"""

/// One canned server-sent-events streaming chunk.
let private streamChunk (content: string) =
    $"data: {{\"id\":\"chatcmpl-canned\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"m\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{content}\"}}}}]}}\n\n"

/// A canned SSE streaming body spelling "hello" across two chunks.
let private streamPayload () =
    streamChunk "hel" + streamChunk "lo" + "data: [DONE]\n\n"

/// A canned JSON response message.
let private jsonResponse (body: string) =
    let response = new HttpResponseMessage(HttpStatusCode.OK)
    response.Content <- new StringContent(body, Encoding.UTF8, "application/json")
    response

/// A canned SSE streaming response message.
let private streamResponse (body: string) =
    let response = new HttpResponseMessage(HttpStatusCode.OK)
    response.Content <- new StringContent(body, Encoding.UTF8, "text/event-stream")
    response

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// Builds test options: the canned key, the preset endpoint, and the preset
/// default model.
let private testOptions (preset: Presets.Preset) =
    let options = OpenAICompatibleProviderOptions()
    options.ApiKey <- "canned-key"
    options.Endpoint <- preset.Endpoint
    options.DefaultModel <- preset.DefaultModel
    options.SupportsReasoning <- preset.Reasoning
    options

/// Builds the provider answering every request with the same responder.
let private scriptedProvider (preset: Presets.Preset) (respond: HttpRequestMessage -> HttpResponseMessage) =
    let handler = new ProviderErrorsTests.QueueHandler(respond)

    let provider =
        OpenAICompatibleProvider.CreateForTests(preset.Id, testOptions preset, handler)

    provider, handler

/// Reads the request URI, failing the test shape when unset.
let private requestUri (request: HttpRequestMessage) : Uri =
    match request.RequestUri with
    | null -> Uri("http://127.0.0.1:9/unset")
    | uri -> uri

/// Reads the bearer parameter, empty when the transport sent no auth.
let private authParameter (request: HttpRequestMessage) : string | null =
    match request.Headers.Authorization with
    | null -> null
    | auth -> auth.Parameter

/// The user history every client call sends.
let private userHistory () : IList<ChatMessage> =
    ResizeArray<ChatMessage>([| ChatMessage(ChatRole.User, "hi") |]) :> IList<ChatMessage>

/// Calls the provider client for the preset default model and returns the
/// text of the canned answer.
let private completeText (provider: OpenAICompatibleProvider) (preset: Presets.Preset) =
    let client =
        (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse($"{preset.Id}/{preset.DefaultModel}"), null)

    use _ = client :> IDisposable

    let response =
        client
            .GetResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    response.Text

// ──────────────────────────────────────────────────────────────────────────
// Per-preset scripted-transport coverage

[<Fact>]
let ``OpenAI preset answers canned text through the SDK pipeline`` () =
    let provider, handler =
        scriptedProvider Presets.OpenAI (fun _ -> jsonResponse (textPayload "gpt-4o-mini" "canned openai"))

    completeText provider Presets.OpenAI |> should equal "canned openai"

    let request = handler.Requests |> Seq.exactlyOne
    (requestUri request).Host |> should equal "api.openai.com"
    authParameter request |> should equal "canned-key"

[<Fact>]
let ``Anthropic preset answers canned text through the SDK pipeline`` () =
    let provider, handler =
        scriptedProvider Presets.Anthropic (fun _ -> jsonResponse (textPayload "claude-sonnet" "canned anthropic"))

    completeText provider Presets.Anthropic |> should equal "canned anthropic"

    let request = handler.Requests |> Seq.exactlyOne
    (requestUri request).Host |> should equal "api.anthropic.com"

[<Fact>]
let ``OllamaCloud preset answers canned text through the SDK pipeline`` () =
    let provider, handler =
        scriptedProvider Presets.OllamaCloud (fun _ -> jsonResponse (textPayload "llama3.1" "canned cloud"))

    completeText provider Presets.OllamaCloud |> should equal "canned cloud"

    let request = handler.Requests |> Seq.exactlyOne
    (requestUri request).Host |> should equal "ollama.com"

[<Fact>]
let ``Compatible preset covers a local Ollama base URL`` () =
    let preset = Presets.compatible "local" "http://127.0.0.1:11434/v1"
    let options = testOptions Presets.OllamaCloud
    options.Endpoint <- preset.Endpoint
    options.DefaultModel <- "llama3.1"

    let handler =
        new ProviderErrorsTests.QueueHandler(fun _ -> jsonResponse (textPayload "llama3.1" "canned local"))

    let provider = OpenAICompatibleProvider.CreateForTests(preset.Id, options, handler)

    let client =
        (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse("local/llama3.1"), null)

    use _ = client :> IDisposable

    let response =
        client
            .GetResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    response.Text |> should equal "canned local"

    let request = handler.Requests |> Seq.exactlyOne
    (requestUri request).Host |> should equal "127.0.0.1"
    (requestUri request).Port |> should equal 11434

[<Fact>]
let ``Usage flows from the canned payload`` () =
    let provider, _ =
        scriptedProvider Presets.OpenAI (fun _ -> jsonResponse (textPayload "gpt-4o-mini" "usage"))

    let client =
        (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse("openai/gpt-4o-mini"), null)

    use _ = client :> IDisposable

    let response =
        client
            .GetResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    match response.Usage with
    | null -> Assert.Fail("The canned payload carries usage.")
    | usage ->
        usage.InputTokenCount |> should equal (Nullable 3L)
        usage.OutputTokenCount |> should equal (Nullable 5L)

// ──────────────────────────────────────────────────────────────────────────
// Tool calls and streaming through the real pipeline

[<Fact>]
let ``Tool calls map through the SDK pipeline`` () =
    let provider, _ =
        scriptedProvider Presets.OpenAI (fun _ -> jsonResponse (toolCallPayload "gpt-4o-mini"))

    let client =
        (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse("openai/gpt-4o-mini"), null)

    use _ = client :> IDisposable

    let response =
        client
            .GetResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    let calls =
        response.Messages
        |> Seq.collect (fun message -> message.Contents)
        |> Seq.choose (fun content ->
            match content with
            | :? FunctionCallContent as call -> Some call
            | _ -> None)
        |> List.ofSeq

    calls.Length |> should equal 1
    calls[0].Name |> should equal "get_weather"
    calls[0].CallId |> should equal "call-1"

[<Fact>]
let ``Streaming chunks flow through the SDK pipeline`` () =
    let provider, _ =
        scriptedProvider Presets.OpenAI (fun _ -> streamResponse (streamPayload ()))

    let client =
        (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse("openai/gpt-4o-mini"), null)

    use _ = client :> IDisposable

    let enumerator =
        client
            .GetStreamingResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
            .GetAsyncEnumerator()

    let text = StringBuilder()
    let mutable running = true

    try
        while running do
            let hasNext = enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult()

            if hasNext then
                for content in enumerator.Current.Contents do
                    match content with
                    | :? TextContent as chunk -> text.Append(chunk.Text) |> ignore
                    | _ -> ()
            else
                running <- false
    finally
        enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult()

    text.ToString() |> should equal "hello"

// ──────────────────────────────────────────────────────────────────────────
// Error mapping, retries, timeout, keys

[<Fact>]
let ``Non-2xx surfaces ProviderException with status and Retry-After`` () =
    let failureResponse = new HttpResponseMessage(HttpStatusCode.TooManyRequests)
    failureResponse.Content <- new StringContent("""{"marker":"sk-canned-secret"}""")
    failureResponse.Headers.TryAddWithoutValidation("Retry-After", "2") |> ignore
    let provider, _ = scriptedProvider Presets.OpenAI (fun _ -> failureResponse)

    let client =
        (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse("openai/gpt-4o-mini"), null)

    use _ = client :> IDisposable

    let failure =
        Assert.Throws<ProviderException>(fun () ->
            client
                .GetResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore)

    failure.ProviderId |> should equal "openai"
    failure.Status |> should equal (Nullable 429)
    failure.RetryAfter |> should equal (Nullable(TimeSpan.FromSeconds 2.0))
    (failure.Message.Contains("sk-canned-secret")) |> should equal false

[<Fact>]
let ``Failed calls run exactly one HTTP attempt: the SDK never retries`` () =
    let failureResponse = new HttpResponseMessage(HttpStatusCode.InternalServerError)
    failureResponse.Content <- new StringContent("""{"error":"boom"}""")
    let provider, handler = scriptedProvider Presets.OpenAI (fun _ -> failureResponse)

    let client =
        (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse("openai/gpt-4o-mini"), null)

    use _ = client :> IDisposable

    Assert.Throws<ProviderException>(fun () ->
        client
            .GetResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
            .GetAwaiter()
            .GetResult()
        |> ignore)
    |> ignore

    handler.Calls |> should equal 1

[<Fact>]
let ``Configured timeout bounds a stalled transport`` () =
    let options = testOptions Presets.OpenAI
    options.Timeout <- TimeSpan.FromSeconds 1.0

    let stalled =
        { new HttpMessageHandler() with
            override _.SendAsync(_request: HttpRequestMessage, cancellationToken: CancellationToken) =
                task {
                    do! Task.Delay(TimeSpan.FromSeconds 60.0, cancellationToken)
                    return jsonResponse (textPayload "gpt-4o-mini" "too late")
                }
        }

    let provider = OpenAICompatibleProvider.CreateForTests("openai", options, stalled)

    let client =
        (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse("openai/gpt-4o-mini"), null)

    use _ = client :> IDisposable
    use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 20.0)
    let started = DateTimeOffset.UtcNow

    Assert.ThrowsAny<Exception>(fun () ->
        client
            .GetResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, timeout.Token)
            .GetAwaiter()
            .GetResult()
        |> ignore)
    |> ignore

    (DateTimeOffset.UtcNow - started)
    |> should be (lessThan (TimeSpan.FromSeconds 20.0))

[<Fact>]
let ``Per-call API key wins over the registered key`` () =
    let provider, handler =
        scriptedProvider Presets.OpenAI (fun _ -> jsonResponse (textPayload "gpt-4o-mini" "keyed"))

    let callOptions = LlmProviderOptions()
    callOptions.ApiKey <- "call-key"

    let client =
        (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse("openai/gpt-4o-mini"), callOptions)

    use _ = client :> IDisposable

    client
        .GetResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
        .GetAwaiter()
        .GetResult()
    |> ignore

    let request = handler.Requests |> Seq.exactlyOne
    authParameter request |> should equal "call-key"

[<Fact>]
let ``Missing API key fails fast with ProviderException`` () =
    let options = testOptions Presets.OpenAI
    options.ApiKey <- Unchecked.defaultof<string>

    let handler =
        new ProviderErrorsTests.QueueHandler(fun _ -> jsonResponse (textPayload "gpt-4o-mini" "unreached"))

    let provider = OpenAICompatibleProvider.CreateForTests("openai", options, handler)

    let failure =
        Assert.Throws<ProviderException>(fun () ->
            (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse("openai/gpt-4o-mini"), null)
            |> ignore)

    failure.ProviderId |> should equal "openai"
    failure.Status.HasValue |> should equal false
    handler.Calls |> should equal 0

[<Fact>]
let ``Reference naming another provider is rejected`` () =
    let provider, _ =
        scriptedProvider Presets.OpenAI (fun _ -> jsonResponse (textPayload "gpt-4o-mini" "unreached"))

    Assert.Throws<ArgumentException>(fun () ->
        (provider :> ILlmProvider).CreateChatClient(ModelReference.Parse("anthropic/claude-sonnet"), null)
        |> ignore)
    |> ignore

// ──────────────────────────────────────────────────────────────────────────
// Identity and capabilities

[<Fact>]
let ``OpenAI provider reports its identity and capabilities`` () =
    let provider, _ =
        scriptedProvider Presets.OpenAI (fun _ -> jsonResponse (textPayload "gpt-4o-mini" "x"))

    let typed = provider :> ILlmProvider

    typed.Id |> should equal "openai"
    typed.DefaultModel |> should equal "gpt-4o-mini"
    typed.Capabilities.Streaming |> should equal true
    typed.Capabilities.ToolCalling |> should equal true
    typed.Capabilities.Reasoning |> should equal true

[<Fact>]
let ``OllamaCloud provider reports no reasoning`` () =
    let provider, _ =
        scriptedProvider Presets.OllamaCloud (fun _ -> jsonResponse (textPayload "llama3.1" "x"))

    (provider :> ILlmProvider).Capabilities.Reasoning |> should equal false

// ──────────────────────────────────────────────────────────────────────────
// Streaming and tool calls through the error-mapping wrapper

/// Drives the error-mapping wrapper over a scripted client: streaming and
/// tool-call shapes pass through untouched, failures surface as-is.
[<Fact>]
let ``Wrapper passes streaming and tool calls through ScriptedChatClient`` () =
    let scripted =
        new ScriptedChatClient(
            ResizeArray<ScriptStep>(
                [|
                    ScriptStep.ToolCall("call-9", "get_weather")
                    ScriptStep.Stream(
                        ResizeArray<AIContent>(
                            [|
                                TextContent("hel") :> AIContent
                                TextContent("lo") :> AIContent
                            |]
                        )
                    )
                |]
            )
        )

    let client = new ErrorMappingChatClient(scripted, "openai", null)

    use _ = client :> IDisposable

    let toolResponse =
        (client :> IChatClient)
            .GetResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    let calls =
        toolResponse.Messages
        |> Seq.collect (fun message -> message.Contents)
        |> Seq.choose (fun content ->
            match content with
            | :? FunctionCallContent as call -> Some call
            | _ -> None)
        |> List.ofSeq

    calls.Length |> should equal 1
    calls[0].Name |> should equal "get_weather"

    let enumerator =
        (client :> IChatClient)
            .GetStreamingResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
            .GetAsyncEnumerator()

    let text = StringBuilder()
    let mutable running = true

    try
        while running do
            let hasNext = enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult()

            if hasNext then
                for content in enumerator.Current.Contents do
                    match content with
                    | :? TextContent as chunk -> text.Append(chunk.Text) |> ignore
                    | _ -> ()
            else
                running <- false
    finally
        enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult()

    text.ToString() |> should equal "hello"

[<Fact>]
let ``Wrapper lets provider failures and aborts through untouched`` () =
    let failure =
        ProviderException("openai", Nullable 429, Nullable(TimeSpan.FromSeconds 1.0), "canned")

    let scripted =
        new ScriptedChatClient(ResizeArray<ScriptStep>([| ScriptStep.Failure(failure) |]))

    let client = new ErrorMappingChatClient(scripted, "openai", null)

    use _ = client :> IDisposable

    let raised =
        Assert.Throws<ProviderException>(fun () ->
            (client :> IChatClient)
                .GetResponseAsync(userHistory (), Unchecked.defaultof<ChatOptions>, CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore)

    Object.ReferenceEquals(raised, failure) |> should equal true
