// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ProviderErrorsTests

open System
open System.Collections.Generic
open System.Net
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Llm.OpenAI
open Xunit

// ──────────────────────────────────────────────────────────────────────────
// Retry-After parsing (internal, no network)

// The internal parser is visible through InternalsVisibleTo; the transport
// tests below cover the same cases black-box through HTTP headers.
[<Fact>]
let ``Retry-After seconds parse to a delay`` () =
    ProviderErrors.parseRetryAfter "2"
    |> should equal (Nullable(TimeSpan.FromSeconds 2.0))

[<Fact>]
let ``Retry-After zero parses to a zero delay`` () =
    ProviderErrors.parseRetryAfter "0" |> should equal (Nullable(TimeSpan.Zero))

[<Fact>]
let ``Retry-After HTTP date parses against now`` () =
    let retryAfter =
        ProviderErrors.parseRetryAfter (DateTimeOffset.UtcNow.AddSeconds(45.0).ToString("r"))

    retryAfter.HasValue |> should equal true
    retryAfter.Value.TotalSeconds |> should be (greaterThan 30.0)
    retryAfter.Value.TotalSeconds |> should be (lessThanOrEqualTo 45.0)

[<Fact>]
let ``Retry-After past date clamps to zero`` () =
    let retryAfter =
        ProviderErrors.parseRetryAfter (DateTimeOffset.UtcNow.AddSeconds(-30.0).ToString("r"))

    retryAfter |> should equal (Nullable(TimeSpan.Zero))

[<Theory>]
[<InlineData(null)>]
[<InlineData("")>]
[<InlineData("  ")>]
[<InlineData("garbage")>]
[<InlineData("-5")>]
let ``Retry-After absent or unparseable reads as null`` (raw: string) =
    ProviderErrors.parseRetryAfter raw |> should equal (Nullable<TimeSpan>())

// ──────────────────────────────────────────────────────────────────────────
// Transport handler mapping (scripted handler, no network)

/// A scripted inner handler: answers every request from a queue and records
/// what arrived. Canned payloads only; no keys, no live calls.
type QueueHandler(respond: HttpRequestMessage -> HttpResponseMessage) =
    inherit HttpMessageHandler()

    let requests = ResizeArray<HttpRequestMessage>()
    let mutable calls = 0

    /// How many requests ran through the handler.
    member _.Calls = calls

    /// Every request the handler saw, in order.
    member _.Requests: IReadOnlyList<HttpRequestMessage> =
        requests :> IReadOnlyList<HttpRequestMessage>

    override _.SendAsync(request: HttpRequestMessage, _: CancellationToken) =
        calls <- calls + 1
        requests.Add(request)

        try
            Task.FromResult(respond request)
        with failure ->
            Task.FromException<HttpResponseMessage>(failure)

/// Builds a canned JSON response with an optional Retry-After header. The
/// body plants a secret-shaped marker so tests can prove messages never
/// carry it.
let private cannedResponse (status: HttpStatusCode) (retryAfter: string | null) (body: string) =
    let response = new HttpResponseMessage(status)
    response.Content <- new StringContent(body)

    match retryAfter with
    | null -> ()
    | value -> response.Headers.TryAddWithoutValidation("Retry-After", value) |> ignore

    response

[<Fact>]
let ``Non-2xx maps to ProviderException with status and Retry-After seconds`` () =
    let handler =
        new QueueHandler(fun _ -> cannedResponse HttpStatusCode.TooManyRequests "2" """{"marker":"sk-canned-secret"}""")

    use client = new HttpClient(new ProviderErrorHandler("openai", handler), true)

    let failure =
        Assert
            .ThrowsAsync<ProviderException>(fun () -> client.GetAsync("http://127.0.0.1:9/v1/chat/completions"))
            .GetAwaiter()
            .GetResult()

    failure.ProviderId |> should equal "openai"
    failure.Status |> should equal (Nullable 429)
    failure.RetryAfter |> should equal (Nullable(TimeSpan.FromSeconds 2.0))
    (failure.Message.Contains("sk-canned-secret")) |> should equal false
    (failure.Message.Contains("openai")) |> should equal true
    (failure.Message.Contains("429")) |> should equal true

[<Fact>]
let ``Missing Retry-After reads as null`` () =
    let handler =
        new QueueHandler(fun _ -> cannedResponse HttpStatusCode.InternalServerError null """{"error":"boom"}""")

    use client = new HttpClient(new ProviderErrorHandler("anthropic", handler), true)

    let failure =
        Assert
            .ThrowsAsync<ProviderException>(fun () -> client.GetAsync("http://127.0.0.1:9/v1/chat/completions"))
            .GetAwaiter()
            .GetResult()

    failure.ProviderId |> should equal "anthropic"
    failure.Status |> should equal (Nullable 500)
    failure.RetryAfter.HasValue |> should equal false

[<Fact>]
let ``Garbage Retry-After reads as null`` () =
    let handler =
        new QueueHandler(fun _ -> cannedResponse HttpStatusCode.BadRequest "garbage" """{"error":"bad"}""")

    use client = new HttpClient(new ProviderErrorHandler("ollamacloud", handler), true)

    let failure =
        Assert
            .ThrowsAsync<ProviderException>(fun () -> client.GetAsync("http://127.0.0.1:9/v1/chat/completions"))
            .GetAwaiter()
            .GetResult()

    failure.Status |> should equal (Nullable 400)
    failure.RetryAfter.HasValue |> should equal false

[<Fact>]
let ``Success responses pass through untouched`` () =
    let handler =
        new QueueHandler(fun _ -> cannedResponse HttpStatusCode.OK null """{"ok":true}""")

    use client = new HttpClient(new ProviderErrorHandler("openai", handler), true)

    let response =
        client.GetAsync("http://127.0.0.1:9/v1/chat/completions").GetAwaiter().GetResult()

    response.IsSuccessStatusCode |> should equal true
    handler.Calls |> should equal 1
