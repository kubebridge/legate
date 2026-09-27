// SPDX-License-Identifier: Apache-2.0
namespace Legate.Llm

open System
open System.Collections.Generic
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open Anthropic
open Legate
open Microsoft.Extensions.AI

// Error mapping and the chat-client wrapper for the native Anthropic
// provider. The SDK's own MEAI adapter (AnthropicClientExtensions
// .AsIChatClient) owns the wire format: thinking blocks surface as
// TextReasoningContent, caller-applied prompt-cache breakpoints pass
// through, and tool shapes travel on the adapter's round-trip, so no thin
// adapter is needed. What the provider adds is failure shaping: the SDK
// throws AnthropicApiException carrying the backend status, which the
// coordinator would otherwise mistake for an unshaped failure, so the
// wrapper maps exactly that to ProviderException with the backend status
// and a secret-free message. The exception carries no response headers, so
// no Retry-After ever reaches the mapping and RetryAfter stays unset; the
// coordinator still honors the 429 through its configured cooldown. The
// backend body never enters messages. Genuine network failures
// (AnthropicIOException, plain HttpRequestException, IO, sockets, timeouts)
// and cancellations propagate untouched for the coordinator's own wrap and
// retry. SDK retries stay off (MaxRetries zero at construction): the
// coordinator owns retry.
module internal AnthropicErrors =

    /// The provider id every mapped failure carries.
    let providerId = "anthropic"

    /// Maps one SDK failure to the exception the caller sees:
    /// AnthropicApiException becomes ProviderException with the backend
    /// status and no Retry-After (the SDK drops response headers); anything
    /// else passes through untouched, including already-shaped provider
    /// failures, cancellations, and network failures the coordinator wraps
    /// and retries itself.
    /// <param name="failure">The failure to map.</param>
    /// <returns>The ProviderException for SDK HTTP failures, otherwise the original failure.</returns>
    let mapError (failure: Exception) : Exception =
        match failure with
        | :? Legate.ProviderException -> failure
        | :? Anthropic.Exceptions.AnthropicApiException as apiFailure ->
            let status = int apiFailure.StatusCode

            let message =
                sprintf
                    "The '%s' provider call failed with status %d (%s)."
                    providerId
                    status
                    (apiFailure.StatusCode.ToString())

            ProviderException(providerId, Nullable status, Nullable<TimeSpan>(), message) :> Exception
        | _ -> failure

/// The IChatClient the Anthropic provider hands out: the SDK's own MEAI
/// adapter with failure shaping. Response and streaming shapes pass through
/// untouched (thinking blocks survive as TextReasoningContent); only
/// failures are rewritten through
/// <see cref="M:Legate.Llm.AnthropicErrors.mapError(System.Exception)" />.
type internal AnthropicChatClient(inner: IChatClient, sdkClient: IDisposable) =

    do
        ArgumentNullException.ThrowIfNull(inner)
        ArgumentNullException.ThrowIfNull(sdkClient)

    interface IChatClient with
        member _.GetResponseAsync(history, options, cancellationToken) =
            task {
                try
                    return! inner.GetResponseAsync(history, options, cancellationToken)
                with failure ->
                    return raise (AnthropicErrors.mapError failure)
            }

        member _.GetStreamingResponseAsync(history, options, cancellationToken) =
            let updates =
                try
                    inner.GetStreamingResponseAsync(history, options, cancellationToken)
                with failure ->
                    raise (AnthropicErrors.mapError failure)

            { new IAsyncEnumerable<ChatResponseUpdate> with
                member _.GetAsyncEnumerator(_) =
                    let enumerator = updates.GetAsyncEnumerator(cancellationToken)

                    { new IAsyncEnumerator<ChatResponseUpdate> with
                        member _.Current = enumerator.Current

                        member _.MoveNextAsync() =
                            ValueTask<bool>(
                                task {
                                    try
                                        return! enumerator.MoveNextAsync()
                                    with failure ->
                                        return raise (AnthropicErrors.mapError failure)
                                }
                            )

                        member _.DisposeAsync() = enumerator.DisposeAsync()
                    }
            }

        member _.GetService(serviceType, serviceKey) =
            inner.GetService(serviceType, serviceKey)

        member _.Dispose() =
            inner.Dispose()
            sdkClient.Dispose()

/// The native Anthropic LLM provider: builds MEAI chat clients over the
/// official Anthropic SDK for the configured model. The provider id is
/// <c>anthropic</c>; capabilities advertise streaming, reasoning, and tool
/// calling. Client construction performs no network I/O: the SDK client
/// only captures the API key, a zero-retry policy (the coordinator owns
/// retry), and the configured timeout.
[<Sealed>]
type AnthropicProvider(configured: AnthropicLlmOptions) =

    do ArgumentNullException.ThrowIfNull(configured)

    do
        match configured.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid 'anthropic' provider options: %s{violation}"))

    // Validated snapshot: Validate rejected nulls and blanks above.
    let snapshotModel: string = configured.Model

    let snapshotTimeout = configured.Timeout
    let snapshotThinking = configured.ThinkingMode
    let snapshotMaxOutput = configured.MaxOutputTokens

    let snapshotKey: string | null = configured.ApiKey
    let mutable testHandler: HttpMessageHandler | null = null

    /// Creates the provider with a scripted inner HTTP handler instead of
    /// network I/O. Internal: scripted-transport tests exercise the real
    /// SDK pipeline (auth, timeout, retry policy) against canned payloads
    /// with no keys and no live calls.
    /// <param name="options">The model, timeout, thinking, and key settings.</param>
    /// <param name="handler">The scripted handler answering requests.</param>
    /// <returns>The provider answering through the scripted handler.</returns>
    static member internal CreateForTests
        (options: AnthropicLlmOptions, handler: HttpMessageHandler)
        : AnthropicProvider =
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(handler)
        let provider = AnthropicProvider(options)
        provider.TestHandler <- handler
        provider

    /// The scripted inner handler, or null for real network I/O. Internal
    /// test seam; production instances always read null.
    member internal _.TestHandler
        with get (): HttpMessageHandler | null = testHandler
        and set (value: HttpMessageHandler | null) = testHandler <- value

    // Resolves the API key for one client: the per-call options win when
    // they carry a key (the coordinator resolves keys per tenant and passes
    // them here), otherwise the configured options apply. Refuses ambient
    // SDK environment-variable fallback: a missing key fails here with a
    // message naming the setting, never a secret.
    member private _.ResolveApiKey(callOptions: LlmProviderOptions | null) : string =
        let callKey: string | null =
            match callOptions with
            | null -> null
            | present -> present.ApiKey

        let candidate: string | null =
            if not (String.IsNullOrWhiteSpace callKey) then
                callKey
            else
                snapshotKey

        match candidate with
        | null ->
            raise (
                Legate.ProviderException(
                    AnthropicErrors.providerId,
                    Nullable<int>(),
                    Nullable<TimeSpan>(),
                    $"No API key is configured for provider '{AnthropicErrors.providerId}': set Legate:Llm:Providers:{AnthropicErrors.providerId}:ApiKey."
                )
            )
        | key when String.IsNullOrWhiteSpace key ->
            raise (
                Legate.ProviderException(
                    AnthropicErrors.providerId,
                    Nullable<int>(),
                    Nullable<TimeSpan>(),
                    $"No API key is configured for provider '{AnthropicErrors.providerId}': set Legate:Llm:Providers:{AnthropicErrors.providerId}:ApiKey."
                )
            )
        | key -> key

    /// Builds the SDK options for one client: the explicit API key (never
    /// ambient), retries disabled, the configured network timeout only,
    /// and the scripted transport underneath when tests installed one.
    /// <param name="apiKey">The explicit API key the client sends.</param>
    /// <returns>The SDK options the chat client constructs with.</returns>
    member private _.BuildSdkOptions(apiKey: string) : Anthropic.Core.ClientOptions =
        let mutable sdkOptions = Anthropic.Core.ClientOptions()
        sdkOptions.ApiKey <- apiKey
        sdkOptions.MaxRetries <- Nullable 0
        sdkOptions.Timeout <- Nullable snapshotTimeout

        match testHandler with
        | null -> ()
        | handler ->
            let transport = new HttpClient(handler, false)
            transport.Timeout <- Timeout.InfiniteTimeSpan
            sdkOptions.HttpClient <- transport

        sdkOptions

    /// The provider id, matching the model reference segment:
    /// <c>anthropic</c>.
    member _.Id = AnthropicErrors.providerId

    /// The model the provider serves when a reference names the provider:
    /// the configured model.
    member _.DefaultModel = snapshotModel

    /// What the provider supports: streaming, reasoning (thinking blocks
    /// surface as TextReasoningContent), and tool calling.
    member _.Capabilities: LlmCapabilities =
        {
            Streaming = true
            Reasoning = true
            ToolCalling = true
        }

    /// Creates a chat client bound to the model. Synchronous: SDK
    /// construction captures settings and never performs network I/O; calls
    /// through the client stay asynchronous.
    /// <param name="model">The model to bind; the reference's provider segment must equal <c>anthropic</c>.</param>
    /// <param name="options">Provider settings (API key), or null to use the configured ones.</param>
    /// <returns>A chat client for the model.</returns>
    /// <exception cref="T:System.ArgumentException">The reference names another provider.</exception>
    /// <exception cref="T:Legate.ProviderException">Neither key source has a key.</exception>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The configured timeout is not positive.</exception>
    member this.CreateChatClient(model: ModelReference, options: LlmProviderOptions | null) : IChatClient =
        if model.Provider <> AnthropicErrors.providerId then
            raise (
                ArgumentException(
                    sprintf
                        "The 'anthropic' provider cannot serve model reference '%s': the provider segment must be 'anthropic'."
                        model.Value,
                    nameof model
                )
            )

        let apiKey = this.ResolveApiKey options

        if snapshotTimeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof configured, "AnthropicLlmOptions.Timeout must be positive."))

        let sdkClient = new AnthropicClient(this.BuildSdkOptions apiKey)

        let inner =
            AnthropicClientExtensions.AsIChatClient(sdkClient, model.Model, snapshotMaxOutput, snapshotThinking)

        new AnthropicChatClient(inner, sdkClient) :> IChatClient

    interface ILlmProvider with
        member this.Id = this.Id
        member this.DefaultModel = this.DefaultModel
        member this.Capabilities = this.Capabilities

        member this.CreateChatClient(model: ModelReference, options: LlmProviderOptions | null) =
            this.CreateChatClient(model, options)
