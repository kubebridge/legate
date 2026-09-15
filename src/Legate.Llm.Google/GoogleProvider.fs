// SPDX-License-Identifier: Apache-2.0
namespace Legate.Llm

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.AI

// Error mapping and the chat-client wrapper for the Google provider. The
// SDK's MEAI adapter (Client.AsIChatClient) owns the wire format: thought
// parts surface as TextReasoningContent, every part travels on
// RawRepresentation, and thought signatures ride the ProtectedData marker the
// adapter reattaches on follow-up calls, so no thin adapter is needed. What
// the provider adds is failure shaping: the SDK throws ClientError (4xx) and
// ServerError (5xx), both HttpRequestException subclasses the coordinator
// would otherwise mistake for retryable network failures, so the wrapper
// maps exactly those two to ProviderException with the backend status and a
// secret-free message. The SDK drops response headers on its errors, so no
// Retry-After ever reaches the mapping and RetryAfter stays unset; the
// coordinator still honors the 429 through its configured cooldown. Genuine
// network failures (plain HttpRequestException, IO, sockets, timeouts) and
// cancellations propagate untouched for the coordinator's own wrap and
// retry. SDK retries stay off (Attempts = 1 at construction): the
// coordinator owns retry.
module internal GoogleErrors =

    /// The provider id every mapped failure carries.
    let providerId = "google"

    /// Maps one SDK failure to the exception the caller sees: ClientError
    /// and ServerError become ProviderException with the backend status and
    /// no Retry-After (the SDK drops response headers); anything else
    /// passes through untouched, including cancellations and network
    /// failures the coordinator wraps and retries itself.
    /// <param name="failure">The failure to map.</param>
    /// <returns>The ProviderException for SDK HTTP failures, otherwise the original failure.</returns>
    let mapError (failure: Exception) : Exception =
        match failure with
        | :? Google.GenAI.ClientError as clientError ->
            let message =
                if String.IsNullOrWhiteSpace clientError.Status then
                    sprintf "The '%s' provider call failed with status %d." providerId clientError.StatusCode
                else
                    sprintf
                        "The '%s' provider call failed with status %d (%s)."
                        providerId
                        clientError.StatusCode
                        clientError.Status

            ProviderException(providerId, Nullable clientError.StatusCode, Nullable<TimeSpan>(), message) :> Exception
        | :? Google.GenAI.ServerError as serverError ->
            let message =
                if String.IsNullOrWhiteSpace serverError.Status then
                    sprintf "The '%s' provider call failed with status %d." providerId serverError.StatusCode
                else
                    sprintf
                        "The '%s' provider call failed with status %d (%s)."
                        providerId
                        serverError.StatusCode
                        serverError.Status

            ProviderException(providerId, Nullable serverError.StatusCode, Nullable<TimeSpan>(), message) :> Exception
        | _ -> failure

/// The IChatClient the Google provider hands out: the SDK's MEAI adapter
/// with failure shaping. Response and streaming shapes pass through
/// untouched (thought signatures survive by reference through the adapter's
/// RawRepresentation and ProtectedData round-trip); only failures are
/// rewritten through <see cref="M:Legate.Llm.GoogleErrors.mapError(System.Exception)" />.
type internal GoogleChatClient(inner: IChatClient, sdkClient: IDisposable) =

    do
        ArgumentNullException.ThrowIfNull(inner)
        ArgumentNullException.ThrowIfNull(sdkClient)

    interface IChatClient with
        member _.GetResponseAsync(history, options, cancellationToken) =
            task {
                try
                    return! inner.GetResponseAsync(history, options, cancellationToken)
                with failure ->
                    return raise (GoogleErrors.mapError failure)
            }

        member _.GetStreamingResponseAsync(history, options, cancellationToken) =
            let updates =
                try
                    inner.GetStreamingResponseAsync(history, options, cancellationToken)
                with failure ->
                    raise (GoogleErrors.mapError failure)

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
                                        return raise (GoogleErrors.mapError failure)
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

/// The Google Gemini LLM provider: builds MEAI chat clients over the Google
/// GenAI SDK for the configured model. The provider id is
/// <c>google</c>; capabilities advertise streaming, reasoning, and tool
/// calling. Client construction performs no network I/O: the SDK client only
/// captures the API key, a single-attempt retry policy (the coordinator
/// owns retry), and the configured timeout.
[<Sealed>]
type GoogleProvider(configured: GoogleLlmOptions) =

    do ArgumentNullException.ThrowIfNull(configured)

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
                configured.ApiKey

        match candidate with
        | null ->
            raise (
                ArgumentException(
                    "No API key is configured for the 'google' provider: set GoogleLlmOptions.ApiKey (Legate:Llm:Providers:Google:ApiKey) or pass LlmProviderOptions with an API key.",
                    nameof callOptions
                )
            )
        | key when String.IsNullOrWhiteSpace key ->
            raise (
                ArgumentException(
                    "No API key is configured for the 'google' provider: set GoogleLlmOptions.ApiKey (Legate:Llm:Providers:Google:ApiKey) or pass LlmProviderOptions with an API key.",
                    nameof callOptions
                )
            )
        | key -> key

    /// The provider id, matching the model reference segment:
    /// <c>google</c>.
    member _.Id = GoogleErrors.providerId

    /// The model the provider serves when a reference names the provider:
    /// the configured model, or the stable Flash constant when the
    /// configuration leaves it blank.
    member _.DefaultModel =
        if String.IsNullOrWhiteSpace configured.Model then
            GoogleLlmOptions.DefaultModel
        else
            configured.Model

    /// What the provider supports: streaming, reasoning (thought parts
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
    /// <param name="model">The model to bind; the reference's provider segment must equal <c>google</c>.</param>
    /// <param name="options">Provider settings (API key), or null to use the configured ones.</param>
    /// <returns>A chat client for the model.</returns>
    /// <exception cref="T:System.ArgumentException">The reference names another provider, or no API key is configured.</exception>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The configured timeout is not positive.</exception>
    member this.CreateChatClient(model: ModelReference, options: LlmProviderOptions | null) : IChatClient =
        if model.Provider <> GoogleErrors.providerId then
            raise (
                ArgumentException(
                    sprintf
                        "The 'google' provider cannot serve model reference '%s': the provider segment must be 'google'."
                        model.Value,
                    nameof model
                )
            )

        let apiKey = this.ResolveApiKey options

        if configured.Timeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof configured, "GoogleLlmOptions.Timeout must be positive."))

        if configured.Timeout.TotalMilliseconds > float Int32.MaxValue then
            raise (
                ArgumentOutOfRangeException(
                    nameof configured,
                    "GoogleLlmOptions.Timeout exceeds the SDK's millisecond timeout range."
                )
            )

        let retryOptions = Google.GenAI.Types.HttpRetryOptions(Attempts = Nullable 1)

        let httpOptions = Google.GenAI.Types.HttpOptions(RetryOptions = retryOptions)
        httpOptions.Timeout <- Nullable(int configured.Timeout.TotalMilliseconds)

        let sdkClient = new Google.GenAI.Client(apiKey = apiKey, httpOptions = httpOptions)
        let inner = sdkClient.AsIChatClient(model.Model)
        new GoogleChatClient(inner, sdkClient) :> IChatClient

    interface ILlmProvider with
        member this.Id = this.Id
        member this.DefaultModel = this.DefaultModel
        member this.Capabilities = this.Capabilities

        member this.CreateChatClient(model: ModelReference, options: LlmProviderOptions | null) =
            this.CreateChatClient(model, options)
