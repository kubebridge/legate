// SPDX-License-Identifier: Apache-2.0
namespace Legate.Llm.OpenAI

open System
open System.ClientModel
open System.ClientModel.Primitives
open System.Net.Http
open System.Threading
open Microsoft.Extensions.AI
open OpenAI
open OpenAI.Chat

// One ILlmProvider over the shared OpenAI-compatible transport. Each
// instance serves a single provider id: it builds OpenAI SDK ChatClients
// (one per model reference) surfaced as IChatClient through the
// Microsoft.Extensions.AI.OpenAI adapter, wrapped in the error-mapping
// decorator. SDK retries are disabled by construction (MaxRetries zero);
// only the configured network timeout applies, and the coordinator above
// owns retry, backoff, cooldown, and deadlines. The per-call ApiKey wins
// when set (the coordinator resolves keys per tenant per call); otherwise
// the registered options key applies. The transport HttpClient is shared
// across the instance's clients: credentials travel per request from the
// SDK credential, never from the shared client.

/// An LLM provider serving one provider id over an OpenAI-compatible
/// endpoint. Constructed by the <c>AddOpenAI</c>, <c>AddAnthropic</c>,
/// <c>AddOllamaCloud</c>, and <c>AddOpenAICompatible</c> registration
/// extensions; hosts implement no members themselves.
/// <param name="providerId">The canonical lowercase provider id, for example <c>openai</c>.</param>
/// <param name="options">The endpoint, default model, timeout, and key settings.</param>
[<Sealed>]
type OpenAICompatibleProvider(providerId: string, options: OpenAICompatibleProviderOptions) =

    do
        if String.IsNullOrWhiteSpace providerId then
            raise (ArgumentException("The provider id must be a non-empty string.", "providerId"))

        ArgumentNullException.ThrowIfNull(options)

    let id = providerId.Trim().ToLowerInvariant()

    do
        if id |> Seq.exists Char.IsWhiteSpace || id.IndexOf('/') >= 0 then
            raise (
                ArgumentException(
                    "The provider id must be a single model-reference segment: no whitespace or slashes.",
                    "providerId"
                )
            )

        match options.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid '{id}' provider options: %s{violation}"))

    // Validated snapshots: Validate rejected nulls above, so the matches
    // below only satisfy the nullable-reference analysis.
    let snapshotEndpoint: string =
        match options.Endpoint with
        | null -> ""
        | value -> value

    let snapshotModel: string =
        match options.DefaultModel with
        | null -> ""
        | value -> value

    let snapshotTimeout = options.Timeout
    let snapshotReasoning = options.SupportsReasoning

    let snapshotKey: string | null = options.ApiKey
    let mutable testHandler: HttpMessageHandler | null = null

    /// Creates the provider with a scripted inner HTTP handler instead of
    /// network I/O. Internal: scripted-transport tests exercise the real
    /// SDK pipeline (endpoint, auth, timeout, retry policy) against canned
    /// payloads with no keys and no live calls.
    /// <param name="providerId">The canonical lowercase provider id.</param>
    /// <param name="options">The endpoint, default model, timeout, and key settings.</param>
    /// <param name="handler">The scripted handler answering requests.</param>
    /// <returns>The provider answering through the scripted handler.</returns>
    static member internal CreateForTests
        (providerId: string, options: OpenAICompatibleProviderOptions, handler: HttpMessageHandler)
        : OpenAICompatibleProvider =
        ArgumentNullException.ThrowIfNull(handler)
        let provider = OpenAICompatibleProvider(providerId, options)
        provider.TestHandler <- handler
        provider

    /// The scripted inner handler, or null for real network I/O. Internal
    /// test seam; production instances always read null.
    member internal _.TestHandler
        with get (): HttpMessageHandler | null = testHandler
        and set (value: HttpMessageHandler | null) = testHandler <- value

    /// Builds the SDK options for one client: the configured endpoint, the
    /// configured network timeout only, retries disabled, and the shared
    /// error-mapping transport underneath.
    /// <param name="transport">The shared transport client.</param>
    /// <returns>The SDK options the chat client constructs with.</returns>
    member private _.BuildSdkOptions(transport: HttpClient) : OpenAIClientOptions =
        let sdkOptions = OpenAIClientOptions()
        sdkOptions.Endpoint <- Uri snapshotEndpoint
        sdkOptions.NetworkTimeout <- snapshotTimeout
        sdkOptions.RetryPolicy <- ClientRetryPolicy(0)
        sdkOptions.Transport <- new HttpClientPipelineTransport(transport)
        sdkOptions

    /// Builds the shared transport client: the error-mapping handler over
    /// the scripted test handler or a fresh socket handler, with the
    /// HttpClient's own timeout disabled so only the SDK NetworkTimeout
    /// governs a round trip.
    /// <returns>The shared transport client.</returns>
    member private _.BuildTransport() : HttpClient =
        let innerHandler: HttpMessageHandler =
            match testHandler with
            | null -> new HttpClientHandler() :> HttpMessageHandler
            | handler -> handler

        let client = new HttpClient(new ProviderErrorHandler(id, innerHandler), true)
        client.Timeout <- Timeout.InfiniteTimeSpan
        client

    /// The provider id, matching the model reference segment, for example
    /// <c>openai</c>.
    member _.ProviderId = id

    interface Legate.ILlmProvider with
        /// The provider id, matching the segment of a
        /// <see cref="T:Legate.ModelReference" />, for example
        /// <c>openai</c>.
        member _.Id = id

        /// The model the provider serves when a reference names only the
        /// provider, taken from the registered options.
        member _.DefaultModel = snapshotModel

        /// What the provider supports: streaming and tool calling for every
        /// preset, reasoning per the preset's family flag.
        member _.Capabilities: Legate.LlmCapabilities =
            {
                Streaming = true
                Reasoning = snapshotReasoning
                ToolCalling = true
            }

        /// Creates a chat client bound to the model over the shared
        /// transport. Synchronous: SDK construction performs no network
        /// I/O; calls through the client stay asynchronous. The per-call
        /// API key wins when set (the coordinator resolves it per tenant);
        /// otherwise the registered options key applies.
        /// <param name="model">The model to bind; the reference's provider segment must equal <see cref="P:Legate.ILlmProvider.Id" />.</param>
        /// <param name="options">Provider settings (API key), or null to use the registered options.</param>
        /// <returns>A chat client for the model.</returns>
        /// <exception cref="T:System.ArgumentException">The reference names another provider.</exception>
        /// <exception cref="T:Legate.ProviderException">Neither key source has a key.</exception>
        member this.CreateChatClient
            (model: Legate.ModelReference, options: Legate.LlmProviderOptions | null)
            : IChatClient =
            if not (String.Equals(model.Provider, id, StringComparison.OrdinalIgnoreCase)) then
                raise (
                    ArgumentException(
                        $"The '{model.Value}' reference names provider '{model.Provider}', not '{id}'.",
                        "model"
                    )
                )

            let callKey: string | null =
                match options with
                | null -> null
                | opts -> opts.ApiKey

            let key: string | null =
                if String.IsNullOrWhiteSpace callKey then
                    snapshotKey
                else
                    callKey

            let apiKey: string =
                match key with
                | null ->
                    raise (
                        Legate.ProviderException(
                            id,
                            Nullable<int>(),
                            Nullable<TimeSpan>(),
                            $"No API key is configured for provider '{id}': set Legate:Llm:Providers:{id}:ApiKey."
                        )
                    )
                | value when String.IsNullOrWhiteSpace value ->
                    raise (
                        Legate.ProviderException(
                            id,
                            Nullable<int>(),
                            Nullable<TimeSpan>(),
                            $"No API key is configured for provider '{id}': set Legate:Llm:Providers:{id}:ApiKey."
                        )
                    )
                | value -> value

            let transport = this.BuildTransport()
            let sdkOptions = this.BuildSdkOptions(transport)

            let chatClient =
                new ChatClient(model.Model, new ApiKeyCredential(apiKey), sdkOptions)

            let adapted = chatClient.AsIChatClient()
            new ErrorMappingChatClient(adapted, id, transport) :> IChatClient
