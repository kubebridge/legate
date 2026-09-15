// SPDX-License-Identifier: Apache-2.0
namespace Legate.Llm.OpenAI

open System

// Per-provider settings for the shared OpenAI-compatible transport, plus the
// per-preset defaults the Add* registration extensions bind over. A host
// registers a preset bound from Legate:Llm:Providers:<Name> (ApiKey,
// Endpoint, DefaultModel, Timeout, SupportsReasoning); only the network
// timeout is configurable on the transport itself. SDK retries are disabled
// by construction: the coordinator in Legate owns retry, backoff, and
// cooldown, so a preset never retries under it. The provider timeout must
// stay below the coordinator's RequestTimeoutSeconds (default 120 s) so the
// coordinator deadline, not the transport, decides the turn outcome; the
// 100 s default keeps that ordering unless the host raises it deliberately.

/// Per-provider settings for the OpenAI-compatible transport: the API key
/// plus the endpoint, default model, network timeout, and reasoning flag.
/// Bound from <c>Legate:Llm:Providers:&lt;Name&gt;</c>; mutable so hosts can
/// set properties before registering. The API key never travels in messages,
/// diagnostics, or logs.
/// <example>
/// Bound from configuration:
/// <code>
/// "Legate": { "Llm": { "Providers": { "openai": { "ApiKey": "..." } } } }
/// </code>
/// </example>
type OpenAICompatibleProviderOptions() =
    inherit Legate.LlmProviderOptions()

    /// The OpenAI-compatible base URL the transport posts chat completions
    /// to, for example <c>https://api.openai.com/v1</c> or a local Ollama
    /// endpoint such as <c>http://127.0.0.1:11434/v1</c>. Must be an
    /// absolute URI. Presets supply the default; hosts override it only for
    /// proxies or self-hosted gateways.
    member val Endpoint: string | null = null with get, set

    /// The model the provider serves when a reference names only the
    /// provider. Presets supply the default; the compatible preset takes it
    /// from configuration or the <c>configure</c> callback instead.
    member val DefaultModel: string | null = null with get, set

    /// How long the transport waits for one provider round trip before the
    /// SDK cancels it. Default 100 s, deliberately below the coordinator's
    /// <c>RequestTimeoutSeconds</c> default of 120 s so the coordinator
    /// deadline owns the turn outcome. Must stay positive.
    member val Timeout: TimeSpan = TimeSpan.FromSeconds 100.0 with get, set

    /// Whether the provider family serves reasoning models. Default false;
    /// the OpenAI and Anthropic presets set it. Advisory only: it feeds the
    /// provider's reported capabilities, never request shaping.
    member val SupportsReasoning: bool = false with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let mutable endpoint = Unchecked.defaultof<Uri>

        if String.IsNullOrWhiteSpace this.Endpoint then
            "Endpoint must be a non-empty absolute URI."
        elif not (Uri.TryCreate(this.Endpoint, UriKind.Absolute, &endpoint)) then
            "Endpoint must be a non-empty absolute URI."
        elif String.IsNullOrWhiteSpace this.DefaultModel then
            "DefaultModel must be a non-empty string."
        elif this.Timeout <= TimeSpan.Zero then
            "Timeout must be positive."
        else
            null

/// The per-preset defaults one shared transport serves. Presets carry
/// endpoint and model defaults only; there is no Anthropic-native SDK and no
/// separate local-Ollama preset (the compatible preset covers any base URL,
/// including a local Ollama endpoint).
module internal Presets =

    /// One preset's endpoint and model defaults plus its capability flag.
    type Preset =
        {
            /// The provider id, matching the <c>Legate:Llm:Providers</c> key.
            Id: string
            /// The OpenAI-compatible base URL default.
            Endpoint: string
            /// The default model default.
            DefaultModel: string
            /// Whether the family serves reasoning models.
            Reasoning: bool
        }

    /// The OpenAI preset: the OpenAI v1 endpoint serving gpt-4o-mini by
    /// default.
    let OpenAI =
        {
            Id = "openai"
            Endpoint = "https://api.openai.com/v1"
            DefaultModel = "gpt-4o-mini"
            Reasoning = true
        }

    /// The Anthropic preset: the Anthropic OpenAI-compatible v1 endpoint
    /// serving claude-sonnet by default. Compatible-shape payloads only;
    /// there is no Anthropic-native SDK in this package.
    let Anthropic =
        {
            Id = "anthropic"
            Endpoint = "https://api.anthropic.com/v1/"
            DefaultModel = "claude-sonnet"
            Reasoning = true
        }

    /// The Ollama Cloud preset: the hosted Ollama OpenAI-compatible v1
    /// endpoint serving llama3.1 by default. Local Ollama goes through the
    /// compatible preset with a loopback base URL instead.
    let OllamaCloud =
        {
            Id = "ollamacloud"
            Endpoint = "https://ollama.com/v1"
            DefaultModel = "llama3.1"
            Reasoning = false
        }

    /// Builds the compatible preset for a caller-supplied provider name and
    /// base URL, covering any OpenAI-compatible endpoint including local
    /// Ollama (use <c>127.0.0.1</c>, never <c>localhost</c>).
    /// <param name="providerName">The provider id and <c>Legate:Llm:Providers</c> key.</param>
    /// <param name="baseUrl">The OpenAI-compatible base URL.</param>
    /// <returns>The preset the registration binds over.</returns>
    /// <exception cref="T:System.ArgumentException">The name is blank or carries whitespace or slashes, or the URL is blank.</exception>
    let compatible (providerName: string) (baseUrl: string) : Preset =
        if String.IsNullOrWhiteSpace providerName then
            raise (ArgumentException("The compatible provider name must be a non-empty string.", "providerName"))

        let name = providerName.Trim().ToLowerInvariant()

        if name |> Seq.exists Char.IsWhiteSpace || name.IndexOf('/') >= 0 then
            raise (
                ArgumentException(
                    "The compatible provider name must be a single model-reference segment: no whitespace or slashes.",
                    "providerName"
                )
            )

        if String.IsNullOrWhiteSpace baseUrl then
            raise (ArgumentException("The compatible base URL must be a non-empty absolute URI.", "baseUrl"))

        {
            Id = name
            Endpoint = baseUrl.Trim()
            DefaultModel = ""
            Reasoning = false
        }
