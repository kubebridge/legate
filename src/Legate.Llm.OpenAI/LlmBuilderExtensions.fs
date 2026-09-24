// SPDX-License-Identifier: Apache-2.0
namespace Legate.Llm.OpenAI

open System
open System.Runtime.CompilerServices
open Microsoft.Extensions.Configuration

// Registration over LegateBuilder.Llm: AddOpenAI, AddAnthropicCompatible,
// AddOllamaCloud, and AddOpenAICompatible(name, baseUrl). Each preset binds
// its Legate:Llm:Providers:<Name> section over the preset defaults
// (endpoint, default model, timeout, reasoning flag), applies the optional
// configure callback, validates, and registers one provider instance. Use
// 127.0.0.1, never localhost, for loopback base URLs.

/// Binds one preset's <c>Legate:Llm:Providers:&lt;Name&gt;</c> section over
/// its defaults and registers the provider. Internal.
module internal Registration =

    /// Binds the preset section, applies the callback, validates, and
    /// registers the provider on the builder.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="preset">The preset defaults to bind over.</param>
    /// <param name="configure">Adjusts the bound options, or null to keep them.</param>
    /// <returns>The same builder, for chaining.</returns>
    let addPreset
        (
            builder: Legate.LlmBuilder,
            configuration: IConfiguration,
            preset: Presets.Preset,
            configure: Action<OpenAICompatibleProviderOptions> | null
        ) : Legate.LlmBuilder =
        ArgumentNullException.ThrowIfNull(builder)
        ArgumentNullException.ThrowIfNull(configuration)

        let options = OpenAICompatibleProviderOptions()
        options.Endpoint <- preset.Endpoint
        options.DefaultModel <- preset.DefaultModel
        options.SupportsReasoning <- preset.Reasoning

        configuration.GetSection($"Legate:Llm:Providers:{preset.Id}").Bind(options)

        match configure with
        | null -> ()
        | callback -> callback.Invoke(options)

        match options.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid Legate:Llm:Providers:{preset.Id}: %s{violation}"))

        builder.AddProvider(OpenAICompatibleProvider(preset.Id, options)) |> ignore
        builder

/// Extension methods registering the OpenAI-compatible provider presets on
/// <see cref="T:Legate.LlmBuilder" /> (<c>builder.Llm.AddOpenAI(...)</c>).
/// Every preset binds its <c>Legate:Llm:Providers:&lt;Name&gt;</c> section
/// (notably <c>ApiKey</c>) over the preset defaults; SDK retries stay
/// disabled and only the configured network timeout applies.
[<Sealed; AbstractClass; Extension>]
type OpenAILlmBuilderExtensions =

    /// Registers the OpenAI preset (<c>openai</c>,
    /// <c>https://api.openai.com/v1</c>, <c>gpt-4o-mini</c>) bound from
    /// <c>Legate:Llm:Providers:openai</c>.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddOpenAI(builder: Legate.LlmBuilder, configuration: IConfiguration) : Legate.LlmBuilder =
        Registration.addPreset (builder, configuration, Presets.OpenAI, null)

    /// Registers the OpenAI preset bound from
    /// <c>Legate:Llm:Providers:openai</c>, adjusted by the callback.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options before registration.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddOpenAI
        (builder: Legate.LlmBuilder, configuration: IConfiguration, configure: Action<OpenAICompatibleProviderOptions>)
        : Legate.LlmBuilder =
        ArgumentNullException.ThrowIfNull(configure)
        Registration.addPreset (builder, configuration, Presets.OpenAI, configure)

    /// Registers the Anthropic compatible preset (<c>anthropic</c>, the
    /// Anthropic OpenAI-compatible v1 endpoint, <c>claude-sonnet</c>)
    /// bound from <c>Legate:Llm:Providers:anthropic</c>. Compatible-shape
    /// payloads only; the native provider lives in
    /// <c>Legate.Llm.Anthropic</c> (<c>builder.Llm.AddAnthropic(...)</c>).
    /// Do not register both under <c>anthropic</c>: the last registration
    /// wins.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddAnthropicCompatible
        (builder: Legate.LlmBuilder, configuration: IConfiguration)
        : Legate.LlmBuilder =
        Registration.addPreset (builder, configuration, Presets.Anthropic, null)

    /// Registers the Anthropic compatible preset bound from
    /// <c>Legate:Llm:Providers:anthropic</c>, adjusted by the callback.
    /// Compatible-shape payloads only; for the native provider see
    /// <c>Legate.Llm.Anthropic</c>.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options before registration.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddAnthropicCompatible
        (builder: Legate.LlmBuilder, configuration: IConfiguration, configure: Action<OpenAICompatibleProviderOptions>)
        : Legate.LlmBuilder =
        ArgumentNullException.ThrowIfNull(configure)
        Registration.addPreset (builder, configuration, Presets.Anthropic, configure)

    /// Registers the Ollama Cloud preset (<c>ollamacloud</c>,
    /// <c>https://ollama.com/v1</c>, <c>llama3.1</c>) bound from
    /// <c>Legate:Llm:Providers:ollamacloud</c>. Local Ollama goes through
    /// <c>AddOpenAICompatible</c> with a loopback base URL instead.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddOllamaCloud(builder: Legate.LlmBuilder, configuration: IConfiguration) : Legate.LlmBuilder =
        Registration.addPreset (builder, configuration, Presets.OllamaCloud, null)

    /// Registers the Ollama Cloud preset bound from
    /// <c>Legate:Llm:Providers:ollamacloud</c>, adjusted by the callback.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options before registration.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddOllamaCloud
        (builder: Legate.LlmBuilder, configuration: IConfiguration, configure: Action<OpenAICompatibleProviderOptions>)
        : Legate.LlmBuilder =
        ArgumentNullException.ThrowIfNull(configure)
        Registration.addPreset (builder, configuration, Presets.OllamaCloud, configure)

    /// Registers a compatible provider under <paramref name="providerName" />
    /// with <paramref name="baseUrl" /> as its endpoint default, bound from
    /// <c>Legate:Llm:Providers:&lt;providerName&gt;</c>. Covers any
    /// OpenAI-compatible endpoint, including local Ollama (use
    /// <c>127.0.0.1</c>, never <c>localhost</c>). The default model comes
    /// from configuration or the callback overload.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="providerName">The provider id and configuration key: one segment, no whitespace or slashes.</param>
    /// <param name="baseUrl">The OpenAI-compatible base URL default.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddOpenAICompatible
        (builder: Legate.LlmBuilder, configuration: IConfiguration, providerName: string, baseUrl: string)
        : Legate.LlmBuilder =
        Registration.addPreset (builder, configuration, Presets.compatible providerName baseUrl, null)

    /// Registers a compatible provider under <paramref name="providerName" />
    /// with <paramref name="baseUrl" /> as its endpoint default, bound from
    /// <c>Legate:Llm:Providers:&lt;providerName&gt;</c> and adjusted by the
    /// callback (notably the default model).
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="providerName">The provider id and configuration key: one segment, no whitespace or slashes.</param>
    /// <param name="baseUrl">The OpenAI-compatible base URL default.</param>
    /// <param name="configure">Adjusts the bound options before registration.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddOpenAICompatible
        (
            builder: Legate.LlmBuilder,
            configuration: IConfiguration,
            providerName: string,
            baseUrl: string,
            configure: Action<OpenAICompatibleProviderOptions>
        ) : Legate.LlmBuilder =
        ArgumentNullException.ThrowIfNull(configure)

        Registration.addPreset (builder, configuration, Presets.compatible providerName baseUrl, configure)
