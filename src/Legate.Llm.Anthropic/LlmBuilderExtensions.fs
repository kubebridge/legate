// SPDX-License-Identifier: Apache-2.0
namespace Legate.Llm

open System
open System.Runtime.CompilerServices
open Microsoft.Extensions.Configuration

// Registration over LegateBuilder.Llm: AddAnthropic binds the
// Legate:Llm:Providers:anthropic section over the native defaults (model,
// timeout, thinking mode, output cap), applies the optional configure
// callback, validates, and registers one provider instance. SDK retries
// stay disabled and only the configured network timeout applies.

/// Binds the <c>Legate:Llm:Providers:anthropic</c> section over the native
/// defaults and registers the provider. Internal.
module internal AnthropicRegistration =

    /// Binds the provider section, applies the callback, validates, and
    /// registers the provider on the builder.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options, or null to keep them.</param>
    /// <returns>The same builder, for chaining.</returns>
    let addAnthropic
        (builder: Legate.LlmBuilder, configuration: IConfiguration, configure: Action<AnthropicLlmOptions> | null)
        : Legate.LlmBuilder =
        ArgumentNullException.ThrowIfNull(builder)
        ArgumentNullException.ThrowIfNull(configuration)

        let options = AnthropicLlmOptions()

        configuration.GetSection(AnthropicLlmOptions.ConfigurationSectionPath).Bind(options)

        match configure with
        | null -> ()
        | callback -> callback.Invoke(options)

        match options.Validate() with
        | null -> ()
        | violation ->
            raise (InvalidOperationException($"Invalid {AnthropicLlmOptions.ConfigurationSectionPath}: %s{violation}"))

        builder.AddProvider(AnthropicProvider(options)) |> ignore
        builder

/// Extension methods registering the native Anthropic provider on
/// <see cref="T:Legate.LlmBuilder" /> (<c>builder.Llm.AddAnthropic(...)</c>).
/// The provider binds its <c>Legate:Llm:Providers:anthropic</c> section
/// (notably <c>ApiKey</c>) over the native defaults; SDK retries stay
/// disabled and only the configured network timeout applies. Do not
/// register this together with the OpenAI-compatible
/// <c>AddAnthropicCompatible</c> preset under the same id: both claim
/// <c>anthropic</c> and the last registration wins.
[<Sealed; AbstractClass; Extension>]
type AnthropicLlmBuilderExtensions =

    /// Registers the native Anthropic provider (<c>anthropic</c>,
    /// <c>claude-sonnet-5</c>) bound from
    /// <c>Legate:Llm:Providers:anthropic</c>, over the official Anthropic
    /// SDK with thinking blocks surfaced as reasoning content.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddAnthropic(builder: Legate.LlmBuilder, configuration: IConfiguration) : Legate.LlmBuilder =
        AnthropicRegistration.addAnthropic (builder, configuration, null)

    /// Registers the native Anthropic provider bound from
    /// <c>Legate:Llm:Providers:anthropic</c>, adjusted by the callback.
    /// <param name="builder">The LLM builder receiving the provider.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options before registration.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddAnthropic
        (builder: Legate.LlmBuilder, configuration: IConfiguration, configure: Action<AnthropicLlmOptions>)
        : Legate.LlmBuilder =
        ArgumentNullException.ThrowIfNull(configure)
        AnthropicRegistration.addAnthropic (builder, configuration, configure)
