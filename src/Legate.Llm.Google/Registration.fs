// SPDX-License-Identifier: Apache-2.0
namespace Legate.Llm

open System
open System.Runtime.CompilerServices
open Legate
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection

// Host registration for the Google Gemini provider. The host binds the
// Legate:Llm:Providers:Google section onto GoogleLlmOptions (API key,
// model override, timeout) and registers the provider as an ILlmProvider
// singleton for the coordinator's registry; the coordinator still resolves
// the per-call API key through its own LlmOptions binding and passes it to
// CreateChatClient. Overloads live on a sealed static class because F# let
// bindings cannot overload.
type GoogleServiceCollectionExtensions =

    /// Registers the Google Gemini provider from a configuration section:
    /// binds the section onto GoogleLlmOptions (missing section means
    /// defaults) and registers the options plus the provider as an
    /// ILlmProvider singleton.
    /// <param name="services">The container to add the provider to.</param>
    /// <param name="section">The <c>Legate:Llm:Providers:Google</c> configuration section to bind.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member AddGoogle(services: IServiceCollection, section: IConfigurationSection) : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)
        ArgumentNullException.ThrowIfNull(section)

        let bound: GoogleLlmOptions | null = section.Get<GoogleLlmOptions>()

        let options =
            match bound with
            | null -> GoogleLlmOptions()
            | configured -> configured

        services.AddSingleton<GoogleLlmOptions>(options) |> ignore
        services.AddSingleton<ILlmProvider, GoogleProvider>() |> ignore
        services

    /// Registers the Google Gemini provider from the application
    /// configuration: reads the
    /// <c>Legate:Llm:Providers:Google</c> section and delegates to the
    /// section overload.
    /// <param name="services">The container to add the provider to.</param>
    /// <param name="configuration">The application configuration holding the provider section.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member AddGoogle(services: IServiceCollection, configuration: IConfiguration) : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)
        ArgumentNullException.ThrowIfNull(configuration)

        GoogleServiceCollectionExtensions.AddGoogle(
            services,
            configuration.GetSection(GoogleLlmOptions.ConfigurationSectionPath)
        )
