// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.RegistrationTests

open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Legate.Llm
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Xunit

/// Builds the application configuration from in-memory pairs.
let private buildConfiguration (pairs: (string * string) seq) : IConfiguration =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build() :> IConfiguration

[<Fact>]
let ``AddGoogle binds the section and registers the provider`` () =
    let configuration =
        buildConfiguration
            [
                "Legate:Llm:Providers:Google:ApiKey", "test-key"
                "Legate:Llm:Providers:Google:Model", "gemini-3.8-flash"
            ]

    let services = ServiceCollection() :> IServiceCollection
    GoogleServiceCollectionExtensions.AddGoogle(services, configuration) |> ignore

    let provider =
        ServiceProviderServiceExtensions.GetRequiredService<ILlmProvider>(services.BuildServiceProvider())

    provider.Id |> should equal "google"
    provider.DefaultModel |> should equal "gemini-3.8-flash"

    let options =
        ServiceProviderServiceExtensions.GetRequiredService<GoogleLlmOptions>(services.BuildServiceProvider())

    options.ApiKey |> should equal "test-key"

[<Fact>]
let ``AddGoogle reads the conventional section path from configuration`` () =
    let configuration =
        buildConfiguration
            [
                "Legate:Llm:Providers:Google:Model", "gemini-3.8-flash"
            ]

    let services = ServiceCollection() :> IServiceCollection
    GoogleServiceCollectionExtensions.AddGoogle(services, configuration) |> ignore

    let provider =
        ServiceProviderServiceExtensions.GetRequiredService<ILlmProvider>(services.BuildServiceProvider())

    provider.DefaultModel |> should equal "gemini-3.8-flash"

[<Fact>]
let ``AddGoogle with a missing section registers defaults`` () =
    let configuration = buildConfiguration []

    let services = ServiceCollection() :> IServiceCollection

    GoogleServiceCollectionExtensions.AddGoogle(
        services,
        configuration.GetSection(GoogleLlmOptions.ConfigurationSectionPath)
    )
    |> ignore

    let provider =
        ServiceProviderServiceExtensions.GetRequiredService<ILlmProvider>(services.BuildServiceProvider())

    provider.Id |> should equal "google"
    provider.DefaultModel |> should equal GoogleLlmOptions.DefaultModel
