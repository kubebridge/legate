// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.OpenAIRegistrationTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Legate.Llm.OpenAI
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Xunit

// Registration coverage for every preset: AddOpenAI, AddAnthropic,
// AddOllamaCloud, and AddOpenAICompatible bind Legate:Llm:Providers:<Name>
// and register one provider instance. Scripted transports only; the canned
// key never leaves the test.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// Builds the application configuration root from in-memory pairs.
let private buildConfig (pairs: (string * string) seq) : IConfiguration =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build() :> IConfiguration

/// Registers through the callback and returns the single provider.
let private registerSingle (configure: LegateBuilder -> unit) : ILlmProvider =
    let services = ServiceCollection()
    let builder = LegateBuilder(services)
    configure builder

    use provider = services.BuildServiceProvider()
    provider.GetServices<ILlmProvider>() |> Seq.exactlyOne

// ──────────────────────────────────────────────────────────────────────────
// Presets

[<Fact>]
let ``AddOpenAI registers the OpenAI preset bound from configuration`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:openai:ApiKey", "canned-key"
            ]

    let provider =
        registerSingle (fun builder -> builder.Llm.AddOpenAI(config) |> ignore)

    provider.Id |> should equal "openai"
    provider.DefaultModel |> should equal "gpt-4o-mini"
    provider.Capabilities.Streaming |> should equal true
    provider.Capabilities.ToolCalling |> should equal true

[<Fact>]
let ``AddOpenAI honours configuration overrides`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:openai:ApiKey", "canned-key"
                "Legate:Llm:Providers:openai:DefaultModel", "gpt-4o"
                "Legate:Llm:Providers:openai:Endpoint", "http://127.0.0.1:11434/v1"
            ]

    let provider =
        registerSingle (fun builder -> builder.Llm.AddOpenAI(config) |> ignore)

    provider.DefaultModel |> should equal "gpt-4o"

[<Fact>]
let ``AddOpenAI honours the configure callback`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:openai:ApiKey", "canned-key"
            ]

    let provider =
        registerSingle (fun builder ->
            builder.Llm.AddOpenAI(
                config,
                Action<OpenAICompatibleProviderOptions>(fun options -> options.DefaultModel <- "gpt-4o")
            )
            |> ignore)

    provider.DefaultModel |> should equal "gpt-4o"

[<Fact>]
let ``AddAnthropic registers the Anthropic preset`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:anthropic:ApiKey", "canned-key"
            ]

    let provider =
        registerSingle (fun builder -> builder.Llm.AddAnthropic(config) |> ignore)

    provider.Id |> should equal "anthropic"
    provider.DefaultModel |> should equal "claude-sonnet"

[<Fact>]
let ``AddOllamaCloud registers the cloud preset`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:ollamacloud:ApiKey", "canned-key"
            ]

    let provider =
        registerSingle (fun builder -> builder.Llm.AddOllamaCloud(config) |> ignore)

    provider.Id |> should equal "ollamacloud"
    provider.DefaultModel |> should equal "llama3.1"

[<Fact>]
let ``AddOpenAICompatible registers a named endpoint`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:acme:ApiKey", "canned-key"
                "Legate:Llm:Providers:acme:DefaultModel", "acme-large"
            ]

    let provider =
        registerSingle (fun builder ->
            builder.Llm.AddOpenAICompatible(config, "acme", "http://127.0.0.1:11434/v1")
            |> ignore)

    provider.Id |> should equal "acme"
    provider.DefaultModel |> should equal "acme-large"

[<Fact>]
let ``AddOpenAICompatible takes the model from the configure callback`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:acme:ApiKey", "canned-key"
            ]

    let provider =
        registerSingle (fun builder ->
            builder.Llm.AddOpenAICompatible(
                config,
                "acme",
                "http://127.0.0.1:11434/v1",
                Action<OpenAICompatibleProviderOptions>(fun options -> options.DefaultModel <- "acme-large")
            )
            |> ignore)

    provider.DefaultModel |> should equal "acme-large"

[<Fact>]
let ``AddOpenAICompatible without a model fails validation`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:acme:ApiKey", "canned-key"
            ]

    let services = ServiceCollection()
    let builder = LegateBuilder(services)

    Assert.Throws<InvalidOperationException>(fun () ->
        builder.Llm.AddOpenAICompatible(config, "acme", "http://127.0.0.1:11434/v1")
        |> ignore)
    |> ignore

[<Theory>]
[<InlineData("")>]
[<InlineData("has space")>]
[<InlineData("has/slash")>]
let ``AddOpenAICompatible rejects bad provider names`` (name: string) =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:acme:ApiKey", "canned-key"
            ]

    let services = ServiceCollection()
    let builder = LegateBuilder(services)

    Assert.Throws<ArgumentException>(fun () ->
        builder.Llm.AddOpenAICompatible(config, name, "http://127.0.0.1:11434/v1")
        |> ignore)
    |> ignore

[<Fact>]
let ``Registration chains on the same builder`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:openai:ApiKey", "canned-key"
                "Legate:Llm:Providers:anthropic:ApiKey", "canned-key"
            ]

    let services = ServiceCollection()
    let builder = LegateBuilder(services)

    let chained = builder.Llm.AddOpenAI(config).AddAnthropic(config)

    Object.ReferenceEquals(chained, builder.Llm) |> should equal true

    use provider = services.BuildServiceProvider()

    provider.GetServices<ILlmProvider>()
    |> Seq.map (fun registered -> registered.Id)
    |> Set.ofSeq
    |> should equal (Set.ofList [ "anthropic"; "openai" ])
