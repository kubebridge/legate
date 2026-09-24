// SPDX-License-Identifier: Apache-2.0
module Legate.Llm.Anthropic.Tests.AnthropicRegistrationTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Legate.Llm
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Xunit

// Registration coverage for the native provider: AddAnthropic binds
// Legate:Llm:Providers:anthropic and registers one provider instance.
// Scripted transports only; the canned key never leaves the test.

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
// Registration

[<Fact>]
let ``AddAnthropic registers the native provider bound from configuration`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:anthropic:ApiKey", "canned-key"
            ]

    let provider =
        registerSingle (fun builder -> builder.Llm.AddAnthropic(config) |> ignore)

    provider.Id |> should equal "anthropic"
    provider.DefaultModel |> should equal AnthropicLlmOptions.DefaultModel
    provider.Capabilities.Streaming |> should equal true
    provider.Capabilities.Reasoning |> should equal true
    provider.Capabilities.ToolCalling |> should equal true

[<Fact>]
let ``AddAnthropic honours configuration overrides`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:anthropic:ApiKey", "canned-key"
                "Legate:Llm:Providers:anthropic:Model", "claude-sonnet-4-5-20250929"
            ]

    let provider =
        registerSingle (fun builder -> builder.Llm.AddAnthropic(config) |> ignore)

    provider.DefaultModel |> should equal "claude-sonnet-4-5-20250929"

[<Fact>]
let ``AddAnthropic honours the configure callback`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:anthropic:ApiKey", "canned-key"
            ]

    let provider =
        registerSingle (fun builder ->
            builder.Llm.AddAnthropic(
                config,
                Action<AnthropicLlmOptions>(fun options -> options.Model <- "claude-sonnet-4-5-20250929")
            )
            |> ignore)

    provider.DefaultModel |> should equal "claude-sonnet-4-5-20250929"

[<Fact>]
let ``AddAnthropic without a model fails validation`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:anthropic:ApiKey", "canned-key"
                "Legate:Llm:Providers:anthropic:Model", "   "
            ]

    let services = ServiceCollection()
    let builder = LegateBuilder(services)

    Assert.Throws<InvalidOperationException>(fun () -> builder.Llm.AddAnthropic(config) |> ignore)
    |> ignore

[<Fact>]
let ``Registration chains on the same builder`` () =
    let config =
        buildConfig
            [
                "Legate:Llm:Providers:anthropic:ApiKey", "canned-key"
            ]

    let services = ServiceCollection()
    let builder = LegateBuilder(services)

    let chained =
        builder.Llm.AddAnthropic(
            config,
            Action<AnthropicLlmOptions>(fun options -> options.Model <- "claude-sonnet-4-5-20250929")
        )

    Object.ReferenceEquals(chained, builder.Llm) |> should equal true

    use provider = services.BuildServiceProvider()

    provider.GetServices<ILlmProvider>()
    |> Seq.map (fun registered -> registered.Id)
    |> Seq.toList
    |> should equal [ "anthropic" ]
