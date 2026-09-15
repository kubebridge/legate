// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.OpenAIProviderOptionsTests

open System
open FsUnit.Xunit
open Legate.Llm.OpenAI
open Xunit

[<Fact>]
let ``Defaults keep the provider timeout below the coordinator deadline`` () =
    let options = OpenAICompatibleProviderOptions()

    options.Timeout |> should equal (TimeSpan.FromSeconds 100.0)

    (options.Timeout < TimeSpan.FromSeconds(float (Legate.LlmCoordinationOptions().RequestTimeoutSeconds)))
    |> should equal true

[<Fact>]
let ``Validate rejects a blank endpoint`` () =
    let options = OpenAICompatibleProviderOptions()
    options.DefaultModel <- "gpt-4o-mini"

    options.Validate() |> should equal "Endpoint must be a non-empty absolute URI."

[<Fact>]
let ``Validate rejects a relative endpoint`` () =
    let options = OpenAICompatibleProviderOptions()
    options.Endpoint <- "not-a-uri"
    options.DefaultModel <- "gpt-4o-mini"

    options.Validate() |> should equal "Endpoint must be a non-empty absolute URI."

[<Fact>]
let ``Validate rejects a blank default model`` () =
    let options = OpenAICompatibleProviderOptions()
    options.Endpoint <- "https://api.openai.com/v1"

    options.Validate() |> should equal "DefaultModel must be a non-empty string."

[<Fact>]
let ``Validate rejects a non-positive timeout`` () =
    let options = OpenAICompatibleProviderOptions()
    options.Endpoint <- "https://api.openai.com/v1"
    options.DefaultModel <- "gpt-4o-mini"
    options.Timeout <- TimeSpan.Zero

    options.Validate() |> should equal "Timeout must be positive."

[<Fact>]
let ``Validate accepts fully set options`` () =
    let options = OpenAICompatibleProviderOptions()
    options.ApiKey <- "canned-key"
    options.Endpoint <- "http://127.0.0.1:11434/v1"
    options.DefaultModel <- "llama3.1"
    options.Timeout <- TimeSpan.FromSeconds 30.0

    options.Validate() |> should equal null

[<Fact>]
let ``OpenAI preset carries the OpenAI endpoint and model`` () =
    let preset = Presets.OpenAI

    preset.Id |> should equal "openai"
    preset.Endpoint |> should equal "https://api.openai.com/v1"
    preset.DefaultModel |> should equal "gpt-4o-mini"
    preset.Reasoning |> should equal true

[<Fact>]
let ``Anthropic preset carries the compatible endpoint and model`` () =
    let preset = Presets.Anthropic

    preset.Id |> should equal "anthropic"
    preset.Endpoint |> should equal "https://api.anthropic.com/v1/"
    preset.DefaultModel |> should equal "claude-sonnet"
    preset.Reasoning |> should equal true

[<Fact>]
let ``OllamaCloud preset carries the cloud endpoint and model`` () =
    let preset = Presets.OllamaCloud

    preset.Id |> should equal "ollamacloud"
    preset.Endpoint |> should equal "https://ollama.com/v1"
    preset.DefaultModel |> should equal "llama3.1"
    preset.Reasoning |> should equal false

[<Fact>]
let ``Compatible preset takes the caller name and base URL`` () =
    let preset = Presets.compatible "Acme" "http://127.0.0.1:11434/v1"

    preset.Id |> should equal "acme"
    preset.Endpoint |> should equal "http://127.0.0.1:11434/v1"

[<Theory>]
[<InlineData("")>]
[<InlineData("  ")>]
[<InlineData("has space")>]
[<InlineData("has/slash")>]
let ``Compatible preset rejects bad provider names`` (name: string) =
    Assert.Throws<ArgumentException>(fun () -> Presets.compatible name "http://127.0.0.1:11434/v1" |> ignore)
    |> ignore

[<Theory>]
[<InlineData("")>]
[<InlineData("  ")>]
let ``Compatible preset rejects a blank base URL`` (baseUrl: string) =
    Assert.Throws<ArgumentException>(fun () -> Presets.compatible "acme" baseUrl |> ignore)
    |> ignore
