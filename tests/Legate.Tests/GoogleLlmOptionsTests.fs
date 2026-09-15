// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.GoogleLlmOptionsTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate.Llm
open Microsoft.Extensions.Configuration
open Xunit

/// Builds the Google provider section from in-memory pairs.
let private buildSection (pairs: (string * string) seq) : IConfigurationSection =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder()
        .AddInMemoryCollection(keyValues)
        .Build()
        .GetSection(GoogleLlmOptions.ConfigurationSectionPath)

[<Fact>]
let ``Defaults serve the stable Flash model with a positive timeout`` () =
    let options = GoogleLlmOptions()
    options.Model |> should equal GoogleLlmOptions.DefaultModel
    GoogleLlmOptions.DefaultModel |> should equal "gemini-3.8-flash"
    options.Timeout |> should equal (TimeSpan.FromSeconds 100.0)
    options.ApiKey |> should equal null

[<Fact>]
let ``Section path converges with the sibling provider packages`` () =
    GoogleLlmOptions.ConfigurationSectionPath
    |> should equal "Legate:Llm:Providers:Google"

[<Fact>]
let ``Binds model key and timeout overrides from configuration`` () =
    let section =
        buildSection
            [
                "Legate:Llm:Providers:Google:ApiKey", "test-key"
                "Legate:Llm:Providers:Google:Model", "gemini-3.8-flash"
                "Legate:Llm:Providers:Google:Timeout", "00:02:00"
            ]

    let bound: GoogleLlmOptions | null = section.Get<GoogleLlmOptions>()

    match bound with
    | null -> Assert.Fail("Configuration binding returned null.") |> ignore
    | options ->
        options.ApiKey |> should equal "test-key"
        options.Model |> should equal "gemini-3.8-flash"
        options.Timeout |> should equal (TimeSpan.FromMinutes 2.0)

[<Fact>]
let ``Missing keys keep the defaults`` () =
    let section =
        buildSection
            [
                "Legate:Llm:Providers:Google:ApiKey", "test-key"
            ]

    let bound: GoogleLlmOptions | null = section.Get<GoogleLlmOptions>()

    match bound with
    | null -> Assert.Fail("Configuration binding returned null.") |> ignore
    | options ->
        options.Model |> should equal GoogleLlmOptions.DefaultModel
        options.Timeout |> should equal (TimeSpan.FromSeconds 100.0)
