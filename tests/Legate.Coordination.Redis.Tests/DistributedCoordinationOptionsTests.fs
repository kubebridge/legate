// SPDX-License-Identifier: Apache-2.0
module Legate.Coordination.Redis.Tests.DistributedCoordinationOptionsTests

open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Legate.Coordination.Redis
open Microsoft.Extensions.Configuration
open Xunit

/// Builds the Legate:Llm:DistributedCoordination section from pairs.
let private buildSection (pairs: (string * string) seq) : IConfigurationSection =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder()
        .AddInMemoryCollection(keyValues)
        .Build()
        .GetSection(DistributedCoordinationOptions.ConfigurationSectionPath)

[<Fact>]
let ``Section path binds at Legate Llm DistributedCoordination`` () =
    DistributedCoordinationOptions.ConfigurationSectionPath
    |> should equal "Legate:Llm:DistributedCoordination"

[<Fact>]
let ``Defaults coordinate locally with the standard prefix`` () =
    let options = DistributedCoordinationOptions()
    options.Mode |> should equal DistributedCoordinationMode.Disabled
    options.KeyPrefix |> should equal "legate:llm"
    options.StartupRequired |> should equal true
    options.FailClosed |> should equal true
    options.Validate() |> should equal null

[<Fact>]
let ``Validate accepts disabled without a connection`` () =
    let options = DistributedCoordinationOptions()
    options.Mode <- DistributedCoordinationMode.Disabled
    options.ConnectionString <- ""
    options.Validate() |> should equal null

[<Fact>]
let ``Validate requires a connection when Redis`` () =
    let options = DistributedCoordinationOptions()
    options.Mode <- DistributedCoordinationMode.Redis
    options.ConnectionString <- ""
    options.Validate() |> should not' (equal null)

[<Fact>]
let ``Validate accepts Redis with a connection`` () =
    let options = DistributedCoordinationOptions()
    options.Mode <- DistributedCoordinationMode.Redis
    options.ConnectionString <- "127.0.0.1:6379"
    options.Validate() |> should equal null

[<Fact>]
let ``Validate rejects an empty prefix`` () =
    let options = DistributedCoordinationOptions()
    options.KeyPrefix <- "  "
    options.Validate() |> should not' (equal null)

[<Fact>]
let ``Validate rejects a prefix carrying the generation tag`` () =
    let options = DistributedCoordinationOptions()
    options.KeyPrefix <- "legate:llm:v1:prod"
    options.Validate() |> should not' (equal null)

[<Fact>]
let ``Validate rejects an undefined mode`` () =
    let options = DistributedCoordinationOptions()
    options.Mode <- enum<DistributedCoordinationMode> 99
    options.Validate() |> should not' (equal null)

[<Fact>]
let ``Section binds mode connection startup and fail-closed`` () =
    let prefix = DistributedCoordinationOptions.ConfigurationSectionPath

    let section =
        buildSection
            [
                prefix + ":Mode", "Redis"
                prefix + ":ConnectionString", "127.0.0.1:6379"
                prefix + ":KeyPrefix", "legate:test"
                prefix + ":StartupRequired", "false"
                prefix + ":FailClosed", "false"
            ]

    let options = DistributedCoordinationOptions()
    section.Bind(options)
    options.Mode |> should equal DistributedCoordinationMode.Redis
    options.ConnectionString |> should equal "127.0.0.1:6379"
    options.KeyPrefix |> should equal "legate:test"
    options.StartupRequired |> should equal false
    options.FailClosed |> should equal false
    options.Validate() |> should equal null
