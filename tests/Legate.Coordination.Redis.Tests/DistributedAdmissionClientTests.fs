// SPDX-License-Identifier: Apache-2.0
module Legate.Coordination.Redis.Tests.DistributedAdmissionClientTests

open System
open System.Threading
open FsUnit.Xunit
open Legate
open Legate.Coordination.Redis
open StackExchange.Redis
open Xunit

/// Builds options that never touch the network: validation runs before any
/// Redis call, so a failing factory proves the guards without connecting.
let private options () =
    let value = DistributedCoordinationOptions()
    value.Mode <- DistributedCoordinationMode.Redis
    value.ConnectionString <- "127.0.0.1:6379"
    value

/// A database factory that fails when invoked: validation must throw
/// before the client touches Redis.
let private failingDatabase () =
    Func<IDatabase>(fun () -> raise (InvalidOperationException("must not touch Redis")))

/// A client over the failing factory.
let private client () =
    RedisDistributedLlmAdmission.CreateForTests(options (), failingDatabase ()) :> IDistributedLlmAdmission

[<Fact>]
let ``Constructor rejects null options`` () =
    let nullOptions = Unchecked.defaultof<DistributedCoordinationOptions>
    let nullMultiplexer = Unchecked.defaultof<IConnectionMultiplexer>

    (fun () -> RedisDistributedLlmAdmission(nullOptions, nullMultiplexer) |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``Outcome factories guard the queue position`` () =
    (fun () -> DistributedAdmissionOutcome.Queued(-1) |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

    DistributedAdmissionOutcome.Acquired().Kind
    |> should equal DistributedAdmissionDecision.Acquired

    DistributedAdmissionOutcome.CooldownActive().Kind
    |> should equal DistributedAdmissionDecision.CooldownActive

    DistributedAdmissionOutcome.Queued(0).QueuePosition |> should equal 0

[<Fact>]
let ``Acquire validates before touching Redis`` () =
    (fun () ->
        (client ())
            .AcquireAsync("", "owner", 1, TimeSpan.FromSeconds 30., TimeSpan.FromMinutes 1., CancellationToken.None)
            .GetAwaiter()
            .GetResult()
        |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``Acquire rejects a non-positive limit before touching Redis`` () =
    (fun () ->
        (client ())
            .AcquireAsync("id", "owner", 0, TimeSpan.FromSeconds 30., TimeSpan.FromMinutes 1., CancellationToken.None)
            .GetAwaiter()
            .GetResult()
        |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

[<Fact>]
let ``Renew validates before touching Redis`` () =
    (fun () ->
        (client ()).RenewAsync("", "owner", TimeSpan.FromSeconds 30., CancellationToken.None).GetAwaiter().GetResult()
        |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``Release and complete validate before touching Redis`` () =
    (fun () ->
        (client ()).ReleaseAsync("id", "", CancellationToken.None).GetAwaiter().GetResult()
        |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () ->
        (client ()).CompleteAsync("id", "", CancellationToken.None).GetAwaiter().GetResult()
        |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``Cooldown validates before touching Redis`` () =
    (fun () ->
        (client ()).StartCooldownAsync("id", TimeSpan.Zero, CancellationToken.None).GetAwaiter().GetResult()
        |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>
