// SPDX-License-Identifier: Apache-2.0
module Legate.Coordination.Redis.Tests.CombinedLimitTests

open System
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate.Coordination.Redis
open Xunit

[<Fact>]
let ``Two clients sharing one instance respect one combined limit`` () : Task =
    task {
        let _, first, second = RedisTestEnvironment.connectSharedPair ()
        let identity = RedisTestEnvironment.freshIdentity "combined"
        let leaseTtl = TimeSpan.FromMinutes 5.
        let waiterTtl = TimeSpan.FromMinutes 5.

        let! firstOutcome = first.AcquireAsync(identity, "owner-a", 1, leaseTtl, waiterTtl, CancellationToken.None)

        firstOutcome.Kind |> should equal DistributedAdmissionDecision.Acquired

        let! secondOutcome = second.AcquireAsync(identity, "owner-b", 1, leaseTtl, waiterTtl, CancellationToken.None)

        secondOutcome.Kind |> should equal DistributedAdmissionDecision.Queued

        let! released = first.ReleaseAsync(identity, "owner-a", CancellationToken.None)
        released |> should equal true

        let! secondRetry = second.AcquireAsync(identity, "owner-b", 1, leaseTtl, waiterTtl, CancellationToken.None)

        secondRetry.Kind |> should equal DistributedAdmissionDecision.Acquired

        let! _ = second.ReleaseAsync(identity, "owner-b", CancellationToken.None)
        return ()
    }

[<Fact>]
let ``Cooldown refuses new work on both clients`` () : Task =
    task {
        let _, first, second = RedisTestEnvironment.connectSharedPair ()
        let identity = RedisTestEnvironment.freshIdentity "cooldown"
        let leaseTtl = TimeSpan.FromMinutes 5.
        let waiterTtl = TimeSpan.FromMinutes 5.

        do! first.StartCooldownAsync(identity, TimeSpan.FromMinutes 1., CancellationToken.None)

        let! outcome = second.AcquireAsync(identity, "owner-a", 4, leaseTtl, waiterTtl, CancellationToken.None)

        outcome.Kind |> should equal DistributedAdmissionDecision.CooldownActive
        return ()
    }
