// SPDX-License-Identifier: Apache-2.0
module Legate.Coordination.Redis.Tests.OwnerFencingTests

open System
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate.Coordination.Redis
open Xunit

[<Fact>]
let ``A lease held by one owner rejects another owner`` () : Task =
    task {
        let options = RedisTestEnvironment.testOptions ()
        let client = RedisTestEnvironment.connectClient options
        let identity = RedisTestEnvironment.freshIdentity "fencing"
        let leaseTtl = TimeSpan.FromMinutes 5.
        let waiterTtl = TimeSpan.FromMinutes 5.

        let! holder = client.AcquireAsync(identity, "owner-a", 1, leaseTtl, waiterTtl, CancellationToken.None)
        holder.Kind |> should equal DistributedAdmissionDecision.Acquired

        let! foreignRenew = client.RenewAsync(identity, "owner-b", leaseTtl, CancellationToken.None)
        foreignRenew |> should equal false

        let! foreignRelease = client.ReleaseAsync(identity, "owner-b", CancellationToken.None)
        foreignRelease |> should equal false

        let! foreignComplete = client.CompleteAsync(identity, "owner-b", CancellationToken.None)
        foreignComplete |> should equal false

        let! ownRenew = client.RenewAsync(identity, "owner-a", leaseTtl, CancellationToken.None)
        ownRenew |> should equal true

        let! ownRelease = client.ReleaseAsync(identity, "owner-a", CancellationToken.None)
        ownRelease |> should equal true
        return ()
    }

[<Fact>]
let ``Complete removes the lease so the next waiter acquires`` () : Task =
    task {
        let options = RedisTestEnvironment.testOptions ()
        let client = RedisTestEnvironment.connectClient options
        let identity = RedisTestEnvironment.freshIdentity "complete"
        let leaseTtl = TimeSpan.FromMinutes 5.
        let waiterTtl = TimeSpan.FromMinutes 5.

        let! holder = client.AcquireAsync(identity, "owner-a", 1, leaseTtl, waiterTtl, CancellationToken.None)
        holder.Kind |> should equal DistributedAdmissionDecision.Acquired

        let! waiter = client.AcquireAsync(identity, "owner-b", 1, leaseTtl, waiterTtl, CancellationToken.None)
        waiter.Kind |> should equal DistributedAdmissionDecision.Queued

        let! completed = client.CompleteAsync(identity, "owner-a", CancellationToken.None)
        completed |> should equal true

        let! retry = client.AcquireAsync(identity, "owner-b", 1, leaseTtl, waiterTtl, CancellationToken.None)
        retry.Kind |> should equal DistributedAdmissionDecision.Acquired

        let! _ = client.ReleaseAsync(identity, "owner-b", CancellationToken.None)
        return ()
    }
