// SPDX-License-Identifier: Apache-2.0
module Legate.Coordination.Redis.Tests.FifoAndTtlTests

open System
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Coordination.Redis
open StackExchange.Redis
open Xunit

[<Fact>]
let ``Waiters queue in FIFO order`` () : Task =
    task {
        let options = RedisTestEnvironment.testOptions ()
        let client = RedisTestEnvironment.connectClient options
        let identity = RedisTestEnvironment.freshIdentity "fifo"
        let leaseTtl = TimeSpan.FromMinutes 5.
        let waiterTtl = TimeSpan.FromMinutes 5.

        let! holder = client.AcquireAsync(identity, "holder", 1, leaseTtl, waiterTtl, CancellationToken.None)
        holder.Kind |> should equal DistributedAdmissionDecision.Acquired

        let! first = client.AcquireAsync(identity, "waiter-1", 1, leaseTtl, waiterTtl, CancellationToken.None)
        first.Kind |> should equal DistributedAdmissionDecision.Queued

        let! second = client.AcquireAsync(identity, "waiter-2", 1, leaseTtl, waiterTtl, CancellationToken.None)
        second.Kind |> should equal DistributedAdmissionDecision.Queued
        second.QueuePosition |> should be (greaterThan first.QueuePosition)

        let! _ = client.ReleaseAsync(identity, "holder", CancellationToken.None)
        let! _ = client.ReleaseAsync(identity, "waiter-1", CancellationToken.None)
        let! _ = client.ReleaseAsync(identity, "waiter-2", CancellationToken.None)
        return ()
    }

[<Fact>]
let ``Expired waiters stop blocking the queue`` () : Task =
    task {
        let options = RedisTestEnvironment.testOptions ()
        let client = RedisTestEnvironment.connectClient options
        let identity = RedisTestEnvironment.freshIdentity "ttl"
        let leaseTtl = TimeSpan.FromMinutes 5.
        let waiterTtl = TimeSpan.FromMinutes 5.

        let multiplexer = ConnectionMultiplexer.Connect(options.ConnectionString)
        let db = multiplexer.GetDatabase()
        let queueKey = RedisScripts.queueKey options.KeyPrefix identity

        // A stale waiter enqueued at the epoch: the next acquire prunes it
        // before queueing, so the fresh waiter lands at the head.
        db.SortedSetAdd(RedisKey.op_Implicit queueKey, RedisValue.op_Implicit "stale-waiter", 0.0)
        |> ignore

        let! _ = client.AcquireAsync(identity, "fresh-waiter", 1, leaseTtl, waiterTtl, CancellationToken.None)

        // With an empty active set the fresh waiter acquires outright; the
        // proof is that the stale entry no longer blocks the queue.
        let remaining = db.SortedSetRangeByRank(RedisKey.op_Implicit queueKey)

        let containsStale =
            remaining |> Array.exists (fun value -> value.ToString() = "stale-waiter")

        containsStale |> should equal false

        let! _ = client.ReleaseAsync(identity, "fresh-waiter", CancellationToken.None)
        return ()
    }
