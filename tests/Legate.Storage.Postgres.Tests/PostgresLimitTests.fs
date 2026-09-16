// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.Postgres
open Legate.Storage.Postgres.Tests.PostgresTestDatabase
open Legate.Testing
open Xunit

// Journal limits are enforced in the store before any part of a batch
// lands: a breach throws EventLimitExceededException with populated
// properties and leaves a partial-write-free journal behind.
module PostgresLimitTests =

    /// Stores with adjusted journal limits over the shared database and a
    /// fresh clock.
    let private limitedStores (configure: PostgresOptions -> unit) : TestClock * ISessionStore * ISessionEventStore =
        let connectionString = ensureReady ()
        let clock = TestClock()
        let options = testOptions connectionString
        configure options

        clock,
        PostgresSessionStore(options, clock) :> ISessionStore,
        PostgresSessionEventStore(options, clock) :> ISessionEventStore

    /// The stores raise synchronously (like the in-memory reference, the
    /// work lands before the Task wraps), so the append arrives as a
    /// thunk: invoking it inside the try observes both sync and async
    /// failures.
    let private expectLimit (limitKind: string) (append: unit -> Task<EventAppendOutcome>) =
        task {
            try
                let! _ = append ()
                failwith "expected EventLimitExceededException"
            with :? EventLimitExceededException as exn ->
                Assert.Equal(limitKind, exn.LimitKind)
                Assert.True(exn.Limit >= 0L)
                Assert.True(exn.Observed > 0L)
        }

    [<Fact>]
    let ``An over-batch append throws batchSize with zero writes`` () =
        task {
            let _, sessions, events =
                limitedStores (fun options -> options.MaxAppendBatchSize <- 2)

            let tenant = freshTenant "limits-batch"
            let! sessionId, claim = claimedSession sessions tenant "owner"

            let batch =
                [
                    delta sessionId claim.TurnId
                    delta sessionId claim.TurnId
                    delta sessionId claim.TurnId
                ]

            do!
                expectLimit "batchSize" (fun () ->
                    events.Append(tenant, sessionId, claim.Token, batch, CancellationToken.None))

            let! replayed = events.Replay(tenant, sessionId, 0L, 10, CancellationToken.None)
            Assert.True(replayed :? EventReplayEndOfStream)
        }

    [<Fact>]
    let ``An oversized event throws perEventBytes with zero writes`` () =
        task {
            let _, sessions, events = limitedStores (fun options -> options.MaxEventBytes <- 1L)

            let tenant = freshTenant "limits-event"
            let! sessionId, claim = claimedSession sessions tenant "owner"

            do!
                expectLimit "perEventBytes" (fun () ->
                    events.Append(
                        tenant,
                        sessionId,
                        claim.Token,
                        [ delta sessionId claim.TurnId ],
                        CancellationToken.None
                    ))

            let! replayed = events.Replay(tenant, sessionId, 0L, 10, CancellationToken.None)
            Assert.True(replayed :? EventReplayEndOfStream)
        }

    [<Fact>]
    let ``A full journal throws perSessionCount with no partial write`` () =
        task {
            let _, sessions, events =
                limitedStores (fun options -> options.MaxEventsPerSession <- 2L)

            let tenant = freshTenant "limits-count"
            let! sessionId, claim = claimedSession sessions tenant "owner"

            let! first =
                events.Append(
                    tenant,
                    sessionId,
                    claim.Token,
                    [
                        delta sessionId claim.TurnId
                        delta sessionId claim.TurnId
                    ],
                    CancellationToken.None
                )

            Assert.True(first :? EventAppended)

            do!
                expectLimit "perSessionCount" (fun () ->
                    events.Append(
                        tenant,
                        sessionId,
                        claim.Token,
                        [ delta sessionId claim.TurnId ],
                        CancellationToken.None
                    ))

            let! replayed = events.Replay(tenant, sessionId, 0L, 10, CancellationToken.None)

            match replayed with
            | :? EventReplayPage as page -> Assert.Equal(2, page.Events.Count)
            | _ -> failwith "expected the two journaled events and nothing more"
        }

    [<Fact>]
    let ``A byte-full journal throws perSessionBytes with zero writes`` () =
        task {
            let _, sessions, events =
                limitedStores (fun options -> options.MaxJournalBytesPerSession <- 1L)

            let tenant = freshTenant "limits-bytes"
            let! sessionId, claim = claimedSession sessions tenant "owner"

            do!
                expectLimit "perSessionBytes" (fun () ->
                    events.Append(
                        tenant,
                        sessionId,
                        claim.Token,
                        [ delta sessionId claim.TurnId ],
                        CancellationToken.None
                    ))

            let! replayed = events.Replay(tenant, sessionId, 0L, 10, CancellationToken.None)
            Assert.True(replayed :? EventReplayEndOfStream)
        }
