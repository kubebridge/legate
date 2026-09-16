// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open System
open System.Threading
open Legate
open Legate.Storage.Postgres.Tests.PostgresTestDatabase
open Xunit

// Stale-token rejection and the takeover race on real PostgreSQL: every
// fenced call verifies the claim token at the last moment, so the loser of
// a takeover has zero effects while the winner still settles.
module PostgresClaimFencingTests =

    [<Fact>]
    let ``A live claim verifies held, renews, appends, and settles`` () =
        task {
            let _, sessions, events, _ = createStores ()
            let tenant = freshTenant "fence-live"

            let! sessionId, claim = claimedSession sessions tenant "owner"

            let! held = sessions.VerifyClaim(tenant, claim, CancellationToken.None)
            Assert.True(held :? TurnLeaseHeld)

            let! renewedState = sessions.RenewClaim(tenant, claim, TimeSpan.FromMinutes 5., CancellationToken.None)

            Assert.True(renewedState :? TurnLeaseRenewed)

            let renewed = (renewedState :?> TurnLeaseRenewed).Claim

            let! appended =
                events.Append(
                    tenant,
                    sessionId,
                    renewed.Token,
                    [ delta sessionId claim.TurnId ],
                    CancellationToken.None
                )

            Assert.True(appended :? EventAppended)

            let! settled = sessions.SettleTurn(tenant, renewed, TurnStatus.Completed, null, CancellationToken.None)

            Assert.True(settled :? TurnSettled)
        }

    [<Fact>]
    let ``A stale claim token is rejected by every fenced call`` () =
        task {
            let clock, sessions, events, _ = createStores ()
            let tenant = freshTenant "fence-stale"

            let! sessionId, claim = claimedSession sessions tenant "owner-a"

            // Takeover: the lease lapses and another owner claims the same
            // turn back through a reply.
            clock.Advance(TimeSpan.FromMinutes 10.)

            let reply =
                ReplyPayload(PermissionDecision("req-1", PermissionDecisionKind.AllowOnce)) :> InboxPayload

            let! _ = sessions.AppendInboxMessage(tenant, sessionId, reply, DeliveryMode.Queue, CancellationToken.None)

            let! taken =
                sessions.ClaimNextTurn(tenant, sessionId, "owner-b", TimeSpan.FromMinutes 5., CancellationToken.None)

            let winner = (taken :?> TurnLeaseRenewed).Claim
            Assert.Equal(claim.TurnId, winner.TurnId)
            Assert.Equal(claim.Attempt + 1, winner.Attempt)

            let! renewLost = sessions.RenewClaim(tenant, claim, TimeSpan.FromMinutes 5., CancellationToken.None)
            Assert.True(renewLost :? TurnLeaseLost || renewLost :? TurnLeaseMissing)

            let usage =
                {
                    UsageSummary.InputTokens = 1L
                    OutputTokens = 2L
                }

            let! checkpointLost = sessions.CheckpointUsage(tenant, claim, usage, CancellationToken.None)
            Assert.True(checkpointLost :? TurnLeaseLost || checkpointLost :? TurnLeaseMissing)

            let! settleLost = sessions.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)
            Assert.True(settleLost :? TurnSettleRejected)

            let! abortLost = sessions.AbortTurn(tenant, claim, CancellationToken.None)
            Assert.True(abortLost :? TurnLeaseLost || abortLost :? TurnLeaseMissing)

            let! appendRejected =
                events.Append(tenant, sessionId, claim.Token, [ delta sessionId claim.TurnId ], CancellationToken.None)

            Assert.True(appendRejected :? EventAppendRejected)

            // The loser wrote nothing: the journal holds no event from the
            // stale token, and the winner still settles.
            let! replayed = events.Replay(tenant, sessionId, 0L, 10, CancellationToken.None)
            Assert.True(replayed :? EventReplayEndOfStream)

            let! settled = sessions.SettleTurn(tenant, winner, TurnStatus.Completed, null, CancellationToken.None)
            Assert.True(settled :? TurnSettled)
        }
