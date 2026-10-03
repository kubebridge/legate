// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open System
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.Postgres
open Legate.Testing
open Xunit

/// PostgreSQL atomic settlement conformance (issue 363) over Testcontainers:
/// Applied, identical and conflicting retry, and stale-authority rejection
/// serialize with database row locks against takeover and host lifecycle writes.
type PostgresSessionSettlementTests() =

    let tenant = TenantId.Create "pg-settlement"

    let freshSession () =
        let options = SessionOptions()
        options.FormatVersion <- 1

        {
            Id = SessionId.New()
            Tenant = tenant
            AgentId = AgentId.New()
            Title = "pg-settlement"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.UtcNow
            UpdatedAt = DateTimeOffset.UtcNow
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = options
            PermissionGrants = Array.empty<string> :> System.Collections.Generic.IReadOnlyList<string>
        }

    let okResult status =
        {
            AssistantText = "done"
            Status = status
            Iterations = 1
            Usage = { InputTokens = 3L; OutputTokens = 5L }
            Outcome = TurnFinished("done") :> TurnOutcome
        }

    [<Fact>]
    member _.``postgres applied then already applied with distinct connections``() =
        task {
            let clock, store, events, _ = PostgresTestDatabase.createStores ()
            let connectionString = PostgresTestDatabase.ensureReady ()
            let options = PostgresTestDatabase.testOptions connectionString

            let settlement =
                PostgresSessionSettlementStore(options, clock) :> ISessionSettlementStore

            Assert.True(settlement.SupportsSettlementJournal events)
            let session = freshSession ()
            let! _ = store.CreateSession(tenant, session, CancellationToken.None)

            let! entry =
                store.AppendInboxMessage(
                    tenant,
                    session.Id,
                    UserMessagePayload(UserMessage.Text "hi"),
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let! lease =
                store.ClaimNextTurn(tenant, session.Id, "owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)

            let claim =
                match lease with
                | :? TurnLeaseHeld as held -> held.Claim
                | :? TurnLeaseRenewed as renewed -> renewed.Claim
                | _ -> failwith "Expected a held claim."

            Assert.True(
                (settlement.AdmitExecution(tenant, session.Id, entry.Position, claim, CancellationToken.None).Result)
            )

            let key = Guid.NewGuid().ToString("N")

            let request =
                SessionSettlementRequest(
                    session.Id,
                    entry.Position,
                    claim,
                    Nullable(),
                    okResult TurnStatus.Completed,
                    key,
                    null
                )

            let! applied = settlement.SettleExecution(tenant, request, CancellationToken.None)
            Assert.Equal(SessionSettlementStatus.Applied, applied.Status)
            let! again = settlement.SettleExecution(tenant, request, CancellationToken.None)
            Assert.Equal(SessionSettlementStatus.AlreadyApplied, again.Status)

            let conflicting =
                SessionSettlementRequest(
                    session.Id,
                    entry.Position,
                    claim,
                    Nullable(),
                    okResult TurnStatus.Failed,
                    Guid.NewGuid().ToString("N"),
                    null
                )

            let! rejected = settlement.SettleExecution(tenant, conflicting, CancellationToken.None)
            Assert.Equal(SessionSettlementStatus.Rejected, rejected.Status)
        }
        :> Task

    [<Fact>]
    member _.``postgres stale claim settles rejected with zero effects``() =
        task {
            let clock, store, _, _ = PostgresTestDatabase.createStores ()
            let connectionString = PostgresTestDatabase.ensureReady ()
            let options = PostgresTestDatabase.testOptions connectionString

            let settlement =
                PostgresSessionSettlementStore(options, clock) :> ISessionSettlementStore

            let session = freshSession ()
            let! _ = store.CreateSession(tenant, session, CancellationToken.None)

            let! entry =
                store.AppendInboxMessage(
                    tenant,
                    session.Id,
                    UserMessagePayload(UserMessage.Text "hi"),
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let! lease =
                store.ClaimNextTurn(tenant, session.Id, "owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)

            let claim =
                match lease with
                | :? TurnLeaseHeld as held -> held.Claim
                | :? TurnLeaseRenewed as renewed -> renewed.Claim
                | _ -> failwith "Expected a held claim."

            let stale = { claim with Token = "stale" }

            Assert.False(
                (settlement.AdmitExecution(tenant, session.Id, entry.Position, stale, CancellationToken.None).Result)
            )

            let request =
                SessionSettlementRequest(
                    session.Id,
                    entry.Position,
                    stale,
                    Nullable(),
                    okResult TurnStatus.Completed,
                    Guid.NewGuid().ToString("N"),
                    null
                )

            let! rejected = settlement.SettleExecution(tenant, request, CancellationToken.None)
            Assert.Equal(SessionSettlementStatus.Rejected, rejected.Status)
        }
        :> Task
