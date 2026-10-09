// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
open Xunit

/// Atomic terminal settlement conformance (issue 363): the narrow provider-supported
/// capability shared by InMemory and real SQLite. Each provider proves Applied,
/// identical and conflicting retry, stale-authority zero-effects, and host-closed
/// precedence under one takeover-serializing boundary.
module SessionSettlementHelpers =

    let tenant = TenantId.Create "settlement"

    let agent = AgentId.New()

    let freshSession (autoClose: bool) =
        let options = SessionOptions()
        options.FormatVersion <- 1
        options.AutoClose <- autoClose

        {
            Id = SessionId.New()
            Tenant = tenant
            AgentId = agent
            Title = "settlement"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.UtcNow
            UpdatedAt = DateTimeOffset.UtcNow
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = options
            PermissionGrants = Array.empty<string> :> System.Collections.Generic.IReadOnlyList<string>
        }

    let okResult (status: TurnStatus) =
        {
            AssistantText = "done"
            Status = status
            Iterations = 1
            Usage =
                {
                    InputTokens = 10L
                    OutputTokens = 20L
                }
            Outcome = TurnFinished("done") :> TurnOutcome
        }

    let claimFor (turn: TurnId) (attempt: int) =
        {
            TurnId = turn
            Token = Ulid.NewUlid().ToString()
            Owner = "owner"
            ExpiresAt = DateTimeOffset.UtcNow.AddMinutes 5.0
            Attempt = attempt
        }

    let setupAsync (store: ISessionStore) (autoClose: bool) =
        task {
            let session = freshSession autoClose
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

            return session, entry, claim
        }

    let requestFor (session: Session) (entry: InboxEntry) (claim: TurnClaim) (key: string) =
        SessionSettlementRequest(
            session.Id,
            entry.Position,
            claim,
            Nullable(),
            okResult TurnStatus.Completed,
            key,
            null
        )

type InMemorySessionSettlementTests() =

    let database = InMemoryDatabase(TimeProvider.System, InMemoryStoreOptions())
    let store = InMemorySessionStore(database) :> ISessionStore
    let settlement = store :?> ISessionSettlementStore
    let events = InMemorySessionEventStore(database) :> ISessionEventStore

    [<Fact>]
    member _.``supports matching journal rejects foreign journal``() =
        Assert.True(settlement.SupportsSettlementJournal events)
        let foreign = InMemorySessionEventStore(InMemoryDatabase()) :> ISessionEventStore
        Assert.False(settlement.SupportsSettlementJournal foreign)

    [<Fact>]
    member _.``applied commits then identical retry returns already applied``() =
        task {
            let! session, entry, claim = SessionSettlementHelpers.setupAsync store false

            Assert.True(
                (settlement
                    .AdmitExecution(
                        SessionSettlementHelpers.tenant,
                        session.Id,
                        entry.Position,
                        claim,
                        CancellationToken.None
                    )
                    .Result)
            )

            let key = Guid.NewGuid().ToString("N")

            let! applied =
                settlement.SettleExecution(
                    SessionSettlementHelpers.tenant,
                    SessionSettlementHelpers.requestFor session entry claim key,
                    CancellationToken.None
                )

            Assert.Equal(SessionSettlementStatus.Applied, applied.Status)
            Assert.Equal(SessionState.Idle, applied.State)

            let! again =
                settlement.SettleExecution(
                    SessionSettlementHelpers.tenant,
                    SessionSettlementHelpers.requestFor session entry claim key,
                    CancellationToken.None
                )

            Assert.Equal(SessionSettlementStatus.AlreadyApplied, again.Status)
            Assert.Equal(applied.State, again.State)
        }
        :> Task

    [<Fact>]
    member _.``conflicting retry rejects without effects``() =
        task {
            let! session, entry, claim = SessionSettlementHelpers.setupAsync store false

            Assert.True(
                (settlement
                    .AdmitExecution(
                        SessionSettlementHelpers.tenant,
                        session.Id,
                        entry.Position,
                        claim,
                        CancellationToken.None
                    )
                    .Result)
            )

            let key = Guid.NewGuid().ToString("N")

            let! applied =
                settlement.SettleExecution(
                    SessionSettlementHelpers.tenant,
                    SessionSettlementHelpers.requestFor session entry claim key,
                    CancellationToken.None
                )

            Assert.Equal(SessionSettlementStatus.Applied, applied.Status)

            let other =
                SessionSettlementRequest(
                    session.Id,
                    entry.Position,
                    claim,
                    Nullable(),
                    SessionSettlementHelpers.okResult TurnStatus.Failed,
                    Guid.NewGuid().ToString("N"),
                    null
                )

            let! rejected = settlement.SettleExecution(SessionSettlementHelpers.tenant, other, CancellationToken.None)
            Assert.Equal(SessionSettlementStatus.Rejected, rejected.Status)
        }
        :> Task

    [<Fact>]
    member _.``stale claim admits nothing and settles rejected``() =
        task {
            let! session, entry, claim = SessionSettlementHelpers.setupAsync store false
            let stale = { claim with Token = "stale" }

            Assert.False(
                (settlement
                    .AdmitExecution(
                        SessionSettlementHelpers.tenant,
                        session.Id,
                        entry.Position,
                        stale,
                        CancellationToken.None
                    )
                    .Result)
            )

            let! rejected =
                settlement.SettleExecution(
                    SessionSettlementHelpers.tenant,
                    SessionSettlementHelpers.requestFor session entry stale (Guid.NewGuid().ToString("N")),
                    CancellationToken.None
                )

            Assert.Equal(SessionSettlementStatus.Rejected, rejected.Status)
            let! pending = store.ReadPendingInbox(SessionSettlementHelpers.tenant, session.Id, CancellationToken.None)
            Assert.Equal(0, pending.Count)
        }
        :> Task

    [<Fact>]
    member _.``host closed session rejects settlement``() =
        task {
            let! session, entry, claim = SessionSettlementHelpers.setupAsync store false

            Assert.True(
                (settlement
                    .AdmitExecution(
                        SessionSettlementHelpers.tenant,
                        session.Id,
                        entry.Position,
                        claim,
                        CancellationToken.None
                    )
                    .Result)
            )

            let! _ = store.CloseSession(SessionSettlementHelpers.tenant, session.Id, CancellationToken.None)

            let! rejected =
                settlement.SettleExecution(
                    SessionSettlementHelpers.tenant,
                    SessionSettlementHelpers.requestFor session entry claim (Guid.NewGuid().ToString("N")),
                    CancellationToken.None
                )

            Assert.Equal(SessionSettlementStatus.Rejected, rejected.Status)
            Assert.Equal(SessionState.Closed, rejected.State)
        }
        :> Task

type SqliteSessionSettlementTests() =

    let openDatabase () =
        let path =
            System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"legate-settlement-{Guid.NewGuid():N}.db")

        let database = SqliteDatabase.Open(path)
        database, path

    [<Fact>]
    member _.``sqlite applied then already applied``() =
        task {
            let database, path = openDatabase ()

            try
                let store = SqliteSessionStore(database) :> ISessionStore
                let settlement = SqliteSessionSettlementStore(database) :> ISessionSettlementStore
                let events = SqliteSessionEventStore(database) :> ISessionEventStore
                Assert.True(settlement.SupportsSettlementJournal events)
                let! session, entry, claim = SessionSettlementHelpers.setupAsync store false

                Assert.True(
                    (settlement
                        .AdmitExecution(
                            SessionSettlementHelpers.tenant,
                            session.Id,
                            entry.Position,
                            claim,
                            CancellationToken.None
                        )
                        .Result)
                )

                let key = Guid.NewGuid().ToString("N")

                let! applied =
                    settlement.SettleExecution(
                        SessionSettlementHelpers.tenant,
                        SessionSettlementHelpers.requestFor session entry claim key,
                        CancellationToken.None
                    )

                Assert.Equal(SessionSettlementStatus.Applied, applied.Status)

                let! again =
                    settlement.SettleExecution(
                        SessionSettlementHelpers.tenant,
                        SessionSettlementHelpers.requestFor session entry claim key,
                        CancellationToken.None
                    )

                Assert.Equal(SessionSettlementStatus.AlreadyApplied, again.Status)
            finally
                (database :> IDisposable).Dispose()

                SqliteTestFixture.deleteDatabaseFiles path
        }
        :> Task

    [<Fact>]
    member _.``sqlite stale claim rejects with zero effects``() =
        task {
            let database, path = openDatabase ()

            try
                let store = SqliteSessionStore(database) :> ISessionStore
                let settlement = SqliteSessionSettlementStore(database) :> ISessionSettlementStore
                let! session, entry, claim = SessionSettlementHelpers.setupAsync store false
                let stale = { claim with Token = "stale" }

                Assert.False(
                    (settlement
                        .AdmitExecution(
                            SessionSettlementHelpers.tenant,
                            session.Id,
                            entry.Position,
                            stale,
                            CancellationToken.None
                        )
                        .Result)
                )

                let! rejected =
                    settlement.SettleExecution(
                        SessionSettlementHelpers.tenant,
                        SessionSettlementHelpers.requestFor session entry stale (Guid.NewGuid().ToString("N")),
                        CancellationToken.None
                    )

                Assert.Equal(SessionSettlementStatus.Rejected, rejected.Status)
            finally
                (database :> IDisposable).Dispose()

                SqliteTestFixture.deleteDatabaseFiles path
        }
        :> Task
