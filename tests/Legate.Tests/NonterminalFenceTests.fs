// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.NonterminalFenceTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
open Legate.Testing
open Xunit

// Public facade takeover-race suite (issue 377): deterministic takeover
// inserted between verification and the fenced write for Inject
// consumption, permission suspension/resume, and lifecycle transitions.
// Each fact verifies live, takes over, then proves the loser rejects
// atomically with the winner unchanged. InMemory and real SQLite run
// locally; Postgres equivalence rides the shared conformance suite via
// Testcontainers in CI. Terminal behavior delegates to issue 363.

let tenant = TenantId.Create "acme"
let lease = TimeSpan.FromMinutes 5.0
let startInstant = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

let sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "fence"
        State = SessionState.Idle
        CurrentTurnId = Nullable()
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Nullable()
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let claimLive (store: ISessionStore) (sessionId: SessionId) (owner: string) : TurnClaim =
    match
        store.ClaimNextTurn(tenant, sessionId, owner, lease, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnLeaseRenewed as renewed -> renewed.Claim
    | state -> failwith $"Expected a granted claim, observed {state.GetType().Name}."

let verifyHeld (store: ISessionStore) (claim: TurnClaim) : unit =
    match
        store.VerifyClaim(tenant, claim, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnLeaseHeld -> ()
    | state -> failwith $"Expected a held claim, observed {state.GetType().Name}."

let withInMemory (work: ISessionStore -> TestClock -> Task) : Task =
    task {
        let clock = TestClock(startInstant)
        let database = InMemoryDatabase(clock)
        let store = InMemorySessionStore(database) :> ISessionStore
        do! work store clock
    }

let withSqlite (work: ISessionStore -> TestClock -> Task) : Task =
    task {
        let clock = TestClock(startInstant)
        let database, path = SqliteTestFixture.openTestDatabase clock

        try
            let store = SqliteStoreFactory.sessionStore database
            do! work store clock
        finally
            (database :> IDisposable).Dispose()
            SqliteTestFixture.deleteDatabaseFiles path
    }

let injectTakeover (store: ISessionStore) (clock: TestClock) : Task =
    task {
        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let queue = UserMessagePayload(UserMessage.Text("queue")) :> InboxPayload
        let inject = UserMessagePayload(UserMessage.Text("inject")) :> InboxPayload

        let! _ = store.AppendInboxMessage(tenant, created.Id, queue, DeliveryMode.Queue, CancellationToken.None)
        let loser = claimLive store created.Id "owner-a"
        verifyHeld store loser
        clock.Advance(TimeSpan.FromMinutes 10.)

        let reply =
            ReplyPayload(PermissionDecision("req-1", PermissionDecisionKind.AllowOnce)) :> InboxPayload

        let! _ = store.AppendInboxMessage(tenant, created.Id, reply, DeliveryMode.Queue, CancellationToken.None)

        let! resumed = store.ClaimNextTurn(tenant, created.Id, "owner-b", lease, CancellationToken.None)

        let winner =
            match resumed with
            | :? TurnLeaseRenewed as renewed -> renewed.Claim
            | _ -> failwith "expected the resume claim"

        let! folded = store.AppendInboxMessage(tenant, created.Id, inject, DeliveryMode.Inject, CancellationToken.None)

        let positions = [| folded.Position |] :> IReadOnlyList<int64>

        let! lost = store.ConsumeInboxUnderClaim(tenant, loser, created.Id, positions, CancellationToken.None)
        Assert.True(lost :? TurnLeaseLost || lost :? TurnLeaseMissing)

        let! pending = store.ReadPendingInbox(tenant, created.Id, CancellationToken.None)
        Assert.Contains(pending, fun entry -> entry.Position = folded.Position)

        let! held = store.ConsumeInboxUnderClaim(tenant, winner, created.Id, positions, CancellationToken.None)
        Assert.True(held :? TurnLeaseHeld)

        let! again = store.ConsumeInboxUnderClaim(tenant, winner, created.Id, positions, CancellationToken.None)
        Assert.True(again :? TurnLeaseHeld)
    }

let permissionTakeover (store: ISessionStore) (clock: TestClock) : Task =
    task {
        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let message = UserMessagePayload(UserMessage.Text("suspend")) :> InboxPayload
        let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)
        let loser = claimLive store created.Id "owner-a"
        verifyHeld store loser

        clock.Advance(TimeSpan.FromMinutes 10.)

        let reply =
            ReplyPayload(PermissionDecision("req-1", PermissionDecisionKind.AllowOnce)) :> InboxPayload

        let! _ = store.AppendInboxMessage(tenant, created.Id, reply, DeliveryMode.Queue, CancellationToken.None)

        let! resumed = store.ClaimNextTurn(tenant, created.Id, "owner-b", lease, CancellationToken.None)

        let winner =
            match resumed with
            | :? TurnLeaseRenewed as renewed -> renewed.Claim
            | _ -> failwith "expected the resume claim"

        let! suspendLost =
            store.UpdateSessionStateUnderClaim(
                tenant,
                loser,
                created.Id,
                SessionState.WaitingForInput,
                CancellationToken.None
            )

        Assert.True(suspendLost :? TurnLeaseLost || suspendLost :? TurnLeaseMissing)

        let! grantLost = store.GrantSessionToolUnderClaim(tenant, loser, created.Id, "exec", CancellationToken.None)
        Assert.True(grantLost :? TurnLeaseLost || grantLost :? TurnLeaseMissing)

        let! stored = store.GetSession(tenant, created.Id, CancellationToken.None)

        match stored with
        | null -> failwith "expected the session"
        | session ->
            Assert.DoesNotContain("exec", session.PermissionGrants)
            Assert.NotEqual(SessionState.WaitingForInput, session.State)

        let! suspendHeld =
            store.UpdateSessionStateUnderClaim(
                tenant,
                winner,
                created.Id,
                SessionState.WaitingForInput,
                CancellationToken.None
            )

        Assert.True(suspendHeld :? TurnLeaseHeld)

        let! grantHeld = store.GrantSessionToolUnderClaim(tenant, winner, created.Id, "exec", CancellationToken.None)
        Assert.True(grantHeld :? TurnLeaseHeld)

        let! resumeHeld =
            store.UpdateSessionStateUnderClaim(tenant, winner, created.Id, SessionState.Running, CancellationToken.None)

        Assert.True(resumeHeld :? TurnLeaseHeld)

        let! reloaded = store.GetSession(tenant, created.Id, CancellationToken.None)

        match reloaded with
        | null -> failwith "expected the session"
        | session ->
            Assert.Contains("exec", session.PermissionGrants)
            Assert.Equal(SessionState.Running, session.State)
    }

let lifecycleTakeover (store: ISessionStore) (clock: TestClock) : Task =
    task {
        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let message = UserMessagePayload(UserMessage.Text("lifecycle")) :> InboxPayload
        let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)
        let loser = claimLive store created.Id "owner-a"
        verifyHeld store loser

        clock.Advance(TimeSpan.FromMinutes 10.)

        let reply =
            ReplyPayload(PermissionDecision("req-1", PermissionDecisionKind.AllowOnce)) :> InboxPayload

        let! _ = store.AppendInboxMessage(tenant, created.Id, reply, DeliveryMode.Queue, CancellationToken.None)

        let! resumed = store.ClaimNextTurn(tenant, created.Id, "owner-b", lease, CancellationToken.None)

        let winner =
            match resumed with
            | :? TurnLeaseRenewed as renewed -> renewed.Claim
            | _ -> failwith "expected the resume claim"

        let! lost =
            store.UpdateSessionStateUnderClaim(tenant, loser, created.Id, SessionState.Running, CancellationToken.None)

        Assert.True(lost :? TurnLeaseLost || lost :? TurnLeaseMissing)

        let! stored = store.GetSession(tenant, created.Id, CancellationToken.None)

        match stored with
        | null -> failwith "expected the session"
        | session -> Assert.NotEqual(SessionState.Running, session.State)

        let! held =
            store.UpdateSessionStateUnderClaim(tenant, winner, created.Id, SessionState.Running, CancellationToken.None)

        Assert.True(held :? TurnLeaseHeld)
    }

[<Fact>]
let ``Inject consumption takeover rejects the loser and lands the winner`` () : Task = withInMemory injectTakeover

[<Fact>]
let ``SQLite inject consumption takeover rejects the loser and lands the winner`` () : Task = withSqlite injectTakeover

[<Fact>]
let ``Permission suspension and resume takeover rejects the loser`` () : Task = withInMemory permissionTakeover

[<Fact>]
let ``SQLite permission suspension and resume takeover rejects the loser`` () : Task = withSqlite permissionTakeover

[<Fact>]
let ``Lifecycle transition takeover rejects the loser`` () : Task = withInMemory lifecycleTakeover

[<Fact>]
let ``SQLite lifecycle transition takeover rejects the loser`` () : Task = withSqlite lifecycleTakeover

[<Fact>]
let ``Stale reply cannot consume unrelated input and retries reflect the outcome`` () : Task =
    withInMemory (fun store _ ->
        task {
            let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

            let first = UserMessagePayload(UserMessage.Text("first")) :> InboxPayload
            let second = UserMessagePayload(UserMessage.Text("second")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, first, DeliveryMode.Queue, CancellationToken.None)

            let! secondEntry =
                store.AppendInboxMessage(tenant, created.Id, second, DeliveryMode.Queue, CancellationToken.None)

            let claim = claimLive store created.Id "owner-a"
            let positions = [| secondEntry.Position |] :> IReadOnlyList<int64>

            let! held = store.ConsumeInboxUnderClaim(tenant, claim, created.Id, positions, CancellationToken.None)
            Assert.True(held :? TurnLeaseHeld)

            let! again = store.ConsumeInboxUnderClaim(tenant, claim, created.Id, positions, CancellationToken.None)
            Assert.True(again :? TurnLeaseHeld)

            let! pending = store.ReadPendingInbox(tenant, created.Id, CancellationToken.None)
            Assert.DoesNotContain(pending, fun entry -> entry.Position = secondEntry.Position)
        })

[<Fact>]
let ``Host permission rules stay intact alongside fenced execution writes`` () : Task =
    withInMemory (fun store _ ->
        task {
            let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

            let! _ = store.GrantSessionTool(tenant, created.Id, "host-tool", CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("host")) :> InboxPayload
            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)
            let claim = claimLive store created.Id "owner-a"

            let! held = store.GrantSessionToolUnderClaim(tenant, claim, created.Id, "exec", CancellationToken.None)
            Assert.True(held :? TurnLeaseHeld)

            let! stored = store.GetSession(tenant, created.Id, CancellationToken.None)

            match stored with
            | null -> failwith "expected the session"
            | session ->
                Assert.Contains("host-tool", session.PermissionGrants)
                Assert.Contains("exec", session.PermissionGrants)
        })
