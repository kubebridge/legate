// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TenantRoutingDuplicateTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Xunit

let private session tenant id title : Session =
    {
        Id = id
        Tenant = tenant
        AgentId = AgentId.New()
        Title = title
        State = SessionState.Idle
        CurrentTurnId = Nullable()
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Nullable()
        WorkspaceBinding = title
        Options = SessionOptions()
        PermissionGrants = Array.empty<string> :> IReadOnlyList<string>
    }

[<Fact>]
let ``issue395 InMemory rejects sequential and concurrent duplicate ids but permits equal ids in independent databases``
    ()
    : Task =
    task {
        let tenant = TenantId.Create "duplicate-tenant"
        let tenantB = TenantId.Create "duplicate-tenant-b"
        let id = SessionId.New()
        let firstDatabase = InMemoryDatabase()
        let firstStore = InMemorySessionStore(firstDatabase) :> ISessionStore
        let first = session tenant id "first"

        let! created = firstStore.CreateSession(tenant, first, CancellationToken.None)
        Assert.Equal("first", created.Title)

        let! pending =
            firstStore.AppendInboxMessage(
                tenant,
                id,
                UserMessagePayload(UserMessage.Text "pending-authority"),
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! lease =
            firstStore.ClaimNextTurn(tenant, id, "cross-tenant-owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)

        let claim = (lease :?> TurnLeaseRenewed).Claim
        let control = firstStore :?> ISessionAbortControlStore

        let! _ = control.BindControlTarget(tenant, id, claim.TurnId, pending.Position, claim, CancellationToken.None)

        let! pendingAuthority =
            firstStore.AppendInboxMessage(
                tenant,
                id,
                UserMessagePayload(UserMessage.Text "pending-authority-after-claim"),
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! crossTenantDuplicate =
            Assert.ThrowsAsync<InvalidSessionStateException>(fun () ->
                firstStore.CreateSession(tenantB, session tenantB id "cross-tenant-duplicate", CancellationToken.None)
                :> Task)

        Assert.Contains("already exists", crossTenantDuplicate.Message, StringComparison.OrdinalIgnoreCase)
        let! preserved = firstStore.GetSession(tenant, id, CancellationToken.None)

        match preserved with
        | null -> failwith "The original in-memory session disappeared after the cross-tenant duplicate."
        | present -> Assert.Equal("first", present.Title)

        let! preservedPending = firstStore.ReadPendingInbox(tenant, id, CancellationToken.None)
        Assert.Contains(preservedPending, fun entry -> entry.Position = pendingAuthority.Position)
        let! preservedTarget = control.ReadAbortTarget(tenant, id, CancellationToken.None)
        Assert.NotNull(preservedTarget)

        let! _ =
            Assert.ThrowsAsync<InvalidSessionStateException>(fun () ->
                firstStore.CreateSession(tenant, session tenant id "sequential-duplicate", CancellationToken.None)
                :> Task)

        let concurrentAttempts =
            [| 1..16 |]
            |> Array.map (fun number ->
                task {
                    try
                        let! _ =
                            firstStore.CreateSession(
                                tenant,
                                session tenant id $"concurrent-{number}",
                                CancellationToken.None
                            )

                        return true
                    with :? InvalidSessionStateException ->
                        return false
                })

        let! outcomes = Task.WhenAll concurrentAttempts
        Assert.Equal(0, outcomes |> Array.filter (fun outcome -> outcome) |> Array.length)

        let crossTenantRaceId = SessionId.New()

        let crossTenantAttempts =
            [| 0..15 |]
            |> Array.map (fun number ->
                task {
                    let winnerTenant = if number % 2 = 0 then tenant else tenantB

                    try
                        let! _ =
                            firstStore.CreateSession(
                                winnerTenant,
                                session winnerTenant crossTenantRaceId $"cross-race-{number}",
                                CancellationToken.None
                            )

                        return winnerTenant, true
                    with :? InvalidSessionStateException ->
                        return winnerTenant, false
                })

        let! crossTenantOutcomes = Task.WhenAll crossTenantAttempts
        Assert.Equal(1, crossTenantOutcomes |> Array.filter snd |> Array.length)

        for candidateTenant in [ tenant; tenantB ] do
            let! row = firstStore.GetSession(candidateTenant, crossTenantRaceId, CancellationToken.None)

            if isNull row then
                let! loserControl =
                    Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                        control.ReadAbortTarget(candidateTenant, crossTenantRaceId, CancellationToken.None) :> Task)

                Assert.Equal(crossTenantRaceId, loserControl.SessionId)

        let independentDatabase = InMemoryDatabase()
        let independentStore = InMemorySessionStore(independentDatabase) :> ISessionStore

        let! independent =
            independentStore.CreateSession(tenantB, session tenantB id "independent", CancellationToken.None)

        Assert.Equal(id, independent.Id)
        Assert.Equal("independent", independent.Title)
    }

[<Fact>]
let ``issue395 exact target abort accepts directly while route is unavailable and preserves the pending barrier``
    ()
    : Task =
    task {
        let tenant = TenantId.Create "abort-direct-395"
        let database = InMemoryDatabase()
        let store = InMemorySessionStore(database) :> ISessionStore
        let events = InMemorySessionEventStore(database) :> ISessionEventStore
        let control = store :?> ISessionAbortControlStore
        let sessionId = SessionId.New()
        let! _ = store.CreateSession(tenant, session tenant sessionId "abort-direct", CancellationToken.None)

        let! entry =
            store.AppendInboxMessage(
                tenant,
                sessionId,
                UserMessagePayload(UserMessage.Text "original"),
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! lease =
            store.ClaimNextTurn(tenant, sessionId, "original-owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)

        let claim = (lease :?> TurnLeaseRenewed).Claim
        let targetTurn = TurnId.New()
        let! _ = control.BindControlTarget(tenant, sessionId, targetTurn, entry.Position, claim, CancellationToken.None)
        let! _ = store.UpdateSessionState(tenant, sessionId, SessionState.Running, CancellationToken.None)

        let mutable routeCalls = 0

        let unavailableRoute _ _ =
            Interlocked.Increment(&routeCalls) |> ignore
            Task.FromException<IActorRef>(SessionScopeRejectedException(SessionScopeRejectionReason.ScopeUnavailable))

        let bus = new SessionEventBus(events, SessionSubscriptionOptions(), null)

        let client =
            new SessionClient(
                store,
                tenant,
                unavailableRoute,
                bus,
                TimeSpan.FromMinutes 1.0,
                RecordingDelay() :> ILlmDelay,
                None
            )

        let! first =
            SessionClientOperations.AbortAsync(
                client,
                sessionId,
                targetTurn,
                StopCause.ExplicitAbort,
                "first-direct-abort",
                CancellationToken.None
            )

        let! retry =
            SessionClientOperations.AbortAsync(
                client,
                sessionId,
                targetTurn,
                StopCause.HostShutdown,
                "different-retry-must-not-win",
                CancellationToken.None
            )

        Assert.Equal(HostAbortOutcome.Accepted, first.Outcome)
        Assert.Equal(HostAbortOutcome.AlreadyAccepted, retry.Outcome)
        Assert.Equal(first.AcceptedAt, retry.AcceptedAt)
        Assert.Equal(first.Cause, retry.Cause)
        Assert.Equal(first.Reason, retry.Reason)
        Assert.Equal(0, routeCalls)

        let! target = control.ReadAbortTarget(tenant, sessionId, CancellationToken.None)

        match target with
        | null -> failwith "The direct abort did not leave a pending target barrier."
        | target ->
            Assert.Equal(ControlTargetState.Active, target.State)

            match target.Stop with
            | null -> failwith "The direct abort target has no immutable receipt."
            | stop ->
                Assert.Equal(HostAbortOutcome.Accepted, stop.Outcome)
                Assert.Equal("first-direct-abort", stop.Reason)

        let! held = store.VerifyClaim(tenant, claim, CancellationToken.None)
        Assert.IsType<TurnLeaseHeld>(held) |> ignore

        let! recovery =
            control.TryRecoverControlTarget(
                tenant,
                sessionId,
                targetTurn,
                "fresh-owner-must-not-activate",
                TimeSpan.FromMinutes 5.0,
                CancellationToken.None
            )

        Assert.Equal(ControlOperationOutcome.Stopped, recovery.Outcome)
        Assert.Null(recovery.Claim)
        Assert.Equal(0, routeCalls)
    }
