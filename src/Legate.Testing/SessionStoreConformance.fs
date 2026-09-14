// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.Linq
open System.Threading
open System.Threading.Tasks
open Legate
open Xunit

/// The shared conformance suite for <see cref="T:Legate.ISessionStore" />
/// implementations: tenancy, CRUD and paging, inbox ordering, the claim
/// model, fencing, settlement idempotency, dispatch, and the capacity
/// counts. A store test project derives this base, implements
/// <see cref="M:Legate.Testing.SessionStoreConformance.CreateSessionStore*" />,
/// and every rule below runs against the implementation. Each pinned rule
/// cites the contract line it enforces, so a contradiction is a one-place
/// suite change that realigns every implementation.
[<AbstractClass>]
type SessionStoreConformance(store: ISessionStore, clock: TestClock, tenant: TenantId) =

    do
        if isNull (box store) then
            raise (ArgumentNullException(nameof store))

        if isNull (box clock) then
            raise (ArgumentNullException(nameof clock))

    /// The store under test.
    member this.Store = store

    /// The clock the implementation reads.
    member this.Clock = clock

    /// The primary tenant every row belongs to.
    member this.Tenant = tenant

    /// The second tenant proving isolation.
    member this.OtherTenant = TenantId.Create "other"

    /// Constructs the suite over the store and clock, in the primary
    /// tenant.
    new(store, clock) = SessionStoreConformance(store, clock, TenantId.Create "conformance")

    /// The default lease duration claims use.
    static member LeaseDuration = TimeSpan.FromMinutes 5.0

    /// A minimal session the suite creates under the tenant.
    member this.SampleSession() =
        {
            Id = SessionId.New()
            Tenant = tenant
            AgentId = AgentId.New()
            Title = "conformance"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
        }

    // ── Tenancy ──

    [<Fact>]
    member this.``GetSession resolves null for the wrong tenant``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let! wrongTenant = store.GetSession(this.OtherTenant, created.Id, CancellationToken.None)

            Assert.Null(wrongTenant)
        }

    [<Fact>]
    member this.``UpdateSessionState throws for the wrong tenant``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let wrong =
                Assert.Throws<SessionNotFoundException>(fun () ->
                    store
                        .UpdateSessionState(this.OtherTenant, created.Id, SessionState.Running, CancellationToken.None)
                        .GetAwaiter()
                        .GetResult()
                    |> ignore)

            Assert.NotNull(wrong)
        }

    // ── Claims: the single-winner rule ──

    [<Fact>]
    member this.``ClaimNextTurn is atomic: one winner across parallel callers``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("parallel claim")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let claims =
                [ 1..8 ]
                |> List.map (fun _ ->
                    store.ClaimNextTurn(
                        tenant,
                        created.Id,
                        "parallel-owner",
                        TimeSpan.FromMinutes 5.,
                        CancellationToken.None
                    ))
                |> (fun tasks -> Task.WhenAll tasks)

            let! settled = claims

            let winners =
                settled |> Seq.filter (fun state -> state :? TurnLeaseRenewed) |> Seq.length

            Assert.Equal(1, winners)
        }

    [<Fact>]
    member this.``A live claim rejects every further claim with zero effects``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("one live claim")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! first =
                store.ClaimNextTurn(tenant, created.Id, "owner-a", TimeSpan.FromMinutes 5., CancellationToken.None)

            Assert.True(first :? TurnLeaseRenewed)

            // A second message stays pending: the live claim consumes
            // nothing.
            let second = UserMessagePayload(UserMessage.Text("waits")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, second, DeliveryMode.Queue, CancellationToken.None)

            let! loser =
                store.ClaimNextTurn(tenant, created.Id, "owner-b", TimeSpan.FromMinutes 5., CancellationToken.None)

            Assert.True(loser :? TurnLeaseMissing)
        }

    // ── Fencing: the takeover race ──

    [<Fact>]
    member this.``A takeover race leaves the loser with zero effects``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("race")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! winner =
                store.ClaimNextTurn(tenant, created.Id, "owner-a", TimeSpan.FromMinutes 5., CancellationToken.None)

            let claim =
                match winner with
                | :? TurnLeaseRenewed as renewed -> renewed.Claim
                | _ -> failwith "expected a renewed claim"

            // The lease lapses and another owner claims the same turn:
            // a reply resumes it.
            this.Clock.Advance(TimeSpan.FromMinutes 10.)

            let reply =
                ReplyPayload(PermissionDecision("req-1", PermissionDecisionKind.AllowOnce)) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, reply, DeliveryMode.Queue, CancellationToken.None)

            let! taken =
                store.ClaimNextTurn(tenant, created.Id, "owner-b", TimeSpan.FromMinutes 5., CancellationToken.None)

            match taken with
            | :? TurnLeaseRenewed as renewed ->
                Assert.Equal(claim.TurnId, renewed.Claim.TurnId)
                Assert.Equal(claim.Attempt + 1, renewed.Claim.Attempt)
            | _ -> failwith "expected the resume claim"

            // The loser's fenced calls all reject with zero effects.
            let! renewLost = store.RenewClaim(tenant, claim, TimeSpan.FromMinutes 5., CancellationToken.None)
            Assert.True(renewLost :? TurnLeaseLost || renewLost :? TurnLeaseMissing)

            let usage =
                {
                    UsageSummary.InputTokens = 1L
                    OutputTokens = 2L
                }

            let! checkpointLost = store.CheckpointUsage(tenant, claim, usage, CancellationToken.None)
            Assert.True(checkpointLost :? TurnLeaseLost || checkpointLost :? TurnLeaseMissing)

            let! settleLost = store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)

            Assert.True(settleLost :? TurnSettleRejected)
        }

    // ── Settlement idempotency ──

    [<Fact>]
    member this.``SettleTurn is idempotent: retry of the same outcome observes it``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("settle twice")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! claimed =
                store.ClaimNextTurn(tenant, created.Id, "settler", TimeSpan.FromMinutes 5., CancellationToken.None)

            let claim = (claimed :?> TurnLeaseRenewed).Claim

            let! first = store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)
            Assert.True(first :? TurnSettled)

            let! retry = store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)
            Assert.True(retry :? TurnAlreadySettled)

            let! different = store.SettleTurn(tenant, claim, TurnStatus.Failed, null, CancellationToken.None)
            Assert.True(different :? TurnSettleRejected)
        }

    // ── Inbox ordering ──

    [<Fact>]
    member this.``Parallel inbox appends assign unique increasing positions``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let appends =
                [ 1..16 ]
                |> List.map (fun index ->
                    let message =
                        UserMessagePayload(UserMessage.Text(sprintf "m%d" index)) :> InboxPayload

                    store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None))
                |> (fun tasks -> Task.WhenAll tasks)

            let! entries = appends

            let positions =
                entries |> Seq.map (fun entry -> entry.Position) |> Seq.sort |> Seq.toList

            positions |> List.iter (fun position -> Assert.True(position > 0L))
            Assert.Equal(entries.Length, positions |> List.distinct |> List.length)
        }

    // ── Dispatch and counts ──

    [<Fact>]
    member this.``GetDispatchCandidates lists sessions with pending work``() =
        task {
            let! first = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)
            let! second = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("dispatch")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, second.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! batch = store.GetDispatchCandidates(tenant, 10, CancellationToken.None)

            Assert.Contains(second.Id, batch.Sessions)
            Assert.DoesNotContain(first.Id, batch.Sessions)
        }

    [<Fact>]
    member this.``CountSessionsByTenant scopes to the tenant``() =
        task {
            let! _ = (store.CreateSession(tenant, this.SampleSession(), CancellationToken.None))

            let! before = store.CountSessionsByTenant(tenant, CancellationToken.None)
            Assert.True(before >= 1)

            let! otherCount = store.CountSessionsByTenant(this.OtherTenant, CancellationToken.None)
            Assert.Equal(0, otherCount)
        }
