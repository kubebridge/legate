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
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    /// Establishes a genuine prime and a separately bound real-entry control target.
    member this.ControlWork() =
        task {
            let control = store :?> ISessionAbortControlStore
            let! session = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let! _ =
                store.AppendInboxMessage(
                    tenant,
                    session.Id,
                    UserMessagePayload(UserMessage.Text "prime"),
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let! lease =
                store.ClaimNextTurn(
                    tenant,
                    session.Id,
                    "control-owner",
                    TimeSpan.FromMinutes 5.0,
                    CancellationToken.None
                )

            let claim =
                match lease with
                | :? TurnLeaseRenewed as lease -> lease.Claim
                | _ -> failwith "Prime not established."

            let! entry =
                store.AppendInboxMessage(
                    tenant,
                    session.Id,
                    UserMessagePayload(UserMessage.Text "real entry"),
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let turn = TurnId.New()

            let! binding =
                control.BindControlTarget(tenant, session.Id, turn, entry.Position, claim, CancellationToken.None)

            Assert.Equal(ControlOperationOutcome.Applied, binding.Outcome)
            let! _ = store.UpdateSessionState(tenant, session.Id, SessionState.Running, CancellationToken.None)
            return control, session.Id, entry, turn, claim
        }

    /// Acceptance changes only intent, with immutable retries and exact tenant isolation.
    [<Fact>]
    member this.``control acceptance preserves current work and immutable first receipt``() =
        task {
            let! control, session, entry, turn, claim = this.ControlWork()

            for delivery in
                [
                    DeliveryMode.Queue
                    DeliveryMode.Inject
                    DeliveryMode.Interrupt
                ] do
                let! _ =
                    store.AppendInboxMessage(
                        tenant,
                        session,
                        UserMessagePayload(UserMessage.Text "unrelated"),
                        delivery,
                        CancellationToken.None
                    )

                ()

            let! before = store.ReadPendingInbox(tenant, session, CancellationToken.None)
            let! sessionBefore = store.GetSession(tenant, session, CancellationToken.None)

            let! accepted =
                control.RequestHostAbort(tenant, session, turn, StopCause.HostShutdown, "first", CancellationToken.None)

            let! duplicate =
                control.RequestHostAbort(
                    tenant,
                    session,
                    turn,
                    StopCause.ExplicitAbort,
                    "second",
                    CancellationToken.None
                )

            Assert.Equal(HostAbortOutcome.Accepted, accepted.Outcome)
            Assert.Equal(HostAbortOutcome.AlreadyAccepted, duplicate.Outcome)
            Assert.Equal(accepted.AcceptedAt, duplicate.AcceptedAt)
            Assert.Equal(accepted.Cause, duplicate.Cause)
            Assert.Equal("first", duplicate.Reason)

            match typeof<HostAbortReceipt>.GetProperty("Reason") with
            | null -> failwith "Missing C# setter"
            | property -> property.SetValue(accepted, "caller mutation")

            let! again =
                control.RequestHostAbort(
                    tenant,
                    session,
                    turn,
                    StopCause.ExplicitAbort,
                    "third",
                    CancellationToken.None
                )

            Assert.Equal("first", again.Reason)
            let! after = store.ReadPendingInbox(tenant, session, CancellationToken.None)
            Assert.Equal<int64>(before |> Seq.map _.Position, after |> Seq.map _.Position)
            Assert.Equal<bool>(before |> Seq.map _.Consumed, after |> Seq.map _.Consumed)
            let! sessionAfter = store.GetSession(tenant, session, CancellationToken.None)

            Assert.Equal(
                System.Text.Json.JsonSerializer.Serialize(sessionBefore),
                System.Text.Json.JsonSerializer.Serialize(sessionAfter)
            )

            let! verified = store.VerifyClaim(tenant, claim, CancellationToken.None)

            let current =
                match verified with
                | :? TurnLeaseHeld as lease -> lease.Claim
                | _ -> failwith "Claim changed."

            Assert.Equal(claim, current)

            let! admission =
                control.CheckControlTarget(tenant, session, turn, entry.Position, claim, CancellationToken.None)

            Assert.Equal(ControlOperationOutcome.Stopped, admission.Outcome)

            let! _ =
                Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                    control.RequestHostAbort(
                        this.OtherTenant,
                        session,
                        turn,
                        StopCause.ExplicitAbort,
                        "wrong",
                        CancellationToken.None
                    ))

            let! stale =
                control.RequestHostAbort(
                    tenant,
                    session,
                    TurnId.New(),
                    StopCause.ExplicitAbort,
                    "stale",
                    CancellationToken.None
                )

            Assert.Equal(HostAbortOutcome.TargetChanged, stale.Outcome)
        }

    /// All terminal statuses retire one entry while retaining the genuine shared prime.
    [<Theory>]
    [<InlineData(TurnStatus.Completed)>]
    [<InlineData(TurnStatus.Failed)>]
    [<InlineData(TurnStatus.Aborted)>]
    member this.``control retirement isolates two real entries sharing a prime``(status: TurnStatus) =
        task {
            let! control, session, entry, turn, claim = this.ControlWork()

            let cause, reason =
                if status = TurnStatus.Aborted then
                    Nullable StopCause.ExplicitAbort, ("local": string | null)
                else
                    Nullable(), null

            let id = Guid.NewGuid().ToString("N")

            let! applied =
                control.TryDecideControlTarget(
                    tenant,
                    session,
                    turn,
                    entry.Position,
                    claim,
                    id,
                    status,
                    cause,
                    reason,
                    CancellationToken.None
                )

            Assert.Equal(ControlOperationOutcome.Applied, applied.Outcome)

            let evidence =
                match applied.Decision with
                | null -> failwith "Missing evidence"
                | decision -> decision

            Assert.Equal(status, evidence.Status)

            let! stopped =
                control.RequestHostAbort(
                    tenant,
                    session,
                    turn,
                    StopCause.HostShutdown,
                    "too late",
                    CancellationToken.None
                )

            Assert.Equal(HostAbortOutcome.AlreadyTerminal, stopped.Outcome)
            Assert.Equal(Nullable status, stopped.TerminalStatus)

            let! notReady =
                control.RetireControlTarget(tenant, session, turn, entry.Position, claim, id, CancellationToken.None)

            Assert.Equal(ControlOperationOutcome.NotReady, notReady.Outcome)
            let! _ = store.MarkInboxConsumed(tenant, session, [| entry.Position |], CancellationToken.None)

            let! retired =
                control.RetireControlTarget(tenant, session, turn, entry.Position, claim, id, CancellationToken.None)

            Assert.Equal(ControlOperationOutcome.Applied, retired.Outcome)

            let! next =
                store.AppendInboxMessage(
                    tenant,
                    session,
                    UserMessagePayload(UserMessage.Text "next"),
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let nextTurn = TurnId.New()

            let! nextBinding =
                control.BindControlTarget(tenant, session, nextTurn, next.Position, claim, CancellationToken.None)

            Assert.Equal(ControlOperationOutcome.Applied, nextBinding.Outcome)

            let! late =
                control.RetireControlTarget(tenant, session, turn, entry.Position, claim, id, CancellationToken.None)

            Assert.Equal(ControlOperationOutcome.AlreadyRetired, late.Outcome)
            let! current = control.ReadAbortTarget(tenant, session, CancellationToken.None)

            match current with
            | null -> failwith "New binding was cleared"
            | target -> Assert.Equal(nextTurn, target.TurnId)

            let! admitted =
                control.CheckControlTarget(tenant, session, nextTurn, next.Position, claim, CancellationToken.None)

            Assert.Equal(ControlOperationOutcome.Applied, admitted.Outcome)
            let! verified = store.VerifyClaim(tenant, claim, CancellationToken.None)
            Assert.IsType<TurnLeaseHeld>(verified) |> ignore
        }

    /// Accepted intent wins before the control decision, even against a final successful result.
    [<Theory>]
    [<InlineData(TurnStatus.Completed)>]
    [<InlineData(TurnStatus.Failed)>]
    member this.``accepted stop wins decision and survives terminal retry``(proposal: TurnStatus) =
        task {
            let! control, session, entry, turn, claim = this.ControlWork()

            let! receipt =
                control.RequestHostAbort(
                    tenant,
                    session,
                    turn,
                    StopCause.HostShutdown,
                    "shutdown",
                    CancellationToken.None
                )

            let id = Guid.NewGuid().ToString("N")

            let! selected =
                control.TryDecideControlTarget(
                    tenant,
                    session,
                    turn,
                    entry.Position,
                    claim,
                    id,
                    proposal,
                    Nullable(),
                    null,
                    CancellationToken.None
                )

            let evidence =
                match selected.Decision with
                | null -> failwith "Missing decision"
                | evidence -> evidence

            Assert.Equal(TurnStatus.Aborted, evidence.Status)
            Assert.Equal(receipt.Cause, evidence.Cause)
            Assert.Equal(receipt.Reason, evidence.Reason)

            let staleClaim =
                { claim with
                    Token = "stale"
                    Owner = "loser"
                }

            let! duplicate =
                control.TryDecideControlTarget(
                    tenant,
                    session,
                    turn,
                    entry.Position,
                    staleClaim,
                    id,
                    proposal,
                    Nullable(),
                    null,
                    CancellationToken.None
                )

            Assert.Equal(ControlOperationOutcome.AlreadyDecided, duplicate.Outcome)
            Assert.Equal(selected.Decision, duplicate.Decision)

            let! changed =
                control.TryDecideControlTarget(
                    tenant,
                    session,
                    turn,
                    entry.Position,
                    claim,
                    id,
                    TurnStatus.Aborted,
                    Nullable StopCause.ExplicitAbort,
                    "different",
                    CancellationToken.None
                )

            Assert.Equal(ControlOperationOutcome.Conflict, changed.Outcome)

            let! competitor =
                control.TryDecideControlTarget(
                    tenant,
                    session,
                    turn,
                    entry.Position,
                    claim,
                    "other",
                    proposal,
                    Nullable(),
                    null,
                    CancellationToken.None
                )

            Assert.Equal(ControlOperationOutcome.DecisionConflict, competitor.Outcome)

            let! lost =
                control.RetireControlTarget(
                    tenant,
                    session,
                    turn,
                    entry.Position,
                    staleClaim,
                    id,
                    CancellationToken.None
                )

            Assert.Equal(ControlOperationOutcome.LostAuthority, lost.Outcome)
            let! _ = store.MarkInboxConsumed(tenant, session, [| entry.Position |], CancellationToken.None)

            let! _ =
                control.RetireControlTarget(tenant, session, turn, entry.Position, claim, id, CancellationToken.None)

            let! _ = store.CloseSession(tenant, session, CancellationToken.None)

            let! retry =
                control.RequestHostAbort(
                    tenant,
                    session,
                    turn,
                    StopCause.ExplicitAbort,
                    "changed",
                    CancellationToken.None
                )

            Assert.Equal(HostAbortOutcome.AlreadyAccepted, retry.Outcome)
            Assert.Equal(receipt.AcceptedAt, retry.AcceptedAt)
        }

    /// Fencing and cancellation refuse before any new control evidence is committed.
    [<Fact>]
    member this.``control rejects wrong prime association cancellation and invalid arguments``() =
        task {
            let! control, session, entry, turn, claim = this.ControlWork()
            let stale = { claim with Owner = "not the owner" }

            let! refused =
                control.TryDecideControlTarget(
                    tenant,
                    session,
                    turn,
                    entry.Position,
                    stale,
                    "decision",
                    TurnStatus.Completed,
                    Nullable(),
                    null,
                    CancellationToken.None
                )

            Assert.Equal(ControlOperationOutcome.LostAuthority, refused.Outcome)

            let! wrongPosition =
                control.TryDecideControlTarget(
                    tenant,
                    session,
                    turn,
                    entry.Position + 1L,
                    claim,
                    "decision",
                    TurnStatus.Completed,
                    Nullable(),
                    null,
                    CancellationToken.None
                )

            Assert.Equal(ControlOperationOutcome.TargetChanged, wrongPosition.Outcome)
            use cancelled = new CancellationTokenSource()
            cancelled.Cancel()

            let! _ =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () ->
                    control.RequestHostAbort(
                        tenant,
                        session,
                        turn,
                        StopCause.ExplicitAbort,
                        "cancelled",
                        cancelled.Token
                    ))

            let! _ =
                Assert.ThrowsAsync<ArgumentOutOfRangeException>(fun () ->
                    control.RequestHostAbort(
                        tenant,
                        session,
                        turn,
                        StopCause.Deadline,
                        "invalid",
                        CancellationToken.None
                    ))

            let! _ =
                Assert.ThrowsAsync<ArgumentException>(fun () ->
                    control.RequestHostAbort(
                        tenant,
                        session,
                        turn,
                        StopCause.ExplicitAbort,
                        String('x', 513),
                        CancellationToken.None
                    ))

            let! current = control.ReadAbortTarget(tenant, session, CancellationToken.None)

            match current with
            | null -> failwith "Binding lost"
            | target -> Assert.Null target.Stop
        }

    // ── Tenancy ──

    /// Unstopped recovery preserves target/entry and fences the old owner without claiming queued work.
    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    member this.``unstopped control recovery preserves original pending or consumed entry``(consumed: bool) =
        task {
            let! control, session, entry, turn, oldClaim = this.ControlWork()

            if consumed then
                let! _ = store.MarkInboxConsumed(tenant, session, [| entry.Position |], CancellationToken.None)
                ()

            let! queued =
                store.AppendInboxMessage(
                    tenant,
                    session,
                    UserMessagePayload(UserMessage.Text "unrelated"),
                    DeliveryMode.Interrupt,
                    CancellationToken.None
                )

            let! held =
                control.TryRecoverControlTarget(
                    tenant,
                    session,
                    turn,
                    "new owner",
                    TimeSpan.FromMinutes 5.0,
                    CancellationToken.None
                )

            Assert.Equal(ControlOperationOutcome.LostAuthority, held.Outcome)
            Assert.Null(held.Claim)
            clock.Advance(TimeSpan.FromMinutes 6.0)

            let! recovered =
                control.TryRecoverControlTarget(
                    tenant,
                    session,
                    turn,
                    "new owner",
                    TimeSpan.FromMinutes 5.0,
                    CancellationToken.None
                )

            Assert.Equal(ControlOperationOutcome.Applied, recovered.Outcome)

            match recovered.Claim, recovered.Entry, recovered.Target with
            | null, _, _
            | _, null, _
            | _, _, null -> failwith "Missing recovered authority or original entry"
            | claim, original, target ->
                Assert.Equal(oldClaim.TurnId, claim.TurnId)
                Assert.NotEqual<string>(oldClaim.Token, claim.Token)
                Assert.Equal(oldClaim.Attempt + 1, claim.Attempt)
                Assert.Equal(entry.Position, original.Position)
                Assert.Equal(consumed, original.Consumed)
                Assert.Equal(turn, target.TurnId)

                let! old =
                    control.CheckControlTarget(tenant, session, turn, entry.Position, oldClaim, CancellationToken.None)

                Assert.Equal(ControlOperationOutcome.LostAuthority, old.Outcome)

                let! winner =
                    control.CheckControlTarget(tenant, session, turn, entry.Position, claim, CancellationToken.None)

                Assert.Equal(ControlOperationOutcome.Applied, winner.Outcome)

            let! pending = store.ReadPendingInbox(tenant, session, CancellationToken.None)
            Assert.Contains(pending, fun item -> item.Position = queued.Position && not item.Consumed)
        }

    /// Recovery never transfers authority for accepted stop or pending terminal work.
    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    member this.``control recovery refuses stopped and terminal pending targets``(decided: bool) =
        task {
            let! control, session, entry, turn, claim = this.ControlWork()

            if decided then
                let! _ =
                    control.TryDecideControlTarget(
                        tenant,
                        session,
                        turn,
                        entry.Position,
                        claim,
                        "report",
                        TurnStatus.Completed,
                        Nullable(),
                        null,
                        CancellationToken.None
                    )

                ()
            else
                let! _ =
                    control.RequestHostAbort(
                        tenant,
                        session,
                        turn,
                        StopCause.HostShutdown,
                        "stop",
                        CancellationToken.None
                    )

                ()

            clock.Advance(TimeSpan.FromMinutes 6.0)

            let! refused =
                control.TryRecoverControlTarget(
                    tenant,
                    session,
                    turn,
                    "new owner",
                    TimeSpan.FromMinutes 5.0,
                    CancellationToken.None
                )

            Assert.Equal(ControlOperationOutcome.Stopped, refused.Outcome)
            Assert.Null(refused.Claim)
            let! rows = store.ReadPendingInbox(tenant, session, CancellationToken.None)
            Assert.Single(rows) |> ignore
        }

    /// Concurrent requests have one immutable winner under the provider serialization boundary.
    [<Fact>]
    member this.``concurrent host stops retain one exact first receipt``() =
        task {
            let! control, session, _, turn, _ = this.ControlWork()
            use start = new ManualResetEventSlim(false)

            let first =
                Task.Run(fun () ->
                    start.Wait()

                    control
                        .RequestHostAbort(
                            tenant,
                            session,
                            turn,
                            StopCause.ExplicitAbort,
                            "first",
                            CancellationToken.None
                        )
                        .GetAwaiter()
                        .GetResult())

            let second =
                Task.Run(fun () ->
                    start.Wait()

                    control
                        .RequestHostAbort(
                            tenant,
                            session,
                            turn,
                            StopCause.HostShutdown,
                            "second",
                            CancellationToken.None
                        )
                        .GetAwaiter()
                        .GetResult())

            start.Set()
            let! responses = Task.WhenAll(first, second)

            Assert.Single(responses |> Array.filter (fun item -> item.Outcome = HostAbortOutcome.Accepted))
            |> ignore

            Assert.Single(
                responses
                |> Array.filter (fun item -> item.Outcome = HostAbortOutcome.AlreadyAccepted)
            )
            |> ignore

            Assert.Equal(responses[0].Reason, responses[1].Reason)
            Assert.Equal(responses[0].Cause, responses[1].Cause)
            Assert.Equal(responses[0].AcceptedAt, responses[1].AcceptedAt)
        }

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
            let! verifyLost = store.VerifyClaim(tenant, claim, CancellationToken.None)
            Assert.True(verifyLost :? TurnLeaseLost)
            Assert.Equal("takenOver", (verifyLost :?> TurnLeaseLost).Reason)

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

    // ── Renewal and verification outcomes ──

    [<Fact>]
    member this.``RenewClaim extends the lease and VerifyClaim observes Held without mutating``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message =
                UserMessagePayload(UserMessage.Text("renew and verify")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! claimed =
                store.ClaimNextTurn(tenant, created.Id, "owner", TimeSpan.FromMinutes 5., CancellationToken.None)

            let claim = (claimed :?> TurnLeaseRenewed).Claim

            // Verify observes without mutating: twice held, same token
            // and expiry.
            let! firstSeen = store.VerifyClaim(tenant, claim, CancellationToken.None)
            Assert.True(firstSeen :? TurnLeaseHeld)

            let! secondSeen = store.VerifyClaim(tenant, claim, CancellationToken.None)
            Assert.True(secondSeen :? TurnLeaseHeld)
            Assert.Equal(claim.Token, (secondSeen :?> TurnLeaseHeld).Claim.Token)
            Assert.Equal(claim.ExpiresAt, (secondSeen :?> TurnLeaseHeld).Claim.ExpiresAt)

            // Renew after time passes moves the expiry forward.
            this.Clock.Advance(TimeSpan.FromMinutes 1.)

            let! renewedState = store.RenewClaim(tenant, claim, TimeSpan.FromMinutes 5., CancellationToken.None)

            Assert.True(renewedState :? TurnLeaseRenewed)

            let renewed = (renewedState :?> TurnLeaseRenewed).Claim
            Assert.True(renewed.ExpiresAt > claim.ExpiresAt)

            let! held = store.VerifyClaim(tenant, renewed, CancellationToken.None)
            Assert.True(held :? TurnLeaseHeld)

            // The renewed claim still settles: the live-claim path from
            // claim through verify and renew to settlement holds end to
            // end.
            let! settled = store.SettleTurn(tenant, renewed, TurnStatus.Completed, null, CancellationToken.None)

            Assert.True(settled :? TurnSettled)
        }

    [<Fact>]
    member this.``An expired lease reports Lost while an unknown turn reports Missing``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("expiry")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! claimed =
                store.ClaimNextTurn(tenant, created.Id, "owner", TimeSpan.FromMinutes 5., CancellationToken.None)

            let claim = (claimed :?> TurnLeaseRenewed).Claim

            // Past the lease with no takeover: the claim is lost to
            // expiry, distinctly not missing. The shared rule pins both
            // the outcome type and the reason string.
            this.Clock.Advance(TimeSpan.FromMinutes 10.)

            let! expired = store.VerifyClaim(tenant, claim, CancellationToken.None)
            Assert.True(expired :? TurnLeaseLost)
            Assert.Equal("expired", (expired :?> TurnLeaseLost).Reason)

            let! renewExpired = store.RenewClaim(tenant, claim, TimeSpan.FromMinutes 5., CancellationToken.None)
            Assert.True(renewExpired :? TurnLeaseLost)
            Assert.Equal("expired", (renewExpired :?> TurnLeaseLost).Reason)

            // A turn the store never issued resolves missing, never
            // lost: the two branches stay distinct.
            let unknown =
                {
                    TurnId = TurnId.New()
                    Token = "unknown-token"
                    Owner = "nobody"
                    ExpiresAt = this.Clock.Instant + TimeSpan.FromMinutes 5.
                    Attempt = 1
                }

            let! missing = store.VerifyClaim(tenant, unknown, CancellationToken.None)
            Assert.True(missing :? TurnLeaseMissing)

            let! renewMissing = store.RenewClaim(tenant, unknown, TimeSpan.FromMinutes 5., CancellationToken.None)
            Assert.True(renewMissing :? TurnLeaseMissing)
        }

    [<Fact>]
    member this.``A takeover race rejects AbortTurn on the loser and still settles for the winner``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("abort race")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! winner =
                store.ClaimNextTurn(tenant, created.Id, "owner-a", TimeSpan.FromMinutes 5., CancellationToken.None)

            let claim = (winner :?> TurnLeaseRenewed).Claim

            this.Clock.Advance(TimeSpan.FromMinutes 10.)

            let reply =
                ReplyPayload(PermissionDecision("req-1", PermissionDecisionKind.AllowOnce)) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, reply, DeliveryMode.Queue, CancellationToken.None)

            let! taken =
                store.ClaimNextTurn(tenant, created.Id, "owner-b", TimeSpan.FromMinutes 5., CancellationToken.None)

            let retaken = (taken :?> TurnLeaseRenewed).Claim
            Assert.Equal(claim.TurnId, retaken.TurnId)

            // The loser aborts nothing.
            let! abortLost = store.AbortTurn(tenant, claim, CancellationToken.None)
            Assert.True(abortLost :? TurnLeaseLost || abortLost :? TurnLeaseMissing)

            // The winner still settles the resumed turn.
            let! settled = store.SettleTurn(tenant, retaken, TurnStatus.Completed, null, CancellationToken.None)

            Assert.True(settled :? TurnSettled)
        }

    // ── Settlement preconditions ──

    [<Fact>]
    member this.``SettleTurn rejects non-terminal statuses``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("terminal guard")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! claimed =
                store.ClaimNextTurn(tenant, created.Id, "settler", TimeSpan.FromMinutes 5., CancellationToken.None)

            let claim = (claimed :?> TurnLeaseRenewed).Claim

            Assert.Throws<InvalidSessionStateException>(fun () ->
                store
                    .SettleTurn(tenant, claim, TurnStatus.Running, null, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            // The bogus settle had no effect: the correct settle still wins.
            let! settled = store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)

            Assert.True(settled :? TurnSettled)
        }

    // ── CurrentTurnId surfacing ──

    [<Fact>]
    member this.``Claim stamps and settlement clears the row's CurrentTurnId``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message =
                UserMessagePayload(UserMessage.Text("current turn stamp")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! claimed =
                store.ClaimNextTurn(tenant, created.Id, "stamper", TimeSpan.FromMinutes 5., CancellationToken.None)

            let claim = (claimed :?> TurnLeaseRenewed).Claim

            let! during = store.GetSession(tenant, created.Id, CancellationToken.None)

            match during with
            | null -> failwith "expected the session"
            | session ->
                Assert.True(session.CurrentTurnId.HasValue)
                Assert.Equal(claim.TurnId, session.CurrentTurnId.Value)

            // Rebinding is blocked while the turn is in flight.
            Assert.Throws<InvalidSessionStateException>(fun () ->
                store
                    .SetSessionAgent(tenant, created.Id, AgentId.New(), CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            let! settled = store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)

            Assert.True(settled :? TurnSettled)

            let! after = store.GetSession(tenant, created.Id, CancellationToken.None)

            match after with
            | null -> failwith "expected the session"
            | session -> Assert.False(session.CurrentTurnId.HasValue)
        }

    // ── Settle-then-rebind: the SetAgent protocol's precondition ──

    [<Fact>]
    member this.``SetSessionAgent succeeds after the claim settles``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message =
                UserMessagePayload(UserMessage.Text("settle then rebind")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! claimed =
                store.ClaimNextTurn(tenant, created.Id, "rebinder", TimeSpan.FromMinutes 5., CancellationToken.None)

            let claim = (claimed :?> TurnLeaseRenewed).Claim

            // The primed claim stamps CurrentTurnId, so the rebind is
            // blocked while it is held: the actor settles before rebinding.
            Assert.Throws<InvalidSessionStateException>(fun () ->
                store
                    .SetSessionAgent(tenant, created.Id, AgentId.New(), CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            let! settled = store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)

            Assert.True(settled :? TurnSettled)

            let rebound = AgentId.New()

            let! afterRebind = store.SetSessionAgent(tenant, created.Id, rebound, CancellationToken.None)

            Assert.Equal(rebound, afterRebind.AgentId)
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

    [<Fact>]
    member this.``ReadPendingInbox returns entries in position order``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            for text in [ "first"; "second"; "third" ] do
                let message = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload

                let! _ =
                    store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

                ()

            let! pending = store.ReadPendingInbox(tenant, created.Id, CancellationToken.None)

            Assert.Equal(3, pending.Count)

            let positions = pending |> Seq.map (fun entry -> entry.Position) |> Seq.toList

            Assert.Equal(3, positions |> List.distinct |> List.length)

            Assert.True(
                positions
                |> List.pairwise
                |> List.forall (fun (earlier, later) -> earlier < later)
            )
        }

    [<Fact>]
    member this.``ReadPendingInbox throws for the wrong tenant``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("tenanted")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            Assert.Throws<SessionNotFoundException>(fun () ->
                store.ReadPendingInbox(this.OtherTenant, created.Id, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore

            // The owning tenant still reads the entry.
            let! pending = store.ReadPendingInbox(tenant, created.Id, CancellationToken.None)

            Assert.Single(pending) |> ignore
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
    member this.``Idle rows with a live turn surface the live-turn predicate until settlement clears it``() =
        task {
            // The orphan-sweep predicate (issue 289): an Idle row whose
            // claim consumed the inbox still carries its turn id through
            // ListSessions, while GetDispatchCandidates stays
            // pending-inbox-only.
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("live turn")) :> InboxPayload

            let! _ = store.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! claimed =
                store.ClaimNextTurn(
                    tenant,
                    created.Id,
                    "live-turn-owner",
                    TimeSpan.FromMinutes 5.,
                    CancellationToken.None
                )

            let claim = (claimed :?> TurnLeaseRenewed).Claim

            let! idle =
                store.ListSessions(
                    tenant,
                    Nullable(SessionState.Idle),
                    Unchecked.defaultof<Nullable<AgentId>>,
                    Unchecked.defaultof<Nullable<DateTimeOffset>>,
                    Unchecked.defaultof<Nullable<DateTimeOffset>>,
                    10,
                    null,
                    CancellationToken.None
                )

            let row = idle.Items |> Seq.find (fun session -> session.Id = created.Id)

            Assert.True(row.CurrentTurnId.HasValue)
            Assert.Equal(claim.TurnId, row.CurrentTurnId.Value)

            let! pending = store.GetDispatchCandidates(tenant, 10, CancellationToken.None)

            Assert.DoesNotContain(created.Id, pending.Sessions)

            let! settled = store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)

            Assert.True(settled :? TurnSettled)

            let! after = store.GetSession(tenant, created.Id, CancellationToken.None)

            match after with
            | null -> failwith "expected the session"
            | session -> Assert.False(session.CurrentTurnId.HasValue)
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

    // ── Permission grants: the AllowForSession memory ──

    [<Fact>]
    member this.``GrantSessionTool persists the grant on the session row``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let! granted = store.GrantSessionTool(tenant, created.Id, "exec", CancellationToken.None)

            Assert.Contains("exec", granted.PermissionGrants)

            // The grant survives a store round-trip: a restarted host
            // reloads it instead of re-asking the policy.
            let! reloaded = store.GetSession(tenant, created.Id, CancellationToken.None)

            match reloaded with
            | null -> failwith "expected the session"
            | session -> Assert.Contains("exec", session.PermissionGrants)
        }

    [<Fact>]
    member this.``GrantSessionTool is idempotent: granting twice stores once``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let! _ = store.GrantSessionTool(tenant, created.Id, "exec", CancellationToken.None)
            let! twice = store.GrantSessionTool(tenant, created.Id, "exec", CancellationToken.None)

            let count =
                twice.PermissionGrants |> Seq.filter (fun grant -> grant = "exec") |> Seq.length

            Assert.Equal(1, count)
        }

    [<Fact>]
    member this.``GrantSessionTool rejects blank names, wrong tenants, and closed sessions``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            Assert.Throws<ArgumentException>(fun () ->
                store.GrantSessionTool(tenant, created.Id, "  ", CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore

            Assert.Throws<SessionNotFoundException>(fun () ->
                store
                    .GrantSessionTool(this.OtherTenant, created.Id, "exec", CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            let! _ = store.CloseSession(tenant, created.Id, CancellationToken.None)

            Assert.Throws<InvalidSessionStateException>(fun () ->
                store.GrantSessionTool(tenant, created.Id, "exec", CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

    [<Fact>]
    member this.``CloseSession evicts the grant memory``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let! _ = store.GrantSessionTool(tenant, created.Id, "exec", CancellationToken.None)

            let! closed = store.CloseSession(tenant, created.Id, CancellationToken.None)

            Assert.Empty(closed.PermissionGrants)

            let! reloaded = store.GetSession(tenant, created.Id, CancellationToken.None)

            match reloaded with
            | null -> failwith "expected the session"
            | session -> Assert.Empty(session.PermissionGrants)
        }

    // ── Completion outbox (issue 84) ──

    /// Builds a completion carrying the key, the shape settlement enqueues.
    member this.SampleCompletion(sessionId: SessionId, idempotencyKey: string) =
        {
            SessionId = sessionId
            TurnResult =
                {
                    AssistantText = "done"
                    Status = TurnStatus.Completed
                    Iterations = 1
                    Usage = { InputTokens = 1L; OutputTokens = 2L }
                    Outcome = null
                }
            Metadata = null
            IdempotencyKey = idempotencyKey
        }

    [<Fact>]
    member this.``EnqueueCompletionOutbox is idempotent and claimable under a lease``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let! first =
                store.EnqueueCompletionOutbox(
                    tenant,
                    this.SampleCompletion(created.Id, "key-1"),
                    CancellationToken.None
                )

            Assert.False(first.Delivered)

            let! retry =
                store.EnqueueCompletionOutbox(
                    tenant,
                    this.SampleCompletion(created.Id, "key-1"),
                    CancellationToken.None
                )

            Assert.Equal(first.CreatedAt, retry.CreatedAt)

            let! claimed = store.ClaimCompletionOutbox("owner-a", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

            Assert.Single(claimed) |> ignore

            let! live = store.VerifyCompletionClaim(tenant, "key-1", "owner-a", CancellationToken.None)
            Assert.True(live)

            let! marked = store.MarkCompletionDelivered(tenant, "key-1", "owner-a", CancellationToken.None)
            Assert.True(marked)

            let! again = store.ClaimCompletionOutbox("owner-b", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

            Assert.Empty(again)
        }

    [<Fact>]
    member this.``A stale outbox owner marks nothing: the retake winner owns the row``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let! _ =
                store.EnqueueCompletionOutbox(
                    tenant,
                    this.SampleCompletion(created.Id, "key-1"),
                    CancellationToken.None
                )

            let! _ = store.ClaimCompletionOutbox("owner-a", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

            this.Clock.Advance(TimeSpan.FromMinutes 6.)

            let! stale = store.MarkCompletionDelivered(tenant, "key-1", "owner-a", CancellationToken.None)
            Assert.False(stale)

            let! retaken = store.ClaimCompletionOutbox("owner-b", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

            Assert.Single(retaken) |> ignore

            let! winner = store.MarkCompletionDelivered(tenant, "key-1", "owner-b", CancellationToken.None)
            Assert.True(winner)
        }

    [<Fact>]
    member this.``PurgeDeliveredCompletions keeps pending rows and recent deliveries``() =
        task {
            let! created = store.CreateSession(tenant, this.SampleSession(), CancellationToken.None)

            let! _ =
                store.EnqueueCompletionOutbox(tenant, this.SampleCompletion(created.Id, "old"), CancellationToken.None)

            this.Clock.Advance(TimeSpan.FromSeconds 1.)

            let! _ = store.ClaimCompletionOutbox("owner-a", 10, TimeSpan.FromHours 1., CancellationToken.None)
            let! _ = store.MarkCompletionDelivered(tenant, "old", "owner-a", CancellationToken.None)

            let! _ =
                store.EnqueueCompletionOutbox(
                    tenant,
                    this.SampleCompletion(created.Id, "pending"),
                    CancellationToken.None
                )

            this.Clock.Advance(TimeSpan.FromDays 8.)

            let cutoff = this.Clock.Instant - TimeSpan.FromDays 7.
            let! purged = store.PurgeDeliveredCompletions(cutoff, CancellationToken.None)

            Assert.Equal(1, purged)

            let! pending = store.ClaimCompletionOutbox("owner-b", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

            Assert.Single(pending) |> ignore
            Assert.Equal("pending", pending |> Seq.head |> (fun row -> row.IdempotencyKey))
        }
