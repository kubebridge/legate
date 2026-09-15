// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionsStoreTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// The generic Deserialize<'T> overload is annotated to return 'T | null,
// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

let nullString = Unchecked.defaultof<string>
let noState = Unchecked.defaultof<Nullable<SessionState>>
let jsonOptions = JsonSerializerOptions()

let tenant = TenantId.Create "acme"
let otherTenant = TenantId.Create "other"

let sessionStamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
let leaseStamp = DateTimeOffset(2024, 1, 2, 3, 4, 6, TimeSpan.Zero)

/// Builds a session with every field set, the common shape for the fake
/// store's rows.
let sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "checkout"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = sessionStamp
        UpdatedAt = sessionStamp
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = "ws://acme/checkout"
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// Builds a claim over a turn, the shape ClaimNextTurn hands out and every
/// fenced call carries back.
let sampleClaim turnId attempt =
    {
        TurnId = turnId
        Token = "token-" + attempt.ToString()
        Owner = "session-actor-1"
        ExpiresAt = leaseStamp
        Attempt = attempt
    }

/// An in-memory ISessionStore implemented entirely from outside the
/// assembly: the C#-friendly-surface proof, mirroring BlobsTests.fs's
/// FakeBlobStore. It implements the documented semantics the tests pin, so
/// each test exercises the contract through behaviour rather than
/// reflection.
type FakeSessionStore() =
    let sessions = Dictionary<string, Session>()
    let inbox = Dictionary<string, ResizeArray<InboxEntry>>()
    let claims = Dictionary<TenantId * TurnId, TurnClaim>()
    let settlement = Dictionary<string, TurnStatus>()
    let outbox = Dictionary<string, CompletionOutboxEntry>()
    let mutable positionCounter = 0L

    let key (tenantId: TenantId) (sessionId: SessionId) =
        sprintf "%s|%s" tenantId.Value sessionId.Value

    let outboxKey (tenantId: TenantId) (idempotencyKey: string) =
        sprintf "%s|%s" tenantId.Value idempotencyKey

    let claimKey (tenantId: TenantId) (turnId: TurnId) = (tenantId, turnId)

    /// The clock outbox lease expiry reads. Tests set a TestClock and
    /// advance it; the default system clock keeps existing tests green.
    member val Clock: TimeProvider = TimeProvider.System with get, set

    interface ISessionStore with
        member _.CreateSession(t, session, _) =
            if sessions.ContainsKey(key t session.Id) then
                raise (InvalidSessionStateException(session.Id, "Exists", "A session with this id already exists."))
            else
                let stored = { session with Tenant = t }
                sessions[key t session.Id] <- stored
                inbox[key t session.Id] <- ResizeArray<InboxEntry>()
                Task.FromResult stored

        member _.GetSession(t, sessionId, _) =
            Task.FromResult(
                match sessions.TryGetValue(key t sessionId) with
                | true, session -> session
                | false, _ -> Unchecked.defaultof<Session>
            )

        member _.ListSessions(t, state, pageSize, continuation, _) =
            if pageSize <= 0 then
                raise (ArgumentOutOfRangeException(nameof pageSize))

            let all =
                sessions.Values
                |> Seq.filter (fun s -> s.Tenant.Equals t)
                |> Seq.filter (fun s -> not state.HasValue || s.State = state.Value)
                |> Seq.sortByDescending (fun s -> s.UpdatedAt)
                |> Array.ofSeq

            let skip = if isNull (box continuation) then 0 else int continuation

            let items = all |> Array.skip skip |> Array.truncate pageSize

            let nextContinuation =
                let next = skip + items.Length

                if next < all.Length then
                    next.ToString()
                else
                    Unchecked.defaultof<string>

            Task.FromResult(
                {
                    Items = items :> IReadOnlyList<Session>
                    Continuation = nextContinuation
                }
            )

        member _.UpdateSessionState(t, sessionId, state, _) =
            match sessions.TryGetValue(key t sessionId) with
            | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
            | true, session ->
                if session.State = SessionState.Closed && state <> SessionState.Closed then
                    raise (InvalidSessionStateException(sessionId, "Closed", "A closed session stays closed."))
                else
                    let updated =
                        { session with
                            State = state
                            UpdatedAt = sessionStamp
                        }

                    sessions[key t sessionId] <- updated
                    Task.FromResult updated

        member _.CloseSession(t, sessionId, _) =
            match sessions.TryGetValue(key t sessionId) with
            | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
            | true, session ->
                let closed =
                    { session with
                        State = SessionState.Closed
                        ClosedAt = Nullable sessionStamp
                        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
                    }

                sessions[key t sessionId] <- closed
                Task.FromResult closed

        member _.GrantSessionTool(t, sessionId, toolName, _) =
            if String.IsNullOrWhiteSpace toolName then
                raise (ArgumentException("The tool name must be a non-empty string.", nameof toolName))

            match sessions.TryGetValue(key t sessionId) with
            | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
            | true, session ->
                if session.State = SessionState.Closed then
                    raise (InvalidSessionStateException(sessionId, "Closed", "A closed session carries no grants."))
                else
                    let grants = ResizeArray<string>(session.PermissionGrants)

                    if not (grants.Contains toolName) then
                        grants.Add toolName

                    let updated =
                        { session with
                            PermissionGrants = grants :> IReadOnlyList<string>
                            UpdatedAt = sessionStamp
                        }

                    sessions[key t sessionId] <- updated
                    Task.FromResult updated

        member _.SetSessionAgent(t, sessionId, agentId, _) =
            match sessions.TryGetValue(key t sessionId) with
            | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
            | true, session ->
                if session.State = SessionState.Running then
                    raise (InvalidSessionStateException(sessionId, "Running", "A running turn pins the agent."))
                else
                    let updated =
                        { session with
                            AgentId = agentId
                            UpdatedAt = sessionStamp
                        }

                    sessions[key t sessionId] <- updated
                    Task.FromResult updated

        member _.SetSessionTitle(t, sessionId, title, _) =
            match sessions.TryGetValue(key t sessionId) with
            | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
            | true, session ->
                let updated =
                    { session with
                        Title = title
                        UpdatedAt = sessionStamp
                    }

                sessions[key t sessionId] <- updated
                Task.FromResult updated

        member _.AppendInboxMessage(t, sessionId, payload, delivery, _) =
            if box payload |> isNull then
                raise (ArgumentNullException(nameof payload))

            match inbox.TryGetValue(key t sessionId) with
            | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
            | true, _ ->
                positionCounter <- positionCounter + 1L

                let entry =
                    {
                        SessionId = sessionId
                        Position = positionCounter
                        Payload = payload
                        Delivery = delivery
                        Consumed = false
                        AppendedAt = sessionStamp
                    }

                inbox[key t sessionId].Add entry
                Task.FromResult entry

        member _.ReadPendingInbox(t, sessionId, _) =
            match inbox.TryGetValue(key t sessionId) with
            | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
            | true, entries ->
                let pending =
                    entries
                    |> Seq.filter (fun e -> not e.Consumed)
                    |> Seq.sortBy (fun e -> e.Position)
                    |> ResizeArray

                Task.FromResult(pending :> IReadOnlyList<InboxEntry>)

        member _.MarkInboxConsumed(t, sessionId, positions, _) =
            if box positions |> isNull then
                raise (ArgumentNullException(nameof positions))

            match inbox.TryGetValue(key t sessionId) with
            | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
            | true, entries ->
                let mutable flipped = 0

                for position in positions do
                    let mutable index = 0

                    while index < entries.Count do
                        let entry = entries[index]

                        if entry.Position = position && not entry.Consumed then
                            entries[index] <- { entry with Consumed = true }
                            flipped <- flipped + 1

                        index <- index + 1

                Task.FromResult flipped

        member _.ClaimNextTurn(t, sessionId, owner, _, _) =
            match inbox.TryGetValue(key t sessionId) with
            | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
            | true, entries ->
                let pendingUser =
                    entries
                    |> Seq.filter (fun e -> not e.Consumed && (e.Payload :? UserMessagePayload))
                    |> Seq.sortBy (fun e -> e.Position)
                    |> Seq.tryHead

                match pendingUser with
                | None -> Task.FromResult(TurnLeaseMissing(Unchecked.defaultof<TurnId>) :> TurnLeaseState)
                | Some _ ->
                    let turnId = TurnId.New()

                    // The fake mints a fresh deterministic token per claim
                    // ("token-<attempt>"); a later claim over the same turn
                    // replaces the stored one, which is what a takeover is
                    // here.
                    let claim =
                        match claims.TryGetValue(claimKey t turnId) with
                        | true, previous ->
                            { previous with
                                Token = "token-" + (previous.Attempt + 1).ToString()
                                Owner = owner
                                Attempt = previous.Attempt + 1
                            }
                        | false, _ -> sampleClaim turnId 1

                    claims[claimKey t turnId] <- claim
                    Task.FromResult(TurnLeaseRenewed(claim) :> TurnLeaseState)

        member _.RenewClaim(t, claim, _, _) =
            if box claim |> isNull then
                raise (ArgumentNullException(nameof claim))

            match claims.TryGetValue(claimKey t claim.TurnId) with
            | false, _ -> Task.FromResult(TurnLeaseMissing(claim.TurnId) :> TurnLeaseState)
            | true, current ->
                if current.Token <> claim.Token then
                    Task.FromResult(TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState)
                else
                    let renewed =
                        { current with
                            ExpiresAt = leaseStamp.AddMinutes 1.0
                        }

                    claims[claimKey t claim.TurnId] <- renewed
                    Task.FromResult(TurnLeaseRenewed(renewed) :> TurnLeaseState)

        member this.ObserveAndRenewClaim(t, claim, leaseDuration, _) =
            FakeSessionStore.renewInternal this t claim leaseDuration

        member _.VerifyClaim(t, claim, _) =
            if box claim |> isNull then
                raise (ArgumentNullException(nameof claim))

            match claims.TryGetValue(claimKey t claim.TurnId) with
            | false, _ -> Task.FromResult(TurnLeaseMissing(claim.TurnId) :> TurnLeaseState)
            | true, current ->
                if current.Token <> claim.Token then
                    Task.FromResult(TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState)
                else
                    Task.FromResult(TurnLeaseHeld(current) :> TurnLeaseState)

        member this.CheckpointUsage(t, claim, _, _) =
            if box claim |> isNull then
                raise (ArgumentNullException(nameof claim))

            FakeSessionStore.verifyInternal this t claim

        member _.SettleTurn(t, claim, status, _, _) =
            if box claim |> isNull then
                raise (ArgumentNullException(nameof claim))

            // The fake records the first outcome; a matching retry observes
            // the already-settled state, a different one is rejected, and a
            // settled turn's claim is gone, so a stale claim rejects too.
            match claims.TryGetValue(claimKey t claim.TurnId) with
            | false, _ ->
                match settlement.TryGetValue(claim.TurnId.Value) with
                | true, applied when applied = status ->
                    Task.FromResult(TurnAlreadySettled(claim.TurnId) :> TurnSettlement)
                | true, _ ->
                    Task.FromResult(TurnSettleRejected(claim.TurnId, "alreadySettledByOther") :> TurnSettlement)
                | false, _ -> Task.FromResult(TurnSettleRejected(claim.TurnId, "staleClaim") :> TurnSettlement)
            | true, current ->
                if current.Token <> claim.Token then
                    Task.FromResult(TurnSettleRejected(claim.TurnId, "staleClaim") :> TurnSettlement)
                else
                    match settlement.TryGetValue(claim.TurnId.Value) with
                    | true, applied when applied = status ->
                        Task.FromResult(TurnAlreadySettled(claim.TurnId) :> TurnSettlement)
                    | true, _ ->
                        Task.FromResult(TurnSettleRejected(claim.TurnId, "alreadySettledByOther") :> TurnSettlement)
                    | false, _ ->
                        settlement[claim.TurnId.Value] <- status
                        claims.Remove(claimKey t claim.TurnId) |> ignore
                        Task.FromResult(TurnSettled(claim.TurnId, status) :> TurnSettlement)

        member _.AbortTurn(t, claim, _) =
            if box claim |> isNull then
                raise (ArgumentNullException(nameof claim))

            match claims.TryGetValue(claimKey t claim.TurnId) with
            | false, _ -> Task.FromResult(TurnLeaseMissing(claim.TurnId) :> TurnLeaseState)
            | true, current ->
                if current.Token <> claim.Token then
                    Task.FromResult(TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState)
                else
                    claims.Remove(claimKey t claim.TurnId) |> ignore
                    Task.FromResult(TurnLeaseHeld(current) :> TurnLeaseState)

        member this.EnqueueCompletionOutbox(t, completion, _) =
            if box completion |> isNull then
                raise (ArgumentNullException(nameof completion))

            if String.IsNullOrWhiteSpace completion.IdempotencyKey then
                raise (
                    ArgumentException("The completion's idempotency key must be a non-empty string.", nameof completion)
                )

            match sessions.TryGetValue(key t completion.SessionId) with
            | false, _ -> raise (SessionNotFoundException(completion.SessionId, "Session not found."))
            | true, _ ->
                match outbox.TryGetValue(outboxKey t completion.IdempotencyKey) with
                | true, existing -> Task.FromResult existing
                | false, _ ->
                    let row: CompletionOutboxEntry =
                        {
                            Tenant = t
                            SessionId = completion.SessionId
                            IdempotencyKey = completion.IdempotencyKey
                            Completion = completion
                            CreatedAt = this.Clock.GetUtcNow()
                            Delivered = false
                            DeliveredAt = Nullable()
                            LeaseOwner = nullString
                            LeaseExpiresAt = Nullable()
                        }

                    outbox[outboxKey t completion.IdempotencyKey] <- row
                    Task.FromResult row

        member this.ClaimCompletionOutbox(owner, maxBatch, leaseDuration, _) =
            if box owner |> isNull then
                raise (ArgumentNullException(nameof owner))

            if maxBatch <= 0 then
                raise (ArgumentOutOfRangeException(nameof maxBatch))

            if leaseDuration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof leaseDuration))

            let now = this.Clock.GetUtcNow()

            let claimed =
                outbox.Values
                |> Seq.filter (fun row -> not row.Delivered)
                |> Seq.filter (fun row ->
                    box row.LeaseOwner |> isNull
                    || not row.LeaseExpiresAt.HasValue
                    || row.LeaseExpiresAt.Value <= now)
                |> Seq.sortBy (fun row -> row.CreatedAt)
                |> Seq.truncate maxBatch
                |> Seq.map (fun row ->
                    let leased =
                        { row with
                            LeaseOwner = owner
                            LeaseExpiresAt = Nullable(now + leaseDuration)
                        }

                    outbox[outboxKey row.Tenant row.IdempotencyKey] <- leased
                    leased)
                |> ResizeArray

            Task.FromResult(claimed :> IReadOnlyList<CompletionOutboxEntry>)

        member this.VerifyCompletionClaim(t, idempotencyKey, owner, _) =
            if String.IsNullOrWhiteSpace idempotencyKey then
                raise (ArgumentException("The idempotency key must be a non-empty string.", nameof idempotencyKey))

            if box owner |> isNull then
                raise (ArgumentNullException(nameof owner))

            match outbox.TryGetValue(outboxKey t idempotencyKey) with
            | false, _ -> Task.FromResult false
            | true, row ->
                Task.FromResult(
                    not row.Delivered
                    && not (box row.LeaseOwner |> isNull)
                    && String.Equals(row.LeaseOwner, owner, StringComparison.Ordinal)
                    && row.LeaseExpiresAt.HasValue
                    && row.LeaseExpiresAt.Value > this.Clock.GetUtcNow()
                )

        member this.MarkCompletionDelivered(t, idempotencyKey, owner, _) =
            if String.IsNullOrWhiteSpace idempotencyKey then
                raise (ArgumentException("The idempotency key must be a non-empty string.", nameof idempotencyKey))

            if box owner |> isNull then
                raise (ArgumentNullException(nameof owner))

            match outbox.TryGetValue(outboxKey t idempotencyKey) with
            | false, _ -> Task.FromResult false
            | true, row when row.Delivered -> Task.FromResult true
            | true, row ->
                if
                    box row.LeaseOwner |> isNull
                    || not (String.Equals(row.LeaseOwner, owner, StringComparison.Ordinal))
                    || not row.LeaseExpiresAt.HasValue
                    || row.LeaseExpiresAt.Value <= this.Clock.GetUtcNow()
                then
                    Task.FromResult false
                else
                    let marked =
                        { row with
                            Delivered = true
                            DeliveredAt = Nullable(this.Clock.GetUtcNow())
                            LeaseOwner = nullString
                            LeaseExpiresAt = Nullable()
                        }

                    outbox[outboxKey t idempotencyKey] <- marked
                    Task.FromResult true

        member _.PurgeDeliveredCompletions(deliveredBefore, _) =
            let victims =
                outbox
                |> Seq.filter (fun pair ->
                    pair.Value.Delivered
                    && pair.Value.DeliveredAt.HasValue
                    && pair.Value.DeliveredAt.Value <= deliveredBefore)
                |> Seq.map (fun pair -> pair.Key)
                |> Seq.toList

            for victim in victims do
                outbox.Remove(victim) |> ignore

            Task.FromResult victims.Length

        member _.GetDispatchCandidates(t, maxBatch, _) =
            if maxBatch <= 0 then
                raise (ArgumentOutOfRangeException(nameof maxBatch))

            let pending =
                sessions.Values
                |> Seq.filter (fun s -> s.Tenant.Equals t)
                |> Seq.filter (fun s ->
                    match inbox.TryGetValue(key t s.Id) with
                    | true, entries -> entries |> Seq.exists (fun e -> not e.Consumed)
                    | false, _ -> false)
                |> Seq.map (fun s -> s.Id)
                |> Seq.truncate maxBatch
                |> ResizeArray

            Task.FromResult(
                {
                    Sessions = pending :> IReadOnlyList<SessionId>
                    HasMore = false
                }
            )

        member _.CountSessionsByAgent(t, agentId, _) =
            Task.FromResult(
                sessions.Values
                |> Seq.filter (fun s -> s.Tenant.Equals t && s.AgentId = agentId)
                |> Seq.length
            )

        member _.CountSessionsByTenant(t, _) =
            Task.FromResult(sessions.Values |> Seq.filter (fun s -> s.Tenant.Equals t) |> Seq.length)

        member _.CountRunningSessions(_) =
            Task.FromResult(
                sessions.Values
                |> Seq.filter (fun s -> s.State = SessionState.Running)
                |> Seq.length
            )

    /// The shared renew path, callable before the interface members exist in
    /// F# initialisation order.
    static member renewInternal
        (instance: FakeSessionStore)
        (t: TenantId)
        (claim: TurnClaim)
        (_leaseDuration: TimeSpan)
        : Task<TurnLeaseState> =
        (instance :> ISessionStore).RenewClaim(t, claim, TimeSpan.FromMinutes 1., CancellationToken.None)

    /// The shared verify path, callable before the interface members exist
    /// in F# initialisation order.
    static member verifyInternal (instance: FakeSessionStore) (t: TenantId) (claim: TurnClaim) : Task<TurnLeaseState> =
        (instance :> ISessionStore).VerifyClaim(t, claim, CancellationToken.None)

    /// Test hook: replaces the stored claim over a turn with a fresh token,
    /// which is how a takeover materialises in this fake; the previous
    /// owner's token goes stale.
    member _.Takeover(t: TenantId, turnId: TurnId) =
        match claims.TryGetValue(claimKey t turnId) with
        | true, previous ->
            let taken =
                { previous with
                    Token = "token-takeover"
                    Owner = "session-actor-2"
                }

            claims[claimKey t turnId] <- taken
            taken
        | false, _ -> failwith "no claim to take over"

// ───────────────────────────────────────────────────────────────────────────
// Shape: claim, lease, and settlement hierarchies

[<Fact>]
let ``TurnClaim carries token, owner, expiry, and attempt`` () =
    let turnId = TurnId.New()
    let claim = sampleClaim turnId 2

    claim.TurnId |> should equal turnId
    claim.Token |> should equal "token-2"
    claim.Owner |> should equal "session-actor-1"
    claim.ExpiresAt |> should equal leaseStamp
    claim.Attempt |> should equal 2

[<Fact>]
let ``TurnLeaseState has exactly the five documented discriminators`` () =
    let lease: TurnLeaseState = TurnLeaseHeld(sampleClaim (TurnId.New()) 1) :> _

    let json = JsonSerializer.Serialize(lease, jsonOptions)
    json.Contains("\"$type\":\"leaseHeld\"") |> should equal true

    let renewed: TurnLeaseState = TurnLeaseRenewed(sampleClaim (TurnId.New()) 1) :> _

    let renewedJson = JsonSerializer.Serialize(renewed, jsonOptions)
    renewedJson.Contains("\"$type\":\"leaseRenewed\"") |> should equal true

    let lost: TurnLeaseState = TurnLeaseLost(TurnId.New(), "expired") :> _

    let lostJson = JsonSerializer.Serialize(lost, jsonOptions)
    lostJson.Contains("\"$type\":\"leaseLost\"") |> should equal true

    let expiring: TurnLeaseState = TurnLeaseExpiring(sampleClaim (TurnId.New()) 1) :> _

    let expiringJson = JsonSerializer.Serialize(expiring, jsonOptions)
    expiringJson.Contains("\"$type\":\"leaseExpiring\"") |> should equal true

    let missing: TurnLeaseState = TurnLeaseMissing(TurnId.New()) :> _

    let missingJson = JsonSerializer.Serialize(missing, jsonOptions)
    missingJson.Contains("\"$type\":\"leaseMissing\"") |> should equal true

[<Fact>]
let ``TurnLeaseState round-trips the held lease to the right subtype`` () =
    let claim = sampleClaim (TurnId.New()) 1
    let lease: TurnLeaseState = TurnLeaseHeld(claim) :> _

    let json = JsonSerializer.Serialize(lease, jsonOptions)

    match JsonSerializer.Deserialize<TurnLeaseState>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? TurnLeaseHeld) |> should equal true
        (restored :?> TurnLeaseHeld).Claim |> should equal claim

[<Fact>]
let ``TurnLeaseState round-trips the lost lease with its reason`` () =
    let turnId = TurnId.New()
    let lease: TurnLeaseState = TurnLeaseLost(turnId, "expired") :> _

    let json = JsonSerializer.Serialize(lease, jsonOptions)

    match JsonSerializer.Deserialize<TurnLeaseState>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? TurnLeaseLost) |> should equal true
        (restored :?> TurnLeaseLost).TurnId |> should equal turnId
        (restored :?> TurnLeaseLost).Reason |> should equal "expired"

[<Fact>]
let ``TurnSettlement has exactly the three documented discriminators`` () =
    let settled: TurnSettlement = TurnSettled(TurnId.New(), TurnStatus.Completed) :> _

    let settledJson = JsonSerializer.Serialize(settled, jsonOptions)
    settledJson.Contains("\"$type\":\"turnSettled\"") |> should equal true

    let already: TurnSettlement = TurnAlreadySettled(TurnId.New()) :> _

    let alreadyJson = JsonSerializer.Serialize(already, jsonOptions)
    alreadyJson.Contains("\"$type\":\"turnAlreadySettled\"") |> should equal true

    let rejected: TurnSettlement = TurnSettleRejected(TurnId.New(), "staleClaim") :> _

    let rejectedJson = JsonSerializer.Serialize(rejected, jsonOptions)
    rejectedJson.Contains("\"$type\":\"turnSettleRejected\"") |> should equal true

[<Fact>]
let ``TurnSettlement round-trips the rejected outcome to the right subtype`` () =
    let turnId = TurnId.New()

    let outcome: TurnSettlement =
        TurnSettleRejected(turnId, "alreadySettledByOther") :> _

    let json = JsonSerializer.Serialize(outcome, jsonOptions)

    match JsonSerializer.Deserialize<TurnSettlement>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? TurnSettleRejected) |> should equal true
        (restored :?> TurnSettleRejected).TurnId |> should equal turnId
        (restored :?> TurnSettleRejected).Reason |> should equal "alreadySettledByOther"

// ───────────────────────────────────────────────────────────────────────────
// Inbox envelope shape and JSON round-trips

[<Fact>]
let ``UserMessagePayload round-trips through the inbox envelope`` () =
    let message = UserMessage.Text "ship it"
    let entry: InboxPayload = UserMessagePayload(message) :> _

    let json = JsonSerializer.Serialize(entry, jsonOptions)
    json.Contains("\"$type\":\"userMessage\"") |> should equal true

    match JsonSerializer.Deserialize<InboxPayload>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? UserMessagePayload) |> should equal true
        (restored :?> UserMessagePayload).Message.Parts.Count |> should equal 1

[<Fact>]
let ``ReplyPayload round-trips through the inbox envelope`` () =
    let reply = PermissionDecision("req-1", PermissionDecisionKind.AllowOnce) :> Reply
    let payload: InboxPayload = ReplyPayload(reply) :> _

    let json = JsonSerializer.Serialize(payload, jsonOptions)
    json.Contains("\"$type\":\"reply\"") |> should equal true

    match JsonSerializer.Deserialize<InboxPayload>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? ReplyPayload) |> should equal true

        match (restored :?> ReplyPayload).Reply with
        | :? PermissionDecision as decision -> decision.RequestId |> should equal "req-1"
        | _ -> failwith "the reply payload lost its decision"

[<Fact>]
let ``UserMessagePayload rejects a null message`` () =
    let nullMessage = Unchecked.defaultof<UserMessage>

    (fun () -> UserMessagePayload(nullMessage) |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``ReplyPayload rejects a null reply`` () =
    let nullReply = Unchecked.defaultof<Reply>

    (fun () -> ReplyPayload(nullReply) |> ignore)
    |> should throw typeof<ArgumentNullException>

// ───────────────────────────────────────────────────────────────────────────
// Contract implementability: the fake pins the documented semantics

[<Fact>]
let ``CreateSession stores the session under its tenant`` () =
    let store = FakeSessionStore() :> ISessionStore
    let session = sampleSession ()

    task {
        let! stored = store.CreateSession(tenant, session, CancellationToken.None)
        stored.Id |> should equal session.Id
        stored.Tenant |> should equal tenant

        let! read = store.GetSession(tenant, session.Id, CancellationToken.None)

        match read with
        | null -> failwith "the created session was not found"
        | found -> found.Id |> should equal session.Id
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Store operations are tenant-scoped`` () =
    let store = FakeSessionStore() :> ISessionStore
    let session = sampleSession ()

    task {
        let! created = store.CreateSession(tenant, session, CancellationToken.None)

        let! crossTenant = store.GetSession(otherTenant, created.Id, CancellationToken.None)
        crossTenant |> should equal null

        let! sameTenant = store.GetSession(tenant, created.Id, CancellationToken.None)

        match sameTenant with
        | null -> failwith "the created session was not found"
        | found -> found.Id |> should equal created.Id
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``UpdateSessionState rejects an unknown session`` () =
    let store = FakeSessionStore() :> ISessionStore

    task {
        try
            let! _ = store.UpdateSessionState(tenant, SessionId.New(), SessionState.Running, CancellationToken.None)
            failwith "expected SessionNotFoundException"
        with :? SessionNotFoundException as exn ->
            exn.Message |> should equal "Session not found."
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``UpdateSessionState keeps Closed terminal`` () =
    let store = FakeSessionStore() :> ISessionStore
    let session = sampleSession ()

    task {
        let! created = store.CreateSession(tenant, session, CancellationToken.None)
        let! closed = store.CloseSession(tenant, created.Id, CancellationToken.None)
        closed.State |> should equal SessionState.Closed

        try
            let! _ = store.UpdateSessionState(tenant, created.Id, SessionState.Idle, CancellationToken.None)
            failwith "expected InvalidSessionStateException"
        with :? InvalidSessionStateException as exn ->
            exn.CurrentState |> should equal "Closed"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``SetSessionAgent rejects a running session`` () =
    let store = FakeSessionStore() :> ISessionStore
    let session = sampleSession ()

    task {
        let! created = store.CreateSession(tenant, session, CancellationToken.None)

        let! _ = store.UpdateSessionState(tenant, created.Id, SessionState.Running, CancellationToken.None)

        try
            let! _ = store.SetSessionAgent(tenant, created.Id, AgentId.New(), CancellationToken.None)
            failwith "expected InvalidSessionStateException"
        with :? InvalidSessionStateException as exn ->
            exn.CurrentState |> should equal "Running"

        ()
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ListSessions pages bounded results with a continuation`` () =
    let store = FakeSessionStore() :> ISessionStore

    task {
        for _ in 1..3 do
            let! _ = store.CreateSession(tenant, sampleSession (), CancellationToken.None)
            ()

        let! first = store.ListSessions(tenant, noState, 2, null, CancellationToken.None)
        first.Items.Count |> should equal 2
        first.Continuation |> should not' (equal null)

        let! second = store.ListSessions(tenant, noState, 2, first.Continuation, CancellationToken.None)
        second.Items.Count |> should equal 1
        second.Continuation |> should equal null
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ListSessions filters by state`` () =
    let store = FakeSessionStore() :> ISessionStore

    task {
        let! first = store.CreateSession(tenant, sampleSession (), CancellationToken.None)
        let! _ = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! _ = store.UpdateSessionState(tenant, first.Id, SessionState.Running, CancellationToken.None)

        let! runningOnly = store.ListSessions(tenant, Nullable SessionState.Running, 10, null, CancellationToken.None)

        runningOnly.Items.Count |> should equal 1
        runningOnly.Items[0].Id |> should equal first.Id
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Inbox appends carry the delivery mode and round-trip pending order`` () =
    let store = FakeSessionStore() :> ISessionStore
    let session = sampleSession ()

    task {
        let! created = store.CreateSession(tenant, session, CancellationToken.None)

        let! queued =
            store.AppendInboxMessage(
                tenant,
                created.Id,
                UserMessagePayload(UserMessage.Text "first") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! injected =
            store.AppendInboxMessage(
                tenant,
                created.Id,
                ReplyPayload(PermissionDecision("req-1", PermissionDecisionKind.AllowOnce) :> Reply) :> InboxPayload,
                DeliveryMode.Inject,
                CancellationToken.None
            )

        queued.Delivery |> should equal DeliveryMode.Queue
        injected.Delivery |> should equal DeliveryMode.Inject
        queued.Position |> should not' (equal injected.Position)
        queued.Consumed |> should equal false

        let! pending = store.ReadPendingInbox(tenant, created.Id, CancellationToken.None)
        pending.Count |> should equal 2
        pending[0].Position |> should equal queued.Position
        pending[1].Position |> should equal injected.Position
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``MarkInboxConsumed removes entries from the pending read`` () =
    let store = FakeSessionStore() :> ISessionStore
    let session = sampleSession ()

    task {
        let! created = store.CreateSession(tenant, session, CancellationToken.None)

        let! first =
            store.AppendInboxMessage(
                tenant,
                created.Id,
                UserMessagePayload(UserMessage.Text "one") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! second =
            store.AppendInboxMessage(
                tenant,
                created.Id,
                UserMessagePayload(UserMessage.Text "two") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! flipped =
            store.MarkInboxConsumed(
                tenant,
                created.Id,
                [ first.Position ] :> IReadOnlyList<int64>,
                CancellationToken.None
            )

        flipped |> should equal 1

        let! pending = store.ReadPendingInbox(tenant, created.Id, CancellationToken.None)
        pending.Count |> should equal 1
        pending[0].Position |> should equal second.Position

        // Consuming the same position again is a no-op.
        let! again =
            store.MarkInboxConsumed(
                tenant,
                created.Id,
                [ first.Position ] :> IReadOnlyList<int64>,
                CancellationToken.None
            )

        again |> should equal 0
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ClaimNextTurn returns the missing lease state on an empty session`` () =
    let store = FakeSessionStore() :> ISessionStore

    task {
        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! lease =
            store.ClaimNextTurn(tenant, created.Id, "session-actor-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        (lease :? TurnLeaseMissing) |> should equal true
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ClaimNextTurn claims a pending user-message turn under a lease`` () =
    let store = FakeSessionStore() :> ISessionStore

    task {
        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                created.Id,
                UserMessagePayload(UserMessage.Text "go") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! lease =
            store.ClaimNextTurn(tenant, created.Id, "session-actor-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        (lease :? TurnLeaseRenewed) |> should equal true

        match lease with
        | :? TurnLeaseRenewed as held ->
            held.Claim.Owner |> should equal "session-actor-1"
            held.Claim.Attempt |> should equal 1
            held.Claim.Token |> should not' (equal null)
            held.Claim.ExpiresAt |> should not' (equal null)
            ()
        | _ -> failwith "unreachable: the lease was a renewal"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``RenewClaim extends the expiry and VerifyClaim holds`` () =
    let store = FakeSessionStore() :> ISessionStore

    task {
        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                created.Id,
                UserMessagePayload(UserMessage.Text "go") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! lease =
            store.ClaimNextTurn(tenant, created.Id, "session-actor-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        let claim =
            match lease with
            | :? TurnLeaseRenewed as held -> held.Claim
            | _ -> failwith "expected a held lease"

        let! renewed = store.RenewClaim(tenant, claim, TimeSpan.FromMinutes 10., CancellationToken.None)

        match renewed with
        | :? TurnLeaseRenewed as renewedLease -> renewedLease.Claim.ExpiresAt |> should equal (leaseStamp.AddMinutes 1.)
        | _ -> failwith "expected a renewed lease"

        let! verified = store.VerifyClaim(tenant, claim, CancellationToken.None)
        (verified :? TurnLeaseHeld) |> should equal true
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``A stale token loses the lease and cannot checkpoint or settle`` () =
    let fake = FakeSessionStore()
    let store = fake :> ISessionStore

    task {
        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                created.Id,
                UserMessagePayload(UserMessage.Text "go") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! lease =
            store.ClaimNextTurn(tenant, created.Id, "session-actor-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        let claim =
            match lease with
            | :? TurnLeaseRenewed as held -> held.Claim
            | _ -> failwith "expected a held lease"

        // Simulate a takeover: the fake replaces the stored claim with a
        // new token; the first owner's token is then stale.
        fake.Takeover(tenant, claim.TurnId) |> ignore

        // The original owner's token is now stale.
        let! verified = store.VerifyClaim(tenant, claim, CancellationToken.None)
        (verified :? TurnLeaseLost) |> should equal true
        (verified :?> TurnLeaseLost).Reason |> should equal "takenOver"

        let! checkpoint =
            store.CheckpointUsage(tenant, claim, { InputTokens = 10L; OutputTokens = 5L }, CancellationToken.None)

        (checkpoint :? TurnLeaseLost) |> should equal true

        let! settled = store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)
        (settled :? TurnSettleRejected) |> should equal true
        (settled :?> TurnSettleRejected).Reason |> should equal "staleClaim"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``SettleTurn is idempotent for the same outcome and rejects a different one`` () =
    let store = FakeSessionStore() :> ISessionStore

    task {
        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                created.Id,
                UserMessagePayload(UserMessage.Text "go") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! lease =
            store.ClaimNextTurn(tenant, created.Id, "session-actor-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        let claim =
            match lease with
            | :? TurnLeaseRenewed as held -> held.Claim
            | _ -> failwith "expected a held lease"

        let! first = store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)
        (first :? TurnSettled) |> should equal true
        (first :?> TurnSettled).Status |> should equal TurnStatus.Completed

        let! retry = store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)
        (retry :? TurnAlreadySettled) |> should equal true

        let! other = store.SettleTurn(tenant, claim, TurnStatus.Failed, null, CancellationToken.None)
        (other :? TurnSettleRejected) |> should equal true
        (other :?> TurnSettleRejected).Reason |> should equal "alreadySettledByOther"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``AbortTurn releases the lease and a later verify misses`` () =
    let store = FakeSessionStore() :> ISessionStore

    task {
        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                created.Id,
                UserMessagePayload(UserMessage.Text "go") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! lease =
            store.ClaimNextTurn(tenant, created.Id, "session-actor-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        let claim =
            match lease with
            | :? TurnLeaseRenewed as held -> held.Claim
            | _ -> failwith "expected a held lease"

        let! aborted = store.AbortTurn(tenant, claim, CancellationToken.None)
        (aborted :? TurnLeaseHeld) |> should equal true

        let! after = store.VerifyClaim(tenant, claim, CancellationToken.None)
        (after :? TurnLeaseMissing) |> should equal true
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``GetDispatchCandidates lists only pending sessions and stays bounded`` () =
    let store = FakeSessionStore() :> ISessionStore

    task {
        let! busy = store.CreateSession(tenant, sampleSession (), CancellationToken.None)
        let! quiet = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! _ =
            store.AppendInboxMessage(
                tenant,
                busy.Id,
                UserMessagePayload(UserMessage.Text "wake") :> InboxPayload,
                DeliveryMode.Queue,
                CancellationToken.None
            )

        let! batch = store.GetDispatchCandidates(tenant, 1, CancellationToken.None)
        batch.Sessions.Count |> should equal 1
        batch.Sessions[0] |> should equal busy.Id
        batch.HasMore |> should equal false

        // The quiet session never appears.
        batch.Sessions[0] |> should not' (equal quiet.Id)

        let! drained = store.GetDispatchCandidates(otherTenant, 5, CancellationToken.None)
        drained.Sessions.Count |> should equal 0
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Count queries scope to the tenant and running state`` () =
    let store = FakeSessionStore() :> ISessionStore

    task {
        let! one = store.CreateSession(tenant, sampleSession (), CancellationToken.None)
        let! _ = store.CreateSession(tenant, sampleSession (), CancellationToken.None)
        let! _ = store.CreateSession(otherTenant, sampleSession (), CancellationToken.None)

        let! _ = store.UpdateSessionState(tenant, one.Id, SessionState.Running, CancellationToken.None)

        let! byAgent = store.CountSessionsByAgent(tenant, one.AgentId, CancellationToken.None)
        byAgent |> should equal 1

        let! byTenant = store.CountSessionsByTenant(tenant, CancellationToken.None)
        byTenant |> should equal 2

        let! running = store.CountRunningSessions(CancellationToken.None)
        running |> should equal 1
    }
    |> (fun t -> t.Wait())

// ───────────────────────────────────────────────────────────────────────────
// Bounded-batch and interface-shape pins

[<Fact>]
let ``DispatchBatch carries its sessions and the has-more flag`` () =
    let batch =
        {
            Sessions = ResizeArray([ SessionId.New() ]) :> IReadOnlyList<SessionId>
            HasMore = true
        }

    batch.Sessions.Count |> should equal 1
    batch.HasMore |> should equal true

[<Fact>]
let ``SessionPage carries its items and continuation`` () =
    let page =
        {
            Items = ResizeArray([ sampleSession () ]) :> IReadOnlyList<Session>
            Continuation = nullString
        }

    page.Items.Count |> should equal 1
    page.Continuation |> should equal null

[<Fact>]
let ``Every ISessionStore method takes a TenantId where the contract demands`` () =
    // The tenancy rule: session, inbox, claim, outbox enqueue/verify/mark,
    // and dispatch operations carry the tenant; only the process-wide
    // cross-tenant scans (delivery batch claim, delivered purge, running
    // count) do not.
    let methods = typeof<ISessionStore>.GetMethods()

    let tenantScoped =
        [|
            "CreateSession"
            "GetSession"
            "ListSessions"
            "UpdateSessionState"
            "CloseSession"
            "SetSessionAgent"
            "SetSessionTitle"
            "AppendInboxMessage"
            "ReadPendingInbox"
            "MarkInboxConsumed"
            "ClaimNextTurn"
            "RenewClaim"
            "ObserveAndRenewClaim"
            "VerifyClaim"
            "CheckpointUsage"
            "SettleTurn"
            "AbortTurn"
            "EnqueueCompletionOutbox"
            "VerifyCompletionClaim"
            "MarkCompletionDelivered"
            "GetDispatchCandidates"
            "CountSessionsByAgent"
            "CountSessionsByTenant"
        |]

    for name in tenantScoped do
        let method = methods |> Array.find (fun m -> m.Name = name)
        let parameters = method.GetParameters()

        parameters
        |> Array.exists (fun p -> p.ParameterType = typeof<TenantId>)
        |> should equal true

    for name in
        [|
            "ClaimCompletionOutbox"
            "PurgeDeliveredCompletions"
            "CountRunningSessions"
        |] do
        let method = methods |> Array.find (fun m -> m.Name = name)

        method.GetParameters()
        |> Array.exists (fun p -> p.ParameterType = typeof<TenantId>)
        |> should equal false

// ───────────────────────────────────────────────────────────────────────────
// Completion outbox (issue 84)

/// Builds a completion carrying the key, the shape settlement enqueues.
let sampleCompletion (sessionId: SessionId) (idempotencyKey: string) : SessionCompletion =
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

/// Creates the fake store with its session row, returning both.
let createStoreWithSession () =
    task {
        let store = FakeSessionStore()
        let! created = (store :> ISessionStore).CreateSession(tenant, sampleSession (), CancellationToken.None)
        return store, created
    }

[<Fact>]
let ``CompletionOutboxEntry JSON round-trip preserves every field`` () =
    let entry: CompletionOutboxEntry =
        {
            Tenant = tenant
            SessionId = SessionId.New()
            IdempotencyKey = "key-1"
            Completion = sampleCompletion (SessionId.New()) "key-1"
            CreatedAt = sessionStamp
            Delivered = false
            DeliveredAt = Nullable()
            LeaseOwner = nullString
            LeaseExpiresAt = Nullable()
        }

    let json = JsonSerializer.Serialize(entry, jsonOptions)
    let restored = deserialize<CompletionOutboxEntry> json

    restored.Tenant |> should equal entry.Tenant
    restored.SessionId |> should equal entry.SessionId
    restored.IdempotencyKey |> should equal "key-1"
    restored.Completion.IdempotencyKey |> should equal "key-1"
    restored.CreatedAt |> should equal sessionStamp
    restored.Delivered |> should equal false
    restored.DeliveredAt.HasValue |> should equal false

[<Fact>]
let ``EnqueueCompletionOutbox stores a pending unleased row`` () =
    task {
        let! store, created = createStoreWithSession ()
        let sessionStore = store :> ISessionStore

        let! row =
            sessionStore.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        row.Tenant |> should equal tenant
        row.SessionId |> should equal created.Id
        row.IdempotencyKey |> should equal "key-1"
        row.Delivered |> should equal false
        row.DeliveredAt.HasValue |> should equal false
        (box row.LeaseOwner |> isNull) |> should equal true
        row.LeaseExpiresAt.HasValue |> should equal false
    }

[<Fact>]
let ``EnqueueCompletionOutbox guards nulls, blank keys, and unknown sessions`` () =
    task {
        let! store, created = createStoreWithSession ()
        let sessionStore = store :> ISessionStore

        Assert.Throws<ArgumentNullException>(fun () ->
            sessionStore
                .EnqueueCompletionOutbox(tenant, Unchecked.defaultof<SessionCompletion>, CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore)
        |> ignore

        Assert.Throws<ArgumentException>(fun () ->
            sessionStore
                .EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "  ", CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore)
        |> ignore

        Assert.Throws<SessionNotFoundException>(fun () ->
            sessionStore
                .EnqueueCompletionOutbox(tenant, sampleCompletion (SessionId.New()) "key-x", CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore)
        |> ignore
    }

[<Fact>]
let ``EnqueueCompletionOutbox is idempotent: the same key observes the first row`` () =
    task {
        let! store, created = createStoreWithSession ()
        let sessionStore = store :> ISessionStore

        let! first =
            sessionStore.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        let! retry =
            sessionStore.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        retry.CreatedAt |> should equal first.CreatedAt
        retry.Delivered |> should equal false

        // One row, not two: a claim observes a single batch entry.
        let! claimed =
            sessionStore.ClaimCompletionOutbox("owner-a", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        claimed.Count |> should equal 1
    }

[<Fact>]
let ``ClaimCompletionOutbox leases oldest-first, bounded, skipping delivered and live leases`` () =
    task {
        let store = FakeSessionStore()
        let clock = TestClock(sessionStamp)
        store.Clock <- clock
        let sessionStore = store :> ISessionStore
        let! created = sessionStore.CreateSession(tenant, sampleSession (), CancellationToken.None)

        for key in [| "key-1"; "key-2"; "key-3" |] do
            let! _ =
                sessionStore.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id key, CancellationToken.None)

            clock.Advance(TimeSpan.FromSeconds 1.)

        let! first = sessionStore.ClaimCompletionOutbox("owner-a", 2, TimeSpan.FromMinutes 5., CancellationToken.None)

        first.Count |> should equal 2
        first[0].IdempotencyKey |> should equal "key-1"
        first[1].IdempotencyKey |> should equal "key-2"
        first[0].LeaseOwner |> should equal "owner-a"

        // A live lease hides the row from every other owner.
        let! hidden = sessionStore.ClaimCompletionOutbox("owner-b", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        hidden
        |> Seq.exists (fun row -> row.IdempotencyKey = "key-1")
        |> should equal false

        hidden
        |> Seq.exists (fun row -> row.IdempotencyKey = "key-3")
        |> should equal true
    }

[<Fact>]
let ``ClaimCompletionOutbox guards owner, batch, and lease`` () =
    task {
        let! store, _ = createStoreWithSession ()
        let sessionStore = store :> ISessionStore

        Assert.Throws<ArgumentNullException>(fun () ->
            sessionStore
                .ClaimCompletionOutbox(nullString, 10, TimeSpan.FromMinutes 5., CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore)
        |> ignore

        Assert.Throws<ArgumentOutOfRangeException>(fun () ->
            sessionStore
                .ClaimCompletionOutbox("owner-a", 0, TimeSpan.FromMinutes 5., CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore)
        |> ignore

        Assert.Throws<ArgumentOutOfRangeException>(fun () ->
            sessionStore
                .ClaimCompletionOutbox("owner-a", 10, TimeSpan.Zero, CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore)
        |> ignore
    }

[<Fact>]
let ``An expired lease is claimable again by another owner`` () =
    task {
        let store = FakeSessionStore()
        let clock = TestClock(sessionStamp)
        store.Clock <- clock
        let sessionStore = store :> ISessionStore
        let! created = sessionStore.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! _ =
            sessionStore.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        let! first = sessionStore.ClaimCompletionOutbox("owner-a", 10, TimeSpan.FromMinutes 5., CancellationToken.None)
        first.Count |> should equal 1

        clock.Advance(TimeSpan.FromMinutes 6.)

        let! retaken =
            sessionStore.ClaimCompletionOutbox("owner-b", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        retaken.Count |> should equal 1
        retaken[0].LeaseOwner |> should equal "owner-b"
    }

[<Fact>]
let ``VerifyCompletionClaim is fail-closed and side-effect free`` () =
    task {
        let! store, created = createStoreWithSession ()
        let sessionStore = store :> ISessionStore

        let! missing = sessionStore.VerifyCompletionClaim(tenant, "nope", "owner-a", CancellationToken.None)
        missing |> should equal false

        let! _ =
            sessionStore.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        // Unleased rows verify false: nothing changed, so the row stays
        // claimable.
        let! unleased = sessionStore.VerifyCompletionClaim(tenant, "key-1", "owner-a", CancellationToken.None)
        unleased |> should equal false

        let! _ = sessionStore.ClaimCompletionOutbox("owner-a", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        let! live = sessionStore.VerifyCompletionClaim(tenant, "key-1", "owner-a", CancellationToken.None)
        live |> should equal true

        let! wrongOwner = sessionStore.VerifyCompletionClaim(tenant, "key-1", "owner-b", CancellationToken.None)
        wrongOwner |> should equal false

        let! wrongTenant = sessionStore.VerifyCompletionClaim(otherTenant, "key-1", "owner-a", CancellationToken.None)
        wrongTenant |> should equal false

        Assert.Throws<ArgumentException>(fun () ->
            sessionStore
                .VerifyCompletionClaim(tenant, "  ", "owner-a", CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore)
        |> ignore
    }

[<Fact>]
let ``MarkCompletionDelivered marks under a live lease and rejects the loser with zero effects`` () =
    task {
        let store = FakeSessionStore()
        let clock = TestClock(sessionStamp)
        store.Clock <- clock
        let sessionStore = store :> ISessionStore
        let! created = sessionStore.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! _ =
            sessionStore.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        // Missing rows mark false.
        let! missing = sessionStore.MarkCompletionDelivered(tenant, "nope", "owner-a", CancellationToken.None)
        missing |> should equal false

        let! _ = sessionStore.ClaimCompletionOutbox("owner-a", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        // The loser marks false with zero effects: the row stays pending
        // under the winner's live lease.
        let! loser = sessionStore.MarkCompletionDelivered(tenant, "key-1", "owner-b", CancellationToken.None)
        loser |> should equal false

        let! winnerVisible =
            sessionStore.ClaimCompletionOutbox("owner-c", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        winnerVisible.Count |> should equal 0

        let! winnerLive = sessionStore.VerifyCompletionClaim(tenant, "key-1", "owner-a", CancellationToken.None)
        winnerLive |> should equal true

        // The winner marks true; the mark is idempotent afterwards.
        let! marked = sessionStore.MarkCompletionDelivered(tenant, "key-1", "owner-a", CancellationToken.None)
        marked |> should equal true

        let! again = sessionStore.MarkCompletionDelivered(tenant, "key-1", "owner-b", CancellationToken.None)
        again |> should equal true

        // Delivered rows never claim again.
        let! claimed =
            sessionStore.ClaimCompletionOutbox("owner-c", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        claimed.Count |> should equal 0

        Assert.Throws<ArgumentNullException>(fun () ->
            sessionStore
                .MarkCompletionDelivered(tenant, "key-1", nullString, CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore)
        |> ignore
    }

[<Fact>]
let ``An expired lease cannot mark: the retake winner owns the row`` () =
    task {
        let store = FakeSessionStore()
        let clock = TestClock(sessionStamp)
        store.Clock <- clock
        let sessionStore = store :> ISessionStore
        let! created = sessionStore.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let! _ =
            sessionStore.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        let! _ = sessionStore.ClaimCompletionOutbox("owner-a", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        clock.Advance(TimeSpan.FromMinutes 6.)

        let! stale = sessionStore.MarkCompletionDelivered(tenant, "key-1", "owner-a", CancellationToken.None)
        stale |> should equal false

        let! _ = sessionStore.ClaimCompletionOutbox("owner-b", 10, TimeSpan.FromMinutes 5., CancellationToken.None)
        let! winner = sessionStore.MarkCompletionDelivered(tenant, "key-1", "owner-b", CancellationToken.None)
        winner |> should equal true
    }

[<Fact>]
let ``PurgeDeliveredCompletions removes only delivered rows at or before the cutoff`` () =
    task {
        let store = FakeSessionStore()
        let clock = TestClock(sessionStamp)
        store.Clock <- clock
        let sessionStore = store :> ISessionStore
        let! created = sessionStore.CreateSession(tenant, sampleSession (), CancellationToken.None)

        for key in
            [|
                "old-delivered"
                "new-delivered"
                "pending"
            |] do
            let! _ =
                sessionStore.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id key, CancellationToken.None)

            clock.Advance(TimeSpan.FromSeconds 1.)

        let! _ = sessionStore.ClaimCompletionOutbox("owner-a", 10, TimeSpan.FromMinutes 50., CancellationToken.None)
        let! _ = sessionStore.MarkCompletionDelivered(tenant, "old-delivered", "owner-a", CancellationToken.None)

        clock.Advance(TimeSpan.FromDays 8.)

        let! _ = sessionStore.ClaimCompletionOutbox("owner-a", 10, TimeSpan.FromMinutes 50., CancellationToken.None)
        let! _ = sessionStore.MarkCompletionDelivered(tenant, "new-delivered", "owner-a", CancellationToken.None)

        let cutoff = clock.GetUtcNow() - TimeSpan.FromDays 7.
        let! purged = sessionStore.PurgeDeliveredCompletions(cutoff, CancellationToken.None)

        purged |> should equal 1

        // The pending row and the recently delivered row survive.
        let! survivor = sessionStore.VerifyCompletionClaim(tenant, "pending", "owner-a", CancellationToken.None)
        survivor |> should equal true

        let! recent = sessionStore.MarkCompletionDelivered(tenant, "new-delivered", "owner-a", CancellationToken.None)
        recent |> should equal true

        let! gone = sessionStore.VerifyCompletionClaim(tenant, "old-delivered", "owner-a", CancellationToken.None)
        gone |> should equal false
    }
