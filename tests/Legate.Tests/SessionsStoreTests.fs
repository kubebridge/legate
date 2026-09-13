// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionsStoreTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
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
    let mutable positionCounter = 0L

    let key (tenantId: TenantId) (sessionId: SessionId) =
        sprintf "%s|%s" tenantId.Value sessionId.Value

    let claimKey (tenantId: TenantId) (turnId: TurnId) = (tenantId, turnId)

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
                    }

                sessions[key t sessionId] <- closed
                Task.FromResult closed

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
    // The tenancy rule: session, inbox, claim, and dispatch operations
    // carry the tenant; only the cross-tenant process-wide count does not.
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

    let processWide = methods |> Array.find (fun m -> m.Name = "CountRunningSessions")

    processWide.GetParameters()
    |> Array.exists (fun p -> p.ParameterType = typeof<TenantId>)
    |> should equal false
