// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.InMemory

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate

/// The in-memory <see cref="T:Legate.ISessionStore" />: session CRUD, the
/// inbox, turn claims under a lease with the fencing semantics the contract
/// pins, dispatch candidates, and the capacity counts. One live claim per
/// session and one open turn per session:
/// <see cref="M:Legate.ISessionStore.ClaimNextTurn" /> consumes the head
/// pending user message into a new turn, or the head pending reply into a
/// resume of the open turn with the attempt incremented; a live unexpired
/// claim makes every further claim the missing branch. Lease expiry reads
/// the database's clock, and every transition runs under the database's
/// gate lock, so claims and settlements are atomic under concurrent
/// callers and a takeover race leaves the loser with zero effects. The
/// stored row's <see cref="P:Legate.Session.CurrentTurnId" /> is stamped on
/// claim and cleared on settlement, the basis of the CurrentTurnId-based
/// state rules.
type InMemorySessionStore(database: InMemoryDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let mintToken () = Ulid.NewUlid().ToString()

    let ok value = Task.FromResult value

    let sessionRow (tenant: TenantId) (sessionId: SessionId) =
        match database.Sessions.TryGetValue((tenant, sessionId)) with
        | true, row -> Some row
        | false, _ -> None

    let requireSession tenant sessionId =
        match sessionRow tenant sessionId with
        | Some row -> row
        | None ->
            raise (SessionNotFoundException(sessionId, sprintf "No session %O exists in tenant %O." sessionId tenant))

    let pendingInOrder (tenant: TenantId) (sessionId: SessionId) =
        match database.Inboxes.TryGetValue((tenant, sessionId)) with
        | true, entries ->
            entries
            |> Seq.filter (fun entry -> not entry.Consumed)
            |> Seq.sortBy (fun entry -> entry.Position)
            |> Seq.toList
        | false, _ -> []

    let consume (tenant: TenantId) (sessionId: SessionId) (position: int64) =
        match database.Inboxes.TryGetValue((tenant, sessionId)) with
        | true, entries ->
            for index in 0 .. entries.Count - 1 do
                if (not entries[index].Consumed) && entries[index].Position = position then
                    entries[index] <- { entries[index] with Consumed = true }
        | false, _ -> ()

    /// The stored grant list, normalised: a null deserialised value reads
    /// as empty, and duplicates collapse, so every stored row carries a
    /// distinct, non-null grant set.
    let storedGrants (session: Session) : IReadOnlyList<string> =
        if isNull (box session.PermissionGrants) then
            ResizeArray<string>() :> IReadOnlyList<string>
        else
            let distinct = ResizeArray<string>()

            for grant in session.PermissionGrants do
                if not (isNull (box grant)) && not (distinct.Contains grant) then
                    distinct.Add grant

            distinct :> IReadOnlyList<string>

    /// What a fenced call against the claim's turn resolved to: the live
    /// claim the token still matches with its session key, an open turn
    /// the token no longer owns (taken over), or nothing (settled or
    /// unknown). The caller runs under the gate lock.
    let resolve (tenant: TenantId) (claim: TurnClaim) =

        let sessionKeyOfTurn =
            database.OpenTurns
            |> Seq.tryFind (fun pair -> (pair.Key |> fst).Equals tenant && pair.Value.TurnId = claim.TurnId)

        match sessionKeyOfTurn with
        | None -> Choice3Of3()
        | Some pair ->
            let sessionId = pair.Key |> snd

            match database.LiveClaims.TryGetValue((tenant, sessionId)) with
            | true, live when String.Equals(live.Token, claim.Token, StringComparison.Ordinal) ->
                Choice1Of3(sessionId, live)
            | _ -> Choice2Of3()

    /// Stamps the stored session row's CurrentTurnId, the field the
    /// CurrentTurnId-based state rules read.
    let stampCurrentTurn (tenant: TenantId) (sessionId: SessionId) (turnId: TurnId option) =
        match sessionRow tenant sessionId with
        | None -> ()
        | Some session ->
            let stamped =
                match turnId with
                | None -> Nullable()
                | Some id -> Nullable id

            let updated =
                { session with
                    CurrentTurnId = stamped
                    UpdatedAt = database.UtcNow
                }

            database.Sessions[(tenant, sessionId)] <- updated

    let applySettlement
        (tenant: TenantId)
        (sessionId: SessionId)
        (claim: TurnClaim)
        (status: TurnStatus)
        (outcome: TurnOutcome | null)
        =
        database.Settlements[(tenant, sessionId, claim.TurnId)] <- status
        database.Outcomes[(tenant, sessionId, claim.TurnId)] <- outcome
        database.LiveClaims.Remove((tenant, sessionId)) |> ignore
        database.OpenTurns.Remove((tenant, sessionId)) |> ignore
        database.CurrentTurnIds.Remove((tenant, sessionId)) |> ignore
        stampCurrentTurn tenant sessionId None

    interface ISessionStore with

        member _.CreateSession(tenant, session, _) =
            if isNull (box session) then
                raise (ArgumentNullException(nameof session))

            lock database.Gate (fun () ->
                match sessionRow tenant session.Id with
                | Some _ ->
                    raise (
                        InvalidSessionStateException(
                            session.Id,
                            nameof SessionState,
                            sprintf "A session %O already exists in tenant %O." session.Id tenant
                        )
                    )
                | None ->
                    let now = database.UtcNow

                    let stored =
                        { session with
                            Tenant = tenant
                            State = SessionState.Idle
                            CurrentTurnId = Nullable()
                            CreatedAt = now
                            UpdatedAt = now
                            ClosedAt = Nullable()
                            PermissionGrants = storedGrants session
                        }

                    database.Sessions[(tenant, session.Id)] <- stored
                    stored)
            |> ok

        member _.GetSession(tenant, sessionId, _) =
            lock database.Gate (fun () -> sessionRow tenant sessionId) |> Option.toObj |> ok

        member _.ListSessions(tenant, state, pageSize, continuation, _) =
            if pageSize <= 0 then
                raise (ArgumentOutOfRangeException(nameof pageSize, "The page size must be positive."))

            lock database.Gate (fun () ->
                // The stable ordering key is the (updatedAt, id) pair; both
                // are rendered as fixed-width ordinal strings so the
                // comparison is total and independent of clock offsets.
                let tokenOf (session: Session) =
                    sprintf "%s|%O" (session.UpdatedAt.ToString "O") session.Id

                let query =
                    database.Sessions.Values
                    |> Seq.filter (fun session -> session.Tenant.Equals tenant)
                    |> Seq.filter (fun session -> state.HasValue |> not || session.State = state.Value)
                    |> Seq.sortByDescending tokenOf
                    |> Seq.toList

                let remaining =
                    match continuation with
                    | null -> query
                    | token ->
                        match query |> List.tryFindIndex (fun session -> tokenOf session = token) with
                        | Some index -> query |> List.skip (index + 1)
                        | None -> []

                let page = remaining |> List.truncate pageSize

                // The final page's continuation is null: hasMore means
                // something sorts after the last item on the page.
                let hasMore = remaining.Length > page.Length

                {
                    Items = page :> IReadOnlyList<Session>
                    Continuation =
                        (match hasMore, List.tryLast page with
                         | true, Some last -> tokenOf last
                         | _ -> null)
                })
            |> ok

        member _.UpdateSessionState(tenant, sessionId, state, _) =
            lock database.Gate (fun () ->
                let session = requireSession tenant sessionId

                if session.State = SessionState.Closed && state <> SessionState.Closed then
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            nameof session.State,
                            "A closed session cannot leave the Closed state."
                        )
                    )

                let updated =
                    { session with
                        State = state
                        UpdatedAt = database.UtcNow
                    }

                database.Sessions[(tenant, sessionId)] <- updated
                updated)
            |> ok

        member _.CloseSession(tenant, sessionId, _) =
            lock database.Gate (fun () ->
                let session = requireSession tenant sessionId

                if session.State = SessionState.Closed then
                    session
                else
                    let now = database.UtcNow

                    let updated =
                        { session with
                            State = SessionState.Closed
                            UpdatedAt = now
                            ClosedAt = Nullable now
                            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
                        }

                    database.Sessions[(tenant, sessionId)] <- updated
                    updated)
            |> ok

        member _.GrantSessionTool(tenant, sessionId, toolName, _) =
            if String.IsNullOrWhiteSpace toolName then
                raise (ArgumentException("The tool name must be a non-empty string.", nameof toolName))

            lock database.Gate (fun () ->
                let session = requireSession tenant sessionId

                if session.State = SessionState.Closed then
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            nameof session.State,
                            "A closed session carries no grant memory."
                        )
                    )

                let grants = ResizeArray<string>(storedGrants session)

                if not (grants.Contains toolName) then
                    grants.Add toolName

                let updated =
                    { session with
                        PermissionGrants = grants :> IReadOnlyList<string>
                        UpdatedAt = database.UtcNow
                    }

                database.Sessions[(tenant, sessionId)] <- updated
                updated)
            |> ok

        member _.SetSessionAgent(tenant, sessionId, agentId, _) =
            lock database.Gate (fun () ->
                let session = requireSession tenant sessionId

                // The rule is CurrentTurnId-based: the store's claim
                // tracking decides, not the lifecycle state the host
                // maintains.
                if database.CurrentTurnIds.ContainsKey(tenant, sessionId) then
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            nameof session.State,
                            "A turn is running or suspended in the session."
                        )
                    )

                let updated =
                    { session with
                        AgentId = agentId
                        UpdatedAt = database.UtcNow
                    }

                database.Sessions[(tenant, sessionId)] <- updated
                updated)
            |> ok

        member _.SetSessionTitle(tenant, sessionId, title, _) =
            if isNull (box title) then
                raise (ArgumentNullException(nameof title))

            lock database.Gate (fun () ->
                let session = requireSession tenant sessionId

                let updated =
                    { session with
                        Title = title
                        UpdatedAt = database.UtcNow
                    }

                database.Sessions[(tenant, sessionId)] <- updated
                updated)
            |> ok

        member _.AppendInboxMessage(tenant, sessionId, payload, delivery, _) =
            if isNull (box payload) then
                raise (ArgumentNullException(nameof payload))

            lock database.Gate (fun () ->
                requireSession tenant sessionId |> ignore

                let position =
                    match database.InboxPositions.TryGetValue((tenant, sessionId)) with
                    | true, next -> next
                    | false, _ -> 1L

                database.InboxPositions[(tenant, sessionId)] <- position + 1L

                let entry =
                    {
                        SessionId = sessionId
                        Position = position
                        Payload = payload
                        Delivery = delivery
                        Consumed = false
                        AppendedAt = database.UtcNow
                    }

                match database.Inboxes.TryGetValue((tenant, sessionId)) with
                | true, entries -> entries.Add entry
                | false, _ ->
                    let entries = List<InboxEntry>()
                    entries.Add entry
                    database.Inboxes[(tenant, sessionId)] <- entries

                entry)
            |> ok

        member _.ReadPendingInbox(tenant, sessionId, _) =
            lock database.Gate (fun () ->
                requireSession tenant sessionId |> ignore
                pendingInOrder tenant sessionId :> IReadOnlyList<InboxEntry>)
            |> ok

        member _.MarkInboxConsumed(tenant, sessionId, positions, _) =
            if isNull (box positions) then
                raise (ArgumentNullException(nameof positions))

            lock database.Gate (fun () ->
                requireSession tenant sessionId |> ignore

                match database.Inboxes.TryGetValue((tenant, sessionId)) with
                | true, entries ->
                    let wanted = HashSet positions
                    let mutable flipped = 0

                    for index in 0 .. entries.Count - 1 do
                        let entry = entries[index]

                        if not entry.Consumed && wanted.Contains entry.Position then
                            entries[index] <- { entry with Consumed = true }
                            flipped <- flipped + 1

                    flipped
                | false, _ -> 0)
            |> ok

        member _.ClaimNextTurn(tenant, sessionId, owner, leaseDuration, _) =
            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            if leaseDuration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

            lock database.Gate (fun () ->
                requireSession tenant sessionId |> ignore

                let now = database.UtcNow
                let expiresAt = now + leaseDuration

                // One live claim per session: an unexpired claim makes
                // every further claim the missing branch, with no effects.
                let claimHeld =
                    match database.LiveClaims.TryGetValue((tenant, sessionId)) with
                    | true, live -> live.ExpiresAt > now
                    | false, _ -> false

                let pending = pendingInOrder tenant sessionId |> List.tryHead

                if claimHeld then
                    TurnLeaseMissing(TurnId.New()) :> TurnLeaseState
                else
                    let openTurn =
                        match database.OpenTurns.TryGetValue((tenant, sessionId)) with
                        | true, row -> Some row
                        | false, _ -> None

                    match pending, openTurn with
                    | Some {
                               Payload = :? ReplyPayload
                               Position = position
                           },
                      Some openRow ->
                        // Resume: consume the reply and re-claim the same
                        // open turn under a fresh token, attempt + 1.
                        consume tenant sessionId position

                        let attempt = openRow.Attempt + 1

                        let claim =
                            {
                                TurnId = openRow.TurnId
                                Token = mintToken ()
                                Owner = owner
                                ExpiresAt = expiresAt
                                Attempt = attempt
                            }

                        database.OpenTurns[(tenant, sessionId)] <- OpenTurnRow(openRow.TurnId, attempt)
                        database.LiveClaims[(tenant, sessionId)] <- claim
                        database.CurrentTurnIds[(tenant, sessionId)] <- claim.TurnId
                        stampCurrentTurn tenant sessionId (Some claim.TurnId)

                        TurnLeaseRenewed claim :> TurnLeaseState
                    | Some {
                               Payload = :? UserMessagePayload
                               Position = position
                           },
                      _ ->
                        // New turn: consume the message and mint a fresh
                        // turn; a stale open turn from a lapsed claim is
                        // abandoned (replaced, never settled).
                        consume tenant sessionId position

                        let turnId = TurnId.New()

                        let claim =
                            {
                                TurnId = turnId
                                Token = mintToken ()
                                Owner = owner
                                ExpiresAt = expiresAt
                                Attempt = 1
                            }

                        database.OpenTurns[(tenant, sessionId)] <- OpenTurnRow(turnId, 1)
                        database.LiveClaims[(tenant, sessionId)] <- claim
                        database.CurrentTurnIds[(tenant, sessionId)] <- turnId
                        stampCurrentTurn tenant sessionId (Some turnId)

                        TurnLeaseRenewed claim :> TurnLeaseState
                    | _ ->
                        // Nothing claimable: a reply with no open turn to
                        // resume, or an empty inbox.
                        TurnLeaseMissing(TurnId.New()) :> TurnLeaseState)
            |> ok

        member _.RenewClaim(tenant, claim, leaseDuration, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            if leaseDuration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

            lock database.Gate (fun () ->
                match resolve tenant claim with
                | Choice1Of3(sessionId, live) ->
                    if live.ExpiresAt <= database.UtcNow then
                        TurnLeaseLost(claim.TurnId, "expired") :> TurnLeaseState
                    else
                        let renewed =
                            { live with
                                ExpiresAt = database.UtcNow + leaseDuration
                            }

                        database.LiveClaims[(tenant, sessionId)] <- renewed
                        TurnLeaseRenewed renewed :> TurnLeaseState
                | Choice2Of3() -> TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                | Choice3Of3() -> TurnLeaseMissing claim.TurnId :> TurnLeaseState)
            |> ok

        member this.ObserveAndRenewClaim(tenant, claim, leaseDuration, cancellationToken) =
            // Plain atomic renew, matching the Wave 1 fake: no
            // cancellation-request carrier type exists in the contracts
            // yet, so the renew leg is the whole behaviour; the abort epic
            // grows this surface with the store.
            (this :> ISessionStore).RenewClaim(tenant, claim, leaseDuration, cancellationToken)

        member _.VerifyClaim(tenant, claim, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            lock database.Gate (fun () ->
                match resolve tenant claim with
                | Choice1Of3(_, live) ->
                    if live.ExpiresAt <= database.UtcNow then
                        TurnLeaseLost(claim.TurnId, "expired") :> TurnLeaseState
                    else
                        TurnLeaseHeld live :> TurnLeaseState
                | Choice2Of3() -> TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                | Choice3Of3() -> TurnLeaseMissing claim.TurnId :> TurnLeaseState)
            |> ok

        member _.CheckpointUsage(tenant, claim, usage, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            lock database.Gate (fun () ->
                match resolve tenant claim with
                | Choice1Of3(sessionId, live) ->
                    if live.ExpiresAt <= database.UtcNow then
                        TurnLeaseLost(claim.TurnId, "expired") :> TurnLeaseState
                    else
                        database.UsageCheckpoints[(tenant, sessionId, claim.TurnId)] <- usage
                        TurnLeaseHeld live :> TurnLeaseState
                | Choice2Of3() -> TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                | Choice3Of3() -> TurnLeaseMissing claim.TurnId :> TurnLeaseState)
            |> ok

        member _.SettleTurn(tenant, claim, status, outcome, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            lock database.Gate (fun () ->
                // The guard runs inside the gate, so the typed exception
                // carries the session the settle targeted; a claim that
                // resolves nowhere falls through to the stale-claim
                // rejection below.
                let sessionOfClaim =
                    database.OpenTurns
                    |> Seq.tryFind (fun pair -> (pair.Key |> fst).Equals tenant && pair.Value.TurnId = claim.TurnId)
                    |> Option.map (fun pair -> pair.Key |> snd)

                match sessionOfClaim, status with
                | Some sessionId, (TurnStatus.Pending | TurnStatus.Running | TurnStatus.Suspended) ->
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            "nonTerminal",
                            "Only terminal statuses (Completed, Aborted, Failed) settle a turn."
                        )
                    )
                | _ -> ()

                match resolve tenant claim with
                | Choice1Of3(sessionId, _: TurnClaim) ->
                    // The token still fences: the first settle wins, and a
                    // lapsed-but-uncontested lease does not unseat it (a
                    // settle is terminal; nothing can take over a turn the
                    // owner is settling).
                    let settlementKey = (tenant, sessionId, claim.TurnId)

                    match database.Settlements.TryGetValue settlementKey with
                    | true, applied when applied = status -> TurnAlreadySettled claim.TurnId :> TurnSettlement
                    | true, _ -> TurnSettleRejected(claim.TurnId, "alreadySettledByOther") :> TurnSettlement
                    | false, _ ->
                        applySettlement tenant sessionId claim status outcome
                        TurnSettled(claim.TurnId, status) :> TurnSettlement
                | Choice2Of3() -> TurnSettleRejected(claim.TurnId, "staleClaim") :> TurnSettlement
                | Choice3Of3() ->
                    // No live claim for the token. When the turn settled
                    // already, a retry of the same outcome observes it and
                    // a different outcome is rejected; otherwise the claim
                    // is stale.
                    let applied =
                        database.Settlements
                        |> Seq.tryFind (fun pair ->
                            (pair.Key |> fun (t, _, _) -> t).Equals tenant
                            && (pair.Key |> fun (_, _, turn) -> turn) = claim.TurnId)

                    match applied with
                    | Some pair when pair.Value = status -> TurnAlreadySettled claim.TurnId :> TurnSettlement
                    | Some _ -> TurnSettleRejected(claim.TurnId, "alreadySettledByOther") :> TurnSettlement
                    | None -> TurnSettleRejected(claim.TurnId, "staleClaim") :> TurnSettlement)
            |> ok

        member _.AbortTurn(tenant, claim, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            lock database.Gate (fun () ->
                match resolve tenant claim with
                | Choice1Of3(sessionId, live) ->
                    // Abort settles Aborted and releases the lease; a
                    // later settle observes the applied settlement.
                    applySettlement tenant sessionId claim TurnStatus.Aborted null
                    TurnLeaseHeld live :> TurnLeaseState
                | Choice2Of3() -> TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                | Choice3Of3() -> TurnLeaseMissing claim.TurnId :> TurnLeaseState)
            |> ok

        member _.GetDispatchCandidates(tenant, maxBatch, _) =
            if maxBatch <= 0 then
                raise (ArgumentOutOfRangeException(nameof maxBatch, "The batch size must be positive."))

            lock database.Gate (fun () ->
                let withPending =
                    database.Inboxes
                    |> Seq.filter (fun pair -> (pair.Key |> fst).Equals tenant)
                    |> Seq.filter (fun pair -> pair.Value |> Seq.exists (fun entry -> not entry.Consumed))
                    |> Seq.map (fun pair -> pair.Key |> snd)
                    |> Seq.distinct
                    |> Seq.sortBy (fun sessionId -> sessionId.Value)
                    |> Seq.toList

                let batch = withPending |> List.truncate maxBatch

                {
                    Sessions = batch :> IReadOnlyList<SessionId>
                    HasMore = withPending.Length > batch.Length
                })
            |> ok

        member _.CountSessionsByAgent(tenant, agentId, _) =
            lock database.Gate (fun () ->
                database.Sessions.Values
                |> Seq.filter (fun session -> session.Tenant.Equals tenant && session.AgentId.Equals agentId)
                |> Seq.length)
            |> ok

        member _.CountSessionsByTenant(tenant, _) =
            lock database.Gate (fun () ->
                database.Sessions.Values
                |> Seq.filter (fun session -> session.Tenant.Equals tenant)
                |> Seq.length)
            |> ok

        member _.CountRunningSessions(_) =
            lock database.Gate (fun () -> database.CurrentTurnIds.Count) |> ok
