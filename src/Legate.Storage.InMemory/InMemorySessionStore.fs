// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.InMemory

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage

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

    let copySession (session: Session) =
        { session with
            Options = SessionOptionsPersistence.Snapshot session.Options
            PermissionGrants =
                if isNull (box session.PermissionGrants) then
                    List<string>()
                else
                    List<string>(session.PermissionGrants)
        }

    let copyCompletion (completion: SessionCompletion) : SessionCompletion =
        match JsonSerializer.Deserialize<SessionCompletion>(JsonSerializer.Serialize completion) with
        | null -> raise (InvalidOperationException("Completion snapshot must be non-null."))
        | copied -> copied

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

    /// Projects a stored outbox row to its contract entry.
    let outboxEntry (row: OutboxRow) : CompletionOutboxEntry =
        {
            Tenant = row.Tenant
            SessionId = row.Completion.SessionId
            DestinationId = row.DestinationId
            IdempotencyKey = row.Completion.IdempotencyKey
            Completion = copyCompletion row.Completion
            CreatedAt = row.CreatedAt
            Delivered = row.Delivered
            DeliveredAt = row.DeliveredAt
            LeaseOwner = row.LeaseOwner
            LeaseExpiresAt = row.LeaseExpiresAt
        }

    let controlState tenant sessionId =
        requireSession tenant sessionId |> ignore

        match database.ControlStates.TryGetValue((tenant, sessionId)) with
        | true, text -> ControlTargetProtocol.decode sessionId text
        | _ ->
            raise (
                InvalidSessionStateException(
                    sessionId,
                    "unsupportedControlFormat",
                    "Legacy session control data is unsupported. Start a new session."
                )
            )

    /// Whether a decided-but-unretired control verdict still owns the entry
    /// (issue 363 plus #393): while this holds the quiescent settlement keeps
    /// the prime so the actor's retireControl still has authority, and the
    /// existing prime settle releases it afterwards exactly as before.
    /// Missing control state reads as nothing pending; an undecodable row
    /// reads the same (every control operation would already have refused
    /// it before settlement runs).
    let controlRetirementPending tenant sessionId position =
        try
            match database.ControlStates.TryGetValue((tenant, sessionId)) with
            | false, _ -> false
            | true, text ->
                ControlTargetProtocol.decode sessionId text
                |> fun state -> ControlTargetProtocol.retirementPendingFor state position
        with _ ->
            false

    let controlContext tenant sessionId =
        let session = requireSession tenant sessionId
        session.Options.ValidatePersistence()

        let prime =
            match database.LiveClaims.TryGetValue((tenant, sessionId)) with
            | true, claim -> Some claim
            | _ -> None

        let entries =
            match database.Inboxes.TryGetValue((tenant, sessionId)) with
            | true, entries ->
                entries
                |> Seq.choose (fun entry ->
                    match entry.Payload with
                    | :? UserMessagePayload -> Some(entry.Position, entry.Consumed)
                    | _ -> None)
                |> Map.ofSeq
            | _ -> Map.empty

        {
            SessionId = sessionId
            Lifecycle = session.State
            Now = database.UtcNow
            Prime = prime
            Entries = entries
        }

    let control tenant sessionId (ct: CancellationToken) operation =
        ct.ThrowIfCancellationRequested()

        lock database.Gate (fun () ->
            ct.ThrowIfCancellationRequested()
            let state = controlState tenant sessionId
            let result, updated = operation (controlContext tenant sessionId) state
            ct.ThrowIfCancellationRequested()

            if not (obj.ReferenceEquals(state, updated)) then
                let snapshot = ControlTargetProtocol.encode updated
                ct.ThrowIfCancellationRequested()
                database.ControlStates[(tenant, sessionId)] <- snapshot

            ct.ThrowIfCancellationRequested()
            result)
        |> ok

    let requireNoBinding tenant sessionId =
        let state = controlState tenant sessionId

        if not (isNull (box state.Binding)) then
            raise (
                InvalidSessionStateException(
                    sessionId,
                    "controlPending",
                    "Current control work must be retired before changing prime ownership or lifecycle."
                )
            )

    interface ISessionAbortControlStore with
        member _.TryRecoverControlTarget(tenant, sessionId, turn, owner, duration, ct) =
            control tenant sessionId ct (fun context state ->
                let outcome, recovered =
                    ControlTargetProtocol.recover context state turn owner duration

                match recovered, state.Binding with
                | Some claim, target ->
                    match target with
                    | null ->
                        raise (
                            InvalidSessionStateException(
                                sessionId,
                                "missingControlTarget",
                                "Recovery has no original target."
                            )
                        )
                    | target ->
                        let entry =
                            database.Inboxes[(tenant, sessionId)]
                            |> Seq.find (fun entry -> entry.Position = target.InboxPosition)

                        ct.ThrowIfCancellationRequested()
                        database.LiveClaims[(tenant, sessionId)] <- { claim with Token = claim.Token }
                        database.OpenTurns[(tenant, sessionId)] <- OpenTurnRow(claim.TurnId, claim.Attempt)

                        {
                            Outcome = outcome
                            Target = target
                            Claim = claim
                            Entry = entry
                        },
                        state
                | _ ->
                    {
                        Outcome = outcome
                        Target = state.Binding
                        Claim = null
                        Entry = null
                    },
                    state)

        member _.ReadAbortTarget(tenant, sessionId, ct) =
            control tenant sessionId ct (fun context state -> ControlTargetProtocol.read context state, state)

        member _.RequestHostAbort(tenant, sessionId, turn, cause, reason, ct) =
            control tenant sessionId ct (fun context state ->
                ControlTargetProtocol.request context state turn cause reason)

        member _.BindControlTarget(tenant, sessionId, turn, position, claim, ct) =
            control tenant sessionId ct (fun context state ->
                ControlTargetProtocol.bind context state turn position claim)

        member _.CheckControlTarget(tenant, sessionId, turn, position, claim, ct) =
            control tenant sessionId ct (fun context state ->
                ControlTargetProtocol.check context state turn position claim, state)

        member _.TryDecideControlTarget(tenant, sessionId, turn, position, claim, id, status, cause, reason, ct) =
            control tenant sessionId ct (fun context state ->
                ControlTargetProtocol.decide context state turn position claim id status cause reason)

        member _.RetireControlTarget(tenant, sessionId, turn, position, claim, id, ct) =
            control tenant sessionId ct (fun context state ->
                ControlTargetProtocol.retire context state turn position claim id)

    interface ISessionSettlementStore with
        member _.SupportsSettlementJournal(eventStore) =
            match eventStore with
            | :? InMemorySessionEventStore as journal -> Object.ReferenceEquals(database, journal.Database)
            | _ -> false

        member _.AdmitExecution(tenant, sessionId, position, claim, _) =
            if isNull (box claim) then
                raise (ArgumentNullException(nameof claim))

            lock database.Gate (fun () ->
                let session =
                    match sessionRow tenant sessionId with
                    | Some row -> row
                    | None ->
                        raise (
                            SessionNotFoundException(
                                sessionId,
                                sprintf "No session %O exists in tenant %O." sessionId tenant
                            )
                        )

                if session.State = SessionState.Closed then
                    false
                else
                    let entryExists =
                        match database.Inboxes.TryGetValue((tenant, sessionId)) with
                        | true, entries -> entries |> Seq.exists (fun entry -> entry.Position = position)
                        | false, _ -> false

                    if not entryExists then
                        false
                    else
                        match resolve tenant claim with
                        | Choice1Of3(id, live) when id = sessionId && live.ExpiresAt > database.UtcNow ->
                            let key = (tenant, sessionId, position)

                            match database.ExecutionAdmissions.TryGetValue key with
                            | true, admitted ->
                                admitted.TurnId = claim.TurnId
                                && admitted.Token = claim.Token
                                && admitted.Attempt = claim.Attempt
                            | _ ->
                                database.ExecutionAdmissions[key] <- claim
                                true
                        | _ -> false)
            |> ok

        member _.SettleExecution(tenant, request, _) =
            if isNull (box request) then
                raise (ArgumentNullException(nameof request))

            if isNull (box request.Claim) then
                raise (ArgumentNullException(nameof request))

            if isNull (box request.Result) then
                raise (ArgumentNullException(nameof request))

            if String.IsNullOrWhiteSpace request.CompletionKey then
                raise (ArgumentException("The completion key must be a non-empty string.", nameof request))

            lock database.Gate (fun () ->
                let sessionId = request.SessionId
                let key = (tenant, sessionId, request.Position)

                let session =
                    match sessionRow tenant sessionId with
                    | Some row -> row
                    | None ->
                        raise (
                            SessionNotFoundException(
                                sessionId,
                                sprintf "No session %O exists in tenant %O." sessionId tenant
                            )
                        )

                let emptyEvents = Array.empty<SessionEvent> :> IReadOnlyList<SessionEvent>

                let rejected () =
                    SessionSettlementOutcome(
                        SessionSettlementStatus.Rejected,
                        session.State,
                        Unchecked.defaultof<TurnResult>,
                        Unchecked.defaultof<SessionCompletion>,
                        Unchecked.defaultof<InboxEntry>,
                        emptyEvents,
                        null
                    )

                let fingerprint =
                    sprintf
                        "%O|%d|%O|%s|%d|%O|%O|%s"
                        sessionId
                        request.Position
                        request.Claim.TurnId
                        request.Claim.Token
                        request.Claim.Attempt
                        request.ExecutionId
                        request.Result.Status
                        request.CompletionKey

                match database.ExecutionSettlements.TryGetValue key with
                | true, (prior, outcome) when prior = fingerprint ->
                    SessionSettlementOutcome(
                        SessionSettlementStatus.AlreadyApplied,
                        outcome.State,
                        outcome.Result,
                        outcome.Completion,
                        outcome.Following,
                        emptyEvents,
                        outcome.JournalReason
                    )
                | true, _ -> rejected ()
                | false, _ ->
                    if session.State = SessionState.Closed then
                        rejected ()
                    else
                        let claim = request.Claim

                        let admitted =
                            match database.ExecutionAdmissions.TryGetValue key with
                            | true, prior ->
                                prior.TurnId = claim.TurnId
                                && prior.Token = claim.Token
                                && prior.Attempt = claim.Attempt
                            | _ -> false

                        match resolve tenant claim with
                        | Choice1Of3(id, _) when id = sessionId && admitted ->
                            let result = request.Result

                            match result.Status with
                            | TurnStatus.Completed
                            | TurnStatus.Aborted
                            | TurnStatus.Failed -> ()
                            | _ -> raise (ArgumentException("Settlement requires a terminal result.", nameof request))

                            let entryExists =
                                match database.Inboxes.TryGetValue((tenant, sessionId)) with
                                | true, entries ->
                                    entries |> Seq.exists (fun entry -> entry.Position = request.Position)
                                | false, _ -> false

                            if not entryExists then
                                rejected ()
                            else
                                let mutable events = emptyEvents
                                let mutable journalReason: string | null = null

                                match request.TerminalEvent with
                                | null -> ()
                                | terminalEvent ->
                                    let journal = InMemorySessionEventStore(database) :> ISessionEventStore
                                    let batch = [| terminalEvent |] :> IReadOnlyList<SessionEvent>

                                    try
                                        match
                                            journal
                                                .Append(tenant, sessionId, claim.Token, batch, CancellationToken.None)
                                                .GetAwaiter()
                                                .GetResult()
                                        with
                                        | :? EventAppended as appended -> events <- appended.Events
                                        | _ -> journalReason <- "rejected"
                                    with
                                    | :? EventLimitExceededException -> journalReason <- "rejected"
                                    | _ -> journalReason <- "failed"

                                consume tenant sessionId request.Position

                                let following =
                                    pendingInOrder tenant sessionId
                                    |> List.filter (fun entry ->
                                        not (isNull (box entry)) && (entry.Payload :? UserMessagePayload))
                                    |> List.sortBy (fun entry ->
                                        (if entry.Delivery = DeliveryMode.Interrupt then 0 else 1), entry.Position)
                                    |> List.tryHead

                                let autoClose =
                                    result.Status = TurnStatus.Completed
                                    && not (isNull (box session.Options))
                                    && session.Options.AutoClose

                                let state =
                                    if autoClose then SessionState.Closed
                                    elif following.IsSome then SessionState.Running
                                    else SessionState.Idle

                                let completion: SessionCompletion | null =
                                    if isNull (box session.Options) then
                                        Unchecked.defaultof<SessionCompletion>
                                    else
                                        match session.Options.CompletionDestinationId with
                                        | null -> Unchecked.defaultof<SessionCompletion>
                                        | destinationId ->
                                            if not (CompletionDestinationRules.IsValid destinationId) then
                                                Unchecked.defaultof<SessionCompletion>
                                            else
                                                let payload =
                                                    {
                                                        SessionId = sessionId
                                                        TurnResult = result
                                                        Metadata = session.Options.Metadata
                                                        IdempotencyKey = request.CompletionKey
                                                    }

                                                match database.Outbox.TryGetValue((tenant, request.CompletionKey)) with
                                                | true, row -> copyCompletion row.Completion
                                                | false, _ ->
                                                    let row =
                                                        OutboxRow(
                                                            tenant,
                                                            destinationId,
                                                            copyCompletion payload,
                                                            database.UtcNow
                                                        )

                                                    database.Outbox[(tenant, request.CompletionKey)] <- row
                                                    copyCompletion row.Completion

                                database.UsageCheckpoints[(tenant, sessionId, claim.TurnId)] <- result.Usage

                                // Quiescent release (issue 363): the prime and
                                // its turn tracking clear only here. While a
                                // decided-but-unretired control verdict still
                                // owns the entry (#393), the prime stays so
                                // the actor's retireControl keeps authority;
                                // the existing prime settle releases it
                                // afterwards exactly as before.
                                if
                                    state <> SessionState.Running
                                    && not (controlRetirementPending tenant sessionId request.Position)
                                then
                                    applySettlement tenant sessionId claim result.Status result.Outcome

                                let stored = requireSession tenant sessionId

                                database.Sessions[(tenant, sessionId)] <-
                                    { stored with
                                        State = state
                                        UpdatedAt = database.UtcNow
                                        ClosedAt =
                                            (if autoClose then
                                                 Nullable database.UtcNow
                                             else
                                                 stored.ClosedAt)
                                        PermissionGrants =
                                            (if autoClose then
                                                 Array.empty<string> :> IReadOnlyList<string>
                                             else
                                                 stored.PermissionGrants)
                                    }

                                let outcome =
                                    SessionSettlementOutcome(
                                        SessionSettlementStatus.Applied,
                                        state,
                                        result,
                                        completion,
                                        (following |> Option.defaultValue Unchecked.defaultof<InboxEntry>),
                                        events,
                                        journalReason
                                    )

                                database.ExecutionSettlements[key] <- (fingerprint, outcome)
                                outcome
                        | _ -> rejected ())
            |> ok

    interface ISessionStore with

        member _.CreateSession(tenant, session, _) =
            if isNull (box session) then
                raise (ArgumentNullException(nameof session))

            let session = copySession session

            lock database.Gate (fun () ->
                match
                    database.Sessions.Values
                    |> Seq.tryFind (fun existing -> existing.Id = session.Id)
                with
                | Some _ ->
                    raise (
                        InvalidSessionStateException(
                            session.Id,
                            nameof SessionState,
                            "The session id already exists in this shared store."
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

                    database.ControlStates[(tenant, session.Id)] <-
                        ControlTargetProtocol.encode (ControlTargetProtocol.fresh ())

                    copySession stored)
            |> ok

        member _.GetSession(tenant, sessionId, _) =
            lock database.Gate (fun () -> sessionRow tenant sessionId |> Option.map copySession)
            |> Option.toObj
            |> ok

        member _.ListRecoveryCandidates(tenant, state, size, token, ct) =
            ct.ThrowIfCancellationRequested()
            RecoveryCandidateCursor.validate state size
            let cursor = RecoveryCandidateCursor.decode tenant state token

            lock database.Gate (fun () ->
                ct.ThrowIfCancellationRequested()

                let identities =
                    database.Sessions.Values |> Seq.filter (fun session -> session.Tenant = tenant)

                let upper =
                    match cursor with
                    | Some cursor -> cursor.Upper
                    | None ->
                        identities
                        |> Seq.map (fun session -> session.Id.ToString())
                        |> Seq.sortWith (fun a b -> StringComparer.Ordinal.Compare(a, b))
                        |> Seq.tryLast
                        |> Option.defaultValue ""

                let last =
                    cursor |> Option.map (fun cursor -> cursor.Last) |> Option.defaultValue ""

                let rows =
                    identities
                    |> Seq.filter (fun session -> session.State = state && session.CurrentTurnId.HasValue)
                    |> Seq.map (fun session -> session.Id.ToString())
                    |> Seq.filter (fun id ->
                        StringComparer.Ordinal.Compare(id, last) > 0
                        && StringComparer.Ordinal.Compare(id, upper) <= 0)
                    |> Seq.sortWith (fun a b -> StringComparer.Ordinal.Compare(a, b))
                    |> Seq.truncate (size + 1)
                    |> Seq.toList

                RecoveryCandidateCursor.page tenant state size upper rows)
            |> ok

        member _.ListSessions(tenant, state, agentId, createdFrom, createdTo, pageSize, continuation, _) =
            if pageSize <= 0 then
                raise (ArgumentOutOfRangeException(nameof pageSize, "The page size must be positive."))

            lock database.Gate (fun () ->
                // The stable ordering key is the (updatedAt, id) pair; both
                // are rendered as fixed-width ordinal strings so the
                // comparison is total and independent of clock offsets. The
                // agent and created-time filters narrow the set before the
                // ordering applies, so paging walks the filtered set.
                let tokenOf (session: Session) =
                    sprintf "%s|%O" (session.UpdatedAt.ToString "O") session.Id

                let query =
                    database.Sessions.Values
                    |> Seq.filter (fun session -> session.Tenant.Equals tenant)
                    |> Seq.filter (fun session -> state.HasValue |> not || session.State = state.Value)
                    |> Seq.filter (fun session -> agentId.HasValue |> not || session.AgentId.Equals(agentId.Value))
                    |> Seq.filter (fun session ->
                        createdFrom.HasValue |> not || session.CreatedAt >= createdFrom.Value)
                    |> Seq.filter (fun session -> createdTo.HasValue |> not || session.CreatedAt <= createdTo.Value)
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
                    SessionPage.Items = (page |> List.map copySession) :> IReadOnlyList<Session>
                    Continuation =
                        (match hasMore, List.tryLast page with
                         | true, Some last -> tokenOf last
                         | _ -> null)
                })
            |> ok

        member _.UpdateSessionState(tenant, sessionId, state, _) =
            lock database.Gate (fun () ->
                let session = requireSession tenant sessionId
                ControlTargetProtocol.requireTransition sessionId state (controlState tenant sessionId)

                if state = SessionState.Idle || state = SessionState.Closed then
                    requireNoBinding tenant sessionId

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
                copySession updated)
            |> ok

        member _.CloseSession(tenant, sessionId, _) =
            lock database.Gate (fun () ->
                let session = requireSession tenant sessionId
                requireNoBinding tenant sessionId

                if session.State = SessionState.Closed then
                    copySession session
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
                    copySession updated)
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
                copySession updated)
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
                requireNoBinding tenant sessionId
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
                    requireNoBinding tenant sessionId
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
                    requireNoBinding tenant sessionId
                    // later settle observes the applied settlement.
                    applySettlement tenant sessionId claim TurnStatus.Aborted null
                    TurnLeaseHeld live :> TurnLeaseState
                | Choice2Of3() -> TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState
                | Choice3Of3() -> TurnLeaseMissing claim.TurnId :> TurnLeaseState)
            |> ok

        member _.EnqueueCompletionOutbox(tenant, destinationId, completion, _) =
            CompletionDestinationRules.Validate destinationId

            if isNull (box completion) then
                raise (ArgumentNullException(nameof completion))

            if String.IsNullOrWhiteSpace completion.IdempotencyKey then
                raise (
                    ArgumentException("The completion's idempotency key must be a non-empty string.", nameof completion)
                )

            lock database.Gate (fun () ->
                requireSession tenant completion.SessionId |> ignore

                match database.Outbox.TryGetValue((tenant, completion.IdempotencyKey)) with
                | true, row ->
                    if row.Completion.SessionId <> completion.SessionId then
                        raise (ArgumentException("The idempotency key belongs to another session."))

                    outboxEntry row
                | false, _ ->
                    let row =
                        OutboxRow(tenant, destinationId, copyCompletion completion, database.UtcNow)

                    database.Outbox[(tenant, completion.IdempotencyKey)] <- row
                    outboxEntry row)
            |> ok

        member _.ClaimCompletionOutbox(owner, maxBatch, leaseDuration, _) =
            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            if maxBatch <= 0 then
                raise (ArgumentOutOfRangeException(nameof maxBatch, "The batch size must be positive."))

            if leaseDuration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

            lock database.Gate (fun () ->
                let now = database.UtcNow

                database.Outbox.Values
                |> Seq.filter (fun row -> not row.Delivered)
                |> Seq.filter (fun row ->
                    isNull (box row.LeaseOwner)
                    || not row.LeaseExpiresAt.HasValue
                    || row.LeaseExpiresAt.Value <= now)
                |> Seq.sortBy (fun row ->
                    row.LeaseExpiresAt.HasValue,
                    row.LeaseExpiresAt.GetValueOrDefault(),
                    row.CreatedAt,
                    row.Completion.IdempotencyKey)
                |> Seq.truncate maxBatch
                |> Seq.map (fun row ->
                    row.LeaseOwner <- owner
                    row.LeaseExpiresAt <- Nullable(now + leaseDuration)
                    outboxEntry row)
                |> Seq.toList
                :> IReadOnlyList<CompletionOutboxEntry>)
            |> ok

        member _.VerifyCompletionClaim(tenant, idempotencyKey, owner, _) =
            if String.IsNullOrWhiteSpace idempotencyKey then
                raise (ArgumentException("The idempotency key must be a non-empty string.", nameof idempotencyKey))

            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            lock database.Gate (fun () ->
                match database.Outbox.TryGetValue((tenant, idempotencyKey)) with
                | false, _ -> false
                | true, row ->
                    not row.Delivered
                    && not (isNull (box row.LeaseOwner))
                    && String.Equals(row.LeaseOwner, owner, StringComparison.Ordinal)
                    && row.LeaseExpiresAt.HasValue
                    && row.LeaseExpiresAt.Value > database.UtcNow)
            |> ok

        member _.RenewCompletionClaim(tenant, key, owner, duration, ct) =
            ct.ThrowIfCancellationRequested()

            if duration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof duration))

            lock database.Gate (fun () ->
                match database.Outbox.TryGetValue((tenant, key)) with
                | true, row when
                    not row.Delivered
                    && row.LeaseOwner = owner
                    && row.LeaseExpiresAt.HasValue
                    && row.LeaseExpiresAt.Value > database.UtcNow
                    ->
                    row.LeaseExpiresAt <- Nullable(database.UtcNow + duration)
                    true
                | _ -> false)
            |> ok

        member _.MarkCompletionDelivered(tenant, idempotencyKey, owner, _) =
            if String.IsNullOrWhiteSpace idempotencyKey then
                raise (ArgumentException("The idempotency key must be a non-empty string.", nameof idempotencyKey))

            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            lock database.Gate (fun () ->
                match database.Outbox.TryGetValue((tenant, idempotencyKey)) with
                | false, _ -> false
                | true, row when row.Delivered -> true
                | true, row ->
                    if isNull (box row.LeaseOwner) then
                        false
                    elif not (String.Equals(row.LeaseOwner, owner, StringComparison.Ordinal)) then
                        false
                    elif not row.LeaseExpiresAt.HasValue || row.LeaseExpiresAt.Value <= database.UtcNow then
                        false
                    else
                        row.Delivered <- true
                        row.DeliveredAt <- Nullable(database.UtcNow)
                        row.LeaseOwner <- Unchecked.defaultof<string>
                        row.LeaseExpiresAt <- Nullable()
                        true)
            |> ok

        member _.PurgeDeliveredCompletions(deliveredBefore, _) =
            lock database.Gate (fun () ->
                let victims =
                    database.Outbox
                    |> Seq.filter (fun pair ->
                        pair.Value.Delivered
                        && pair.Value.DeliveredAt.HasValue
                        && pair.Value.DeliveredAt.Value <= deliveredBefore)
                    |> Seq.map (fun pair -> pair.Key)
                    |> Seq.toList

                for key in victims do
                    database.Outbox.Remove(key) |> ignore

                victims.Length)
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
