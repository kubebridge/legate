// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionEventStoreTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Xunit

// The generic Deserialize<'T> overload is annotated to return 'T | null,
// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

let jsonOptions = JsonSerializerOptions()

let tenant = TenantId.Create "acme"
let otherTenant = TenantId.Create "other"

let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
let noSequence = Unchecked.defaultof<Nullable<int64>>

/// An in-memory ISessionEventStore implemented entirely from outside the
/// assembly: the C#-friendly-surface proof, mirroring SessionsStoreTests.fs's
/// FakeSessionStore. It implements the documented semantics the tests pin,
/// so each test exercises the contract through behaviour rather than
/// reflection.
type FakeSessionEventStore() =
    // The journals: key -> (sequence -> stamped event). The live flag
    // models the cleanup lifecycle: once completed, the journal is gone
    // (Replay reports the journal expired, cleanup is not claimable
    // again). The token sets model the turn-claim fence the contract
    // appends under.
    let journals = Dictionary<string, Dictionary<int64, SessionEvent>>()
    let live = Dictionary<string, bool>()
    let tokens = Dictionary<string, HashSet<string>>()
    let cleanups = Dictionary<string, EventCleanupClaim>()
    let mutable claimCounter = 0

    let key (tenantId: TenantId) (sessionId: SessionId) =
        sprintf "%s|%s" tenantId.Value sessionId.Value

    let nextSequence (journal: Dictionary<int64, SessionEvent>) =
        let mutable maxSeq = 0L

        for entry in journal do
            if entry.Key > maxSeq then
                maxSeq <- entry.Key

        maxSeq + 1L

    let jsonSize (event: SessionEvent) =
        int64 (JsonSerializer.Serialize(event, jsonOptions).Length)

    // The fake's own limit values: host-configured runtime options in the
    // contract's terms, with small numbers so breaches are easy to force.
    let eventBytesLimit = 500L
    let countLimit = 5L
    let bytesLimit = 1200L
    let batchSizeLimit = 3L

    interface ISessionEventStore with
        member _.Append(t, sessionId, claimToken, events, _) =
            if isNull (box events) then
                raise (ArgumentNullException(nameof events))

            if isNull (box claimToken) then
                raise (ArgumentNullException(nameof claimToken))

            for event in events do
                if box event |> isNull then
                    raise (ArgumentNullException(nameof events))

            if events.Count = 0 then
                raise (ArgumentException("The batch must not be empty.", nameof events))

            match journals.TryGetValue(key t sessionId) with
            | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
            | true, journal ->
                match tokens.TryGetValue(key t sessionId) with
                | false, _ -> raise (SessionNotFoundException(sessionId, "Session not found."))
                | true, accepted when not (accepted.Contains claimToken) ->
                    Task.FromResult(EventAppendRejected(sessionId, "staleClaim") :> EventAppendOutcome)
                | true, _ ->
                    // Limit checks run before anything lands: a breach
                    // throws with the structured properties populated and
                    // never leaves a partial write.
                    if int64 events.Count > batchSizeLimit then
                        raise (
                            EventLimitExceededException(
                                "batchSize",
                                batchSizeLimit,
                                int64 events.Count,
                                "The event batch exceeds the configured batch-size limit."
                            )
                        )
                    else
                        let mutable largest = 0L

                        let batchBytes =
                            let mutable total = 0L

                            for event in events do
                                let size = jsonSize event

                                if size > largest then
                                    largest <- size

                                total <- total + size

                            total

                        if largest > eventBytesLimit then
                            raise (
                                EventLimitExceededException(
                                    "perEventBytes",
                                    eventBytesLimit,
                                    largest,
                                    "An event exceeds the configured per-event byte limit."
                                )
                            )
                        else
                            let journalBytes =
                                let mutable total = 0L

                                for entry in journal do
                                    total <- total + jsonSize entry.Value

                                total

                            if int64 journal.Count + int64 events.Count > countLimit then
                                raise (
                                    EventLimitExceededException(
                                        "perSessionCount",
                                        countLimit,
                                        int64 journal.Count + int64 events.Count,
                                        "The journal would exceed the configured per-session count limit."
                                    )
                                )
                            elif journalBytes + batchBytes > bytesLimit then
                                raise (
                                    EventLimitExceededException(
                                        "perSessionBytes",
                                        bytesLimit,
                                        journalBytes + batchBytes,
                                        "The journal would exceed the configured per-session byte limit."
                                    )
                                )
                            else
                                let stamped = ResizeArray<SessionEvent>()

                                for event in events do
                                    let sequence = nextSequence journal

                                    // The store stamps a fresh instance: the
                                    // input events carry an empty sequence and
                                    // are never mutated.
                                    let copy =
                                        match event with
                                        | :? TextDeltaEvent as delta ->
                                            TextDeltaEvent(
                                                event.SessionId,
                                                event.TurnId,
                                                Nullable sequence,
                                                event.Timestamp,
                                                delta.Text
                                            )
                                            :> SessionEvent
                                        | :? UsageEvent as usage ->
                                            UsageEvent(
                                                event.SessionId,
                                                event.TurnId,
                                                Nullable sequence,
                                                event.Timestamp,
                                                usage.InputTokens,
                                                usage.OutputTokens
                                            )
                                            :> SessionEvent
                                        | :? TurnStartedEvent ->
                                            TurnStartedEvent(
                                                event.SessionId,
                                                event.TurnId,
                                                Nullable sequence,
                                                event.Timestamp
                                            )
                                            :> SessionEvent
                                        | :? TurnCompletedEvent ->
                                            TurnCompletedEvent(
                                                event.SessionId,
                                                event.TurnId,
                                                Nullable sequence,
                                                event.Timestamp
                                            )
                                            :> SessionEvent
                                        | other ->
                                            failwithf
                                                "the fake journals only the event kinds the tests use, not %s"
                                                (other.GetType().Name)

                                    journal[sequence] <- copy
                                    stamped.Add copy

                                Task.FromResult(
                                    EventAppended(stamped :> IReadOnlyList<SessionEvent>) :> EventAppendOutcome
                                )

        member _.Replay(t, sessionId, fromSequence, limit, _) =
            if limit <= 0 then
                raise (ArgumentOutOfRangeException(nameof limit))

            if fromSequence < 0L then
                raise (ArgumentOutOfRangeException(nameof fromSequence))

            match journals.TryGetValue(key t sessionId) with
            | false, _ -> Task.FromResult(EventReplayUnknownSession(sessionId) :> EventReplayOutcome)
            | true, _ ->
                match live.TryGetValue(key t sessionId) with
                | true, false -> Task.FromResult(EventReplayJournalExpired(sessionId) :> EventReplayOutcome)
                | _ ->
                    let journal = journals[key t sessionId]

                    let page =
                        journal
                        |> Seq.filter (fun entry -> entry.Key > fromSequence)
                        |> Seq.sortBy (fun entry -> entry.Key)
                        |> Seq.truncate limit
                        |> Seq.map (fun entry -> entry.Value)
                        |> ResizeArray

                    if page.Count = 0 then
                        Task.FromResult(EventReplayEndOfStream(sessionId) :> EventReplayOutcome)
                    else
                        let next = (page[page.Count - 1]).Sequence.Value

                        Task.FromResult(
                            EventReplayPage(sessionId, page :> IReadOnlyList<SessionEvent>, Nullable next)
                            :> EventReplayOutcome
                        )

        member _.TryClaimCleanup(t, sessionId, owner, leaseDuration, _) =
            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            if leaseDuration <= TimeSpan.Zero then
                raise (ArgumentOutOfRangeException(nameof leaseDuration))

            match journals.TryGetValue(key t sessionId) with
            | false, _ -> Task.FromResult(EventCleanupNotClaimable(sessionId, "unknownSession") :> EventCleanupState)
            | true, _ ->
                match live.TryGetValue(key t sessionId) with
                | true, false ->
                    Task.FromResult(EventCleanupNotClaimable(sessionId, "journalGone") :> EventCleanupState)
                | _ ->
                    match cleanups.TryGetValue(key t sessionId) with
                    | true, held when held.ExpiresAt > stamp ->
                        Task.FromResult(EventCleanupNotClaimable(sessionId, "leaseHeld") :> EventCleanupState)
                    | _ ->
                        claimCounter <- claimCounter + 1

                        let claim =
                            {
                                SessionId = sessionId
                                Token = "cleanup-token-" + claimCounter.ToString()
                                Owner = owner
                                ExpiresAt = stamp.Add leaseDuration
                            }

                        cleanups[key t sessionId] <- claim
                        Task.FromResult(EventCleanupClaimed(claim) :> EventCleanupState)

        member _.CompleteCleanup(t, sessionId, claimToken, _) =
            if isNull (box claimToken) then
                raise (ArgumentNullException(nameof claimToken))

            match cleanups.TryGetValue(key t sessionId) with
            | false, _ -> Task.FromResult(EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)
            | true, claim when claim.Token <> claimToken ->
                Task.FromResult(EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)
            | true, claim when claim.ExpiresAt <= stamp ->
                Task.FromResult(EventCleanupRejected(sessionId, "leaseExpired") :> EventCleanupSettlement)
            | true, _ ->
                live[key t sessionId] <- false
                cleanups.Remove(key t sessionId) |> ignore
                Task.FromResult(EventCleanupApplied(sessionId, true) :> EventCleanupSettlement)

        member _.DeferCleanup(t, sessionId, claimToken, _) =
            if isNull (box claimToken) then
                raise (ArgumentNullException(nameof claimToken))

            match cleanups.TryGetValue(key t sessionId) with
            | false, _ -> Task.FromResult(EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)
            | true, claim when claim.Token <> claimToken ->
                Task.FromResult(EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)
            | true, claim when claim.ExpiresAt <= stamp ->
                Task.FromResult(EventCleanupRejected(sessionId, "leaseExpired") :> EventCleanupSettlement)
            | true, _ ->
                cleanups.Remove(key t sessionId) |> ignore
                Task.FromResult(EventCleanupApplied(sessionId, false) :> EventCleanupSettlement)

    /// Registers a session with an empty journal and the one turn claim
    /// token the fake accepts on Append.
    member _.RegisterSession(sessionId: SessionId, claimToken: string) =
        journals[key tenant sessionId] <- Dictionary<int64, SessionEvent>()
        live[key tenant sessionId] <- true
        tokens[key tenant sessionId] <- HashSet<string>([ claimToken ])

    /// Test hook: replaces the stored token for a session with a fresh
    /// one, which is how a takeover materialises here; the previous
    /// owner's token goes stale.
    member _.Takeover(sessionId: SessionId) =
        let accepted = tokens[key tenant sessionId]
        accepted.Clear()
        accepted.Add "token-takeover" |> ignore

    /// Test hook: expires every cleanup lease by rewinding its expiry,
    /// which is how lease expiry materialises here.
    member _.ExpireLease(sessionId: SessionId) =
        match cleanups.TryGetValue(key tenant sessionId) with
        | true, claim ->
            cleanups[key tenant sessionId] <-
                { claim with
                    ExpiresAt = stamp.AddSeconds -1.0
                }
        | false, _ -> failwith "no cleanup lease to expire"

    /// Test hook: marks the journal gone (as a completed cleanup would)
    /// without removing the session row, which is how journal expiry
    /// materialises here.
    member _.ExpireJournal(sessionId: SessionId) = live[key tenant sessionId] <- false

    /// Test hook: the highest stamped sequence in a session's journal, so
    /// tests can assert zero effects after a rejected append.
    member _.MaxSequence(sessionId: SessionId) =
        let journal = journals[key tenant sessionId]

        let mutable maxSeq = 0L

        for entry in journal do
            if entry.Key > maxSeq then
                maxSeq <- entry.Key

        maxSeq

// ───────────────────────────────────────────────────────────────────────────
// Shared builders

let turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"

/// Builds an in-flight event with an empty sequence, the shape the journal
/// writer hands the store.
let inFlight (sessionId: SessionId) (text: string) : SessionEvent =
    TextDeltaEvent(sessionId, turnId, noSequence, stamp, text) :> SessionEvent

/// Builds a fake store with one registered session under the accepted
/// token "token-1"; returns the store (as the fake, for the hooks), the
/// session id, and the interface.
let fresh () =
    let fake = FakeSessionEventStore()
    let sessionId = SessionId.New()
    fake.RegisterSession(sessionId, "token-1")
    (fake, sessionId, fake :> ISessionEventStore)

// ───────────────────────────────────────────────────────────────────────────
// Shape: outcome families and $type discriminators

[<Fact>]
let ``EventAppendOutcome has exactly the two documented discriminators`` () =
    let appended: EventAppendOutcome =
        EventAppended(ResizeArray() :> IReadOnlyList<SessionEvent>) :> _

    let appendedJson = JsonSerializer.Serialize(appended, jsonOptions)
    appendedJson.Contains("\"$type\":\"eventAppended\"") |> should equal true

    let rejected: EventAppendOutcome =
        EventAppendRejected(SessionId.New(), "staleClaim") :> _

    let rejectedJson = JsonSerializer.Serialize(rejected, jsonOptions)
    rejectedJson.Contains("\"$type\":\"eventAppendRejected\"") |> should equal true

[<Fact>]
let ``EventReplayOutcome has exactly the four documented discriminators`` () =
    let page: EventReplayOutcome =
        EventReplayPage(SessionId.New(), ResizeArray() :> IReadOnlyList<SessionEvent>, Nullable 1L) :> _

    let pageJson = JsonSerializer.Serialize(page, jsonOptions)
    pageJson.Contains("\"$type\":\"eventReplayPage\"") |> should equal true

    let endOfStream: EventReplayOutcome = EventReplayEndOfStream(SessionId.New()) :> _

    let endJson = JsonSerializer.Serialize(endOfStream, jsonOptions)
    endJson.Contains("\"$type\":\"eventReplayEndOfStream\"") |> should equal true

    let unknown: EventReplayOutcome = EventReplayUnknownSession(SessionId.New()) :> _

    let unknownJson = JsonSerializer.Serialize(unknown, jsonOptions)

    unknownJson.Contains("\"$type\":\"eventReplayUnknownSession\"")
    |> should equal true

    let expired: EventReplayOutcome = EventReplayJournalExpired(SessionId.New()) :> _

    let expiredJson = JsonSerializer.Serialize(expired, jsonOptions)

    expiredJson.Contains("\"$type\":\"eventReplayJournalExpired\"")
    |> should equal true

[<Fact>]
let ``EventCleanupState has exactly the two documented discriminators`` () =
    let claimed: EventCleanupState =
        EventCleanupClaimed(
            {
                SessionId = SessionId.New()
                Token = "t"
                Owner = "o"
                ExpiresAt = stamp
            }
        )
        :> _

    let claimedJson = JsonSerializer.Serialize(claimed, jsonOptions)
    claimedJson.Contains("\"$type\":\"eventCleanupClaimed\"") |> should equal true

    let notClaimable: EventCleanupState =
        EventCleanupNotClaimable(SessionId.New(), "leaseHeld") :> _

    let notClaimableJson = JsonSerializer.Serialize(notClaimable, jsonOptions)

    notClaimableJson.Contains("\"$type\":\"eventCleanupNotClaimable\"")
    |> should equal true

[<Fact>]
let ``EventCleanupSettlement has exactly the two documented discriminators`` () =
    let applied: EventCleanupSettlement =
        EventCleanupApplied(SessionId.New(), true) :> _

    let appliedJson = JsonSerializer.Serialize(applied, jsonOptions)
    appliedJson.Contains("\"$type\":\"eventCleanupApplied\"") |> should equal true

    let rejected: EventCleanupSettlement =
        EventCleanupRejected(SessionId.New(), "staleClaim") :> _

    let rejectedJson = JsonSerializer.Serialize(rejected, jsonOptions)
    rejectedJson.Contains("\"$type\":\"eventCleanupRejected\"") |> should equal true

[<Fact>]
let ``EventAppended round-trips its stamped events through $type`` () =
    let sessionId = SessionId.New()
    let stamped = [ inFlight sessionId "one" ] :> IReadOnlyList<SessionEvent>

    let outcome: EventAppendOutcome = EventAppended(stamped) :> _

    let json = JsonSerializer.Serialize(outcome, jsonOptions)

    match deserialize<EventAppendOutcome> json with
    | :? EventAppended as restored ->
        restored.Events.Count |> should equal 1
        (restored.Events[0] :?> TextDeltaEvent).Text |> should equal "one"
    | _ -> failwith "the appended outcome lost its subtype"

[<Fact>]
let ``EventAppendRejected round-trips its reason`` () =
    let sessionId = SessionId.New()

    let outcome: EventAppendOutcome = EventAppendRejected(sessionId, "staleClaim") :> _

    let json = JsonSerializer.Serialize(outcome, jsonOptions)

    match deserialize<EventAppendOutcome> json with
    | :? EventAppendRejected as restored ->
        restored.SessionId |> should equal sessionId
        restored.Reason |> should equal "staleClaim"
    | _ -> failwith "the rejected outcome lost its subtype"

[<Fact>]
let ``EventReplayPage round-trips its events and cursor`` () =
    let sessionId = SessionId.New()
    let events = [ inFlight sessionId "delta" ] :> IReadOnlyList<SessionEvent>

    let outcome: EventReplayOutcome =
        EventReplayPage(sessionId, events, Nullable 7L) :> _

    let json = JsonSerializer.Serialize(outcome, jsonOptions)

    match deserialize<EventReplayOutcome> json with
    | :? EventReplayPage as restored ->
        restored.SessionId |> should equal sessionId
        restored.Events.Count |> should equal 1
        restored.NextCursor |> should equal (Nullable 7L)
    | _ -> failwith "the page outcome lost its subtype"

[<Fact>]
let ``EventCleanupClaim round-trips its fields`` () =
    let claim =
        {
            SessionId = SessionId.New()
            Token = "cleanup-token-9"
            Owner = "worker-1"
            ExpiresAt = stamp
        }

    let json = JsonSerializer.Serialize(claim, jsonOptions)
    let restored = deserialize<EventCleanupClaim> json

    restored.SessionId |> should equal claim.SessionId
    restored.Token |> should equal "cleanup-token-9"
    restored.Owner |> should equal "worker-1"
    restored.ExpiresAt |> should equal stamp

[<Fact>]
let ``EventCleanupClaimed round-trips its claim through $type`` () =
    let claim =
        {
            SessionId = SessionId.New()
            Token = "cleanup-token-3"
            Owner = "worker-2"
            ExpiresAt = stamp
        }

    let state: EventCleanupState = EventCleanupClaimed(claim) :> _

    let json = JsonSerializer.Serialize(state, jsonOptions)

    match deserialize<EventCleanupState> json with
    | :? EventCleanupClaimed as restored ->
        restored.Claim.Token |> should equal "cleanup-token-3"
        restored.Claim.Owner |> should equal "worker-2"
    | _ -> failwith "the claimed state lost its subtype"

// ───────────────────────────────────────────────────────────────────────────
// Contract implementability: the fake pins the documented semantics

[<Fact>]
let ``Append stamps per-session monotonic gap-free sequences across batches`` () =
    let fake, sessionId, store = fresh ()

    task {
        let! first =
            store.Append(
                tenant,
                sessionId,
                "token-1",
                ([
                    inFlight sessionId "a"
                    inFlight sessionId "b"
                ]
                :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )

        let stampedOne =
            match first with
            | :? EventAppended as appended -> appended.Events
            | _ -> failwith "expected the appended outcome"

        stampedOne.Count |> should equal 2
        stampedOne[0].Sequence.Value |> should equal 1L
        stampedOne[1].Sequence.Value |> should equal 2L
        stampedOne[0].Sequence.Value |> should equal (stampedOne[0].Sequence.Value)

        let! second =
            store.Append(
                tenant,
                sessionId,
                "token-1",
                ([ inFlight sessionId "c" ] :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )

        let stampedTwo =
            match second with
            | :? EventAppended as appended -> appended.Events
            | _ -> failwith "expected the appended outcome"

        stampedTwo[0].Sequence.Value |> should equal 3L

        // Sequences are per-session: another session restarts at 1.
        let otherSession = SessionId.New()
        fake.RegisterSession(otherSession, "token-1")

        let! third =
            store.Append(
                tenant,
                otherSession,
                "token-1",
                ([ inFlight otherSession "x" ] :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )

        let stampedThree =
            match third with
            | :? EventAppended as appended -> appended.Events
            | _ -> failwith "expected the appended outcome"

        stampedThree[0].Sequence.Value |> should equal 1L
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Append sequences stay tenant-scoped`` () =
    let _, sessionId, store = fresh ()

    task {
        let! _ =
            store.Append(
                tenant,
                sessionId,
                "token-1",
                ([ inFlight sessionId "a" ] :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )

        // The same session id in another tenant does not resolve at all:
        // the fake registers per (tenant, session) pair, and the session
        // was registered under the tenant fixture only.
        try
            let! _ =
                store.Append(
                    otherTenant,
                    sessionId,
                    "token-1",
                    ([ inFlight sessionId "b" ] :> IReadOnlyList<SessionEvent>),
                    CancellationToken.None
                )

            failwith "expected SessionNotFoundException"
        with :? SessionNotFoundException as exn ->
            exn.SessionId |> should equal sessionId
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``A stale claim token rejects the append with zero writes`` () =
    let fake, sessionId, store = fresh ()

    task {
        // A takeover replaces the accepted token; the old owner's token
        // goes stale.
        fake.Takeover(sessionId)

        let! rejected =
            store.Append(
                tenant,
                sessionId,
                "token-1",
                ([ inFlight sessionId "loser-write" ] :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )

        (rejected :? EventAppendRejected) |> should equal true
        (rejected :?> EventAppendRejected).Reason |> should equal "staleClaim"

        // The loser had no effects: the journal carries nothing.
        fake.MaxSequence(sessionId) |> should equal 0L

        // The winner still appends under the takeover token.
        let! accepted =
            store.Append(
                tenant,
                sessionId,
                "token-takeover",
                ([ inFlight sessionId "winner-write" ] :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )

        (accepted :? EventAppended) |> should equal true
        (accepted :?> EventAppended).Events[0].Sequence.Value |> should equal 1L
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Append on an unknown session throws the control-plane exception`` () =
    let _, _, store = fresh ()

    task {
        try
            let! _ =
                store.Append(
                    tenant,
                    SessionId.New(),
                    "token-1",
                    ([ inFlight (SessionId.New()) "ghost" ] :> IReadOnlyList<SessionEvent>),
                    CancellationToken.None
                )

            failwith "expected SessionNotFoundException"
        with :? SessionNotFoundException as exn ->
            exn.Message |> should equal "Session not found."
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Replay pages in sequence order and honours the limit`` () =
    let _, sessionId, store = fresh ()

    task {
        // Two batches, staying under the fake's batch-size limit.
        let firstBatch =
            [ 1..2 ] |> List.map (fun n -> inFlight sessionId $"ev-{n}") :> IReadOnlyList<SessionEvent>

        let secondBatch =
            [ 3..4 ] |> List.map (fun n -> inFlight sessionId $"ev-{n}") :> IReadOnlyList<SessionEvent>

        let! _ = store.Append(tenant, sessionId, "token-1", firstBatch, CancellationToken.None)
        let! _ = store.Append(tenant, sessionId, "token-1", secondBatch, CancellationToken.None)

        let! page = store.Replay(tenant, sessionId, 0L, 2, CancellationToken.None)

        (page :? EventReplayPage) |> should equal true

        match page with
        | :? EventReplayPage as p ->
            p.Events.Count |> should equal 2
            p.Events[0].Sequence.Value |> should equal 1L
            p.Events[1].Sequence.Value |> should equal 2L
            (p.Events[0] :?> TextDeltaEvent).Text |> should equal "ev-1"
            p.NextCursor |> should equal (Nullable 2L)
        | _ -> failwith "unreachable"

        // Continuation from the cursor returns the rest.
        let! second = store.Replay(tenant, sessionId, 2L, 2, CancellationToken.None)

        match second with
        | :? EventReplayPage as p ->
            p.Events.Count |> should equal 2
            p.Events[0].Sequence.Value |> should equal 3L
            p.NextCursor |> should equal (Nullable 4L)
        | _ -> failwith "expected a second page"

        // Past the last event: end of stream, distinct from a page.
        let! tail = store.Replay(tenant, sessionId, 4L, 2, CancellationToken.None)
        (tail :? EventReplayEndOfStream) |> should equal true
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Replay resolves the four outcomes distinctly`` () =
    let fake, sessionId, store = fresh ()

    task {
        // Unknown session (never registered, other tenant or not).
        let! unknown = store.Replay(tenant, SessionId.New(), 0L, 5, CancellationToken.None)
        (unknown :? EventReplayUnknownSession) |> should equal true

        (unknown :?> EventReplayUnknownSession).SessionId
        |> should not' (equal sessionId)

        // Expired journal: the session exists, its journal is gone.
        fake.ExpireJournal(sessionId)

        let! expired = store.Replay(tenant, sessionId, 0L, 5, CancellationToken.None)
        (expired :? EventReplayJournalExpired) |> should equal true

        // End of stream: a live session with an empty journal.
        let emptySession = SessionId.New()
        fake.RegisterSession(emptySession, "token-1")

        let! exhausted = store.Replay(tenant, emptySession, 0L, 5, CancellationToken.None)
        (exhausted :? EventReplayEndOfStream) |> should equal true

        // A normal page is a distinct fourth outcome.
        let! _ =
            store.Append(
                tenant,
                emptySession,
                "token-1",
                ([ inFlight emptySession "one" ] :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )

        let! page = store.Replay(tenant, emptySession, 0L, 5, CancellationToken.None)
        (page :? EventReplayPage) |> should equal true
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Cleanup leases one worker and refuses a second claimant`` () =
    let _, sessionId, store = fresh ()

    task {
        let! first =
            store.TryClaimCleanup(tenant, sessionId, "worker-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        (first :? EventCleanupClaimed) |> should equal true

        let claim =
            match first with
            | :? EventCleanupClaimed as held -> held.Claim
            | _ -> failwith "unreachable"

        claim.Owner |> should equal "worker-1"
        claim.Token |> should not' (equal null)

        let! second =
            store.TryClaimCleanup(tenant, sessionId, "worker-2", TimeSpan.FromMinutes 5., CancellationToken.None)

        (second :? EventCleanupNotClaimable) |> should equal true
        (second :?> EventCleanupNotClaimable).Reason |> should equal "leaseHeld"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``CompleteCleanup archives under the token and a stale token cannot settle`` () =
    let _, sessionId, store = fresh ()

    task {
        let! claimed =
            store.TryClaimCleanup(tenant, sessionId, "worker-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        let claim =
            match claimed with
            | :? EventCleanupClaimed as held -> held.Claim
            | _ -> failwith "expected a claim"

        // A different worker's token is stale: nothing is archived.
        let! rejected = store.CompleteCleanup(tenant, sessionId, "someone-elses-token", CancellationToken.None)
        (rejected :? EventCleanupRejected) |> should equal true
        (rejected :?> EventCleanupRejected).Reason |> should equal "staleClaim"

        // The journal still replays: nothing was deleted.
        let! stillThere = store.Replay(tenant, sessionId, 0L, 5, CancellationToken.None)
        (stillThere :? EventReplayEndOfStream) |> should equal true

        let! applied = store.CompleteCleanup(tenant, sessionId, claim.Token, CancellationToken.None)
        (applied :? EventCleanupApplied) |> should equal true
        (applied :?> EventCleanupApplied).Completed |> should equal true

        // The journal is gone now.
        let! gone = store.Replay(tenant, sessionId, 0L, 5, CancellationToken.None)
        (gone :? EventReplayJournalExpired) |> should equal true

        // Settling again is a stale claim: the lease is gone.
        let! again = store.CompleteCleanup(tenant, sessionId, claim.Token, CancellationToken.None)
        (again :? EventCleanupRejected) |> should equal true
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``DeferCleanup releases the journal intact and an expired lease cannot settle`` () =
    let fake, sessionId, store = fresh ()

    task {
        let! claimed =
            store.TryClaimCleanup(tenant, sessionId, "worker-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        let claim =
            match claimed with
            | :? EventCleanupClaimed as held -> held.Claim
            | _ -> failwith "expected a claim"

        // The lease expires before the worker settles: the settlement is
        // rejected and nothing was deleted.
        fake.ExpireLease(sessionId)

        let! expired = store.DeferCleanup(tenant, sessionId, claim.Token, CancellationToken.None)
        (expired :? EventCleanupRejected) |> should equal true
        (expired :?> EventCleanupRejected).Reason |> should equal "leaseExpired"

        // The journal stays intact; the expired lease frees it for a new
        // claimant, who defers and leaves the journal replayable.
        let! reclaimed =
            store.TryClaimCleanup(tenant, sessionId, "worker-2", TimeSpan.FromMinutes 5., CancellationToken.None)

        let newClaim =
            match reclaimed with
            | :? EventCleanupClaimed as held -> held.Claim
            | _ -> failwith "expected a re-claim after expiry"

        newClaim.Owner |> should equal "worker-2"

        let! deferred = store.DeferCleanup(tenant, sessionId, newClaim.Token, CancellationToken.None)
        (deferred :? EventCleanupApplied) |> should equal true
        (deferred :?> EventCleanupApplied).Completed |> should equal false

        // Deferred cleanup keeps the journal replayable.
        let! intact = store.Replay(tenant, sessionId, 0L, 5, CancellationToken.None)
        (intact :? EventReplayEndOfStream) |> should equal true
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``TryClaimCleanup resolves unknown and cleaned-up journals as not claimable`` () =
    let fake, sessionId, store = fresh ()

    task {
        let! unknown =
            store.TryClaimCleanup(tenant, SessionId.New(), "worker-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        (unknown :? EventCleanupNotClaimable) |> should equal true
        (unknown :?> EventCleanupNotClaimable).Reason |> should equal "unknownSession"

        fake.ExpireJournal(sessionId)

        let! gone =
            store.TryClaimCleanup(tenant, sessionId, "worker-1", TimeSpan.FromMinutes 5., CancellationToken.None)

        (gone :? EventCleanupNotClaimable) |> should equal true
        (gone :?> EventCleanupNotClaimable).Reason |> should equal "journalGone"
    }
    |> (fun t -> t.Wait())

// ───────────────────────────────────────────────────────────────────────────
// Limit breaches throw before any part of the batch lands

[<Fact>]
let ``An oversized event throws EventLimitExceededException with populated properties and no write`` () =
    let fake, sessionId, store = fresh ()
    let huge = String('x', 600)

    task {
        try
            let! _ =
                store.Append(
                    tenant,
                    sessionId,
                    "token-1",
                    ([ inFlight sessionId huge ] :> IReadOnlyList<SessionEvent>),
                    CancellationToken.None
                )

            failwith "expected EventLimitExceededException"
        with :? EventLimitExceededException as exn ->
            // Observed is the persisted JSON size of the event, which is
            // the text plus the envelope fields, so it is larger than the
            // 600-character text and the breach is by that measure.
            exn.LimitKind |> should equal "perEventBytes"
            exn.Limit |> should equal 500L
            exn.Observed |> should equal 766L
            (exn.Observed > 600L) |> should equal true
            exn.Message.Contains("per-event byte limit") |> should equal true

        fake.MaxSequence(sessionId) |> should equal 0L
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``An oversized batch throws with the batch-size kind and no write`` () =
    let fake, sessionId, store = fresh ()

    let events =
        [ 1..4 ] |> List.map (fun n -> inFlight sessionId $"ev-{n}") :> IReadOnlyList<SessionEvent>

    task {
        try
            let! _ = store.Append(tenant, sessionId, "token-1", events, CancellationToken.None)
            failwith "expected EventLimitExceededException"
        with :? EventLimitExceededException as exn ->
            exn.LimitKind |> should equal "batchSize"
            exn.Limit |> should equal 3L
            exn.Observed |> should equal 4L

        fake.MaxSequence(sessionId) |> should equal 0L
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Exceeding the per-session count throws and leaves no partial batch`` () =
    let fake, sessionId, store = fresh ()

    task {
        // Fill the journal to exactly the count limit.
        for n in 1..5 do
            let! _ =
                store.Append(
                    tenant,
                    sessionId,
                    "token-1",
                    ([ inFlight sessionId $"fill-{n}" ] :> IReadOnlyList<SessionEvent>),
                    CancellationToken.None
                )

            ()

        fake.MaxSequence(sessionId) |> should equal 5L

        try
            let! _ =
                store.Append(
                    tenant,
                    sessionId,
                    "token-1",
                    ([ inFlight sessionId "overflow" ] :> IReadOnlyList<SessionEvent>),
                    CancellationToken.None
                )

            failwith "expected EventLimitExceededException"
        with :? EventLimitExceededException as exn ->
            exn.LimitKind |> should equal "perSessionCount"
            exn.Limit |> should equal 5L
            exn.Observed |> should equal 6L

        fake.MaxSequence(sessionId) |> should equal 5L
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Exceeding the per-session byte limit throws and leaves no partial batch`` () =
    let fake, sessionId, store = fresh ()

    task {
        // Two big-but-legal events land; a third would breach the byte limit.
        let big =
            [
                inFlight sessionId (String('a', 300))
                inFlight sessionId (String('b', 300))
            ]
            :> IReadOnlyList<SessionEvent>

        let! _ = store.Append(tenant, sessionId, "token-1", big, CancellationToken.None)
        fake.MaxSequence(sessionId) |> should equal 2L

        try
            let! _ =
                store.Append(
                    tenant,
                    sessionId,
                    "token-1",
                    ([
                        inFlight sessionId (String('c', 300))
                    ]
                    :> IReadOnlyList<SessionEvent>),
                    CancellationToken.None
                )

            failwith "expected EventLimitExceededException"
        with :? EventLimitExceededException as exn ->
            exn.LimitKind |> should equal "perSessionBytes"

        fake.MaxSequence(sessionId) |> should equal 2L
    }
    |> (fun t -> t.Wait())

// ───────────────────────────────────────────────────────────────────────────
// Null and bound guards the contract documents

[<Fact>]
let ``Append rejects a null batch or token and Replay rejects a non-positive limit`` () =
    let _, sessionId, store = fresh ()
    let nullEvents = Unchecked.defaultof<IReadOnlyList<SessionEvent>>
    let nullToken = Unchecked.defaultof<string>

    task {
        (fun () ->
            store.Append(
                tenant,
                sessionId,
                nullToken,
                ([ inFlight sessionId "x" ] :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )
            |> ignore)
        |> should throw typeof<ArgumentNullException>

        (fun () ->
            store.Append(tenant, sessionId, "token-1", nullEvents, CancellationToken.None)
            |> ignore)
        |> should throw typeof<ArgumentNullException>

        (fun () -> store.Replay(tenant, sessionId, 0L, 0, CancellationToken.None) |> ignore)
        |> should throw typeof<ArgumentOutOfRangeException>

        (fun () -> store.Replay(tenant, sessionId, -1L, 5, CancellationToken.None) |> ignore)
        |> should throw typeof<ArgumentOutOfRangeException>

        (fun () ->
            store.TryClaimCleanup(tenant, sessionId, nullToken, TimeSpan.FromMinutes 1., CancellationToken.None)
            |> ignore)
        |> should throw typeof<ArgumentNullException>

        (fun () ->
            store.TryClaimCleanup(tenant, sessionId, "worker", TimeSpan.Zero, CancellationToken.None)
            |> ignore)
        |> should throw typeof<ArgumentOutOfRangeException>

        ()
    }
    |> (fun t -> t.Wait())

// ───────────────────────────────────────────────────────────────────────────
// Interface-shape pins

[<Fact>]
let ``Every ISessionEventStore method takes a TenantId and returns Task`` () =
    let methods = typeof<ISessionEventStore>.GetMethods()
    methods.Length |> should equal 5

    for method in methods do
        let paramOk =
            method.GetParameters()
            |> Array.exists (fun p -> p.ParameterType = typeof<TenantId>)

        paramOk |> should equal true
        method.ReturnType.IsSubclassOf(typeof<Task>) |> should equal true
