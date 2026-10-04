// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.RealTurnOwnershipTests

open System
open System.Collections.Generic
open System.Threading
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Xunit

// Issue 374: production ownership binds to the real durable turn alongside
// the intact prime. Every accepted user message carries a stable durable
// TurnId tied to its inbox entry; Session.CurrentTurnId plus the turn row own
// execution, suspension, resume, restart, takeover, and settlement through
// the existing claim-token fence with the #363 committed outcome. Execution
// events journal the real TurnId; the #373 AppendHostEvents host path with
// the default-TurnId sentinel stays unchanged. Prime wiring is untouched as
// the handoff for #400.

let private tenant = TenantId.Create "acme"

let private sampleSession (state: SessionState) =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "real-turn"
        State = state
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private createStores () =
    let database = InMemoryDatabase()
    let store = InMemorySessionStore(database) :> ISessionStore
    database, store

let private createSession (store: ISessionStore) =
    store.CreateSession(tenant, sampleSession SessionState.Idle, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()

let private appendText (store: ISessionStore) (sessionId: SessionId) (text: string) (delivery: DeliveryMode) =
    let payload = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload

    store.AppendInboxMessage(tenant, sessionId, payload, delivery, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()

let private appendPermissionReply (store: ISessionStore) (sessionId: SessionId) =
    let reply =
        PermissionDecision("request-1", PermissionDecisionKind.AllowOnce) :> Reply

    let payload = ReplyPayload(reply) :> InboxPayload

    store.AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()

let private claim (store: ISessionStore) (sessionId: SessionId) (owner: string) =
    store.ClaimNextTurn(tenant, sessionId, owner, TimeSpan.FromMinutes 5.0, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()

let private requireSession (store: ISessionStore) (sessionId: SessionId) : Session =
    match store.GetSession(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult() with
    | null -> failwith "Expected the session row to exist."
    | session -> session

let private isDefault (turnId: TurnId) = isNull (box turnId.Value)

[<Fact>]
let ``Accepted user messages carry distinct stable real-turn identities`` () =
    let _, store = createStores ()
    let session = createSession store
    let first = appendText store session.Id "first" DeliveryMode.Queue
    let second = appendText store session.Id "second" DeliveryMode.Queue

    isDefault first.TurnId |> should equal false
    isDefault second.TurnId |> should equal false
    first.TurnId.Equals(second.TurnId) |> should equal false
    (first.Position = second.Position) |> should equal false

[<Fact>]
let ``Reply entries carry the default sentinel and never start a turn`` () =
    let _, store = createStores ()
    let session = createSession store
    let entry = appendPermissionReply store session.Id
    isDefault entry.TurnId |> should equal true

    match claim store session.Id "owner-1" with
    | :? TurnLeaseMissing -> ()
    | other -> failwithf "Expected missing (reply with no open turn), got %O." other

[<Fact>]
let ``Claim binds the accepted entry durable identity, not a fresh synthetic one`` () =
    let _, store = createStores ()
    let session = createSession store
    let entry = appendText store session.Id "hello" DeliveryMode.Queue

    match claim store session.Id "owner-1" with
    | :? TurnLeaseRenewed as renewed ->
        renewed.Claim.TurnId.Equals(entry.TurnId) |> should equal true
        let stored = requireSession store session.Id
        stored.CurrentTurnId.HasValue |> should equal true
        stored.CurrentTurnId.Value.Equals(entry.TurnId) |> should equal true
    | other -> failwithf "Expected a granted claim, got %O." other

[<Fact>]
let ``Distinct queued entries yield distinct durable claims with no shared bootstrap`` () =
    let _, store = createStores ()
    let session = createSession store
    let first = appendText store session.Id "first" DeliveryMode.Queue
    let second = appendText store session.Id "second" DeliveryMode.Queue

    let firstClaim =
        match claim store session.Id "owner-1" with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | other -> failwithf "Expected first claim, got %O." other

    firstClaim.TurnId.Equals(first.TurnId) |> should equal true

    // A live claim blocks the second entry: single-winner, no double claim.
    match claim store session.Id "owner-2" with
    | :? TurnLeaseMissing -> ()
    | other -> failwithf "Expected missing while live claim held, got %O." other

    // Settle the first turn, then the second entry claims under its own identity.
    match
        store.SettleTurn(tenant, firstClaim, TurnStatus.Completed, null, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnSettled -> ()
    | other -> failwithf "Expected settled, got %O." other

    match claim store session.Id "owner-2" with
    | :? TurnLeaseRenewed as renewed ->
        renewed.Claim.TurnId.Equals(second.TurnId) |> should equal true
        (renewed.Claim.TurnId.Equals(first.TurnId)) |> should equal false
    | other -> failwithf "Expected second claim, got %O." other

[<Fact>]
let ``Concurrent claims on one session have exactly one winner`` () =
    let _, store = createStores ()
    let session = createSession store
    appendText store session.Id "race" DeliveryMode.Queue |> ignore

    let first = claim store session.Id "owner-a"
    let second = claim store session.Id "owner-b"

    let granted =
        [ first; second ]
        |> List.filter (fun state ->
            match state with
            | :? TurnLeaseRenewed -> true
            | _ -> false)
        |> List.length

    granted |> should equal 1

    match first, second with
    | (:? TurnLeaseRenewed as winner), (:? TurnLeaseMissing)
    | (:? TurnLeaseMissing), (:? TurnLeaseRenewed as winner) ->
        (winner.Claim.Owner = "owner-a" || winner.Claim.Owner = "owner-b")
        |> should equal true
    | _ -> failwith "Expected exactly one winner and one missing."

[<Fact>]
let ``Stale completion cannot settle or clear the next turn`` () =
    let _, store = createStores ()
    let session = createSession store
    appendText store session.Id "first" DeliveryMode.Queue |> ignore
    appendText store session.Id "second" DeliveryMode.Queue |> ignore

    let firstClaim =
        match claim store session.Id "owner-1" with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | other -> failwithf "Expected first claim, got %O." other

    // Abort the first turn (takeover-style release), then claim the second.
    match
        store.AbortTurn(tenant, firstClaim, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnLeaseHeld -> ()
    | other -> failwithf "Expected held after abort, got %O." other

    let secondClaim =
        match claim store session.Id "owner-2" with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | other -> failwithf "Expected second claim, got %O." other

    (secondClaim.TurnId.Equals(firstClaim.TurnId)) |> should equal false

    // The stale first claim cannot settle the next turn.
    match
        store.SettleTurn(tenant, firstClaim, TurnStatus.Completed, null, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnSettleRejected -> ()
    | other -> failwithf "Expected stale-claim rejection, got %O." other

    let stored = requireSession store session.Id
    stored.CurrentTurnId.HasValue |> should equal true
    stored.CurrentTurnId.Value.Equals(secondClaim.TurnId) |> should equal true

[<Fact>]
let ``Resolving a session alone claims and journals nothing`` () =
    let database, store = createStores ()
    let session = createSession store
    appendText store session.Id "pending" DeliveryMode.Queue |> ignore
    let journal = InMemorySessionEventStore(database) :> ISessionEventStore

    // A resolve-style read observes the session and inbox without consuming,
    // claiming, or journaling.
    let stored = requireSession store session.Id
    stored.CurrentTurnId.HasValue |> should equal false

    let pending =
        store.ReadPendingInbox(tenant, session.Id, CancellationToken.None).GetAwaiter().GetResult()

    pending.Count |> should equal 1

    match journal.Replay(tenant, session.Id, 0L, 100, CancellationToken.None).GetAwaiter().GetResult() with
    | :? EventReplayEndOfStream -> ()
    | other -> failwithf "Expected empty journal, got %O." other

[<Fact>]
let ``Unsupported old formats reject fail-closed with a clean-start diagnostic`` () =
    let legacy = SessionOptions()
    legacy.FormatVersion <- 0

    try
        legacy.ValidatePersistence()
        failwith "Expected CompletionRoutingException."
    with :? CompletionRoutingException as refusal ->
        refusal.Reason |> should equal CompletionRoutingReason.UnsupportedFormat
        refusal.Message.Contains("clean-start") |> should equal true
