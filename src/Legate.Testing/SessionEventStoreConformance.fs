// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate
open Xunit

/// The shared conformance suite for
/// <see cref="T:Legate.ISessionEventStore" /> implementations: sequences,
/// batch atomicity, the claim fence (a stale token writes nothing), replay
/// outcomes, and the cleanup lease. The suite needs both the event store
/// and the session store over the same backing state, because appends
/// fence on the session store's live claim token.
[<AbstractClass>]
type SessionEventStoreConformance
    (eventStore: ISessionEventStore, sessionStore: ISessionStore, clock: TestClock, tenant: TenantId) =

    do
        if isNull (box eventStore) then
            raise (ArgumentNullException(nameof eventStore))

        if isNull (box sessionStore) then
            raise (ArgumentNullException(nameof sessionStore))

    /// The event store under test.
    member this.EventStore = eventStore

    /// The session store over the same backing state, the fence source.
    member this.SessionStore = sessionStore

    /// The clock the implementation reads.
    member this.Clock = clock

    /// The primary tenant every row belongs to.
    member this.Tenant = tenant

    /// The second tenant proving isolation.
    member this.OtherTenant = TenantId.Create "other"

    /// Constructs the suite over the stores and clock, in the primary
    /// tenant.
    new(eventStore, sessionStore, clock) =
        SessionEventStoreConformance(eventStore, sessionStore, clock, TenantId.Create "conformance")

    /// Creates a session and claims its turn, returning the session id and
    /// the live claim token, the state every append fences on.
    member this.ClaimedSession() =
        task {
            let session =
                {
                    Id = SessionId.New()
                    Tenant = tenant
                    AgentId = AgentId.New()
                    Title = "events"
                    State = SessionState.Idle
                    CurrentTurnId = Nullable()
                    CreatedAt = DateTimeOffset.MinValue
                    UpdatedAt = DateTimeOffset.MinValue
                    ClosedAt = Nullable()
                    WorkspaceBinding = null
                    Options = SessionOptions()
                }

            let! created = sessionStore.CreateSession(tenant, session, CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("journal")) :> InboxPayload

            let! _ =
                sessionStore.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! claimed =
                sessionStore.ClaimNextTurn(
                    tenant,
                    created.Id,
                    "event-owner",
                    TimeSpan.FromMinutes 5.,
                    CancellationToken.None
                )

            let claim = (claimed :?> TurnLeaseRenewed).Claim
            return (created.Id, claim)
        }

    /// One delta event the suite appends.
    member this.Delta(sessionId: SessionId, turnId: TurnId) =
        TextDeltaEvent(sessionId, turnId, Nullable(), DateTimeOffset.MinValue, "delta") :> SessionEvent

    [<Fact>]
    member this.``Append assigns gap-free per-session sequences``() =
        task {
            let! sessionId, claim = this.ClaimedSession()

            let first = this.Delta(sessionId, claim.TurnId)

            let second = this.Delta(sessionId, claim.TurnId)

            let! outcome =
                eventStore.Append(
                    tenant,
                    sessionId,
                    claim.Token,
                    ([ first; second ] :> IReadOnlyList<_>),
                    CancellationToken.None
                )

            match outcome with
            | :? EventAppended as appended ->
                let sequences =
                    appended.Events |> Seq.map (fun event -> event.Sequence.Value) |> Seq.toList

                Assert.Equal<int64 list>([ 1L; 2L ], sequences)
            | _ -> failwith "expected the appended outcome"
        }

    [<Fact>]
    member this.``A stale claim token writes nothing``() =
        task {
            let! sessionId, claim = this.ClaimedSession()

            let! rejected =
                eventStore.Append(
                    tenant,
                    sessionId,
                    "stale-token",
                    [ this.Delta(sessionId, claim.TurnId) ],
                    CancellationToken.None
                )

            Assert.True(rejected :? EventAppendRejected)

            let! replay = eventStore.Replay(tenant, sessionId, 0L, 10, CancellationToken.None)

            match replay with
            | :? EventReplayEndOfStream -> ()
            | _ -> failwith "expected end of stream after the rejected append"
        }

    [<Fact>]
    member this.``Replay pages by cursor and ends of stream``() =
        task {
            let! sessionId, claim = this.ClaimedSession()

            let batch =
                ([
                    for _index in 1..3 -> this.Delta(sessionId, claim.TurnId)
                ]
                :> IReadOnlyList<_>)

            let! appended = eventStore.Append(tenant, sessionId, claim.Token, batch, CancellationToken.None)

            let! _stamped = Task.FromResult((appended :?> EventAppended).Events)

            let! first = eventStore.Replay(tenant, sessionId, 0L, 2, CancellationToken.None)

            match first with
            | :? EventReplayPage as page ->
                Assert.Equal(2, page.Events.Count)
                Assert.Equal(2L, page.NextCursor.Value)
            | _ -> failwith "expected the first page"

            let cursor = (first :?> EventReplayPage).NextCursor.Value
            let! second = eventStore.Replay(tenant, sessionId, cursor, 2, CancellationToken.None)

            match second with
            | :? EventReplayPage as page -> Assert.Equal(1, page.Events.Count)
            | :? EventReplayEndOfStream -> ()
            | _ -> failwith "expected the tail"

            let! unknown = eventStore.Replay(this.OtherTenant, sessionId, 0L, 2, CancellationToken.None)

            let unknownOutcome: EventReplayOutcome = unknown
            Assert.True(unknownOutcome :? EventReplayUnknownSession)
        }

    [<Fact>]
    member this.``Cleanup lease is single-winner and fenced``() =
        task {
            let! sessionId, claim = this.ClaimedSession()

            let! appended =
                eventStore.Append(
                    tenant,
                    sessionId,
                    claim.Token,
                    [ this.Delta(sessionId, claim.TurnId) ],
                    CancellationToken.None
                )

            Assert.True(appended :? EventAppended)

            let! granted =
                eventStore.TryClaimCleanup(
                    tenant,
                    sessionId,
                    "cleanup-worker",
                    TimeSpan.FromMinutes 5.,
                    CancellationToken.None
                )

            let lease = (granted :?> EventCleanupClaimed).Claim

            let! second =
                eventStore.TryClaimCleanup(
                    tenant,
                    sessionId,
                    "cleanup-worker-2",
                    TimeSpan.FromMinutes 5.,
                    CancellationToken.None
                )

            Assert.True(second :? EventCleanupNotClaimable)

            let! deferred = eventStore.DeferCleanup(tenant, sessionId, lease.Token, CancellationToken.None)

            Assert.True(
                (deferred :? EventCleanupApplied)
                && not (deferred :?> EventCleanupApplied).Completed
            )

            let! replanted =
                eventStore.TryClaimCleanup(
                    tenant,
                    sessionId,
                    "cleanup-worker",
                    TimeSpan.FromMinutes 5.,
                    CancellationToken.None
                )

            let lease = (replanted :?> EventCleanupClaimed).Claim

            let! completed = eventStore.CompleteCleanup(tenant, sessionId, lease.Token, CancellationToken.None)

            Assert.True(
                (completed :? EventCleanupApplied)
                && (completed :?> EventCleanupApplied).Completed
            )

            let! expired = eventStore.Replay(tenant, sessionId, 0L, 10, CancellationToken.None)

            Assert.True(expired :? EventReplayJournalExpired)
        }

    [<Fact>]
    member this.``UserMessageEvent stamps and replays``() =
        task {
            let! sessionId, claim = this.ClaimedSession()

            let message = UserMessage.Text("injected")

            let injected =
                UserMessageEvent(sessionId, claim.TurnId, Nullable(), DateTimeOffset.MinValue, message) :> SessionEvent

            let! outcome = eventStore.Append(tenant, sessionId, claim.Token, [ injected ], CancellationToken.None)

            match outcome with
            | :? EventAppended as appended ->
                Assert.Equal(1, appended.Events.Count)
                Assert.Equal(1L, appended.Events[0].Sequence.Value)

                let! replayed = eventStore.Replay(tenant, sessionId, 0L, 10, CancellationToken.None)

                match replayed with
                | :? EventReplayPage as page ->
                    Assert.Equal(1, page.Events.Count)

                    match page.Events[0] with
                    | :? UserMessageEvent as roundTripped ->
                        Assert.Equal(1L, roundTripped.Sequence.Value)
                        Assert.Equal(sessionId, roundTripped.SessionId)
                    | _ -> failwith "expected the user message event"
                | _ -> failwith "expected the replay page"
            | _ -> failwith "expected the appended outcome"
        }

    [<Fact>]
    member this.``ContextPrunedEvent stamps and replays``() =
        task {
            let! sessionId, claim = this.ClaimedSession()

            let pruned =
                ContextPrunedEvent(sessionId, claim.TurnId, Nullable(), DateTimeOffset.MinValue, 2, 100L, 60L)
                :> SessionEvent

            let! outcome = eventStore.Append(tenant, sessionId, claim.Token, [ pruned ], CancellationToken.None)

            match outcome with
            | :? EventAppended as appended ->
                Assert.Equal(1, appended.Events.Count)
                Assert.Equal(1L, appended.Events[0].Sequence.Value)

                let! replayed = eventStore.Replay(tenant, sessionId, 0L, 10, CancellationToken.None)

                match replayed with
                | :? EventReplayPage as page ->
                    Assert.Equal(1, page.Events.Count)

                    match page.Events[0] with
                    | :? ContextPrunedEvent as roundTripped ->
                        Assert.Equal(1L, roundTripped.Sequence.Value)
                        Assert.Equal(2, roundTripped.PrunedCount)
                        Assert.Equal(100L, roundTripped.BeforeEstimate)
                        Assert.Equal(60L, roundTripped.AfterEstimate)
                    | _ -> failwith "expected the context pruned event"
                | _ -> failwith "expected the replay page"
            | _ -> failwith "expected the appended outcome"
        }
