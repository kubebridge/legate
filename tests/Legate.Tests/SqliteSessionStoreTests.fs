// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.Sqlite
open Legate.Testing
open Xunit

/// The SQLite session store derives the shared conformance suite over a
/// temp-file database, against the deterministic TestClock: one clock drives
/// both the store's lease stamps and the suite's time advances.
type SqliteSessionStoreTests private (store, clock, database: SqliteDatabase, path: string) =
    inherit SessionStoreConformance(store, clock)

    new() =
        let clock = TestClock()
        let database, path = SqliteTestFixture.openTestDatabase clock

        new SqliteSessionStoreTests(SqliteStoreFactory.sessionStore database, clock, database, path)

    [<Theory>]
    [<InlineData(false, false)>]
    [<InlineData(false, true)>]
    [<InlineData(true, false)>]
    member this.``fresh SQLite host blocks accepted and decided work before activation``
        (decided: bool, consumed: bool)
        =
        task {
            let! control, session, entry, turn, claim = this.ControlWork()

            for delivery in
                [
                    DeliveryMode.Queue
                    DeliveryMode.Inject
                    DeliveryMode.Interrupt
                ] do
                let! _ =
                    this.Store.AppendInboxMessage(
                        this.Tenant,
                        session,
                        UserMessagePayload(UserMessage.Text "unrelated"),
                        delivery,
                        CancellationToken.None
                    )

                ()

            if consumed then
                let! _ =
                    this.Store.MarkInboxConsumed(this.Tenant, session, [| entry.Position |], CancellationToken.None)

                ()

            let! before = this.Store.ReadPendingInbox(this.Tenant, session, CancellationToken.None)

            let! first =
                control.RequestHostAbort(
                    this.Tenant,
                    session,
                    turn,
                    StopCause.ExplicitAbort,
                    "original",
                    CancellationToken.None
                )

            if decided then
                let! _ =
                    control.TryDecideControlTarget(
                        this.Tenant,
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

            (database :> IDisposable).Dispose()
            use reopened = SqliteDatabase.Open(path, clock)
            let store = SqliteStoreFactory.sessionStore reopened
            let freshControl = store :?> ISessionAbortControlStore

            let! retry =
                freshControl.RequestHostAbort(
                    this.Tenant,
                    session,
                    turn,
                    StopCause.HostShutdown,
                    "later",
                    CancellationToken.None
                )

            Assert.Equal(HostAbortOutcome.AlreadyAccepted, retry.Outcome)
            Assert.Equal(first.AcceptedAt, retry.AcceptedAt)
            let! target = freshControl.ReadAbortTarget(this.Tenant, session, CancellationToken.None)

            match target with
            | null -> failwith "Durable target disappeared"
            | target ->
                Assert.Equal(
                    (if decided then
                         ControlTargetState.TerminalPendingRetirement
                     else
                         ControlTargetState.Active),
                    target.State
                )

            let mutable executions = 0
            let mutable routes = 0

            let runner: SessionActor.SuspendableRunner =
                fun _ _ _ _ _ _ _ _ _ _ _ ->
                    executions <- executions + 1
                    failwith "Runner admission is forbidden"

            let delay =
                { new ILlmDelay with
                    member _.Delay(_, _) = Task.CompletedTask
                }

            let factory =
                SessionActor.spawnSuspendFactory
                    store
                    this.Tenant
                    (SqliteStoreFactory.eventStore reopened)
                    delay
                    (TimeSpan.FromMinutes 1.0)
                    "fresh host"
                    (TimeSpan.FromMinutes 5.0)
                    runner
                    (fun _ _ ->
                        routes <- routes + 1
                        failwith "Unavailable route")
                    null
                    (fun _ _ _ -> Task.FromResult true)
                    None

            let refusal =
                Assert.Throws<InvalidSessionStateException>(fun () ->
                    factory (session.ToString()) Unchecked.defaultof<Akka.Actor.IActorContext> "blocked"
                    |> ignore)

            Assert.Equal("controlPending", refusal.CurrentState)
            Assert.Equal(0, routes)
            Assert.Equal(0, executions)
            let! after = store.ReadPendingInbox(this.Tenant, session, CancellationToken.None)
            Assert.Equal<int64>(before |> Seq.map _.Position, after |> Seq.map _.Position)
            let! authority = store.VerifyClaim(this.Tenant, claim, CancellationToken.None)
            Assert.IsType<TurnLeaseHeld>(authority) |> ignore
        }

    [<Fact>]
    member this.``SQLite host acceptance never deserializes live sink options``() =
        task {
            let! control, session, _, turn, _ = this.ControlWork()
            use connection = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={path}")
            connection.Open()
            use cmd = connection.CreateCommand()
            cmd.CommandText <- "UPDATE sessions SET options_json='not a live options graph' WHERE tenant=$t AND id=$sid"
            cmd.Parameters.AddWithValue("$t", this.Tenant.ToString()) |> ignore
            cmd.Parameters.AddWithValue("$sid", session.ToString()) |> ignore
            cmd.ExecuteNonQuery() |> ignore

            let! accepted =
                control.RequestHostAbort(
                    this.Tenant,
                    session,
                    turn,
                    StopCause.ExplicitAbort,
                    "route independent",
                    CancellationToken.None
                )

            Assert.Equal(HostAbortOutcome.Accepted, accepted.Outcome)
        }

    interface IDisposable with
        member _.Dispose() =
            (database :> IDisposable).Dispose()
            SqliteTestFixture.deleteDatabaseFiles path
