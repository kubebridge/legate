// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open System
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.Postgres
open Legate.Testing
open Xunit

/// The PostgreSQL session store derives the shared conformance suite over
/// the Testcontainers database, with the suite's deterministic TestClock
/// driving both the store's lease stamps and the suite's time advances.
/// Truncates after every fact: the suite reuses tenants and keys across
/// facts, assuming per-test isolation.
type PostgresSessionStoreTests private (store: ISessionStore, clock: TestClock) =
    inherit SessionStoreConformance(store, clock)

    new() =
        let clock, store, _, _ = PostgresTestDatabase.createStores ()
        new PostgresSessionStoreTests(store, clock)

    [<Fact>]
    member this.``Postgres fresh provider retains accepted intent and pending control verdict``() =
        task {
            let! control, session, entry, turn, claim = this.ControlWork()

            let! original =
                control.RequestHostAbort(
                    this.Tenant,
                    session,
                    turn,
                    StopCause.HostShutdown,
                    "first",
                    CancellationToken.None
                )

            let fresh =
                PostgresSessionStore(PostgresTestDatabase.testOptions (PostgresTestDatabase.ensureReady ()), clock)
                :> ISessionStore

            let remoteControl = fresh :?> ISessionAbortControlStore

            let! decided =
                remoteControl.TryDecideControlTarget(
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

            match decided.Decision with
            | null -> failwith "No control evidence"
            | selected -> Assert.Equal(TurnStatus.Aborted, selected.Status)

            let! duplicate =
                remoteControl.RequestHostAbort(
                    this.Tenant,
                    session,
                    turn,
                    StopCause.ExplicitAbort,
                    "changed",
                    CancellationToken.None
                )

            Assert.Equal(HostAbortOutcome.AlreadyAccepted, duplicate.Outcome)
            Assert.Equal(original.AcceptedAt, duplicate.AcceptedAt)
            let! current = remoteControl.ReadAbortTarget(this.Tenant, session, CancellationToken.None)

            match current with
            | null -> failwith "Binding lost"
            | target -> Assert.Equal(ControlTargetState.TerminalPendingRetirement, target.State)

            let! pending = fresh.ReadPendingInbox(this.Tenant, session, CancellationToken.None)
            Assert.Single(pending) |> ignore
            let! stillOwned = fresh.VerifyClaim(this.Tenant, claim, CancellationToken.None)
            Assert.IsType<TurnLeaseHeld>(stillOwned) |> ignore
        }

    [<Fact>]
    member this.``Postgres acceptance versus decision serializes across provider connections``() =
        task {
            let! control, session, entry, turn, claim = this.ControlWork()

            let fresh =
                PostgresSessionStore(PostgresTestDatabase.testOptions (PostgresTestDatabase.ensureReady ()), clock)
                :> ISessionAbortControlStore

            use start = new ManualResetEventSlim(false)

            let abort =
                Task.Run(fun () ->
                    start.Wait()

                    control
                        .RequestHostAbort(
                            this.Tenant,
                            session,
                            turn,
                            StopCause.HostShutdown,
                            "race",
                            CancellationToken.None
                        )
                        .GetAwaiter()
                        .GetResult())

            let decision =
                Task.Run(fun () ->
                    start.Wait()

                    fresh
                        .TryDecideControlTarget(
                            this.Tenant,
                            session,
                            turn,
                            entry.Position,
                            claim,
                            "race-report",
                            TurnStatus.Completed,
                            Nullable(),
                            null,
                            CancellationToken.None
                        )
                        .GetAwaiter()
                        .GetResult())

            start.Set()
            let! receipt = abort
            let! selected = decision

            match selected.Decision with
            | null -> failwith "Decision missing"
            | evidence ->
                Assert.Equal(ControlOperationOutcome.Applied, selected.Outcome)

                if receipt.Outcome = HostAbortOutcome.Accepted then
                    Assert.Equal(TurnStatus.Aborted, evidence.Status)
                else
                    Assert.Equal(HostAbortOutcome.AlreadyTerminal, receipt.Outcome)
                    Assert.Equal(TurnStatus.Completed, evidence.Status)

            let! pending = this.Store.ReadPendingInbox(this.Tenant, session, CancellationToken.None)
            Assert.Single(pending) |> ignore
        }

    interface IDisposable with
        member _.Dispose() = PostgresTestDatabase.truncate ()
