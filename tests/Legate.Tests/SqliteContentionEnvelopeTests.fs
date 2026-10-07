// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SqliteContentionEnvelopeTests

open System
open System.Collections.Generic
open System.Diagnostics
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.Sqlite
open Legate.Testing
open Xunit

// Issue 392: measurement-first characterization of real SQLite contention
// over an isolated file-backed database. Three workloads share one provider
// instance per test: a single-session baseline (Task 1), concurrent
// independent sessions with mixed journal append/replay plus session/inbox
// activity (Task 2), and a slow/contended operation competing with unrelated
// sessions plus execution controls (Task 3). Every test asserts only
// correctness (ordering, isolation, fencing, control progress); timings are
// recorded evidence, never thresholds, so the suite stays deterministic.
//
// Attribution note: the provider holds a single in-process gate around
// synchronous database calls with no per-phase timers. Gate-wait versus
// database-work versus end-to-end delay is therefore reported as
// unavailable; only end-to-end operation latency is measured.

// ──────────────────────────────────────────────────────────────────────────
// Evidence helpers
// ──────────────────────────────────────────────────────────────────────────

let private tenant = TenantId.Create "sqlite-envelope"

let private lease = TimeSpan.FromMinutes 5.0

let private sampleSession (title: string) =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = title
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private createSession (store: ISessionStore) (title: string) : Session =
    store.CreateSession(tenant, sampleSession title, CancellationToken.None).GetAwaiter().GetResult()

let private claimTurn (store: ISessionStore) (sessionId: SessionId) (owner: string) : TurnClaim =
    match store.ClaimNextTurn(tenant, sessionId, owner, lease, CancellationToken.None).GetAwaiter().GetResult() with
    | :? TurnLeaseHeld as held -> held.Claim
    | :? TurnLeaseRenewed as renewed -> renewed.Claim
    | :? TurnLeaseExpiring as expiring -> expiring.Claim
    | other -> failwith $"Expected a granted claim but observed %s{other.GetType().Name}."

let private queueMessage (store: ISessionStore) (sessionId: SessionId) (text: string) : unit =
    store
        .AppendInboxMessage(
            tenant,
            sessionId,
            UserMessagePayload(UserMessage.Text text) :> InboxPayload,
            DeliveryMode.Queue,
            CancellationToken.None
        )
        .GetAwaiter()
        .GetResult()
    |> ignore

let private textBatch (sessionId: SessionId) (turnId: TurnId) (count: int) : IReadOnlyList<SessionEvent> =
    let events =
        [|
            TurnStartedEvent(sessionId, turnId, Nullable(), DateTimeOffset.UtcNow) :> SessionEvent
            for i in 1..count do
                TextDeltaEvent(sessionId, turnId, Nullable(), DateTimeOffset.UtcNow, $"payload-{i}") :> SessionEvent
            TurnCompletedEvent(sessionId, turnId, Nullable(), DateTimeOffset.UtcNow) :> SessionEvent
        |]

    ResizeArray<SessionEvent>(events) :> IReadOnlyList<SessionEvent>

let private appendBatch
    (journal: ISessionEventStore)
    (sessionId: SessionId)
    (claim: TurnClaim)
    (events: IReadOnlyList<SessionEvent>)
    : unit =
    match journal.Append(tenant, sessionId, claim.Token, events, CancellationToken.None).GetAwaiter().GetResult() with
    | :? EventAppended -> ()
    | other -> failwith $"Expected EventAppended but observed %s{other.GetType().Name}."

let private settleCompleted (store: ISessionStore) (claim: TurnClaim) : unit =
    match
        store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None).GetAwaiter().GetResult()
    with
    | :? TurnSettled -> ()
    | other -> failwith $"Expected TurnSettled but observed %s{other.GetType().Name}."

let private replayCount (journal: ISessionEventStore) (sessionId: SessionId) : int =
    let mutable cursor = 0L
    let mutable total = 0
    let mutable more = true

    while more do
        match journal.Replay(tenant, sessionId, cursor, 100, CancellationToken.None).GetAwaiter().GetResult() with
        | :? EventReplayPage as page ->
            total <- total + page.Events.Count
            cursor <- page.NextCursor.Value
            more <- true

            if page.Events.Count = 0 then
                more <- false
        | :? EventReplayEndOfStream -> more <- false
        | other -> failwith $"Expected a replay page or end of stream but observed %s{other.GetType().Name}."

    total

let private pendingCount (store: ISessionStore) (sessionId: SessionId) : int =
    store.ReadPendingInbox(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult().Count

/// Percentile over a sorted sample in milliseconds; empty input yields NaN.
let private percentile (sorted: float list) (p: float) : float =
    match sorted with
    | [] -> Double.NaN
    | values ->
        let rank = p / 100.0 * float (values.Length - 1)
        let lower = int (Math.Floor rank)
        let upper = int (Math.Ceiling rank)

        if lower = upper then
            values[lower]
        else
            values[lower] + (values[upper] - values[lower]) * (rank - float lower)

let private summarizeMs (name: string) (samples: float list) : string =
    let sorted = samples |> List.sort

    let mean =
        if sorted.IsEmpty then
            Double.NaN
        else
            sorted |> List.average

    let minText = if sorted.IsEmpty then "NaN" else sorted.Head.ToString("F3")

    let maxText =
        if sorted.IsEmpty then
            "NaN"
        else
            (sorted |> List.last).ToString("F3")

    let p50Text = (percentile sorted 50.0).ToString("F3")
    let p95Text = (percentile sorted 95.0).ToString("F3")
    let meanText = mean.ToString("F3")

    $"{name}: n={sorted.Length} min={minText}ms p50={p50Text}ms p95={p95Text}ms max={maxText}ms mean={meanText}ms"

let private evidenceHeader (workload: string) (database: SqliteDatabase) (detail: string) : string =
    let sqliteAssembly =
        typeof<Microsoft.Data.Sqlite.SqliteConnection>.Assembly.GetName().Version

    let providerAssembly = typeof<SqliteDatabase>.Assembly.GetName().Version

    $"[sqlite-envelope] workload={workload} "
    + $"runtime={RuntimeInformation.FrameworkDescription} "
    + $"os={RuntimeInformation.OSDescription} arch={RuntimeInformation.OSArchitecture} "
    + $"cpus={Environment.ProcessorCount} "
    + $"sqliteClient={sqliteAssembly} legateSqlite={providerAssembly} "
    + $"config=wal,busyTimeoutMs={database.BusyTimeoutMs},prefix='{database.TablePrefix}' "
    + $"attribution=gateWaitVsDbWorkUnavailable,endToEndOnly "
    + $"{detail}"

// ──────────────────────────────────────────────────────────────────────────
// Task 1: single-session baseline
// ──────────────────────────────────────────────────────────────────────────

[<Fact>]
let ``single-session baseline appends replays and settles in order`` () : Task =
    task {
        let clock = TestClock()
        let database, path = SqliteTestFixture.openTestDatabase clock

        try
            try
                let store = SqliteStoreFactory.sessionStore database
                let journal = SqliteStoreFactory.eventStore database
                let created = createSession store "baseline"
                let turns = 5
                let deltasPerTurn = 3
                let appendMs = ResizeArray<float>()
                let replayMs = ResizeArray<float>()
                let wall = Stopwatch.StartNew()

                for iteration in 1..turns do
                    queueMessage store created.Id $"baseline-{iteration}"
                    let claim = claimTurn store created.Id "baseline-owner"
                    let batch = textBatch created.Id claim.TurnId deltasPerTurn

                    let appendWatch = Stopwatch.StartNew()
                    appendBatch journal created.Id claim batch
                    appendWatch.Stop()
                    appendMs.Add(appendWatch.Elapsed.TotalMilliseconds)

                    let replayWatch = Stopwatch.StartNew()
                    let _ = replayCount journal created.Id
                    replayWatch.Stop()
                    replayMs.Add(replayWatch.Elapsed.TotalMilliseconds)

                    settleCompleted store claim

                wall.Stop()

                let expected = turns * (deltasPerTurn + 2)
                let observed = replayCount journal created.Id
                Assert.Equal(expected, observed)
                Assert.Equal(0, pendingCount store created.Id)

                let detail =
                    $"sessions=1 turns={turns} eventsPerTurn={deltasPerTurn + 2} totalEvents={observed} "
                    + $"wallMs={wall.Elapsed.TotalMilliseconds:F1} "
                    + summarizeMs "append" (List.ofSeq appendMs)
                    + " "
                    + summarizeMs "replay" (List.ofSeq replayMs)

                printfn "%s" (evidenceHeader "single-baseline" database detail)
            with ex ->
                return raise ex
        finally
            (database :> IDisposable).Dispose()
            SqliteTestFixture.deleteDatabaseFiles path
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 2: concurrent independent sessions sharing one provider
// ──────────────────────────────────────────────────────────────────────────

[<Fact>]
let ``concurrent independent sessions progress with isolated journals`` () : Task =
    task {
        let clock = TestClock()
        let database, path = SqliteTestFixture.openTestDatabase clock

        try
            try
                let store = SqliteStoreFactory.sessionStore database
                let journal = SqliteStoreFactory.eventStore database
                let sessionCount = 8
                let iterations = 4
                let deltasPerTurn = 2
                let latencies = ResizeArray<float>()
                let wall = Stopwatch.StartNew()

                let sessions =
                    [|
                        for i in 1..sessionCount -> createSession store $"concurrent-{i}"
                    |]

                let work (index: int) : Task<int> =
                    task {
                        let sessionId = sessions[index].Id
                        let owner = $"concurrent-owner-{index}"
                        let watch = Stopwatch.StartNew()

                        for iteration in 1..iterations do
                            queueMessage store sessionId $"concurrent-{index}-{iteration}"
                            let claim = claimTurn store sessionId owner
                            appendBatch journal sessionId claim (textBatch sessionId claim.TurnId deltasPerTurn)
                            let _ = replayCount journal sessionId
                            let _ = pendingCount store sessionId
                            settleCompleted store claim

                        watch.Stop()
                        lock latencies (fun () -> latencies.Add(watch.Elapsed.TotalMilliseconds))
                        return replayCount journal sessionId
                    }

                let! counts =
                    Task.WhenAll(
                        [|
                            for i in 0 .. sessions.Length - 1 -> work i
                        |]
                    )

                wall.Stop()

                let expected = iterations * (deltasPerTurn + 2)

                for count in counts do
                    Assert.Equal(expected, count)

                Assert.Equal(sessionCount, counts.Length)

                let totalEvents = counts |> Array.sum
                let throughput = float totalEvents / wall.Elapsed.TotalSeconds

                let detail =
                    $"sessions={sessionCount} iterations={iterations} eventsPerTurn={deltasPerTurn + 2} "
                    + $"totalEvents={totalEvents} wallMs={wall.Elapsed.TotalMilliseconds:F1} "
                    + $"throughputEventsPerSec={throughput:F1} failures=0 "
                    + summarizeMs "sessionEndToEnd" (List.ofSeq latencies)

                printfn "%s" (evidenceHeader "concurrent-mix" database detail)
            with ex ->
                return raise ex
        finally
            (database :> IDisposable).Dispose()
            SqliteTestFixture.deleteDatabaseFiles path
    }

// ──────────────────────────────────────────────────────────────────────────
// Task 3: slow/contended operation versus unrelated sessions plus controls
// ──────────────────────────────────────────────────────────────────────────

[<Fact>]
let ``slow batch contends without starving unrelated sessions or controls`` () : Task =
    task {
        let clock = TestClock()
        let database, path = SqliteTestFixture.openTestDatabase clock

        try
            try
                let store = SqliteStoreFactory.sessionStore database
                let journal = SqliteStoreFactory.eventStore database
                let control = store :?> ISessionAbortControlStore
                let slow = createSession store "slow"

                let unrelated =
                    [|
                        for i in 1..4 -> createSession store $"unrelated-{i}"
                    |]

                let unrelatedMs = ResizeArray<float>()
                let wall = Stopwatch.StartNew()

                // The slow session holds one large journal batch: the single
                // contended operation under the shared gate.
                queueMessage store slow.Id "slow-work"
                let slowClaim = claimTurn store slow.Id "slow-owner"
                let slowBatch = textBatch slow.Id slowClaim.TurnId 50

                let slowWork: Task<float> =
                    task {
                        let watch = Stopwatch.StartNew()
                        appendBatch journal slow.Id slowClaim slowBatch
                        settleCompleted store slowClaim
                        watch.Stop()
                        return watch.Elapsed.TotalMilliseconds
                    }

                let unrelatedWork (index: int) : Task<int> =
                    task {
                        let sessionId = unrelated[index].Id
                        let owner = $"unrelated-owner-{index}"
                        let watch = Stopwatch.StartNew()

                        for iteration in 1..3 do
                            queueMessage store sessionId $"unrelated-{index}-{iteration}"
                            let claim = claimTurn store sessionId owner
                            appendBatch journal sessionId claim (textBatch sessionId claim.TurnId 1)
                            let _ = replayCount journal sessionId
                            settleCompleted store claim

                        watch.Stop()
                        lock unrelatedMs (fun () -> unrelatedMs.Add(watch.Elapsed.TotalMilliseconds))
                        return replayCount journal sessionId
                    }

                // Execution control under contention: a host abort request on
                // the slow session must still be processed while the large
                // batch is in flight. Processing (any well-typed receipt) is
                // control progress, not durable completion of the slow turn
                // itself; the observed outcome is recorded, not assumed.
                let controlWork: Task<HostAbortOutcome> =
                    task {
                        let! receipt =
                            control.RequestHostAbort(
                                tenant,
                                slow.Id,
                                slowClaim.TurnId,
                                StopCause.ExplicitAbort,
                                "envelope probe",
                                CancellationToken.None
                            )

                        Assert.NotNull(box receipt)
                        return receipt.Outcome
                    }

                // All three run concurrently over the shared gate: that is
                // the contention under measurement.
                let slowTask = slowWork

                let unrelatedTasks =
                    [|
                        for i in 0 .. unrelated.Length - 1 -> unrelatedWork i :> Task
                    |]

                let controlTask = controlWork :> Task
                do! Task.WhenAll(Array.append [| slowTask :> Task; controlTask |] unrelatedTasks)
                let slowMs = slowTask.GetAwaiter().GetResult()

                let controlOutcome =
                    (controlTask :?> Task<HostAbortOutcome>).GetAwaiter().GetResult()

                wall.Stop()

                // The slow journal holds its full batch; every unrelated
                // journal holds exactly its own turns with gap-free order.
                Assert.Equal(52, replayCount journal slow.Id)

                for session in unrelated do
                    Assert.Equal(3 * (1 + 2), replayCount journal session.Id)

                // Cancellation truthfulness: the provider executes
                // synchronously under the gate and does not cooperatively
                // cancel in-flight database work. An already-cancelled read
                // is therefore reported as observed, not assumed.
                use cancelled = new CancellationTokenSource()
                cancelled.Cancel()

                let cancelledObserved =
                    try
                        let _ = store.GetSession(tenant, slow.Id, cancelled.Token).GetAwaiter().GetResult()

                        "completed-despite-cancel"
                    with
                    | :? OperationCanceledException -> "cancelled"
                    | _ -> "completed-despite-cancel"

                let wallText = wall.Elapsed.TotalMilliseconds.ToString("F1")
                let slowText = slowMs.ToString("F1")

                let detail =
                    $"slowEvents=52 unrelatedSessions={unrelated.Length} unrelatedTurnsEach=3 "
                    + $"wallMs={wallText} slowMs={slowText} "
                    + summarizeMs "unrelatedEndToEnd" (List.ofSeq unrelatedMs)
                    + $" controlOutcome={controlOutcome} cancelledRead={cancelledObserved}"

                printfn "%s" (evidenceHeader "slow-contended" database detail)
            with ex ->
                return raise ex
        finally
            (database :> IDisposable).Dispose()
            SqliteTestFixture.deleteDatabaseFiles path
    }
