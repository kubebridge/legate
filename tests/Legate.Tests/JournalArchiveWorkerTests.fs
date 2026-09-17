// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.JournalArchiveWorkerTests

open System
open System.Collections.Generic
open System.IO
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
open Legate.Testing
open Microsoft.Data.Sqlite
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Xunit

// Leased journal archive worker (issue 111): retention gating, the
// facade-tenant sweep source shared with the session-expiry sweeper,
// archive-then-verify-then-delete ordering, defer-with-backoff that never
// deletes, stale-claim loser zero effects, paged discovery, standalone
// Legate:Archive binding, and the SQLite guarded alter for pre-column
// files. All time advances through the TestClock the in-memory database
// reads: never sleeps. Archive roots are fresh temp directories per fact.

let archiveTenant = TenantId.Create "archive-acme"
let otherTenant = TenantId.Create "archive-other"

/// Fresh archive root, unique per fact.
let tempArchiveRoot () : string =
    Path.Combine(Path.GetTempPath(), "legate-archive-" + Guid.NewGuid().ToString("N"))

/// Best-effort temp cleanup: cleanup must never fail a fact.
let deleteArchiveRoot (root: string) : unit =
    try
        if Directory.Exists root then
            Directory.Delete(root, true)
    with _ ->
        ()

/// Archive knobs over the root: seven-day retention unless stated.
let archiveOptions (root: string) : JournalArchiveOptions =
    let options = JournalArchiveOptions()
    options.ArchiveDirectory <- root
    options.RetentionDelay <- TimeSpan.FromDays 7.0
    options.PollInterval <- TimeSpan.FromHours 1.0
    options.LeaseDuration <- TimeSpan.FromMinutes 10.0
    options.VerifyBackoff <- TimeSpan.FromMinutes 1.0
    options

/// One delta event the facts append.
let delta (sessionId: SessionId) (turnId: TurnId) : SessionEvent =
    TextDeltaEvent(sessionId, turnId, Nullable(), DateTimeOffset.MinValue, "delta") :> SessionEvent

/// Creates a session with a journal of eventCount events, then closes it:
/// the shape the worker archives. Returns the session id.
let closedSessionWithJournal
    (sessionStore: ISessionStore)
    (eventStore: ISessionEventStore)
    (tenant: TenantId)
    (eventCount: int)
    : Task<SessionId> =
    task {
        let session =
            {
                Id = SessionId.New()
                Tenant = tenant
                AgentId = AgentId.New()
                Title = "archive"
                State = SessionState.Idle
                CurrentTurnId = Nullable()
                CreatedAt = DateTimeOffset.MinValue
                UpdatedAt = DateTimeOffset.MinValue
                ClosedAt = Nullable()
                WorkspaceBinding = null
                Options = SessionOptions()
                PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
            }

        let! created = sessionStore.CreateSession(tenant, session, CancellationToken.None)

        let message = UserMessagePayload(UserMessage.Text("journal")) :> InboxPayload

        let! _ =
            sessionStore.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

        let! claimed =
            sessionStore.ClaimNextTurn(
                tenant,
                created.Id,
                "archive-setup-owner",
                TimeSpan.FromHours 1.0,
                CancellationToken.None
            )

        let claim = (claimed :?> TurnLeaseRenewed).Claim

        let batch =
            ([
                for _ in 1..eventCount -> delta created.Id claim.TurnId
            ]
            :> Collections.Generic.IReadOnlyList<_>)

        let! appended = eventStore.Append(tenant, created.Id, claim.Token, batch, CancellationToken.None)

        if not (appended :? EventAppended) then
            failwith "expected the setup batch appended"

        let! _ = sessionStore.CloseSession(tenant, created.Id, CancellationToken.None)
        return created.Id
    }

/// Builds the worker over the stores, resolving the sweep tenant from the
/// provider exactly like the hosted loop does: a registered
/// SessionClientOptions scopes the sweep, otherwise the default tenant.
/// Private: the worker type stays internal to the runtime.
let private archiveWorker
    (sessionStore: ISessionStore)
    (eventStore: ISessionEventStore)
    (options: JournalArchiveOptions)
    (clock: TestClock)
    (delay: ILlmDelay)
    (random: ILlmRandom)
    (clientOptions: SessionClientOptions option)
    : JournalArchiveWorker =
    let services = ServiceCollection()
    services.AddSingleton<ISessionStore>(sessionStore) |> ignore
    services.AddSingleton<ISessionEventStore>(eventStore) |> ignore

    match clientOptions with
    | None -> ()
    | Some clients -> services.AddSingleton<SessionClientOptions>(clients) |> ignore

    let provider = services.BuildServiceProvider()

    JournalArchiveWorker(provider, Options.Create(options), clock, delay, random)

/// Archives through the real writer: replays the journal into the file at
/// the path and byte-verifies it. Top-level with full annotations so a
/// writer signature drift fails here, at the definition, instead of at
/// every call site.
let realWriteVerify
    (eventStore: ISessionEventStore)
    (tenant: TenantId)
    (sessionId: SessionId)
    (path: string)
    : Task<bool> =
    JournalArchiveFiles.writeAndVerifyAsync
        eventStore
        tenant
        sessionId
        path
        JournalArchive.MaxReplayPageSize
        CancellationToken.None

/// Counts CompleteCleanup calls while delegating everything: the
/// never-deletes proof.
type CompleteCountingStore(inner: ISessionEventStore) =
    let mutable completes = 0

    /// How many CompleteCleanup calls arrived.
    member _.CompleteCalls = completes

    interface ISessionEventStore with
        member _.Append(tenant, sessionId, token, events, cancellationToken) =
            inner.Append(tenant, sessionId, token, events, cancellationToken)

        member _.Replay(tenant, sessionId, cursor, limit, cancellationToken) =
            inner.Replay(tenant, sessionId, cursor, limit, cancellationToken)

        member _.TryClaimCleanup(tenant, sessionId, owner, duration, cancellationToken) =
            inner.TryClaimCleanup(tenant, sessionId, owner, duration, cancellationToken)

        member _.CompleteCleanup(tenant, sessionId, token, location, cancellationToken) =
            completes <- completes + 1
            inner.CompleteCleanup(tenant, sessionId, token, location, cancellationToken)

        member _.DeferCleanup(tenant, sessionId, token, cancellationToken) =
            inner.DeferCleanup(tenant, sessionId, token, cancellationToken)

[<Fact>]
let ``Retention gate leaves young closed sessions untouched`` () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    let sessionStore = InMemoryStoreFactory.sessionStore database
    let eventStore = InMemoryStoreFactory.eventStore database
    let root = tempArchiveRoot ()

    try
        task {
            let! sessionId = closedSessionWithJournal sessionStore eventStore TenantId.Default 2

            let worker =
                archiveWorker
                    sessionStore
                    eventStore
                    (archiveOptions root)
                    clock
                    (RecordingDelay() :> ILlmDelay)
                    (SeededRandom(7) :> ILlmRandom)
                    None

            // Closed an instant ago: the seven-day retention is not met.
            let! archived = worker.RunOnceAsync(CancellationToken.None)
            archived |> should equal 0

            // Untouched: the journal still pages and no lease is held.
            let! replayed = eventStore.Replay(TenantId.Default, sessionId, 0L, 10, CancellationToken.None)

            match replayed with
            | :? EventReplayPage as page -> page.Events.Count |> should equal 2
            | _ -> failwith "expected the journal intact"

            let! claimable =
                eventStore.TryClaimCleanup(
                    TenantId.Default,
                    sessionId,
                    "probe",
                    TimeSpan.FromMinutes 5.0,
                    CancellationToken.None
                )

            claimable :? EventCleanupClaimed |> should equal true

            File.Exists(JournalArchiveFiles.archivePath root TenantId.Default sessionId)
            |> should equal false

            // Past the retention: the next pass archives.
            clock.Advance(TimeSpan.FromDays 8.0)

            let! archived = worker.RunOnceAsync(CancellationToken.None)
            archived |> should equal 1

            let! expired = eventStore.Replay(TenantId.Default, sessionId, 0L, 10, CancellationToken.None)

            match expired with
            | :? EventReplayJournalExpired as gone ->
                let expected: string | null =
                    JournalArchiveFiles.archivePath root TenantId.Default sessionId

                gone.ArchiveLocation |> should equal expected
            | _ -> failwith "expected the expired journal with its pointer"
        }
        |> (fun t -> t.Wait())
    finally
        deleteArchiveRoot root

[<Fact>]
let ``Archive precedes delete and verifies byte-for-byte`` () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    let sessionStore = InMemoryStoreFactory.sessionStore database
    let eventStore = InMemoryStoreFactory.eventStore database
    let root = tempArchiveRoot ()

    try
        task {
            let! sessionId = closedSessionWithJournal sessionStore eventStore TenantId.Default 5
            clock.Advance(TimeSpan.FromDays 8.0)

            let worker =
                archiveWorker
                    sessionStore
                    eventStore
                    (archiveOptions root)
                    clock
                    (RecordingDelay() :> ILlmDelay)
                    (SeededRandom(7) :> ILlmRandom)
                    None

            let! archived = worker.RunOnceAsync(CancellationToken.None)
            archived |> should equal 1

            let path = JournalArchiveFiles.archivePath root TenantId.Default sessionId
            File.Exists path |> should equal true

            // No temp files linger beside the published archive.
            let directory: string = string (Path.GetDirectoryName path)

            Directory.GetFiles(directory) |> Array.length |> should equal 1

            let lines = File.ReadAllLines path
            lines.Length |> should equal 5

            for line in lines do
                use document = JsonDocument.Parse line

                let mutable discriminator = Unchecked.defaultof<JsonElement>

                document.RootElement.TryGetProperty("$type", &discriminator)
                |> should equal true

                discriminator.GetString() |> should equal "textDelta"

            // Delete ran only after the verify: the journal is gone with
            // its pointer.
            let! expired = eventStore.Replay(TenantId.Default, sessionId, 0L, 10, CancellationToken.None)

            match expired with
            | :? EventReplayJournalExpired as gone ->
                let expected: string | null = path
                gone.ArchiveLocation |> should equal expected
            | _ -> failwith "expected the expired journal with its pointer"
        }
        |> (fun t -> t.Wait())
    finally
        deleteArchiveRoot root

[<Fact>]
let ``Paged discovery archives a large closed backlog`` () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    let sessionStore = InMemoryStoreFactory.sessionStore database
    let eventStore = InMemoryStoreFactory.eventStore database
    let root = tempArchiveRoot ()

    try
        task {
            let sessions = ResizeArray<SessionId>()

            // Past one ListSessions page (50): the pass must follow the
            // continuation.
            for _ in 1..55 do
                let! sessionId = closedSessionWithJournal sessionStore eventStore TenantId.Default 1
                sessions.Add sessionId

            clock.Advance(TimeSpan.FromDays 8.0)

            let worker =
                archiveWorker
                    sessionStore
                    eventStore
                    (archiveOptions root)
                    clock
                    (RecordingDelay() :> ILlmDelay)
                    (SeededRandom(7) :> ILlmRandom)
                    None

            let! archived = worker.RunOnceAsync(CancellationToken.None)
            archived |> should equal 55

            for sessionId in sessions |> Seq.take 3 do
                let! expired = eventStore.Replay(TenantId.Default, sessionId, 0L, 10, CancellationToken.None)

                match expired with
                | :? EventReplayJournalExpired as gone ->
                    let expected: string | null =
                        JournalArchiveFiles.archivePath root TenantId.Default sessionId

                    gone.ArchiveLocation |> should equal expected
                | _ -> failwith "expected every journal expired with its pointer"
        }
        |> (fun t -> t.Wait())
    finally
        deleteArchiveRoot root

[<Fact>]
let ``Sweep covers the facade tenant when one is registered`` () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    let sessionStore = InMemoryStoreFactory.sessionStore database
    let eventStore = InMemoryStoreFactory.eventStore database
    let root = tempArchiveRoot ()

    try
        task {
            let! other = closedSessionWithJournal sessionStore eventStore otherTenant 1
            let! plain = closedSessionWithJournal sessionStore eventStore TenantId.Default 1
            clock.Advance(TimeSpan.FromDays 8.0)

            let clients = SessionClientOptions()
            clients.Tenant <- otherTenant

            let worker =
                archiveWorker
                    sessionStore
                    eventStore
                    (archiveOptions root)
                    clock
                    (RecordingDelay() :> ILlmDelay)
                    (SeededRandom(7) :> ILlmRandom)
                    (Some clients)

            let! archived = worker.RunOnceAsync(CancellationToken.None)
            archived |> should equal 1

            let! expired = eventStore.Replay(otherTenant, other, 0L, 10, CancellationToken.None)
            expired :? EventReplayJournalExpired |> should equal true

            // Outside the sweep tenant: untouched.
            let! replayed = eventStore.Replay(TenantId.Default, plain, 0L, 10, CancellationToken.None)
            replayed :? EventReplayPage |> should equal true
        }
        |> (fun t -> t.Wait())
    finally
        deleteArchiveRoot root

[<Fact>]
let ``Sweep covers the default tenant without a facade client`` () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    let sessionStore = InMemoryStoreFactory.sessionStore database
    let eventStore = InMemoryStoreFactory.eventStore database
    let root = tempArchiveRoot ()

    try
        task {
            let! sessionId = closedSessionWithJournal sessionStore eventStore TenantId.Default 1
            clock.Advance(TimeSpan.FromDays 8.0)

            let worker =
                archiveWorker
                    sessionStore
                    eventStore
                    (archiveOptions root)
                    clock
                    (RecordingDelay() :> ILlmDelay)
                    (SeededRandom(7) :> ILlmRandom)
                    None

            let! archived = worker.RunOnceAsync(CancellationToken.None)
            archived |> should equal 1

            let! expired = eventStore.Replay(TenantId.Default, sessionId, 0L, 10, CancellationToken.None)

            match expired with
            | :? EventReplayJournalExpired as gone ->
                let expected: string | null =
                    JournalArchiveFiles.archivePath root TenantId.Default sessionId

                gone.ArchiveLocation |> should equal expected
            | _ -> failwith "expected the expired journal with its pointer"
        }
        |> (fun t -> t.Wait())
    finally
        deleteArchiveRoot root

[<Fact>]
let ``Verify failure defers with backoff and never deletes`` () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    let sessionStore = InMemoryStoreFactory.sessionStore database
    let inner = InMemoryStoreFactory.eventStore database
    let counting = CompleteCountingStore(inner)
    let eventStore = counting :> ISessionEventStore
    let root = tempArchiveRoot ()
    let delay = RecordingDelay()
    let backoff = TimeSpan.FromMinutes 1.0

    try
        task {
            let! sessionId = closedSessionWithJournal sessionStore inner archiveTenant 2

            let! granted =
                inner.TryClaimCleanup(
                    archiveTenant,
                    sessionId,
                    "archive-worker",
                    TimeSpan.FromMinutes 10.0,
                    CancellationToken.None
                )

            let token = (granted :?> EventCleanupClaimed).Claim.Token

            let! archived =
                JournalArchive.archiveOneAsync
                    eventStore
                    archiveTenant
                    sessionId
                    token
                    root
                    (fun _ -> Task.FromResult false)
                    (delay :> ILlmDelay)
                    (SeededRandom(7) :> ILlmRandom)
                    backoff
                    CancellationToken.None

            archived |> should equal false
            counting.CompleteCalls |> should equal 0

            // Deferred, not deleted: the journal still pages and the lease
            // is released back to claimable.
            let! replayed = eventStore.Replay(archiveTenant, sessionId, 0L, 10, CancellationToken.None)
            replayed :? EventReplayPage |> should equal true

            let! replanted =
                eventStore.TryClaimCleanup(
                    archiveTenant,
                    sessionId,
                    "archive-worker",
                    TimeSpan.FromMinutes 10.0,
                    CancellationToken.None
                )

            replanted :? EventCleanupClaimed |> should equal true

            // The backoff waited with jitter: within [backoff, 2 * backoff).
            delay.Recorded.Count |> should equal 1
            (delay.Recorded[0] >= backoff) |> should equal true
            (delay.Recorded[0] < backoff + backoff) |> should equal true
        }
        |> (fun t -> t.Wait())
    finally
        deleteArchiveRoot root

[<Fact>]
let ``Archive failure defers with backoff and never deletes`` () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    let sessionStore = InMemoryStoreFactory.sessionStore database
    let inner = InMemoryStoreFactory.eventStore database
    let counting = CompleteCountingStore(inner)
    let eventStore = counting :> ISessionEventStore
    let root = tempArchiveRoot ()
    let delay = RecordingDelay()
    let backoff = TimeSpan.FromMinutes 1.0

    try
        task {
            let! sessionId = closedSessionWithJournal sessionStore inner archiveTenant 2

            let! granted =
                inner.TryClaimCleanup(
                    archiveTenant,
                    sessionId,
                    "archive-worker",
                    TimeSpan.FromMinutes 10.0,
                    CancellationToken.None
                )

            let token = (granted :?> EventCleanupClaimed).Claim.Token

            let failure = InvalidOperationException("disk full")

            let! archived =
                JournalArchive.archiveOneAsync
                    eventStore
                    archiveTenant
                    sessionId
                    token
                    root
                    (fun _ -> Task.FromException<bool>(failure))
                    (delay :> ILlmDelay)
                    (SeededRandom(7) :> ILlmRandom)
                    backoff
                    CancellationToken.None

            archived |> should equal false
            counting.CompleteCalls |> should equal 0

            let! replayed = eventStore.Replay(archiveTenant, sessionId, 0L, 10, CancellationToken.None)
            replayed :? EventReplayPage |> should equal true

            delay.Recorded.Count |> should equal 1
            (delay.Recorded[0] >= backoff) |> should equal true
        }
        |> (fun t -> t.Wait())
    finally
        deleteArchiveRoot root

[<Fact>]
let ``Stale-claim loser after a winner deletes nothing`` () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    let sessionStore = InMemoryStoreFactory.sessionStore database
    let eventStore = InMemoryStoreFactory.eventStore database
    let root = tempArchiveRoot ()
    let delay = RecordingDelay()

    try
        task {
            let! sessionId = closedSessionWithJournal sessionStore eventStore archiveTenant 2

            // The winner archives under its live claim.
            let! winnerGranted =
                eventStore.TryClaimCleanup(
                    archiveTenant,
                    sessionId,
                    "winner",
                    TimeSpan.FromMinutes 10.0,
                    CancellationToken.None
                )

            let winnerToken = (winnerGranted :?> EventCleanupClaimed).Claim.Token
            let winnerBackoff = TimeSpan.FromMinutes 1.0

            let! won =
                JournalArchive.archiveOneAsync
                    eventStore
                    archiveTenant
                    sessionId
                    winnerToken
                    root
                    (realWriteVerify eventStore archiveTenant sessionId)
                    (delay :> ILlmDelay)
                    (SeededRandom(7) :> ILlmRandom)
                    winnerBackoff
                    CancellationToken.None

            won |> should equal true

            let path = JournalArchiveFiles.archivePath root archiveTenant sessionId
            let winnerBytes = File.ReadAllBytes path

            // The loser arrives with a token from before the win: its
            // replay already reports the expired journal, so it writes
            // nothing and settles nothing.
            let loserBackoff = TimeSpan.FromMinutes 1.0

            let! lost =
                JournalArchive.archiveOneAsync
                    eventStore
                    archiveTenant
                    sessionId
                    "stale-token"
                    root
                    (realWriteVerify eventStore archiveTenant sessionId)
                    (delay :> ILlmDelay)
                    (SeededRandom(7) :> ILlmRandom)
                    loserBackoff
                    CancellationToken.None

            lost |> should equal false

            File.ReadAllBytes path |> should equal winnerBytes

            let! expired = eventStore.Replay(archiveTenant, sessionId, 0L, 10, CancellationToken.None)

            match expired with
            | :? EventReplayJournalExpired as gone ->
                let expected: string | null = path
                gone.ArchiveLocation |> should equal expected
            | _ -> failwith "expected the winner's pointer intact"
        }
        |> (fun t -> t.Wait())
    finally
        deleteArchiveRoot root

[<Fact>]
let ``Lapsed lease loser leaves the journal intact and no orphan`` () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    let sessionStore = InMemoryStoreFactory.sessionStore database
    let eventStore = InMemoryStoreFactory.eventStore database
    let root = tempArchiveRoot ()
    let delay = RecordingDelay()

    try
        task {
            let! sessionId = closedSessionWithJournal sessionStore eventStore archiveTenant 2

            let! granted =
                eventStore.TryClaimCleanup(
                    archiveTenant,
                    sessionId,
                    "loser",
                    TimeSpan.FromMinutes 10.0,
                    CancellationToken.None
                )

            let token = (granted :?> EventCleanupClaimed).Claim.Token

            // The lease lapses with no winner: the settlement rejects.
            clock.Advance(TimeSpan.FromMinutes 11.0)

            let lapsedBackoff = TimeSpan.FromMinutes 1.0

            let! archived =
                JournalArchive.archiveOneAsync
                    eventStore
                    archiveTenant
                    sessionId
                    token
                    root
                    (realWriteVerify eventStore archiveTenant sessionId)
                    (delay :> ILlmDelay)
                    (SeededRandom(7) :> ILlmRandom)
                    lapsedBackoff
                    CancellationToken.None

            archived |> should equal false

            // Zero effects: the journal still pages, the rejected attempt
            // removed its own file, and no deferral was recorded (the lease
            // is already gone).
            let! replayed = eventStore.Replay(archiveTenant, sessionId, 0L, 10, CancellationToken.None)
            replayed :? EventReplayPage |> should equal true

            File.Exists(JournalArchiveFiles.archivePath root archiveTenant sessionId)
            |> should equal false

            delay.Recorded.Count |> should equal 0
        }
        |> (fun t -> t.Wait())
    finally
        deleteArchiveRoot root

[<Fact>]
let ``Archive paths stay inside the root`` () =
    let root = tempArchiveRoot ()

    JournalArchiveFiles.sanitizeSegment "a/b..\\c" |> should equal "a_b___c"
    JournalArchiveFiles.sanitizeSegment "" |> should equal "_"

    let hostile = TenantId.Create "../../evil"

    let path = JournalArchiveFiles.archivePath root hostile (SessionId.New())

    path.StartsWith(root + string Path.DirectorySeparatorChar, StringComparison.Ordinal)
    |> should equal true

    Path.GetFileName path |> should equal "events.jsonl"

[<Fact>]
let ``JournalArchiveOptions validate`` () =
    JournalArchiveOptions().Validate() |> should equal null

    let empty = JournalArchiveOptions()
    empty.ArchiveDirectory <- ""
    empty.Validate() |> should equal null

    let immediate = JournalArchiveOptions()
    immediate.RetentionDelay <- TimeSpan.Zero
    immediate.Validate() |> should equal null

    let negativeRetention = JournalArchiveOptions()
    negativeRetention.RetentionDelay <- TimeSpan.FromDays -1.0
    negativeRetention.Validate() |> should not' (equal null)

    let noPoll = JournalArchiveOptions()
    noPoll.PollInterval <- TimeSpan.Zero
    noPoll.Validate() |> should not' (equal null)

    let noLease = JournalArchiveOptions()
    noLease.LeaseDuration <- TimeSpan.Zero
    noLease.Validate() |> should not' (equal null)

    let noBackoff = JournalArchiveOptions()
    noBackoff.VerifyBackoff <- TimeSpan.Zero
    noBackoff.Validate() |> should not' (equal null)

    let badPath = JournalArchiveOptions()
    badPath.ArchiveDirectory <- "bad" + string (char 0) + "dir"
    badPath.Validate() |> should not' (equal null)

[<Fact>]
let ``JournalArchiveOptions bind standalone under Legate-Archive`` () =
    let values =
        dict
            [
                "Legate:Archive:ArchiveDirectory", "/var/legate/archive"
                "Legate:Archive:RetentionDelay", "7.00:00:00"
                "Legate:Archive:PollInterval", "01:00:00"
                "Legate:Archive:LeaseDuration", "00:10:00"
                "Legate:Archive:VerifyBackoff", "00:01:00"
            ]

    let configuration =
        ConfigurationBuilder().AddInMemoryCollection(values).Build() :> IConfiguration

    let services = ServiceCollection()
    services.AddSingleton<IConfiguration>(configuration) |> ignore
    JournalArchiveRegistration.register services

    // Registering again never duplicates the worker: host registrations
    // win and ours stays single.
    JournalArchiveRegistration.register services

    let workers =
        services
        |> Seq.filter (fun descriptor ->
            descriptor.ServiceType = typeof<IHostedService>
            && descriptor.ImplementationType = typeof<JournalArchiveWorker>)
        |> Seq.length

    workers |> should equal 1

    use provider = services.BuildServiceProvider()
    let resolved = provider.GetRequiredService<IOptions<JournalArchiveOptions>>().Value

    resolved.ArchiveDirectory |> should equal "/var/legate/archive"
    resolved.RetentionDelay |> should equal (TimeSpan.FromDays 7.0)
    resolved.PollInterval |> should equal (TimeSpan.FromHours 1.0)
    resolved.LeaseDuration |> should equal (TimeSpan.FromMinutes 10.0)
    resolved.VerifyBackoff |> should equal (TimeSpan.FromMinutes 1.0)
    resolved.Validate() |> should equal null

[<Fact>]
let ``Registration without configuration keeps the defaults`` () =
    let services = ServiceCollection()
    JournalArchiveRegistration.register services

    use provider = services.BuildServiceProvider()
    let resolved = provider.GetRequiredService<IOptions<JournalArchiveOptions>>().Value

    resolved.ArchiveDirectory |> should equal null
    resolved.RetentionDelay |> should equal (TimeSpan.FromDays 7.0)
    resolved.Validate() |> should equal null

[<Fact>]
let ``SQLite guarded alter adds the pointer to pre-column files`` () =
    let path =
        Path.Combine(Path.GetTempPath(), "legate-archive-" + Guid.NewGuid().ToString("N") + ".db")

    try
        // A file predating the pointer: the three-column marker shape.
        use setup = new SqliteConnection($"Data Source=%s{path}")
        setup.Open()

        use create = setup.CreateCommand()

        create.CommandText <-
            """CREATE TABLE "journal_archive" (
  session_id TEXT NOT NULL PRIMARY KEY,
  tenant TEXT NOT NULL,
  archived_at TEXT NOT NULL
);"""

        create.ExecuteNonQuery() |> ignore
        setup.Close()

        use database = SqliteDatabase.Open(path, TestClock())

        use probe = new SqliteConnection($"Data Source=%s{path}")
        probe.Open()

        use columns = probe.CreateCommand()
        columns.CommandText <- "PRAGMA table_info(\"journal_archive\")"

        use reader = columns.ExecuteReader()

        let names =
            [
                while reader.Read() do
                    reader.GetString(1)
            ]

        names |> should contain "archive_path"

        // The altered table serves the pointer end to end.
        let sessionStore = SqliteStoreFactory.sessionStore database
        let eventStore = SqliteStoreFactory.eventStore database

        task {
            let! sessionId = closedSessionWithJournal sessionStore eventStore archiveTenant 2

            let! granted =
                eventStore.TryClaimCleanup(
                    archiveTenant,
                    sessionId,
                    "archive-worker",
                    TimeSpan.FromMinutes 10.0,
                    CancellationToken.None
                )

            let token = (granted :?> EventCleanupClaimed).Claim.Token
            let pointer = "/archives/legacy/events.jsonl"

            let! completed =
                eventStore.CompleteCleanup(archiveTenant, sessionId, token, pointer, CancellationToken.None)

            (completed :? EventCleanupApplied) |> should equal true

            let! expired = eventStore.Replay(archiveTenant, sessionId, 0L, 10, CancellationToken.None)

            match expired with
            | :? EventReplayJournalExpired as gone ->
                let expected: string | null = pointer
                gone.ArchiveLocation |> should equal expected
            | _ -> failwith "expected the expired journal with its pointer"
        }
        |> (fun t -> t.Wait())
    finally
        SqliteTestFixture.deleteDatabaseFiles path
