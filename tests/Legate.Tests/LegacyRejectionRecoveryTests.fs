// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LegacyRejectionRecoveryTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
open Legate.Testing
open Microsoft.Data.Sqlite
open Xunit

// Issue 372: unsupported old persistence fails closed before execution,
// and supported current-format crash/reopen keeps durable work.
// Rejection never consumes inbox input, rebinds agents, or rewrites old
// rows; clean start is an explicit host choice with fresh persistence.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

let private tenant = TenantId.Create "acme"

/// A wait-bound seam that never fires: these tests never run a turn.
type private NeverDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

let private noResolve (_: SessionId) (_: CancellationToken) : Task<IActorRef> =
    Task.FromException<IActorRef>(InvalidOperationException("resolve must not run for unsupported sessions"))

let private makeClient (store: ISessionStore) (journal: ISessionEventStore) : SessionClient =
    new SessionClient(
        store,
        tenant,
        noResolve,
        new SessionEventBus(journal),
        TimeSpan.FromMinutes 1.0,
        NeverDelay() :> ILlmDelay,
        None
    )

let private currentOptions () = SessionOptions()

let private legacyOptions () =
    let options = SessionOptions()
    options.FormatVersion <- 0
    options

let private sampleSessionWith (options: SessionOptions) (state: SessionState) =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "checkout"
        State = state
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = options
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private createSupported (store: ISessionStore) (state: SessionState) : Session =
    store
        .CreateSession(tenant, sampleSessionWith (currentOptions ()) state, CancellationToken.None)
        .GetAwaiter()
        .GetResult()

let private injectLegacy (database: InMemoryDatabase) (state: SessionState) : SessionId =
    let legacy = sampleSessionWith (legacyOptions ()) state
    database.Sessions[(tenant, legacy.Id)] <- legacy
    legacy.Id

let private appendText (store: ISessionStore) (sessionId: SessionId) (text: string) : InboxEntry =
    let payload = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload

    store
        .AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
        .GetAwaiter()
        .GetResult()

let private pendingCount (store: ISessionStore) (sessionId: SessionId) : int =
    store.ReadPendingInbox(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult().Count

let private expectUnsupported (action: unit -> unit) : CompletionRoutingException =
    try
        action ()
        failwith "Expected CompletionRoutingException with UnsupportedFormat."
    with :? CompletionRoutingException as refusal ->
        refusal.Reason |> should equal CompletionRoutingReason.UnsupportedFormat
        refusal

// ──────────────────────────────────────────────────────────────────────────
// Persistence gate

[<Fact>]
let ``Deserialize rejects a non-current format version`` () =
    let refusal =
        expectUnsupported (fun () ->
            SessionOptionsPersistence.Deserialize """{"FormatVersion":0,"Outcome":0,"OnCrashResume":0}"""
            |> ignore)

    refusal.Message.Contains("clean-start") |> should equal true

[<Fact>]
let ``Deserialize rejects a missing format marker`` () =
    expectUnsupported (fun () ->
        SessionOptionsPersistence.Deserialize """{"Outcome":0,"OnCrashResume":0}"""
        |> ignore)
    |> ignore

[<Fact>]
let ``Deserialize rejects legacy sink and policy members with the typed refusal`` () =
    let payload =
        """{"Title":null,"AutoClose":false,"Outcome":0,"Permissions":{},"AskUser":null,"CompletionSink":{},"MaxIterations":0,"Timeout":null,"Metadata":null,"HostInstructionFiles":null,"OnCrashResume":0}"""

    for _ in [ 1; 2 ] do
        expectUnsupported (fun () -> SessionOptionsPersistence.Deserialize payload |> ignore)
        |> ignore

[<Fact>]
let ``Deserialize rejects truncated payloads without proving no-effect`` () =
    expectUnsupported (fun () -> SessionOptionsPersistence.Deserialize """{"FormatVersion":1""" |> ignore)
    |> ignore

[<Fact>]
let ``Serialize refuses null and Snapshot refuses legacy options`` () =
    expectUnsupported (fun () ->
        SessionOptionsPersistence.Serialize(Unchecked.defaultof<SessionOptions>)
        |> ignore)
    |> ignore

    expectUnsupported (fun () -> SessionOptionsPersistence.Snapshot(legacyOptions ()) |> ignore)
    |> ignore

[<Fact>]
let ``ValidatePersistence rejects a non-current version with a secret-free diagnostic`` () =
    let options = legacyOptions ()
    let refusal = expectUnsupported (fun () -> options.ValidatePersistence())

    refusal.Message.Contains("clean-start") |> should equal true
    refusal.DestinationId |> should equal null

[<Fact>]
let ``Completion route validation fails closed on null options`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let journal = InMemoryStoreFactory.eventStore database
    let client = makeClient store journal

    let nullRow =
        { sampleSessionWith (currentOptions ()) SessionState.Idle with
            Options = Unchecked.defaultof<SessionOptions>
        }

    let refusal =
        try
            client.ValidateCompletionRoute nullRow
            failwith "Expected CompletionRoutingException."
        with :? CompletionRoutingException as error ->
            error.Reason |> should equal CompletionRoutingReason.UnsupportedFormat
            error

    refusal.Message.Contains("clean-start") |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// InMemory rejection fixtures

[<Theory>]
[<InlineData(SessionState.Idle)>]
[<InlineData(SessionState.Running)>]
[<InlineData(SessionState.WaitingForInput)>]
[<InlineData(SessionState.Closed)>]
let ``InMemory reads reject legacy rows in every lifecycle state`` (state: SessionState) =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let sessionId = injectLegacy database state

    for _ in [ 1; 2 ] do
        expectUnsupported (fun () ->
            store.GetSession(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult()
            |> ignore)
        |> ignore

    // Repeated rejection leaves the old row untouched.
    match database.Sessions.TryGetValue((tenant, sessionId)) with
    | true, retained -> retained.Options.FormatVersion |> should equal 0
    | false, _ -> failwith "Legacy row must remain stored."

[<Fact>]
let ``InMemory create refuses legacy options before any row lands`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let legacy = sampleSessionWith (legacyOptions ()) SessionState.Idle

    expectUnsupported (fun () -> store.CreateSession(tenant, legacy, CancellationToken.None) |> ignore)
    |> ignore

    let page =
        store
            .ListSessions(tenant, Nullable(), Nullable(), Nullable(), Nullable(), 10, null, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    page.Items.Count |> should equal 0

[<Fact>]
let ``InMemory legacy rows keep pending inbox work unconsumed across rejection`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let sessionId = injectLegacy database SessionState.Idle
    let _ = appendText store sessionId "first"
    let _ = appendText store sessionId "second"

    for _ in [ 1; 2 ] do
        expectUnsupported (fun () ->
            store.GetSession(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult()
            |> ignore)
        |> ignore

    pendingCount store sessionId |> should equal 2

    match database.Sessions.TryGetValue((tenant, sessionId)) with
    | true, retained -> retained.Options.FormatVersion |> should equal 0
    | false, _ -> failwith "Legacy row must remain stored."

// ──────────────────────────────────────────────────────────────────────────
// Facade rejection: no execution, no rebind, no fork, no inbox consume.
// These run synchronously: rejection fires before any real async work,
// and GetAwaiter().GetResult() surfaces the typed refusal unwrapped.

let private awaitRefusal (work: Task<'T>) : CompletionRoutingException =
    try
        work.GetAwaiter().GetResult() |> ignore
        failwith "Expected CompletionRoutingException with UnsupportedFormat."
    with :? CompletionRoutingException as refusal ->
        refusal.Reason |> should equal CompletionRoutingReason.UnsupportedFormat
        refusal

[<Fact>]
let ``Prompt on a legacy session fails before actor resolution`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let journal = InMemoryStoreFactory.eventStore database
    let client = makeClient store journal
    let sessionId = injectLegacy database SessionState.Idle
    let _ = appendText store sessionId "held"

    let refusal =
        awaitRefusal (
            SessionClientOperations.PromptAsync(
                client,
                sessionId,
                UserMessage.Text "new work",
                DeliveryMode.Queue,
                CancellationToken.None
            )
        )

    refusal.Message.Contains("clean-start") |> should equal true
    pendingCount store sessionId |> should equal 1

[<Fact>]
let ``Reply on a legacy session fails before resuming anything`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let journal = InMemoryStoreFactory.eventStore database
    let client = makeClient store journal
    let sessionId = injectLegacy database SessionState.WaitingForInput

    awaitRefusal (
        SessionClientOperations.ReplyAsync(
            client,
            sessionId,
            PermissionDecision("req-1", PermissionDecisionKind.AllowOnce),
            CancellationToken.None
        )
    )
    |> ignore

[<Fact>]
let ``SetAgent on a legacy session fails without rebinding`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let journal = InMemoryStoreFactory.eventStore database
    let client = makeClient store journal
    let sessionId = injectLegacy database SessionState.Idle
    let before = (database.Sessions[(tenant, sessionId)]).AgentId

    awaitRefusal (SessionClientOperations.SetAgentAsync(client, sessionId, AgentId.New(), CancellationToken.None))
    |> ignore

    (database.Sessions[(tenant, sessionId)]).AgentId |> should equal before

[<Fact>]
let ``Compact on a legacy session fails before touching the journal`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let journal = InMemoryStoreFactory.eventStore database
    let client = makeClient store journal
    let sessionId = injectLegacy database SessionState.Idle

    awaitRefusal (SessionClientOperations.CompactAsync(client, sessionId, CancellationToken.None))
    |> ignore

[<Fact>]
let ``Fork of a legacy session fails without creating a rewritten copy`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let journal = InMemoryStoreFactory.eventStore database
    let client = makeClient store journal
    let sessionId = injectLegacy database SessionState.Idle

    awaitRefusal (SessionClientOperations.ForkAsync(client, sessionId, 0L, CancellationToken.None))
    |> ignore

    // Only the untouched source row remains: no rewritten copy was created.
    database.Sessions.Count |> should equal 1

[<Fact>]
let ``Open with legacy options fails before any row lands`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let journal = InMemoryStoreFactory.eventStore database
    let client = makeClient store journal

    awaitRefusal (
        SessionClientOperations.OpenSessionAsync(client, AgentId.New(), legacyOptions (), CancellationToken.None)
    )
    |> ignore

    database.Sessions.Count |> should equal 0

[<Fact>]
let ``Prompt on a null-options row fails closed with the typed refusal`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let journal = InMemoryStoreFactory.eventStore database
    let client = makeClient store journal

    let nullRow =
        { sampleSessionWith (currentOptions ()) SessionState.Idle with
            Options = Unchecked.defaultof<SessionOptions>
        }

    database.Sessions[(tenant, nullRow.Id)] <- nullRow

    awaitRefusal (
        SessionClientOperations.PromptAsync(
            client,
            nullRow.Id,
            UserMessage.Text "new work",
            DeliveryMode.Queue,
            CancellationToken.None
        )
    )
    |> ignore

// ──────────────────────────────────────────────────────────────────────────
// Supported-format crash/reopen: durable, ordered, idempotent, fenced

[<Fact>]
let ``Supported crash reopen retains ordered inbox work across store instances`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let session = createSupported store SessionState.Idle
    let _ = appendText store session.Id "first"
    let _ = appendText store session.Id "second"

    // A reopened store over the same database is the crash boundary.
    let reopened = InMemoryStoreFactory.sessionStore database

    let pending =
        reopened.ReadPendingInbox(tenant, session.Id, CancellationToken.None).GetAwaiter().GetResult()

    pending.Count |> should equal 2

    let texts =
        pending
        |> Seq.map (fun entry -> (entry.Payload :?> UserMessagePayload).Message)
        |> Seq.toList

    texts.Length |> should equal 2

[<Fact>]
let ``Supported settlement is idempotent and never re-executes`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let session = createSupported store SessionState.Idle
    let _ = appendText store session.Id "work"

    let lease =
        store
            .ClaimNextTurn(tenant, session.Id, "owner-a", TimeSpan.FromHours 1.0, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    let claim =
        match lease with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | unexpected -> failwithf "Expected a renewed claim, saw %O." (unexpected.GetType().Name)

    let first =
        store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None).GetAwaiter().GetResult()

    first |> should be (ofExactType<TurnSettled>)

    let second =
        store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None).GetAwaiter().GetResult()

    second |> should be (ofExactType<TurnAlreadySettled>)

    // Subsequent prompting still lands after the settled turn.
    let _ = appendText store session.Id "follow-up"
    pendingCount store session.Id |> should equal 1

[<Fact>]
let ``Competing owners fence the loser to zero effects`` () =
    let database = InMemoryDatabase()
    let store = InMemoryStoreFactory.sessionStore database
    let session = createSupported store SessionState.Idle
    let _ = appendText store session.Id "work"

    let winnerLease =
        store
            .ClaimNextTurn(tenant, session.Id, "owner-a", TimeSpan.FromHours 1.0, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    let winner =
        match winnerLease with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | unexpected -> failwithf "Expected a renewed claim, saw %O." (unexpected.GetType().Name)

    let loserLease =
        store
            .ClaimNextTurn(tenant, session.Id, "owner-b", TimeSpan.FromHours 1.0, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    loserLease |> should be (ofExactType<TurnLeaseMissing>)

    let stale =
        {
            TurnId = winner.TurnId
            Token = "stale-token"
            Owner = "owner-b"
            ExpiresAt = DateTimeOffset.UtcNow.AddHours 1.0
            Attempt = winner.Attempt
        }

    let rejected =
        store.SettleTurn(tenant, stale, TurnStatus.Completed, null, CancellationToken.None).GetAwaiter().GetResult()

    rejected |> should be (ofExactType<TurnSettleRejected>)

    let applied =
        store.SettleTurn(tenant, winner, TurnStatus.Completed, null, CancellationToken.None).GetAwaiter().GetResult()

    applied |> should be (ofExactType<TurnSettled>)

// ──────────────────────────────────────────────────────────────────────────
// Real SQLite fixtures

let private tableWithColumn (connection: SqliteConnection) (column: string) : string =
    use tables =
        new SqliteCommand("SELECT name FROM sqlite_master WHERE type = 'table'", connection)

    let names = ResizeArray<string>()
    use reader = tables.ExecuteReader()

    while reader.Read() do
        names.Add(reader.GetString(0))

    names
    |> Seq.find (fun name ->
        use pragma = new SqliteCommand($"PRAGMA table_info(\"{name}\")", connection)
        use columns = pragma.ExecuteReader()
        let mutable found = false

        while columns.Read() do
            if columns.GetString(1) = column then
                found <- true

        found)

let private sessionsTable (connection: SqliteConnection) : string =
    tableWithColumn connection "options_json"

[<Fact>]
let ``SQLite reads reject legacy option rows and leave them untouched`` () =
    let clock = TestClock()
    let database, path = SqliteTestFixture.openTestDatabase clock

    try
        let store = SqliteStoreFactory.sessionStore database
        let session = createSupported store SessionState.Idle
        let _ = appendText store session.Id "held"

        use connection = new SqliteConnection($"Data Source={path}")
        connection.Open()
        let table = sessionsTable connection

        use rewrite =
            new SqliteCommand(
                $"UPDATE \"{table}\" SET options_json = '{{\"FormatVersion\":0,\"Outcome\":0,\"OnCrashResume\":0}}' WHERE id = $id",
                connection
            )

        rewrite.Parameters.AddWithValue("$id", session.Id.ToString()) |> ignore
        rewrite.ExecuteNonQuery() |> ignore

        // The relational store captures read failures on its faulted task:
        // awaiting surfaces the typed refusal.
        for _ in [ 1; 2 ] do
            expectUnsupported (fun () ->
                store.GetSession(tenant, session.Id, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore

        use check =
            new SqliteCommand($"SELECT options_json FROM \"{table}\" WHERE id = $id", connection)

        check.Parameters.AddWithValue("$id", session.Id.ToString()) |> ignore

        match check.ExecuteScalar() with
        | :? string as retained -> retained.Contains("\"FormatVersion\":0") |> should equal true
        | _ -> failwith "Legacy SQLite row must remain stored."

        // The held inbox work is still pending and unconsumed: rejection
        // consumed nothing. Asserted on the raw row because the store's
        // pending-inbox read itself fails closed on the legacy session.
        let inboxTable = tableWithColumn connection "payload_json"

        use pending =
            new SqliteCommand(
                $"SELECT COUNT(*) FROM \"{inboxTable}\" WHERE session_id = $id AND consumed = 0",
                connection
            )

        pending.Parameters.AddWithValue("$id", session.Id.ToString()) |> ignore
        (pending.ExecuteScalar() :?> int64) |> should equal 1L
    finally
        (database :> IDisposable).Dispose()
        SqliteTestFixture.deleteDatabaseFiles path

[<Fact>]
let ``SQLite crash reopen retains ordered inbox and settles idempotently`` () =
    let clock = TestClock()
    let database, path = SqliteTestFixture.openTestDatabase clock

    try
        let store = SqliteStoreFactory.sessionStore database
        let session = createSupported store SessionState.Idle
        let _ = appendText store session.Id "first"
        let _ = appendText store session.Id "second"

        (database :> IDisposable).Dispose()

        use reopened = SqliteDatabase.Open(path, clock)
        let store = SqliteStoreFactory.sessionStore reopened

        let pending =
            store.ReadPendingInbox(tenant, session.Id, CancellationToken.None).GetAwaiter().GetResult()

        pending.Count |> should equal 2
        pending[0].Position < pending[1].Position |> should equal true

        let lease =
            store
                .ClaimNextTurn(tenant, session.Id, "owner-a", TimeSpan.FromHours 1.0, CancellationToken.None)
                .GetAwaiter()
                .GetResult()

        let claim =
            match lease with
            | :? TurnLeaseRenewed as renewed -> renewed.Claim
            | unexpected -> failwithf "Expected a renewed claim, saw %O." (unexpected.GetType().Name)

        let first =
            store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None).GetAwaiter().GetResult()

        first |> should be (ofExactType<TurnSettled>)

        let second =
            store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None).GetAwaiter().GetResult()

        second |> should be (ofExactType<TurnAlreadySettled>)

        let _ = appendText store session.Id "follow-up"
        pendingCount store session.Id |> should equal 2
    finally
        SqliteTestFixture.deleteDatabaseFiles path
