// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionExpiryTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.FileSystem
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Xunit

// Session expiry and workspace idle teardown (issue 110): the additive
// Sessions:Expiry knob, the clock-seamed sweeper closing idle-beyond-Expiry
// sessions through the idempotent store close, and the startup gate failing
// fast when expiry is enabled without a durable blob store. All time
// advances through the TestClock the in-memory database reads: never sleeps.

let tenant = TenantId.Create "acme"

/// A stub provider: startup validation only checks presence, so the stub
/// never creates a chat client.
type StubProvider() =

    interface ILlmProvider with
        member _.Id = "stub"
        member _.DefaultModel = "stub/model"

        member _.Capabilities =
            {
                Streaming = false
                Reasoning = false
                ToolCalling = false
            }

        member _.CreateChatClient(_, _) : IChatClient =
            raise (NotSupportedException("The stub provider creates no chat clients."))

/// An Idle session row; the store stamps CreatedAt/UpdatedAt from its clock.
let sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "expiry"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// A store over a fresh database on a TestClock, plus the clock.
let createStore () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    InMemoryStoreFactory.sessionStore database, InMemoryStoreFactory.blobStore database, clock

/// Builds the Legate section from in-memory pairs, mirroring the binding
/// tests' helper.
let private buildSection (pairs: (string * string) seq) : IConfigurationSection =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build().GetSection("Legate")

/// Requires the session row or fails the test: GetSession returns null
/// when the id does not exist in the tenant.
let private require (session: Session | null) =
    match session with
    | null -> failwith "The session row is missing."
    | live -> live

// ──────────────────────────────────────────────────────────────────────────
// Options knob

[<Fact>]
let ``Expiry defaults to disabled`` () =
    let options = SessionsOptions()
    options.Expiry.HasValue |> should equal false
    options.Validate() |> should equal null

[<Fact>]
let ``Expiry accepts a positive bound`` () =
    let options = SessionsOptions(Expiry = Nullable(TimeSpan.FromMinutes 30.0))
    options.Expiry.Value |> should equal (TimeSpan.FromMinutes 30.0)
    options.Validate() |> should equal null

[<Fact>]
let ``Expiry rejects zero and negative bounds`` () =
    SessionsOptions(Expiry = Nullable TimeSpan.Zero).Validate()
    |> should equal "Expiry must be positive when set."

    SessionsOptions(Expiry = Nullable(TimeSpan.FromMinutes -1.0)).Validate()
    |> should equal "Expiry must be positive when set."

[<Fact>]
let ``Binds Sessions Expiry from configuration`` () =
    let section = buildSection [ "Legate:Sessions:Expiry", "30m" ]
    let options = LegateOptionsBinding.bind section
    options.Sessions.Expiry.HasValue |> should equal true
    options.Sessions.Expiry.Value |> should equal (TimeSpan.FromMinutes 30.0)
    options.Validate() |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Sweeper

[<Fact>]
let ``Idle beyond Expiry closes and active stays open`` () =
    task {
        let store, _, clock = createStore ()
        let sessions = SessionsOptions(Expiry = Nullable(TimeSpan.FromMinutes 30.0))

        let! idle = store.CreateSession(tenant, sampleSession (), CancellationToken.None)
        let! active = store.CreateSession(tenant, sampleSession (), CancellationToken.None)

        // Ten minutes pass, then the active session sees state traffic, so
        // its UpdatedAt refreshes while the idle session's does not.
        clock.Advance(TimeSpan.FromMinutes 10.0)

        let! _ = store.UpdateSessionState(tenant, active.Id, SessionState.Idle, CancellationToken.None)

        // Twenty-five more minutes: the idle session sat 35 minutes, the
        // active one 25.
        clock.Advance(TimeSpan.FromMinutes 25.0)

        let! closed = SessionExpiry.passOnceAsync store tenant sessions clock CancellationToken.None

        closed |> should equal 1

        let! idleAfter = store.GetSession(tenant, idle.Id, CancellationToken.None)
        let idleLive = require idleAfter
        idleLive.State |> should equal SessionState.Closed
        idleLive.ClosedAt.HasValue |> should equal true
        idleLive.PermissionGrants.Count |> should equal 0

        let! activeAfter = store.GetSession(tenant, active.Id, CancellationToken.None)
        (require activeAfter).State |> should equal SessionState.Idle
    }

[<Fact>]
let ``Double close is a no-op with grants evicted and closed observed`` () =
    task {
        let store, _, clock = createStore ()
        let sessions = SessionsOptions(Expiry = Nullable(TimeSpan.FromMinutes 30.0))

        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)
        let! _ = store.GrantSessionTool(tenant, created.Id, "read_file", CancellationToken.None)

        clock.Advance(TimeSpan.FromHours 1.0)

        let! first = SessionExpiry.closeExpiredAsync store tenant created.Id CancellationToken.None
        first.State |> should equal SessionState.Closed
        first.PermissionGrants.Count |> should equal 0

        let! second = SessionExpiry.closeExpiredAsync store tenant created.Id CancellationToken.None
        second.State |> should equal SessionState.Closed
        second.PermissionGrants.Count |> should equal 0

        // The closed session is observed as closed and never reaped again.
        let! observed = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require observed).State |> should equal SessionState.Closed

        let! reaped = SessionExpiry.passOnceAsync store tenant sessions clock CancellationToken.None
        reaped |> should equal 0
    }

[<Fact>]
let ``Disabled expiry closes nothing`` () =
    task {
        let store, _, clock = createStore ()

        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)
        clock.Advance(TimeSpan.FromDays 30.0)

        let! closed = SessionExpiry.passOnceAsync store tenant (SessionsOptions()) clock CancellationToken.None

        closed |> should equal 0

        let! after = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require after).State |> should equal SessionState.Idle
    }

[<Fact>]
let ``RunOnceAsync closes idle sessions on the real service`` () =
    task {
        let clock = TestClock()
        let database = InMemoryDatabase(clock)
        let store = InMemoryStoreFactory.sessionStore database
        let sessions = SessionsOptions(Expiry = Nullable(TimeSpan.FromMinutes 30.0))

        let services = ServiceCollection()
        services.AddSingleton<ISessionStore>(store) |> ignore

        services.AddSingleton<SessionClientOptions>(SessionClientOptions(Tenant = tenant))
        |> ignore

        services.Configure<LegateOptions>(Action<LegateOptions>(fun target -> target.Sessions <- sessions))
        |> ignore

        use provider = services.BuildServiceProvider()
        let options = provider.GetRequiredService<IOptions<LegateOptions>>()
        let delay = RecordingDelay() :> ILlmDelay
        let service = SessionExpiryService(provider, options, clock, delay)

        let! created = store.CreateSession(tenant, sampleSession (), CancellationToken.None)
        clock.Advance(TimeSpan.FromHours 1.0)

        let! reaped = service.RunOnceAsync(CancellationToken.None)

        reaped |> should equal 1

        let! after = store.GetSession(tenant, created.Id, CancellationToken.None)
        (require after).State |> should equal SessionState.Closed
    }

// ──────────────────────────────────────────────────────────────────────────
// Startup gate

/// Builds a container with every required registration plus the given
/// blob store (or none), expiry set to 30 minutes unless disabled.
let private buildValidationProvider (blobStore: IBlobStore | null) (expiry: Nullable<TimeSpan>) =
    let services = ServiceCollection()
    let clock = TestClock()
    let database = InMemoryDatabase(clock)

    services.AddSingleton<ISessionStore>(InMemoryStoreFactory.sessionStore database)
    |> ignore

    let workspaceRoot =
        System.IO.Path.Combine(System.IO.Path.GetTempPath(), sprintf "legate-expiry-%s" (Ulid.NewUlid().ToString()))

    let runtime =
        Legate.Workspace.Process.ProcessWorkspaceRuntime(
            Legate.Workspace.Process.ProcessWorkspaceRuntimeOptions(Root = workspaceRoot),
            null,
            null
        )

    services.AddSingleton<IWorkspaceRuntime>(runtime) |> ignore
    services.AddSingleton<ILlmProvider>(StubProvider()) |> ignore

    if not (isNull (box blobStore)) then
        match blobStore with
        | null -> ()
        | present -> services.AddSingleton<IBlobStore>(present) |> ignore

    services.Configure<LegateOptions>(Action<LegateOptions>(fun target -> target.Sessions.Expiry <- expiry))
    |> ignore

    LegateStartupChecks.register services
    services.BuildServiceProvider()

let private startValidation (provider: IServiceProvider) : Task =
    task {
        let validation =
            provider.GetServices<IHostedService>()
            |> Seq.find (fun service -> service :? LegateStartupValidation)

        do! validation.StartAsync(CancellationToken.None)
    }

[<Fact>]
let ``Startup fails with a single message on expiry without a blob store`` () =
    task {
        use provider = buildValidationProvider null (Nullable(TimeSpan.FromMinutes 30.0))

        try
            do! startValidation provider
            Assert.Fail("Startup should have failed without a blob store.")
        with :? InvalidOperationException as failed ->
            failed.Message.Contains("Sessions:Expiry") |> should equal true
            failed.Message.Contains("durable blob store") |> should equal true
    }

[<Fact>]
let ``Startup fails on expiry with the InMemory blob store`` () =
    task {
        let clock = TestClock()

        let durable =
            buildValidationProvider
                (InMemoryStoreFactory.blobStore (InMemoryDatabase(clock)))
                (Nullable(TimeSpan.FromMinutes 30.0))

        use provider = durable

        try
            do! startValidation provider
            Assert.Fail("Startup should have failed with the InMemory blob store.")
        with :? InvalidOperationException as failed ->
            failed.Message.Contains("Sessions:Expiry") |> should equal true
            failed.Message.Contains("InMemory") |> should equal true
    }

[<Fact>]
let ``Startup passes on expiry with a durable blob store`` () =
    task {
        let root =
            System.IO.Path.Combine(System.IO.Path.GetTempPath(), sprintf "legate-blobs-%s" (Ulid.NewUlid().ToString()))

        let durableStore =
            FileSystemBlobStore(FileSystemStorageOptions(RootDirectory = root)) :> IBlobStore

        use provider =
            buildValidationProvider durableStore (Nullable(TimeSpan.FromMinutes 30.0))

        do! startValidation provider
    }

[<Fact>]
let ``Startup passes with expiry disabled and no blob store`` () =
    task {
        use provider = buildValidationProvider null (Nullable<TimeSpan>())

        do! startValidation provider
    }
