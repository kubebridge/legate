// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open FluentMigrator.Runner
open Legate
open Legate.Storage.Migrations
open Legate.Storage.Postgres
open Legate.Testing
open Microsoft.Extensions.DependencyInjection
open Testcontainers.PostgreSql
open Xunit

// Shared Testcontainers harness for the PostgreSQL suites: one container
// and one migrated database per test run.
//
// xUnit 2.9 has no dynamic-skip mechanism (verified against the 2.9.3
// runner assemblies: no SkipException handling, inaccessible
// SkipException constructors, no Assert.Skip), so suites that need a
// container are compiled out on Windows (see the fsproj conditions) and
// the Windows CI leg never attempts them. The Linux CI leg runs
// everything for real against Docker. Where Docker is unavailable the
// gate fails loudly with the remedy instead of passing vacuously.
// Undisposed containers are reaped by the Testcontainers resource reaper
// when the test process exits.
module PostgresTestDatabase =

    let private gate = obj ()

    let mutable private cached: string option = None
    let mutable private skipped: string option = None

    /// The pinned server image the suites run against.
    let PostgresImage = "postgres:16-alpine"

    /// Starts the container, migrates the baseline into it, and returns
    /// the connection string every suite shares. Rows never collide
    /// across suites: every test mints fresh session, turn, and agent ids.
    let private startAndMigrate () : string =
        let container =
            PostgreSqlBuilder(PostgresImage).WithDatabase("legate_tests").Build()

        try
            container.StartAsync().GetAwaiter().GetResult() |> ignore

            let connectionString = container.GetConnectionString()
            let services = ServiceCollection()

            // Defaults: schema legate, no table prefix.
            AddLegateMigrations services |> ignore

            services.ConfigureRunner(fun builder ->
                builder
                    .AddPostgres()
                    .WithGlobalConnectionString(connectionString)
                    .ScanIn(typeof<MigrationOptions>.Assembly)
                    .For.Migrations()
                |> ignore)
            |> ignore

            use provider = services.BuildServiceProvider()
            use scope = provider.CreateScope()
            let runner = scope.ServiceProvider.GetRequiredService<IMigrationRunner>()
            runner.MigrateUp()

            connectionString
        with ex ->
            try
                container.StopAsync().GetAwaiter().GetResult() |> ignore
            with _ ->
                ()

            raise (InvalidOperationException($"The Postgres test container failed to start: {ex.Message}", ex))

    /// Fails the calling fact with the reason: xUnit 2.9 cannot report a
    /// dynamic skip, so an unavailable container is a loud failure naming
    /// the remedy, never a vacuous pass.
    let private failGate (reason: string) : 'T =
        raise (InvalidOperationException(reason))

    /// Whether the host deliberately opts into containers on Windows
    /// (LEGATE_POSTGRES_DOCKER=1 with Linux-container Docker): the only
    /// way the suites attempt a container there.
    let private windowsOptIn () =
        match Environment.GetEnvironmentVariable("LEGATE_POSTGRES_DOCKER") with
        | null -> false
        | value ->
            let normalized = value.Trim().ToLowerInvariant()
            normalized = "1" || normalized = "true"

    /// Returns the shared connection string, starting and migrating the
    /// container once. Fails loudly with the remedy where Docker is
    /// unavailable; Windows without the opt-in indicates a
    /// compile-exclusion fault (the Docker suites do not compile there
    /// by default).
    let ensureReady () : string =
        lock gate (fun () ->
            match skipped, cached with
            | Some reason, _ -> failGate reason
            | None, Some connectionString -> connectionString
            | None, None ->
                if RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && not (windowsOptIn ()) then
                    let reason =
                        "Postgres Testcontainers suites do not run on Windows without LEGATE_POSTGRES_DOCKER=1: the Windows CI leg never attempts them."

                    skipped <- Some reason
                    failGate reason
                else
                    try
                        let connectionString = startAndMigrate ()
                        cached <- Some connectionString
                        connectionString
                    with ex ->
                        let reason =
                            $"Postgres Testcontainers suites need a running Docker daemon; start Docker and re-run. Cause: {ex.Message}"

                        skipped <- Some reason
                        failGate reason)

    /// Builds options on the shared database for one suite, with the
    /// suite's own clock and limits.
    let testOptions (connectionString: string) : PostgresOptions =
        let options = PostgresOptions()
        options.ConnectionString <- connectionString
        options

    /// Creates the three stores over the shared database with one
    /// deterministic clock, the shape the conformance suites pin. Stores
    /// return on their contract interfaces: the implementations are
    /// explicit, like the in-memory reference.
    let createStores () : TestClock * ISessionStore * ISessionEventStore * PostgresAgentStore =
        let connectionString = ensureReady ()
        let clock = TestClock()
        let options = testOptions connectionString

        clock,
        PostgresSessionStore(options, clock) :> ISessionStore,
        PostgresSessionEventStore(options, clock) :> ISessionEventStore,
        PostgresAgentStore(options, clock)

    /// A fresh tenant id per test, so suites never share rows even where
    /// truncation does not run.
    let freshTenant (prefix: string) =
        let suffix = Guid.NewGuid().ToString("N")
        TenantId.Create($"{prefix}-{suffix}")

    /// Empties every baseline table after a conformance fact: the suites
    /// reuse fixed tenants and keys across facts (key-1 in the outbox
    /// suites, for instance), assuming the per-test isolation an
    /// in-memory database gives them. The migration VersionInfo stays:
    /// tables persist, only rows go. No foreign keys exist, so no
    /// cascade is needed.
    let truncate () : unit =
        match cached with
        | None -> ()
        | Some connectionString ->
            use connection = new Npgsql.NpgsqlConnection(connectionString)
            connection.Open()

            use command =
                new Npgsql.NpgsqlCommand(
                    "TRUNCATE \"legate\".\"sessions\", \"legate\".\"inbox\", \"legate\".\"turns\", \"legate\".\"events\", \"legate\".\"cleanup_claims\", \"legate\".\"agents\", \"legate\".\"custom_tools\", \"legate\".\"schedule_occurrences\", \"legate\".\"outbox\", \"legate\".\"session_grants\"",
                    connection
                )

            command.ExecuteNonQuery() |> ignore

    /// A minimal session under the tenant, the shape the conformance
    /// suites create.
    let sampleSession (tenant: TenantId) : Session =
        {
            Id = SessionId.New()
            Tenant = tenant
            AgentId = AgentId.New()
            Title = "postgres"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    /// A minimal agent under the tenant.
    let sampleAgent (tenant: TenantId) : Agent =
        {
            Id = AgentId.New()
            Tenant = tenant
            Name = "postgres-agent"
            Description = null
            Model = ModelReference.Parse("test/model-a")
            SystemPrompt = "You are under test."
            EnvironmentVariables = null
            PermissionDefaults = null
            ToolSelection = null
            PackageReference = null
            Enabled = true
            Schedule = null
            RowVersion = 0UL
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
        }

    /// Creates a session and claims its turn, returning the session id and
    /// the live claim, the state every fenced call verifies against.
    let claimedSession (sessions: ISessionStore) (tenant: TenantId) (owner: string) : Task<SessionId * TurnClaim> =
        task {
            let! created = sessions.CreateSession(tenant, sampleSession tenant, CancellationToken.None)

            let message = UserMessagePayload(UserMessage.Text("fenced")) :> InboxPayload

            let! _ =
                sessions.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

            let! claimed =
                sessions.ClaimNextTurn(tenant, created.Id, owner, TimeSpan.FromMinutes 5., CancellationToken.None)

            return (created.Id, (claimed :?> TurnLeaseRenewed).Claim)
        }

    /// One delta event the fencing tests append.
    let delta (sessionId: SessionId) (turnId: TurnId) : SessionEvent =
        TextDeltaEvent(sessionId, turnId, Nullable(), DateTimeOffset.MinValue, "delta") :> SessionEvent
