// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SqliteSecondProcessTests

open System
open System.Threading
open FsUnit.Xunit
open Legate
open Legate.Storage.Sqlite
open Legate.Testing
open Microsoft.Data.Sqlite
open Xunit

// The single-process guard: a second opener of the same file receives the
// typed locked error (never a raw SqliteException), the first database keeps
// working, and non-lock SQLite failures surface as the typed storage error
// through the same boundary. Two database instances in the test stand in for
// two processes: the guard is the lock sidecar, not shared memory.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// <summary>
/// Creates and stores one session through the session store.
/// </summary>
let private createSession (store: ISessionStore) (tenant: TenantId) : Session =
    let session =
        {
            Id = SessionId.New()
            Tenant = tenant
            AgentId = AgentId.New()
            Title = "second-process"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = System.DateTimeOffset.MinValue
            UpdatedAt = System.DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
            PermissionGrants = ResizeArray<string>() :> System.Collections.Generic.IReadOnlyList<string>
        }

    store.CreateSession(tenant, session, CancellationToken.None).GetAwaiter().GetResult()

[<Fact>]
let ``A second opener of the same file receives the typed locked error`` () =
    let clock = TestClock()
    let database, path = SqliteTestFixture.openTestDatabase clock

    use first = database

    try
        // The first database works before the second opener arrives.
        let tenant = TenantId.Create "second-process"
        let store = SqliteStoreFactory.sessionStore first
        let created = createSession store tenant

        // The second opener fails with the typed locked error: never a raw
        // SQLite or IO error, and the message names the path.
        let locked =
            Assert.Throws<SqliteLockedException>(fun () -> SqliteDatabase.Open(path, TestClock()) |> ignore)

        locked.Path |> should equal path

        // The first database is unaffected: its rows still read back.
        let reread: Session | null =
            store.GetSession(tenant, created.Id, CancellationToken.None).GetAwaiter().GetResult()

        match box reread with
        | null -> failwith "expected the session to read back"
        | _ ->
            let found: Session = reread |> box |> unbox
            found.Id |> should equal created.Id
    finally
        SqliteTestFixture.deleteDatabaseFiles path

[<Fact>]
let ``A disposed database releases the file for the next opener`` () =
    let path = SqliteTestFixture.tempDatabasePath ()

    try
        let first = SqliteDatabase.Open(path, TestClock())
        (first :> System.IDisposable).Dispose()

        // The guard is released: opening the same file works again and the
        // earlier rows are still there.
        use second = SqliteDatabase.Open(path, TestClock())
        let store = SqliteStoreFactory.sessionStore second
        let tenant = TenantId.Create "reopen"
        let created = createSession store tenant

        let reread: Session | null =
            store.GetSession(tenant, created.Id, CancellationToken.None).GetAwaiter().GetResult()

        match box reread with
        | null -> failwith "expected the session to read back"
        | _ -> ()
    finally
        SqliteTestFixture.deleteDatabaseFiles path

[<Fact>]
let ``A non-lock SQLite failure surfaces as the typed storage error`` () =
    let path = SqliteTestFixture.tempDatabasePath ()

    try
        // Fault injection without mocks: open with a table prefix whose
        // tables were never migrated, so the first write fails inside
        // SQLite (no such table) and the boundary maps it to the typed
        // storage error.
        let options = SqliteStorageOptions()
        options.Path <- path
        options.TablePrefix <- "missing_"
        options.RunMigrations <- false

        use database = SqliteDatabase.Open(options, TestClock())
        let store = SqliteStoreFactory.sessionStore database
        let tenant = TenantId.Create "fault"

        let failure =
            Assert.Throws<SqliteStorageException>(fun () -> createSession store tenant |> ignore)

        failure.Path |> should equal path

        // The boundary never leaks the raw provider error.
        failure |> should be (ofExactType<SqliteStorageException>)
    finally
        SqliteTestFixture.deleteDatabaseFiles path

[<Fact>]
let ``The typed errors derive LegateException`` () =
    let locked = SqliteLockedException("path", "locked")
    let storage = SqliteStorageException("path", "failed")
    locked |> should be (ofExactType<SqliteLockedException>)
    storage |> should be (ofExactType<SqliteStorageException>)
    (locked :> exn) |> should be (ofExactType<SqliteLockedException>)
    (storage :> LegateException) |> should not' (equal null)
