// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.WorkspaceRebindTests

open System
open System.Collections.Generic
open System.IO
open System.Linq
open System.Text
open System.Threading
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Legate.Workspace.Process
open Xunit

// Workspace idle teardown and blob re-bind (issue 110): the clock-seamed
// tracker disposing idle execution vehicles while keeping files, and the
// restore step re-staging input/ from the session blob scope and output/
// from the artifact scope. The process runtime is real (files on disk);
// time advances through TestClock: never sleeps.

let tenant = TenantId.Create "acme"

/// An Idle session row for workspace binding; a null binding binds the
/// derived per-session directory.
let sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "rebind"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// A process runtime over a fresh temp root, plus the root.
let createRuntime () =
    let root =
        Path.Combine(Path.GetTempPath(), sprintf "legate-rebind-%s" (Ulid.NewUlid().ToString()))

    let runtime =
        ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = root), null, null)

    runtime, root

/// A blob store over a fresh database, plus the database clock.
let createBlobs () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    InMemoryStoreFactory.blobStore database, clock

/// Reads a workspace file fully.
let readAllAsync (workspace: IWorkspace) (path: string) =
    task {
        use! stream = workspace.ReadFile(path, CancellationToken.None)
        use memory = new MemoryStream()
        do! stream.CopyToAsync(memory, CancellationToken.None)
        return memory.ToArray()
    }

let utf8 (text: string) = Encoding.UTF8.GetBytes text

// ──────────────────────────────────────────────────────────────────────────
// Teardown

[<Fact>]
let ``Teardown disposes the vehicle and keeps files for re-bind`` () =
    task {
        let runtime, _ = createRuntime ()
        let clock = TestClock()
        let tracker = SessionWorkspaceTracker(clock)
        let session = sampleSession ()
        let idleAfter = TimeSpan.FromMinutes 10.0

        let! workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)
        do! workspace.WriteFile("input/data.csv", utf8 "a,b", CancellationToken.None)
        tracker.Track(tenant, session.Id, workspace)
        tracker.Count |> should equal 1

        clock.Advance(TimeSpan.FromMinutes 11.0)

        let! tornDown = tracker.TeardownIdleAsync(idleAfter, CancellationToken.None)
        tornDown |> should equal 1
        tracker.Count |> should equal 0

        // The vehicle is gone: the old binding refuses work.
        try
            do! workspace.WriteFile("input/more.csv", utf8 "x", CancellationToken.None)
            Assert.Fail("The torn-down workspace should refuse writes.")
        with :? ObjectDisposedException ->
            ()

        // The files survive: re-binding over the same state reads them back.
        let! rebound = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)
        let! bytes = readAllAsync rebound "input/data.csv"
        bytes.SequenceEqual(utf8 "a,b") |> should equal true
    }

[<Fact>]
let ``Touch keeps a workspace alive and non-positive idle tears down nothing`` () =
    task {
        let runtime, _ = createRuntime ()
        let clock = TestClock()
        let tracker = SessionWorkspaceTracker(clock)
        let session = sampleSession ()
        let idleAfter = TimeSpan.FromMinutes 10.0

        let! workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)
        tracker.Track(tenant, session.Id, workspace)

        clock.Advance(TimeSpan.FromMinutes 8.0)
        tracker.Touch(tenant, session.Id)
        clock.Advance(TimeSpan.FromMinutes 8.0)

        // Sixteen minutes since track, eight since touch: still alive.
        let! kept = tracker.TeardownIdleAsync(idleAfter, CancellationToken.None)
        kept |> should equal 0
        tracker.Count |> should equal 1

        // A non-positive bound never sweeps, even with entries present.
        let! none = tracker.TeardownIdleAsync(TimeSpan.Zero, CancellationToken.None)
        none |> should equal 0
        tracker.Count |> should equal 1

        clock.Advance(TimeSpan.FromMinutes 3.0)

        let! tornDown = tracker.TeardownIdleAsync(idleAfter, CancellationToken.None)
        tornDown |> should equal 1
        tracker.Count |> should equal 0
    }

// ──────────────────────────────────────────────────────────────────────────
// Restore

[<Fact>]
let ``Rebind restores input and output bytes from blob scopes`` () =
    task {
        let blobStore, _ = createBlobs ()
        let runtime, _ = createRuntime ()
        let session = sampleSession ()
        let inputBytes = utf8 "order-id,amount\n1,42\n"
        let outputBytes = utf8 "result: ok"

        let! _ =
            blobStore.Put(
                BlobKeys.ForSession(tenant, session.Id, "input/data.csv"),
                BlobContent(inputBytes, "text/csv"),
                CancellationToken.None
            )

        let! _ =
            blobStore.Put(
                BlobKeys.ForSession(tenant, session.Id, "transcript.json"),
                BlobContent(utf8 "{}", "application/json"),
                CancellationToken.None
            )

        let! _ =
            blobStore.Put(
                BlobKeys.ForArtifact(tenant, session.Id, "report.txt"),
                BlobContent(outputBytes, "text/plain"),
                CancellationToken.None
            )

        let! workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

        let! inputCount, outputCount =
            WorkspaceRebind.restoreAsync blobStore tenant session.Id workspace CancellationToken.None

        // Only the input/ blob restores as input: the transcript stays in
        // the blob scope and never lands in the workspace.
        inputCount |> should equal 1
        outputCount |> should equal 1

        let! restoredInput = readAllAsync workspace "input/data.csv"
        restoredInput.SequenceEqual(inputBytes) |> should equal true

        let! restoredOutput = readAllAsync workspace "output/report.txt"
        restoredOutput.SequenceEqual(outputBytes) |> should equal true

        let! transcriptExists = workspace.Exists("input/transcript.json", CancellationToken.None)
        transcriptExists |> should equal false
    }

[<Fact>]
let ``Restore with empty scopes restores nothing`` () =
    task {
        let blobStore, _ = createBlobs ()
        let runtime, _ = createRuntime ()
        let session = sampleSession ()

        let! workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

        let! inputCount, outputCount =
            WorkspaceRebind.restoreAsync blobStore tenant session.Id workspace CancellationToken.None

        inputCount |> should equal 0
        outputCount |> should equal 0
    }
