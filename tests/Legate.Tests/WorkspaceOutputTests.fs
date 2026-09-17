// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.WorkspaceOutputTests

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

// Per-attempt workspace output persistence (issue 115): a live claim
// persists every output/ file under attempts/{n}/output/{rel} keys, while a
// lost, unknown, or missing claim writes zero blobs. Multi-attempt outputs
// stay isolated per attempt, and invoking persist before settlement lands
// (proven at helper level with a live claim, then settled). The process
// runtime is real (files on disk); time advances through TestClock: never
// sleeps.

let tenant = TenantId.Create "acme"

let private startInstant = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

let private lease = TimeSpan.FromSeconds 120.0

/// Fresh clock-backed session and blob stores; every fact owns its database.
let private createStores () =
    let clock = TestClock(startInstant)
    let database = InMemoryDatabase(clock)
    let sessions = InMemorySessionStore(database) :> ISessionStore
    let blobs = InMemoryStoreFactory.blobStore database
    (clock, sessions, blobs)

/// A minimal Idle session row, mirroring the claim fence suite sample.
let private sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "output-persist"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// A process runtime over a fresh temp root.
let private createRuntime () =
    let root =
        Path.Combine(Path.GetTempPath(), sprintf "legate-output-%s" (Ulid.NewUlid().ToString()))

    ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = root), null, null)

let private utf8 (text: string) = Encoding.UTF8.GetBytes text

/// Claims the next turn, failing the test unless the store grants it.
let private claimTurn (store: ISessionStore) (sessionId: SessionId) (owner: string) : TurnClaim =
    match
        store.ClaimNextTurn(tenant, sessionId, owner, lease, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnLeaseRenewed as renewed -> renewed.Claim
    | state -> failwith $"Expected a granted claim, observed %s{state.GetType().Name}."

/// Binds a workspace for a claimed session: creates the session, queues one
/// message, claims the turn, and binds over the process runtime.
let private bindClaimed () =
    task {
        let _, sessions, blobs = createStores ()
        let! session = sessions.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let payload = UserMessagePayload(UserMessage.Text("go")) :> InboxPayload

        let! _ = sessions.AppendInboxMessage(tenant, session.Id, payload, DeliveryMode.Queue, CancellationToken.None)

        let claim = claimTurn sessions session.Id "owner-a"
        let runtime = createRuntime ()
        let! workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)
        return sessions, blobs, session, claim, workspace
    }

/// Collects one blob listing into a list.
let private listAllAsync (blobs: IBlobStore) (prefix: string) =
    task {
        let found = ResizeArray<string>()

        use enumerator =
            (blobs.List(prefix, CancellationToken.None)).GetAsyncEnumerator(CancellationToken.None)

        let mutable go = true

        while go do
            let! has = enumerator.MoveNextAsync().AsTask()

            if has then found.Add enumerator.Current else go <- false

        return found |> List.ofSeq
    }

let private artifactPrefix (sessionId: SessionId) =
    (BlobKeys.ArtifactPrefix(tenant, sessionId)).TrimEnd('/')

let private attemptKey (sessionId: SessionId) (attempt: int) (rel: string) =
    BlobKeys.ForArtifact(tenant, sessionId, BlobKeys.AttemptOutputName(attempt, rel))

let private getAsync (blobs: IBlobStore) (key: string) = blobs.Get(key, CancellationToken.None)

/// Reads a workspace file fully.
let private readAllAsync (workspace: IWorkspace) (path: string) =
    task {
        use! stream = workspace.ReadFile(path, CancellationToken.None)
        use memory = new MemoryStream()
        do! stream.CopyToAsync(memory, CancellationToken.None)
        return memory.ToArray()
    }

// ──────────────────────────────────────────────────────────────────────────
// Live claims persist

[<Fact>]
let ``Live claim persists every output file under the attempt key`` () =
    task {
        let! sessions, blobs, session, claim, workspace = bindClaimed ()

        do! workspace.WriteFile("output/report.txt", utf8 "result: ok", CancellationToken.None)
        do! workspace.WriteFile("output/sub/nested.txt", utf8 "nested", CancellationToken.None)
        do! workspace.WriteFile("input/keep.csv", utf8 "a,b", CancellationToken.None)
        do! workspace.WriteFile("scratch/tmp.txt", utf8 "tmp", CancellationToken.None)

        let! persisted =
            WorkspaceOutput.persistAsync sessions blobs tenant session.Id claim 1 workspace CancellationToken.None

        persisted |> should equal 2

        let! report = getAsync blobs (attemptKey session.Id 1 "report.txt")
        report.SequenceEqual(utf8 "result: ok") |> should equal true

        let! nested = getAsync blobs (attemptKey session.Id 1 "sub/nested.txt")
        nested.SequenceEqual(utf8 "nested") |> should equal true

        // Input and scratch never persist: only output/ feeds the artifact scope.
        let! input = getAsync blobs (attemptKey session.Id 1 "input/keep.csv")
        Assert.Null(input)

        let! scratch = getAsync blobs (attemptKey session.Id 1 "scratch/tmp.txt")
        Assert.Null(scratch)

        let! legacy = getAsync blobs (BlobKeys.ForArtifact(tenant, session.Id, "report.txt"))
        Assert.Null(legacy)
    }

[<Fact>]
let ``Missing output directory persists nothing`` () =
    task {
        let! sessions, blobs, session, claim, workspace = bindClaimed ()

        let! persisted =
            WorkspaceOutput.persistAsync sessions blobs tenant session.Id claim 1 workspace CancellationToken.None

        persisted |> should equal 0

        let! keys = listAllAsync blobs (artifactPrefix session.Id)
        keys |> should be Empty
    }

// ──────────────────────────────────────────────────────────────────────────
// Stale attempts write zero blobs

[<Fact>]
let ``Expired claim persists nothing`` () =
    task {
        let clock, sessions, blobs = createStores ()
        let! session = sessions.CreateSession(tenant, sampleSession (), CancellationToken.None)

        let payload = UserMessagePayload(UserMessage.Text("go")) :> InboxPayload

        let! _ = sessions.AppendInboxMessage(tenant, session.Id, payload, DeliveryMode.Queue, CancellationToken.None)

        let claim = claimTurn sessions session.Id "owner-a"
        let runtime = createRuntime ()
        let! workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)
        do! workspace.WriteFile("output/report.txt", utf8 "result: ok", CancellationToken.None)

        clock.Advance(TimeSpan.FromSeconds 121.0)

        let! persisted =
            WorkspaceOutput.persistAsync sessions blobs tenant session.Id claim 1 workspace CancellationToken.None

        persisted |> should equal 0

        let! keys = listAllAsync blobs (artifactPrefix session.Id)
        keys |> should be Empty
    }

[<Fact>]
let ``Unknown claim persists nothing`` () =
    task {
        let! sessions, blobs, session, _, workspace = bindClaimed ()
        do! workspace.WriteFile("output/report.txt", utf8 "result: ok", CancellationToken.None)

        // The session exists in the store, but this claim was never minted,
        // so verification reads missing.
        let missing =
            {
                TurnId = TurnId.New()
                Token = "never-minted"
                Owner = "owner-a"
                ExpiresAt = startInstant.Add(lease)
                Attempt = 1
            }

        let! persisted =
            WorkspaceOutput.persistAsync sessions blobs tenant session.Id missing 1 workspace CancellationToken.None

        persisted |> should equal 0

        let! keys = listAllAsync blobs (artifactPrefix session.Id)
        keys |> should be Empty
    }

[<Fact>]
let ``Null claim persists nothing`` () =
    task {
        let! sessions, blobs, session, _, workspace = bindClaimed ()
        do! workspace.WriteFile("output/report.txt", utf8 "result: ok", CancellationToken.None)

        let! persisted =
            WorkspaceOutput.persistAsync
                sessions
                blobs
                tenant
                session.Id
                Unchecked.defaultof<TurnClaim>
                1
                workspace
                CancellationToken.None

        persisted |> should equal 0

        let! keys = listAllAsync blobs (artifactPrefix session.Id)
        keys |> should be Empty
    }

// ──────────────────────────────────────────────────────────────────────────
// Attempts and settlement

[<Fact>]
let ``Per-attempt outputs stay isolated`` () =
    task {
        let! sessions, blobs, session, claim, workspace = bindClaimed ()

        do! workspace.WriteFile("output/report.txt", utf8 "v1", CancellationToken.None)
        do! workspace.WriteFile("output/old.txt", utf8 "old", CancellationToken.None)

        let! first =
            WorkspaceOutput.persistAsync sessions blobs tenant session.Id claim 1 workspace CancellationToken.None

        first |> should equal 2

        let! _ = workspace.DeleteFile("output/old.txt", CancellationToken.None)
        do! workspace.WriteFile("output/report.txt", utf8 "v2", CancellationToken.None)
        do! workspace.WriteFile("output/new.txt", utf8 "new", CancellationToken.None)

        let! second =
            WorkspaceOutput.persistAsync sessions blobs tenant session.Id claim 2 workspace CancellationToken.None

        second |> should equal 2

        let! firstReport = getAsync blobs (attemptKey session.Id 1 "report.txt")
        firstReport.SequenceEqual(utf8 "v1") |> should equal true

        let! secondReport = getAsync blobs (attemptKey session.Id 2 "report.txt")
        secondReport.SequenceEqual(utf8 "v2") |> should equal true

        // Attempt 1 keeps what attempt 2 no longer carries, and attempt 2
        // carries what attempt 1 never saw.
        let! firstOld = getAsync blobs (attemptKey session.Id 1 "old.txt")
        firstOld.SequenceEqual(utf8 "old") |> should equal true

        let! secondOld = getAsync blobs (attemptKey session.Id 2 "old.txt")
        Assert.Null(secondOld)

        let! secondNew = getAsync blobs (attemptKey session.Id 2 "new.txt")
        secondNew.SequenceEqual(utf8 "new") |> should equal true
    }

[<Fact>]
let ``Pre-settle persist lands before settlement`` () =
    task {
        let! sessions, blobs, session, claim, workspace = bindClaimed ()
        do! workspace.WriteFile("output/report.txt", utf8 "result: ok", CancellationToken.None)

        // The helper runs while the turn is still live, before any settle.
        let! persisted =
            WorkspaceOutput.persistAsync
                sessions
                blobs
                tenant
                session.Id
                claim
                claim.Attempt
                workspace
                CancellationToken.None

        persisted |> should equal 1

        let! _ = sessions.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)

        let! report = getAsync blobs (attemptKey session.Id claim.Attempt "report.txt")
        report.SequenceEqual(utf8 "result: ok") |> should equal true
    }

[<Fact>]
let ``Persist then restore round-trips through a rebound workspace`` () =
    task {
        let! sessions, blobs, session, claim, workspace = bindClaimed ()
        let inputBytes = utf8 "order-id,amount\n1,42\n"
        let outputBytes = utf8 "result: ok"

        do! workspace.WriteFile("output/report.txt", outputBytes, CancellationToken.None)

        let! _ =
            blobs.Put(
                BlobKeys.ForSession(tenant, session.Id, "input/data.csv"),
                BlobContent(inputBytes, "text/csv"),
                CancellationToken.None
            )

        let! persisted =
            WorkspaceOutput.persistAsync sessions blobs tenant session.Id claim 1 workspace CancellationToken.None

        persisted |> should equal 1

        // The vehicle is rebound: outputs are gone from the workspace while
        // the blobs hold them.
        let! _ = workspace.DeleteFile("output/report.txt", CancellationToken.None)

        let! inputCount, outputCount =
            WorkspaceRebind.restoreAsync blobs tenant session.Id workspace CancellationToken.None

        inputCount |> should equal 1
        outputCount |> should equal 1

        let! restoredInput = readAllAsync workspace "input/data.csv"
        restoredInput.SequenceEqual(inputBytes) |> should equal true

        let! restoredOutput = readAllAsync workspace "output/report.txt"
        restoredOutput.SequenceEqual(outputBytes) |> should equal true
    }
