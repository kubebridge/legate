// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.WorkspaceRuntimeTests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Xunit

let nullString = Unchecked.defaultof<string>

let nullOptions = Unchecked.defaultof<WorkspaceOptions>

let session () =
    {
        Id = SessionId.New()
        Tenant = TenantId.Default
        AgentId = AgentId.New()
        Title = "checkout"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
        UpdatedAt = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = nullString
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// A workspace fake over one dictionary, proving IWorkspace is implementable
/// from outside the assembly with only BCL types (the C#-friendly shape) and
/// pinning the documented confined-ops semantics through behavior.
type FakeWorkspace(root: WorkspaceRoot) =
    let files = Dictionary<string, byte[]>()
    let lockObj = obj ()
    let mutable disposed = 0

    /// Validates a workspace-relative path against the documented rules:
    /// no rooted paths or drive prefixes, no leading or trailing slash, no
    /// '.' or '..' segments, no backslashes, no NUL, non-empty.
    static member ValidatePath(path: string) =
        if isNull (box path) then
            raise (ArgumentNullException(nameof path))

        if path = "" then
            raise (WorkspaceException(null |> box |> unbox, "A workspace path must not be empty."))

        if path.Length >= 2 && path[1] = ':' then
            raise (WorkspaceException(path, "A workspace path must not be a rooted path or carry a drive prefix."))

        if path.StartsWith('/') || path.StartsWith('\\') then
            raise (WorkspaceException(path, "A workspace path must be relative, without a leading slash."))

        let segments = path.Split('/')

        for segment in segments do
            if segment = "" then
                raise (WorkspaceException(path, "A workspace path must not contain empty segments."))

            if segment = "." || segment = ".." then
                raise (WorkspaceException(path, "A workspace path must not contain '.' or '..' segments."))

            if segment.Contains('\\') || segment.Contains('\u0000') then
                raise (WorkspaceException(path, "A workspace path must not contain backslashes or NUL characters."))

    member _.DisposeCount = disposed

    interface IWorkspace with
        member _.Root = root

        member _.Exec(_command, _timeout, _env, _cancellationToken) =
            Task.FromResult(WorkspaceExecResult(0, "", "", false))

        member _.Exists(path, _) =
            FakeWorkspace.ValidatePath path
            lock lockObj (fun () -> Task.FromResult(files.ContainsKey path))

        member _.ReadFile(path, _) =
            FakeWorkspace.ValidatePath path

            lock lockObj (fun () ->
                match files.TryGetValue path with
                | true, bytes -> Task.FromResult(new MemoryStream(bytes) :> Stream)
                | false, _ -> raise (FileNotFoundException("No file exists at this workspace path.", path)))

        member _.WriteFile(path, content, _) =
            FakeWorkspace.ValidatePath path

            lock lockObj (fun () ->
                files[path] <- content
                Task.CompletedTask)

        member _.DeleteFile(path, _) =
            FakeWorkspace.ValidatePath path
            lock lockObj (fun () -> Task.FromResult(files.Remove path))

        member _.DisposeAsync() =
            disposed <- disposed + 1
            ValueTask.CompletedTask

/// A runtime fake handing out one shared fake workspace per session,
/// proving IWorkspaceRuntime is implementable from outside the assembly and
/// pinning the documented bind semantics through behavior.
type FakeRuntime() =
    let bound = Dictionary<SessionId, FakeWorkspace>()
    let lockObj = obj ()

    interface IWorkspaceRuntime with
        member this.Bind(session, options, _) =
            if isNull (box session) then
                raise (ArgumentNullException(nameof session))

            lock lockObj (fun () ->
                match bound.TryGetValue session.Id with
                | true, workspace ->
                    // Stable binding: the same session re-binds over the
                    // same workspace state.
                    Task.FromResult(workspace :> IWorkspace)
                | false, _ ->
                    // The session's WorkspaceBinding is honoured when
                    // present: the runtime key derives from it, not from a
                    // fresh mint.
                    let key =
                        match session.WorkspaceBinding with
                        | binding when not (isNull (box binding)) -> binding
                        | _ -> session.Id.Value

                    // Honouring the binding means binding over the
                    // workspace the binding names, so the fake's root path
                    // is the binding itself.
                    let root = WorkspaceRoot("fake", key)
                    let workspace = FakeWorkspace(root)
                    bound[session.Id] <- workspace
                    ignore options
                    Task.FromResult(workspace :> IWorkspace))

        member _.CheckReadiness(_) =
            Task.FromResult(WorkspaceReadiness(true, nullString))

// ───────────────────────────────────────────────────────────────────────────
// WorkspaceOptions

[<Fact>]
let ``WorkspaceOptions defaults to an empty idle teardown`` () =
    let options = WorkspaceOptions()

    options.IdleTeardownAfter.HasValue |> should equal false

[<Fact>]
let ``WorkspaceOptions accepts a set idle teardown`` () =
    let options =
        WorkspaceOptions(IdleTeardownAfter = Nullable(TimeSpan.FromMinutes 10.))

    options.IdleTeardownAfter |> should equal (Nullable(TimeSpan.FromMinutes 10.))

// ───────────────────────────────────────────────────────────────────────────
// WorkspaceRoot null guards

[<Fact>]
let ``WorkspaceRoot rejects a null runtime id`` () =
    (fun () -> WorkspaceRoot(null |> box |> unbox, "C:/tmp/ws") |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``WorkspaceRoot rejects a blank runtime id`` () =
    (fun () -> WorkspaceRoot("   ", nullString) |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``WorkspaceRoot exposes the runtime id and nullable path`` () =
    let withPath = WorkspaceRoot("docker", "/workspaces/abc")
    let withoutPath = WorkspaceRoot("fake", nullString)

    withPath.RuntimeId |> should equal "docker"
    withPath.Path |> should equal "/workspaces/abc"
    withoutPath.Path |> should equal null

// ───────────────────────────────────────────────────────────────────────────
// WorkspaceReadiness shape

[<Fact>]
let ``WorkspaceReadiness ready has a null reason`` () =
    let ready = WorkspaceReadiness(true, nullString)

    ready.IsReady |> should equal true
    ready.Reason |> should equal null

[<Fact>]
let ``WorkspaceReadiness unready carries the cause`` () =
    let unready = WorkspaceReadiness(false, "The Docker daemon is unreachable.")

    unready.IsReady |> should equal false
    unready.Reason |> should equal "The Docker daemon is unreachable."

// ───────────────────────────────────────────────────────────────────────────
// WorkspaceExecResult guards and shape

[<Fact>]
let ``WorkspaceExecResult carries all four members`` () =
    let result = WorkspaceExecResult(3, "out", "err", true)

    result.ExitCode |> should equal 3
    result.StandardOutput |> should equal "out"
    result.StandardError |> should equal "err"
    result.TimedOut |> should equal true

[<Fact>]
let ``WorkspaceExecResult rejects null stdout`` () =
    (fun () -> WorkspaceExecResult(0, null |> box |> unbox, "", false) |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``WorkspaceExecResult rejects null stderr`` () =
    (fun () -> WorkspaceExecResult(0, "", null |> box |> unbox, false) |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``WorkspaceExecResult accepts empty streams`` () =
    let result = WorkspaceExecResult(0, "", "", false)

    result.StandardOutput |> should equal ""
    result.StandardError |> should equal ""
    result.TimedOut |> should equal false

// ───────────────────────────────────────────────────────────────────────────
// IWorkspace confined file ops: the fake pins the path rules

[<Fact>]
let ``Exists rejects empty, rooted, and traversal paths`` () =
    let workspace = FakeWorkspace(WorkspaceRoot("fake", "/ws")) :> IWorkspace

    (fun () -> workspace.Exists("", CancellationToken.None).Wait())
    |> should throw typeof<WorkspaceException>

    (fun () -> workspace.Exists("/etc/passwd", CancellationToken.None).Wait())
    |> should throw typeof<WorkspaceException>

    (fun () -> workspace.Exists("a/../b", CancellationToken.None).Wait())
    |> should throw typeof<WorkspaceException>

    (fun () -> workspace.Exists("a/b", CancellationToken.None).Wait()) |> ignore

    (fun () -> workspace.Exists("a\\b", CancellationToken.None).Wait())
    |> should throw typeof<WorkspaceException>

    (fun () -> workspace.Exists("a\u0000b", CancellationToken.None).Wait())
    |> should throw typeof<WorkspaceException>

[<Fact>]
let ``Write, read, exists, and delete round-trip inside the root`` () =
    let workspace = FakeWorkspace(WorkspaceRoot("fake", "C:/tmp/ws")) :> IWorkspace

    task {
        do! workspace.WriteFile("out/result.txt", Encoding.UTF8.GetBytes "hello", CancellationToken.None)

        let! exists = workspace.Exists("out/result.txt", CancellationToken.None)
        exists |> should equal true

        use! stream = workspace.ReadFile("out/result.txt", CancellationToken.None)
        use reader = new StreamReader(stream)
        (reader.ReadToEnd()) |> should equal "hello"

        let! deleted = workspace.DeleteFile("out/result.txt", CancellationToken.None)
        deleted |> should equal true

        let! missing = workspace.Exists("out/result.txt", CancellationToken.None)
        missing |> should equal false
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ReadFile throws FileNotFoundException when the file is absent`` () =
    let workspace = FakeWorkspace(WorkspaceRoot("fake", "C:/tmp/ws")) :> IWorkspace

    (fun () -> workspace.ReadFile("out/nope.txt", CancellationToken.None).Wait())
    |> should throw typeof<FileNotFoundException>

[<Fact>]
let ``DeleteFile returns false when the file did not exist`` () =
    let workspace = FakeWorkspace(WorkspaceRoot("fake", "C:/tmp/ws")) :> IWorkspace

    task {
        let! deleted = workspace.DeleteFile("out/never.txt", CancellationToken.None)
        deleted |> should equal false
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Exec returns the execution result shape`` () =
    let workspace = FakeWorkspace(WorkspaceRoot("fake", "C:/tmp/ws")) :> IWorkspace

    task {
        let! result =
            workspace.Exec("dotnet --version", Nullable<TimeSpan>(), null |> box |> unbox, CancellationToken.None)

        result.ExitCode |> should equal 0
        result.StandardOutput |> should equal ""
        result.StandardError |> should equal ""
        result.TimedOut |> should equal false
    }
    |> (fun t -> t.Wait())

// ───────────────────────────────────────────────────────────────────────────
// IWorkspaceRuntime: bind semantics pinned through the fake

[<Fact>]
let ``Bind returns a workspace and binds stably per session`` () =
    let runtime = FakeRuntime() :> IWorkspaceRuntime

    task {
        let session = session ()
        let! first = runtime.Bind(session, nullOptions, CancellationToken.None)
        let! second = runtime.Bind(session, nullOptions, CancellationToken.None)

        first |> should not' (equal null)
        first.Root.RuntimeId |> should equal "fake"
        first |> should equal second
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Bind honours the session workspace binding`` () =
    let runtime = FakeRuntime() :> IWorkspaceRuntime

    task {
        let withBinding =
            { session () with
                WorkspaceBinding = "ws://acme/checkout"
            }

        let! bound = runtime.Bind(withBinding, nullOptions, CancellationToken.None)

        bound.Root.Path |> should equal "ws://acme/checkout"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``CheckReadiness reports the readiness snapshot`` () =
    let runtime = FakeRuntime() :> IWorkspaceRuntime

    task {
        let! readiness = runtime.CheckReadiness(CancellationToken.None)

        readiness.IsReady |> should equal true
        readiness.Reason |> should equal null
    }
    |> (fun t -> t.Wait())
