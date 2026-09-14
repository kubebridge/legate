// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Workspace.Process
open Xunit

module ProcessWorkspaceRuntimeTests =

    let private freshRoot () =
        let root =
            Path.Combine(Path.GetTempPath(), "legate-wsrt-tests", Ulid.NewUlid().ToString())

        Directory.CreateDirectory root |> ignore
        root

    let private optionsFor root =
        ProcessWorkspaceRuntimeOptions(Root = root)

    let private sessionFor () =
        {
            Id = SessionId.New()
            Tenant = TenantId.Create "runtime-tests"
            AgentId = AgentId.New()
            Title = "workspace runtime"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
        }

    [<Fact>]
    let ``Bind creates the input output scratch layout`` () =
        task {
            let root = freshRoot ()
            let runtime = ProcessWorkspaceRuntime(optionsFor root, null, null)

            let session = sessionFor ()

            let! workspace = (runtime :> IWorkspaceRuntime).Bind(session, null, CancellationToken.None)

            let directory =
                (match workspace.Root.Path with
                 | null -> failwith "the process runtime exposes its root path"
                 | path -> path)

            Assert.True(Directory.Exists(Path.Combine(directory, "input")))
            Assert.True(Directory.Exists(Path.Combine(directory, "output")))
            Assert.True(Directory.Exists(Path.Combine(directory, "scratch")))
        }

    [<Fact>]
    let ``Re-binding honours the session's WorkspaceBinding`` () =
        task {
            let root = freshRoot ()
            let runtime = ProcessWorkspaceRuntime(optionsFor root, null, null)

            let firstSession =
                { sessionFor () with
                    WorkspaceBinding = "rebound-session"
                }

            let! firstWorkspace = (runtime :> IWorkspaceRuntime).Bind(firstSession, null, CancellationToken.None)

            do!
                firstWorkspace.WriteFile(
                    "scratch/state.txt",
                    System.Text.Encoding.UTF8.GetBytes "survives",
                    CancellationToken.None
                )

            do! firstWorkspace.DisposeAsync()

            let! rebound = (runtime :> IWorkspaceRuntime).Bind(firstSession, null, CancellationToken.None)

            let! exists = rebound.Exists("scratch/state.txt", CancellationToken.None)
            Assert.True(exists)
        }

    [<Fact>]
    let ``A binding that escapes the root throws and binds nothing`` () =
        task {
            let root = freshRoot ()

            let runtime =
                ProcessWorkspaceRuntime(optionsFor root, null, null) :> IWorkspaceRuntime

            let escaping =
                { sessionFor () with
                    WorkspaceBinding = "../escape"
                }

            Assert.Throws<WorkspaceException>(fun () ->
                runtime.Bind(escaping, null, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore

            Assert.False(Directory.Exists(Path.Combine(Path.GetFullPath root, "escape")))
        }

    [<Fact>]
    let ``CheckReadiness reports an unready result on an unwritable root`` () =
        task {
            let root = freshRoot ()

            // A FILE occupies the runtime root: the readiness probe cannot
            // create its temp file under it, and the host reports why.
            let blocked = Path.Combine(root, "occupied")
            do! File.WriteAllTextAsync(blocked, "not a directory")

            let runtime =
                ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = blocked), null, null) :> IWorkspaceRuntime

            let! readiness = runtime.CheckReadiness(CancellationToken.None)

            Assert.False(readiness.IsReady)
            Assert.NotNull(readiness.Reason)
        }

    [<Fact>]
    let ``CheckReadiness reports ready on a writable root`` () =
        task {
            let root = freshRoot ()

            let runtime =
                ProcessWorkspaceRuntime(optionsFor root, null, null) :> IWorkspaceRuntime

            let! readiness = runtime.CheckReadiness(CancellationToken.None)

            Assert.True(readiness.IsReady)
            Assert.Null(readiness.Reason)
        }

    [<Fact>]
    let ``Bind throws WorkspaceException on an uncreatable root with no fallback`` () =
        task {
            let root = freshRoot ()

            // A FILE occupies the runtime root: the bind cannot create the
            // session's directory under it, and the failure surfaces
            // instead of falling back.
            let blocked = Path.Combine(root, "blocked")
            do! File.WriteAllTextAsync(blocked, "not a directory")

            let runtime =
                ProcessWorkspaceRuntime(ProcessWorkspaceRuntimeOptions(Root = blocked), null, null) :> IWorkspaceRuntime

            Assert.Throws<WorkspaceException>(fun () ->
                runtime.Bind(sessionFor (), null, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

module ProcessWorkspaceRuntimeWarningTests =

    open Microsoft.Extensions.FileProviders
    open Microsoft.Extensions.Hosting
    open Microsoft.Extensions.Primitives
    open Microsoft.Extensions.Logging

    /// A logger that counts the warnings it received.
    type WarningCountingLogger() =
        let mutable warnings = 0

        member _.Warnings = warnings

        interface ILogger with
            member _.BeginScope<'TState when 'TState: not null>(_: 'TState) : System.IDisposable =
                new NopDisposable() :> System.IDisposable

            member _.IsEnabled(logLevel: LogLevel) : bool = logLevel = LogLevel.Warning

            member _.Log<'TState>
                (
                    logLevel: LogLevel,
                    _eventId: EventId,
                    _state: 'TState,
                    _ex: exn,
                    _formatter: Func<'TState, exn, string>
                ) : unit =
                if logLevel = LogLevel.Warning then
                    warnings <- warnings + 1

    and NopDisposable() =
        interface System.IDisposable with
            member _.Dispose() = ()

    /// An empty file provider the fake environment hands out.
    type NopFileProvider() =
        interface IFileProvider with
            member _.GetFileInfo(_subpath: string) : IFileInfo = Unchecked.defaultof<IFileInfo>

            member _.GetDirectoryContents(_subpath: string) : IDirectoryContents =
                Unchecked.defaultof<IDirectoryContents>

            member _.Watch(_filter: string) : IChangeToken = Unchecked.defaultof<IChangeToken>

    /// A host environment fixture carrying its environment name.
    type FakeHostEnvironment(name: string) =
        interface IHostEnvironment with
            member _.ApplicationName = name

            member _.ApplicationName
                with set (_value: string) = ()

            member _.ContentRootPath = "/"

            member _.ContentRootPath
                with set (_value: string) = ()

            member _.ContentRootFileProvider = NopFileProvider() :> IFileProvider

            member _.ContentRootFileProvider
                with set (_value: IFileProvider) = ()

            member _.EnvironmentName = name

            member _.EnvironmentName
                with set (_value: string) = ()

    [<Fact>]
    let ``The runtime logs the unsafe label outside Development`` () =
        let options = ProcessWorkspaceRuntimeOptions(Root = Path.GetTempPath())

        // Construction runs the warning leg outside Development and skips
        // it inside Development; both paths must complete without throwing
        // and the warning leg must invoke the logger.
        let productionLogger = WarningCountingLogger()

        let production =
            ProcessWorkspaceRuntime(options, FakeHostEnvironment("Production"), productionLogger :> ILogger)

        Assert.Equal(1, productionLogger.Warnings)

        let developmentLogger = WarningCountingLogger()

        let development =
            ProcessWorkspaceRuntime(options, FakeHostEnvironment("Development"), developmentLogger :> ILogger)

        Assert.Equal(0, developmentLogger.Warnings)

        Assert.NotNull(box production)
        Assert.NotNull(box development)
