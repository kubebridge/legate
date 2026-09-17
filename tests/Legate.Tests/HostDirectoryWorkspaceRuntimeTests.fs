// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open Legate
open Legate.Workspace.HostDirectory
open Xunit

module HostDirectoryWorkspaceRuntimeTests =

    let private freshRoot () =
        let root =
            Path.Combine(Path.GetTempPath(), "legate-hdrt-tests", Ulid.NewUlid().ToString())

        Directory.CreateDirectory root |> ignore
        root

    let private optionsFor root =
        HostDirectoryWorkspaceRuntimeOptions(Root = root)

    let private sessionFor () =
        {
            Id = SessionId.New()
            Tenant = TenantId.Create "host-directory-runtime-tests"
            AgentId = AgentId.New()
            Title = "host directory runtime"
            State = SessionState.Idle
            CurrentTurnId = Nullable()
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Nullable()
            WorkspaceBinding = null
            Options = SessionOptions()
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    let private rootOf (workspace: IWorkspace) =
        match workspace.Root.Path with
        | null -> failwith "the host directory runtime exposes its root path"
        | path -> path

    [<Fact>]
    let ``Bind binds the configured root itself`` () =
        task {
            let root = freshRoot ()
            let runtime = HostDirectoryWorkspaceRuntime(optionsFor root, null, null)

            let! workspace = (runtime :> IWorkspaceRuntime).Bind(sessionFor (), null, CancellationToken.None)

            Assert.Equal("workspace.host-directory", workspace.Root.RuntimeId)
            Assert.Equal(Path.GetFullPath root, rootOf workspace)
        }

    [<Fact>]
    let ``AllowOutsideRoot defaults to false`` () =
        Assert.False(HostDirectoryWorkspaceRuntimeOptions(Root = Path.GetTempPath()).AllowOutsideRoot)

    [<Fact>]
    let ``Bind honours a nested binding as a confined sub-path`` () =
        task {
            let root = freshRoot ()
            let runtime = HostDirectoryWorkspaceRuntime(optionsFor root, null, null)

            let bound =
                { sessionFor () with
                    WorkspaceBinding = "sessions/alpha"
                }

            let! workspace = (runtime :> IWorkspaceRuntime).Bind(bound, null, CancellationToken.None)

            let directory = rootOf workspace
            Assert.Equal(Path.GetFullPath(Path.Combine(root, "sessions", "alpha")), directory)
            Assert.True(Directory.Exists directory)

            // State written through the binding survives a re-bind.
            do! workspace.WriteFile("state.txt", Text.Encoding.UTF8.GetBytes "survives", CancellationToken.None)

            do! workspace.DisposeAsync()

            let! rebound = (runtime :> IWorkspaceRuntime).Bind(bound, null, CancellationToken.None)

            let! exists = rebound.Exists("state.txt", CancellationToken.None)
            Assert.True(exists)
        }

    [<Fact>]
    let ``A binding that escapes the root throws and binds nothing`` () =
        task {
            let root = freshRoot ()

            let runtime =
                HostDirectoryWorkspaceRuntime(optionsFor root, null, null) :> IWorkspaceRuntime

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
    let ``A binding through an escaping symlink throws`` () =
        task {
            let root = freshRoot ()

            let outside =
                Path.Combine(Path.GetTempPath(), "legate-hdrt-outside", Ulid.NewUlid().ToString())

            Directory.CreateDirectory outside |> ignore

            Directory.CreateSymbolicLink(Path.Combine(root, "escape-link"), outside)
            |> ignore

            let runtime =
                HostDirectoryWorkspaceRuntime(optionsFor root, null, null) :> IWorkspaceRuntime

            let linked =
                { sessionFor () with
                    WorkspaceBinding = "escape-link"
                }

            Assert.Throws<WorkspaceException>(fun () ->
                runtime.Bind(linked, null, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

    [<Fact>]
    let ``Bind rejects a null session`` () =
        task {
            let runtime =
                HostDirectoryWorkspaceRuntime(optionsFor (freshRoot ()), null, null) :> IWorkspaceRuntime

            Assert.Throws<ArgumentNullException>(fun () ->
                runtime.Bind(Unchecked.defaultof<Session>, null, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

    [<Fact>]
    let ``Options without a root throw`` () =
        Assert.Throws<ArgumentException>(fun () ->
            HostDirectoryWorkspaceRuntime(HostDirectoryWorkspaceRuntimeOptions(), null, null)
            |> ignore)
        |> ignore

        Assert.Throws<ArgumentNullException>(fun () ->
            HostDirectoryWorkspaceRuntime(Unchecked.defaultof<HostDirectoryWorkspaceRuntimeOptions>, null, null)
            |> ignore)
        |> ignore

    [<Fact>]
    let ``CheckReadiness reports ready on a writable root`` () =
        task {
            let root = freshRoot ()

            let runtime =
                HostDirectoryWorkspaceRuntime(optionsFor root, null, null) :> IWorkspaceRuntime

            let! readiness = runtime.CheckReadiness(CancellationToken.None)

            Assert.True(readiness.IsReady)
            Assert.Null(readiness.Reason)
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
                HostDirectoryWorkspaceRuntime(HostDirectoryWorkspaceRuntimeOptions(Root = blocked), null, null)
                :> IWorkspaceRuntime

            let! readiness = runtime.CheckReadiness(CancellationToken.None)

            Assert.False(readiness.IsReady)
            Assert.NotNull(readiness.Reason)
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
                HostDirectoryWorkspaceRuntime(HostDirectoryWorkspaceRuntimeOptions(Root = blocked), null, null)
                :> IWorkspaceRuntime

            let boundSession =
                { sessionFor () with
                    WorkspaceBinding = "session"
                }

            Assert.Throws<WorkspaceException>(fun () ->
                runtime.Bind(boundSession, null, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

module HostDirectoryWorkspaceRuntimeWarningTests =

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
        let options = HostDirectoryWorkspaceRuntimeOptions(Root = Path.GetTempPath())

        // Construction runs the warning leg outside Development and skips
        // it inside Development; both paths must complete without throwing
        // and the warning leg must invoke the logger.
        let productionLogger = WarningCountingLogger()

        let production =
            HostDirectoryWorkspaceRuntime(options, FakeHostEnvironment("Production"), productionLogger :> ILogger)

        Assert.Equal(1, productionLogger.Warnings)

        let developmentLogger = WarningCountingLogger()

        let development =
            HostDirectoryWorkspaceRuntime(options, FakeHostEnvironment("Development"), developmentLogger :> ILogger)

        Assert.Equal(0, developmentLogger.Warnings)

        Assert.NotNull(box production)
        Assert.NotNull(box development)
