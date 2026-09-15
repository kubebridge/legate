// SPDX-License-Identifier: Apache-2.0
namespace Legate.Mcp

open System
open System.Collections.Generic
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options

// The MCP tool source: an IToolSource (plus IToolSourceLifecycle for
// process-lifetime client reuse) over configured MCP servers. Servers
// start lazily on the first GetTools and the connected sessions are reused
// for the process lifetime; any start, list, naming, or collision failure
// degrades to zero tools with a logged reason, per the IToolSource
// empty-list policy. Tool names are the sanitized {server}_{tool} pair,
// resolved across servers per the configured collision policy, and every
// projected function carries the title, schemas, and annotation hints,
// with a real destructiveHint == true as boolean true so the merged
// permission policy asks before running it.

/// Connects the configured MCP servers and exposes their tools as
/// <see cref="T:Microsoft.Extensions.AI.AIFunction" />s. Servers start
/// lazily on the first <c>GetTools</c> and sessions are reused for the
/// process lifetime; any failure degrades to zero tools with a logged
/// reason, never an error. Register through
/// <c>LegateBuilder.AddMcp(configuration)</c>, which binds
/// <c>Legate:Tools:Mcp</c>, or construct directly.
[<Sealed>]
type McpToolSource
    private
    (
        options: McpOptions,
        logDegrade: Action<string | null>,
        connector: IMcpServerConnector,
        httpClient: HttpClient | null
    ) =

    do
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(logDegrade)
        ArgumentNullException.ThrowIfNull(connector)

    let gate = new SemaphoreSlim(1, 1)
    let mutable started = false
    let mutable sessions: IMcpServerSession list = []
    let mutable failure: string | null = null
    let mutable stopped = false

    /// Creates the source from bound options with the production SDK
    /// connector. Servers start lazily on first use; degrade reasons go
    /// to the logger.
    /// <param name="options">The bound MCP options. Must not be null.</param>
    /// <param name="logger">The logger carrying degrade reasons. Must not be null.</param>
    new(options: IOptions<McpOptions>, logger: ILogger<McpToolSource>) =
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(logger)
        let owned = new HttpClient()

        let logDegrade =
            Action<string | null>(fun reason ->
                logger.LogWarning("MCP tool source degraded to zero tools: {Reason}.", box reason))

        McpToolSource(
            options.Value,
            logDegrade,
            SdkMcpServerConnector(logger :> ILogger, owned) :> IMcpServerConnector,
            owned
        )

    /// Creates the source over an explicit degrade sink and connector.
    /// Internal: tests capture the sink and script the connector, so no
    /// test spawns a subprocess or opens a socket.
    internal new(options: McpOptions, logDegrade: Action<string | null>, connector: IMcpServerConnector) =
        McpToolSource(options, logDegrade, connector, Unchecked.defaultof<HttpClient>)

    /// The degrade reason, or null while healthy. Internal: tests assert
    /// the logged reason through the capturing logger instead.
    member internal _.Failure: string | null = failure

    /// Connects every configured server once, recording the first failure
    /// and keeping zero sessions from then on.
    /// <param name="cancellationToken">Token that abandons the start.</param>
    /// <returns>A task completing when the source is started.</returns>
    member private this.EnsureStartedAsync(cancellationToken: CancellationToken) : Task =
        task {
            if not started then
                do! gate.WaitAsync(cancellationToken)

                try
                    if not started then
                        match options.Validate() with
                        | null ->
                            let connected = ResizeArray<IMcpServerSession>()
                            let mutable error: string | null = null

                            for server in options.Servers do
                                if isNull (box error) then
                                    try
                                        let! session = connector.ConnectAsync(server, cancellationToken)
                                        connected.Add(session)
                                    with ex ->
                                        error <-
                                            $"MCP server '{server.Name}' failed to start: {ex.GetType().Name}: {ex.Message}"

                            if isNull (box error) then
                                sessions <- connected |> Seq.toList
                            else
                                failure <- error
                                logDegrade.Invoke(error)
                        | violation ->
                            failure <- violation
                            logDegrade.Invoke(violation)

                        started <- true
                finally
                    gate.Release() |> ignore
        }
        :> Task

    /// Lists one session's tools, projecting each onto its assigned name.
    /// Any failure degrades the whole source to zero tools.
    /// <param name="pairs">The session/discovered pairs.</param>
    /// <param name="assigned">The assigned names in discovery order.</param>
    /// <returns>The projected tools.</returns>
    member private _.ProjectTools
        (pairs: (IMcpServerSession * McpDiscovery.McpDiscoveredTool) list, assigned: string list)
        : IReadOnlyList<AITool> =
        let tools = ResizeArray<AITool>()

        List.iter2
            (fun (session: IMcpServerSession, discovered: McpDiscovery.McpDiscoveredTool) (name: string) ->
                let invoke =
                    Func<AIFunctionArguments, CancellationToken, Task<string>>(fun arguments cancellationToken ->
                        task {
                            let args =
                                if isNull (box arguments) then
                                    Dictionary<string, obj>(StringComparer.Ordinal)
                                    :> IReadOnlyDictionary<string, obj>
                                else
                                    // AIFunctionArguments carries nullable values at runtime; the
                                    // session contract takes them as obj, so erase the
                                    // annotation here rather than copying the table.
                                    unbox<IReadOnlyDictionary<string, obj>> (box arguments)

                            return! session.CallToolAsync(discovered.ToolName, args, cancellationToken)
                        })

                tools.Add(McpProjectionFactory.create name discovered invoke :> AITool))
            pairs
            assigned

        tools :> IReadOnlyList<AITool>

    interface IToolSource with
        member this.GetTools(_context: ToolSourceContext) : Task<IReadOnlyList<AITool>> =
            task {
                try
                    do! this.EnsureStartedAsync(CancellationToken.None)

                    if not (isNull (box failure)) then
                        return ResizeArray<AITool>() :> IReadOnlyList<AITool>
                    else
                        let pairs = ResizeArray<IMcpServerSession * McpDiscovery.McpDiscoveredTool>()
                        let mutable error: string | null = null

                        for session in sessions do
                            if isNull (box error) then
                                try
                                    let! listed = session.ListToolsAsync(CancellationToken.None)

                                    if not (isNull (box listed)) then
                                        for discovered in listed do
                                            if not (isNull (box discovered)) then
                                                pairs.Add(session, discovered)
                                with ex ->
                                    error <-
                                        $"MCP server '{session.ServerName}' failed to list tools: {ex.GetType().Name}: {ex.Message}"

                        if not (isNull (box error)) then
                            logDegrade.Invoke(error)
                            return ResizeArray<AITool>() :> IReadOnlyList<AITool>
                        else
                            let sanitized =
                                pairs
                                |> Seq.map (fun (_, discovered) ->
                                    McpNaming.buildServerToolName discovered.ServerName discovered.ToolName)
                                |> Seq.toList

                            match McpNaming.resolve options.CollisionPolicy sanitized with
                            | Ok assigned -> return this.ProjectTools(pairs |> Seq.toList, assigned)
                            | Error message ->
                                logDegrade.Invoke(message)
                                return ResizeArray<AITool>() :> IReadOnlyList<AITool>
                with ex ->
                    logDegrade.Invoke($"{ex.GetType().Name}: {ex.Message}")
                    return ResizeArray<AITool>() :> IReadOnlyList<AITool>
            }

    interface IToolSourceLifecycle with
        member this.StartAsync(cancellationToken: CancellationToken) : Task =
            this.EnsureStartedAsync(cancellationToken)

        member _.StopAsync(cancellationToken: CancellationToken) : Task =
            task {
                let! acquired =
                    task {
                        try
                            do! gate.WaitAsync(cancellationToken)
                            return true
                        with _ ->
                            return false
                    }

                if acquired then
                    try
                        if not stopped then
                            stopped <- true

                            for session in sessions do
                                try
                                    do! session.DisposeAsync().AsTask()
                                with _ ->
                                    ()

                            sessions <- []

                            match box httpClient with
                            | :? HttpClient as owned -> owned.Dispose()
                            | _ -> ()
                    finally
                        gate.Release() |> ignore
            }
            :> Task
