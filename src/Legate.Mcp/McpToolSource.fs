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
// permission policy asks before running it. Binary result blocks are
// staged through the optional per-session artifact sink: GetTools
// resolves the asking session's quota-accounted sink and each projected
// tool closes over it, so the Mcp layer never derives tenant keys; a null
// factory keeps today's placeholder text.

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
        httpClient: HttpClient | null,
        observeCall: Action<McpInvocation.McpCallObservation> | null,
        artifactSinks: Func<ToolSourceContext, IArtifactSink> | null,
        artifactCaps: McpArtifacts.McpArtifactCaps
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
            owned,
            null,
            null,
            McpArtifacts.McpArtifactCaps.Default
        )

    /// Creates the source over an explicit degrade sink and connector.
    /// Internal: tests capture the sink and script the connector, so no
    /// test spawns a subprocess or opens a socket. Call observations go
    /// nowhere.
    internal new(options: McpOptions, logDegrade: Action<string | null>, connector: IMcpServerConnector) =
        McpToolSource(
            options,
            logDegrade,
            connector,
            Unchecked.defaultof<HttpClient>,
            null,
            null,
            McpArtifacts.McpArtifactCaps.Default
        )

    /// Creates the source over an explicit degrade sink, connector, and
    /// call-observation sink. Internal: tests capture the observation sink
    /// to assert per-call stamping; a null sink observes nothing.
    /// <param name="options">The bound MCP options. Must not be null.</param>
    /// <param name="logDegrade">The degrade sink. Must not be null.</param>
    /// <param name="connector">The scripted or SDK connector. Must not be null.</param>
    /// <param name="observeCall">The call-observation sink, or null to observe nothing.</param>
    internal new
        (
            options: McpOptions,
            logDegrade: Action<string | null>,
            connector: IMcpServerConnector,
            observeCall: Action<McpInvocation.McpCallObservation> | null
        ) =
        McpToolSource(
            options,
            logDegrade,
            connector,
            Unchecked.defaultof<HttpClient>,
            observeCall,
            null,
            McpArtifacts.McpArtifactCaps.Default
        )

    /// Creates the source over an explicit degrade sink, connector,
    /// call-observation sink, and per-session artifact sink factory.
    /// Internal: tests resolve the factory per asking session against a
    /// recording sink; a null factory keeps today's placeholder
    /// output. A factory that throws or returns null degrades one
    /// session's tools to placeholders, never to an error.
    /// <param name="options">The bound MCP options. Must not be null.</param>
    /// <param name="logDegrade">The degrade sink. Must not be null.</param>
    /// <param name="connector">The scripted or SDK connector. Must not be null.</param>
    /// <param name="observeCall">The call-observation sink, or null to observe nothing.</param>
    /// <param name="artifactSinks">Resolves the asking session's quota-accounted artifact sink, or null for placeholders.</param>
    /// <param name="artifactCaps">The header-only validation bounds.</param>
    internal new
        (
            options: McpOptions,
            logDegrade: Action<string | null>,
            connector: IMcpServerConnector,
            observeCall: Action<McpInvocation.McpCallObservation> | null,
            artifactSinks: Func<ToolSourceContext, IArtifactSink> | null,
            artifactCaps: McpArtifacts.McpArtifactCaps
        ) =
        McpToolSource(
            options,
            logDegrade,
            connector,
            Unchecked.defaultof<HttpClient>,
            observeCall,
            artifactSinks,
            artifactCaps
        )

    /// Creates the source over bound options, a logger, and a per-session
    /// artifact sink factory, owning its HTTP client and SDK connector.
    /// Internal: the service-extensions registration builds the source
    /// this way so tool binaries stage quota-accounted.
    /// <param name="options">The bound MCP options. Must not be null.</param>
    /// <param name="logger">The logger carrying degrade reasons. Must not be null.</param>
    /// <param name="artifactSinks">Resolves the asking session's quota-accounted artifact sink, or null for placeholders.</param>
    internal new
        (
            options: McpOptions,
            logger: ILogger<McpToolSource>,
            artifactSinks: Func<ToolSourceContext, IArtifactSink> | null
        ) =
        ArgumentNullException.ThrowIfNull(logger)
        let owned = new HttpClient()

        let logDegrade =
            Action<string | null>(fun reason ->
                logger.LogWarning("MCP tool source degraded to zero tools: {Reason}.", box reason))

        McpToolSource(
            options,
            logDegrade,
            SdkMcpServerConnector(logger :> ILogger, owned) :> IMcpServerConnector,
            owned,
            null,
            artifactSinks,
            McpArtifacts.McpArtifactCaps.Default
        )

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

    /// The overrides for the named server, or null when the server is
    /// unconfigured or carries none. Lookup misses yield null: unknown
    /// names are ignored, never an error.
    /// <param name="serverName">The configured server name.</param>
    /// <returns>The server's overrides, or null.</returns>
    member private _.OverridesFor(serverName: string) : McpServerOverrides | null =
        if isNull (box options) || isNull (box options.Servers) || isNull (box serverName) then
            null
        else
            match
                options.Servers
                |> Seq.tryFind (fun server -> not (isNull (box server)) && server.Name = serverName)
            with
            | Some server -> server.Overrides
            | None -> null

    /// Whether the server tool is disabled by its server's overrides.
    /// Unknown servers and unknown tool names read as enabled.
    /// <param name="serverName">The configured server name.</param>
    /// <param name="toolName">The server's tool name.</param>
    /// <returns>True when the tool is disabled; otherwise false.</returns>
    member private this.IsDisabled(serverName: string, toolName: string) : bool =
        match box (this.OverridesFor(serverName)) with
        | :? McpServerOverrides as overrides when not (isNull (box overrides.DisabledTools)) ->
            overrides.DisabledTools
            |> Seq.exists (fun name -> String.Equals(name, toolName, StringComparison.Ordinal))
        | _ -> false

    /// Applies the server's description rewrite, when it names this tool.
    /// Unknown names keep the discovered description.
    /// <param name="discovered">The discovered tool.</param>
    /// <returns>The tool with the rewritten description, or unchanged.</returns>
    member private this.WithDescription(discovered: McpDiscovery.McpDiscoveredTool) : McpDiscovery.McpDiscoveredTool =
        match box (this.OverridesFor(discovered.ServerName)) with
        | :? McpServerOverrides as overrides when not (isNull (box overrides.DescriptionOverrides)) ->
            let mutable rewrite = Unchecked.defaultof<string>

            if
                overrides.DescriptionOverrides.TryGetValue(discovered.ToolName, &rewrite)
                && not (isNull (box rewrite))
            then
                { discovered with
                    Description = rewrite
                }
            else
                discovered
        | _ -> discovered

    /// Resolves the asking session's artifact sink: the factory's
    /// quota-accounted sink, or null when no factory is wired, the factory
    /// throws, or it returns null. Sink wiring never fails tool listing.
    /// <param name="context">The tenant, agent, and session asking for its tools.</param>
    /// <returns>The session's sink, or null for placeholders.</returns>
    member private _.ArtifactSinkFor(context: ToolSourceContext) : IArtifactSink | null =
        if isNull (box artifactSinks) then
            null
        else
            try
                artifactSinks.Invoke(context)
            with _ ->
                null

    /// Lists one session's tools, projecting each onto its assigned name.
    /// Any failure degrades the whole source to zero tools.
    /// <param name="pairs">The session/discovered pairs.</param>
    /// <param name="assigned">The assigned names in discovery order.</param>
    /// <param name="artifactSink">The asking session's quota-accounted sink, or null for placeholders.</param>
    /// <returns>The projected tools.</returns>
    member private _.ProjectTools
        (
            pairs: (IMcpServerSession * McpDiscovery.McpDiscoveredTool) list,
            assigned: string list,
            artifactSink: IArtifactSink | null
        ) : IReadOnlyList<AITool> =
        let tools = ResizeArray<AITool>()

        let effectiveCaps =
            if isNull (box artifactCaps) then
                McpArtifacts.McpArtifactCaps.Default
            else
                artifactCaps

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

                            return!
                                McpInvocation.invokeWithArtifactsAsync
                                    session
                                    name
                                    discovered.ToolName
                                    args
                                    cancellationToken
                                    observeCall
                                    artifactSink
                                    effectiveCaps
                        })

                tools.Add(McpProjectionFactory.create name discovered invoke :> AITool))
            pairs
            assigned

        tools :> IReadOnlyList<AITool>

    interface IToolSource with
        member this.GetTools(context: ToolSourceContext) : Task<IReadOnlyList<AITool>> =
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
                            let effective =
                                pairs
                                |> Seq.toList
                                |> List.filter (fun (session, discovered) ->
                                    not (this.IsDisabled(session.ServerName, discovered.ToolName)))
                                |> List.map (fun (session, discovered) -> session, this.WithDescription(discovered))

                            let sanitized =
                                effective
                                |> List.map (fun (_, discovered) ->
                                    McpNaming.buildServerToolName discovered.ServerName discovered.ToolName)

                            match McpNaming.resolve options.CollisionPolicy sanitized with
                            | Ok assigned ->
                                return this.ProjectTools(effective, assigned, this.ArtifactSinkFor(context))
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
