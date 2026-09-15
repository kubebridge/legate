// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate.Mcp

open System
open System.Collections.Generic
open System.Net.Http
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open ModelContextProtocol
open ModelContextProtocol.Client
open ModelContextProtocol.Protocol

// The SDK adapter: the only module referencing ModelContextProtocol types.
// Every server dials through one IMcpServerConnector, which connects and
// returns a session holding the client for the process lifetime. Stdio
// servers launch through StdioClientTransport; HTTP servers dial through
// HttpClientTransport pinned to StreamableHttp (never AutoDetect, never
// Sse), so a legacy SSE-only server fails fast with a bounded startup
// error naming the server. Tests never touch this module: they script
// IMcpServerConnector, so no test spawns a subprocess or opens a socket.

// Nullness warning 3261 is suppressed in this file: the SDK surface
// carries no F# nullability annotations (option objects surface nulls the
// analysis cannot prove absent), and every server is validated before its
// values reach the SDK, so validated invariants hold by construction.

// ──────────────────────────
// Session and connector contracts

/// One connected MCP server: lists its tools and forwards calls for the
/// process lifetime. Internal: scripted by tests, SDK-backed in
/// production.
type internal IMcpServerSession =
    inherit IAsyncDisposable

    /// The configured server name.
    abstract ServerName: string

    /// Lists the server's current tools as SDK-free records.
    /// <param name="cancellationToken">Token that abandons the list.</param>
    /// <returns>The server's tools.</returns>
    abstract ListToolsAsync: cancellationToken: CancellationToken -> Task<IReadOnlyList<McpDiscovery.McpDiscoveredTool>>

    /// Forwards one call to the named server tool. Minimal passthrough:
    /// full per-call semantics belong to the per-call follow-up.
    /// <param name="toolName">The server's tool name.</param>
    /// <param name="arguments">The call arguments.</param>
    /// <param name="cancellationToken">Token that abandons the call.</param>
    /// <returns>The text result.</returns>
    abstract CallToolAsync:
        toolName: string * arguments: IReadOnlyDictionary<string, obj> * cancellationToken: CancellationToken ->
            Task<string>

/// Connects one configured server. Internal: scripted by tests,
/// SDK-backed in production.
type internal IMcpServerConnector =

    /// Connects the server and returns its process-lifetime session.
    /// <param name="server">The validated server options.</param>
    /// <param name="cancellationToken">Token that abandons the connect.</param>
    /// <returns>The connected session.</returns>
    abstract ConnectAsync: server: McpServerOptions * cancellationToken: CancellationToken -> Task<IMcpServerSession>

// ──────────────────────────
// Transport options

/// Builds the SDK transport options for one validated server. Internal:
/// unit-tested without connecting, so stdio and streamable-HTTP wiring is
/// verified with no subprocess and no socket.
module internal SdkTransportOptions =

    /// Builds the stdio transport options: the command, a copy of the
    /// arguments, and a copy of the environment variables.
    /// <param name="server">The validated stdio server options.</param>
    /// <returns>The SDK stdio transport options.</returns>
    let buildStdioOptions (server: McpServerOptions) : StdioClientTransportOptions =
        ArgumentNullException.ThrowIfNull(server)

        match server.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid MCP server: {violation}"))

        // Validate just proved Command non-empty; the type test carries
        // that invariant past the nullness analysis (null never matches
        // :? string).
        let command: string =
            match box server.Command with
            | :? string as kept when not (String.IsNullOrWhiteSpace kept) -> kept
            | _ -> raise (InvalidOperationException("Invalid MCP server: stdio servers need a non-empty Command."))

        let options = StdioClientTransportOptions(Command = command)
        options.Name <- server.Name

        let arguments = ResizeArray<string>()

        if not (isNull (box server.Arguments)) then
            for argument in server.Arguments do
                if not (isNull (box argument)) then
                    arguments.Add(argument)

        options.Arguments <- arguments :> IList<string>

        let environment = Dictionary<string, string>(StringComparer.Ordinal)

        if not (isNull (box server.EnvironmentVariables)) then
            for KeyValue(key, value) in server.EnvironmentVariables do
                if not (isNull (box key)) && not (isNull (box value)) then
                    environment[key] <- value

        options.EnvironmentVariables <- environment :> IDictionary<string, string>
        options

    /// Builds the HTTP transport options: the endpoint plus a copy of the
    /// headers, pinned to streamable HTTP. SSE is never selected.
    /// <param name="server">The validated HTTP server options.</param>
    /// <returns>The SDK HTTP transport options.</returns>
    let buildHttpOptions (server: McpServerOptions) : HttpClientTransportOptions =
        ArgumentNullException.ThrowIfNull(server)

        match server.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid MCP server: {violation}"))

        let endpoint: Uri =
            match box server.Url with
            | :? string as raw when not (String.IsNullOrWhiteSpace raw) ->
                let mutable parsed = Unchecked.defaultof<Uri>

                if Uri.TryCreate(raw, UriKind.Absolute, &parsed) then
                    parsed
                else
                    raise (InvalidOperationException("Invalid MCP server: HTTP servers need an absolute Url."))
            | _ -> raise (InvalidOperationException("Invalid MCP server: HTTP servers need an absolute Url."))

        let options = HttpClientTransportOptions(Endpoint = endpoint)
        options.TransportMode <- HttpTransportMode.StreamableHttp
        options.Name <- server.Name

        let headers = Dictionary<string, string>(StringComparer.Ordinal)

        if not (isNull (box server.Headers)) then
            for KeyValue(key, value) in server.Headers do
                if not (isNull (box key)) && not (isNull (box value)) then
                    headers[key] <- value

        options.AdditionalHeaders <- headers :> IDictionary<string, string>
        options

// ──────────────────────────
// Protocol mapping

/// SDK-free mapping helpers: protocol tools onto discovered records and
/// call results onto text. Internal.
module internal McpProtocolMapping =

    /// Maps one protocol tool onto the SDK-free record.
    /// <param name="serverName">The owning server's configured name.</param>
    /// <param name="tool">The protocol tool.</param>
    /// <returns>The discovered tool.</returns>
    let mapProtocolTool (serverName: string) (tool: Tool) : McpDiscovery.McpDiscoveredTool =
        ArgumentNullException.ThrowIfNull(tool)
        let annotations = tool.Annotations

        let title =
            if not (isNull (box tool.Title)) then tool.Title
            elif isNull (box annotations) then null
            else annotations.Title

        {
            ServerName = serverName
            ToolName = tool.Name
            Title = title
            Description = tool.Description
            InputSchema = tool.InputSchema
            OutputSchema =
                if tool.OutputSchema.HasValue then
                    Some tool.OutputSchema.Value
                else
                    None
            DestructiveHint =
                not (isNull (box annotations))
                && annotations.DestructiveHint.HasValue
                && annotations.DestructiveHint.Value
            ReadOnlyHint =
                if isNull (box annotations) || not annotations.ReadOnlyHint.HasValue then
                    None
                else
                    Some annotations.ReadOnlyHint.Value
            IdempotentHint =
                if isNull (box annotations) || not annotations.IdempotentHint.HasValue then
                    None
                else
                    Some annotations.IdempotentHint.Value
            OpenWorldHint =
                if isNull (box annotations) || not annotations.OpenWorldHint.HasValue then
                    None
                else
                    Some annotations.OpenWorldHint.Value
            AnnotationTitle = if isNull (box annotations) then null else annotations.Title
        }

    /// Renders one call result as text: the text blocks joined by
    /// newlines. Non-text blocks (binary payloads belong to their
    /// follow-up) contribute a short placeholder naming the block type,
    /// never the bytes.
    /// <param name="result">The call result, or null.</param>
    /// <returns>The text result, never null.</returns>
    let renderCallResult (result: CallToolResult | null) : string =
        if isNull (box result) || isNull (box result.Content) then
            ""
        else
            let parts = ResizeArray<string>()

            for block in result.Content do
                if not (isNull (box block)) then
                    match block with
                    | :? TextContentBlock as text -> parts.Add(if isNull (box text.Text) then "" else text.Text)
                    | _ -> parts.Add($"[non-text content: {block.Type}]")

            String.Join("\n", parts)

// ──────────────────────────
// SDK session and connector

/// One connected SDK server: the client lives until disposed.
/// Internal.
type internal SdkMcpServerSession(serverName: string, client: McpClient) =

    do
        ArgumentNullException.ThrowIfNull(serverName)
        ArgumentNullException.ThrowIfNull(client)

    interface IMcpServerSession with
        member _.ServerName: string = serverName

        member _.ListToolsAsync
            (cancellationToken: CancellationToken)
            : Task<IReadOnlyList<McpDiscovery.McpDiscoveredTool>> =
            task {
                let! tools = client.ListToolsAsync(Unchecked.defaultof<RequestOptions>, cancellationToken)

                let listed = ResizeArray<McpDiscovery.McpDiscoveredTool>()

                if not (isNull (box tools)) then
                    for tool in tools do
                        if not (isNull (box tool)) && not (isNull (box tool.ProtocolTool)) then
                            listed.Add(McpProtocolMapping.mapProtocolTool serverName tool.ProtocolTool)

                return listed :> IReadOnlyList<McpDiscovery.McpDiscoveredTool>
            }

        member _.CallToolAsync
            (toolName: string, arguments: IReadOnlyDictionary<string, obj>, cancellationToken: CancellationToken)
            : Task<string> =
            task {
                ArgumentNullException.ThrowIfNull(toolName)

                let! result =
                    client.CallToolAsync(
                        toolName,
                        arguments,
                        Unchecked.defaultof<IProgress<ProgressNotificationValue>>,
                        Unchecked.defaultof<RequestOptions>,
                        cancellationToken
                    )

                return McpProtocolMapping.renderCallResult result
            }

    interface IAsyncDisposable with
        member _.DisposeAsync() : ValueTask = client.DisposeAsync()

/// The production connector: stdio servers launch through
/// StdioClientTransport, HTTP servers dial streamable HTTP. The shared
/// HttpClient is used, never disposed here: its owner disposes it.
/// Internal.
type internal SdkMcpServerConnector(logger: ILogger | null, httpClient: HttpClient | null) =

    interface IMcpServerConnector with
        member _.ConnectAsync
            (server: McpServerOptions, cancellationToken: CancellationToken)
            : Task<IMcpServerSession> =
            task {
                ArgumentNullException.ThrowIfNull(server)

                match server.Validate() with
                | null -> ()
                | violation -> raise (InvalidOperationException($"Invalid MCP server '{server.Name}': {violation}"))

                let transport: IClientTransport =
                    if server.IsStdio then
                        StdioClientTransport(
                            SdkTransportOptions.buildStdioOptions server,
                            Unchecked.defaultof<ILoggerFactory>
                        )
                        :> IClientTransport
                    elif server.IsHttp then
                        match box httpClient with
                        | :? HttpClient as owned ->
                            HttpClientTransport(
                                SdkTransportOptions.buildHttpOptions server,
                                owned,
                                Unchecked.defaultof<ILoggerFactory>,
                                false
                            )
                            :> IClientTransport
                        | _ ->
                            raise (
                                InvalidOperationException(
                                    $"Invalid MCP server '{server.Name}': no HttpClient for streamable HTTP."
                                )
                            )
                    else
                        raise (
                            InvalidOperationException(
                                $"Invalid MCP server '{server.Name}': set Command for stdio or Url for streamable HTTP."
                            )
                        )

                if not (isNull (box logger)) then
                    logger.LogDebug("Connecting MCP server {ServerName}.", server.Name)

                let! client =
                    McpClient.CreateAsync(
                        transport,
                        Unchecked.defaultof<McpClientOptions>,
                        Unchecked.defaultof<ILoggerFactory>,
                        cancellationToken
                    )

                return SdkMcpServerSession(server.Name, client) :> IMcpServerSession
            }
