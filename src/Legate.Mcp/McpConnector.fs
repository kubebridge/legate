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

/// One binary payload extracted beside the rendered text: the mime type
/// plus the decoded bytes. Internal: the per-call invocation validates
/// these against the caps and stores them through the caller-supplied
/// artifact sink, substituting a text reference.
type internal McpBinaryPart =
    {
        /// The payload mime type, never null: empty server values read as
        /// <c>application/octet-stream</c>.
        MimeType: string
        /// The decoded payload bytes, or null when the base64 was blank
        /// or malformed: null fails validation downstream with a bounded
        /// rejection, never an exception.
        Bytes: byte[] | null
        /// The display name from the resource URI tail, or null for bare
        /// content blocks without a URI.
        Name: string | null
        /// The exact placeholder the text rendering emitted for this
        /// block, used for ordered substitution.
        Placeholder: string
    }

/// One SDK-free call result: the rendered text, whether the server
/// flagged it as an error, and the binary payloads extracted beside the
/// text. Internal: the per-call invocation maps the text onto
/// model-facing error texts and, when a sink is present, stores the
/// binaries as artifacts.
type internal McpCallResult =
    {
        /// The rendered text result, never null.
        Text: string
        /// Whether the server flagged the result as an error.
        IsError: bool
        /// The binary payloads extracted beside the text, in block order;
        /// empty when the result carried no binary blocks, never null.
        Binaries: IReadOnlyList<McpBinaryPart>
    }

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

    /// Forwards one call to the named server tool, returning the rendered
    /// text with the server's error flag. Transport failures raise; the
    /// per-call invocation maps both onto model-facing error texts.
    /// <param name="toolName">The server's tool name.</param>
    /// <param name="arguments">The call arguments.</param>
    /// <param name="cancellationToken">Token that abandons the call.</param>
    /// <returns>The text result with the error flag.</returns>
    abstract CallToolAsync:
        toolName: string * arguments: IReadOnlyDictionary<string, obj> * cancellationToken: CancellationToken ->
            Task<McpCallResult>

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

    /// The placeholder one non-text block contributes to the rendered
    /// text: the block type named, never the bytes. Kept in one helper
    /// so the text rendering and the binary extraction emit byte-identical
    /// placeholders for ordered substitution.
    /// <param name="blockType">The block type, as carried on the block.</param>
    /// <returns>The placeholder text.</returns>
    let private placeholderFor (blockType: string) : string = $"[non-text content: {blockType}]"

    /// Reads the mime type, defaulting empty server values to
    /// <c>application/octet-stream</c>.
    /// <param name="mimeType">The server mime type, or null.</param>
    /// <returns>The mime type, never null.</returns>
    let private mimeOrDefault (mimeType: string | null) : string =
        if String.IsNullOrWhiteSpace mimeType then
            "application/octet-stream"
        else
            mimeType

    /// Copies one SDK decoded payload into an array. The SDK carries the
    /// raw bytes on <c>DecodedData</c> (the <c>Data</c>/<c>Blob</c>
    /// properties hold base64 text); an empty payload reads as an empty
    /// array, which fails validation downstream with a bounded rejection.
    /// <param name="data">The decoded SDK payload.</param>
    /// <returns>The payload bytes, never null.</returns>
    let private copyOf (data: ReadOnlyMemory<byte>) : byte[] = data.ToArray()

    /// Reads the display name from a resource URI tail: the text after
    /// the final slash, cut at any query, fragment, or parameter mark.
    /// Blank URIs read as null.
    /// <param name="uri">The resource URI, or null.</param>
    /// <returns>The URI tail, or null.</returns>
    let private resourceName (uri: string | null) : string | null =
        if String.IsNullOrWhiteSpace uri then
            null
        else
            let trimmed = uri.Trim()
            let slash = trimmed.LastIndexOf('/')
            let tail = if slash < 0 then trimmed else trimmed.Substring(slash + 1)
            let cut = tail.IndexOfAny([| '?'; '#'; ';' |])
            let name = if cut < 0 then tail else tail.Substring(0, cut)

            if String.IsNullOrWhiteSpace name then null else name

    /// Splits one call result into its rendered text plus the binary
    /// payloads extracted beside it: image and audio blocks plus embedded
    /// blob resources yield mime plus bytes; text blocks, text resources,
    /// resource links, and unknown blocks contribute only their
    /// placeholder. The text is byte-identical to
    /// <c>renderCallResult</c>.
    /// <param name="result">The call result, or null.</param>
    /// <returns>The rendered text with the binaries in block order.</returns>
    let splitCallResult (result: CallToolResult | null) : string * IReadOnlyList<McpBinaryPart> =
        if isNull (box result) || isNull (box result.Content) then
            "", ResizeArray<McpBinaryPart>() :> IReadOnlyList<McpBinaryPart>
        else
            let parts = ResizeArray<string>()
            let binaries = ResizeArray<McpBinaryPart>()

            for block in result.Content do
                if not (isNull (box block)) then
                    match block with
                    | :? TextContentBlock as text -> parts.Add(if isNull (box text.Text) then "" else text.Text)
                    | :? ImageContentBlock as image ->
                        let placeholder = placeholderFor image.Type
                        parts.Add(placeholder)

                        binaries.Add(
                            {
                                MimeType = mimeOrDefault image.MimeType
                                Bytes = copyOf image.DecodedData
                                Name = null
                                Placeholder = placeholder
                            }
                        )
                    | :? AudioContentBlock as audio ->
                        let placeholder = placeholderFor audio.Type
                        parts.Add(placeholder)

                        binaries.Add(
                            {
                                MimeType = mimeOrDefault audio.MimeType
                                Bytes = copyOf audio.DecodedData
                                Name = null
                                Placeholder = placeholder
                            }
                        )
                    | :? EmbeddedResourceBlock as embedded ->
                        let placeholder = placeholderFor embedded.Type
                        parts.Add(placeholder)

                        match box embedded.Resource with
                        | :? BlobResourceContents as blob ->
                            binaries.Add(
                                {
                                    MimeType = mimeOrDefault blob.MimeType
                                    Bytes = copyOf blob.DecodedData
                                    Name = resourceName blob.Uri
                                    Placeholder = placeholder
                                }
                            )
                        | _ -> ()
                    | _ -> parts.Add(placeholderFor block.Type)

            String.Join("\n", parts), binaries :> IReadOnlyList<McpBinaryPart>

    /// Renders one call result as text: the text blocks joined by
    /// newlines. Non-text blocks contribute a short placeholder naming
    /// the block type, never the bytes; binary payloads are extracted
    /// beside the text by <c>splitCallResult</c>.
    /// <param name="result">The call result, or null.</param>
    /// <returns>The text result, never null.</returns>
    let renderCallResult (result: CallToolResult | null) : string = splitCallResult result |> fst

// ──────────────────────────
// Elicitation

/// The elicitation rejector: headless sessions never prompt a user, so
/// every server elicitation request is declined and the declined call
/// fails into the per-call error mapping as an <c>Error calling</c> text
/// with the turn continuing. Internal: unit-tested by invoking the
/// delegate directly, so no test needs a live server.
module internal McpElicitation =

    /// The elicitation action declining every request.
    [<Literal>]
    let DeclineAction = "decline"

    /// Builds client options declining every elicitation request: the
    /// delegate ignores the request and returns the decline, so the
    /// server fails the eliciting call into the error mapping.
    /// <returns>Client options carrying the rejector.</returns>
    let buildClientOptions () : McpClientOptions =
        let reject =
            Func<ElicitRequestParams, CancellationToken, ValueTask<ElicitResult>>(fun _ _ ->
                ValueTask<ElicitResult>(ElicitResult(Action = DeclineAction)))

        let handlers = McpClientHandlers()
        handlers.ElicitationHandler <- reject

        let options = McpClientOptions()
        options.Handlers <- handlers
        options

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
            : Task<McpCallResult> =
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

                let text, binaries = McpProtocolMapping.splitCallResult result

                return
                    {
                        Text = text
                        IsError = not (isNull (box result)) && result.IsError.HasValue && result.IsError.Value
                        Binaries = binaries
                    }
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
                        McpElicitation.buildClientOptions (),
                        Unchecked.defaultof<ILoggerFactory>,
                        cancellationToken
                    )

                return SdkMcpServerSession(server.Name, client) :> IMcpServerSession
            }
