// SPDX-License-Identifier: Apache-2.0
namespace Legate.Mcp

open System
open System.Collections.Generic

// Host-facing options for the MCP tool source, bound from
// Legate:Tools:Mcp. One McpServerOptions names one server and exactly one
// transport: stdio (Command plus optional Arguments and
// EnvironmentVariables, launched as a child process) or streamable HTTP
// (Url plus optional Headers, connecting to an existing server). Legacy SSE
// is not offered: the connector always dials streamable HTTP, so an
// SSE-only server fails fast with a bounded startup error naming it.
// McpOptions carries the server list plus the cross-server name-collision
// policy, defaulting to Fail so a duplicate never silently renames a
// model-facing tool.

/// How the MCP tool source resolves two servers exposing the same
/// sanitized tool name. Bound from <c>Legate:Tools:Mcp:CollisionPolicy</c>.
type McpNameCollisionPolicy =
    /// Duplicate sanitized names degrade the whole source to zero tools
    /// with a logged reason. The default: a collision never silently
    /// renames a model-facing tool.
    | Fail = 0
    /// Duplicate sanitized names are renamed deterministically with
    /// <c>_2</c>, <c>_3</c> suffixes, truncated to 128 characters and
    /// re-validated; a residual collision still degrades to zero tools.
    | Suffix = 1

/// One MCP server: a name plus exactly one transport. Stdio servers carry
/// a command with optional arguments and environment variables; HTTP
/// servers carry a URL with optional headers. Mutable so configuration
/// binding and object initialisers both work.
[<Sealed>]
type McpServerOptions() =

    /// The server name keying logs and the <c>{server}_{tool}</c> name
    /// prefix. Must be a non-empty string.
    member val Name: string | null = null with get, set

    /// The stdio command launching the server as a child process, for
    /// example <c>npx</c>. Set for stdio servers; null for HTTP servers.
    member val Command: string | null = null with get, set

    /// The stdio command arguments. Null reads as empty.
    member val Arguments: IList<string> = ResizeArray<string>() :> IList<string> with get, set

    /// Extra environment variables for the stdio child process. Null
    /// reads as empty; values are never logged.
    member val EnvironmentVariables: IDictionary<string, string> =
        Dictionary<string, string>(StringComparer.Ordinal) :> IDictionary<string, string> with get, set

    /// The streamable-HTTP endpoint of an existing server, for example
    /// <c>http://127.0.0.1:8080/mcp</c>. Must be an absolute URI. Set for
    /// HTTP servers; null for stdio servers.
    member val Url: string | null = null with get, set

    /// Extra HTTP headers sent with streamable-HTTP requests, for example
    /// an authorization header. Null reads as empty; values are never
    /// logged.
    member val Headers: IDictionary<string, string> =
        Dictionary<string, string>(StringComparer.Ordinal) :> IDictionary<string, string> with get, set

    /// Whether this server dials stdio: a non-empty command with no URL.
    /// <returns>True for stdio servers; otherwise false.</returns>
    member this.IsStdio: bool =
        not (String.IsNullOrWhiteSpace this.Command)
        && String.IsNullOrWhiteSpace this.Url

    /// Whether this server dials streamable HTTP: a non-empty URL with no
    /// command.
    /// <returns>True for HTTP servers; otherwise false.</returns>
    member this.IsHttp: bool =
        not (String.IsNullOrWhiteSpace this.Url)
        && String.IsNullOrWhiteSpace this.Command

    /// Returns null when this server names exactly one valid transport,
    /// otherwise a message for the first violation.
    /// <returns>The first violation's message, or null when the server is valid.</returns>
    member this.Validate() : string | null =
        if String.IsNullOrWhiteSpace this.Name then
            "Name must be a non-empty string."
        elif not (this.IsStdio || this.IsHttp) then
            if String.IsNullOrWhiteSpace this.Command && String.IsNullOrWhiteSpace this.Url then
                "Exactly one transport is required: set Command for stdio or Url for streamable HTTP."
            else
                "Command and Url are mutually exclusive: set exactly one transport."
        elif this.IsHttp then
            let mutable endpoint = Unchecked.defaultof<Uri>

            if not (Uri.TryCreate(this.Url, UriKind.Absolute, &endpoint)) then
                "Url must be a non-empty absolute URI."
            else
                null
        else
            null

/// The MCP tool source settings: the servers to connect plus the
/// cross-server name-collision policy. Bound from
/// <c>Legate:Tools:Mcp</c>; mutable so hosts can set properties before
/// registering. The collision policy defaults to
/// <see cref="F:Legate.Mcp.McpNameCollisionPolicy.Fail" />.
[<Sealed>]
type McpOptions() =

    /// The configuration section path this options type binds from.
    static member ConfigurationSectionPath: string = "Legate:Tools:Mcp"

    /// The servers to connect, in order. Empty serves zero tools.
    member val Servers: List<McpServerOptions> = List<McpServerOptions>() with get, set

    /// How duplicate sanitized tool names across servers resolve. Default
    /// <see cref="F:Legate.Mcp.McpNameCollisionPolicy.Fail" />: degrade to
    /// zero tools with a logged reason rather than silently renaming.
    member val CollisionPolicy: McpNameCollisionPolicy = McpNameCollisionPolicy.Fail with get, set

    /// Returns null when the policy is defined, every server validates,
    /// and server names are unique; otherwise a message for the first
    /// violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        if
            this.CollisionPolicy <> McpNameCollisionPolicy.Fail
            && this.CollisionPolicy <> McpNameCollisionPolicy.Suffix
        then
            "CollisionPolicy must be Fail or Suffix."
        elif isNull (box this.Servers) then
            "Servers must not be null."
        else
            let seen = HashSet<string>(StringComparer.Ordinal)
            let mutable violation: string | null = null
            let mutable index = 0

            while isNull (box violation) && index < this.Servers.Count do
                let server = this.Servers[index]

                if isNull (box server) then
                    violation <- $"Servers[{index}] must not be null."
                else
                    match server.Validate() with
                    | null ->
                        // Validate just proved Name non-empty; the type
                        // test carries that invariant past the nullness
                        // analysis (null never matches :? string).
                        match box server.Name with
                        | :? string as raw when not (String.IsNullOrWhiteSpace raw) ->
                            let name = raw.Trim()

                            if not (seen.Add(name)) then
                                violation <- $"Duplicate server name '{name}'."
                        | _ -> violation <- $"Servers[{index}]: Name must be a non-empty string."
                    | problem -> violation <- $"Servers[{index}]: {problem}"

                index <- index + 1

            violation
