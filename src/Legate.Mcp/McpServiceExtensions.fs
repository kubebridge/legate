// SPDX-License-Identifier: Apache-2.0
namespace Legate.Mcp

open System
open System.Runtime.CompilerServices
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection

// Registration over LegateBuilder: AddMcp binds the Legate:Tools:Mcp
// section onto McpOptions, applies the optional configure callback,
// validates eagerly, and registers deferred composable options plus the
// tool source. AddMcpServersFromConfig appends mcp.json file servers
// through Configure, so section and file servers compose no matter which
// entry point runs first. Uses only the public builder surface: the
// options through IOptions, the container through Services, and the
// source through ToolsBuilder.AddSource.

// Binds the Legate:Tools:Mcp section and registers the source. Internal.
module internal McpRegistration =

    /// True once an McpToolSource registration exists, so AddMcp and
    /// AddMcpServersFromConfig together still register exactly one source.
    let hasMcpSource (services: IServiceCollection) : bool =
        services
        |> Seq.exists (fun descriptor ->
            descriptor.ServiceType = typeof<Legate.IToolSource>
            && descriptor.ImplementationType = typeof<McpToolSource>)

    /// Binds the section, applies the callback, validates eagerly, and
    /// registers deferred options plus the source exactly once.
    /// <param name="builder">The Legate builder receiving the source.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options, or null to keep them.</param>
    /// <returns>The same builder, for chaining.</returns>
    let addMcp
        (builder: Legate.LegateBuilder, configuration: IConfiguration, configure: Action<McpOptions> | null)
        : Legate.LegateBuilder =
        ArgumentNullException.ThrowIfNull(builder)
        ArgumentNullException.ThrowIfNull(configuration)

        // Fail fast before registering, preserving the #77 startup error.
        let probe = McpOptions()
        configuration.GetSection(McpOptions.ConfigurationSectionPath).Bind(probe)

        match configure with
        | null -> ()
        | callback -> callback.Invoke(probe)

        match probe.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid {McpOptions.ConfigurationSectionPath}: {violation}"))

        // Deferred and composable: the section binds at resolve time, so
        // file servers appended through Configure join the section
        // servers no matter which entry point runs first.
        builder.Services.AddOptions<McpOptions>() |> ignore

        let section = configuration.GetSection(McpOptions.ConfigurationSectionPath)

        builder.Services.Configure<McpOptions>(Action<McpOptions>(fun options -> section.Bind(options)))
        |> ignore

        match configure with
        | null -> ()
        | callback -> builder.Services.Configure<McpOptions>(callback) |> ignore

        if not (hasMcpSource builder.Services) then
            builder.Tools.AddSource<McpToolSource>() |> ignore

        builder

/// Extension methods registering the MCP tool source on
/// <see cref="T:Legate.LegateBuilder" /> (<c>builder.AddMcp(...)</c>).
/// Binds the <c>Legate:Tools:Mcp</c> section (servers plus the
/// <c>CollisionPolicy</c>, default <c>Fail</c>) and registers the source
/// the runtime resolves session tools through.
[<Sealed; AbstractClass; Extension>]
type McpLegateBuilderExtensions =

    /// Binds <c>Legate:Tools:Mcp</c> and registers the MCP tool source.
    /// <param name="builder">The Legate builder receiving the source.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddMcp(builder: Legate.LegateBuilder, configuration: IConfiguration) : Legate.LegateBuilder =
        McpRegistration.addMcp (builder, configuration, null)

    /// Binds <c>Legate:Tools:Mcp</c>, adjusts it with the callback, and
    /// registers the MCP tool source.
    /// <param name="builder">The Legate builder receiving the source.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options before registration.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddMcp
        (builder: Legate.LegateBuilder, configuration: IConfiguration, configure: Action<McpOptions>)
        : Legate.LegateBuilder =
        ArgumentNullException.ThrowIfNull(configure)
        McpRegistration.addMcp (builder, configuration, configure)

/// Extension methods appending mcp.json file servers onto
/// <see cref="T:Legate.ToolsBuilder" />
/// (<c>legate.Tools.AddMcpServersFromConfig(path)</c>). Accepts the
/// Claude-style dialect: a top-level <c>mcpServers</c> map whose entries
/// carry <c>command</c>, <c>args</c>, and <c>env</c> for stdio servers or
/// <c>url</c> and <c>headers</c> for streamable-HTTP servers. Transport is
/// inferred by presence: a non-empty <c>command</c> with no URL is stdio,
/// a non-empty <c>url</c> with no command is HTTP, and both-set or
/// neither-set fails startup naming the entry key. Every other field
/// (<c>type</c>, <c>cwd</c>, and editor extras) is inert. <c>${VAR}</c>
/// placeholders expand from the process environment in all five fields;
/// an unset or empty variable fails startup naming the server key and the
/// variable name, and expanded values never appear in error messages.
/// Fields for the other transport (for example <c>headers</c> on a stdio
/// entry) are ignored.
[<Sealed; AbstractClass; Extension>]
type McpToolsBuilderExtensions =

    /// Loads MCP servers from a Claude-style mcp.json file and appends
    /// them to the bound <c>McpOptions.Servers</c>, registering the MCP
    /// tool source when none is registered yet. Composes with
    /// <c>AddMcp</c> in either order: section and file servers resolve
    /// together, and the two entry points still register exactly one
    /// source. Missing files, malformed JSON, invalid entries, and unset
    /// placeholders throw <see cref="T:System.InvalidOperationException" />
    /// naming the path or the offending <c>mcpServers</c> key.
    /// <param name="builder">The tools builder receiving the file servers.</param>
    /// <param name="path">The mcp.json file path. Must not be null or empty.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member AddMcpServersFromConfig(builder: Legate.ToolsBuilder, path: string) : Legate.ToolsBuilder =
        ArgumentNullException.ThrowIfNull(builder)

        if isNull (box path) then
            raise (ArgumentNullException(nameof path))

        if String.IsNullOrWhiteSpace path then
            raise (ArgumentException("An MCP server configuration path must be a non-empty string.", nameof path))

        // Fail fast before registering: the loader names the path or the
        // offending mcpServers key without ever logging expanded values.
        let servers = McpConfigFile.loadServers path

        // Deferred append, safe to repeat: file and section servers join
        // at resolve time no matter which entry point runs first.
        builder.Services.AddOptions<McpOptions>() |> ignore

        builder.Services.Configure<McpOptions>(
            Action<McpOptions>(fun options ->
                for server in servers do
                    options.Servers.Add(server))
        )
        |> ignore

        if not (McpRegistration.hasMcpSource builder.Services) then
            builder.AddSource<McpToolSource>() |> ignore

        builder
