// SPDX-License-Identifier: Apache-2.0
namespace Legate.Mcp

open System
open System.Runtime.CompilerServices
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options

// Registration over LegateBuilder: AddMcp binds the Legate:Tools:Mcp
// section onto McpOptions, applies the optional configure callback,
// validates, registers the bound options, and registers McpToolSource as
// a tool source. Uses only the public builder surface: the options
// instance through IOptions and the source through ToolsBuilder.AddSource.

// Binds the Legate:Tools:Mcp section and registers the source. Internal.
module internal McpRegistration =

    /// Binds the section, applies the callback, validates, and registers.
    /// <param name="builder">The Legate builder receiving the source.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options, or null to keep them.</param>
    /// <returns>The same builder, for chaining.</returns>
    let addMcp
        (builder: Legate.LegateBuilder, configuration: IConfiguration, configure: Action<McpOptions> | null)
        : Legate.LegateBuilder =
        ArgumentNullException.ThrowIfNull(builder)
        ArgumentNullException.ThrowIfNull(configuration)

        let options = McpOptions()
        configuration.GetSection(McpOptions.ConfigurationSectionPath).Bind(options)

        match configure with
        | null -> ()
        | callback -> callback.Invoke(options)

        match options.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid {McpOptions.ConfigurationSectionPath}: {violation}"))

        builder.Services.AddSingleton<IOptions<McpOptions>>(Options.Create(options))
        |> ignore

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
