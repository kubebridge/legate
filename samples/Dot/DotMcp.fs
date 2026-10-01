// SPDX-License-Identifier: Apache-2.0
module Dot.DotMcp

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Text.Json
open Legate.Mcp
open Microsoft.Extensions.Configuration

// Dot YAML MCP servers (issue 336): the Dot:Mcp:Servers map in the
// mcp.json field dialect (command, args, env for stdio; url, headers for
// remote). Transport inference mirrors McpConfigFile.loadEntry verbatim:
// a non-empty command with no URL is stdio, a non-empty URL with no
// command is HTTP, and both-set or neither-set fails naming the server and
// the field; fields for the other transport are ignored. ${VAR}
// expansion is inherited from the DotConfig YAML provider (values read
// here are already expanded exactly once); DotMcp never expands again.
// Same-name collisions with the --mcp file resolve file-wins in
// Program.buildServices through fileServerKeys plus a shadow filter, so
// this module only parses and scans.

// ──────────────────────────────────────────────────────────────────────────
// Section parsing

/// Fails loading with a startup error naming the Dot:Mcp:Servers entry.
// Values never flow through here, so secrets cannot leak into messages.
/// <param name="name">The server name owning the value.</param>
/// <param name="detail">The violation, naming the field.</param>
/// <returns>Never returns.</returns>
let private failEntry (name: string) (detail: string) : 'T =
    raise (InvalidOperationException($"Invalid Dot:Mcp:Servers entry '{name}': {detail}"))

/// Reads the ordered args list: the args sequence in numeric index order.
// Missing reads as empty; a scalar under args fails naming the server and
// the field. YAML scalars coerce to text (unlike the strict mcp.json
// dialect); null reads as empty.
/// <param name="name">The server name owning the value.</param>
/// <param name="section">The server section.</param>
/// <returns>The arguments in index order.</returns>
let private readArgs (name: string) (section: IConfigurationSection) : IList<string> =
    let sub = section.GetSection("args")
    let kids = sub.GetChildren() |> Seq.toList

    if kids.IsEmpty && not (isNull (box sub.Value)) then
        failEntry name "'args' must be a list of strings."

    let ordered =
        kids
        |> List.map (fun child ->
            let text =
                match Option.ofObj child.Value with
                | None -> ""
                | Some value -> value

            match Int32.TryParse(child.Key) with
            | true, index -> index, text
            | _ -> failEntry name "'args' must be a list of strings.")
        |> List.sortBy fst
        |> List.map snd

    ResizeArray<string>(ordered) :> IList<string>

/// Reads one string-map field (env or headers) with keys literal: missing
// reads as empty; a scalar under the field fails naming the server and the
// field. Values arrive already ${VAR}-expanded from the DotConfig YAML
// provider, so only null coerces to empty here.
/// <param name="name">The server name owning the value.</param>
/// <param name="section">The server section.</param>
/// <param name="field">The map field name.</param>
/// <returns>The map entries.</returns>
let private readMap (name: string) (section: IConfigurationSection) (field: string) : IDictionary<string, string> =
    let sub = section.GetSection(field)
    let kids = sub.GetChildren() |> Seq.toList

    if kids.IsEmpty && not (isNull (box sub.Value)) then
        failEntry name $"'{field}' must be an object with string values."

    let values = Dictionary<string, string>(StringComparer.Ordinal)

    for child in kids do
        let text =
            match Option.ofObj child.Value with
            | None -> ""
            | Some value -> value

        values[child.Key] <- text

    values :> IDictionary<string, string>

/// Maps one Dot:Mcp:Servers entry onto McpServerOptions: infers stdio
// from command versus HTTP from url exactly as McpConfigFile.loadEntry
// does, and validates the result, failing naming the server throughout.
// Fields for the other transport (for example headers on a stdio entry)
// are ignored.
/// <param name="name">The server name.</param>
/// <param name="section">The server section.</param>
/// <returns>The server options.</returns>
let private loadEntry (name: string) (section: IConfigurationSection) : McpServerOptions =
    let command = section["command"]
    let url = section["url"]
    let hasCommand = not (String.IsNullOrWhiteSpace command)
    let hasUrl = not (String.IsNullOrWhiteSpace url)

    if hasCommand && hasUrl then
        failEntry name "set exactly one transport: 'command' for stdio or 'url' for streamable HTTP, not both."
    elif not (hasCommand || hasUrl) then
        failEntry name "set exactly one transport: 'command' for stdio or 'url' for streamable HTTP."

    let server = McpServerOptions(Name = name)

    if hasCommand then
        server.Command <- command
        server.Arguments <- readArgs name section
        server.EnvironmentVariables <- readMap name section "env"
    else
        server.Url <- url
        server.Headers <- readMap name section "headers"

    match server.Validate() with
    | null -> server
    | problem -> failEntry name problem

/// Loads the Dot:Mcp:Servers map from the merged configuration into
// server options in section order. Absent reads as empty; invalid entries
// fail naming the server and the offending field. Values are already
// ${VAR}-expanded by the DotConfig YAML provider: an unset or empty
// variable already failed at config load naming the key and the variable.
/// <param name="config">The merged configuration.</param>
/// <returns>The server options, one per Dot:Mcp:Servers entry.</returns>
let loadYamlServers (config: IConfiguration) : McpServerOptions list =
    ArgumentNullException.ThrowIfNull(config)

    [
        for child in config.GetSection("Dot:Mcp:Servers").GetChildren() do
            yield loadEntry child.Key child
    ]

// ──────────────────────────────────────────────────────────────────────────
// File-key scan (same-name file-wins)

// Scans the top-level mcpServers keys of a --mcp file for the same-name
// shadow filter. Unreadable or malformed files read as empty on purpose:
// the canonical AddMcpServersFromConfig loader fails naming the path or
// the entry right after, so the scan never produces a second error.
/// <param name="path">The mcp.json file path.</param>
/// <returns>The file's server names.</returns>
let fileServerKeys (path: string) : Set<string> =
    try
        if String.IsNullOrWhiteSpace path || not (File.Exists(path)) then
            Set.empty
        else
            let text = File.ReadAllText(path, Encoding.UTF8)

            use parsed = JsonDocument.Parse(text)

            let root = parsed.RootElement
            let mutable servers = Unchecked.defaultof<JsonElement>

            if
                root.ValueKind = JsonValueKind.Object
                && root.TryGetProperty("mcpServers", &servers)
                && servers.ValueKind = JsonValueKind.Object
            then
                [
                    for entry in servers.EnumerateObject() -> entry.Name
                ]
                |> Set.ofList
            else
                Set.empty
    with _ ->
        Set.empty
