// SPDX-License-Identifier: Apache-2.0
module Dot.DotConfig

open System
open System.Collections.Generic
open System.IO
open System.Text
open Microsoft.Extensions.Configuration
open YamlDotNet.RepresentationModel

// Dot YAML configuration (issue 328): user and project appsettings.yaml
// scopes with gitignored .local.yaml siblings, flattened through a custom
// YamlDotNet provider (in-tree dependency, no new package) in the fixed
// order user < user-local < project < project-local, then environment
// variables, then CLI flags strongest. String values expand ${NAME} from
// the process environment with the McpConfigFile rules verbatim; unset or
// empty fails startup naming the key without echoing values.

// ──────────────────────────────────────────────────────────────────────────
// Paths

/// Resolves the per-user dot config directory: DOT_CONFIG_HOME/dot when
/// set (cross-platform override, also used by the keyless smokes to point
/// at a temp dir), else %USERPROFILE%\.config\dot on Windows, else
/// $XDG_CONFIG_HOME/dot else ~/.config/dot on Unix. The database default
/// no longer follows this directory on Windows (see defaultDbPath).
/// <returns>The user config directory.</returns>
let userConfigDir () : string =
    match Option.ofObj (Environment.GetEnvironmentVariable("DOT_CONFIG_HOME")) with
    | Some home when not (String.IsNullOrWhiteSpace home) -> Path.Combine(home.Trim(), "dot")
    | _ ->
        if OperatingSystem.IsWindows() then
            Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".config", "dot")
        else
            let baseDir =
                match Option.ofObj (Environment.GetEnvironmentVariable("XDG_CONFIG_HOME")) with
                | None -> Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".config")
                | Some xdg when String.IsNullOrWhiteSpace xdg ->
                    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".config")
                | Some xdg -> xdg.Trim()

            Path.Combine(baseDir, "dot")

/// The user-scope committed file path.
/// <returns>The user appsettings.yaml path.</returns>
let userConfigPath () : string =
    Path.Combine(userConfigDir (), "appsettings.yaml")

/// The user-scope local-override path (gitignored, never committed).
/// <returns>The user appsettings.local.yaml path.</returns>
let userLocalPath () : string =
    Path.Combine(userConfigDir (), "appsettings.local.yaml")

/// The project-scope committed file path under the working directory.
/// <param name="projectDir">The project working directory.</param>
/// <returns>The project .dot/appsettings.yaml path.</returns>
let projectConfigPath (projectDir: string) : string =
    Path.Combine(projectDir, ".dot", "appsettings.yaml")

/// The project-scope local-override path (gitignored, never committed).
/// <param name="projectDir">The project working directory.</param>
/// <returns>The project .dot/appsettings.local.yaml path.</returns>
let projectLocalPath (projectDir: string) : string =
    Path.Combine(projectDir, ".dot", "appsettings.local.yaml")

/// The four optional YAML sources in load order (weakest first):
/// user, user-local, project, project-local. Missing files are never
/// errors; the provider skips them.
/// <param name="projectDir">The project working directory.</param>
/// <returns>The ordered config file paths.</returns>
let orderedPaths (projectDir: string) : string list =
    [
        userConfigPath ()
        userLocalPath ()
        projectConfigPath projectDir
        projectLocalPath projectDir
    ]

// ──────────────────────────────────────────────────────────────────────────
// ${VAR} expansion (McpConfigFile rules verbatim)

/// True for plain environment-variable names: a letter or underscore
/// followed by letters, digits, or underscores. Only ${NAME} with a name
/// of this shape expands; every other $-sequence stays literal.
let private isVariableName (name: string) : bool =
    not (String.IsNullOrEmpty name)
    && (Char.IsLetter name[0] || name[0] = '_')
    && Seq.forall (fun c -> Char.IsLetterOrDigit c || c = '_') name

/// Expands ${NAME} placeholders in one YAML value from the process
/// environment. An unset or empty variable fails startup naming the config
/// key and the variable; values never appear in messages.
/// <param name="path">The YAML file owning the value, for errors.</param>
/// <param name="key">The :-joined config key owning the value.</param>
/// <param name="value">The raw YAML value; never null.</param>
/// <returns>The expanded value.</returns>
let private expandValue (path: string) (key: string) (value: string) : string =
    let output = StringBuilder(value.Length)
    let mutable index = 0

    while index < value.Length do
        if value[index] = '$' && index + 1 < value.Length && value[index + 1] = '{' then
            let close = value.IndexOf('}', index + 2)

            if close < 0 then
                output.Append(value.Substring(index)) |> ignore
                index <- value.Length
            else
                let name = value.Substring(index + 2, close - index - 2)

                if isVariableName name then
                    match Option.ofObj (Environment.GetEnvironmentVariable(name)) with
                    | Some expanded when not (String.IsNullOrWhiteSpace expanded) -> output.Append(expanded) |> ignore
                    | _ ->
                        raise (
                            InvalidOperationException(
                                $"Dot configuration value '{key}' in '{path}' needs environment variable '{name}' to be set."
                            )
                        )
                else
                    output.Append(value.Substring(index, close - index + 1)) |> ignore

                index <- close + 1
        else
            output.Append(value[index]) |> ignore
            index <- index + 1

    output.ToString()

/// Narrows one possibly-null YAML scalar value to non-null text, mapping
/// null to empty: the single null-handling point for YamlDotNet scalar
/// values.
/// <param name="value">The scalar value, possibly null.</param>
/// <returns>The value, or empty when null.</returns>
let private scalarText (value: string | null) : string =
    match value with
    | null -> ""
    | text -> text

// ──────────────────────────────────────────────────────────────────────────
// YAML provider (flatten to :-joined keys)

/// Flattens one YAML node into the data map under the key prefix.
/// Scalar leaves only; mappings recurse, sequences index by position.
/// <param name="path">The YAML file owning the value, for errors.</param>
/// <param name="data">The flattened key/value sink.</param>
/// <param name="prefix">The :-joined key prefix for this node.</param>
/// <param name="node">The YAML node to flatten.</param>
let rec private flattenInto
    (path: string)
    (data: IDictionary<string, string>)
    (prefix: string)
    (node: YamlNode)
    : unit =
    match node with
    | :? YamlScalarNode as scalar -> data[prefix] <- expandValue path prefix (scalarText scalar.Value)
    | :? YamlMappingNode as mapping ->
        for pair in mapping.Children do
            match pair.Key with
            | :? YamlScalarNode as key ->
                let name = scalarText key.Value

                if String.IsNullOrWhiteSpace name then
                    raise (
                        InvalidOperationException(
                            $"Dot configuration file '{path}' must hold a mapping with plain names (bad key under '{prefix}')."
                        )
                    )
                else
                    let segment = name.Trim()
                    let full = if prefix = "" then segment else prefix + ":" + segment
                    flattenInto path data full pair.Value
            | _ ->
                raise (
                    InvalidOperationException(
                        $"Dot configuration file '{path}' must hold a mapping with plain names (bad key under '{prefix}')."
                    )
                )
    | :? YamlSequenceNode as sequence ->
        let mutable index = 0

        for child in sequence.Children do
            flattenInto path data (prefix + ":" + string index) child
            index <- index + 1
    | _ ->
        raise (
            InvalidOperationException(
                $"Dot configuration file '{path}' holds an unsupported value at '{prefix}': only maps, lists, and scalars load."
            )
        )

/// Loads one YAML file into flattened pairs. Missing files read as empty;
/// empty documents read as empty; malformed YAML and non-mapping roots fail
/// naming the path; values never appear in messages.
/// <param name="path">The YAML file path.</param>
/// <returns>The flattened pairs.</returns>
let loadYamlFile (path: string) : IDictionary<string, string> =
    let data =
        Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) :> IDictionary<string, string>

    if File.Exists(path) then
        let text = File.ReadAllText(path, Encoding.UTF8)

        if not (String.IsNullOrWhiteSpace text) then
            try
                use reader = new StringReader(text)
                let stream = YamlStream()
                stream.Load(reader)

                if stream.Documents.Count > 0 then
                    let rootObj = box stream.Documents[0].RootNode

                    if not (isNull rootObj) then
                        match stream.Documents[0].RootNode with
                        | :? YamlMappingNode as mapping when mapping.Children.Count = 0 -> ()
                        | :? YamlMappingNode as mapping ->
                            for pair in mapping.Children do
                                match pair.Key with
                                | :? YamlScalarNode as key ->
                                    let name = scalarText key.Value

                                    if String.IsNullOrWhiteSpace name then
                                        raise (
                                            InvalidOperationException(
                                                $"Dot configuration file '{path}' must hold a mapping with plain names."
                                            )
                                        )
                                    else
                                        flattenInto path data (name.Trim()) pair.Value
                                | _ ->
                                    raise (
                                        InvalidOperationException(
                                            $"Dot configuration file '{path}' must hold a mapping with plain names."
                                        )
                                    )
                        | :? YamlScalarNode as scalar when String.IsNullOrWhiteSpace(scalarText scalar.Value) -> ()
                        | _ ->
                            raise (
                                InvalidOperationException(
                                    $"Dot configuration file '{path}' must hold a YAML mapping at the top level."
                                )
                            )
            with
            | :? InvalidOperationException -> reraise ()
            | :? YamlDotNet.Core.YamlException as ex ->
                raise (InvalidOperationException($"Dot configuration file '{path}' is not valid YAML: {ex.Message}"))

    data

/// A configuration source for one optional YAML file.
/// <param name="path">The YAML file path.</param>
type YamlConfigurationSource(path: string) =
    member _.Path: string = path

    interface IConfigurationSource with
        member _.Build(_: IConfigurationBuilder) : IConfigurationProvider =
            YamlConfigurationProvider(path) :> IConfigurationProvider

/// A configuration provider that flattens one optional YAML file.
/// <param name="path">The YAML file path.</param>
and YamlConfigurationProvider(path: string) =
    inherit ConfigurationProvider()

    override _.Load() : unit =
        let pairs = loadYamlFile path
        let store = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)

        for pair in pairs do
            store[pair.Key] <- pair.Value

        base.Data <- store :> IDictionary<string, string>

/// Builds the source for one optional YAML file in the fixed load order.
/// <param name="path">The YAML file path.</param>
/// <returns>The configuration source.</returns>
let yamlSource (path: string) : IConfigurationSource =
    YamlConfigurationSource(path) :> IConfigurationSource

// ──────────────────────────────────────────────────────────────────────────
// Effective resolution (flags > env > files > defaults)

/// Reads a trimmed non-empty value, or null when absent.
/// <param name="config">The merged configuration.</param>
/// <param name="key">The :-joined key.</param>
/// <returns>The trimmed value, or null.</returns>
let private getNonEmpty (config: IConfiguration) (key: string) : string | null =
    match Option.ofObj config[key] with
    | None -> null
    | Some raw when String.IsNullOrWhiteSpace raw -> null
    | Some raw -> raw.Trim()

/// Resolves the default provider: the --provider flag wins, else the
/// Dot:Provider file/env value, else null for automatic.
/// <param name="config">The merged configuration.</param>
/// <param name="flag">The --provider value, or null.</param>
/// <returns>The effective provider, or null.</returns>
let resolveProvider (config: IConfiguration) (flag: string | null) : string | null =
    match Option.ofObj flag with
    | None -> getNonEmpty config "Dot:Provider"
    | Some raw when String.IsNullOrWhiteSpace raw -> getNonEmpty config "Dot:Provider"
    | Some raw -> raw.Trim()

/// Resolves the model reference: the --model flag wins, else the Dot:Model
/// file/env value, else null for the provider default.
/// <param name="config">The merged configuration.</param>
/// <param name="flag">The --model value, or null.</param>
/// <returns>The effective model, or null.</returns>
let resolveModel (config: IConfiguration) (flag: string | null) : string | null =
    match Option.ofObj flag with
    | None -> getNonEmpty config "Dot:Model"
    | Some raw when String.IsNullOrWhiteSpace raw -> getNonEmpty config "Dot:Model"
    | Some raw -> raw.Trim()

/// Resolves the ask default: the --ask flag forces true, else the Dot:Ask
/// file/env value, else false. Unparsable values fail naming the key.
/// <param name="config">The merged configuration.</param>
/// <param name="flag">True when --ask was passed.</param>
/// <returns>The effective ask default.</returns>
let resolveAsk (config: IConfiguration) (flag: bool) : bool =
    if flag then
        true
    else
        match Option.ofObj (getNonEmpty config "Dot:Ask") with
        | None -> false
        | Some raw ->
            match Boolean.TryParse(raw) with
            | true, parsed -> parsed
            | _ -> raise (InvalidOperationException("Dot configuration value 'Dot:Ask' must be true or false."))

/// Resolves the default SQLite file path before DOT_DB_PATH and
/// Dot:DbPath overrides: %APPDATA%/dot/dot.db on Windows (pinned, never
/// following userConfigDir or DOT_CONFIG_HOME, so moving the configuration
/// never orphans existing sessions), else the per-user dot config dir plus
/// dot.db (DOT_CONFIG_HOME/dot, $XDG_CONFIG_HOME/dot else ~/.config/dot on
/// Unix).
/// <returns>The default database file path.</returns>
let defaultDbPath () : string =
    if OperatingSystem.IsWindows() then
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "dot", "dot.db")
    else
        Path.Combine(userConfigDir (), "dot.db")

/// Resolves the SQLite file path: DOT_DB_PATH wins, else the Dot:DbPath
/// file/env value, else the per-user default (defaultDbPath).
/// <param name="config">The merged configuration.</param>
/// <returns>The database file path.</returns>
let resolveDbPath (config: IConfiguration) : string =
    match Option.ofObj (Environment.GetEnvironmentVariable("DOT_DB_PATH")) with
    | Some raw when not (String.IsNullOrWhiteSpace raw) -> raw.Trim()
    | _ ->
        match Option.ofObj (getNonEmpty config "Dot:DbPath") with
        | None -> defaultDbPath ()
        | Some filePath -> filePath

/// Resolves the workspace root: DOT_WORKSPACE_ROOT wins, else the
/// Dot:WorkspaceRoot file/env value, else the process working directory.
/// <param name="config">The merged configuration.</param>
/// <returns>The workspace root.</returns>
let resolveWorkspaceRoot (config: IConfiguration) : string =
    match Option.ofObj (Environment.GetEnvironmentVariable("DOT_WORKSPACE_ROOT")) with
    | Some raw when not (String.IsNullOrWhiteSpace raw) -> raw.Trim()
    | _ ->
        match Option.ofObj (getNonEmpty config "Dot:WorkspaceRoot") with
        | None -> Environment.CurrentDirectory
        | Some root -> root

// ──────────────────────────────────────────────────────────────────────────
// Provider keys (file/env bridge, compound wins over plain)

/// The Legate-section key carrying the provider API key.
/// <param name="id">The provider id.</param>
/// <returns>The :-joined key.</returns>
let private compoundKey (id: string) : string = $"Legate:Llm:Providers:{id}:ApiKey"

/// The Dot-section key carrying the provider API key.
/// <param name="id">The provider id.</param>
/// <returns>The :-joined key.</returns>
let private dotKey (id: string) : string = $"Dot:Providers:{id}:ApiKey"

/// The plain environment variable carrying the provider key.
/// <param name="id">The provider id.</param>
/// <returns>The env var name.</returns>
let private plainVar (id: string) : string =
    match id.ToLowerInvariant() with
    | "anthropic" -> "ANTHROPIC_API_KEY"
    | "openai" -> "OPENAI_API_KEY"
    | "google" -> "GOOGLE_API_KEY"
    | "ollamacloud" -> "OLLAMA_API_KEY"
    | _ -> ""

/// The compound environment variable for the bridge.
/// <param name="id">The provider id.</param>
/// <returns>The __-joined env var name.</returns>
let private compoundVar (id: string) : string =
    match id.ToLowerInvariant() with
    | "google" -> "Legate__Llm__Providers__Google__ApiKey"
    | _ -> $"Legate__Llm__Providers__{id}__ApiKey"

/// The provider ids dot bridges.
/// <returns>The bridged provider ids.</returns>
let private bridgedIds () : string list =
    [
        "anthropic"
        "openai"
        "google"
        "ollamacloud"
    ]

/// Reads the effective provider key: the Legate compound binding wins,
/// else the Dot-section file value, else the plain env key.
/// <param name="config">The merged configuration.</param>
/// <param name="id">The provider id.</param>
/// <returns>The effective key, or null.</returns>
let getProviderKey (config: IConfiguration) (id: string) : string | null =
    match Option.ofObj (getNonEmpty config (compoundKey id)) with
    | Some compound -> compound
    | None ->
        match Option.ofObj (getNonEmpty config (dotKey id)) with
        | Some fileKey -> fileKey
        | None ->
            match Option.ofObj (Environment.GetEnvironmentVariable(plainVar id)) with
            | None -> null
            | Some raw when String.IsNullOrWhiteSpace raw -> null
            | Some raw -> raw.Trim()

/// True when the effective provider key is present.
/// <param name="config">The merged configuration.</param>
/// <param name="id">The provider id.</param>
/// <returns>True when a key is configured.</returns>
let hasProviderKey (config: IConfiguration) (id: string) : bool =
    not (isNull (box (getProviderKey config id)))

/// True when any live provider key is present in the effective config.
/// <param name="config">The merged configuration.</param>
/// <returns>True when live mode can serve.</returns>
let hasLiveKey (config: IConfiguration) : bool =
    bridgedIds () |> List.exists (hasProviderKey config)

/// Bridges file and plain provider keys into the compound environment
/// binding the Add* registration reads. An explicit compound value always
/// wins: the bridge only fills blanks. Process memory only.
/// <param name="config">The merged configuration.</param>
let bridgeProviderKeys (config: IConfiguration) : unit =
    ArgumentNullException.ThrowIfNull(config)

    for id in bridgedIds () do
        match Option.ofObj (Environment.GetEnvironmentVariable(compoundVar id)) with
        | Some raw when not (String.IsNullOrWhiteSpace raw) -> ()
        | _ ->
            match Option.ofObj (getProviderKey config id) with
            | None -> ()
            | Some raw when String.IsNullOrWhiteSpace raw -> ()
            | Some value -> Environment.SetEnvironmentVariable(compoundVar id, value)

// ──────────────────────────────────────────────────────────────────────────
// Redacted dump

/// True when the key points inside an MCP credential map: every Headers,
// Env, or EnvironmentVariables map value masks regardless of its leaf
// name (Authorization matches no ApiKey/Secret/Token/Password pattern, so
// name-matching alone would print it).
/// <param name="key">The :-joined config key.</param>
/// <returns>True when the value must be masked.</returns>
let private isMcpCredentialMapKey (key: string) : bool =
    key.IndexOf(":headers:", StringComparison.OrdinalIgnoreCase) >= 0
    || key.IndexOf(":env:", StringComparison.OrdinalIgnoreCase) >= 0
    || key.IndexOf(":environmentvariables:", StringComparison.OrdinalIgnoreCase) >= 0

/// True when the key looks secret-like: it carries ApiKey, Secret, Token,
// or Password (case-insensitive), or it sits inside an MCP Headers/Env
// credential map.
/// <param name="key">The :-joined config key.</param>
/// <returns>True when the value must be masked.</returns>
let isSecretKey (key: string) : bool =
    not (isNull (box key))
    && (key.IndexOf("apikey", StringComparison.OrdinalIgnoreCase) >= 0
        || key.IndexOf("secret", StringComparison.OrdinalIgnoreCase) >= 0
        || key.IndexOf("token", StringComparison.OrdinalIgnoreCase) >= 0
        || key.IndexOf("password", StringComparison.OrdinalIgnoreCase) >= 0
        || isMcpCredentialMapKey key)

/// Collects the Dot and Legate sections of the merged configuration.
/// Only these prefixes dump: the full environment never does.
/// <param name="config">The merged configuration.</param>
/// <returns>The sorted key/value pairs.</returns>
let collectDumpPairs (config: IConfiguration) : (string * string) list =
    let pairs = ResizeArray<string * string>()

    let rec walk (section: IConfigurationSection) : unit =
        for child in section.GetChildren() do
            let kids = child.GetChildren() |> Seq.toList

            if kids.IsEmpty then
                let value =
                    match Option.ofObj child.Value with
                    | None -> ""
                    | Some text -> text

                pairs.Add(child.Path, value)
            else
                walk child

    walk (config.GetSection("Dot"))
    walk (config.GetSection("Legate"))
    pairs |> List.ofSeq |> List.sortBy fst

/// Prints the effective configuration with secret values masked, one
/// sorted key=value line per entry on stdout (pipe-clean). Resolved
/// values (flags, DOT_DB_PATH/DOT_WORKSPACE_ROOT aliases, defaults) win
/// over the raw merged entries under the same key.
/// <param name="config">The merged configuration.</param>
/// <param name="provider">The effective provider, or null.</param>
/// <param name="model">The effective model, or null.</param>
/// <param name="dbPath">The resolved database path.</param>
/// <param name="workspaceRoot">The resolved workspace root.</param>
/// <param name="ask">The effective ask default.</param>
let printEffective
    (config: IConfiguration)
    (provider: string | null)
    (model: string | null)
    (dbPath: string)
    (workspaceRoot: string)
    (ask: bool)
    : unit =
    ArgumentNullException.ThrowIfNull(config)

    let table = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)

    for key, value in collectDumpPairs config do
        table[key] <- value

    table["Dot:Provider"] <-
        match Option.ofObj provider with
        | None -> ""
        | Some raw -> raw

    table["Dot:Model"] <-
        match Option.ofObj model with
        | None -> ""
        | Some raw -> raw

    table["Dot:DbPath"] <- dbPath
    table["Dot:WorkspaceRoot"] <- workspaceRoot
    table["Dot:Ask"] <- if ask then "true" else "false"

    let ordered =
        table
        |> Seq.map (fun pair -> pair.Key, pair.Value)
        |> Seq.sortBy fst
        |> List.ofSeq

    for key, value in ordered do
        let shown = if isSecretKey key then "***" else value
        Console.Out.WriteLine($"{key}={shown}")

    Console.Out.Flush()
