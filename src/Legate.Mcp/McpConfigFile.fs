// SPDX-License-Identifier: Apache-2.0
namespace Legate.Mcp

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Text.Json

// Reads a Claude-style mcp.json file: a top-level mcpServers map whose
// entries carry command/args/env (stdio) or url/headers (streamable HTTP).
// Transport is inferred exactly as McpServerOptions.IsStdio/IsHttp infer
// it: a non-empty command with no URL is stdio, a non-empty URL with no
// command is HTTP, and both-set or neither-set is a startup error naming
// the entry key. Every other field (type, cwd, and any editor-specific
// extras) is inert: it never selects a transport. ${VAR} placeholders
// expand from the process environment in command, every args element,
// every env value, url, and every headers value; an unset or empty
// variable fails startup naming the server key and the variable name.
// Expanded values never appear in error messages. Files are read as UTF-8
// (a byte-order mark is accepted) and JSON errors name the file path.

// JSON shape handling and placeholder expansion for mcp.json files.
// Internal: the single public entry point is
// ToolsBuilder.AddMcpServersFromConfig, which fails fast through here.
module internal McpConfigFile =

    /// Fails loading with a startup error naming the entry key. Values
    /// never flow through here, so secrets cannot leak into messages.
    let private failEntry key detail =
        raise (InvalidOperationException($"Invalid mcpServers entry '{key}': {detail}"))

    /// True for plain environment-variable names: a letter or underscore
    /// followed by letters, digits, or underscores. Only ${NAME} with a
    /// name of this shape expands; every other $-sequence stays literal.
    /// In particular ${VAR:-default}, $VAR, and ~/tilde never expand.
    let private isVariableName (name: string) : bool =
        not (String.IsNullOrEmpty name)
        && (Char.IsLetter name[0] || name[0] = '_')
        && Seq.forall (fun c -> Char.IsLetterOrDigit c || c = '_') name

    /// Expands ${NAME} placeholders in one configured value from the
    /// process environment. An unset or empty variable fails startup
    /// naming the server key and the variable name; the value itself is
    /// never logged or embedded in the message.
    /// <param name="key">The mcpServers key owning the value.</param>
    /// <param name="value">The raw configured value; never null.</param>
    /// <returns>The expanded value.</returns>
    let private expandText (key: string) (value: string) : string =
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
                        match Environment.GetEnvironmentVariable(name) with
                        | null
                        | "" -> failEntry key $"environment variable '{name}' is not set or empty."
                        | expanded -> output.Append(expanded) |> ignore
                    else
                        output.Append(value.Substring(index, close - index + 1)) |> ignore

                    index <- close + 1
            else
                output.Append(value[index]) |> ignore
                index <- index + 1

        output.ToString()

    /// Expands an optional value, preserving absence as null.
    let private expandOptional (key: string) (value: string | null) : string | null =
        match value with
        | null -> null
        | text -> expandText key text

    /// Reads an optional string field: missing and JSON null both read as
    /// null (absent); any other non-string shape fails naming the key.
    let private fieldString (key: string) (element: JsonElement) (name: string) : string | null =
        let mutable found = Unchecked.defaultof<JsonElement>

        if
            not (element.TryGetProperty(name, &found))
            || found.ValueKind = JsonValueKind.Null
        then
            null
        elif found.ValueKind = JsonValueKind.String then
            match found.GetString() with
            | null -> failEntry key $"'{name}' must be a string."
            | text -> text
        else
            failEntry key $"'{name}' must be a string."

    /// Reads the text of a JSON string element, failing the entry when
    /// the runtime yields null (only possible for non-string kinds, which
    /// the callers already exclude by ValueKind).
    let private stringText (key: string) (name: string) (element: JsonElement) : string =
        match element.GetString() with
        | null -> failEntry key $"'{name}' must be a string."
        | text -> text

    /// Reads an optional string-array field with placeholders expanded:
    /// missing and JSON null read as empty; any non-string element fails
    /// naming the key.
    let private fieldList (key: string) (element: JsonElement) (name: string) : ResizeArray<string> =
        let values = ResizeArray<string>()
        let mutable found = Unchecked.defaultof<JsonElement>

        if element.TryGetProperty(name, &found) && found.ValueKind <> JsonValueKind.Null then
            if found.ValueKind <> JsonValueKind.Array then
                failEntry key $"'{name}' must be an array of strings."

            for item in found.EnumerateArray() do
                if item.ValueKind <> JsonValueKind.String then
                    failEntry key $"'{name}' must be an array of strings."

                values.Add(expandText key (stringText key name item))

        values

    /// Reads an optional string-map field with placeholders expanded in
    /// the values: missing and JSON null read as empty; any non-string
    /// value fails naming the key. Keys stay literal: only values expand.
    let private fieldMap (key: string) (element: JsonElement) (name: string) : Dictionary<string, string> =
        let values = Dictionary<string, string>(StringComparer.Ordinal)
        let mutable found = Unchecked.defaultof<JsonElement>

        if element.TryGetProperty(name, &found) && found.ValueKind <> JsonValueKind.Null then
            if found.ValueKind <> JsonValueKind.Object then
                failEntry key $"'{name}' must be an object with string values."

            for property in found.EnumerateObject() do
                if property.Value.ValueKind <> JsonValueKind.String then
                    failEntry key $"'{name}' must be an object with string values."

                values[property.Name] <- expandText key (stringText key name property.Value)

        values

    /// Maps one mcpServers entry onto McpServerOptions: expands the five
    /// accepted fields, infers stdio from command versus HTTP from url,
    /// and validates the result, failing naming the key throughout.
    /// Fields for the other transport (for example headers on a stdio
    /// entry) are ignored.
    let private loadEntry (key: string) (element: JsonElement) : McpServerOptions =
        if element.ValueKind <> JsonValueKind.Object then
            failEntry key "the entry must be an object with optional 'command', 'args', 'env', 'url', and 'headers'."

        let command = expandOptional key (fieldString key element "command")
        let url = expandOptional key (fieldString key element "url")
        let hasCommand = not (String.IsNullOrWhiteSpace command)
        let hasUrl = not (String.IsNullOrWhiteSpace url)

        if hasCommand && hasUrl then
            failEntry key "set exactly one transport: 'command' for stdio or 'url' for streamable HTTP, not both."
        elif not (hasCommand || hasUrl) then
            failEntry key "set exactly one transport: 'command' for stdio or 'url' for streamable HTTP."

        let server = McpServerOptions(Name = key)

        if hasCommand then
            server.Command <- command
            server.Arguments <- fieldList key element "args" :> IList<string>
            server.EnvironmentVariables <- fieldMap key element "env" :> IDictionary<string, string>
        else
            server.Url <- url
            server.Headers <- fieldMap key element "headers" :> IDictionary<string, string>

        match server.Validate() with
        | null -> server
        | problem -> failEntry key problem

    /// Loads the mcpServers map from a JSON file into server options in
    /// file order. Missing files and malformed JSON fail naming the path;
    /// invalid entries and unset placeholders fail naming the entry key.
    /// <param name="path">The mcp.json file path.</param>
    /// <returns>The server options, one per mcpServers entry.</returns>
    let loadServers (path: string) : McpServerOptions list =
        ArgumentNullException.ThrowIfNull(path)

        if not (File.Exists(path)) then
            raise (InvalidOperationException($"MCP server configuration file '{path}' was not found."))

        let text = File.ReadAllText(path, Encoding.UTF8)

        use parsed =
            try
                JsonDocument.Parse(text)
            with :? JsonException as ex ->
                raise (
                    InvalidOperationException($"MCP server configuration file '{path}' is not valid JSON: {ex.Message}")
                )

        let root = parsed.RootElement
        let mutable serversElement = Unchecked.defaultof<JsonElement>

        if root.ValueKind <> JsonValueKind.Object then
            raise (
                InvalidOperationException(
                    $"MCP server configuration file '{path}' must contain an 'mcpServers' object with one entry per server."
                )
            )

        if
            not (root.TryGetProperty("mcpServers", &serversElement))
            || serversElement.ValueKind <> JsonValueKind.Object
        then
            raise (
                InvalidOperationException(
                    $"MCP server configuration file '{path}' must contain an 'mcpServers' object with one entry per server."
                )
            )

        [
            for entry in serversElement.EnumerateObject() do
                yield loadEntry entry.Name entry.Value
        ]
