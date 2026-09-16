// SPDX-License-Identifier: Apache-2.0
module McpFixture.Program

open System
open System.Text.Json
open System.Text.Json.Nodes
open System.Threading.Tasks

// Scripted MCP server over stdio for the LegateCli end-to-end check
// (transferred from #76): speaks JSON-RPC 2.0 with the initialize,
// tools/list, tools/call, and ping methods over newline-delimited frames
// (LSP Content-Length framing is accepted too), echoing the client's
// protocol version so version negotiation always agrees. One tool,
// "echo", returning "fixture echo: <text>". Stdout carries protocol
// frames only; diagnostics go to stderr so a stray log line never
// corrupts the stream.

/// Parses JSON into a detached node, falling back to an empty string
/// node for null documents (request ids are never null here).
let private parseNode (json: string) : JsonNode =
    match Option.ofObj (JsonNode.Parse(json)) with
    | Some node -> node
    | None ->
        match Option.ofObj (JsonValue.Create("")) with
        | Some empty -> empty :> JsonNode
        | None -> raise (InvalidOperationException("The JSON factory returned no node."))

/// Frames a response around the request id node, detached from the
/// request document.
let private frame (id: JsonElement) (result: JsonNode | null) (error: JsonNode | null) : string =
    let root = JsonObject()
    root["jsonrpc"] <- JsonValue.Create("2.0")
    root["id"] <- parseNode (id.GetRawText())

    match box result with
    | null -> ()
    | _ -> root["result"] <- result

    match box error with
    | null -> ()
    | _ -> root["error"] <- error

    root.ToJsonString()

let private respond (id: JsonElement) (result: JsonObject) : Task =
    task {
        do! Console.Out.WriteLineAsync(frame id result null)
        do! Console.Out.FlushAsync()
    }

let private fail (id: JsonElement) (code: int) (message: string) : Task =
    let error = JsonObject()
    error["code"] <- JsonValue.Create(code)
    error["message"] <- JsonValue.Create(message)

    task {
        do! Console.Out.WriteLineAsync(frame id null error)
        do! Console.Out.FlushAsync()
    }

let private tryGet (root: JsonElement) (name: string) : JsonElement option =
    let mutable value = Unchecked.defaultof<JsonElement>

    if root.ValueKind = JsonValueKind.Object && root.TryGetProperty(name, &value) then
        Some value
    else
        None

let private textValue (element: JsonElement) : string | null =
    if element.ValueKind = JsonValueKind.String then
        element.GetString()
    else
        null

let private objectOrEmpty (root: JsonElement) (name: string) : JsonElement =
    match tryGet root name with
    | Some parameters when parameters.ValueKind = JsonValueKind.Object -> parameters
    | _ -> JsonDocument.Parse("{}").RootElement

let private handleInitialize (id: JsonElement) (paramsOf: JsonElement) : Task =
    let version: string =
        match tryGet paramsOf "protocolVersion" with
        | Some asked ->
            match Option.ofObj (textValue asked) with
            | Some text when not (String.IsNullOrWhiteSpace text) -> text
            | _ -> "2024-11-05"
        | None -> "2024-11-05"

    let tools = JsonObject()
    let capabilities = JsonObject()
    capabilities["tools"] <- tools

    let serverInfo = JsonObject()
    serverInfo["name"] <- JsonValue.Create("legate-fixture")
    serverInfo["version"] <- JsonValue.Create("0.1.0")

    let result = JsonObject()
    result["protocolVersion"] <- JsonValue.Create(version)
    result["capabilities"] <- capabilities
    result["serverInfo"] <- serverInfo

    respond id result

let private handleListTools (id: JsonElement) : Task =
    let textProperty = JsonObject()
    textProperty["type"] <- JsonValue.Create("string")

    let properties = JsonObject()
    properties["text"] <- textProperty

    let required = JsonArray()
    required.Add(JsonValue.Create("text"))

    let schema = JsonObject()
    schema["type"] <- JsonValue.Create("object")
    schema["properties"] <- properties
    schema["required"] <- required

    let tool = JsonObject()
    tool["name"] <- JsonValue.Create("echo")
    tool["description"] <- JsonValue.Create("Echoes the input text back with a fixture prefix.")
    tool["inputSchema"] <- schema

    let tools = JsonArray()
    tools.Add(tool)

    let result = JsonObject()
    result["tools"] <- tools

    respond id result

let private handleCallTool (id: JsonElement) (paramsOf: JsonElement) : Task =
    task {
        let name =
            match tryGet paramsOf "name" with
            | Some called -> textValue called
            | None -> null

        if not (String.Equals(name, "echo", StringComparison.Ordinal)) then
            return! fail id -32602 $"Unknown tool '{name}'."
        else
            let arguments = objectOrEmpty paramsOf "arguments"

            let text =
                match tryGet arguments "text" with
                | Some value -> textValue value
                | None -> null

            if isNull (box text) then
                return! fail id -32602 "The echo tool needs a string 'text' argument."
            else
                let content = JsonObject()
                content["type"] <- JsonValue.Create("text")
                content["text"] <- JsonValue.Create($"fixture echo: %s{text}")

                let contents = JsonArray()
                contents.Add(content)

                let result = JsonObject()
                result["content"] <- contents

                return! respond id result
    }

let private handle (root: JsonElement) : Task =
    task {
        let hasId =
            match tryGet root "id" with
            | Some id when id.ValueKind <> JsonValueKind.Null -> Some id
            | _ -> None

        let method =
            match tryGet root "method" with
            | Some name -> textValue name
            | None -> null

        if String.IsNullOrWhiteSpace method then
            match hasId with
            | Some id -> return! fail id -32600 "Missing method."
            | None -> return ()
        else
            let paramsOf = objectOrEmpty root "params"

            match method with
            | "initialize" ->
                match hasId with
                | Some id -> return! handleInitialize id paramsOf
                | None -> return ()
            | "tools/list" ->
                match hasId with
                | Some id -> return! handleListTools id
                | None -> return ()
            | "tools/call" ->
                match hasId with
                | Some id -> return! handleCallTool id paramsOf
                | None -> return ()
            | "ping" ->
                match hasId with
                | Some id -> return! respond id (JsonObject())
                | None -> return ()
            | _ ->
                // Unknown notifications are ignored; unknown requests fail
                // with MethodNotFound so the client degrades explicitly.
                match hasId with
                | Some id -> return! fail id -32601 $"Method not found: {method}."
                | None -> return ()
    }

/// Reads one frame: LSP Content-Length framing when the line opens with
/// it, else the line itself. Returns None at end of stream.
let private readFrame () : Task<string | null> =
    task {
        let! raw = Console.In.ReadLineAsync()

        match raw with
        | null -> return null
        | line when line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase) ->
            let lengthText = line.Substring("Content-Length:".Length).Trim()

            match Int32.TryParse(lengthText) with
            | false, _ -> return null
            | true, length ->
                // Consume the header terminator, then the body chars. The
                // body is UTF-8 JSON; reading per char keeps the sample
                // dependency-free.
                let mutable blank = Console.In.ReadLine()

                while not (isNull (box blank)) && blank <> "" do
                    blank <- Console.In.ReadLine()

                let buffer = Array.zeroCreate<char> length
                let mutable read = 0

                while read < length do
                    let! chunk = Console.In.ReadAsync(buffer, read, length - read)

                    if chunk = 0 then read <- length else read <- read + chunk

                let body: string | null = String(buffer, 0, read)
                return body
        | line ->
            let out: string | null = line
            return out
    }

[<EntryPoint>]
let main _ : int =
    let runAsync () : Task =
        task {
            let mutable go = true

            while go do
                let! frame = readFrame ()

                match frame with
                | null -> go <- false
                | text when String.IsNullOrWhiteSpace text -> ()
                | text ->
                    try
                        use document = JsonDocument.Parse(text)
                        do! handle document.RootElement
                    with error ->
                        Console.Error.WriteLine($"legate-fixture: dropping a malformed frame: {error.Message}")
        }

    try
        runAsync().GetAwaiter().GetResult()
        0
    with error ->
        Console.Error.WriteLine($"legate-fixture: fatal: {error.Message}")
        1
