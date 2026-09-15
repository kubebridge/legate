// SPDX-License-Identifier: Apache-2.0
module internal Legate.Mcp.McpDiscovery

open System
open System.Text.Json

// The SDK-free shape of one discovered MCP tool. The connector (the only
// module referencing ModelContextProtocol types) maps each protocol Tool
// onto this record, so naming, projection, and the tool source never touch
// SDK types: the adapter isolation containing SDK 2.x drift. Hints are
// plain values, never SDK nullables on the surface of this module's
// internal callers: DestructiveHint is true only for a real
// destructiveHint == true, matching the permission policy's trigger.

// ──────────────────────────
// Discovered tool

/// One MCP tool as the source sees it: identity plus the metadata the
/// projection forwards. Internal: the tool source projects these onto
/// AIFunctions.
type McpDiscoveredTool =
    {
        /// The owning server's configured name.
        ServerName: string
        /// The tool name as the server reported it.
        ToolName: string
        /// The human-readable title, when the server sent one.
        Title: string | null
        /// The human-readable description, when the server sent one.
        Description: string | null
        /// The JSON Schema for the tool's parameters.
        InputSchema: JsonElement
        /// The JSON Schema for the tool's structured output, when sent.
        OutputSchema: JsonElement option
        /// True only for a real annotations destructiveHint == true.
        DestructiveHint: bool
        /// The annotations readOnlyHint, when sent.
        ReadOnlyHint: bool option
        /// The annotations idempotentHint, when sent.
        IdempotentHint: bool option
        /// The annotations openWorldHint, when sent.
        OpenWorldHint: bool option
        /// The annotations title, when sent.
        AnnotationTitle: string | null
    }

// ──────────────────────────
// Schemas

/// The minimal parameters schema used when a server sends no usable
/// input schema.
let private defaultInputSchemaDocument = JsonDocument.Parse("""{"type":"object"}""")

/// The minimal parameters schema used when a server sends no usable
/// input schema: <c>{"type":"object"}</c>.
let DefaultInputSchema: JsonElement = defaultInputSchemaDocument.RootElement

/// Parses a server-sent input schema, falling back to the default object
/// schema when the text is missing or not a JSON object with a schema
/// shape.
/// <param name="json">The raw schema text, or null.</param>
/// <returns>The parsed schema, or the default object schema.</returns>
let parseInputSchema (json: string | null) : JsonElement =
    // The type test narrows past the nullness analysis: null never
    // matches :? string, so text is non-null.
    match box json with
    | :? string as text when not (String.IsNullOrWhiteSpace text) ->
        try
            let document = JsonDocument.Parse text
            let root = document.RootElement

            if root.ValueKind = JsonValueKind.Object then
                root
            else
                DefaultInputSchema
        with
        | :? JsonException -> DefaultInputSchema
        | :? ArgumentException -> DefaultInputSchema
    | _ -> DefaultInputSchema
