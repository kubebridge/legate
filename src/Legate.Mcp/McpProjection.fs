// SPDX-License-Identifier: Apache-2.0
namespace Legate.Mcp

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Projection of discovered MCP tools onto AIFunctions. The model-facing
// name arrives already sanitized and collision-resolved; this module
// forwards description, the input schema as JsonSchema, the output schema
// as ReturnJsonSchema, and the annotation hints as AdditionalProperties.
// Only a real destructiveHint == true is projected, as boolean true under
// the "destructiveHint" key, which is exactly what
// AskForWritesAndExecPermissionPolicy reads to ask before running the
// tool. Invocation itself just forwards to the session owning the tool:
// full per-call semantics (fencing, errors, binary payloads) belong to the
// per-call follow-up, not this connection surface.

// ──────────────────────────
// Projected function

/// The AdditionalProperties key carrying the projected destructive hint.
/// Only a boolean true is ever stored; the permission policy asks exactly
/// then.
[<Sealed>]
type McpProjection() =

    /// The AdditionalProperties key carrying the projected destructive
    /// hint. Only a boolean true is ever stored under it.
    static member DestructiveHintKey: string = "destructiveHint"

/// An AIFunction projecting one discovered MCP tool: the sanitized name,
/// the server description, the server schemas, and the hint properties.
/// Internal: built only through <c>McpProjection.create</c>.
type internal McpProjectedFunction
    (
        name: string,
        description: string,
        inputSchema: JsonElement,
        outputSchema: Nullable<JsonElement>,
        properties: IReadOnlyDictionary<string, obj>,
        invoke: Func<AIFunctionArguments, CancellationToken, Task<string>>
    ) =
    inherit AIFunction()

    do
        ArgumentNullException.ThrowIfNull(name)
        ArgumentNullException.ThrowIfNull(properties)
        ArgumentNullException.ThrowIfNull(invoke)

    override _.Name: string = name
    override _.Description: string = description
    override _.JsonSchema: JsonElement = inputSchema
    override _.ReturnJsonSchema: Nullable<JsonElement> = outputSchema
    override _.AdditionalProperties: IReadOnlyDictionary<string, obj> = properties
    override _.JsonSerializerOptions: JsonSerializerOptions = JsonSerializerOptions.Default
    override _.UnderlyingMethod: Reflection.MethodInfo | null = null

    override _.InvokeCoreAsync(arguments: AIFunctionArguments, cancellationToken: CancellationToken) : ValueTask<obj> =
        ValueTask<obj>(
            task {
                let! text = invoke.Invoke(arguments, cancellationToken)
                // Invoke never yields null (the session guarantees text),
                // but coalesce anyway: the result must never be null.
                return (if isNull (box text) then "" else text) :> obj
            }
        )

// ──────────────────────────
// Factory

/// Builds projected AIFunctions from discovered tools. Internal.
module internal McpProjectionFactory =

    /// Boxes a value for AdditionalProperties. The MEAI surface types
    /// property values as non-null obj while servers may send nulls; the
    /// value rides through unchanged at runtime (null included), so this
    /// only carries the value past the nullness analysis.
    /// <param name="value">The value to box.</param>
    /// <returns>The value as a non-null obj.</returns>
    let private boxValue<'T> (value: 'T) : obj = unbox<obj> (box value)

    /// Builds the AdditionalProperties for one discovered tool: the title
    /// when sent, the annotation hints when sent, and
    /// <c>destructiveHint</c> as boolean true only for a real
    /// destructiveHint == true.
    /// <param name="discovered">The discovered tool.</param>
    /// <returns>The properties the projected function carries.</returns>
    let buildProperties (discovered: McpDiscovery.McpDiscoveredTool) : IReadOnlyDictionary<string, obj> =
        let properties = Dictionary<string, obj>(StringComparer.Ordinal)

        let title =
            if not (isNull (box discovered.Title)) then
                discovered.Title
            elif not (isNull (box discovered.AnnotationTitle)) then
                discovered.AnnotationTitle
            else
                null

        if not (isNull (box title)) then
            properties["title"] <- boxValue title

        properties["mcpServer"] <- boxValue discovered.ServerName
        properties["mcpTool"] <- boxValue discovered.ToolName

        match discovered.ReadOnlyHint with
        | Some flag -> properties["readOnlyHint"] <- boxValue flag
        | None -> ()

        match discovered.IdempotentHint with
        | Some flag -> properties["idempotentHint"] <- boxValue flag
        | None -> ()

        match discovered.OpenWorldHint with
        | Some flag -> properties["openWorldHint"] <- boxValue flag
        | None -> ()

        if discovered.DestructiveHint then
            properties[McpProjection.DestructiveHintKey] <- boxValue true

        properties :> IReadOnlyDictionary<string, obj>

    /// Projects one discovered tool onto an AIFunction under its assigned
    /// model-facing name.
    /// <param name="assignedName">The sanitized, collision-resolved model-facing name.</param>
    /// <param name="discovered">The discovered tool.</param>
    /// <param name="invoke">Forwards one call to the owning session. Per-call semantics belong to the follow-up; this surface only forwards.</param>
    /// <returns>The model-facing function.</returns>
    let create
        (assignedName: string)
        (discovered: McpDiscovery.McpDiscoveredTool)
        (invoke: Func<AIFunctionArguments, CancellationToken, Task<string>>)
        : AIFunction =
        ArgumentNullException.ThrowIfNull(assignedName)
        ArgumentNullException.ThrowIfNull(invoke)
        Legate.ToolNameRules.Validate assignedName |> ignore

        let description: string =
            if not (isNull (box discovered.Description)) then
                unbox<string> (box discovered.Description)
            elif not (isNull (box discovered.Title)) then
                unbox<string> (box discovered.Title)
            else
                assignedName

        let outputSchema =
            match discovered.OutputSchema with
            | Some schema -> Nullable<JsonElement>(schema)
            | None -> Nullable<JsonElement>()

        McpProjectedFunction(
            assignedName,
            description,
            discovered.InputSchema,
            outputSchema,
            buildProperties discovered,
            invoke
        )
        :> AIFunction
