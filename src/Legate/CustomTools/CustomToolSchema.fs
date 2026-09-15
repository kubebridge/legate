// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Text.Json

// Input-schema resolution for agent custom tools (issue 74). Each tool's
// InputSchema text is parsed once at load: a JSON object (or boolean)
// serves as the tool's JsonSchema verbatim, while null or blank means the
// host chose the permissive fallback silently. Anything else is invalid and
// falls back to the permissive object schema with a model-visible
// diagnostic suffix on the tool description, plus a host-side Warning
// carrying only the parse reason. The schema text, secrets, headers, and
// arguments are never logged anywhere on this path.

/// How one custom tool's input schema resolved at load. Internal: the
/// custom tool source owns the only call site.
type internal SchemaResolution =
    {
        /// The parsed schema document serving the tool's JsonSchema. The
        /// tool function holds this reference so the served RootElement
        /// never borrows a collected document; per-tool documents are
        /// owned by their function, the permissive document is process-wide
        /// and never disposed.
        Schema: JsonDocument
        /// The model-visible description suffix: empty when the schema
        /// served as configured, the fallback diagnostic otherwise.
        Diagnostic: string
        /// The schema-parse reason for the host Warning log, or None when
        /// nothing is logged. Carries the reason only, never the schema
        /// text, secrets, headers, or arguments.
        ParseReason: string option
    }

/// Input-schema parsing with the permissive fallback. Internal: the custom
/// tool source owns the only call site.
module internal CustomToolSchema =

    /// The permissive fallback schema served when the host configures none
    /// or configures an invalid one: any JSON object.
    [<Literal>]
    let PermissiveJson =
        """{"type":"object","description":"Accepts any JSON object."}"""

    /// The single parsed permissive document every fallback shares.
    /// Process-wide and never disposed.
    let permissiveDocument = JsonDocument.Parse PermissiveJson

    /// The model-visible diagnostic suffix appended to a tool's
    /// description when its configured schema was invalid.
    [<Literal>]
    let FallbackDiagnostic = "Input schema was invalid; accepting any JSON object."

    /// Resolves one tool's InputSchema text: null or blank takes the
    /// permissive fallback silently (the host's explicit choice), a JSON
    /// object or boolean serves verbatim, and anything else falls back
    /// with the diagnostic suffix and the parse reason for the host log.
    /// Never throws: unknown failures fall back with the failure type as
    /// the reason, which carries no schema text.
    /// <param name="inputSchema">The configured schema text, or null.</param>
    /// <returns>How the schema resolved.</returns>
    let resolve (inputSchema: string | null) : SchemaResolution =
        let fallback (reason: string option) : SchemaResolution =
            match reason with
            | None ->
                {
                    Schema = permissiveDocument
                    Diagnostic = ""
                    ParseReason = None
                }
            | Some _ ->
                {
                    Schema = permissiveDocument
                    Diagnostic = " " + FallbackDiagnostic
                    ParseReason = reason
                }

        match Option.ofObj inputSchema with
        | None -> fallback None
        | Some text when String.IsNullOrWhiteSpace text -> fallback None
        | Some text ->
            try
                let document = JsonDocument.Parse text

                match document.RootElement.ValueKind with
                | JsonValueKind.Object
                | JsonValueKind.True
                | JsonValueKind.False ->
                    {
                        Schema = document
                        Diagnostic = ""
                        ParseReason = None
                    }
                | _ ->
                    document.Dispose()
                    fallback (Some "The input schema must be a JSON object or boolean.")
            with
            | :? JsonException as invalid -> fallback (Some invalid.Message)
            | unexpected -> fallback (Some(unexpected.GetType().Name))
