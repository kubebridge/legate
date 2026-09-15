// SPDX-License-Identifier: Apache-2.0
module Legate.Mcp.McpNaming

open System

// Sanitized {server}_{tool} names plus the Fail/Suffix cross-server
// collision policy. Sanitizing replaces every character outside
// [a-zA-Z0-9_-] with an underscore and truncates to 128 characters, then
// pins the result through ToolNameRules.Validate so the source, the
// runtime, and every consumer agree on the single name rule. Fail (the
// default) degrades the whole source to zero tools on any duplicate;
// Suffix renames deterministically with _2, _3 suffixes truncated back to
// 128 and re-validated, failing on a residual collision.

// ──────────────────────────
// Naming

/// The maximum tool-name length, matching
/// <see cref="P:Legate.ToolNameRules.Pattern" />.
let MaxNameLength = 128

/// Replaces every character outside [a-zA-Z0-9_-] with an underscore and
/// truncates to 128 characters. A null or all-invalid value yields
/// underscores, never an empty string, so the result always feeds
/// ToolNameRules.Validate.
/// <param name="value">The raw text to sanitize, or null.</param>
/// <returns>The sanitized text, one to 128 characters of [a-zA-Z0-9_-].</returns>
let sanitize (value: string | null) : string =
    // The type test narrows past the nullness analysis: null never
    // matches :? string, so kept is non-null.
    let text: string =
        match box value with
        | :? string as kept -> kept
        | _ -> ""

    let buffer = Array.zeroCreate<char> (min text.Length MaxNameLength)
    let mutable length = 0
    let mutable index = 0

    while index < text.Length && length < MaxNameLength do
        let c = text[index]

        buffer[length] <-
            if
                (c >= 'a' && c <= 'z')
                || (c >= 'A' && c <= 'Z')
                || (c >= '0' && c <= '9')
                || c = '_'
                || c = '-'
            then
                c
            else
                '_'

        length <- length + 1
        index <- index + 1

    if length = 0 then "_" else String(buffer, 0, length)

/// Builds the model-facing name for one server tool: the sanitized
/// <c>{server}_{tool}</c> pair, validated against the shared tool-name
/// rule.
/// <param name="serverName">The owning server's name.</param>
/// <param name="toolName">The server's tool name.</param>
/// <returns>The sanitized, validated model-facing name.</returns>
let buildServerToolName (serverName: string) (toolName: string) : string =
    sanitize (serverName + "_" + toolName) |> Legate.ToolNameRules.Validate

// ──────────────────────────
// Collisions

/// Assigns the final model-facing names for one discovery pass under the
/// configured policy: Fail reports the first duplicate, Suffix renames
/// deterministically with _2, _3 suffixes truncated to 128 characters and
/// re-validated. Input order is preserved.
/// <param name="policy">How duplicate sanitized names resolve.</param>
/// <param name="names">The sanitized names in discovery order.</param>
/// <returns>The assigned names in discovery order, or the first collision's message.</returns>
let resolve (policy: McpNameCollisionPolicy) (names: string list) : Result<string list, string> =
    match policy with
    | McpNameCollisionPolicy.Fail ->
        let seen = System.Collections.Generic.HashSet<string>(StringComparer.Ordinal)
        let mutable duplicate: string option = None

        for name in names do
            if duplicate.IsNone && not (seen.Add(name)) then
                duplicate <- Some name

        match duplicate with
        | Some name ->
            Error
                $"Duplicate MCP tool name '{name}': set Legate:Tools:Mcp:CollisionPolicy to Suffix to rename, or give the servers disjoint tools."
        | None -> Ok names
    | McpNameCollisionPolicy.Suffix ->
        let used = System.Collections.Generic.HashSet<string>(StringComparer.Ordinal)
        let assigned = System.Collections.Generic.List<string>()
        let mutable failure: string option = None

        for name in names do
            if failure.IsNone then
                if used.Add(name) then
                    assigned.Add(name)
                else
                    let mutable placed: string option = None
                    let mutable suffix = 2

                    while placed.IsNone && failure.IsNone do
                        if suffix > 9999 then
                            failure <- Some $"Duplicate MCP tool name '{name}': suffix space exhausted."

                        if failure.IsNone then
                            let tail = "_" + string suffix
                            let headLength = MaxNameLength - tail.Length

                            let head =
                                if name.Length > headLength then
                                    name.Substring(0, headLength)
                                else
                                    name

                            let candidate = head + tail

                            if Legate.ToolNameRules.TryValidate candidate && used.Add(candidate) then
                                placed <- Some candidate
                            else
                                suffix <- suffix + 1

                    match placed with
                    | Some candidate -> assigned.Add(candidate)
                    | None ->
                        if failure.IsNone then
                            failure <- Some $"Duplicate MCP tool name '{name}': suffix renames still collide."

        match failure with
        | Some message -> Error message
        | None -> Ok(assigned |> Seq.toList)
    | _ -> Error "CollisionPolicy must be Fail or Suffix."
