// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open Microsoft.Extensions.AI

// Built-in permission policies: pure, synchronous, in-memory
// IPermissionPolicy implementations over the Permissions.fs contract. Every
// Evaluate is a host rule the runtime calls inline per tool call: no I/O, no
// clock, no randomness, and no secrets or tool arguments in deny reasons
// (the exception-message rule). Unknown PermissionDecisionKind values are
// rejected by PermissionsOptions.Validate and configuration binding, not by
// these policies; where a decision must still map, the mapping fails closed
// to Deny. Public signatures stay BCL-only (no option, list, or DU).
module internal PermissionPoliciesInternals =

    /// The AdditionalProperties key carrying the MCP ToolAnnotations
    /// destructiveHint projection. Only a boolean true triggers Ask.
    let destructiveHintKey = "destructiveHint"

    /// Reads the destructive projection off one tool: true only when the
    /// key is present with a boolean true value. Missing keys, non-bool
    /// values, and false never trigger.
    let isDestructive (tool: AITool) : bool =
        if isNull (box tool) then
            false
        elif isNull (box tool.AdditionalProperties) then
            false
        else
            let mutable boxed = Unchecked.defaultof<obj>

            if tool.AdditionalProperties.TryGetValue(destructiveHintKey, &boxed) then
                match boxed with
                | :? bool as flag -> flag
                | _ -> false
            else
                false

    /// Ordinal, case-sensitive glob match: '*' spans any sequence (including
    /// the empty sequence and characters such as ':'), '?' spans exactly one
    /// character, and every other character matches itself. A null pattern
    /// never matches; a null value matches as the empty string.
    let globMatches (pattern: string) (value: string) : bool =
        if isNull (box pattern) then
            false
        else
            let text = if isNull (box value) then "" else value
            let mutable px = 0
            let mutable vx = 0
            let mutable star = -1
            let mutable mark = 0
            let mutable failed = false

            while vx < text.Length && not failed do
                if px < pattern.Length && (pattern[px] = '?' || pattern[px] = text[vx]) then
                    px <- px + 1
                    vx <- vx + 1
                elif px < pattern.Length && pattern[px] = '*' then
                    star <- px
                    mark <- vx
                    px <- px + 1
                elif star <> -1 then
                    px <- star + 1
                    mark <- mark + 1
                    vx <- mark
                else
                    failed <- true

            while px < pattern.Length && pattern[px] = '*' do
                px <- px + 1

            not failed && px = pattern.Length

// ──────────────────────────────────────────────────────────────────────────
// AllowAll

/// The permission default that lets every tool call execute. Sealed so the
/// default cannot drift; register a real policy to gate calls.
[<Sealed>]
type AllowAllPermissionPolicy() =

    /// Allows every tool call.
    interface IPermissionPolicy with
        member _.Evaluate(_request: PermissionRequest) = PermissionVerdict.Allow

// ──────────────────────────────────────────────────────────────────────────
// AskForWritesAndExec

/// Asks the host before running tools that change the world: the closed
/// write/exec set (<c>write_file</c>, <c>write_binary_base64</c>,
/// <c>edit_file</c>, <c>exec</c>) and any tool whose
/// <c>AdditionalProperties["destructiveHint"]</c> is boolean true (the MCP
/// ToolAnnotations destructiveHint projection). Everything else allows.
/// Matching is ordinal and case-sensitive. The policy is synchronous and
/// in-memory: it reads only the request tool name and the tool table handed
/// to the constructor, never I/O. A null tool table disables the
/// destructive lookup while the closed set still asks.
/// <param name="tools">The session tool table for the destructive lookup, or null for the closed set only.</param>
[<Sealed>]
type AskForWritesAndExecPermissionPolicy(tools: IReadOnlyDictionary<string, AITool> | null) =

    static let writeTools =
        HashSet<string>(
            [|
                "write_file"
                "write_binary_base64"
                "edit_file"
                "exec"
            |],
            StringComparer.Ordinal
        )

    /// Creates a policy over the closed write/exec set only: no destructive
    /// lookup, so only the four write/exec names ask.
    new() = AskForWritesAndExecPermissionPolicy(null)

    /// Asks for closed-set and destructively annotated tools, allows the
    /// rest.
    interface IPermissionPolicy with
        member _.Evaluate(request: PermissionRequest) =
            ArgumentNullException.ThrowIfNull(request)

            let name =
                if isNull (box request.ToolName) then
                    ""
                else
                    request.ToolName

            if writeTools.Contains name then
                PermissionVerdict.Ask
            else
                match Option.ofObj tools with
                | None -> PermissionVerdict.Allow
                | Some table ->
                    let mutable tool: AITool = Unchecked.defaultof<AITool>

                    if table.TryGetValue(name, &tool) && PermissionPoliciesInternals.isDestructive tool then
                        PermissionVerdict.Ask
                    else
                        PermissionVerdict.Allow

// ──────────────────────────────────────────────────────────────────────────
// RuleBased

/// Evaluates <c>PermissionsOptions.Rules</c> in order against the request
/// tool name: the first rule whose <c>ToolPattern</c> glob-matches wins.
/// Glob matching is ordinal and case-sensitive (<c>*</c> spans any sequence
/// including <c>:</c> so <c>mcp:github:*</c> works, <c>?</c> spans one
/// character). <see cref="F:Legate.PermissionDecisionKind.AllowOnce" /> and
/// <see cref="F:Legate.PermissionDecisionKind.AllowForSession" /> both map to
/// <see cref="T:Legate.AllowVerdict" /> (AllowForSession memory is the
/// runtime's, never the policy's);
/// <see cref="F:Legate.PermissionDecisionKind.Deny" /> maps to
/// <see cref="T:Legate.DenyVerdict" /> with a fixed reason naming the
/// pattern only, never secrets or tool arguments. No rule match falls back
/// to <c>DefaultDecision</c> mapped the same way (the default-deny reason is
/// fixed likewise). Rules cannot express Ask: the decision kind has no Ask
/// case, so asking comes only from host-reply policies. The policy snapshots
/// the rules and default at construction: later option mutation never moves
/// an Evaluate. Unknown decisions fail closed to Deny; hosts reject them
/// up front with <c>PermissionsOptions.Validate</c>.
/// <param name="options">The permission options to bind: rules in order plus the default decision. Must not be null.</param>
[<Sealed>]
type RuleBasedPermissionPolicy(options: PermissionsOptions) =

    do ArgumentNullException.ThrowIfNull(options)

    let rules: (string * PermissionDecisionKind)[] =
        if isNull (box options.Rules) then
            [||]
        else
            options.Rules
            |> Seq.choose (fun rule ->
                if isNull (box rule) then
                    None
                elif String.IsNullOrEmpty rule.ToolPattern then
                    None
                else
                    match Option.ofObj rule.ToolPattern with
                    | Some pattern -> Some(pattern, rule.Decision)
                    | None -> None)
            |> Array.ofSeq

    let defaultDecision = options.DefaultDecision

    let toVerdict (pattern: string) (decision: PermissionDecisionKind) : PermissionVerdict =
        match decision with
        | PermissionDecisionKind.AllowOnce
        | PermissionDecisionKind.AllowForSession -> PermissionVerdict.Allow
        | _ -> PermissionVerdict.Deny($"Denied by permission rule '{pattern}'.")

    let defaultVerdict =
        match defaultDecision with
        | PermissionDecisionKind.AllowOnce
        | PermissionDecisionKind.AllowForSession -> PermissionVerdict.Allow
        | _ -> PermissionVerdict.Deny("Denied by the default permission decision.")

    /// Applies the first matching rule, or the default when none matches.
    interface IPermissionPolicy with
        member _.Evaluate(request: PermissionRequest) =
            ArgumentNullException.ThrowIfNull(request)

            let name =
                if isNull (box request.ToolName) then
                    ""
                else
                    request.ToolName

            let mutable verdict = defaultVerdict
            let mutable index = 0

            while index < rules.Length do
                let pattern, decision = rules[index]

                if PermissionPoliciesInternals.globMatches pattern name then
                    verdict <- toVerdict pattern decision
                    index <- rules.Length
                else
                    index <- index + 1

            verdict
