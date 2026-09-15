// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.PermissionPoliciesTests

open System
open System.Collections.Generic
open System.Text.Json
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Xunit

let nullString = Unchecked.defaultof<string>

let sampleSessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let sampleTurnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"

// The preview sentinel stands in for redacted arguments: deny reasons must
// never carry it (the exception-message rule), so reason tests assert its
// absence alongside the request id.
let requestFor (toolName: string) : PermissionRequest =
    {
        SessionId = sampleSessionId
        TurnId = sampleTurnId
        ToolName = toolName
        ToolSourceId = nullString
        ArgumentPreview = "preview-sentinel-must-never-leak"
        RequestId = "req-sentinel-1"
    }

let evaluate (policy: IPermissionPolicy) (toolName: string) : PermissionVerdict = policy.Evaluate(requestFor toolName)

let ruleBased (rules: (string * PermissionDecisionKind) list) (defaultDecision: PermissionDecisionKind) =
    let options = PermissionsOptions(DefaultDecision = defaultDecision)

    for pattern, decision in rules do
        options.Rules.Add(PermissionRuleOptions(ToolPattern = pattern, Decision = decision))

    RuleBasedPermissionPolicy(options) :> IPermissionPolicy

let toolWithHint (name: string) (hint: (obj | null) option) : AITool =
    let method = Func<string>(fun () -> "ok")
    let factoryOptions = AIFunctionFactoryOptions(Name = name)

    match hint with
    // Factory tools share an immutable empty AdditionalProperties table, so
    // hand the hint in through the factory options, mirroring how an MCP
    // source annotates its tools.
    | Some value ->
        let props = AdditionalPropertiesDictionary()
        props.Add("destructiveHint", value)
        factoryOptions.AdditionalProperties <- props :> IReadOnlyDictionary<string, obj | null>
    | None -> ()

    AIFunctionFactory.Create(method, factoryOptions) :> AITool

let toolsOf (pairs: (string * AITool) list) : IReadOnlyDictionary<string, AITool> =
    let table = Dictionary<string, AITool>()

    for name, tool in pairs do
        table[name] <- tool

    table :> IReadOnlyDictionary<string, AITool>

// ──────────────────────────────────────────────────────────────────────────
// AllowAllPermissionPolicy

[<Fact>]
let ``AllowAll allows read tools`` () =
    let policy = AllowAllPermissionPolicy() :> IPermissionPolicy

    for name in
        [
            "read_file"
            "list_files"
            "glob"
            "grep"
        ] do
        (evaluate policy name) :? AllowVerdict |> should equal true

[<Fact>]
let ``AllowAll allows write and exec tools`` () =
    let policy = AllowAllPermissionPolicy() :> IPermissionPolicy

    for name in
        [
            "write_file"
            "write_binary_base64"
            "edit_file"
            "exec"
            "mcp:github:get_issue"
        ] do
        (evaluate policy name) :? AllowVerdict |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// AskForWritesAndExecPermissionPolicy: closed set

[<Fact>]
let ``Ask asks for every closed write and exec name`` () =
    let policy = AskForWritesAndExecPermissionPolicy() :> IPermissionPolicy

    for name in
        [
            "write_file"
            "write_binary_base64"
            "edit_file"
            "exec"
        ] do
        (evaluate policy name) :? AskVerdict |> should equal true

[<Fact>]
let ``Ask allows reads and out-of-scope tools`` () =
    let policy = AskForWritesAndExecPermissionPolicy() :> IPermissionPolicy

    for name in
        [
            "read_file"
            "read_binary_base64"
            "list_files"
            "glob"
            "grep"
            "get_download_url"
            "ask_user"
        ] do
        (evaluate policy name) :? AllowVerdict |> should equal true

[<Fact>]
let ``Ask matches the closed set ordinally`` () =
    let policy = AskForWritesAndExecPermissionPolicy() :> IPermissionPolicy

    (evaluate policy "WRITE_FILE") :? AllowVerdict |> should equal true
    (evaluate policy "Exec") :? AllowVerdict |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// AskForWritesAndExecPermissionPolicy: destructiveHint

[<Fact>]
let ``Ask asks when destructiveHint is boolean true`` () =
    let tool = toolWithHint "my_mcp_tool" (Some(box true))

    let policy =
        AskForWritesAndExecPermissionPolicy(toolsOf [ "my_mcp_tool", tool ]) :> IPermissionPolicy

    (evaluate policy "my_mcp_tool") :? AskVerdict |> should equal true

[<Fact>]
let ``Ask allows when destructiveHint is false or missing`` () =
    let missing = toolWithHint "missing_hint" None
    let explicitFalse = toolWithHint "false_hint" (Some(box false))

    let policy =
        AskForWritesAndExecPermissionPolicy(
            toolsOf
                [
                    "missing_hint", missing
                    "false_hint", explicitFalse
                ]
        )
        :> IPermissionPolicy

    (evaluate policy "missing_hint") :? AllowVerdict |> should equal true
    (evaluate policy "false_hint") :? AllowVerdict |> should equal true
    (evaluate policy "unknown_tool") :? AllowVerdict |> should equal true

[<Fact>]
let ``Ask allows when destructiveHint is a non-bool`` () =
    let tool = toolWithHint "string_hint" (Some(box "true"))

    let policy =
        AskForWritesAndExecPermissionPolicy(toolsOf [ "string_hint", tool ]) :> IPermissionPolicy

    (evaluate policy "string_hint") :? AllowVerdict |> should equal true

[<Fact>]
let ``Ask still asks for the closed set when the hint is false`` () =
    let tool = toolWithHint "exec" (Some(box false))

    let policy =
        AskForWritesAndExecPermissionPolicy(toolsOf [ "exec", tool ]) :> IPermissionPolicy

    (evaluate policy "exec") :? AskVerdict |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// RuleBasedPermissionPolicy

[<Fact>]
let ``RuleBased first match wins`` () =
    let denyFirst =
        ruleBased
            [
                "exec", PermissionDecisionKind.Deny
                "*", PermissionDecisionKind.AllowOnce
            ]
            PermissionDecisionKind.Deny

    (evaluate denyFirst "exec") :? DenyVerdict |> should equal true

    let allowFirst =
        ruleBased
            [
                "*", PermissionDecisionKind.AllowOnce
                "exec", PermissionDecisionKind.Deny
            ]
            PermissionDecisionKind.Deny

    (evaluate allowFirst "exec") :? AllowVerdict |> should equal true

[<Fact>]
let ``RuleBased star spans colons for mcp names`` () =
    let policy =
        ruleBased [ "mcp:*", PermissionDecisionKind.Deny ] PermissionDecisionKind.AllowOnce

    (evaluate policy "mcp:github:get_issue") :? DenyVerdict |> should equal true
    (evaluate policy "exec") :? AllowVerdict |> should equal true

[<Fact>]
let ``RuleBased empty rules fall back to the default`` () =
    let allowDefault = ruleBased [] PermissionDecisionKind.AllowOnce
    (evaluate allowDefault "exec") :? AllowVerdict |> should equal true

    let sessionDefault = ruleBased [] PermissionDecisionKind.AllowForSession
    (evaluate sessionDefault "exec") :? AllowVerdict |> should equal true

    let denyDefault = ruleBased [] PermissionDecisionKind.Deny
    (evaluate denyDefault "exec") :? DenyVerdict |> should equal true

[<Fact>]
let ``RuleBased AllowOnce and AllowForSession both allow`` () =
    let allowOnce =
        ruleBased
            [
                "exec", PermissionDecisionKind.AllowOnce
            ]
            PermissionDecisionKind.Deny

    (evaluate allowOnce "exec") :? AllowVerdict |> should equal true

    let allowSession =
        ruleBased
            [
                "exec", PermissionDecisionKind.AllowForSession
            ]
            PermissionDecisionKind.Deny

    (evaluate allowSession "exec") :? AllowVerdict |> should equal true

[<Fact>]
let ``RuleBased deny reason names the pattern only`` () =
    let policy =
        ruleBased [ "mcp:*", PermissionDecisionKind.Deny ] PermissionDecisionKind.AllowOnce

    match evaluate policy "mcp:github:get_issue" with
    | :? DenyVerdict as deny ->
        deny.Reason.Contains("mcp:*") |> should equal true
        deny.Reason.Contains("preview-sentinel-must-never-leak") |> should equal false
        deny.Reason.Contains("req-sentinel-1") |> should equal false
    | _ -> failwith "expected a DenyVerdict"

[<Fact>]
let ``RuleBased default deny reason carries no request contents`` () =
    let policy = ruleBased [] PermissionDecisionKind.Deny

    match evaluate policy "exec" with
    | :? DenyVerdict as deny ->
        deny.Reason.Contains("preview-sentinel-must-never-leak") |> should equal false
        deny.Reason.Contains("req-sentinel-1") |> should equal false
    | _ -> failwith "expected a DenyVerdict"

[<Fact>]
let ``RuleBased question mark spans one character and matching is case-sensitive`` () =
    let policy =
        ruleBased
            [
                "read_????", PermissionDecisionKind.Deny
            ]
            PermissionDecisionKind.AllowOnce

    (evaluate policy "read_file") :? DenyVerdict |> should equal true

    let cased =
        ruleBased
            [
                "READ_*", PermissionDecisionKind.Deny
            ]
            PermissionDecisionKind.AllowOnce

    (evaluate cased "read_file") :? AllowVerdict |> should equal true

[<Fact>]
let ``RuleBased star alone matches every tool`` () =
    let policy =
        ruleBased [ "*", PermissionDecisionKind.Deny ] PermissionDecisionKind.AllowOnce

    (evaluate policy "anything_at_all") :? DenyVerdict |> should equal true
