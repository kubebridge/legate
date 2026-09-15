// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SubAgentsTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Legate.Agents
open Xunit

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// Builds one agent row the way the store serves it.
let private listedAgent (name: string) (description: string | null) : Agent =
    {
        Id = AgentId.New()
        Tenant = TenantId.Default
        Name = name
        Description = description
        Model = ModelReference.Parse "test/model"
        SystemPrompt = "Prompt."
        EnvironmentVariables = null
        PermissionDefaults = null
        ToolSelection = null
        PackageReference = null
        Enabled = true
        Schedule = null
        RowVersion = 0UL
        CreatedAt = DateTimeOffset.UtcNow
        UpdatedAt = DateTimeOffset.UtcNow
    }

/// Builds one parsed file definition for the diagnostics.
let private fileDefinition
    (name: string)
    (description: string | null)
    (tools: string list)
    : AgentFileParser.AgentFileDefinition =
    {
        Name = name
        Description = description
        Model = AgentFileParser.defaultModel
        Enabled = true
        Tools = tools
        SystemPrompt = "Prompt."
    }

// ──────────────────────────────────────────────────────────────────────────
// Vocabulary

[<Fact>]
let ``Builtin vocabulary holds the twelve shipped tool names`` () =
    SubAgents.builtinToolNames
    |> should
        equal
        [
            "read_file"
            "write_file"
            "list_files"
            "read_binary_base64"
            "write_binary_base64"
            "edit_file"
            "glob"
            "grep"
            "exec"
            "ask_user"
            "skill"
            "get_download_url"
        ]

[<Fact>]
let ``Known tools match ordinally and case-sensitively`` () =
    SubAgents.isKnownTool "read_file" |> should equal true
    SubAgents.isKnownTool "Read_File" |> should equal false
    SubAgents.isKnownTool "turbo_laser" |> should equal false
    SubAgents.isKnownTool Unchecked.defaultof<string> |> should equal false

[<Fact>]
let ``Explore tools are the read-only subset`` () =
    SubAgents.exploreToolNames
    |> should
        equal
        [
            "read_file"
            "list_files"
            "read_binary_base64"
            "glob"
            "grep"
        ]

    for name in SubAgents.exploreToolNames do
        SubAgents.isKnownTool name |> should equal true

    SubAgents.exploreToolNames |> List.contains "write_file" |> should equal false
    SubAgents.exploreToolNames |> List.contains "exec" |> should equal false
    SubAgents.exploreToolNames |> List.contains "edit_file" |> should equal false

[<Fact>]
let ``Unknown tools filter in file order`` () =
    SubAgents.unknownTools
        [
            "read_file"
            "turbo_laser"
            "glob"
            "hyperdrive"
        ]
    |> should equal [ "turbo_laser"; "hyperdrive" ]

// ──────────────────────────────────────────────────────────────────────────
// ToolSelection mapping

[<Fact>]
let ``Empty allowlist maps to the null runtime default`` () =
    isNull (box (SubAgents.toToolSelection [])) |> should equal true

[<Fact>]
let ``Present allowlist maps verbatim with empty tool sources`` () =
    let selection = SubAgents.toToolSelection [ "read_file"; "glob" ]

    isNull (box selection) |> should equal false

    match box selection with
    | :? ToolSelection as selected ->
        selected.BuiltIns |> Seq.toList |> should equal [ "read_file"; "glob" ]
        selected.ToolSources.Count |> should equal 0
    | _ -> failwith "Expected a tool selection for a present allowlist."

// ──────────────────────────────────────────────────────────────────────────
// Built-ins

[<Fact>]
let ``Explore ships read-only with its description and prompt`` () =
    let explore = SubAgents.createExploreAgent TenantId.Default

    explore.Name |> should equal "explore"
    explore.Description |> should equal SubAgents.ExploreDescription
    explore.SystemPrompt |> should equal SubAgents.ExploreSystemPrompt
    explore.Enabled |> should equal true
    explore.Tenant |> should equal TenantId.Default

    match box explore.ToolSelection with
    | :? ToolSelection as selected -> selected.BuiltIns |> Seq.toList |> should equal SubAgents.exploreToolNames
    | _ -> failwith "Expected explore to carry a tool selection."

[<Fact>]
let ``General ships the runtime default with its description and prompt`` () =
    let general = SubAgents.createGeneralAgent TenantId.Default

    general.Name |> should equal "general"
    general.Description |> should equal SubAgents.GeneralDescription
    general.SystemPrompt |> should equal SubAgents.GeneralSystemPrompt
    general.Enabled |> should equal true
    general.Tenant |> should equal TenantId.Default
    isNull (box general.ToolSelection) |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Diagnostics

[<Fact>]
let ``Clean definitions diagnose nothing`` () =
    let events =
        SubAgents.diagnoseDefinition
            (fileDefinition "helper" "Helps out" [ "read_file" ])
            (SessionId.New())
            (TurnId.New())
            DateTimeOffset.UtcNow

    events |> should be Empty

[<Fact>]
let ``Missing description diagnoses with the agent name`` () =
    let sessionId = SessionId.New()
    let turnId = TurnId.New()
    let stamp = DateTimeOffset.UtcNow

    let events =
        SubAgents.diagnoseDefinition (fileDefinition "helper" null [ "read_file" ]) sessionId turnId stamp

    events.Length |> should equal 1

    let flagged = events[0] :?> AgentInvalidEvent
    flagged.AgentName |> should equal "helper"
    flagged.SessionId |> should equal sessionId
    flagged.TurnId |> should equal turnId
    flagged.Timestamp |> should equal stamp

[<Fact>]
let ``Blank description diagnoses like a missing one`` () =
    let events =
        SubAgents.diagnoseDefinition
            (fileDefinition "helper" "   " [ "read_file" ])
            (SessionId.New())
            (TurnId.New())
            DateTimeOffset.UtcNow

    events.Length |> should equal 1

[<Fact>]
let ``Unknown tools diagnose naming the offenders`` () =
    let events =
        SubAgents.diagnoseDefinition
            (fileDefinition "helper" "Helps out" [ "read_file"; "turbo_laser" ])
            (SessionId.New())
            (TurnId.New())
            DateTimeOffset.UtcNow

    events.Length |> should equal 1

    let flagged = events[0] :?> AgentInvalidEvent
    flagged.AgentName |> should equal "helper"
    flagged.Reason.Contains("turbo_laser") |> should equal true

[<Fact>]
let ``Both problems diagnose twice`` () =
    let events =
        SubAgents.diagnoseDefinition
            (fileDefinition "helper" null [ "turbo_laser" ])
            (SessionId.New())
            (TurnId.New())
            DateTimeOffset.UtcNow

    events.Length |> should equal 2

// ──────────────────────────────────────────────────────────────────────────
// Task description

[<Fact>]
let ``Task description lists names and descriptions in ordinal order`` () =
    let agents =
        ResizeArray<Agent>(
            [|
                listedAgent "zeta" "Last."
                listedAgent "alpha" "First."
            |]
        )
        :> IReadOnlyList<Agent>

    SubAgents.buildTaskDescription agents
    |> should equal "Available sub-agents:\n- alpha: First.\n- zeta: Last."

[<Fact>]
let ``Task description placeholders missing descriptions`` () =
    let agents =
        ResizeArray<Agent>([| listedAgent "helper" null |]) :> IReadOnlyList<Agent>

    SubAgents.buildTaskDescription agents
    |> should equal "Available sub-agents:\n- helper: (no description)"

[<Fact>]
let ``Task description with no agents names none`` () =
    let agents = ResizeArray<Agent>() :> IReadOnlyList<Agent>

    SubAgents.buildTaskDescription agents
    |> should equal "Available sub-agents:\nnone"

[<Fact>]
let ``Built-ins list under the task description`` () =
    let agents =
        ResizeArray<Agent>(
            [|
                SubAgents.createGeneralAgent TenantId.Default
                SubAgents.createExploreAgent TenantId.Default
            |]
        )
        :> IReadOnlyList<Agent>

    SubAgents.buildTaskDescription agents
    |> should
        equal
        ("Available sub-agents:\n- explore: "
         + SubAgents.ExploreDescription
         + "\n- general: "
         + SubAgents.GeneralDescription)
