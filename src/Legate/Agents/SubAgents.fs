// SPDX-License-Identifier: Apache-2.0
module internal Legate.Agents.SubAgents

open System
open System.Collections.Generic
open Legate

// Sub-agent definitions and built-ins (issue 71): the single built-in tool
// name source file agents validate against, the two shipped sub-agents
// (explore with read-only tools, general with the runtime default), the
// load-time diagnostics for invalid file definitions, and the task-tool
// description contract listing the available sub-agents. The task tool
// itself stays in #72: this module only builds its description text so the
// listing contract is pinned and cross-verified there.

// ──────────────────────────────────────────────────────────────────────────
// Built-in tool vocabulary

/// Every built-in tool name a file agent may allowlist, in a fixed order:
/// the file tools, the edit tool, the search tools, exec, the turn tools,
/// and the download-URL tool. The single vocabulary source the file store
/// resolves allowlists against at load: names outside this set diagnose as
/// unknown, never fail the load. The <c>task</c> tool is absent on purpose:
/// it lands in #72, so a file naming it today diagnoses as unknown.
let builtinToolNames: string list =
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

/// Tests one tool name against the vocabulary, ordinally and
/// case-sensitively: names are model-facing identifiers, never normalised.
/// <param name="name">The tool name to test.</param>
/// <returns>true when the name is a known built-in; otherwise false.</returns>
let isKnownTool (name: string) : bool =
    if isNull (box name) then
        false
    else
        builtinToolNames
        |> List.exists (fun known -> String.Equals(known, name, StringComparison.Ordinal))

/// The allowlist names outside the vocabulary, in file order.
/// <param name="tools">The allowlist to check.</param>
/// <returns>The unknown names, in the order given.</returns>
let unknownTools (tools: string list) : string list =
    tools |> List.filter (fun name -> not (isKnownTool name))

/// The read-only subset backing the <c>explore</c> sub-agent: the text and
/// binary reads, the directory listing, and the two search tools. Writes,
/// edits, exec, and the turn/download tools stay out: exploration observes,
/// never changes.
/// <returns>The read-only tool names, in vocabulary order.</returns>
let exploreToolNames: string list =
    [
        "read_file"
        "list_files"
        "read_binary_base64"
        "glob"
        "grep"
    ]

// ──────────────────────────────────────────────────────────────────────────
// ToolSelection mapping

/// Maps one parsed allowlist onto the agent's tool selection: an empty
/// allowlist (the key absent) maps to null, the runtime default of every
/// tool, while a present list maps to a ToolSelection carrying exactly
/// those names verbatim, unknown names included (they diagnose separately
/// and the runtime resolves them). An explicit empty list is therefore the
/// same as an absent key: both mean the default, never no tools.
/// <param name="tools">The parsed allowlist, in file order.</param>
/// <returns>Null for the runtime default, otherwise the selection.</returns>
let toToolSelection (tools: string list) : ToolSelection | null =
    match tools with
    | [] -> null
    | names ->
        let selection = ToolSelection()
        selection.BuiltIns <- ResizeArray<string>(names) :> IReadOnlyList<string>
        selection.ToolSources <- ResizeArray<string>() :> IReadOnlyList<string>
        selection

// ──────────────────────────────────────────────────────────────────────────
// Built-in sub-agents

/// The name of the read-only exploration sub-agent.
[<Literal>]
let ExploreName = "explore"

/// The name of the general-purpose sub-agent.
[<Literal>]
let GeneralName = "general"

/// What the <c>explore</c> sub-agent is for, listed by the task tool.
[<Literal>]
let ExploreDescription =
    "Explores the workspace with read-only tools and reports back what it found."

/// What the <c>general</c> sub-agent is for, listed by the task tool.
[<Literal>]
let GeneralDescription =
    "A general-purpose sub-agent for tasks that need the full tool set."

/// The system prompt the <c>explore</c> sub-agent runs with.
[<Literal>]
let ExploreSystemPrompt =
    "You are a read-only exploration sub-agent. Inspect workspace files with your tools and report your findings concisely. Never modify files or run commands."

/// The system prompt the <c>general</c> sub-agent runs with.
[<Literal>]
let GeneralSystemPrompt =
    "You are a general-purpose sub-agent. Carry out the task you were given and report the outcome concisely."

/// Builds one built-in sub-agent row over the given tenant: a fresh id, the
/// default model, enabled, and the built-in's prompt and tool selection.
/// <param name="tenant">The tenant the agent is served under.</param>
/// <param name="name">The built-in's name.</param>
/// <param name="description">The built-in's description.</param>
/// <param name="systemPrompt">The built-in's system prompt.</param>
/// <param name="tools">The built-in's allowlist; empty means the runtime default.</param>
/// <returns>The built-in agent row.</returns>
let private createBuiltin
    (tenant: TenantId)
    (name: string)
    (description: string)
    (systemPrompt: string)
    (tools: string list)
    : Agent =
    let stamp = DateTimeOffset.UtcNow

    {
        Id = AgentId.New()
        Tenant = tenant
        Name = name
        Description = description
        Model = AgentFileParser.defaultModel
        SystemPrompt = systemPrompt
        EnvironmentVariables = null
        PermissionDefaults = null
        ToolSelection = toToolSelection tools
        PackageReference = null
        Enabled = true
        Schedule = null
        RowVersion = 0UL
        CreatedAt = stamp
        UpdatedAt = stamp
    }

/// Builds the <c>explore</c> sub-agent row: the read-only tool selection.
/// Hosts seed it through <c>AgentsBuilder.Add</c>; user definitions with the
/// same name win over it through the file store's later-definitions-win
/// merge.
/// <param name="tenant">The tenant the agent is served under.</param>
/// <returns>The explore agent row.</returns>
let createExploreAgent (tenant: TenantId) : Agent =
    createBuiltin tenant ExploreName ExploreDescription ExploreSystemPrompt exploreToolNames

/// Builds the <c>general</c> sub-agent row: the runtime-default (full) tool
/// selection. Hosts seed it through <c>AgentsBuilder.Add</c>; user
/// definitions with the same name win over it through the file store's
/// later-definitions-win merge.
/// <param name="tenant">The tenant the agent is served under.</param>
/// <returns>The general agent row.</returns>
let createGeneralAgent (tenant: TenantId) : Agent =
    createBuiltin tenant GeneralName GeneralDescription GeneralSystemPrompt []

// ──────────────────────────────────────────────────────────────────────────
// Diagnostics

/// Diagnoses one merged file definition: a missing or blank description and
/// any unknown allowlist names each map to one AgentInvalidEvent. The agent
/// itself is always kept: these events explain, never exclude, so an
/// invalid definition is never a silent drop and never fails the load.
/// Missing names and bad YAML stay fatal in the parser and never reach here.
/// <param name="definition">The merged definition to diagnose.</param>
/// <param name="sessionId">The session the diagnostics run for.</param>
/// <param name="turnId">The turn the diagnostics run inside.</param>
/// <param name="timestamp">When the diagnostics ran: the stamp every event carries.</param>
/// <returns>Zero, one, or two diagnostic events for the definition.</returns>
let diagnoseDefinition
    (definition: AgentFileParser.AgentFileDefinition)
    (sessionId: SessionId)
    (turnId: TurnId)
    (timestamp: DateTimeOffset)
    : SessionEvent list =
    let noSequence = Unchecked.defaultof<Nullable<int64>>
    let events = ResizeArray<SessionEvent>()

    if String.IsNullOrWhiteSpace definition.Description then
        AgentInvalidEvent(
            sessionId,
            turnId,
            noSequence,
            timestamp,
            definition.Name,
            sprintf "The agent '%s' has no description: the task tool lists it without one." definition.Name
        )
        :> SessionEvent
        |> events.Add

    match unknownTools definition.Tools with
    | [] -> ()
    | unknown ->
        AgentInvalidEvent(
            sessionId,
            turnId,
            noSequence,
            timestamp,
            definition.Name,
            sprintf "The agent '%s' names unknown tools: %s." definition.Name (String.Join(", ", unknown))
        )
        :> SessionEvent
        |> events.Add

    events |> Seq.toList

// ──────────────────────────────────────────────────────────────────────────
// Task-tool description

/// Placeholder the listing carries when an agent has no usable
/// description, so the task tool still names it.
[<Literal>]
let MissingDescriptionPlaceholder = "(no description)"

/// Builds the task tool's sub-agent listing over the given agents, sorted
/// ordinally by name: one <c>- name: description</c> line per agent under
/// the <c>Available sub-agents:</c> header. Agents without a description
/// list with the placeholder, never a blank. With no agents the listing is
/// the header plus <c>none</c>, so #72 embeds a total function of the store.
/// <param name="agents">The agents to list.</param>
/// <returns>The listing text the task tool description embeds.</returns>
let buildTaskDescription (agents: IReadOnlyList<Agent>) : string =
    let rows =
        if isNull (box agents) then
            []
        else
            agents
            |> Seq.filter (fun agent -> not (isNull (box agent)))
            |> Seq.sortWith (fun left right -> String.Compare(left.Name, right.Name, StringComparison.Ordinal))
            |> Seq.map (fun agent ->
                let description =
                    match Option.ofObj agent.Description with
                    | Some text when not (String.IsNullOrWhiteSpace text) -> text
                    | _ -> MissingDescriptionPlaceholder

                sprintf "- %s: %s" agent.Name description)
            |> Seq.toList

    match rows with
    | [] -> "Available sub-agents:\nnone"
    | lines -> String.Join("\n", "Available sub-agents:" :: lines)
