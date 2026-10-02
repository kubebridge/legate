// SPDX-License-Identifier: Apache-2.0
module Dot.DotSkills

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging

// Dot sample skill (issue 309): one read-only review skill packaged into
// each ensured agent and served to the model through the public
// SkillTool.Create factory. Unknown names diagnose cleanly through the
// runtime's available-skills error; onLoaded stays log-only because hosts
// have no journal seam, so no SkillLoadedEvent is journaled from dot (the
// README names this limitation).

// ──────────────────────────────────────────────────────────────────────────
// Sample package

/// The packaged version dot uploads: re-uploading replaces atomically and
/// stays active, so the ensure path uploads idempotently on every open.
let sampleVersion = "1.0.0"

/// The host-supplied source label of dot's sample package upload.
let sampleSource = "dot-sample"

/// The sample skill name served through the skill tool.
let sampleSkillName = "review"

/// The package-relative SKILL.md path of the sample skill.
let sampleSkillPath = ".agent/skills/review/SKILL.md"

/// Seeds the session-shared nested runners into the durable agent catalog.
/// Explore receives only read-only tools; general inherits the parent pool.
let ensureSubAgentsAsync (store: IAgentStore) (model: ModelReference) (ct: CancellationToken) : Task =
    task {
        let! existing = store.ListAgents(TenantId.Default, ct)

        for name, description, prompt, tools in
            [
                "explore",
                "Read-only workspace exploration and code research.",
                "You are a read-only exploration sub-agent. Inspect files and report findings concisely. Never modify files or execute commands.",
                Some
                    [
                        "read_file"
                        "list_files"
                        "glob"
                        "grep"
                    ]
                "general",
                "Delegated implementation and multi-step tasks.",
                "Carry out the delegated task with the available tools and report your outcome concisely.",
                None
            ] do
            if not (existing |> Seq.exists (fun agent -> agent.Name = name)) then
                let now = DateTimeOffset.UtcNow

                let selection =
                    match tools with
                    | None -> null
                    | Some names ->
                        let value = ToolSelection()
                        value.BuiltIns <- ResizeArray<string>(names)
                        value

                let agent: Agent =
                    {
                        Id = AgentId.New()
                        Tenant = TenantId.Default
                        Name = name
                        Description = description
                        Model = model
                        SystemPrompt = prompt
                        EnvironmentVariables = null
                        PermissionDefaults = null
                        ToolSelection = selection
                        PackageReference = null
                        Enabled = true
                        Schedule = null
                        RowVersion = 0UL
                        CreatedAt = now
                        UpdatedAt = now
                    }

                let! _ = store.UpdateIfUnchanged(TenantId.Default, agent, 0UL, ct)
                ()
    }

/// The sample review SKILL.md text: YAML frontmatter the runtime discovery
/// parses (name, description, read-only allowed-tools) plus a short
/// read-only review guide. REVIEW-SKILL-309 marks the content the
/// skill-load smoke run asserts on.
let reviewSkillText =
    """---
name: review
description: Reviews a change for correctness, clarity, and risk using read-only tools.
allowed-tools:
  - read_file
  - list_files
  - glob
  - grep
---
# Review skill (REVIEW-SKILL-309)

Review the change under review with read-only tools only: never write,
edit, or execute.

1. Read the files under review with `read_file` and list the tree with
   `list_files`; search with `glob` and `grep` for callers and related
   tests.
2. Report correctness issues first (wrong behaviour, missing handling),
   then clarity (naming, structure), then risk (what could break and how
   to verify).
3. Keep the report short: one line per finding with the file and line.
"""

/// Builds the sample package entries: the single review SKILL.md.
/// <returns>The upload entries.</returns>
let private sampleEntries () : IAsyncEnumerable<AgentPackageEntry> =
    let prepared =
        [|
            AgentPackageEntry(sampleSkillPath, new MemoryStream(Encoding.UTF8.GetBytes reviewSkillText))
        |]

    { new IAsyncEnumerable<AgentPackageEntry> with
        member _.GetAsyncEnumerator(_: CancellationToken) =
            let mutable index = -1

            { new IAsyncEnumerator<AgentPackageEntry> with
                member _.MoveNextAsync() =
                    index <- index + 1
                    ValueTask<bool>(index < prepared.Length)

                member _.Current: AgentPackageEntry = prepared[index]

                member _.DisposeAsync() : ValueTask = ValueTask()
            }
    }

/// Uploads the sample review skill package for one agent: idempotent, the
/// re-upload replaces atomically and stays active.
/// <param name="store">The package store. Must not be null.</param>
/// <param name="tenant">The tenant the agent belongs to.</param>
/// <param name="agentId">The agent whose package to upload.</param>
/// <param name="cancellationToken">Abandons the upload.</param>
let uploadSamplePackageAsync
    (store: IAgentPackageStore)
    (tenant: TenantId)
    (agentId: AgentId)
    (cancellationToken: CancellationToken)
    : Task =
    ArgumentNullException.ThrowIfNull(store)

    task {
        let! _ = store.UploadPackage(tenant, agentId, sampleVersion, sampleSource, sampleEntries (), cancellationToken)

        ()
    }

// ──────────────────────────────────────────────────────────────────────────
// The session-bound skill tool source

/// The dot skill tool source: serves the public skill built-in bound to the
/// asking session's package scope, with a log-only load callback (hosts
/// have no journal seam). A session the loader cannot serve degrades to the
/// contract's empty list (never null, never an error) with the reason
/// logged without secrets or tool arguments.
type SkillToolSource(store: IAgentPackageStore, logger: ILogger<SkillToolSource> | null) =

    do ArgumentNullException.ThrowIfNull(store)

    /// Logs a degraded-source reason carrying no secrets or tool arguments.
    /// <param name="reason">The fixed reason text.</param>
    /// <param name="error">The failure, logged by type name only.</param>
    let logDegraded (reason: string) (error: exn | null) : unit =
        match logger with
        | null -> ()
        | log ->
            match error with
            | null -> log.LogWarning("The dot skill tool source is degraded: {Reason}.", reason)
            | failure ->
                log.LogWarning(
                    "The dot skill tool source is degraded: {Reason} ({ErrorType}).",
                    reason,
                    failure.GetType().Name
                )

    interface IToolSource with

        /// Resolves the skill tool for one session over its package scope.
        /// <param name="context">The tenant, agent, and session asking for its tools.</param>
        /// <returns>The skill tool, or the empty list when degraded.</returns>
        member _.GetTools(context: ToolSourceContext) : Task<IReadOnlyList<AITool>> =
            task {
                try
                    // ToolSourceContext carries no null annotation, so the
                    // check is a runtime guard for non-F# callers.
                    if isNull (box context) then
                        logDegraded "no tool source context" null
                        return ResizeArray<AITool>() :> IReadOnlyList<AITool>
                    else
                        let onLoaded =
                            Func<SkillLoadedEvent, Task>(fun loaded ->
                                match logger with
                                | null -> Task.CompletedTask
                                | log ->
                                    log.LogInformation("The dot skill tool loaded skill '{Skill}'.", loaded.SkillName)
                                    Task.CompletedTask)

                        let tool =
                            SkillTool.Create(
                                store,
                                context.Tenant,
                                context.AgentId,
                                context.SessionId,
                                TurnId.New(),
                                onLoaded
                            )

                        let tools = ResizeArray<AITool>()
                        tools.Add(tool :> AITool)
                        return tools :> IReadOnlyList<AITool>
                with error ->
                    logDegraded "the skill tool could not be built" error
                    return ResizeArray<AITool>() :> IReadOnlyList<AITool>
            }
