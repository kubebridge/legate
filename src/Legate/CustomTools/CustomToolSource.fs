// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging

// Agent custom HTTP tools as an IToolSource (issue 74). Per session the
// source lists the agent's enabled custom tools, re-sanitises each name
// with first-claimant-wins dedupe (built-ins keep first claim, every
// rename and loser is Warning-logged), resolves each input schema once
// (invalid schemas fall back with the model-visible diagnostic plus a
// Warning carrying only the parse reason), and builds one AIFunction per
// loadable tool. Rows with no endpoint or no signing secret drop out with
// a Warning naming the tool only. A store that cannot serve degrades to
// the contract's empty list with a Warning, never an error; cancellation
// propagates. Permission gating and the claim fence stay with TurnLoop's
// per-tool hooks, so this source adds no exemptions: null policy still
// means the host's explicit no-gate choice.

/// The canonical built-in tool names custom tools lose collisions to.
/// Literals, not references, so this file stays independent of the tool
/// modules in compile order; CustomToolSourceTests pins the set against
/// each tool's static name.
module internal CustomToolBuiltIns =

    /// Every model-facing built-in name, fixed so built-ins keep first
    /// claim no matter when a source registers.
    let names: string[] =
        [|
            "ask_user"
            "edit_file"
            "exec"
            "get_download_url"
            "glob"
            "grep"
            "skill"
            "task"
        |]

    /// The names as the ordinal set the dedupe claims against.
    let asSet: Set<string> = Set.ofArray names

/// An IToolSource building one signed AIFunction per enabled
/// AgentCustomTool. Internal: hosts register it through
/// ToolsBuilder.AddCustomTools, never construct it.
[<Sealed>]
type internal CustomToolSource
    (
        toolStore: IAgentCustomToolStore,
        resolver: IHostAddressResolver,
        guardOptions: SsrfGuardOptions,
        builtInNames: IReadOnlyCollection<string> | null,
        requestTimeout: TimeSpan,
        logger: ILogger | null
    ) =

    do
        ArgumentNullException.ThrowIfNull(toolStore)
        ArgumentNullException.ThrowIfNull(resolver)
        ArgumentNullException.ThrowIfNull(guardOptions)

    let log: ILogger =
        match Option.ofObj logger with
        | Some live -> live
        | None -> Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance :> ILogger

    let reserved =
        match Option.ofObj builtInNames with
        | None -> CustomToolBuiltIns.asSet
        | Some names -> names |> Seq.filter (fun name -> not (String.IsNullOrEmpty name)) |> Set.ofSeq

    let timeout =
        if requestTimeout > TimeSpan.Zero then
            requestTimeout
        else
            CustomToolFunction.DefaultTimeout

    /// Builds the source with the default request bound (30 s), the
    /// canonical built-in set, and no logger.
    /// <param name="toolStore">The store listing the agent's enabled custom tools. Must not be null.</param>
    /// <param name="resolver">The address resolver seam. Must not be null.</param>
    /// <param name="guardOptions">The host-level SSRF allow/deny lists. Must not be null.</param>
    new(toolStore: IAgentCustomToolStore, resolver: IHostAddressResolver, guardOptions: SsrfGuardOptions) =
        CustomToolSource(toolStore, resolver, guardOptions, null, CustomToolFunction.DefaultTimeout, null)

    /// Reports one claimed name: renames and losers Warning-log with
    /// names only, never secrets, headers, or arguments.
    /// <param name="claimed">The claimed name to report.</param>
    member private _.ReportClaim(claimed: ClaimedName<AgentCustomTool>) : unit =
        match claimed.Fate with
        | Claimed -> ()
        | ClaimedRenamed ->
            log.LogWarning(
                "The custom tool '{Original}' serves as '{Sanitized}': the name was sanitised.",
                claimed.Original,
                claimed.Sanitized
            )
        | DuplicateLoser kept ->
            log.LogWarning(
                "The custom tool '{Original}' is dropped: '{Sanitized}' is already served by '{Kept}'.",
                claimed.Original,
                claimed.Sanitized,
                kept
            )
        | ReservedLoser ->
            log.LogWarning(
                "The custom tool '{Original}' is dropped: '{Sanitized}' is a built-in tool name.",
                claimed.Original,
                claimed.Sanitized
            )

    /// Builds the tool function for one winning row: resolves the schema
    /// (logging the parse reason on fallback), composes the description,
    /// and binds the endpoint, headers, and secret. Rows with no endpoint
    /// or no signing secret drop out with a Warning naming the tool only.
    /// <param name="sanitized">The sanitised model-facing name.</param>
    /// <param name="row">The tool row to build from.</param>
    /// <returns>The tool function, or None when the row cannot serve.</returns>
    member private _.BuildTool(sanitized: string, row: AgentCustomTool) : AIFunction option =
        if isNull (box row.Endpoint) then
            log.LogWarning("The custom tool '{Tool}' is dropped: it has no endpoint.", sanitized)
            None
        elif isNull (box row.SigningSecret) || row.SigningSecret.Length = 0 then
            log.LogWarning("The custom tool '{Tool}' is dropped: it has no signing secret.", sanitized)
            None
        else
            let resolution = CustomToolSchema.resolve row.InputSchema

            match resolution.ParseReason with
            | Some reason ->
                log.LogWarning(
                    "The custom tool '{Tool}' input schema was invalid ({Reason}); accepting any JSON object.",
                    sanitized,
                    reason
                )
            | None -> ()

            let description =
                let baseText =
                    match Option.ofObj row.Description with
                    | Some text when not (String.IsNullOrWhiteSpace text) -> text
                    | _ -> sprintf "Invokes the custom tool '%s' over HTTP." sanitized

                baseText + resolution.Diagnostic

            Some(
                CustomToolFunction(
                    sanitized,
                    description,
                    resolution.Schema,
                    row.Endpoint,
                    row.Headers,
                    row.SigningSecret,
                    resolver,
                    guardOptions,
                    timeout
                )
                :> AIFunction
            )

    interface IToolSource with
        /// Resolves the agent's enabled custom tools to signed functions:
        /// lists, sanitises with first-claimant-wins dedupe against the
        /// built-in set, resolves schemas, and builds one function per
        /// loadable tool. Degrades to the empty list when the store cannot
        /// serve; cancellation propagates.
        /// <param name="context">The tenant, agent, and session asking for its tools.</param>
        /// <returns>The tools offered to the model, free of null entries.</returns>
        member this.GetTools(context: ToolSourceContext) : Task<IReadOnlyList<AITool>> =
            if isNull (box context) then
                raise (ArgumentNullException(nameof context))

            task {
                let! rows =
                    task {
                        try
                            return! toolStore.ListCustomTools(context.Tenant, context.AgentId, CancellationToken.None)
                        with
                        | :? OperationCanceledException as canceled ->
                            return! Task.FromException<IReadOnlyList<AgentCustomTool>>(canceled)
                        | failed ->
                            log.LogWarning(
                                failed,
                                "The custom tool source contributes no tools: the store could not be listed."
                            )

                            return ResizeArray<AgentCustomTool>() :> IReadOnlyList<AgentCustomTool>
                    }

                let candidates =
                    if isNull (box rows) then
                        []
                    else
                        rows
                        |> Seq.filter (fun row -> not (isNull (box row)))
                        |> Seq.map (fun row ->
                            let original = if isNull (box row.Name) then None else Some row.Name
                            (original, row))
                        |> List.ofSeq

                let (claimed, unusable) = CustomToolNames.claimNames reserved candidates

                for original in unusable do
                    log.LogWarning("The custom tool '{Original}' is dropped: it has no usable name.", original)

                let tools = ResizeArray<AITool>()

                for winner in claimed do
                    this.ReportClaim(winner)

                    match winner.Fate with
                    | Claimed
                    | ClaimedRenamed ->
                        match this.BuildTool(winner.Sanitized, winner.Payload) with
                        | Some tool -> tools.Add(tool)
                        | None -> ()
                    | DuplicateLoser _
                    | ReservedLoser -> ()

                return tools :> IReadOnlyList<AITool>
            }
