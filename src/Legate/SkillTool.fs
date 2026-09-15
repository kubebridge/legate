// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Nullness warning 3261 is suppressed in this file: MEAI invocation
// surfaces nulls (null arguments, null streams, null package info) that the
// F# nullable analysis cannot prove absent, and the loader treats every one
// as an expected branch rather than failing.
// Built-in skill tool (issue 68): the single model-facing AIFunction loading
// one packaged skill's SKILL.md from the active package version in
// IAgentPackageStore (authoritative, tenant-isolated) with the companion
// list discovery (issue 67) stages. Unknown names return an Error result
// listing the available skill names ordinally; content over the discovery
// bound (SkillDiscovery.MaxSkillFileBytes, 262144 bytes) is cut with
// TurnLoop.TruncationMarker ([truncated]). Each successful load builds one
// SkillLoadedEvent carrying the skill name and the companion list; the host
// journals it under the turn claim so Subscribe observes it, and the bus is
// a live fan-out of the same journal, not a separate event. The loader
// itself bypasses IPermissionPolicy (package metadata is already authorized
// by the session bind, so there is nothing for a policy to decide; the
// TurnLoop skill branch documents the bypass): companion files are read
// afterward through the normal file tools with the policy applied.
// allowed-tools stays advisory text on the discovery block; enforcement
// lives in TurnLoop. Rejected: an IToolSource-contributed loader
// (host-owned, collision-prone; the loader must always exist like
// ask_user), workspace-copy-only content (the workspace is scratch, the
// store is authoritative), and bus-only observability (hosts render the
// journal).

/// The tool's identity: its model-facing name and description, kept in
/// one internal module so the function type and the factory serve the
/// same literals. The name is the TurnLoop skill constant itself, so the
/// schema the model reads and the bypass the loop applies can never drift
/// apart.
module internal SkillIdentity =

    /// The tool's name as the model calls it: the skill-content loader.
    let name = TurnLoop.SkillToolName

    /// The tool's description as the model reads it: what the tool does,
    /// its arguments, the stable result shape, the size bound with its
    /// marker, the unknown-name error, and the journaled event.
    let description =
        "Loads a packaged skill's content (skill). "
        + "Pass the skill to load in 'name' (required), as listed in <available_skills>. "
        + "Returns the skill's full SKILL.md plus its companion file list under '## Companions'. "
        + "Content over 262144 bytes is cut with [truncated]. "
        + "Unknown names return an Error listing the available skills. "
        + "Each load records a skillLoaded journal event the host can render."

/// The tool's argument schema, kept in one internal module so the schema
/// tests assert on the same JSON the tool serves.
module internal SkillSchema =

    /// The JSON schema served on the tool's JsonSchema: name is a required
    /// string, the skill to load as listed in available_skills.
    let json =
        """{"type":"object","description":"Arguments for the skill tool.","properties":{"name":{"type":"string","description":"The skill to load, as listed in <available_skills>. Required."}},"required":["name"],"additionalProperties":false}"""

    /// The single parsed schema document backing every tool instance; the
    /// JsonSchema elements borrow it, so it lives as long as the process.
    let document = JsonDocument.Parse json

    /// Reads one raw argument by name; missing reads as null.
    /// <param name="args">The call's arguments.</param>
    /// <param name="name">The argument to read.</param>
    /// <returns>The raw value, or null when the argument is missing.</returns>
    let argument (args: AIFunctionArguments) (name: string) : obj | null =
        let mutable value: obj | null = null

        if args.TryGetValue(name, &value) then value else null

    /// Reads one argument value as text: plain strings and JSON string
    /// elements; anything else is not text.
    /// <param name="value">The raw argument value.</param>
    /// <returns>The text, or null when the value is not text.</returns>
    let asText (value: obj | null) : string | null =
        match value with
        | null -> null
        | :? string as text -> text
        | :? JsonElement as element when element.ValueKind = JsonValueKind.String -> element.GetString()
        | _ -> null

/// The turn-time content loader behind the skill tool: pure store reads
/// plus the stable result format and the journaled event. Internal so the
/// function type stays the only AIFunction surface; tests drive loadAsync
/// directly for format cases and through the function for wiring.
module internal SkillLoader =

    /// Everything one skill load needs: the store and tenant scope to read
    /// from, the agent whose active version resolves, and the identities
    /// the journaled event carries.
    type SkillLoadRequest =
        {
            /// The package store the active version resolves from.
            Store: IAgentPackageStore
            /// The tenant the agent belongs to.
            Tenant: TenantId
            /// The agent whose skill loads.
            AgentId: AgentId
            /// The session the load runs for.
            SessionId: SessionId
            /// The turn the load runs inside.
            TurnId: TurnId
            /// When the load ran: the stamp the journaled event carries.
            Timestamp: DateTimeOffset
            /// The requested skill name, as listed by package info.
            SkillName: string
        }

    /// What one skill load produced: the stable result text the model
    /// reads, plus the journaled event for a successful load or None when
    /// the name was unknown or the file unreadable (nothing journaled).
    type SkillLoadOutcome =
        {
            /// The result text: the formatted skill content, or the
            /// unknown-name or unreadable-file error.
            ResultText: string
            /// The event to journal under the turn claim, or None when the
            /// load produced no content.
            LoadedEvent: SkillLoadedEvent option
        }

    /// Formats the stable skill result: the skill header, the SKILL.md
    /// content verbatim (already bounded with the marker when cut), and
    /// the ordinal companion list excluding SKILL.md itself.
    /// <param name="skillName">The loaded skill name.</param>
    /// <param name="content">The SKILL.md content, already bounded.</param>
    /// <param name="companions">The companion paths in ordinal order, excluding SKILL.md.</param>
    /// <returns>The result text the model reads.</returns>
    let formatResult (skillName: string) (content: string) (companions: string list) : string =
        let body = if isNull content then "" else content
        let builder = StringBuilder()

        builder.Append("# Skill: ").Append(skillName).Append("\n\n").Append(body)
        |> ignore

        builder.Append("\n\n## Companions") |> ignore

        match companions with
        | [] -> builder.Append("\n(none)") |> ignore
        | paths ->
            for path in paths do
                builder.Append("\n- ").Append(path) |> ignore

        builder.ToString()

    /// Formats the unknown-name error: the requested name plus the
    /// available skill names in ordinal order, or none when the package
    /// lists no skills.
    /// <param name="requested">The name the model asked for.</param>
    /// <param name="available">The available skill names in ordinal order.</param>
    /// <returns>The Error result text.</returns>
    let unknownSkillError (requested: string) (available: string list) : string =
        match available with
        | [] -> sprintf "Error: unknown skill '%s'. Available skills: none." requested
        | names -> sprintf "Error: unknown skill '%s'. Available skills: %s." requested (String.Join(", ", names))

    /// Reads one store stream up to the discovery bound, cutting with the
    /// truncation flag when the stream runs past it, and disposes the
    /// stream. References SkillDiscovery.MaxSkillFileBytes literally so the
    /// loader and discovery can never drift apart (the loader truncates
    /// where discovery reports invalid).
    /// <param name="source">The stream to drain; always disposed.</param>
    /// <returns>The decoded text plus true when the stream ran past the bound.</returns>
    let readBoundedTruncating (source: Stream) : string * bool =
        use _source = source
        use memory = new MemoryStream()
        let buffer = Array.zeroCreate<byte> 8192
        let mutable finished = false
        let mutable truncated = false

        while not finished do
            let read = source.Read(buffer, 0, buffer.Length)

            if read = 0 then
                finished <- true
            elif memory.Length + int64 read > int64 SkillDiscovery.MaxSkillFileBytes then
                let room = SkillDiscovery.MaxSkillFileBytes - int memory.Length

                if room > 0 then
                    memory.Write(buffer, 0, room)

                truncated <- true
                finished <- true
            else
                memory.Write(buffer, 0, read)

        (Encoding.UTF8.GetString(memory.ToArray()), truncated)

    /// Loads one skill from the active package version: resolves the
    /// version, matches the requested name ordinally against the listed
    /// skills, reads SKILL.md bounded with the marker, and lists the
    /// companions live from the store (the same paths discovery stages).
    /// Unknown names and unreadable files return Error text with no event;
    /// store failures propagate to the TurnLoop Error mapping.
    /// <param name="request">The store, scope, identities, and requested name the load runs with.</param>
    /// <param name="cancellationToken">Token that abandons the load.</param>
    /// <returns>The result text plus the event to journal, or the error with no event.</returns>
    let loadAsync (request: SkillLoadRequest) (cancellationToken: CancellationToken) : Task<SkillLoadOutcome> =
        task {
            let! info = request.Store.GetPackageInfo(request.Tenant, request.AgentId, cancellationToken)

            match Option.ofObj info with
            | None ->
                return
                    {
                        ResultText = unknownSkillError request.SkillName []
                        LoadedEvent = None
                    }
            | Some package when isNull (box package.ActiveVersion) ->
                return
                    {
                        ResultText = unknownSkillError request.SkillName []
                        LoadedEvent = None
                    }
            | Some package ->
                let version = package.ActiveVersion

                let available =
                    if isNull (box package.Skills) then
                        []
                    else
                        package.Skills
                        |> Seq.filter (fun name -> not (isNull (box name)))
                        |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
                        |> List.ofSeq

                let known =
                    available
                    |> List.exists (fun name -> String.Equals(name, request.SkillName, StringComparison.Ordinal))

                if not known then
                    return
                        {
                            ResultText = unknownSkillError request.SkillName available
                            LoadedEvent = None
                        }
                else
                    let! stream =
                        request.Store.ReadFile(
                            request.Tenant,
                            request.AgentId,
                            version,
                            SkillDiscovery.skillFilePath request.SkillName,
                            cancellationToken
                        )

                    match Option.ofObj stream with
                    | None ->
                        return
                            {
                                ResultText = sprintf "Error: the skill '%s' has no readable SKILL.md." request.SkillName
                                LoadedEvent = None
                            }
                    | Some source ->
                        let text, truncated = readBoundedTruncating source

                        let content = if truncated then text + TurnLoop.TruncationMarker else text

                        let! files =
                            request.Store.ListFiles(
                                request.Tenant,
                                request.AgentId,
                                version,
                                SkillDiscovery.skillPrefix request.SkillName,
                                cancellationToken
                            )

                        let skillFile = SkillDiscovery.skillFilePath request.SkillName

                        let companions =
                            if isNull (box files) then
                                []
                            else
                                files
                                |> Seq.filter (fun path ->
                                    not (isNull (box path))
                                    && not (String.Equals(path, skillFile, StringComparison.Ordinal)))
                                |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
                                |> List.ofSeq

                        let noSequence = Unchecked.defaultof<Nullable<int64>>

                        let loaded =
                            SkillLoadedEvent(
                                request.SessionId,
                                request.TurnId,
                                noSequence,
                                request.Timestamp,
                                request.SkillName,
                                ResizeArray<string>(companions) :> IReadOnlyList<string>
                            )

                        return
                            {
                                ResultText = formatResult request.SkillName content companions
                                LoadedEvent = Some loaded
                            }
        }

    /// Inserts the skill tool into the turn's tool map under its name,
    /// replacing any previous entry. The map rides the existing TurnLoop
    /// path: runSuspendableAsync bypasses the permission gate for the skill
    /// name (package metadata needs no decision) and verifies the claim at
    /// the last moment through TurnLoopOptions.VerifyClaim, so a takeover
    /// loser never invokes the loader; the caller journals the loaded event
    /// under the same claim through the fenced journal path. No new gating
    /// lives here.
    /// <param name="tools">The turn's tool map, mutated in place. Must not be null.</param>
    /// <param name="tool">The skill tool to insert. Must not be null.</param>
    /// <returns>The inserted skill tool.</returns>
    let addTo (tools: IDictionary<string, AITool>) (tool: AIFunction) : AIFunction =
        ArgumentNullException.ThrowIfNull(tools)
        ArgumentNullException.ThrowIfNull(tool)
        tools[SkillIdentity.name] <- tool :> AITool
        tool

/// The <c>skill</c> function served to the model. Internal: hosts hold
/// the <see cref="T:Microsoft.Extensions.AI.AIFunction" /> that
/// <see cref="T:Legate.SkillTool" /> hands back, never this type. The
/// function validates the name, loads the content from the active package
/// version, notifies the journal callback on success, and returns the
/// stable result text; unknown names return Error text listing the
/// available skills. A missing or blank name raises
/// <see cref="T:Legate.ToolException" />. Cancellation propagates as-is.
[<Sealed>]
type internal SkillFunction
    (
        store: IAgentPackageStore,
        tenant: TenantId,
        agentId: AgentId,
        sessionId: SessionId,
        turnId: TurnId,
        onLoaded: Func<SkillLoadedEvent, Task>
    ) =
    inherit AIFunction()

    do
        ArgumentNullException.ThrowIfNull(store)

        if isNull (box onLoaded) then
            raise (ArgumentNullException(nameof onLoaded))

    /// This tool's name for error text.
    override _.Name = SkillIdentity.name

    /// This tool's description for the model.
    override _.Description = SkillIdentity.description

    /// This tool's argument schema.
    override _.JsonSchema = SkillSchema.document.RootElement

    /// Validates the call's skill name, loads the content, notifies the
    /// journal callback on success, and returns the stable result text.
    /// Unknown names and unreadable files return Error text with no
    /// journal callback; store failures propagate for the TurnLoop Error
    /// mapping. A missing or blank name raises
    /// <see cref="T:Legate.ToolException" />.
    /// Cancellation propagates as-is.
    override _.InvokeCoreAsync
        (args: AIFunctionArguments, cancellationToken: CancellationToken)
        : ValueTask<obj | null> =
        ValueTask<obj | null>(
            task {
                cancellationToken.ThrowIfCancellationRequested()

                let args = if isNull (box args) then AIFunctionArguments() else args

                match Option.ofObj (SkillSchema.asText (SkillSchema.argument args "name")) with
                | Some name when not (String.IsNullOrWhiteSpace name) ->
                    let! outcome =
                        SkillLoader.loadAsync
                            {
                                Store = store
                                Tenant = tenant
                                AgentId = agentId
                                SessionId = sessionId
                                TurnId = turnId
                                Timestamp = DateTimeOffset.UtcNow
                                SkillName = name
                            }
                            cancellationToken

                    match outcome.LoadedEvent with
                    | Some loaded -> do! onLoaded.Invoke loaded
                    | None -> ()

                    return outcome.ResultText :> obj
                | _ ->
                    return
                        raise (
                            ToolException(
                                SkillIdentity.name,
                                "The skill tool needs a name: pass the skill to load in 'name'."
                            )
                        )
            }
        )

/// Builds the <c>skill</c> built-in tool: the schema the model loads
/// packaged skill content with. The loader reads the active package
/// version from the bound store (authoritative, tenant-isolated) and
/// notifies the journal callback with one
/// <see cref="T:Legate.SkillLoadedEvent" /> per successful load; the
/// caller journals that event under the turn claim. The loader reads
/// package metadata only and bypasses the permission gate (see
/// <see cref="F:Legate.TurnLoop.SkillToolName" />); companion files are
/// read afterward through the normal file tools with the policy applied.
type SkillTool private () =

    /// The tool's name as the model calls it: the skill-content loader.
    /// Validated against <see cref="T:Legate.ToolNameRules" /> when the
    /// tool is built.
    static member ToolName: string = SkillIdentity.name

    /// The tool's description as the model reads it: what the tool does,
    /// its arguments, the stable result shape, the size bound with its
    /// marker, the unknown-name error, and the journaled event.
    static member Description: string = SkillIdentity.description

    /// Builds the tool bound to one package scope and one turn: content
    /// loads resolve against the agent's active version, and each
    /// successful load notifies the journal callback with the event the
    /// caller journals under the turn claim. Bind one tool per turn so the
    /// event carries the loading turn's identities.
    /// <param name="store">The package store the loader reads from. Must not be null.</param>
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose skill loads.</param>
    /// <param name="sessionId">The session the load runs for.</param>
    /// <param name="turnId">The turn the load runs inside.</param>
    /// <param name="onLoaded">The journal callback receiving each successful load's event. Must not be null.</param>
    /// <returns>The tool to offer to the model.</returns>
    static member Create
        (
            store: IAgentPackageStore,
            tenant: TenantId,
            agentId: AgentId,
            sessionId: SessionId,
            turnId: TurnId,
            onLoaded: Func<SkillLoadedEvent, Task>
        ) : AIFunction =
        ArgumentNullException.ThrowIfNull(store)

        if isNull (box onLoaded) then
            raise (ArgumentNullException(nameof onLoaded))

        ToolNameRules.Validate(SkillTool.ToolName) |> ignore
        SkillFunction(store, tenant, agentId, sessionId, turnId, onLoaded) :> AIFunction
