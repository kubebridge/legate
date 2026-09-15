// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open YamlDotNet.RepresentationModel

// Runtime-internal skill discovery (issue 67): enumerate the active package
// version's skills, parse each SKILL.md frontmatter as real YAML, build the
// pure <available_skills> block #66 embeds, and stage companion files into
// the workspace at their package-relative paths for #68's turn-time load.
// Invalid skills are never dropped silently: each maps to one
// SkillInvalidEvent carrying the skill name and the client-safe reason.
//
// allowed-tools is advisory text here; enforcement lives in #68/TurnLoop.
// Staging overwrites unconditionally: IWorkspace.WriteFile has no
// create-only mode and replaces atomically. YamlDotNet is referenced only
// by this runtime project so Legate.Abstractions stays BCL-only.
module internal SkillDiscovery =

    /// Upper bound on one SKILL.md or companion read during discovery: a
    /// larger file maps to a SkillInvalidEvent rather than landing in
    /// memory unbounded.
    [<Literal>]
    let MaxSkillFileBytes = 262144

    /// One skill whose frontmatter parsed: the block entry plus the name
    /// companion staging scopes to.
    type SkillSummary =
        {
            /// The skill name, verbatim from the frontmatter's name field.
            Name: string
            /// The skill description, verbatim except a block-scalar
            /// trailing newline chomp; empty when the field is absent.
            Description: string
            /// The advisory allowed-tools text, or null when the field is
            /// absent: a scalar verbatim, or a sequence of tool names joined
            /// with a comma and a space. Never enforced here.
            AllowedTools: string | null
        }

    /// Everything one discovery pass needs: the store and tenant scope to
    /// read from, the workspace to stage into, and the identities one
    /// diagnostic event carries.
    type SkillDiscoveryRequest =
        {
            /// The package store discovery reads the active version from.
            Store: IAgentPackageStore
            /// The tenant the agent belongs to.
            Tenant: TenantId
            /// The agent whose skills are discovered.
            AgentId: AgentId
            /// The bound workspace companions stage into.
            Workspace: IWorkspace
            /// The session discovery runs for.
            SessionId: SessionId
            /// The turn discovery runs inside.
            TurnId: TurnId
            /// When the pass ran: the stamp every diagnostic event carries.
            Timestamp: DateTimeOffset
        }

    /// What one discovery pass produced: the block for the prompt, the
    /// package-relative companion paths that landed in the workspace, and
    /// one diagnostic event per skipped skill or failed companion.
    type SkillDiscoveryResult =
        {
            /// The <available_skills> block over the valid skills, empty of
            /// skill lines when none parsed.
            AvailableSkillsBlock: string
            /// The package-relative companion paths staged into the
            /// workspace, in discovery order.
            StagedCompanions: string list
            /// One SkillInvalidEvent per skipped skill or failed companion.
            Diagnostics: SessionEvent list
        }

    /// Splits a SKILL.md document into its YAML frontmatter: the lines
    /// between the leading and closing delimiter lines, where a delimiter
    /// is a line whose trimmed form is exactly ---. Total: any input maps
    /// to frontmatter or a reason, never an exception.
    /// <param name="text">The SKILL.md text, or null when unreadable.</param>
    /// <returns>The frontmatter lines joined with line feeds, or the reason there is none.</returns>
    let splitFrontmatter (text: string) : Result<string, string> =
        if isNull (box text) then
            Error "the SKILL.md is missing: there is no text to split"
        else
            let lines = text.Split '\n' |> Array.map (fun line -> line.TrimEnd '\r')

            if lines.Length = 0 || lines[0].Trim() <> "---" then
                Error "the SKILL.md has no YAML frontmatter: the first line is not '---'"
            else
                let closer =
                    lines
                    |> Array.indexed
                    |> Array.tryPick (fun (index, line) ->
                        if index > 0 && line.Trim() = "---" then
                            Some index
                        else
                            None)

                match closer with
                | Some closing -> Ok(String.Join("\n", lines[1 .. closing - 1]))
                | None -> Error "the SKILL.md frontmatter has no closing '---' delimiter"

    /// Narrows one possibly-null YAML scalar value to non-null text,
    /// mapping null to empty: the single null-handling point for
    /// YamlDotNet scalar values, whose nullability the F# compiler cannot
    /// prove absent.
    /// <param name="value">The scalar value, possibly null.</param>
    /// <returns>The value, or empty when null.</returns>
    let private orEmpty (value: string | null) : string =
        match value with
        | null -> ""
        | text -> text

    /// Reads the skill fields out of one frontmatter mapping: name is
    /// required and must be a non-blank scalar, description is optional
    /// text defaulting to empty, and allowed-tools is optional text or a
    /// list of tool names joined with a comma and a space. Unknown fields
    /// are ignored so newer writers stay forward-compatible; a repeated
    /// field or a mistyped known field is invalid.
    /// <param name="mapping">The frontmatter's root mapping.</param>
    /// <returns>The parsed summary, or the reason the mapping is not a skill.</returns>
    let private readMapping (mapping: YamlMappingNode) : Result<SkillSummary, string> =
        let fields = Dictionary<string, YamlNode>(StringComparer.Ordinal)
        let mutable failure: string | null = null

        for pair in mapping.Children do
            if isNull (box failure) then
                match pair.Key with
                | :? YamlScalarNode as key ->
                    let field = orEmpty key.Value

                    if fields.ContainsKey field then
                        failure <- sprintf "the frontmatter repeats the '%s' field" field
                    else
                        fields[field] <- pair.Value
                | _ -> failure <- "the frontmatter fields must be plain names"

        match failure with
        | null ->
            let name =
                match fields.TryGetValue "name" with
                | true, (:? YamlScalarNode as scalar) -> orEmpty scalar.Value
                | _ -> ""

            if String.IsNullOrWhiteSpace name then
                Error "the skill frontmatter has no 'name'"
            else
                let descriptionResult =
                    match fields.TryGetValue "description" with
                    | false, _ -> Ok ""
                    | true, node ->
                        match node with
                        | :? YamlScalarNode as scalar -> Ok((orEmpty scalar.Value).TrimEnd('\r', '\n'))
                        | _ -> Error "the 'description' field must be plain text"

                match descriptionResult with
                | Error reason -> Error reason
                | Ok description ->
                    let toolsResult: Result<string | null, string> =
                        match fields.TryGetValue "allowed-tools" with
                        | false, _ -> Ok null
                        | true, node ->
                            match node with
                            | :? YamlScalarNode as scalar -> Ok(orEmpty scalar.Value)
                            | :? YamlSequenceNode as sequence ->
                                let tools = ResizeArray<string>()
                                let mutable bad = false

                                for child in sequence.Children do
                                    match child with
                                    | :? YamlScalarNode as item ->
                                        match item.Value with
                                        | null -> bad <- true
                                        | text -> tools.Add text
                                    | _ -> bad <- true

                                if bad then
                                    Error "the 'allowed-tools' list must hold plain tool names"
                                else
                                    Ok(String.Join(", ", tools))
                            | _ -> Error "the 'allowed-tools' field must be text or a list of tool names"

                    match toolsResult with
                    | Error reason -> Error reason
                    | Ok tools ->
                        Ok
                            {
                                Name = name
                                Description = description
                                AllowedTools = tools
                            }
        | reason -> Error reason

    /// Parses one frontmatter document into a skill summary with
    /// YamlDotNet's representation model, so multi-line and quoted values
    /// parse as YAML defines them. A YamlException maps to the invalid
    /// reason; any other input maps there too, so parsing is total over
    /// package text.
    /// <param name="yaml">The frontmatter text between the delimiters.</param>
    /// <returns>The parsed summary, or the reason the frontmatter is not a skill.</returns>
    let parseFrontmatter (yaml: string) : Result<SkillSummary, string> =
        try
            use reader = new StringReader(yaml)
            let stream = YamlStream()
            stream.Load reader

            if stream.Documents.Count = 0 then
                Error "the frontmatter holds no YAML document"
            else
                match stream.Documents[0].RootNode with
                | :? YamlMappingNode as mapping -> readMapping mapping
                | _ -> Error "the frontmatter must be a YAML mapping of skill fields"
        with :? YamlDotNet.Core.YamlException as ex ->
            Error(sprintf "the frontmatter is not valid YAML: %s" ex.Message)

    /// Parses one SKILL.md document: split the frontmatter, then parse it
    /// as YAML. Total: any input maps to a summary or a reason.
    /// <param name="text">The SKILL.md text, or null when unreadable.</param>
    /// <returns>The parsed summary, or the reason the document is not a skill.</returns>
    let parseSkillDocument (text: string) : Result<SkillSummary, string> =
        match splitFrontmatter text with
        | Error reason -> Error reason
        | Ok yaml -> parseFrontmatter yaml

    /// Escapes one attribute value of the skills block.
    /// <param name="value">The raw attribute text.</param>
    /// <returns>The text with XML attribute metacharacters escaped.</returns>
    let private escapeAttribute (value: string) : string =
        value.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("\"", "&quot;")

    /// Builds the pure available-skills block over the valid skills,
    /// ordered ordinally by name: one self-closing skill line per skill
    /// carrying the name, the description, and the advisory allowed-tools
    /// text only when the skill specified it.
    /// <param name="skills">The valid skill summaries.</param>
    /// <returns>The block text #66 embeds in the prompt.</returns>
    let buildAvailableSkillsBlock (skills: SkillSummary list) : string =
        let lines = ResizeArray<string>()
        lines.Add "<available_skills>"

        for skill in
            skills
            |> List.sortWith (fun left right -> String.CompareOrdinal(left.Name, right.Name)) do
            let name = escapeAttribute skill.Name
            let description = escapeAttribute skill.Description

            match skill.AllowedTools with
            | null -> lines.Add(sprintf "<skill name=\"%s\" description=\"%s\" />" name description)
            | tools ->
                lines.Add(
                    sprintf
                        "<skill name=\"%s\" description=\"%s\" allowed-tools=\"%s\" />"
                        name
                        description
                        (escapeAttribute tools)
                )

        lines.Add "</available_skills>"
        String.Join("\n", lines)

    /// The SKILL.md path of one listed skill name. Internal so the skill
    /// loader (issue 68) resolves the same path discovery reads.
    /// <param name="skillName">The skill name package info listed.</param>
    /// <returns>The package-relative SKILL.md path.</returns>
    let skillFilePath (skillName: string) : string =
        ".agent/skills/" + skillName + "/SKILL.md"

    /// The companion prefix of one listed skill name: everything staged
    /// lives under it. Internal so the skill loader (issue 68) lists the
    /// same companions discovery stages.
    /// <param name="skillName">The skill name package info listed.</param>
    /// <returns>The package-relative skill directory prefix.</returns>
    let skillPrefix (skillName: string) : string = ".agent/skills/" + skillName + "/"

    /// Reads one store stream up to the discovery bound and disposes it.
    /// Internal so the skill loader (issue 68) reuses the same bound;
    /// the loader truncates with the marker where discovery reports invalid.
    /// <param name="source">The stream to drain; always disposed.</param>
    /// <returns>The bytes, or the reason the file cannot be discovered.</returns>
    let readBounded (source: Stream) : Result<byte[], string> =
        try
            use _source = source
            use memory = new MemoryStream()
            let buffer = Array.zeroCreate<byte> 8192
            let mutable total = 0
            let mutable finished = false
            let mutable tooLarge = false

            while not finished do
                let read = source.Read(buffer, 0, buffer.Length)

                if read = 0 then
                    finished <- true
                elif total + read > MaxSkillFileBytes then
                    tooLarge <- true
                    finished <- true
                else
                    total <- total + read
                    memory.Write(buffer, 0, read)

            if tooLarge then
                Error(sprintf "the file exceeds the %d-byte discovery bound" MaxSkillFileBytes)
            else
                Ok(memory.ToArray())
        with ex ->
            Error(sprintf "the file could not be read: %s" ex.Message)

    /// Loads and parses one listed skill's SKILL.md from the active
    /// version.
    /// <param name="store">The package store to read from.</param>
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose skill loads.</param>
    /// <param name="version">The active version to read from.</param>
    /// <param name="skillName">The skill name package info listed.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The parsed summary, or the reason the skill is invalid.</returns>
    let private loadSkill
        (
            store: IAgentPackageStore,
            tenant: TenantId,
            agentId: AgentId,
            version: string,
            skillName: string,
            cancellationToken: CancellationToken
        ) : Task<Result<SkillSummary, string>> =
        task {
            let! stream = store.ReadFile(tenant, agentId, version, skillFilePath skillName, cancellationToken)

            match stream with
            | null -> return Error(sprintf "the skill '%s' has no readable SKILL.md" skillName)
            | source ->
                match readBounded source with
                | Error reason -> return Error reason
                | Ok bytes -> return parseSkillDocument (Encoding.UTF8.GetString bytes)
        }

    /// Runs one discovery pass over the active package version: parse each
    /// listed skill, stage every companion under its directory into the
    /// workspace at its package-relative path (always overwriting, per the
    /// WriteFile contract), and report each skip as a diagnostic event.
    /// A failed companion excludes only that file: the skill stays in the
    /// block.
    /// <param name="request">The store, scope, workspace, and identities the pass runs with.</param>
    /// <param name="cancellationToken">Token that abandons the pass.</param>
    /// <returns>The block, the staged companion paths, and the diagnostics.</returns>
    let discoverAndStage
        (request: SkillDiscoveryRequest)
        (cancellationToken: CancellationToken)
        : Task<SkillDiscoveryResult> =
        task {
            let empty =
                {
                    AvailableSkillsBlock = buildAvailableSkillsBlock []
                    StagedCompanions = []
                    Diagnostics = []
                }

            let! info = request.Store.GetPackageInfo(request.Tenant, request.AgentId, cancellationToken)

            match info with
            | null -> return empty
            | package ->
                match package.ActiveVersion with
                | null -> return empty
                | version ->
                    let summaries = ResizeArray<SkillSummary>()
                    let staged = ResizeArray<string>()
                    let diagnostics = ResizeArray<SessionEvent>()
                    let noSequence = Unchecked.defaultof<Nullable<int64>>

                    let invalid skillName reason =
                        SkillInvalidEvent(
                            request.SessionId,
                            request.TurnId,
                            noSequence,
                            request.Timestamp,
                            skillName,
                            reason
                        )
                        :> SessionEvent
                        |> diagnostics.Add

                    for skillName in package.Skills do
                        let! parsed =
                            loadSkill (
                                request.Store,
                                request.Tenant,
                                request.AgentId,
                                version,
                                skillName,
                                cancellationToken
                            )

                        match parsed with
                        | Error reason -> invalid skillName reason
                        | Ok summary ->
                            summaries.Add summary

                            let! files =
                                request.Store.ListFiles(
                                    request.Tenant,
                                    request.AgentId,
                                    version,
                                    skillPrefix skillName,
                                    cancellationToken
                                )

                            let skillFile = skillFilePath skillName

                            for path in files do
                                if path <> skillFile then
                                    let! source =
                                        request.Store.ReadFile(
                                            request.Tenant,
                                            request.AgentId,
                                            version,
                                            path,
                                            cancellationToken
                                        )

                                    match source with
                                    | null ->
                                        invalid skillName (sprintf "the companion '%s' is no longer readable" path)
                                    | stream ->
                                        match readBounded stream with
                                        | Error reason -> invalid skillName reason
                                        | Ok bytes ->
                                            try
                                                do! request.Workspace.WriteFile(path, bytes, cancellationToken)
                                                staged.Add path
                                            with ex ->
                                                invalid skillName (sprintf "staging '%s' failed: %s" path ex.Message)

                    return
                        {
                            AvailableSkillsBlock = buildAvailableSkillsBlock (summaries |> Seq.toList)
                            StagedCompanions = staged |> Seq.toList
                            Diagnostics = diagnostics |> Seq.toList
                        }
        }
