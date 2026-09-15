// SPDX-License-Identifier: Apache-2.0
module internal Legate.Agents.AgentFileParser

open System
open System.Collections.Generic
open System.IO
open Legate
open YamlDotNet.Serialization

// Frontmatter schema for one `.agent/agents/*.md` file. The YAML block
// between the opening and closing `---` markers carries the known keys;
// the body after the closing marker maps verbatim to the agent's
// SystemPrompt. Unknown frontmatter keys are ignored so prompt-related
// experimentation never breaks the load. Uses YamlDotNet, the same YAML
// library as the skill discovery epic (#67), with a separate agent schema:
// skill frontmatter keys differ from agent keys, so only the library is
// shared, never the parser.

// ──────────────────────────────────────────────────────────────────────────
// Parsed shape

/// The fallback model reference when frontmatter omits <c>model</c>: file
/// agents stay loadable and the host resolves the reference before a turn
/// runs.
let internal defaultModel: ModelReference = ModelReference.Parse "legate/default"

/// One parsed agent file: the known frontmatter keys plus the body as the
/// system prompt.
type internal AgentFileDefinition =
    {
        /// The agent's display name, from the required <c>name</c> key.
        Name: string
        /// What the agent is for, from <c>description</c>, or null.
        Description: string | null
        /// The model the agent runs on, from <c>model</c>, or the default.
        Model: ModelReference
        /// Whether new sessions are accepted, from <c>enabled</c>.
        Enabled: bool
        /// The verbatim body after the closing marker.
        SystemPrompt: string
    }

// ──────────────────────────────────────────────────────────────────────────
// Frontmatter split and YAML mapping

/// Splits raw file text into frontmatter YAML and body. The file must open
/// with a <c>---</c> line and close the block with a second <c>---</c>
/// line; the body is everything after the closing line's break, verbatim.
/// <param name="path">The file the text was read from, for diagnostics.</param>
/// <param name="text">The raw file text.</param>
/// <returns>The frontmatter YAML and the verbatim body.</returns>
/// <exception cref="T:System.InvalidOperationException">The markers are missing.</exception>
let private splitFrontmatter (path: string) (text: string) : string * string =
    let lineOf (start: int) =
        let lineEnd = text.IndexOf('\n', start)
        let endPos = if lineEnd < 0 then text.Length else lineEnd
        let line = text.Substring(start, endPos - start)
        let trimmed = line.TrimEnd('\r').Trim()
        trimmed, endPos

    let first, firstEnd = lineOf 0

    if first <> "---" then
        raise (
            InvalidOperationException(sprintf "Agent file '%s' is missing its opening '---' frontmatter marker." path)
        )

    let mutable pos = if firstEnd < text.Length then firstEnd + 1 else text.Length
    let mutable found: (string * string) option = None

    while found.IsNone && pos <= text.Length do
        let line, lineEnd = lineOf pos

        if line = "---" then
            let yaml = text.Substring(firstEnd + 1, pos - (firstEnd + 1))

            let body =
                if lineEnd < text.Length then
                    text.Substring(lineEnd + 1)
                else
                    ""

            found <- Some(yaml, body)
        elif lineEnd >= text.Length then
            pos <- text.Length + 1
        else
            pos <- lineEnd + 1

    match found with
    | Some pair -> pair
    | None ->
        raise (
            InvalidOperationException(sprintf "Agent file '%s' is missing its closing '---' frontmatter marker." path)
        )

/// Reads one frontmatter value, case-insensitively: absent keys surface as
/// null boxed values.
/// <param name="values">The parsed frontmatter mapping.</param>
/// <param name="key">The key to read.</param>
/// <returns>The boxed value, or null when absent.</returns>
let private valueOf (values: Dictionary<string, obj>) (key: string) : obj | null =
    let mutable boxed: obj | null = null

    for pair in values do
        if String.Equals(pair.Key, key, StringComparison.OrdinalIgnoreCase) then
            boxed <- pair.Value

    boxed

/// Parses one agent file into its definition. Fail-fast: invalid YAML or a
/// missing <c>name</c> raises naming the file, so a broken CLI agent file
/// is loud, never silent.
/// <param name="path">The agent file to parse.</param>
/// <returns>The parsed definition with the body as the system prompt.</returns>
/// <exception cref="T:System.ArgumentNullException">The path is null.</exception>
/// <exception cref="T:System.InvalidOperationException">The markers are missing, the YAML is invalid, or <c>name</c> is missing.</exception>
let parseFile (path: string) : AgentFileDefinition =
    ArgumentNullException.ThrowIfNull(path)

    let text =
        try
            File.ReadAllText(path)
        with ex ->
            raise (InvalidOperationException(sprintf "Agent file '%s' could not be read." path, ex))

    let yaml, body = splitFrontmatter path text

    let values: Dictionary<string, obj> =
        try
            if String.IsNullOrWhiteSpace yaml then
                Dictionary<string, obj>(StringComparer.OrdinalIgnoreCase)
            else
                let parsed =
                    DeserializerBuilder().Build().Deserialize<Dictionary<string, obj>>(yaml)

                if isNull (box parsed) then
                    Dictionary<string, obj>(StringComparer.OrdinalIgnoreCase)
                else
                    parsed
        with ex ->
            raise (InvalidOperationException(sprintf "Agent file '%s' has invalid YAML frontmatter." path, ex))

    let name =
        match valueOf values "name" with
        | :? string as raw when not (String.IsNullOrWhiteSpace raw) -> raw.Trim()
        | _ ->
            raise (
                InvalidOperationException(
                    sprintf "Agent file '%s' is missing its required 'name' frontmatter key." path
                )
            )

    let description =
        match valueOf values "description" with
        | null -> null
        | :? string as raw -> raw
        | _ ->
            raise (
                InvalidOperationException(
                    sprintf "Agent file '%s' has a non-string 'description' frontmatter value." path
                )
            )

    let model =
        match valueOf values "model" with
        | null -> defaultModel
        | :? string as raw when not (String.IsNullOrWhiteSpace raw) ->
            try
                ModelReference.Parse(raw.Trim())
            with ex ->
                raise (
                    InvalidOperationException(
                        sprintf "Agent file '%s' has an invalid 'model' frontmatter value." path,
                        ex
                    )
                )
        | _ ->
            raise (
                InvalidOperationException(sprintf "Agent file '%s' has a non-string 'model' frontmatter value." path)
            )

    let enabled =
        match valueOf values "enabled" with
        | null -> true
        | :? bool as flag -> flag
        | :? string as raw ->
            let mutable flag = false

            if Boolean.TryParse(raw.Trim(), &flag) then
                flag
            else
                raise (
                    InvalidOperationException(
                        sprintf "Agent file '%s' has a non-boolean 'enabled' frontmatter value." path
                    )
                )
        | _ ->
            raise (
                InvalidOperationException(sprintf "Agent file '%s' has a non-boolean 'enabled' frontmatter value." path)
            )

    {
        Name = name
        Description = description
        Model = model
        Enabled = enabled
        SystemPrompt = body
    }
