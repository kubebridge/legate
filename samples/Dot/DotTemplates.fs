// SPDX-License-Identifier: Apache-2.0
module Dot.DotTemplates

open System
open System.IO

// Dot prompt templates (issue 309): Markdown files under
// <cwd>/.agent/templates/<name>.md expanded verbatim as the next prompt.
// No substitution syntax: the file text becomes the user message unchanged.
// Unknown names error listing the available names.

// ──────────────────────────────────────────────────────────────────────────
// Resolution

/// The template directory segments under the working directory.
let private templateSegments = [| ".agent"; "templates" |]

/// The template file extension.
let templateExtension = ".md"

/// Resolves the templates directory under one working directory.
/// <param name="workingDirectory">The working directory. Must not be null.</param>
/// <returns>The templates directory path.</returns>
let templatesDirectory (workingDirectory: string) : string =
    ArgumentNullException.ThrowIfNull(workingDirectory)
    Path.Combine(workingDirectory, templateSegments[0], templateSegments[1])

/// True when the template name is safe to resolve: non-blank with no
/// separators, no drive prefix, and no dot segments, so resolution can
/// never escape the templates directory.
/// <param name="name">The template name.</param>
/// <returns>True when the name resolves safely.</returns>
let isValidName (name: string | null) : bool =
    match name with
    | null -> false
    | text when String.IsNullOrWhiteSpace text -> false
    | text ->
        let trimmed = text.Trim()

        trimmed.Length > 0
        && not (trimmed.Contains('/', StringComparison.Ordinal))
        && not (trimmed.Contains('\\'))
        && not (trimmed.Contains('\u0000'))
        && trimmed <> "."
        && trimmed <> ".."
        && not (trimmed.Contains("..", StringComparison.Ordinal))
        && (trimmed.Length < 2 || trimmed[1] <> ':')

/// Lists the available template names: the .md file names under the
/// templates directory without their extension, ordinally sorted. A
/// missing directory lists as empty.
/// <param name="workingDirectory">The working directory.</param>
/// <returns>The available template names.</returns>
let listTemplates (workingDirectory: string) : string list =
    if String.IsNullOrWhiteSpace workingDirectory then
        []
    else
        try
            let directory = templatesDirectory workingDirectory

            if Directory.Exists directory then
                Directory.EnumerateFiles(directory, "*.md", SearchOption.TopDirectoryOnly)
                |> Seq.map Path.GetFileNameWithoutExtension
                |> Seq.choose (fun name ->
                    match Option.ofObj name with
                    | Some valid when isValidName valid -> Some valid
                    | _ -> None)
                |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
                |> List.ofSeq
            else
                []
        with _ ->
            []

/// Reads one template's text verbatim: Some content when the name is valid
/// and the file exists, None otherwise (unknown names and unreadable files
/// share the missing branch; the caller lists the available names).
/// <param name="workingDirectory">The working directory.</param>
/// <param name="name">The template name.</param>
/// <returns>The template text, or None when the template is missing.</returns>
let tryReadTemplate (workingDirectory: string) (name: string) : string option =
    if String.IsNullOrWhiteSpace workingDirectory || not (isValidName name) then
        None
    else
        try
            let path =
                Path.Combine(templatesDirectory workingDirectory, name.Trim() + templateExtension)

            if File.Exists path then
                Some(File.ReadAllText path)
            else
                None
        with _ ->
            None
