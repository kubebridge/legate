// SPDX-License-Identifier: Apache-2.0
module Dot.DotContext

open System
open System.Collections.Generic
open System.IO

// Dot context files (issue 309): pi-style project instructions resolved at
// session open and passed as SessionOptions.HostInstructionFiles, which the
// runtime re-reads every turn (missing, unreadable, blank, or over-bound
// files skip silently, so one bad path never fails a turn). Dot's default
// prompt is empty (agents carry SystemPrompt=""), so a present SYSTEM.md
// occupies the lead host-file slot as the project prompt and every AGENTS.md
// appends after it; absent files simply contribute nothing.

// ──────────────────────────────────────────────────────────────────────────
// Resolution

/// The AGENTS.md file name resolved in every directory from the filesystem
/// root down to the working directory.
let agentsFileName = "AGENTS.md"

/// The SYSTEM.md file name resolved upward from the working directory.
let systemFileName = "SYSTEM.md"

/// Lists the directories from the filesystem root down to the given
/// directory, root first.
/// <param name="directory">The directory to walk up from. Must not be null.</param>
/// <returns>The directory chain, root first.</returns>
let private chainFromRoot (directory: string) : string list =
    let mutable chain: string list = []
    let mutable current = Path.GetFullPath directory
    let mutable go = true

    while go do
        chain <- current :: chain

        match Option.ofObj (Path.GetDirectoryName current) with
        | None -> go <- false
        | Some parent when String.Equals(parent, current, StringComparison.Ordinal) -> go <- false
        | Some parent -> current <- parent

    chain

/// Resolves the host instruction files for one working directory: the
/// nearest SYSTEM.md upward from the directory first (the override slot),
/// then the AGENTS.md chain from the filesystem root down to the directory
/// (root first), existing files only. The runtime re-reads every listed
/// path on every turn, so an edit between turns steers the next turn.
/// <param name="workingDirectory">The working directory to resolve from.</param>
/// <returns>The ordered host-file paths, possibly empty.</returns>
let resolveHostInstructionFiles (workingDirectory: string) : IReadOnlyList<string> =
    let files = ResizeArray<string>()

    if not (String.IsNullOrWhiteSpace workingDirectory) then
        try
            let full = Path.GetFullPath workingDirectory
            let chain = chainFromRoot full

            let nearestSystem =
                chain
                |> List.rev
                |> List.tryPick (fun directory ->
                    let candidate = Path.Combine(directory, systemFileName)

                    if File.Exists candidate then Some candidate else None)

            match nearestSystem with
            | Some system -> files.Add system
            | None -> ()

            for directory in chain do
                let candidate = Path.Combine(directory, agentsFileName)

                if File.Exists candidate then
                    files.Add candidate
        with _ ->
            ()

    files :> IReadOnlyList<string>
