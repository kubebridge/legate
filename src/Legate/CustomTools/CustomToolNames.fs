// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System

// Sanitised model-facing names for agent custom tools (issue 74). Store
// names are validated against ToolNameRules on upsert, but the invocation
// path re-sanitises defensively at load: every character outside
// [a-zA-Z0-9_-] becomes an underscore, overlong names truncate to 128
// characters, and empty or null names drop out. First-claimant-wins dedupe
// then decides which configured tool serves each sanitised form: the first
// tool to claim a form wins, later duplicates lose, and names a built-in
// already serves lose to the built-in. Every rename and every loser is
// reported so the host sees what the model actually calls.

/// What loading decided for one configured tool name.
type internal NameFate =
    /// The name serves its tool unchanged: first claimant, no rename.
    | Claimed
    /// The name serves its tool under a sanitised form; the original is
    /// reported so the host sees the rename.
    | ClaimedRenamed
    /// Another configured tool already claimed the sanitised form; this
    /// tool is dropped. Carries the winning tool's original name.
    | DuplicateLoser of keptOriginal: string
    /// A built-in tool already serves the sanitised form; this tool is
    /// dropped and the built-in keeps first claim.
    | ReservedLoser

/// One configured name with its sanitised form, loading fate, and the
/// caller-supplied payload (the tool row) travelling with it.
type internal ClaimedName<'T> =
    {
        /// The configured name, exactly as the store returned it.
        Original: string
        /// The sanitised model-facing name.
        Sanitized: string
        /// What loading decided for this name.
        Fate: NameFate
        /// The caller-supplied payload for the configured name.
        Payload: 'T
    }

/// Sanitised naming plus first-claimant-wins dedupe for custom tools.
/// Internal: the custom tool source owns the only call site.
module internal CustomToolNames =

    /// The maximum served name length, matching
    /// <see cref="P:Legate.ToolNameRules.Pattern" />.
    [<Literal>]
    let MaxLength = 128

    /// Renders an unusable original for reports: null reads as
    /// <c>&lt;null&gt;</c> so logs never carry a null entry.
    /// <param name="original">The original name, possibly null.</param>
    /// <returns>The report text.</returns>
    let renderUnusable (original: string | null) : string =
        match Option.ofObj original with
        | Some name -> name
        | None -> "<null>"

    /// Tests one character against the served alphabet: ASCII letters,
    /// digits, underscores, and dashes.
    /// <param name="value">The character to test.</param>
    /// <returns>True when the character survives sanitising unchanged.</returns>
    let private isAllowed (value: char) : bool =
        (value >= 'a' && value <= 'z')
        || (value >= 'A' && value <= 'Z')
        || (value >= '0' && value <= '9')
        || value = '_'
        || value = '-'

    /// Sanitises one configured tool name to the served alphabet:
    /// every outside character becomes an underscore and the result
    /// truncates to <see cref="F:Legate.CustomToolNames.MaxLength" />.
    /// <param name="name">The configured name, or null.</param>
    /// <returns>The sanitised name, or None when there is nothing usable.</returns>
    let sanitize (name: string | null) : string option =
        match Option.ofObj name with
        | None -> None
        | Some text when text.Length = 0 -> None
        | Some text ->
            let sanitized =
                let chars =
                    text
                    |> Seq.map (fun value -> if isAllowed value then value else '_')
                    |> Seq.truncate MaxLength
                    |> Seq.toArray

                String(chars)

            if ToolNameRules.TryValidate sanitized then
                Some sanitized
            else
                None

    /// Claims sanitised names first-claimant-wins: candidates are
    /// considered in order, the first to name a sanitised form wins it
    /// unless a reserved (built-in) name already serves it, and every
    /// later duplicate loses. Comparison is ordinal, matching
    /// <see cref="T:Legate.ToolNameRules" />.
    /// <param name="reserved">The sanitised names built-ins already serve.</param>
    /// <param name="candidates">The configured names with their payloads, in load order.</param>
    /// <returns>The claimed names in load order, plus the unusable originals (null renders as &lt;null&gt;).</returns>
    let claimNames<'T>
        (reserved: Set<string>)
        (candidates: (string option * 'T) list)
        : ClaimedName<'T> list * string list =
        let folder
            (claimed: Map<string, string>)
            (decided: ClaimedName<'T> list)
            (unusable: string list)
            (original: string option, payload: 'T)
            =
            match original with
            | None -> (claimed, decided, renderUnusable null :: unusable)
            | Some name ->
                match sanitize name with
                | None -> (claimed, decided, name :: unusable)
                | Some sanitized ->
                    match Map.tryFind sanitized claimed with
                    | Some kept ->
                        let loser =
                            {
                                Original = name
                                Sanitized = sanitized
                                Fate = DuplicateLoser kept
                                Payload = payload
                            }

                        (claimed, loser :: decided, unusable)
                    | None when Set.contains sanitized reserved ->
                        let loser =
                            {
                                Original = name
                                Sanitized = sanitized
                                Fate = ReservedLoser
                                Payload = payload
                            }

                        (claimed, loser :: decided, unusable)
                    | None ->
                        let fate = if sanitized = name then Claimed else ClaimedRenamed

                        let winner =
                            {
                                Original = name
                                Sanitized = sanitized
                                Fate = fate
                                Payload = payload
                            }

                        (Map.add sanitized name claimed, winner :: decided, unusable)

        let (_, decided, unusable) =
            candidates
            |> List.fold (fun (c, d, u) candidate -> folder c d u candidate) (Map.empty, [], [])

        (List.rev decided, List.rev unusable)
