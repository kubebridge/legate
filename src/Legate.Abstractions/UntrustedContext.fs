// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic

// Prompt-rendering helper for UserMessage metadata. The future ReAct loop
// (issues 38-41 own the loop core, budgets, streaming, and the inject fold)
// appends the block rendered here to the user turn text before the provider
// call; journaling and cell derivation keep the verbatim metadata untouched,
// so this rendering is prompt-only and never a second source of truth.
// Rendering is pure and deterministic: reserved control keys are removed,
// the rest sort ordinally with the preserve-list first, and output caps at
// a char bound with a head/tail excerpt plus a truncation marker.

/// Tuning for <see cref="T:Legate.UntrustedContext" />: the rendered char
/// bound and the identifier keys that render first so they survive
/// truncation. Mutable with defaults so hosts can set properties before
/// use; the formatter never mutates it.
type UntrustedContextOptions() =

    /// The maximum rendered block chars, framing and marker included. The
    /// default 16,384 mirrors the BridgeMCP metadata cap. Must be positive
    /// and large enough to hold the block framing; smaller values throw
    /// <see cref="T:System.ArgumentOutOfRangeException" /> at format time.
    member val MaxCharacters: int = 16384 with get, set

    /// Identifier keys (for example request or correlation ids) that render
    /// before every other entry so a truncated block keeps them. Matched
    /// ordinal-ignore-case; entries naming reserved control keys or absent
    /// from the metadata have no effect. Empty by default; a null
    /// assignment formats as empty.
    member val PreserveKeys: IList<string> = ResizeArray<string>() :> IList<string> with get, set

/// Pure prompt-rendering helper that formats a user message's host metadata
/// as an explicitly untrusted block for the model. The loop appends the
/// returned block to the user turn text before the provider call; the
/// journal and derived cells keep the verbatim metadata.
/// <remarks>
/// Rendering rules, in order: null or empty metadata renders null;
/// reserved <c>legate</c> control keys are removed (a spoofed control key
/// never reaches the prompt); the rest sort ordinally with the configured
/// preserve-list first; each entry renders verbatim on its own
/// <c>key=value</c> line inside an <c>untrusted-user-context</c> block;
/// output caps at the configured char bound with a deterministic head/tail
/// excerpt plus a truncation marker. Keys and values render verbatim (no
/// escaping): the block tags mark the whole section untrusted, and only
/// the bound keeps hostile values in check.
/// </remarks>
[<Sealed>]
type UntrustedContext private () =

    static let header = "<untrusted-user-context>\n"
    static let footer = "\n</untrusted-user-context>"
    static let truncationMarker = "\n[truncated]\n"

    // Runtime-owned control namespace: stripped before rendering so host
    // metadata can never spoof a runtime directive. Future runtime control
    // keys must live under this namespace to stay stripped.
    static let isReservedKey (key: string) : bool =
        key.Equals("legate", StringComparison.OrdinalIgnoreCase)
        || key.StartsWith("legate.", StringComparison.OrdinalIgnoreCase)

    /// Formats host metadata as an <c>untrusted-user-context</c> block
    /// under the default tuning (16,384 chars, no preserve-list).
    /// <param name="metadata">Host metadata carried with the message, or null when there is none.</param>
    /// <returns>The block to append to the user turn text, or null when there is no renderable metadata.</returns>
    static member Format(metadata: IReadOnlyDictionary<string, string> | null) : string | null =
        UntrustedContext.Format(metadata, UntrustedContextOptions())

    /// Formats host metadata as an <c>untrusted-user-context</c> block:
    /// reserved control keys removed, the rest ordinally sorted with the
    /// preserve-list first, capped at the configured char bound with a
    /// deterministic head/tail excerpt plus a truncation marker.
    /// <param name="metadata">Host metadata carried with the message, or null when there is none.</param>
    /// <param name="options">The char bound and preserve-list. Must not be null.</param>
    /// <returns>The block to append to the user turn text, or null when there is no renderable metadata.</returns>
    /// <exception cref="T:System.ArgumentNullException">The options are null.</exception>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The char bound is not positive or cannot hold the block framing.</exception>
    static member Format
        (metadata: IReadOnlyDictionary<string, string> | null, options: UntrustedContextOptions)
        : string | null =
        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        if options.MaxCharacters < 1 then
            raise (
                ArgumentOutOfRangeException(nameof options, "UntrustedContextOptions.MaxCharacters must be positive.")
            )

        if isNull (box metadata) then
            null
        else
            let entries = ResizeArray<string * string>()

            for entry in metadata do
                if
                    not (isNull (box entry.Key))
                    && entry.Key.Length > 0
                    && not (isReservedKey entry.Key)
                then
                    let value = if isNull (box entry.Value) then "" else entry.Value
                    entries.Add(entry.Key, value)

            if entries.Count = 0 then
                null
            else
                let preserve =
                    let set = HashSet<string>(StringComparer.OrdinalIgnoreCase)

                    if not (isNull (box options.PreserveKeys)) then
                        for key in options.PreserveKeys do
                            if not (isNull (box key)) && key.Length > 0 then
                                set.Add key |> ignore

                    set

                // Stable partition of the ordinal sort: preserved entries
                // first, ordinal order kept inside each group.
                let preserved, rest =
                    entries
                    |> Seq.sortWith (fun (left, _) (right, _) -> String.CompareOrdinal(left, right))
                    |> Seq.toList
                    |> List.partition (fun (key, _) -> preserve.Contains key)

                let lines =
                    (preserved @ rest)
                    |> List.map (fun (key, value) -> key + "=" + value)
                    |> List.toArray

                let body = String.Join("\n", lines)
                let full = header + body + footer

                if full.Length <= options.MaxCharacters then
                    full
                else
                    let budget =
                        options.MaxCharacters - header.Length - truncationMarker.Length - footer.Length

                    if budget < 1 then
                        raise (
                            ArgumentOutOfRangeException(
                                nameof options,
                                "UntrustedContextOptions.MaxCharacters is too small to hold the block framing."
                            )
                        )

                    // The head takes the ceiling half so preserve-list
                    // identifiers at the start of the body survive.
                    let headLength = (budget + 1) / 2
                    let tailLength = budget - headLength

                    let excerpt =
                        body.Substring(0, headLength)
                        + truncationMarker
                        + body.Substring(body.Length - tailLength)

                    header + excerpt + footer
