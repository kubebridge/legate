// SPDX-License-Identifier: Apache-2.0
module Dot.DotDedup

open System
open Legate

// Consumer-side deduplication primitives for the dot host (issue 385):
// the durable event identity plus the settlement suffix rule shared by the
// plain REPL engine, the fullscreen renderer fold, and the one-shot print
// path. Samples/Dot only: no src/ changes, contracts consumed as-is.
// Compiled before ReplEngine so every consumer links one definition.

/// Names the session an event belongs to: the durable identity scope.
/// Never throws and never returns null: unknown shapes read as empty.
/// <param name="evt">The event naming the session. Must not be null.</param>
/// <returns>The session key, or empty for unknown shapes.</returns>
let sessionKeyOf (evt: SessionEvent) : string =
    try
        let text = evt.SessionId.ToString()

        if isNull (box text) then "" else text
    with _ ->
        ""

/// Names the turn an event was raised inside: the streamed-prefix scope.
/// Never throws and never returns null: the unstamped host-operation
/// sentinel reads as empty and keeps the active turn.
/// <param name="evt">The event naming the turn. Must not be null.</param>
/// <returns>The turn key, or empty for the unstamped sentinel.</returns>
let turnKeyOf (evt: SessionEvent) : string =
    try
        let text = evt.TurnId.ToString()

        if isNull (box text) then "" else text
    with _ ->
        ""

/// Names the durable identity of one journaled event: the (session,
/// sequence) pair the store stamps on append. Duplicate transport delivery
/// repeats the identical pair, so consumers deduplicate on it; distinct
/// same-text events carry distinct sequences and stay distinct. In-flight
/// events carry no sequence and never deduplicate.
/// <param name="evt">The event to identify. Null reads as no identity.</param>
/// <returns>The durable identity, or None for null and in-flight events.</returns>
let durableKeyOf (evt: SessionEvent) : string option =
    if isNull (box evt) then
        None
    elif evt.Sequence.HasValue then
        Some $"{sessionKeyOf evt}:{evt.Sequence.Value}"
    else
        None

/// Names the genuinely unrendered suffix of settlement text: the
/// settlement with the already-streamed prefix stripped. Settlement that
/// exactly repeats the prefix renders nothing; settlement the prefix fully
/// covers renders nothing; settlement that does not continue the prefix
/// (event/settlement races, turn or session mismatch) renders fully, so
/// valid output is never suppressed. Never invents text: the result is
/// always empty or a suffix of the settlement itself.
/// <param name="streamed">The assistant prefix already rendered. Null reads as empty.</param>
/// <param name="settlement">The settlement assistant text. Null reads as empty.</param>
/// <returns>The suffix to render, or empty when nothing new remains.</returns>
let settlementSuffix (streamed: string | null) (settlement: string | null) : string =
    let prefix =
        match streamed with
        | null -> ""
        | text -> text

    let finalText =
        match settlement with
        | null -> ""
        | text -> text

    if finalText = "" then
        ""
    elif prefix = "" then
        finalText
    elif finalText.StartsWith(prefix, StringComparison.Ordinal) then
        finalText.Substring(prefix.Length)
    elif prefix.StartsWith(finalText, StringComparison.Ordinal) then
        ""
    else
        finalText
