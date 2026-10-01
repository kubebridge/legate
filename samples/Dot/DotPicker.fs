// SPDX-License-Identifier: Apache-2.0
module Dot.DotPicker

open System

// Shared navigable picker for the dot TUI (issue 334): one stdlib-only
// pure model (list, filter, up/down navigate, select, cancel) reused by
// every selection command instead of per-command bespoke UI. The TUI
// captures keys only: a pick resolves to the exact line the plain REPL
// already routes ("/resume <id>", "/model <id>", "/fork <sequence>"),
// which DotTui feeds through ReplEngine.HandleLineAsync verbatim, so the
// TUI stays a second skin with identical outcomes. TTY-free: every
// function is pure over its inputs, so navigation, filtering, triggers,
// and line resolution are proven headlessly without a terminal. No new
// NuGet dependencies; samples/Dot only, no src/ changes.

// ──────────────────────────────────────────────────────────────────────────
// Model

/// Which command the picker serves: the picked key always resolves to a
/// verbatim engine line, never to a divergent behavior.
type PickerKind =
    /// Pick a stored session to attach: resolves to "/resume <id>".
    | ResumeSession
    /// Pick a registered provider to switch to: resolves to "/model <id>".
    | SwitchModel
    /// Pick a journal sequence to branch at: resolves to "/fork <sequence>".
    | ForkAtSequence

/// One pickable row: the key the resolved line carries plus its display.
type PickerItem =
    {
        /// The id the resolved command line carries (session id, provider
        /// id, or journal sequence text).
        Key: string
        /// The primary display text (session title, provider id, event name).
        Label: string
        /// The secondary display text (session state, model default, detail).
        Detail: string
    }

/// The picker state: the served command, its items in stable order, the
/// typed filter, and the cursor over the visible (filtered) rows.
type PickerState =
    {
        /// The command the picker serves.
        Kind: PickerKind
        /// The full item list in stable order. Never null.
        Items: PickerItem list
        /// The typed substring filter. Never null.
        Filter: string
        /// The cursor over the visible rows, zero-based.
        Cursor: int
    }

/// What one picker key means: stay open, pick the cursor row, or cancel.
type PickerOutcome =
    /// Stay open on the next state.
    | Stay
    /// Select the cursor row and close.
    | Pick of PickerItem
    /// Close without selecting.
    | Cancel

// ──────────────────────────────────────────────────────────────────────────
// Construction and views

/// The maximum typed filter characters retained.
let maxFilterChars = 120

/// The maximum picker rows the overlay paints (the frame budget caps the
/// rest with a "more" marker).
let maxVisibleRows = 8

/// Builds a picker over the items with a clear filter at the first row.
/// <param name="kind">The command the picker serves.</param>
/// <param name="items">The items in stable order. Null becomes empty.</param>
/// <returns>The picker state.</returns>
let fromItems (kind: PickerKind) (items: PickerItem list) : PickerState =
    {
        Kind = kind
        Items =
            if isNull (box items) then
                []
            else
                items |> List.filter (fun item -> not (isNull (box item)))
        Filter = ""
        Cursor = 0
    }

/// Names the header the overlay paints for one picker kind.
/// <param name="kind">The command the picker serves.</param>
/// <returns>The header text.</returns>
let headerText (kind: PickerKind) : string =
    match kind with
    | ResumeSession -> "resume session"
    | SwitchModel -> "switch model"
    | ForkAtSequence -> "fork at sequence"

/// True when the item matches the filter: the empty filter matches
/// everything, else a case-insensitive substring over key, label, and
/// detail together.
/// <param name="filter">The typed filter. Null matches everything.</param>
/// <param name="item">The item to test.</param>
/// <returns>True when the row stays visible.</returns>
let matchesFilter (filter: string | null) (item: PickerItem) : bool =
    if isNull (box item) then
        false
    else
        match filter with
        | null -> true
        | text when String.IsNullOrWhiteSpace text -> true
        | text ->
            let haystack = $"{item.Key}\n{item.Label}\n{item.Detail}"
            haystack.IndexOf(text.Trim(), StringComparison.OrdinalIgnoreCase) >= 0

/// The rows the overlay paints: the items matching the filter, in stable
/// order.
/// <param name="state">The picker state.</param>
/// <returns>The visible rows.</returns>
let visibleItems (state: PickerState) : PickerItem list =
    state.Items |> List.filter (matchesFilter state.Filter)

/// The visible row count.
/// <param name="state">The picker state.</param>
/// <returns>The visible rows.</returns>
let count (state: PickerState) : int = (visibleItems state).Length

/// True when no row is visible (empty items or a filter matching nothing).
/// <param name="state">The picker state.</param>
/// <returns>True when nothing is visible.</returns>
let isEmpty (state: PickerState) : bool = count state = 0

/// Sets the filter and returns the cursor to the first row.
/// <param name="filter">The filter text. Null clears.</param>
/// <param name="state">The picker state.</param>
/// <returns>The filtered state.</returns>
let setFilter (filter: string | null) (state: PickerState) : PickerState =
    let trimmed =
        match filter with
        | null -> ""
        | text when text.Length <= maxFilterChars -> text
        | text -> text.Substring(0, maxFilterChars)

    { state with
        Filter = trimmed
        Cursor = 0
    }

/// Moves the cursor one visible row up, wrapping past the first row to
/// the last. Empty pickers stay put.
/// <param name="state">The picker state.</param>
/// <returns>The moved state.</returns>
let moveUp (state: PickerState) : PickerState =
    let total = count state

    if total <= 0 then
        state
    else
        { state with
            Cursor = (state.Cursor - 1 + total) % total
        }

/// Moves the cursor one visible row down, wrapping past the last row to
/// the first. Empty pickers stay put.
/// <param name="state">The picker state.</param>
/// <returns>The moved state.</returns>
let moveDown (state: PickerState) : PickerState =
    let total = count state

    if total <= 0 then
        state
    else
        { state with
            Cursor = (state.Cursor + 1) % total
        }

/// The cursor row, or None when nothing is visible or the cursor is out
/// of range.
/// <param name="state">The picker state.</param>
/// <returns>The selected row, or None.</returns>
let selectedItem (state: PickerState) : PickerItem option =
    let visible = visibleItems state

    if state.Cursor >= 0 && state.Cursor < visible.Length then
        Some visible[state.Cursor]
    else
        None

// ──────────────────────────────────────────────────────────────────────────
// Key handling: the picker owns up/down/enter/esc while open, the editor
// stays suspended.

/// True when no modifier is held (Shift alone still types).
let private isPlain (modifiers: ConsoleModifiers) : bool =
    not (modifiers.HasFlag(ConsoleModifiers.Control))
    && not (modifiers.HasFlag(ConsoleModifiers.Alt))

/// Decodes one console key over the picker: Up/Down navigate (wrapping),
/// Enter selects the cursor row (empty stays open), Esc cancels,
/// Backspace edits the filter, and plain printable characters extend it
/// (returning the cursor to the first row). Anything else stays open
/// unchanged, so the editor suspension never loses a keystroke silently:
/// unhandled keys simply do nothing.
/// <param name="state">The current picker state.</param>
/// <param name="key">The console key.</param>
/// <returns>The next state and its outcome.</returns>
let applyPickerKey (state: PickerState) (key: ConsoleKeyInfo) : PickerState * PickerOutcome =
    if key.Key = ConsoleKey.Escape then
        state, Cancel
    elif key.Key = ConsoleKey.UpArrow then
        moveUp state, Stay
    elif key.Key = ConsoleKey.DownArrow then
        moveDown state, Stay
    elif key.Key = ConsoleKey.Enter && isPlain key.Modifiers then
        match selectedItem state with
        | Some item -> state, Pick item
        | None -> state, Stay
    elif key.Key = ConsoleKey.Backspace then
        let next =
            if state.Filter = "" then
                state
            else
                { state with
                    Filter = state.Filter.Substring(0, state.Filter.Length - 1)
                    Cursor = 0
                }

        next, Stay
    elif
        isPlain key.Modifiers
        && key.KeyChar <> '\u0000'
        && not (Char.IsControl(key.KeyChar))
        && state.Filter.Length < maxFilterChars
    then
        { state with
            Filter = state.Filter + string key.KeyChar
            Cursor = 0
        },
        Stay
    else
        state, Stay

// ──────────────────────────────────────────────────────────────────────────
// Command assist: bare triggers plus verbatim line resolution. The TUI
// never parses arguments: only exact bare command names open a picker,
// and a pick resolves to the same line typing the id would send, which
// the engine routes exactly as in the plain REPL.

// Which long-running command the progress treatment covers.
type ProgressKind =
    /// "/compact": the transcript compaction with its deferred path.
    | CompactProgress
    /// "/export <file>": the journal export with its file target.
    | ExportProgress of string

/// Names the picker a bare submitted line opens, if any: only exact bare
/// command names trigger ("/sessions" and "/resume" share the session
/// picker; "/tree" and "/fork" share the sequence picker). Lines carrying
/// arguments route verbatim with no picker, and new commands debuting in
/// the REPL route verbatim until wired here.
/// <param name="text">The submitted line. Null opens nothing.</param>
/// <returns>The picker to open, or None.</returns>
let pickerForBare (text: string | null) : PickerKind option =
    match text with
    | null -> None
    | content ->
        match content.Trim() with
        | "/sessions"
        | "/resume" -> Some ResumeSession
        | "/model" -> Some SwitchModel
        | "/tree"
        | "/fork" -> Some ForkAtSequence
        | _ -> None

/// Names the long-running command a submitted line starts, if any: exact
/// "/compact" or the "/export" command prefix (bare or with a target).
/// The TUI paints the progress marker and routes fire-and-forget so the
/// key pump never blocks; every other line awaits verbatim as today.
/// <param name="text">The submitted line. Null starts nothing.</param>
/// <returns>The progress to paint, or None.</returns>
let progressFor (text: string | null) : ProgressKind option =
    match text with
    | null -> None
    | content ->
        let trimmed = content.Trim()

        if trimmed = "/compact" then
            Some CompactProgress
        elif
            trimmed.StartsWith("/export", StringComparison.Ordinal)
            && (trimmed.Length = "/export".Length
                || Char.IsWhiteSpace(trimmed["/export".Length]))
        then
            Some(ExportProgress(trimmed.Substring("/export".Length).Trim()))
        else
            None

/// Resolves a pick to the exact line the plain REPL routes for the same
/// choice: typing the picked key would send this line.
/// <param name="kind">The command the picker serves.</param>
/// <param name="key">The picked key. Null or blank resolves to empty.</param>
/// <returns>The verbatim engine line, or empty when nothing was picked.</returns>
let commandLineFor (kind: PickerKind) (key: string | null) : string =
    match key with
    | null -> ""
    | text when String.IsNullOrWhiteSpace text -> ""
    | text ->
        let trimmed = text.Trim()

        match kind with
        | ResumeSession -> "/resume " + trimmed
        | SwitchModel -> "/model " + trimmed
        | ForkAtSequence -> "/fork " + trimmed
