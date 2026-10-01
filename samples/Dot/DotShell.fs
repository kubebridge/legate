// SPDX-License-Identifier: Apache-2.0
module Dot.DotShell

open System
open Legate

// Pure fullscreen layout shell for the dot TUI (issue 331): the ASCII
// banner header, the transcript viewport over already-rendered plain
// EVENT lines, and the status bar (short session id, current model, turn
// state). stdlib-only and TTY-free: every function is pure over
// width/height so resize reflow and the colorless path are unit-tested
// without a terminal. The run loop in DotTui.fs paints these regions from
// the Subscribe event stream (transcript lines via ReplEngine.renderEvent,
// status via updateTurnState) and owns the alternate-screen teardown.

// ──────────────────────────────────────────────────────────────────────────
// Banner

/// The ASCII dot banner heading every fullscreen frame.
let bannerLines =
    [
        "     _       _   "
        "  __| | ___ | |_ "
        " / _` |/ _ \\| __|"
        "| (_| | (_) | |_ "
        " \\__,_|\\___/ \\__|"
    ]

/// The shell title under the banner: names the input box and the plain
/// output escape (the streaming renderer is a later child that paints
/// into this frame).
let shellTitle = "dot fullscreen (Enter sends, --no-tui for plain)"

/// The input-box quit hint in the frame footer: Esc on an empty buffer,
/// Ctrl+Q, or /quit quits (bare q types now that the input box owns keys).
let quitHint = "> [Esc on empty input, Ctrl+Q, or /quit quits]"

// ──────────────────────────────────────────────────────────────────────────
// Turn-state mapping

/// The turn state a shell session starts in: no turn is executing.
let initialTurnState: SessionState = SessionState.Idle

/// Derives the status-bar turn state from one Subscribe event: a started
/// turn runs, permission/question suspension waits for host input, its
/// resolve resumes, and any settled turn returns to idle. Unknown events
/// (deltas, usage, compaction) keep the current state so fast streams do
/// not flicker the bar.
/// <param name="current">The current turn state.</param>
/// <param name="evt">The Subscribe event just observed.</param>
/// <returns>The next turn state.</returns>
let updateTurnState (current: SessionState) (evt: SessionEvent) : SessionState =
    if isNull (box evt) then
        current
    else
        match evt with
        | :? TurnStartedEvent -> SessionState.Running
        | :? PermissionRequestedEvent -> SessionState.WaitingForInput
        | :? QuestionAskedEvent -> SessionState.WaitingForInput
        | :? PermissionResolvedEvent -> SessionState.Running
        | :? QuestionAnsweredEvent -> SessionState.Running
        | :? TurnCompletedEvent -> SessionState.Idle
        | :? TurnAbortedEvent -> SessionState.Idle
        | :? TurnFailedEvent -> SessionState.Idle
        | :? SessionClosedEvent -> SessionState.Closed
        | _ -> current

// ──────────────────────────────────────────────────────────────────────────
// Status bar

/// The short session id the status bar names: the first eight ULID
/// characters, enough to match /sessions output.
/// <param name="sessionId">The session the shell shows.</param>
/// <returns>The eight-character prefix, or placeholders for null.</returns>
let shortSessionId (sessionId: SessionId) : string =
    if isNull (box sessionId) || isNull (box sessionId.Value) then
        "????????"
    elif sessionId.Value.Length <= 8 then
        sessionId.Value
    else
        sessionId.Value.Substring(0, 8)

/// The status-bar line: short session id, current model, and live turn
/// state across the scripted turn.
/// <param name="sessionId">The session the shell shows.</param>
/// <param name="model">The current model reference.</param>
/// <param name="state">The live turn state.</param>
/// <returns>The rendered status line.</returns>
let statusText (sessionId: SessionId) (model: ModelReference) (state: SessionState) : string =
    let modelText =
        if isNull (box model) || isNull (box model.Value) then
            "unknown"
        else
            model.Value

    $"session {shortSessionId sessionId} | model {modelText} | {state}"

// ──────────────────────────────────────────────────────────────────────────
// Alternate-screen constants

/// Enters the alternate screen with the cursor hidden: written once before
/// the first paint.
let alternateEnter = "\u001b[?1049h\u001b[?25l"

/// Restores the cursor and the primary screen: the teardown every quit,
/// abort, and crash path runs in try/finally.
let alternateExit = "\u001b[?25h\u001b[?1049l"

/// Homes the cursor and clears the alternate screen before each repaint.
let homeClear = "\u001b[H\u001b[2J"

// ──────────────────────────────────────────────────────────────────────────
// Minimum size (issue 335)

/// The smallest terminal the fullscreen shell lays out fully: narrower or
/// shorter windows keep running but show the notice below instead of
/// corrupting the frame.
let minimumWidth = 40

/// The smallest terminal height the fullscreen shell lays out fully (see
/// minimumWidth).
let minimumHeight = 12

/// The notice tiny windows show above the transcript: names the floor and
/// the plain escape. Never crashes: the frame keeps the banner, the tail,
/// and the quit hint around it.
let minimumSizeMessage =
    "terminal too small (min 40x12): resize, or --no-tui for plain output"

/// True when the window is below the minimum floor in either dimension.
/// Raw (unclamped) inputs: 0x0 and stubbed sizes count as tiny.
/// <param name="width">The console width in columns.</param>
/// <param name="height">The console height in rows.</param>
/// <returns>True when the notice applies.</returns>
let isMinimumSize (width: int) (height: int) : bool =
    width < minimumWidth || height < minimumHeight

// ──────────────────────────────────────────────────────────────────────────
// Frame

/// Renders one fullscreen frame as plain text with optional ANSI colors:
/// the ASCII dot banner header, the scrollable transcript viewport (the
/// tail window over the scrollback lines, never the live cursor alone),
/// the status bar, the input-box rows, and the quit hint footer. Pure over
/// width/height so resize and colorless terminals are covered without a
/// TTY: narrow windows truncate with a marker, short windows keep the
/// banner plus the tail, windows below the 40x12 minimum floor show the
/// minimum-size notice above the transcript without crashing, and
/// useColor=false strips every escape except the
/// layout newlines. The input rows reserve their own frame budget, so the
/// transcript shrinks instead of corrupting when the box grows.
/// <param name="width">The console width in columns.</param>
/// <param name="height">The console height in rows.</param>
/// <param name="useColor">True to color the banner and status bar.</param>
/// <param name="transcript">The scrollback lines, oldest first.</param>
/// <param name="status">The status-bar line.</param>
/// <param name="inputLines">The input-box rows, already width-clamped.</param>
/// <returns>The rendered frame.</returns>
let renderFrameWithInput
    (width: int)
    (height: int)
    (useColor: bool)
    (transcript: string list)
    (status: string)
    (inputLines: string list)
    : string =
    let safeWidth = max 20 (min 240 width)
    let safeHeight = max 8 (min 100 height)
    let plainLines = transcript |> List.filter (fun line -> not (isNull (box line)))

    let safeInput =
        if isNull (box inputLines) then
            []
        else
            inputLines |> List.filter (fun line -> not (isNull (box line)))

    let paintBanner =
        if useColor then
            bannerLines |> List.map (fun art -> "\u001b[1;36m" + art + "\u001b[0m")
        else
            bannerLines

    let paintStatus =
        if useColor then
            "\u001b[1m" + status + "\u001b[0m"
        else
            status

    let header = paintBanner @ [ shellTitle ]
    let footer = [ paintStatus ] @ safeInput @ [ quitHint ]

    let notice =
        if isMinimumSize width height then
            [ minimumSizeMessage ]
        else
            []

    let budget = max 1 (safeHeight - header.Length - notice.Length - footer.Length - 1)

    let visible =
        if plainLines.Length <= budget then
            plainLines
        else
            plainLines |> List.skip (plainLines.Length - budget)

    let trim (line: string) : string =
        if line.Length <= safeWidth then
            line
        else
            line.Substring(0, max 0 (safeWidth - 1)) + ">"

    String.Join("\n", (header @ notice @ visible @ [ "" ] @ footer) |> List.map trim)
    + "\n"

/// Renders one fullscreen frame without input rows: the layout-shell frame
/// the spike proved, with the full height budget for the transcript.
/// <param name="width">The console width in columns.</param>
/// <param name="height">The console height in rows.</param>
/// <param name="useColor">True to color the banner and status bar.</param>
/// <param name="transcript">The scrollback lines, oldest first.</param>
/// <param name="status">The status-bar line.</param>
/// <returns>The rendered frame.</returns>
let renderFrame (width: int) (height: int) (useColor: bool) (transcript: string list) (status: string) : string =
    renderFrameWithInput width height useColor transcript status []

// ──────────────────────────────────────────────────────────────────────────
// Picker overlay (issue 334)

/// The keys the picker overlay owns while open (the editor stays
/// suspended): Up/Down navigate, Enter selects, Esc cancels, typing
/// filters, Backspace edits the filter.
let pickerHint = "Up/Down navigate | Enter select | Esc cancel | type to filter"

/// Clamps one overlay row to the width with the trim marker.
/// <param name="width">The console width in columns.</param>
/// <param name="row">The row to clamp.</param>
/// <returns>The clamped row.</returns>
let private clampOverlayRow (width: int) (row: string) : string =
    let safeWidth = max 20 (min 240 width)

    if isNull (box row) then ""
    elif row.Length <= safeWidth then row
    else row.Substring(0, safeWidth - 1) + ">"

/// Renders the picker overlay as input-region rows plus the cursor screen
/// position: the served-command header, the filter row, and the visible
/// item rows with the cursor marker, capped to the overlay budget with a
/// "more" marker. Pure over width so narrow windows truncate with a
/// marker and useColor=false strips every escape except the layout
/// newlines, exactly like the input region.
/// <param name="width">The console width in columns.</param>
/// <param name="useColor">True to highlight the cursor row.</param>
/// <param name="picker">The picker state to paint.</param>
/// <returns>The rows and the zero-based (row, column) cursor position.</returns>
let renderPickerOverlay (width: int) (useColor: bool) (picker: DotPicker.PickerState) : string list * (int * int) =
    let safeWidth = max 20 (min 240 width)

    let header = $"pick {DotPicker.headerText picker.Kind} ({pickerHint})"
    let filterRow = $"> {picker.Filter}"

    let visible = DotPicker.visibleItems picker

    let rows =
        visible
        |> List.truncate DotPicker.maxVisibleRows
        |> List.mapi (fun index item ->
            let marker = if index = picker.Cursor then "> " else "  "

            let detail =
                if String.IsNullOrWhiteSpace item.Detail then
                    ""
                else
                    $" {item.Detail.Trim()}"

            let row = $"{marker}{item.Key} {item.Label}{detail}"

            if useColor && index = picker.Cursor then
                "\u001b[7m" + row + "\u001b[0m"
            else
                row)

    let more =
        if visible.Length > DotPicker.maxVisibleRows then
            [
                $"  ... {visible.Length - DotPicker.maxVisibleRows} more (type to filter)"
            ]
        elif visible.IsEmpty then
            [ "  (no matches)" ]
        else
            []

    let painted =
        [ header; filterRow ] @ rows @ more |> List.map (clampOverlayRow safeWidth)

    let cursorCol = min (2 + picker.Filter.Length) (safeWidth - 1)

    painted, (1, cursorCol)
