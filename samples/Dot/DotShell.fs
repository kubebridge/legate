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
        "██████╗   ██████╗  ████████╗"
        "██╔══██╗ ██╔═══██╗ ╚══██╔══╝"
        "██║  ██║ ██║   ██║    ██║   "
        "██║  ██║ ██║   ██║    ██║   "
        "██████╔╝ ╚██████╔╝    ██║   "
        "╚═════╝   ╚═════╝     ╚═╝   "
    ]

/// The shell title under the banner: names the input box and the plain
/// output escape (the streaming renderer is a later child that paints
/// into this frame).
let shellTitle = "Your workspace. Your agent."

/// The input-box quit hint in the frame footer: Esc on an empty buffer,
/// Ctrl+Q, or /quit quits (bare q types now that the input box owns keys).
let quitHint = "> [Ctrl+C, Ctrl+D, Ctrl+Q, or /quit quits]"

// ──────────────────────────────────────────────────────────────────────────
// Turn-state mapping

/// The turn state a shell session starts in: no turn is executing.
let initialTurnState: SessionState = SessionState.Idle

/// Derives the status-bar turn state from one Subscribe event: a started
/// turn runs, permission/question suspension waits for host input, its
/// resolve resumes, and any settled turn returns to idle. Unknown events
/// (deltas, usage, compaction) keep the current state so fast streams do
/// not flicker the bar. This is session-lifecycle evidence from the
/// attachment stream only: authoritative accepted-operation state
/// (accepted-queued, executing, awaiting input, committed
/// success/failure/abort, explicit unknown/unavailable) comes from
/// GetOperationResultAsync via ReplEngine.DescribeOperationState, and
/// local task running or ending is never presented as runtime state.
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

/// Names one authoritative operation observation status explicitly:
/// Pending is accepted-queued with no committed winner; Terminal is the
/// committed winning result; Unknown means the session, entry, or
/// association is missing or mismatched; Unavailable means storage
/// failed. Unknown and Unavailable never read as running, successful,
/// resumed, or idle.
/// <param name="status">The authoritative operation status.</param>
/// <returns>The explicit state text.</returns>
let describeOperationStatus (status: OperationStatus) : string =
    match status with
    | OperationStatus.Pending -> "accepted-queued (no committed result yet)"
    | OperationStatus.Terminal -> "committed (winning terminal result present)"
    | OperationStatus.Unknown -> "unknown (stale, missing, or mismatched evidence)"
    | OperationStatus.Unavailable -> "unavailable (storage failed; never terminal)"
    | _ -> "unknown (unrecognized status)"

/// Names one committed terminal TurnStatus explicitly: an idle session
/// may retain a failed last operation, so committed failure or abort is
/// shown as such rather than as idle or running.
/// <param name="status">The committed turn status.</param>
/// <returns>The explicit outcome text.</returns>
let describeTerminalStatus (status: TurnStatus) : string =
    match status with
    | TurnStatus.Completed -> "committed-success"
    | TurnStatus.Aborted -> "committed-abort"
    | TurnStatus.Failed -> "committed-failure"
    | _ -> $"committed status={status}"

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

/// The status-bar line with explicit authoritative operation evidence:
/// the session lifecycle plus the accepted-operation observation
/// (accepted-queued, executing, awaiting input, committed
/// success/failure/abort, or explicit unknown/unavailable). Unavailable,
/// stale, or insufficient evidence reads explicitly as
/// unknown/unavailable, never as running, successful, resumed, or idle.
/// <param name="sessionId">The session the shell shows.</param>
/// <param name="model">The current model reference.</param>
/// <param name="state">The live turn state.</param>
/// <param name="operation">The authoritative operation text, or null for unknown.</param>
/// <returns>The rendered status line.</returns>
let statusTextWithOperation
    (sessionId: SessionId)
    (model: ModelReference)
    (state: SessionState)
    (operation: string | null)
    : string =
    let baseLine = statusText sessionId model state

    let operationText =
        if isNull (box operation) then
            "unknown (insufficient evidence)"
        else
            let text = unbox<string> (box operation)

            if String.IsNullOrWhiteSpace text then
                "unknown (insufficient evidence)"
            else
                text

    $"{baseLine} | {operationText}"

// ──────────────────────────────────────────────────────────────────────────
// Alternate-screen constants

/// Enters the alternate screen with the cursor hidden: written once before
/// the first paint.
let alternateEnter = "\u001b[?1049h\u001b[?25l\u001b[2J"

/// Restores the cursor and the primary screen: the teardown every quit,
/// abort, and crash path runs in try/finally.
let alternateExit = "\u001b[0m\u001b[?25h\u001b[?1049l"

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

/// Removes terminal controls from untrusted transcript and editor text.
let plainText (text: string) : string =
    if isNull (box text) then
        ""
    else
        Text.RegularExpressions.Regex.Replace(text, "\u001b\\[[0-?]*[ -/]*[@-~]", "")
        |> Seq.filter (fun c -> not (Char.IsControl c))
        |> Seq.toArray
        |> String

/// Keeps the current model and state visible by shortening the workspace path.
let workspaceStatus (width: int) (directory: string) (model: ModelReference) (state: SessionState) : string =
    let columns = max 1 (width - 1)
    let modelText = plainText model.Value
    let modelBudget = max 1 (columns / 3)

    let modelText =
        if modelText.Length <= modelBudget then
            modelText
        else
            modelText.Substring(0, modelBudget - 1) + "…"

    let suffix = $" · {modelText} · {state.ToString().ToLowerInvariant()}"
    let pathBudget = max 0 (columns - suffix.Length)
    let path = plainText directory

    let path =
        if path.Length <= pathBudget then
            path
        elif pathBudget > 1 then
            "…" + path.Substring(path.Length - pathBudget + 1)
        else
            ""

    path + suffix

/// One laid-out terminal screen, including the absolute zero-based input cursor.
type Screen =
    {
        Rows: string array
        CursorRow: int
        CursorCol: int
        Width: int
    }

/// Semantic session cells; style is applied only after sanitizing and wrapping.
type CellStyle =
    | Plain
    | User
    | Assistant
    | Reasoning
    | Tool
    | Error

/// A complete message or a growing streaming cell in the transcript.
type SessionCell = { Style: CellStyle; Text: string }

/// Wraps at word boundaries while preserving paragraph breaks and indentation.
let wrapText (columns: int) (text: string) : string list =
    let columns = max 1 columns

    let wrapLine (raw: string) =
        let mutable remaining = plainText (raw.Replace("\t", "    "))
        let rows = ResizeArray<string>()

        while remaining.Length > columns do
            let space = remaining.LastIndexOf(' ', columns, columns + 1)
            let cut = if space > columns / 2 then space else columns
            rows.Add(remaining.Substring(0, cut))
            remaining <- remaining.Substring(cut + (if cut = space then 1 else 0))

        rows.Add remaining
        List.ofSeq rows

    text.Replace("\r\n", "\n").Replace('\r', '\n').Split('\n')
    |> Array.toList
    |> List.collect wrapLine

/// Paints padded user cells, subdued reasoning, and separated assistant text.
let renderCells (columns: int) (useColor: bool) (welcome: bool) (cells: SessionCell list) : string list =
    let columns = max 1 columns

    let paint code text =
        if useColor then $"\u001b[{code}m{text}\u001b[0m" else text

    cells
    |> List.collect (fun cell ->
        let inset = if columns >= 4 && cell.Style <> Plain then 1 else 0

        let body =
            wrapText (columns - 2 * inset) cell.Text
            |> List.map (fun line -> String(' ', inset) + line)

        match cell.Style with
        | User ->
            let shade (line: string) =
                (line.PadRight columns) |> paint "48;2;16;39;77;38;2;224;234;248"

            [ shade "" ] @ (body |> List.map shade) @ [ shade ""; "" ]
        | Reasoning -> (body |> List.map (paint "3;38;2;148;159;164")) @ [ "" ]
        | Assistant ->
            DotMarkdown.render (columns - 2 * inset) useColor plainText wrapText cell.Text
            |> List.map (fun line -> String(' ', inset) + line)
            |> fun lines -> lines @ [ "" ]
        | Tool -> (body |> List.map (paint DotMarkdown.softBlue)) @ [ "" ]
        | Error -> (body |> List.map (paint "38;2;231;144;137")) @ [ "" ]
        | Plain ->
            body
            |> List.map (fun line ->
                if line.StartsWith("[") then paint DotMarkdown.softBlue line
                elif welcome then paint "38;2;135;139;145" line
                else line))

/// Builds a fixed-height screen. The composer and status stay at the bottom;
/// the welcome art belongs to scrollback and leaves room for conversation.
let renderScreenWithCells
    width
    height
    useColor
    welcome
    transcript
    status
    inputLines
    (cursorRow, cursorCol)
    scrollOffset
    : Screen =
    let width = max 1 (min 500 width)
    let height = max 1 (min 200 height)
    // Leave the last column unused to avoid terminal autowrap at the margin.
    let columns = max 1 (width - 1)

    let trim (text: string) =
        let text = plainText text

        if text.Length <= columns then
            text
        else
            text.Substring(0, max 0 (columns - 1)) + "…"

    let color code text =
        if useColor && text <> "" then
            $"\u001b[{code}m{text}\u001b[0m"
        else
            text

    let center text =
        String(' ', max 0 ((columns - (plainText text).Length) / 2)) + text |> trim

    let input = inputLines |> List.truncate (max 1 (height - 4)) |> List.map trim
    let input = if input.IsEmpty then [ "  " ] else input
    let bodyHeight = max 0 (height - input.Length - 3)
    let rule = String('─', columns) |> color DotMarkdown.blue

    let header =
        if welcome then
            let art =
                if width >= 40 && bodyHeight >= 14 then
                    [ ""; "" ]
                    @ (bannerLines |> List.map center)
                    @ [ ""; center shellTitle; ""; "" ]
                else
                    [ center "Dot"; "" ]

            art |> List.map (color DotMarkdown.blue)
        else
            []

    let content = renderCells columns useColor welcome transcript

    let all = header @ content
    let offset = min (max 0 scrollOffset) (max 0 (all.Length - bodyHeight))
    let finish = max 0 (all.Length - offset)
    let visible = all |> List.take finish |> List.skip (max 0 (finish - bodyHeight))
    let body = visible @ List.replicate (max 0 (bodyHeight - visible.Length)) ""

    let rows =
        body
        @ [ rule ]
        @ input
        @ [
            rule
            trim status |> color "38;2;135;139;145"
        ]

    let rows = rows |> List.truncate height |> List.toArray

    {
        Rows = rows
        CursorRow = min (height - 1) (bodyHeight + 1 + min (input.Length - 1) (max 0 cursorRow))
        CursorCol = min (columns - 1) (max 0 cursorCol)
        Width = width
    }

/// Plain-line adapter for welcome screens and headless layout tests.
let renderScreen width height useColor welcome transcript status inputLines cursor scrollOffset : Screen =
    let cells = transcript |> List.map (fun line -> { Style = Plain; Text = line })
    renderScreenWithCells width height useColor welcome cells status inputLines cursor scrollOffset

/// Emits only changed rows, with absolute cursor addressing and no newlines.
/// Identical screens emit nothing, including no cursor hide/show flicker.
let screenUpdate (previous: Screen option) (current: Screen) : string =
    if previous = Some current then
        ""
    else
        let output = Text.StringBuilder("\u001b[?2026h\u001b[?25l")

        let resized =
            previous
            |> Option.exists (fun old -> old.Width <> current.Width || old.Rows.Length <> current.Rows.Length)

        if resized then
            output.Append("\u001b[2J") |> ignore

        for index in 0 .. current.Rows.Length - 1 do
            let unchanged =
                not resized
                && (previous
                    |> Option.exists (fun old -> index < old.Rows.Length && old.Rows[index] = current.Rows[index]))

            if not unchanged then
                output.Append($"\u001b[{index + 1};1H\u001b[0m\u001b[2K").Append(current.Rows[index])
                |> ignore

        output
            .Append($"\u001b[0m\u001b[{current.CursorRow + 1};{current.CursorCol + 1}H\u001b[?25h\u001b[?2026l")
            .ToString()

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
            bannerLines |> List.map (DotMarkdown.paint true ("1;" + DotMarkdown.blue))
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
