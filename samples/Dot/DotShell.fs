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

/// The shell title under the banner: names the quit-only input contract
/// and the plain-output escape (the renderer and input box are later
/// children that paint into this frame).
let shellTitle = "dot layout shell (q to quit, --no-tui for plain)"

/// The quit-only input hint in the frame footer.
let quitHint = "> [q to quit - renderer and input box land in later children]"

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
// Frame

/// Renders one fullscreen frame as plain text with optional ANSI colors:
/// the ASCII dot banner header, the scrollable transcript viewport (the
/// tail window over the scrollback lines, never the live cursor alone),
/// and the status bar plus quit hint footer. Pure over width/height so
/// resize and colorless terminals are covered without a TTY: narrow
/// windows truncate with a marker, short windows keep the banner plus the
/// tail, and useColor=false strips every escape except the layout
/// newlines.
/// <param name="width">The console width in columns.</param>
/// <param name="height">The console height in rows.</param>
/// <param name="useColor">True to color the banner and status bar.</param>
/// <param name="transcript">The scrollback lines, oldest first.</param>
/// <param name="status">The status-bar line.</param>
/// <returns>The rendered frame.</returns>
let renderFrame (width: int) (height: int) (useColor: bool) (transcript: string list) (status: string) : string =
    let safeWidth = max 20 (min 240 width)
    let safeHeight = max 8 (min 100 height)
    let plainLines = transcript |> List.filter (fun line -> not (isNull (box line)))

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
    let footer = [ paintStatus; quitHint ]

    let budget = max 1 (safeHeight - header.Length - footer.Length - 1)

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

    String.Join("\n", (header @ visible @ [ "" ] @ footer) |> List.map trim) + "\n"
