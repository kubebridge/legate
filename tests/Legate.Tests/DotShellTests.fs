// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DotShellTests

open System
open Legate
open Dot.DotShell
open FsUnit.Xunit
open Xunit

// Layout shell proofs (issue 331): the pure DotShell frame regions
// (banner, transcript viewport, status bar), the Subscribe-driven
// turn-state mapping, and the alternate-screen teardown constants. TTY-free:
// every case runs against pure width/height inputs, so resize reflow and
// the colorless path are proven without a terminal. The linked DotShell.fs
// compiles here without referencing the Dot exe.

let private freshIds () : SessionId * TurnId = SessionId.New(), TurnId.New()

let private stamp = DateTimeOffset.UtcNow

let private at (sequence: int) : Nullable<int64> = Nullable<int64>(int64 sequence)

let private started (sid: SessionId) (tid: TurnId) : SessionEvent =
    TurnStartedEvent(sid, tid, at 1, stamp) :> SessionEvent

let private delta (sid: SessionId) (tid: TurnId) : SessionEvent =
    TextDeltaEvent(sid, tid, at 2, stamp, "hello") :> SessionEvent

let private permissionAsked (sid: SessionId) (tid: TurnId) : SessionEvent =
    PermissionRequestedEvent(sid, tid, at 3, stamp, "req-1", "read_file") :> SessionEvent

let private permissionAnswered (sid: SessionId) (tid: TurnId) : SessionEvent =
    PermissionResolvedEvent(sid, tid, at 4, stamp, "req-1", PermissionDecisionKind.AllowOnce) :> SessionEvent

let private questionAsked (sid: SessionId) (tid: TurnId) : SessionEvent =
    QuestionAskedEvent(sid, tid, at 3, stamp, "q-1", "continue?") :> SessionEvent

let private questionAnswered (sid: SessionId) (tid: TurnId) : SessionEvent =
    QuestionAnsweredEvent(sid, tid, at 4, stamp, "q-1", "yes") :> SessionEvent

let private completed (sid: SessionId) (tid: TurnId) : SessionEvent =
    TurnCompletedEvent(sid, tid, at 5, stamp) :> SessionEvent

let private aborted (sid: SessionId) (tid: TurnId) : SessionEvent =
    TurnAbortedEvent(sid, tid, at 5, stamp, StopCause.ExplicitAbort, "aborted") :> SessionEvent

let private failed (sid: SessionId) (tid: TurnId) : SessionEvent =
    TurnFailedEvent(sid, tid, at 5, stamp, "boom") :> SessionEvent

let private statusFor (sid: SessionId) : string =
    statusText sid (ModelReference.Parse("scripted/scripted")) SessionState.Running

[<Fact>]
let ``Banner heads every frame above the transcript`` () =
    let frame =
        renderFrame 80 24 false [ "EVENT seq=1 TurnStartedEvent" ] (statusFor (SessionId.New()))

    frame.Contains(bannerLines.Head) |> should equal true
    frame.Contains(shellTitle) |> should equal true
    frame.Contains("EVENT seq=1 TurnStartedEvent") |> should equal true

[<Fact>]
let ``Short windows keep the banner plus the tail`` () =
    let sid = SessionId.New()
    let lines = [ for n in 1..50 -> $"line-%02d{n}" ]
    let frame = renderFrame 80 12 false lines (statusFor sid)

    frame.Contains("line-50") |> should equal true
    frame.Contains("line-01") |> should equal false
    frame.Contains(bannerLines.Head) |> should equal true
    frame.Contains(statusFor sid) |> should equal true

[<Fact>]
let ``Resize reflows without losing the tail`` () =
    let sid = SessionId.New()
    let status = statusFor sid
    let lines = [ for n in 1..50 -> $"line-%02d{n}" ]
    let wide = renderFrame 80 24 false lines status
    let narrow = renderFrame 40 10 false lines status

    wide.Contains("line-50") |> should equal true
    narrow.Contains("line-50") |> should equal true
    narrow.Contains("line-01") |> should equal false

    let longLine = String.replicate 100 "a"
    let trimmed = renderFrame 40 24 false [ longLine ] status

    trimmed.Contains(longLine.Substring(0, 39) + ">") |> should equal true
    trimmed.Contains(longLine) |> should equal false

[<Fact>]
let ``Degenerate and huge sizes clamp instead of throwing`` () =
    let sid = SessionId.New()
    let status = statusFor sid
    let small = renderFrame 0 0 false [] status
    let huge = renderFrame 10000 10000 false [ "ok" ] status

    small.Contains(status.Substring(0, 19) + ">") |> should equal true
    huge.Contains("ok") |> should equal true

    huge.Split('\n')
    |> Array.forall (fun line -> line.Length <= 240)
    |> should equal true

[<Fact>]
let ``Colorless path strips every escape`` () =
    let frame =
        renderFrame 80 24 false [ "EVENT seq=1 TurnStartedEvent" ] (statusFor (SessionId.New()))

    frame.Contains("\u001b") |> should equal false

[<Fact>]
let ``Color path paints the banner and the status bar`` () =
    let frame =
        renderFrame 80 24 true [ "EVENT seq=1 TurnStartedEvent" ] (statusFor (SessionId.New()))

    frame.Contains("\u001b") |> should equal true

[<Fact>]
let ``Null transcript lines never reach the viewport`` () =
    let frame =
        renderFrame 80 24 false [ Unchecked.defaultof<string>; "ok" ] (statusFor (SessionId.New()))

    frame.Contains("ok") |> should equal true

[<Fact>]
let ``Status names the short id model and turn state`` () =
    let sid = SessionId.New()

    let line =
        statusText sid (ModelReference.Parse("scripted/scripted")) SessionState.Running

    line.Contains(sid.Value.Substring(0, 8)) |> should equal true
    line.Contains("scripted/scripted") |> should equal true
    line.Contains("Running") |> should equal true

[<Fact>]
let ``Short id is the eight-character ULID prefix`` () =
    let sid = SessionId.New()

    shortSessionId sid |> should equal (sid.Value.Substring(0, 8))
    (shortSessionId sid).Length |> should equal 8

[<Fact>]
let ``Scripted turn walks Idle Running Waiting Running Idle`` () =
    let sid, tid = freshIds ()

    let events =
        [
            started sid tid
            delta sid tid
            permissionAsked sid tid
            permissionAnswered sid tid
            completed sid tid
        ]

    let states = events |> List.scan updateTurnState SessionState.Idle |> List.tail

    states
    |> should
        equal
        [
            SessionState.Running
            SessionState.Running
            SessionState.WaitingForInput
            SessionState.Running
            SessionState.Idle
        ]

[<Fact>]
let ``Question suspension waits for input then resumes`` () =
    let sid, tid = freshIds ()

    updateTurnState SessionState.Running (questionAsked sid tid)
    |> should equal SessionState.WaitingForInput

    updateTurnState SessionState.WaitingForInput (questionAnswered sid tid)
    |> should equal SessionState.Running

[<Fact>]
let ``Abort and failure return the bar to Idle`` () =
    let sid, tid = freshIds ()

    updateTurnState SessionState.Running (aborted sid tid)
    |> should equal SessionState.Idle

    updateTurnState SessionState.WaitingForInput (failed sid tid)
    |> should equal SessionState.Idle

[<Fact>]
let ``Deltas and null events keep the current state`` () =
    let sid, tid = freshIds ()

    updateTurnState SessionState.Idle (delta sid tid)
    |> should equal SessionState.Idle

    updateTurnState SessionState.Running (delta sid tid)
    |> should equal SessionState.Running

    updateTurnState SessionState.Idle Unchecked.defaultof<SessionEvent>
    |> should equal SessionState.Idle

[<Fact>]
let ``Teardown restores the cursor and the primary screen`` () =
    alternateExit.Contains("?25h") |> should equal true
    alternateExit.Contains("?1049l") |> should equal true
    alternateEnter.Contains("?1049h") |> should equal true
    alternateEnter.Contains("?25l") |> should equal true
    homeClear.Contains("[H") |> should equal true

    let cursorAt = alternateExit.IndexOf("?25h", StringComparison.Ordinal)

    let screenAt = alternateExit.IndexOf("?1049l", StringComparison.Ordinal)

    (cursorAt < screenAt) |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Minimum-size floor (issue 335): tiny windows show the notice above the
// transcript and keep the banner, the tail, and the quit hint instead of
// crashing; roomy windows show no notice.

[<Fact>]
let ``Minimum floor is forty by twelve`` () =
    isMinimumSize 39 24 |> should equal true
    isMinimumSize 40 11 |> should equal true
    isMinimumSize 0 0 |> should equal true
    isMinimumSize 40 12 |> should equal false
    isMinimumSize 80 24 |> should equal false

[<Fact>]
let ``Tiny windows show the notice and keep the frame`` () =
    let sid = SessionId.New()
    let status = statusFor sid
    let frame = renderFrame 30 8 false [ "ok" ] status

    frame.Contains("terminal too small") |> should equal true
    frame.Contains(bannerLines.Head) |> should equal true
    frame.Contains("ok") |> should equal true
    frame.Contains("Ctrl+Q") |> should equal true
    frame.Contains("\u001b") |> should equal false

[<Fact>]
let ``Tiny windows keep the tail under the notice`` () =
    let sid = SessionId.New()
    let lines = [ for n in 1..50 -> $"line-%02d{n}" ]
    let frame = renderFrame 40 10 false lines (statusFor sid)

    frame.Contains("terminal too small") |> should equal true
    frame.Contains("line-50") |> should equal true
    frame.Contains("line-01") |> should equal false

[<Fact>]
let ``Roomy windows show no notice`` () =
    let frame =
        renderFrame 80 24 false [ "EVENT seq=1 TurnStartedEvent" ] (statusFor (SessionId.New()))

    frame.Contains("terminal too small") |> should equal false

[<Fact>]
let ``Status bar sits above the quit hint`` () =
    let sid = SessionId.New()
    let status = statusFor sid

    let lines =
        (renderFrame 80 24 false [ "EVENT seq=1 TurnStartedEvent" ] status).Split('\n')

    let statusIndex = lines |> Array.findIndex (fun line -> line = status)
    let quitIndex = lines |> Array.findIndex (fun line -> line = quitHint)

    (statusIndex < quitIndex) |> should equal true
    lines[lines.Length - 2] |> should equal quitHint

// ──────────────────────────────────────────────────────────────────────────
// Picker overlay (issue 334): the shared picker rows dock into the input
// region budget while open, so the transcript shrinks instead of
// corrupting and narrow/colorless terminals keep their contracts.

let private pickerItem (key: string) (label: string) (detail: string) : Dot.DotPicker.PickerItem =
    {
        Dot.DotPicker.Key = key
        Dot.DotPicker.Label = label
        Dot.DotPicker.Detail = detail
    }

let private resumePicker () : Dot.DotPicker.PickerState =
    Dot.DotPicker.fromItems
        Dot.DotPicker.ResumeSession
        [
            pickerItem "01JAAA" "first chat" "[Active]"
            pickerItem "01JBBB" "second chat" "[Active]"
        ]

[<Fact>]
let ``Overlay paints the header filter and cursor-marked rows`` () =
    let rows, (cursorRow, cursorCol) = renderPickerOverlay 80 false (resumePicker ())

    rows[0].Contains("pick resume session") |> should equal true
    rows[0].Contains("Esc cancel") |> should equal true
    rows[1] |> should equal "> "
    rows[2].StartsWith("> 01JAAA", StringComparison.Ordinal) |> should equal true

    rows[3].StartsWith("  01JBBB", StringComparison.Ordinal) |> should equal true

    cursorRow |> should equal 1
    cursorCol |> should equal 2
    rows |> List.forall (fun row -> row.Length <= 80) |> should equal true

[<Fact>]
let ``Overlay filter narrows rows and names the cursor`` () =
    let picker = Dot.DotPicker.setFilter "second" (resumePicker ())
    let rows, (_, cursorCol) = renderPickerOverlay 80 false picker

    rows |> List.length |> should equal 3
    rows[2].Contains("01JBBB") |> should equal true
    cursorCol |> should equal (2 + "second".Length)

    let missing, _ =
        renderPickerOverlay 80 false (Dot.DotPicker.setFilter "no-such-row" (resumePicker ()))

    missing
    |> List.exists (fun row -> row.Contains("(no matches)", StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Overlay caps rows with a more marker`` () =
    let many =
        Dot.DotPicker.fromItems
            Dot.DotPicker.ForkAtSequence
            [
                for n in 1..20 -> pickerItem (string n) $"TurnCompletedEvent" $"seq={n}"
            ]

    let rows, _ = renderPickerOverlay 80 false many

    rows |> List.length |> should equal (2 + Dot.DotPicker.maxVisibleRows + 1)

    rows
    |> List.last
    |> should equal $"  ... {20 - Dot.DotPicker.maxVisibleRows} more (type to filter)"

[<Fact>]
let ``Colorless overlay strips every escape and color highlights the cursor`` () =
    let plain, _ = renderPickerOverlay 80 false (resumePicker ())

    plain |> List.exists (fun row -> row.Contains("\u001b")) |> should equal false

    let colored, _ = renderPickerOverlay 80 true (resumePicker ())

    colored
    |> List.exists (fun row -> row.Contains("\u001b[7m", StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Narrow overlay truncates with a marker`` () =
    let rows, _ = renderPickerOverlay 20 false (resumePicker ())

    rows |> List.forall (fun row -> row.Length <= 20) |> should equal true

    rows
    |> List.exists (fun row -> row.EndsWith(">", StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Overlay rows dock into the frame input budget`` () =
    let sid = SessionId.New()
    let rows, _ = renderPickerOverlay 80 false (resumePicker ())

    let frame =
        renderFrameWithInput 80 24 false [ "line-1"; "line-2" ] (statusFor sid) rows

    frame.Contains("pick resume session") |> should equal true
    frame.Contains("01JAAA") |> should equal true
    frame.Contains("line-2") |> should equal true
    frame.Contains("\u001b") |> should equal false

[<Fact>]
let ``Idle screen emits no terminal writes`` () =
    let screen =
        renderScreen 120 36 true true [ "[Context]"; "  AGENTS.md" ] "idle" [ "> " ] (0, 2) 0

    screenUpdate (Some screen) screen |> should equal ""

[<Fact>]
let ``Typing patches only the composer row without clearing or scrolling`` () =
    let before = renderScreen 80 24 true true [] "idle" [ "> " ] (0, 2) 0
    let after = renderScreen 80 24 true true [] "idle" [ "> hello" ] (0, 7) 0
    let update = screenUpdate (Some before) after
    update.Contains("\u001b[2J") |> should equal false
    update.Contains("\n") |> should equal false
    update.Contains(bannerLines.Head) |> should equal false
    update.Contains("> hello") |> should equal true
    update.Contains("\u001b[?25h") |> should equal true
    after.CursorRow |> should equal 21
    after.CursorCol |> should equal 7

[<Theory>]
[<InlineData(120, 36)>]
[<InlineData(80, 24)>]
[<InlineData(40, 12)>]
[<InlineData(20, 8)>]
[<InlineData(5, 3)>]
let ``Live frame fits terminal and keeps cursor in bounds`` (width: int, height: int) =
    let screen =
        renderScreen width height false true [ String.replicate 500 "x" ] "idle" [ "> hello"; "  second" ] (1, 8) 0

    screen.Rows.Length |> should equal height
    screen.Rows |> Array.forall (fun row -> row.Length < width) |> should equal true
    Assert.InRange(screen.CursorRow, 0, height - 1)
    Assert.InRange(screen.CursorCol, 0, width - 1)

[<Fact>]
let ``Scrollback and resize preserve fixed bottom composer`` () =
    let lines = [ for n in 1..60 -> $"line-{n}" ]
    let latest = renderScreen 80 24 false false lines "idle" [ "> draft" ] (0, 7) 0
    let older = renderScreen 80 24 false false lines "idle" [ "> draft" ] (0, 7) 10
    latest.Rows |> Array.contains "line-60" |> should equal true
    older.Rows |> Array.contains "line-50" |> should equal true
    older.Rows |> Array.contains "line-60" |> should equal false
    older.CursorRow |> should equal latest.CursorRow
    let resized = renderScreen 40 12 false false lines "idle" [ "> draft" ] (0, 7) 0
    let update = screenUpdate (Some latest) resized
    update.Contains("\u001b[2J") |> should equal true
    update.Contains("\n") |> should equal false

[<Fact>]
let ``Transcript cannot inject terminal controls into screen`` () =
    let screen =
        renderScreen 80 24 false false [ "hello\u001b[2J\r\tworld" ] "idle" [ "> " ] (0, 2) 0

    screen.Rows[0] |> should equal "hello"
    screen.Rows[1] |> should equal "    world"

[<Fact>]
let ``User cells have full width shading with vertical padding`` () =
    let rows = renderCells 39 true false [ { Style = User; Text = "hi" } ]
    rows.Length |> should equal 4

    for row in rows |> List.take 3 do
        row.Contains("48;2;16;39;77") |> should equal true
        (plainText row).Length |> should equal 39

    plainText rows[1] |> should equal (" hi".PadRight 39)
    rows[3] |> should equal ""

[<Fact>]
let ``Reasoning is italic and does not leak styling into the response`` () =
    let rows =
        renderCells
            79
            true
            false
            [
                {
                    Style = Reasoning
                    Text = "Considering.\n\nDone."
                }
                { Style = Assistant; Text = "Hello!" }
            ]

    rows[0].StartsWith("\u001b[3;38;2;148;159;164m", StringComparison.Ordinal)
    |> should equal true

    rows[0].EndsWith("\u001b[0m", StringComparison.Ordinal) |> should equal true

    rows
    |> List.find (fun row -> row.Contains("Hello!"))
    |> fun row -> row.Contains("\u001b[3;") |> should equal false

[<Fact>]
let ``Cells wrap words and preserve paragraphs without accepting ANSI`` () =
    let rows =
        renderCells
            20
            false
            false
            [
                {
                    Style = Assistant
                    Text = "one two three four five\n\n\u001b[2Jlast"
                }
            ]

    rows
    |> should
        equal
        [
            " one two three four"
            " five"
            " "
            " last"
            ""
        ]

    rows
    |> List.forall (fun row -> row.Length <= 20 && not (row.Contains('\u001b')))
    |> should equal true

[<Fact>]
let ``Growing a response only repaints its rows and preserves the user cell`` () =
    let cells text =
        [
            { Style = User; Text = "hi" }
            { Style = Assistant; Text = text }
        ]

    let before =
        renderScreenWithCells 80 24 true false (cells "Hel") "running" [ "> " ] (0, 2) 0

    let after =
        renderScreenWithCells 80 24 true false (cells "Hello!") "running" [ "> " ] (0, 2) 0

    let update = screenUpdate (Some before) after
    update.Contains("48;2;16;39;77") |> should equal false
    update.Contains("\u001b[2J") |> should equal false
    update.Contains("Hello!") |> should equal true

[<Fact>]
let ``Markdown tables align columns and reflow at narrow widths`` () =
    let markdown = "| Name | Count |\n| :--- | ---: |\n| Alpha | 12 |\n| Beta | 3 |"
    let wide = Dot.DotMarkdown.render 50 true plainText wrapText markdown

    wide
    |> List.exists (fun row -> (plainText row).StartsWith("┌"))
    |> should equal true

    let data =
        wide
        |> List.filter (fun row -> (plainText row).StartsWith("│"))
        |> List.map plainText

    data |> List.map String.length |> List.distinct |> List.length |> should equal 1
    let narrow = Dot.DotMarkdown.render 10 false plainText wrapText markdown
    narrow |> List.exists ((=) "Name:") |> should equal false
    narrow |> List.forall (fun row -> row.Length <= 10) |> should equal true

    String.concat "\n" narrow
    |> fun text -> text.Contains("Count: 12") |> should equal true

[<Fact>]
let ``Markdown preserves code and styles headings bold and inline code`` () =
    let markdown = "# Heading\n**bold** and `code`\n```fsharp\n    let x = 1\n```"
    let rows = Dot.DotMarkdown.render 60 true plainText wrapText markdown
    rows.Head.Contains("1;38;2;112;172;255") |> should equal true
    rows[1].Contains("\u001b[1m") |> should equal true
    rows[1].Contains("48;2;20;36;64") |> should equal true

    rows
    |> List.exists (fun row -> (plainText row).Contains("    let x = 1"))
    |> should equal true

    let plain = Dot.DotMarkdown.render 60 false plainText wrapText markdown
    plain |> List.exists (fun row -> row.Contains('\u001b')) |> should equal false
    plain[1] |> should equal "bold and code"
