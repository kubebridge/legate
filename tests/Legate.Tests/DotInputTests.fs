// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DotInputTests

open System
open Dot.DotInput
open FsUnit.Xunit
open Xunit

// Input box proofs (issue 332): the pure DotInput editor model, key
// decoder, session history, slash hints, width-clamped renderer, and the
// intent classification DotTui routes through ReplEngine.HandleLineAsync.
// TTY-free: keys are constructed ConsoleKeyInfo values, so multiline
// editing, paste handling, history browsing, hints, and the full key map
// (including abort-without-kill and empty-sends-nothing) are proven without
// a terminal. The linked DotInput.fs compiles here without referencing the
// Dot exe.

let private charKey (value: char) : ConsoleKeyInfo =
    ConsoleKeyInfo(value, ConsoleKey.A, false, false, false)

let private plainKey (key: ConsoleKey) : ConsoleKeyInfo =
    ConsoleKeyInfo('\u0000', key, false, false, false)

let private enterKey: ConsoleKeyInfo =
    ConsoleKeyInfo('\r', ConsoleKey.Enter, false, false, false)

let private altEnterKey: ConsoleKeyInfo =
    ConsoleKeyInfo('\u0000', ConsoleKey.Enter, false, true, false)

let private ctrlKey (key: ConsoleKey) (control: char) : ConsoleKeyInfo =
    ConsoleKeyInfo(control, key, false, false, true)

let private altKey (key: ConsoleKey) : ConsoleKeyInfo =
    ConsoleKeyInfo('\u0000', key, false, true, false)

let private escapeKey: ConsoleKeyInfo =
    ConsoleKeyInfo('\x1B', ConsoleKey.Escape, false, false, false)

let private typeText (text: string) : EditorState * History * InputIntent =
    let mutable editor = empty
    let mutable history = emptyHistory
    let mutable intent = Noop

    for value in text do
        let nextEditor, nextHistory, nextIntent = applyKey editor history (charKey value)
        editor <- nextEditor
        history <- nextHistory
        intent <- nextIntent

    editor, history, intent

let private press (editor: EditorState) (history: History) (key: ConsoleKeyInfo) = applyKey editor history key

// ──────────────────────────────────────────────────────────────────────────
// Editor model

[<Fact>]
let ``Empty buffer is one blank line at the origin`` () =
    empty.Lines |> should equal [ "" ]
    empty.Row |> should equal 0
    empty.Col |> should equal 0
    isBlank empty |> should equal true

[<Fact>]
let ``FromText splits newlines strips carriage returns and ends at the tail`` () =
    let editor = fromText "line1\r\nline2\rline3"

    editor.Lines |> should equal [ "line1"; "line2"; "line3" ]
    editor.Row |> should equal 2
    editor.Col |> should equal 5
    toText editor |> should equal "line1\nline2\nline3"

[<Fact>]
let ``Typing inserts and arrows walk across the line join`` () =
    let editor, _, _ = typeText "ab"

    let left = moveLeft editor
    left.Col |> should equal 1

    let start = lineStart editor
    start.Col |> should equal 0
    (moveLeft start).Col |> should equal 0

    let joined =
        insertNewline start |> fun split -> { split with Row = 1; Col = 0 } |> moveLeft

    joined.Row |> should equal 0
    joined.Col |> should equal 0

[<Fact>]
let ``Vertical moves clamp to shorter lines and hold at the edges`` () =
    let editor = fromText "longer\nab"

    let up = moveUp { editor with Row = 1; Col = 2 }
    (up.Row, up.Col) |> should equal (0, 2)

    let down = moveDown { editor with Row = 0; Col = 5 }
    (down.Row, down.Col) |> should equal (1, 2)

    (moveUp { editor with Row = 0; Col = 0 })
    |> should equal { editor with Row = 0; Col = 0 }

    moveDown editor |> should equal editor

[<Fact>]
let ``Word moves skip whitespace then the word`` () =
    let editor = fromText "hello world  test"

    wordBack (lineEnd editor) |> fun moved -> moved.Col |> should equal 13
    wordBack (wordBack (lineEnd editor)) |> fun moved -> moved.Col |> should equal 6
    wordForward empty |> fun moved -> moved.Col |> should equal 0

    let forward = wordForward { editor with Col = 0 }
    forward.Col |> should equal 6

[<Fact>]
let ``Backspace and delete join lines at the edges`` () =
    let editor = fromText "ab\ncd"

    let joined = backspace { editor with Row = 1; Col = 0 }
    joined.Lines |> should equal [ "abcd" ]
    (joined.Row, joined.Col) |> should equal (0, 2)

    let joinedRight = deleteChar { editor with Row = 0; Col = 2 }
    joinedRight.Lines |> should equal [ "abcd" ]

    backspace empty |> should equal empty

    deleteChar (fromText "x" |> fun single -> { single with Col = 1 })
    |> fun kept -> kept.Lines |> should equal [ "x" ]

[<Fact>]
let ``Kill keys clear words lines and join at the end`` () =
    let editor = fromText "hello world"

    killWordBack (lineEnd editor) |> toText |> should equal "hello "
    killToStart (lineEnd editor) |> toText |> should equal ""
    killToEnd { editor with Col = 5 } |> toText |> should equal "hello"

    let twoLines = fromText "ab\ncd"
    killToEnd { twoLines with Row = 0; Col = 2 } |> toText |> should equal "abcd"

[<Fact>]
let ``Paste splits embedded newlines and keeps huge single lines whole`` () =
    let pasted = insertText empty "line1\nline2\nline3"
    pasted.Lines |> should equal [ "line1"; "line2"; "line3" ]

    let huge = insertText empty (String.replicate 10000 "a")
    huge.Lines.Length |> should equal 1
    huge.Lines[0].Length |> should equal 10000

// ──────────────────────────────────────────────────────────────────────────
// Intent classification (the engine routing contract)

[<Fact>]
let ``SubmitLine is None for blank and verbatim otherwise`` () =
    submitLine "" |> should equal None
    submitLine "   " |> should equal None
    submitLine null |> should equal None
    submitLine "hello" |> should equal (Some "hello")
    submitLine "/sessions" |> should equal (Some "/sessions")

[<Fact>]
let ``SteerLine prefixes plain buffers and passes slash buffers through`` () =
    steerLine "" |> should equal None
    steerLine "  " |> should equal None
    steerLine "hello" |> should equal (Some "/steer hello")
    steerLine "/sessions" |> should equal (Some "/sessions")
    steerLine "  /abort  " |> should equal (Some "  /abort  ")

[<Fact>]
let ``ClassifySubmit quits on quit verbs and submits everything else`` () =
    classifySubmit "" |> should equal Noop
    classifySubmit "   " |> should equal Noop
    classifySubmit "/quit" |> should equal QuitTui
    classifySubmit "  /exit  " |> should equal QuitTui
    classifySubmit "hello" |> should equal (SubmitText "hello")
    classifySubmit "/sessions" |> should equal (SubmitText "/sessions")
    classifySubmit "/quit now" |> should equal (SubmitText "/quit now")

[<Fact>]
let ``ClassifySteer sends nothing blank and steers plain text`` () =
    classifySteer "" |> should equal Noop
    classifySteer "hello" |> should equal (SteerText "hello")
    classifySteer "/sessions" |> should equal (SubmitText "/sessions")

// ──────────────────────────────────────────────────────────────────────────
// Key map

[<Fact>]
let ``Bare q and printables type`` () =
    let editor, _, intent = press empty emptyHistory (charKey 'q')
    toText editor |> should equal "q"
    intent |> should equal Noop

[<Fact>]
let ``Enter submits non-blank and clears to history`` () =
    let typed, _, _ = typeText "hello"
    let editor, history, intent = press typed emptyHistory enterKey

    intent |> should equal (SubmitText "hello")
    isBlank editor |> should equal true
    history.Entries |> should equal [ "hello" ]

[<Fact>]
let ``Enter on blank sends nothing`` () =
    let editor, history, intent = press empty emptyHistory enterKey

    intent |> should equal Noop
    isBlank editor |> should equal true
    history.Entries |> List.isEmpty |> should equal true

[<Fact>]
let ``Ctrl+O and Alt+Enter insert newlines`` () =
    let typed, _, _ = typeText "ab"

    let ctrlO, _, intentO = press typed emptyHistory (ctrlKey ConsoleKey.O '\x0F')
    toText ctrlO |> should equal "ab\n"
    intentO |> should equal Noop

    let altEnter, _, intentAlt = press typed emptyHistory altEnterKey
    toText altEnter |> should equal "ab\n"
    intentAlt |> should equal Noop

[<Fact>]
let ``Ctrl+S steers plain buffers and passes slash buffers through`` () =
    let typed, _, _ = typeText "hello"
    let editor, history, intent = press typed emptyHistory (ctrlKey ConsoleKey.S '\x13')

    intent |> should equal (SteerText "hello")
    isBlank editor |> should equal true
    history.Entries |> should equal [ "hello" ]

    let slashed, _, _ = typeText "/sessions"
    let _, _, slashIntent = press slashed emptyHistory (ctrlKey ConsoleKey.S '\x13')
    slashIntent |> should equal (SubmitText "/sessions")

    let _, _, blankIntent = press empty emptyHistory (ctrlKey ConsoleKey.S '\x13')
    blankIntent |> should equal Noop

[<Fact>]
let ``Ctrl+C aborts without quitting and preserves the buffer`` () =
    let typed, _, _ = typeText "draft"
    let editor, history, intent = press typed emptyHistory (ctrlKey ConsoleKey.C '\x03')

    intent |> should equal AbortTurn
    toText editor |> should equal "draft"
    history.Entries |> List.isEmpty |> should equal true

[<Fact>]
let ``Ctrl+Q and empty Esc quit while non-empty Esc only leaves browse`` () =
    let _, _, quitQ = press empty emptyHistory (ctrlKey ConsoleKey.Q '\x11')
    quitQ |> should equal QuitTui

    let _, _, quitEsc = press empty emptyHistory escapeKey
    quitEsc |> should equal QuitTui

    let typed, _, _ = typeText "draft"
    let editor, _, intent = press typed emptyHistory escapeKey
    intent |> should equal Noop
    toText editor |> should equal "draft"

[<Fact>]
let ``Alt+B and Alt+F move by word regardless of KeyChar`` () =
    let editor = fromText "hello world"

    let back, _, _ = press (lineEnd editor) emptyHistory (altKey ConsoleKey.B)
    back.Col |> should equal 6

    let forward, _, _ = press { editor with Col = 0 } emptyHistory (altKey ConsoleKey.F)
    forward.Col |> should equal 6

[<Fact>]
let ``Function keys are ignored`` () =
    let typed, _, _ = typeText "ab"
    let editor, history, intent = press typed emptyHistory (plainKey ConsoleKey.F1)

    toText editor |> should equal "ab"
    intent |> should equal Noop
    history |> should equal emptyHistory

// ──────────────────────────────────────────────────────────────────────────
// Session history

[<Fact>]
let ``History appends on submit and browses newest first`` () =
    let first, _, _ = typeText "one"
    let _, afterOne, intentOne = press first emptyHistory enterKey
    intentOne |> should equal (SubmitText "one")

    let second, _, _ = typeText "two"
    let _, both, _ = press second afterOne enterKey
    both.Entries |> should equal [ "one"; "two" ]

    let older, browsing, _ = press empty both (plainKey ConsoleKey.UpArrow)
    toText older |> should equal "two"

    let oldest, _, _ = press older browsing (plainKey ConsoleKey.UpArrow)
    toText oldest |> should equal "one"

    // Oldest holds: further Up stays.
    let held, _, _ = press oldest browsing (plainKey ConsoleKey.UpArrow)
    toText held |> should equal "one"

[<Fact>]
let ``History restores the draft past the newest entry`` () =
    let typed, history, _ = typeText "one"
    let _, submitted, _ = press typed history enterKey

    let draft, _, _ = typeText "draft"
    let shown, browsing, _ = press draft submitted (ctrlKey ConsoleKey.P '\x10')
    toText shown |> should equal "one"

    let restored, cleared, _ = press shown browsing (ctrlKey ConsoleKey.N '\x0E')
    toText restored |> should equal "draft"
    cleared.Cursor |> should equal None

[<Fact>]
let ``Edits reset the browse index`` () =
    let typed, history, _ = typeText "one"
    let _, submitted, _ = press typed history enterKey

    let shown, browsing, _ = press empty submitted (plainKey ConsoleKey.UpArrow)
    toText shown |> should equal "one"

    let edited, reset, intent = press shown browsing (charKey '!')
    toText edited |> should equal "one!"
    reset.Cursor |> should equal None
    intent |> should equal Noop

[<Fact>]
let ``Interior arrows move while edge arrows browse`` () =
    let typed, history, _ = typeText "one"
    let _, submitted, _ = press typed history enterKey

    let multi = fromText "ab\ncd"

    // Up inside the buffer moves; Up on the first row browses.
    let moved, _, intentMove =
        press { multi with Row = 1; Col = 1 } submitted (plainKey ConsoleKey.UpArrow)

    (moved.Row, moved.Col) |> should equal (0, 1)
    intentMove |> should equal Noop

    let shown, _, intentBrowse =
        press { multi with Row = 0; Col = 1 } submitted (plainKey ConsoleKey.UpArrow)

    toText shown |> should equal "one"
    intentBrowse |> should equal Noop

    // Down on the last row with no browse does nothing.
    let held, _, intentHeld = press multi submitted (plainKey ConsoleKey.DownArrow)
    held |> should equal multi
    intentHeld |> should equal Noop

[<Fact>]
let ``Up with no history never moves the cursor`` () =
    let typed, history, _ = typeText "ab"
    let start = lineStart typed
    let held, _, intent = press start history (plainKey ConsoleKey.UpArrow)

    held |> should equal start
    intent |> should equal Noop

// ──────────────────────────────────────────────────────────────────────────
// Slash hints

[<Fact>]
let ``Hints stay silent off slash`` () =
    queryHints "hello" [] |> List.isEmpty |> should equal true
    queryHints "" [] |> List.isEmpty |> should equal true
    queryHints null [] |> List.isEmpty |> should equal true

[<Fact>]
let ``Bare slash lists the first five commands in table order`` () =
    let hints = queryHints "/" []

    hints.Length |> should equal 5

    hints
    |> List.map (fun hint -> hint.Name)
    |> should
        equal
        [
            "/new"
            "/sessions"
            "/resume"
            "/model"
            "/steer"
        ]

    hints
    |> List.forall (fun hint -> not (String.IsNullOrWhiteSpace hint.Description))
    |> should equal true

[<Fact>]
let ``Prefix narrows to matching commands`` () =
    queryHints "/mo" []
    |> List.map (fun hint -> hint.Name)
    |> should equal [ "/model" ]

    queryHints "/s" []
    |> List.map (fun hint -> hint.Name)
    |> should equal [ "/sessions"; "/steer"; "/session" ]

    queryHints "/zzz" [] |> List.isEmpty |> should equal true

[<Fact>]
let ``Template names match with descriptions and truncate at five`` () =
    let templates =
        [
            "commit"
            "check"
            "clean"
            "close"
            "config"
            "chore"
        ]

    let hints = queryHints "/c" templates

    hints.Length |> should equal maxHints
    hints[0].Name |> should equal "/compact"
    hints[1].Name |> should equal "/clone"

    hints
    |> List.forall (fun hint -> hint.Description = "prompt template" || hint.Name.StartsWith("/c"))
    |> should equal true

    let names = queryHints "/comm" [ "commit" ] |> List.map (fun hint -> hint.Name)
    names |> should equal [ "/commit" ]

// ──────────────────────────────────────────────────────────────────────────
// Renderer

[<Fact>]
let ``Rows never exceed the width and long lines carry the marker`` () =
    let editor = fromText (String.replicate 100 "a")
    let rows, _ = renderInputRegion 80 10 editor []

    rows |> List.forall (fun row -> row.Length <= 80) |> should equal true
    rows[0] |> should equal ("> " + String.replicate 77 "a" + ">")
    rows |> List.last |> should equal (clampRow 80 hintBarText)

[<Fact>]
let ``Huge pastes render clamped with the marker present`` () =
    let editor = insertText empty (String.replicate 10000 "b")
    let rows, _ = renderInputRegion 80 10 editor []

    rows |> List.forall (fun row -> row.Length <= 80) |> should equal true
    rows |> List.exists (fun row -> row.EndsWith(">")) |> should equal true

[<Fact>]
let ``Cursor tracks the prompt prefix and the scroll window`` () =
    let editor = fromText "ab"
    let _, (row, col) = renderInputRegion 80 10 { editor with Col = 1 } []

    (row, col) |> should equal (0, 3)

    let tall = fromText "1\n2\n3\n4\n5\n6"
    let rows, (tallRow, _) = renderInputRegion 80 5 tall []

    rows.Length |> should equal 5
    tallRow |> should equal 3
    rows |> List.forall (fun line -> line.Length <= 80) |> should equal true

[<Fact>]
let ``Hint rows render under the buffer above the hint bar`` () =
    let editor = fromText "/s"
    let hints = queryHints "/s" []
    let rows, _ = renderInputRegion 80 10 editor hints

    rows[0] |> should equal "> /s"
    rows[1].Contains("/sessions") |> should equal true
    rows |> List.last |> should equal (clampRow 80 hintBarText)
    rows |> List.forall (fun row -> row.Length <= 80) |> should equal true

[<Fact>]
let ``Degenerate sizes clamp instead of throwing`` () =
    let rows, (row, col) = renderInputRegion 0 0 (fromText "hello") []

    rows |> List.forall (fun line -> line.Length <= 20) |> should equal true
    (row >= 0 && col >= 0) |> should equal true

[<Fact>]
let ``CursorLine reads the cursor row`` () =
    let editor = fromText "first\nsecond"
    cursorLine { editor with Row = 0; Col = 0 } |> should equal "first"
    cursorLine editor |> should equal "second"
