// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DotPickerTests

open System
open Dot.DotPicker
open FsUnit.Xunit
open Xunit

// Shared picker proofs (issue 334): the pure DotPicker model over the
// rows every selection command reuses (navigate, filter, select, cancel)
// plus the command assist (bare triggers, progress detection, verbatim
// line resolution) the TUI routes through ReplEngine.HandleLineAsync.
// TTY-free: keys are constructed ConsoleKeyInfo values, so the overlay
// focus contract is proven without a terminal. The linked DotPicker.fs
// compiles here without referencing the Dot exe.

let private item (key: string) (label: string) (detail: string) : PickerItem =
    {
        Key = key
        Label = label
        Detail = detail
    }

let private sessionsPicker () : PickerState =
    fromItems
        ResumeSession
        [
            item "01JAAA" "first chat" "[Active]"
            item "01JBBB" "second chat" "[Active]"
            item "01JCCC" "review notes" "[Closed]"
        ]

let private upKey: ConsoleKeyInfo =
    ConsoleKeyInfo('\u0000', ConsoleKey.UpArrow, false, false, false)

let private downKey: ConsoleKeyInfo =
    ConsoleKeyInfo('\u0000', ConsoleKey.DownArrow, false, false, false)

let private enterKey: ConsoleKeyInfo =
    ConsoleKeyInfo('\r', ConsoleKey.Enter, false, false, false)

let private escapeKey: ConsoleKeyInfo =
    ConsoleKeyInfo('\x1B', ConsoleKey.Escape, false, false, false)

let private backspaceKey: ConsoleKeyInfo =
    ConsoleKeyInfo('\b', ConsoleKey.Backspace, false, false, false)

let private typeKey (value: char) : ConsoleKeyInfo =
    ConsoleKeyInfo(value, ConsoleKey.A, false, false, false)

// ──────────────────────────────────────────────────────────────────────────
// Construction and views

[<Fact>]
let ``Empty filter shows every row in stable order`` () =
    let state = sessionsPicker ()
    count state |> should equal 3
    isEmpty state |> should equal false

    visibleItems state
    |> List.map (fun row -> row.Key)
    |> should equal [ "01JAAA"; "01JBBB"; "01JCCC" ]

    selectedItem state |> should equal (Some(item "01JAAA" "first chat" "[Active]"))

[<Fact>]
let ``Null items and null filters never throw`` () =
    let state = fromItems ResumeSession Unchecked.defaultof<PickerItem list>
    count state |> should equal 0
    isEmpty state |> should equal true
    selectedItem state |> should equal None
    matchesFilter null (item "k" "label" "detail") |> should equal true
    matchesFilter null Unchecked.defaultof<PickerItem> |> should equal false

[<Fact>]
let ``Filter matches key label and detail case-insensitively`` () =
    let state = sessionsPicker ()

    visibleItems (setFilter "second" state)
    |> List.map (fun row -> row.Key)
    |> should equal [ "01JBBB" ]

    visibleItems (setFilter "01jccc" state)
    |> List.map (fun row -> row.Key)
    |> should equal [ "01JCCC" ]

    visibleItems (setFilter "closed" state)
    |> List.map (fun row -> row.Key)
    |> should equal [ "01JCCC" ]

    visibleItems (setFilter "   " state) |> List.length |> should equal 3

    let missing = setFilter "no-such-row" state
    count missing |> should equal 0
    isEmpty missing |> should equal true
    selectedItem missing |> should equal None

[<Fact>]
let ``Setting the filter returns the cursor to the first row`` () =
    let state = sessionsPicker () |> moveDown |> moveDown
    state.Cursor |> should equal 2

    let filtered = setFilter "chat" state
    filtered.Cursor |> should equal 0
    count filtered |> should equal 2

// ──────────────────────────────────────────────────────────────────────────
// Navigation

[<Fact>]
let ``Down and up move with wraparound`` () =
    let state = sessionsPicker ()

    let first = moveDown state
    first.Cursor |> should equal 1

    selectedItem first
    |> should equal (Some(item "01JBBB" "second chat" "[Active]"))

    let wrapped = state |> moveDown |> moveDown |> moveDown

    wrapped.Cursor |> should equal 0

    let wrappedUp = moveUp state
    wrappedUp.Cursor |> should equal 2

    selectedItem wrappedUp
    |> should equal (Some(item "01JCCC" "review notes" "[Closed]"))

[<Fact>]
let ``Navigation on an empty picker stays put`` () =
    let state = setFilter "no-such-row" (sessionsPicker ())
    moveUp state |> should equal state
    moveDown state |> should equal state

// ──────────────────────────────────────────────────────────────────────────
// Key handling: the overlay focus contract

[<Fact>]
let ``Escape cancels without selecting`` () =
    let state, outcome = applyPickerKey (sessionsPicker ()) escapeKey
    outcome |> should equal Cancel
    state |> should equal (sessionsPicker ())

[<Fact>]
let ``Enter selects the cursor row`` () =
    let state = sessionsPicker () |> moveDown
    let next, outcome = applyPickerKey state enterKey
    outcome |> should equal (Pick(item "01JBBB" "second chat" "[Active]"))
    next |> should equal state

[<Fact>]
let ``Enter on an empty picker stays open`` () =
    let state = setFilter "no-such-row" (sessionsPicker ())
    let next, outcome = applyPickerKey state enterKey
    outcome |> should equal Stay
    next |> should equal state

[<Fact>]
let ``Arrow keys navigate and stay open`` () =
    let state = sessionsPicker ()

    let downed, downOutcome = applyPickerKey state downKey
    downOutcome |> should equal Stay
    downed.Cursor |> should equal 1

    let upped, upOutcome = applyPickerKey downed upKey
    upOutcome |> should equal Stay
    upped.Cursor |> should equal 0

[<Fact>]
let ``Typing filters and Backspace edits the filter`` () =
    let state = sessionsPicker ()

    let typed, typedOutcome = applyPickerKey state (typeKey 's')
    typedOutcome |> should equal Stay
    typed.Filter |> should equal "s"
    typed.Cursor |> should equal 0

    let penso, _ = applyPickerKey typed (typeKey 'e')
    let narrowed, _ = applyPickerKey penso (typeKey 'c')
    count narrowed |> should equal 1

    let edited, editedOutcome = applyPickerKey narrowed backspaceKey
    editedOutcome |> should equal Stay
    edited.Filter |> should equal "se"

    let cleared =
        edited
        |> fun current -> fst (applyPickerKey current backspaceKey)
        |> fun current -> fst (applyPickerKey current backspaceKey)

    cleared.Filter |> should equal ""
    count cleared |> should equal 3

    let kept, _ = applyPickerKey (setFilter "" state) backspaceKey
    kept |> should equal (setFilter "" state)

[<Fact>]
let ``Control keys and unknown keys stay open unchanged`` () =
    let state = sessionsPicker ()
    let ctrlS = ConsoleKeyInfo('\u0013', ConsoleKey.S, false, false, true)
    let f1 = ConsoleKeyInfo('\u0000', ConsoleKey.F1, false, false, false)

    let keptCtrl, ctrlOutcome = applyPickerKey state ctrlS
    ctrlOutcome |> should equal Stay
    keptCtrl |> should equal state

    let keptF1, f1Outcome = applyPickerKey state f1
    f1Outcome |> should equal Stay
    keptF1 |> should equal state

// ──────────────────────────────────────────────────────────────────────────
// Command assist: bare triggers only, never a parser

[<Fact>]
let ``Bare selection commands open their picker`` () =
    pickerForBare "/sessions" |> should equal (Some ResumeSession)
    pickerForBare "/resume" |> should equal (Some ResumeSession)
    pickerForBare "/model" |> should equal (Some SwitchModel)
    pickerForBare "/tree" |> should equal (Some ForkAtSequence)
    pickerForBare "/fork" |> should equal (Some ForkAtSequence)
    pickerForBare "  /resume  " |> should equal (Some ResumeSession)

[<Fact>]
let ``Lines with arguments route verbatim with no picker`` () =
    pickerForBare "/resume 01JAAA" |> should equal None
    pickerForBare "/model openai" |> should equal None
    pickerForBare "/fork 12" |> should equal None
    pickerForBare "/sessions extra" |> should equal None
    pickerForBare "hello" |> should equal None
    pickerForBare "/quit" |> should equal None
    pickerForBare "" |> should equal None
    pickerForBare null |> should equal None

[<Fact>]
let ``Picks resolve to the exact line typing the id would send`` () =
    commandLineFor ResumeSession "01JAAA" |> should equal "/resume 01JAAA"
    commandLineFor SwitchModel "openai" |> should equal "/model openai"
    commandLineFor ForkAtSequence "12" |> should equal "/fork 12"
    commandLineFor ResumeSession null |> should equal ""
    commandLineFor SwitchModel "   " |> should equal ""

[<Fact>]
let ``Every REPL command routes with no picker unless bare`` () =
    // Bare selection commands open their picker after the verbatim
    // listing; everything else (arguments, prompts, lifecycle) routes
    // with no picker, so the TUI never parses.
    let expected: (string * PickerKind option) list =
        [
            "/new title", None
            "/sessions", Some ResumeSession
            "/resume 1", None
            "/model", Some SwitchModel
            "/model openai", None
            "/steer text", None
            "/follow text", None
            "/abort", None
            "/compact", None
            "/tree", Some ForkAtSequence
            "/fork 1", None
            "/clone", None
            "/session", None
            "/export out.jsonl", None
            "/review", None
            "/quit", None
            "plain text", None
        ]

    for line, want in expected do
        pickerForBare line |> should equal want

// ──────────────────────────────────────────────────────────────────────────
// Progress detection

[<Fact>]
let ``Compact and export lines take the progress path`` () =
    progressFor "/compact" |> should equal (Some CompactProgress)
    progressFor "  /compact  " |> should equal (Some CompactProgress)

    progressFor "/export out.jsonl"
    |> should equal (Some(ExportProgress "out.jsonl"))

    progressFor "/export" |> should equal (Some(ExportProgress ""))
    progressFor null |> should equal None

[<Fact>]
let ``Prompt and selection lines await verbatim with no progress`` () =
    progressFor "hello" |> should equal None
    progressFor "/sessions" |> should equal None
    progressFor "/resume 1" |> should equal None
    progressFor "/model openai" |> should equal None
    progressFor "/exported out.jsonl" |> should equal None
    progressFor "/compact-now" |> should equal None
