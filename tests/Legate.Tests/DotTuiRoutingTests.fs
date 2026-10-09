// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DotTuiRoutingTests

open System
open System.Threading.Tasks
open Legate
open Dot
open Dot.DotTuiRouting
open FsUnit.Xunit
open Xunit

type private Dispatch =
    | Line of string
    | Question of string * string
    | Permission of string * PermissionDecisionKind
    | OpenPicker of DotPicker.PickerKind

let private key code =
    ConsoleKeyInfo('\000', code, false, false, false)

let private character value =
    ConsoleKeyInfo(value, ConsoleKey.A, false, false, false)

let private ctrl code =
    ConsoleKeyInfo('\000', code, false, false, true)

let private enter = key ConsoleKey.Enter

let private picker =
    DotPicker.fromItems
        DotPicker.SwitchModel
        [
            {
                DotPicker.Key = "one"
                DotPicker.Label = "match one"
                DotPicker.Detail = "first"
            }
            {
                DotPicker.Key = "two"
                DotPicker.Label = "match two"
                DotPicker.Detail = "second"
            }
            {
                DotPicker.Key = "other"
                DotPicker.Label = "excluded"
                DotPicker.Detail = "third"
            }
        ]
    |> DotPicker.setFilter "match"
    |> DotPicker.moveDown

type private Harness() =
    let state = create ()
    let mutable view = DotRender.empty
    let dispatches = ResizeArray<Dispatch>()
    let sid, tid = SessionId.New(), TurnId.New()
    let stamp = DateTimeOffset.UnixEpoch
    let mutable sequence = 0L

    let next () =
        sequence <- sequence + 1L
        Nullable sequence

    let callbacks =
        {
            SnapshotRenderer = fun () -> view
            UpdateRenderer = fun update -> view <- update view
            HandleLineAsync =
                fun line ->
                    dispatches.Add(Line line)
                    Task.FromResult(line <> "/quit")
            ReplyQuestionAsync =
                fun id answer ->
                    dispatches.Add(Question(id, answer))
                    Task.CompletedTask
            ReplyPermissionAsync =
                fun id decision ->
                    dispatches.Add(Permission(id, decision))
                    Task.CompletedTask
            OpenPickerAsync =
                fun kind ->
                    dispatches.Add(OpenPicker kind)
                    Task.FromResult(Some { picker with Kind = kind })
        }

    member _.State = state
    member _.View = view
    member _.Callbacks = callbacks
    member _.Dispatches = List.ofSeq dispatches
    member _.Key key = handleKeyAsync callbacks state key
    member _.VisiblePicker = visiblePicker view state

    member _.Ask id =
        view <- DotRender.apply view (QuestionAskedEvent(sid, tid, next (), stamp, id, "answer?"))

    member _.Answer id =
        view <- DotRender.apply view (QuestionAnsweredEvent(sid, tid, next (), stamp, id, "recorded"))

    member _.Request id =
        view <- DotRender.apply view (PermissionRequestedEvent(sid, tid, next (), stamp, id, "read_file"))

    member _.Resolve id =
        view <-
            DotRender.apply
                view
                (PermissionResolvedEvent(sid, tid, next (), stamp, id, PermissionDecisionKind.AllowOnce))

[<Theory>]
[<InlineData("", false)>]
[<InlineData(" \t  ", false)>]
[<InlineData("  answer \t", false)>]
[<InlineData("first\n second\n", false)>]
[<InlineData("/quit", false)>]
[<InlineData("/exit", false)>]
[<InlineData("/model", false)>]
[<InlineData("", true)>]
[<InlineData(" \t  ", true)>]
[<InlineData("  answer \t", true)>]
[<InlineData("first\n second\n", true)>]
[<InlineData("/quit", true)>]
let ``question Enter dispatches exact editor text once and nothing else`` (text: string) (withPicker: bool) =
    task {
        let h = Harness()
        h.State.Picker <- if withPicker then Some picker else None
        let saved = h.State.Picker
        h.State.Editor <- DotInput.fromText text
        h.Ask "q-exact"
        do! h.Key enter
        h.Dispatches |> should equal [ Question("q-exact", text) ]
        h.State.Editor |> should equal DotInput.empty
        h.State.Picker |> should equal saved
        h.State.Go |> should equal true
        // Replies do not resolve renderer prompts until the stream says so.
        DotRender.hasPendingQuestion h.View |> should equal true
        h.VisiblePicker |> should equal None
        // Preserve the existing history policy, including command-looking text.
        let _, expectedHistory, _ =
            DotInput.applyKey (DotInput.fromText text) DotInput.emptyHistory enter

        h.State.History |> should equal expectedHistory
    }

[<Fact>]
let ``question arrival suspends complete picker through editing and resumes selection`` () =
    task {
        let h = Harness()
        h.State.Picker <- Some picker
        h.VisiblePicker |> should equal (Some picker)
        h.Ask "q-arrival"

        for input in
            [
                character ' '
                character 'x'
                character 'y'
                key ConsoleKey.LeftArrow
                key ConsoleKey.Backspace
                key ConsoleKey.End
                ctrl ConsoleKey.O
                character 'z'
                key ConsoleKey.UpArrow
                key ConsoleKey.DownArrow
                key ConsoleKey.Escape
            ] do
            do! h.Key input
            h.State.Picker |> should equal (Some picker)
            h.VisiblePicker |> should equal None
            Assert.Empty h.Dispatches

        DotInput.toText h.State.Editor |> should equal " y\nz"
        do! h.Key enter
        h.Dispatches |> should equal [ Question("q-arrival", " y\nz") ]
        h.State.Picker |> should equal (Some picker)
        h.VisiblePicker |> should equal None
        h.Answer "q-arrival"
        h.VisiblePicker |> should equal (Some picker)
        do! h.Key(key ConsoleKey.UpArrow)
        h.State.Picker |> should equal (Some { picker with Cursor = 0 })
        do! h.Key enter
        h.State.Picker |> should equal None

        h.Dispatches
        |> should
            equal
            [
                Question("q-arrival", " y\nz")
                Line "/model one"
            ]
    }

[<Theory>]
[<InlineData('a', PermissionDecisionKind.AllowOnce)>]
[<InlineData('s', PermissionDecisionKind.AllowForSession)>]
[<InlineData('d', PermissionDecisionKind.Deny)>]
let ``permission arrival answers without touching picker and resumes it`` letter decision =
    task {
        let h = Harness()
        h.State.Picker <- Some picker
        h.State.Editor <- DotInput.fromText "draft"
        h.Request "p-arrival"
        h.VisiblePicker |> should equal None
        do! h.Key(character letter)
        h.Dispatches |> should equal [ Permission("p-arrival", decision) ]
        h.State.Editor |> should equal (DotInput.fromText "draft")
        h.State.Picker |> should equal (Some picker)
        h.VisiblePicker |> should equal None
        h.Resolve "p-arrival"
        h.VisiblePicker |> should equal (Some picker)
        do! h.Key(key ConsoleKey.DownArrow)
        h.State.Picker |> should equal (Some { picker with Cursor = 0 })
        do! h.Key enter

        h.Dispatches
        |> should
            equal
            [
                Permission("p-arrival", decision)
                Line "/model one"
            ]

        h.State.Picker |> should equal None
    }

[<Fact>]
let ``permission non-answer keys keep editor fallback without mutating picker`` () =
    task {
        let h = Harness()
        h.State.Picker <- Some picker
        h.Request "p-edit"
        h.State.Editor <- DotInput.fromText "draft"
        let mutable editor = h.State.Editor
        let mutable history = h.State.History

        for input in
            [
                key ConsoleKey.UpArrow
                key ConsoleKey.DownArrow
                character 'x'
                key ConsoleKey.Backspace
                key ConsoleKey.Escape
            ] do
            let nextEditor, nextHistory, intent = DotInput.applyKey editor history input
            intent |> should equal DotInput.Noop
            editor <- nextEditor
            history <- nextHistory
            do! h.Key input
            h.State.Editor |> should equal editor
            h.State.History |> should equal history
            h.State.Picker |> should equal (Some picker)
            Assert.Empty h.Dispatches
    }

[<Theory>]
[<InlineData(false)>]
[<InlineData(true)>]
let ``permission fallback fork submission preserves and resumes complete picker`` openerReturnsNone =
    task {
        let h = Harness()
        h.State.Picker <- Some picker
        h.Request "p-fork"
        let opened = ResizeArray<DotPicker.PickerKind>()

        let callbacks =
            { h.Callbacks with
                OpenPickerAsync =
                    fun kind ->
                        opened.Add kind

                        Task.FromResult(
                            if openerReturnsNone then
                                None
                            else
                                Some(DotPicker.fromItems kind [])
                        )
            }

        for letter in "/fork" do
            do! handleKeyAsync callbacks h.State (character letter)
            h.State.Picker |> should equal (Some picker)
            h.VisiblePicker |> should equal None

        Assert.Empty h.Dispatches
        do! handleKeyAsync callbacks h.State enter
        h.Dispatches |> should equal [ Line "/fork" ]
        List.ofSeq opened |> should equal [ DotPicker.ForkAtSequence ]
        h.State.Editor |> should equal DotInput.empty
        h.State.Picker |> should equal (Some picker)
        DotRender.hasPendingPermission h.View |> should equal true
        h.VisiblePicker |> should equal None
        do! h.Key(character 'a')
        h.State.Picker |> should equal (Some picker)
        h.Resolve "p-fork"
        h.VisiblePicker |> should equal (Some picker)
        do! h.Key enter
        h.State.Picker |> should equal None

        h.Dispatches
        |> should
            equal
            [
                Line "/fork"
                Permission("p-fork", PermissionDecisionKind.AllowOnce)
                Line "/model two"
            ]
    }

[<Fact>]
let ``overlapping prompts preserve permission priority first pending ids and suspension`` () =
    task {
        let h = Harness()
        h.State.Picker <- Some picker
        h.Ask "q-first"
        h.Ask "q-second"
        h.Request "p-first"
        h.Request "p-second"

        for id in [ "p-first"; "p-second" ] do
            do! h.Key(character 'a')
            h.State.Editor |> should equal DotInput.empty
            h.State.Picker |> should equal (Some picker)
            h.Resolve id
            h.VisiblePicker |> should equal None

        for id in [ "q-first"; "q-second" ] do
            h.State.Editor <- DotInput.fromText "reply"
            do! h.Key enter
            h.State.Picker |> should equal (Some picker)
            h.VisiblePicker |> should equal None
            h.Answer id

        h.VisiblePicker |> should equal (Some picker)

        h.Dispatches
        |> should
            equal
            [
                Permission("p-first", PermissionDecisionKind.AllowOnce)
                Permission("p-second", PermissionDecisionKind.AllowOnce)
                Question("q-first", "reply")
                Question("q-second", "reply")
            ]
    }

[<Theory>]
[<InlineData("")>]
[<InlineData(" \t ")>]
[<InlineData("/quit")>]
let ``permission fallback Enter answers a concurrent question verbatim`` text =
    task {
        let h = Harness()
        h.State.Picker <- Some picker
        h.Request "p"
        h.Ask "q"
        h.State.Editor <- DotInput.fromText text
        do! h.Key enter
        h.Dispatches |> should equal [ Question("q", text) ]
        h.State.Picker |> should equal (Some picker)
        h.Answer "q"
        h.VisiblePicker |> should equal None
    }

[<Theory>]
[<InlineData(false)>]
[<InlineData(true)>]
let ``explicit controls keep prompt semantics and never dismiss suspended picker`` question =
    task {
        for input, line in [ key ConsoleKey.Escape, "/quit" ] do
            let h = Harness()
            h.State.Picker <- Some picker
            if question then h.Ask "q" else h.Request "p"
            do! h.Key input
            h.Dispatches |> should equal [ Line line ]
            h.State.Picker |> should equal (Some picker)
            h.State.Go |> should equal (line <> "/quit")
    }

[<Theory>]
[<InlineData(false)>]
[<InlineData(true)>]
let ``exit shortcuts bypass prompt and picker queue drains`` question =
    task {
        for consoleKey in
            [
                ConsoleKey.C
                ConsoleKey.D
                ConsoleKey.Q
            ] do
            let h = Harness()
            h.State.Picker <- Some picker
            if question then h.Ask "q" else h.Request "p"
            do! h.Key(ctrl consoleKey)
            Assert.Empty h.Dispatches
            h.State.Go |> should equal false
            h.State.Picker |> should equal (Some picker)
    }

[<Fact>]
let ``ordinary picker filtering navigation selection and cancellation still dispatch correctly`` () =
    task {
        let h = Harness()
        h.State.Editor <- DotInput.fromText "/model"
        do! h.Key enter

        h.Dispatches
        |> should
            equal
            [
                Line "/model"
                OpenPicker DotPicker.SwitchModel
            ]

        h.State.Picker |> should equal (Some picker)
        do! h.Key(key ConsoleKey.Backspace)

        h.State.Picker
        |> should
            equal
            (Some
                { picker with
                    Filter = "matc"
                    Cursor = 0
                })

        do! h.Key(character 'h')
        do! h.Key(key ConsoleKey.DownArrow)
        h.State.Picker |> should equal (Some picker)
        do! h.Key(key ConsoleKey.UpArrow)
        do! h.Key enter

        h.Dispatches
        |> should
            equal
            [
                Line "/model"
                OpenPicker DotPicker.SwitchModel
                Line "/model one"
            ]

        h.State.Picker |> should equal None
        h.State.Picker <- Some picker
        do! h.Key(key ConsoleKey.Escape)
        h.State.Picker |> should equal None
        h.State.Go |> should equal true
        h.Dispatches.Length |> should equal 3
        let emptyPicker = DotPicker.fromItems DotPicker.ResumeSession []
        h.State.Picker <- Some emptyPicker

        for input in
            [
                enter
                key ConsoleKey.UpArrow
                key ConsoleKey.DownArrow
            ] do
            do! h.Key input
            h.State.Picker |> should equal (Some emptyPicker)
            h.Dispatches.Length |> should equal 3
    }

[<Fact>]
let ``ordinary editor blank submit steer abort and quit retain dispatch and history`` () =
    task {
        let h = Harness()
        h.State.Editor <- DotInput.fromText " \t "
        do! h.Key enter
        Assert.Empty h.Dispatches
        h.State.Editor |> should equal (DotInput.fromText " \t ")
        h.State.Editor <- DotInput.fromText " hello "
        do! h.Key enter
        h.State.Editor |> should equal DotInput.empty
        h.State.Editor <- DotInput.fromText "direction"
        do! h.Key(ctrl ConsoleKey.S)
        h.State.Editor <- DotInput.fromText "/abort"
        do! h.Key enter
        h.State.Go |> should equal true
        h.State.Editor <- DotInput.fromText "/quit"
        do! h.Key enter
        h.State.Go |> should equal false
        h.State.History.Entries |> should equal [ " hello "; "direction"; "/abort" ]

        h.Dispatches
        |> should
            equal
            [
                Line " hello "
                Line "/steer direction"
                Line "/abort"
                Line "/quit"
            ]
    }

[<Fact>]
let ``progress commands dispatch without blocking later abort`` () =
    task {
        let h = Harness()
        let completion = TaskCompletionSource<bool>()
        let lines = ResizeArray<string>()

        let callbacks =
            { h.Callbacks with
                HandleLineAsync =
                    fun line ->
                        lines.Add line

                        if line = "/compact" then
                            completion.Task
                        else
                            Task.FromResult true
            }

        h.State.Editor <- DotInput.fromText "/compact"
        do! handleKeyAsync callbacks h.State enter

        DotRender.toViewportLines h.View
        |> List.exists (fun line -> line.Contains("compact"))
        |> should equal true

        h.State.Editor <- DotInput.fromText "/abort"
        do! handleKeyAsync callbacks h.State enter
        List.ofSeq lines |> should equal [ "/compact"; "/abort" ]
        completion.SetResult true
    }
