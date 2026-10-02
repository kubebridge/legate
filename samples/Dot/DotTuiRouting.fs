// SPDX-License-Identifier: Apache-2.0
module Dot.DotTuiRouting

open System
open System.Threading.Tasks
open Legate

// The interactive key pump's state and dispatch, independent of console I/O.
// Only the serial key pump mutates this state; renderer access stays locked
// by the host callbacks because subscription events arrive beside the pump.
type State =
    {
        mutable Editor: DotInput.EditorState
        mutable History: DotInput.History
        mutable Picker: DotPicker.PickerState option
        mutable Go: bool
    }

let create () =
    {
        Editor = DotInput.empty
        History = DotInput.emptyHistory
        Picker = None
        Go = true
    }

type Callbacks =
    {
        SnapshotRenderer: unit -> DotRender.RendererState
        UpdateRenderer: (DotRender.RendererState -> DotRender.RendererState) -> unit
        HandleLineAsync: string -> Task<bool>
        ReplyQuestionAsync: string -> string -> Task
        ReplyPermissionAsync: string -> PermissionDecisionKind -> Task
        OpenPickerAsync: DotPicker.PickerKind -> Task<DotPicker.PickerState option>
    }

// Input ownership and paint use the same snapshot rule. Suspension never
// changes the stored picker, even while a reply awaits its resolution event.
let visiblePicker (view: DotRender.RendererState) (state: State) =
    if DotRender.hasPendingPermission view || DotRender.hasPendingQuestion view then
        None
    else
        state.Picker

let private replyQuestion (callbacks: Callbacks) (state: State) id answer =
    task {
        try
            do! callbacks.ReplyQuestionAsync id answer
        with _ ->
            ()

        state.Editor <- DotInput.empty
    }

let private route (callbacks: Callbacks) (state: State) (intent: DotInput.InputIntent) =
    task {
        let view = callbacks.SnapshotRenderer()

        let suspendedPicker =
            if DotRender.hasPendingPermission view || DotRender.hasPendingQuestion view then
                state.Picker
            else
                None

        match DotRender.firstQuestion view with
        | Some pending when intent <> DotInput.QuitTui ->
            match intent with
            | DotInput.SubmitText text ->
                do! replyQuestion callbacks state pending.QuestionId (DotRender.answerForSubmit text)
            | DotInput.AbortTurn ->
                let! _ = callbacks.HandleLineAsync "/abort"
                ()
            | DotInput.QuitTui ->
                let! keepGoing = callbacks.HandleLineAsync "/quit"
                state.Go <- state.Go && keepGoing
            | DotInput.SteerText _
            | DotInput.Noop -> ()
        | _ ->
            match intent with
            | DotInput.SubmitText text ->
                match DotInput.submitLine text with
                | Some line ->
                    match DotPicker.progressFor line with
                    | Some progress ->
                        callbacks.UpdateRenderer(fun renderer ->
                            match progress with
                            | DotPicker.CompactProgress -> DotRender.markCompactRunning renderer
                            | DotPicker.ExportProgress target -> DotRender.markExportRunning renderer target)

                        // Keep long commands nonblocking, including their existing
                        // exception handling and completion through the line ring.
                        let _ =
                            task {
                                try
                                    let! _ = callbacks.HandleLineAsync line
                                    ()
                                with _ ->
                                    ()
                            }

                        ()
                    | None ->
                        let! keepGoing = callbacks.HandleLineAsync line
                        state.Go <- state.Go && keepGoing

                        match DotPicker.pickerForBare line with
                        | Some kind ->
                            let! picker = callbacks.OpenPickerAsync kind
                            // Fallback commands retain their dispatch, but cannot
                            // replace a picker suspended by prompt input ownership.
                            state.Picker <- suspendedPicker |> Option.orElse picker
                        | None -> ()
                | None -> ()
            | DotInput.SteerText text ->
                match DotInput.steerLine text with
                | Some routed ->
                    let! keepGoing = callbacks.HandleLineAsync routed
                    state.Go <- state.Go && keepGoing
                | None -> ()
            | DotInput.AbortTurn ->
                let! _ = callbacks.HandleLineAsync "/abort"
                ()
            | DotInput.QuitTui ->
                let! keepGoing = callbacks.HandleLineAsync "/quit"
                state.Go <- state.Go && keepGoing
            | DotInput.Noop -> ()
    }

let private decode (callbacks: Callbacks) (state: State) key =
    task {
        match DotRender.firstQuestion (callbacks.SnapshotRenderer()) with
        | Some pending when DotInput.isPlainEnter key ->
            let answer = DotInput.toText state.Editor
            // Keep the decoder's existing history policy, but never dispatch
            // its submit classification: even /quit and blank text are answers.
            let _, history, _ = DotInput.applyKey state.Editor state.History key
            state.History <- history
            do! replyQuestion callbacks state pending.QuestionId answer
        | _ ->
            let editor, history, intent = DotInput.applyKey state.Editor state.History key
            state.Editor <- editor
            state.History <- history
            do! route callbacks state intent
    }

let handleKeyAsync (callbacks: Callbacks) (state: State) (key: ConsoleKeyInfo) : Task =
    task {
        try
            let view = callbacks.SnapshotRenderer()

            match visiblePicker view state with
            | _ when DotInput.isExitKey key -> state.Go <- false
            | Some shown ->
                let next, outcome = DotPicker.applyPickerKey shown key

                match outcome with
                | DotPicker.Stay -> state.Picker <- Some next
                | DotPicker.Cancel -> state.Picker <- None
                | DotPicker.Pick item ->
                    state.Picker <- None
                    let line = DotPicker.commandLineFor shown.Kind item.Key

                    if line <> "" then
                        let! _ = callbacks.HandleLineAsync line
                        ()
            | None ->
                match DotRender.firstPermission view with
                | Some pending ->
                    match DotRender.decisionForKey key with
                    | Some decision ->
                        try
                            do! callbacks.ReplyPermissionAsync pending.RequestId decision
                        with _ ->
                            ()
                    | None -> do! decode callbacks state key
                | None ->
                    if
                        key.Modifiers.HasFlag(ConsoleModifiers.Control)
                        && not (key.Modifiers.HasFlag(ConsoleModifiers.Alt))
                        && (key.Key = ConsoleKey.T || key.KeyChar = '\u0014')
                    then
                        let current = callbacks.SnapshotRenderer()

                        let target =
                            current.Order
                            |> List.tryFind (fun id ->
                                match current.Tools.TryFind id with
                                | Some card when card.Overflow > 0 -> true
                                | _ -> false)

                        match target with
                        | Some id -> callbacks.UpdateRenderer(fun renderer -> DotRender.toggleExpanded renderer id)
                        | None -> ()
                    else
                        do! decode callbacks state key
        with _ ->
            ()
    }
