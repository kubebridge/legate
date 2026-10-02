// SPDX-License-Identifier: Apache-2.0
module Dot.DotTui

open System
open System.Threading
open System.Threading.Tasks
open Legate

// Fullscreen host over the shared REPL engine. Immutable view snapshots
// invalidate the layout only when something changes. DotShell diffs the
// resulting rows, so the key/resize poll never writes to an idle terminal.

// ──────────────────────────────────────────────────────────────────────────
// Fallback selector (issues 330, 335)

// The pure selector lives in DotTuiMode (TuiRequest, shouldUseTui,
// plainReason, describeSelection, readRequest): this module owns only the
// console run loop over it, so the fallback selector stays byte-identical
// and the pipe smoke never loads TUI code. Tests link DotTuiMode directly.

// ──────────────────────────────────────────────────────────────────────────
// Fullscreen input loop (issues 331, 332)

// The pure frame lives in DotShell (banner header, transcript viewport,
// status bar, input-box rows): this module owns only the console run loop
// over it, so the fallback selector above stays byte-identical and the pipe
// smoke never loads TUI code.

/// The current console size, falling back to 80x24 when redirected or
/// stubbed (keeps the proof renderable under pipes and tests).
let private windowSize () : int * int =
    try
        max 1 Console.WindowWidth, max 1 Console.WindowHeight
    with _ ->
        80, 24

/// A TextWriter sink for the engine's line output: the fullscreen loop
/// drains it into the transcript viewport instead of letting EVENT and
/// RESULT lines corrupt the alternate screen. Lock-guarded: the engine
/// drain writes beside the paint loop.
type private LineRing() =
    inherit IO.TextWriter()

    let gate = obj ()
    let lines = ResizeArray<string>()

    override _.Encoding: Text.Encoding = Text.Encoding.UTF8

    override _.WriteLine(value: string) : unit =
        lock gate (fun () -> lines.Add(if isNull (box value) then "" else value))

    /// Drains the buffered lines oldest first, clearing the ring.
    /// <returns>The drained lines.</returns>
    member _.Drain() : string list =
        lock gate (fun () ->
            let drained = lines |> List.ofSeq
            lines.Clear()
            drained)

/// Runs the fullscreen session: opens one durable session through the
/// shared engine, paints the layout shell (banner, transcript viewport,
/// status bar, input box with history and slash hints) into the alternate
/// screen, and pumps console keys through the DotInput decoder. Submitted
/// lines route through Engine.HandleLineAsync verbatim, so every slash
/// command behaves exactly as in the plain REPL: plain text queues, Ctrl+S
/// steers via /steer, /abort stops the turn, Ctrl+C/D/Q exit via bounded abort,
/// and Esc on an empty buffer or /quit drains and exits.
/// The transcript viewport paints the streaming renderer blocks (issue
/// 333): assistant markdown-lite, tool-call cards, inline permission and
/// question widgets, and turn lifecycle markers folded from the same
/// Subscribe stream the REPL prints. Permission answers ride ReplyAsync
/// inline (a/s/d) and question answers ride the input box through
/// ReplyAsync, taking over the #332 console-reader deferral. Resize is
/// covered by re-reading the window size every frame; colorless terminals
/// (NO_COLOR or TERM=dumb) render the same layout without ANSI colors.
/// Quit, abort, and failure paths all restore the cursor and the primary
/// screen in try/finally (always 0: the REPL exit contract).
let runSpikeAsync
    (client: SessionClient)
    (agents: IAgentStore)
    (packages: IAgentPackageStore)
    (initialModel: ModelReference)
    (providerOptions: ReplEngine.ProviderOption list)
    (configuration: Microsoft.Extensions.Configuration.IConfiguration)
    (waitBound: TimeSpan)
    (cancellationToken: CancellationToken)
    : Task<int> =
    task {
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(agents)
        ArgumentNullException.ThrowIfNull(packages)
        ArgumentNullException.ThrowIfNull(providerOptions)

        let request = DotTuiMode.readRequest false
        let useColor = not request.NoColor && not request.TermDumb
        let entered = not request.OutputRedirected

        // TreatControlCAsInput lets the key pump handle exit consistently,
        // including while a picker or permission widget owns focus.
        // The CancelKeyPress handler stays as the backup quit path
        // (Ctrl+Break and runtimes without TreatControlCAsInput support).
        let mutable quitRequested = false

        use breakCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)

        let onCancel =
            ConsoleCancelEventHandler(fun _ args ->
                args.Cancel <- true
                quitRequested <- true

                try
                    breakCts.Cancel()
                with _ ->
                    ())

        // The engine owns the session, the turn queue, and every slash
        // handler: the TUI captures and routes only. Its Subscribe events
        // fold into the renderer viewport blocks through the OnEvent hook
        // (single subscription, no duplicate streams); its diagnostic
        // lines land in the ring the viewport retains for parity. The TUI
        // owns approvals inline, so the engine never blocks on the console
        // reader beside the key pump.
        let ring = new LineRing()

        let engine =
            ReplEngine.Engine(client, agents, packages, Console.In, ring, waitBound, initialModel, providerOptions)

        engine.SetTuiOwnsApprovals(true)

        let rendererGate = obj ()
        let mutable renderer = DotRender.empty

        engine.SetOnEvent(Some(fun evt -> lock rendererGate (fun () -> renderer <- DotRender.apply renderer evt)))

        let savedTreatControlC =
            try
                Console.TreatControlCAsInput
            with _ ->
                false

        try
            try
                Console.CancelKeyPress.AddHandler(onCancel)
            with _ ->
                ()

            try
                Console.TreatControlCAsInput <- true
            with _ ->
                ()

            do! engine.OpenSessionAsync("dot tui", cancellationToken)

            if entered then
                try
                    Console.Out.Write(DotShell.alternateEnter)
                    Console.Out.Flush()
                with _ ->
                    ()

            let mutable editor = DotInput.empty
            let mutable history = DotInput.emptyHistory
            let mutable go = true
            let mutable scrollOffset = 0
            let mutable previousScreen: DotShell.Screen option = None
            let mutable lastPaintKey = None
            let mutable templates: string list = []
            let mutable wasSlash = false

            let context =
                DotContext.resolveHostInstructionFiles Environment.CurrentDirectory
                |> List.ofSeq

            let welcomeLines =
                [ "[Context]" ]
                @ (if context.IsEmpty then
                       [ "  No project instructions" ]
                   else
                       context |> List.map (fun path -> "  " + path))
                @ [
                    ""
                    "[Skills]"
                    "  " + DotSkills.sampleSkillName
                    ""
                    "[Sub-agents]"
                    "  explore · general (use /agents to list)"
                    ""
                    "Enter to send · / for commands · Ctrl+O for a new line"
                ]

            // The open selection picker, if any: while open it owns
            // Up/Down/Enter/Esc and the editor stays suspended. A pick
            // resolves to the verbatim engine line typing the id would
            // send; Esc closes with no routing.
            let mutable picker: DotPicker.PickerState option = None

            // Opens the picker serving one bare command: items come from
            // the same reads the verbatim listing just printed (stored
            // sessions, registered providers, journal positions), so the
            // rows match the viewport. A failed read keeps the listing
            // and opens nothing; the verbatim lines already prove parity.
            let openPickerAsync (kind: DotPicker.PickerKind) : Task =
                task {
                    try
                        match kind with
                        | DotPicker.ResumeSession ->
                            let! page =
                                SessionClientListingOperations.ListSessionsAsync(
                                    client,
                                    SessionListOptions(),
                                    cancellationToken
                                )

                            let items =
                                if isNull (box page) || isNull (box page.Items) then
                                    []
                                else
                                    page.Items
                                    |> Seq.filter (fun session -> not (isNull (box session)))
                                    |> Seq.map (fun session ->
                                        {
                                            DotPicker.Key = session.Id.ToString()
                                            DotPicker.Label = if isNull (box session.Title) then "" else session.Title
                                            DotPicker.Detail = $"[{session.State}]"
                                        })
                                    |> List.ofSeq

                            picker <- Some(DotPicker.fromItems kind items)
                        | DotPicker.SwitchModel ->
                            let! items =
                                DotModels.discoverAsync
                                    configuration
                                    providerOptions
                                    engine.CurrentModel
                                    cancellationToken

                            picker <- Some(DotPicker.fromItems kind items)
                        | DotPicker.ForkAtSequence ->
                            let! events = DotExport.readAllEventsAsync client engine.CurrentSessionId cancellationToken

                            let items =
                                if isNull (box events) then
                                    []
                                else
                                    events
                                    |> Seq.filter (fun evt -> not (isNull (box evt)) && evt.Sequence.HasValue)
                                    |> Seq.map (fun evt ->
                                        {
                                            DotPicker.Key = evt.Sequence.Value.ToString()
                                            DotPicker.Label = evt.GetType().Name
                                            DotPicker.Detail = $"seq={evt.Sequence.Value}"
                                        })
                                    |> List.ofSeq
                                    |> fun all ->
                                        if all.Length <= 100 then
                                            all
                                        else
                                            all |> List.skip (all.Length - 100)

                            picker <- Some(DotPicker.fromItems kind items)
                    with _ ->
                        picker <- None
                }

            let snapshotRenderer () : DotRender.RendererState = lock rendererGate (fun () -> renderer)

            let drainRing () : unit =
                try
                    // Live EVENT lines duplicate the OnEvent journal fold
                    // (the same Subscribe stream), so the ring drops only
                    // them: everything else folds into the viewport,
                    // including TREE lines (/tree pages ReadEventsAsync
                    // outside any turn with no journal equivalent).
                    for line in ring.Drain() do
                        if not (DotRender.isJournalDuplicate line) then
                            lock rendererGate (fun () -> renderer <- DotRender.addLine renderer line)
                with _ ->
                    ()

            let paint () : unit =
                try
                    let width, height = windowSize ()

                    let view = snapshotRenderer ()

                    let state =
                        if DotRender.hasPendingPermission view || DotRender.hasPendingQuestion view then
                            SessionState.WaitingForInput
                        elif engine.IsTurnRunning then
                            SessionState.Running
                        else
                            SessionState.Idle

                    let status = DotShell.statusText engine.CurrentSessionId engine.CurrentModel state
                    let key = (width, height, editor, picker, view, status, scrollOffset)

                    if lastPaintKey <> Some key then
                        let slash = (DotInput.cursorLine editor).StartsWith("/", StringComparison.Ordinal)

                        if slash && not wasSlash then
                            templates <- DotTemplates.listTemplates Environment.CurrentDirectory

                        wasSlash <- slash
                        let hints = DotInput.queryHints (DotInput.cursorLine editor) templates

                        let inputRows, cursor =
                            match picker with
                            | Some shown -> DotShell.renderPickerOverlay (width - 1) false shown
                            | None ->
                                let rows, cursor =
                                    DotInput.renderInputRegion (width - 1) (max 3 (min 10 (height / 3))) editor hints
                                // The welcome screen documents shortcuts; keep the composer quiet.
                                rows |> List.take (max 1 (rows.Length - 1)), cursor

                        let welcome = view.Display.IsEmpty && view.Lifecycle.IsEmpty && view.Order.IsEmpty

                        let transcript =
                            if welcome then
                                welcomeLines
                                @ (view.Diagnostics |> List.filter (fun line -> not (line.StartsWith("SESSION "))))
                                |> List.map (fun text ->
                                    {
                                        DotShell.Style = DotShell.Plain
                                        DotShell.Text = text
                                    })
                            else
                                DotRender.toSessionCells view

                        let status =
                            if scrollOffset > 0 then
                                $"↑ scrollback · End latest · {status}"
                            else
                                DotShell.workspaceStatus width Environment.CurrentDirectory engine.CurrentModel state

                        let screen =
                            DotShell.renderScreenWithCells
                                width
                                height
                                useColor
                                welcome
                                transcript
                                status
                                inputRows
                                cursor
                                scrollOffset

                        let update = DotShell.screenUpdate previousScreen screen

                        if entered && update <> "" then
                            Console.Out.Write(update)
                            Console.Out.Flush()

                        previousScreen <- Some screen
                        lastPaintKey <- Some key
                with _ ->
                    ()

            let route (intent: DotInput.InputIntent) : Task =
                task {
                    let view = snapshotRenderer ()

                    match DotRender.firstQuestion view with
                    | Some pending when intent <> DotInput.QuitTui ->
                        match intent with
                        | DotInput.SubmitText text ->
                            // Verbatim like the REPL console reader: the
                            // blank drop is bypassed, so a blank answer
                            // resumes "" through the same ReplyAsync path.
                            let answer = DotRender.answerForSubmit text

                            try
                                do! engine.ReplyQuestionAsync(pending.QuestionId, answer, cancellationToken)
                            with _ ->
                                ()

                            editor <- DotInput.empty
                        | DotInput.AbortTurn ->
                            let! _ = engine.HandleLineAsync("/abort", cancellationToken)
                            ()
                        | DotInput.QuitTui ->
                            let! keepGoing = engine.HandleLineAsync("/quit", cancellationToken)
                            go <- go && keepGoing
                        | DotInput.SteerText _
                        | DotInput.Noop -> ()
                    | _ ->
                        match intent with
                        | DotInput.SubmitText text ->
                            match DotInput.submitLine text with
                            | Some line ->
                                if not (line.StartsWith("/", StringComparison.Ordinal)) then
                                    scrollOffset <- 0
                                    lock rendererGate (fun () -> renderer <- DotRender.addUserMessage renderer line)

                                match DotPicker.progressFor line with
                                | Some progress ->
                                    // Long-running commands paint their
                                    // start marker and route
                                    // fire-and-forget, so the key pump
                                    // never blocks: the completion line
                                    // lands through the ring while input
                                    // stays routable and /abort still lands.
                                    lock rendererGate (fun () ->
                                        renderer <-
                                            match progress with
                                            | DotPicker.CompactProgress -> DotRender.markCompactRunning renderer
                                            | DotPicker.ExportProgress target ->
                                                DotRender.markExportRunning renderer target)

                                    let _ =
                                        task {
                                            try
                                                let! _ = engine.HandleLineAsync(line, cancellationToken)
                                                ()
                                            with _ ->
                                                ()
                                        }

                                    ()
                                | None ->
                                    let! keepGoing = engine.HandleLineAsync(line, cancellationToken)
                                    go <- go && keepGoing

                                    // Bare selection commands routed
                                    // verbatim first (identical lines),
                                    // then open the shared picker for
                                    // one-key follow-up; Esc dismisses.
                                    match DotPicker.pickerForBare line with
                                    | Some kind -> do! openPickerAsync kind
                                    | None -> ()
                            | None -> ()
                        | DotInput.SteerText text ->
                            match DotInput.steerLine text with
                            | Some routed ->
                                let! keepGoing = engine.HandleLineAsync(routed, cancellationToken)
                                go <- go && keepGoing
                            | None -> ()
                        | DotInput.AbortTurn ->
                            let! _ = engine.HandleLineAsync("/abort", cancellationToken)
                            ()
                        | DotInput.QuitTui ->
                            let! keepGoing = engine.HandleLineAsync("/quit", cancellationToken)
                            go <- go && keepGoing
                        | DotInput.Noop -> ()
                }

            // True when the modifiers carry exactly Control (plus optionally
            // Shift, which terminals report for Ctrl+Shift+letter).
            let isCtrlOnly (modifiers: ConsoleModifiers) : bool =
                modifiers.HasFlag(ConsoleModifiers.Control)
                && not (modifiers.HasFlag(ConsoleModifiers.Alt))

            // Key pump: inline approvals first (single owner), then the pure
            // decoder. A pending permission answers from a/s/d without
            // touching the editor; a pending question answers from the input
            // box submit through ReplyAsync; Ctrl+T toggles the first
            // truncated tool card. Everything else runs the DotInput decoder
            // with edits local and only submit/steer/abort/quit touching the
            // engine. Guarded by entered and KeyAvailable so piped stdin
            // never blocks or throws out of the loop.
            let pollKeys () : Task =
                task {
                    if entered && go && not quitRequested then
                        try
                            let mutable readCount = 0

                            while Console.KeyAvailable && go && readCount < 256 do
                                readCount <- readCount + 1
                                let key = Console.ReadKey(true)
                                let view = snapshotRenderer ()

                                match picker with
                                | _ when DotInput.isExitKey key ->
                                    go <- false
                                    quitRequested <- true
                                    breakCts.Cancel()
                                | Some shown ->
                                    // Picker focus: the picker owns
                                    // Up/Down/Enter/Esc and the editor
                                    // stays suspended. A pick resolves to
                                    // the verbatim engine line typing the
                                    // id would send.
                                    let next, outcome = DotPicker.applyPickerKey shown key

                                    match outcome with
                                    | DotPicker.Stay -> picker <- Some next
                                    | DotPicker.Cancel -> picker <- None
                                    | DotPicker.Pick item ->
                                        picker <- None

                                        let line = DotPicker.commandLineFor shown.Kind item.Key

                                        if line <> "" then
                                            let! _ = engine.HandleLineAsync(line, cancellationToken)
                                            ()
                                | None when key.Key = ConsoleKey.PageUp ->
                                    scrollOffset <- scrollOffset + max 1 (snd (windowSize ()) / 2)
                                | None when key.Key = ConsoleKey.PageDown ->
                                    scrollOffset <- max 0 (scrollOffset - max 1 (snd (windowSize ()) / 2))
                                | None when key.Key = ConsoleKey.End && DotInput.isBlank editor -> scrollOffset <- 0
                                | None ->
                                    match DotRender.firstPermission view with
                                    | Some pending ->
                                        match DotRender.decisionForKey key with
                                        | Some decision ->
                                            try
                                                do!
                                                    engine.ReplyPermissionAsync(
                                                        pending.RequestId,
                                                        decision,
                                                        cancellationToken
                                                    )
                                            with _ ->
                                                ()
                                        | None ->
                                            let nextEditor, nextHistory, intent = DotInput.applyKey editor history key

                                            editor <- nextEditor
                                            history <- nextHistory
                                            do! route intent
                                    | None ->
                                        match DotRender.firstQuestion view with
                                        | Some pending when DotInput.isBlank editor && DotInput.isPlainEnter key ->
                                            // Blank answers resume ""
                                            // verbatim like the REPL
                                            // console reader (a blank
                                            // buffer otherwise decodes to
                                            // Noop and would idle).
                                            try
                                                do! engine.ReplyQuestionAsync(pending.QuestionId, "", cancellationToken)
                                            with _ ->
                                                ()

                                            editor <- DotInput.empty
                                        | _ when
                                            isCtrlOnly key.Modifiers
                                            && (key.Key = ConsoleKey.T || key.KeyChar = '\u0014')
                                            ->
                                            let current = snapshotRenderer ()

                                            let target =
                                                current.Order
                                                |> List.tryFind (fun id ->
                                                    match current.Tools.TryFind id with
                                                    | Some card when card.Overflow > 0 -> true
                                                    | _ -> false)

                                            match target with
                                            | Some id ->
                                                lock rendererGate (fun () ->
                                                    renderer <- DotRender.toggleExpanded renderer id)
                                            | None -> ()
                                        | _ ->
                                            let nextEditor, nextHistory, intent = DotInput.applyKey editor history key

                                            editor <- nextEditor
                                            history <- nextHistory
                                            do! route intent
                        with _ ->
                            ()
                }

            paint ()

            while go && not quitRequested && not cancellationToken.IsCancellationRequested do
                do! pollKeys ()
                drainRing ()
                paint ()

                try
                    do! Task.Delay(16, breakCts.Token)
                with :? OperationCanceledException ->
                    ()

            drainRing ()
            paint ()

            // Exit shortcuts must not wait for the normal /quit queue drain:
            // a provider call or unanswered approval can keep it alive forever.
            // Best-effort abort is bounded, then host shutdown owns teardown.
            if quitRequested && engine.IsTurnRunning then
                use exitCts = new CancellationTokenSource(TimeSpan.FromSeconds 2.)

                try
                    let! _ = engine.HandleLineAsync("/abort", exitCts.Token).WaitAsync(exitCts.Token)
                    ()
                with _ ->
                    ()

            return 0
        finally
            try
                Console.CancelKeyPress.RemoveHandler(onCancel)
            with _ ->
                ()

            try
                Console.TreatControlCAsInput <- savedTreatControlC
            with _ ->
                ()

            if entered then
                try
                    Console.Out.Write(DotShell.alternateExit)
                    Console.Out.Flush()
                with _ ->
                    ()
    }

// ──────────────────────────────────────────────────────────────────────────
// Headless TUI smoke (issue 335)

// Runs the CI-only headless TUI smoke the DOT_TUI_SMOKE environment
// trigger selects: opens one scripted session through the shared engine,
// paints the layout shell (banner, transcript viewport, status bar) as
// plain frames on stdout, routes the piped stdin lines through
// Engine.HandleLineAsync verbatim (so "hello" runs one scripted turn and
// "/quit" exits), and exits 0. Never enters the alternate screen and never
// reads console keys, so there is nothing to restore: stdout carries zero
// TUI escapes and stderr names the teardown. The harness pipes at most a
// few lines; EOF or /quit ends the run.
let runHeadlessSmokeAsync
    (client: SessionClient)
    (agents: IAgentStore)
    (packages: IAgentPackageStore)
    (initialModel: ModelReference)
    (providerOptions: ReplEngine.ProviderOption list)
    (waitBound: TimeSpan)
    (cancellationToken: CancellationToken)
    : Task<int> =
    task {
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(agents)
        ArgumentNullException.ThrowIfNull(packages)
        ArgumentNullException.ThrowIfNull(providerOptions)

        let ring = new LineRing()

        let engine =
            ReplEngine.Engine(client, agents, packages, Console.In, ring, waitBound, initialModel, providerOptions)

        engine.SetTuiOwnsApprovals(true)

        let rendererGate = obj ()
        let mutable renderer = DotRender.empty

        engine.SetOnEvent(Some(fun evt -> lock rendererGate (fun () -> renderer <- DotRender.apply renderer evt)))

        try
            do! engine.OpenSessionAsync("dot tui smoke", cancellationToken)

            let sessionId = engine.CurrentSessionId

            let drainRing () : unit =
                try
                    for line in ring.Drain() do
                        if not (DotRender.isJournalDuplicate line) then
                            lock rendererGate (fun () -> renderer <- DotRender.addLine renderer line)
                with _ ->
                    ()

            let paint () : unit =
                try
                    drainRing ()

                    let view = lock rendererGate (fun () -> renderer)

                    let state =
                        if DotRender.hasPendingPermission view || DotRender.hasPendingQuestion view then
                            SessionState.WaitingForInput
                        elif engine.IsTurnRunning then
                            SessionState.Running
                        else
                            SessionState.Idle

                    let frame =
                        DotShell.renderFrameWithInput
                            80
                            24
                            false
                            (DotRender.toViewportLines view)
                            (DotShell.statusText sessionId initialModel state)
                            []

                    Console.Out.Write(frame)
                    Console.Out.Flush()
                with _ ->
                    ()

            paint ()

            let mutable go = true
            let mutable seen = 0

            while go && seen < 8 && not cancellationToken.IsCancellationRequested do
                let line: string | null =
                    try
                        Console.In.ReadLine()
                    with _ ->
                        null

                match line with
                | null -> go <- false
                | text ->
                    seen <- seen + 1

                    try
                        let! keepGoing = engine.HandleLineAsync(text, cancellationToken)
                        go <- go && keepGoing
                    with _ ->
                        ()

                    paint ()

            drainRing ()
            paint ()
            Console.Error.WriteLine("dot: TUI-SMOKE ok (booted, rendered, exited; no alternate screen entered).")
            return 0
        with error ->
            Console.Error.WriteLine($"dot: TUI-SMOKE failed: {error.Message}")
            return 1
    }
