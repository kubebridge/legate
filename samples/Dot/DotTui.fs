// SPDX-License-Identifier: Apache-2.0
module Dot.DotTui

open System
open System.Threading
open System.Threading.Tasks
open Legate

// Fullscreen TUI over the existing engine events (issues 330-332): the
// stdlib-only fullscreen shell plus the multiline input box, over the same
// ReplEngine handlers the plain REPL drives. No new NuGet dependencies: the
// shell is the ANSI alternate screen plus System.Console sizing, so the
// piped path loads zero TUI assemblies and stays byte-identical to today.
// Scorecard verdict (issue 330): stdlib-only wins; Terminal.Gui v2 is
// rejected for its heavy driver/dependency tail that breaks minimal-deps
// and offline restore, and Spectre.Console live is rejected because it is
// a rich inline renderer, not a fullscreen shell.

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
        max 20 Console.WindowWidth, max 8 Console.WindowHeight
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
/// steers via /steer, Ctrl+C aborts the turn via /abort without killing
/// dot, and Esc on an empty buffer, Ctrl+Q, or /quit drains and exits.
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

        // Ctrl+C aborts through the input decoder, never through process
        // teardown: TreatControlCAsInput delivers it as a key, the decoder
        // maps it to the abort intent, and the turn aborts while dot keeps
        // running. The CancelKeyPress handler stays as the backup quit path
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

            let sessionId = engine.CurrentSessionId

            if entered then
                try
                    Console.Out.Write(DotShell.alternateEnter)
                    Console.Out.Flush()
                with _ ->
                    ()

            let input = DotTuiRouting.create ()

            // Opens the picker serving one bare command: items come from
            // the same reads the verbatim listing just printed (stored
            // sessions, registered providers, journal positions), so the
            // rows match the viewport. A failed read keeps the listing
            // and opens nothing; the verbatim lines already prove parity.
            let openPickerAsync (kind: DotPicker.PickerKind) : Task<DotPicker.PickerState option> =
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

                            return Some(DotPicker.fromItems kind items)
                        | DotPicker.SwitchModel ->
                            let items =
                                providerOptions
                                |> List.map (fun option ->
                                    {
                                        DotPicker.Key = option.Id
                                        DotPicker.Label = option.Id
                                        DotPicker.Detail = $"default {option.DefaultModel}"
                                    })

                            return Some(DotPicker.fromItems kind items)
                        | DotPicker.ForkAtSequence ->
                            let! events = DotExport.readAllEventsAsync client sessionId cancellationToken

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

                            return Some(DotPicker.fromItems kind items)
                    with _ ->
                        return None
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

                    let templates = DotTemplates.listTemplates Environment.CurrentDirectory

                    let hints = DotInput.queryHints (DotInput.cursorLine input.Editor) templates

                    let view = snapshotRenderer ()

                    let inputRows, (cursorRow, cursorCol) =
                        match DotTuiRouting.visiblePicker view input with
                        | Some shown -> DotShell.renderPickerOverlay width useColor shown
                        | None ->
                            let inputMax = max 4 (min 12 (height / 3))
                            DotInput.renderInputRegion width inputMax input.Editor hints

                    let state =
                        if DotRender.hasPendingPermission view || DotRender.hasPendingQuestion view then
                            SessionState.WaitingForInput
                        elif engine.IsTurnRunning then
                            SessionState.Running
                        else
                            SessionState.Idle

                    let frame =
                        DotShell.renderFrameWithInput
                            width
                            height
                            useColor
                            (DotRender.toViewportLines view)
                            (DotShell.statusText sessionId initialModel state)
                            inputRows

                    if entered then
                        Console.Out.Write(DotShell.homeClear + frame)

                        // The frame ends with a newline past the quit hint:
                        // climb back to the buffer cursor row and column.
                        let up = 1 + (inputRows.Length - cursorRow)
                        Console.Out.Write($"\u001b[{up}A\u001b[{cursorCol + 1}G")
                        Console.Out.Flush()
                    else
                        Console.Error.Write(frame)
                with _ ->
                    ()

            let routing: DotTuiRouting.Callbacks =
                {
                    SnapshotRenderer = snapshotRenderer
                    UpdateRenderer = fun update -> lock rendererGate (fun () -> renderer <- update renderer)
                    HandleLineAsync = fun line -> engine.HandleLineAsync(line, cancellationToken)
                    ReplyQuestionAsync = fun id answer -> engine.ReplyQuestionAsync(id, answer, cancellationToken)
                    ReplyPermissionAsync =
                        fun id decision -> engine.ReplyPermissionAsync(id, decision, cancellationToken)
                    OpenPickerAsync = openPickerAsync
                }

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
                    if entered && input.Go && not quitRequested then
                        try
                            if Console.KeyAvailable then
                                let key = Console.ReadKey(true)
                                do! DotTuiRouting.handleKeyAsync routing input key
                        with _ ->
                            ()
                }

            paint ()

            while input.Go && not quitRequested && not cancellationToken.IsCancellationRequested do
                drainRing ()
                paint ()
                do! pollKeys ()

                try
                    do! Task.Delay(50, breakCts.Token)
                with :? OperationCanceledException ->
                    ()

            drainRing ()
            paint ()

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
