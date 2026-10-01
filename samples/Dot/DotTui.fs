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
// Fallback selector (issue 330)

// How dot was asked to render: the --no-tui flag plus the four plain
// conditions. Pure data so tests and the pipe smoke assert it without a
// terminal.
type TuiRequest =
    {
        /// The --no-tui flag forces plain output.
        NoTuiFlag: bool
        /// Console.IsOutputRedirected (pipes, CI capture) forces plain.
        OutputRedirected: bool
        /// NO_COLOR is set and non-empty: plain, no ANSI colors.
        NoColor: bool
        /// TERM=dumb (or empty): plain, the terminal cannot do fullscreen.
        TermDumb: bool
        /// A CI environment variable is set: plain, there is no human to see
        /// the fullscreen shell.
        Ci: bool
    }

/// True when dot may open the fullscreen shell: every plain condition is
/// clear. Piped stdout never loads the TUI path.
let shouldUseTui (request: TuiRequest) : bool =
    not request.NoTuiFlag
    && not request.OutputRedirected
    && not request.NoColor
    && not request.TermDumb
    && not request.Ci

/// Names why the selector stays on plain output, or empty for fullscreen.
let plainReason (request: TuiRequest) : string =
    if request.NoTuiFlag then "--no-tui"
    elif request.OutputRedirected then "redirected-stdout"
    elif request.NoColor then "NO_COLOR"
    elif request.TermDumb then "TERM=dumb"
    elif request.Ci then "CI"
    else ""

/// True when the value reads as a CI marker: set and non-empty, and not
/// the literal "false" (some agents export CI=false when interactive).
let private isMarkerSet (value: string | null) : bool =
    match value with
    | null -> false
    | raw when String.IsNullOrWhiteSpace raw -> false
    | raw -> not (String.Equals(raw.Trim(), "false", StringComparison.OrdinalIgnoreCase))

/// True when any well-known CI environment variable marks the run.
let isCiEnvironment () : bool =
    isMarkerSet (Environment.GetEnvironmentVariable("CI"))
    || isMarkerSet (Environment.GetEnvironmentVariable("GITHUB_ACTIONS"))
    || isMarkerSet (Environment.GetEnvironmentVariable("GITLAB_CI"))
    || isMarkerSet (Environment.GetEnvironmentVariable("JENKINS_URL"))
    || isMarkerSet (Environment.GetEnvironmentVariable("TF_BUILD"))
    || isMarkerSet (Environment.GetEnvironmentVariable("CONTINUOUS_INTEGRATION"))

/// Reads the live selector request: the flag plus redirected stdout and
/// the NO_COLOR / TERM / CI environment. Never throws for missing
/// variables; a security-stubbed IsOutputRedirected falls back to plain.
let readRequest (noTuiFlag: bool) : TuiRequest =
    let redirected =
        try
            Console.IsOutputRedirected
        with _ ->
            true

    let noColor =
        not (String.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("NO_COLOR")))

    let term =
        match Environment.GetEnvironmentVariable("TERM") with
        | null -> ""
        | raw -> raw.Trim().ToLowerInvariant()

    {
        NoTuiFlag = noTuiFlag
        OutputRedirected = redirected
        NoColor = noColor
        TermDumb = term = "" || term = "dumb" || term = "unknown"
        Ci = isCiEnvironment ()
    }

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
/// Resize is covered by re-reading the window size every frame; colorless
/// terminals (NO_COLOR or TERM=dumb) render the same layout without ANSI
/// colors. Quit, abort, and failure paths all restore the cursor and the
/// primary screen in try/finally (always 0: the REPL exit contract).
/// Approval answers under --ask ride the engine's console reader until the
/// renderer child paints them inline: prefer the plain REPL for --ask runs.
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

        let request = readRequest false
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
        // handler: the TUI captures and routes only. Its line output lands
        // in the ring the viewport paints; its console reader stays for the
        // approval path the renderer child takes over.
        let ring = new LineRing()

        let engine =
            ReplEngine.Engine(client, agents, packages, Console.In, ring, waitBound, initialModel, providerOptions)

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

            let collected = ResizeArray<string>()
            let mutable editor = DotInput.empty
            let mutable history = DotInput.emptyHistory
            let mutable go = true

            let drainRing () : unit =
                try
                    for line in ring.Drain() do
                        collected.Add(line)
                with _ ->
                    ()

            let paint () : unit =
                try
                    let width, height = windowSize ()

                    let templates = DotTemplates.listTemplates Environment.CurrentDirectory

                    let hints = DotInput.queryHints (DotInput.cursorLine editor) templates

                    let inputMax = max 4 (min 12 (height / 3))

                    let inputRows, (cursorRow, cursorCol) =
                        DotInput.renderInputRegion width inputMax editor hints

                    let state =
                        if engine.IsApprovalPending then
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
                            (collected |> List.ofSeq)
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

            let route (intent: DotInput.InputIntent) : Task =
                task {
                    match intent with
                    | DotInput.SubmitText text ->
                        match DotInput.submitLine text with
                        | Some line ->
                            let! keepGoing = engine.HandleLineAsync(line, cancellationToken)
                            go <- go && keepGoing
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

            // Key pump: quit-only polling is gone (bare q types now); every
            // key runs the pure decoder, edits stay local, and only submit,
            // steer, abort, and quit intents touch the engine. Guarded by
            // entered and KeyAvailable so piped stdin never blocks or throws
            // out of the loop.
            let pollKeys () : Task =
                task {
                    if entered && go && not quitRequested then
                        try
                            if Console.KeyAvailable then
                                let key = Console.ReadKey(true)
                                let nextEditor, nextHistory, intent = DotInput.applyKey editor history key
                                editor <- nextEditor
                                history <- nextHistory
                                do! route intent
                        with _ ->
                            ()
                }

            paint ()

            while go && not quitRequested && not cancellationToken.IsCancellationRequested do
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
