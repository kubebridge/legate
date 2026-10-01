// SPDX-License-Identifier: Apache-2.0
module Dot.DotTui

open System
open System.Threading
open System.Threading.Tasks
open Legate

// Spike-only TUI foundation for issue 330: a stdlib-only fullscreen
// skeleton over the existing engine events plus the pure TTY fallback
// selector the whole EPIC builds on. No new NuGet dependencies: the
// fullscreen shell is the ANSI alternate screen plus System.Console
// sizing, so the piped path loads zero TUI assemblies and stays
// byte-identical to today. Scorecard verdict (Task 1): stdlib-only wins;
// Terminal.Gui v2 is rejected for its heavy driver/dependency tail that
// breaks minimal-deps and offline restore, and Spectre.Console live is
// rejected because it is a rich inline renderer, not a fullscreen shell.

// ──────────────────────────────────────────────────────────────────────────
// Fallback selector (Task 2)

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
// Layout shell (issue 331)

// The pure frame lives in DotShell (banner header, transcript viewport,
// status bar): this module owns only the console run loop over it, so the
// fallback selector above stays byte-identical and the pipe smoke never
// loads TUI code.

/// The current console size, falling back to 80x24 when redirected or
/// stubbed (keeps the proof renderable under pipes and tests).
let private windowSize () : int * int =
    try
        max 20 Console.WindowWidth, max 8 Console.WindowHeight
    with _ ->
        80, 24

/// True when the event terminates the proof turn.
let private isProofTerminal (evt: SessionEvent) : bool =
    not (isNull (box evt))
    && (evt :? TurnCompletedEvent || evt :? TurnAbortedEvent || evt :? TurnFailedEvent)

/// Maps the settled turn status to the process exit code, copying the
/// samples/Headless map verbatim (0 completed / 2 aborted / 1 failed, 3
/// anything else).
let private exitFor (status: TurnStatus) : int =
    match status with
    | TurnStatus.Completed -> 0
    | TurnStatus.Aborted -> 2
    | TurnStatus.Failed -> 1
    | TurnStatus.Pending
    | TurnStatus.Running
    | TurnStatus.Suspended
    | _ -> 3

/// Runs the layout shell proof: opens one durable session under the
/// ensured scripted agent, streams exactly one scripted Subscribe turn
/// into the alternate-screen layout shell (banner, transcript viewport,
/// status bar), and exits cleanly. Resize is covered by re-reading the
/// window size every frame; colorless terminals (NO_COLOR or TERM=dumb)
/// render the same layout without ANSI colors. Input is quit-only
/// (q/Escape/Ctrl+C); the caller's cancellation token (never None) drives
/// the turn wait and the stream. Quit, abort, crash, and failure paths all
/// restore the cursor and the primary screen in try/finally (0 completed /
/// 2 aborted / 1 failed / 0 clean quit / 1 anything else).
let runSpikeAsync
    (client: SessionClient)
    (agents: IAgentStore)
    (packages: IAgentPackageStore)
    (initialModel: ModelReference)
    (cancellationToken: CancellationToken)
    : Task<int> =
    task {
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(agents)
        ArgumentNullException.ThrowIfNull(packages)

        let request = readRequest false
        let useColor = not request.NoColor && not request.TermDumb
        let entered = not request.OutputRedirected

        // Ctrl+C quits through the same teardown as q: cancel the turn
        // wait and the stream, then fall through to the finally restore.
        // Quit is cooperative: the prompt task observes turnCts, the
        // Subscribe loop observes streamCts, and the wait loop below polls
        // quitRequested, so every path lands in the teardown finally.
        let mutable quitRequested = false

        use turnCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
        use streamCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)

        let onCancel =
            ConsoleCancelEventHandler(fun _ args ->
                args.Cancel <- true
                quitRequested <- true

                try
                    turnCts.Cancel()
                with _ ->
                    ()

                try
                    streamCts.Cancel()
                with _ ->
                    ())

        try
            try
                Console.CancelKeyPress.AddHandler(onCancel)
            with _ ->
                ()

            let! agentId = ReplEngine.ensureModelAgentAsync agents packages initialModel cancellationToken
            let options = SessionOptions()
            options.Title <- "dot layout shell"
            options.HostInstructionFiles <- DotContext.resolveHostInstructionFiles Environment.CurrentDirectory

            let! session = SessionClientOperations.OpenSessionAsync(client, agentId, options, cancellationToken)
            let query = "tui-shell-proof"

            if entered then
                try
                    Console.Out.Write(DotShell.alternateEnter)
                    Console.Out.Flush()
                with _ ->
                    ()

            let collected = ResizeArray<string>()
            collected.Add("BOOT seq=- TurnStarted")

            let mutable phase = DotShell.initialTurnState

            let paint () : unit =
                try
                    let width, height = windowSize ()
                    let current = lock collected (fun () -> phase)

                    let frame =
                        DotShell.renderFrame
                            width
                            height
                            useColor
                            (collected |> List.ofSeq)
                            (DotShell.statusText session.Id initialModel current)

                    if entered then
                        Console.Out.Write(DotShell.homeClear + frame)
                        Console.Out.Flush()
                    else
                        Console.Error.Write(frame)
                with _ ->
                    ()

            paint ()

            // Quit-only input: q/Q/Escape quits the proof. Guarded by
            // entered and KeyAvailable so piped stdin never blocks or
            // throws out of the wait loop.
            let pollQuit () : unit =
                if entered && not quitRequested then
                    try
                        if Console.KeyAvailable then
                            let key = Console.ReadKey(true)

                            if key.KeyChar = 'q' || key.KeyChar = 'Q' || key.Key = ConsoleKey.Escape then
                                quitRequested <- true

                                try
                                    turnCts.Cancel()
                                with _ ->
                                    ()

                                try
                                    streamCts.Cancel()
                                with _ ->
                                    ()
                    with _ ->
                        ()

            let streamTask =
                task {
                    try
                        let stream =
                            SessionClientOperations.Subscribe(client, session.Id, 0L, streamCts.Token)

                        let enumerator = stream.GetAsyncEnumerator(streamCts.Token)

                        try
                            let mutable go = true

                            while go do
                                try
                                    let! has = enumerator.MoveNextAsync().AsTask()

                                    if not has then
                                        go <- false
                                    else
                                        let evt = enumerator.Current

                                        if not (isNull (box evt)) then
                                            let line = ReplEngine.renderEvent evt

                                            lock collected (fun () ->
                                                collected.Add(line)
                                                phase <- DotShell.updateTurnState phase evt)

                                            paint ()

                                            if isProofTerminal evt then
                                                go <- false
                                with :? OperationCanceledException ->
                                    go <- false
                        finally
                            try
                                enumerator.DisposeAsync().AsTask() |> ignore
                            with _ ->
                                ()
                    with
                    | :? OperationCanceledException -> ()
                    | error ->
                        lock collected (fun () -> collected.Add($"STREAM-FAILED {error.Message}"))
                        paint ()
                }

            let! settled =
                task {
                    try
                        let! result =
                            SessionClientExtensions.PromptAndWaitAsync(
                                client,
                                session.Id,
                                UserMessage.Text query,
                                turnCts.Token
                            )

                        return Some result
                    with
                    | :? OperationCanceledException when quitRequested -> return None
                    | error ->
                        if not quitRequested then
                            lock collected (fun () -> collected.Add($"TURN-FAILED {error.Message}"))
                            paint ()

                        return None
                }

            try
                let deadline = DateTimeOffset.UtcNow.AddSeconds(10.0)
                let mutable waited = false

                while not waited do
                    pollQuit ()

                    let terminal =
                        lock collected (fun () ->
                            collected
                            |> Seq.exists (fun line ->
                                line.Contains("TurnCompleted")
                                || line.Contains("TurnAborted")
                                || line.Contains("TurnFailed")))

                    if terminal || quitRequested || DateTimeOffset.UtcNow >= deadline then
                        waited <- true
                    else
                        do! Task.Delay(50)
            with _ ->
                ()

            try
                streamCts.Cancel()
            with _ ->
                ()

            try
                do! streamTask.WaitAsync(TimeSpan.FromSeconds(5.0))
            with _ ->
                ()

            paint ()

            match settled with
            | Some result -> return exitFor result.Status
            | None when quitRequested -> return 0
            | None -> return 1
        finally
            try
                Console.CancelKeyPress.RemoveHandler(onCancel)
            with _ ->
                ()

            if entered then
                try
                    Console.Out.Write(DotShell.alternateExit)
                    Console.Out.Flush()
                with _ ->
                    ()
    }
