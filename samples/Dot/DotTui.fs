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
// Skeleton frame (Task 3)

// The ASCII banner heading every fullscreen frame.
let bannerLine = "     _       _   "

/// Renders one fullscreen frame as plain text with optional ANSI colors:
/// the ASCII dot banner header, the scrollable transcript lines, and the
/// input box footer. Pure over width/height so resize and colorless
/// terminals are covered without a TTY: narrow windows truncate, short
/// windows keep the banner plus the tail, and useColor=false strips every
/// escape except the layout newlines.
let renderFrame (width: int) (height: int) (useColor: bool) (transcript: string list) : string =
    let safeWidth = max 20 (min 240 width)
    let safeHeight = max 8 (min 100 height)
    let plainLines = transcript |> List.filter (fun line -> not (isNull (box line)))

    let banner =
        if useColor then
            "\u001b[1;36m" + bannerLine + "\u001b[0m"
        else
            bannerLine

    let header =
        [
            banner
            "dot TUI spike (scripted proof, --no-tui for plain)"
        ]

    let footer =
        [
            "> [spike proof runs one scripted turn, then exits]"
        ]

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

/// Runs the spike proof: opens one durable session under the ensured
/// scripted agent, streams exactly one scripted Subscribe turn into the
/// alternate-screen skeleton, and exits cleanly. Resize is covered by
/// re-reading the window size every frame; colorless terminals (NO_COLOR
/// or TERM=dumb) render the same layout without ANSI colors; any failure
/// restores the primary screen before returning 1.
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

        try
            let! agentId = ReplEngine.ensureModelAgentAsync agents packages initialModel cancellationToken
            let options = SessionOptions()
            options.Title <- "dot tui spike"
            options.HostInstructionFiles <- DotContext.resolveHostInstructionFiles Environment.CurrentDirectory

            let! session = SessionClientOperations.OpenSessionAsync(client, agentId, options, cancellationToken)
            let query = "tui-spike-proof"

            if entered then
                try
                    Console.Out.Write("\u001b[?1049h\u001b[?25l")
                    Console.Out.Flush()
                with _ ->
                    ()

            use streamCts = new CancellationTokenSource()
            let collected = ResizeArray<string>()
            collected.Add("BOOT seq=- TurnStarted")

            let paint () : unit =
                try
                    let width, height = windowSize ()
                    let frame = renderFrame width height useColor (collected |> List.ofSeq)

                    if entered then
                        Console.Out.Write("\u001b[H\u001b[2J" + frame)
                        Console.Out.Flush()
                    else
                        Console.Error.Write(frame)
                with _ ->
                    ()

            paint ()

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
                                            let line = $"EVENT seq={evt.Sequence} {evt.GetType().Name}"
                                            lock collected (fun () -> collected.Add(line))
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
                                CancellationToken.None
                            )

                        return Some result
                    with error ->
                        lock collected (fun () -> collected.Add($"TURN-FAILED {error.Message}"))
                        paint ()
                        return None
                }

            try
                let deadline = DateTimeOffset.UtcNow.AddSeconds(10.0)
                let mutable waited = false

                while not waited do
                    let terminal =
                        lock collected (fun () ->
                            collected
                            |> Seq.exists (fun line ->
                                line.Contains("TurnCompleted")
                                || line.Contains("TurnAborted")
                                || line.Contains("TurnFailed")))

                    if terminal || DateTimeOffset.UtcNow >= deadline then
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
            | None -> return 1
        finally
            if entered then
                try
                    Console.Out.Write("\u001b[?25h\u001b[?1049l")
                    Console.Out.Flush()
                with _ ->
                    ()
    }
