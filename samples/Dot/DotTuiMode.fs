// SPDX-License-Identifier: Apache-2.0
module Dot.DotTuiMode

open System

// Pure fullscreen/plain selector for the dot TUI (issues 330, 335): the
// --no-tui flag plus the four plain conditions. Pure data so tests and the
// pipe smoke assert it without a terminal. Lives in its own module (no
// Legate references) so the unit suite links only this file: DotTui owns
// the console run loop over it, so the fallback selector stays
// byte-identical and the pipe smoke never loads TUI code.

// How dot was asked to render: the --no-tui flag plus the four plain
// conditions.
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
/// Priority order is flag first: --no-tui wins over the environment, then
/// redirected stdout, NO_COLOR, TERM, and CI.
let plainReason (request: TuiRequest) : string =
    if request.NoTuiFlag then "--no-tui"
    elif request.OutputRedirected then "redirected-stdout"
    elif request.NoColor then "NO_COLOR"
    elif request.TermDumb then "TERM=dumb"
    elif request.Ci then "CI"
    else ""

/// Names the display mode for --help and the plain-REPL startup hint:
/// "fullscreen", or "plain (<reason>)" with the plainReason above.
let describeSelection (request: TuiRequest) : string =
    if shouldUseTui request then
        "fullscreen"
    else
        $"plain ({plainReason request})"

/// True when the value reads as a CI marker: set and non-empty, and not
/// the literal "false" (some agents export CI=false when interactive).
let isMarkerSet (value: string | null) : bool =
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

/// The CI-only headless TUI smoke trigger: DOT_TUI_SMOKE set and non-empty
/// (the same marker rule as CI, so DOT_TUI_SMOKE=false stays off). Never a
/// CLI flag: the smoke harness sets it in the environment and pipes stdin,
/// so --help and the pipe contract never change.
let isHeadlessSmokeRequested () : bool =
    isMarkerSet (Environment.GetEnvironmentVariable("DOT_TUI_SMOKE"))
