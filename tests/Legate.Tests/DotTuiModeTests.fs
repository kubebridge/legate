// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DotTuiModeTests

open System
open Dot.DotTuiMode
open FsUnit.Xunit
open Xunit

// The env-var test below mutates process environment and xunit runs
// collections in parallel, so it owns a dedicated non-parallel collection.
// The guard matters beyond this file: DotReplSmokeTests spawns dot children
// that inherit the testhost environment, so a DOT_TUI_SMOKE="1" window would
// flip a parallel REPL smoke into the headless path. A DisableParallelization
// collection never overlaps any other test, so serializing this single writer
// is sufficient with no changes to the smoke suites.
[<CollectionDefinition("DotTuiEnv", DisableParallelization = true)>]
type DotTuiEnvCollection() = class end

// Selector proofs (issue 335): the pure DotTuiMode fullscreen/plain matrix
// (--no-tui, redirected stdout, NO_COLOR, TERM dumb/empty/unknown, CI
// markers including the CI=false nuance), the reason priority order, and
// the display-mode description --help and the startup hint print. TTY-free:
// every case runs against pure TuiRequest inputs, so mode selection is
// proven without a terminal. The linked DotTuiMode.fs compiles here without
// referencing the Dot exe.

let private fullscreen: TuiRequest =
    {
        NoTuiFlag = false
        OutputRedirected = false
        NoColor = false
        TermDumb = false
        Ci = false
    }

[<Fact>]
let ``Clear conditions open the fullscreen shell`` () =
    shouldUseTui fullscreen |> should equal true
    plainReason fullscreen |> should equal ""
    describeSelection fullscreen |> should equal "fullscreen"

[<Fact>]
let ``NoTui flag forces plain and names itself`` () =
    let request = { fullscreen with NoTuiFlag = true }

    shouldUseTui request |> should equal false
    plainReason request |> should equal "--no-tui"
    describeSelection request |> should equal "plain (--no-tui)"

[<Fact>]
let ``Redirected stdout forces plain`` () =
    let request =
        { fullscreen with
            OutputRedirected = true
        }

    shouldUseTui request |> should equal false
    plainReason request |> should equal "redirected-stdout"
    describeSelection request |> should equal "plain (redirected-stdout)"

[<Fact>]
let ``NoColor forces plain`` () =
    let request = { fullscreen with NoColor = true }

    shouldUseTui request |> should equal false
    plainReason request |> should equal "NO_COLOR"
    describeSelection request |> should equal "plain (NO_COLOR)"

[<Fact>]
let ``Dumb terminal forces plain`` () =
    let request = { fullscreen with TermDumb = true }

    shouldUseTui request |> should equal false
    plainReason request |> should equal "TERM=dumb"
    describeSelection request |> should equal "plain (TERM=dumb)"

[<Fact>]
let ``CI marker forces plain`` () =
    let request = { fullscreen with Ci = true }

    shouldUseTui request |> should equal false
    plainReason request |> should equal "CI"
    describeSelection request |> should equal "plain (CI)"

[<Fact>]
let ``Reason priority is flag stdout color term then CI`` () =
    plainReason
        { fullscreen with
            NoTuiFlag = true
            OutputRedirected = true
            Ci = true
        }
    |> should equal "--no-tui"

    plainReason
        { fullscreen with
            OutputRedirected = true
            NoColor = true
            TermDumb = true
            Ci = true
        }
    |> should equal "redirected-stdout"

    plainReason
        { fullscreen with
            NoColor = true
            TermDumb = true
            Ci = true
        }
    |> should equal "NO_COLOR"

    plainReason
        { fullscreen with
            TermDumb = true
            Ci = true
        }
    |> should equal "TERM=dumb"

[<Fact>]
let ``Marker reads set non-empty and not the literal false`` () =
    isMarkerSet null |> should equal false
    isMarkerSet "" |> should equal false
    isMarkerSet "   " |> should equal false
    isMarkerSet "false" |> should equal false
    isMarkerSet "FALSE" |> should equal false
    isMarkerSet " False " |> should equal false
    isMarkerSet "1" |> should equal true
    isMarkerSet "true" |> should equal true
    isMarkerSet "CI" |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Env vars (dedicated collection: mutates process environment)

[<Collection("DotTuiEnv")>]
type EnvTriggerTests() =

    [<Fact>]
    member _.``Headless smoke trigger follows the marker rule``() =
        let previous = Environment.GetEnvironmentVariable("DOT_TUI_SMOKE")

        try
            Environment.SetEnvironmentVariable("DOT_TUI_SMOKE", "1")
            isHeadlessSmokeRequested () |> should equal true

            Environment.SetEnvironmentVariable("DOT_TUI_SMOKE", "false")
            isHeadlessSmokeRequested () |> should equal false

            Environment.SetEnvironmentVariable("DOT_TUI_SMOKE", null)
            isHeadlessSmokeRequested () |> should equal false
        finally
            Environment.SetEnvironmentVariable("DOT_TUI_SMOKE", previous)
