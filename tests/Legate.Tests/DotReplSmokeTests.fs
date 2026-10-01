// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DotReplSmokeTests

open System
open System.Diagnostics
open System.IO
open FsUnit.Xunit
open Xunit

// Dot REPL smoke test (issues 304, 306, 307, 308): runs the built dot
// binary as a subprocess on scripted transports (no live keys, no external
// services) through the interactive loop over piped stdin: prompt/stream/
// settle with assistant text, /new, store-backed /sessions, /resume
// continuing context across processes on one DOT_DB_PATH file, /model
// listing and mid-session switching with transcript survival, /abort,
// /compact, /quit, unknown-command usage, the opt-in ask policy with
// inline permission approval, and the settle-deadline report path. The
// issue 306 coding probes run the dot-local tools over the smoke workdir
// (the process working directory): writes and edits with zero friction
// under the default policy, deny/session answers under --ask, the
// outside-root fence, exec, --help text, and the --mcp attach. Issue 307
// adds --provider/--model parsing and startup selection coverage: dummy
// env keys register the live providers with no network (selection only,
// the REPL quits before any turn runs), so unknown providers name the known
// ids, a provider without its key names the env var, an unparsable model
// names the known providers, and one key without --provider just works.
// Issue 308 adds steering over the slow slow-steer probe (slow-echo tool):
// /steer interrupts (prior RESULT Aborted plus the new turn), /follow
// folds in (Inject, one Completed settle), plain input queues (Queue, two
// Completed settles), /tree lists journal positions and /fork branches the
// prefix leaving the source untouched (plus a free /clone at the tail),
// and /abort//compact behave mid-turn and idle with the Deferred compact
// path. Mirrors the LegateCli smoke precedent; each run gets its own temp
// database file.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// Infers Debug/Release from the test assembly's own directory.
let private configurationName () : string =
    let directory = AppContext.BaseDirectory

    if directory.Contains("Release", StringComparison.OrdinalIgnoreCase) then
        "Release"
    else
        "Debug"

/// Locates a built sample DLL, failing with the searched path.
let private sampleDll (project: string) (assembly: string) : string =
    let root =
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."))

    let config = configurationName ()

    let candidate =
        Path.Combine(root, "samples", project, "bin", config, "net10.0", assembly)

    if File.Exists(candidate) then
        candidate
    else
        failwith $"Expected the built sample at '{candidate}': build the solution first."

/// Runs dot with piped stdin on one database file, returning exit code,
/// stdout, and stderr.
let private runDotEnv
    (dotDll: string)
    (arguments: string list)
    (stdin: string)
    (workdir: string)
    (dbPath: string)
    (extraEnv: (string * string) list)
    : int * string * string =
    let info = ProcessStartInfo("dotnet")
    info.ArgumentList.Add(dotDll)

    for argument in arguments do
        info.ArgumentList.Add(argument)

    info.RedirectStandardInput <- true
    info.RedirectStandardOutput <- true
    info.RedirectStandardError <- true
    info.UseShellExecute <- false
    info.WorkingDirectory <- workdir
    info.Environment["Logging__LogLevel__Default"] <- "None"
    info.Environment["DOTNET_NOLOGO"] <- "1"
    info.Environment["DOT_DB_PATH"] <- dbPath

    for key, value in extraEnv do
        info.Environment[key] <- value

    match Process.Start(info) with
    | null -> failwith "Could not start the dot process."
    | child ->
        use _ = child

        try
            child.StandardInput.Write(stdin)
            child.StandardInput.Close()

            let finished = child.WaitForExit(int (TimeSpan.FromMinutes(3.0).TotalMilliseconds))

            if not finished then
                try
                    child.Kill(true)
                with _ ->
                    ()

                failwith "The dot smoke run timed out after three minutes."

            let stdout = child.StandardOutput.ReadToEnd()
            let stderr = child.StandardError.ReadToEnd()
            (child.ExitCode, stdout, stderr)
        finally
            try
                child.StandardInput.Dispose()
            with _ ->
                ()

/// Runs dot with piped stdin on one database file and no extra
/// environment, returning exit code, stdout, and stderr.
let private runDot
    (dotDll: string)
    (arguments: string list)
    (stdin: string)
    (workdir: string)
    (dbPath: string)
    : int * string * string =
    runDotEnv dotDll arguments stdin workdir dbPath []

/// Joins script lines into piped stdin with a trailing newline.
let private script (lines: string list) : string =
    String.Join(Environment.NewLine, lines) + Environment.NewLine

/// Creates a fresh temp workdir with its own database file path.
let private freshWorkdir () : string * string =
    let workdir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName())
    Directory.CreateDirectory(workdir) |> ignore
    (workdir, Path.Combine(workdir, "dot.db"))

/// Fails unless the combined output carries the marker.
let private check (output: string) (marker: string) : unit =
    if not (output.Contains(marker, StringComparison.Ordinal)) then
        failwith $"The dot smoke output misses '{marker}'. Full output:{Environment.NewLine}{output}"

/// Fails when the combined output carries the marker.
let private checkAbsent (output: string) (marker: string) : unit =
    if output.Contains(marker, StringComparison.Ordinal) then
        failwith $"The dot smoke output unexpectedly carries '{marker}'. Full output:{Environment.NewLine}{output}"

/// Counts how many times the marker occurs in the combined output.
let private countOccurrences (output: string) (marker: string) : int =
    let mutable count = 0
    let mutable index = output.IndexOf(marker, StringComparison.Ordinal)

    while index >= 0 do
        count <- count + 1
        index <- output.IndexOf(marker, index + marker.Length, StringComparison.Ordinal)

    count

/// Fails unless the marker occurs exactly once in the combined output.
let private checkOnce (output: string) (marker: string) : unit =
    let count = countOccurrences output marker

    if count <> 1 then
        failwith
            $"The dot smoke output carries '{marker}' {count} times, expected once. Full output:{Environment.NewLine}{output}"

/// Reads a file produced by a probe turn, failing with the smoke output
/// when it is missing.
let private probeFile (output: string) (path: string) : string =
    if File.Exists(path) then
        File.ReadAllText(path)
    else
        failwith $"The dot smoke run produced no file at '{path}'. Full output:{Environment.NewLine}{output}"

/// Fails when the combined output carries an ERROR line.
let private checkNoErrors (output: string) : unit =
    if output.Contains("ERROR ", StringComparison.Ordinal) then
        failwith $"The dot smoke output carries an error line. Full output:{Environment.NewLine}{output}"

/// Reads the session id from the run's SESSION line.
let private sessionIdOf (output: string) : string =
    output.Split([| Environment.NewLine |], StringSplitOptions.RemoveEmptyEntries)
    |> Array.tryFind (fun line -> line.StartsWith("SESSION ", StringComparison.Ordinal))
    |> function
        | None -> failwith $"The dot smoke output has no SESSION line. Full output:{Environment.NewLine}{output}"
        | Some line ->
            let parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries)

            if parts.Length < 2 then
                failwith $"The dot SESSION line carries no id. Full output:{Environment.NewLine}{output}"
            else
                parts[1]

[<Fact>]
let ``Repl streams turns serves slash commands and prints usage for unknown`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "hello"
                        "/sessions"
                        "/new second"
                        "/sessions"
                        "/resume 1"
                        "/resume nonsense-id"
                        "/compact"
                        "/abort"
                        "/bogus-command"
                        "again"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "Dot REPL"
        check output "SESSION "
        check output "EVENT "
        check output "RESULT Completed"
        check output "dot scripted answer"
        check output "END-RESULT"
        check output "SESSIONS 1"
        check output "SESSIONS 2"
        check output "RESUMED "
        check output "RESUME-FAILED 'nonsense-id'"
        check output "COMPACT "
        check output "ABORTED"
        check output "UNKNOWN-COMMAND /bogus-command"

        check
            output
            "Commands: /new [title], /sessions, /resume <id-or-index>, /model [provider[/model]], /steer <text>, /follow <text>, /abort, /compact, /tree, /fork <sequence>, /clone, /session, /export <file>, /<template>, /quit."

        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Ask policy suspends for inline permission approval`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot dotDll [ "--scripted"; "--ask" ] (script [ "hello"; "allow"; "/quit" ]) workdir dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "PermissionRequestedEvent tool=scripted-echo"
        check output "PERMISSION tool=scripted-echo"
        check output "PermissionResolvedEvent"
        check output "RESULT Completed"
        check output "dot scripted answer"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Settle deadline reports while the turn keeps running`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        // The slow-turn probe waits a second in the scripted client while
        // the 60ms settle bound lapses: the REPL reports DEADLINE and
        // keeps running into /quit with the turn left to settle alone.
        let exit, stdout, stderr =
            runDot
                dotDll
                [
                    "--scripted"
                    "--wait-minutes"
                    "0.001"
                ]
                (script [ "slow-turn probe"; "/quit" ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "DEADLINE"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Stored sessions resume across processes and list`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let _, firstOut, _ =
            runDot dotDll [ "--scripted" ] (script [ "first hello"; "/quit" ]) workdir dbPath

        let created = sessionIdOf (firstOut + Environment.NewLine)
        check firstOut "RESULT Completed"

        let exit, resumeOut, resumeErr =
            runDot dotDll [ "--scripted"; "--resume"; created ] (script [ "second hello"; "/quit" ]) workdir dbPath

        let resumeOutput = resumeOut + Environment.NewLine + resumeErr

        exit |> should equal 0
        check resumeOutput $"RESUMED {created}"
        check resumeOutput "RESULT Completed"
        check resumeOutput "dot scripted answer"
        checkNoErrors resumeOutput

        let listExit, listOut, listErr =
            runDot dotDll [ "--scripted"; "--sessions" ] "" workdir dbPath

        let listOutput = listOut + Environment.NewLine + listErr

        listExit |> should equal 0
        check listOutput "SESSIONS 1"
        check listOutput created
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Unknown startup resume falls back to a fresh session`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [
                    "--scripted"
                    "--resume"
                    "not-a-session-id"
                ]
                (script [ "/quit" ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "RESUME-FAILED 'not-a-session-id'"
        check output "SESSION "
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Bad flags fail with usage`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let boundExit, boundOut, boundErr =
            runDot dotDll [ "--scripted"; "--wait-minutes"; "0" ] (script [ "/quit" ]) workdir dbPath

        let boundOutput = boundOut + Environment.NewLine + boundErr

        boundExit |> should equal 2
        check boundOutput "positive"

        let unknownExit, unknownOut, unknownErr =
            runDot dotDll [ "--scripted"; "--bogus" ] (script [ "/quit" ]) workdir dbPath

        let unknownOutput = unknownOut + Environment.NewLine + unknownErr

        unknownExit |> should equal 2
        check unknownOutput "Unknown flag '--bogus'"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

// ──────────────────────────────────────────────────────────────────────────
// Coding tools and approval policy (issue 306): the scripted coding
// probes run the dot-local tools over the smoke workdir (the process
// working directory) under the default allow-all policy and the opt-in
// --ask policy.

[<Fact>]
let ``Default policy writes a file with zero friction`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot dotDll [ "--scripted" ] (script [ "coding-write a hello page"; "/quit" ]) workdir dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "RESULT Completed"
        check output "coding-write done"
        checkAbsent output "PERMISSION tool="

        let content = probeFile output (Path.Combine(workdir, "hello.html"))

        content
        |> should equal "<!DOCTYPE html>\n<html>\n<body>\n<h1>hello from dot</h1>\n</body>\n</html>\n"

        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Default policy writes then edits in one turn`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot dotDll [ "--scripted" ] (script [ "coding-edit the page"; "/quit" ]) workdir dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "RESULT Completed"
        check output "coding-edit done"
        checkAbsent output "PERMISSION tool="

        let content = probeFile output (Path.Combine(workdir, "site", "index.html"))

        content |> should equal "<h1>hello, edited</h1>\n"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Ask policy deny runs nothing and the turn continues`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted"; "--ask" ]
                (script
                    [
                        "coding-write a page"
                        "deny"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "PERMISSION tool=write_file"
        check output "decision=Deny"
        check output "RESULT Completed"

        if File.Exists(Path.Combine(workdir, "hello.html")) then
            failwith $"The denied write still landed. Full output:{Environment.NewLine}{output}"

        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Ask policy remembers allow for session`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted"; "--ask" ]
                (script
                    [
                        "coding-two files please"
                        "s"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "decision=AllowForSession"
        check output "RESULT Completed"
        // One prompt for two same-tool calls: the first grant covers the
        // session, so the second call runs silently.
        checkOnce output "PERMISSION tool="

        let first = probeFile output (Path.Combine(workdir, "probe-a.txt"))
        let second = probeFile output (Path.Combine(workdir, "probe-b.txt"))

        first |> should equal "alpha\n"
        second |> should equal "beta\n"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Writes outside the workspace root are rejected`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot dotDll [ "--scripted" ] (script [ "coding-outside escape"; "/quit" ]) workdir dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "RESULT Completed"

        let parent =
            match Path.GetDirectoryName(workdir) with
            | null -> workdir
            | dir -> dir

        let escaped = Path.Combine(parent, "outside-evil.txt")

        if File.Exists(escaped) then
            try
                File.Delete(escaped)
            with _ ->
                ()

            failwith $"The outside-root write landed at '{escaped}'. Full output:{Environment.NewLine}{output}"

        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Exec runs under the default policy`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot dotDll [ "--scripted" ] (script [ "coding-exec version"; "/quit" ]) workdir dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "RESULT Completed"
        check output "coding-exec done"
        checkAbsent output "PERMISSION tool="
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Help names the sandboxing expectation and the ask escape`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot dotDll [ "--help" ] (script [ "/quit" ]) workdir dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 2
        check output "sandbox"
        check output "--ask"
        check output "--mcp"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Missing mcp path fails naming the path`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()
    let missing = Path.Combine(workdir, "nope.json")

    try
        let exit, stdout, stderr =
            runDot dotDll [ "--scripted"; "--mcp"; missing ] (script [ "/quit" ]) workdir dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 1
        check output "does not exist"
        check output missing
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Empty mcp config attaches and stays green`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()
    let mcpPath = Path.Combine(workdir, "mcp.json")

    try
        File.WriteAllText(mcpPath, """{"mcpServers": {}}""")

        let exit, stdout, stderr =
            runDot dotDll [ "--scripted"; "--mcp"; mcpPath ] (script [ "hello"; "/quit" ]) workdir dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "RESULT Completed"
        check output "dot scripted answer"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

// ──────────────────────────────────────────────────────────────────────────
// Provider and model selection (issue 307): dummy env keys register the
// live providers with no network behind them (selection and session open
// only; the REPL quits before any turn runs).

[<Fact>]
let ``Help documents the provider flags and default order`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot dotDll [ "--help" ] (script [ "/quit" ]) workdir dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 2
        check output "--provider"
        check output "--model"
        check output "anthropic, openai, google"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Unknown provider fails naming the known ids`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDotEnv
                dotDll
                [ "--provider"; "nope" ]
                (script [ "/quit" ])
                workdir
                dbPath
                [
                    "ANTHROPIC_API_KEY", "dummy-anthropic"
                    "OPENAI_API_KEY", "dummy-openai"
                ]

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 1
        check output "Unknown provider 'nope'"
        check output "anthropic"
        check output "openai"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Provider without a key names its env var`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDotEnv
                dotDll
                [ "--provider"; "google" ]
                (script [ "/quit" ])
                workdir
                dbPath
                [
                    "ANTHROPIC_API_KEY", "dummy-anthropic"
                ]

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 1
        check output "Unknown provider 'google'"
        check output "GOOGLE_API_KEY"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Unparsable model fails naming the known providers`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDotEnv
                dotDll
                [ "--model"; "noslash" ]
                (script [ "/quit" ])
                workdir
                dbPath
                [
                    "ANTHROPIC_API_KEY", "dummy-anthropic"
                ]

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 1
        check output "noslash"
        check output "known providers: anthropic"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Provider and model flags need values`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let providerExit, providerOut, providerErr =
            runDot dotDll [ "--scripted"; "--provider" ] (script [ "/quit" ]) workdir dbPath

        let providerOutput = providerOut + Environment.NewLine + providerErr

        providerExit |> should equal 2
        check providerOutput "--provider"

        let modelExit, modelOut, modelErr =
            runDot dotDll [ "--scripted"; "--model" ] (script [ "/quit" ]) workdir dbPath

        let modelOutput = modelOut + Environment.NewLine + modelErr

        modelExit |> should equal 2
        check modelOutput "--model"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``One key without a provider pick just works`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDotEnv
                dotDll
                []
                (script [ "/quit" ])
                workdir
                dbPath
                [
                    "ANTHROPIC_API_KEY", "dummy-anthropic"
                ]

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "SESSION "
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Explicit provider pick starts`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDotEnv
                dotDll
                [ "--provider"; "openai" ]
                (script [ "/quit" ])
                workdir
                dbPath
                [
                    "ANTHROPIC_API_KEY", "dummy-anthropic"
                    "OPENAI_API_KEY", "dummy-openai"
                ]

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "SESSION "
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Model lists options and switches mid-session`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "hello"
                        "/model"
                        "/model scripted/round-two"
                        "second hello"
                        "/model nope/x"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "MODEL scripted/scripted"
        check output "MODEL-SWITCHED scripted/round-two applied at once"
        // Both turns ran under one session across the switch: the canned
        // queue advanced through the pre- and post-switch prompts.
        check output "dot scripted answer one"
        check output "dot scripted answer two"
        // The failed switch reports without killing the REPL: the ERROR
        // line is expected here, so no checkNoErrors on this run.
        check output "ERROR Unknown provider 'nope'"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Resume marker names the stored session agent`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        // Open under the default scripted model, switch to round-two, quit:
        // the stored session agent is now round-two.
        let _, firstOut, firstErr =
            runDot dotDll [ "--scripted" ] (script [ "/model scripted/round-two"; "/quit" ]) workdir dbPath

        let firstOutput = firstOut + Environment.NewLine + firstErr
        let created = sessionIdOf firstOut

        check firstOutput "MODEL-SWITCHED scripted/round-two applied at once"

        // Resume in a fresh process defaulting to scripted/scripted: the
        // bare marker must name the stored round-two agent, and the
        // explicit stored ref must report already (attach-time sync).
        let exit, resumeOut, resumeErr =
            runDot
                dotDll
                [ "--scripted"; "--resume"; created ]
                (script
                    [
                        "/model"
                        "/model scripted/round-two"
                        "/quit"
                    ])
                workdir
                dbPath

        let resumeOutput = resumeOut + Environment.NewLine + resumeErr

        exit |> should equal 0
        check resumeOutput $"RESUMED {created}"
        check resumeOutput "MODEL scripted/round-two"
        check resumeOutput "* scripted default scripted"
        check resumeOutput "MODEL already scripted/round-two"
        checkNoErrors resumeOutput
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

// ──────────────────────────────────────────────────────────────────────────
// Steering, follow-ups, tree, and fork (issue 308): every path proves
// itself against the slow slow-steer probe (slow-echo tool, three seconds
// under allow-all), so the piped steering line lands while the turn still
// runs.

[<Fact>]
let ``Steer interrupts the slow turn and starts the new turn`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "slow-steer start"
                        "/steer steered hello"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        // The pre-empted turn settles visibly as Aborted, never silently,
        // then the steering turn completes.
        check output "RESULT Aborted"
        check output "RESULT Completed"
        check output "END-RESULT"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Follow folds into the slow turn without interrupting`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "slow-steer start"
                        "/follow extra context"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        // Inject folds at the next iteration boundary: one settle, Completed.
        check output "RESULT Completed"
        checkAbsent output "RESULT Aborted"
        checkOnce output "RESULT Completed"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Plain input queues behind the slow turn`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "slow-steer start"
                        "second hello"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        // Queue waits for the turn to finish: two settles, both Completed.
        checkAbsent output "RESULT Aborted"

        if countOccurrences output "RESULT Completed" <> 2 then
            failwith $"The dot smoke output misses the queued second settle. Full output:{Environment.NewLine}{output}"

        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Tree lists positions and fork carries the prefix`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "hello"
                        "/tree"
                        "/fork 1"
                        "/tree"
                        "/sessions"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "RESULT Completed"
        check output "TREE "
        check output "FORKED "
        check output "RESUMED "
        check output "SESSIONS 2"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Clone duplicates the active branch`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "hello"
                        "/clone"
                        "/sessions"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "FORKED "
        check output "RESUMED "
        check output "SESSIONS 2"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Abort settles the slow turn visibly`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "slow-steer start"
                        "/abort"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "ABORTED"
        check output "RESULT Aborted"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Compact defers mid-turn and completes idle`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "slow-steer start"
                        "/compact"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "COMPACT deferred"
        check output "RESULT Completed"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Steering commands need text and fork needs a sequence`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "/steer"
                        "/follow"
                        "/fork"
                        "/fork nope"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "ERROR /steer needs text"
        check output "ERROR /follow needs text"
        check output "ERROR /fork needs a sequence"
        check output "is not a number"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

// ──────────────────────────────────────────────────────────────────────────
// Context files, skills, templates, export, and session (issue 309): the
// scripted probes run marker-bearing AGENTS.md/SYSTEM.md through the
// every-turn re-read, the sample review skill through the skill tool, a
// temp .agent/templates file through /name, and the journal through
// /export JSONL/HTML plus /session counts.

[<Fact>]
let ``Context files steer every turn and edits apply on the next turn`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        File.WriteAllText(
            Path.Combine(workdir, "AGENTS.md"),
            "# dot context probe\n\nCTX-AGENTS-ALPHA-309 steering active.\n"
        )

        File.WriteAllText(
            Path.Combine(workdir, "SYSTEM.md"),
            "# dot system probe\n\nCTX-SYSTEM-ONE-309 project prompt.\n"
        )

        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "ctx-check first"
                        "ctx-edit rewrite"
                        "ctx-check second"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        // The first turn saw the files as opened; the rewrite landed
        // through write_file mid-run and the third turn re-read it.
        check output "CTX-SEEN-309 agents=alpha system=one"
        check output "CTX-EDIT-DONE-309"
        check output "CTX-SEEN-309 agents=beta system=one"

        let agents = File.ReadAllText(Path.Combine(workdir, "AGENTS.md"))

        agents.Contains("CTX-AGENTS-BETA-309", StringComparison.Ordinal)
        |> should equal true

        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Skill loads the sample review skill and missing names it`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    // The host log carries the load: the runtime journals no per-call
    // tool events on this path, so the log-only onLoaded line is the
    // host-observable proof the skill tool served the uploaded package.
    let skillLog =
        [
            "Logging__LogLevel__Dot.DotSkills.SkillToolSource", "Information"
        ]

    try
        let loadExit, loadOut, loadErr =
            runDotEnv dotDll [ "--scripted" ] (script [ "skill-load probe"; "/quit" ]) workdir dbPath skillLog

        let loadOutput = loadOut + Environment.NewLine + loadErr

        loadExit |> should equal 0
        check loadOutput "SKILL-LOAD-DONE-309"
        check loadOutput "RESULT Completed"
        check loadOutput "loaded skill 'review'"
        checkNoErrors loadOutput

        let missingExit, missingOut, missingErr =
            runDotEnv dotDll [ "--scripted" ] (script [ "skill-missing probe"; "/quit" ]) workdir dbPath skillLog

        let missingOutput = missingOut + Environment.NewLine + missingErr

        missingExit |> should equal 0
        check missingOutput "SKILL-MISSING-DONE-309"
        check missingOutput "RESULT Completed"
        // The unknown name diagnosed without loading anything.
        checkAbsent missingOutput "loaded skill"
        checkNoErrors missingOutput
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Template expands verbatim and missing lists available`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()
    let templates = Path.Combine(workdir, ".agent", "templates")

    try
        Directory.CreateDirectory(templates) |> ignore
        // The template body carries a probe marker: expansion enqueues it
        // verbatim, so the skill probe firing proves the file text became
        // the prompt.
        File.WriteAllText(Path.Combine(templates, "doskill.md"), "skill-load probe")

        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "/doskill"
                        "/nosuchtemplate309"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "TEMPLATE doskill"
        check output "SKILL-LOAD-DONE-309"
        check output "UNKNOWN-COMMAND /nosuchtemplate309"
        check output "TEMPLATES doskill"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Export writes JSONL and HTML and refuses escape`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot
                dotDll
                [ "--scripted" ]
                (script
                    [
                        "hello"
                        "/export transcript.jsonl"
                        "/export page.html"
                        "/export ../evil.jsonl"
                        "/quit"
                    ])
                workdir
                dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "EXPORTED "
        check output "transcript.jsonl"
        check output "page.html"
        // The escape is refused naming the invalid target; the ERROR line
        // is expected here, so no checkNoErrors on this run.
        check output "ERROR The /export target is invalid"

        let jsonl = probeFile output (Path.Combine(workdir, "transcript.jsonl"))

        for line in jsonl.Split([| '\n' |], StringSplitOptions.RemoveEmptyEntries) do
            check line "$type"

        let html = probeFile output (Path.Combine(workdir, "page.html"))
        check html "<table"
        check html "TurnCompletedEvent"

        let parent =
            match Path.GetDirectoryName(workdir) with
            | null -> workdir
            | dir -> dir

        if File.Exists(Path.Combine(parent, "evil.jsonl")) then
            failwith $"The escaped export landed outside the workdir. Full output:{Environment.NewLine}{output}"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Export refuses symlink escape and allows in-workspace links`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()
    let outsideDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName())

    try
        Directory.CreateDirectory(outsideDir) |> ignore
        Directory.CreateDirectory(Path.Combine(workdir, "real")) |> ignore

        let linksReady =
            try
                Directory.CreateSymbolicLink(Path.Combine(workdir, "link-escape"), outsideDir)
                |> ignore

                Directory.CreateSymbolicLink(Path.Combine(workdir, "link-ok"), Path.Combine(workdir, "real"))
                |> ignore

                true
            with
            | :? UnauthorizedAccessException -> false
            | :? IOException -> false
            | :? PlatformNotSupportedException -> false

        if linksReady then
            let exit, stdout, stderr =
                runDot
                    dotDll
                    [ "--scripted" ]
                    (script
                        [
                            "hello"
                            "/export link-escape/escaped.jsonl"
                            "/export link-ok/ok.jsonl"
                            "/quit"
                        ])
                    workdir
                    dbPath

            let output = stdout + Environment.NewLine + stderr

            exit |> should equal 0
            // The link escape is refused with the inside-workspace error;
            // the ERROR line is expected here, so no checkNoErrors.
            check output "ERROR The /export target must stay inside the working directory"
            // The in-workspace link resolves to its target, so the EXPORTED
            // line names the resolved file, not the link path.
            check output "EXPORTED "
            check output "ok.jsonl"

            if File.Exists(Path.Combine(outsideDir, "escaped.jsonl")) then
                failwith $"The symlinked export landed outside the workdir. Full output:{Environment.NewLine}{output}"

            if File.Exists(Path.Combine(workdir, "link-escape", "escaped.jsonl")) then
                failwith $"The symlinked export landed through the link. Full output:{Environment.NewLine}{output}"

            let ok = probeFile output (Path.Combine(workdir, "link-ok", "ok.jsonl"))

            for line in ok.Split([| '\n' |], StringSplitOptions.RemoveEmptyEntries) do
                check line "$type"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

        try
            Directory.Delete(outsideDir, true)
        with _ ->
            ()

[<Fact>]
let ``Session reports counts and tokens without prices`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()

    try
        let exit, stdout, stderr =
            runDot dotDll [ "--scripted" ] (script [ "hello"; "/session"; "/quit" ]) workdir dbPath

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check output "messages=1 turns=1"
        check output "input-tokens=0 output-tokens=0"
        checkAbsent output "price"
        checkAbsent output "cost"
        checkAbsent output "$"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

// ──────────────────────────────────────────────────────────────────────────
// YAML configuration (issue 328): --print-config smokes prove the four-file
// precedence chain, env and flag wins, ${VAR} expansion, secret masking,
// and startup errors. Every run is keyless: the dump exits before the
// database opens and before any provider registers.

/// Creates a fresh temp config base with an empty dot/ user dir, returning
/// the base (for DOT_CONFIG_HOME) and the user dot dir.
let private freshConfigBase () : string * string =
    let baseDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName())
    let userDot = Path.Combine(baseDir, "dot")
    Directory.CreateDirectory(userDot) |> ignore
    (baseDir, userDot)

/// Writes a YAML file, creating its directory first.
let private writeYaml (path: string) (content: string) : unit =
    match Path.GetDirectoryName(path) with
    | null -> ()
    | dir -> Directory.CreateDirectory(dir) |> ignore

    File.WriteAllText(path, content)

/// Runs dot --print-config with an isolated user config base, returning
/// exit code, stdout, and stderr.
let private runPrintConfig
    (dotDll: string)
    (workdir: string)
    (dbPath: string)
    (configBase: string)
    (extraEnv: (string * string) list)
    (extraArgs: string list)
    : int * string * string =
    runDotEnv
        dotDll
        ([ "--print-config" ] @ extraArgs)
        ""
        workdir
        dbPath
        ((("DOT_CONFIG_HOME", configBase)) :: extraEnv)

/// Parses key=value dump lines into a map, failing on malformed lines.
let private dumpMap (stdout: string) : Map<string, string> =
    stdout.Split([| '\n' |], StringSplitOptions.RemoveEmptyEntries)
    |> Array.choose (fun line ->
        let trimmed = line.Trim().Trim([| '\r' |])

        let index = trimmed.IndexOf('=')

        if index < 0 then
            None
        else
            Some(trimmed.Substring(0, index), trimmed.Substring(index + 1)))
    |> Map.ofArray

/// Reads a dump value, failing with the full stdout when the key is absent.
let private dumpValue (dump: Map<string, string>) (stdout: string) (key: string) : string =
    match Map.tryFind key dump with
    | None -> failwith $"The --print-config dump misses '{key}'. Full output:{Environment.NewLine}{stdout}"
    | Some value -> value

[<Fact>]
let ``PrintConfig shows safe defaults when no files exist`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()
    let configBase, _ = freshConfigBase ()

    try
        let exit, stdout, stderr = runPrintConfig dotDll workdir dbPath configBase [] []

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        // Missing files are never errors: safe hardcoded defaults win.
        let dump = dumpMap stdout

        dumpValue dump stdout "Dot:Provider" |> should equal ""
        dumpValue dump stdout "Dot:Model" |> should equal ""
        dumpValue dump stdout "Dot:Ask" |> should equal "false"
        // The helper pins DOT_DB_PATH, so the env default shows; the
        // workspace defaults to the process working directory.
        dumpValue dump stdout "Dot:DbPath" |> should equal dbPath
        dumpValue dump stdout "Dot:WorkspaceRoot" |> should equal workdir
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

        try
            Directory.Delete(configBase, true)
        with _ ->
            ()

[<Fact>]
let ``PrintConfig proves the file precedence chain with env and flag wins`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()
    let configBase, userDot = freshConfigBase ()
    let projectDot = Path.Combine(workdir, ".dot")

    try
        let userFile = Path.Combine(userDot, "appsettings.yaml")
        let userLocal = Path.Combine(userDot, "appsettings.local.yaml")
        let projectFile = Path.Combine(projectDot, "appsettings.yaml")
        let projectLocal = Path.Combine(projectDot, "appsettings.local.yaml")

        let dumpOf extraEnv extraArgs =
            let exit, stdout, stderr =
                runPrintConfig dotDll workdir dbPath configBase extraEnv extraArgs

            let output = stdout + Environment.NewLine + stderr
            exit |> should equal 0
            (dumpMap stdout, output)

        // User scope alone.
        writeYaml userFile "Dot:\n  Provider: user-prov\n  Model: user-prov/m1\n"

        let dump, _ = dumpOf [] []

        dumpValue dump "" "Dot:Provider" |> should equal "user-prov"
        dumpValue dump "" "Dot:Model" |> should equal "user-prov/m1"

        // User-local beats user.
        writeYaml userLocal "Dot:\n  Provider: userlocal-prov\n"

        let dump, _ = dumpOf [] []

        dumpValue dump "" "Dot:Provider" |> should equal "userlocal-prov"
        dumpValue dump "" "Dot:Model" |> should equal "user-prov/m1"

        // Project beats user-local; project Model lands too.
        writeYaml projectFile "Dot:\n  Provider: proj-prov\n  Model: proj-prov/m2\n"

        let dump, _ = dumpOf [] []

        dumpValue dump "" "Dot:Provider" |> should equal "proj-prov"
        dumpValue dump "" "Dot:Model" |> should equal "proj-prov/m2"

        // Project-local beats project.
        writeYaml projectLocal "Dot:\n  Provider: projlocal-prov\n"

        let dump, _ = dumpOf [] []

        dumpValue dump "" "Dot:Provider" |> should equal "projlocal-prov"

        // Environment beats every file.
        let dump, _ = dumpOf [ "Dot__Provider", "env-prov" ] []

        dumpValue dump "" "Dot:Provider" |> should equal "env-prov"

        // CLI flags beat the environment.
        let dump, _ = dumpOf [ "Dot__Provider", "env-prov" ] [ "--provider"; "flag-prov" ]

        dumpValue dump "" "Dot:Provider" |> should equal "flag-prov"

        // The pinned DOT_DB_PATH env beats a file DbPath, and a file Ask
        // default of true shows until --ask (already true) holds it too.
        writeYaml projectLocal "Dot:\n  Provider: projlocal-prov\n  DbPath: /tmp/file-marker-328.db\n  Ask: true\n"

        let dump, output = dumpOf [] []

        dumpValue dump output "Dot:DbPath" |> should equal dbPath
        dumpValue dump output "Dot:Ask" |> should equal "true"

        let dump, _ = dumpOf [] [ "--ask" ]

        dumpValue dump "" "Dot:Ask" |> should equal "true"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

        try
            Directory.Delete(configBase, true)
        with _ ->
            ()

[<Fact>]
let ``PrintConfig flattens nested maps and sequences`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()
    let configBase, userDot = freshConfigBase ()

    try
        writeYaml
            (Path.Combine(userDot, "appsettings.yaml"))
            "Dot:\n  Provider: user-prov\n  Tags:\n    - alpha\n    - beta\nLegate:\n  Test:\n    Deep: hello-328\n"

        let exit, stdout, stderr = runPrintConfig dotDll workdir dbPath configBase [] []

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0

        let dump = dumpMap stdout

        dumpValue dump stdout "Dot:Tags:0" |> should equal "alpha"
        dumpValue dump stdout "Dot:Tags:1" |> should equal "beta"
        dumpValue dump stdout "Legate:Test:Deep" |> should equal "hello-328"
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

        try
            Directory.Delete(configBase, true)
        with _ ->
            ()

[<Fact>]
let ``PrintConfig masks secret values`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()
    let configBase, userDot = freshConfigBase ()
    let firstSecret = "super-secret-328-ABC"
    let secondSecret = "another-secret-328-XYZ"

    try
        writeYaml
            (Path.Combine(userDot, "appsettings.yaml"))
            $"Dot:\n  Providers:\n    anthropic:\n      ApiKey: {firstSecret}\nLegate:\n  Llm:\n    Providers:\n      openai:\n        ApiKey: {secondSecret}\n"

        let exit, stdout, stderr = runPrintConfig dotDll workdir dbPath configBase [] []

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check stdout "Dot:Providers:anthropic:ApiKey=***"
        check stdout "Legate:Llm:Providers:openai:ApiKey=***"
        checkAbsent output firstSecret
        checkAbsent output secondSecret
        checkNoErrors output
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

        try
            Directory.Delete(configBase, true)
        with _ ->
            ()

[<Fact>]
let ``PrintConfig expands placeholders and fails on unset variables`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()
    let configBase, userDot = freshConfigBase ()

    try
        // A set variable expands inline.
        writeYaml (Path.Combine(userDot, "appsettings.yaml")) "Dot:\n  Model: 'prefix-${DOT_SMOKE_SET_328}-suffix'\n"

        let exit, stdout, stderr =
            runPrintConfig dotDll workdir dbPath configBase [ "DOT_SMOKE_SET_328", "expanded-42" ] []

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0
        check stdout "Dot:Model=prefix-expanded-42-suffix"
        checkNoErrors output

        // An unset variable fails startup naming the variable and the
        // config key, without echoing any value.
        writeYaml
            (Path.Combine(userDot, "appsettings.yaml"))
            "Dot:\n  Model: 'prefix-${DOT_SMOKE_MISSING_328}-suffix'\n"

        let badExit, badOut, badErr = runPrintConfig dotDll workdir dbPath configBase [] []

        let badOutput = badOut + Environment.NewLine + badErr

        badExit |> should equal 1
        check badOutput "DOT_SMOKE_MISSING_328"
        check badOutput "Dot:Model"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

        try
            Directory.Delete(configBase, true)
        with _ ->
            ()

[<Fact>]
let ``Bad YAML fails naming the path`` () =
    let dotDll = sampleDll "Dot" "Dot.dll"
    let workdir, dbPath = freshWorkdir ()
    let configBase, userDot = freshConfigBase ()
    let badPath = Path.Combine(userDot, "appsettings.yaml")

    try
        writeYaml badPath "Dot: [unclosed\n  Provider: nope\n"

        let exit, stdout, stderr = runPrintConfig dotDll workdir dbPath configBase [] []

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 1
        check output badPath
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

        try
            Directory.Delete(configBase, true)
        with _ ->
            ()
