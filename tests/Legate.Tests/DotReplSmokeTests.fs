// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DotReplSmokeTests

open System
open System.Diagnostics
open System.IO
open FsUnit.Xunit
open Xunit

// Dot REPL smoke test (issues 304, 306): runs the built dot binary as a
// subprocess on scripted transports (no live keys, no external services)
// through the interactive loop over piped stdin: prompt/stream/settle
// with assistant text, /new, store-backed /sessions, /resume continuing
// context across processes on one DOT_DB_PATH file, /abort, /compact,
// /quit, unknown-command usage, the opt-in ask policy with inline
// permission approval, and the settle-deadline report path. The issue 306
// coding probes run the dot-local tools over the smoke workdir (the
// process working directory): writes and edits with zero friction under
// the default policy, deny/session answers under --ask, the outside-root
// fence, exec, --help text, and the --mcp attach. Mirrors the
// LegateCli smoke precedent; each run gets its own temp database file.

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
let private runDot
    (dotDll: string)
    (arguments: string list)
    (stdin: string)
    (workdir: string)
    (dbPath: string)
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
        check output "Commands: /new [title], /sessions, /resume <id-or-index>, /abort, /compact, /quit."
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
