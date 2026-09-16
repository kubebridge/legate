// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LegateCliSmokeTests

open System
open System.Diagnostics
open System.IO
open System.Text
open FsUnit.Xunit
open Legate
open Xunit

// LegateCli smoke test (issue 96): runs the built CLI binary as a
// subprocess on scripted transports (no live keys, no external
// services) through the full loop: prompt, streamed events, permission
// approval, the mcp.json fixture attach with an agent tool call
// (transferred #76 check), /compact, /abort, /agent-stubbed, /new,
// in-process /resume, and a second prompt. A second invocation proves
// --resume against an unknown id fails honestly on the InMemory store.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// Infers Debug/Release from the test assembly's own directory.
let private configurationName () : string =
    let directory = AppContext.BaseDirectory

    if directory.Contains("Release", StringComparison.OrdinalIgnoreCase) then
        "Release"
    else
        "Debug"

/// Locates a built sample DLL, failing with the searched paths.
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

let private writeTemp (directory: string) (name: string) (content: string) : string =
    let path = Path.Combine(directory, name)
    File.WriteAllText(path, content)
    path

let private smokeScript: string =
    String.Join(
        Environment.NewLine,
        [
            "hello"
            "allow"
            "call the fixture"
            "allow"
            "/compact"
            "/abort"
            "/agent my-agent"
            "/new second"
            "/resume 1"
            "again"
            "/quit"
        ]
    )
    + Environment.NewLine

/// Runs the CLI with piped stdin, returning exit code, stdout, stderr.
let private runCli (cliDll: string) (arguments: string list) (stdin: string) (workdir: string) : int * string * string =
    let info = ProcessStartInfo("dotnet")
    info.ArgumentList.Add(cliDll)

    for argument in arguments do
        info.ArgumentList.Add(argument)

    info.RedirectStandardInput <- true
    info.RedirectStandardOutput <- true
    info.RedirectStandardError <- true
    info.UseShellExecute <- false
    info.WorkingDirectory <- workdir
    info.Environment["Logging__LogLevel__Default"] <- "None"
    info.Environment["DOTNET_NOLOGO"] <- "1"

    match Process.Start(info) with
    | null -> failwith "Could not start the LegateCli process."
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

                failwith "The LegateCli smoke run timed out after three minutes."

            let stdout = child.StandardOutput.ReadToEnd()
            let stderr = child.StandardError.ReadToEnd()
            (child.ExitCode, stdout, stderr)
        finally
            try
                child.StandardInput.Dispose()
            with _ ->
                ()

[<Fact>]
let ``Scripted run streams events approves tools calls the fixture and resumes`` () =
    let cliDll = sampleDll "LegateCli" "LegateCli.dll"
    let fixtureDll = sampleDll "McpFixture" "McpFixture.dll"
    let workdir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName())
    Directory.CreateDirectory(workdir) |> ignore

    try
        let fixturePath = fixtureDll.Replace("\\", "/")

        let mcpJson =
            """{"mcpServers": {"fixture": {"command": "dotnet", "args": ["""
            + "\""
            + fixturePath
            + "\"]}}}"

        let mcpPath = writeTemp workdir "mcp.json" mcpJson

        let exit, stdout, stderr =
            runCli cliDll [ "--scripted"; "--mcp"; mcpPath ] smokeScript workdir

        let output = stdout + Environment.NewLine + stderr

        let check (marker: string) : unit =
            if not (output.Contains(marker, StringComparison.Ordinal)) then
                failwith $"The smoke output misses '{marker}'. Full output:{Environment.NewLine}{output}"

        exit |> should equal 0
        check "SESSION "
        check "PermissionRequestedEvent tool=scripted-echo"
        check "PERMISSION tool=scripted-echo"
        check "PermissionResolvedEvent"
        check "RESULT Completed"
        check "scripted answer one"
        check "PermissionRequestedEvent tool=fixture_echo"
        check "PERMISSION tool=fixture_echo"
        check "fixture says hi"
        check "COMPACT not-needed"
        check "ABORTED"
        check "AGENT-STUBBED my-agent"
        check "RESUMED "
        check "scripted answer two"

        if output.Contains("ERROR ", StringComparison.Ordinal) then
            failwith $"The smoke output carries an error line. Full output:{Environment.NewLine}{output}"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()

[<Fact>]
let ``Resume against an unknown id fails honestly on the InMemory store`` () =
    let cliDll = sampleDll "LegateCli" "LegateCli.dll"
    let workdir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName())
    Directory.CreateDirectory(workdir) |> ignore

    try
        let unknown = SessionId.New().ToString()
        let script = "/quit" + Environment.NewLine

        let exit, stdout, stderr =
            runCli cliDll [ "--scripted"; "--resume"; unknown ] script workdir

        let output = stdout + Environment.NewLine + stderr

        exit |> should equal 0

        if not (output.Contains("RESUME-FAILED", StringComparison.Ordinal)) then
            failwith $"The smoke output misses RESUME-FAILED. Full output:{Environment.NewLine}{output}"

        if not (output.Contains("SESSION ", StringComparison.Ordinal)) then
            failwith $"The smoke output misses the fallback SESSION. Full output:{Environment.NewLine}{output}"
    finally
        try
            Directory.Delete(workdir, true)
        with _ ->
            ()
