// SPDX-License-Identifier: Apache-2.0
#r "nuget:Fake.Core.Target, 5.23"
#r "nuget:Fake.IO.FileSystem, 5.23"

open Fake.Core
open Fake.IO

#load ".build/Helpers.fs"
open Helpers

initializeContext ()

let rootPath = Path.getFullName "."
let solution = "Legate.slnx"

Target.create "Clean" (fun _ -> run dotnet [ "clean"; solution ] rootPath)

Target.create "Restore" (fun _ ->
    run dotnet [ "tool"; "restore" ] rootPath
    run dotnet [ "restore"; solution ] rootPath)

Target.create "Build" (fun _ -> run dotnet [ "build"; solution; "--no-restore" ] rootPath)

// Runs every test project in the solution.
Target.create "Test" (fun _ -> run dotnet [ "test"; solution; "--no-build" ] rootPath)

// The single source of truth for the F# formatting scope. Both Format and
// CheckFormat read this list, and CI invokes the targets rather than restating
// the paths, so adding a tree here is the only edit a new scope needs.
let fantomasPaths =
    [
        "src"
        "tests"
        "samples"
        "build.fsx"
    ]

let existingFantomasPaths () =
    fantomasPaths
    |> List.filter (fun path ->
        let full = Path.combine rootPath path
        System.IO.Directory.Exists full || System.IO.File.Exists full)

let runFantomas extraArgs =
    run dotnet [ "tool"; "restore" ] rootPath
    run dotnet ([ "fantomas" ] @ extraArgs @ existingFantomasPaths ()) rootPath

Target.create "Format" (fun _ -> runFantomas [])

// Non-mutating gate. fantomas exits 99 when a file needs formatting and
// Helpers.createProcess pipes through CreateProcess.ensureExitCode, so drift
// fails the target without extra handling.
Target.create "CheckFormat" (fun _ -> runFantomas [ "--check" ])

// The single source of truth for the Apache 2.0 SPDX header scope. CI calls
// this target rather than restating the paths, so adding a tree here is the
// only edit a new scope needs.
let headerRoots = [ "src"; "tests"; "samples"; ".build" ]

let headerExtensions = Set.ofList [ ".fs"; ".fsi"; ".fsx" ]

let generatedDirs = Set.ofList [ "obj"; "bin"; ".git" ]

let isGeneratedPath (path: string) =
    path.Split(
        [|
            System.IO.Path.DirectorySeparatorChar
            System.IO.Path.AltDirectorySeparatorChar
        |]
    )
    |> Array.exists generatedDirs.Contains

let headerCandidates () =
    let fromRoot root =
        System.IO.Directory.EnumerateFiles(root, "*", System.IO.SearchOption.AllDirectories)
        |> Seq.filter (fun f -> headerExtensions.Contains(System.IO.Path.GetExtension f))
        |> Seq.filter (isGeneratedPath >> not)
        |> Seq.toList

    let roots =
        headerRoots
        |> List.choose (fun rel ->
            let full = Path.combine rootPath rel
            if System.IO.Directory.Exists full then Some full else None)

    Path.combine rootPath "build.fsx" :: List.collect fromRoot roots

let filesMissingSpdxHeader () =
    headerCandidates ()
    |> List.filter (fun file ->
        System.IO.File.ReadAllText file
        |> fun text -> text.Contains "SPDX-License-Identifier: Apache-2.0" |> not)
    |> List.sort

// Fails the build when any F# source file lacks the Apache 2.0 SPDX header.
Target.create "CheckHeaders" (fun _ ->
    match filesMissingSpdxHeader () with
    | [] -> printfn "All source files carry the SPDX header."
    | missing ->
        missing |> List.iter (printfn "Missing SPDX header: %s")
        failwithf "%d file(s) missing the SPDX-License-Identifier: Apache-2.0 header." missing.Length)

// Smoke-runs both samples on scripted transports (no keys, no network):
// Headless proves one prompt, its exit code, and its signed webhook, and
// CSharpHost proves the C# facade path with a permission reply. Runs on the
// Debug binaries the Build target produced, so it depends on Build.
Target.create "SmokeSamples" (fun _ ->
    run
        dotnet
        [
            "run"
            "--project"
            "samples/Headless/Headless.fsproj"
            "--no-build"
            "--"
            "--scripted"
        ]
        rootPath

    run
        dotnet
        [
            "run"
            "--project"
            "samples/CSharpHost/CSharpHost.csproj"
            "--no-build"
            "--"
            "--scripted"
        ]
        rootPath)

// Produces Release NuGet packages for every packable project into ./artifacts.
// Builds in Release itself, so it does not depend on the Debug Build target.
Target.create "Pack" (fun _ ->
    run
        dotnet
        [
            "pack"
            solution
            "--configuration"
            "Release"
            "--output"
            "artifacts"
        ]
        rootPath)

// Builds the DocFX documentation site from Docs/docfx.json into Docs/_site.
// Builds the solution in Release first so the DLL+XML metadata sources
// exist, then runs docfx (both through the pinned local tools, so this
// target starts with a tool restore like Restore does).
Target.create "Docs" (fun _ ->
    run dotnet [ "tool"; "restore" ] rootPath

    run
        dotnet
        [
            "build"
            solution
            "--configuration"
            "Release"
        ]
        rootPath

    run dotnet [ "docfx"; "Docs/docfx.json" ] rootPath)

open Fake.Core.TargetOperators

"Clean" ==> "Restore" ==> "Build" ==> "Test"

"Build" ==> "SmokeSamples"

"Restore" ==> "Pack"

Target.runOrDefaultWithArguments "Build"
