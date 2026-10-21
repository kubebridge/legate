// SPDX-License-Identifier: Apache-2.0
module Pages

open System
open System.IO
open System.Diagnostics
open System.Text.RegularExpressions

let private require condition message =
    if not condition then
        failwith message

let private git root arguments =
    let info = ProcessStartInfo("git")
    info.WorkingDirectory <- root
    info.RedirectStandardOutput <- true
    info.UseShellExecute <- false

    for argument in arguments do
        info.ArgumentList.Add argument

    use child = Process.Start info
    let output = child.StandardOutput.ReadToEnd().Trim()
    child.WaitForExit()
    require (child.ExitCode = 0) $"git failed: {arguments}"
    output

let private validTag (tag: string) =
    Regex.IsMatch(tag, @"^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$")

// Git owns version ordering, including numeric components and stable > suffix.
// Do not substitute lexical sorting or the triggering event's ref here.
let private selectTag root =
    git
        root
        [
            "-c"
            "versionsort.suffix=-"
            "tag"
            "--sort=-version:refname"
        ]
    |> fun text -> text.Split('\n', StringSplitOptions.RemoveEmptyEntries)
    |> Array.tryFind validTag
    |> Option.defaultWith (fun () -> failwith "No qualifying released documentation tag exists.")

let private sourcePair root =
    let main =
        git
            root
            [
                "rev-parse"
                "refs/remotes/origin/main^{commit}"
            ]

    let tag = selectTag root

    let docs =
        git
            root
            [
                "rev-parse"
                $"refs/tags/{tag}^{{commit}}"
            ]

    for sha in [ main; docs ] do
        require (Regex.IsMatch(sha, "^[0-9a-f]{40}$")) "Invalid source SHA."

    main, tag, docs

let resolve root =
    // --prune-tags removes deleted local tags on persistent self-hosted runners.
    git
        root
        [
            "fetch"
            "--force"
            "--prune"
            "--prune-tags"
            "origin"
            "+refs/heads/main:refs/remotes/origin/main"
            "+refs/tags/*:refs/tags/*"
        ]
    |> ignore

    let main, tag, docs = sourcePair root
    let text = $"MAIN_SHA={main}\nDOCS_TAG={tag}\nDOCS_SHA={docs}\n"
    printf "%s" text
    let output = Environment.GetEnvironmentVariable "GITHUB_OUTPUT"

    if not (String.IsNullOrEmpty output) then
        File.AppendAllText(output, text)

let private files root =
    Directory.GetFiles(root, "*", SearchOption.AllDirectories)

let private copyTree source destination =
    for file in files source do
        require
            ((File.GetAttributes(file) &&& FileAttributes.ReparsePoint) = enum 0)
            $"Symlink is not a publication input: {file}"

        let target = Path.Combine(destination, Path.GetRelativePath(source, file))
        Directory.CreateDirectory(Path.GetDirectoryName target) |> ignore
        File.Copy(file, target, true)

let private sameTree source destination =
    for file in files source do
        let target = Path.Combine(destination, Path.GetRelativePath(source, file))

        require
            (File.Exists target && File.ReadAllBytes(file) = File.ReadAllBytes(target))
            $"Publication changed or lost {file}"

// Read only the fixed fields in DocFX's generated ManagedReference items.
// References are excluded: their namespace specs can name nonexistent parents.
let private namespaceMetadata docs =
    let metadata = Path.GetFullPath(Path.Combine(docs, "../api"))
    require (Directory.Exists metadata) "Generated API YAML is required to verify namespace normalization."
    let mutable declared = Set.empty
    let mutable containing = Map.empty

    for file in Directory.GetFiles(metadata, "*.yml") do
        let items = (File.ReadAllText file).Split("\nreferences:")[0]
        let uid = Regex.Match(items, @"(?m)^- uid: ([A-Za-z0-9_.]+)\r?$")

        if uid.Success && Regex.IsMatch(items, @"(?m)^  type: Namespace\r?$") then
            declared <- declared.Add uid.Groups[1].Value

        let ns = Regex.Match(items, @"(?m)^  namespace: ([A-Za-z0-9_.]+)\r?$")

        if ns.Success then
            containing <- containing.Add(Path.GetFileNameWithoutExtension(file) + ".html", ns.Groups[1].Value)

    require (not declared.IsEmpty) "No declared namespaces in generated API metadata."
    declared, containing

let private namespaceRow =
    Regex(@"<h6><strong>Namespace</strong>: (?<body>[^\r\n]*?)</h6>")

let private namespaceAnchor =
    Regex("<a class=\"xref\" href=\"(?<uid>[A-Za-z0-9_.]+)\\.html\">(?<label>[^<]+)</a>")

let private normalizeHtml (declared: Set<string>) (ns: string) apiRoot (html: string) =
    let changes = ResizeArray<string>()

    let transformed =
        namespaceRow.Replace(
            html,
            MatchEvaluator(fun row ->
                namespaceAnchor.Replace(
                    row.Value,
                    MatchEvaluator(fun anchor ->
                        let uid = anchor.Groups["uid"].Value
                        let label = anchor.Groups["label"].Value
                        let absent = not (File.Exists(Path.Combine(apiRoot, uid + ".html")))

                        if
                            ns.StartsWith(uid + ".", StringComparison.Ordinal)
                            && label = (uid.Split('.') |> Array.last)
                            && absent
                            && not (declared.Contains uid)
                        then
                            changes.Add(uid + ".html")
                            $"<span class=\"xref\">{label}</span>"
                        else
                            anchor.Value)
                ))
        )

    transformed, List.ofSeq changes

let private normalizeCopy docs destination =
    let declared, containing = namespaceMetadata docs
    let apiRoot = Path.Combine(docs, "api")

    for ns in declared do
        require (File.Exists(Path.Combine(apiRoot, ns + ".html"))) $"Declared namespace page is missing: {ns}"

    // Inventory every eligible change before modifying the publication copy.
    let inventory =
        Directory.GetFiles(apiRoot, "*.html")
        |> Array.choose (fun file ->
            match containing.TryFind(Path.GetFileName file) with
            | None -> None
            | Some ns ->
                require (declared.Contains ns) $"Containing namespace is undeclared: {ns}"
                let original = File.ReadAllText file
                let updated, changes = normalizeHtml declared ns apiRoot original

                if changes.IsEmpty then
                    None
                else
                    Some(file, updated, changes))

    for file, _, changes in inventory do
        for target in changes do
            printfn "Namespace normalization: api/%s -> %s (undeclared intermediate)" (Path.GetFileName file) target

    let targets =
        inventory
        |> Array.collect (fun (_, _, changes) -> List.toArray changes)
        |> Array.distinct

    printfn
        "Namespace inventory: %d pages, %d anchors, %d distinct missing intermediate targets: %s"
        inventory.Length
        (inventory |> Array.sumBy (fun (_, _, changes) -> changes.Length))
        targets.Length
        (String.Join(", ", targets))

    let allowed =
        inventory
        |> Array.map (fun (file, updated, _) ->
            let relative = Path.GetRelativePath(docs, file)
            let original = File.ReadAllBytes file
            let utf8 = System.Text.UTF8Encoding(false, true)

            let preamble =
                if original.Length >= 3 && original[0..2] = [| 0xEFuy; 0xBBuy; 0xBFuy |] then
                    original[0..2]
                else
                    [||]

            require
                (Array.append preamble (utf8.GetBytes(File.ReadAllText file)) = original)
                $"Unexpected generated HTML encoding: {file}"

            let bytes = Array.append preamble (utf8.GetBytes updated)
            File.WriteAllBytes(Path.Combine(destination, relative), bytes)
            relative, bytes)
        |> Map.ofArray
    // Exact permitted delta: only inventoried anchor wrappers can differ.
    for file in files docs do
        let relative = Path.GetRelativePath(docs, file)

        let expected =
            allowed.TryFind relative
            |> Option.defaultWith (fun () -> File.ReadAllBytes file)

        require (File.ReadAllBytes(Path.Combine(destination, relative)) = expected) $"Unexpected docs delta: {relative}"

    require ((files docs).Length = (files destination).Length) "Documentation file inventory changed."
    // The output is stable when fed back through the same normalizer.
    for file, updated, _ in inventory do
        let ns = containing[Path.GetFileName file]

        require
            (fst (normalizeHtml declared ns (Path.Combine(destination, "api")) updated) = updated)
            "Namespace normalization is not idempotent."

let private checkStructure root =
    for path in
        [
            "index.html"
            "getting-started-cli.html"
            "getting-started-hosted.html"
            "api/index.html"
            "toc.html"
            "index.json"
            "styles/docfx.vendor.min.js"
            "styles/docfx.js"
            "styles/docfx.css"
            "styles/search-worker.min.js"
        ] do
        require (File.Exists(Path.Combine(root, path))) $"Incomplete DocFX output: {path}"

    require (Directory.GetFiles(Path.Combine(root, "api"), "Legate*.html").Length > 0) "No generated API reference."

let private checkDocs root =
    checkStructure root
    // Resolve URLs as they will be served, not as filesystem-relative strings.
    // Root escapes would silently point at the showcase after nesting DocFX.
    let origin = Uri("https://legate.kubebridge.com/docs/")
    let mutable checkedLinks = 0

    for file in Directory.GetFiles(root, "*.html", SearchOption.AllDirectories) do
        let page = Uri(origin, Path.GetRelativePath(root, file).Replace('\\', '/'))
        let html = File.ReadAllText file

        for matched in Regex.Matches(html, "(?:href|src|data-src)=\"([^\"]+)\"") do
            let link = System.Net.WebUtility.HtmlDecode matched.Groups[1].Value
            let target = Uri(page, link)

            if target.Host = origin.Host && target.Scheme = "https" then
                require
                    (target.AbsolutePath.StartsWith("/docs/", StringComparison.Ordinal))
                    $"DocFX link escapes /docs/: {file}: {link}"

                let relative =
                    Uri.UnescapeDataString(target.AbsolutePath.Substring("/docs/".Length))

                let path = Path.Combine(root, relative)
                require (File.Exists path || Directory.Exists path) $"Missing DocFX link/resource: {file}: {link}"
                checkedLinks <- checkedLinks + 1

    printfn "DocFX prefix check passed (%d local HTML links/resources)." checkedLinks

let compose showcase docs destination =
    require (Directory.Exists showcase && Directory.Exists docs) "Both showcase and documentation inputs are required."

    require
        (not (
            Directory.EnumerateFileSystemEntries(showcase)
            |> Seq.exists (fun path -> String.Equals(Path.GetFileName path, "docs", StringComparison.OrdinalIgnoreCase))
        ))
        "Showcase conflicts with reserved /docs/ path."

    require
        ((File.ReadAllText(Path.Combine(showcase, "index.html")))
            .Contains("Run Millions of Agents on a single box | Legate"))
        "Original showcase title missing."

    checkStructure docs
    // Fixed build output only. Never delete an input, checkout, or live site.
    let fullDestination = Path.GetFullPath destination

    for input in [ showcase; docs ] do
        let fullInput = Path.GetFullPath input

        require
            (not (
                fullInput.StartsWith(
                    fullDestination + string Path.DirectorySeparatorChar,
                    StringComparison.OrdinalIgnoreCase
                )
             )
             && fullInput <> fullDestination)
            "Output must not contain an input."

    if Directory.Exists destination then
        Directory.Delete(destination, true)

    Directory.CreateDirectory destination |> ignore
    copyTree showcase destination
    copyTree docs (Path.Combine(destination, "docs"))
    normalizeCopy docs (Path.Combine(destination, "docs"))
    checkDocs (Path.Combine(destination, "docs"))
    sameTree showcase destination

    printfn
        "Complete Pages artifact passed (%d showcase files, %d docs files): %s"
        (files showcase).Length
        (files docs).Length
        destination

let private expectFailure action =
    let failed =
        try
            action ()
            false
        with _ ->
            true

    require failed "Expected fail-closed check to reject invalid input."

let private deleteFixture path =
    if Directory.Exists path then
        // Git objects are read-only on Windows.
        for file in files path do
            File.SetAttributes(file, FileAttributes.Normal)

        Directory.Delete(path, true)

let private namespaceRegressions temp publishedDocs =
    let api = Path.Combine(publishedDocs, "api")
    let fixtureApi = Path.Combine(temp, "namespace-fixture")
    Directory.CreateDirectory fixtureApi |> ignore

    for name in [ "Legate"; "Legate.Workspace.Docker" ] do
        File.WriteAllText(Path.Combine(fixtureApi, name + ".html"), "exists")

    let declared = Set.ofList [ "Legate"; "Legate.Workspace.Docker" ]

    let row =
        "<h6><strong>Namespace</strong>: <a class=\"xref\" href=\"Legate.html\">Legate</a>.<a class=\"xref\" href=\"Legate.Workspace.html\">Workspace</a>.<a class=\"xref\" href=\"Legate.Workspace.Docker.html\">Docker</a></h6>"

    let expected =
        row.Replace(
            "<a class=\"xref\" href=\"Legate.Workspace.html\">Workspace</a>",
            "<span class=\"xref\">Workspace</span>"
        )

    let normalized, changes =
        normalizeHtml declared "Legate.Workspace.Docker" fixtureApi row

    require
        (normalized = expected && changes = [ "Legate.Workspace.html" ])
        "Observed breadcrumb treatment changed labels or valid anchors."

    require
        (normalizeHtml declared "Legate.Workspace.Docker" fixtureApi normalized = (normalized, []))
        "Already-normalized input changed."

    require
        (fst (normalizeHtml (declared.Add "Legate.Workspace") "Legate.Workspace.Docker" fixtureApi row) = row)
        "Missing declared namespace must not be normalized."

    require
        (fst (normalizeHtml declared "Legate.Workspace" fixtureApi row) = row)
        "Missing leaf namespace must not be normalized."

    let plain = "<p><a class=\"xref\" href=\"Legate.Workspace.html\">Workspace</a></p>"

    require
        (fst (normalizeHtml declared "Legate.Workspace.Docker" fixtureApi plain) = plain)
        "Non-namespace content changed."

    let cleanApi = Path.Combine(temp, "defect-free")
    Directory.CreateDirectory cleanApi |> ignore

    for name in
        [
            "Legate"
            "Legate.Workspace"
            "Legate.Workspace.Docker"
        ] do
        File.WriteAllText(Path.Combine(cleanApi, name + ".html"), "exists")

    require (normalizeHtml declared "Legate.Workspace.Docker" cleanApi row = (row, [])) "Valid parent link changed."
    let probe = Path.Combine(api, "publication-regression.html")
    // These are strict final-output checks, not calls to the normalizer.
    for html in
        [
            plain.Replace("Legate.Workspace.html", "Missing.Namespace.html")
            row.Replace("Legate.Workspace.html", "Missing.Namespace.html")
            "<a href=\"/index.html\">escape</a>"
            "<script src=\"missing.js\"></script>"
            "<link href=\"missing.css\">"
        ] do
        File.WriteAllText(probe, html)
        expectFailure (fun () -> checkDocs publishedDocs)
        File.Delete probe

    for relative in
        [
            "api/Legate.Workspace.Docker.html"
            "styles/docfx.css"
            "styles/docfx.js"
            "styles/search-worker.min.js"
            "index.json"
        ] do
        let file = Path.Combine(publishedDocs, relative)
        let bytes = File.ReadAllBytes file
        File.Delete file

        try
            expectFailure (fun () -> checkDocs publishedDocs)
        finally
            File.WriteAllBytes(file, bytes)

    checkDocs publishedDocs

    printfn
        "Namespace regressions passed: exact wrappers/labels, valid parents, declared/leaf/outside-row failures, CSS/JS/search failures, prefix escape, defect-free input and idempotence."

let regress root docs =
    let inputBytes = files docs |> Array.map (fun file -> file, File.ReadAllBytes file)
    let temp = Path.Combine(root, "artifacts/pages-regression")
    deleteFixture temp
    Directory.CreateDirectory temp |> ignore
    let repo = Path.Combine(temp, "repo")
    Directory.CreateDirectory repo |> ignore
    let g args = git repo args
    g [ "init" ] |> ignore
    // Identity is per-command, not a change to the user's git configuration.
    let commit (name: string) =
        File.WriteAllText(Path.Combine(repo, "source"), name)
        g [ "add"; "source" ] |> ignore

        g
            [
                "-c"
                "user.name=Pages test"
                "-c"
                "user.email=pages@example.invalid"
                "commit"
                "-m"
                name
            ]
        |> ignore

        g [ "rev-parse"; "HEAD" ]

    let first = commit "main one"

    g
        [
            "update-ref"
            "refs/remotes/origin/main"
            first
        ]
    |> ignore

    g [ "tag"; "not-a-version" ] |> ignore
    expectFailure (fun () -> selectTag repo |> ignore)

    for tag in
        [
            "v1.2.0"
            "v1.10.0-alpha.2"
            "v1.10.0-alpha.10"
        ] do
        g [ "tag"; tag ] |> ignore

    require (selectTag repo = "v1.10.0-alpha.10") "Numeric/prerelease Git ordering changed."
    g [ "tag"; "v1.10.0" ] |> ignore
    require (selectTag repo = "v1.10.0") "Stable must outrank its suffixes."
    let second = commit "main two and release"

    g
        [
            "update-ref"
            "refs/remotes/origin/main"
            second
        ]
    |> ignore

    g
        [
            "-c"
            "user.name=Pages test"
            "-c"
            "user.email=pages@example.invalid"
            "tag"
            "-a"
            "v2.0.0"
            "-m"
            "release"
            "--"
            second
        ]
    |> ignore
    // Simulates requests acquiring the lock in either order. The request ref
    // is intentionally not an input to production sourcePair.
    for requests in
        [
            [ "main"; "v2.0.0" ]
            [ "v2.0.0"; "main" ]
            [ "v1.2.0"; "old manual"; "v2.0.0" ]
        ] do
        for _ in requests do
            require (sourcePair repo = (second, "v2.0.0", second)) "Delayed request downgraded sources."

    g [ "tag"; "v0.9.0"; first ] |> ignore
    require (sourcePair repo = (second, "v2.0.0", second)) "Backfilled tag downgraded docs."

    let third = commit "main update before release"

    g
        [
            "update-ref"
            "refs/remotes/origin/main"
            third
        ]
    |> ignore

    require (sourcePair repo = (third, "v2.0.0", second)) "Main update lost released docs."
    g [ "tag"; "v2.1.0"; third ] |> ignore
    require (sourcePair repo = (third, "v2.1.0", third)) "Release after main lost showcase."
    let fourth = commit "release before main update"
    g [ "tag"; "v2.2.0"; fourth ] |> ignore
    require (sourcePair repo = (third, "v2.2.0", fourth)) "Release used event showcase instead of current main."
    let fifth = commit "main update after release"

    g
        [
            "update-ref"
            "refs/remotes/origin/main"
            fifth
        ]
    |> ignore

    for _ in
        [
            "old tag request"
            "old main request"
            "latest manual request"
        ] do
        require (sourcePair repo = (fifth, "v2.2.0", fourth)) "Out-of-order execution lost latest source pair."

    let destination = Path.Combine(temp, "site")
    let showcase = Path.Combine(root, "site")
    compose showcase docs destination
    File.WriteAllText(Path.Combine(destination, "stale.html"), "must disappear")
    compose showcase docs destination
    require (not (File.Exists(Path.Combine(destination, "stale.html")))) "Stale publication survived reconstruction."
    let conflict = Path.Combine(temp, "conflict")
    copyTree showcase conflict
    File.WriteAllText(Path.Combine(conflict, "docs"), "collision")
    expectFailure (fun () -> compose conflict docs destination)
    let incomplete = Path.Combine(temp, "incomplete")
    Directory.CreateDirectory incomplete |> ignore
    expectFailure (fun () -> compose showcase incomplete destination)
    sameTree showcase destination
    checkDocs (Path.Combine(destination, "docs"))
    namespaceRegressions temp (Path.Combine(destination, "docs"))

    for file, bytes in inputBytes do
        require (File.ReadAllBytes file = bytes) $"Generated input evidence changed: {file}"

    let workflows = Path.Combine(root, ".github/workflows")

    let owners =
        Directory.GetFiles(workflows, "*.yml")
        |> Array.filter (fun path -> (File.ReadAllText path).Contains("actions/deploy-pages@"))

    require
        (owners.Length = 1 && Path.GetFileName owners[0] = "pages.yml")
        "Pages must have exactly one deployment owner."

    let workflow = File.ReadAllText owners[0]

    for marker in
        [
            "group: pages"
            "cancel-in-progress: false"
            "tags: [\"v*\"]"
            "-t ResolvePages"
            "-t ComposePages"
            "path: artifacts/pages"
        ] do
        require (workflow.Contains marker) $"Missing Pages workflow contract: {marker}"

    printfn
        "Pages regressions passed: ordering, stable/prerelease, missing/invalid tags, sequential/delayed request model, backfill, clean reconstruction, collisions, incomplete docs, single non-cancelling owner. (Not a live concurrency exercise.)"

    deleteFixture temp
