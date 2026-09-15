// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.IO
open System.Reflection
open System.Runtime.InteropServices
open System.Text
open System.Text.RegularExpressions
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Shared implementation behind BuiltinSearchTools.CreateGlobTool and
// CreateGrepTool: the result cap, the .gitignore subset filter, and the
// confined enumeration under WorkspaceRoot.Path. Internal so the public
// surface stays the factories plus their name and cap constants.
module internal SearchInternals =

    /// The name the model calls the glob tool by, validated once at factory time.
    let globToolName = "glob"

    /// The name the model calls the grep tool by, validated once at factory time.
    let grepToolName = "grep"

    /// The result cap when the model omits max_results.
    let defaultMaxResults = 100

    /// The upper bound the model may request through max_results.
    let maxMaxResults = 1000

    /// The model-usable contract for glob: pattern shape, the gitignore
    /// subset, the cap, and the relative-path results.
    let globDescription =
        "List workspace files matching a glob pattern (glob). "
        + "Patterns support * (any run inside one path segment), ? (one character inside one segment), "
        + "and ** (any depth, as in src/**/*.fs). A pattern without a slash matches the file name at any depth; "
        + "a pattern with a slash matches the whole workspace-relative path. "
        + "The workspace .gitignore is respected when a .gitignore file is present at the workspace root "
        + "(supported subset: blank and # comment lines, *, ?, **, a trailing / for directories, and ! negation in file order); "
        + "with no .gitignore file nothing is filtered. "
        + "Returns slash-separated workspace-relative paths, at most max_results entries "
        + "(default 100, between 1 and 1000), followed by a truncated/total/returned footer so the model can narrow. "
        + "Requires a path-exposing workspace runtime: runtimes without a root path fail instead of falling back."

    /// The model-usable contract for grep: regex shape, the file filter, the
    /// gitignore subset, the cap, and the match lines.
    let grepDescription =
        "Search workspace file contents with a .NET regular expression (grep), one match per line. "
        + "file_filter is an optional glob (*, ?, **) limiting which files are searched, in the same shape as the glob tool; "
        + "omit it or pass null to search every file. "
        + "The workspace .gitignore is respected exactly as the glob tool documents it. "
        + "Returns path:line: text triples with slash-separated workspace-relative paths, at most max_results matches "
        + "(default 100, between 1 and 1000), followed by a truncated/total/returned footer so the model can narrow. "
        + "Requires a path-exposing workspace runtime: runtimes without a root path fail instead of falling back."

    /// Resolves the caller's max_results to an effective cap, failing loudly
    /// outside 1..1000. The factory registers a schema default of 100, so an
    /// omitted max_results already arrives as 100.
    let resolveMaxResults (toolName: string) (max_results: int) : int =
        if max_results < 1 || max_results > maxMaxResults then
            raise (ToolException(toolName, "max_results must be between 1 and 1000."))
        else
            max_results

    /// Returns the workspace's exposed root as an absolute path, or fails
    /// with a typed error when the runtime exposes none. There is never an
    /// Exec fallback: null-Path runtimes cannot enumerate.
    let rootOrThrow (toolName: string) (workspace: IWorkspace) : string =
        if isNull (box workspace) then
            raise (ArgumentNullException(nameof workspace))

        let root = workspace.Root

        if isNull (box root) then
            raise (
                ToolException(
                    toolName,
                    "The workspace does not expose a root path; this tool requires a path-exposing runtime."
                )
            )

        match root.Path with
        | null ->
            raise (
                ToolException(
                    toolName,
                    "The workspace does not expose a root path; this tool requires a path-exposing runtime."
                )
            )
        | path -> Path.GetFullPath path

    // ────────────────────────────────────────────────────────────────────
    // Glob and gitignore matching

    /// One parsed .gitignore line: whether it re-includes, and the anchored
    /// matcher for the workspace-relative slash-separated path.
    type IgnoreRule = { Negated: bool; Expression: Regex }

    /// Translates a glob body (*, ?, **) to a regex fragment. A double star
    /// followed by a slash matches zero or more directories so **/x also
    /// matches x at the root; any other double star matches anything.
    let translateBody (body: string) : string =
        let builder = StringBuilder()
        let mutable i = 0

        while i < body.Length do
            if body[i] = '*' && i + 1 < body.Length && body[i + 1] = '*' then
                if i + 2 < body.Length && body[i + 2] = '/' then
                    builder.Append("(.*/)?") |> ignore
                    i <- i + 3
                else
                    builder.Append(".*") |> ignore
                    i <- i + 2
            elif body[i] = '*' then
                builder.Append("[^/]*") |> ignore
                i <- i + 1
            elif body[i] = '?' then
                builder.Append("[^/]") |> ignore
                i <- i + 1
            else
                builder.Append(Regex.Escape(string body[i])) |> ignore
                i <- i + 1

        builder.ToString()

    /// Translates a glob body to a full regex body, giving a trailing /**
    /// the directory-and-contents meaning (src/** matches src itself).
    let bodyToRegex (body: string) : string =
        if body.EndsWith("/**", StringComparison.Ordinal) then
            translateBody (body.Substring(0, body.Length - 3)) + "(/.*)?"
        else
            translateBody body

    /// Parses one .gitignore line to a rule, or None for blank lines,
    /// comments, and lines that carry no pattern. Supported subset: *,
    /// ?, **, a trailing / for directories, and ! negation in file order.
    let tryParseRule (line: string) : IgnoreRule option =
        if line.Trim() = "" then
            None
        elif line.StartsWith("#", StringComparison.Ordinal) then
            None
        else
            let mutable pattern = line
            let mutable negated = false

            if pattern.StartsWith("!", StringComparison.Ordinal) then
                negated <- true
                pattern <- pattern.Substring(1)

            let mutable dirOnly = false

            if pattern.EndsWith("/", StringComparison.Ordinal) then
                dirOnly <- true
                pattern <- pattern.TrimEnd('/')

            if pattern = "" then
                None
            else
                if pattern.StartsWith("/", StringComparison.Ordinal) then
                    pattern <- pattern.Substring(1)

                let hasSlash = pattern.Contains("/")
                let core = bodyToRegex pattern

                let regexText =
                    if hasSlash then
                        if dirOnly then
                            sprintf "^%s(/.*)?$" core
                        else
                            sprintf "^%s$" core
                    elif dirOnly then
                        sprintf "(^|.*/)%s(/.*)?$" core
                    else
                        sprintf "(^|.*/)%s$" core

                Some
                    {
                        Negated = negated
                        Expression = Regex(regexText, RegexOptions.CultureInvariant)
                    }

    /// Loads the .gitignore rules at the workspace root, in file order, or
    /// an empty list when no .gitignore file is present: absent means no
    /// filtering. An unreadable file degrades to no filtering.
    let loadRules (root: string) : IgnoreRule list =
        let file = Path.Combine(root, ".gitignore")

        if not (File.Exists file) then
            []
        else
            try
                File.ReadAllLines(file, Encoding.UTF8)
                |> Array.toList
                |> List.choose tryParseRule
            with
            | :? IOException -> []
            | :? UnauthorizedAccessException -> []

    /// Reports whether a workspace-relative slash-separated path is ignored:
    /// every matching rule flips the verdict in file order, so the last
    /// match wins and ! re-includes.
    let isIgnored (rules: IgnoreRule list) (relative: string) : bool =
        let mutable ignored = false

        for rule in rules do
            if rule.Expression.IsMatch(relative) then
                ignored <- not rule.Negated

        ignored

    /// Compiles a search glob (*, ?, **) to an anchored matcher. A pattern
    /// without a slash matches the file name at any depth; a pattern with a
    /// slash matches the whole workspace-relative path.
    let compileGlob (pattern: string) : Regex =
        let core = bodyToRegex pattern

        let regexText =
            if pattern.Contains("/") then
                sprintf "^%s$" core
            else
                sprintf "(^|.*/)%s$" core

        Regex(regexText, RegexOptions.CultureInvariant)

    // ────────────────────────────────────────────────────────────────────
    // Enumeration and result shaping

    /// Enumerates every file under the root as a sorted slash-separated
    /// workspace-relative path. Entries that would escape the root are
    /// dropped so enumeration stays confined.
    let enumerateRelativeFiles (root: string) : string list =
        Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories)
        |> Seq.map (fun full -> Path.GetRelativePath(root, full).Replace(Path.DirectorySeparatorChar, '/'))
        |> Seq.filter (fun relative -> not (relative.StartsWith("..", StringComparison.Ordinal)))
        |> Seq.sort
        |> List.ofSeq

    /// The cap footer every search result carries: whether the cap cut the
    /// list, the total observed, and the count returned.
    let footer (truncated: bool) (total: int) (returned: int) : string =
        sprintf "truncated: %b, total: %d, returned: %d" truncated total returned

    /// Runs one glob invocation: confined enumeration, pattern match, the
    /// shared gitignore filter, and the shared cap.
    let globAsync
        (workspace: IWorkspace)
        (pattern: string)
        (max_results: int)
        (cancellationToken: CancellationToken)
        : Task<string> =
        task {
            if isNull (box pattern) || pattern = "" then
                raise (ToolException(globToolName, "The glob pattern must not be null or empty."))

            let limit = resolveMaxResults globToolName max_results
            let root = rootOrThrow globToolName workspace
            cancellationToken.ThrowIfCancellationRequested()

            let rules = loadRules root
            let matcher = compileGlob pattern

            let matched =
                enumerateRelativeFiles root
                |> List.filter (fun relative -> matcher.IsMatch(relative) && not (isIgnored rules relative))

            let returned = matched |> List.truncate limit

            let lines =
                if returned.IsEmpty then
                    "(no matches)"
                else
                    String.concat "\n" returned

            return sprintf "%s\n%s" lines (footer (matched.Length > limit) matched.Length returned.Length)
        }

    /// Runs one grep invocation: confined enumeration, the shared gitignore
    /// and file filters, then streaming line reads with the regex and the
    /// shared cap over matching lines.
    let grepAsync
        (workspace: IWorkspace)
        (pattern: string)
        (file_filter: string)
        (max_results: int)
        (cancellationToken: CancellationToken)
        : Task<string> =
        task {
            if isNull (box pattern) || pattern = "" then
                raise (ToolException(grepToolName, "The search pattern must not be null or empty."))

            let expression =
                try
                    Regex(pattern, RegexOptions.CultureInvariant)
                with :? ArgumentException ->
                    raise (ToolException(grepToolName, "The search pattern is not a valid regular expression."))

            let limit = resolveMaxResults grepToolName max_results
            let root = rootOrThrow grepToolName workspace
            cancellationToken.ThrowIfCancellationRequested()

            let rules = loadRules root

            let fileMatcher =
                if isNull (box file_filter) || file_filter = "" then
                    None
                else
                    Some(compileGlob file_filter)

            let candidates =
                enumerateRelativeFiles root
                |> List.filter (fun relative -> not (isIgnored rules relative))
                |> List.filter (fun relative ->
                    match fileMatcher with
                    | None -> true
                    | Some matcher -> matcher.IsMatch(relative))

            let matches = ResizeArray<string>()
            let mutable total = 0

            for relative in candidates do
                cancellationToken.ThrowIfCancellationRequested()

                let full =
                    Path.GetFullPath(Path.Combine(root, relative.Replace('/', Path.DirectorySeparatorChar)))

                if full.StartsWith(root + string Path.DirectorySeparatorChar, StringComparison.Ordinal) then
                    try
                        let mutable lineNumber = 0

                        for line in File.ReadLines(full, Encoding.UTF8) do
                            lineNumber <- lineNumber + 1

                            if expression.IsMatch(line) then
                                total <- total + 1

                                if matches.Count < limit then
                                    matches.Add(sprintf "%s:%d: %s" relative lineNumber line)
                    with
                    | :? IOException -> ()
                    | :? UnauthorizedAccessException -> ()

            let lines =
                if matches.Count = 0 then
                    "(no matches)"
                else
                    String.concat "\n" matches

            return sprintf "%s\n%s" lines (footer (total > limit) total matches.Count)
        }

/// The per-workspace invoker behind the glob AIFunction. An instance method
/// (not a lambda) so the max_results default of 100 survives on the
/// MethodInfo the factory registers: the model may omit max_results.
type internal GlobInvoker(workspace: IWorkspace) =

    member _.InvokeAsync
        (
            pattern: string,
            [<Optional; DefaultParameterValue(100)>] max_results: int,
            cancellationToken: CancellationToken
        ) : Task<string> =
        SearchInternals.globAsync workspace pattern max_results cancellationToken

/// The per-workspace invoker behind the grep AIFunction. An instance method
/// (not a lambda) so the file_filter null default and the max_results
/// default of 100 survive on the MethodInfo the factory registers.
type internal GrepInvoker(workspace: IWorkspace) =

    member _.InvokeAsync
        (
            pattern: string,
            [<Optional; DefaultParameterValue("")>] file_filter: string,
            [<Optional; DefaultParameterValue(100)>] max_results: int,
            cancellationToken: CancellationToken
        ) : Task<string> =
        SearchInternals.grepAsync workspace pattern file_filter max_results cancellationToken

/// Factories for the glob and grep built-in tools, bound per turn to the session's workspace.
[<Sealed; AbstractClass>]
type BuiltinSearchTools =

    /// The default result cap when the model omits max_results.
    static member DefaultMaxResults: int = SearchInternals.defaultMaxResults

    /// The upper bound the model may request through max_results.
    static member MaxMaxResults: int = SearchInternals.maxMaxResults

    /// The name the model calls the glob tool by.
    static member GlobToolName: string = SearchInternals.globToolName

    /// The name the model calls the grep tool by.
    static member GrepToolName: string = SearchInternals.grepToolName

    /// Builds the glob tool over the given workspace: confined file-name
    /// search under the workspace's exposed root path with the shared
    /// gitignore filter and result cap.
    /// <param name="workspace">The bound workspace the tool enumerates.</param>
    /// <returns>The glob tool offered to the model.</returns>
    static member CreateGlobTool(workspace: IWorkspace) : AITool =
        ArgumentNullException.ThrowIfNull(workspace)

        let name = ToolNameRules.Validate(SearchInternals.globToolName)

        let method =
            match
                typeof<GlobInvoker>
                    .GetMethod("InvokeAsync", BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic)
            with
            | null -> raise (InvalidOperationException("The glob invoker is missing its InvokeAsync method."))
            | found -> found

        AIFunctionFactory.Create(
            method,
            GlobInvoker(workspace) :> obj,
            name,
            SearchInternals.globDescription,
            Unchecked.defaultof<System.Text.Json.JsonSerializerOptions>
        )

    /// Builds the grep tool over the given workspace: confined regex content
    /// search under the workspace's exposed root path with the shared
    /// gitignore filter and result cap.
    /// <param name="workspace">The bound workspace the tool searches.</param>
    /// <returns>The grep tool offered to the model.</returns>
    static member CreateGrepTool(workspace: IWorkspace) : AITool =
        ArgumentNullException.ThrowIfNull(workspace)

        let name = ToolNameRules.Validate(SearchInternals.grepToolName)

        let method =
            match
                typeof<GrepInvoker>
                    .GetMethod("InvokeAsync", BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic)
            with
            | null -> raise (InvalidOperationException("The grep invoker is missing its InvokeAsync method."))
            | found -> found

        AIFunctionFactory.Create(
            method,
            GrepInvoker(workspace) :> obj,
            name,
            SearchInternals.grepDescription,
            Unchecked.defaultof<System.Text.Json.JsonSerializerOptions>
        )
