// SPDX-License-Identifier: Apache-2.0
module Dot.CodingTools

open System
open System.Collections.Generic
open System.IO
open System.Reflection
open System.Text
open System.Text.Json
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging

// Dot-local coding tool source: the seven coding built-ins bound to the
// session's HostDirectory workspace. edit_file/glob/grep ride the public
// BuiltinEditTools/BuiltinSearchTools factories; read_file/write_file/
// list_files/exec are dot-local AIFunctions over the public IWorkspace
// primitives mirroring the runtime contracts in Legate.Tools.FileTools
// (names, descriptions, path rules, caps, truncation markers, result
// shapes) and Legate.ExecTool (name, description, the 1..300 s clamp, the
// 100 KB per-stream cap, the exit_code/stdout/stderr/timed_out JSON). The
// runtime modules are the contract source: a behavior change there must be
// mirrored here. Root fencing stays on in every policy mode: invalid or
// escaping paths fail as ToolException and writes under input/ are
// refused, mirroring the runtime tools.

// ──────────────────────────────────────────────────────────────────────────
// Mirrored runtime contracts (contract source: Legate.Tools.FileTools,
// Legate.ExecTool, Legate.TurnLoop.TruncationMarker)

/// Names, descriptions, and limits copied from the runtime tool contracts
/// so the model meets the same surface dot-locally as in the runtime.
module private ToolContracts =

    /// The model-facing name of the text read tool (FileTools.readFileName).
    let readFileName = "read_file"

    /// The model-facing name of the text write tool (FileTools.writeFileName).
    let writeFileName = "write_file"

    /// The model-facing name of the directory listing tool (FileTools.listFilesName).
    let listFilesName = "list_files"

    /// The model-facing name of the shell tool (ExecTool.ToolName).
    let execToolName = "exec"

    /// What read_file does, shown to the model (FileTools.readFileDescription).
    let readFileDescription =
        "Reads a workspace file as line-numbered text. Takes a workspace-relative path with an optional 1-based startLine/endLine range and a maxBytes size cap; longer output is cut at the cap with a truncation marker."

    /// What write_file does, shown to the model (FileTools.writeFileDescription).
    let writeFileDescription =
        "Writes UTF-8 text to a workspace file, creating parent directories and overwriting any existing content. The input/ area is read-only and refused with a hint."

    /// What list_files does, shown to the model (FileTools.listFilesDescription).
    let listFilesDescription =
        "Lists one workspace directory level as workspace-relative paths, directories suffixed with '/'. Takes an optional workspace-relative directory defaulting to the root; needs a runtime that exposes a filesystem path."

    /// What exec does, shown to the model (ExecTool.Description).
    let execDescription =
        "Run a shell command in the session workspace and report its exit code, standard output, standard error, and whether it timed out."

    /// The default text read cap in bytes (FileTools.defaultReadMaxBytes).
    let defaultReadMaxBytes = 1_048_576

    /// The minimum exec timeout in whole seconds (ExecTool.MinTimeoutSeconds).
    let minTimeoutSeconds = 1

    /// The maximum exec timeout in whole seconds (ExecTool.MaxTimeoutSeconds).
    let maxTimeoutSeconds = 300

    /// The per-stream exec output cap in characters (ExecTool.MaxOutputCharsPerStream).
    let maxOutputCharsPerStream = 102400

    /// The truncation marker appended when output is cut (TurnLoop.TruncationMarker).
    let truncationMarker = "[truncated]"

// ──────────────────────────────────────────────────────────────────────────
// Mirrored path rules and IO helpers (contract source: Legate.Tools.FileTools)

/// Validates a workspace-relative path against the IWorkspace contract
/// rules and returns it unchanged (FileTools.validatePath).
/// <param name="toolName">The calling tool, carried on the ToolException.</param>
/// <param name="path">The path to validate.</param>
/// <returns>The validated path, unchanged.</returns>
let private validatePath (toolName: string) (path: string) : string =
    if isNull (box path) then
        raise (ToolException(toolName, "A file tool path must not be null."))

    if path = "" then
        raise (ToolException(toolName, "A file tool path must not be empty."))

    if path.Length >= 2 && path[1] = ':' then
        raise (ToolException(toolName, "A file tool path must not be a rooted path or carry a drive prefix."))

    if path.StartsWith('/') || path.StartsWith('\\') then
        raise (ToolException(toolName, "A file tool path must be relative, without a leading slash."))

    if path.EndsWith('/') then
        raise (ToolException(toolName, "A file tool path must not end with a slash."))

    let segments = path.Split('/')

    for segment in segments do
        if segment = "" then
            raise (ToolException(toolName, "A file tool path must not contain empty segments."))

        if segment = "." || segment = ".." then
            raise (ToolException(toolName, "A file tool path must not contain '.' or '..' segments."))

        if segment.Contains('\\') then
            raise (ToolException(toolName, "A file tool path must not contain backslashes."))

        if segment.Contains('\u0000') then
            raise (ToolException(toolName, "A file tool path must not contain NUL characters."))

    path

/// Refuses writes under the read-only input/ area with a hint
/// (FileTools.refuseInputWrite).
/// <param name="toolName">The calling tool, carried on the ToolException.</param>
/// <param name="relativePath">The validated workspace-relative path.</param>
let private refuseInputWrite (toolName: string) (relativePath: string) : unit =
    if
        relativePath.Equals("input", StringComparison.OrdinalIgnoreCase)
        || relativePath.StartsWith("input/", StringComparison.OrdinalIgnoreCase)
    then
        raise (
            ToolException(
                toolName,
                "The input/ area is read-only: file tools can read and list it, but writes must go under a different path such as output/."
            )
        )

/// Reports whether a resolved absolute path lies under the resolved input/
/// directory (FileTools.isUnderInputDir).
/// <param name="resolvedPath">The resolved absolute target path.</param>
/// <param name="resolvedRoot">The resolved absolute workspace root.</param>
/// <returns>True when the target is the input/ area.</returns>
let private isUnderInputDir (resolvedPath: string) (resolvedRoot: string) : bool =
    let inputDir = Path.GetFullPath(Path.Combine(resolvedRoot, "input"))

    resolvedPath.Equals(inputDir, StringComparison.OrdinalIgnoreCase)
    || resolvedPath.StartsWith(inputDir + string Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)

/// Resolves one link component to an absolute path when the path is a
/// symbolic link, or null when it is not a link, does not exist, or cannot
/// be resolved (FileTools.resolveLinkTarget).
/// <param name="path">The absolute component path to inspect.</param>
/// <returns>The absolute link target, or null when there is none.</returns>
let private resolveLinkTarget (path: string) : string | null =
    let absolutize (linkPath: string) (target: string) =
        if Path.IsPathRooted target then
            Path.GetFullPath target
        else
            let parent: string | null = Path.GetDirectoryName linkPath

            let baseDir =
                match parent with
                | null -> linkPath
                | dir -> dir

            Path.GetFullPath(Path.Combine(baseDir, target))

    try
        let rawTarget: string | null =
            if Directory.Exists path then
                (DirectoryInfo path).LinkTarget
            elif File.Exists path then
                (FileInfo path).LinkTarget
            else
                null

        match rawTarget with
        | null -> null
        | target -> absolutize path target
    with
    | :? IOException -> null
    | :? UnauthorizedAccessException -> null

/// Resolves a validated workspace-relative path to a host absolute path,
/// checking at call time that it stays under the resolved workspace root:
/// a GetFullPath prefix check plus component-wise symlink resolution,
/// re-checked after every hop (FileTools.resolveHostPath). Returns null
/// when the runtime exposes no filesystem path.
/// <param name="toolName">The calling tool, carried on the ToolException.</param>
/// <param name="workspace">The bound workspace.</param>
/// <param name="relativePath">The validated workspace-relative path.</param>
/// <returns>The absolute host path, or null when the runtime exposes no filesystem path.</returns>
let private resolveHostPath (toolName: string) (workspace: IWorkspace) (relativePath: string) : string | null =
    match workspace.Root.Path with
    | null -> null
    | rootPath ->
        let comparison =
            if OperatingSystem.IsWindows() then
                StringComparison.OrdinalIgnoreCase
            else
                StringComparison.Ordinal

        let resolvedRoot = Path.GetFullPath rootPath

        let check (fullPath: string) =
            let inside =
                fullPath = resolvedRoot
                || fullPath.StartsWith(resolvedRoot + string Path.DirectorySeparatorChar, comparison)

            if not inside then
                raise (
                    ToolException(
                        toolName,
                        "A file tool path must stay inside the workspace root: the resolved path escapes the workspace."
                    )
                )

        let mutable current = resolvedRoot

        for segment in relativePath.Split('/') do
            current <- Path.Combine(current, segment)
            check current

            let mutable hops = 0
            let mutable settled = false

            while not settled do
                match resolveLinkTarget current with
                | null -> settled <- true
                | target ->
                    hops <- hops + 1

                    if hops > 40 then
                        raise (
                            ToolException(
                                toolName,
                                "A file tool path must stay inside the workspace root: the resolved path escapes the workspace."
                            )
                        )

                    check target
                    current <- target

        let fullPath = Path.GetFullPath current
        check fullPath
        fullPath

/// Refuses writes whose resolved target lands under the read-only input/
/// area (FileTools.refuseResolvedInputWrite). No-op when the runtime
/// exposes no filesystem path.
/// <param name="toolName">The calling tool, carried on the ToolException.</param>
/// <param name="workspace">The bound workspace.</param>
/// <param name="resolvedPath">The resolved absolute target, or null.</param>
let private refuseResolvedInputWrite (toolName: string) (workspace: IWorkspace) (resolvedPath: string | null) : unit =
    match resolvedPath with
    | null -> ()
    | resolved ->
        match workspace.Root.Path with
        | null -> ()
        | rootPath ->
            let resolvedRoot = Path.GetFullPath rootPath

            if isUnderInputDir resolved resolvedRoot then
                raise (
                    ToolException(
                        toolName,
                        "The input/ area is read-only: file tools can read and list it, but writes must go under a different path such as output/."
                    )
                )

/// Streams a workspace file up to the cap, aborting early once the cap is
/// exceeded (FileTools.readCapped).
/// <param name="stream">The open read stream.</param>
/// <param name="capBytes">How many bytes to keep; must be at least 1.</param>
/// <param name="cancellationToken">Token that abandons the read.</param>
/// <returns>The bytes kept and whether the stream held more.</returns>
let private readCapped (stream: Stream) (capBytes: int) (cancellationToken: CancellationToken) : Task<byte[] * bool> =
    task {
        use memory = new MemoryStream()
        let buffer = Array.zeroCreate<byte> 8192
        let mutable total = 0
        let mutable truncated = false
        let mutable finished = false

        while not finished do
            let! read = stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)

            if read = 0 then
                finished <- true
            else
                let remaining = capBytes - total

                if remaining <= 0 then
                    truncated <- true
                    finished <- true
                elif read > remaining then
                    memory.Write(buffer, 0, remaining)
                    total <- total + remaining
                    truncated <- true
                    finished <- true
                else
                    memory.Write(buffer, 0, read)
                    total <- total + read

        return (memory.ToArray(), truncated)
    }

// ──────────────────────────────────────────────────────────────────────────
// Dot-local file tools (shapes mirror FileTools.FileToolHost)

/// The method holding one dot-local file tool's implementation, bound to a
/// workspace. Methods stay instance members with stable names so the
/// factories resolve them by name; F# optional arguments surface as
/// optional tool parameters.
type private DotFileHost(workspace: IWorkspace) =

    /// Reads a workspace file as line-numbered text with an optional
    /// 1-based line range and byte cap.
    member _.ReadFileAsync
        (path: string, cancellationToken: CancellationToken, ?startLine: int, ?endLine: int, ?maxBytes: int)
        : Task<string> =
        task {
            let relative = validatePath ToolContracts.readFileName path
            resolveHostPath ToolContracts.readFileName workspace relative |> ignore

            let first = defaultArg startLine 1
            let last = defaultArg endLine Int32.MaxValue

            if first < 1 || last < first then
                raise (
                    ToolException(
                        ToolContracts.readFileName,
                        "The line range is invalid: startLine and endLine are 1-based line numbers and endLine must not precede startLine."
                    )
                )

            let cap = defaultArg maxBytes ToolContracts.defaultReadMaxBytes

            if cap < 1 then
                raise (ToolException(ToolContracts.readFileName, "The size cap must be at least 1 byte."))

            try
                use! stream = workspace.ReadFile(relative, cancellationToken)
                let! bytes, truncated = readCapped stream cap cancellationToken
                let text = Encoding.UTF8.GetString(bytes, 0, bytes.Length)

                let lines =
                    if bytes.Length = 0 then
                        [||]
                    else
                        let raw = text.Split('\n')
                        let count = if text.EndsWith('\n') then raw.Length - 1 else raw.Length

                        Array.init (max count 0) (fun index -> raw[index].TrimEnd('\r'))

                let total = lines.Length
                let startIndex = min (first - 1) total
                let endIndex = min last total

                let selected =
                    if startIndex >= endIndex then
                        [||]
                    else
                        lines[startIndex .. endIndex - 1]

                let numbered =
                    selected
                    |> Array.mapi (fun index line -> sprintf "%d: %s" (startIndex + index + 1) line)
                    |> String.concat "\n"

                if truncated then
                    let marker =
                        sprintf "... [output truncated at %d bytes: narrow startLine/endLine or raise maxBytes]" cap

                    if numbered = "" then
                        return marker
                    else
                        return numbered + "\n" + marker
                else
                    return numbered
            with
            | :? ToolException as toolError -> return raise toolError
            | :? FileNotFoundException ->
                return raise (ToolException(ToolContracts.readFileName, "The file does not exist in the workspace."))
            | ex when not (ex :? OperationCanceledException) ->
                return raise (ToolException(ToolContracts.readFileName, "The read_file tool could not read the file."))
        }

    /// Writes UTF-8 text to a workspace file, creating parent directories
    /// and refusing the read-only input/ area.
    member _.WriteFileAsync(path: string, content: string, cancellationToken: CancellationToken) : Task<string> =
        task {
            let relative = validatePath ToolContracts.writeFileName path
            refuseInputWrite ToolContracts.writeFileName relative

            let resolved = resolveHostPath ToolContracts.writeFileName workspace relative
            refuseResolvedInputWrite ToolContracts.writeFileName workspace resolved

            if isNull (box content) then
                raise (ToolException(ToolContracts.writeFileName, "The write_file content must not be null."))

            let bytes = Encoding.UTF8.GetBytes content

            try
                do! workspace.WriteFile(relative, bytes, cancellationToken)
                return sprintf "Wrote %d bytes to %s." bytes.Length relative
            with
            | :? ToolException as toolError -> return raise toolError
            | ex when not (ex :? OperationCanceledException) ->
                return
                    raise (ToolException(ToolContracts.writeFileName, "The write_file tool could not write the file."))
        }

    /// Lists one workspace directory level as workspace-relative paths.
    member _.ListFilesAsync(cancellationToken: CancellationToken, ?directory: string) : Task<string> =
        task {
            cancellationToken.ThrowIfCancellationRequested()

            let rootValue: string =
                match workspace.Root.Path with
                | null ->
                    raise (
                        ToolException(
                            ToolContracts.listFilesName,
                            "The list_files tool needs a workspace filesystem path, but the runtime does not expose one."
                        )
                    )
                | exposed -> exposed

            let relative =
                match directory with
                | None -> ""
                | Some dir when isNull (box dir) -> ""
                | Some "" -> ""
                | Some dir -> validatePath ToolContracts.listFilesName dir

            let fullDir =
                match resolveHostPath ToolContracts.listFilesName workspace relative with
                | null ->
                    raise (
                        ToolException(
                            ToolContracts.listFilesName,
                            "The list_files tool needs a workspace filesystem path, but the runtime does not expose one."
                        )
                    )
                | dir -> dir

            let resolvedRoot = Path.GetFullPath rootValue

            try
                if File.Exists fullDir then
                    raise (
                        ToolException(ToolContracts.listFilesName, "The list_files target is a file, not a directory.")
                    )

                if not (Directory.Exists fullDir) then
                    raise (ToolException(ToolContracts.listFilesName, "The directory does not exist in the workspace."))

                let entries =
                    Directory.EnumerateFileSystemEntries fullDir
                    |> Seq.map (fun entry ->
                        let entryRelative =
                            Path.GetRelativePath(resolvedRoot, entry).Replace(Path.DirectorySeparatorChar, '/')

                        if Directory.Exists entry then
                            entryRelative + "/"
                        else
                            entryRelative)
                    |> Seq.sortWith (fun left right -> String.Compare(left, right, StringComparison.Ordinal))
                    |> String.concat "\n"

                return entries
            with
            | :? ToolException as toolError -> return raise toolError
            | ex when not (ex :? OperationCanceledException) ->
                return
                    raise (
                        ToolException(ToolContracts.listFilesName, "The list_files tool could not list the directory.")
                    )
        }

/// Resolves one host method by name, failing fast when it is missing.
/// <param name="name">The DotFileHost method name.</param>
/// <returns>The method the factories build their function over.</returns>
let private hostMethod (name: string) =
    let flags = BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic

    match typeof<DotFileHost>.GetMethod(name, flags) with
    | null -> raise (InvalidOperationException(sprintf "DotFileHost.%s is missing." name))
    | method -> method

/// Builds one dot-local file tool over the workspace: plain-string
/// results stay raw so the turn loop's string fast-path sees them
/// unquoted (the FileTools.createTool precedent).
/// <param name="workspace">The bound workspace the tool runs in.</param>
/// <param name="methodName">The DotFileHost method backing the tool.</param>
/// <param name="toolName">The model-facing tool name.</param>
/// <param name="description">What the tool does, shown to the model.</param>
/// <returns>The tool offered to the model.</returns>
let private createFileTool
    (workspace: IWorkspace)
    (methodName: string)
    (toolName: string)
    (description: string)
    : AIFunction =
    ArgumentNullException.ThrowIfNull workspace
    let host = DotFileHost workspace
    let options = AIFunctionFactoryOptions()
    options.Name <- toolName
    options.Description <- description

    options.MarshalResult <-
        Func<obj, Type, CancellationToken, ValueTask<obj>>(fun result _ _ -> ValueTask<obj>(result))

    AIFunctionFactory.Create(hostMethod methodName, host :> obj, options)

// ──────────────────────────────────────────────────────────────────────────
// Dot-local exec tool (contract source: Legate.ExecTool)

/// Clamps a timeout in whole seconds into the fixed 1..300 s bounds
/// (ExecTool.clampTimeoutSeconds).
/// <param name="seconds">The requested whole seconds.</param>
/// <returns>The seconds clamped into the fixed bounds.</returns>
let private clampTimeoutSeconds (seconds: int) : int =
    if seconds < ToolContracts.minTimeoutSeconds then
        ToolContracts.minTimeoutSeconds
    elif seconds > ToolContracts.maxTimeoutSeconds then
        ToolContracts.maxTimeoutSeconds
    else
        seconds

/// Truncates one captured output stream to the per-stream cap, appending
/// the truncation marker when cut (ExecTool.truncateStream).
/// <param name="value">The captured stream text, or null.</param>
/// <returns>The bounded stream text, never null.</returns>
let private truncateStream (value: string | null) : string =
    let text =
        match value with
        | null -> ""
        | text -> text

    if text.Length > ToolContracts.maxOutputCharsPerStream then
        text.Substring(0, ToolContracts.maxOutputCharsPerStream)
        + ToolContracts.truncationMarker
    else
        text

/// Copies the agent environment allowlist for injection: only entries
/// whose key satisfies AgentEnvironmentKeys ride along (ExecTool.selectAgentEnv).
/// <param name="agentEnv">The agent's environment variables, or null when the agent sets none.</param>
/// <returns>The allowlisted copy to inject, or null when the agent sets none.</returns>
/// Copies the allowlisted agent environment entries into a fresh table:
/// only entries whose key satisfies AgentEnvironmentKeys ride along.
/// <param name="source">The agent's environment variables. Must not be null.</param>
/// <returns>The allowlisted copy to inject.</returns>
let private copyAgentEnv (source: IReadOnlyDictionary<string, string>) : IReadOnlyDictionary<string, string> =
    ArgumentNullException.ThrowIfNull(source)

    let selected = Dictionary<string, string>(StringComparer.Ordinal)

    for KeyValue(key, value) in source do
        if
            not (isNull (box key))
            && not (isNull (box value))
            && AgentEnvironmentKeys.TryValidate key
        then
            selected[key] <- value

    selected :> IReadOnlyDictionary<string, string>

/// Copies the agent environment allowlist for injection, or null when the
/// agent sets none (ExecTool.selectAgentEnv).
/// <param name="agentEnv">The agent's environment variables, or null when the agent sets none.</param>
/// <returns>The allowlisted copy to inject, or null when the agent sets none.</returns>
let private selectAgentEnv
    (agentEnv: IReadOnlyDictionary<string, string> | null)
    : IReadOnlyDictionary<string, string> | null =
    match agentEnv with
    | null -> null
    | source -> copyAgentEnv source

/// Builds the tool result JSON with the pinned keys exit_code, stdout,
/// stderr, and timed_out (ExecTool.resultJson).
/// <param name="result">The primitive result. Must not be null.</param>
/// <returns>The JSON result text.</returns>
let private execResultJson (result: WorkspaceExecResult) : string =
    ArgumentNullException.ThrowIfNull(result)

    let node = JsonObject()
    node["exit_code"] <- JsonValue.Create(result.ExitCode)
    node["stdout"] <- JsonValue.Create(truncateStream result.StandardOutput)
    node["stderr"] <- JsonValue.Create(truncateStream result.StandardError)
    node["timed_out"] <- JsonValue.Create(result.TimedOut)
    node.ToJsonString()

/// Creates the dot-local exec AIFunction bound to one workspace with the
/// agent environment allowlist injected on every call (ExecTool.create).
/// The command passes verbatim; timeout_seconds carries whole seconds
/// always clamped to 1..300; thrown messages never embed the command or
/// environment values.
/// <param name="workspace">The bound workspace the command executes in.</param>
/// <param name="agentEnv">The agent's environment variables to allowlist-inject, or null when the agent sets none.</param>
/// <returns>The model-facing exec tool.</returns>
let private createExecTool (workspace: IWorkspace) (agentEnv: IReadOnlyDictionary<string, string> | null) : AIFunction =
    ArgumentNullException.ThrowIfNull(workspace)
    ToolContracts.execToolName |> ToolNameRules.Validate |> ignore

    let env = selectAgentEnv agentEnv

    let method =
        Func<string, int, CancellationToken, Task<string>>(fun command timeoutSeconds cancellationToken ->
            task {
                if isNull (box command) then
                    raise (ArgumentNullException(nameof command))

                let timeout = TimeSpan.FromSeconds(float (clampTimeoutSeconds timeoutSeconds))
                let! result = workspace.Exec(command, Nullable(timeout), env, cancellationToken)
                return execResultJson result
            })

    AIFunctionFactory.Create(
        method,
        ToolContracts.execToolName,
        ToolContracts.execDescription,
        Unchecked.defaultof<JsonSerializerOptions>
    )

// ──────────────────────────────────────────────────────────────────────────
// The session-bound tool source

/// The dot-local coding tool source: resolves the asking session through
/// the session store, binds its workspace through the registered runtime,
/// and offers the seven coding built-ins over that workspace. A session
/// the store does not know, or a workspace that will not bind, degrades
/// to the contract's empty list (never null, never an error) with the
/// reason logged without secrets or tool arguments.
type CodingToolSource
    (sessions: ISessionStore, agents: IAgentStore, runtime: IWorkspaceRuntime, logger: ILogger<CodingToolSource> | null)
    =

    do ArgumentNullException.ThrowIfNull(sessions)
    do ArgumentNullException.ThrowIfNull(agents)
    do ArgumentNullException.ThrowIfNull(runtime)

    /// Logs a degraded-source reason carrying no secrets or tool arguments.
    /// <param name="reason">The fixed reason text.</param>
    /// <param name="error">The failure, logged by type name only.</param>
    let logDegraded (reason: string) (error: exn | null) : unit =
        match logger with
        | null -> ()
        | log ->
            match error with
            | null -> log.LogWarning("The dot coding tool source is degraded: {Reason}.", reason)
            | failure ->
                log.LogWarning(
                    "The dot coding tool source is degraded: {Reason} ({ErrorType}).",
                    reason,
                    failure.GetType().Name
                )

    interface IToolSource with

        /// Resolves the seven coding tools for one session over its bound
        /// workspace.
        /// <param name="context">The tenant, agent, and session asking for its tools.</param>
        /// <param name="cancellationToken">Abandons the resolution.</param>
        /// <returns>The tools offered to the model, or the empty list when degraded.</returns>
        member _.GetTools
            (context: ToolSourceContext, cancellationToken: CancellationToken)
            : Task<IReadOnlyList<AITool>> =
            task {
                try
                    // ToolSourceContext carries no null annotation, so the
                    // check is a runtime guard for non-F# callers.
                    if isNull (box context) then
                        logDegraded "no tool source context" null
                        return ResizeArray<AITool>() :> IReadOnlyList<AITool>
                    else
                        let! session = sessions.GetSession(context.Tenant, context.SessionId, cancellationToken)

                        match session with
                        | null ->
                            logDegraded "the session is unknown to the session store" null
                            return ResizeArray<AITool>() :> IReadOnlyList<AITool>
                        | known ->
                            let! workspace = runtime.Bind(known, null, cancellationToken)

                            let! agent = agents.GetAgent(context.Tenant, context.AgentId, cancellationToken)

                            let env =
                                match agent with
                                | null -> null
                                | holder -> selectAgentEnv holder.EnvironmentVariables

                            let tools = ResizeArray<AITool>()

                            tools.Add(
                                createFileTool
                                    workspace
                                    "ReadFileAsync"
                                    (ToolNameRules.Validate ToolContracts.readFileName)
                                    ToolContracts.readFileDescription
                                :> AITool
                            )

                            tools.Add(
                                createFileTool
                                    workspace
                                    "WriteFileAsync"
                                    (ToolNameRules.Validate ToolContracts.writeFileName)
                                    ToolContracts.writeFileDescription
                                :> AITool
                            )

                            tools.Add(
                                createFileTool
                                    workspace
                                    "ListFilesAsync"
                                    (ToolNameRules.Validate ToolContracts.listFilesName)
                                    ToolContracts.listFilesDescription
                                :> AITool
                            )

                            tools.Add(BuiltinEditTools.CreateEditFileTool(workspace))
                            tools.Add(BuiltinSearchTools.CreateGlobTool(workspace))
                            tools.Add(BuiltinSearchTools.CreateGrepTool(workspace))
                            tools.Add(createExecTool workspace env :> AITool)

                            return tools :> IReadOnlyList<AITool>
                with
                | :? OperationCanceledException as canceled ->
                    return! Task.FromException<IReadOnlyList<AITool>>(canceled)
                | error ->
                    logDegraded "the session workspace could not be bound" error
                    return ResizeArray<AITool>() :> IReadOnlyList<AITool>
            }
