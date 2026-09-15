// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tools

open System
open System.IO
open System.Reflection
open System.Text
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI
open Legate

// Built-in file tools over IWorkspace: read_file, write_file, list_files,
// read_binary_base64, and write_binary_base64. The tools never touch the
// file system directly: file bytes flow through IWorkspace streams, and
// directory enumeration resolves through the workspace root path the
// runtime exposes. Every path argument is validated against the IWorkspace
// contract rules (no rooted paths or drive prefixes, no leading or trailing
// slashes, no '.' or '..' segments, no backslashes, no NUL characters, and
// no empty segments) with a call-time GetFullPath prefix check plus a
// component-wise symlink resolution against the resolved root, so a call
// can never reach outside the workspace. All tool failures surface as
// ToolException carrying the tool name; messages never embed paths,
// secrets, or tool arguments. Cancellation propagates unwrapped.
module internal FileTools =

    /// The model-facing name of the text read tool.
    let readFileName = "read_file"

    /// The model-facing name of the text write tool.
    let writeFileName = "write_file"

    /// The model-facing name of the directory listing tool.
    let listFilesName = "list_files"

    /// The model-facing name of the base64 read tool.
    let readBinaryName = "read_binary_base64"

    /// The model-facing name of the base64 write tool.
    let writeBinaryName = "write_binary_base64"

    /// The binary size cap in bytes (10 MiB): the default maxBytes for the
    /// binary tools. Payloads over the cap are rejected, never truncated.
    let binaryCapBytes = 10_485_760

    /// The default text read cap in bytes (1 MiB): read_file cuts output at
    /// this size with a truncation marker unless the caller passes maxBytes.
    let defaultReadMaxBytes = 1_048_576

    /// What read_file does, shown to the model.
    let readFileDescription =
        "Reads a workspace file as line-numbered text. Takes a workspace-relative path with an optional 1-based startLine/endLine range and a maxBytes size cap; longer output is cut at the cap with a truncation marker."

    /// What write_file does, shown to the model.
    let writeFileDescription =
        "Writes UTF-8 text to a workspace file, creating parent directories and overwriting any existing content. The input/ area is read-only and refused with a hint."

    /// What list_files does, shown to the model.
    let listFilesDescription =
        "Lists one workspace directory level as workspace-relative paths, directories suffixed with '/'. Takes an optional workspace-relative directory defaulting to the root; needs a runtime that exposes a filesystem path."

    /// What read_binary_base64 does, shown to the model.
    let readBinaryDescription =
        "Reads a workspace file as base64 text. Files over the maxBytes cap (default 10 MiB) are rejected."

    /// What write_binary_base64 does, shown to the model.
    let writeBinaryDescription =
        "Writes base64-decoded bytes to a workspace file, creating parent directories and overwriting any existing content. Payloads over the maxBytes cap (default 10 MiB) are rejected; the input/ area is read-only."

    /// Validates a workspace-relative path against the IWorkspace contract
    /// rules and returns it unchanged.
    /// <param name="toolName">The calling tool, carried on the ToolException.</param>
    /// <param name="path">The path to validate.</param>
    /// <returns>The validated path, unchanged.</returns>
    /// <exception cref="T:Legate.ToolException">The path is null, empty, rooted, carries a drive prefix, or contains a leading slash, trailing slash, empty segment, '.', '..', backslash, or NUL character.</exception>
    let validatePath (toolName: string) (path: string) : string =
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

    /// Refuses writes under the read-only input/ area with a hint. The
    /// first-segment comparison is ordinal-ignore-case: Windows
    /// filesystems resolve INPUT/ onto input/, so a case-sensitive guard
    /// would let a case variant through.
    /// <param name="toolName">The calling tool, carried on the ToolException.</param>
    /// <param name="relativePath">The validated workspace-relative path.</param>
    /// <exception cref="T:Legate.ToolException">The path is the input/ area.</exception>
    let refuseInputWrite (toolName: string) (relativePath: string) : unit =
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

    /// Reports whether a resolved absolute path lies under the resolved
    /// input/ directory. The comparison is ordinal-ignore-case on every
    /// platform: the sound rule for a read-only area on case-insensitive
    /// filesystems, conservative elsewhere.
    /// <param name="resolvedPath">The resolved absolute target path.</param>
    /// <param name="resolvedRoot">The resolved absolute workspace root.</param>
    /// <returns>True when the target is the input/ area.</returns>
    let isUnderInputDir (resolvedPath: string) (resolvedRoot: string) : bool =
        let inputDir = Path.GetFullPath(Path.Combine(resolvedRoot, "input"))

        resolvedPath.Equals(inputDir, StringComparison.OrdinalIgnoreCase)
        || resolvedPath.StartsWith(inputDir + string Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)

    /// Refuses writes whose resolved target lands under the read-only
    /// input/ area. The lexical guard cannot see through a symlink that
    /// points at input/, so this runs after resolveHostPath against the
    /// resolved absolute target. No-op when the runtime exposes no
    /// filesystem path: the lexical guard already ran.
    /// <param name="toolName">The calling tool, carried on the ToolException.</param>
    /// <param name="workspace">The bound workspace.</param>
    /// <param name="resolvedPath">The resolved absolute target, or null.</param>
    /// <exception cref="T:Legate.ToolException">The resolved target is the input/ area.</exception>
    let refuseResolvedInputWrite (toolName: string) (workspace: IWorkspace) (resolvedPath: string | null) : unit =
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

    /// Resolves one link component to an absolute path when the path is a
    /// symbolic link, or null when it is not a link, does not exist, or
    /// cannot be resolved. The target is read off the link itself and
    /// relative targets resolve against the link's own directory, never the
    /// process working directory.
    /// <param name="path">The absolute component path to inspect.</param>
    /// <returns>The absolute link target, or null when there is none.</returns>
    let resolveLinkTarget (path: string) : string | null =
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
    /// checking at call time that it stays under the resolved workspace
    /// root: a GetFullPath prefix check plus component-wise symlink
    /// resolution, re-checked after every hop. Returns null when the runtime
    /// exposes no filesystem path: the runtime then owns confinement.
    /// <param name="toolName">The calling tool, carried on the ToolException.</param>
    /// <param name="workspace">The bound workspace.</param>
    /// <param name="relativePath">The validated workspace-relative path.</param>
    /// <returns>The absolute host path, or null when the runtime exposes no filesystem path.</returns>
    /// <exception cref="T:Legate.ToolException">The resolved path escapes the workspace root.</exception>
    let resolveHostPath (toolName: string) (workspace: IWorkspace) (relativePath: string) : string | null =
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

                // Follow link chains hop by hop, re-checking after each:
                // the resolved target may itself be a link. Cycles fail
                // closed past the hop budget instead of looping forever.
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

    /// Streams a workspace file up to the cap, aborting early once the cap
    /// is exceeded so memory never grows past cap plus one chunk.
    /// <param name="stream">The open read stream.</param>
    /// <param name="capBytes">How many bytes to keep; must be at least 1.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The bytes kept and whether the stream held more.</returns>
    let readCapped (stream: Stream) (capBytes: int) (cancellationToken: CancellationToken) : Task<byte[] * bool> =
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

    /// Decodes base64 text up to the cap, aborting early once the decoded
    /// size would exceed the cap so memory never grows past the cap. A
    /// length pre-check rejects obviously over-cap payloads before
    /// decoding a single quad; the quad-by-quad loop then enforces the cap
    /// during decode, mirroring readCapped's early abort.
    /// <param name="toolName">The calling tool, carried on the ToolException.</param>
    /// <param name="base64Content">The base64 text to decode. Must not be null.</param>
    /// <param name="capBytes">How many decoded bytes to accept; must be at least 1.</param>
    /// <returns>The decoded bytes, at most capBytes long.</returns>
    /// <exception cref="T:Legate.ToolException">The text is not valid base64, or decodes past the cap.</exception>
    let decodeCappedBase64 (toolName: string) (base64Content: string) (capBytes: int) : byte[] =
        let isWhitespace (c: char) =
            c = ' ' || c = '\t' || c = '\r' || c = '\n' || c = '\f'

        let rejectOverCap () =
            raise (
                ToolException(toolName, "The payload exceeds the binary size cap: writes over the cap are rejected.")
            )

        let rejectInvalid () =
            raise (ToolException(toolName, sprintf "The %s content is not valid base64." toolName))

        let mutable effectiveLen = 0

        for c in base64Content do
            if not (isWhitespace c) then
                effectiveLen <- effectiveLen + 1

        let mutable padding = 0
        let mutable index = base64Content.Length - 1
        let mutable scanning = true

        while index >= 0 && scanning do
            let c = base64Content[index]

            if isWhitespace c then
                index <- index - 1
            elif c = '=' then
                padding <- padding + 1
                index <- index - 1
            else
                scanning <- false

        if effectiveLen % 4 = 0 && padding <= 2 then
            let estimated = effectiveLen / 4 * 3 - padding

            if estimated > capBytes then
                rejectOverCap ()

        use memory = new MemoryStream()
        let quad = Array.zeroCreate<char> 4
        let mutable quadPos = 0
        let mutable total = 0
        let mutable seenPadding = false

        let flushQuad () =
            if quad[0] = '=' || quad[1] = '=' || (quad[2] = '=' && quad[3] <> '=') then
                rejectInvalid ()

            let bytes =
                try
                    Convert.FromBase64CharArray(quad, 0, 4)
                with :? FormatException ->
                    rejectInvalid ()

            if total + bytes.Length > capBytes then
                rejectOverCap ()

            memory.Write(bytes, 0, bytes.Length)
            total <- total + bytes.Length

        for c in base64Content do
            if isWhitespace c then
                ()
            else
                if c = '=' then
                    seenPadding <- true
                elif seenPadding then
                    rejectInvalid ()

                quad[quadPos] <- c
                quadPos <- quadPos + 1

                if quadPos = 4 then
                    flushQuad ()
                    quadPos <- 0

        if quadPos <> 0 then
            rejectInvalid ()

        memory.ToArray()

    /// The method holding one tool's implementation, bound to a workspace.
    /// Methods stay instance members with stable names so the factories can
    /// resolve them by name; F# optional arguments surface as optional
    /// (nullable with a null default) tool parameters.
    type internal FileToolHost(workspace: IWorkspace) =

        /// Reads a workspace file as line-numbered text with an optional
        /// 1-based line range and byte cap.
        member _.ReadFileAsync
            (path: string, cancellationToken: CancellationToken, ?startLine: int, ?endLine: int, ?maxBytes: int)
            : Task<string> =
            task {
                let relative = validatePath readFileName path
                resolveHostPath readFileName workspace relative |> ignore

                let first = defaultArg startLine 1
                let last = defaultArg endLine Int32.MaxValue

                if first < 1 || last < first then
                    raise (
                        ToolException(
                            readFileName,
                            "The line range is invalid: startLine and endLine are 1-based line numbers and endLine must not precede startLine."
                        )
                    )

                let cap = defaultArg maxBytes defaultReadMaxBytes

                if cap < 1 then
                    raise (ToolException(readFileName, "The size cap must be at least 1 byte."))

                try
                    use! stream = workspace.ReadFile(relative, cancellationToken)
                    let! bytes, truncated = readCapped stream cap cancellationToken
                    let text = Encoding.UTF8.GetString(bytes, 0, bytes.Length)

                    // A trailing newline terminates the last line rather
                    // than starting a phantom empty one.
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
                    return raise (ToolException(readFileName, "The file does not exist in the workspace."))
                | ex when not (ex :? OperationCanceledException) ->
                    return raise (ToolException(readFileName, "The read_file tool could not read the file."))
            }

        /// Writes UTF-8 text to a workspace file, creating parent
        /// directories and refusing the read-only input/ area.
        member _.WriteFileAsync(path: string, content: string, cancellationToken: CancellationToken) : Task<string> =
            task {
                let relative = validatePath writeFileName path
                refuseInputWrite writeFileName relative

                let resolved = resolveHostPath writeFileName workspace relative
                refuseResolvedInputWrite writeFileName workspace resolved

                if isNull (box content) then
                    raise (ToolException(writeFileName, "The write_file content must not be null."))

                let bytes = Encoding.UTF8.GetBytes content

                try
                    do! workspace.WriteFile(relative, bytes, cancellationToken)
                    return sprintf "Wrote %d bytes to %s." bytes.Length relative
                with
                | :? ToolException as toolError -> return raise toolError
                | ex when not (ex :? OperationCanceledException) ->
                    return raise (ToolException(writeFileName, "The write_file tool could not write the file."))
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
                                listFilesName,
                                "The list_files tool needs a workspace filesystem path, but the runtime does not expose one."
                            )
                        )
                    | exposed -> exposed

                let relative =
                    match directory with
                    | None -> ""
                    | Some dir when isNull (box dir) -> ""
                    | Some "" -> ""
                    | Some dir -> validatePath listFilesName dir

                let fullDir =
                    match resolveHostPath listFilesName workspace relative with
                    | null ->
                        // Unreachable: Root.Path was checked above, so the
                        // resolver always returns a path here.
                        raise (
                            ToolException(
                                listFilesName,
                                "The list_files tool needs a workspace filesystem path, but the runtime does not expose one."
                            )
                        )
                    | dir -> dir

                let resolvedRoot = Path.GetFullPath rootValue

                try
                    if File.Exists fullDir then
                        raise (ToolException(listFilesName, "The list_files target is a file, not a directory."))

                    if not (Directory.Exists fullDir) then
                        raise (ToolException(listFilesName, "The directory does not exist in the workspace."))

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
                    return raise (ToolException(listFilesName, "The list_files tool could not list the directory."))
            }

        /// Reads a workspace file as base64 text, rejecting files over the
        /// cap.
        member _.ReadBinaryAsync(path: string, cancellationToken: CancellationToken, ?maxBytes: int) : Task<string> =
            task {
                let relative = validatePath readBinaryName path
                resolveHostPath readBinaryName workspace relative |> ignore

                let cap = defaultArg maxBytes binaryCapBytes

                if cap < 1 then
                    raise (ToolException(readBinaryName, "The size cap must be at least 1 byte."))

                try
                    use! stream = workspace.ReadFile(relative, cancellationToken)
                    let! bytes, truncated = readCapped stream cap cancellationToken

                    if truncated then
                        return
                            raise (
                                ToolException(
                                    readBinaryName,
                                    "The file exceeds the binary size cap: reads over the cap are rejected."
                                )
                            )
                    else
                        return Convert.ToBase64String(bytes)
                with
                | :? ToolException as toolError -> return raise toolError
                | :? FileNotFoundException ->
                    return raise (ToolException(readBinaryName, "The file does not exist in the workspace."))
                | ex when not (ex :? OperationCanceledException) ->
                    return raise (ToolException(readBinaryName, "The read_binary_base64 tool could not read the file."))
            }

        /// Writes base64-decoded bytes to a workspace file, rejecting
        /// over-cap payloads and the read-only input/ area.
        member _.WriteBinaryAsync
            (path: string, base64Content: string, cancellationToken: CancellationToken, ?maxBytes: int)
            : Task<string> =
            task {
                let relative = validatePath writeBinaryName path
                refuseInputWrite writeBinaryName relative

                let resolved = resolveHostPath writeBinaryName workspace relative
                refuseResolvedInputWrite writeBinaryName workspace resolved

                if isNull (box base64Content) then
                    raise (ToolException(writeBinaryName, "The write_binary_base64 content must not be null."))

                let cap = defaultArg maxBytes binaryCapBytes

                if cap < 1 then
                    raise (ToolException(writeBinaryName, "The size cap must be at least 1 byte."))

                let bytes = decodeCappedBase64 writeBinaryName base64Content cap

                try
                    do! workspace.WriteFile(relative, bytes, cancellationToken)
                    return sprintf "Wrote %d bytes to %s." bytes.Length relative
                with
                | :? ToolException as toolError -> return raise toolError
                | ex when not (ex :? OperationCanceledException) ->
                    return
                        raise (ToolException(writeBinaryName, "The write_binary_base64 tool could not write the file."))
            }

    /// Resolves one host method by name, failing fast when it is missing.
    /// Host methods stay internal (everything outside the contract is), so
    /// the lookup spans non-public instance methods.
    /// <param name="name">The FileToolHost method name.</param>
    /// <returns>The method the factories build their function over.</returns>
    let private hostMethod (name: string) =
        let flags = BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic

        match typeof<FileToolHost>.GetMethod(name, flags) with
        | null -> raise (InvalidOperationException(sprintf "FileToolHost.%s is missing." name))
        | method -> method

    /// Builds one file tool over the workspace.
    /// <param name="workspace">The bound workspace the tool runs in. Must not be null.</param>
    /// <param name="methodName">The FileToolHost method backing the tool.</param>
    /// <param name="toolName">The model-facing tool name.</param>
    /// <param name="description">What the tool does, shown to the model.</param>
    /// <returns>The tool offered to the model.</returns>
    let private createTool (workspace: IWorkspace) (methodName: string) (toolName: string) (description: string) =
        ArgumentNullException.ThrowIfNull workspace
        let host = FileToolHost workspace
        let options = AIFunctionFactoryOptions()
        options.Name <- toolName
        options.Description <- description
        // Results are plain strings, already JSON-safe: keep them raw so
        // TurnLoop's string fast-path sees them unquoted. The default
        // marshals every result through a JSON round-trip, which would hand
        // the loop a JsonElement whose ToString adds quotes around the text.
        options.MarshalResult <-
            Func<obj, Type, CancellationToken, ValueTask<obj>>(fun result _ _ -> ValueTask<obj>(result))

        AIFunctionFactory.Create(hostMethod methodName, host :> obj, options)

    /// Creates the read_file tool: line-numbered text reads with an
    /// optional 1-based line range and byte cap over the workspace.
    /// <param name="workspace">The bound workspace the tool runs in. Must not be null.</param>
    /// <returns>The read_file tool offered to the model.</returns>
    let createReadFileTool (workspace: IWorkspace) : AIFunction =
        createTool workspace "ReadFileAsync" readFileName readFileDescription

    /// Creates the write_file tool: UTF-8 text writes that create parent
    /// directories and refuse the read-only input/ area.
    /// <param name="workspace">The bound workspace the tool runs in. Must not be null.</param>
    /// <returns>The write_file tool offered to the model.</returns>
    let createWriteFileTool (workspace: IWorkspace) : AIFunction =
        createTool workspace "WriteFileAsync" writeFileName writeFileDescription

    /// Creates the list_files tool: one-level directory listings as
    /// workspace-relative paths.
    /// <param name="workspace">The bound workspace the tool runs in. Must not be null.</param>
    /// <returns>The list_files tool offered to the model.</returns>
    let createListFilesTool (workspace: IWorkspace) : AIFunction =
        createTool workspace "ListFilesAsync" listFilesName listFilesDescription

    /// Creates the read_binary_base64 tool: base64 reads capped at 10 MiB
    /// by default, rejected past the cap.
    /// <param name="workspace">The bound workspace the tool runs in. Must not be null.</param>
    /// <returns>The read_binary_base64 tool offered to the model.</returns>
    let createReadBinaryTool (workspace: IWorkspace) : AIFunction =
        createTool workspace "ReadBinaryAsync" readBinaryName readBinaryDescription

    /// Creates the write_binary_base64 tool: base64 writes capped at 10
    /// MiB by default that refuse the read-only input/ area.
    /// <param name="workspace">The bound workspace the tool runs in. Must not be null.</param>
    /// <returns>The write_binary_base64 tool offered to the model.</returns>
    let createWriteBinaryTool (workspace: IWorkspace) : AIFunction =
        createTool workspace "WriteBinaryAsync" writeBinaryName writeBinaryDescription
