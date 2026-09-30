// SPDX-License-Identifier: Apache-2.0
module Dot.DotExport

open System
open System.Collections.Generic
open System.IO
open System.Net
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Legate

// Dot transcript export and session summary (issue 309): /export pages the
// journal to JSONL (one $type-polymorphic event per line) or escaped static
// HTML, fenced under the working directory; /session counts one journal
// pass. Never prices: the runtime carries no cost by construction, so the
// summary reports tokens only.

// ──────────────────────────────────────────────────────────────────────────
// Path fencing (mirrors the CodingTools workspace fence)

/// Validates an export target against the workspace-relative path rules:
// /// relative, no drive prefix or leading slash, no trailing slash, and no
// /// empty, dot, backslash, or NUL segments.
// /// <param name="target">The export target the user typed.</param>
// /// <returns>The validated target, unchanged.</returns>
let private validateTarget (target: string) : string =
    if isNull (box target) then
        raise (ArgumentException("The /export target must not be null.", "target"))

    if target = "" then
        raise (ArgumentException("The /export target must not be empty.", "target"))

    if target.Length >= 2 && target[1] = ':' then
        raise (ArgumentException("The /export target must not carry a drive prefix.", "target"))

    if target.StartsWith('/') || target.StartsWith('\\') then
        raise (ArgumentException("The /export target must be relative, without a leading slash.", "target"))

    if target.EndsWith('/') then
        raise (ArgumentException("The /export target must not end with a slash.", "target"))

    for segment in target.Split('/') do
        if segment = "" then
            raise (ArgumentException("The /export target must not contain empty segments.", "target"))

        if segment = "." || segment = ".." then
            raise (ArgumentException("The /export target must not contain '.' or '..' segments.", "target"))

        if segment.Contains('\\') then
            raise (ArgumentException("The /export target must not contain backslashes.", "target"))

        if segment.Contains('\u0000') then
            raise (ArgumentException("The /export target must not contain NUL characters.", "target"))

    target

/// Resolves one link component to an absolute path when the path is a
/// symbolic link, or null when it is not a link, does not exist, or cannot
/// be resolved (mirrors the CodingTools link-target walk: DotExport
/// compiles before CodingTools, so the helper is duplicated here rather
/// than called).
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

/// Resolves an export target under the working directory, refusing escape:
/// the resolved path must stay inside the resolved root.
/// <param name="workingDirectory">The working directory exports land under.</param>
/// <param name="target">The export target the user typed.</param>
/// <returns>The absolute export path, or the refusal reason.</returns>
let resolveExportPath (workingDirectory: string) (target: string) : Result<string, string> =
    if String.IsNullOrWhiteSpace workingDirectory then
        Error "The working directory is unknown: /export cannot resolve its target."
    else
        try
            let relative = validateTarget target
            let resolvedRoot = Path.GetFullPath workingDirectory

            let comparison =
                if OperatingSystem.IsWindows() then
                    StringComparison.OrdinalIgnoreCase
                else
                    StringComparison.Ordinal

            let escapeMessage =
                "The /export target must stay inside the working directory: the resolved path escapes it."

            let isInside (fullPath: string) =
                fullPath.Equals(resolvedRoot, comparison)
                || fullPath.StartsWith(resolvedRoot + string Path.DirectorySeparatorChar, comparison)

            let mutable current = resolvedRoot
            let mutable escaped = false

            for segment in relative.Split('/') do
                if not escaped then
                    current <- Path.Combine(current, segment)

                    if not (isInside current) then
                        escaped <- true
                    else
                        let mutable hops = 0
                        let mutable settled = false

                        while not settled && not escaped do
                            match resolveLinkTarget current with
                            | null -> settled <- true
                            | linkTarget ->
                                hops <- hops + 1

                                if hops > 40 then escaped <- true
                                elif not (isInside linkTarget) then escaped <- true
                                else current <- linkTarget

            if escaped then
                Error escapeMessage
            else
                let fullPath = Path.GetFullPath current

                if isInside fullPath then
                    Ok fullPath
                else
                    Error escapeMessage
        with :? ArgumentException as invalid ->
            Error $"The /export target is invalid: {invalid.Message}"

// ──────────────────────────────────────────────────────────────────────────
// Journal reads

/// Pages the session journal to its end: ReadEventsAsync from the first
/// event in 100-event pages, following the /tree precedent.
/// <param name="client">The session client.</param>
/// <param name="sessionId">The session whose journal to read.</param>
/// <param name="cancellationToken">Abandons the read.</param>
/// <returns>The journaled events, in sequence order.</returns>
let readAllEventsAsync
    (client: SessionClient)
    (sessionId: SessionId)
    (cancellationToken: CancellationToken)
    : Task<IReadOnlyList<SessionEvent>> =
    ArgumentNullException.ThrowIfNull(client)

    task {
        let collected = ResizeArray<SessionEvent>()
        let mutable cursor = 0L
        let mutable paging = true

        while paging do
            let! page = SessionClientOperations.ReadEventsAsync(client, sessionId, cursor, 100, cancellationToken)

            if isNull (box page) || page.Count = 0 then
                paging <- false
            else
                for evt in page do
                    if not (isNull (box evt)) then
                        collected.Add(evt)

                        if evt.Sequence.HasValue && evt.Sequence.Value > cursor then
                            cursor <- evt.Sequence.Value

                if page.Count < 100 then
                    paging <- false

        return collected :> IReadOnlyList<SessionEvent>
    }

/// Summarises one journal pass: accepted prompts (started turns) plus
/// folded follow-ups as messages, completed turns, and the UsageEvent token
/// sums. Tool calls and live usage are absent from the journal on this
/// path (the runtime journals neither per-call tool events nor usage
/// checkpoints for facade-driven turns), so the REPL adds its settled-turn
/// accumulation on top; never prices.
/// <param name="events">The journaled events.</param>
/// <returns>User messages, completed turns, journal input tokens, and journal output tokens.</returns>
let summarize (events: SessionEvent seq) : int * int * int64 * int64 =
    let mutable messages = 0
    let mutable turns = 0
    let mutable inputTokens = 0L
    let mutable outputTokens = 0L

    if not (isNull (box events)) then
        for evt in events do
            if not (isNull (box evt)) then
                if evt :? UserMessageEvent || evt :? TurnStartedEvent then
                    messages <- messages + 1
                elif evt :? TurnCompletedEvent then
                    turns <- turns + 1
                elif evt :? UsageEvent then
                    let usage = evt :?> UsageEvent
                    inputTokens <- inputTokens + usage.InputTokens
                    outputTokens <- outputTokens + usage.OutputTokens

    (messages, turns, inputTokens, outputTokens)

// ──────────────────────────────────────────────────────────────────────────
// Writers

/// The JSON options exports serialise with: the plain defaults the durable
/// layer speaks, so every line carries its $type discriminator. Reused by
/// the --mode json stdout stream, so the pipe and the file share one wire
/// format.
/// <returns>The export JSON options.</returns>
let exportOptions () : JsonSerializerOptions = JsonSerializerOptions()

/// Serializes one journaled event to its JSONL line with the export
/// options: one $type-polymorphic event per line on stdout in --mode json.
/// <param name="evt">The event to serialize. Must not be null.</param>
/// <returns>The JSON line.</returns>
let toJsonLine (evt: SessionEvent) : string =
    ArgumentNullException.ThrowIfNull(evt)
    let options = exportOptions ()
    JsonSerializer.Serialize(box evt, typeof<SessionEvent>, options)

/// Writes the journal as JSONL: one $type-polymorphic event per line,
/// overwriting any existing content like write_file.
/// <param name="path">The absolute export path. Must not be null.</param>
/// <param name="events">The journaled events.</param>
/// <param name="cancellationToken">Abandons the write.</param>
let writeJsonlAsync (path: string) (events: SessionEvent seq) (cancellationToken: CancellationToken) : Task =
    ArgumentNullException.ThrowIfNull(path)

    task {
        use writer = new StreamWriter(path, false, Encoding.UTF8)

        if not (isNull (box events)) then
            for evt in events do
                cancellationToken.ThrowIfCancellationRequested()

                if not (isNull (box evt)) then
                    do! writer.WriteLineAsync(toJsonLine evt)

        do! writer.FlushAsync()
    }

/// Renders one event's summary cell for the HTML export.
/// <param name="evt">The event to summarise.</param>
/// <returns>The summary text.</returns>
let private eventSummary (evt: SessionEvent) : string =
    match evt with
    | :? ToolCallStartedEvent as started when not (isNull (box started)) ->
        $"tool {started.ToolName} id {started.ToolCallId}"
    | :? ToolCallCompletedEvent as completed when not (isNull (box completed)) ->
        match Option.ofObj completed.Error with
        | Some reason -> $"settled with error: {reason}"
        | None -> "settled"
    | :? UsageEvent as usage when not (isNull (box usage)) ->
        $"input tokens {usage.InputTokens}, output tokens {usage.OutputTokens}"
    | :? TurnFailedEvent as failed when not (isNull (box failed)) -> $"reason: {failed.Reason}"
    | :? TurnAbortedEvent as aborted when not (isNull (box aborted)) -> $"reason: {aborted.Reason}"
    | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
        $"tool {asked.ToolName} id {asked.RequestId}"
    | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) ->
        $"id {resolved.RequestId} decision {resolved.Decision}"
    | :? QuestionAskedEvent as asked when not (isNull (box asked)) -> $"id {asked.QuestionId}: {asked.Question}"
    | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) -> $"id {answered.QuestionId}"
    | :? CompactedEvent as compacted when not (isNull (box compacted)) ->
        $"before {compacted.BeforeEstimate} after {compacted.AfterEstimate}"
    | :? CompactionFailedEvent as failed when not (isNull (box failed)) -> $"reason: {failed.Reason}"
    | :? UserMessageEvent -> "user message"
    | _ -> ""

/// Writes the journal as escaped static HTML: a table of sequence, event
/// type, timestamp, and summary, overwriting any existing content.
/// <param name="path">The absolute export path. Must not be null.</param>
/// <param name="events">The journaled events.</param>
/// <param name="cancellationToken">Abandons the write.</param>
let writeHtmlAsync (path: string) (events: SessionEvent seq) (cancellationToken: CancellationToken) : Task =
    ArgumentNullException.ThrowIfNull(path)

    task {
        let builder = StringBuilder()
        builder.AppendLine("<!DOCTYPE html>") |> ignore
        builder.AppendLine("<html>") |> ignore

        builder.AppendLine("<head><meta charset=\"utf-8\"><title>Dot transcript export</title></head>")
        |> ignore

        builder.AppendLine("<body>") |> ignore
        builder.AppendLine("<h1>Dot transcript export</h1>") |> ignore
        builder.AppendLine("<table>") |> ignore

        builder.AppendLine("<tr><th>sequence</th><th>event</th><th>timestamp</th><th>summary</th></tr>")
        |> ignore

        if not (isNull (box events)) then
            for evt in events do
                cancellationToken.ThrowIfCancellationRequested()

                if not (isNull (box evt)) then
                    let sequence =
                        if evt.Sequence.HasValue then
                            evt.Sequence.Value.ToString()
                        else
                            "-"

                    builder.Append("<tr><td>") |> ignore
                    builder.Append(WebUtility.HtmlEncode sequence) |> ignore
                    builder.Append("</td><td>") |> ignore
                    builder.Append(WebUtility.HtmlEncode(evt.GetType().Name)) |> ignore
                    builder.Append("</td><td>") |> ignore
                    builder.Append(WebUtility.HtmlEncode(evt.Timestamp.ToString("o"))) |> ignore
                    builder.Append("</td><td>") |> ignore
                    builder.Append(WebUtility.HtmlEncode(eventSummary evt)) |> ignore
                    builder.AppendLine("</td></tr>") |> ignore

        builder.AppendLine("</table>") |> ignore
        builder.AppendLine("</body>") |> ignore
        builder.AppendLine("</html>") |> ignore

        do! File.WriteAllTextAsync(path, builder.ToString(), cancellationToken)
    }
