// SPDX-License-Identifier: Apache-2.0
module LegateCli.Engine

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate

// REPL engine over the session client facade: prompt loop, Subscribe
// streaming to the writer, permission/question console prompts, and the
// /compact, /abort, /agent (stubbed per R2), /new, /sessions, /resume, and
// /quit commands. Transport-agnostic: the host wires the chat client,
// tools, and policy. Reads and writes through the given reader/writer so
// scripted transports drive it without a console.

/// How the CLI was asked to start.
type CliStart =
    {
        /// Provider id for real mode; ignored scripted.
        Provider: string
        /// Model reference (provider/model) or null for the provider default.
        Model: string | null
        /// mcp.json path or null to skip the attach.
        McpPath: string | null
        /// Session id to attach at startup or null to open.
        Resume: string | null
        /// Scripted transports instead of live providers.
        Scripted: bool
        /// Settle wait bound in minutes.
        WaitMinutes: float
    }

/// Parses the CLI arguments into a start plan. Unknown flags fail with a
/// usage error naming the flag; missing values fail naming the flag.
let parseArgs (argv: string[]) : CliStart =
    let mutable provider = "anthropic"
    let mutable model: string | null = null
    let mutable mcp: string | null = null
    let mutable resume: string | null = null
    let mutable scripted = false
    let mutable waitMinutes = 5.0

    let mutable index = 0

    let take (flag: string) : string =
        index <- index + 1

        if index >= argv.Length then
            raise (ArgumentException($"The {flag} flag needs a value.", flag))

        argv[index]

    while index < argv.Length do
        match argv[index] with
        | "--provider" -> provider <- take "--provider"
        | "--model" -> model <- take "--model"
        | "--mcp" -> mcp <- take "--mcp"
        | "--resume" -> resume <- take "--resume"
        | "--scripted" -> scripted <- true
        | "--wait-minutes" ->
            let raw = take "--wait-minutes"

            match Double.TryParse(raw) with
            | true, minutes when minutes > 0.0 -> waitMinutes <- minutes
            | _ ->
                raise (
                    ArgumentException("The --wait-minutes flag needs a positive number of minutes.", "--wait-minutes")
                )
        | "--help"
        | "-h" ->
            raise (
                ArgumentException(
                    "Usage: LegateCli [--provider <id>] [--model <provider/model>] [--mcp <path>] [--resume <session-id>] [--scripted] [--wait-minutes <n>]",
                    "--help"
                )
            )
        | unknown -> raise (ArgumentException($"Unknown flag '{unknown}'.", unknown))

        index <- index + 1

    {
        Provider = provider
        Model = model
        McpPath = mcp
        Resume = resume
        Scripted = scripted
        WaitMinutes = waitMinutes
    }

/// One REPL session: its id, title, and event cursor.
type private ReplSession =
    {
        Id: SessionId
        Title: string
        mutable Cursor: int64
    }

/// The REPL engine: drives one current session through prompt, stream,
/// reply, and settle over the given reader/writer.
type Engine(client: SessionClient, reader: System.IO.TextReader, writer: System.IO.TextWriter, waitBound: TimeSpan) =

    do
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(reader)
        ArgumentNullException.ThrowIfNull(writer)

        if waitBound <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof waitBound, "The settle wait bound must be positive."))

    let sessions = ResizeArray<ReplSession>()
    let mutable current = -1

    let line (text: string) : unit =
        writer.WriteLine(text)
        writer.Flush()

    let currentSession () : ReplSession =
        if current < 0 || current >= sessions.Count then
            raise (InvalidOperationException("The REPL has no current session."))

        sessions[current]

    /// Finds a session by 1-based open index or session id text.
    /// <param name="text">The index or id the user typed.</param>
    /// <returns>The matching session index, or -1.</returns>
    let findSession (text: string) : int =
        match Int32.TryParse(text.Trim()) with
        | true, number when number >= 1 && number <= sessions.Count -> number - 1
        | _ ->
            let mutable parsed = Unchecked.defaultof<SessionId>

            if SessionId.TryParse(text, &parsed) then
                sessions
                |> Seq.tryFindIndex (fun session -> session.Id.Equals(parsed))
                |> Option.defaultValue -1
            else
                -1

    /// Renders one journaled event as a stable single line.
    /// <param name="evt">The event to render.</param>
    /// <returns>The rendered line.</returns>
    let renderEvent (evt: SessionEvent) : string =
        let sequence =
            if evt.Sequence.HasValue then
                evt.Sequence.Value.ToString()
            else
                "-"

        let detail =
            match evt with
            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
                $" tool={asked.ToolName} id={asked.RequestId}"
            | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) ->
                $" id={resolved.RequestId} decision={resolved.Decision}"
            | :? QuestionAskedEvent as asked when not (isNull (box asked)) ->
                $" id={asked.QuestionId} question={asked.Question}"
            | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) -> $" id={answered.QuestionId}"
            | :? TurnFailedEvent as failed when not (isNull (box failed)) -> $" reason={failed.Reason}"
            | :? TurnAbortedEvent as aborted when not (isNull (box aborted)) -> $" reason={aborted.Reason}"
            | :? CompactedEvent as compacted when not (isNull (box compacted)) ->
                $" before={compacted.BeforeEstimate} after={compacted.AfterEstimate}"
            | :? CompactionFailedEvent as failed when not (isNull (box failed)) -> $" reason={failed.Reason}"
            | :? UserMessageEvent -> " user-message"
            | _ -> ""

        $"EVENT seq={sequence} {evt.GetType().Name}{detail}"

    /// Answers one permission request from the console.
    /// <param name="asked">The pending permission request.</param>
    /// <param name="cancellationToken">Abandons the reply.</param>
    let answerPermission (asked: PermissionRequestedEvent) (cancellationToken: CancellationToken) : Task =
        task {
            line $"PERMISSION tool={asked.ToolName} id={asked.RequestId} [a]llow once, allow for [s]ession, [d]eny:"

            let! rawChoice = reader.ReadLineAsync()

            let choiceText =
                match rawChoice with
                | null -> ""
                | text -> text.Trim().ToLowerInvariant()

            let decision =
                match choiceText with
                | "s"
                | "session" -> PermissionDecisionKind.AllowForSession
                | "d"
                | "deny" -> PermissionDecisionKind.Deny
                | _ -> PermissionDecisionKind.AllowOnce

            let! _ =
                SessionClientOperations.ReplyAsync(
                    client,
                    asked.SessionId,
                    PermissionDecision(asked.RequestId, decision),
                    cancellationToken
                )

            ()
        }

    /// Answers one agent question from the console.
    /// <param name="asked">The pending question.</param>
    /// <param name="cancellationToken">Abandons the reply.</param>
    let answerQuestion (asked: QuestionAskedEvent) (cancellationToken: CancellationToken) : Task =
        task {
            line $"QUESTION id={asked.QuestionId}: {asked.Question}"
            line "ANSWER:"

            let! rawAnswer = reader.ReadLineAsync()

            let answer =
                match rawAnswer with
                | null -> ""
                | text -> text

            let! _ =
                SessionClientOperations.ReplyAsync(
                    client,
                    asked.SessionId,
                    QuestionAnswer(asked.QuestionId, answer),
                    cancellationToken
                )

            ()
        }

    /// Streams one turn's events until the subscriber is cancelled,
    /// answering permission requests and questions inline.
    /// <param name="session">The session streaming.</param>
    /// <param name="cancellationToken">Stops the stream.</param>
    member private _.StreamAsync(session: ReplSession, cancellationToken: CancellationToken) : Task =
        task {
            let stream =
                SessionClientOperations.Subscribe(client, session.Id, session.Cursor, cancellationToken)

            let enumerator = stream.GetAsyncEnumerator(cancellationToken)

            try
                let mutable go = true

                while go do
                    try
                        let! has = enumerator.MoveNextAsync().AsTask()

                        if not has then
                            go <- false
                        else
                            let evt = enumerator.Current

                            if not (isNull (box evt)) then
                                if evt.Sequence.HasValue && evt.Sequence.Value > session.Cursor then
                                    session.Cursor <- evt.Sequence.Value

                                line (renderEvent evt)

                                match evt with
                                | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
                                    do! answerPermission asked cancellationToken
                                | :? QuestionAskedEvent as asked when not (isNull (box asked)) ->
                                    do! answerQuestion asked cancellationToken
                                | _ -> ()
                    with :? OperationCanceledException ->
                        go <- false
            finally
                try
                    enumerator.DisposeAsync().AsTask() |> ignore
                with _ ->
                    ()
        }

    /// Prompts the current session and streams the turn to the result.
    /// <param name="session">The session to prompt.</param>
    /// <param name="text">The user text.</param>
    /// <param name="cancellationToken">Abandons the turn.</param>
    member private this.PromptFlowAsync
        (session: ReplSession, text: string, cancellationToken: CancellationToken)
        : Task =
        task {
            // Queue the settle waiter before the prompt lands: a settle
            // with no waiter only records.
            let wait =
                SessionClientOperations.WaitForSettleAsync(client, session.Id, waitBound, cancellationToken)

            let! _ =
                SessionClientOperations.PromptAsync(
                    client,
                    session.Id,
                    UserMessage.Text text,
                    DeliveryMode.Queue,
                    cancellationToken
                )

            use streamCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            let stream = this.StreamAsync(session, streamCts.Token)

            try
                let! result = wait
                line $"RESULT {result.Status}"

                if not (String.IsNullOrEmpty result.AssistantText) then
                    line result.AssistantText

                line "END-RESULT"
            finally
                try
                    streamCts.Cancel()
                with _ ->
                    ()

                // Best-effort join: the cancel above already unwinds the
                // enumerator, so a stuck stream never blocks the REPL.
                try
                    stream.Wait(TimeSpan.FromSeconds 5.0) |> ignore
                with _ ->
                    ()
        }

    /// Opens a session and makes it current.
    /// <param name="title">The session title.</param>
    /// <param name="cancellationToken">Abandons the open.</param>
    member private _.OpenAsync(title: string, cancellationToken: CancellationToken) : Task =
        task {
            let options = SessionOptions()
            options.Title <- title

            let! created = SessionClientOperations.OpenSessionAsync(client, AgentId.New(), options, cancellationToken)

            sessions.Add(
                {
                    Id = created.Id
                    Title = created.Title
                    Cursor = 0L
                }
            )

            current <- sessions.Count - 1
            line $"SESSION {created.Id} {created.Title}"
        }

    /// Attaches an existing in-process session by id or open index.
    /// <param name="text">The id or 1-based index.</param>
    /// <param name="cancellationToken">Abandons the probe.</param>
    /// <returns>True when the attach landed.</returns>
    member private _.AttachAsync(text: string, cancellationToken: CancellationToken) : Task<bool> =
        task {
            let found = findSession text

            if found >= 0 then
                current <- found
                line $"RESUMED {sessions[found].Id}"
                return true
            else
                let mutable parsed = Unchecked.defaultof<SessionId>

                if not (SessionId.TryParse(text, &parsed)) then
                    line $"RESUME-FAILED '{text}' is not a session id or open index."
                    return false
                else
                    try
                        // Probe the journal: unknown sessions throw
                        // SessionNotFoundException here.
                        let! _ = SessionClientOperations.ReadEventsAsync(client, parsed, 0L, 1, cancellationToken)

                        match sessions |> Seq.tryFindIndex (fun session -> session.Id.Equals(parsed)) with
                        | Some known -> current <- known
                        | None ->
                            sessions.Add({ Id = parsed; Title = ""; Cursor = 0L })
                            current <- sessions.Count - 1

                        line $"RESUMED {parsed}"
                        return true
                    with :? SessionNotFoundException ->
                        line $"RESUME-FAILED no session {parsed} in this process."
                        return false
        }

    /// Handles one input line. Returns false when the REPL should exit.
    /// <param name="inputLine">The line read, or null at end of input.</param>
    /// <param name="cancellationToken">Abandons the turn.</param>
    /// <returns>False when the REPL should exit.</returns>
    member private this.HandleAsync(inputLine: string | null, cancellationToken: CancellationToken) : Task<bool> =
        task {
            match inputLine with
            | null -> return false
            | line -> return! this.HandleLineAsync(line, cancellationToken)
        }

    /// Handles one trimmed input line. Returns false when the REPL should exit.
    /// <param name="input">The trimmed input line.</param>
    /// <param name="cancellationToken">Abandons the turn.</param>
    /// <returns>False when the REPL should exit.</returns>
    member private this.HandleLineAsync(input: string, cancellationToken: CancellationToken) : Task<bool> =
        task {
            let text = input.Trim()

            if text = "" then
                return true
            elif text = "/quit" || text = "/exit" then
                return false
            elif text = "/compact" then
                try
                    let session = currentSession ()

                    let! outcome = SessionClientOperations.CompactAsync(client, session.Id, cancellationToken)

                    match outcome with
                    | :? SessionCompacted as compacted ->
                        line $"COMPACT completed {compacted.BeforeEstimate}->{compacted.AfterEstimate}"
                    | :? SessionCompactDeferred -> line "COMPACT deferred"
                    | :? SessionCompactFenced -> line "COMPACT fenced"
                    | _ -> line "COMPACT not-needed"
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif text = "/abort" then
                try
                    let session = currentSession ()

                    do!
                        SessionClientOperations.AbortAsync(
                            client,
                            session.Id,
                            StopCause.ExplicitAbort,
                            "repl /abort",
                            cancellationToken
                        )

                    line "ABORTED"
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif text.StartsWith("/agent", StringComparison.Ordinal) then
                let name = text.Substring("/agent".Length).Trim()
                line $"AGENT-STUBBED {name}: agent switching arrives with Wave 3."
                return true
            elif text.StartsWith("/new", StringComparison.Ordinal) then
                let title = text.Substring("/new".Length).Trim()

                try
                    do! this.OpenAsync((if title = "" then "repl" else title), cancellationToken)
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif text = "/sessions" then
                line $"SESSIONS {sessions.Count}"

                for index = 0 to sessions.Count - 1 do
                    let marker = if index = current then "*" else " "
                    line $"{marker} [{index + 1}] {sessions[index].Id} {sessions[index].Title}"

                return true
            elif text.StartsWith("/resume", StringComparison.Ordinal) then
                let target = text.Substring("/resume".Length).Trim()

                try
                    let! _ = this.AttachAsync(target, cancellationToken)
                    ()
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif text.StartsWith("/") then
                line $"UNKNOWN-COMMAND {text}"
                return true
            else
                try
                    do! this.PromptFlowAsync(currentSession (), text, cancellationToken)
                with error ->
                    line $"ERROR {error.Message}"

                return true
        }

    /// Runs the REPL until /quit or end of input.
    /// <param name="resume">The session id to attach at startup, or null to open.</param>
    /// <param name="cancellationToken">Abandons the REPL.</param>
    /// <returns>The process exit code.</returns>
    member this.RunAsync(resume: string | null, cancellationToken: CancellationToken) : Task<int> =
        task {
            line "Legate CLI (InMemory session store: --resume works within this process only)."

            match resume with
            | null -> do! this.OpenAsync("repl", cancellationToken)
            | raw when String.IsNullOrWhiteSpace raw -> do! this.OpenAsync("repl", cancellationToken)
            | raw ->
                let! attached = this.AttachAsync(raw.Trim(), cancellationToken)

                if not attached then
                    do! this.OpenAsync("repl", cancellationToken)

            line "Commands: /new [title], /sessions, /resume <id-or-index>, /compact, /abort, /agent <name>, /quit."

            let mutable go = true

            while go do
                writer.Write("> ")
                writer.Flush()

                let! inputLine = reader.ReadLineAsync()
                let! keepGoing = this.HandleAsync(inputLine, cancellationToken)
                go <- keepGoing

            return 0
        }
