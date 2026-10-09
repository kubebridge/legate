// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ConversationEvidenceTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Xunit

// Conversation evidence (issue 380): versioned provider-required tool
// evidence (call id/name/arguments plus paired result content) with
// truthful current-format recovery and transcript derivation. Every fact
// below pins one ledger validation clause: contract round-trip with
// provider-format field assertions (not merely a successful response),
// JournalWriter redaction/bounds for the new fields, recovery pairing with
// explicit rejection (never fabrication), derivation correlation with
// continuation/report dedupe and transient reasoning, store JSON payload
// fidelity (Sqlite/Postgres persist JSON text, so STJ round-trip plus the
// shared conformance suite cover Restamp), and public DI facade coverage
// through Subscribe/replay-adjacent reads. Production suspendable Inject
// folds still journal nothing (SessionPermissions hardcodes ignore); that
// gap is the PR's stated residual risk, covered here at the event level.

let private sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let private turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAX"
let private stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
let private noSequence = Unchecked.defaultof<Nullable<int64>>

// Unchecked.defaultof<string> rather than a bare null literal: under
// Nullable=enable the literal trips F# nullness checking on string-typed
// parameters (the EventsTests precedent).
let private nullString = Unchecked.defaultof<string>

let private options = JsonSerializerOptions()

let private roundTrip (event: SessionEvent) : SessionEvent =
    let json = JsonSerializer.Serialize(event, options)

    match JsonSerializer.Deserialize(json, typeof<SessionEvent>, options) with
    | null -> failwith "The tool event round-trip deserialised to null."
    | back -> back :?> SessionEvent

let private userEvent (text: string) : SessionEvent =
    UserMessageEvent(sessionId, turnId, noSequence, stamp, UserMessage.Text text) :> SessionEvent

let private callNameOf (message: ChatMessage) : string =
    let mutable name: string = Unchecked.defaultof<string>

    if not (isNull (box message)) && not (isNull (box message.Contents)) then
        for content in message.Contents do
            if isNull (box name) && not (isNull (box content)) then
                match content with
                | :? FunctionCallContent as call when not (isNull (box call)) -> name <- call.Name
                | _ -> ()

    name

let private resultTextOf (message: ChatMessage) : string =
    let mutable text: string = Unchecked.defaultof<string>

    if not (isNull (box message)) && not (isNull (box message.Contents)) then
        for content in message.Contents do
            if isNull (box text) && not (isNull (box content)) then
                match content with
                | :? FunctionResultContent as result when not (isNull (box result)) ->
                    match result.Result with
                    | :? string as value -> text <- value
                    | _ -> ()
                | _ -> ()

    text

// ────────────────── Contract ──────────────────

[<Fact>]
let ``Tool evidence round-trips arguments and result content`` () =
    let started =
        ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", """{"path":"a.txt"}""")

    let completed =
        ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, "file text")

    match roundTrip (started :> SessionEvent) with
    | :? ToolCallStartedEvent as back ->
        back.ToolCallId |> should equal "call-1"
        back.ToolName |> should equal "read_file"
        back.ArgumentsJson |> should equal """{"path":"a.txt"}"""
    | other -> failwith $"Expected a tool start but rebuilt '{other.GetType().Name}'."

    match roundTrip (completed :> SessionEvent) with
    | :? ToolCallCompletedEvent as back ->
        back.ToolCallId |> should equal "call-1"
        back.Error |> should equal nullString
        back.ResultText |> should equal "file text"
    | other -> failwith $"Expected a tool completion but rebuilt '{other.GetType().Name}'."

[<Fact>]
let ``Recovery asserts provider call and result content, not merely success`` () =
    let events =
        ResizeArray<SessionEvent>(
            [|
                userEvent "hello"
                ToolCallStartedEvent(
                    sessionId,
                    turnId,
                    noSequence,
                    stamp,
                    "call-1",
                    "read_file",
                    """{"path":"a.txt"}"""
                )
                :> SessionEvent
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, "file text")
                :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecover events with
    | Error rejection -> failwith $"Expected recovery but got '{rejection}'."
    | Ok history ->
        history.Count |> should equal 3
        history[0].Role |> should equal ChatRole.User
        history[1].Role |> should equal ChatRole.Assistant
        callNameOf history[1] |> should equal "read_file"
        history[2].Role |> should equal ChatRole.Tool
        resultTextOf history[2] |> should equal "file text"

// ────────────────── JournalWriter ──────────────────

[<Fact>]
let ``JournalWriter redacts secrets inside tool arguments and results`` () =
    let started =
        ToolCallStartedEvent(
            sessionId,
            turnId,
            noSequence,
            stamp,
            "call-1",
            "read_file",
            """{"key":"sk-ant-secret12345678"}"""
        )
        :> SessionEvent

    let completed =
        ToolCallCompletedEvent(
            sessionId,
            turnId,
            noSequence,
            stamp,
            "call-1",
            nullString,
            "token xoxb-secret-value-here"
        )
        :> SessionEvent

    match JournalWriter.sanitizeEvent started with
    | :? ToolCallStartedEvent as clean ->
        match clean.ArgumentsJson with
        | null -> failwith "Expected redacted arguments, not null."
        | args ->
            args.Contains("sk-ant-secret12345678") |> should equal false
            args.Contains(JournalWriter.RedactedText) |> should equal true
    | other -> failwith $"Expected a tool start but sanitized '{other.GetType().Name}'."

    match JournalWriter.sanitizeEvent completed with
    | :? ToolCallCompletedEvent as clean ->
        match clean.ResultText with
        | null -> failwith "Expected redacted result, not null."
        | result ->
            result.Contains("xoxb-secret-value-here") |> should equal false
            result.Contains(JournalWriter.RedactedText) |> should equal true
    | other -> failwith $"Expected a tool completion but sanitized '{other.GetType().Name}'."

[<Fact>]
let ``JournalWriter bounds oversized tool arguments and results without dropping`` () =
    let bigArgs = String('a', JournalWriter.MaxTextChars + 100)
    let bigResult = String('b', JournalWriter.MaxTextChars + 100)

    let started =
        ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", bigArgs) :> SessionEvent

    let completed =
        ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, bigResult) :> SessionEvent

    match JournalWriter.boundEvent started with
    | :? ToolCallStartedEvent as bounded ->
        bounded.ToolCallId |> should equal "call-1"
        bounded.ToolName |> should equal "read_file"

        match bounded.ArgumentsJson with
        | null -> failwith "Expected bounded arguments, not null."
        | args -> args.Contains(JournalWriter.TruncationMarker) |> should equal true
    | other -> failwith $"Expected a tool start but bounded '{other.GetType().Name}'."

    match JournalWriter.boundEvent completed with
    | :? ToolCallCompletedEvent as bounded ->
        bounded.ToolCallId |> should equal "call-1"

        match bounded.ResultText with
        | null -> failwith "Expected bounded result, not null."
        | result -> result.Contains(JournalWriter.TruncationMarker) |> should equal true
    | other -> failwith $"Expected a tool completion but bounded '{other.GetType().Name}'."

// ────────────────── Recovery ──────────────────

[<Fact>]
let ``Recovery preserves suspend and resume ordering around tool evidence`` () =
    let events =
        ResizeArray<SessionEvent>(
            [|
                userEvent "hello"
                PermissionRequestedEvent(sessionId, turnId, noSequence, stamp, "req-1", "write_file") :> SessionEvent
                PermissionResolvedEvent(sessionId, turnId, noSequence, stamp, "req-1", PermissionDecisionKind.AllowOnce)
                :> SessionEvent
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "write_file", """{"path":"b"}""")
                :> SessionEvent
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, "wrote")
                :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecover events with
    | Error rejection -> failwith $"Expected recovery but got '{rejection}'."
    | Ok history ->
        history.Count |> should equal 5
        history[0].Role |> should equal ChatRole.User
        history[1].Role |> should equal ChatRole.System
        history[2].Role |> should equal ChatRole.System
        history[3].Role |> should equal ChatRole.Assistant
        history[4].Role |> should equal ChatRole.Tool

[<Fact>]
let ``Recovery preserves partial-output failure content`` () =
    let events =
        ResizeArray<SessionEvent>(
            [|
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "run", """{}""") :> SessionEvent
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", "timeout", "partial bytes")
                :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecover events with
    | Error rejection -> failwith $"Expected recovery but got '{rejection}'."
    | Ok history ->
        history.Count |> should equal 2
        resultTextOf history[1] |> should equal "partial bytes"

[<Fact>]
let ``Recovery rejects missing arguments instead of fabricating the call`` () =
    let events =
        ResizeArray<SessionEvent>(
            [|
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", nullString)
                :> SessionEvent
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, "text")
                :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecover events with
    | Error(ConversationRecovery.MissingToolArguments callId) -> callId |> should equal "call-1"
    | Error other -> failwith $"Expected missing-arguments but got '{other}'."
    | Ok _ -> failwith "Expected rejection for missing arguments."

[<Fact>]
let ``Recovery rejects malformed arguments and unpaired completions`` () =
    let malformed =
        ResizeArray<SessionEvent>(
            [|
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", "not-json")
                :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecover malformed with
    | Error(ConversationRecovery.InvalidToolArguments callId) -> callId |> should equal "call-1"
    | Error other -> failwith $"Expected invalid-arguments but got '{other}'."
    | Ok _ -> failwith "Expected rejection for malformed arguments."

    let unpaired =
        ResizeArray<SessionEvent>(
            [|
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-9", nullString, "text")
                :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecover unpaired with
    | Error(ConversationRecovery.UnpairedToolCompletion callId) -> callId |> should equal "call-9"
    | Error other -> failwith $"Expected unpaired-completion but got '{other}'."
    | Ok _ -> failwith "Expected rejection for an unpaired completion."

[<Fact>]
let ``Recovery rejects a success without result text`` () =
    let events =
        ResizeArray<SessionEvent>(
            [|
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", """{}""")
                :> SessionEvent
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, nullString)
                :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecover events with
    | Error(ConversationRecovery.MissingToolResult callId) -> callId |> should equal "call-1"
    | Error other -> failwith $"Expected missing-result but got '{other}'."
    | Ok _ -> failwith "Expected rejection for a success without result text."

[<Fact>]
let ``Recovery drops an aborted start and dedupes repeated reports`` () =
    let events =
        ResizeArray<SessionEvent>(
            [|
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-hung", "run", """{}""") :> SessionEvent
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", """{"path":"a"}""")
                :> SessionEvent
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", """{"path":"a"}""")
                :> SessionEvent
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, "text")
                :> SessionEvent
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, "text")
                :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecover events with
    | Error rejection -> failwith $"Expected recovery but got '{rejection}'."
    | Ok history ->
        let calls =
            history
            |> Seq.sumBy (fun message ->
                if isNull (box message) || isNull (box message.Contents) then
                    0
                else
                    message.Contents
                    |> Seq.sumBy (fun content ->
                        match content with
                        | :? FunctionCallContent -> 1
                        | _ -> 0))

        calls |> should equal 1

[<Fact>]
let ``Recovery is invariant to journal chunking`` () =
    let journal =
        [|
            userEvent "hello"
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "Hel") :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "lo") :> SessionEvent
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", """{"path":"a"}""")
            :> SessionEvent
            ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, "call-1", "streamed") :> SessionEvent
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, "file text")
            :> SessionEvent
        |]

    let recoverSlice (slices: SessionEvent list list) =
        let flat =
            slices
            |> List.concat
            |> ResizeArray
            |> fun rows -> rows :> IReadOnlyList<SessionEvent>

        match ConversationRecovery.tryRecover flat with
        | Error rejection -> failwith $"Expected recovery but got '{rejection}'."
        | Ok history -> history.Count

    let whole = recoverSlice [ Array.toList journal ]

    let split =
        recoverSlice
            [
                journal[0..1] |> Array.toList
                journal[2..] |> Array.toList
            ]

    let single = recoverSlice (journal |> Array.toList |> List.map List.singleton)

    split |> should equal whole
    single |> should equal whole

// ────────────────── Derivation ──────────────────

[<Fact>]
let ``Derivation keeps ordered tool correlation with transient reasoning`` () =
    let events =
        ResizeArray<SessionEvent>(
            [|
                TextDeltaEvent(sessionId, turnId, noSequence, stamp, "Hel") :> SessionEvent
                ReasoningDeltaEvent(sessionId, turnId, noSequence, stamp, "thinking") :> SessionEvent
                TextDeltaEvent(sessionId, turnId, noSequence, stamp, "lo") :> SessionEvent
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", """{}""")
                :> SessionEvent
                ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, "call-1", "bytes") :> SessionEvent
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, "bytes")
                :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    let cells =
        SessionCellDeriver.Fold(sessionId, turnId, UserMessage.Text "hello", stamp, events)

    // Reasoning is transient (no cell) but ends the open text run, so the
    // deltas around it derive two assistant cells in order.
    let kinds = cells |> Seq.map (fun cell -> cell.Kind) |> Seq.toList

    kinds
    |> should
        equal
        [
            SessionCellKind.User
            SessionCellKind.Assistant
            SessionCellKind.Assistant
            SessionCellKind.ToolCall
            SessionCellKind.ToolResult
        ]

    cells[1].Content |> should equal "Hel"
    cells[2].Content |> should equal "lo"

    let toolCall = cells[3]
    toolCall.ToolCallId |> should equal "call-1"
    toolCall.ToolName |> should equal "read_file"

    let toolResult = cells[4]
    toolResult.ToolCallId |> should equal "call-1"
    toolResult.Content |> should equal "bytes"
    toolResult.IsError |> should equal false

[<Fact>]
let ``Derivation dedupes repeated continuation reports`` () =
    let events =
        ResizeArray<SessionEvent>(
            [|
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", """{}""")
                :> SessionEvent
                ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file", """{}""")
                :> SessionEvent
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, "bytes")
                :> SessionEvent
                ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString, "bytes")
                :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    let cells = SessionCellDeriver.Fold(sessionId, turnId, null, stamp, events)
    let kinds = cells |> Seq.map (fun cell -> cell.Kind) |> Seq.toList

    kinds
    |> should
        equal
        [
            SessionCellKind.ToolCall
            SessionCellKind.ToolResult
        ]

[<Fact>]
let ``An empty journal never reports a complete transcript`` () =
    let events = ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>
    let cells = SessionCellDeriver.Fold(sessionId, turnId, null, stamp, events)
    cells.Count |> should equal 0

    let read =
        TranscriptReader.Read(events, ReadTranscriptOptions(IncludeSubAgentCells = true))

    read.Count |> should equal 0

// ────────────────── Facade ──────────────────

let private scripted (steps: Legate.Testing.ScriptStep list) : Legate.Testing.ScriptedChatClient =
    new Legate.Testing.ScriptedChatClient(
        ResizeArray<Legate.Testing.ScriptStep>(steps) :> IReadOnlyList<Legate.Testing.ScriptStep>
    )

let private sourced (tools: AITool list) : Legate.Testing.StaticToolSource =
    new Legate.Testing.StaticToolSource(ResizeArray<AITool>(tools) :> IReadOnlyList<AITool>)

let private createServices
    (chatClient: Legate.Testing.ScriptedChatClient)
    (tools: Legate.Testing.StaticToolSource)
    : IServiceCollection =
    let database = InMemoryDatabase()

    let services = ServiceCollection() :> IServiceCollection

    LegateServiceCollectionExtensions.AddLegate(
        services,
        ?configure =
            Some(fun (builder: LegateBuilder) ->
                builder.Llm.AddProvider(BuilderTests.StubLlmProvider()) |> ignore

                builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore
                builder.Workspace.UseRuntime(BuilderTests.StubWorkspaceRuntime()) |> ignore

                builder.Tools.AddSource(tools) |> ignore)
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    services.AddSingleton<IChatClient>(chatClient) |> ignore
    services

let private stopQuietly (service: LocalActorSystemService) : Task =
    task {
        try
            do! (service :> IHostedService).StopAsync(CancellationToken.None)
        with _ ->
            ()
    }

let private withClient (provider: IServiceProvider) (work: SessionClient -> Task<'T>) : Task<'T> =
    task {
        let service =
            provider.GetServices<IHostedService>()
            |> Seq.pick (fun service ->
                match service with
                | :? LocalActorSystemService as local -> Some local
                | _ -> None)

        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let client = provider.GetRequiredService<SessionClient>()

        try
            let! outcome = work client
            do! stopQuietly service
            return outcome
        with ex ->
            do! stopQuietly service
            return raise ex
    }

[<Fact>]
let ``Facade settles a turn with journaled evidence and isolated transcripts`` () : Task =
    task {
        let chat =
            scripted
                [
                    Legate.Testing.ScriptStep.Text "done"
                ]

        use provider = (createServices chat (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! first =
                        SessionClientOperations.OpenSessionAsync(
                            client,
                            AgentId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                            null,
                            CancellationToken.None
                        )

                    let! second =
                        SessionClientOperations.OpenSessionAsync(
                            client,
                            AgentId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                            null,
                            CancellationToken.None
                        )

                    let settleTask =
                        SessionClientOperations.WaitForSettleAsync(
                            client,
                            first.Id,
                            TimeSpan.FromSeconds 10.0,
                            CancellationToken.None
                        )

                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            first.Id,
                            UserMessage.Text "hello",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! settled = settleTask

                    (isNull (box settled)) |> should equal false

                    let! events =
                        SessionClientOperations.ReadEventsAsync(client, first.Id, 0L, 100, CancellationToken.None)

                    (isNull (box events)) |> should equal false
                    (events.Count > 0) |> should equal true

                    let! firstCells =
                        SessionClientOperations.ReadTranscriptAsync(client, first.Id, CancellationToken.None)

                    let! secondCells =
                        SessionClientOperations.ReadTranscriptAsync(client, second.Id, CancellationToken.None)

                    (isNull (box firstCells)) |> should equal false
                    (isNull (box secondCells)) |> should equal false
                    secondCells.Count |> should equal 0

                    for cell in firstCells do
                        cell.SessionId |> should equal first.Id
                })
    }
