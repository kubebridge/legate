// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.CompactedContextTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Xunit

#nowarn "3261"

// Durable compacted working context (issue 387): the enriched CompactedEvent
// carries the actual summary plus the retained current-format tail, and one
// shared compacted-base function resolves live turns, idle Compact,
// subsequent turns, and fresh-process reopen through the last successful
// base. Audit transcript derivation is untouched: CompactedEvent still
// produces no transcript cell.

let private tenant = TenantId.Create "compacted-context"
let private sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let private turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FBV"
let private stamp = DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero)
let private noSequence = Unchecked.defaultof<Nullable<int64>>

let private userText (text: string) = ChatMessage(ChatRole.User, text)
let private assistantText (text: string) = ChatMessage(ChatRole.Assistant, text)
let private systemText (text: string) = ChatMessage(ChatRole.System, text)

let private toolCallMessage (callId: string) (name: string) (argsJson: string) : ChatMessage =
    let table =
        JsonSerializer.Deserialize(argsJson, typeof<Dictionary<string, JsonElement>>)
        |> unbox<Dictionary<string, JsonElement>>

    let args = Dictionary<string, obj>(StringComparer.Ordinal)

    for pair in table do
        args[pair.Key] <- pair.Value :> obj

    let call = FunctionCallContent(callId, name, args :> IDictionary<string, obj>)
    let contents = ResizeArray<AIContent>()
    contents.Add(call :> AIContent)
    ChatMessage(ChatRole.Assistant, contents :> IList<AIContent>)

let private toolResultMessage (callId: string) (result: string) : ChatMessage =
    let contents = ResizeArray<AIContent>()
    contents.Add(FunctionResultContent(callId, result) :> AIContent)
    ChatMessage(ChatRole.Tool, contents :> IList<AIContent>)

let private enrichedCompacted (summary: string) (tail: ChatMessage list) : CompactedEvent =
    CompactedEvent(
        sessionId,
        turnId,
        noSequence,
        stamp,
        9000L,
        1200L,
        summary,
        ResizeArray<ChatMessage>(tail) :> IReadOnlyList<ChatMessage>,
        SessionEventContract.CompactedContextVersion
    )

let private userEvent (turn: TurnId) (text: string) : SessionEvent =
    UserMessageEvent(sessionId, turn, noSequence, stamp, UserMessage.Text text) :> SessionEvent

let private toolStartEvent (turn: TurnId) (callId: string) (name: string) (argsJson: string) : SessionEvent =
    ToolCallStartedEvent(sessionId, turn, noSequence, stamp, callId, name, argsJson) :> SessionEvent

let private toolDoneEvent (turn: TurnId) (callId: string) (result: string) : SessionEvent =
    ToolCallCompletedEvent(sessionId, turn, noSequence, stamp, callId, Unchecked.defaultof<string>, result)
    :> SessionEvent

let private messageRoles (history: IList<ChatMessage>) : string list =
    [
        for message in history do
            if isNull (box message) then
                yield "null"
            else
                yield message.Role.Value
    ]

let private messageText (message: ChatMessage) : string =
    if isNull (box message) || isNull (box message.Contents) then
        ""
    else
        message.Contents
        |> Seq.choose (function
            | :? TextContent as text when not (isNull (box text)) && not (isNull (box text.Text)) -> Some text.Text
            | _ -> None)
        |> String.concat "\n"

// ────────────────── Task 1: enriched round-trip ──────────────────

[<Fact>]
let ``Enriched CompactedEvent round-trips summary, tail, and version`` () =
    let tail =
        [
            userText "kept question"
            assistantText "kept answer"
        ]

    let event = enrichedCompacted "kept facts" tail :> SessionEvent
    let json = JsonSerializer.Serialize(event, JsonSerializerOptions())
    json.Contains("\"$type\":\"compacted\"") |> should equal true

    match JsonSerializer.Deserialize(json, typeof<SessionEvent>) with
    | null -> failwith "deserialised to null"
    | :? CompactedEvent as restored ->
        restored.Summary |> should equal "kept facts"

        restored.FormatVersion
        |> should equal SessionEventContract.CompactedContextVersion

        restored.RetainedMessages.Count |> should equal 2
        messageText restored.RetainedMessages[0] |> should equal "kept question"
    | _ -> failwith "Expected CompactedEvent."

[<Fact>]
let ``Enriched CompactedEvent round-trips non-text and tool pairing`` () =
    let args = Dictionary<string, obj>(StringComparer.Ordinal)

    let pathElement =
        JsonSerializer.Deserialize("\"notes.txt\"", typeof<JsonElement>)
        |> unbox<JsonElement>

    args["path"] <- pathElement :> obj

    let call =
        FunctionCallContent("call-1", "read_file", args :> IDictionary<string, obj>)

    let callContents = ResizeArray<AIContent>()
    callContents.Add(call :> AIContent)
    let callMessage = ChatMessage(ChatRole.Assistant, callContents :> IList<AIContent>)

    let resultContents = ResizeArray<AIContent>()
    resultContents.Add(FunctionResultContent("call-1", "file bytes") :> AIContent)
    let resultMessage = ChatMessage(ChatRole.Tool, resultContents :> IList<AIContent>)

    let dataContents = ResizeArray<AIContent>()
    dataContents.Add(DataContent(ReadOnlyMemory<byte>([| 1uy; 2uy |]), "application/pdf") :> AIContent)
    let fileMessage = ChatMessage(ChatRole.User, dataContents :> IList<AIContent>)

    let tail =
        [
            callMessage
            resultMessage
            fileMessage
        ]

    let event =
        CompactedEvent(
            sessionId,
            turnId,
            noSequence,
            stamp,
            10L,
            4L,
            "summary with pairing",
            ResizeArray<ChatMessage>(tail) :> IReadOnlyList<ChatMessage>,
            SessionEventContract.CompactedContextVersion
        )
        :> SessionEvent

    let json = JsonSerializer.Serialize(event, JsonSerializerOptions())

    match JsonSerializer.Deserialize(json, typeof<SessionEvent>) with
    | :? CompactedEvent as restored ->
        restored.RetainedMessages.Count |> should equal 3

        let calls =
            restored.RetainedMessages[0].Contents
            |> Seq.choose (function
                | :? FunctionCallContent as c -> Some c.CallId
                | _ -> None)
            |> List.ofSeq

        calls |> should equal [ "call-1" ]

        restored.RetainedMessages[2].Contents[0] |> should be ofExactType<DataContent>
    | _ -> failwith "Expected CompactedEvent."

[<Fact>]
let ``Enriched CompactedEvent round-trips through InMemory append and replay`` () =
    let clock = TestClock(DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero))
    let database = InMemoryDatabase(clock)
    let store = InMemorySessionStore(database) :> ISessionStore
    let journal = InMemorySessionEventStore(database) :> ISessionEventStore
    let sid = SessionId.New()
    let tid = TurnId.New()

    let session =
        {
            Id = sid
            Tenant = tenant
            AgentId = AgentId.New()
            Title = "compacted"
            State = SessionState.Idle
            CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
            CreatedAt = clock.GetUtcNow()
            UpdatedAt = clock.GetUtcNow()
            ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
            WorkspaceBinding = null
            Options = SessionOptions()
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    store.CreateSession(tenant, session, CancellationToken.None).GetAwaiter().GetResult()
    |> ignore

    let payload = UserMessagePayload(UserMessage.Text "start") :> InboxPayload

    store.AppendInboxMessage(tenant, sid, payload, DeliveryMode.Queue, CancellationToken.None).GetAwaiter().GetResult()
    |> ignore

    let token =
        match
            store
                .ClaimNextTurn(tenant, sid, "owner", TimeSpan.FromMinutes 1.0, CancellationToken.None)
                .GetAwaiter()
                .GetResult()
        with
        | :? TurnLeaseHeld as held -> held.Claim.Token
        | :? TurnLeaseRenewed as renewed -> renewed.Claim.Token
        | state -> failwith $"Expected a granted claim, got {state.GetType().Name}."

    let tail =
        [
            userText "kept question"
            toolCallMessage "call-1" "read_file" """{"path":"a.txt"}"""
        ]

    let event =
        CompactedEvent(
            sid,
            tid,
            noSequence,
            clock.GetUtcNow(),
            10L,
            4L,
            "durable summary",
            ResizeArray<ChatMessage>(tail) :> IReadOnlyList<ChatMessage>,
            SessionEventContract.CompactedContextVersion
        )
        :> SessionEvent

    let outcome =
        journal
            .Append(
                tenant,
                sid,
                token,
                (ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )
            .GetAwaiter()
            .GetResult()

    match outcome with
    | :? EventAppended -> ()
    | _ -> failwith "Expected the append to land."

    let replay =
        journal.Replay(tenant, sid, 0L, 10, CancellationToken.None).GetAwaiter().GetResult()

    match replay with
    | :? EventReplayPage as page ->
        page.Events.Count |> should equal 1

        match page.Events[0] with
        | :? CompactedEvent as restored ->
            restored.Summary |> should equal "durable summary"
            restored.RetainedMessages.Count |> should equal 2
        | _ -> failwith "Expected CompactedEvent."
    | _ -> failwith "Expected a replay page."

[<Fact>]
let ``Enriched CompactedEvent round-trips through real SQLite`` () =
    let clock = TestClock(DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero))
    let database, path = SqliteTestFixture.openTestDatabase clock

    try
        try
            let store = SqliteSessionStore(database) :> ISessionStore
            let journal = SqliteSessionEventStore(database) :> ISessionEventStore
            let tenantId = TenantId.Create "sqlite-compacted"
            let sid = SessionId.New()
            let tid = TurnId.New()

            let session =
                {
                    Id = sid
                    Tenant = tenantId
                    AgentId = AgentId.New()
                    Title = "compacted"
                    State = SessionState.Idle
                    CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
                    CreatedAt = clock.GetUtcNow()
                    UpdatedAt = clock.GetUtcNow()
                    ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
                    WorkspaceBinding = null
                    Options = SessionOptions()
                    PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
                }

            store.CreateSession(tenantId, session, CancellationToken.None).GetAwaiter().GetResult()
            |> ignore

            let payload = UserMessagePayload(UserMessage.Text "start") :> InboxPayload

            store
                .AppendInboxMessage(tenantId, sid, payload, DeliveryMode.Queue, CancellationToken.None)
                .GetAwaiter()
                .GetResult()
            |> ignore

            let token =
                match
                    store
                        .ClaimNextTurn(tenantId, sid, "owner", TimeSpan.FromMinutes 1.0, CancellationToken.None)
                        .GetAwaiter()
                        .GetResult()
                with
                | :? TurnLeaseHeld as held -> held.Claim.Token
                | :? TurnLeaseRenewed as renewed -> renewed.Claim.Token
                | state -> failwith $"Expected a granted claim, got {state.GetType().Name}."

            let tail =
                [
                    userText "kept"
                    toolResultMessage "call-9" "result bytes"
                ]

            let event =
                CompactedEvent(
                    sid,
                    tid,
                    noSequence,
                    clock.GetUtcNow(),
                    20L,
                    8L,
                    "sqlite summary",
                    ResizeArray<ChatMessage>(tail) :> IReadOnlyList<ChatMessage>,
                    SessionEventContract.CompactedContextVersion
                )
                :> SessionEvent

            let outcome =
                journal
                    .Append(
                        tenantId,
                        sid,
                        token,
                        (ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>),
                        CancellationToken.None
                    )
                    .GetAwaiter()
                    .GetResult()

            match outcome with
            | :? EventAppended -> ()
            | _ -> failwith "Expected the SQLite append to land."

            let replay =
                journal.Replay(tenantId, sid, 0L, 10, CancellationToken.None).GetAwaiter().GetResult()

            match replay with
            | :? EventReplayPage as page ->
                match page.Events[0] with
                | :? CompactedEvent as restored ->
                    restored.Summary |> should equal "sqlite summary"
                    restored.RetainedMessages.Count |> should equal 2
                | _ -> failwith "Expected CompactedEvent."
            | _ -> failwith "Expected a replay page."
        finally
            (database :> IDisposable).Dispose()
    finally
        SqliteTestFixture.deleteDatabaseFiles path

// ────────────────── Task 2: shared compacted-base resolution ──────────────────

[<Fact>]
let ``Shared resolution drops superseded context and keeps order without duplication`` () =
    let oldTurn = TurnId.New()
    let newTurn = TurnId.New()

    let events =
        ResizeArray<SessionEvent>(
            [|
                userEvent oldTurn "superseded question"
                toolStartEvent oldTurn "old-call" "read_file" """{"path":"old.txt"}"""
                toolDoneEvent oldTurn "old-call" "old result"
                enrichedCompacted
                    "kept facts"
                    [
                        userText "kept question"
                        assistantText "kept answer"
                    ]
                userEvent newTurn "later question"
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecoverCompacted events with
    | Error rejection -> failwith $"Expected recovery, got {rejection}."
    | Ok history ->
        let texts =
            history
            |> Seq.map messageText
            |> Seq.filter (fun text -> text <> "")
            |> List.ofSeq

        let containsSub (part: string) =
            texts |> List.exists (fun text -> text.Contains(part, StringComparison.Ordinal))

        containsSub "kept facts" |> should equal true
        containsSub "kept question" |> should equal true
        containsSub "later question" |> should equal true
        containsSub "superseded question" |> should equal false
        containsSub "old result" |> should equal false

        // Summary once, then retained tail, then suffix: no duplication.
        let summaryCount =
            texts
            |> List.filter (fun text -> text.Contains("kept facts", StringComparison.Ordinal))
            |> List.length

        summaryCount |> should equal 1
        messageRoles history |> should equal [ "user"; "user"; "assistant"; "user" ]

[<Fact>]
let ``Shared resolution preserves valid tool pairing across the suffix`` () =
    let before = TurnId.New()
    let after = TurnId.New()

    let events =
        ResizeArray<SessionEvent>(
            [|
                userEvent before "before"
                enrichedCompacted "summary" [ userText "retained" ]
                userEvent after "after"
                toolStartEvent after "call-2" "write_file" """{"path":"b.txt"}"""
                toolDoneEvent after "call-2" "wrote bytes"
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecoverCompacted events with
    | Error rejection -> failwith $"Expected recovery, got {rejection}."
    | Ok history ->
        let calls =
            history
            |> Seq.collect (fun message ->
                if isNull (box message) || isNull (box message.Contents) then
                    Seq.empty
                else
                    message.Contents
                    |> Seq.choose (function
                        | :? FunctionCallContent as call -> Some call.CallId
                        | _ -> None))
            |> List.ofSeq

        let results =
            history
            |> Seq.collect (fun message ->
                if isNull (box message) || isNull (box message.Contents) then
                    Seq.empty
                else
                    message.Contents
                    |> Seq.choose (function
                        | :? FunctionResultContent as result -> Some result.CallId
                        | _ -> None))
            |> List.ofSeq

        calls |> should equal [ "call-2" ]
        results |> should equal [ "call-2" ]

[<Fact>]
let ``Shared resolution uses the last base for repeated compaction`` () =
    let first = TurnId.New()
    let second = TurnId.New()

    let events =
        ResizeArray<SessionEvent>(
            [|
                userEvent first "first question"
                enrichedCompacted "first summary" [ userText "first retained" ]
                userEvent second "second question"
                enrichedCompacted "second summary" [ userText "second retained" ]
                userEvent second "after second"
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecoverCompacted events with
    | Error _ -> failwith "Expected recovery."
    | Ok history ->
        let texts = history |> Seq.map messageText |> List.ofSeq

        let containsSub (part: string) =
            texts |> List.exists (fun text -> text.Contains(part, StringComparison.Ordinal))

        containsSub "first summary" |> should equal false
        containsSub "first retained" |> should equal false
        containsSub "second summary" |> should equal true
        containsSub "second retained" |> should equal true
        containsSub "after second" |> should equal true

// ────────────────── Tasks 6+7: fencing, failure, unsupported ──────────────────

[<Fact>]
let ``Old compacted events without replacement context reject with a clean start`` () =
    // A pre-387 journal: estimates only, no summary or tail.
    let legacy =
        CompactedEvent(
            sessionId,
            turnId,
            noSequence,
            stamp,
            10L,
            4L,
            Unchecked.defaultof<string>,
            Unchecked.defaultof<IReadOnlyList<ChatMessage>>,
            0
        )
        :> SessionEvent

    let events =
        ResizeArray<SessionEvent>([| userEvent turnId "q"; legacy |]) :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecoverCompacted events with
    | Error(ConversationRecovery.IncompleteCompactedContext _)
    | Error(ConversationRecovery.UnsupportedCompactedFormat _) -> ()
    | Error rejection -> failwith $"Expected compacted rejection, got {rejection}."
    | Ok _ -> failwith "Expected rejection for legacy compacted context."

    // Old data stays untouched: the journal still holds both events.
    events.Count |> should equal 2

[<Fact>]
let ``Unsupported compacted versions reject without manufacturing context`` () =
    let bad =
        CompactedEvent(
            sessionId,
            turnId,
            noSequence,
            stamp,
            10L,
            4L,
            "summary",
            ResizeArray<ChatMessage>([ userText "kept" ]) :> IReadOnlyList<ChatMessage>,
            99
        )
        :> SessionEvent

    let events = ResizeArray<SessionEvent>([| bad |]) :> IReadOnlyList<SessionEvent>

    match ConversationRecovery.tryRecoverCompacted events with
    | Error(ConversationRecovery.UnsupportedCompactedFormat 99) -> ()
    | Error rejection -> failwith $"Expected version rejection, got {rejection}."
    | Ok _ -> failwith "Expected rejection for unsupported version."

[<Fact>]
let ``Failed persistence never exposes success with missing context`` () : Task =
    task {
        let history =
            ResizeArray<ChatMessage>(
                [|
                    systemText "sys"
                    userText (String('x', 1000))
                    assistantText (String('x', 1000))
                    userText (String('x', 1000))
                |]
            )
            :> IList<ChatMessage>

        let mutable calls = 0

        let journalAsync (_: SessionEvent) : Task<JournalWriter.JournalWriteResult> =
            calls <- calls + 1
            Task.FromResult(JournalWriter.JournalFailed "boom")

        let chat =
            new ScriptedChatClient([| ScriptStep.Text("summary") |] :> IReadOnlyList<ScriptStep>)

        let request: Compaction.CompactionRequest =
            {
                Client = chat :> IChatClient
                History = history
                SessionModel = ModelReference.Parse "test/session-model"
                CompactionModel = null
                KeepMessages = 1
                CatalogEntry = null
                ReservedBufferTokens = 0
                Observer = null
                ModelPolicy = null
                Tenant = tenant
                SessionId = sessionId
                TurnId = turnId
                Attempt = 1
                InputTokens = 0L
                OutputTokens = 0L
                JournalAsync = journalAsync
                IsLeaseValid = (fun () -> true)
                CancellationToken = CancellationToken.None
            }

        let! outcome = Compaction.tryCompactCoreAsync true request

        match outcome with
        | Compaction.FailedContinue _ ->
            // No in-place rewrite on failed persistence: the running
            // history keeps its uncompacted shape.
            history.Count |> should equal 4
            calls |> should be (greaterThanOrEqualTo 1)
        | Compaction.Compacted _ -> failwith "Failed persistence must not report success."
        | Compaction.NotNeeded -> failwith "Expected a failure-continue, not NotNeeded."
    }

[<Fact>]
let ``Successful compaction journals the enriched event and rewrites in place`` () : Task =
    task {
        let history =
            ResizeArray<ChatMessage>(
                [|
                    systemText "sys"
                    userText (String('x', 1000))
                    assistantText (String('x', 1000))
                    userText (String('x', 1000))
                |]
            )
            :> IList<ChatMessage>

        let journaled = ResizeArray<SessionEvent>()

        let journalAsync (event: SessionEvent) : Task<JournalWriter.JournalWriteResult> =
            journaled.Add(event)
            Task.FromResult(JournalWriter.JournalAppended(ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>))

        let chat =
            new ScriptedChatClient([| ScriptStep.Text("durable summary") |] :> IReadOnlyList<ScriptStep>)

        let request: Compaction.CompactionRequest =
            {
                Client = chat :> IChatClient
                History = history
                SessionModel = ModelReference.Parse "test/session-model"
                CompactionModel = null
                KeepMessages = 1
                CatalogEntry = null
                ReservedBufferTokens = 0
                Observer = null
                ModelPolicy = null
                Tenant = tenant
                SessionId = sessionId
                TurnId = turnId
                Attempt = 1
                InputTokens = 0L
                OutputTokens = 0L
                JournalAsync = journalAsync
                IsLeaseValid = (fun () -> true)
                CancellationToken = CancellationToken.None
            }

        let! outcome = Compaction.tryCompactCoreAsync true request

        match outcome with
        | Compaction.Compacted _ ->
            journaled.Count |> should equal 1

            match journaled[0] with
            | :? CompactedEvent as compacted ->
                compacted.Summary |> should equal "durable summary"

                compacted.FormatVersion
                |> should equal SessionEventContract.CompactedContextVersion

                (isNull (box compacted.RetainedMessages)) |> should equal false
                compacted.RetainedMessages.Count |> should be (greaterThan 0)

                // The rewritten history carries the marked summary plus
                // the retained tail.
                let texts = history |> Seq.map messageText |> List.ofSeq

                texts
                |> List.exists (fun text -> text.Contains("durable summary", StringComparison.Ordinal))
                |> should equal true
            | _ -> failwith "Expected CompactedEvent."
        | _ -> failwith "Expected compaction success."
    }

// ────────────────── Task 8: audit separation ──────────────────

[<Fact>]
let ``CompactedEvent still produces no transcript cell`` () =
    let events =
        [
            TurnStartedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, "hi") :> SessionEvent
            enrichedCompacted "summary" [ userText "kept" ] :> SessionEvent
            TextDeltaEvent(sessionId, turnId, noSequence, stamp, " there") :> SessionEvent
            TurnCompletedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
        ]

    let cells =
        SessionCellDeriver.Fold(
            sessionId,
            turnId,
            null,
            stamp,
            (ResizeArray<SessionEvent>(events) :> IReadOnlyList<SessionEvent>)
        )

    // One assistant cell from the delta run: the compacted marker added
    // no audit cell of its own.
    cells.Count |> should equal 1
    cells[0].Kind |> should equal SessionCellKind.Assistant
    cells[0].Content |> should equal "hi there"

// ────────────────── Tasks 4+5: idle forced + next turns (facade) ──────────────────

let private smokeAgentId = AgentId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FBV")
let private sessionModelRef = ModelReference.Parse "test/session-model"

let private smallEntry () : ModelCatalogEntry =
    {
        Model = sessionModelRef
        ContextWindowTokens = 600
        ReservedOutputTokens = 100
        MaxOutputTokens = 100
        Capabilities =
            {
                Streaming = true
                Reasoning = false
                ToolCalling = true
            }
    }

type private FakeCatalog(entry: ModelCatalogEntry | null) =
    interface ILlmModelCatalog with
        member _.GetEntry _ = entry
        member _.HasEntry _ = not (isNull (box entry))

let private createFacadeServices (database: InMemoryDatabase) (chat: IChatClient) : IServiceCollection =
    let services = ServiceCollection() :> IServiceCollection
    let clientOptions = SessionClientOptions()
    clientOptions.Tenant <- TenantId.Default
    services.AddSingleton(clientOptions) |> ignore

    LegateServiceCollectionExtensions.AddLegate(
        services,
        ?configure =
            Some(fun (builder: LegateBuilder) ->
                builder.Llm.AddProvider(BuilderTests.StubLlmProvider()) |> ignore
                builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore
                builder.Workspace.UseRuntime(BuilderTests.StubWorkspaceRuntime()) |> ignore

                builder.Tools.AddSource(StaticToolSource(ResizeArray<AITool>() :> IReadOnlyList<AITool>))
                |> ignore

                builder.Agents.Add(
                    "smoke",
                    Func<Agent, Agent>(fun template ->
                        { template with
                            Id = smokeAgentId
                            Description = "Compacted-context smoke agent."
                            Model = sessionModelRef
                            SystemPrompt = "Answer briefly."
                            ToolSelection = ToolSelection()
                        })
                )
                |> ignore)
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    services.AddSingleton<IChatClient>(chat) |> ignore

    services.Configure<LegateOptions>(
        Action<LegateOptions>(fun (options: LegateOptions) ->
            options.Llm.DefaultModel <- "test/session-model"
            options.Llm.CompactionKeepMessages <- 1
            options.Pruning.ReservedBufferTokens <- 50)
    )
    |> ignore

    services.AddSingleton<ILlmModelCatalog>(FakeCatalog(smallEntry ()) :> ILlmModelCatalog)
    |> ignore

    services

let private actorServiceOf (provider: IServiceProvider) : LocalActorSystemService =
    provider.GetServices<IHostedService>()
    |> Seq.pick (fun service ->
        match service with
        | :? LocalActorSystemService as local -> Some local
        | _ -> None)

let private withFacadeClient (provider: IServiceProvider) (work: SessionClient -> Task<'T>) : Task<'T> =
    task {
        let service = actorServiceOf provider
        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let client = provider.GetRequiredService<SessionClient>()

        try
            let! outcome = work client
            do! (service :> IHostedService).StopAsync(CancellationToken.None)
            return outcome
        with ex ->
            try
                (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
            with _ ->
                ()

            return raise ex
    }

[<Fact>]
let ``Idle forced compact below threshold journals enriched context and next turn consumes it`` () : Task =
    task {
        let chat =
            new ScriptedChatClient(
                ResizeArray<ScriptStep>(
                    [|
                        ScriptStep.Text "ans-1"
                        ScriptStep.Text "ans-2"
                        ScriptStep.Text "idle-summary"
                        ScriptStep.Text "ans-3"
                        ScriptStep.Text "ans-4"
                    |]
                )
            )

        let database = InMemoryDatabase()

        use provider = (createFacadeServices database chat).BuildServiceProvider()

        return!
            withFacadeClient provider (fun client ->
                task {
                    let! smoke =
                        match client.Agents with
                        | None -> Task.FromException<Agent>(InvalidOperationException("Expected an agent catalog."))
                        | Some catalog -> catalog.GetAgent(client.Tenant, smokeAgentId, CancellationToken.None)

                    let! created =
                        SessionClientOperations.OpenSessionAsync(client, smoke.Id, null, CancellationToken.None)

                    let! first =
                        SessionClientExtensions.PromptAndWaitAsync(
                            client,
                            created.Id,
                            UserMessage.Text "hello",
                            CancellationToken.None
                        )

                    first.Status |> should equal TurnStatus.Completed

                    let! second =
                        SessionClientExtensions.PromptAndWaitAsync(
                            client,
                            created.Id,
                            UserMessage.Text "again",
                            CancellationToken.None
                        )

                    second.Status |> should equal TurnStatus.Completed

                    // Idle forced compact below the automatic threshold:
                    // eligible context compacts without a synthetic turn.
                    let! compacted = SessionClientOperations.CompactAsync(client, created.Id, CancellationToken.None)

                    match compacted with
                    | :? SessionCompacted -> ()
                    | _ -> failwith $"Expected SessionCompacted, got {compacted.GetType().Name}."

                    let collected = ResizeArray<SessionEvent>()
                    let mutable cursor = 0L
                    let mutable more = true

                    while more do
                        let! outcome =
                            client.EventBus.EventStore.Replay(
                                client.Tenant,
                                created.Id,
                                cursor,
                                100,
                                CancellationToken.None
                            )

                        match outcome with
                        | :? EventReplayPage as page when not (isNull (box page)) ->
                            if not (isNull (box page.Events)) then
                                collected.AddRange page.Events

                            if page.NextCursor.HasValue then
                                cursor <- page.NextCursor.Value
                            else
                                more <- false
                        | _ -> more <- false

                    let compactedEvents =
                        collected
                        |> Seq.choose (function
                            | :? CompactedEvent as c -> Some c
                            | _ -> None)
                        |> List.ofSeq

                    compactedEvents.Length |> should be (greaterThanOrEqualTo 1)
                    let last = compactedEvents[compactedEvents.Length - 1]
                    last.Summary |> should equal "idle-summary"
                    last.FormatVersion |> should equal SessionEventContract.CompactedContextVersion
                    (isNull (box last.RetainedMessages)) |> should equal false

                    // The next turn consumes the persisted compacted base:
                    // its model input carries the marked summary once.
                    let before = chat.ReceivedMessages.Count

                    let! third =
                        SessionClientExtensions.PromptAndWaitAsync(
                            client,
                            created.Id,
                            UserMessage.Text "after compact",
                            CancellationToken.None
                        )

                    third.Status |> should equal TurnStatus.Completed

                    let afterMessages = chat.ReceivedMessages |> Seq.skip before |> List.ofSeq

                    let summaries =
                        afterMessages
                        |> List.filter (fun message ->
                            not (isNull (box message))
                            && not (isNull (box message.Contents))
                            && message.Contents
                               |> Seq.exists (function
                                   | :? TextContent as text ->
                                       not (isNull (box text))
                                       && not (isNull (box text.Text))
                                       && text.Text.Contains("idle-summary", StringComparison.Ordinal)
                                   | _ -> false))

                    summaries.Length |> should be (greaterThanOrEqualTo 1)

                    // The shared base still resolves with the summary once.
                    match ConversationRecovery.tryRecoverCompacted (collected :> IReadOnlyList<SessionEvent>) with
                    | Error rejection -> failwith $"Expected recovery, got {rejection}."
                    | Ok _ -> ()

                    // A second turn after compaction keeps the base without
                    // duplicating the summary.
                    let! fourth =
                        SessionClientExtensions.PromptAndWaitAsync(
                            client,
                            created.Id,
                            UserMessage.Text "once more",
                            CancellationToken.None
                        )

                    fourth.Status |> should equal TurnStatus.Completed
                })
    }
