// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.CompactionTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Testing
open Legate.Tests.TurnLoopTests
open Microsoft.Extensions.AI
open Xunit

// Compaction (issue 45): the threshold check reuses ContextPruning.Estimate
// and PruneThreshold over transient message-mapped cells, the rewrite keeps
// the system message plus a marked summary plus the last K messages, and
// the runner summarises through the turn's client with the session or
// override model. Success journals CompactedEvent and folds summariser
// usage into the turn totals; denial, provider errors, and empty summaries
// journal CompactionFailedEvent and continue uncompacted.

let tenant = TenantId.Create "acme"
let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let nullString = Unchecked.defaultof<string>
let nullEntry = Unchecked.defaultof<ModelCatalogEntry>

let longText (length: int) = String('x', length)

let systemMessage (text: string) = ChatMessage(ChatRole.System, text)
let userMessage (text: string) = ChatMessage(ChatRole.User, text)
let assistantMessage (text: string) = ChatMessage(ChatRole.Assistant, text)

/// Six 1000-char turns after a system message: estimates near 1530, over
/// the 1200 test threshold below.
let overHistory () : IList<ChatMessage> =
    ResizeArray<ChatMessage>(
        [|
            systemMessage "sys"
            userMessage (longText 1000)
            assistantMessage (longText 1000)
            userMessage (longText 1000)
            assistantMessage (longText 1000)
            userMessage (longText 1000)
            assistantMessage (longText 1000)
        |]
    )
    :> IList<ChatMessage>

/// Twelve 1000-char turns: still over the test threshold after a keep-10
/// compaction, so the next boundary compacts again.
let largeHistory () : IList<ChatMessage> =
    let messages = ResizeArray<ChatMessage>()
    messages.Add(systemMessage "sys")

    for index in 1..12 do
        if index % 2 = 1 then
            messages.Add(userMessage (longText 1000))
        else
            messages.Add(assistantMessage (longText 1000))

    messages :> IList<ChatMessage>

let smallHistory () : IList<ChatMessage> =
    ResizeArray<ChatMessage>(
        [|
            userMessage "hi"
            assistantMessage "hello"
        |]
    )
    :> IList<ChatMessage>

let catalogEntry (window: int) (reserve: int) : ModelCatalogEntry =
    {
        Model = ModelReference.Parse "test/session-model"
        ContextWindowTokens = window
        ReservedOutputTokens = reserve
        MaxOutputTokens = reserve
        Capabilities =
            {
                Streaming = true
                Reasoning = false
                ToolCalling = true
            }
    }

let testEntry = catalogEntry 1500 200
let testThreshold = ContextPruning.PruneThreshold(testEntry, 100)

/// Records journaled events and lands every append.
let private recordingJournal
    (journal: ResizeArray<SessionEvent>)
    : SessionEvent -> Task<JournalWriter.JournalWriteResult> =
    fun event ->
        journal.Add(event)

        Task.FromResult(JournalWriter.JournalAppended(ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>))

/// Records usage checkpoints; settlements stay empty.
type RecordingObserver() =
    let checkpoints = ResizeArray<UsageCheckpoint>()

    /// The checkpoints in arrival order.
    member _.Checkpoints = checkpoints :> IReadOnlyList<UsageCheckpoint>

    interface IUsageObserver with
        member _.OnCheckpoint(usage: UsageCheckpoint) = checkpoints.Add(usage)
        member _.OnSettled(_usage: UsageSettlement) = ()

/// Denies every model call with the given message.
type DenyModelPolicy(message: string) =
    interface IModelPolicy with
        member _.Authorize(_tenant: TenantId, _provider: string, _model: string) =
            ModelDenied(message) :> ModelPolicyDecision

/// Records authorised provider/model pairs and allows every call.
type RecordingPolicy() =
    let calls = ResizeArray<string * string>()

    /// The authorised pairs in order.
    member _.Calls = calls :> IReadOnlyList<string * string>

    interface IModelPolicy with
        member _.Authorize(_tenant: TenantId, provider: string, model: string) =
            calls.Add((provider, model))
            ModelPolicyDecision.Allow

let private baseDeps (client: IChatClient) (journal: ResizeArray<SessionEvent>) : Compaction.CompactionHookDeps =
    {
        Client = client
        SessionModel = ModelReference.Parse "test/session-model"
        CompactionModel = nullString
        KeepMessages = 2
        CatalogEntry = testEntry
        ReservedBufferTokens = 100
        Observer = Unchecked.defaultof<IUsageObserver>
        ModelPolicy = Unchecked.defaultof<IModelPolicy>
        Tenant = tenant
        SessionId = sessionId
        TurnId = turnId
        Attempt = 1
        JournalAsync = recordingJournal journal
        IsLeaseValid = (fun () -> true)
    }

let private baseRequest
    (client: IChatClient)
    (history: IList<ChatMessage>)
    (journal: ResizeArray<SessionEvent>)
    : Compaction.CompactionRequest =
    let deps = baseDeps client journal

    {
        Client = deps.Client
        History = history
        SessionModel = deps.SessionModel
        CompactionModel = deps.CompactionModel
        KeepMessages = deps.KeepMessages
        CatalogEntry = deps.CatalogEntry
        ReservedBufferTokens = deps.ReservedBufferTokens
        Observer = deps.Observer
        ModelPolicy = deps.ModelPolicy
        Tenant = deps.Tenant
        SessionId = deps.SessionId
        TurnId = deps.TurnId
        Attempt = deps.Attempt
        InputTokens = 0L
        OutputTokens = 0L
        JournalAsync = deps.JournalAsync
        IsLeaseValid = deps.IsLeaseValid
        CancellationToken = CancellationToken.None
    }

let private runRequest (request: Compaction.CompactionRequest) : Compaction.CompactionOutcome =
    Compaction.tryCompactAsync request |> fun task -> task.GetAwaiter().GetResult()

// ──────────────────────────────────────────────────────────────────────────
// Threshold and rewrite

[<Fact>]
let ``Over-threshold history crosses while small history stays under`` () =
    testThreshold |> should equal 1200

    let over, overEstimate, _ =
        Compaction.shouldCompact (overHistory ()) sessionId turnId testEntry 100

    over |> should equal true
    (overEstimate > int64 testThreshold) |> should equal true

    let under, _, _ =
        Compaction.shouldCompact (smallHistory ()) sessionId turnId testEntry 100

    under |> should equal false

[<Fact>]
let ``Unknown model falls back to the catalog defaults for the threshold`` () =
    let over, estimate, threshold =
        Compaction.shouldCompact (overHistory ()) sessionId turnId nullEntry 100

    threshold |> should equal (ContextPruning.PruneThreshold(nullEntry, 100))
    // 128k window, 20k reserve, 10k buffer: far above the test history.
    over |> should equal false
    (estimate > 0L) |> should equal true

[<Fact>]
let ``Rewrite keeps the system message plus the summary plus the last K`` () =
    let history = overHistory ()

    match Compaction.planRewrite history "the gist" 2 with
    | None -> failwith "Expected a rewrite plan."
    | Some planned ->
        planned.Length |> should equal 4
        planned[0].Role |> should equal ChatRole.System
        planned[0].Text |> should equal "sys"
        planned[1].Role |> should equal ChatRole.User

        (planned[1].Text.StartsWith(Compaction.SummaryMarker, StringComparison.Ordinal))
        |> should equal true

        (planned[1].Text.Contains("the gist")) |> should equal true
        planned[2] |> should equal history[history.Count - 2]
        planned[3] |> should equal history[history.Count - 1]

[<Fact>]
let ``Rewrite without a system prefix starts at the summary`` () =
    let history =
        ResizeArray<ChatMessage>(
            [|
                userMessage "one"
                assistantMessage "two"
            |]
        )
        :> IList<ChatMessage>

    match Compaction.planRewrite history "the gist" 1 with
    | None -> failwith "Expected a rewrite plan."
    | Some planned ->
        planned.Length |> should equal 2
        planned[0].Role |> should equal ChatRole.User
        planned[1] |> should equal history[1]

[<Fact>]
let ``Rewrite with keep zero keeps only the system message and the summary`` () =
    match Compaction.planRewrite (overHistory ()) "the gist" 0 with
    | None -> failwith "Expected a rewrite plan."
    | Some planned ->
        planned.Length |> should equal 2
        planned[0].Role |> should equal ChatRole.System
        planned[1].Role |> should equal ChatRole.User

[<Fact>]
let ``Rewrite plans nothing when the tail already covers the transcript`` () =
    Compaction.planRewrite (overHistory ()) "the gist" 10 |> should equal None
    Compaction.planRewrite (smallHistory ()) "the gist" 5 |> should equal None

[<Fact>]
let ``Rewrite rejects nulls and negative keep counts`` () =
    let history = overHistory ()

    (fun () -> Compaction.planRewrite history nullString 2 |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> Compaction.planRewrite history "gist" -1 |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

    (fun () -> Compaction.wouldReplace history -1 |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

// ──────────────────────────────────────────────────────────────────────────
// Runner: success

[<Fact>]
let ``Over-threshold transcript compacts, journals CompactedEvent, and folds usage`` () =
    let journal = ResizeArray<SessionEvent>()
    let observer = RecordingObserver()

    let client =
        scripted
            [
                ScriptStep.Text("the gist", 30L, 12L)
            ]

    let history = overHistory ()
    let before = Compaction.estimateHistory history sessionId turnId

    let request =
        { baseRequest (client :> IChatClient) history journal with
            Observer = observer :> IUsageObserver
        }

    match runRequest request with
    | Compaction.Compacted(beforeEstimate, afterEstimate, inputTokens, outputTokens) ->
        beforeEstimate |> should equal before
        (afterEstimate < beforeEstimate) |> should equal true
        inputTokens |> should equal 30L
        outputTokens |> should equal 12L

        afterEstimate
        |> should equal (Compaction.estimateHistory history sessionId turnId)

        // The rewrite landed: system, marked summary, last two.
        history.Count |> should equal 4
        history[0].Text |> should equal "sys"

        (history[1].Text.StartsWith(Compaction.SummaryMarker, StringComparison.Ordinal))
        |> should equal true

        (history[1].Text.Contains("the gist")) |> should equal true

        // One audit event with the same estimates.
        journal.Count |> should equal 1
        let compacted = journal[0] :?> CompactedEvent
        compacted.BeforeEstimate |> should equal beforeEstimate
        compacted.AfterEstimate |> should equal afterEstimate

        // One checkpoint with the turn-cumulative totals under the
        // session model: no override was configured.
        observer.Checkpoints.Count |> should equal 1
        let checkpoint = observer.Checkpoints[0]
        checkpoint.Provider |> should equal "test"
        checkpoint.Model |> should equal "session-model"
        checkpoint.InputTokens |> should equal 30L
        checkpoint.OutputTokens |> should equal 12L
        checkpoint.SessionId |> should equal sessionId
        checkpoint.TurnId |> should equal turnId
        checkpoint.Attempt |> should equal 1
    | outcome -> failwith $"Expected Compacted, observed %A{outcome}."

[<Fact>]
let ``Under-threshold transcript needs nothing and never calls the provider`` () =
    let journal = ResizeArray<SessionEvent>()
    let client = scripted []
    let history = smallHistory ()

    runRequest (baseRequest (client :> IChatClient) history journal)
    |> should equal Compaction.NotNeeded

    client.Calls |> should equal 0
    journal.Count |> should equal 0
    history.Count |> should equal 2

[<Fact>]
let ``Override model authorises and reports under the override`` () =
    let journal = ResizeArray<SessionEvent>()
    let observer = RecordingObserver()
    let policy = RecordingPolicy()
    let client = scripted [ ScriptStep.Text("the gist", 3L, 4L) ]
    let history = overHistory ()

    let request =
        { baseRequest (client :> IChatClient) history journal with
            CompactionModel = "other/compact-model"
            Observer = observer :> IUsageObserver
            ModelPolicy = policy :> IModelPolicy
        }

    match runRequest request with
    | Compaction.Compacted(_, _, inputTokens, outputTokens) ->
        inputTokens |> should equal 3L
        outputTokens |> should equal 4L
        policy.Calls |> Seq.toList |> should equal [ ("other", "compact-model") ]
        observer.Checkpoints.Count |> should equal 1
        observer.Checkpoints[0].Provider |> should equal "other"
        observer.Checkpoints[0].Model |> should equal "compact-model"
    | outcome -> failwith $"Expected Compacted, observed %A{outcome}."

// ──────────────────────────────────────────────────────────────────────────
// Runner: failure continues

[<Fact>]
let ``Model denial emits CompactionFailedEvent and continues uncompacted`` () =
    let journal = ResizeArray<SessionEvent>()
    let client = scripted []
    let history = overHistory ()

    let request =
        { baseRequest (client :> IChatClient) history journal with
            ModelPolicy = DenyModelPolicy("quota spent") :> IModelPolicy
        }

    match runRequest request with
    | Compaction.FailedContinue(reason, inputTokens, outputTokens) ->
        reason |> should equal "quota spent"
        inputTokens |> should equal 0L
        outputTokens |> should equal 0L

        // No provider call ran; the history is untouched.
        client.Calls |> should equal 0
        history.Count |> should equal 7

        journal.Count |> should equal 1
        let failed = journal[0] :?> CompactionFailedEvent
        failed.Reason |> should equal "quota spent"
    | outcome -> failwith $"Expected FailedContinue, observed %A{outcome}."

[<Fact>]
let ``Provider error emits CompactionFailedEvent and continues uncompacted`` () =
    let journal = ResizeArray<SessionEvent>()

    let client =
        scripted
            [
                ScriptStep.Failure(InvalidOperationException("boom"))
            ]

    let history = overHistory ()

    match runRequest (baseRequest (client :> IChatClient) history journal) with
    | Compaction.FailedContinue(reason, _, _) ->
        (reason.Contains("boom")) |> should equal true
        history.Count |> should equal 7

        journal.Count |> should equal 1
        let failed = journal[0] :?> CompactionFailedEvent
        (failed.Reason.Contains("boom")) |> should equal true
    | outcome -> failwith $"Expected FailedContinue, observed %A{outcome}."

[<Fact>]
let ``Empty summary emits CompactionFailedEvent and continues uncompacted`` () =
    let journal = ResizeArray<SessionEvent>()
    let client = scripted [ ScriptStep.Text("") ]
    let history = overHistory ()

    match runRequest (baseRequest (client :> IChatClient) history journal) with
    | Compaction.FailedContinue(reason, _, _) ->
        (reason.Contains("empty summary")) |> should equal true
        history.Count |> should equal 7
        journal.Count |> should equal 1
        (journal[0] :? CompactionFailedEvent) |> should equal true
    | outcome -> failwith $"Expected FailedContinue, observed %A{outcome}."

[<Fact>]
let ``Invalid override emits CompactionFailedEvent without calling the provider`` () =
    let journal = ResizeArray<SessionEvent>()
    let client = scripted []
    let history = overHistory ()

    let request =
        { baseRequest (client :> IChatClient) history journal with
            CompactionModel = "not a reference"
        }

    match runRequest request with
    | Compaction.FailedContinue(reason, _, _) ->
        (reason.Contains("not a valid model reference")) |> should equal true
        client.Calls |> should equal 0
        history.Count |> should equal 7
        journal.Count |> should equal 1
    | outcome -> failwith $"Expected FailedContinue, observed %A{outcome}."

[<Fact>]
let ``Cancellation propagates instead of failing over to continue`` () =
    use cancelled = new CancellationTokenSource()
    cancelled.Cancel()

    let journal = ResizeArray<SessionEvent>()
    let client = scripted [ ScriptStep.Text("the gist") ]
    let history = overHistory ()

    let request =
        { baseRequest (client :> IChatClient) history journal with
            CancellationToken = cancelled.Token
        }

    (fun () -> runRequest request |> ignore)
    |> should throw typeof<OperationCanceledException>

    journal.Count |> should equal 0

/// Flips the lease the first time the provider is touched: the boundary
/// check passed, then the takeover landed before the journal write.
type FlipLeaseClient(inner: ScriptedChatClient, onCall: unit -> unit) =
    interface IChatClient with
        member _.GetResponseAsync(history, options, cancellationToken) =
            onCall ()
            (inner :> IChatClient).GetResponseAsync(history, options, cancellationToken)

        member _.GetStreamingResponseAsync(_, _, _) =
            raise (NotSupportedException("The flip client never streams."))

        member _.GetService(_, _) = null
        member _.Dispose() = ()

[<Fact>]
let ``Takeover loser journals nothing and loses the lease`` () =
    let journal = ResizeArray<SessionEvent>()
    let inner = scripted [ ScriptStep.Text("the gist") ]
    let mutable alive = true
    let client = new FlipLeaseClient(inner, (fun () -> alive <- false))
    let history = overHistory ()

    let request =
        { baseRequest (client :> IChatClient) history journal with
            IsLeaseValid = (fun () -> alive)
        }

    (fun () -> runRequest request |> ignore)
    |> should throw typeof<TurnLoop.TurnLeaseLostException>

    journal.Count |> should equal 0

// ──────────────────────────────────────────────────────────────────────────
// TurnLoop boundary

let private loopOptions (deps: Compaction.CompactionHookDeps) : TurnLoop.TurnLoopOptions =
    { TurnLoop.TurnLoopOptions.Default with
        Compaction = Some(Compaction.createHook deps)
    }

let private noDrain () : IReadOnlyList<InboxEntry> =
    ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>

let private runBoundaryLoop
    (client: ScriptedChatClient)
    (history: IList<ChatMessage>)
    (tools: IReadOnlyDictionary<string, AITool>)
    (options: TurnLoop.TurnLoopOptions)
    : TurnLoop.TurnLoopCompletion =
    TurnLoop.runAsyncWithDeltasAndInjects
        (client :> IChatClient)
        history
        tools
        options
        (NeverDelay() :> ILlmDelay)
        CancellationToken.None
        alwaysLeased
        ignore
        ignore
        noDrain
        (fun _ -> ())
        (fun _ -> ())
    |> fun task -> task.GetAwaiter().GetResult()

[<Fact>]
let ``Boundary compacts the over-threshold transcript and the turn continues`` () =
    let journal = ResizeArray<SessionEvent>()

    let client =
        scripted
            [
                ScriptStep.Text("the gist", 30L, 12L)
                textStep "final answer"
            ]

    let history = overHistory ()

    let completion =
        runBoundaryLoop client history (makeTools []) (loopOptions (baseDeps (client :> IChatClient) journal))

    completion.Result.Status |> should equal TurnStatus.Completed
    completion.Result.AssistantText |> should equal "final answer"
    completion.Result.Usage.InputTokens |> should equal 30L
    completion.Result.Usage.OutputTokens |> should equal 12L

    // One compaction plus the final assistant message.
    journal.Count |> should equal 1
    let compacted = journal[0] :?> CompactedEvent
    (compacted.BeforeEstimate > compacted.AfterEstimate) |> should equal true

    history.Count |> should equal 5

    (history[1].Text.StartsWith(Compaction.SummaryMarker, StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Boundary continues the turn after a model denial`` () =
    let journal = ResizeArray<SessionEvent>()
    let client = scripted [ textStep "done" ]
    let history = overHistory ()

    let deps = baseDeps (client :> IChatClient) journal

    let deps =
        { deps with
            ModelPolicy = DenyModelPolicy("quota spent") :> IModelPolicy
        }

    let completion = runBoundaryLoop client history (makeTools []) (loopOptions deps)

    completion.Result.Status |> should equal TurnStatus.Completed
    completion.Result.AssistantText |> should equal "done"

    // The failure audit landed; the history was never rewritten.
    journal.Count |> should equal 1
    let failed = journal[0] :?> CompactionFailedEvent
    failed.Reason |> should equal "quota spent"
    history.Count |> should equal 8

    history
    |> Seq.exists (fun message -> message.Text.StartsWith(Compaction.SummaryMarker, StringComparison.Ordinal))
    |> should equal false

[<Fact>]
let ``Boundary skips compaction under the threshold`` () =
    let journal = ResizeArray<SessionEvent>()
    let client = scripted [ textStep "done" ]
    let history = smallHistory ()

    let completion =
        runBoundaryLoop client history (makeTools []) (loopOptions (baseDeps (client :> IChatClient) journal))

    completion.Result.Status |> should equal TurnStatus.Completed
    journal.Count |> should equal 0
    client.Calls |> should equal 1

[<Fact>]
let ``Boundary compacts once per iteration while the transcript stays over`` () =
    let journal = ResizeArray<SessionEvent>()

    let client =
        scripted
            [
                ScriptStep.Text("sum-1")
                callStep "c1" "lookup"
                ScriptStep.Text("sum-2")
                textStep "done"
            ]

    let invocations = ref []
    let history = largeHistory ()

    let deps = baseDeps (client :> IChatClient) journal
    let deps = { deps with KeepMessages = 10 }

    let completion =
        runBoundaryLoop
            client
            history
            (makeTools
                [
                    "lookup", stubTool "lookup" "row-1" invocations
                ])
            (loopOptions deps)

    completion.Result.Status |> should equal TurnStatus.Completed
    completion.Result.AssistantText |> should equal "done"
    invocations.Value |> should equal [ "lookup" ]

    // Two boundaries, one pass each: two summariser calls plus two turn
    // calls, two audit events. A per-boundary loop would have consumed
    // more summaries within the first boundary.
    client.Calls |> should equal 4
    journal.Count |> should equal 2
    (journal[0] :? CompactedEvent) |> should equal true
    (journal[1] :? CompactedEvent) |> should equal true

[<Fact>]
let ``Suspendable entry shares the same boundary mechanism`` () =
    let journal = ResizeArray<SessionEvent>()

    let client =
        scripted
            [
                ScriptStep.Text("the gist", 5L, 7L)
                textStep "final"
            ]

    let history = overHistory ()

    let completion =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (makeTools [])
            (loopOptions (baseDeps (client :> IChatClient) journal))
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            alwaysLeased
            noDrain
            (fun _ -> ())
            (fun _ -> ())
            Unchecked.defaultof<IPermissionPolicy>
            sessionId
            turnId
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    completion.Result.Status |> should equal TurnStatus.Completed
    completion.Result.AssistantText |> should equal "final"
    completion.Result.Usage.InputTokens |> should equal 5L
    completion.Result.Usage.OutputTokens |> should equal 7L

    journal.Count |> should equal 1
    (journal[0] :? CompactedEvent) |> should equal true
    history.Count |> should equal 5

// ──────────────────────────────────────────────────────────────────────────
// On-demand history rebuild and force hook (issue 46)

/// Three short text turns: under every test threshold, but replaceable
/// past keep two, so only a forced pass compacts it.
let private forceHistory () : IList<ChatMessage> =
    ResizeArray<ChatMessage>(
        [|
            userMessage "alpha"
            assistantMessage "beta"
            userMessage "gamma"
        |]
    )
    :> IList<ChatMessage>

[<Fact>]
let ``messagesFromCells round-trips text histories through identical cells`` () =
    for history in [ overHistory (); smallHistory () ] do
        let cells = Compaction.toEstimateCells history sessionId turnId
        let rebuilt = Compaction.messagesFromCells cells
        let roundTripped = Compaction.toEstimateCells rebuilt sessionId turnId
        roundTripped.Count |> should equal cells.Count

        for index in 0 .. cells.Count - 1 do
            roundTripped[index].Kind |> should equal cells[index].Kind
            roundTripped[index].Content |> should equal cells[index].Content
            roundTripped[index].ToolName |> should equal cells[index].ToolName

        ContextPruning.Estimate(roundTripped)
        |> should equal (ContextPruning.Estimate(cells))

[<Fact>]
let ``messagesFromCells preserves estimates over journal-shaped cells`` () =
    let cell
        (kind: SessionCellKind)
        (content: string)
        (toolName: string | null)
        (toolCallId: string | null)
        : SessionCell =
        {
            Id = Unchecked.defaultof<CellId>
            SessionId = sessionId
            TurnId = turnId
            Kind = kind
            Content = content
            ToolName = toolName
            ToolCallId = toolCallId
            IsError = false
            Iteration = 0
            Metadata = Unchecked.defaultof<IReadOnlyDictionary<string, string>>
            Artifacts = Unchecked.defaultof<IReadOnlyList<string>>
            Timestamp = DateTimeOffset.UtcNow
        }

    let cells =
        ResizeArray<SessionCell>(
            [|
                cell SessionCellKind.User "what is the refund policy?" null null
                cell SessionCellKind.Assistant "Let me look that up." null null
                cell SessionCellKind.ToolCall "" "lookup" "call-1"
                cell SessionCellKind.ToolResult "30 days with receipt" "lookup" "call-1"
                cell SessionCellKind.System "quota spent" null null
            |]
        )
        :> IReadOnlyList<SessionCell>

    let rebuilt = Compaction.messagesFromCells cells
    rebuilt.Count |> should equal 5
    rebuilt[0].Role |> should equal ChatRole.User
    rebuilt[2].Role |> should equal ChatRole.Assistant
    rebuilt[3].Role |> should equal ChatRole.Tool
    rebuilt[4].Role |> should equal ChatRole.System

    Compaction.estimateHistory rebuilt sessionId turnId
    |> should equal (ContextPruning.Estimate(cells))

[<Fact>]
let ``createForceHook compacts an armed request once at the next boundary`` () =
    let journal = ResizeArray<SessionEvent>()
    let client = scripted [ ScriptStep.Text("forced gist") ]
    let force = Compaction.CompactForce()

    let hook =
        Compaction.createForceHook force (baseDeps (client :> IChatClient) journal)

    let history = forceHistory ()

    force.Request()

    let firstInput, firstOutput =
        hook history 0L 0L CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    let secondInput, secondOutput =
        hook history 0L 0L CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    // The armed boundary compacted past the threshold gate; the next
    // boundary found the flag consumed and the history small, so the
    // script kept exactly one summariser call.
    client.Calls |> should equal 1
    journal.Count |> should equal 1
    (journal[0] :? CompactedEvent) |> should equal true
    history.Count |> should equal 3
    (firstInput, firstOutput) |> should equal (0L, 0L)
    (secondInput, secondOutput) |> should equal (firstInput, firstOutput)

[<Fact>]
let ``createForceHook without an armed request behaves like createHook`` () =
    let journal = ResizeArray<SessionEvent>()
    let client = scripted [ ScriptStep.Text("unused gist") ]

    let hook =
        Compaction.createForceHook (Compaction.CompactForce()) (baseDeps (client :> IChatClient) journal)

    let inputTokens, outputTokens =
        hook (smallHistory ()) 3L 4L CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    (inputTokens, outputTokens) |> should equal (3L, 4L)
    client.Calls |> should equal 0
    journal.Count |> should equal 0
