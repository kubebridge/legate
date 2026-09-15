// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TaskToolTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Agents
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.Time.Testing
open Xunit

// Tests for the nested task tool (issue 72): the schema carrier, the
// definition lookup and depth gate, the filtered pool, the nested run
// with its markers and shaped results, the deadline and model rules, the
// suspension propagation with resume, and the fencing that keeps a
// takeover loser effect-free. TurnLoopTests owns the shared doubles
// (scripted, callStep, textStep, stubTool, makeTools, NeverDelay,
// RecordingClockDelay, toolMessages); this module only adds task-shaped
// steps, the agent store seeding, and the hook builders.

// ───────────────────────────────────────────────────────────────────────────
// Helpers

let private tenant = TenantId.Default

/// Seeds an in-memory agent store with the given rows.
let private seedStore (agents: Agent list) : IAgentStore =
    let database = InMemoryDatabase(TimeProvider.System)
    let store = InMemoryAgentStore(database) :> IAgentStore

    for agent in agents do
        store.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None).GetAwaiter().GetResult()
        |> ignore

    store

/// Builds one agent row carrying the given tool selection.
let private customAgent (name: string) (selection: ToolSelection | null) : Agent =
    {
        Id = AgentId.New()
        Tenant = tenant
        Name = name
        Description = "Custom."
        Model = ModelReference.Parse "test/model"
        SystemPrompt = "Custom prompt."
        EnvironmentVariables = null
        PermissionDefaults = null
        ToolSelection = selection
        PackageReference = null
        Enabled = true
        Schedule = null
        RowVersion = 0UL
        CreatedAt = DateTimeOffset.UtcNow
        UpdatedAt = DateTimeOffset.UtcNow
    }

/// The task call arguments for one sub-agent run.
let private taskArguments (subagent: string) (taskText: string) : IDictionary<string, obj> =
    let args = Dictionary<string, obj>()
    args["subagent"] <- subagent :> obj
    args["task"] <- taskText :> obj
    args :> IDictionary<string, obj>

/// One scripted task call step carrying its arguments.
let private taskCallStep (callId: string) (subagent: string) (taskText: string) : ScriptStep =
    ScriptStep.ToolCall(callId, TurnLoop.TaskToolName, taskArguments subagent taskText)

/// Builds the hook scope over the seeded store.
let private hookDeps
    (store: IAgentStore)
    (config: SubAgentsOptions)
    (journal: (SessionEvent -> Task<JournalWriter.JournalWriteResult>) option)
    : TaskRunner.TaskHookDeps =
    {
        Store = store
        Tenant = tenant
        Config = config
        Depth = 0
        ResolveClient = None
        Journal = journal
    }

/// Builds one parent scope for the hook.
let private hookRequest
    (call: FunctionCallContent)
    (tools: IReadOnlyDictionary<string, AITool>)
    (options: TurnLoop.TurnLoopOptions)
    (client: IChatClient)
    : TurnLoop.TaskNestedRequest =
    {
        Call = call
        Tools = tools
        Options = options
        Client = client
        Delay = TurnLoopTests.NeverDelay() :> ILlmDelay
        CancellationToken = CancellationToken.None
        IsLeaseValid = TurnLoopTests.alwaysLeased
        Policy = Unchecked.defaultof<IPermissionPolicy>
        SessionId = SessionId.New()
        ParentTurnId = TurnId.New()
        NewRequestId = None
        AllowedForSession = HashSet<string>()
    }

/// Runs one parent turn through the suspendable loop synchronously.
let private runParent
    (client: ScriptedChatClient)
    (history: IList<ChatMessage>)
    (tools: IReadOnlyDictionary<string, AITool>)
    (options: TurnLoop.TurnLoopOptions)
    (policy: IPermissionPolicy)
    (delay: ILlmDelay)
    (verifyClaim: (unit -> Task<bool>) option)
    : TurnLoop.TurnLoopCompletion =
    let parentOptions =
        { options with
            VerifyClaim = verifyClaim
        }

    TurnLoop.runSuspendableAsync
        (client :> IChatClient)
        history
        tools
        parentOptions
        delay
        CancellationToken.None
        TurnLoopTests.alwaysLeased
        (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
        ignore
        ignore
        policy
        (SessionId.New())
        (TurnId.New())
        None
        (HashSet<string>())
    |> fun task -> task.GetAwaiter().GetResult()

/// A policy asking for exec and allowing everything else.
type private AskExecPolicy() =
    interface IPermissionPolicy with
        member _.Evaluate(request: PermissionRequest) =
            if String.Equals(request.ToolName, "exec", StringComparison.Ordinal) then
                PermissionVerdict.Ask
            else
                PermissionVerdict.Allow

/// The tool result text answering one call id in the history.
let private resultFor (history: IList<ChatMessage>) (callId: string) : string =
    TurnLoopTests.toolMessages history
    |> List.collect (fun message ->
        message.Contents
        |> Seq.choose (fun content ->
            match content with
            | :? FunctionResultContent as result when
                not (isNull (box result))
                && String.Equals(result.CallId, callId, StringComparison.Ordinal)
                ->
                match result.Result with
                | :? string as text -> Some(if isNull (box text) then "" else text)
                | null -> Some("")
                | other ->
                    match other.ToString() with
                    | null -> Some("")
                    | value -> Some(value)
            | _ -> None)
        |> List.ofSeq)
    |> List.tryHead
    |> Option.defaultValue ""

// ───────────────────────────────────────────────────────────────────────────
// Carrier

[<Fact>]
let ``Task tool name is the loop constant`` () =
    TaskTool.ToolName |> should equal "task"
    TaskTool.ToolName |> should equal TurnLoop.TaskToolName

[<Fact>]
let ``Create rejects null and blank descriptions`` () =
    Assert.Throws<ArgumentNullException>(fun () -> TaskTool.Create(Unchecked.defaultof<string>) |> ignore)
    |> ignore

    Assert.Throws<ArgumentException>(fun () -> TaskTool.Create("  ") |> ignore)
    |> ignore

[<Fact>]
let ``Carrier validates arguments before raising out of contract`` () =
    let carrier = TaskTool.Create("Runs sub-agents.")

    let missingArgs = AIFunctionArguments()

    let missing =
        Assert.Throws<ToolException>(fun () ->
            carrier.InvokeAsync(missingArgs, CancellationToken.None).GetAwaiter().GetResult()
            |> ignore)

    missing.ToolName |> should equal "task"

    let args = AIFunctionArguments()
    args["subagent"] <- "explore" :> obj
    args["task"] <- "look around" :> obj

    Assert.Throws<InvalidOperationException>(fun () ->
        carrier.InvokeAsync(args, CancellationToken.None).GetAwaiter().GetResult()
        |> ignore)
    |> ignore

[<Fact>]
let ``Description lists the available sub-agents`` () =
    let agents =
        ResizeArray<Agent>(
            [|
                SubAgents.createGeneralAgent tenant
                SubAgents.createExploreAgent tenant
            |]
        )
        :> IReadOnlyList<Agent>

    let description = TaskTool.DescriptionFor(agents)

    description.Contains("Available sub-agents:") |> should equal true

    description.Contains("- explore: Explores the workspace with read-only tools and reports back what it found.")
    |> should equal true

    description.Contains("- general: A general-purpose sub-agent for tasks that need the full tool set.")
    |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Lookup and depth

[<Fact>]
let ``Unknown sub-agent returns a tool error listing the available names`` () =
    let store = seedStore [ SubAgents.createExploreAgent tenant ]
    let deps = hookDeps store (SubAgentsOptions()) None

    let call =
        FunctionCallContent("c1", TurnLoop.TaskToolName, taskArguments "nope" "do it")

    let outcome =
        TaskRunner.runAsync
            deps
            (hookRequest
                call
                (TurnLoopTests.makeTools [])
                TurnLoop.TurnLoopOptions.Default
                (TurnLoopTests.scripted [] :> IChatClient))
        |> fun task -> task.GetAwaiter().GetResult()

    outcome.Text.StartsWith("Error: unknown sub-agent 'nope'.", StringComparison.Ordinal)
    |> should equal true

    outcome.Text.Contains("explore") |> should equal true
    outcome.Iterations |> should equal 0

[<Fact>]
let ``Depth limit returns a tool error without running`` () =
    let client = TurnLoopTests.scripted [ TurnLoopTests.textStep "never" ]
    let store = seedStore [ SubAgents.createExploreAgent tenant ]
    let config = SubAgentsOptions(MaxDepth = 1)

    let deps =
        { hookDeps store config None with
            Depth = 1
        }

    let call =
        FunctionCallContent("c1", TurnLoop.TaskToolName, taskArguments "explore" "do it")

    let outcome =
        TaskRunner.runAsync
            deps
            (hookRequest call (TurnLoopTests.makeTools []) TurnLoop.TurnLoopOptions.Default (client :> IChatClient))
        |> fun task -> task.GetAwaiter().GetResult()

    outcome.Text.StartsWith("Error: sub-agent depth limit 1 reached:", StringComparison.Ordinal)
    |> should equal true

    client.Calls |> should equal 0

[<Fact>]
let ``Blank arguments return tool errors`` () =
    let store = seedStore [ SubAgents.createExploreAgent tenant ]
    let deps = hookDeps store (SubAgentsOptions()) None
    let tools = TurnLoopTests.makeTools []
    let options = TurnLoop.TurnLoopOptions.Default
    let client = TurnLoopTests.scripted [] :> IChatClient

    let blankSubagent =
        FunctionCallContent("c1", TurnLoop.TaskToolName, taskArguments "" "do it")

    let first =
        TaskRunner.runAsync deps (hookRequest blankSubagent tools options client)
        |> fun task -> task.GetAwaiter().GetResult()

    first.Text.StartsWith("Error: The task tool needs a sub-agent:", StringComparison.Ordinal)
    |> should equal true

    let blankTask =
        FunctionCallContent("c2", TurnLoop.TaskToolName, taskArguments "explore" "  ")

    let second =
        TaskRunner.runAsync deps (hookRequest blankTask tools options client)
        |> fun task -> task.GetAwaiter().GetResult()

    second.Text.StartsWith("Error: The task tool needs a task:", StringComparison.Ordinal)
    |> should equal true

    let args = Dictionary<string, obj>()
    args["subagent"] <- "explore" :> obj
    args["task"] <- "do it" :> obj
    args["model"] <- "not-a-reference" :> obj
    let badModel = FunctionCallContent("c3", TurnLoop.TaskToolName, args)

    let third =
        TaskRunner.runAsync deps (hookRequest badModel tools options client)
        |> fun task -> task.GetAwaiter().GetResult()

    third.Text.StartsWith("Error: The task tool needs a valid model override:", StringComparison.Ordinal)
    |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Pool

[<Fact>]
let ``Pool excludes terminal tools and task at max depth`` () =
    let invocations = ref []

    let parent =
        TurnLoopTests.makeTools
            [
                "read_file", TurnLoopTests.stubTool "read_file" "r" invocations
                "ask_user", TurnLoopTests.stubTool "ask_user" "q" invocations
                "skill", TurnLoopTests.stubTool "skill" "s" invocations
                "task", TurnLoopTests.stubTool "task" "t" invocations
                "exec", TurnLoopTests.stubTool "exec" "e" invocations
            ]

    let pool = TaskPool.buildPool parent null 0 1

    pool.ContainsKey("read_file") |> should equal true
    pool.ContainsKey("skill") |> should equal true
    pool.ContainsKey("exec") |> should equal true
    pool.ContainsKey("ask_user") |> should equal false
    pool.ContainsKey("task") |> should equal false

[<Fact>]
let ``Pool keeps task below max depth`` () =
    let invocations = ref []

    let parent =
        TurnLoopTests.makeTools
            [
                "task", TurnLoopTests.stubTool "task" "t" invocations
            ]

    let pool = TaskPool.buildPool parent null 0 2

    pool.ContainsKey("task") |> should equal true

[<Fact>]
let ``Pool applies the definition allowlist`` () =
    let invocations = ref []

    let parent =
        TurnLoopTests.makeTools
            [
                "read_file", TurnLoopTests.stubTool "read_file" "r" invocations
                "exec", TurnLoopTests.stubTool "exec" "e" invocations
            ]

    let selection = ToolSelection()
    selection.BuiltIns <- ResizeArray<string>([| "exec" |]) :> IReadOnlyList<string>

    let pool = TaskPool.buildPool parent selection 0 1

    pool.ContainsKey("exec") |> should equal true
    pool.ContainsKey("read_file") |> should equal false

// ───────────────────────────────────────────────────────────────────────────
// Nested run

[<Fact>]
let ``Delegate to explore wraps the nested result and folds iterations`` () =
    let store = seedStore [ SubAgents.createExploreAgent tenant ]
    let hook = TaskRunner.createHook (hookDeps store (SubAgentsOptions()) None)

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some hook
        }

    let carrier =
        TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

    let tools = TurnLoopTests.makeTools [ "task", carrier ]

    let client =
        TurnLoopTests.scripted
            [
                taskCallStep "c-task" "explore" "look around"
                TurnLoopTests.textStep "found it"
                TurnLoopTests.textStep "parent-done"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        runParent
            client
            history
            tools
            options
            Unchecked.defaultof<IPermissionPolicy>
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            None

    completion.Result.Status |> should equal TurnStatus.Completed
    completion.Result.AssistantText |> should equal "parent-done"

    let taskResult = resultFor history "c-task"

    taskResult.Contains("<task_result>") |> should equal true
    taskResult.Contains("found it") |> should equal true

    // One parent iteration plus one nested iteration.
    completion.Result.Iterations |> should equal 2

[<Fact>]
let ``Nested markers carry the parent call id under a nested turn`` () =
    let store = seedStore [ SubAgents.createGeneralAgent tenant ]
    let captured = ResizeArray<SessionEvent>()

    let journal (event: SessionEvent) =
        captured.Add(event)
        Task.FromResult(JournalWriter.JournalAppended(ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>))

    let hook =
        TaskRunner.createHook (hookDeps store (SubAgentsOptions()) (Some journal))

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some hook
        }

    let invocations = ref []
    let exec = TurnLoopTests.stubTool "exec" "out" invocations

    let carrier =
        TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

    let tools = TurnLoopTests.makeTools [ "task", carrier; "exec", exec ]

    let client =
        TurnLoopTests.scripted
            [
                taskCallStep "c-task" "general" "run it"
                TurnLoopTests.callStep "c-exec" "exec"
                TurnLoopTests.textStep "sub-done"
                TurnLoopTests.textStep "parent-done"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let parentTurn = TurnId.New()

    let completion =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            tools
            options
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            CancellationToken.None
            TurnLoopTests.alwaysLeased
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            Unchecked.defaultof<IPermissionPolicy>
            (SessionId.New())
            parentTurn
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    completion.Result.Status |> should equal TurnStatus.Completed
    invocations.Value |> should equal [ "exec" ]

    let taskResult = resultFor history "c-task"
    taskResult.Contains("sub-done") |> should equal true

    let started =
        captured
        |> Seq.choose (fun event ->
            match event with
            | :? ToolCallStartedEvent as s -> Some s
            | _ -> None)
        |> List.ofSeq

    let outputs =
        captured
        |> Seq.choose (fun event ->
            match event with
            | :? ToolCallOutputEvent as o -> Some o
            | _ -> None)
        |> List.ofSeq

    let completed =
        captured
        |> Seq.choose (fun event ->
            match event with
            | :? ToolCallCompletedEvent as c -> Some c
            | _ -> None)
        |> List.ofSeq

    started.Length |> should equal 1
    outputs.Length |> should equal 1
    completed.Length |> should equal 1

    let marker = started.Head
    marker.ToolCallId |> should equal "c-task"
    marker.ToolName |> should equal "exec"
    (marker.TurnId.Equals parentTurn) |> should equal false

    outputs.Head.ToolCallId |> should equal "c-task"
    outputs.Head.Output |> should equal "out"
    (outputs.Head.TurnId.Equals marker.TurnId) |> should equal true

    completed.Head.ToolCallId |> should equal "c-task"
    completed.Head.Error |> should equal null
    (completed.Head.TurnId.Equals marker.TurnId) |> should equal true

[<Fact>]
let ``Nested deadline is the shorter of parent budget and configured timeout`` () =
    let runWith (parentTimeout: TimeSpan) (configuredTimeout: TimeSpan) : TimeSpan list =
        let store = seedStore [ SubAgents.createExploreAgent tenant ]
        let config = SubAgentsOptions(Timeout = configuredTimeout)
        let hook = TaskRunner.createHook (hookDeps store config None)

        let options =
            { TurnLoop.TurnLoopOptions.Default with
                MaxIterations = 50
                Timeout = parentTimeout
                TaskNested = Some hook
            }

        let carrier =
            TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

        let tools = TurnLoopTests.makeTools [ "task", carrier ]

        let client =
            TurnLoopTests.scripted
                [
                    taskCallStep "c-task" "explore" "look"
                    TurnLoopTests.textStep "nested"
                    TurnLoopTests.textStep "parent"
                ]

        let clock = FakeTimeProvider()
        let recorded = ResizeArray<TimeSpan>()
        let delay = TurnLoopTests.RecordingClockDelay(clock, recorded) :> ILlmDelay
        let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

        let completion =
            runParent client history tools options Unchecked.defaultof<IPermissionPolicy> delay None

        completion.Result.Status |> should equal TurnStatus.Completed
        recorded |> List.ofSeq

    let narrow = runWith (TimeSpan.FromMinutes 30.0) (TimeSpan.FromMinutes 5.0)
    narrow |> should contain (TimeSpan.FromMinutes 5.0)

    let wide = runWith (TimeSpan.FromMinutes 3.0) (TimeSpan.FromMinutes 30.0)
    wide |> should contain (TimeSpan.FromMinutes 3.0)

[<Fact>]
let ``Model override resolves through the reference`` () =
    let store = seedStore [ SubAgents.createExploreAgent tenant ]

    let nested =
        TurnLoopTests.scripted
            [
                TurnLoopTests.textStep "nested-says-hi"
            ]

    let mutable resolved: ModelReference = Unchecked.defaultof<ModelReference>

    let resolve (reference: ModelReference) : IChatClient =
        resolved <- reference
        nested :> IChatClient

    let baseDeps = hookDeps store (SubAgentsOptions()) None

    let deps =
        { baseDeps with
            ResolveClient = Some resolve
        }

    let hook = TaskRunner.createHook deps

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some hook
        }

    let carrier =
        TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

    let tools = TurnLoopTests.makeTools [ "task", carrier ]

    let args = Dictionary<string, obj>()
    args["subagent"] <- "explore" :> obj
    args["task"] <- "look" :> obj
    args["model"] <- "acme/probe" :> obj

    let client =
        TurnLoopTests.scripted
            [
                ScriptStep.ToolCall("c-task", TurnLoop.TaskToolName, args)
                TurnLoopTests.textStep "parent"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        runParent
            client
            history
            tools
            options
            Unchecked.defaultof<IPermissionPolicy>
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            None

    completion.Result.Status |> should equal TurnStatus.Completed
    resolved.Provider |> should equal "acme"
    resolved.Model |> should equal "probe"

    let taskResult = resultFor history "c-task"
    taskResult.Contains("nested-says-hi") |> should equal true

[<Fact>]
let ``Compaction hook runs inside nested turns`` () =
    let store = seedStore [ SubAgents.createExploreAgent tenant ]
    let hook = TaskRunner.createHook (hookDeps store (SubAgentsOptions()) None)
    let compactions = ref 0

    let compact (_history: IList<ChatMessage>) (input: int64) (output: int64) (_: CancellationToken) =
        compactions.Value <- compactions.Value + 1
        Task.FromResult((input, output))

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some hook
            Compaction = Some compact
        }

    let carrier =
        TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

    let tools = TurnLoopTests.makeTools [ "task", carrier ]

    let client =
        TurnLoopTests.scripted
            [
                taskCallStep "c-task" "explore" "look"
                TurnLoopTests.textStep "nested"
                TurnLoopTests.textStep "parent"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        runParent
            client
            history
            tools
            options
            Unchecked.defaultof<IPermissionPolicy>
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            None

    completion.Result.Status |> should equal TurnStatus.Completed
    (compactions.Value >= 1) |> should equal true

[<Fact>]
let ``Nested provider failure shapes as sub-agent failed`` () =
    let store = seedStore [ SubAgents.createExploreAgent tenant ]
    let hook = TaskRunner.createHook (hookDeps store (SubAgentsOptions()) None)

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some hook
        }

    let carrier =
        TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

    let tools = TurnLoopTests.makeTools [ "task", carrier ]

    let client =
        TurnLoopTests.scripted
            [
                taskCallStep "c-task" "explore" "look"
                ScriptStep.Failure(InvalidOperationException("boom"))
                TurnLoopTests.textStep "parent"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        runParent
            client
            history
            tools
            options
            Unchecked.defaultof<IPermissionPolicy>
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            None

    completion.Result.Status |> should equal TurnStatus.Completed

    let taskResult = resultFor history "c-task"
    taskResult.Contains("Sub-agent failed: boom") |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Suspension

[<Fact>]
let ``Nested permission ask suspends the parent turn`` () =
    let store = seedStore [ SubAgents.createGeneralAgent tenant ]
    let hook = TaskRunner.createHook (hookDeps store (SubAgentsOptions()) None)

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some hook
        }

    let invocations = ref []
    let exec = TurnLoopTests.stubTool "exec" "out" invocations

    let carrier =
        TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

    let tools = TurnLoopTests.makeTools [ "task", carrier; "exec", exec ]

    let client =
        TurnLoopTests.scripted
            [
                taskCallStep "c-task" "general" "run it"
                TurnLoopTests.callStep "c-exec" "exec"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        runParent
            client
            history
            tools
            options
            (AskExecPolicy() :> IPermissionPolicy)
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            None

    completion.Result.Status |> should equal TurnStatus.Suspended
    invocations.Value.Length |> should equal 0

    match completion.Suspension with
    | None -> failwith "the parent turn should suspend on the nested ask"
    | Some suspension ->
        suspension.Kind |> should equal TurnLoop.PermissionSuspension
        suspension.ToolName |> should equal "exec"
        (String.IsNullOrWhiteSpace suspension.RequestId) |> should equal false

        match suspension.Nested with
        | None -> failwith "the parent suspension should carry the nested resume"
        | Some _ -> ()

[<Fact>]
let ``Nested suspension resumes through the parent carrier`` () =
    let store = seedStore [ SubAgents.createGeneralAgent tenant ]
    let hook = TaskRunner.createHook (hookDeps store (SubAgentsOptions()) None)

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some hook
        }

    let invocations = ref []
    let exec = TurnLoopTests.stubTool "exec" "out" invocations

    let carrier =
        TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

    let tools = TurnLoopTests.makeTools [ "task", carrier; "exec", exec ]

    let client =
        TurnLoopTests.scripted
            [
                taskCallStep "c-task" "general" "run it"
                TurnLoopTests.callStep "c-exec" "exec"
                TurnLoopTests.textStep "sub-done"
                TurnLoopTests.textStep "parent-done"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let policy = AskExecPolicy() :> IPermissionPolicy
    let delay = TurnLoopTests.NeverDelay() :> ILlmDelay

    let suspended = runParent client history tools options policy delay None

    suspended.Result.Status |> should equal TurnStatus.Suspended

    let cursor =
        match suspended.Suspension with
        | None -> failwith "the parent turn should suspend on the nested ask"
        | Some suspension -> suspension

    let nested =
        match cursor.Nested with
        | None -> failwith "the parent suspension should carry the nested resume"
        | Some resume -> resume

    let resumed =
        nested.ResumeAsync
            (PermissionDecision(cursor.RequestId, PermissionDecisionKind.AllowOnce))
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    resumed.Result.Status |> should equal TurnStatus.Completed
    resumed.Result.AssistantText |> should equal "parent-done"
    invocations.Value |> should equal [ "exec" ]

    let taskResult = resultFor history "c-task"
    taskResult.Contains("<task_result>") |> should equal true
    taskResult.Contains("sub-done") |> should equal true

[<Fact>]
let ``Nested question suspends the parent as a question`` () =
    let store = seedStore [ SubAgents.createGeneralAgent tenant ]
    let hook = TaskRunner.createHook (hookDeps store (SubAgentsOptions()) None)

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some hook
        }

    let carrier =
        TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

    let tools = TurnLoopTests.makeTools [ "task", carrier ]

    let args = Dictionary<string, obj>()
    args["question"] <- "which region?" :> obj

    let client =
        TurnLoopTests.scripted
            [
                taskCallStep "c-task" "general" "run it"
                ScriptStep.ToolCall("c-ask", TurnLoop.AskUserToolName, args)
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        runParent
            client
            history
            tools
            options
            Unchecked.defaultof<IPermissionPolicy>
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            None

    completion.Result.Status |> should equal TurnStatus.Suspended

    match completion.Suspension with
    | None -> failwith "the parent turn should suspend on the nested question"
    | Some suspension ->
        suspension.Kind |> should equal TurnLoop.QuestionSuspension
        suspension.QuestionText |> should equal "which region?"

// ───────────────────────────────────────────────────────────────────────────
// Fencing

[<Fact>]
let ``Takeover loser performs zero nested effects`` () =
    let store = seedStore [ SubAgents.createGeneralAgent tenant ]
    let captured = ResizeArray<SessionEvent>()

    let journal (event: SessionEvent) =
        captured.Add(event)
        Task.FromResult(JournalWriter.JournalAppended(ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>))

    let hook =
        TaskRunner.createHook (hookDeps store (SubAgentsOptions()) (Some journal))

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some hook
        }

    let invocations = ref []
    let exec = TurnLoopTests.stubTool "exec" "out" invocations

    let carrier =
        TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

    let tools = TurnLoopTests.makeTools [ "task", carrier; "exec", exec ]

    let client =
        TurnLoopTests.scripted
            [
                taskCallStep "c-task" "general" "run it"
                TurnLoopTests.callStep "c-exec" "exec"
                TurnLoopTests.textStep "never"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let fences = ref 0

    let verifyClaim () =
        fences.Value <- fences.Value + 1

        if fences.Value = 1 then
            Task.FromResult(true)
        else
            Task.FromResult(false)

    let outcome =
        try
            runParent
                client
                history
                tools
                options
                Unchecked.defaultof<IPermissionPolicy>
                (TurnLoopTests.NeverDelay() :> ILlmDelay)
                (Some verifyClaim)
            |> ignore

            "completed"
        with
        | :? TurnLoop.TurnLeaseLostException -> "fenced"
        | ex -> "unexpected: " + ex.GetType().FullName

    outcome |> should equal "fenced"
    invocations.Value.Length |> should equal 0
    captured.Count |> should equal 0

[<Fact>]
let ``Rejected journal write fences the nested run`` () =
    let store = seedStore [ SubAgents.createGeneralAgent tenant ]

    let journal (_: SessionEvent) =
        Task.FromResult(JournalWriter.JournalRejected("stale claim"))

    let hook =
        TaskRunner.createHook (hookDeps store (SubAgentsOptions()) (Some journal))

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some hook
        }

    let invocations = ref []
    let exec = TurnLoopTests.stubTool "exec" "out" invocations

    let carrier =
        TaskTool.Create(TaskTool.DescriptionFor(ResizeArray<Agent>() :> IReadOnlyList<Agent>))

    let tools = TurnLoopTests.makeTools [ "task", carrier; "exec", exec ]

    let client =
        TurnLoopTests.scripted
            [
                taskCallStep "c-task" "general" "run it"
                TurnLoopTests.callStep "c-exec" "exec"
                TurnLoopTests.textStep "never"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let outcome =
        try
            runParent
                client
                history
                tools
                options
                Unchecked.defaultof<IPermissionPolicy>
                (TurnLoopTests.NeverDelay() :> ILlmDelay)
                None
            |> ignore

            "completed"
        with
        | :? TurnLoop.TurnLeaseLostException -> "fenced"
        | ex -> "unexpected: " + ex.GetType().FullName

    // The exec tool ran (the claim was live for execution), but the
    // rejected marker journal fenced the nested run before it settled.
    outcome |> should equal "fenced"
