// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Legate.Agents
open Microsoft.Extensions.AI

// Nullness warning 3261 is suppressed in this file: MEAI invocation
// surfaces nulls (null arguments, null call fields, null agent rows) that
// the F# nullable analysis cannot prove absent, and the runner treats every
// one as an expected branch rather than failing.
// Nested task tool (issue 72): the single model-facing AIFunction running
// one sub-agent definition from #71 (the explore/general built-ins plus
// user definitions through IAgentStore) as a nested in-process turn loop.
// The loop intercepts task calls into the TurnLoopOptions.TaskNested hook
// before any invocation (exactly like ask_user and skill); the function
// itself only carries the schema and raises on direct calls. The hook
// filters the parent tool map into the nested pool (terminal tools always
// excluded, task itself excluded at the depth limit, the definition's
// tools allowlist applied on top), resolves the optional ModelReference
// override, and runs the nested loop through
// TurnLoop.runSuspendableAsync sharing the session workspace and journal:
// the same client shape, policy, delay seam, lease hooks, claim fence,
// compaction hook, and AllowForSession memory, with only the timeout
// narrowed to min(parent budget, configured sub-agent timeout). A nested
// suspension raises TurnLoop.TaskNestedSuspended carrying the nested
// cursor, so the parent parks carrying it and every reply re-enters the
// nested loop before the parent continues. Nested tool invocations journal
// tool-call started/output/completed markers under a fresh nested turn id
// carrying the parent call's id on ToolCallId: the transcript read links
// them back to the parent call, and the parent call's own started marker
// belongs to the future parent write side, never to this runner. The
// transcript read already links any turn carrying a foreign tool-call id,
// so markers unattributed until that write side lands are kept, never
// gated. Rejected: a separate session/actor per sub-agent (breaks the
// shared workspace/journal and the parent-tool-call-id correlation, and
// duplicates the suspend/resume plumbing).

/// The tool's identity: its model-facing name and description, kept in
/// one internal module so the function type and the factory serve the
/// same literals. The name is the TurnLoop task constant itself, so the
/// schema the model reads and the nested runner the loop drives can never
/// drift apart.
module internal TaskIdentity =

    /// The tool's name as the model calls it: the nested sub-agent runner.
    let name = TurnLoop.TaskToolName

    /// The description head every task tool carries: what the tool does,
    /// its arguments, the result and failure shapes, the depth rule, the
    /// terminal exclusion, and the suspension contract. The available
    /// sub-agent listing follows it, built per turn from the store.
    let descriptionHead =
        "Runs a sub-agent as a nested turn (task). "
        + "Pass the sub-agent to run in 'subagent' (required, one name as listed under Available sub-agents), "
        + "the objective in 'task' (required), and an optional model override in 'model' "
        + "(provider/model form; absent means the session model). "
        + "The sub-agent runs with its own filtered tool pool sharing the session workspace and journal: "
        + "ask_user is unavailable inside sub-agents, and 'task' itself is unavailable at the depth limit. "
        + "The nested run applies the same compaction and permission policy as the parent; "
        + "a nested permission request suspends the parent turn. "
        + "Success returns the sub-agent's result under <task_result>; a failed nested run returns 'Sub-agent failed: ...'; "
        + "a call past the depth limit or naming an unknown sub-agent returns an Error."

    /// Builds the tool description for one agent listing: the head plus
    /// the #71 sub-agent listing the cross-verification pins.
    /// <param name="agents">The available sub-agents to list, or null for none.</param>
    /// <returns>The description the model reads.</returns>
    let describe (agents: IReadOnlyList<Agent>) : string =
        descriptionHead + "\n" + SubAgents.buildTaskDescription agents

/// The tool's argument schema, kept in one internal module so the schema
/// tests assert on the same JSON the tool serves.
module internal TaskSchema =

    /// The JSON schema served on the tool's JsonSchema: subagent and task
    /// are required strings, model is an optional provider/model override.
    let json =
        """{"type":"object","description":"Arguments for the task tool.","properties":{"subagent":{"type":"string","description":"The sub-agent to run, as listed in the task tool description. Required."},"task":{"type":"string","description":"The objective the sub-agent carries out. Required."},"model":{"type":"string","description":"An optional model override in provider/model form. Absent means the session model."}},"required":["subagent","task"],"additionalProperties":false}"""

    /// The single parsed schema document backing every tool instance; the
    /// JsonSchema elements borrow it, so it lives as long as the process.
    let document = JsonDocument.Parse json

    /// Reads one raw argument by name; missing reads as null.
    /// <param name="args">The call's arguments.</param>
    /// <param name="name">The argument to read.</param>
    /// <returns>The raw value, or null when the argument is missing.</returns>
    let argument (args: AIFunctionArguments) (name: string) : obj | null =
        let mutable value: obj | null = null

        if args.TryGetValue(name, &value) then value else null

    /// Reads one argument value as text: plain strings and JSON string
    /// elements; anything else is not text.
    /// <param name="value">The raw argument value.</param>
    /// <returns>The text, or null when the value is not text.</returns>
    let asText (value: obj | null) : string | null =
        match value with
        | null -> null
        | :? string as text -> text
        | :? JsonElement as element when element.ValueKind = JsonValueKind.String -> element.GetString()
        | _ -> null

/// The nested tool pool: the filtered view of the parent map one nested
/// run may call. Terminal tools are the loop-special tools that end the
/// calling turn instead of returning into it: ask_user suspends, so a
/// nested question would park the parent on a sub-agent question the
/// contract never requires (only permission requests propagate). The
/// skill loader stays: it returns package metadata, never suspends.
module internal TaskPool =

    /// Tool names never offered to a nested run: the turn-ending ask_user.
    /// Named explicitly because no terminal set exists elsewhere in the
    /// codebase; extending it is a one-line, fully tested change.
    let terminalToolNames: string list = [ TurnLoop.AskUserToolName ]

    /// Builds the nested pool from the parent map: terminal tools out,
    /// task itself out once the nested run reaches the depth limit, and
    /// the definition's built-in allowlist intersected on top (a null or
    /// empty selection means the runtime default of every tool). Entries
    /// are shared instances: the nested run filters the map, never
    /// rebinds turn-scoped tools. Tool-source scoping is not re-applied:
    /// sources merged into the parent map cannot be un-merged.
    /// <param name="parentTools">The parent turn's resolved tool map, or null for empty.</param>
    /// <param name="selection">The sub-agent's tool selection, or null for the runtime default.</param>
    /// <param name="depth">The depth the task call runs at; 0 is the top-level turn.</param>
    /// <param name="maxDepth">The configured depth limit.</param>
    /// <returns>The nested pool, keyed ordinally by tool name.</returns>
    let buildPool
        (parentTools: IReadOnlyDictionary<string, AITool>)
        (selection: ToolSelection | null)
        (depth: int)
        (maxDepth: int)
        : IReadOnlyDictionary<string, AITool> =
        let table = Dictionary<string, AITool>(StringComparer.Ordinal)

        let allowBuiltIns =
            match Option.ofObj selection with
            | None -> None
            | Some selected when isNull (box selected.BuiltIns) -> None
            | Some selected ->
                selected.BuiltIns
                |> Seq.filter (fun name -> not (isNull (box name)))
                |> fun names -> HashSet<string>(names, StringComparer.Ordinal)
                |> Some

        let nestedDepth = depth + 1

        if not (isNull (box parentTools)) then
            for entry in parentTools do
                if not (isNull (box entry.Key)) && not (isNull (box entry.Value)) then
                    let isTerminal =
                        terminalToolNames
                        |> List.exists (fun terminal -> String.Equals(entry.Key, terminal, StringComparison.Ordinal))

                    let isTaskAtLimit =
                        nestedDepth >= maxDepth
                        && String.Equals(entry.Key, TurnLoop.TaskToolName, StringComparison.Ordinal)

                    let isAllowed =
                        match allowBuiltIns with
                        | None -> true
                        | Some allowed -> allowed.Contains entry.Key

                    if not isTerminal && not isTaskAtLimit && isAllowed then
                        table[entry.Key] <- entry.Value

        table :> IReadOnlyDictionary<string, AITool>

/// The stable result shapes the model reads: success wrapped so the
/// parent turn can attribute it, failures prefixed so the parent turn can
/// branch on them. Argument and lookup problems are plain tool errors
/// (the model continues); only a run that started and settled badly
/// carries the failure prefix.
module internal TaskResult =

    /// Wraps one nested assistant text as the task call's tool result.
    /// <param name="text">The nested assistant text.</param>
    /// <returns>The wrapped result text.</returns>
    let wrapResult (text: string) : string =
        let body = if isNull text then "" else text
        "<task_result>\n" + body + "\n</task_result>"

    /// Shapes one nested failure: the reason the nested run settled
    /// badly, never secrets or tool arguments.
    /// <param name="reason">Why the nested run failed.</param>
    /// <returns>The failure text.</returns>
    let subAgentFailed (reason: string) : string =
        let message =
            if String.IsNullOrWhiteSpace reason then
                "the sub-agent turn failed"
            else
                reason

        "Sub-agent failed: " + message

    /// Shapes a call past the depth limit as a tool error: the run never
    /// started, so the model continues.
    /// <param name="maxDepth">The configured depth limit.</param>
    /// <returns>The depth error text.</returns>
    let depthExceeded (maxDepth: int) : string =
        sprintf "Error: sub-agent depth limit %d reached: the task tool cannot run a sub-agent here." maxDepth

    /// Shapes an unknown sub-agent name as a tool error listing the
    /// enabled names in ordinal order, mirroring the skill tool's
    /// unknown-name error.
    /// <param name="requested">The name the model asked for.</param>
    /// <param name="available">The enabled sub-agent names in ordinal order.</param>
    /// <returns>The Error result text.</returns>
    let unknownSubAgent (requested: string) (available: string list) : string =
        match available with
        | [] -> sprintf "Error: unknown sub-agent '%s'. Available sub-agents: none." requested
        | names ->
            sprintf "Error: unknown sub-agent '%s'. Available sub-agents: %s." requested (String.Join(", ", names))

/// The turn-time nested runner behind the task tool: definition lookup,
/// pool filtering, the nested loop, the journaled markers, and the resume.
/// Internal so the TurnLoop hook stays the only runner surface; tests
/// drive createHook directly and through the parent loop.
module internal TaskRunner =

    /// Everything one nested scope needs that the parent scope does not
    /// carry: the definition source and tenant, the depth and timeout
    /// contract, the model-override client resolution, and the fenced
    /// journal sink. The parent claim token rides the sink the session
    /// assembly built, so nested markers journal under the parent claim
    /// and a takeover loser appends nothing.
    type TaskHookDeps =
        {
            /// The agent store the definitions resolve from. Never null.
            Store: IAgentStore
            /// The tenant the definitions belong to.
            Tenant: TenantId
            /// The depth and timeout contract. Never null.
            Config: SubAgentsOptions
            /// The depth the hooked turn runs at; 0 is the top-level turn.
            Depth: int
            /// Resolves the model-override client, or None to always run
            /// nested turns on the parent client.
            ResolveClient: (ModelReference -> IChatClient) option
            /// The fenced single-event journal sink for the nested markers,
            /// or None to journal nothing.
            Journal: (SessionEvent -> Task<JournalWriter.JournalWriteResult>) option
        }

    /// No-op Inject drain: the nested run never folds parent injects.
    let private noNestedInjects () : IReadOnlyList<InboxEntry> =
        ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>

    /// No-op Inject hook: records and marks nothing.
    let private ignoreNestedInject (_: InboxEntry) : unit = ()

    /// Maps one journal outcome onto the nested run: appended continues,
    /// rejected means the parent claim moved on (the loser stops like a
    /// fenced-out tool call), and failed faults the nested run into the
    /// failure shape instead of journaling half a turn.
    /// <param name="outcome">The journal write outcome. Must not be null.</param>
    let private applyJournalOutcome (outcome: JournalWriter.JournalWriteResult) : unit =
        match outcome with
        | JournalWriter.JournalAppended _ -> ()
        | JournalWriter.JournalRejected reason ->
            raise (
                TurnLoop.TurnLeaseLostException(
                    if String.IsNullOrWhiteSpace reason then
                        "The turn lease was lost."
                    else
                        reason
                )
            )
        | JournalWriter.JournalFailed reason ->
            raise (
                InvalidOperationException(
                    if String.IsNullOrWhiteSpace reason then
                        "The sub-agent journal write failed."
                    else
                        sprintf "The sub-agent journal write failed: %s." reason
                )
            )

    /// Journals one nested marker through the fenced sink after the
    /// last-moment lease check, so a takeover loser journals nothing even
    /// when the sink itself would accept the write.
    /// <param name="journal">The fenced sink. Must not be null.</param>
    /// <param name="isLeaseValid">The last-moment claim fence. Must not be null.</param>
    /// <param name="event">The marker to journal. Must not be null.</param>
    let private journalMarkerAsync
        (journal: SessionEvent -> Task<JournalWriter.JournalWriteResult>)
        (isLeaseValid: unit -> bool)
        (event: SessionEvent)
        : Task<unit> =
        task {
            if not (isLeaseValid ()) then
                raise (TurnLoop.TurnLeaseLostException())

            let! outcome = journal event
            applyJournalOutcome outcome
        }

    /// Builds the nested observation hook from the sink: every settled
    /// nested invocation journals its started, output, and completed
    /// markers under the nested turn carrying the parent call's id, so
    /// the transcript read links them back to the parent call.
    /// <param name="journal">The fenced sink, or None to journal nothing.</param>
    /// <param name="isLeaseValid">The last-moment claim fence.</param>
    /// <param name="sessionId">The session the nested run belongs to.</param>
    /// <param name="nestedTurn">The nested turn the markers belong to.</param>
    /// <param name="parentCallId">The parent task call id every marker carries.</param>
    /// <returns>The observation hook for the nested options, or None.</returns>
    let private observerOf
        (journal: (SessionEvent -> Task<JournalWriter.JournalWriteResult>) option)
        (isLeaseValid: unit -> bool)
        (sessionId: SessionId)
        (nestedTurn: TurnId)
        (parentCallId: string)
        : (TurnLoop.ToolCallObservation -> Task<unit>) option =
        match journal with
        | None -> None
        | Some sink ->
            Some(fun observation ->
                task {
                    let noSequence = Unchecked.defaultof<Nullable<int64>>
                    let stamp = DateTimeOffset.UtcNow

                    do!
                        journalMarkerAsync
                            sink
                            isLeaseValid
                            (ToolCallStartedEvent(
                                sessionId,
                                nestedTurn,
                                noSequence,
                                stamp,
                                parentCallId,
                                observation.ToolName
                            )
                            :> SessionEvent)

                    do!
                        journalMarkerAsync
                            sink
                            isLeaseValid
                            (ToolCallOutputEvent(
                                sessionId,
                                nestedTurn,
                                noSequence,
                                stamp,
                                parentCallId,
                                observation.Text
                            )
                            :> SessionEvent)

                    do!
                        journalMarkerAsync
                            sink
                            isLeaseValid
                            (ToolCallCompletedEvent(
                                sessionId,
                                nestedTurn,
                                noSequence,
                                stamp,
                                parentCallId,
                                Option.toObj observation.Error
                            )
                            :> SessionEvent)
                })

    /// Builds the nested history: the definition's system prompt leads
    /// when present, then the task text as the user message, mirroring
    /// the session runners' history shapes.
    /// <param name="agent">The sub-agent definition. Must not be null.</param>
    /// <param name="taskText">The objective text. Must not be null.</param>
    /// <returns>The nested history.</returns>
    let historyOf (agent: Agent) (taskText: string) : IList<ChatMessage> =
        if isNull (box agent) then
            raise (ArgumentNullException(nameof agent))

        let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

        if not (String.IsNullOrWhiteSpace agent.SystemPrompt) then
            history.Add(ChatMessage(ChatRole.System, agent.SystemPrompt))

        history.Add(ChatMessage(ChatRole.User, if isNull taskText then "" else taskText))
        history

    /// Lists the enabled definitions for one tenant in ordinal name order:
    /// the availability set both the description and the lookup share, so
    /// the model can only name what the lookup resolves.
    /// <param name="store">The agent store. Must not be null.</param>
    /// <param name="tenant">The tenant the definitions belong to.</param>
    /// <param name="cancellationToken">Abandons the read.</param>
    /// <returns>The enabled agents in ordinal name order.</returns>
    let listAvailableAsync
        (store: IAgentStore)
        (tenant: TenantId)
        (cancellationToken: CancellationToken)
        : Task<Agent list> =
        task {
            let! agents = store.ListAgents(tenant, cancellationToken)

            if isNull (box agents) then
                return []
            else
                return
                    agents
                    |> Seq.filter (fun agent ->
                        not (isNull (box agent)) && not (isNull (box agent.Name)) && agent.Enabled)
                    |> Seq.sortWith (fun left right -> String.Compare(left.Name, right.Name, StringComparison.Ordinal))
                    |> List.ofSeq
        }

    /// Builds one settled hook result from shaped text and nested totals.
    /// <param name="text">The shaped result text.</param>
    /// <param name="iterations">Model iterations the nested run spent.</param>
    /// <param name="inputTokens">Input tokens the nested run spent.</param>
    /// <param name="outputTokens">Output tokens the nested run spent.</param>
    /// <returns>The settled hook result.</returns>
    let private resultOf
        (text: string)
        (iterations: int)
        (inputTokens: int64)
        (outputTokens: int64)
        : TurnLoop.TaskNestedResult =
        {
            Text = text
            Iterations = iterations
            InputTokens = inputTokens
            OutputTokens = outputTokens
        }

    /// Shapes one settled nested turn into the hook result: a completed
    /// turn wraps its assistant text, any other status shapes its reason
    /// as a failure. Every status is named: incomplete matches are build
    /// errors here, and a suspended completion never reaches this path
    /// (suspensions raise instead of returning).
    /// <param name="result">The settled nested turn result. Must not be null.</param>
    /// <returns>The shaped hook result.</returns>
    let shapeSettled (result: TurnResult) : TurnLoop.TaskNestedResult =
        if isNull (box result) then
            raise (ArgumentNullException(nameof result))

        let text =
            match result.Status with
            | TurnStatus.Completed -> TaskResult.wrapResult result.AssistantText
            | TurnStatus.Failed ->
                match box result.Outcome with
                | :? TurnFailed as failed when not (isNull (box failed)) && not (isNull failed.Reason) ->
                    TaskResult.subAgentFailed failed.Reason
                | _ -> TaskResult.subAgentFailed null
            | TurnStatus.Suspended -> TaskResult.subAgentFailed "the sub-agent suspended without a cursor"
            | TurnStatus.Aborted -> TaskResult.subAgentFailed "the sub-agent turn was aborted"
            | TurnStatus.Pending
            | TurnStatus.Running -> TaskResult.subAgentFailed "the sub-agent turn never settled"
            | unknown -> TaskResult.subAgentFailed (sprintf "the sub-agent turn ended as %O" unknown)

        resultOf text result.Iterations result.Usage.InputTokens result.Usage.OutputTokens

    /// Parses one task call into its sub-agent, objective, and optional
    /// model override: missing or blank inputs and unparsable overrides
    /// return the tool error text, so the parent turn continues with the
    /// error instead of faulting on it.
    /// <param name="args">The call's arguments. Must not be null.</param>
    /// <returns>The parsed call, or the tool error text.</returns>
    let private parseCall (args: AIFunctionArguments) : Result<string * string * ModelReference option, string> =
        let subagent = TaskSchema.asText (TaskSchema.argument args "subagent")
        let taskText = TaskSchema.asText (TaskSchema.argument args "task")
        let modelRaw = TaskSchema.asText (TaskSchema.argument args "model")

        if String.IsNullOrWhiteSpace subagent then
            Error "Error: The task tool needs a sub-agent: pass the sub-agent to run in 'subagent'."
        elif String.IsNullOrWhiteSpace taskText then
            Error "Error: The task tool needs a task: pass the objective the sub-agent carries out in 'task'."
        elif String.IsNullOrWhiteSpace modelRaw then
            Ok(subagent, taskText, None)
        else
            let mutable parsed = Unchecked.defaultof<ModelReference>

            if ModelReference.TryParse(modelRaw, &parsed) then
                Ok(subagent, taskText, Some parsed)
            else
                Error "Error: The task tool needs a valid model override: pass 'model' in provider/model form."

    /// Resolves the model-override client: a parsed override resolves
    /// through the hook's resolver, absent means the parent client, and a
    /// null resolution falls back to the parent client.
    /// <param name="model">The parsed override, or None for the parent client.</param>
    /// <param name="parentClient">The parent turn's client. Must not be null.</param>
    /// <param name="resolveClient">The override resolver, or None to keep the parent client.</param>
    /// <returns>The client the nested run calls.</returns>
    let private resolveNestedClient
        (model: ModelReference option)
        (parentClient: IChatClient)
        (resolveClient: (ModelReference -> IChatClient) option)
        : IChatClient =
        match model with
        | None -> parentClient
        | Some reference ->
            match resolveClient with
            | Some resolve ->
                let client = resolve reference

                if isNull (box client) then parentClient else client
            | None -> parentClient

    /// Continues one suspended nested run with the host's reply: permission
    /// replies resume permission suspensions, question answers resume
    /// question suspensions, and anything else raises instead of resuming
    /// the wrong cursor. A settled nested run shapes into the hook
    /// result; a re-suspended one raises TaskNestedSuspended again with
    /// the fresh cursor and the resume rebuilt around it.
    /// <param name="cursor">The nested suspend cursor. Must not be null.</param>
    /// <param name="client">The client the nested run calls. Must not be null.</param>
    /// <param name="pool">The nested tool pool. Must not be null.</param>
    /// <param name="options">The nested tuning and budget. Must not be null.</param>
    /// <param name="delay">The delay seam the nested deadline fires off. Must not be null.</param>
    /// <param name="isLeaseValid">The lease hook the nested loop checks. Must not be null.</param>
    /// <param name="policy">The permission policy, or null for no gate.</param>
    /// <param name="allowed">The shared session memory. Must not be null.</param>
    /// <returns>The resume the parent suspension carries.</returns>
    let rec private resumeOf
        (cursor: TurnLoop.TurnLoopSuspension)
        (client: IChatClient)
        (pool: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        (isLeaseValid: unit -> bool)
        (policy: IPermissionPolicy)
        (allowed: HashSet<string>)
        : TurnLoop.TaskNestedResume =
        if isNull (box cursor) then
            raise (ArgumentNullException(nameof cursor))

        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(pool)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(isLeaseValid)
        ArgumentNullException.ThrowIfNull(allowed)

        fun (reply: Reply) (resumeToken: CancellationToken) ->
            task {
                let! resumed =
                    match box reply with
                    | :? PermissionDecision as decision when
                        not (isNull (box decision)) && cursor.Kind = TurnLoop.PermissionSuspension
                        ->
                        TurnLoop.resumePermissionAsync
                            cursor
                            decision.Decision
                            client
                            cursor.HistorySnapshot
                            pool
                            options
                            delay
                            resumeToken
                            isLeaseValid
                            policy
                            allowed
                    | :? QuestionAnswer as answer when
                        not (isNull (box answer)) && cursor.Kind = TurnLoop.QuestionSuspension
                        ->
                        TurnLoop.resumeQuestionAsync
                            cursor
                            answer.Answer
                            client
                            cursor.HistorySnapshot
                            pool
                            options
                            delay
                            resumeToken
                            isLeaseValid
                            policy
                            allowed
                    | _ ->
                        raise (
                            InvalidOperationException(
                                "The nested resume received a reply that does not match the suspension."
                            )
                        )

                match resumed.Suspension with
                | Some fresh ->
                    return!
                        Task.FromException<TurnLoop.TaskNestedResult>(
                            TurnLoop.TaskNestedSuspended(
                                fresh,
                                resumeOf fresh client pool options delay isLeaseValid policy allowed
                            )
                        )
                | None -> return shapeSettled resumed.Result
            }

    /// Runs one nested sub-agent turn over the parent scope: validates the
    /// call, resolves the enabled definition ordinally by name, filters
    /// the nested pool, and runs the nested loop under the nested
    /// deadline. Suspensions raise TaskNestedSuspended; lease loss and
    /// cancellation propagate; every other failure shapes into the
    /// failure text so the parent turn continues.
    /// <param name="deps">The nested scope. Must not be null.</param>
    /// <param name="request">The parent scope. Must not be null.</param>
    /// <returns>The settled hook result.</returns>
    let rec runAsync (deps: TaskHookDeps) (request: TurnLoop.TaskNestedRequest) : Task<TurnLoop.TaskNestedResult> =
        if isNull (box deps) then
            raise (ArgumentNullException(nameof deps))

        if isNull (box request) then
            raise (ArgumentNullException(nameof request))

        ArgumentNullException.ThrowIfNull(deps.Store)
        ArgumentNullException.ThrowIfNull(deps.Config)

        task {
            let args =
                if isNull (box request.Call) || isNull (box request.Call.Arguments) then
                    AIFunctionArguments()
                else
                    AIFunctionArguments(request.Call.Arguments)

            match parseCall args with
            | Error text -> return resultOf text 0 0L 0L
            | Ok(subagent, taskText, model) ->
                if deps.Depth >= deps.Config.MaxDepth then
                    return resultOf (TaskResult.depthExceeded deps.Config.MaxDepth) 0 0L 0L
                else
                    let! available = listAvailableAsync deps.Store deps.Tenant request.CancellationToken

                    let definition =
                        available
                        |> List.tryFind (fun agent -> String.Equals(agent.Name, subagent, StringComparison.Ordinal))

                    match definition with
                    | None ->
                        let names = available |> List.map (fun agent -> agent.Name)
                        return resultOf (TaskResult.unknownSubAgent subagent names) 0 0L 0L
                    | Some agent ->
                        ArgumentNullException.ThrowIfNull(request.Client)
                        ArgumentNullException.ThrowIfNull(request.Delay)
                        ArgumentNullException.ThrowIfNull(request.IsLeaseValid)

                        let client = resolveNestedClient model request.Client deps.ResolveClient

                        let pool =
                            TaskPool.buildPool request.Tools agent.ToolSelection deps.Depth deps.Config.MaxDepth

                        let nestedTurn = TurnId.New()
                        let history = historyOf agent taskText

                        let nestedTimeout =
                            if request.Options.Timeout < deps.Config.Timeout then
                                request.Options.Timeout
                            else
                                deps.Config.Timeout

                        let nestedDeps = { deps with Depth = deps.Depth + 1 }
                        let nestedHook = createHook nestedDeps

                        let parentCallId =
                            if isNull (box request.Call) || isNull request.Call.CallId then
                                ""
                            else
                                request.Call.CallId

                        let observer =
                            observerOf deps.Journal request.IsLeaseValid request.SessionId nestedTurn parentCallId

                        let nestedOptions =
                            { request.Options with
                                Timeout = nestedTimeout
                                TaskNested = Some nestedHook
                                OnToolCall = observer
                            }

                        try
                            let! completion =
                                TurnLoop.runSuspendableAsync
                                    client
                                    history
                                    pool
                                    nestedOptions
                                    request.Delay
                                    request.CancellationToken
                                    request.IsLeaseValid
                                    noNestedInjects
                                    ignoreNestedInject
                                    ignoreNestedInject
                                    request.Policy
                                    request.SessionId
                                    nestedTurn
                                    request.NewRequestId
                                    request.AllowedForSession

                            match completion.Suspension with
                            | Some cursor ->
                                return!
                                    Task.FromException<TurnLoop.TaskNestedResult>(
                                        TurnLoop.TaskNestedSuspended(
                                            cursor,
                                            resumeOf
                                                cursor
                                                client
                                                pool
                                                nestedOptions
                                                request.Delay
                                                request.IsLeaseValid
                                                request.Policy
                                                (if isNull (box request.AllowedForSession) then
                                                     HashSet<string>()
                                                 else
                                                     request.AllowedForSession)
                                        )
                                    )
                            | None -> return shapeSettled completion.Result
                        with
                        | :? OperationCanceledException as canceled ->
                            return! Task.FromException<TurnLoop.TaskNestedResult>(canceled)
                        | :? TurnLoop.TurnLeaseLostException as lost ->
                            return! Task.FromException<TurnLoop.TaskNestedResult>(lost)
                        | :? TurnLoop.TaskNestedSuspended as suspended ->
                            return! Task.FromException<TurnLoop.TaskNestedResult>(suspended)
                        | ex ->
                            let message = if isNull ex.Message then ex.GetType().Name else ex.Message
                            return resultOf (TaskResult.subAgentFailed message) 0 0L 0L
        }

    /// Builds the task-tool nested hook for one depth: the TurnLoop branch
    /// invokes it per task call with the parent scope.
    /// <param name="deps">The nested scope. Must not be null.</param>
    /// <returns>The hook the loop drives.</returns>
    and createHook (deps: TaskHookDeps) : TurnLoop.TaskNestedRun =
        if isNull (box deps) then
            raise (ArgumentNullException(nameof deps))

        ArgumentNullException.ThrowIfNull(deps.Store)
        ArgumentNullException.ThrowIfNull(deps.Config)
        fun request -> runAsync deps request

/// The <c>task</c> function served to the model. Internal: hosts hold the
/// <see cref="T:Microsoft.Extensions.AI.AIFunction" /> that
/// <see cref="T:Legate.TaskTool" /> hands back, never this type. The
/// function validates the call and then raises: the turn loop intercepts
/// task calls into the nested runner before any invocation, so a direct
/// call is out of contract.
[<Sealed>]
type internal TaskFunction(description: string) =
    inherit AIFunction()

    do
        if isNull (box description) then
            raise (ArgumentNullException(nameof description))

    /// This tool's name for error text.
    override _.Name = TaskIdentity.name

    /// This tool's description for the model.
    override _.Description = description

    /// This tool's argument schema.
    override _.JsonSchema = TaskSchema.document.RootElement

    /// Validates the call's sub-agent and task, then raises: execution
    /// belongs to the turn loop's nested runner, never to this function.
    /// A missing or blank sub-agent or task raises
    /// <see cref="T:Legate.ToolException" />; a well-formed direct call
    /// raises <see cref="T:System.InvalidOperationException" />.
    /// Cancellation propagates as-is.
    override _.InvokeCoreAsync
        (args: AIFunctionArguments, cancellationToken: CancellationToken)
        : ValueTask<obj | null> =
        ValueTask<obj | null>(
            task {
                cancellationToken.ThrowIfCancellationRequested()

                let args = if isNull (box args) then AIFunctionArguments() else args

                let subagent = TaskSchema.asText (TaskSchema.argument args "subagent")
                let taskText = TaskSchema.asText (TaskSchema.argument args "task")

                if String.IsNullOrWhiteSpace subagent then
                    return
                        raise (
                            ToolException(
                                TaskIdentity.name,
                                "The task tool needs a sub-agent: pass the sub-agent to run in 'subagent'."
                            )
                        )
                elif String.IsNullOrWhiteSpace taskText then
                    return
                        raise (
                            ToolException(
                                TaskIdentity.name,
                                "The task tool needs a task: pass the objective the sub-agent carries out in 'task'."
                            )
                        )
                else
                    return
                        raise (
                            InvalidOperationException(
                                "The task tool runs through the turn loop's nested runner: invoke it through a turn, never directly."
                            )
                        )
            }
        )

/// Builds the <c>task</c> built-in tool: the schema the model runs
/// sub-agents with. The carrier never executes: the turn loop intercepts
/// its calls into the nested runner (see
/// <see cref="F:Legate.TurnLoop.TaskToolName" />), and the per-turn
/// session assembly binds the description to the currently available
/// sub-agents through <see cref="M:Legate.TaskTool.DescriptionFor" />.
type TaskTool private () =

    /// The tool's name as the model calls it: the nested sub-agent
    /// runner. Validated against <see cref="T:Legate.ToolNameRules" />
    /// when the tool is built.
    static member ToolName: string = TaskIdentity.name

    /// Builds the tool description for the available sub-agents: the
    /// usage head plus the #71 listing, so the description cross-verifies
    /// the definitions criterion once the tool exists.
    /// <param name="agents">The available sub-agents to list, or null for none.</param>
    /// <returns>The description the model reads.</returns>
    static member DescriptionFor(agents: IReadOnlyList<Agent>) : string = TaskIdentity.describe agents

    /// Builds the tool carrier with a fixed description: the schema the
    /// model calls, with no execution behind it. Per-turn assembly lists
    /// the available sub-agents, builds the description through
    /// <see cref="M:Legate.TaskTool.DescriptionFor" />, and binds one
    /// carrier per turn.
    /// <param name="description">The tool description the model reads. Must be a non-empty string.</param>
    /// <returns>The tool to offer to the model.</returns>
    static member Create(description: string) : AIFunction =
        if isNull (box description) then
            raise (ArgumentNullException(nameof description))

        if String.IsNullOrWhiteSpace description then
            raise (ArgumentException("The task tool needs a non-empty description.", nameof description))

        ToolNameRules.Validate(TaskTool.ToolName) |> ignore
        TaskFunction(description) :> AIFunction
