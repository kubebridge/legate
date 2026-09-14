// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Internal Akka-free ReAct loop core. Per iteration the loop calls
// non-streaming GetResponseAsync, appends the response messages, executes the
// response's function calls sequentially in order, appends the function
// results, and repeats until a response carries no function calls. Unknown
// tool names and tool exceptions map to Error: texts and continue;
// cancellation and lease loss propagate. Tool results over the configured
// char limit are truncated with a single marker. No budgets, no streaming,
// no inject fold, no metadata wrap: those belong to follow-up issues.
//
// Nullness warning 3261 is suppressed in this file: MEAI interop surfaces
// nulls (null responses, messages, contents, usage, result objects) that the
// F# nullable analysis cannot prove absent, and the loop treats every one as
// empty rather than failing.
module internal TurnLoop =

    /// Marker appended when a tool result exceeds the configured char limit.
    [<Literal>]
    let TruncationMarker = "[truncated]"

    /// Tool result text used when the model calls a tool name absent from the resolved map.
    [<Literal>]
    let UnknownToolMessage = "Error: Unknown tool"

    /// Default maximum tool-result chars before truncation.
    [<Literal>]
    let DefaultMaxToolResultChars = 4000

    /// Internal loop tuning. Only the tool-result char limit lives here;
    /// budgets, streaming, inject, and metadata belong to follow-up issues.
    type TurnLoopOptions =
        {
            /// Maximum tool-result chars before truncation with <see cref="TruncationMarker" />.
            MaxToolResultChars: int
        }

        /// Default tuning: 4000 chars before truncation.
        static member Default =
            {
                MaxToolResultChars = DefaultMaxToolResultChars
            }

    /// Raised when the lease-check hook reports the turn lease is lost.
    /// Propagates instead of completing so the actor can fence the loser
    /// with zero further effects.
    type TurnLeaseLostException(message: string) =
        inherit Exception(message)

        /// Creates the exception with the default lease-lost message.
        new() = TurnLeaseLostException("The turn lease was lost.")

    /// Truncates a tool result to the configured limit, appending
    /// TruncationMarker when cut. Exactly-at-limit passes through.
    let truncateToolResult (options: TurnLoopOptions) (text: string) : string =
        let value = if isNull text then "" else text

        if value.Length > options.MaxToolResultChars then
            value.Substring(0, options.MaxToolResultChars) + TruncationMarker
        else
            value

    /// Converts a tool return value to text. Null becomes empty, strings
    /// pass through, anything else uses ToString. Never includes arguments.
    let private toolValueToString (value: obj) : string =
        if isNull value then
            ""
        else
            match value with
            | :? string as text -> if isNull text then "" else text
            | other ->
                let text = other.ToString()
                if isNull text then "" else text

    /// Maps a tool exception to an Error: continuation. Message only, never
    /// secrets or arguments; an empty message falls back to the type name.
    let private toolExceptionToString (ex: Exception) : string =
        let message =
            if isNull ex || String.IsNullOrEmpty ex.Message then
                ex.GetType().Name
            else
                ex.Message

        "Error: " + message

    /// Collects function calls in order across every response message.
    let private collectCalls (messages: IList<ChatMessage>) : FunctionCallContent list =
        [
            for message in messages do
                if not (isNull message) && not (isNull message.Contents) then
                    for content in message.Contents do
                        if not (isNull content) then
                            match content with
                            | :? FunctionCallContent as call when not (isNull call) -> yield call
                            | _ -> ()
        ]

    /// Adds usage counts, treating missing usage and missing counters as zero.
    let private addUsage (inputTokens: int64 byref) (outputTokens: int64 byref) (usage: UsageDetails) =
        if not (isNull usage) then
            if usage.InputTokenCount.HasValue then
                inputTokens <- inputTokens + usage.InputTokenCount.Value

            if usage.OutputTokenCount.HasValue then
                outputTokens <- outputTokens + usage.OutputTokenCount.Value

    /// Invokes one resolved tool and returns its result text. Unknown names
    /// and non-invokable tools map to UnknownToolMessage; tool exceptions
    /// map to Error: texts. Cancellation propagates.
    let private invokeOneAsync
        (tools: IReadOnlyDictionary<string, AITool>)
        (call: FunctionCallContent)
        (cancellationToken: CancellationToken)
        : Task<string> =
        task {
            let mutable tool = Unchecked.defaultof<AITool>
            let found = tools.TryGetValue(call.Name, &tool)

            if not found || isNull tool then
                return UnknownToolMessage
            else
                match tool with
                | :? AIFunction as fn ->
                    try
                        let args =
                            if isNull call.Arguments then
                                AIFunctionArguments()
                            else
                                AIFunctionArguments(call.Arguments)

                        let! result = fn.InvokeAsync(args, cancellationToken)
                        return toolValueToString result
                    with
                    | :? OperationCanceledException as canceled -> return! Task.FromException<string>(canceled)
                    | ex -> return toolExceptionToString ex
                | _ -> return UnknownToolMessage
        }

    /// Runs the ReAct loop to completion. Appends response and tool-result
    /// messages to history in order and returns the final assistant text as
    /// a Completed TurnResult. Checks the lease hook before every provider
    /// call and every tool invocation; cancellation and lease loss propagate.
    let runAsync
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoopOptions)
        (cancellationToken: CancellationToken)
        (isLeaseValid: unit -> bool)
        : Task<TurnResult> =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(history)
        ArgumentNullException.ThrowIfNull(tools)
        ArgumentNullException.ThrowIfNull(isLeaseValid)

        if options.MaxToolResultChars <= 0 then
            raise (ArgumentOutOfRangeException(nameof options, "MaxToolResultChars must be positive."))

        let chatOptions = ChatOptions()
        chatOptions.Tools <- ResizeArray<AITool>(tools.Values) :> IList<AITool>

        let rec loop (iterations: int) (inputTokens: int64) (outputTokens: int64) : Task<TurnResult> =
            task {
                cancellationToken.ThrowIfCancellationRequested()

                if not (isLeaseValid ()) then
                    return! Task.FromException<TurnResult>(TurnLeaseLostException())
                else
                    let! response = client.GetResponseAsync(history, chatOptions, cancellationToken)
                    let nextIterations = iterations + 1
                    let mutable nextInput = inputTokens
                    let mutable nextOutput = outputTokens

                    if isNull response then
                        return
                            {
                                AssistantText = ""
                                Status = TurnStatus.Completed
                                Iterations = nextIterations
                                Usage =
                                    {
                                        InputTokens = nextInput
                                        OutputTokens = nextOutput
                                    }
                                Outcome = null
                            }
                    else
                        addUsage &nextInput &nextOutput response.Usage

                        if not (isNull response.Messages) then
                            for message in response.Messages do
                                if not (isNull message) then
                                    history.Add(message)

                        let calls =
                            if isNull response.Messages then
                                []
                            else
                                collectCalls response.Messages

                        if calls.IsEmpty then
                            let assistantText = if isNull response.Text then "" else response.Text

                            return
                                {
                                    AssistantText = assistantText
                                    Status = TurnStatus.Completed
                                    Iterations = nextIterations
                                    Usage =
                                        {
                                            InputTokens = nextInput
                                            OutputTokens = nextOutput
                                        }
                                    Outcome = null
                                }
                        else
                            for call in calls do
                                cancellationToken.ThrowIfCancellationRequested()

                                if not (isLeaseValid ()) then
                                    raise (TurnLeaseLostException())

                                let! rawText = invokeOneAsync tools call cancellationToken
                                let text = truncateToolResult options rawText
                                let resultContent = FunctionResultContent(call.CallId, text)

                                let toolMessage =
                                    ChatMessage(
                                        ChatRole.Tool,
                                        ResizeArray<AIContent>([| resultContent :> AIContent |]) :> IList<AIContent>
                                    )

                                history.Add(toolMessage)

                            return! loop nextIterations nextInput nextOutput
            }

        loop 0 0L 0L
