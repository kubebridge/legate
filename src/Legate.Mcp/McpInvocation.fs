// SPDX-License-Identifier: Apache-2.0
module internal Legate.Mcp.McpInvocation

open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

// Per-call invocation: the single path every projected MCP tool invokes
// through. It forwards the turn's token to the session (the projected
// InvokeCoreAsync token arrives here from TurnLoop.invokeOneWithErrorAsync,
// so cancelling the turn abandons the call), maps server-flagged errors to
// "Error from" texts and transport failures to "Error calling" texts, and
// stamps one observation per settled call into the injectable sink. Only
// cancellation propagates: every other failure returns as text, so the
// turn continues. Observations carry no turn or call ids: main-path
// journaling does not exist here (only TaskTool.observerOf emits
// SessionEvents), and the session path journals these observations with
// real ids later. Tests assert the observations; nothing here is journaled.

// ──────────────────────────
// Observation

/// One settled MCP call: the model-facing name, the returned text, whether
/// the text is an error, and how long the call took. Internal: asserted in
/// tests here and journaled with real ids by the session path later.
type McpCallObservation =
    {
        /// The model-facing assigned name of the called tool.
        ToolName: string
        /// The returned text: the result or the mapped error.
        Text: string
        /// Whether the text is a mapped error.
        IsError: bool
        /// How long the call took, never negative.
        Duration: TimeSpan
    }

// ──────────────────────────
// Invocation

/// Invokes one server tool through the session and returns its
/// model-facing text: the result, <c>Error from {name}: ...</c> when the
/// server flagged the result as an error, or
/// <c>Error calling {name}: {Type}: {message}</c> when the transport
/// raised. Cancellation propagates and stamps nothing: an abandoned call
/// never settled. Every settled call stamps one observation into the
/// sink; a null sink observes nothing.
/// <param name="session">The connected session owning the tool.</param>
/// <param name="assignedName">The model-facing assigned tool name, carried on errors and observations.</param>
/// <param name="toolName">The server's tool name.</param>
/// <param name="arguments">The call arguments.</param>
/// <param name="cancellationToken">The turn's token: abandoning it abandons the call.</param>
/// <param name="observe">The observation sink, or null to observe nothing.</param>
/// <returns>The model-facing text result.</returns>
let invokeAsync
    (session: IMcpServerSession)
    (assignedName: string)
    (toolName: string)
    (arguments: IReadOnlyDictionary<string, obj>)
    (cancellationToken: CancellationToken)
    (observe: Action<McpCallObservation> | null)
    : Task<string> =
    task {
        ArgumentNullException.ThrowIfNull(session)
        ArgumentNullException.ThrowIfNull(assignedName)
        ArgumentNullException.ThrowIfNull(toolName)

        let watch = Stopwatch.StartNew()

        let observeOne (text: string) (isError: bool) : unit =
            if not (isNull (box observe)) then
                observe.Invoke(
                    {
                        ToolName = assignedName
                        Text = text
                        IsError = isError
                        Duration = watch.Elapsed
                    }
                )

        try
            let! callResult = session.CallToolAsync(toolName, arguments, cancellationToken)
            let text = if isNull (box callResult.Text) then "" else callResult.Text

            if callResult.IsError then
                let mapped = $"Error from {assignedName}: {text}"
                observeOne mapped true
                return mapped
            else
                observeOne text false
                return text
        with
        | :? OperationCanceledException as canceled -> return! Task.FromException<string>(canceled)
        | ex ->
            let mapped = $"Error calling {assignedName}: {ex.GetType().Name}: {ex.Message}"
            observeOne mapped true
            return mapped
    }
