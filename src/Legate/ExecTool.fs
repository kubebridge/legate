// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

open System
open System.Collections.Generic
open System.Text.Json
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Nullness warning 3261 is suppressed in this file: MEAI invocation and
// the workspace primitive surface nulls (a missing command argument, null
// streams, a null agent env) that the F# nullable analysis cannot prove
// absent, and the tool treats every one as empty or absent rather than
// failing.
// Built-in exec tool: the single model-facing AIFunction mapping verbatim
// onto the merged IWorkspace.Exec primitive. The command string reaches the
// primitive byte-identical (no shell re-quoting: the primitive owns shell,
// kill, and env semantics), the timeout is clamped to the fixed 1..300 s
// bounds, standard output and standard error are truncated independently at
// the fixed 100 KB per-stream cap, and the injected environment carries only
// the agent environment allowlist (never host environment widened by the
// tool: the primitive inherits its own execution context). The result shape
// is {exit_code, stdout, stderr, timed_out}. Secrets never reach events,
// logs, or the permission preview: the function throws without embedding the
// command or environment values, the TurnLoop permission request carries an
// empty preview by construction, and tool output is journaled through
// JournalWriter redaction downstream. Rejected: a new process primitive, now
// configurability of the clamp/truncation constants, and explicit host-env
// passing.
module internal ExecTool =

    /// The model-facing tool name the turn map keys the exec tool by.
    /// Validated against ToolNameRules on creation.
    [<Literal>]
    let ToolName = "exec"

    /// The minimum timeout_seconds the tool accepts: smaller values clamp
    /// up. A fixed constant per the plan ledger.
    [<Literal>]
    let MinTimeoutSeconds = 1

    /// The maximum timeout_seconds the tool accepts: larger values clamp
    /// down. A fixed constant per the plan ledger.
    [<Literal>]
    let MaxTimeoutSeconds = 300

    /// The per-stream output cap, 100 KB measured in characters, applied to
    /// standard output and standard error independently. A fixed constant
    /// per the plan ledger. Truncation happens in the tool immediately on
    /// return, before any event, log, or result text is built, so unbounded
    /// primitive output never reaches memory held past the call.
    [<Literal>]
    let MaxOutputCharsPerStream = 102400

    /// The model-facing tool description carried on the AIFunction.
    [<Literal>]
    let Description =
        "Run a shell command in the session workspace and report its exit code, standard output, standard error, and whether it timed out."

    /// Clamps a timeout in whole seconds into the fixed 1..300 s bounds.
    /// <param name="seconds">The requested whole seconds.</param>
    /// <returns>The seconds clamped into MinTimeoutSeconds..MaxTimeoutSeconds.</returns>
    let clampTimeoutSeconds (seconds: int) : int =
        if seconds < MinTimeoutSeconds then MinTimeoutSeconds
        elif seconds > MaxTimeoutSeconds then MaxTimeoutSeconds
        else seconds

    /// Truncates one captured output stream to MaxOutputCharsPerStream,
    /// appending TurnLoop.TruncationMarker when cut. Exactly-at-limit passes
    /// through; null reads as empty (the primitive contract promises
    /// non-null, this only defends the JSON shape).
    /// <param name="value">The captured stream text, or null.</param>
    /// <returns>The bounded stream text, never null.</returns>
    let truncateStream (value: string | null) : string =
        let text = if isNull value then "" else value

        if text.Length > MaxOutputCharsPerStream then
            text.Substring(0, MaxOutputCharsPerStream) + TurnLoop.TruncationMarker
        else
            text

    /// Copies the agent environment allowlist for injection: only entries
    /// whose key satisfies AgentEnvironmentKeys ride along. Null stays null
    /// (the primitive then inherits its execution context); null keys,
    /// null values, and invalid keys are dropped. The host environment is
    /// never read here, so host-only variables cannot leak into the
    /// injected set. Values may carry secrets and are copied verbatim, never
    /// logged.
    /// <param name="agentEnv">The agent's environment variables, or null when the agent sets none.</param>
    /// <returns>The allowlisted copy to inject, or null when the agent sets none.</returns>
    let selectAgentEnv
        (agentEnv: IReadOnlyDictionary<string, string> | null)
        : IReadOnlyDictionary<string, string> | null =
        match agentEnv with
        | null -> null
        | source ->
            let selected = Dictionary<string, string>(StringComparer.Ordinal)

            for KeyValue(key, value) in source do
                if
                    not (isNull (box key))
                    && not (isNull (box value))
                    && AgentEnvironmentKeys.TryValidate key
                then
                    selected[key] <- value

            selected :> IReadOnlyDictionary<string, string>

    /// Builds the tool result JSON with the pinned keys exit_code, stdout,
    /// stderr, and timed_out. Streams are truncated here, immediately, so
    /// the text leaving the tool is already bounded.
    /// <param name="result">The primitive result. Must not be null.</param>
    /// <returns>The JSON result text.</returns>
    let private resultJson (result: WorkspaceExecResult) : string =
        ArgumentNullException.ThrowIfNull(result)

        let node = JsonObject()
        node["exit_code"] <- JsonValue.Create(result.ExitCode)
        node["stdout"] <- JsonValue.Create(truncateStream result.StandardOutput)
        node["stderr"] <- JsonValue.Create(truncateStream result.StandardError)
        node["timed_out"] <- JsonValue.Create(result.TimedOut)
        node.ToJsonString()

    /// Creates the exec AIFunction bound to one workspace with the agent
    /// environment allowlist injected on every call. The command argument
    /// passes verbatim; timeout_seconds carries whole seconds always clamped
    /// to 1..300, so every exec through the tool runs under a bounded
    /// timeout (the primitive's own runtime default stays for direct
    /// primitive callers); the invocation CancellationToken flows through to
    /// the primitive. Failures propagate to the TurnLoop Error mapping;
    /// thrown messages never embed the command or environment values.
    /// <param name="workspace">The bound workspace the command executes in. Must not be null.</param>
    /// <param name="agentEnv">The agent's environment variables to allowlist-inject, or null when the agent sets none.</param>
    /// <returns>The model-facing exec tool.</returns>
    let create (workspace: IWorkspace) (agentEnv: IReadOnlyDictionary<string, string> | null) : AIFunction =
        ArgumentNullException.ThrowIfNull(workspace)
        ToolNameRules.Validate ToolName |> ignore

        let env = selectAgentEnv agentEnv

        let method =
            Func<string, int, CancellationToken, Task<string>>(fun command timeoutSeconds cancellationToken ->
                task {
                    if isNull (box command) then
                        raise (ArgumentNullException(nameof command))

                    let timeout = TimeSpan.FromSeconds(float (clampTimeoutSeconds timeoutSeconds))
                    let! result = workspace.Exec(command, Nullable(timeout), env, cancellationToken)
                    return resultJson result
                })

        AIFunctionFactory.Create(method, ToolName, Description, Unchecked.defaultof<JsonSerializerOptions>)

    /// Inserts the exec tool into the turn's tool map under ToolName,
    /// replacing any previous entry. The map rides the existing TurnLoop
    /// path: runSuspendableAsync gates every call through the permission
    /// Ask verdicts and runAsync/runSuspendableAsync verify the claim at the
    /// last moment through TurnLoopOptions.VerifyClaim, so a takeover loser
    /// never invokes the tool. No new gating lives here.
    /// <param name="tools">The turn's tool map, mutated in place. Must not be null.</param>
    /// <param name="workspace">The bound workspace the command executes in. Must not be null.</param>
    /// <param name="agentEnv">The agent's environment variables to allowlist-inject, or null when the agent sets none.</param>
    /// <returns>The inserted exec tool.</returns>
    let addTo
        (tools: IDictionary<string, AITool>)
        (workspace: IWorkspace)
        (agentEnv: IReadOnlyDictionary<string, string> | null)
        : AIFunction =
        ArgumentNullException.ThrowIfNull(tools)

        let fn = create workspace agentEnv
        tools[ToolName] <- fn :> AITool
        fn
