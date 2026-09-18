// SPDX-License-Identifier: Apache-2.0
module internal Legate.Mcp.McpInvocation

open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open Legate

// Per-call invocation: the single path every projected MCP tool invokes
// through. It forwards the turn's token to the session (the projected
// InvokeCoreAsync token arrives here from TurnLoop.invokeOneWithErrorAsync,
// so cancelling the turn abandons the call), maps server-flagged errors to
// "Error from" texts and transport failures to "Error calling" texts, and
// stamps one observation per settled call into the injectable sink. Only
// cancellation propagates: every other failure returns as text, so the
// turn continues. Binary blocks beside the text are validated header-only
// and stored through the caller-supplied artifact sink (already scoped to
// the session by the caller: this layer never derives tenant keys),
// substituting a text reference carrying mime, size, dimensions, and name
// so no SessionEvent wire change is needed; any validation or storage
// failure falls back to text and never fails the turn. Observations carry
// no turn or call ids: main-path journaling does not exist here (only
// TaskTool.observerOf emits SessionEvents), and the session path journals
// these observations with real ids later. Tests assert the observations;
// nothing here is journaled.

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

/// Invokes one server tool through the session with artifact handling
/// and returns its model-facing text: the result with binary placeholders
/// substituted by stored-artifact references (or bounded rejections),
/// <c>Error from {name}: ...</c> when the server flagged the result as an
/// error, or <c>Error calling {name}: {Type}: {message}</c> when the
/// transport raised. A null sink keeps today's placeholder text.
/// Validation failures substitute a bounded rejection; storage failures
/// return the original text. Cancellation propagates and stamps nothing:
/// an abandoned call never settled. Every settled call stamps one
/// observation into the sink; a null sink observes nothing.
/// <param name="session">The connected session owning the tool.</param>
/// <param name="assignedName">The model-facing assigned tool name, carried on errors and observations.</param>
/// <param name="toolName">The server's tool name.</param>
/// <param name="arguments">The call arguments.</param>
/// <param name="cancellationToken">The turn's token: abandoning it abandons the call and the store.</param>
/// <param name="observe">The observation sink, or null to observe nothing.</param>
/// <param name="artifactSink">The session's quota-accounted artifact sink, or null to keep placeholders. Never derived here.</param>
/// <param name="caps">The header-only validation bounds.</param>
/// <returns>The model-facing text result.</returns>
let invokeWithArtifactsAsync
    (session: IMcpServerSession)
    (assignedName: string)
    (toolName: string)
    (arguments: IReadOnlyDictionary<string, obj>)
    (cancellationToken: CancellationToken)
    (observe: Action<McpCallObservation> | null)
    (artifactSink: IArtifactSink | null)
    (caps: McpArtifacts.McpArtifactCaps)
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

        let effectiveCaps =
            if isNull (box caps) then
                McpArtifacts.McpArtifactCaps.Default
            else
                caps

        try
            let! callResult = session.CallToolAsync(toolName, arguments, cancellationToken)
            let text = if isNull (box callResult.Text) then "" else callResult.Text

            if callResult.IsError then
                let mapped = $"Error from {assignedName}: {text}"
                observeOne mapped true
                return mapped
            else
                match box artifactSink with
                | :? IArtifactSink as sink when not (isNull (box callResult.Binaries)) && callResult.Binaries.Count > 0 ->
                    let binaries = callResult.Binaries
                    let replacements = ResizeArray<string>()
                    let mutable fallback = false
                    let mutable index = 0

                    while not fallback && index < binaries.Count do
                        let binary = binaries.[index]

                        let mime =
                            match box binary.MimeType with
                            | null -> "application/octet-stream"
                            | :? string as carried -> carried
                            | _ -> "application/octet-stream"

                        match box (McpArtifacts.checkCaps mime binary.Bytes effectiveCaps) with
                        | :? string as rejection ->
                            let size =
                                match box binary.Bytes with
                                | :? array<byte> as payload -> payload.Length
                                | _ -> 0

                            replacements.Add(McpArtifacts.formatRejection mime size rejection)
                        | _ ->
                            match box binary.Bytes with
                            | :? array<byte> as payload ->
                                let name = McpArtifacts.buildStorageName binary.Name mime

                                let! stored =
                                    task {
                                        try
                                            let! outcome =
                                                sink.StoreAsync(name, BlobContent(payload, mime), cancellationToken)

                                            return Some outcome
                                        with
                                        | :? OperationCanceledException as canceled ->
                                            return! Task.FromException<ArtifactSinkOutcome option>(canceled)
                                        | _ -> return None
                                    }

                                match stored with
                                | Some(StoredArtifact(reference, _)) -> replacements.Add(reference)
                                | Some(RejectedArtifact reason) ->
                                    replacements.Add(McpArtifacts.formatRejection mime payload.Length reason)
                                | _ -> fallback <- true
                            | _ -> replacements.Add(McpArtifacts.formatRejection mime 0 "undecodable payload")

                        index <- index + 1

                    if fallback then
                        observeOne text false
                        return text
                    else
                        let final =
                            McpArtifacts.substitute text binaries (replacements :> IReadOnlyList<string>)

                        observeOne final false
                        return final
                | _ ->
                    observeOne text false
                    return text
        with
        | :? OperationCanceledException as canceled -> return! Task.FromException<string>(canceled)
        | ex ->
            let mapped = $"Error calling {assignedName}: {ex.GetType().Name}: {ex.Message}"
            observeOne mapped true
            return mapped
    }

/// Invokes one server tool through the session and returns its
/// model-facing text: the result, <c>Error from {name}: ...</c> when the
/// server flagged the result as an error, or
/// <c>Error calling {name}: {Type}: {message}</c> when the transport
/// raised. Cancellation propagates and stamps nothing: an abandoned call
/// never settled. Every settled call stamps one observation into the
/// sink; a null sink observes nothing. Binary placeholders are kept as
/// today: pass an artifact sink to
/// <c>invokeWithArtifactsAsync</c> for store-then-reference handling.
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
    invokeWithArtifactsAsync
        session
        assignedName
        toolName
        arguments
        cancellationToken
        observe
        null
        McpArtifacts.McpArtifactCaps.Default
