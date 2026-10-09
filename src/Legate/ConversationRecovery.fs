// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

// Nullness warning 3261 is suppressed in this file: event text fields are
// runtime-nullable (older journals and MEAI interop hand nulls the F#
// nullable analysis cannot prove absent), and recovery treats every one as
// explicit missing data (rejection) rather than failing. Rejection paths
// are pinned by ConversationEvidence tests, not by the type checker.

open System
open System.Collections.Generic
open System.Text.Json
open Microsoft.Extensions.AI

// Current-format recovery builder (issue 380): the pure ordered
// journal-to-provider-history fold the Idle/crash paths consume. It rebuilds
// the provider ChatMessage history directly from journaled SessionEvents,
// never from lossy display cells: tool starts carry the JSON arguments and
// completions carry the paired result text, so the call rebuild is exact.
// Anything that cannot rebuild exactly rejects explicitly instead of
// fabricating arguments, results, or valid state: a null/missing arguments
// payload, malformed arguments JSON, a completion without its start, or a
// success without result text. An aborted start without its completion drops
// (the call never settled, so the model never saw a result), mirroring the
// crash-rehydrate precedent; duplicate starts/completions for one call id
// keep the first and drop the rest, so repeated continuations or reports
// never create distinct duplicate logical entries. Display-only streaming
// (TextDelta/ToolCallOutput fragments are already folded into their
// completion result text) never replays: output fragments accumulate for
// the transcript, not for provider history. User non-text parts copy by
// reference, so supported images and files survive; reasoning deltas stay
// transient and produce no message. Control and progress markers
// (permission, question, usage, compaction, terminals, skills, agents) become
// System text or are skipped as documented per branch: they order the
// conversation but never fabricate a call. Pages only transport: the fold
// reads the concatenated journal, so every page split folds identically.
module internal ConversationRecovery =

    /// Why current-format recovery refused a journal before any provider
    /// execution. Every case names the offending call or event, never
    /// secrets or tool arguments.
    type RecoveryRejection =
        /// A tool start carried no arguments payload (a pre-380 journal).
        | MissingToolArguments of callId: string
        /// A tool start's arguments payload was not a JSON object.
        | InvalidToolArguments of callId: string
        /// A tool completion arrived without its start (no name/arguments).
        | UnpairedToolCompletion of callId: string
        /// A successful completion carried no result text.
        | MissingToolResult of callId: string
        /// The journal batch itself was unusable (null list).
        | InvalidJournal of reason: string
        /// A compacted event carried no durable replacement context
        /// (a pre-387 journal with a null/empty summary or null tail).
        | IncompleteCompactedContext of reason: string
        /// A compacted event carried an unsupported compacted-context
        /// version.
        | UnsupportedCompactedFormat of version: int

    /// Parses one tool arguments JSON object string into the argument table
    /// a FunctionCallContent carries. Null/whitespace never parses here:
    /// the caller rejects those as missing before calling.
    /// <param name="callId">The call the arguments belong to (for the error).</param>
    /// <param name="argumentsJson">The JSON object string. Must not be null.</param>
    /// <returns>The argument table, or the rejection.</returns>
    let private parseArguments
        (callId: string)
        (argumentsJson: string)
        : Result<IDictionary<string, obj>, RecoveryRejection> =
        if isNull (box argumentsJson) then
            Error(MissingToolArguments(callId))
        else
            try
                let table =
                    JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(argumentsJson)

                let args =
                    if isNull (box table) then
                        Dictionary<string, obj>() :> IDictionary<string, obj>
                    else
                        let boxed = Dictionary<string, obj>(table.Count, StringComparer.Ordinal)

                        for pair in table do
                            boxed[pair.Key] <- pair.Value :> obj

                        boxed :> IDictionary<string, obj>

                Ok args
            with _ ->
                Error(InvalidToolArguments(callId))

    /// Rebuilds the provider history from one session's journaled events in
    /// sequence order. Pure: reads the inputs, returns fresh messages, no I/O.
    /// <param name="events">The journaled events in sequence order. Must not be null and must not contain null.</param>
    /// <returns>The provider history, or the explicit rejection.</returns>
    let tryRecover (events: IReadOnlyList<SessionEvent>) : Result<IList<ChatMessage>, RecoveryRejection> =
        if isNull (box events) then
            Error(InvalidJournal "The event batch must not be null.")
        else
            let history = ResizeArray<ChatMessage>()

            let starts =
                Dictionary<string, string * IDictionary<string, obj>>(StringComparer.Ordinal)

            let seenStarts = HashSet<string>(StringComparer.Ordinal)
            let seenCompletions = HashSet<string>(StringComparer.Ordinal)
            let pendingText = Text.StringBuilder()
            let mutable pendingSince: DateTimeOffset option = None
            let mutable rejected: RecoveryRejection option = None

            let flushText () =
                if pendingText.Length > 0 then
                    history.Add(ChatMessage(ChatRole.Assistant, pendingText.ToString()))
                    pendingText.Clear() |> ignore
                    pendingSince <- None

            let systemText (text: string) =
                flushText ()

                if not (isNull text) then
                    history.Add(ChatMessage(ChatRole.System, text))

            let mutable index = 0

            while index < events.Count && rejected.IsNone do
                let event = events[index]
                index <- index + 1

                if isNull (box event) then
                    rejected <- Some(InvalidJournal "The event batch must not contain null.")
                else
                    match event with
                    | :? UserMessageEvent as user when not (isNull (box user)) ->
                        flushText ()

                        let parts =
                            if isNull (box user.Message) || isNull (box user.Message.Parts) then
                                ResizeArray<AIContent>() :> IList<AIContent>
                            else
                                let copied = ResizeArray<AIContent>(user.Message.Parts.Count)

                                for part in user.Message.Parts do
                                    if not (isNull (box part)) then
                                        copied.Add(part)

                                copied :> IList<AIContent>

                        history.Add(ChatMessage(ChatRole.User, parts))
                    | :? TextDeltaEvent as delta when not (isNull (box delta)) ->
                        if isNull (box delta.Text) then
                            ()
                        else
                            if pendingSince.IsNone then
                                pendingSince <- Some delta.Timestamp

                            pendingText.Append(delta.Text) |> ignore
                    | :? ReasoningDeltaEvent ->
                        // Transient, never provider history: reasoning
                        // produces no message, matching the transcript fold.
                        ()
                    | :? ToolCallStartedEvent as started when not (isNull (box started)) ->
                        flushText ()

                        let callId = if isNull started.ToolCallId then "" else started.ToolCallId

                        if seenStarts.Contains(callId) then
                            // Duplicate report: keep the first logical entry.
                            ()
                        else
                            seenStarts.Add(callId) |> ignore

                            if isNull (box started.ArgumentsJson) then
                                rejected <- Some(MissingToolArguments callId)
                            else
                                match parseArguments callId started.ArgumentsJson with
                                | Error rejection -> rejected <- Some rejection
                                | Ok args ->
                                    let name = if isNull started.ToolName then "" else started.ToolName
                                    starts[callId] <- (name, args)
                                    let call = FunctionCallContent(callId, name, args)
                                    let contents = ResizeArray<AIContent>()
                                    contents.Add(call :> AIContent)
                                    history.Add(ChatMessage(ChatRole.Assistant, contents :> IList<AIContent>))
                    | :? ToolCallOutputEvent ->
                        // Display-only streaming: the completion's result
                        // text is the provider evidence, never the fragments.
                        ()
                    | :? ToolCallCompletedEvent as completed when not (isNull (box completed)) ->
                        flushText ()

                        let callId =
                            if isNull completed.ToolCallId then
                                ""
                            else
                                completed.ToolCallId

                        if seenCompletions.Contains(callId) then
                            // Duplicate report: keep the first logical entry.
                            ()
                        else
                            seenCompletions.Add(callId) |> ignore

                            match starts.TryGetValue(callId) with
                            | false, _ -> rejected <- Some(UnpairedToolCompletion callId)
                            | true, _ ->
                                let isError = not (isNull (box completed.Error))

                                if not isError && isNull (box completed.ResultText) then
                                    rejected <- Some(MissingToolResult callId)
                                else
                                    let resultText =
                                        if isNull (box completed.ResultText) then
                                            if isNull (box completed.Error) then "" else completed.Error
                                        else
                                            completed.ResultText

                                    let contents = ResizeArray<AIContent>()
                                    contents.Add(FunctionResultContent(callId, resultText) :> AIContent)
                                    history.Add(ChatMessage(ChatRole.Tool, contents :> IList<AIContent>))
                                    starts.Remove(callId) |> ignore
                    | :? PermissionRequestedEvent as requested when not (isNull (box requested)) ->
                        systemText requested.ToolName
                    | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) ->
                        systemText (resolved.Decision.ToString())
                    | :? QuestionAskedEvent as asked when not (isNull (box asked)) -> systemText asked.Question
                    | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) ->
                        systemText answered.Answer
                    | :? TurnFailedEvent as failed when not (isNull (box failed)) -> systemText failed.Reason
                    | :? CompactionFailedEvent as failed when not (isNull (box failed)) -> systemText failed.Reason
                    | :? SkillInvalidEvent as invalid when not (isNull (box invalid)) -> systemText invalid.Reason
                    | :? AgentInvalidEvent as invalid when not (isNull (box invalid)) -> systemText invalid.Reason
                    | :? ContextPrunedEvent as pruned when not (isNull (box pruned)) ->
                        systemText (
                            $"pruned {pruned.PrunedCount} tool result(s): estimated tokens {pruned.BeforeEstimate} -> {pruned.AfterEstimate}"
                        )
                    | :? AgentSwitchedEvent as switched when not (isNull (box switched)) ->
                        systemText ($"switched agent {switched.PreviousAgentId} -> {switched.NewAgentId}")
                    | _ ->
                        // Progress markers (turnStarted, usage, compacted,
                        // turnCompleted, turnAborted, sessionClosed,
                        // skillLoaded) carry no provider history: hosts read
                        // them from the event stream. They never break an
                        // open assistant-text run, so the run continues.
                        ()

            match rejected with
            | Some rejection -> Error rejection
            | None ->
                flushText ()
                // Aborted starts without completions drop: the call never
                // settled, so the model never saw a result. History already
                // holds the start's Assistant call message; remove those
                // trailing unpaired calls so the history never replays an
                // open call. Only trailing unpaired calls drop: a settled
                // history never holds them.
                if starts.Count = 0 then
                    Ok(history :> IList<ChatMessage>)
                else
                    let unpaired = HashSet<string>(starts.Keys, StringComparer.Ordinal)

                    if unpaired.Count = 0 then
                        Ok(history :> IList<ChatMessage>)
                    else
                        let kept = ResizeArray<ChatMessage>(history.Count)

                        for message in history do
                            let mutable drop = false

                            if not (isNull (box message)) && not (isNull (box message.Contents)) then
                                for content in message.Contents do
                                    if not drop && not (isNull (box content)) then
                                        match content with
                                        | :? FunctionCallContent as call when not (isNull (box call)) ->
                                            if unpaired.Contains(call.CallId) then
                                                drop <- true
                                        | _ -> ()

                            if not drop then
                                kept.Add(message)

                        Ok(kept :> IList<ChatMessage>)

    /// Copies one provider message with a fresh contents list, sharing the
    /// content objects: the history owns its list, the journal keeps its
    /// own. Null messages copy as null; null contents copy as an empty
    /// user text (the recovery never emits null lists).
    /// <param name="message">The message to copy. May be null.</param>
    /// <returns>The copied message.</returns>
    let private copyMessage (message: ChatMessage) : ChatMessage =
        if isNull (box message) then
            null
        else
            let contents =
                if isNull (box message.Contents) then
                    ResizeArray<AIContent>() :> IList<AIContent>
                else
                    let copied = ResizeArray<AIContent>(message.Contents.Count)

                    for content in message.Contents do
                        copied.Add(content)

                    copied :> IList<AIContent>

            ChatMessage(message.Role, contents)

    /// Builds the marked summary message the rewritten history carries:
    /// one user message with the shared marker plus the raw summary,
    /// mirroring <see cref="M:Legate.Compaction.planRewrite" />.
    /// <param name="summary">The raw summary text. Must not be null.</param>
    /// <returns>The summary message.</returns>
    let summaryMessageOf (summary: string) : ChatMessage =
        // Mirrors Compaction.SummaryMarker without referencing the later
        // module (compile order): the marker is part of the durable
        // compacted-context contract.
        ChatMessage(ChatRole.User, "[legate-compacted-summary]" + "\n" + summary)

    /// Validates one compacted event's durable replacement context.
    /// <param name="compacted">The compacted event. Must not be null.</param>
    /// <returns>The validated event, or the explicit rejection.</returns>
    let private validateCompacted (compacted: CompactedEvent) : Result<CompactedEvent, RecoveryRejection> =
        if compacted.FormatVersion <> SessionEventContract.CompactedContextVersion then
            Error(UnsupportedCompactedFormat compacted.FormatVersion)
        elif String.IsNullOrWhiteSpace compacted.Summary then
            Error(IncompleteCompactedContext "The compacted event carries no summary text.")
        elif isNull (box compacted.RetainedMessages) then
            Error(IncompleteCompactedContext "The compacted event carries no retained context.")
        else
            Ok compacted

    /// Resolves one session's provider history through the last successful
    /// compacted base (issue 387): folds the journal to the last
    /// CompactedEvent, drops superseded pre-compaction model context, then
    /// appends the post-compaction suffix through the current-format
    /// builder in conversational order. Live turns, idle Compact,
    /// subsequent turns, and fresh-process reopen all resolve through
    /// this one function, so repeated compaction uses the current
    /// compacted state, never an obsolete replay. With no compacted base
    /// this is the ordinary builder; audit transcript derivation is
    /// untouched (CompactedEvent still produces no transcript cell).
    /// Unsupported or incomplete compacted state rejects explicitly with
    /// a clean start before any provider execution, without migrating,
    /// rebinding, or manufacturing retained context.
    /// <param name="events">The journaled events in sequence order. Must not be null and must not contain null.</param>
    /// <returns>The provider history, or the explicit rejection.</returns>
    let tryRecoverCompacted (events: IReadOnlyList<SessionEvent>) : Result<IList<ChatMessage>, RecoveryRejection> =
        if isNull (box events) then
            Error(InvalidJournal "The event batch must not be null.")
        else
            let mutable lastIndex = -1
            let mutable lastCompacted: CompactedEvent | null = null

            for index in 0 .. events.Count - 1 do
                match events[index] with
                | :? CompactedEvent as compacted when not (isNull (box compacted)) ->
                    lastIndex <- index
                    lastCompacted <- compacted
                | _ -> ()

            match lastCompacted with
            | null -> tryRecover events
            | compacted ->
                match validateCompacted compacted with
                | Error rejection -> Error rejection
                | Ok valid ->
                    let suffix = ResizeArray<SessionEvent>()

                    for index in lastIndex + 1 .. events.Count - 1 do
                        suffix.Add(events[index])

                    match tryRecover (suffix :> IReadOnlyList<SessionEvent>) with
                    | Error rejection -> Error rejection
                    | Ok suffixHistory ->
                        let history = ResizeArray<ChatMessage>()
                        history.Add(summaryMessageOf valid.Summary)

                        if not (isNull (box valid.RetainedMessages)) then
                            for message in valid.RetainedMessages do
                                history.Add(copyMessage message)

                        for message in suffixHistory do
                            history.Add(copyMessage message)

                        Ok(history :> IList<ChatMessage>)
