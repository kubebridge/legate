// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

// Nullness warning 3261 is suppressed in this file: event text fields are
// runtime-nullable (stores and MEAI interop hand nulls the F# nullable
// analysis cannot prove absent), and the writer treats every one as
// pass-through null rather than failing. Null-in/null-out is pinned by
// JournalWriterTests facts, not by the type checker.

open System
open System.Collections.Generic
open System.Text
open System.Text.Json
open System.Text.RegularExpressions
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions

// Journal writer (issue 48): the sole append path into ISessionEventStore.
// Every event is sanitized (table-driven secret-shape redaction to
// [REDACTED]) and bounded (deterministic reduction, never a drop) before it
// reaches the store, then appended under a claim fence with bounded retries
// and a typed write result the caller branches on.
//
// What lives where, by rejection: sanitising in the store is rejected (the
// contract fixes the store to persist-what-given and validate-only-bounds);
// sanitising in Abstractions is rejected (the writer needs ClaimFence and
// store wiring, which are runtime-internal); dropping oversized events is
// rejected (reduced-forms-never-drop). User metadata stays verbatim: the
// redactor replaces secret shapes inside text fields but never strips
// metadata keys, and untrusted tagging stays prompt-render-only per #42
// (UntrustedContext); the bounder only trims metadata past its field bound.
module internal JournalWriter =

    /// Replacement text for every redacted secret shape.
    [<Literal>]
    let RedactedText = "[REDACTED]"

    /// Marker appended when the bounder reduces a text field, mirroring
    /// TurnLoop.TruncationMarker.
    [<Literal>]
    let TruncationMarker = "[truncated]"

    /// Maximum chars per text field before truncation with TruncationMarker.
    /// Exactly-at-limit passes through.
    [<Literal>]
    let MaxTextChars = 32768

    /// Maximum estimated bytes per event, using the same UTF-8 JSON length
    /// estimator the stores enforce, so a bounded event sits strictly below
    /// any host-configured store limit at or above this bound.
    [<Literal>]
    let MaxEventBytes = 65536L

    /// Maximum UserMessage metadata entries before the deterministic
    /// ordinal-key trim. The trim keeps the first entries by ordinal key, so
    /// the same oversized metadata always reduces to the same 64 entries.
    [<Literal>]
    let MaxMetadataFields = 64

    /// Total append attempts (the initial try plus retries). Retries carry
    /// no delay: the actor thread cannot sleep, and backoff policy belongs
    /// to later issues; transient blips get a bounded second chance.
    [<Literal>]
    let MaxAppendAttempts = 3

    /// Stable reason surfacing every fenced-out write alike: the gate
    /// skipping without calling and the store rejecting a stale token both
    /// mean the claim no longer owns the journal, so both report this.
    [<Literal>]
    let StaleClaimReason = "staleClaim"

    /// Reason carried by JournalFailed when a non-retryable store outcome
    /// lands (an unknown outcome shape, for instance). Never contains
    /// secrets or tool arguments.
    [<Literal>]
    let AppendRejectedReason = "The journal append was rejected by the event store."

    /// Reason carried by JournalFailed when the attempts run out. Never
    /// contains secrets or tool arguments: only the attempt count travels,
    /// never the exception text.
    let appendAttemptsExhaustedReason =
        sprintf "The journal append failed after %d attempts." MaxAppendAttempts

    // ────────────────── Redaction ──────────────────

    /// One redaction rule: the stable name plus the secret shape it matches.
    /// Rules apply in list order, specific shapes before generic ones, and
    /// every match is replaced with RedactedText. Conservative by design:
    /// bare identifiers (ULIDs, request ids) are never matched on shape
    /// alone, because claim tokens share the ULID shape with ordinary ids;
    /// tokens redact only in a labelled context (the labelled rule below).
    let private redactionRules: (string * Regex) list =
        let options = RegexOptions.Compiled ||| RegexOptions.CultureInvariant

        [
            ("pem-block",
             Regex(@"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----", options))
            ("anthropic-key", Regex(@"sk-ant-[A-Za-z0-9\-_]{8,}", options))
            ("openai-key", Regex(@"sk-[A-Za-z0-9\-_]{8,}", options))
            ("github-token", Regex(@"gh[pousr]_[A-Za-z0-9_]{8,}|github_pat_[A-Za-z0-9_]{8,}", options))
            ("aws-access-key", Regex(@"\bAKIA[0-9A-Z]{16}\b", options))
            ("google-api-key", Regex(@"\bAIza[0-9A-Za-z\-_]{35}\b", options))
            ("slack-token", Regex(@"\bxox[bpas]-[A-Za-z0-9\-]+\b", options))
            ("bearer-token", Regex(@"(?i)\bbearer\s+[A-Za-z0-9\-._~+/=]{8,}", options))
            ("basic-auth", Regex(@"(?i)\bbasic\s+[A-Za-z0-9\-._~+/=]{8,}", options))
            ("connection-password", Regex(@"(?i)\b(password|pwd)=[^;\s""']+", options))
            ("uri-credential", Regex(@"(?<=://)[^/\s@]+:[^@\s/]*@", options))
            ("env-assignment",
             Regex(
                 @"(?im)^\s*[A-Z][A-Z0-9_]*?(?:KEY|SECRET|TOKEN|PASSWORD|PASSWD|_PWD|CREDENTIAL|PRIVATE)[A-Z0-9_]*\s*=\s*[^\r\n]+",
                 options
             ))
            ("labelled-secret",
             Regex(
                 @"(?i)\b(api[_-]?key|api[_-]?secret|secret[_-]?key|client[_-]?secret|private[_-]?key|auth[_-]?token|access[_-]?token|refresh[_-]?token|claim[\s_-]?token|session[\s_-]?token|id[\s_-]?token|password|passwd|pwd|secret|token)\b[""']?\s*[:=]\s*[""']?\S+",
                 options
             ))
        ]

    /// Replaces every secret shape in free text with RedactedText. Null
    /// stays null; text without secret shapes passes through untouched.
    /// <param name="text">The text to redact, or null.</param>
    /// <returns>The redacted text, or null when the input was null.</returns>
    let redactText (text: string) : string =
        if isNull text then
            null
        else
            redactionRules
            |> List.fold (fun current (_, rule: Regex) -> rule.Replace(current, RedactedText)) text

    /// Redacts a nullable text field: null stays null.
    /// <param name="value">The field value, or null.</param>
    /// <returns>The redacted value, or null when the input was null.</returns>
    let private redactField (value: string | null) : string | null =
        if isNull (box value) then null else redactText value

    /// Redacts the text parts of a user message in order, preserving every
    /// non-text part by reference and every null entry as null. Metadata is
    /// untouched here: the journal keeps it verbatim per #42.
    /// <param name="parts">The message parts, or null.</param>
    /// <returns>The redacted parts, or null when the input was null.</returns>
    let private redactParts (parts: IReadOnlyList<AIContent>) : IReadOnlyList<AIContent> =
        if isNull (box parts) then
            null
        else
            let redacted = ResizeArray<AIContent>(parts.Count)

            for part in parts do
                if isNull (box part) then
                    redacted.Add(null)
                else
                    match part with
                    | :? TextContent as text when not (isNull (box text)) ->
                        if isNull (box text.Text) then
                            redacted.Add(part)
                        else
                            redacted.Add(TextContent(redactText text.Text) :> AIContent)
                    | _ -> redacted.Add(part)

            redacted :> IReadOnlyList<AIContent>

    /// Maps the text parts of a user message through the given function,
    /// preserving every non-text part by reference and every null entry as
    /// null. Null text stays null. The generic transform behind the
    /// UserMessage branch of mapTexts, so truncation and shrinking reduce
    /// message parts symmetric with every other text-bearing kind.
    /// <param name="map">The null-safe function applied to each text part.</param>
    /// <param name="parts">The message parts, or null.</param>
    /// <returns>The mapped parts, or null when the input was null.</returns>
    let private mapParts (map: string -> string) (parts: IReadOnlyList<AIContent>) : IReadOnlyList<AIContent> =
        if isNull (box parts) then
            null
        else
            let mapped = ResizeArray<AIContent>(parts.Count)

            for part in parts do
                if isNull (box part) then
                    mapped.Add(null)
                else
                    match part with
                    | :? TextContent as text when not (isNull (box text)) ->
                        if isNull (box text.Text) then
                            mapped.Add(part)
                        else
                            mapped.Add(TextContent(map text.Text) :> AIContent)
                    | _ -> mapped.Add(part)

            mapped :> IReadOnlyList<AIContent>

    /// Maps every text-bearing field of an event through the given function,
    /// rebuilding the event with the same ids, sequence, and timestamp.
    /// Id, name, and numeric fields (tool names, request ids, token counts)
    /// are never text-bearing and pass through. Unknown future kinds pass
    /// through unchanged: the writer never drops an event it does not know.
    /// <param name="map">The null-safe function applied to each text field.</param>
    /// <param name="event">The event to map. Must not be null.</param>
    /// <returns>The event with mapped text fields.</returns>
    let private mapTexts (map: string -> string) (event: SessionEvent) : SessionEvent =
        if isNull (box event) then
            raise (ArgumentNullException(nameof event))

        match event with
        | :? TextDeltaEvent as source when not (isNull (box source)) ->
            TextDeltaEvent(source.SessionId, source.TurnId, source.Sequence, source.Timestamp, map source.Text)
            :> SessionEvent
        | :? ReasoningDeltaEvent as source when not (isNull (box source)) ->
            ReasoningDeltaEvent(source.SessionId, source.TurnId, source.Sequence, source.Timestamp, map source.Text)
            :> SessionEvent
        | :? ToolCallOutputEvent as source when not (isNull (box source)) ->
            ToolCallOutputEvent(
                source.SessionId,
                source.TurnId,
                source.Sequence,
                source.Timestamp,
                source.ToolCallId,
                map source.Output
            )
            :> SessionEvent
        | :? ToolCallCompletedEvent as source when not (isNull (box source)) ->
            ToolCallCompletedEvent(
                source.SessionId,
                source.TurnId,
                source.Sequence,
                source.Timestamp,
                source.ToolCallId,
                map source.Error
            )
            :> SessionEvent
        | :? QuestionAskedEvent as source when not (isNull (box source)) ->
            QuestionAskedEvent(
                source.SessionId,
                source.TurnId,
                source.Sequence,
                source.Timestamp,
                source.QuestionId,
                map source.Question
            )
            :> SessionEvent
        | :? QuestionAnsweredEvent as source when not (isNull (box source)) ->
            QuestionAnsweredEvent(
                source.SessionId,
                source.TurnId,
                source.Sequence,
                source.Timestamp,
                source.QuestionId,
                map source.Answer
            )
            :> SessionEvent
        | :? TurnAbortedEvent as source when not (isNull (box source)) ->
            TurnAbortedEvent(
                source.SessionId,
                source.TurnId,
                source.Sequence,
                source.Timestamp,
                source.Cause,
                map source.Reason
            )
            :> SessionEvent
        | :? TurnFailedEvent as source when not (isNull (box source)) ->
            TurnFailedEvent(source.SessionId, source.TurnId, source.Sequence, source.Timestamp, map source.Reason)
            :> SessionEvent
        | :? CompactionFailedEvent as source when not (isNull (box source)) ->
            CompactionFailedEvent(source.SessionId, source.TurnId, source.Sequence, source.Timestamp, map source.Reason)
            :> SessionEvent
        | :? SkillInvalidEvent as source when not (isNull (box source)) ->
            SkillInvalidEvent(
                source.SessionId,
                source.TurnId,
                source.Sequence,
                source.Timestamp,
                source.SkillName,
                map source.Reason
            )
            :> SessionEvent
        | :? UserMessageEvent as source when not (isNull (box source)) ->
            let mapped = mapParts map source.Message.Parts

            let message = UserMessage(mapped, source.Message.Metadata)

            UserMessageEvent(source.SessionId, source.TurnId, source.Sequence, source.Timestamp, message)
            :> SessionEvent
        | _ -> event

    /// Redacts every text-bearing field of an event (Text, Output, Question,
    /// Answer, Reason, Error, message parts) through the redaction table.
    /// Metadata stays verbatim.
    /// <param name="event">The event to sanitize. Must not be null.</param>
    /// <returns>The sanitized event, same kind and ids.</returns>
    let sanitizeEvent (event: SessionEvent) : SessionEvent =
        if isNull (box event) then
            raise (ArgumentNullException(nameof event))

        match event with
        | :? UserMessageEvent as source when not (isNull (box source)) ->
            let redacted = redactParts source.Message.Parts

            let message = UserMessage(redacted, source.Message.Metadata)

            UserMessageEvent(source.SessionId, source.TurnId, source.Sequence, source.Timestamp, message)
            :> SessionEvent
        | _ -> mapTexts redactField event

    // ────────────────── Bounding ──────────────────

    /// Truncates one text field to MaxTextChars with TruncationMarker.
    /// Null stays null; exactly-at-limit passes through.
    /// <param name="value">The field value, or null.</param>
    /// <returns>The bounded value, or null when the input was null.</returns>
    let private truncateField (value: string) : string =
        if isNull value then
            null
        elif value.Length > MaxTextChars then
            value.Substring(0, MaxTextChars) + TruncationMarker
        else
            value

    /// The longest text field of an event, or 0 when it carries none. Null
    /// fields count as empty.
    /// <param name="event">The event to measure. Must not be null.</param>
    /// <returns>The longest text field length.</returns>
    let private maxTextLength (event: SessionEvent) : int =
        if isNull (box event) then
            raise (ArgumentNullException(nameof event))

        let length (value: string) =
            if isNull value then 0 else value.Length

        match event with
        | :? TextDeltaEvent as source when not (isNull (box source)) -> length source.Text
        | :? ReasoningDeltaEvent as source when not (isNull (box source)) -> length source.Text
        | :? ToolCallOutputEvent as source when not (isNull (box source)) -> length source.Output
        | :? ToolCallCompletedEvent as source when not (isNull (box source)) -> length source.Error
        | :? QuestionAskedEvent as source when not (isNull (box source)) -> length source.Question
        | :? QuestionAnsweredEvent as source when not (isNull (box source)) -> length source.Answer
        | :? TurnAbortedEvent as source when not (isNull (box source)) -> length source.Reason
        | :? TurnFailedEvent as source when not (isNull (box source)) -> length source.Reason
        | :? CompactionFailedEvent as source when not (isNull (box source)) -> length source.Reason
        | :? SkillInvalidEvent as source when not (isNull (box source)) ->
            max (length source.SkillName) (length source.Reason)
        | :? UserMessageEvent as source when not (isNull (box source)) ->
            if isNull (box source.Message) || isNull (box source.Message.Parts) then
                0
            else
                source.Message.Parts
                |> Seq.fold
                    (fun longest part ->
                        match part with
                        | :? TextContent as text when not (isNull (box text)) -> max longest (length text.Text)
                        | _ -> longest)
                    0
        | _ -> 0

    /// Trims UserMessage metadata past MaxMetadataFields, keeping the first
    /// entries by ordinal key so the same oversized metadata always reduces
    /// to the same entries. Values stay verbatim; only the count is bounded.
    /// <param name="event">The event to trim.</param>
    /// <returns>The event with trimmed metadata, or the event unchanged.</returns>
    let private trimEventMetadata (event: SessionEvent) : SessionEvent =
        match event with
        | :? UserMessageEvent as source when not (isNull (box source)) ->
            let metadata = source.Message.Metadata

            if isNull (box metadata) || metadata.Count <= MaxMetadataFields then
                event
            else
                let kept = Dictionary<string, string>()

                metadata
                |> Seq.sortBy (fun entry -> entry.Key)
                |> Seq.truncate MaxMetadataFields
                |> Seq.iter (fun entry -> kept[entry.Key] <- entry.Value)

                let message = UserMessage(source.Message.Parts, kept)

                UserMessageEvent(source.SessionId, source.TurnId, source.Sequence, source.Timestamp, message)
                :> SessionEvent
        | _ -> event

    /// The estimated size of one event in bytes: the UTF-8 length of its
    /// serialised JSON form, the same estimator the stores enforce, so the
    /// writer and the stores agree on what fits.
    /// <param name="event">The event to estimate. Must not be null.</param>
    /// <returns>The estimated byte size.</returns>
    let estimateEventBytes (event: SessionEvent) : int64 =
        if isNull (box event) then
            raise (ArgumentNullException(nameof event))

        int64 (Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(event, JsonSerializerOptions())))

    /// Halves every text field currently at the longest length, keeping the
    /// truncation marker on non-empty results. A step in the byte-fit loop,
    /// not a bound on its own.
    /// <param name="event">The event to shrink.</param>
    /// <returns>The event with its longest text fields halved.</returns>
    let private halveLongestTexts (event: SessionEvent) : SessionEvent =
        let longest = maxTextLength event

        if longest <= 0 then
            event
        else
            mapTexts
                (fun value ->
                    if isNull value || value.Length <> longest then
                        value
                    else
                        let half = longest / 2

                        if half <= 0 then
                            ""
                        else
                            value.Substring(0, half) + TruncationMarker)
                event

    /// Reduces an event until its estimate fits MaxEventBytes: text fields
    /// truncate to MaxTextChars first, then the longest fields halve until
    /// the estimate fits or stops shrinking (binary parts no text reduction
    /// can move, for instance). The event always survives: a still-oversized
    /// event passes through for the store to report, never dropped here.
    /// <param name="event">The event to bound. Must not be null.</param>
    /// <returns>The bounded event, same kind and ids.</returns>
    let boundEvent (event: SessionEvent) : SessionEvent =
        if isNull (box event) then
            raise (ArgumentNullException(nameof event))

        let truncated = mapTexts truncateField event |> trimEventMetadata

        let mutable current = truncated
        let mutable currentBytes = estimateEventBytes current
        let mutable fitting = true

        while currentBytes > MaxEventBytes && fitting do
            let shrunk = halveLongestTexts current
            let shrunkBytes = estimateEventBytes shrunk

            if shrunkBytes < currentBytes then
                current <- shrunk
                currentBytes <- shrunkBytes
            else
                fitting <- false

        current

    /// Sanitizes then bounds an event: secret shapes first (redaction can
    /// only shrink), then the deterministic size reduction.
    /// <param name="event">The event to prepare. Must not be null.</param>
    /// <returns>The prepared event, same kind and ids.</returns>
    let prepareEvent (event: SessionEvent) : SessionEvent =
        if isNull (box event) then
            raise (ArgumentNullException(nameof event))

        event |> sanitizeEvent |> boundEvent

    // ────────────────── Fenced append ──────────────────

    /// What the journal writer decided when a prepared batch landed: the
    /// caller must branch on the outcome. A result value, never an
    /// exception: a fenced-out write is an expected branch of the takeover
    /// lifecycle, and an exhausted retry budget fails the turn with the
    /// typed reason instead of throwing past the actor.
    type JournalWriteResult =

        /// The append landed: every event is journaled with its per-session
        /// monotonic sequence stamped. Carries the stored shape.
        | JournalAppended of events: IReadOnlyList<SessionEvent>

        /// The append was fenced out (the gate skipped without calling, or
        /// the store rejected a stale token): nothing was written and
        /// nothing is retried. Carries the stable reason ("staleClaim").
        | JournalRejected of reason: string

        /// The append failed after the bounded retries (or hit a
        /// non-retryable store breach): nothing partial landed. Carries the
        /// typed reason the caller fails the turn with.
        | JournalFailed of reason: string

    /// Whether a store failure is worth another attempt: limit breaches,
    /// unknown sessions, and contract violations are deterministic, so
    /// retrying them only burns the budget. Anything else (a transient
    /// store blip) retries to the attempt bound.
    /// <param name="ex">The failure to classify. Must not be null.</param>
    /// <returns>True when another attempt may land.</returns>
    let private isRetryable (ex: Exception) : bool =
        if isNull (box ex) then
            raise (ArgumentNullException(nameof ex))

        match ex with
        | :? EventLimitExceededException -> false
        | :? SessionNotFoundException -> false
        | :? ArgumentException -> false
        | _ -> true

    /// The typed reason for a deterministic store refusal: limit breaches
    /// name the breached kind (a fixed vocabulary, never secrets); anything
    /// else reports the generic rejection.
    /// <param name="ex">The refusal. Must not be null.</param>
    /// <returns>The typed reason.</returns>
    let private rejectionReason (ex: Exception) : string =
        if isNull (box ex) then
            raise (ArgumentNullException(nameof ex))

        match ex with
        | :? EventLimitExceededException as limited when not (isNull (box limited)) ->
            let kind =
                if isNull (box limited.LimitKind) then
                    "unknown"
                else
                    limited.LimitKind

            sprintf "The journal append was rejected by the event store: %s." kind
        | _ -> AppendRejectedReason

    /// Validates a batch and prepares every event in order: sanitize then
    /// bound, never dropping, so the prepared batch carries the same count
    /// in the same order.
    /// <param name="events">The events to prepare. Must not be null or empty and must carry no nulls.</param>
    /// <returns>The prepared events, in order.</returns>
    let private prepareBatch (events: IReadOnlyList<SessionEvent>) : IReadOnlyList<SessionEvent> =
        if isNull (box events) then
            raise (ArgumentNullException(nameof events))

        if events.Count = 0 then
            raise (ArgumentException("The event batch must not be empty.", nameof events))

        let prepared = ResizeArray<SessionEvent>(events.Count)

        for event in events do
            if isNull (box event) then
                raise (ArgumentNullException(nameof events))

            prepared.Add(prepareEvent event)

        prepared :> IReadOnlyList<SessionEvent>

    // ────────────────── Live publish ──────────────────

    /// The live publish notification the session event bus observes: every
    /// stamped batch that lands triggers it synchronously before the append
    /// returns, so a subscriber attached before the trigger never misses the
    /// batch. Fenced-out and failed writes trigger nothing. Internal: hosts
    /// never publish; only the bus subscribes.
    let private published = Event<TenantId * SessionId * IReadOnlyList<SessionEvent>>()

    /// The live publish notification the session event bus observes.
    /// <returns>The publish event.</returns>
    let internal Published: IEvent<TenantId * SessionId * IReadOnlyList<SessionEvent>> =
        published.Publish

    /// Notifies live subscribers of one stamped batch. No-ops on empty
    /// batches; never throws past the caller (a throwing subscriber must
    /// not fail the turn): handler faults are swallowed.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appended.</param>
    /// <param name="stamped">The stamped events, in append order.</param>
    let private notifyPublished
        (tenant: TenantId)
        (sessionId: SessionId)
        (stamped: IReadOnlyList<SessionEvent>)
        : unit =
        if not (isNull (box stamped)) && stamped.Count > 0 then
            try
                published.Trigger(tenant, sessionId, stamped)
            with _ ->
                ()

    /// Runs one prepared append with bounded retries: rejections propagate
    /// without retry or write, transient failures retry to MaxAppendAttempts
    /// and then fail with the typed reason, and cancellation propagates
    /// instead of settling into a result.
    /// <param name="appendOnce">One attempt over the prepared batch.</param>
    /// <param name="cancellationToken">Abandons the append.</param>
    /// <returns>The write result.</returns>
    let private appendWithRetry
        (appendOnce: CancellationToken -> Task<EventAppendOutcome>)
        (cancellationToken: CancellationToken)
        : Task<JournalWriteResult> =
        if isNull (box appendOnce) then
            raise (ArgumentNullException(nameof appendOnce))

        let rec attempt (n: int) : Task<JournalWriteResult> =
            task {
                try
                    let! outcome = appendOnce cancellationToken

                    match outcome with
                    | _ when isNull (box outcome) -> return JournalFailed AppendRejectedReason
                    | :? EventAppended as appended when not (isNull (box appended)) ->
                        let stamped =
                            if isNull (box appended.Events) then
                                ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>
                            else
                                appended.Events

                        return JournalAppended stamped
                    | :? EventAppendRejected as rejected when not (isNull (box rejected)) ->
                        let reason =
                            if isNull (box rejected.Reason) then
                                StaleClaimReason
                            else
                                rejected.Reason

                        return JournalRejected reason
                    | _ -> return JournalFailed AppendRejectedReason
                with ex ->
                    if ex :? OperationCanceledException then
                        return! Task.FromException<JournalWriteResult>(ex)
                    elif isRetryable ex && n < MaxAppendAttempts then
                        return! attempt (n + 1)
                    elif isRetryable ex then
                        return JournalFailed appendAttemptsExhaustedReason
                    else
                        return JournalFailed(rejectionReason ex)
            }

        attempt 1

    /// Appends prepared events under the claim's last-moment fence: verifies
    /// through ClaimFence.appendEventsAsync, so a fenced-out append is
    /// skipped with zero effects before the store is even called, and
    /// branches the store's outcome the same way. Transient failures retry
    /// bounded, then fail with the typed reason. A landed batch publishes
    /// its stamped events to live subscribers before returning; fenced-out
    /// and failed writes publish nothing.
    /// <param name="sessionStore">The session store verifying the claim. Must not be null.</param>
    /// <param name="eventStore">The journal the events append to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="claim">The claim fencing the append. Must not be null.</param>
    /// <param name="events">The events to append, in order. Must not be null or empty and must carry no nulls.</param>
    /// <param name="cancellationToken">Abandons the append.</param>
    /// <returns>The write result: stamped events, the stable rejection, or the typed failure.</returns>
    let appendAsync
        (sessionStore: ISessionStore)
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (claim: TurnClaim)
        (events: IReadOnlyList<SessionEvent>)
        (cancellationToken: CancellationToken)
        : Task<JournalWriteResult> =
        ArgumentNullException.ThrowIfNull(sessionStore)
        ArgumentNullException.ThrowIfNull(eventStore)

        if isNull (box claim) then
            raise (ArgumentNullException(nameof claim))

        let prepared = prepareBatch events

        task {
            let! result =
                appendWithRetry
                    (fun cancellationToken ->
                        task {
                            let! gated =
                                ClaimFence.appendEventsAsync
                                    sessionStore
                                    eventStore
                                    tenant
                                    sessionId
                                    claim
                                    prepared
                                    cancellationToken

                            // A fenced-out gate surfaces as the same stale-claim
                            // rejection the store would have returned: both mean the
                            // claim no longer owns the journal, so the caller
                            // branches once.
                            match gated with
                            | Some outcome -> return outcome
                            | None -> return EventAppendRejected(sessionId, StaleClaimReason) :> EventAppendOutcome
                        })
                    cancellationToken

            match result with
            | JournalAppended stamped -> notifyPublished tenant sessionId stamped
            | JournalRejected _ -> ()
            | JournalFailed _ -> ()

            return result
        }

    /// Appends prepared events under the journal token's store-side fence:
    /// the store verifies the token at the last moment and rejects a stale
    /// one with zero writes. The session actor routes through this overload
    /// because it carries the token string, not the claim object; the fence
    /// still guarantees a takeover loser appends nothing. Transient failures
    /// retry bounded, then fail with the typed reason. A landed batch
    /// publishes its stamped events to live subscribers before returning;
    /// fenced-out and failed writes publish nothing.
    /// <param name="eventStore">The journal the events append to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="claimToken">The opaque turn claim token fencing the journal write. Must not be null.</param>
    /// <param name="events">The events to append, in order. Must not be null or empty and must carry no nulls.</param>
    /// <param name="cancellationToken">Abandons the append.</param>
    /// <returns>The write result: stamped events, the stable rejection, or the typed failure.</returns>
    let appendWithTokenAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (claimToken: string)
        (events: IReadOnlyList<SessionEvent>)
        (cancellationToken: CancellationToken)
        : Task<JournalWriteResult> =
        ArgumentNullException.ThrowIfNull(eventStore)

        if isNull (box claimToken) then
            raise (ArgumentNullException(nameof claimToken))

        let prepared = prepareBatch events

        task {
            let! result =
                appendWithRetry
                    (fun cancellationToken ->
                        eventStore.Append(tenant, sessionId, claimToken, prepared, cancellationToken))
                    cancellationToken

            match result with
            | JournalAppended stamped -> notifyPublished tenant sessionId stamped
            | JournalRejected _ -> ()
            | JournalFailed _ -> ()

            return result
        }

    // ────────────────── Scoped outcome logging (issue 93) ──────────────────

    /// Resolves a nullable logger to a live one: the given logger, or the
    /// NullLogger when it is null. Local (not via LoggingScopes) because
    /// this module compiles before LoggingScopes and must not reference it.
    /// <param name="logger">The logger, or null for no logging.</param>
    /// <returns>The live logger, never null.</returns>
    let private resolveOutcomeLogger (logger: ILogger | null) : ILogger =
        match Option.ofObj logger with
        | Some live -> live
        | None -> NullLogger.Instance :> ILogger

    /// Reports one append outcome under the caller's pre-built six-key
    /// logging scope (built with LoggingScopes.createScope by modules
    /// compiled after LoggingScopes). Reasons are already secret-free
    /// (fixed vocabularies, never secrets or tool arguments) and still
    /// travel through redactText. Appended outcomes log at Information,
    /// rejections and failures at Warning.
    /// <param name="logger">The logger, or null for no logging.</param>
    /// <param name="scope">The pre-built scope entries, or null for no scope entries.</param>
    /// <param name="result">The append outcome to report.</param>
    let reportOutcome
        (logger: ILogger | null)
        (scope: IReadOnlyList<KeyValuePair<string, obj>> | null)
        (result: JournalWriteResult)
        : unit =
        let log = resolveOutcomeLogger logger

        use _scope =
            if isNull (box scope) then
                log.BeginScope(Dictionary<string, obj>())
            else
                log.BeginScope(scope)

        match result with
        | JournalAppended stamped ->
            let count = if isNull (box stamped) then 0 else stamped.Count
            log.LogInformation("The journal appended {Count} events.", count)
        | JournalRejected reason -> log.LogWarning("The journal rejected the append: {Reason}", redactText reason)
        | JournalFailed reason -> log.LogWarning("The journal failed the append: {Reason}", redactText reason)

    /// Appends under the claim fence and reports the outcome under the
    /// caller's scope.
    /// <param name="sessionStore">The session store verifying the claim. Must not be null.</param>
    /// <param name="eventStore">The journal the events append to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="claim">The claim fencing the append. Must not be null.</param>
    /// <param name="events">The events to append, in order. Must not be null or empty and must carry no nulls.</param>
    /// <param name="cancellationToken">Abandons the append.</param>
    /// <param name="logger">The logger, or null for no logging.</param>
    /// <param name="scope">The pre-built scope entries, or null for no scope entries.</param>
    /// <returns>The write result.</returns>
    let appendAsyncWithLogger
        (sessionStore: ISessionStore)
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (claim: TurnClaim)
        (events: IReadOnlyList<SessionEvent>)
        (cancellationToken: CancellationToken)
        (logger: ILogger | null)
        (scope: IReadOnlyList<KeyValuePair<string, obj>> | null)
        : Task<JournalWriteResult> =
        task {
            let! result = appendAsync sessionStore eventStore tenant sessionId claim events cancellationToken
            reportOutcome logger scope result
            return result
        }

    /// Appends under the journal token fence and reports the outcome under
    /// the caller's scope.
    /// <param name="eventStore">The journal the events append to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="claimToken">The opaque turn claim token fencing the journal write. Must not be null.</param>
    /// <param name="events">The events to append, in order. Must not be null or empty and must carry no nulls.</param>
    /// <param name="cancellationToken">Abandons the append.</param>
    /// <param name="logger">The logger, or null for no logging.</param>
    /// <param name="scope">The pre-built scope entries, or null for no scope entries.</param>
    /// <returns>The write result.</returns>
    let appendWithTokenAsyncWithLogger
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (claimToken: string)
        (events: IReadOnlyList<SessionEvent>)
        (cancellationToken: CancellationToken)
        (logger: ILogger | null)
        (scope: IReadOnlyList<KeyValuePair<string, obj>> | null)
        : Task<JournalWriteResult> =
        task {
            let! result = appendWithTokenAsync eventStore tenant sessionId claimToken events cancellationToken
            reportOutcome logger scope result
            return result
        }
