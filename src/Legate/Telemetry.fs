// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Diagnostics
open System.Diagnostics.Metrics

// Metrics and tracing (issue 94): the single BCL-only telemetry owner for
// the runtime. One Meter ("Legate") carries the counters and histograms, one
// ActivitySource ("Legate") carries the turn, provider-call, and tool-call
// spans. No OpenTelemetry SDK packages: export lives in the issue 92 sample,
// which reads these BCL instruments. Usage and billing semantics stay out:
// IUsageObserver (merged) owns those.
//
// Tag rules: metric instruments carry only bounded tags (status, provider,
// model, tool, outcome: names only, never ids, arguments, or output), so no
// series explodes with sessions or turns. Span tags always carry the six
// canonical logging-scope names (issue 93) plus the same bounded extras, so
// a span joins to its log lines on SessionId/TurnId. Every record call is
// guarded: telemetry never throws and never blocks the turn.
//
// Instrument names (the documented surface the mirror test asserts):
// legate.turns.started, legate.turns.settled, legate.provider.calls,
// legate.provider.latency, legate.provider.fail_open_admissions,
// legate.tool.calls, legate.tool.latency,
// legate.session.queue.depth, legate.lease.renewals, legate.dispatch.latency,
// legate.serialization.rejected.
// Span operations: Legate.Turn, Legate.ProviderCall, Legate.ToolCall.
//
// Rejected: per-module Meter/ActivitySource instances (one named pair keeps
// the names documented here); wall-clock reads at the call sites (latency
// flows through timestamp/elapsedMilliseconds, the single monotonic seam,
// measured around existing awaits with no new sleeps); tool-call counts for
// control calls that never invoke (structured finish/fail settles, permission
// denials, ask suspensions): tool.calls counts actual invocations only.
module internal Telemetry =

    /// The meter every Legate instrument reports through.
    [<Literal>]
    let MeterName = "Legate"

    /// The activity source every Legate span starts from.
    [<Literal>]
    let ActivitySourceName = "Legate"

    /// Counter: turns the ReAct loop started. No tags.
    [<Literal>]
    let TurnsStartedName = "legate.turns.started"

    /// Counter: turns the loop finished with a completion, by status
    /// (Completed, Failed, Suspended). Fenced losers and aborts that
    /// propagate as exceptions never settle, so they record no settled point.
    [<Literal>]
    let TurnsSettledName = "legate.turns.settled"

    /// Counter: coordinated provider calls that reached the invoke phase, by
    /// provider, model, and ok/error. Admission rejections (queue full,
    /// deadline before admission) never contact a provider and record nothing.
    [<Literal>]
    let ProviderCallsName = "legate.provider.calls"

    /// Histogram (milliseconds): coordinated provider-call latency around the
    /// invoke phase with retries, by provider and model.
    [<Literal>]
    let ProviderLatencyName = "legate.provider.latency"

    /// Counter: coordinated provider calls admitted locally while the
    /// distributed seam was unavailable in fail-open mode, by provider.
    /// One point per locally-admitted call, so fail-open volume stays
    /// visible during a Redis outage. Provider tag only: names only, never
    /// ids.
    [<Literal>]
    let FailOpenAdmissionsName = "legate.provider.fail_open_admissions"

    /// Counter: tool invocations that settled, by tool name and ok/error.
    /// Actual invocations only: control paths that never invoke (structured
    /// finish/fail, denials, suspensions) record nothing here.
    [<Literal>]
    let ToolCallsName = "legate.tool.calls"

    /// Histogram (milliseconds): tool-invocation latency around the invoke,
    /// by tool name.
    [<Literal>]
    let ToolLatencyName = "legate.tool.latency"

    /// UpDownCounter: session inbox queue depth across the process. Plus one
    /// on every inbox append, minus one on every settle consume. No tags:
    /// per-session series would explode with sessions.
    [<Literal>]
    let QueueDepthName = "legate.session.queue.depth"

    /// Counter: claim-heartbeat renewals, by outcome (continued, renewNow,
    /// leaseLost, cancelled).
    [<Literal>]
    let LeaseRenewalsName = "legate.lease.renewals"

    /// Histogram (milliseconds): dispatch latency from the oldest pending
    /// inbox entry's append to the dispatcher starting its session. No tags:
    /// per-session series would explode with sessions.
    [<Literal>]
    let DispatchLatencyName = "legate.dispatch.latency"

    /// Counter: wire payloads the versioned envelope refused, by reason
    /// (unknownManifest, newerVersion, oversized, failed). Every refusal
    /// fails closed: the payload never deserialises and Akka drops the
    /// message. Older-than-current manifests record failed, never a
    /// distinct tag.
    [<Literal>]
    let SerializationRejectedName = "legate.serialization.rejected"

    /// Metric tag: why the wire envelope refused a payload. One of the
    /// Rejection reason names below.
    [<Literal>]
    let ReasonTag = "reason"

    /// Rejection reason: the manifest names no registered wire case.
    [<Literal>]
    let RejectionUnknownManifest = "unknownManifest"

    /// Rejection reason: the manifest version is newer than the registered
    /// wire-case version.
    [<Literal>]
    let RejectionNewerVersion = "newerVersion"

    /// Rejection reason: the payload exceeds the wire-case byte bound (or
    /// the configured global maximum) and is refused before deserialising.
    [<Literal>]
    let RejectionOversized = "oversized"

    /// Rejection reason: the payload failed to deserialise or to map back
    /// onto its domain message, including older-than-current manifests and
    /// known manifests carrying an unknown inner $type.
    [<Literal>]
    let RejectionFailed = "failed"

    /// Span operation: one turn of the ReAct loop.
    [<Literal>]
    let TurnActivityName = "Legate.Turn"

    /// Span operation: one coordinated provider call.
    [<Literal>]
    let ProviderActivityName = "Legate.ProviderCall"

    /// Span operation: one tool invocation.
    [<Literal>]
    let ToolActivityName = "Legate.ToolCall"

    /// Metric tag: the terminal-or-parked status (turns) or ok/error (calls).
    [<Literal>]
    let StatusTag = "status"

    /// Metric and span tag: the provider id (canonical lowercase).
    [<Literal>]
    let ProviderTag = "provider"

    /// Metric and span tag: the model name.
    [<Literal>]
    let ModelTag = "model"

    /// Metric and span tag: the tool name.
    [<Literal>]
    let ToolTag = "tool"

    /// Metric tag: the lease-renewal outcome.
    [<Literal>]
    let OutcomeTag = "outcome"

    /// Span tag: the session the span belongs to. Mirrors
    /// LoggingScopes.SessionIdKey; the mirror test fails on drift.
    [<Literal>]
    let SessionIdTag = "SessionId"

    /// Span tag: the turn the span belongs to. Mirrors
    /// LoggingScopes.TurnIdKey.
    [<Literal>]
    let TurnIdTag = "TurnId"

    /// Span tag: the agent the session runs as. Mirrors
    /// LoggingScopes.AgentIdKey.
    [<Literal>]
    let AgentIdTag = "AgentId"

    /// Span tag: the tenant the session belongs to. Mirrors
    /// LoggingScopes.TenantIdKey.
    [<Literal>]
    let TenantIdTag = "TenantId"

    /// Span tag: the 1-based attempt the turn runs under. Mirrors
    /// LoggingScopes.AttemptKey.
    [<Literal>]
    let AttemptTag = "Attempt"

    /// Span tag: the owner holding the turn claim. Mirrors
    /// LoggingScopes.ClaimOwnerKey.
    [<Literal>]
    let ClaimOwnerTag = "ClaimOwner"

    /// Every canonical id tag name, in stable order. Mirrors
    /// LoggingScopes.AllKeys; the mirror test fails on drift.
    let CanonicalTagNames: string[] =
        [|
            SessionIdTag
            TurnIdTag
            AgentIdTag
            TenantIdTag
            AttemptTag
            ClaimOwnerTag
        |]

    /// Metric status: the call settled without raising.
    [<Literal>]
    let StatusOk = "ok"

    /// Metric status: the call raised.
    [<Literal>]
    let StatusError = "error"

    /// Metric status fallback: a null or blank status never emits blank.
    [<Literal>]
    let StatusUnknown = "unknown"

    /// Lease outcome: the lease stays live under the carried claim.
    [<Literal>]
    let LeaseContinued = "continued"

    /// Lease outcome: the lease still holds but is close to expiry.
    [<Literal>]
    let LeaseRenewNow = "renewNow"

    /// Lease outcome: the lease is gone (expired, taken over, or missing).
    [<Literal>]
    let LeaseLost = "leaseLost"

    /// Lease outcome: the host asked to cancel the turn.
    [<Literal>]
    let LeaseCancelled = "cancelled"

    let private meter = new Meter(MeterName)

    let private activitySource = new ActivitySource(ActivitySourceName)

    let private turnsStartedCounter: Counter<int64> =
        meter.CreateCounter<int64>(TurnsStartedName, description = "Turns the ReAct loop started.")

    let private turnsSettledCounter: Counter<int64> =
        meter.CreateCounter<int64>(
            TurnsSettledName,
            description = "Turns the ReAct loop finished with a completion, by status."
        )

    let private providerCallsCounter: Counter<int64> =
        meter.CreateCounter<int64>(
            ProviderCallsName,
            description = "Coordinated provider calls that reached the invoke phase."
        )

    let private providerLatencyHistogram: Histogram<double> =
        meter.CreateHistogram<double>(
            ProviderLatencyName,
            "ms",
            "Coordinated provider-call latency around the invoke phase."
        )

    let private failOpenAdmissionsCounter: Counter<int64> =
        meter.CreateCounter<int64>(
            FailOpenAdmissionsName,
            description = "Coordinated provider calls admitted locally while the distributed seam was unavailable."
        )

    let private toolCallsCounter: Counter<int64> =
        meter.CreateCounter<int64>(ToolCallsName, description = "Tool invocations that settled.")

    let private toolLatencyHistogram: Histogram<double> =
        meter.CreateHistogram<double>(ToolLatencyName, "ms", "Tool-invocation latency around the invoke.")

    let private queueDepthCounter: UpDownCounter<int64> =
        meter.CreateUpDownCounter<int64>(QueueDepthName, description = "Session inbox queue depth across the process.")

    let private leaseRenewalsCounter: Counter<int64> =
        meter.CreateCounter<int64>(LeaseRenewalsName, description = "Claim-heartbeat renewals, by outcome.")

    let private dispatchLatencyHistogram: Histogram<double> =
        meter.CreateHistogram<double>(
            DispatchLatencyName,
            "ms",
            "Dispatch latency from the oldest pending inbox append to the session start."
        )

    let private serializationRejectedCounter: Counter<int64> =
        meter.CreateCounter<int64>(
            SerializationRejectedName,
            description = "Wire payloads the versioned envelope refused, by reason."
        )

    /// Treats a null string as empty: ids and names always emit something.
    /// <param name="value">The value to normalise, or null.</param>
    /// <returns>The value, or empty when it was null.</returns>
    let private text (value: string | null) : string =
        match value with
        | null -> ""
        | live -> live

    /// Bounds a metric status: null or blank never emits blank.
    /// <param name="status">The status to bound, or null.</param>
    /// <returns>The status, or unknown when it was blank.</returns>
    let private boundedStatus (status: string | null) : string =
        match status with
        | null -> StatusUnknown
        | live when String.IsNullOrWhiteSpace live -> StatusUnknown
        | live -> live

    /// Captures the monotonic start point for a latency measurement: the
    /// single clock seam, read around an existing await, never a new sleep.
    /// <returns>The timestamp to hand to elapsedMilliseconds.</returns>
    let timestamp () : int64 = Stopwatch.GetTimestamp()

    /// Reads the milliseconds since a timestamp: the matching half of the
    /// single clock seam. Never negative.
    /// <param name="start">The timestamp timestamp returned.</param>
    /// <returns>The elapsed milliseconds, or 0 on a bad reading.</returns>
    let elapsedMilliseconds (start: int64) : float =
        try
            max 0.0 (Stopwatch.GetElapsedTime(start).TotalMilliseconds)
        with _ ->
            0.0

    /// Builds the six canonical id tags in stable order. Values pass through
    /// verbatim (empty for unknown); names are the CanonicalTagNames the
    /// mirror test pins to LoggingScopes.AllKeys.
    /// <param name="sessionId">The session, or null for unknown.</param>
    /// <param name="turnId">The turn, or null for unknown.</param>
    /// <param name="agentId">The agent, or null for unknown.</param>
    /// <param name="tenantId">The tenant, or null for unknown.</param>
    /// <param name="attempt">The 1-based attempt, or null for unknown.</param>
    /// <param name="claimOwner">The claim owner, or null for unknown.</param>
    /// <returns>The six id tags, in CanonicalTagNames order.</returns>
    let idTagsFor
        (sessionId: string | null)
        (turnId: string | null)
        (agentId: string | null)
        (tenantId: string | null)
        (attempt: string | null)
        (claimOwner: string | null)
        : IReadOnlyList<KeyValuePair<string, obj>> =
        ResizeArray<KeyValuePair<string, obj>>(
            [|
                KeyValuePair<string, obj>(SessionIdTag, (text sessionId) :> obj)
                KeyValuePair<string, obj>(TurnIdTag, (text turnId) :> obj)
                KeyValuePair<string, obj>(AgentIdTag, (text agentId) :> obj)
                KeyValuePair<string, obj>(TenantIdTag, (text tenantId) :> obj)
                KeyValuePair<string, obj>(AttemptTag, (text attempt) :> obj)
                KeyValuePair<string, obj>(ClaimOwnerTag, (text claimOwner) :> obj)
            |]
        )
        :> IReadOnlyList<KeyValuePair<string, obj>>

    /// Reads one scope value as text: strings pass through, other values
    /// (the Attempt int) render with ToString, missing keys read empty.
    /// <param name="table">The scope entries by key.</param>
    /// <param name="key">The key to read.</param>
    /// <returns>The value text, or empty when missing.</returns>
    let private scopeText (table: Map<string, obj>) (key: string) : string =
        match Map.tryFind key table with
        | None -> ""
        | Some value ->
            if isNull (box value) then
                ""
            else
                match value with
                | :? string as textValue -> textValue
                | other ->
                    let rendered: string | null = other.ToString()

                    match rendered with
                    | null -> ""
                    | live -> live

    /// Builds the six canonical id tags from a logging scope: the same keys
    /// the log lines carry, so a span joins to its lines. A null scope or a
    /// missing key reads empty; the names are always present.
    /// <param name="scope">The scope entries, or null for all-empty tags.</param>
    /// <returns>The six id tags, in CanonicalTagNames order.</returns>
    let idTagsFromScope
        (scope: IReadOnlyList<KeyValuePair<string, obj>> | null)
        : IReadOnlyList<KeyValuePair<string, obj>> =
        try
            match scope with
            | null -> idTagsFor null null null null null null
            | live ->
                let table =
                    live
                    |> Seq.filter (fun entry -> not (isNull (box entry.Key)))
                    |> Seq.map (fun entry -> entry.Key, entry.Value)
                    |> Map.ofSeq

                idTagsFor
                    (scopeText table SessionIdTag)
                    (scopeText table TurnIdTag)
                    (scopeText table AgentIdTag)
                    (scopeText table TenantIdTag)
                    (scopeText table AttemptTag)
                    (scopeText table ClaimOwnerTag)
        with _ ->
            idTagsFor null null null null null null

    /// Starts a scope span: the activity when a listener samples, otherwise
    /// a no-op disposable. Never throws and never returns null.
    /// <param name="operation">The span operation name.</param>
    /// <param name="tags">The span tags.</param>
    /// <returns>The scope to dispose when the span ends.</returns>
    let private startScope (operation: string) (tags: IReadOnlyList<KeyValuePair<string, obj>>) : IDisposable =
        try
            let entries =
                if isNull (box tags) then
                    ResizeArray<KeyValuePair<string, obj>>() :> IEnumerable<KeyValuePair<string, obj>>
                else
                    tags :> IEnumerable<KeyValuePair<string, obj>>

            let activity =
                activitySource.StartActivity(
                    operation,
                    ActivityKind.Internal,
                    Unchecked.defaultof<ActivityContext>,
                    entries
                )

            match activity with
            | null ->
                { new IDisposable with
                    member _.Dispose() = ()
                }
            | live -> live :> IDisposable
        with _ ->
            { new IDisposable with
                member _.Dispose() = ()
            }

    /// Starts the turn span with the six canonical id tags.
    /// <param name="idTags">The id tags, built with idTagsFor or idTagsFromScope.</param>
    /// <returns>The scope to dispose when the turn settles.</returns>
    let startTurnScope (idTags: IReadOnlyList<KeyValuePair<string, obj>>) : IDisposable =
        startScope TurnActivityName idTags

    /// Starts the provider-call span with the id tags plus provider and
    /// model. Never throws and never returns null.
    /// <param name="provider">The provider id, or null for unknown.</param>
    /// <param name="model">The model name, or null for unknown.</param>
    /// <param name="idTags">The id tags, built with idTagsFor or idTagsFromScope.</param>
    /// <returns>The scope to dispose when the call settles.</returns>
    let startProviderScope
        (provider: string | null)
        (model: string | null)
        (idTags: IReadOnlyList<KeyValuePair<string, obj>>)
        : IDisposable =
        let entries = ResizeArray<KeyValuePair<string, obj>>()

        if not (isNull (box idTags)) then
            for entry in idTags do
                entries.Add(entry)

        match provider with
        | null -> ()
        | live when String.IsNullOrEmpty live -> ()
        | live -> entries.Add(KeyValuePair<string, obj>(ProviderTag, live :> obj))

        match model with
        | null -> ()
        | live when String.IsNullOrEmpty live -> ()
        | live -> entries.Add(KeyValuePair<string, obj>(ModelTag, live :> obj))

        startScope ProviderActivityName (entries :> IReadOnlyList<KeyValuePair<string, obj>>)

    /// Starts the tool-call span with the id tags plus the tool name. Never
    /// throws and never returns null.
    /// <param name="toolName">The tool name, or null for unknown.</param>
    /// <param name="idTags">The id tags, built with idTagsFor or idTagsFromScope.</param>
    /// <returns>The scope to dispose when the call settles.</returns>
    let startToolScope (toolName: string | null) (idTags: IReadOnlyList<KeyValuePair<string, obj>>) : IDisposable =
        let entries = ResizeArray<KeyValuePair<string, obj>>()

        if not (isNull (box idTags)) then
            for entry in idTags do
                entries.Add(entry)

        match toolName with
        | null -> ()
        | live when String.IsNullOrEmpty live -> ()
        | live -> entries.Add(KeyValuePair<string, obj>(ToolTag, live :> obj))

        startScope ToolActivityName (entries :> IReadOnlyList<KeyValuePair<string, obj>>)

    /// Records one started turn. Never throws.
    let recordTurnStarted () : unit =
        try
            turnsStartedCounter.Add(1L)
        with _ ->
            ()

    /// Records one finished turn completion by status. Never throws.
    /// <param name="status">The terminal-or-parked status name.</param>
    let recordTurnSettled (status: string | null) : unit =
        try
            let mutable tags = TagList()
            tags.Add(StatusTag, (boundedStatus status :> obj))
            turnsSettledCounter.Add(1L, &tags)
        with _ ->
            ()

    /// Records one coordinated provider call that reached the invoke phase.
    /// Never throws.
    /// <param name="provider">The provider id, or null for unknown.</param>
    /// <param name="model">The model name, or null for unknown.</param>
    /// <param name="status">Ok or error.</param>
    let recordProviderCall (provider: string | null) (model: string | null) (status: string | null) : unit =
        try
            let mutable tags = TagList()
            tags.Add(ProviderTag, ((text provider) :> obj))
            tags.Add(ModelTag, ((text model) :> obj))
            tags.Add(StatusTag, ((boundedStatus status) :> obj))
            providerCallsCounter.Add(1L, &tags)
        with _ ->
            ()

    /// Records one provider-call latency sample in milliseconds. Never throws.
    /// <param name="milliseconds">The elapsed milliseconds.</param>
    /// <param name="provider">The provider id, or null for unknown.</param>
    /// <param name="model">The model name, or null for unknown.</param>
    let recordProviderLatency (milliseconds: float) (provider: string | null) (model: string | null) : unit =
        try
            let mutable tags = TagList()
            tags.Add(ProviderTag, ((text provider) :> obj))
            tags.Add(ModelTag, ((text model) :> obj))
            providerLatencyHistogram.Record(max 0.0 milliseconds, &tags)
        with _ ->
            ()

    /// Records one coordinated provider call admitted locally while the
    /// distributed seam was unavailable in fail-open mode. Never throws.
    /// <param name="provider">The provider id, or null for unknown.</param>
    let recordFailOpenAdmission (provider: string | null) : unit =
        try
            let mutable tags = TagList()
            tags.Add(ProviderTag, ((text provider) :> obj))
            failOpenAdmissionsCounter.Add(1L, &tags)
        with _ ->
            ()

    /// Records one settled tool invocation. Never throws.
    /// <param name="toolName">The tool name, or null for unknown.</param>
    /// <param name="status">Ok or error.</param>
    let recordToolCall (toolName: string | null) (status: string | null) : unit =
        try
            let mutable tags = TagList()
            tags.Add(ToolTag, ((text toolName) :> obj))
            tags.Add(StatusTag, ((boundedStatus status) :> obj))
            toolCallsCounter.Add(1L, &tags)
        with _ ->
            ()

    /// Records one tool-invocation latency sample in milliseconds. Never throws.
    /// <param name="milliseconds">The elapsed milliseconds.</param>
    /// <param name="toolName">The tool name, or null for unknown.</param>
    let recordToolLatency (milliseconds: float) (toolName: string | null) : unit =
        try
            let mutable tags = TagList()
            tags.Add(ToolTag, ((text toolName) :> obj))
            toolLatencyHistogram.Record(max 0.0 milliseconds, &tags)
        with _ ->
            ()

    /// Moves the queue-depth gauge by the delta: plus one on inbox append,
    /// minus one on settle consume. Never throws.
    /// <param name="delta">How the pending count moved.</param>
    let addQueueDepth (delta: int) : unit =
        try
            queueDepthCounter.Add(int64 delta)
        with _ ->
            ()

    /// Records one claim-heartbeat renewal by outcome. Never throws.
    /// <param name="outcome">One of the Lease outcome names.</param>
    let recordLeaseRenewal (outcome: string | null) : unit =
        try
            let mutable tags = TagList()
            tags.Add(OutcomeTag, ((boundedStatus outcome) :> obj))
            leaseRenewalsCounter.Add(1L, &tags)
        with _ ->
            ()

    /// Records one dispatch-latency sample in milliseconds: how long the
    /// oldest pending inbox entry waited before the dispatcher started its
    /// session. No tags, so per-session waits never explode the series.
    /// The session queue depth rides the existing queue-depth gauge (plus
    /// one on inbox append, minus one on settle consume), so dispatch adds
    /// no depth instrument of its own. Never throws.
    /// <param name="milliseconds">The elapsed milliseconds.</param>
    let recordDispatchLatency (milliseconds: float) : unit =
        try
            let mutable tags = TagList()
            dispatchLatencyHistogram.Record(max 0.0 milliseconds, &tags)
        with _ ->
            ()

    /// Bounds a rejection reason: null or blank never emits blank.
    /// <param name="reason">The reason to bound, or null.</param>
    /// <returns>The reason, or failed when it was blank.</returns>
    let private boundedReason (reason: string | null) : string =
        match reason with
        | null -> RejectionFailed
        | live when String.IsNullOrWhiteSpace live -> RejectionFailed
        | live -> live

    /// Records one wire payload the versioned envelope refused, by reason.
    /// Never throws.
    /// <param name="reason">One of the Rejection reason names.</param>
    let recordSerializationRejected (reason: string | null) : unit =
        try
            let mutable tags = TagList()
            tags.Add(ReasonTag, ((boundedReason reason) :> obj))
            serializationRejectedCounter.Add(1L, &tags)
        with _ ->
            ()
