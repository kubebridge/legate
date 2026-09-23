// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic

// Legate configuration contract. LegateOptions is the root bound from the
// Legate configuration section (env vars override with Legate__Section__Key);
// each nested section carries mutable properties with single-node defaults
// and a Validate() that returns null when valid or the first violation
// otherwise, mirroring LlmCoordinationOptions. The runtime binder in
// LegateOptionsBinding binds an IConfigurationSection onto the root, parses
// durations and flat-choice enums from raw strings, then throws on the first
// composite violation. No AddLegate wiring lives here: registration belongs
// to issue 28.
// Note: LlmProviderOptions is reused from Llm.fs and WorkspaceOptions from
// WorkspaceRuntime.fs, so this file defines no new types under those names;
// the workspace configuration section is WorkspaceSectionOptions to avoid
// colliding with the per-runtime WorkspaceOptions base class.

/// How the runtime is deployed: a single process or a clustered Akka.NET
/// deployment. Bound from configuration; unknown values fail binding.
type ClusterMode =

    /// A single process with local session actors. The default: no
    /// remoting, no Kubernetes, no external coordination.
    | Local = 0

    /// A sharded Akka.NET cluster with seed-node discovery. Session
    /// entities run on nodes carrying the session role; nodes without
    /// it hold sharding proxies.
    | StaticSeeds = 1

    /// A sharded Akka.NET cluster bootstrapped through Akka.Management
    /// Kubernetes discovery (issue 138). The registered
    /// <see cref="T:Legate.IClusterBootstrap" /> hook (if any) supplies the
    /// management plus discovery HOCON and starts cluster formation;
    /// without a hook the node joins itself and runs as a singleton.
    /// SeedNodes is ignored in both cases.
    | Kubernetes = 2

/// What a session does with the turn interrupted by a crash once the
/// runtime resumes. Bound from configuration; unknown values fail binding.
type TurnCrashResume =

    /// The interrupted turn settles as failed; the host retries explicitly.
    | Fail = 0

    /// The runtime re-runs the interrupted turn as a new attempt.
    | RetryTurn = 1

/// Which workspace runtime serves a session's workspace. Bound from
/// configuration; unknown values fail binding.
type WorkspaceMode =

    /// A scratch directory on the local process, no sandbox.
    | Process = 0

    /// A directory on the host, bound per session.
    | HostDirectory = 1

    /// An isolated container over the Docker CLI.
    | Docker = 2

/// Sub-agent settings: how deep the task tool nests and how long one
/// nested run may spend. Bound from the <c>Legate:Sessions:SubAgents</c>
/// configuration section; mutable so hosts can set properties before
/// registering. Defaults allow one nesting level with a 10 minute
/// per-run timeout; each nested run is further bounded by the remaining
/// parent budget, whichever is shorter.
type SubAgentsOptions() =

    /// How deep task-tool nesting runs. 0 is the top-level turn, 1 is one
    /// sub-agent level: a task call at this depth or deeper returns a tool
    /// error instead of running. Default 1.
    member val MaxDepth: int = 1 with get, set

    /// How long one nested sub-agent run may spend. The effective nested
    /// deadline is the shorter of this timeout and the parent turn's
    /// remaining budget. Default 10 minutes.
    member val Timeout: TimeSpan = TimeSpan.FromMinutes 10.0 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let violations =
            [|
                if this.MaxDepth < 1 then
                    "MaxDepth must be at least 1."
                if this.Timeout <= TimeSpan.Zero then
                    "Timeout must be positive."
            |]

        if violations.Length = 0 then
            null
        else
            Array.head violations

/// Session admission and lease settings: how many sessions the dispatcher
/// admits and how turn leases renew. Bound from the <c>Legate</c>
/// configuration section; mutable so hosts can set properties before
/// registering. Defaults admit 4 sessions with a 60 s lease renewed every
/// 15 s.
type SessionsOptions() =

    /// The sessions the dispatcher admits per process. Default 4.
    member val Capacity: int = 4 with get, set

    /// The sessions admitted per tenant. Default 100.
    member val MaxSessionsPerTenant: int = 100 with get, set

    /// How long a claimed turn lease lasts without renewal. Default 60 s.
    member val LeaseDuration: TimeSpan = TimeSpan.FromSeconds 60.0 with get, set

    /// How often a claimed turn lease renews. Default 15 s; must stay below
    /// half the lease so a missed heartbeat never loses the lease.
    member val LeaseRenewalInterval: TimeSpan = TimeSpan.FromSeconds 15.0 with get, set

    /// How long a session may sit idle (no state change) before the expiry
    /// sweeper closes it, or empty (HasValue is false) meaning expiry is
    /// disabled and sessions live until the host closes them. Default
    /// empty. When set, must be positive; enabling expiry without a durable
    /// blob store fails startup, because workspace input and output
    /// restored on re-bind would otherwise be lost.
    member val Expiry: Nullable<TimeSpan> = Nullable<TimeSpan>() with get, set

    /// Sub-agent settings: the task tool's nesting depth and per-run
    /// timeout. Never null.
    member val SubAgents: SubAgentsOptions = SubAgentsOptions() with get, set

    /// Whether the session facade titles untitled sessions from their first
    /// prompt. Default false: sessions keep the host's title (or the empty
    /// string) unless the host opts in. Bound from
    /// <c>Legate:Sessions:AutoTitle</c>.
    member val AutoTitle: bool = false with get, set

    /// The title model in <c>provider/model</c> form, or null to fall back
    /// to <c>Legate:Llm:Compaction</c> and then the session/default model
    /// resolution. Only read when <see cref="P:Legate.SessionsOptions.AutoTitle" />
    /// is enabled. Bound from <c>Legate:Sessions:AutoTitleModel</c>.
    member val AutoTitleModel: string | null = null with get, set

    /// The live subscribers one session holds on its owning entity in
    /// cluster modes (and on the process-local bus in Local mode). A new
    /// subscription past the cap rejects with the typed limit error
    /// instead of evicting an existing one. Default 512. Bound from
    /// <c>Legate:Sessions:MaxSubscribersPerSession</c>.
    member val MaxSubscribersPerSession: int = 512 with get, set

    /// The events one cross-node subscriber buffers before it is
    /// considered slow. A subscriber past the bound disconnects with the
    /// typed lagged error instead of stalling the owning turn. Default
    /// 128. Bound from <c>Legate:Sessions:PerSubscriberBufferSize</c>.
    member val PerSubscriberBufferSize: int = 128 with get, set

    /// How many recent journaled events the owning entity keeps in its
    /// bounded replay cache for resuming subscribers. Older cursors fall
    /// back to <c>ISessionEventStore.Replay</c>; evicted cache entries
    /// redeliver from the store with duplicates allowed and no gaps.
    /// Default 256. Bound from
    /// <c>Legate:Sessions:SubscriptionReplayCacheSize</c>.
    member val SubscriptionReplayCacheSize: int = 256 with get, set

    /// The largest single session event the owning entity streams to a
    /// remote subscriber, in bytes. Events above the bound refuse before
    /// crossing; the global <c>Cluster:MaxWirePayloadBytes</c> caps every
    /// manifest on top of this per-event bound. Default 1 MiB. Bound from
    /// <c>Legate:Sessions:SubscriptionMaxEventPayloadBytes</c>.
    member val SubscriptionMaxEventPayloadBytes: int = 1048576 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        if isNull (box this.SubAgents) then
            "SubAgents must not be null."
        else
            let mutable parsed = Unchecked.defaultof<ModelReference>

            let titleModelInvalid =
                match Option.ofObj this.AutoTitleModel with
                | Some model when not (String.IsNullOrWhiteSpace model) -> not (ModelReference.TryParse(model, &parsed))
                | _ -> false

            let violations =
                [|
                    if this.Capacity < 1 then
                        "Capacity must be at least 1."
                    if this.MaxSessionsPerTenant < 1 then
                        "MaxSessionsPerTenant must be at least 1."
                    if this.LeaseDuration <= TimeSpan.Zero then
                        "LeaseDuration must be positive."
                    if this.LeaseRenewalInterval <= TimeSpan.Zero then
                        "LeaseRenewalInterval must be positive."
                    if
                        this.LeaseDuration > TimeSpan.Zero
                        && this.LeaseRenewalInterval > TimeSpan.Zero
                        && this.LeaseRenewalInterval >= this.LeaseDuration.Divide 2.0
                    then
                        "LeaseRenewalInterval must be less than half LeaseDuration."
                    if this.Expiry.HasValue && this.Expiry.Value <= TimeSpan.Zero then
                        "Expiry must be positive when set."
                    if titleModelInvalid then
                        "AutoTitleModel must be a valid model reference in provider/model form."
                    if this.MaxSubscribersPerSession < 1 then
                        "MaxSubscribersPerSession must be at least 1."
                    if this.PerSubscriberBufferSize < 1 then
                        "PerSubscriberBufferSize must be at least 1."
                    if this.SubscriptionReplayCacheSize < 1 then
                        "SubscriptionReplayCacheSize must be at least 1."
                    if this.SubscriptionMaxEventPayloadBytes < 1 then
                        "SubscriptionMaxEventPayloadBytes must be at least 1."
                |]

            if violations.Length <> 0 then
                Array.head violations
            else
                let subAgentsViolation = this.SubAgents.Validate()

                if isNull (box subAgentsViolation) then
                    null
                else
                    $"SubAgents: %s{subAgentsViolation}"

/// Dispatcher settings: how often the polling dispatcher sweeps sessions
/// with pending work, how many candidates one sweep drains at most, and how
/// many sessions of one agent may run at once. Bound from the
/// <c>Legate:Dispatcher</c> configuration section; mutable so hosts can set
/// properties before registering. Defaults sweep every 5 seconds, drain at
/// most 50 candidates per pass, and admit at most 4 sessions per agent. The
/// poll stays authoritative: an in-process wake only shortens the wait for
/// the next sweep, never replaces it.
type DispatcherOptions() =

    /// How often the dispatcher sweeps the tenant for sessions with pending
    /// work. Default 5 seconds.
    member val PollInterval: TimeSpan = TimeSpan.FromSeconds 5.0 with get, set

    /// How many dispatch candidates one sweep drains at most. Default 50:
    /// bounded batches keep a large pending backlog from growing the pass
    /// without bound; the pass follows the batch's HasMore flag.
    member val MaxBatchSize: int = 50 with get, set

    /// How many sessions of one agent the dispatcher admits at once, the
    /// per-agent side of the conjunctive capacity gate (process, tenant,
    /// agent: a session stays queued while any limit trips). Default 4.
    member val MaxSessionsPerAgent: int = 4 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let violations =
            [|
                if this.PollInterval <= TimeSpan.Zero then
                    "PollInterval must be positive."
                if this.MaxBatchSize < 1 then
                    "MaxBatchSize must be at least 1."
                if this.MaxSessionsPerAgent < 1 then
                    "MaxSessionsPerAgent must be at least 1."
            |]

        if violations.Length = 0 then
            null
        else
            Array.head violations

/// Per-turn defaults: the iteration and wall-clock budgets a turn runs
/// under, how prompts to a busy session deliver, and what a crash does to
/// the interrupted turn. Bound from the <c>Legate</c> configuration section;
/// mutable so hosts can set properties before registering. Defaults run at
/// most 50 iterations for 30 minutes, queue prompts, and fail the
/// interrupted turn.
type TurnsOptions() =

    /// The model iterations a turn may spend when the session was opened
    /// without an explicit budget. Default 50.
    member val DefaultMaxIterations: int = 50 with get, set

    /// The wall-clock time a turn may spend when the session was opened
    /// without an explicit budget. Default 30 minutes.
    member val DefaultTimeout: TimeSpan = TimeSpan.FromMinutes 30.0 with get, set

    /// How a prompt to a session already running a turn delivers when the
    /// call passes no explicit mode. Default
    /// <see cref="F:Legate.DeliveryMode.Queue" />.
    member val DefaultDelivery: DeliveryMode = DeliveryMode.Queue with get, set

    /// What a session does with the turn interrupted by a crash once the
    /// runtime resumes. Default
    /// <see cref="F:Legate.TurnCrashResume.Fail" />.
    member val CrashResume: TurnCrashResume = TurnCrashResume.Fail with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let violations =
            [|
                if this.DefaultMaxIterations < 1 then
                    "DefaultMaxIterations must be at least 1."
                if this.DefaultTimeout <= TimeSpan.Zero then
                    "DefaultTimeout must be positive."
                if not (Enum.IsDefined(typeof<DeliveryMode>, this.DefaultDelivery)) then
                    "DefaultDelivery has an unknown delivery mode."
                if not (Enum.IsDefined(typeof<TurnCrashResume>, this.CrashResume)) then
                    "CrashResume has an unknown crash resume."
            |]

        if violations.Length = 0 then
            null
        else
            Array.head violations

/// One configured permission rule: the tool-name pattern the rule applies
/// to and the decision the runtime assumes for it. Bound from the
/// <c>Legate</c> configuration section; mutable so hosts can set properties
/// before registering.
type PermissionRuleOptions() =

    /// The tool-name pattern the rule applies to, or null when the rule is
    /// unconfigured. Must be a non-empty string when set.
    member val ToolPattern: string | null = null with get, set

    /// The decision the runtime assumes for calls matching the pattern.
    /// Default <see cref="F:Legate.PermissionDecisionKind.Deny" />.
    member val Decision: PermissionDecisionKind = PermissionDecisionKind.Deny with get, set

    /// Returns null when the rule is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the rule is valid.</returns>
    member this.Validate() : string | null =
        let violations =
            [|
                if String.IsNullOrWhiteSpace this.ToolPattern then
                    "ToolPattern must be a non-empty string."
                if not (Enum.IsDefined(typeof<PermissionDecisionKind>, this.Decision)) then
                    "Decision has an unknown permission decision."
            |]

        if violations.Length = 0 then
            null
        else
            Array.head violations

/// Permission defaults: the decision the runtime assumes and how long it
/// waits for a host answer, plus the configured per-tool rules. Bound from
/// the <c>Legate</c> configuration section; mutable so hosts can set
/// properties before registering. Defaults deny unconfigured calls and wait
/// 5 minutes for a host answer.
type PermissionsOptions() =

    /// The decision the runtime assumes when no rule matches. Default
    /// <see cref="F:Legate.PermissionDecisionKind.Deny" />.
    member val DefaultDecision: PermissionDecisionKind = PermissionDecisionKind.Deny with get, set

    /// How long the runtime waits for a host answer before the suspended
    /// turn fails. Default 5 minutes.
    member val AskTimeout: TimeSpan = TimeSpan.FromMinutes 5.0 with get, set

    /// The per-tool rules, in the order the host configured. Empty means no
    /// configured rules.
    member val Rules: List<PermissionRuleOptions> = List<PermissionRuleOptions>() with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        if not (Enum.IsDefined(typeof<PermissionDecisionKind>, this.DefaultDecision)) then
            "DefaultDecision has an unknown permission decision."
        elif this.AskTimeout <= TimeSpan.Zero then
            "AskTimeout must be positive."
        elif isNull (box this.Rules) then
            "Rules must not be null."
        else
            let mutable violation: string | null = null
            let mutable index = 0

            while isNull (box violation) && index < this.Rules.Count do
                let rule = this.Rules[index]

                if isNull (box rule) then
                    violation <- $"Rules[%d{index}]: rule must not be null."
                else
                    let ruleViolation = rule.Validate()

                    if not (isNull (box ruleViolation)) then
                        violation <- $"Rules[%d{index}]: %s{ruleViolation}"

                index <- index + 1

            violation

/// What a session does when the model asks the host a question through
/// ask_user but no host can answer (a headless run). Bound from
/// configuration; unknown values fail binding.
type AskUserMode =

    /// The turn fails fast instead of hallucinating user input. The
    /// default: a question with no host to answer it is a turn failure,
    /// never a silent empty answer.
    | Fail = 0

    /// The turn continues with the configured canned answer as the
    /// question's tool result, without suspending.
    | AnswerWith = 1

/// The ask_user headless policy: what a session does with a question no
/// host can answer. Bound from the <c>Legate</c> configuration section;
/// mutable so hosts can set properties before registering. Defaults fail
/// the turn fast.
type AskUserOptions() =

    /// What a session does with an unanswerable question. Default
    /// <see cref="F:Legate.AskUserMode.Fail" />.
    member val Mode: AskUserMode = AskUserMode.Fail with get, set

    /// The answer an <c>AnswerWith</c> turn continues with, verbatim. Must
    /// be a non-empty string when the mode is
    /// <see cref="F:Legate.AskUserMode.AnswerWith" />; ignored otherwise.
    /// Default null.
    member val CannedAnswer: string | null = null with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        if not (Enum.IsDefined(typeof<AskUserMode>, this.Mode)) then
            "Mode has an unknown ask-user mode."
        elif
            this.Mode = AskUserMode.AnswerWith
            && String.IsNullOrWhiteSpace this.CannedAnswer
        then
            "CannedAnswer must be a non-empty string when Mode is AnswerWith."
        else
            null

/// LLM settings: the default model, the compaction model, the coordinator
/// knobs, the per-provider settings keyed by provider id, and whether the
/// coordinator uses distributed admission. Bound from the <c>Legate</c>
/// configuration section; mutable so hosts can set properties before
/// registering. Defaults name no model, coordinate locally, and register no
/// providers.
type LlmOptions() =

    /// The default model in <c>provider/model</c> form, or null when the
    /// host always names the model per agent.
    member val DefaultModel: string | null = null with get, set

    /// The compaction model in <c>provider/model</c> form, or null when
    /// compaction summarises through the session's model. Bound from
    /// <c>Legate:Llm:Compaction</c>.
    member val Compaction: string | null = null with get, set

    /// How many of the most recent history messages compaction keeps after
    /// the summary when it rewrites the transcript. Default 10.
    member val CompactionKeepMessages: int = 10 with get, set

    /// The coordinator's rate, concurrency, and retry knobs. Never null.
    member val Coordination: LlmCoordinationOptions = LlmCoordinationOptions() with get, set

    /// The per-provider settings keyed by provider id, for example
    /// <c>anthropic</c>. Empty means no configured providers.
    member val Providers: Dictionary<string, LlmProviderOptions> =
        Dictionary<string, LlmProviderOptions>() with get, set

    /// Whether the coordinator admits work through the distributed admission
    /// seam. Default false: a single node coordinates locally.
    member val DistributedCoordination: bool = false with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let mutable parsed = Unchecked.defaultof<ModelReference>

        let defaultInvalid =
            match Option.ofObj this.DefaultModel with
            | Some model when not (String.IsNullOrWhiteSpace model) -> not (ModelReference.TryParse(model, &parsed))
            | _ -> false

        let compactionInvalid =
            match Option.ofObj this.Compaction with
            | Some model when not (String.IsNullOrWhiteSpace model) -> not (ModelReference.TryParse(model, &parsed))
            | _ -> false

        if defaultInvalid then
            "DefaultModel must be a valid model reference in provider/model form."
        elif compactionInvalid then
            "Compaction must be a valid model reference in provider/model form."
        elif this.CompactionKeepMessages < 0 then
            "CompactionKeepMessages must be at least 0."
        elif isNull (box this.Coordination) then
            "Coordination must not be null."
        else
            this.ValidateMaps()

    /// Validates the coordination knobs and the provider map after the
    /// default-model check passed.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member private this.ValidateMaps() : string | null =
        let coordinationViolation = this.Coordination.Validate()

        if not (isNull (box coordinationViolation)) then
            coordinationViolation
        elif isNull (box this.Providers) then
            "Providers must not be null."
        else
            let mutable violation: string | null = null

            for entry in this.Providers do
                if isNull (box violation) then
                    if String.IsNullOrWhiteSpace entry.Key then
                        violation <- "Providers must be keyed by a non-empty provider id."
                    elif isNull (box entry.Value) then
                        violation <- $"Providers['%s{entry.Key}']: settings must not be null."

            violation

/// Workspace settings for the configuration section: which runtime serves
/// sessions, where host-directory workspaces live, and how long idle
/// workspaces survive. Bound from the <c>Legate</c> configuration section;
/// mutable so hosts can set properties before registering. Named
/// WorkspaceSectionOptions because WorkspaceOptions is already the
/// per-runtime base class in WorkspaceRuntime.fs. Defaults run scratch
/// process workspaces with a 10 minute idle teardown.
type WorkspaceSectionOptions() =

    /// Which runtime serves a session's workspace. Default
    /// <see cref="F:Legate.WorkspaceMode.Process" />.
    member val Mode: WorkspaceMode = WorkspaceMode.Process with get, set

    /// Where host-directory workspaces live, or null when the mode needs no
    /// root.
    member val RootPath: string | null = null with get, set

    /// How long a workspace may sit idle before the runtime tears down its
    /// execution vehicle. Default 10 minutes.
    member val IdleTeardownAfter: TimeSpan = TimeSpan.FromMinutes 10.0 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let violations =
            [|
                if not (Enum.IsDefined(typeof<WorkspaceMode>, this.Mode)) then
                    "Mode has an unknown workspace mode."
                if not (isNull (box this.RootPath)) && String.IsNullOrWhiteSpace this.RootPath then
                    "RootPath must be a non-empty path when set."
                if this.IdleTeardownAfter <= TimeSpan.Zero then
                    "IdleTeardownAfter must be positive."
            |]

        if violations.Length = 0 then
            null
        else
            Array.head violations

/// Headless completion delivery: how many attempts a completion gets,
/// how long the runtime waits between them, how long delivered outbox rows
/// are retained for idempotency before the purge removes them, and how the
/// re-drive service polls and leases. Bound from the <c>Legate</c>
/// configuration section; mutable so hosts can set properties before
/// registering. Defaults deliver 3 attempts with a 30 s retry delay, retain
/// delivered rows for 7 days, re-drive every 30 s, and lease deliveries for
/// 60 s.
type CompletionOptions() =

    /// The delivery attempts a headless completion gets. Default 3.
    member val MaxDeliveryAttempts: int = 3 with get, set

    /// How long the runtime waits between delivery attempts. Default 30 s.
    member val RetryDelay: TimeSpan = TimeSpan.FromSeconds 30.0 with get, set

    /// How long a delivered outbox row is retained for idempotency before
    /// the re-drive purge removes it. Default 7 days: covers weekend-long
    /// outages and crash recovery with bounded storage. Pending rows are
    /// never purged, however old.
    member val DeliveredRetention: TimeSpan = TimeSpan.FromDays 7.0 with get, set

    /// How often the re-drive service polls the outbox for pending rows.
    /// Default 30 s, the <see cref="P:Legate.CompletionOptions.RetryDelay" />
    /// cadence.
    member val RedriveInterval: TimeSpan = TimeSpan.FromSeconds 30.0 with get, set

    /// How long a re-drive delivery lease lasts before it expires and
    /// another owner may claim the row. Default 60 s.
    member val ClaimLeaseDuration: TimeSpan = TimeSpan.FromSeconds 60.0 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let violations =
            [|
                if this.MaxDeliveryAttempts < 1 then
                    "MaxDeliveryAttempts must be at least 1."
                if this.RetryDelay < TimeSpan.Zero then
                    "RetryDelay must not be negative."
                if this.DeliveredRetention <= TimeSpan.Zero then
                    "DeliveredRetention must be positive."
                if this.RedriveInterval <= TimeSpan.Zero then
                    "RedriveInterval must be positive."
                if this.ClaimLeaseDuration <= TimeSpan.Zero then
                    "ClaimLeaseDuration must be positive."
            |]

        if violations.Length = 0 then
            null
        else
            Array.head violations

/// Cluster settings: the deployment mode, the seed nodes StaticSeeds
/// mode discovers through, this node's roles, the session sharding knobs,
/// and how long the actor systems wait for graceful shutdown. Bound from
/// the <c>Legate</c> configuration section; mutable so hosts can set
/// properties before registering. Defaults run a single node with no seed
/// nodes, no roles, 128 shards at hash version 1, and a 30 s shutdown
/// grace.
type ClusterOptions() =

    /// How the runtime is deployed. Default
    /// <see cref="F:Legate.ClusterMode.Local" />.
    member val Mode: ClusterMode = ClusterMode.Local with get, set

    /// The seed nodes StaticSeeds mode discovers through, in host:port
    /// form. Empty means no seed nodes. Ignored in Kubernetes mode, where
    /// the <see cref="T:Legate.IClusterBootstrap" /> hook discovers peers.
    member val SeedNodes: List<string> = List<string>() with get, set

    /// This node's cluster roles, for example session or api. Empty means
    /// the node carries no role: it still joins, but sharding hosts no
    /// session entities on it. Entries must be non-empty.
    member val Roles: List<string> = List<string>() with get, set

    /// The role sharding hosts session entities on: nodes without this
    /// role hold proxies. Default session. Must be a non-empty string.
    member val SessionRole: string = "session" with get, set

    /// How many shards the session region spreads entities over. Part of
    /// the cluster version stamp: every node must agree, and a mismatch
    /// fails the node. Default 128. Must be at least 1.
    member val ShardCount: int = 128 with get, set

    /// The shard hash version mixed into the cluster version stamp. Bump
    /// when the extractor changes so mixed nodes fail fast instead of
    /// mis-routing. Default 1. Must be at least 1.
    member val ShardHashVersion: int = 1 with get, set

    /// How long the actor systems wait for graceful shutdown
    /// (coordinated shutdown on host stop) before the host continues
    /// stopping. Default 30 s; must stay positive.
    member val ShutdownGraceSeconds: TimeSpan = TimeSpan.FromSeconds 30.0 with get, set

    /// The largest wire payload any node accepts in bytes, bounding every
    /// versioned-envelope manifest on top of its per-case bound. Payloads
    /// above this size (or above their case bound) are refused before
    /// deserialising. Default 1 MiB. Must be at least 1.
    member val MaxWirePayloadBytes: int = 1048576 with get, set

    /// How long the split-brain resolver waits for the cluster to stabilize
    /// before downing unreachable nodes. Maps to
    /// <c>akka.cluster.split-brain-resolver.stable-after</c>. Default 20 s;
    /// must stay positive.
    member val StableAfter: TimeSpan = TimeSpan.FromSeconds 20.0 with get, set

    /// How long the cluster waits after downing before removing the downed
    /// node. <see cref="F:System.TimeSpan.Zero" /> emits <c>off</c> (the
    /// Akka default: never remove automatically); a positive value emits
    /// the duration. Maps to <c>akka.cluster.down-removal-margin</c>.
    /// Default <see cref="F:System.TimeSpan.Zero" />. Must not be negative.
    member val DownRemovalMargin: TimeSpan = TimeSpan.Zero with get, set

    /// What the split-brain resolver does when the cluster is unstable.
    /// Null emits <c>on</c> (the Akka default: down all unreachable when
    /// unstable); <see cref="F:System.TimeSpan.Zero" /> emits <c>off</c>;
    /// a positive value emits the duration. Maps to
    /// <c>akka.cluster.split-brain-resolver.down-all-when-unstable</c>.
    /// Default null. When set, must not be negative.
    member val DownAllWhenUnstable: Nullable<TimeSpan> = Nullable<TimeSpan>() with get, set

    /// How long the node waits for seed nodes to answer before giving up
    /// the join. Maps to <c>akka.cluster.seed-node-timeout</c>. Default
    /// 5 s; must stay positive.
    member val JoinTimeout: TimeSpan = TimeSpan.FromSeconds 5.0 with get, set

    /// The Legate-enforced total bound on the cluster hosted service's
    /// stop: leaving plus the running-turn drain runs inside
    /// <see cref="P:Legate.ClusterOptions.ShutdownGraceSeconds" />, and the
    /// whole stop is hard-capped by this deadline. This is a Legate-level
    /// bound, never rendered into Akka HOCON. Default 60 s; must stay
    /// positive and should exceed
    /// <see cref="P:Legate.ClusterOptions.ShutdownGraceSeconds" /> so the
    /// cap never truncates the drain wait.
    member val HostExitDeadline: TimeSpan = TimeSpan.FromSeconds 60.0 with get, set

    /// How many Up members the cluster start waits for before
    /// <c>StartAsync</c> completes, so a node never serves traffic before
    /// its quorum formed. This is a Legate-level startup gate, never
    /// rendered into Akka HOCON: the wait is bounded by
    /// <see cref="P:Legate.ClusterOptions.JoinTimeout" />, and an unmet
    /// quorum fails startup with
    /// <see cref="T:Legate.DeadlineExceededException" />. Default 1, which
    /// completes as soon as this node is Up (today's singleton behavior).
    /// Must be at least 1.
    member val MinimumMembers: int = 1 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        if not (Enum.IsDefined(typeof<ClusterMode>, this.Mode)) then
            "Mode has an unknown cluster mode."
        elif this.ShutdownGraceSeconds <= TimeSpan.Zero then
            "ShutdownGraceSeconds must be positive."
        elif this.StableAfter <= TimeSpan.Zero then
            "StableAfter must be positive."
        elif this.DownRemovalMargin < TimeSpan.Zero then
            "DownRemovalMargin must not be negative."
        elif
            this.DownAllWhenUnstable.HasValue
            && this.DownAllWhenUnstable.Value < TimeSpan.Zero
        then
            "DownAllWhenUnstable must not be negative when set."
        elif this.JoinTimeout <= TimeSpan.Zero then
            "JoinTimeout must be positive."
        elif this.HostExitDeadline <= TimeSpan.Zero then
            "HostExitDeadline must be positive."
        elif this.MinimumMembers < 1 then
            "MinimumMembers must be at least 1."
        elif isNull (box this.SeedNodes) then
            "SeedNodes must not be null."
        elif this.Mode = ClusterMode.StaticSeeds && this.SeedNodes.Count = 0 then
            "SeedNodes must not be empty in StaticSeeds mode."
        elif this.ShardCount < 1 then
            "ShardCount must be at least 1."
        elif this.ShardHashVersion < 1 then
            "ShardHashVersion must be at least 1."
        elif this.MaxWirePayloadBytes < 1 then
            "MaxWirePayloadBytes must be at least 1."
        elif String.IsNullOrWhiteSpace this.SessionRole then
            "SessionRole must be a non-empty string."
        elif isNull (box this.Roles) then
            "Roles must not be null."
        else
            let mutable violation: string | null = null
            let mutable index = 0

            while isNull (box violation) && index < this.SeedNodes.Count do
                if String.IsNullOrWhiteSpace this.SeedNodes[index] then
                    violation <- $"SeedNodes[%d{index}] must be a non-empty host:port value."

                index <- index + 1

            index <- 0

            while isNull (box violation) && index < this.Roles.Count do
                if String.IsNullOrWhiteSpace this.Roles[index] then
                    violation <- $"Roles[%d{index}] must be a non-empty string."

                index <- index + 1

            violation

// ──────────────────────────────────────────────────────────────────────────
// Schedules

/// Agent schedule settings: how often the schedule evaluator sweeps agents
/// with enabled schedules for due occurrences. Bound from the
/// <c>Legate:Schedules</c> configuration section; mutable so hosts can set
/// properties before registering. Defaults sweep every 60 seconds: 5-field
/// cron ticks at minute boundaries, so a 60 s sweep fires each occurrence
/// within a minute of its time without busy-looping.
type ScheduleOptions() =

    /// How often the evaluator sweeps the tenant for agents with enabled
    /// schedules. Default 60 seconds.
    member val PollInterval: TimeSpan = TimeSpan.FromSeconds 60.0 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        if this.PollInterval <= TimeSpan.Zero then
            "PollInterval must be positive."
        else
            null

// ──────────────────────────────────────────────────────────────────────────
// Root

/// Context-pruning settings: how much of the model's context window pruning
/// holds back beyond the reserved output, how many recent assistant turns
/// pruning never touches, and the marker pruning writes into replaced tool
/// results. Bound from the <c>Legate</c> configuration section; mutable so
/// hosts can set properties before registering. Defaults reserve 10,000
/// tokens of buffer and protect the last 2 assistant turns.
type ContextPruningOptions() =

    /// The marker text pruning writes into an eligible tool-result cell's
    /// content. Hosts can filter on this single detectable constant.
    static member DefaultPrunedMarker = "[legate-pruned-tool-result]"

    /// The tokens pruning holds back beyond the model's reserved output
    /// when deriving the prune threshold from the catalog entry. Default
    /// 10,000.
    member val ReservedBufferTokens: int = 10_000 with get, set

    /// How many of the most recent assistant turns pruning never touches.
    /// A turn counts as recent when its assistant cell is among the last
    /// this many assistant cells in transcript order. Default 2.
    member val KeepLastAssistantTurns: int = 2 with get, set

    /// The replacement text pruning writes into an eligible tool-result
    /// cell. Default <see cref="P:Legate.ContextPruningOptions.DefaultPrunedMarker" />.
    member val PrunedMarker: string = ContextPruningOptions.DefaultPrunedMarker with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let violations =
            [|
                if this.ReservedBufferTokens < 0 then
                    "ReservedBufferTokens must be at least 0."
                if this.KeepLastAssistantTurns < 0 then
                    "KeepLastAssistantTurns must be at least 0."
                if String.IsNullOrWhiteSpace this.PrunedMarker then
                    "PrunedMarker must be a non-empty string."
            |]

        if violations.Length = 0 then
            null
        else
            Array.head violations

// ──────────────────────────────────────────────────────────────────────────
// Root

/// The Legate configuration root, bound from the <c>Legate</c>
/// configuration section. Hosts set section properties before registering;
/// the binder validates the composite and throws on the first violation.
/// Defaults produce a single-node runtime: local sessions, queued prompts,
/// local LLM coordination, scratch workspaces, and no cluster.
type LegateOptions() =

    /// Session admission and lease settings. Never null.
    member val Sessions: SessionsOptions = SessionsOptions() with get, set

    /// Dispatcher settings. Never null.
    member val Dispatcher: DispatcherOptions = DispatcherOptions() with get, set

    /// Per-turn defaults. Never null.
    member val Turns: TurnsOptions = TurnsOptions() with get, set

    /// Permission defaults and rules. Never null.
    member val Permissions: PermissionsOptions = PermissionsOptions() with get, set

    /// LLM settings. Never null.
    member val Llm: LlmOptions = LlmOptions() with get, set

    /// Workspace settings. Never null.
    member val Workspace: WorkspaceSectionOptions = WorkspaceSectionOptions() with get, set

    /// Headless completion delivery. Never null.
    member val Completion: CompletionOptions = CompletionOptions() with get, set

    /// The ask_user headless policy. Never null.
    member val AskUser: AskUserOptions = AskUserOptions() with get, set

    /// Cluster settings. Never null.
    member val Cluster: ClusterOptions = ClusterOptions() with get, set

    /// Context-pruning settings. Never null.
    member val Pruning: ContextPruningOptions = ContextPruningOptions() with get, set

    /// Agent schedule settings. Never null.
    member val Schedules: ScheduleOptions = ScheduleOptions() with get, set

    /// Returns null when every section is in range, otherwise the first
    /// violation prefixed with its section path.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        if isNull (box this.Sessions) then
            "Sessions must not be null."
        elif isNull (box this.Dispatcher) then
            "Dispatcher must not be null."
        elif isNull (box this.Turns) then
            "Turns must not be null."
        elif isNull (box this.Permissions) then
            "Permissions must not be null."
        elif isNull (box this.Llm) then
            "Llm must not be null."
        elif isNull (box this.Workspace) then
            "Workspace must not be null."
        elif isNull (box this.Completion) then
            "Completion must not be null."
        elif isNull (box this.AskUser) then
            "AskUser must not be null."
        elif isNull (box this.Cluster) then
            "Cluster must not be null."
        elif isNull (box this.Pruning) then
            "Pruning must not be null."
        elif isNull (box this.Schedules) then
            "Schedules must not be null."
        else
            let sections: (string * (unit -> string | null)) list =
                [
                    "Sessions", (fun () -> this.Sessions.Validate())
                    "Dispatcher", (fun () -> this.Dispatcher.Validate())
                    "Turns", (fun () -> this.Turns.Validate())
                    "Permissions", (fun () -> this.Permissions.Validate())
                    "Llm", (fun () -> this.Llm.Validate())
                    "Workspace", (fun () -> this.Workspace.Validate())
                    "Completion", (fun () -> this.Completion.Validate())
                    "AskUser", (fun () -> this.AskUser.Validate())
                    "Cluster", (fun () -> this.Cluster.Validate())
                    "Pruning", (fun () -> this.Pruning.Validate())
                    "Schedules", (fun () -> this.Schedules.Validate())
                ]

            let mutable violation: string | null = null

            for name, validate in sections do
                if isNull (box violation) then
                    let sectionViolation = validate ()

                    if not (isNull (box sectionViolation)) then
                        violation <- $"%s{name}: %s{sectionViolation}"

            violation
