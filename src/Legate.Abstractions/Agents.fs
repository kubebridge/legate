// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text.RegularExpressions

// Agent definition contracts. An Agent is what a host stores and what the
// runtime reads to run a session: identity, tenant, model configuration,
// prompt, environment variables, permission defaults, tool selection, the
// optional schedule, and persistence bookkeeping. The runtime-relevant
// shape only; host-specific references stay with the host, and a schedule
// rides on the agent as a nullable field the store persists with it while
// its cron and time-zone semantics stay with the dispatcher epic. All
// types serialise with System.Text.Json and are constructible from C#
// through property setters.

// The pattern const and its compiled matcher, shared by Validate and
// TryValidate, live in an internal module so the single regex instance is
// built once and the rule is defined in one place. Anchors are \A and \z,
// not ^ and $: .NET $ also matches immediately before a trailing line feed,
// so ^...$ would admit a newline-suffixed key.
module internal AgentEnvironmentKeysInternals =

    let Pattern = @"\A[A-Z_][A-Z0-9_]{0,63}\z"

    let PatternRegex = Regex(Pattern, RegexOptions.CultureInvariant)

/// Static validation for the environment variable keys an
/// <see cref="T:Legate.Agent" /> may carry. Keys are process-environment
/// names, not secrets; values live with the host and are never logged.
/// <exception cref="T:System.ArgumentException">A key passed to Validate fails the pattern.</exception>
type AgentEnvironmentKeys() =

    /// The single key rule, shared by Validate and TryValidate so the
    /// contract lives in one place: an uppercase letter or underscore
    /// followed by up to 63 more uppercase letters, digits, or underscores
    /// (<c>[A-Z_][A-Z0-9_]{0,63}</c>), anchored with <c>\A</c> and <c>\z</c>
    /// so the whole key, including its last character, must match. Matched
    /// ordinally: no trimming, no culture, no normalisation; the key is
    /// used exactly as given.
    static member Pattern: string = AgentEnvironmentKeysInternals.Pattern

    /// The one key rule shared by Validate and TryValidate: matched
    /// ordinally against <see cref="P:Legate.AgentEnvironmentKeys.Pattern" />
    /// with no trimming, so input is used exactly as given.
    /// <param name="key">The environment variable key to test.</param>
    /// <returns>true when the key matches the pattern; otherwise false.</returns>
    static member private KeyIsValid(key: string) =
        AgentEnvironmentKeysInternals.PatternRegex.IsMatch key

    /// Validates an environment variable key and returns it unchanged.
    /// <param name="key">The key to validate, for example "MY_KEY_1".</param>
    /// <returns>The validated key, unchanged.</returns>
    /// <exception cref="T:System.ArgumentNullException">The key is null.</exception>
    /// <exception cref="T:System.ArgumentException">The key is empty, contains lowercase letters, starts with a digit, exceeds 64 characters, or contains any character outside [A-Z0-9_] (dashes, whitespace, trailing line feeds, or non-ASCII included).</exception>
    static member Validate(key: string) : string =
        if isNull (box key) then
            raise (ArgumentNullException(nameof key))

        if not (AgentEnvironmentKeys.KeyIsValid key) then
            raise (
                ArgumentException(
                    "An environment variable key must match [A-Z_][A-Z0-9_]{0,63}: uppercase letters, digits, and underscores only, at most 64 characters, starting with an uppercase letter or underscore.",
                    nameof key
                )
            )

        key

    /// Attempts to validate an environment variable key; returns false for
    /// null and for any key that fails
    /// <see cref="P:Legate.AgentEnvironmentKeys.Pattern" />.
    /// <param name="key">The key to test.</param>
    /// <returns>true when the key matches the pattern; otherwise false.</returns>
    static member TryValidate(key: string | null) : bool =
        if isNull (box key) then
            false
        else
            let key =
                match Option.ofObj key with
                | Some k -> k
                | None -> ""

            AgentEnvironmentKeysInternals.PatternRegex.IsMatch key

/// The permission defaults an agent's sessions start from: what the runtime
/// answers when a tool call needs a decision the host has not configured a
/// policy for. A reference type with mutable properties so absent JSON
/// properties keep the safe defaults and C# object initialisers work; null
/// on <see cref="T:Legate.Agent" /> means the runtime default.
type PermissionDefaults() =

    /// The decision the runtime assumes when nothing more specific is
    /// configured. Defaults to
    /// <see cref="F:Legate.PermissionDecisionKind.Deny" /> so an
    /// unconfigured agent never executes tools without the host allowing
    /// it.
    member val DefaultDecision: PermissionDecisionKind = PermissionDecisionKind.Deny with get, set

    /// Whether the runtime may ask the host when the default decision is
    /// insufficient. Defaults to true: an agent configured only with a
    /// default decision still surfaces permission requests to the host.
    member val AskWhenUnresolved: bool = true with get, set

/// Which tools an agent may call: which built-in tool names are enabled and
/// which registered tool sources apply. A reference type with mutable
/// properties so absent JSON properties keep the safe defaults and C#
/// object initialisers work; null on <see cref="T:Legate.Agent" /> means the
/// runtime default. Built-in names stay strings because built-ins live in
/// the Legate runtime, which the contracts cannot reference; the runtime
/// resolves the names when it loads the agent.
type ToolSelection() =

    /// The names of the built-in tools enabled for the agent, in the order
    /// the host configured; empty means no built-in tools.
    member val BuiltIns: IReadOnlyList<string> = ResizeArray<string>() :> IReadOnlyList<string> with get, set

    /// The names of the registered tool sources whose tools apply to the
    /// agent; empty means every registered source applies.
    member val ToolSources: IReadOnlyList<string> = ResizeArray<string>() :> IReadOnlyList<string> with get, set

/// One scheduled prompt an agent's definition carries: a cron expression,
/// the time zone it fires in, and the message each firing prompts the agent
/// with. Opaque strings to the store and the runtime: the dispatcher epic
/// (scheduling) owns the cron and IANA time-zone formats and rejects
/// invalid values when the agent is saved with a schedule. Constructible
/// from C# through property setters and serialises with System.Text.Json.
[<CLIMutable; NoComparison>]
type AgentSchedule =
    {
        /// The cron expression describing when the prompt fires. Format and
        /// validation stay with the dispatcher epic; the store persists the
        /// string verbatim.
        Cron: string
        /// The IANA time-zone id the cron fires in, for example
        /// "Europe/Berlin". Format and validation stay with the dispatcher
        /// epic; the store persists the string verbatim.
        TimeZone: string
        /// The message each firing prompts the agent with.
        Message: string
        /// Whether the schedule is active. Disabled schedules stay stored
        /// and skipped.
        Enabled: bool
    }

/// One custom HTTP tool an agent may call: a model-facing name, the HTTP
/// endpoint to invoke, and the signing secret that authenticates each call.
/// The store keys tools by <see cref="P:Legate.AgentCustomTool.Name" /> per
/// agent and validates the name against
/// <see cref="P:Legate.ToolNameRules.Pattern" /> on upsert; name
/// sanitisation and cross-source collisions stay with the invocation epic.
/// <see cref="P:Legate.AgentCustomTool.SigningSecret" /> is opaque bytes the
/// host has already protected; the record is sensitive as a whole and must
/// never be logged. Constructible from C# through property setters and
/// serialises with System.Text.Json.
[<CLIMutable; NoComparison>]
type AgentCustomTool =
    {
        /// The tenant the tool belongs to.
        Tenant: TenantId
        /// The agent the tool is enabled for.
        AgentId: AgentId
        /// The model-facing tool name the runtime invokes the tool by.
        /// Validated against <see cref="P:Legate.ToolNameRules.Pattern" />
        /// when the store upserts the tool.
        Name: string
        /// What the tool does, or null when the host supplies none.
        Description: string | null
        /// The absolute HTTP endpoint each invocation calls.
        Endpoint: Uri
        /// The JSON Schema text of the tool's input, or null to use the
        /// permissive-schema fallback the invocation epic defines.
        InputSchema: string | null
        /// Headers each invocation sends, or null when the tool sends none.
        /// Values are host data and never logged.
        Headers: IReadOnlyDictionary<string, string> | null
        /// The opaque signing secret each invocation authenticates with:
        /// non-empty bytes the host has already protected. Treated as
        /// sensitive; never logged, never embedded in exception messages.
        SigningSecret: byte[]
        /// Whether the tool is callable. Disabled tools stay stored and
        /// unlisted.
        Enabled: bool
        /// The optimistic-concurrency row version; 0 means the tool has
        /// never been persisted. The store stamps it: previous plus one on
        /// upsert, one on insert.
        RowVersion: uint64
        /// When the tool was created.
        CreatedAt: DateTimeOffset
        /// When the tool was last updated.
        UpdatedAt: DateTimeOffset
    }

/// The durable definition of an agent: what a host stores and what the
/// runtime reads to run a session. PackageReference is opaque to the
/// runtime; RowVersion is the optimistic-concurrency token whose increment
/// semantics the store epic owns (0 means never persisted). Schedule is the
/// agent's optional dispatcher schedule, carried on the definition so the
/// store's one write path saves it with the agent. Constructible from C#
/// through property setters and serialises with System.Text.Json; absent
/// optional properties deserialise as null and mean the runtime default.
/// Hosts validate invariants (non-empty Name, well-formed SystemPrompt,
/// environment keys) through the runtime when the agent is loaded; this
/// record carries the stored shape.
[<CLIMutable; NoComparison>]
type Agent =
    {
        /// The agent's durable identifier.
        Id: AgentId
        /// The tenant the agent belongs to; hosts supply it through
        /// <see cref="M:Legate.TenantId.Create(System.String)" /> or
        /// <see cref="F:Legate.TenantId.Default" />.
        Tenant: TenantId
        /// The agent's display name; the runtime epic validates non-emptiness on load.
        Name: string
        /// What the agent is for, or null when the host supplies none.
        Description: string | null
        /// The model the agent runs on; the registry resolves the
        /// reference's provider segment before a turn runs.
        Model: ModelReference
        /// The system prompt prepended to every turn.
        SystemPrompt: string
        /// The environment variable names the agent's workspace sets, or
        /// null when the agent sets none. Keys are validated through
        /// <see cref="T:Legate.AgentEnvironmentKeys" /> when the runtime
        /// loads the agent; values are host data, never logged.
        EnvironmentVariables: IReadOnlyDictionary<string, string> | null
        /// The permission defaults the agent's sessions start with, or null
        /// to use the runtime default.
        PermissionDefaults: PermissionDefaults | null
        /// Which built-ins and tool sources apply to the agent, or null to
        /// use the runtime default.
        ToolSelection: ToolSelection | null
        /// An opaque reference the host uses to locate the agent's package
        /// contents, or null when the agent has none. The runtime never
        /// interprets it.
        PackageReference: string | null
        /// Whether the runtime accepts new sessions for the agent. Disabled
        /// agents keep existing sessions runnable.
        Enabled: bool
        /// The agent's schedule, or null when the agent runs on demand
        /// only. The store persists it with the agent; the dispatcher epic
        /// owns the schedule's cron and time-zone semantics and validates
        /// them when the agent is saved with a schedule.
        Schedule: AgentSchedule | null
        /// The optimistic-concurrency row version; 0 means the agent has
        /// never been persisted. The store epic owns the increment
        /// semantics.
        RowVersion: uint64
        /// When the agent was created.
        CreatedAt: DateTimeOffset
        /// When the agent was last updated.
        UpdatedAt: DateTimeOffset
    }
