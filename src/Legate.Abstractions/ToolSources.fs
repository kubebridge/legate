// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text.RegularExpressions
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Tool source contracts. An IToolSource is how a host (and Legate.Mcp, in
// the runtime epic) contributes tools to one session: the runtime asks each
// registered source for the tools of the session's tenant, agent, and
// session id, and offers every returned tool to the model. A source is
// asynchronous and owns its own failure policy: a source that cannot serve
// a session right now reports that by returning an empty list, which the
// runtime reads as "this source contributes no tools", never as an error.
// The naming rule and the cross-source collision policy are pinned here so
// a source, the runtime, and every consumer agree without a second
// contract. Sources return raw tool results; image and video result
// handling is a runtime concern.

// The pattern const and its compiled matcher, shared by Validate and
// TryValidate, live in an internal module so the single regex instance is
// built once and the rule is defined in one place. Anchors are \A and \z,
// not ^ and $: .NET $ also matches immediately before a trailing line feed,
// so ^...$ would admit a newline-suffixed name.
module internal ToolNameRulesInternals =

    let Pattern = @"\A[a-zA-Z0-9_-]{1,128}\z"

    let PatternRegex = Regex(Pattern, RegexOptions.CultureInvariant)

/// Static validation for the tool names an
/// <see cref="T:Legate.IToolSource" /> may contribute. A name is what the
/// model calls the tool by and what the runtime keys permission decisions,
/// events, and collision detection on; names are matched ordinally and
/// used exactly as given.
/// <exception cref="T:System.ArgumentException">A name passed to Validate fails the pattern.</exception>
type ToolNameRules() =

    /// The single name rule, shared by Validate and TryValidate so the
    /// contract lives in one place: one to 128 characters of ASCII letters,
    /// digits, underscores, or dashes (<c>[a-zA-Z0-9_-]{1,128}</c>),
    /// anchored with <c>\A</c> and <c>\z</c> so the whole name, including
    /// its last character, must match. Matched ordinally: no trimming, no
    /// culture, no normalisation; the name is used exactly as given.
    static member Pattern: string = ToolNameRulesInternals.Pattern

    /// The one name rule shared by Validate and TryValidate: matched
    /// ordinally against <see cref="P:Legate.ToolNameRules.Pattern" />
    /// with no trimming, so input is used exactly as given.
    /// <param name="name">The tool name to test.</param>
    /// <returns>true when the name matches the pattern; otherwise false.</returns>
    static member private NameIsValid(name: string) =
        ToolNameRulesInternals.PatternRegex.IsMatch name

    /// Validates a tool name and returns it unchanged.
    /// <param name="name">The name to validate, for example "lookup_order".</param>
    /// <returns>The validated name, unchanged.</returns>
    /// <exception cref="T:System.ArgumentNullException">The name is null.</exception>
    /// <exception cref="T:System.ArgumentException">The name is empty, exceeds 128 characters, or contains any character outside [a-zA-Z0-9_-] (spaces, dots, slashes, trailing line feeds, or non-ASCII included).</exception>
    static member Validate(name: string) : string =
        if isNull (box name) then
            raise (ArgumentNullException(nameof name))

        if not (ToolNameRules.NameIsValid name) then
            raise (
                ArgumentException(
                    "A tool name must match [a-zA-Z0-9_-]{1,128}: one to 128 ASCII letters, digits, underscores, or dashes.",
                    nameof name
                )
            )

        name

    /// Attempts to validate a tool name; returns false for null and for
    /// any name that fails
    /// <see cref="P:Legate.ToolNameRules.Pattern" />.
    /// <param name="name">The name to test.</param>
    /// <returns>true when the name matches the pattern; otherwise false.</returns>
    static member TryValidate(name: string | null) : bool =
        if isNull (box name) then
            false
        else
            let name =
                match Option.ofObj name with
                | Some n -> n
                | None -> ""

            ToolNameRulesInternals.PatternRegex.IsMatch name

/// What an <see cref="T:Legate.IToolSource" /> needs to resolve one
/// session's tools: the tenant, agent, and session asking. Carries only
/// identifiers, so a source never sees runtime internals; anything else a
/// source needs stays with the host, reached through the source itself.
/// Constructible from C# through property setters and serialises with
/// System.Text.Json.
[<CLIMutable; NoComparison>]
type ToolSourceContext =
    {
        /// The tenant the session belongs to; a source may scope its
        /// tools or credentials by it.
        Tenant: TenantId
        /// The agent the session converses with.
        AgentId: AgentId
        /// The session asking for its tools.
        SessionId: SessionId
    }

/// How a host (and Legate.Mcp for plain MCP servers) contributes tools to a
/// session. The runtime asks every registered source for the tools of the
/// session's context and offers the combined set to the model. The contract
/// pins four rules:
/// <list type="bullet">
/// <item><description><b>Naming.</b> Every returned tool's name must satisfy
/// <see cref="P:Legate.ToolNameRules.Pattern" />; validate once when the tool
/// is built, and the runtime rejects a non-conforming name when it assembles
/// the session's tool set.</description></item>
/// <item><description><b>Degraded conditions.</b> A source that cannot serve
/// a session right now (its backing server is unreachable, for instance)
/// returns an empty list and logs the reason itself; an empty list is a
/// normal result, never an error.</description></item>
/// <item><description><b>Empty means empty.</b> An empty list contributes no
/// tools to the session; the runtime never reads it as a failure or a
/// signal to fall back to other sources.</description></item>
/// <item><description><b>Collisions.</b> Names must be unique within a
/// source, not across sources: the runtime detects duplicate names across
/// sources and, per its configured option, fails loudly or renames
/// deterministically. A source should namespace its names (for example
/// <c>mcp_getServerStatus</c>) to avoid triggering either behaviour and can
/// never rely on another source's names.</description></item>
/// </list>
type IToolSource =

    /// Resolves the tools this source contributes to one session. The
    /// runtime calls this when it builds a session's tool set, not per tool
    /// call; sources may resolve asynchronously.
    /// <param name="context">The tenant, agent, and session asking for its tools.</param>
    /// <returns>The tools offered to the model, never null and free of null entries: an empty list when the source contributes none or is degraded, per the interface's rules.</returns>
    abstract GetTools: context: ToolSourceContext -> Task<IReadOnlyList<AITool>>

/// Optional lifecycle for a source that owns resources outside a
/// <see cref="M:Legate.IToolSource.GetTools(Legate.ToolSourceContext)" />
/// call, such as a subprocess (an MCP server, for instance) or a pooled
/// connection. The lifetime is per source instance: the runtime starts each
/// lifecycle source once before it resolves tools through it and stops it
/// during shutdown. Sources are asked for tools whether or not they have
/// been started, so a source must tolerate
/// <see cref="M:Legate.IToolSourceLifecycle.StartAsync" /> never having run
/// and degrade to its empty-list behaviour; wiring (where instances live
/// and when exactly start and stop run) is the runtime epic's to own.
type IToolSourceLifecycle =

    /// Starts the source's resources. Called at most once per source
    /// instance, never per session, before tools are resolved through it.
    /// <param name="cancellationToken">Token that abandons the start.</param>
    /// <returns>A task that completes when the source is ready to serve.</returns>
    abstract StartAsync: cancellationToken: CancellationToken -> Task

    /// Stops the source's resources and releases what it owns, subprocesses
    /// included. Called at most once per source instance, never per
    /// session, and never before StartAsync completes.
    /// <param name="cancellationToken">Token that abandons the stop.</param>
    /// <returns>A task that completes when the source has released its resources.</returns>
    abstract StopAsync: cancellationToken: CancellationToken -> Task
