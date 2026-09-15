// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

open System
open System.Collections.Generic
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions

// Logging scopes and redaction (issue 93): the canonical scope key names
// every runtime log line carries (SessionId, TurnId, AgentId, TenantId,
// Attempt, ClaimOwner), a BeginScope helper, and a redactForLog wrapper
// over JournalWriter.redactText. All wiring stays internal; no public
// surface change. Sibling issue 94 mirrors these canonical names as tags.
// Rejected: per-module scope key strings (drift); a log-local redaction
// regex fork (diverges from the journal guarantees); ILogger on public
// Abstractions signatures (breaks the C#-friendly boundary rules).
module internal LoggingScopes =

    /// The session the log line belongs to.
    [<Literal>]
    let SessionIdKey = "SessionId"

    /// The turn the log line belongs to.
    [<Literal>]
    let TurnIdKey = "TurnId"

    /// The agent the session runs as.
    [<Literal>]
    let AgentIdKey = "AgentId"

    /// The tenant the session belongs to.
    [<Literal>]
    let TenantIdKey = "TenantId"

    /// The 1-based attempt the turn runs under.
    [<Literal>]
    let AttemptKey = "Attempt"

    /// The owner holding the turn claim.
    [<Literal>]
    let ClaimOwnerKey = "ClaimOwner"

    /// Every canonical scope key, in stable order.
    let AllKeys: string[] =
        [|
            SessionIdKey
            TurnIdKey
            AgentIdKey
            TenantIdKey
            AttemptKey
            ClaimOwnerKey
        |]

    /// Resolves a nullable logger to a live one: the given logger, or the
    /// NullLogger when it is null (the CustomToolSource precedent).
    /// <param name="logger">The logger, or null for no logging.</param>
    /// <returns>The live logger, never null.</returns>
    let resolveLogger (logger: ILogger | null) : ILogger =
        match Option.ofObj logger with
        | Some live -> live
        | None -> NullLogger.Instance :> ILogger

    /// Builds the six-entry scope every runtime log line carries. Unknown
    /// values pass as empty strings (or 0 for the attempt) so the keys are
    /// always present even when the module knows only a subset of the ids.
    /// <param name="tenant">The tenant, or null for unknown.</param>
    /// <param name="sessionId">The session, or null for unknown.</param>
    /// <param name="turnId">The turn, or null for unknown.</param>
    /// <param name="agentId">The agent, or null for unknown.</param>
    /// <param name="attempt">The 1-based attempt, or 0 for unknown.</param>
    /// <param name="claimOwner">The claim owner, or null for unknown.</param>
    /// <returns>The six scope entries, in AllKeys order.</returns>
    let createScope
        (tenant: string | null)
        (sessionId: string | null)
        (turnId: string | null)
        (agentId: string | null)
        (attempt: int)
        (claimOwner: string | null)
        : IReadOnlyList<KeyValuePair<string, obj>> =
        let text (value: string | null) : obj =
            if isNull (box value) then "" :> obj else value :> obj

        ResizeArray<KeyValuePair<string, obj>>(
            [|
                KeyValuePair<string, obj>(SessionIdKey, text sessionId)
                KeyValuePair<string, obj>(TurnIdKey, text turnId)
                KeyValuePair<string, obj>(AgentIdKey, text agentId)
                KeyValuePair<string, obj>(TenantIdKey, text tenant)
                KeyValuePair<string, obj>(AttemptKey, attempt :> obj)
                KeyValuePair<string, obj>(ClaimOwnerKey, text claimOwner)
            |]
        )
        :> IReadOnlyList<KeyValuePair<string, obj>>

    /// Begins the logging scope on the logger: the scope entries above, so
    /// every line logged inside the returned scope carries all six keys.
    /// Scopes contain the sync handler block only, never across awaits (the
    /// Akka-thread disposal risk). A null logger or null scope returns a
    /// no-op disposable instead of failing.
    /// <param name="logger">The logger, or null for no scope.</param>
    /// <param name="scope">The scope entries, or null for an empty scope.</param>
    /// <returns>The scope to dispose at the end of the handler block.</returns>
    let beginScope (logger: ILogger | null) (scope: IReadOnlyList<KeyValuePair<string, obj>> | null) : IDisposable =
        match Option.ofObj logger with
        | None ->
            { new IDisposable with
                member _.Dispose() = ()
            }
        | Some live ->
            let entries =
                if isNull (box scope) then
                    createScope null null null null 0 null
                else
                    scope

            live.BeginScope(entries)

    /// Redacts secret shapes from free text for log lines: the same
    /// table-driven redaction the journal applies, so logs carry no more
    /// than the journal does. Null stays null.
    /// <param name="text">The text to redact, or null.</param>
    /// <returns>The redacted text, or null when the input was null.</returns>
    let redactForLog (text: string) : string = JournalWriter.redactText text
