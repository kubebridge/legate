// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic

// The typed exception family the public API throws. Control-plane
// precondition failures throw these; anything after a turn is accepted is a
// turn outcome returned through TurnResult. Structured context travels on
// properties, never parsed from messages, and messages never embed secrets
// or tool arguments.

/// Raised when a call references a session that does not exist.
/// <param name="sessionId">The id of the session that could not be found.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type SessionNotFoundException(sessionId: SessionId, message: string) =
    inherit LegateException(message)

    /// The id of the session that could not be found.
    member _.SessionId = sessionId

/// Raised when a call targets a session in a state that does not allow it,
/// for example prompting a closed session.
/// <param name="sessionId">The id of the session that was in the unexpected state.</param>
/// <param name="currentState">The state the session was in when the call was made.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type InvalidSessionStateException(sessionId: SessionId, currentState: string, message: string) =
    inherit LegateException(message)

    /// The id of the session that was in the unexpected state.
    member _.SessionId = sessionId

    /// The state the session was in when the call was made.
    member _.CurrentState = currentState

/// Raised when admission control rejects a request, for example because the
/// coordinator is saturated or the host has exhausted its allowance.
/// <param name="reason">The structured reason admission rejected the request.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type AdmissionRejectedException(reason: string, message: string) =
    inherit LegateException(message)

    /// The structured reason admission rejected the request.
    member _.Reason = reason

/// Raised when an LLM provider call fails before a turn outcome can carry the
/// failure, such as rate limiting or an upstream error the host may act on.
/// <param name="providerId">The id of the provider that failed.</param>
/// <param name="status">The HTTP status the provider returned, when the failure has one.</param>
/// <param name="retryAfter">The delay the provider asked callers to respect before retrying, when it advertises one.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type ProviderException(providerId: string, status: Nullable<int>, retryAfter: Nullable<TimeSpan>, message: string) =
    inherit LegateException(message)

    /// The id of the provider that failed.
    member _.ProviderId = providerId

    /// The HTTP status the provider returned, when the failure has one.
    member _.Status = status

    /// The delay the provider asked callers to respect before retrying, when it advertises one.
    member _.RetryAfter = retryAfter

/// Raised when a bounded operation exceeds its deadline, such as a wait on
/// PromptAndWait or a bounded control-plane call.
/// <param name="operationName">The name of the operation that exceeded its deadline.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type DeadlineExceededException(operationName: string, message: string) =
    inherit LegateException(message)

    /// The name of the operation that exceeded its deadline.
    member _.OperationName = operationName

/// Raised when a workspace operation fails, such as binding or tearing down
/// the sandbox a turn runs in.
/// <param name="workspacePath">The workspace path the failing operation bound to, or null when the failure is not tied to a specific path.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type WorkspaceException(workspacePath: string, message: string) =
    inherit LegateException(message)

    /// The workspace path the failing operation bound to, or null when the
    /// failure is not tied to a specific path.
    member _.WorkspacePath = workspacePath

/// Raised when a tool fails at the infrastructure level, such as an MCP
/// server that cannot be started or a tool source that fails to load.
/// <param name="toolName">The name of the tool that failed.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type ToolException(toolName: string, message: string) =
    inherit LegateException(message)

    /// The name of the tool that failed.
    member _.ToolName = toolName

/// Raised when a call resolves a model through a provider that no
/// <see cref="T:Legate.ILlmProviderRegistry" /> has registered. Control-plane
/// precondition failure: nothing was sent to the provider. The message lists
/// the registered provider ids; it never embeds secrets.
/// <param name="providerId">The id of the provider that was not registered.</param>
/// <param name="registeredProviders">The ids of the registered providers.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type ProviderNotRegisteredException(providerId: string, registeredProviders: IReadOnlyList<string>, message: string) =
    inherit LegateException(message)

    /// The id of the provider that was not registered.
    member _.ProviderId = providerId

    /// The ids of the providers the registry knows about.
    member _.RegisteredProviders: IReadOnlyList<string> = registeredProviders

/// Raised when a blob key or prefix fails contract validation, such as a
/// rooted path, a <c>..</c> segment, a backslash, or a NUL character. The
/// offending key travels on a property so hosts can report it without
/// parsing messages.
/// <param name="key">The offending key or prefix exactly as passed.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type InvalidBlobKeyException(key: string, message: string) =
    inherit LegateException(message)

    /// The offending key or prefix exactly as passed.
    member _.Key = key

/// Raised when an event-journal write breaches one of the runtime's
/// configured limits: the per-event byte limit, the per-session event-count
/// limit, the per-session byte limit, or the append batch-size limit. The
/// store reports the breach before any part of the batch lands, so a
/// rejected append never leaves a partial write. The limit values
/// themselves are runtime options the host configures; only the reporting
/// shape is contractual.
/// <param name="limitKind">Which limit was breached: "perEventBytes", "perSessionCount", "perSessionBytes", or "batchSize".</param>
/// <param name="limit">The configured limit that was breached.</param>
/// <param name="observed">The observed size or count that breached the limit.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type EventLimitExceededException(limitKind: string, limit: int64, observed: int64, message: string) =
    inherit LegateException(message)

    /// Which limit was breached: "perEventBytes", "perSessionCount",
    /// "perSessionBytes", or "batchSize". Never contains secrets or tool
    /// arguments.
    member _.LimitKind = limitKind

    /// The configured limit that was breached.
    member _.Limit = limit

    /// The observed size or count that breached the limit.
    member _.Observed = observed

/// Raised when a call references an agent that does not exist, such as
/// upserting a custom tool onto a missing agent.
/// <param name="agentId">The id of the agent that could not be found.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type AgentNotFoundException(agentId: AgentId, message: string) =
    inherit LegateException(message)

    /// The id of the agent that could not be found.
    member _.AgentId = agentId

/// Raised when a write is attempted on an agent store that only implements
/// reads, such as a file-based store that loads agents and custom tools
/// from a directory it never writes to. Read-only implementations are
/// documented and supported for both agent store contracts; every write
/// method throws this instead of failing some other way.
/// <param name="operation">The name of the store operation the store refused.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type ReadOnlyAgentStoreException(operation: string, message: string) =
    inherit LegateException(message)

    /// The name of the store operation the store refused. Never contains
    /// secrets or tool arguments.
    member _.Operation = operation

/// Raised when a package entry path fails the
/// <see cref="T:Legate.AgentPackagePaths" /> normalisation rules, such as a
/// rooted path, a <c>..</c> segment, or an empty result. The offending path
/// travels on a property so hosts can report it without parsing messages.
/// <param name="path">The offending path exactly as passed.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type InvalidPackagePathException(path: string, message: string) =
    inherit LegateException(message)

    /// The offending path exactly as passed, before normalisation.
    member _.Path = path

/// Raised when a <see cref="M:Legate.IAgentPackageLeaseService.WithLease*" />
/// precondition fails: the lease could not be acquired, a renewal was lost
/// or timed out, or the lease was lost during the work. The operation
/// context travels on properties so hosts can report it without parsing
/// messages; it never embeds secrets.
/// <param name="agentId">The agent whose package lease failed.</param>
/// <param name="owner">The owner identity that held or wanted the lease.</param>
/// <param name="operation">The lease operation that failed: "acquire", "renew", or "hold".</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type PackageLeaseException(agentId: AgentId, owner: string, operation: string, message: string) =
    inherit LegateException(message)

    /// The agent whose package lease failed.
    member _.AgentId = agentId

    /// The owner identity that held or wanted the lease. Never contains
    /// secrets.
    member _.Owner = owner

    /// The lease operation that failed: "acquire" when the lease could not
    /// be acquired, "renew" when a renewal was lost or timed out, or
    /// "hold" when the lease was lost during the work. Never contains
    /// secrets or tool arguments.
    member _.Operation = operation
