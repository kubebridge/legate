// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System

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
