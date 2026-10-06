// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System

/// How an accepted operation entered the session inbox: the durable
/// correlation the receipt carries. Queue appends and acts on the message
/// once the running turn finishes; Inject folds into the running turn at
/// its next iteration boundary without starting an independent turn;
/// Interrupt pre-empts the running turn and starts a new one; Reply
/// resumes the suspended real turn and never starts one.
type OperationKind =
    /// Queued delivery: acted on once the running turn finishes, or starts a turn when idle.
    | Queue = 0
    /// Injected delivery: folded into the running turn, never an independent turn.
    | Inject = 1
    /// Interrupt delivery: pre-empts the running turn and starts a new turn.
    | Interrupt = 2
    /// A host reply resuming the suspended turn, never starting one.
    | Reply = 3

/// A durable receipt for one accepted session operation: the stable
/// operation-specific identity the caller uses to observe the operation
/// after reconnection, session reload, and current-format process restart.
/// The identity is the accepted inbox entry's immutable per-session
/// position plus its durable turn identity (stamped at accept for user
/// messages; the default sentinel for reply entries and legacy rows).
/// Distinct accepted inputs never share a position, so concurrent receipts
/// are distinguishable even when they share no turn. Acceptance is not
/// execution: it promises neither that execution has started nor that it
/// succeeded. Possession grants neither access nor turn ownership; reads
/// enforce tenant isolation and current host authorization.
/// <param name="sessionId">The session the operation was accepted into.</param>
/// <param name="position">The immutable per-session inbox position.</param>
/// <param name="operationId">The durable turn identity stamped at accept, or the default sentinel for replies and legacy rows.</param>
/// <param name="kind">How the operation entered the inbox.</param>
/// <param name="acceptedAt">When the entry was appended.</param>
[<Sealed>]
type AcceptedOperation
    (sessionId: SessionId, position: int64, operationId: TurnId, kind: OperationKind, acceptedAt: DateTimeOffset) =

    /// The session the operation was accepted into.
    member _.SessionId = sessionId

    /// The immutable per-session inbox position of the accepted entry.
    member _.Position = position

    /// The durable turn identity stamped at accept for user messages, or the
    /// default sentinel for reply entries and legacy rows. Correlation is
    /// evidence, never claim authority.
    member _.OperationId = operationId

    /// How the operation entered the inbox.
    member _.Kind = kind

    /// When the entry was appended.
    member _.AcceptedAt = acceptedAt

/// The durable observation status of one accepted operation behind
/// <c>GetOperationResultAsync</c>: pending, terminal, unknown, or
/// unavailable. Pending means accepted but with no committed winning
/// result yet; Terminal means the committed winning result is present;
/// Unknown means the session, entry, or association is missing or
/// mismatched in this tenant; Unavailable means storage failed. Unknown
/// and Unavailable never carry a terminal result and never become a
/// fabricated success or another operation's result.
type OperationStatus =
    /// Accepted with no committed winning result yet.
    | Pending = 0
    /// The committed winning terminal result is present.
    | Terminal = 1
    /// The session, entry, or association is missing or mismatched in this tenant.
    | Unknown = 2
    /// Storage failed; the observation is unavailable, never terminal.
    | Unavailable = 3

/// The durable observation of one accepted operation: its status, the
/// associated real turn when known, and the committed winning terminal
/// result when terminal. The winning result is the single authoritative
/// <c>execution_settlements</c> row for (tenant, session, position);
/// stale or losing completion reports never replace it. Sinkless sessions
/// are observable the same way; completion delivery or outbox cleanup
/// does not destroy this record inside the documented retention window.
/// Retention is bounded, never permanent, and incomplete journal history
/// never reconstructs an outcome.
/// <param name="sessionId">The session the operation was accepted into.</param>
/// <param name="position">The immutable per-session inbox position.</param>
/// <param name="kind">How the operation entered the inbox.</param>
/// <param name="status">The observation status.</param>
/// <param name="turnId">The associated real turn when known, or empty while unknown.</param>
/// <param name="result">The committed winning terminal result, or null unless terminal.</param>
[<Sealed>]
type OperationResult
    (
        sessionId: SessionId,
        position: int64,
        kind: OperationKind,
        status: OperationStatus,
        turnId: Nullable<TurnId>,
        result: TurnResult | null
    ) =

    /// The session the operation was accepted into.
    member _.SessionId = sessionId

    /// The immutable per-session inbox position of the accepted entry.
    member _.Position = position

    /// How the operation entered the inbox.
    member _.Kind = kind

    /// The observation status.
    member _.Status = status

    /// The associated real turn when known, or empty while unknown. Reply
    /// entries associate with the suspended turn they resume when known.
    member _.TurnId = turnId

    /// The committed winning terminal result, or null unless the status is Terminal.
    member _.Result: TurnResult | null = result
