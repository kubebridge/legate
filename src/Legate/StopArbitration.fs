// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System

// Typed stop-cause arbitration (issue 35): exactly one cause wins between a
// turn settlement and a stop request, and the winner is the only side that
// produces effects. The cell is pure (no I/O, no clock): the session actor
// holds one per running turn and applies arrivals in mailbox order, so the
// actor's single-threaded sequencing is the first-wins order and the
// store's fenced SettleTurn/AbortTurn is the durable second line. Lease
// loss never settles: the loser propagates with zero further effects, so
// the cause mapping returns None for it. Rejected: reusing Close for abort
// (turn control versus session lifecycle stay separate), a real-clock
// deadline (the TurnLoop deadline fires off the injected ILlmDelay seam),
// and a string-only reason (the StopCause enum is the branchable cause).
module internal StopArbitration =

    /// Which side won the race between a turn settlement and a stop
    /// request. Exactly one wins; the loser produces zero effects.
    type ArbitrationWinner =

        /// The turn's completion landed first: the carried result stands and
        /// a later stop is a no-op.
        | SettlementWins

        /// The stop landed first: the turn settles under the carried cause
        /// and a later completion is stale.
        | StopWins of cause: StopCause

    /// Whether the turn's terminal write is still claimable. One cell per
    /// running turn: Undecided while the turn runs, Decided once either side
    /// lands first.
    type ArbitrationState =

        /// The turn is still running: the next arrival wins.
        | Undecided

        /// Either side already landed: further arrivals are stale no-ops.
        | Decided of winner: ArbitrationWinner

    /// Applies a settlement arrival: the first arrival wins and later ones
    /// are stale.
    /// <param name="state">The arbitration cell.</param>
    /// <returns>The cell after the arrival and whether this arrival won.</returns>
    let applySettlement (state: ArbitrationState) : ArbitrationState * bool =
        match state with
        | Undecided -> Decided SettlementWins, true
        | Decided _ -> state, false

    /// Applies a stop arrival: the first arrival wins and later ones
    /// (including a second abort) are stale, so the first cause stands.
    /// <param name="state">The arbitration cell.</param>
    /// <param name="cause">The stop cause arriving.</param>
    /// <returns>The cell after the arrival and whether this arrival won.</returns>
    let applyStop (state: ArbitrationState) (cause: StopCause) : ArbitrationState * bool =
        match state with
        | Undecided -> Decided(StopWins cause), true
        | Decided _ -> state, false

    /// Maps a settlement-producing stop cause to the terminal status and
    /// outcome the turn settles under. Abort-family causes settle Aborted
    /// with a TurnAborted carrying who and why; the deadline settles Failed
    /// with a TurnFailed carrying the timeout reason. Lease loss settles
    /// nothing (None): the loser propagates TurnLeaseLostException with
    /// zero further effects instead of writing a terminal state.
    /// <param name="cause">The stop cause that won.</param>
    /// <param name="reason">Why the turn stopped. Must not be null. Never contains secrets or tool arguments.</param>
    /// <returns>The terminal status and outcome, or None when the cause settles nothing.</returns>
    let settlementFor (cause: StopCause) (reason: string) : (TurnStatus * TurnOutcome) option =
        if isNull (box reason) then
            raise (ArgumentNullException(nameof reason))

        match cause with
        | StopCause.ExplicitAbort
        | StopCause.HostShutdown -> Some(TurnStatus.Aborted, TurnAborted(cause, reason) :> TurnOutcome)
        | StopCause.Deadline -> Some(TurnStatus.Failed, TurnFailed(reason) :> TurnOutcome)
        | StopCause.LeaseLoss -> None
        | unknown ->
            raise (
                ArgumentOutOfRangeException(
                    nameof cause,
                    sprintf
                        "Unknown stop cause: %O. Expected ExplicitAbort, Deadline, LeaseLoss, or HostShutdown."
                        unknown
                )
            )
