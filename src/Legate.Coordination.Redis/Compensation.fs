// SPDX-License-Identifier: Apache-2.0
namespace Legate.Coordination.Redis

open System
open System.Collections.Concurrent

// Unknown-outcome compensation: a script call whose reply is lost after a
// connection failure must be compensated exactly once, never double-applied
// and never silently dropped. The ledger keys on the caller-supplied
// operation id: the first resolution probes once and compensates at most
// once, later resolutions for the same id report AlreadyResolved without
// touching Redis. Internal: the client resolves through this; the unit
// tests drive it with fakes.

/// <summary>
/// What one unknown-outcome resolution did. Internal.
/// </summary>
type internal UnknownOutcomeResolution =

    /// <summary>
    /// The probe found the effect and the compensation ran once.
    /// </summary>
    | Compensated = 0

    /// <summary>
    /// The probe found no effect: nothing to compensate.
    /// </summary>
    | ConfirmedAbsent = 1

    /// <summary>
    /// The operation id already resolved: the probe and the compensation
    /// did not run again.
    /// </summary>
    | AlreadyResolved = 2

/// <summary>
/// Exactly-once ledger for unknown script outcomes. Internal: one instance
/// per client.
/// </summary>
type internal CompensationLedger() =

    let resolved = ConcurrentDictionary<string, UnknownOutcomeResolution>()

    /// <summary>
    /// Resolves one unknown outcome exactly once: runs the probe, runs the
    /// compensation only when the probe found the effect, and records the
    /// operation id so repeats never re-apply.
    /// </summary>
    /// <param name="operationId">The idempotent operation id. Must be non-empty.</param>
    /// <param name="probe">Returns true when the lost script's effect landed.</param>
    /// <param name="compensate">Removes the landed effect. Runs at most once per operation id.</param>
    /// <returns>What the resolution did.</returns>
    member _.Resolve(operationId: string, probe: Func<bool>, compensate: Action) : UnknownOutcomeResolution =
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId)
        ArgumentNullException.ThrowIfNull(probe)
        ArgumentNullException.ThrowIfNull(compensate)

        // Winner elects itself before probing: a concurrent loser sees the
        // placeholder and returns without probing or compensating twice.
        if not (resolved.TryAdd(operationId, UnknownOutcomeResolution.AlreadyResolved)) then
            UnknownOutcomeResolution.AlreadyResolved
        else
            let landed =
                try
                    probe.Invoke()
                with _ ->
                    // A failed probe cannot prove absence: compensate rather
                    // than risk silently dropping a landed lease.
                    true

            let resolution =
                if landed then
                    try
                        compensate.Invoke()
                    with _ ->
                        ()

                    UnknownOutcomeResolution.Compensated
                else
                    UnknownOutcomeResolution.ConfirmedAbsent

            resolved.[operationId] <- resolution
            resolution
