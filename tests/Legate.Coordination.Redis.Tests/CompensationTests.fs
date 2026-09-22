// SPDX-License-Identifier: Apache-2.0
module Legate.Coordination.Redis.Tests.CompensationTests

open System
open FsUnit.Xunit
open Legate.Coordination.Redis
open Xunit

[<Fact>]
let ``Unknown outcome with a landed effect compensates once`` () =
    let ledger = CompensationLedger()
    let mutable compensations = 0
    let probe = Func<bool>(fun () -> true)
    let compensate = Action(fun () -> compensations <- compensations + 1)

    let first = ledger.Resolve("op-1", probe, compensate)
    let second = ledger.Resolve("op-1", probe, compensate)

    first |> should equal UnknownOutcomeResolution.Compensated
    second |> should equal UnknownOutcomeResolution.AlreadyResolved
    compensations |> should equal 1

[<Fact>]
let ``Unknown outcome with no effect never compensates`` () =
    let ledger = CompensationLedger()
    let mutable compensations = 0
    let probe = Func<bool>(fun () -> false)
    let compensate = Action(fun () -> compensations <- compensations + 1)

    let resolution = ledger.Resolve("op-absent", probe, compensate)

    resolution |> should equal UnknownOutcomeResolution.ConfirmedAbsent
    compensations |> should equal 0

[<Fact>]
let ``A failed probe compensates rather than dropping silently`` () =
    let ledger = CompensationLedger()
    let mutable compensations = 0

    let probe = Func<bool>(fun () -> raise (InvalidOperationException("probe failed")))

    let compensate = Action(fun () -> compensations <- compensations + 1)

    let resolution = ledger.Resolve("op-probe-fails", probe, compensate)

    resolution |> should equal UnknownOutcomeResolution.Compensated
    compensations |> should equal 1

[<Fact>]
let ``Distinct operation ids resolve independently`` () =
    let ledger = CompensationLedger()
    let mutable compensations = 0
    let probe = Func<bool>(fun () -> true)
    let compensate = Action(fun () -> compensations <- compensations + 1)

    ledger.Resolve("op-a", probe, compensate) |> ignore
    ledger.Resolve("op-b", probe, compensate) |> ignore

    compensations |> should equal 2

[<Fact>]
let ``Outcome parsing maps fakes without Redis`` () =
    AdmissionParsing.toOutcome "acquired" 0
    |> fun outcome -> outcome.Kind |> should equal DistributedAdmissionDecision.Acquired

    AdmissionParsing.toOutcome "queued" 2
    |> fun outcome -> outcome.Kind |> should equal DistributedAdmissionDecision.Queued

    AdmissionParsing.toOutcome "queued" 2
    |> fun outcome -> outcome.QueuePosition |> should equal 2

    AdmissionParsing.toOutcome "cooldown" 0
    |> fun outcome -> outcome.Kind |> should equal DistributedAdmissionDecision.CooldownActive

    AdmissionParsing.toOutcome "unknown-status" 0
    |> fun outcome -> outcome.Kind |> should equal DistributedAdmissionDecision.CooldownActive
