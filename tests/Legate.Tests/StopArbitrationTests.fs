// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.StopArbitrationTests

open System
open FsUnit.Xunit
open Legate
open Xunit

// Typed stop-cause arbitration (issue 35): exactly one of settlement and
// stop wins per turn, and the winner is the only side that produces
// effects. The cell is pure, so these tests prove the first-wins order both
// ways plus the cause mapping without an actor or a store.

// ───────────────────────────────────────────────────────────────────────────
// First-wins arbitration

[<Fact>]
let ``Stop arriving first wins and a later settlement is stale`` () =
    let decided, won =
        StopArbitration.applyStop StopArbitration.Undecided StopCause.ExplicitAbort

    won |> should equal true

    let after, settledWon = StopArbitration.applySettlement decided
    settledWon |> should equal false
    after |> should equal decided

[<Fact>]
let ``Settlement arriving first wins and a later stop is stale`` () =
    let decided, won = StopArbitration.applySettlement StopArbitration.Undecided
    won |> should equal true

    let after, stopWon = StopArbitration.applyStop decided StopCause.ExplicitAbort

    stopWon |> should equal false
    after |> should equal decided

[<Fact>]
let ``A second abort keeps the first cause`` () =
    let decided, firstWon =
        StopArbitration.applyStop StopArbitration.Undecided StopCause.ExplicitAbort

    firstWon |> should equal true

    let after, secondWon = StopArbitration.applyStop decided StopCause.HostShutdown

    secondWon |> should equal false
    after |> should equal decided

    match after with
    | StopArbitration.Decided(StopArbitration.StopWins cause) -> cause |> should equal StopCause.ExplicitAbort
    | _ -> failwith "Expected the first stop cause to stand."

[<Fact>]
let ``A second settlement is stale`` () =
    let decided, firstWon = StopArbitration.applySettlement StopArbitration.Undecided
    firstWon |> should equal true

    let after, secondWon = StopArbitration.applySettlement decided
    secondWon |> should equal false
    after |> should equal decided

[<Fact>]
let ``The winner records which side landed first`` () =
    let stopped, _ =
        StopArbitration.applyStop StopArbitration.Undecided StopCause.HostShutdown

    match stopped with
    | StopArbitration.Decided(StopArbitration.StopWins cause) -> cause |> should equal StopCause.HostShutdown
    | _ -> failwith "Expected a stop winner carrying its cause."

    let settled, _ = StopArbitration.applySettlement StopArbitration.Undecided

    match settled with
    | StopArbitration.Decided StopArbitration.SettlementWins -> ()
    | _ -> failwith "Expected a settlement winner."

// ───────────────────────────────────────────────────────────────────────────
// Cause mapping

[<Fact>]
let ``Explicit abort settles Aborted with a TurnAborted carrying who and why`` () =
    match StopArbitration.settlementFor StopCause.ExplicitAbort "host stop" with
    | Some(status, outcome) ->
        status |> should equal TurnStatus.Aborted

        match outcome with
        | :? TurnAborted as aborted ->
            aborted.Cause |> should equal StopCause.ExplicitAbort
            aborted.Reason |> should equal "host stop"
        | _ -> failwith "Expected a TurnAborted outcome."
    | None -> failwith "Expected the abort to settle."

[<Fact>]
let ``Host shutdown settles Aborted like an explicit abort`` () =
    match StopArbitration.settlementFor StopCause.HostShutdown "host draining" with
    | Some(status, outcome) ->
        status |> should equal TurnStatus.Aborted

        match outcome with
        | :? TurnAborted as aborted ->
            aborted.Cause |> should equal StopCause.HostShutdown
            aborted.Reason |> should equal "host draining"
        | _ -> failwith "Expected a TurnAborted outcome."
    | None -> failwith "Expected the shutdown to settle."

[<Fact>]
let ``Deadline settles Failed with a TurnFailed carrying the reason`` () =
    match StopArbitration.settlementFor StopCause.Deadline TurnLoop.TimeoutExceededMessage with
    | Some(status, outcome) ->
        status |> should equal TurnStatus.Failed

        match outcome with
        | :? TurnFailed as failed -> failed.Reason |> should equal TurnLoop.TimeoutExceededMessage
        | _ -> failwith "Expected a TurnFailed outcome."
    | None -> failwith "Expected the deadline to settle."

[<Fact>]
let ``Lease loss settles nothing: the loser propagates with zero effects`` () =
    StopArbitration.settlementFor StopCause.LeaseLoss "taken over"
    |> should equal None

[<Fact>]
let ``An unknown cause is rejected instead of mapped`` () =
    (fun () -> StopArbitration.settlementFor (enum<StopCause> 99) "bogus" |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

[<Fact>]
let ``A null reason is rejected`` () =
    let nullReason = Unchecked.defaultof<string>

    (fun () -> StopArbitration.settlementFor StopCause.ExplicitAbort nullReason |> ignore)
    |> should throw typeof<ArgumentNullException>
