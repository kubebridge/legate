// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.CustomToolNamesTests

open FsUnit.Xunit
open Legate
open Xunit

// Tests for custom tool name sanitising plus first-claimant-wins dedupe
// (issue 74, task 1): the pure naming rules the custom tool source loads
// through, covering the valid passthrough, invalid-character, overlong,
// and empty vectors plus dedupe with collision reports.

// ───────────────────────────────────────────────────────────────────────────
// sanitize

[<Fact>]
let ``Valid names pass through unchanged`` () =
    CustomToolNames.sanitize "lookup_order-2"
    |> should equal (Some "lookup_order-2")

    CustomToolNames.sanitize "a" |> should equal (Some "a")
    CustomToolNames.sanitize "ABCxyz019_-" |> should equal (Some "ABCxyz019_-")

[<Fact>]
let ``Invalid characters become underscores`` () =
    CustomToolNames.sanitize "my tool!" |> should equal (Some "my_tool_")
    CustomToolNames.sanitize "a.b/c:d" |> should equal (Some "a_b_c_d")

[<Fact>]
let ``Overlong names truncate to 128 characters`` () =
    let long = String.replicate 200 "a"

    match CustomToolNames.sanitize long with
    | None -> failwith "Expected the overlong name to truncate, not drop."
    | Some sanitized ->
        sanitized.Length |> should equal 128
        sanitized |> should equal (String.replicate 128 "a")

[<Fact>]
let ``Null and empty names drop out`` () =
    CustomToolNames.sanitize null |> should equal None
    CustomToolNames.sanitize "" |> should equal None

[<Fact>]
let ``Names that only truncate still validate`` () =
    // 129 valid characters truncate to a valid 128-character name.
    let long = String.replicate 129 "b"

    match CustomToolNames.sanitize long with
    | None -> failwith "Expected the truncated name to validate."
    | Some sanitized ->
        ToolNameRules.TryValidate sanitized |> should equal true
        sanitized.Length |> should equal 128

// ───────────────────────────────────────────────────────────────────────────
// claimNames

[<Fact>]
let ``Distinct names all claim`` () =
    let (claimed, unusable) =
        CustomToolNames.claimNames Set.empty [ Some "alpha", 1; Some "beta", 2 ]

    unusable |> should be Empty
    claimed.Length |> should equal 2
    claimed[0].Sanitized |> should equal "alpha"
    claimed[0].Fate |> should equal Claimed
    claimed[0].Payload |> should equal 1
    claimed[1].Sanitized |> should equal "beta"
    claimed[1].Fate |> should equal Claimed

[<Fact>]
let ``Renames claim but report the original`` () =
    let (claimed, unusable) = CustomToolNames.claimNames Set.empty [ Some "my tool", 1 ]

    unusable |> should be Empty
    claimed.Length |> should equal 1
    claimed[0].Sanitized |> should equal "my_tool"
    claimed[0].Fate |> should equal ClaimedRenamed
    claimed[0].Original |> should equal "my tool"

[<Fact>]
let ``First claimant wins duplicates and the loser names the winner`` () =
    let (claimed, unusable) =
        CustomToolNames.claimNames Set.empty [ Some "my tool", 1; Some "my_tool", 2 ]

    unusable |> should be Empty
    claimed.Length |> should equal 2

    claimed[0].Fate |> should equal ClaimedRenamed
    claimed[0].Payload |> should equal 1

    match claimed[1].Fate with
    | DuplicateLoser kept ->
        kept |> should equal "my tool"
        claimed[1].Sanitized |> should equal "my_tool"
        claimed[1].Payload |> should equal 2
    | fate -> failwith $"Expected a duplicate loser, got %A{fate}."

[<Fact>]
let ``Reserved built-in names lose to the built-in`` () =
    let (claimed, unusable) =
        CustomToolNames.claimNames (Set.ofList [ "exec" ]) [ Some "exec", 1 ]

    unusable |> should be Empty
    claimed.Length |> should equal 1
    claimed[0].Fate |> should equal ReservedLoser
    claimed[0].Sanitized |> should equal "exec"

[<Fact>]
let ``Null and empty originals are reported unusable`` () =
    let (claimed, unusable) =
        CustomToolNames.claimNames Set.empty [ None, 1; Some "", 2 ]

    claimed |> should be Empty
    unusable |> should equal [ "<null>"; "" ]

[<Fact>]
let ``Dedupe is ordinal: casing does not collide`` () =
    let (claimed, unusable) =
        CustomToolNames.claimNames Set.empty [ Some "Exec", 1; Some "exec", 2 ]

    unusable |> should be Empty
    claimed.Length |> should equal 2

    claimed
    |> List.forall (fun winner -> winner.Fate = Claimed)
    |> should equal true
