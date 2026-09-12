// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TenancyTests

open System
open FsUnit.Xunit
open Legate
open Xunit

[<Fact>]
let ``Default tenant is stable and equal to itself`` () =
    TenantId.Default |> should equal TenantId.Default
    TenantId.Default.Value |> should equal "default"

[<Fact>]
let ``Create trims and compares ordinally`` () =
    let a = TenantId.Create "  acme "
    let b = TenantId.Create "acme"
    a |> should equal b
    a.GetHashCode() |> should equal (b.GetHashCode())
    TenantId.Create "Acme" |> should not' (equal b)

[<Fact>]
let ``Create rejects blank values`` () =
    (fun () -> TenantId.Create "   " |> ignore)
    |> should throw typeof<ArgumentException>
