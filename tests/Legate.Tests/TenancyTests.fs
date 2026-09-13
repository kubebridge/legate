// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TenancyTests

open System
open System.Text.Json
open FsUnit.Xunit
open Legate
open Xunit

/// The generic Deserialize<'T> overload is annotated to return 'T | null,
/// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

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

[<Fact>]
let ``TenantId serialises and deserialises as a plain string`` () =
    let tenant = TenantId.Create "acme"

    JsonSerializer.Serialize tenant |> should equal "\"acme\""

    deserialize<TenantId> "\"acme\"" |> should equal tenant

[<Fact>]
let ``TenantId deserialisation trims and canonicalises`` () =
    deserialize<TenantId> "\"  acme  \"" |> should equal (TenantId.Create "acme")

[<Fact>]
let ``TenantId deserialisation rejects malformed payloads`` () =
    (fun () -> deserialize<TenantId> "null" |> ignore)
    |> should throw typeof<JsonException>

    (fun () -> deserialize<TenantId> "42" |> ignore)
    |> should throw typeof<JsonException>

    (fun () -> deserialize<TenantId> "{}" |> ignore)
    |> should throw typeof<JsonException>

[<Fact>]
let ``TenantId deserialisation rejects blank and whitespace-only strings`` () =
    (fun () -> deserialize<TenantId> "\"\"" |> ignore)
    |> should throw typeof<JsonException>

    (fun () -> deserialize<TenantId> "\"   \"" |> ignore)
    |> should throw typeof<JsonException>

[<Fact>]
let ``TenantId serialises inside a record without extra shape`` () =
    let payload =
        {|
            tenant = TenantId.Create "acme"
            name = "checkout"
        |}

    let document = JsonSerializer.Serialize payload |> JsonDocument.Parse

    document.RootElement.GetProperty("tenant").GetString() |> should equal "acme"

    document.RootElement.GetProperty("name").GetString() |> should equal "checkout"
