// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionAddressTests

open System
open System.Text
open Legate
open Xunit

[<Theory>]
[<InlineData("default")>]
[<InlineData("a.b/c+?=&")>]
[<InlineData("租户.été")>]
let ``issue395 canonical addresses roundtrip without tenant inference`` tenant =
    let address = SessionAddress(TenantId.Create tenant, SessionId.New())
    Assert.Equal(Some address, SessionAddress.TryParse address.Key)
    Assert.DoesNotContain("/", address.Key)
    Assert.DoesNotContain("=", address.Key)

[<Fact>]
let ``issue395 equal ids have unequal complete route keys`` () =
    let id = SessionId.New()
    let first = SessionAddress(TenantId.Create "a.b", id)
    let second = SessionAddress(TenantId.Create "a/b", id)
    Assert.NotEqual(first, second)
    Assert.NotEqual<string>(first.Key, second.Key)
    Assert.Equal("2.1.100", SessionSharding.versionStamp 1 100)

[<Fact>]
let ``issue395 old default malformed and noncanonical identities reject`` () =
    let id = SessionId.New()
    let valid = SessionAddress(TenantId.Default, id)

    let rawTenant =
        Convert.ToBase64String(Encoding.UTF8.GetBytes(" default ")).TrimEnd('=')

    for key in
        [|
            id.Value
            ""
            "s2.." + id.Value
            "s2._w." + id.Value
            "s2." + rawTenant + "." + id.Value
            valid.Key.ToLowerInvariant()
            valid.Key.Replace("ZGVmYXVsdA", "ZGVmYXVsdA==")
        |] do
        Assert.Equal(None, SessionAddress.TryParse key)

    Assert.Throws<ArgumentException>(fun () -> SessionAddress(Unchecked.defaultof<TenantId>, id) |> ignore)
    |> ignore

    Assert.Throws<ArgumentException>(fun () ->
        SessionAddress(TenantId.Default, Unchecked.defaultof<SessionId>) |> ignore)
    |> ignore
