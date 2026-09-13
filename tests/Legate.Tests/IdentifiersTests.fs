// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.IdentifiersTests

open System
open System.Text.Json
open FsUnit.Xunit
open Legate
open Xunit

let validA = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let validB = "01D7CB31YQKCJPY9FDTN2WTAFF"
let nullString = Unchecked.defaultof<string>

/// The generic Deserialize<'T> overload is annotated to return 'T | null,
/// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

[<Fact>]
let ``New produces a canonical 26 character uppercase ULID`` () =
    let session = SessionId.New()
    session.ToString().Length |> should equal 26
    session.ToString() |> should equal (session.ToString().ToUpperInvariant())
    session.Value |> should equal (session.ToString())

    let turn = TurnId.New()
    turn.ToString().Length |> should equal 26

    let agent = AgentId.New()
    agent.ToString().Length |> should equal 26

[<Fact>]
let ``New identifiers are distinct`` () =
    SessionId.New() |> should not' (equal (SessionId.New()))
    TurnId.New() |> should not' (equal (TurnId.New()))
    AgentId.New() |> should not' (equal (AgentId.New()))

[<Fact>]
let ``Parse canonicalises lowercase input to uppercase`` () =
    SessionId.Parse(validA.ToLowerInvariant()).Value |> should equal validA
    TurnId.Parse(validA.ToLowerInvariant()).Value |> should equal validA
    AgentId.Parse(validA.ToLowerInvariant()).Value |> should equal validA

[<Fact>]
let ``Parse round-trips through ToString`` () =
    SessionId.Parse(validA.ToLowerInvariant()).ToString() |> should equal validA
    TurnId.Parse(validA.ToLowerInvariant()).ToString() |> should equal validA
    AgentId.Parse(validA).ToString() |> should equal validA

    let fresh = TurnId.New()
    TurnId.Parse(fresh.ToString()) |> should equal fresh

[<Fact>]
let ``SessionId equality and hashing are ordinal`` () =
    let a = SessionId.Parse validA
    let b = SessionId.Parse validA
    let c = SessionId.Parse validB

    a |> should equal b
    a.GetHashCode() |> should equal (b.GetHashCode())
    a.Value |> should equal (b.Value)
    a |> should not' (equal c)
    a.GetHashCode() |> should not' (equal (c.GetHashCode()))

[<Fact>]
let ``TurnId equality and hashing are ordinal`` () =
    let a = TurnId.Parse validA
    let b = TurnId.Parse validA
    let c = TurnId.Parse validB

    a |> should equal b
    a.GetHashCode() |> should equal (b.GetHashCode())
    a |> should not' (equal c)

[<Fact>]
let ``AgentId equality and hashing are ordinal`` () =
    let a = AgentId.Parse validA
    let b = AgentId.Parse validA
    let c = AgentId.Parse validB

    a |> should equal b
    a.GetHashCode() |> should equal (b.GetHashCode())
    a |> should not' (equal c)

[<Fact>]
let ``Different identifier types never compare equal`` () =
    let session = SessionId.Parse validA :> obj
    let turn = TurnId.Parse validA :> obj
    let agent = AgentId.Parse validA :> obj

    session |> should not' (equal turn)
    session |> should not' (equal agent)
    turn |> should not' (equal agent)

[<Fact>]
let ``Parse throws LegateIdentifierException on invalid input`` () =
    (fun () -> SessionId.Parse "not-a-ulid" |> ignore)
    |> should throw typeof<LegateIdentifierException>

    (fun () -> SessionId.Parse "" |> ignore)
    |> should throw typeof<LegateIdentifierException>

    (fun () -> TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FA" |> ignore)
    |> should throw typeof<LegateIdentifierException>

    (fun () -> AgentId.Parse(validA.Replace("0", "o")) |> ignore)
    |> should throw typeof<LegateIdentifierException>

[<Fact>]
let ``Parse throws ArgumentNullException on null input`` () =
    (fun () -> SessionId.Parse nullString |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> TurnId.Parse nullString |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> AgentId.Parse nullString |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``TryParse mirrors the out parameter form`` () =
    let mutable session = Unchecked.defaultof<SessionId>

    SessionId.TryParse(validA, &session) |> should equal true
    session.Value |> should equal validA
    SessionId.TryParse(validA.ToLowerInvariant(), &session) |> should equal true
    session.Value |> should equal validA

    let mutable turn = Unchecked.defaultof<TurnId>

    TurnId.TryParse(validA, &turn) |> should equal true
    turn.Value |> should equal validA

    let mutable agent = Unchecked.defaultof<AgentId>

    AgentId.TryParse(validA, &agent) |> should equal true
    agent.Value |> should equal validA

[<Fact>]
let ``TryParse returns false for invalid input without throwing`` () =
    let mutable session = Unchecked.defaultof<SessionId>

    SessionId.TryParse("not-a-ulid", &session) |> should equal false
    SessionId.TryParse("", &session) |> should equal false
    SessionId.TryParse("   ", &session) |> should equal false
    SessionId.TryParse(nullString, &session) |> should equal false
    SessionId.TryParse("01ARZ3NDEKTSV4RRFFQ69G5FA", &session) |> should equal false

    SessionId.TryParse(validA.ToLowerInvariant().Replace("f", "u"), &session)
    |> should equal false

    let mutable turn = Unchecked.defaultof<TurnId>
    TurnId.TryParse("not-a-ulid", &turn) |> should equal false
    TurnId.TryParse(nullString, &turn) |> should equal false

    let mutable agent = Unchecked.defaultof<AgentId>
    AgentId.TryParse("not-a-ulid", &agent) |> should equal false
    AgentId.TryParse("", &agent) |> should equal false

[<Fact>]
let ``Boxed Equals delegates without recursion`` () =
    let a = SessionId.Parse validA
    let b = SessionId.Parse validA
    let c = SessionId.Parse validB

    (a :> obj).Equals(b :> obj) |> should equal true
    (a :> obj).Equals(c :> obj) |> should equal false
    (a :> obj).Equals(validA :> obj) |> should equal false
    (a :> obj).Equals(null) |> should equal false

    (a :> IEquatable<SessionId>).Equals(c) |> should equal false

    let turn = TurnId.Parse validA
    (turn :> obj).Equals(a :> obj) |> should equal false

[<Fact>]
let ``SessionId serialises and deserialises as a plain string`` () =
    let session = SessionId.Parse validA

    JsonSerializer.Serialize session |> should equal ("\"" + validA + "\"")
    deserialize<SessionId> ("\"" + validA + "\"") |> should equal session

    deserialize<SessionId> ("\"" + validA.ToLowerInvariant() + "\"")
    |> should equal session

[<Fact>]
let ``TurnId serialises and deserialises as a plain string`` () =
    let turn = TurnId.Parse validA

    JsonSerializer.Serialize turn |> should equal ("\"" + validA + "\"")
    deserialize<TurnId> ("\"" + validA + "\"") |> should equal turn

[<Fact>]
let ``AgentId serialises and deserialises as a plain string`` () =
    let agent = AgentId.Parse validA

    JsonSerializer.Serialize agent |> should equal ("\"" + validA + "\"")
    deserialize<AgentId> ("\"" + validA + "\"") |> should equal agent

[<Fact>]
let ``JSON deserialisation rejects invalid and non-string payloads`` () =
    (fun () -> deserialize<SessionId> "\"not-a-ulid\"" |> ignore)
    |> should throw typeof<JsonException>

    (fun () -> deserialize<TurnId> "null" |> ignore)
    |> should throw typeof<JsonException>

    (fun () -> deserialize<AgentId> "12345" |> ignore)
    |> should throw typeof<JsonException>

// ───────────────────────────────────────────────────────────────────────────
// CellId

[<Fact>]
let ``CellId New produces a canonical 26 character uppercase ULID`` () =
    let cell = CellId.New()
    cell.ToString().Length |> should equal 26
    cell.ToString() |> should equal (cell.ToString().ToUpperInvariant())
    cell.Value |> should equal (cell.ToString())

[<Fact>]
let ``CellId New values are distinct`` () =
    CellId.New() |> should not' (equal (CellId.New()))

[<Fact>]
let ``CellId Parse canonicalises lowercase input to uppercase`` () =
    CellId.Parse(validA.ToLowerInvariant()).Value |> should equal validA

[<Fact>]
let ``CellId Parse round-trips through ToString`` () =
    CellId.Parse(validA.ToLowerInvariant()).ToString() |> should equal validA

    let fresh = CellId.New()
    CellId.Parse(fresh.ToString()) |> should equal fresh

[<Fact>]
let ``CellId equality and hashing are ordinal`` () =
    let a = CellId.Parse validA
    let b = CellId.Parse validA
    let c = CellId.Parse validB

    a |> should equal b
    a.GetHashCode() |> should equal (b.GetHashCode())
    a.Value |> should equal (b.Value)
    a |> should not' (equal c)
    a.GetHashCode() |> should not' (equal (c.GetHashCode()))

[<Fact>]
let ``CellId never compares equal to other identifier types`` () =
    let session = SessionId.Parse validA :> obj
    let turn = TurnId.Parse validA :> obj
    let agent = AgentId.Parse validA :> obj
    let cell = CellId.Parse validA :> obj

    cell |> should not' (equal session)
    cell |> should not' (equal turn)
    cell |> should not' (equal agent)

[<Fact>]
let ``CellId Parse throws LegateIdentifierException on invalid input`` () =
    (fun () -> CellId.Parse "not-a-ulid" |> ignore)
    |> should throw typeof<LegateIdentifierException>

    (fun () -> CellId.Parse "" |> ignore)
    |> should throw typeof<LegateIdentifierException>

    (fun () -> CellId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FA" |> ignore)
    |> should throw typeof<LegateIdentifierException>

[<Fact>]
let ``CellId Parse throws ArgumentNullException on null input`` () =
    (fun () -> CellId.Parse nullString |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``CellId TryParse mirrors the out parameter form`` () =
    let mutable cell = Unchecked.defaultof<CellId>

    CellId.TryParse(validA, &cell) |> should equal true
    cell.Value |> should equal validA
    CellId.TryParse(validA.ToLowerInvariant(), &cell) |> should equal true
    cell.Value |> should equal validA

[<Fact>]
let ``CellId TryParse returns false for invalid input without throwing`` () =
    let mutable cell = Unchecked.defaultof<CellId>

    CellId.TryParse("not-a-ulid", &cell) |> should equal false
    CellId.TryParse("", &cell) |> should equal false
    CellId.TryParse("   ", &cell) |> should equal false
    CellId.TryParse(nullString, &cell) |> should equal false
    CellId.TryParse("01ARZ3NDEKTSV4RRFFQ69G5FA", &cell) |> should equal false

[<Fact>]
let ``CellId default is the unstamped state and stays equal to itself`` () =
    // The store stamps the id on persist, so an unstamped cell carries the
    // default struct. Its null string must be safe to hash, compare, and
    // round-trip until then.
    let unstamped = Unchecked.defaultof<CellId>

    unstamped.Value |> should equal null
    unstamped.GetHashCode() |> should equal 0
    unstamped |> should equal (Unchecked.defaultof<CellId>)
    (unstamped :> obj).Equals(null) |> should equal false
    unstamped.ToString() |> should equal null

[<Fact>]
let ``CellId serialises and deserialises as a plain string`` () =
    let cell = CellId.Parse validA

    JsonSerializer.Serialize cell |> should equal ("\"" + validA + "\"")
    deserialize<CellId> ("\"" + validA + "\"") |> should equal cell

    deserialize<CellId> ("\"" + validA.ToLowerInvariant() + "\"")
    |> should equal cell

[<Fact>]
let ``CellId JSON deserialisation rejects invalid payloads and accepts the unstamped null`` () =
    (fun () -> deserialize<CellId> "\"not-a-ulid\"" |> ignore)
    |> should throw typeof<JsonException>

    (fun () -> deserialize<CellId> "12345" |> ignore)
    |> should throw typeof<JsonException>

    // Null is the unstamped id: the store stamps the cell id on persist,
    // so a derived cell serialises null until then (mirroring an
    // in-flight event's empty Sequence).
    deserialize<CellId> "null" |> should equal (Unchecked.defaultof<CellId>)
