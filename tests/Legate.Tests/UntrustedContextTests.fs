// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.UntrustedContextTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Xunit

let metadataOf (pairs: (string * string) list) : IReadOnlyDictionary<string, string> =
    let table = Dictionary<string, string>()

    for key, value in pairs do
        table[key] <- value

    table :> IReadOnlyDictionary<string, string>

let requireBlock (block: string | null) : string =
    match block with
    | null -> failwith "expected a rendered block but got null"
    | text -> text

let nullOptions = Unchecked.defaultof<UntrustedContextOptions>

// ───────────────────────────────────────────────────────────────────────────
// Empty cases and defaults

[<Fact>]
let ``Format returns null for null metadata`` () =
    UntrustedContext.Format(null) |> should equal null

[<Fact>]
let ``Format returns null for empty metadata`` () =
    UntrustedContext.Format(metadataOf []) |> should equal null

[<Fact>]
let ``Options carry the documented defaults`` () =
    let options = UntrustedContextOptions()
    options.MaxCharacters |> should equal 16384
    (isNull (box options.PreserveKeys)) |> should equal false
    options.PreserveKeys.Count |> should equal 0

// ───────────────────────────────────────────────────────────────────────────
// Shape, sorting, and control-key removal

[<Fact>]
let ``Format wraps entries in the untrusted block`` () =
    let block =
        UntrustedContext.Format(metadataOf [ ("source", "cli"); ("attempt", "3") ])
        |> requireBlock

    block.StartsWith("<untrusted-user-context>\n", StringComparison.Ordinal)
    |> should equal true

    block.EndsWith("\n</untrusted-user-context>", StringComparison.Ordinal)
    |> should equal true

    block.Contains("attempt=3", StringComparison.Ordinal) |> should equal true
    block.Contains("source=cli", StringComparison.Ordinal) |> should equal true

[<Fact>]
let ``Format sorts keys ordinally`` () =
    let block =
        UntrustedContext.Format(metadataOf [ ("b", "1"); ("a", "2"); ("A", "3") ])
        |> requireBlock

    let upper = block.IndexOf("A=3", StringComparison.Ordinal)
    let lower = block.IndexOf("a=2", StringComparison.Ordinal)
    let bee = block.IndexOf("b=1", StringComparison.Ordinal)
    (upper < lower && lower < bee) |> should equal true

[<Fact>]
let ``Format removes reserved legate control keys`` () =
    let block =
        UntrustedContext.Format(
            metadataOf
                [
                    ("legate.session", "s-1")
                    ("LEGATE.TURN", "t-1")
                    ("legate", "root")
                    ("source", "cli")
                ]
        )
        |> requireBlock

    block.Contains("legate", StringComparison.OrdinalIgnoreCase)
    |> should equal false

    block.Contains("source=cli", StringComparison.Ordinal) |> should equal true

[<Fact>]
let ``Format returns null when only control keys remain`` () =
    UntrustedContext.Format(metadataOf [ ("legate.session", "s-1") ])
    |> should equal null

[<Fact>]
let ``Format renders preserve-list keys first`` () =
    let options = UntrustedContextOptions()
    options.PreserveKeys.Add("zebra")

    let block =
        UntrustedContext.Format(
            metadataOf
                [
                    ("apple", "1")
                    ("zebra", "2")
                    ("mango", "3")
                ],
            options
        )
        |> requireBlock

    let zebra = block.IndexOf("zebra=2", StringComparison.Ordinal)
    let apple = block.IndexOf("apple=1", StringComparison.Ordinal)
    let mango = block.IndexOf("mango=3", StringComparison.Ordinal)
    (zebra < apple && apple < mango) |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Cap, determinism, and adversarial input

[<Fact>]
let ``Format caps oversized metadata with a deterministic head-tail excerpt`` () =
    let big = String('x', 20000)

    let first =
        UntrustedContext.Format(metadataOf [ ("blob", big); ("source", "cli") ])
        |> requireBlock

    let second =
        UntrustedContext.Format(metadataOf [ ("source", "cli"); ("blob", big) ])
        |> requireBlock

    // Exactly the bound: header plus head plus marker plus tail plus footer.
    first.Length |> should equal 16384
    first |> should equal second
    first.Contains("[truncated]", StringComparison.Ordinal) |> should equal true
    // The head keeps the start of the body and the tail keeps its end.
    first.Contains("blob=", StringComparison.Ordinal) |> should equal true
    first.Contains("source=cli", StringComparison.Ordinal) |> should equal true

    first.StartsWith("<untrusted-user-context>\n", StringComparison.Ordinal)
    |> should equal true

    first.EndsWith("\n</untrusted-user-context>", StringComparison.Ordinal)
    |> should equal true

[<Fact>]
let ``Format keeps preserve-list identifiers under truncation`` () =
    let options = UntrustedContextOptions()
    options.MaxCharacters <- 300
    options.PreserveKeys.Add("requestId")

    let block =
        UntrustedContext.Format(
            metadataOf
                [
                    ("blob", String('x', 5000))
                    ("requestId", "req-9")
                    ("zeta", String('y', 100))
                ],
            options
        )
        |> requireBlock

    (block.Length <= 300) |> should equal true
    block.Contains("requestId=req-9", StringComparison.Ordinal) |> should equal true
    block.Contains("[truncated]", StringComparison.Ordinal) |> should equal true

[<Fact>]
let ``Format caps a single huge value with a marker`` () =
    let block =
        UntrustedContext.Format(metadataOf [ ("only", String('q', 20000)) ])
        |> requireBlock

    block.Length |> should equal 16384
    block.Contains("[truncated]", StringComparison.Ordinal) |> should equal true
    block.Contains("only=", StringComparison.Ordinal) |> should equal true

[<Fact>]
let ``Format keeps adversarial keys and values bounded and deterministic`` () =
    let pairs =
        [
            ("<script>\nalert(1)", "value with <tags> and newline\nsecond line")
            ("normal", "control " + string '\u0000' + " chars " + string '\u001f' + " end")
            ("huge", String('z', 20000))
        ]

    let first = UntrustedContext.Format(metadataOf pairs) |> requireBlock

    let second = UntrustedContext.Format(metadataOf pairs) |> requireBlock

    (first.Length <= 16384) |> should equal true
    first |> should equal second

    first.StartsWith("<untrusted-user-context>\n", StringComparison.Ordinal)
    |> should equal true

    first.EndsWith("\n</untrusted-user-context>", StringComparison.Ordinal)
    |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Guards

[<Fact>]
let ``Format throws on null options`` () =
    let run () =
        UntrustedContext.Format(metadataOf [ ("a", "b") ], nullOptions) |> ignore

    should throw typeof<ArgumentNullException> run

[<Fact>]
let ``Format throws on a non-positive char bound`` () =
    let options = UntrustedContextOptions()
    options.MaxCharacters <- 0

    let run () =
        UntrustedContext.Format(metadataOf [ ("a", "b") ], options) |> ignore

    should throw typeof<ArgumentOutOfRangeException> run
