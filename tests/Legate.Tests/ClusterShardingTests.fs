// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ClusterShardingTests

open System
open System.Threading
open Akka.Cluster.Sharding
open Akka.Cluster.Sharding
open FsUnit.Xunit
open Legate
open Xunit

// The obsolete ShardingEnvelope-era overload below is part of the asserted
// extractor contract, so this file keeps warning 44 off.
#nowarn "44"

// Session sharding math (issue 126): the region name, the deterministic
// FNV-1a extractor over the session id, and the version stamp. Pure
// arithmetic with no statics or randomness, so repeated calls agree
// within and across processes.

/// Smuggles a null past F# nullness checking for defensive-validation
/// tests, mirroring LegateOptionsTests.
let private nullString: string = Unchecked.defaultof<string>

// ──────────────────────────────────────────────────────────────────────────
// Region name

[<Fact>]
let ``Shard type name is the stable session region`` () =
    SessionSharding.shardTypeName |> should equal "session"

// ──────────────────────────────────────────────────────────────────────────
// FNV-1a

[<Fact>]
let ``FNV-1a is deterministic across calls`` () =
    SessionSharding.fnv1a32 "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    |> should equal (SessionSharding.fnv1a32 "01ARZ3NDEKTSV4RRFFQ69G5FAV")

[<Fact>]
let ``FNV-1a answers the golden hash and the empty basis`` () =
    SessionSharding.fnv1a32 "01ARZ3NDEKTSV4RRFFQ69G5FAV" |> should equal 1543523712u
    SessionSharding.fnv1a32 "" |> should equal 2166136261u

[<Fact>]
let ``FNV-1a rejects a null input`` () =
    (fun () -> SessionSharding.fnv1a32 nullString |> ignore)
    |> should throw typeof<ArgumentNullException>

// ──────────────────────────────────────────────────────────────────────────
// Shard placement

[<Fact>]
let ``Shard ids answer the golden placements`` () =
    SessionSharding.shardIdFor "01ARZ3NDEKTSV4RRFFQ69G5FAV" 128 |> should equal "0"
    SessionSharding.shardIdFor "01ARZ3NDEKTSV4RRFFQ69G5FAW" 128 |> should equal "19"
    SessionSharding.shardIdFor "01J9Z8Y7X6W5V4U3T2S1R0QPNM" 128 |> should equal "85"
    SessionSharding.shardIdFor "session-alpha" 128 |> should equal "54"
    SessionSharding.shardIdFor "session-beta" 128 |> should equal "76"

[<Fact>]
let ``Shard ids are deterministic and stay in range`` () =
    let ids =
        [
            "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            "01ARZ3NDEKTSV4RRFFQ69G5FAW"
            "01J9Z8Y7X6W5V4U3T2S1R0QPNM"
            "session-alpha"
            "session-beta"
        ]

    for sessionId in ids do
        let first = SessionSharding.shardIdFor sessionId 128
        let second = SessionSharding.shardIdFor sessionId 128
        second |> should equal first

        let parsed = Int32.Parse(first)
        (parsed >= 0 && parsed < 128) |> should equal true

    ids
    |> List.map (fun sessionId -> SessionSharding.shardIdFor sessionId 128)
    |> List.distinct
    |> List.length
    |> should equal 5

[<Fact>]
let ``Shard ids follow the shard count`` () =
    SessionSharding.shardIdFor "01ARZ3NDEKTSV4RRFFQ69G5FAV" 32 |> should equal "0"
    SessionSharding.shardIdFor "01ARZ3NDEKTSV4RRFFQ69G5FAV" 1 |> should equal "0"

[<Fact>]
let ``Shard ids reject null ids and empty counts`` () =
    (fun () -> SessionSharding.shardIdFor nullString 128 |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> SessionSharding.shardIdFor "session-alpha" 0 |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

// ──────────────────────────────────────────────────────────────────────────
// Version stamp

[<Fact>]
let ``Version stamp is stable for identical inputs`` () =
    SessionSharding.versionStamp 1 128 |> should equal "1.1.128"

    SessionSharding.versionStamp 2 32
    |> should equal (SessionSharding.versionStamp 2 32)

[<Fact>]
let ``Version stamp moves with either knob`` () =
    let baseline = SessionSharding.versionStamp 1 128
    SessionSharding.versionStamp 1 32 |> should not' (equal baseline)
    SessionSharding.versionStamp 2 128 |> should not' (equal baseline)

[<Fact>]
let ``Version stamp rejects knob values below one`` () =
    (fun () -> SessionSharding.versionStamp 0 128 |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

    (fun () -> SessionSharding.versionStamp 1 0 |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

// ──────────────────────────────────────────────────────────────────────────
// Extractor

/// Builds the extractor the region runs.
let private extractor () : IMessageExtractor =
    SessionSharding.SessionMessageExtractor(128) :> IMessageExtractor

/// A bare session message: routable only inside an envelope.
let private bareMessage () : obj =
    SessionActorMessage.QueuePrompt(UserMessagePayload(UserMessage.Text "hello"), CancellationToken.None) :> obj

/// Builds a region envelope by reflection: the Akka constructor
/// parameters import as oblivious, which strict F# rejects at the call
/// site even for non-null arguments.
let private enveloped (sessionId: string) (message: obj) : obj =
    match Activator.CreateInstance(typeof<ShardingEnvelope>, [| sessionId :> obj; message |]) with
    | null -> raise (InvalidOperationException("ShardingEnvelope activation returned null."))
    | envelope -> envelope

[<Fact>]
let ``Extractor rejects a non-positive shard count`` () =
    (fun () -> SessionSharding.SessionMessageExtractor(0) |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

[<Fact>]
let ``Extractor reads the entity id from envelopes and bare strings`` () =
    SessionSharding.entityIdForMessage (enveloped "session-alpha" (42 :> obj))
    |> should equal "session-alpha"

    SessionSharding.entityIdForMessage "session-alpha"
    |> should equal "session-alpha"

    SessionSharding.entityIdForMessage (bareMessage ())
    |> isNull
    |> should equal true

    SessionSharding.entityIdForMessage (42 :> obj) |> isNull |> should equal true

[<Fact>]
let ``Extractor unwraps the entity message from envelopes`` () =
    let payload: obj = 42 :> obj

    Assert.Same(payload, SessionSharding.entityMessageFor (enveloped "session-alpha" payload))

    Assert.Same(payload, SessionSharding.entityMessageFor payload)

[<Fact>]
let ``Extractor shards by the deterministic hash`` () =
    SessionSharding.shardIdFor "session-alpha" 128
    |> should equal (SessionSharding.shardIdFor "session-alpha" 128)

    // The class delegates to the same math the region runs.
    let extractor = extractor ()

    extractor.ShardId("session-alpha", null)
    |> should equal (SessionSharding.shardIdFor "session-alpha" 128)

    extractor.ShardId("session-alpha", "ignored-hint" :> obj)
    |> should equal (SessionSharding.shardIdFor "session-alpha" 128)
