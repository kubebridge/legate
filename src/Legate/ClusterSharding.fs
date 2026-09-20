// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Text
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.Cluster.Sharding

// Session sharding math and the mode seam in front of every resolve.
// Local mode resolves through the in-process session router; the cluster
// modes resolve through the region proxy in ClusterActorSystem. Both
// spawn the same SessionActor through the facade-wired factory, so Local
// behavior is unchanged and the actor code is shared. Shard placement is
// a deterministic FNV-1a-32 over the session id modulo the shard count:
// no coordination, identical on every node and every process. The shard
// configuration is versioned as a stable stamp published per member
// (akka.cluster.app-version); the guard in ClusterActorSystem fails
// closed on a peer stamp mismatch instead of mis-routing.

// ──────────────────────────────────────────────────────────────────────────
// Mode seam

/// Resolves a session id to its actor, whichever mode hosts it: the
/// local router child in Local mode, the shard region proxy in the
/// cluster modes. Internal so no Akka type ever crosses the public API.
type internal ISessionResolver =

    /// Resolves the session actor for a session id: the same id returns
    /// the same actor, distinct ids return distinct actors.
    /// <param name="sessionId">The session whose actor to resolve.</param>
    /// <param name="cancellationToken">Cancels the resolve.</param>
    /// <returns>The session actor.</returns>
    abstract ResolveSessionAsync: sessionId: string * cancellationToken: CancellationToken -> Task<IActorRef>

// ──────────────────────────────────────────────────────────────────────────
// Sharding math

/// The session shard region: its name, its deterministic extractor, and
/// its version stamp. Internal so no Akka type ever crosses the public API.
module internal SessionSharding =

    /// The shard region (entity type) name, stable so every node agrees.
    let shardTypeName = "session"

    /// FNV-1a 32-bit offset basis.
    let private fnvOffsetBasis = 2166136261u

    /// FNV-1a 32-bit prime.
    let private fnvPrime = 16777619u

    /// Hashes text with FNV-1a over its UTF-8 bytes. Pure arithmetic, so
    /// the result is identical on every node and every process.
    /// <param name="text">The text to hash. Must not be null.</param>
    /// <returns>The 32-bit FNV-1a hash.</returns>
    let fnv1a32 (text: string) : uint32 =
        ArgumentNullException.ThrowIfNull(text)

        let mutable hash = fnvOffsetBasis

        for byteValue in Encoding.UTF8.GetBytes(text) do
            hash <- (hash ^^^ uint32 byteValue) * fnvPrime

        hash

    /// Maps a session id to its shard: the FNV-1a hash modulo the shard
    /// count, rendered as a decimal string (shard ids are strings).
    /// <param name="sessionId">The session id to place. Must not be null.</param>
    /// <param name="shardCount">The shard count. Must be at least 1.</param>
    /// <returns>The shard id for the session.</returns>
    let shardIdFor (sessionId: string) (shardCount: int) : string =
        ArgumentNullException.ThrowIfNull(sessionId)

        if shardCount < 1 then
            raise (ArgumentOutOfRangeException(nameof shardCount, "The shard count must be at least 1."))

        (fnv1a32 sessionId % uint32 shardCount).ToString()

    /// Builds the cluster version stamp for a shard configuration:
    /// schema 1, the hash version, the shard count. Dotted numerics stay
    /// parseable as an Akka app-version; identical inputs give identical
    /// stamps, and any change to either knob changes the stamp so mixed
    /// nodes fail closed instead of mis-routing.
    /// <param name="hashVersion">The shard hash version. Must be at least 1.</param>
    /// <param name="shardCount">The shard count. Must be at least 1.</param>
    /// <returns>The stable version stamp.</returns>
    let versionStamp (hashVersion: int) (shardCount: int) : string =
        if hashVersion < 1 then
            raise (ArgumentOutOfRangeException(nameof hashVersion, "The hash version must be at least 1."))

        if shardCount < 1 then
            raise (ArgumentOutOfRangeException(nameof shardCount, "The shard count must be at least 1."))

        $"1.%d{hashVersion}.%d{shardCount}"

    /// Reads the entity id from a region message: the envelope's id,
    /// a bare string by its own value (the wire-safe resolve marker),
    /// or null for a bare session message, which carries no id and is
    /// unroutable. Pure so tests assert it without touching Akka.
    /// <param name="message">The region message. Must not be null.</param>
    /// <returns>The entity id, or null when unroutable.</returns>
    let entityIdForMessage (message: obj) : string | null =
        ArgumentNullException.ThrowIfNull(message)

        match message with
        | :? ShardingEnvelope as envelope -> envelope.EntityId
        | :? string as sessionId -> sessionId
        | _ -> null

    /// Unwraps the entity payload from a region message: the envelope's
    /// inner message, or the message itself when bare. Pure so tests
    /// assert it without touching Akka.
    /// <param name="message">The region message. Must not be null.</param>
    /// <returns>The message the entity receives.</returns>
    let entityMessageFor (message: obj) : obj =
        ArgumentNullException.ThrowIfNull(message)

        match message with
        | :? ShardingEnvelope as envelope -> envelope.Message
        | _ -> message

    /// Extracts the session entity and shard from region messages. The
    /// proxies always wrap payloads in a ShardingEnvelope carrying the
    /// session id; bare strings resolve by their own value (the wire-safe
    /// resolve marker: strings cross remoting without a custom
    /// serializer); a bare session message carries no id and is
    /// unroutable. ShardId ignores the hint: placement is the
    /// deterministic hash, never message content. The Akka
    /// IMessageExtractor parameters import as oblivious, so the class
    /// only delegates to the pure functions above, which carry F#
    /// signatures and the unit tests.
    /// <param name="shardCount">The shard count. Must be at least 1.</param>
    type SessionMessageExtractor(shardCount: int) =

        do
            if shardCount < 1 then
                raise (ArgumentOutOfRangeException(nameof shardCount, "The shard count must be at least 1."))

        interface IMessageExtractor with
            member _.EntityId(message: obj) : string | null = entityIdForMessage message

            member _.EntityMessage(message: obj) : obj = entityMessageFor message

            member _.ShardId(entityId: string, _messageHint: obj) : string = shardIdFor entityId shardCount

            member _.ShardId(message: obj) : string | null =
                match entityIdForMessage message with
                | null -> null
                | entityId -> shardIdFor entityId shardCount
