// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.BlobsTests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Xunit

let tenant = TenantId.Create "acme"
let sessionId = SessionId.New()
let payload = Encoding.UTF8.GetBytes "{\"ok\":true}"
let json = BlobContent(payload, "application/json")

/// Minimal fake over one dictionary proving the three contract interfaces
/// are implementable from outside the assembly (the C#-friendly shape) and
/// pinning the documented missing-key semantics through behavior.
type FakeBlobStore() =
    let blobs = Dictionary<string, BlobContent * string>()
    let mutable etagCounter = 0L
    let lockObj = obj ()

    let etagOf key : string | null =
        match blobs.TryGetValue key with
        | true, (_, etag) -> etag
        | false, _ -> null

    let metadataOf (content: BlobContent) etag =
        BlobMetadata(content.ContentType, int64 content.Bytes.Length, etag)

    let put key content =
        etagCounter <- etagCounter + 1L
        let etag = etagCounter.ToString("D20")
        blobs[key] <- (content, etag)
        metadataOf content etag

    interface IBlobStore with
        member _.Get(key, _) =
            Task.FromResult(
                match blobs.TryGetValue key with
                | true, (content, _) -> content.Bytes
                | false, _ -> null |> box |> unbox
            )

        member _.Put(key, content, _) = Task.FromResult(put key content)

        member _.CompareExchange(key, content, expectedEtag, _) =
            lock lockObj (fun () ->
                if etagOf key <> expectedEtag then
                    // etag mismatch: nothing written, null returned.
                    Task.FromResult(Unchecked.defaultof<BlobMetadata>)
                else
                    Task.FromResult(put key content))

        member _.OpenRead(key, _) =
            match blobs.TryGetValue key with
            | true, (content, _) ->
                let stream = new MemoryStream(content.Bytes)
                Task.FromResult(stream :> Stream)
            | false, _ -> raise (FileNotFoundException("No blob exists under this key.", key))

        member _.OpenWrite(key, contentType, _) =
            let stream = new MemoryStream()
            let committed = ref false

            let writeStream: Stream =
                { new Stream() with
                    member _.CanRead = false
                    member _.CanSeek = false
                    member _.CanWrite = true
                    member _.Length = stream.Length

                    member _.Position
                        with get () = stream.Position
                        and set value = stream.Position <- value

                    member _.Flush() = ()
                    member _.Seek(offset, origin) = stream.Seek(offset, origin)
                    member _.SetLength(value) = stream.SetLength value
                    member _.Read(buffer, offset, count) = stream.Read(buffer, offset, count)
                    member _.Write(buffer, offset, count) = stream.Write(buffer, offset, count)

                    override _.Dispose(disposing: bool) =
                        base.Dispose(disposing)

                        if disposing && not committed.Value then
                            committed.Value <- true
                            put key (BlobContent(stream.ToArray(), contentType)) |> ignore
                }

            Task.FromResult writeStream

        member _.List(prefix, _) =
            let keys =
                blobs.Keys
                |> Seq.filter (fun k -> k.StartsWith prefix)
                |> Seq.sort
                |> Array.ofSeq

            let index = ref -1

            { new IAsyncEnumerable<string> with
                member _.GetAsyncEnumerator(_) : IAsyncEnumerator<string> =
                    { new IAsyncEnumerator<string> with
                        member _.Current = keys[index.Value]

                        member _.MoveNextAsync() =
                            index.Value <- index.Value + 1
                            ValueTask<bool>(index.Value < keys.Length)

                        member _.DisposeAsync() = ValueTask.CompletedTask
                    }
            }

        member _.DeletePrefix(prefix, _) =
            let matching =
                blobs.Keys |> Seq.filter (fun k -> k.StartsWith prefix) |> Array.ofSeq

            for k in matching do
                blobs.Remove k |> ignore

            Task.FromResult matching.Length

        member _.GetMetadata(key, _) =
            Task.FromResult(
                match blobs.TryGetValue key with
                | true, (content, etag) -> metadataOf content etag
                | false, _ -> null |> box |> unbox
            )

        member _.TryGetPresignedUrl(_, _, _) = Task.FromResult null

    interface ISessionBlobStore with
        member this.Get(name, ct) =
            (this :> IBlobStore).Get(BlobKeys.ForSession(tenant, sessionId, name), ct)

        member this.Put(name, content, ct) =
            (this :> IBlobStore).Put(BlobKeys.ForSession(tenant, sessionId, name), content, ct)

        member this.CompareExchange(name, content, expectedEtag, ct) =
            (this :> IBlobStore)
                .CompareExchange(BlobKeys.ForSession(tenant, sessionId, name), content, expectedEtag, ct)

        member this.OpenRead(name, ct) =
            (this :> IBlobStore).OpenRead(BlobKeys.ForSession(tenant, sessionId, name), ct)

        member this.OpenWrite(name, contentType, ct) =
            (this :> IBlobStore).OpenWrite(BlobKeys.ForSession(tenant, sessionId, name), contentType, ct)

        member this.List(prefix, ct) =
            (this :> IBlobStore).List(BlobKeys.SessionPrefix(tenant, sessionId) + prefix, ct)

        member this.DeletePrefix(prefix, ct) =
            (this :> IBlobStore).DeletePrefix(BlobKeys.SessionPrefix(tenant, sessionId) + prefix, ct)

        member this.GetMetadata(name, ct) =
            (this :> IBlobStore).GetMetadata(BlobKeys.ForSession(tenant, sessionId, name), ct)

        member this.TryGetPresignedUrl(name, expiry, ct) =
            (this :> IBlobStore).TryGetPresignedUrl(BlobKeys.ForSession(tenant, sessionId, name), expiry, ct)

    interface IArtifactBlobStore with
        member this.Get(name, ct) =
            (this :> IBlobStore).Get(BlobKeys.ForArtifact(tenant, sessionId, name), ct)

        member this.Put(name, content, ct) =
            (this :> IBlobStore).Put(BlobKeys.ForArtifact(tenant, sessionId, name), content, ct)

        member this.CompareExchange(name, content, expectedEtag, ct) =
            (this :> IBlobStore)
                .CompareExchange(BlobKeys.ForArtifact(tenant, sessionId, name), content, expectedEtag, ct)

        member this.OpenRead(name, ct) =
            (this :> IBlobStore).OpenRead(BlobKeys.ForArtifact(tenant, sessionId, name), ct)

        member this.OpenWrite(name, contentType, ct) =
            (this :> IBlobStore).OpenWrite(BlobKeys.ForArtifact(tenant, sessionId, name), contentType, ct)

        member this.List(prefix, ct) =
            (this :> IBlobStore).List(BlobKeys.ArtifactPrefix(tenant, sessionId) + prefix, ct)

        member this.DeletePrefix(prefix, ct) =
            (this :> IBlobStore).DeletePrefix(BlobKeys.ArtifactPrefix(tenant, sessionId) + prefix, ct)

        member this.GetMetadata(name, ct) =
            (this :> IBlobStore).GetMetadata(BlobKeys.ForArtifact(tenant, sessionId, name), ct)

        member this.TryGetPresignedUrl(name, expiry, ct) =
            (this :> IBlobStore).TryGetPresignedUrl(BlobKeys.ForArtifact(tenant, sessionId, name), expiry, ct)

// ───────────────────────────────────────────────────────────────────────────
// BlobKeys.Validate: every rejection rule

[<Fact>]
let ``Validate rejects null`` () =
    (fun () -> BlobKeys.Validate(null |> box |> unbox) |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``Validate rejects empty`` () =
    (fun () -> BlobKeys.Validate "" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

[<Fact>]
let ``Validate rejects leading slash`` () =
    (fun () -> BlobKeys.Validate "/absolute" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

[<Fact>]
let ``Validate rejects drive prefix`` () =
    (fun () -> BlobKeys.Validate "C:\\temp\\blob" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

[<Fact>]
let ``Validate rejects dot segment`` () =
    (fun () -> BlobKeys.Validate "a/./b" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

[<Fact>]
let ``Validate rejects dotdot segment`` () =
    (fun () -> BlobKeys.Validate "a/../b" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

[<Fact>]
let ``Validate rejects backslash`` () =
    (fun () -> BlobKeys.Validate "a\\b" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

[<Fact>]
let ``Validate rejects NUL`` () =
    (fun () -> BlobKeys.Validate "a\u0000b" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

[<Fact>]
let ``Validate rejects internal empty segment`` () =
    (fun () -> BlobKeys.Validate "a//b" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

[<Fact>]
let ``Validate rejects trailing slash`` () =
    (fun () -> BlobKeys.Validate "a/b/" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

[<Fact>]
let ``Validate accepts multi-segment keys`` () =
    BlobKeys.Validate "sessions/01J/transcript.json"
    |> should equal "sessions/01J/transcript.json"

[<Fact>]
let ``Validate accepts unicode segments`` () =
    BlobKeys.Validate "sitzungen/\u00fcbertragung.json"
    |> should equal "sitzungen/\u00fcbertragung.json"

// ───────────────────────────────────────────────────────────────────────────
// BlobKeys.ValidatePrefix

[<Fact>]
let ``ValidatePrefix accepts the empty prefix`` () =
    BlobKeys.ValidatePrefix "" |> should equal ""

[<Fact>]
let ``ValidatePrefix accepts normal prefixes`` () =
    BlobKeys.ValidatePrefix "sessions/01J" |> should equal "sessions/01J"

[<Fact>]
let ``ValidatePrefix rejects traversal like a key`` () =
    (fun () -> BlobKeys.ValidatePrefix "a/../b" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

[<Fact>]
let ``ValidatePrefix rejects a trailing slash like a key`` () =
    (fun () -> BlobKeys.ValidatePrefix "a/b/" |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

// ───────────────────────────────────────────────────────────────────────────
// Derivation: determinism, scope separation, tenant-escape injectivity

[<Fact>]
let ``Session keys are deterministic`` () =
    let first = BlobKeys.ForSession(tenant, sessionId, "transcript.json")
    let second = BlobKeys.ForSession(tenant, sessionId, "transcript.json")

    first |> should equal second

    first
    |> should equal $"sessions/{tenant.Value}/{sessionId.Value}/transcript.json"

[<Fact>]
let ``Artifact keys use the artifacts scope`` () =
    BlobKeys.ForArtifact(tenant, sessionId, "result.txt")
    |> should equal $"artifacts/{tenant.Value}/{sessionId.Value}/result.txt"

[<Fact>]
let ``Session and artifact prefixes never collide`` () =
    let sessionPrefix = BlobKeys.SessionPrefix(tenant, sessionId)
    let artifactPrefix = BlobKeys.ArtifactPrefix(tenant, sessionId)

    sessionPrefix |> should equal $"sessions/{tenant.Value}/{sessionId.Value}/"
    artifactPrefix |> should equal $"artifacts/{tenant.Value}/{sessionId.Value}/"
    sessionPrefix.StartsWith artifactPrefix |> should equal false
    artifactPrefix.StartsWith sessionPrefix |> should equal false

[<Fact>]
let ``Distinct tenants derive distinct single-segment keys`` () =
    let a = BlobKeys.ForSession(TenantId.Create "a", sessionId, "blob")
    let slashB = BlobKeys.ForSession(TenantId.Create "a/b", sessionId, "blob")

    a |> should not' (equal slashB)

    // Without escaping, tenant "a/b" would embed a slash and
    // "sessions/a/b/..." would list under the "sessions/a/" prefix; the
    // escape keeps every tenant inside exactly one segment.
    let segments = slashB.Split('/')
    segments.Length |> should equal 4
    segments[1] |> should equal "a%002Fb"

[<Fact>]
let ``Tenant escaping is injective`` () =
    // Under variable-width escapes, tenant "%25" and tenant U+2525 would
    // both escape to "%2525"; fixed-width escapes keep them distinct.
    let literalPercent = BlobKeys.ForSession(TenantId.Create "%25", sessionId, "blob")
    let literalChar = BlobKeys.ForSession(TenantId.Create "\u2525", sessionId, "blob")

    literalPercent |> should not' (equal literalChar)

    (literalPercent.Split('/'))[1] |> should equal "%002525"

[<Fact>]
let ``A traversal tenant cannot produce a traversal segment`` () =
    let key = BlobKeys.ForSession(TenantId.Create "..", sessionId, "blob")
    let segments = key.Split('/')

    segments.Length |> should equal 4
    segments[1] |> should equal "%002E%002E"
    segments |> Array.exists (fun s -> s = "..") |> should equal false

[<Fact>]
let ``Relative names are enforced against traversal`` () =
    (fun () -> BlobKeys.ForSession(tenant, sessionId, "../escape") |> ignore)
    |> should throw typeof<InvalidBlobKeyException>

// ───────────────────────────────────────────────────────────────────────────
// Contract implementability: the fake pins the documented semantics

[<Fact>]
let ``Get returns null for a missing blob and bytes for a present one`` () =
    let store = FakeBlobStore() :> IBlobStore

    task {
        let! missing = store.Get("sessions/missing", CancellationToken.None)
        missing |> should equal null

        let! written = store.Put("sessions/present", json, CancellationToken.None)
        written.SizeBytes |> should equal (int64 payload.Length)

        let! fetched = store.Get("sessions/present", CancellationToken.None)
        fetched |> should equal payload
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``CompareExchange matches etags and create-if-absent with null`` () =
    let store = FakeBlobStore() :> IBlobStore

    task {
        // Null expected etag: create-if-absent.
        let! created = store.CompareExchange("sessions/ce", json, null |> box |> unbox, CancellationToken.None)
        created |> should not' (equal null)

        // Mismatched etag: nothing written.
        let! rejected = store.CompareExchange("sessions/ce", json, "bogus", CancellationToken.None)
        rejected |> should equal null

        // Matching etag: overwrite.
        let! current = store.GetMetadata("sessions/ce", CancellationToken.None)

        let! exchanged =
            match current with
            | null -> failwith "unreachable: metadata was just written"
            | valid -> store.CompareExchange("sessions/ce", json, valid.Etag, CancellationToken.None)

        exchanged |> should not' (equal null)

        match exchanged with
        | null -> failwith "unreachable: exchange succeeded"
        | written ->
            match current with
            | null -> failwith "unreachable: metadata exists"
            | prior -> written.Etag |> should not' (equal prior.Etag)
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``OpenRead throws FileNotFoundException for a missing blob`` () =
    let store = FakeBlobStore() :> IBlobStore

    (fun () -> store.OpenRead("sessions/nope", CancellationToken.None).Wait())
    |> should throw typeof<FileNotFoundException>

[<Fact>]
let ``OpenWrite commits atomically on successful dispose`` () =
    let store = FakeBlobStore() :> IBlobStore

    task {
        let! stream = store.OpenWrite("sessions/atomic", "text/plain", CancellationToken.None)

        stream.Write(Encoding.UTF8.GetBytes "committed", 0, 9)
        stream.Dispose()

        let! metadata = store.GetMetadata("sessions/atomic", CancellationToken.None)
        metadata |> should not' (equal null)

        match metadata with
        | null -> failwith "unreachable: blob was just written"
        | valid -> valid.ContentType |> should equal "text/plain"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``List returns matching keys in order`` () =
    let store = FakeBlobStore() :> IBlobStore

    task {
        let! first = store.Put("sessions/01/b", json, CancellationToken.None)
        let! second = store.Put("sessions/01/a", json, CancellationToken.None)
        let! third = store.Put("other/x", json, CancellationToken.None)
        (first, second, third) |> ignore

        let mutable listed = []

        let enumerator =
            store.List("sessions/", CancellationToken.None).GetAsyncEnumerator()

        try
            let mutable more = true

            while more do
                let! moved = enumerator.MoveNextAsync()
                more <- moved

                if moved then
                    listed <- enumerator.Current :: listed
        finally
            enumerator.DisposeAsync().AsTask().Wait()

        listed |> List.rev |> should equal [ "sessions/01/a"; "sessions/01/b" ]
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``DeletePrefix returns the deleted count`` () =
    let store = FakeBlobStore() :> IBlobStore

    task {
        let! one = store.Put("pfx/1", json, CancellationToken.None)
        let! two = store.Put("pfx/2", json, CancellationToken.None)
        let! three = store.Put("keep/3", json, CancellationToken.None)
        (one, two, three) |> ignore

        let! deleted = store.DeletePrefix("pfx/", CancellationToken.None)
        deleted |> should equal 2

        let! kept = store.Get("keep/3", CancellationToken.None)
        kept |> should equal payload

        let! gone = store.Get("pfx/1", CancellationToken.None)
        gone |> should equal null
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``TryGetPresignedUrl returns null when the backend cannot presign`` () =
    let store = FakeBlobStore() :> IBlobStore

    task {
        let! url = store.TryGetPresignedUrl("sessions/any", TimeSpan.FromMinutes 5., CancellationToken.None)
        url |> should equal null
    }
    |> (fun t -> t.Wait())
