// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Globalization
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks

// Blob storage contracts. IBlobStore is the primitive the S3 and local
// directory backends implement; the typed scopes (ISessionBlobStore,
// IArtifactBlobStore) are what session and artifact code hold, deriving keys
// deterministically from tenant and session so implementations cannot cross
// tenants. BlobKeys is the single shared helper validating keys and prefixes
// and deriving scope keys; every implementation validates through it, so the
// rules are tested once and cannot drift. Missing-key semantics are pinned
// on the interface docs: Get/GetMetadata return null when absent, OpenRead
// throws FileNotFoundException, CompareExchange returns false-shaped null on
// etag mismatch, Put returns the new metadata, OpenWrite commits atomically
// on successful dispose, DeletePrefix returns the deleted count, and
// TryGetPresignedUrl returns null when the backend cannot presign.

/// The payload and content type of a blob write. Value type carrying a
/// reference to the bytes so large payloads are not copied; the runtime
/// never inspects the payload. Both members must be non-null; store
/// implementations validate on write.
/// <param name="bytes">The blob payload. Must not be null.</param>
/// <param name="contentType">The content type to store with the blob, for example "application/json". Must not be null.</param>
[<Struct>]
type BlobContent(bytes: byte[], contentType: string) =

    /// The blob payload. Must not be null.
    member _.Bytes: byte[] = bytes

    /// The content type stored with the blob, for example "application/json".
    /// Must not be null.
    member _.ContentType = contentType

/// Server-side metadata about a stored blob: the content type it was written
/// with, its size in bytes, and an opaque version token a
/// <see cref="T:Legate.IBlobStore" /> hands back for optimistic concurrency.
/// A reference type so the pinned missing-key semantics ("returns null when
/// absent") can use nullability on the interface surface.
/// <param name="contentType">The content type stored with the blob.</param>
/// <param name="sizeBytes">The blob size in bytes.</param>
/// <param name="etag">The opaque version token, unique per write.</param>
[<Sealed>]
type BlobMetadata(contentType: string, sizeBytes: int64, etag: string) =

    do
        if isNull (box contentType) then
            raise (ArgumentNullException(nameof contentType))

        if isNull (box etag) then
            raise (ArgumentNullException(nameof etag))

    /// The content type stored with the blob.
    member _.ContentType = contentType

    /// The blob size in bytes.
    member _.SizeBytes: int64 = sizeBytes

    /// The opaque version token, unique per write and never reused.
    member _.Etag = etag

    /// Compares two metadata values ordinally across all fields.
    /// <param name="other">The metadata to compare against.</param>
    /// <returns>true when content type, size, and etag all match.</returns>
    member this.Equals(other: BlobMetadata) =
        String.Equals(this.ContentType, other.ContentType, StringComparison.Ordinal)
        && this.SizeBytes = other.SizeBytes
        && String.Equals(this.Etag, other.Etag, StringComparison.Ordinal)

    /// Hashes all fields ordinally.
    override _.GetHashCode() =
        let mutable hash = 17
        hash <- hash * 31 + StringComparer.Ordinal.GetHashCode contentType
        hash <- hash * 31 + sizeBytes.GetHashCode()
        hash <- hash * 31 + StringComparer.Ordinal.GetHashCode etag
        hash

    /// Compares against a boxed metadata value without recursing.
    /// <param name="other">The object to compare against.</param>
    /// <returns>true when the other object is equal metadata.</returns>
    override this.Equals(other: obj | null) =
        match other with
        | :? BlobMetadata as metadata -> this.Equals(metadata: BlobMetadata)
        | _ -> false

    /// Implements ordinal equality for the generic collection surface.
    /// <param name="other">The metadata to compare against.</param>
    /// <returns>true when content type, size, and etag all match.</returns>
    interface IEquatable<BlobMetadata> with
        member this.Equals(other: BlobMetadata | null) =
            match other with
            | null -> false
            | valid -> this.Equals(valid: BlobMetadata)

// ───────────────────────────────────────────────────────────────────────────
// Shared key validation and scope-key derivation
//
// One helper, tested once: every store implementation validates through
// BlobKeys so the rules cannot drift between backends.

/// The single shared helper for blob key and prefix validation and for
/// deriving the deterministic scope keys the typed stores hand out. Keys are
/// slash-separated relative paths: no rooted paths, no <c>.</c> or <c>..</c>
/// segments, no backslashes, no NUL characters, and no empty segments (which
/// also bans leading and trailing slashes). Prefixes follow the same rules
/// except that the empty string is the valid list-everything prefix.
/// <exception cref="T:Legate.InvalidBlobKeyException">A key or prefix fails validation.</exception>
type BlobKeys() =

    /// Reports whether the character is allowed unescaped in a derived
    /// scope-key segment.
    static member private IsSegmentCharSafe(c: char) =
        Char.IsAsciiLetterOrDigit c || c = '_' || c = '-'

    /// The one segment rule shared by key and prefix validation: a segment
    /// is non-empty and contains no backslash or NUL character.
    /// <param name="segment">The segment to validate.</param>
    /// <returns>true when the segment is valid; otherwise false.</returns>
    static member private SegmentIsValid(segment: string) =
        segment.Length > 0
        && not (segment.Contains('\\'))
        && not (segment.Contains('\u0000'))

    /// Splits and validates a raw key or non-empty prefix into its segments,
    /// rejecting the empty string, rooted paths, and drive prefixes.
    /// <param name="value">The key or prefix to split.</param>
    /// <returns>The validated segments, in order.</returns>
    static member private SplitValidated(value: string) : string[] =
        if isNull (box value) then
            raise (ArgumentNullException(nameof value))

        if value = "" then
            raise (InvalidBlobKeyException(value, "A blob key must not be empty."))

        if value.Length >= 2 && value[1] = ':' then
            raise (InvalidBlobKeyException(value, "A blob key must not be a rooted path or carry a drive prefix."))

        if value.StartsWith('/') || value.StartsWith('\\') then
            raise (InvalidBlobKeyException(value, "A blob key must be relative, without a leading slash."))

        let segments = value.Split('/')

        for segment in segments do
            if not (BlobKeys.SegmentIsValid segment) then
                raise (
                    InvalidBlobKeyException(
                        value,
                        "A blob key segment must be non-empty and contain no backslash or NUL character."
                    )
                )

            if segment = "." || segment = ".." then
                raise (InvalidBlobKeyException(value, "A blob key must not contain '.' or '..' segments."))

        segments

    /// Validates a blob key and returns it unchanged.
    /// <param name="key">The key to validate.</param>
    /// <returns>The validated key.</returns>
    /// <exception cref="T:Legate.InvalidBlobKeyException">The key is null, empty, rooted, carries a drive prefix, or contains a '.', '..', backslash, NUL, or empty segment.</exception>
    static member Validate(key: string) : string =
        BlobKeys.SplitValidated key |> ignore
        key

    /// Validates a list prefix. The empty string lists everything and is
    /// always valid; non-empty prefixes follow the key rules.
    /// <param name="prefix">The prefix to validate.</param>
    /// <returns>The validated prefix.</returns>
    /// <exception cref="T:Legate.InvalidBlobKeyException">The prefix fails the key rules; a null prefix throws ArgumentNullException.</exception>
    static member ValidatePrefix(prefix: string) : string =
        if prefix = "" then
            prefix
        else
            BlobKeys.SplitValidated prefix |> ignore
            prefix

    /// Percent-escapes every character of a tenant id outside
    /// [A-Za-z0-9_-] as a fixed-width uppercase <c>%XXXX</c> hex escape per
    /// UTF-16 code unit, so distinct tenants always map to distinct segments
    /// (the escape is injective: '%' itself is escaped and every escape is
    /// exactly five characters) and no escaped segment can form a traversal
    /// or introduce a separator.
    /// <param name="tenant">The tenant id to escape.</param>
    /// <returns>The escaped tenant segment.</returns>
    static member private EscapeTenant(tenant: string) =
        let builder = StringBuilder()

        for c in tenant do
            if BlobKeys.IsSegmentCharSafe c then
                builder.Append(c) |> ignore
            else
                builder.Append('%') |> ignore

                let code = (int c).ToString("X4", CultureInfo.InvariantCulture)
                builder.Append(code) |> ignore

        builder.ToString()

    /// Derives the deterministic key for a name under a scope: the tenant
    /// segment is percent-escaped so distinct tenants never collide and
    /// cannot traverse; the session id is a canonical ULID and already
    /// safe; the name is validated as a relative key.
    /// <param name="scope">The top-level scope directory, "sessions" or "artifacts".</param>
    /// <param name="tenant">The tenant that owns the blob.</param>
    /// <param name="sessionId">The session the blob belongs to.</param>
    /// <param name="name">The relative name of the blob under the session. Validated like a key.</param>
    /// <returns>The deterministic scope key.</returns>
    /// <exception cref="T:Legate.InvalidBlobKeyException">The name fails key validation.</exception>
    static member private ScopeKey(scope: string, tenant: TenantId, sessionId: SessionId, name: string) =
        let validatedName = BlobKeys.Validate name
        let tenantSegment = BlobKeys.EscapeTenant tenant.Value

        sprintf "%s/%s/%s/%s" scope tenantSegment sessionId.Value validatedName

    /// Derives the deterministic list prefix for a scope: everything under
    /// <c>{scope}/{escapedTenant}/{sessionId}/</c>. The trailing slash
    /// keeps prefix listings from reaching sibling sessions.
    /// <param name="scope">The top-level scope directory, "sessions" or "artifacts".</param>
    /// <param name="tenant">The tenant that owns the scope.</param>
    /// <param name="sessionId">The session the prefix is scoped to.</param>
    /// <returns>The deterministic scope prefix, ending with a slash.</returns>
    static member private ScopePrefix(scope: string, tenant: TenantId, sessionId: SessionId) =
        sprintf "%s/%s/%s/" scope (BlobKeys.EscapeTenant tenant.Value) sessionId.Value

    /// Derives the deterministic session-scope key for a relative name:
    /// <c>sessions/{escapedTenant}/{sessionId}/{name}</c>.
    /// <param name="tenant">The tenant that owns the session.</param>
    /// <param name="sessionId">The session the blob belongs to.</param>
    /// <param name="name">The relative name of the blob under the session, for example "transcript.json". Validated like a key.</param>
    /// <returns>The deterministic session-scope key.</returns>
    /// <exception cref="T:Legate.InvalidBlobKeyException">The name fails key validation.</exception>
    static member ForSession(tenant: TenantId, sessionId: SessionId, name: string) =
        BlobKeys.ScopeKey("sessions", tenant, sessionId, name)

    /// Derives the deterministic artifact-scope key for a relative name:
    /// <c>artifacts/{escapedTenant}/{sessionId}/{name}</c>.
    /// <param name="tenant">The tenant that owns the session.</param>
    /// <param name="sessionId">The session the artifact belongs to.</param>
    /// <param name="name">The relative name of the artifact under the session, for example "turn-01-result.txt". Validated like a key.</param>
    /// <returns>The deterministic artifact-scope key.</returns>
    /// <exception cref="T:Legate.InvalidBlobKeyException">The name fails key validation.</exception>
    static member ForArtifact(tenant: TenantId, sessionId: SessionId, name: string) =
        BlobKeys.ScopeKey("artifacts", tenant, sessionId, name)

    /// Derives the deterministic session-scope list prefix:
    /// <c>sessions/{escapedTenant}/{sessionId}/</c>.
    /// <param name="tenant">The tenant that owns the session.</param>
    /// <param name="sessionId">The session the prefix is scoped to.</param>
    /// <returns>The deterministic session-scope prefix, ending with a slash.</returns>
    static member SessionPrefix(tenant: TenantId, sessionId: SessionId) =
        BlobKeys.ScopePrefix("sessions", tenant, sessionId)

    /// Derives the deterministic artifact-scope list prefix:
    /// <c>artifacts/{escapedTenant}/{sessionId}/</c>.
    /// <param name="tenant">The tenant that owns the session.</param>
    /// <param name="sessionId">The session the prefix is scoped to.</param>
    /// <returns>The deterministic artifact-scope prefix, ending with a slash.</returns>
    static member ArtifactPrefix(tenant: TenantId, sessionId: SessionId) =
        BlobKeys.ScopePrefix("artifacts", tenant, sessionId)

// ───────────────────────────────────────────────────────────────────────────
// The primitive store contract

/// Low-level blob storage primitives: the contract the S3 and local
/// directory backends implement. All methods validate keys and prefixes
/// through <see cref="T:Legate.BlobKeys" /> and throw
/// <see cref="T:Legate.InvalidBlobKeyException" /> on invalid input.
type IBlobStore =

    /// Reads a blob's bytes. Returns null when the key does not exist.
    /// <param name="key">The key of the blob to read.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The blob payload, or null when absent.</returns>
    abstract Get: key: string * cancellationToken: CancellationToken -> Task<byte[] | null>

    /// Writes a blob, creating or overwriting it.
    /// <param name="key">The key to write under.</param>
    /// <param name="content">The payload and content type to write.</param>
    /// <param name="cancellationToken">Token that abandons the write.</param>
    /// <returns>The metadata of the blob after the write.</returns>
    abstract Put: key: string * content: BlobContent * cancellationToken: CancellationToken -> Task<BlobMetadata>

    /// Optimistic compare-exchange: overwrites the blob only when its
    /// current etag matches <paramref name="expectedEtag" />. A null
    /// expected etag means create-if-absent. Nothing is written when the
    /// etag does not match.
    /// <param name="key">The key to write under.</param>
    /// <param name="content">The payload and content type to write.</param>
    /// <param name="expectedEtag">The etag the caller last observed, or null to require absence.</param>
    /// <param name="cancellationToken">Token that abandons the exchange.</param>
    /// <returns>The metadata written, or null when the etag did not match.</returns>
    abstract CompareExchange:
        key: string * content: BlobContent * expectedEtag: string | null * cancellationToken: CancellationToken ->
            Task<BlobMetadata | null>

    /// Opens a read stream over an existing blob. Throws
    /// <see cref="T:System.IO.FileNotFoundException" /> when the key does
    /// not exist.
    /// <param name="key">The key of the blob to read.</param>
    /// <param name="cancellationToken">Token that abandons the open.</param>
    /// <returns>A stream over the blob's bytes.</returns>
    abstract OpenRead: key: string * cancellationToken: CancellationToken -> Task<Stream>

    /// Opens a write stream that commits atomically on successful dispose:
    /// readers never see a partial blob, and the previous content stays
    /// visible when the write fails or is abandoned.
    /// <param name="key">The key to write under.</param>
    /// <param name="contentType">The content type to store with the blob.</param>
    /// <param name="cancellationToken">Token that abandons the write.</param>
    /// <returns>A stream that commits the blob when disposed successfully.</returns>
    abstract OpenWrite: key: string * contentType: string * cancellationToken: CancellationToken -> Task<Stream>

    /// Lists keys with the given prefix in lexicographic order.
    /// <param name="prefix">The prefix to list under; the empty string lists everything.</param>
    /// <param name="cancellationToken">Token that abandons the enumeration.</param>
    /// <returns>The matching keys, lexicographically ordered.</returns>
    abstract List: prefix: string * cancellationToken: CancellationToken -> IAsyncEnumerable<string>

    /// Deletes every blob under the prefix.
    /// <param name="prefix">The prefix to clear; the empty string clears everything.</param>
    /// <param name="cancellationToken">Token that abandons the deletion.</param>
    /// <returns>How many blobs were deleted.</returns>
    abstract DeletePrefix: prefix: string * cancellationToken: CancellationToken -> Task<int>

    /// Reads a blob's metadata. Returns null when the key does not exist.
    /// <param name="key">The key of the blob to describe.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The blob's metadata, or null when absent.</returns>
    abstract GetMetadata: key: string * cancellationToken: CancellationToken -> Task<BlobMetadata | null>

    /// Tries to obtain a presigned URL for the blob. Returns null when the
    /// backend cannot presign (the local directory store without a host
    /// base URL, for instance).
    /// <param name="key">The key of the blob to sign.</param>
    /// <param name="expiry">How long the URL stays valid.</param>
    /// <param name="cancellationToken">Token that abandons the signing.</param>
    /// <returns>The presigned URL, or null when the backend cannot presign.</returns>
    abstract TryGetPresignedUrl:
        key: string * expiry: TimeSpan * cancellationToken: CancellationToken -> Task<Uri | null>

// ───────────────────────────────────────────────────────────────────────────
// Typed scopes
//
// The same verbs, bound to one tenant and session. Implementations derive
// every key through BlobKeys, so no sequence of calls can reach outside the
// caller's tenant and session.

/// Session-scoped blob storage for one tenant and session: the transcript
/// and any other session blobs. Every name is relative to the session's
/// scope; implementations derive keys through
/// <see cref="T:Legate.BlobKeys" />, so a call can never cross tenants or
/// sessions.
type ISessionBlobStore =

    /// Reads a session blob's bytes. Returns null when the name does not
    /// exist.
    /// <param name="name">The relative name of the blob under the session.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The blob payload, or null when absent.</returns>
    abstract Get: name: string * cancellationToken: CancellationToken -> Task<byte[] | null>

    /// Writes a session blob, creating or overwriting it.
    /// <param name="name">The relative name of the blob under the session.</param>
    /// <param name="content">The payload and content type to write.</param>
    /// <param name="cancellationToken">Token that abandons the write.</param>
    /// <returns>The metadata of the blob after the write.</returns>
    abstract Put: name: string * content: BlobContent * cancellationToken: CancellationToken -> Task<BlobMetadata>

    /// Optimistic compare-exchange on a session blob. A null expected etag
    /// means create-if-absent. Nothing is written when the etag does not
    /// match.
    /// <param name="name">The relative name of the blob under the session.</param>
    /// <param name="content">The payload and content type to write.</param>
    /// <param name="expectedEtag">The etag the caller last observed, or null to require absence.</param>
    /// <param name="cancellationToken">Token that abandons the exchange.</param>
    /// <returns>The metadata written, or null when the etag did not match.</returns>
    abstract CompareExchange:
        name: string * content: BlobContent * expectedEtag: string | null * cancellationToken: CancellationToken ->
            Task<BlobMetadata | null>

    /// Opens a read stream over an existing session blob. Throws
    /// <see cref="T:System.IO.FileNotFoundException" /> when the name does
    /// not exist.
    /// <param name="name">The relative name of the blob under the session.</param>
    /// <param name="cancellationToken">Token that abandons the open.</param>
    /// <returns>A stream over the blob's bytes.</returns>
    abstract OpenRead: name: string * cancellationToken: CancellationToken -> Task<Stream>

    /// Opens a write stream that commits atomically on successful dispose.
    /// <param name="name">The relative name of the blob under the session.</param>
    /// <param name="contentType">The content type to store with the blob.</param>
    /// <param name="cancellationToken">Token that abandons the write.</param>
    /// <returns>A stream that commits the blob when disposed successfully.</returns>
    abstract OpenWrite: name: string * contentType: string * cancellationToken: CancellationToken -> Task<Stream>

    /// Lists session blob names with the given relative prefix, in
    /// lexicographic order.
    /// <param name="prefix">The relative prefix to list under; the empty string lists the whole session scope.</param>
    /// <param name="cancellationToken">Token that abandons the enumeration.</param>
    /// <returns>The matching relative names, lexicographically ordered.</returns>
    abstract List: prefix: string * cancellationToken: CancellationToken -> IAsyncEnumerable<string>

    /// Deletes every session blob under the relative prefix.
    /// <param name="prefix">The relative prefix to clear; the empty string clears the whole session scope.</param>
    /// <param name="cancellationToken">Token that abandons the deletion.</param>
    /// <returns>How many blobs were deleted.</returns>
    abstract DeletePrefix: prefix: string * cancellationToken: CancellationToken -> Task<int>

    /// Reads a session blob's metadata. Returns null when the name does not
    /// exist.
    /// <param name="name">The relative name of the blob under the session.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The blob's metadata, or null when absent.</returns>
    abstract GetMetadata: name: string * cancellationToken: CancellationToken -> Task<BlobMetadata | null>

    /// Tries to obtain a presigned URL for a session blob. Returns null
    /// when the backend cannot presign.
    /// <param name="name">The relative name of the blob under the session.</param>
    /// <param name="expiry">How long the URL stays valid.</param>
    /// <param name="cancellationToken">Token that abandons the signing.</param>
    /// <returns>The presigned URL, or null when the backend cannot presign.</returns>
    abstract TryGetPresignedUrl:
        name: string * expiry: TimeSpan * cancellationToken: CancellationToken -> Task<Uri | null>

/// Artifact-scoped blob storage for one tenant and session: turn outputs and
/// other artifacts. Every name is relative to the session's artifact scope;
/// implementations derive keys through <see cref="T:Legate.BlobKeys" />, so
/// a call can never cross tenants or sessions.
type IArtifactBlobStore =

    /// Reads an artifact's bytes. Returns null when the name does not
    /// exist.
    /// <param name="name">The relative name of the artifact under the session.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The artifact payload, or null when absent.</returns>
    abstract Get: name: string * cancellationToken: CancellationToken -> Task<byte[] | null>

    /// Writes an artifact, creating or overwriting it.
    /// <param name="name">The relative name of the artifact under the session.</param>
    /// <param name="content">The payload and content type to write.</param>
    /// <param name="cancellationToken">Token that abandons the write.</param>
    /// <returns>The metadata of the artifact after the write.</returns>
    abstract Put: name: string * content: BlobContent * cancellationToken: CancellationToken -> Task<BlobMetadata>

    /// Optimistic compare-exchange on an artifact. A null expected etag
    /// means create-if-absent. Nothing is written when the etag does not
    /// match.
    /// <param name="name">The relative name of the artifact under the session.</param>
    /// <param name="content">The payload and content type to write.</param>
    /// <param name="expectedEtag">The etag the caller last observed, or null to require absence.</param>
    /// <param name="cancellationToken">Token that abandons the exchange.</param>
    /// <returns>The metadata written, or null when the etag did not match.</returns>
    abstract CompareExchange:
        name: string * content: BlobContent * expectedEtag: string | null * cancellationToken: CancellationToken ->
            Task<BlobMetadata | null>

    /// Opens a read stream over an existing artifact. Throws
    /// <see cref="T:System.IO.FileNotFoundException" /> when the name does
    /// not exist.
    /// <param name="name">The relative name of the artifact under the session.</param>
    /// <param name="cancellationToken">Token that abandons the open.</param>
    /// <returns>A stream over the artifact's bytes.</returns>
    abstract OpenRead: name: string * cancellationToken: CancellationToken -> Task<Stream>

    /// Opens a write stream that commits atomically on successful dispose.
    /// <param name="name">The relative name of the artifact under the session.</param>
    /// <param name="contentType">The content type to store with the artifact.</param>
    /// <param name="cancellationToken">Token that abandons the write.</param>
    /// <returns>A stream that commits the artifact when disposed successfully.</returns>
    abstract OpenWrite: name: string * contentType: string * cancellationToken: CancellationToken -> Task<Stream>

    /// Lists artifact names with the given relative prefix, in
    /// lexicographic order.
    /// <param name="prefix">The relative prefix to list under; the empty string lists the whole artifact scope.</param>
    /// <param name="cancellationToken">Token that abandons the enumeration.</param>
    /// <returns>The matching relative names, lexicographically ordered.</returns>
    abstract List: prefix: string * cancellationToken: CancellationToken -> IAsyncEnumerable<string>

    /// Deletes every artifact under the relative prefix.
    /// <param name="prefix">The relative prefix to clear; the empty string clears the whole artifact scope.</param>
    /// <param name="cancellationToken">Token that abandons the deletion.</param>
    /// <returns>How many artifacts were deleted.</returns>
    abstract DeletePrefix: prefix: string * cancellationToken: CancellationToken -> Task<int>

    /// Reads an artifact's metadata. Returns null when the name does not
    /// exist.
    /// <param name="name">The relative name of the artifact under the session.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The artifact's metadata, or null when absent.</returns>
    abstract GetMetadata: name: string * cancellationToken: CancellationToken -> Task<BlobMetadata | null>

    /// Tries to obtain a presigned URL for an artifact. Returns null when
    /// the backend cannot presign.
    /// <param name="name">The relative name of the artifact under the session.</param>
    /// <param name="expiry">How long the URL stays valid.</param>
    /// <param name="cancellationToken">Token that abandons the signing.</param>
    /// <returns>The presigned URL, or null when the backend cannot presign.</returns>
    abstract TryGetPresignedUrl:
        name: string * expiry: TimeSpan * cancellationToken: CancellationToken -> Task<Uri | null>
