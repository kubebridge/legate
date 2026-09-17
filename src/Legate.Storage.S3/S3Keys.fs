// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3

open System
open System.Collections.Generic
open System.Globalization
open System.Text
open System.Text.Json
open Legate

// The S3 key layout and the small JSON documents behind the package
// pointer commit. Internal but isolated in this one module, so the
// shared-versioning work can lift the layout without behavior change.
// Blob keys map 1:1 to object keys (validated through BlobKeys by the
// callers); package objects live under packages/{tenant}/{agent}/ with a
// per-version entry prefix, a per-version metadata document, and one
// active-version pointer document committed through a conditional write.

// ──────────────────────────────────────────────────────────────────────────
// Tenant escaping

/// <summary>
/// The S3 key layout behind the blob and package stores. Internal: the
/// stores call this, and the shared-versioning work lifts it without
/// behavior change.
/// </summary>
module internal S3Keys =

    /// <summary>
    /// The top-level prefix every package object lives under.
    /// </summary>
    let packagesRoot = "packages"

    /// <summary>
    /// Percent-escapes every character of a tenant id outside
    /// [A-Za-z0-9_-] as a fixed-width uppercase <c>%XXXX</c> hex escape
    /// per UTF-16 code unit, mirroring
    /// <see cref="T:Legate.BlobKeys" /> so distinct tenants never collide
    /// and no tenant value can introduce a separator or traversal.
    /// </summary>
    /// <param name="tenant">The tenant id to escape.</param>
    /// <returns>The escaped tenant segment.</returns>
    let escapeTenant (tenant: TenantId) =
        let builder = StringBuilder()

        for c in tenant.Value do
            if Char.IsAsciiLetterOrDigit c || c = '_' || c = '-' then
                builder.Append(c) |> ignore
            else
                builder.Append('%') |> ignore
                builder.Append((int c).ToString("X4", CultureInfo.InvariantCulture)) |> ignore

        builder.ToString()

    /// <summary>
    /// The package scope prefix for one agent:
    /// <c>packages/{escapedTenant}/{agentId}/</c>.
    /// </summary>
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <returns>The scope prefix, ending with a slash.</returns>
    let agentScope (tenant: TenantId) (agentId: AgentId) =
        sprintf "%s/%s/%s/" packagesRoot (escapeTenant tenant) agentId.Value

    /// <summary>
    /// The entry prefix for one stored version:
    /// <c>packages/{escapedTenant}/{agentId}/{version}/</c>. The version
    /// is validated first, so it can never introduce a separator.
    /// </summary>
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <param name="version">The version to prefix, validated against the version rule.</param>
    /// <returns>The version entry prefix, ending with a slash.</returns>
    let versionPrefix (tenant: TenantId) (agentId: AgentId) (version: string) =
        sprintf "%s%s/" (agentScope tenant agentId) (PackageVersions.Validate version)

    /// <summary>
    /// The object key for one versioned entry: the version prefix plus
    /// the canonical entry path.
    /// </summary>
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <param name="version">The version holding the entry, validated against the version rule.</param>
    /// <param name="path">The entry path, normalised to canonical form.</param>
    /// <returns>The entry object key.</returns>
    let entryKey (tenant: TenantId) (agentId: AgentId) (version: string) (path: string) =
        sprintf "%s%s" (versionPrefix tenant agentId version) (AgentPackagePaths.Normalise path)

    /// <summary>
    /// The metadata document key for one version:
    /// <c>packages/{escapedTenant}/{agentId}/versions/{version}.json</c>.
    /// </summary>
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <param name="version">The version the document describes, validated against the version rule.</param>
    /// <returns>The version metadata object key.</returns>
    let versionMetadataKey (tenant: TenantId) (agentId: AgentId) (version: string) =
        sprintf "%sversions/%s.json" (agentScope tenant agentId) (PackageVersions.Validate version)

    /// <summary>
    /// The prefix every version metadata document lives under:
    /// <c>packages/{escapedTenant}/{agentId}/versions/</c>.
    /// </summary>
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <returns>The versions prefix, ending with a slash.</returns>
    let versionsPrefix (tenant: TenantId) (agentId: AgentId) =
        sprintf "%sversions/" (agentScope tenant agentId)

    /// <summary>
    /// Recovers the version from a version metadata object key, or null
    /// when the key is outside the versions prefix or misses the suffix.
    /// </summary>
    /// <param name="prefix">The versions prefix the key must start with.</param>
    /// <param name="key">The full object key to strip.</param>
    /// <returns>The version, or null when the key does not name a version document.</returns>
    let versionOfMetadataKey (prefix: string) (key: string) : string | null =
        if
            key.StartsWith(prefix, StringComparison.Ordinal)
            && key.EndsWith(".json", StringComparison.Ordinal)
            && key.Length > prefix.Length + ".json".Length
        then
            key.Substring(prefix.Length, key.Length - prefix.Length - ".json".Length)
        else
            null

    /// <summary>
    /// The active-version pointer key for one agent:
    /// <c>packages/{escapedTenant}/{agentId}/active.json</c>.
    /// </summary>
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <returns>The pointer object key.</returns>
    let pointerKey (tenant: TenantId) (agentId: AgentId) =
        sprintf "%sactive.json" (agentScope tenant agentId)

    /// <summary>
    /// The JSON options behind the pointer and version documents:
    /// camelCase, matching the codebase convention.
    /// </summary>
    let jsonOptions =
        let options = JsonSerializerOptions()
        options.PropertyNamingPolicy <- JsonNamingPolicy.CamelCase
        options

    /// <summary>
    /// The active-version pointer document: which version deploys read,
    /// and the host-supplied source label of the last write. Persisted
    /// as a string dictionary (a missing source reads back null), so the
    /// serializer never binds the record shape.
    /// </summary>
    type ActivePointer =
        {
            /// <summary>
            /// The active version.
            /// </summary>
            Version: string
            /// <summary>
            /// The host-supplied opaque source label, or null.
            /// </summary>
            Source: string | null
        }

    /// <summary>
    /// Reads an optional string entry off a decoded document: null when
    /// the key is absent.
    /// </summary>
    /// <param name="document">The decoded document.</param>
    /// <param name="key">The entry to read.</param>
    /// <returns>The entry value, or null when absent.</returns>
    let private optionalEntry (document: Dictionary<string, string>) (key: string) : string | null =
        match document.TryGetValue key with
        | true, value -> value
        | false, _ -> null

    /// <summary>
    /// Serialises the active-version pointer document.
    /// </summary>
    /// <param name="pointer">The pointer to serialise.</param>
    /// <returns>The document bytes, UTF-8 encoded.</returns>
    let serializePointer (pointer: ActivePointer) : byte[] =
        let document = Dictionary<string, string>()
        document["version"] <- pointer.Version

        match pointer.Source with
        | null -> ()
        | source -> document["source"] <- source

        JsonSerializer.SerializeToUtf8Bytes(document, jsonOptions)

    /// <summary>
    /// Deserialises the active-version pointer document.
    /// </summary>
    /// <param name="bytes">The document bytes.</param>
    /// <returns>The pointer.</returns>
    let deserializePointer (bytes: byte[]) : ActivePointer | null =
        let document =
            JsonSerializer.Deserialize<Dictionary<string, string>>(bytes, jsonOptions)

        match document with
        | null -> null
        | decoded ->
            match decoded.TryGetValue "version" with
            | false, _ -> raise (JsonException("The package pointer document has no version entry."))
            | true, version ->
                {
                    Version = version
                    Source = optionalEntry decoded "source"
                }

    /// <summary>
    /// One stored version's metadata document: the first-publication
    /// stamp replaces preserve, plus the source label of the last write
    /// to the version. Persisted as a string dictionary like the
    /// pointer, so the serializer never binds the record shape.
    /// </summary>
    type VersionMetadata =
        {
            /// <summary>
            /// When the version was first published, ISO 8601.
            /// </summary>
            CreatedAt: string
            /// <summary>
            /// The host-supplied opaque source label, or null.
            /// </summary>
            Source: string | null
        }

    /// <summary>
    /// Serialises a version metadata document.
    /// </summary>
    /// <param name="metadata">The metadata to serialise.</param>
    /// <returns>The document bytes, UTF-8 encoded.</returns>
    let serializeVersionMetadata (metadata: VersionMetadata) : byte[] =
        let document = Dictionary<string, string>()
        document["createdAt"] <- metadata.CreatedAt

        match metadata.Source with
        | null -> ()
        | source -> document["source"] <- source

        JsonSerializer.SerializeToUtf8Bytes(document, jsonOptions)

    /// <summary>
    /// Deserialises a version metadata document.
    /// </summary>
    /// <param name="bytes">The document bytes.</param>
    /// <returns>The metadata.</returns>
    let deserializeVersionMetadata (bytes: byte[]) : VersionMetadata | null =
        let document =
            JsonSerializer.Deserialize<Dictionary<string, string>>(bytes, jsonOptions)

        match document with
        | null -> null
        | decoded ->
            match decoded.TryGetValue "createdAt" with
            | false, _ -> raise (JsonException("The version document has no createdAt entry."))
            | true, createdAt ->
                {
                    CreatedAt = createdAt
                    Source = optionalEntry decoded "source"
                }
