// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.IO
open System.Text.Json.Serialization
open System.Text.RegularExpressions
open System.Threading
open System.Threading.Tasks

// Agent package contracts. IAgentPackageStore is the durable store contract
// for agent packages (the AGENTS.md instructions, .agent/skills, and
// .agent/agents folders the runtime loads per ARCHITECTURE.md's load step)
// versioned per agent, and IAgentPackageLeaseService coordinates concurrent
// package writers through a renewing lease. Every method takes a TenantId;
// the isolation is enforced in the stores, not only in the host. Path
// normalisation is contractual through AgentPackagePaths so every
// implementation's rules are tested once and cannot drift, mirroring
// BlobKeys; version strings are validated through PackageVersions, mirroring
// ToolNameRules. Entries are streams so ZIP uploads and GitHub folder
// mappings stream identically without materialising packages.
//
// Lease-agnostic writes: the store contract never checks a lease itself.
// Guarding writes with a lease is the caller's composition of the two
// services (acquire or WithLease, then write inside it); the store stays
// independently implementable, and the coordination-backed lease service
// stays independently implementable of the storage-backed package store.
//
// Result objects, not exceptions, for lease states: acquire and renew
// outcomes are expected branches (another writer holds the lease, the lease
// expired), mirroring the EventCleanupState and TurnLeaseState precedents.
// Control-plane preconditions still throw: WithLease throws
// PackageLeaseException when it cannot acquire or loses its lease.

// ───────────────────────────────────────────────────────────────────────────
// Path and version rules
//
// One helper, tested once: every store implementation validates through
// AgentPackagePaths so the rules cannot drift between backends.

/// The single shared helper for agent-package entry path normalisation and
/// validation, mirroring <see cref="T:Legate.BlobKeys" /> for blob keys.
/// Package paths are slash-separated relative paths: backslashes fold to
/// slashes, duplicate separators collapse, leading and trailing separators
/// vanish, and the result must be non-empty with every segment non-empty and
/// free of <c>.</c>, <c>..</c>, and NUL characters. Drive prefixes and
/// rooted paths throw. Normalisation is part of the store contract: every
/// store method validates through it, so the rules are tested once.
/// <exception cref="T:Legate.InvalidPackagePathException">A path fails normalisation or validation.</exception>
type AgentPackagePaths() =

    /// Splits and validates a raw package path into its segments, folding
    /// backslashes to slashes and rejecting the empty result, drive
    /// prefixes, and dot segments.
    /// <param name="path">The path to split.</param>
    /// <returns>The validated segments, in order.</returns>
    static member private SplitValidated(path: string) : string[] =
        if isNull (box path) then
            raise (ArgumentNullException(nameof path))

        if path.Length >= 2 && path[1] = ':' then
            raise (InvalidPackagePathException(path, "A package path must not carry a drive prefix."))

        let folded = path.Replace('\\', '/')
        let segments = folded.Split('/')

        let kept = ResizeArray<string>()

        for segment in segments do
            // Duplicated, leading, and trailing separators collapse because
            // empty segments are dropped rather than rejected.
            if segment.Length > 0 then
                if segment = "." || segment = ".." then
                    raise (InvalidPackagePathException(path, "A package path must not contain '.' or '..' segments."))

                if segment.Contains('\u0000') then
                    raise (InvalidPackagePathException(path, "A package path must not contain NUL characters."))

                kept.Add segment

        if kept.Count = 0 then
            raise (InvalidPackagePathException(path, "A package path must be non-empty after normalisation."))

        kept.ToArray()

    /// Normalises a package entry path and returns the canonical form: the
    /// path hosts and uploaders must present and what
    /// <see cref="T:Legate.AgentPackageEntry" />.Path and every store
    /// method key on.
    /// <param name="path">The raw path to normalise, for example ".agent\skills\deploy\SKILL.md".</param>
    /// <returns>The canonical slash-separated relative path with duplicate, leading, and trailing separators collapsed.</returns>
    /// <exception cref="T:System.ArgumentNullException">The path is null.</exception>
    /// <exception cref="T:Legate.InvalidPackagePathException">The path carries a drive prefix, contains a '.', '..', or NUL-containing segment, or normalises to empty.</exception>
    static member Normalise(path: string) : string =
        let segments = AgentPackagePaths.SplitValidated path
        String.Join("/", segments)

    /// Validates that a raw path normalises cleanly and returns the
    /// canonical form, the shared rule every store method applies to path
    /// parameters.
    /// <param name="path">The raw path to validate and canonicalise.</param>
    /// <returns>The canonical path.</returns>
    /// <exception cref="T:System.ArgumentNullException">The path is null.</exception>
    /// <exception cref="T:Legate.InvalidPackagePathException">The path fails normalisation.</exception>
    static member Validate(path: string) : string = AgentPackagePaths.Normalise path

/// The single shared rule for agent-package version strings, mirroring
/// <see cref="T:Legate.ToolNameRules" /> for tool names. Versions are
/// matched ordinally with no trimming, no culture, and no normalisation:
/// they are storage keys, not parsed semver.
module internal PackageVersionsInternals =

    let Pattern = @"\A[A-Za-z0-9][A-Za-z0-9._-]{0,63}\z"

    let PatternRegex = Regex(Pattern, RegexOptions.CultureInvariant)

/// Static validation for agent-package version strings. A version is the
/// name a package version is stored and listed under; it is matched
/// ordinally and used exactly as given.
/// <exception cref="T:System.ArgumentException">A version passed to Validate fails the pattern.</exception>
type PackageVersions() =

    /// The single version rule, shared by Validate and TryValidate so the
    /// contract lives in one place: one to 64 characters of ASCII letters,
    /// digits, dots, underscores, or dashes, starting with an ASCII letter
    /// or digit (<c>[A-Za-z0-9][A-Za-z0-9._-]{0,63}</c>), anchored with
    /// <c>\A</c> and <c>\z</c> so the whole string, including its last
    /// character, must match. Matched ordinally: no trimming, no culture,
    /// no normalisation; the version is used exactly as given.
    static member Pattern: string = PackageVersionsInternals.Pattern

    /// The one version rule shared by Validate and TryValidate: matched
    /// ordinally against <see cref="P:Legate.PackageVersions.Pattern" />
    /// with no trimming, so input is used exactly as given.
    /// <param name="version">The version string to test.</param>
    /// <returns>true when the version matches the pattern; otherwise false.</returns>
    static member private VersionIsValid(version: string) =
        PackageVersionsInternals.PatternRegex.IsMatch version

    /// Validates a version string and returns it unchanged.
    /// <param name="version">The version to validate, for example "1.4.2".</param>
    /// <returns>The validated version, unchanged.</returns>
    /// <exception cref="T:System.ArgumentNullException">The version is null.</exception>
    /// <exception cref="T:System.ArgumentException">The version is empty, exceeds 64 characters, starts with a dot, underscore, or dash, or contains any character outside [A-Za-z0-9._-] (spaces, slashes, colons, trailing line feeds, or non-ASCII included).</exception>
    static member Validate(version: string) : string =
        if isNull (box version) then
            raise (ArgumentNullException(nameof version))

        if not (PackageVersions.VersionIsValid version) then
            raise (
                ArgumentException(
                    "A package version must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}: one to 64 ASCII letters, digits, dots, underscores, or dashes, starting with a letter or digit.",
                    nameof version
                )
            )

        version

    /// Attempts to validate a version string; returns false for null and
    /// for any version that fails
    /// <see cref="P:Legate.PackageVersions.Pattern" />.
    /// <param name="version">The version string to test.</param>
    /// <returns>true when the version matches the pattern; otherwise false.</returns>
    static member TryValidate(version: string | null) : bool =
        if isNull (box version) then
            false
        else
            let version = Option.ofObj version |> Option.defaultValue ""

            PackageVersions.VersionIsValid version

// ───────────────────────────────────────────────────────────────────────────
// Package shapes

/// One entry of a package upload: the entry's relative path and its content
/// as a stream. The store consumes streams so ZIP uploads and GitHub folder
/// mappings stream identically; large packages are never materialised into
/// byte arrays. The path is normalised at construction time, so every
/// consumer sees the canonical form.
/// <param name="path">The entry's relative path, normalised through <see cref="M:Legate.AgentPackagePaths.Normalise*" />. Must not be null.</param>
/// <param name="content">The entry's content. Must not be null.</param>
[<Sealed>]
type AgentPackageEntry(path: string, content: Stream) =

    do
        if isNull (box path) then
            raise (ArgumentNullException(nameof path))

        if isNull (box content) then
            raise (ArgumentNullException(nameof content))

        // Normalised on entry: construction fails fast on an invalid path,
        // so no consumer can hold a non-canonical entry.
        AgentPackagePaths.Normalise path |> ignore

    // The canonical form is validated once here; members re-normalise so
    // the helper stays the single source of the rules.
    member _.Path: string = AgentPackagePaths.Normalise path

    /// The entry's content stream. Must not be null.
    member _.Content: Stream = content

    /// Compares two entries ordinally across canonical path and content
    /// reference.
    /// <param name="other">The entry to compare against.</param>
    /// <returns>true when both entries carry the same canonical path and the same content reference.</returns>
    member this.Equals(other: AgentPackageEntry) =
        String.Equals(this.Path, other.Path, StringComparison.Ordinal)
        && obj.ReferenceEquals(this.Content, other.Content)

    /// Hashes the canonical path.
    override _.GetHashCode() =
        StringComparer.Ordinal.GetHashCode(AgentPackagePaths.Normalise path)

    /// Compares against a boxed entry without recursing.
    /// <param name="other">The object to compare against.</param>
    /// <returns>true when the other object is an equal entry.</returns>
    override this.Equals(other: obj | null) =
        match other with
        | :? AgentPackageEntry as entry -> this.Equals(entry: AgentPackageEntry)
        | _ -> false

    /// Implements ordinal equality for the generic collection surface.
    /// <param name="other">The entry to compare against.</param>
    /// <returns>true when both entries carry the same canonical path and the same content reference.</returns>
    interface IEquatable<AgentPackageEntry> with
        member this.Equals(other: AgentPackageEntry | null) =
            match other with
            | null -> false
            | valid -> this.Equals(valid: AgentPackageEntry)

    /// Returns the canonical path.
    override _.ToString() = AgentPackagePaths.Normalise path

/// Server-side metadata about an agent's package, derived from the stored
/// package layout (<c>AGENTS.md</c>, <c>.agent/skills/*/SKILL.md</c>,
/// <c>.agent/agents/*/AGENT.md</c>, per ARCHITECTURE.md's load step). A
/// reference type so the pinned absent-package semantics ("returns null when
/// nothing was uploaded") can use nullability on the interface surface.
[<Sealed>]
type AgentPackageInfo
    (
        instructions: string | null,
        skills: IReadOnlyList<string>,
        subAgents: IReadOnlyList<string>,
        source: string | null,
        activeVersion: string | null
    ) =

    do
        if isNull (box skills) then
            raise (ArgumentNullException(nameof skills))

        if isNull (box subAgents) then
            raise (ArgumentNullException(nameof subAgents))

    /// The AGENTS.md instructions text, or null when the package has none.
    member _.Instructions: string | null = instructions

    /// The skill names under .agent/skills, sorted lexicographically.
    member _.Skills: IReadOnlyList<string> = skills

    /// The sub-agent names under .agent/agents, sorted lexicographically.
    member _.SubAgents: IReadOnlyList<string> = subAgents

    /// The host-supplied opaque source label of the package's last write,
    /// or null when the package has never been written.
    member _.Source: string | null = source

    /// The active version string, or null when no version has ever been
    /// uploaded or the active version was deleted.
    member _.ActiveVersion: string | null = activeVersion

/// A stored package version's metadata. A reference type so ListVersions
/// returns a homogeneous C#-friendly list.
/// <param name="version">The version string.</param>
/// <param name="createdAt">When the version was created.</param>
[<Sealed>]
type AgentPackageVersion(version: string, createdAt: DateTimeOffset) =

    do
        if isNull (box version) then
            raise (ArgumentNullException(nameof version))

    /// The version string.
    member _.Version = version

    /// When the version was created. Re-uploading an existing version
    /// replaces its contents but preserves this stamp, so it reflects the
    /// version's first publication.
    member _.CreatedAt: DateTimeOffset = createdAt

    /// Compares two versions ordinally across all fields.
    /// <param name="other">The version to compare against.</param>
    /// <returns>true when version and creation stamp both match.</returns>
    member this.Equals(other: AgentPackageVersion) =
        String.Equals(this.Version, other.Version, StringComparison.Ordinal)
        && this.CreatedAt = other.CreatedAt

    /// Hashes all fields ordinally.
    override _.GetHashCode() =
        let mutable hash = 17
        hash <- hash * 31 + StringComparer.Ordinal.GetHashCode version
        hash <- hash * 31 + createdAt.GetHashCode()
        hash

    /// Compares against a boxed version without recursing.
    /// <param name="other">The object to compare against.</param>
    /// <returns>true when the other object is an equal version.</returns>
    override this.Equals(other: obj | null) =
        match other with
        | :? AgentPackageVersion as version -> this.Equals(version: AgentPackageVersion)
        | _ -> false

    /// Implements ordinal equality for the generic collection surface.
    /// <param name="other">The version to compare against.</param>
    /// <returns>true when version and creation stamp both match.</returns>
    interface IEquatable<AgentPackageVersion> with
        member this.Equals(other: AgentPackageVersion | null) =
            match other with
            | null -> false
            | valid -> this.Equals(valid: AgentPackageVersion)

    /// Returns the version string.
    override _.ToString() = version

// ───────────────────────────────────────────────────────────────────────────
// The package store contract

/// The durable store contract for agent packages: the versioned contents
/// the runtime loads per ARCHITECTURE.md's load step. Versions are keyed by
/// tenant, agent, and validated version string; the layout
/// (<c>AGENTS.md</c>, <c>.agent/skills/*/SKILL.md</c>,
/// <c>.agent/agents/*/AGENT.md</c>) is normative and
/// <see cref="M:Legate.IAgentPackageStore.GetPackageInfo*" /> derives from
/// it. Every method takes the tenant the data belongs to and must not see
/// or touch another tenant's rows (isolation is enforced here, not only in
/// the host). The contract does not require the agent to exist in
/// <see cref="T:Legate.IAgentStore" />: the package store is decoupled from
/// the agent-row store, and a host may stage packages before the agent row
/// exists.
///
/// <para>Lease-agnostic writes: this contract never checks a package lease
/// itself; guarding writes with
/// <see cref="T:Legate.IAgentPackageLeaseService" /> is the caller's
/// composition of the two services, so the store stays implementable from
/// plain storage (S3, file system) and the lease service stays implementable
/// from plain coordination.</para>
///
/// <para>Active-version semantics: <see cref="M:Legate.IAgentPackageStore.UploadPackage*" />
/// publishes a version and makes it active (deploy semantics: activation
/// folds into the publish, and there is no separate activate method).
/// <see cref="M:Legate.IAgentPackageStore.ReplacePackageVersion*" /> never
/// changes active status. <see cref="M:Legate.IAgentPackageStore.DeletePackageVersion*" />
/// on the active version clears the active version to null, deliberately and
/// idempotently: nothing else auto-promotes, so the next upload's activation
/// is the only path back to an active version.</para>
type IAgentPackageStore =

    /// Reads the agent package's derived info: instructions from AGENTS.md,
    /// the skill and sub-agent directory names, the host-supplied source
    /// label of the last write, and the active version. Returns null when
    /// no version has ever been uploaded for the agent in the tenant.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose package to describe.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The package info, or null when nothing was ever uploaded.</returns>
    abstract GetPackageInfo:
        tenant: TenantId * agentId: AgentId * cancellationToken: CancellationToken -> Task<AgentPackageInfo | null>

    /// Reads one file from one stored version. Returns null when the
    /// version or path is absent, mirroring
    /// <see cref="M:Legate.IBlobStore.Get*" />, so missing reads are an
    /// expected branch and never an exception. The stream is readable from
    /// its start; large entries are not materialised into memory.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose package to read from.</param>
    /// <param name="version">The version to read from, validated against <see cref="P:Legate.PackageVersions.Pattern" />.</param>
    /// <param name="path">The entry's relative path, normalised through <see cref="M:Legate.AgentPackagePaths.Normalise*" />.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>A readable stream over the entry's bytes, or null when the version or path is absent.</returns>
    /// <exception cref="T:System.ArgumentException">The version fails <see cref="P:Legate.PackageVersions.Pattern" />.</exception>
    /// <exception cref="T:Legate.InvalidPackagePathException">The path fails normalisation.</exception>
    abstract ReadFile:
        tenant: TenantId * agentId: AgentId * version: string * path: string * cancellationToken: CancellationToken ->
            Task<Stream | null>

    /// Lists the entry paths stored under one version's prefix, in
    /// lexicographic (ordinal) order: the prefix scan skill discovery stages
    /// companions with. The prefix is a package-relative directory path,
    /// normalised through <see cref="M:Legate.AgentPackagePaths.Normalise*" />
    /// (a trailing separator is collapsed, so a skill directory scans as
    /// either <c>.agent/skills/deploy</c> or
    /// <c>.agent/skills/deploy/</c>); a stored path matches when it equals
    /// the normalised prefix or starts with the prefix plus a separator.
    /// Empty when the version has no entries under the prefix, including
    /// when the version itself is absent: absent data is an expected branch
    /// and never an exception, mirroring
    /// <see cref="M:Legate.IAgentPackageStore.ReadFile*" />.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose package to scan.</param>
    /// <param name="version">The version to scan, validated against <see cref="P:Legate.PackageVersions.Pattern" />.</param>
    /// <param name="prefix">The package-relative directory prefix to scan, normalised through <see cref="M:Legate.AgentPackagePaths.Normalise*" />.</param>
    /// <param name="cancellationToken">Token that abandons the scan.</param>
    /// <returns>The matching canonical paths, lexicographically ordered; empty when none match.</returns>
    /// <exception cref="T:System.ArgumentNullException">The prefix is null.</exception>
    /// <exception cref="T:System.ArgumentException">The version fails <see cref="P:Legate.PackageVersions.Pattern" />.</exception>
    /// <exception cref="T:Legate.InvalidPackagePathException">The prefix fails normalisation.</exception>
    abstract ListFiles:
        tenant: TenantId * agentId: AgentId * version: string * prefix: string * cancellationToken: CancellationToken ->
            Task<IReadOnlyList<string>>

    /// Publishes a package version from a stream of entries and makes it
    /// the active version: deploy semantics, folding activation into the
    /// one write that semantically publishes. Atomic: a failed upload
    /// leaves no partial version, and the previously active version stays
    /// active. Duplicate normalised paths in one upload throw
    /// <see cref="T:System.ArgumentException" /> before anything is
    /// stored. Re-uploading an existing version replaces its contents
    /// atomically and leaves it active; the version's CreatedAt is
    /// preserved. The source label is opaque host data stored with the
    /// package and surfaced by
    /// <see cref="M:Legate.IAgentPackageStore.GetPackageInfo*" />.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose package to upload.</param>
    /// <param name="version">The version to publish, validated against <see cref="P:Legate.PackageVersions.Pattern" />.</param>
    /// <param name="source">The host-supplied opaque source label, for example "github-sync" or "manual-zip". Must not be null.</param>
    /// <param name="entries">The entries to store. Every path is normalised; duplicate normalised paths are rejected.</param>
    /// <param name="cancellationToken">Token that abandons the upload.</param>
    /// <returns>The version's metadata, with CreatedAt stamped by the store.</returns>
    /// <exception cref="T:System.ArgumentNullException">The source is null.</exception>
    /// <exception cref="T:System.ArgumentException">The version fails <see cref="P:Legate.PackageVersions.Pattern" /> or two entries normalise to the same path.</exception>
    /// <exception cref="T:Legate.InvalidPackagePathException">An entry path fails normalisation.</exception>
    abstract UploadPackage:
        tenant: TenantId *
        agentId: AgentId *
        version: string *
        source: string *
        entries: IAsyncEnumerable<AgentPackageEntry> *
        cancellationToken: CancellationToken ->
            Task<AgentPackageVersion>

    /// Atomically replaces one existing version's contents and never
    /// changes active status: the GitHub-sync refresh path, where a
    /// version's contents are re-read and re-written in place while older
    /// and newer versions stay untouched. The version's CreatedAt is
    /// preserved. An unknown version throws
    /// <see cref="T:System.ArgumentException" />; duplicate normalised
    /// paths throw <see cref="T:System.ArgumentException" />.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose version to replace.</param>
    /// <param name="version">The existing version to replace, validated against <see cref="P:Legate.PackageVersions.Pattern" />.</param>
    /// <param name="source">The host-supplied opaque source label of the replacing write. Must not be null.</param>
    /// <param name="entries">The replacement entries. Every path is normalised; duplicate normalised paths are rejected.</param>
    /// <param name="cancellationToken">Token that abandons the replace.</param>
    /// <returns>The version's metadata, with CreatedAt preserved.</returns>
    /// <exception cref="T:System.ArgumentNullException">The source is null.</exception>
    /// <exception cref="T:System.ArgumentException">The version is unknown, fails <see cref="P:Legate.PackageVersions.Pattern" />, or two entries normalise to the same path.</exception>
    /// <exception cref="T:Legate.InvalidPackagePathException">An entry path fails normalisation.</exception>
    abstract ReplacePackageVersion:
        tenant: TenantId *
        agentId: AgentId *
        version: string *
        source: string *
        entries: IAsyncEnumerable<AgentPackageEntry> *
        cancellationToken: CancellationToken ->
            Task<AgentPackageVersion>

    /// Deletes one version. Idempotent: deleting an absent version returns
    /// false and changes nothing. Deleting the active version clears the
    /// active version to null: documented and deliberate, so a deleted
    /// version is never silently served. Nothing else auto-activates.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose version to delete.</param>
    /// <param name="version">The version to delete, validated against <see cref="P:Legate.PackageVersions.Pattern" />.</param>
    /// <param name="cancellationToken">Token that abandons the delete.</param>
    /// <returns>true when the version existed and was deleted; false when it was already absent.</returns>
    /// <exception cref="T:System.ArgumentException">The version fails <see cref="P:Legate.PackageVersions.Pattern" />.</exception>
    abstract DeletePackageVersion:
        tenant: TenantId * agentId: AgentId * version: string * cancellationToken: CancellationToken -> Task<bool>

    /// Lists the agent's stored versions in lexicographic order, unpaged:
    /// a package's versions are few. Empty when the agent has none.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose versions to list.</param>
    /// <param name="cancellationToken">Token that abandons the list.</param>
    /// <returns>The stored versions, lexicographically ordered; empty when none.</returns>
    abstract ListVersions:
        tenant: TenantId * agentId: AgentId * cancellationToken: CancellationToken ->
            Task<IReadOnlyList<AgentPackageVersion>>

// ───────────────────────────────────────────────────────────────────────────
// Package leases
//
// Coordination for concurrent package writers (manual ZIP upload, GitHub
// sync, host tooling). The lease covers the agent's whole package, not one
// version, because a writer replaces versions wholesale. The lease service
// is implementable independently of the package store: the store never
// checks leases, and guarding writes is the caller's composition.

/// A granted lease over an agent's whole package. The token is opaque,
/// minted by the service, and never parsed: callers present it verbatim on
/// renew, verify, and release. Immutable: renewal returns a fresh instance
/// with the new expiry rather than mutating this one.
[<Sealed>]
type AgentPackageLease(tenant: TenantId, agentId: AgentId, owner: string, token: string, expiresAt: DateTimeOffset) =

    do
        if isNull (box owner) then
            raise (ArgumentNullException(nameof owner))

        if isNull (box token) then
            raise (ArgumentNullException(nameof token))

    /// The tenant the leased package belongs to.
    member _.Tenant = tenant

    /// The agent whose package is leased.
    member _.AgentId = agentId

    /// The lease owner identity, for example the sync worker or upload
    /// session that holds it. Never contains secrets.
    member _.Owner = owner

    /// The opaque lease token, minted by the service and never parsed.
    /// Callers present it verbatim; stores fence on it at the last moment.
    member _.Token: string = token

    /// When the lease expires unless renewed.
    member _.ExpiresAt: DateTimeOffset = expiresAt

    /// Compares two leases ordinally across all fields.
    /// <param name="other">The lease to compare against.</param>
    /// <returns>true when tenant, agent, owner, token, and expiry all match.</returns>
    member this.Equals(other: AgentPackageLease) =
        this.Tenant.Equals other.Tenant
        && this.AgentId.Equals other.AgentId
        && String.Equals(this.Owner, other.Owner, StringComparison.Ordinal)
        && String.Equals(this.Token, other.Token, StringComparison.Ordinal)
        && this.ExpiresAt = other.ExpiresAt

    /// Hashes all fields.
    override _.GetHashCode() =
        let mutable hash = 17
        hash <- hash * 31 + StringComparer.Ordinal.GetHashCode tenant.Value
        hash <- hash * 31 + StringComparer.Ordinal.GetHashCode agentId.Value
        hash <- hash * 31 + StringComparer.Ordinal.GetHashCode owner
        hash <- hash * 31 + StringComparer.Ordinal.GetHashCode token
        hash <- hash * 31 + expiresAt.GetHashCode()
        hash

    /// Compares against a boxed lease without recursing.
    /// <param name="other">The object to compare against.</param>
    /// <returns>true when the other object is an equal lease.</returns>
    override this.Equals(other: obj | null) =
        match other with
        | :? AgentPackageLease as lease -> this.Equals(lease: AgentPackageLease)
        | _ -> false

    /// Implements ordinal equality for the generic collection surface.
    /// <param name="other">The lease to compare against.</param>
    /// <returns>true when tenant, agent, owner, token, and expiry all match.</returns>
    interface IEquatable<AgentPackageLease> with
        member this.Equals(other: AgentPackageLease | null) =
            match other with
            | null -> false
            | valid -> this.Equals(valid: AgentPackageLease)

/// What the service decided when a package-lease acquire or renew call
/// landed: the caller must branch on the outcome. A result object, never an
/// exception: another writer holding the lease or a lost lease is an
/// expected branch of concurrent writing, mirroring
/// <see cref="T:Legate.EventCleanupState" />. Serialises polymorphically:
/// every concrete state carries a stable <c>$type</c> discriminator on the
/// wire.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<PackageLeaseAcquired>, "packageLeaseAcquired")>]
[<JsonDerivedType(typeof<PackageLeaseUnavailable>, "packageLeaseUnavailable")>]
type PackageLeaseState() = class end

/// The lease was granted: the caller may write the agent's package under
/// the lease's token until it expires, keeping it alive through renew.
/// <param name="lease">The granted lease.</param>
and [<Sealed>] PackageLeaseAcquired(lease: AgentPackageLease) =
    inherit PackageLeaseState()

    /// The granted lease, carrying the token renew, verify, and release
    /// must present.
    member _.Lease = lease

/// The lease was not granted: another writer holds the agent's package
/// lease and it has not expired. Nothing changed; the caller retries later
/// or gives up.
/// <param name="tenant">The tenant whose package was requested.</param>
/// <param name="agentId">The agent whose package was requested.</param>
/// <param name="reason">Why the lease was not granted. Never contains secrets or tool arguments.</param>
and [<Sealed>] PackageLeaseUnavailable(tenant: TenantId, agentId: AgentId, reason: string) =
    inherit PackageLeaseState()

    /// The tenant whose package was requested.
    member _.Tenant = tenant

    /// The agent whose package was requested.
    member _.AgentId = agentId

    /// Why the lease was not granted: "leaseHeld" while another owner's
    /// lease has not expired. Never contains secrets or tool arguments.
    member _.Reason = reason

/// What the service decided when a lease renew landed: the caller must
/// branch on the outcome. A result object, never an exception: a lost lease
/// is an expected branch of the lease lifecycle (it expired and another
/// writer re-acquired), mirroring <see cref="T:Legate.EventCleanupState" />.
/// Serialises polymorphically: every concrete renewal carries a stable
/// <c>$type</c> discriminator on the wire.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<PackageLeaseRenewed>, "packageLeaseRenewed")>]
[<JsonDerivedType(typeof<PackageLeaseLost>, "packageLeaseLost")>]
type PackageLeaseRenewal() = class end

/// The renewal was applied: the lease now carries a fresh token and a new
/// expiry, and the old lease instance is stale.
/// <param name="lease">The renewed lease, with a fresh token and expiry.</param>
and [<Sealed>] PackageLeaseRenewed(lease: AgentPackageLease) =
    inherit PackageLeaseRenewal()

    /// The renewed lease, with a fresh token and the new expiry. The
    /// instance passed to renew is stale after this.
    member _.Lease = lease

/// The renewal was rejected because the lease is no longer held by this
/// token: it expired and another writer re-acquired, or it was released.
/// Nothing was renewed; the caller must stop writing immediately.
/// <param name="reason">Why the renewal was lost. Never contains secrets or tool arguments.</param>
and [<Sealed>] PackageLeaseLost(reason: string) =
    inherit PackageLeaseRenewal()

    /// Why the renewal was lost: "staleToken" when the token no longer
    /// owns the lease (it expired or was released), or "leaseExpired" when
    /// the lease lapsed before the renewal landed. Never contains secrets
    /// or tool arguments.
    member _.Reason = reason

/// Tuning for <see cref="M:Legate.IAgentPackageLeaseService.WithLease*" />:
/// a plain mutable class with defaults, mirroring the options-class
/// convention. Defaults document the pinned semantics: renew every 10
/// seconds, and a renewal that takes longer than 5 seconds counts as
/// failed.
type PackageLeaseOptions() =

    /// How often the held lease is renewed during the work. Default 10
    /// seconds.
    member val RenewInterval: TimeSpan = TimeSpan.FromSeconds 10. with get, set

    /// How long a single renewal may take before it counts as failed and
    /// the work token is cancelled. Default 5 seconds.
    member val RenewalTimeout: TimeSpan = TimeSpan.FromSeconds 5. with get, set

/// Coordination for concurrent package writers: a renewing, fenced lease
/// over one agent's whole package. Acquire and renew return result objects
/// the caller branches on (another writer holds the lease; a renewal is
/// lost); verify and release return plain predicates; and
/// <see cref="M:Legate.IAgentPackageLeaseService.WithLease*" /> wraps work
/// with the pinned semantics: acquire first (failure throws
/// <see cref="T:Legate.PackageLeaseException" />), renew every
/// <see cref="P:Legate.PackageLeaseOptions.RenewInterval" />, a renewal
/// that is lost or exceeds
/// <see cref="P:Legate.PackageLeaseOptions.RenewalTimeout" /> immediately
/// cancels the work token (the cancelled work surfaces
/// <see cref="T:System.OperationCanceledException" /> and WithLease throws
/// <see cref="T:Legate.PackageLeaseException" />), and release runs
/// best-effort in a finally block.
///
/// <para>Lease-agnostic stores: this service composes with, but never
/// enforces on, <see cref="T:Legate.IAgentPackageStore" />; the store stays
/// independently implementable and the fence is the caller's composition.
/// The opaque token is verified at the last moment: a correlation id is
/// evidence, not authority.</para>
type IAgentPackageLeaseService =

    /// Acquires a lease over the agent's package. A result object, never
    /// an exception: another writer holding the lease is an expected
    /// branch.
    /// <param name="tenant">The tenant whose package to lease.</param>
    /// <param name="agentId">The agent whose package to lease.</param>
    /// <param name="owner">The caller's owner identity, for example the sync worker or upload session. Must not be null.</param>
    /// <param name="leaseDuration">How long the lease lasts without renewal.</param>
    /// <param name="cancellationToken">Token that abandons the acquire.</param>
    /// <returns>The granted lease, or the unavailable outcome with the reason.</returns>
    /// <exception cref="T:System.ArgumentNullException">The owner is null.</exception>
    abstract Acquire:
        tenant: TenantId *
        agentId: AgentId *
        owner: string *
        leaseDuration: TimeSpan *
        cancellationToken: CancellationToken ->
            Task<PackageLeaseState>

    /// Renews a held lease. Fenced: the lease's opaque token is verified
    /// at the last moment, and a lost renewal is an expected branch
    /// returned as the lost outcome.
    /// <param name="lease">The lease to renew.</param>
    /// <param name="leaseDuration">The renewed duration from the renewal instant.</param>
    /// <param name="cancellationToken">Token that abandons the renewal.</param>
    /// <returns>The renewed lease with a fresh token and expiry, or the lost outcome with the reason.</returns>
    abstract Renew:
        lease: AgentPackageLease * leaseDuration: TimeSpan * cancellationToken: CancellationToken ->
            Task<PackageLeaseRenewal>

    /// Reports whether the lease is still held by its token.
    /// <param name="lease">The lease to verify.</param>
    /// <param name="cancellationToken">Token that abandons the check.</param>
    /// <returns>true when the token still holds the lease; otherwise false.</returns>
    abstract Verify: lease: AgentPackageLease * cancellationToken: CancellationToken -> Task<bool>

    /// Releases the lease. Best-effort: a released or expired lease is an
    /// expected no-op.
    /// <param name="lease">The lease to release.</param>
    /// <param name="cancellationToken">Token that abandons the release.</param>
    /// <returns>true when this call released the lease; false when it was already released or expired.</returns>
    abstract Release: lease: AgentPackageLease * cancellationToken: CancellationToken -> Task<bool>

    /// Runs work while holding the lease. Semantics, pinned:
    /// <list type="bullet">
    /// <item><description>Acquire first: an unavailable acquire throws
    /// <see cref="T:Legate.PackageLeaseException" />.</description></item>
    /// <item><description>Renew every
    /// <see cref="P:Legate.PackageLeaseOptions.RenewInterval" /> while the
    /// work runs.</description></item>
    /// <item><description>A renewal that is lost, or that exceeds
    /// <see cref="P:Legate.PackageLeaseOptions.RenewalTimeout" />, counts
    /// as failed and immediately cancels the work token; the cancelled
    /// work surfaces <see cref="T:System.OperationCanceledException" />,
    /// and WithLease throws
    /// <see cref="T:Legate.PackageLeaseException" />.</description></item>
    /// <item><description>Release runs best-effort in a finally block,
    /// also on failure paths.</description></item>
    /// </list>
    /// <param name="tenant">The tenant whose package to lease.</param>
    /// <param name="agentId">The agent whose package to lease.</param>
    /// <param name="owner">The caller's owner identity. Must not be null.</param>
    /// <param name="leaseDuration">How long the lease lasts without renewal.</param>
    /// <param name="options">The renewal tuning, or the defaults when null.</param>
    /// <param name="work">The work to run while the lease is held. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the lease lifecycle.</param>
    /// <returns>The work's result.</returns>
    /// <exception cref="T:Legate.PackageLeaseException">The lease could not be acquired, or was lost or timed out during the work.</exception>
    abstract WithLease:
        tenant: TenantId *
        agentId: AgentId *
        owner: string *
        leaseDuration: TimeSpan *
        options: PackageLeaseOptions | null *
        work: Func<CancellationToken, Task<'T>> *
        cancellationToken: CancellationToken ->
            Task<'T>

    /// The non-generic form of WithLease for work that returns no value.
    /// <param name="tenant">The tenant whose package to lease.</param>
    /// <param name="agentId">The agent whose package to lease.</param>
    /// <param name="owner">The caller's owner identity. Must not be null.</param>
    /// <param name="leaseDuration">How long the lease lasts without renewal.</param>
    /// <param name="options">The renewal tuning, or the defaults when null.</param>
    /// <param name="work">The work to run while the lease is held. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the lease lifecycle.</param>
    /// <returns>A task completing when the work and release both finish.</returns>
    /// <exception cref="T:Legate.PackageLeaseException">The lease could not be acquired, or was lost or timed out during the work.</exception>
    abstract WithLease:
        tenant: TenantId *
        agentId: AgentId *
        owner: string *
        leaseDuration: TimeSpan *
        options: PackageLeaseOptions | null *
        work: Func<CancellationToken, Task> *
        cancellationToken: CancellationToken ->
            Task
