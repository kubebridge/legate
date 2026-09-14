// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.InMemory

open System
open System.Collections.Generic
open Legate

/// Options for the in-memory stores: the journal limits the event store
/// enforces before any write lands. All values are optional and default to
/// unbounded (0); a positive value bounds the corresponding dimension. The
/// limits mirror the host-configured runtime options the
/// <see cref="T:Legate.ISessionEventStore" /> contract names: the contract
/// fixes only how a breach is reported
/// (<see cref="T:Legate.EventLimitExceededException" /> before any part of
/// the batch lands), not the values.
type InMemoryStoreOptions() =

    /// The maximum size of one journaled event, in bytes, estimated as the
    /// UTF-8 encoded length of the event's serialised JSON form. 0 means
    /// unbounded.
    member val MaxEventBytes: int64 = 0L with get, set

    /// The maximum number of events one session's journal may hold. 0 means
    /// unbounded.
    member val MaxEventsPerSession: int64 = 0L with get, set

    /// The maximum total bytes one session's journal may hold. 0 means
    /// unbounded.
    member val MaxJournalBytesPerSession: int64 = 0L with get, set

    /// The maximum number of events one append may carry. 0 means unbounded.
    member val MaxAppendBatchSize: int = 0 with get, set

/// The shared in-memory database behind every store: one gate lock, every
/// table, and the injected clock lease expiry reads. The event store fences
/// appends on the session store's claim tokens and the custom-tool store
/// resolves agents through the agent rows, so one database instance backs
/// every store the runtime registers; constructing separate databases
/// splits that shared state. The <see cref="T:Legate.Storage.InMemory.InMemoryStoreFactory" />
/// module hands out every store over this one instance.
///
/// <para>Threading: all mutations run under one lock, so every store call is
/// atomic against every other call on the same database instance. This
/// makes the in-memory stores correct, not concurrent: throughput is not
/// the goal, tests and samples are.</para>
type InMemoryDatabase(timeProvider: TimeProvider, options: InMemoryStoreOptions) =

    do
        if isNull (box timeProvider) then
            raise (ArgumentNullException(nameof timeProvider))

        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

    // One lock guards every table: the event store fences appends on the
    // session store's claim tokens and the custom-tool store resolves
    // agents through the agent rows, so cross-store transitions need one
    // critical section.
    let gate = obj ()

    // Sessions keyed by (tenant, session id).
    let sessions = Dictionary<(TenantId * SessionId), Session>()

    // Inbox rows per (tenant, session id), append-ordered.
    let inboxes = Dictionary<(TenantId * SessionId), List<InboxEntry>>()

    // Next inbox position per (tenant, session id).
    let inboxPositions = Dictionary<(TenantId * SessionId), int64>()

    // Open turns per (tenant, session id): the rows claims mint from. A
    // turn exists here from claim (message consumed) until settle/abort.
    let openTurns = Dictionary<(TenantId * SessionId), OpenTurnRow>()

    // The live claim per (tenant, session id): one live claim per session.
    let liveClaims = Dictionary<(TenantId * SessionId), TurnClaim>()

    // Terminal settlements per (tenant, session id, turn id): first
    // outcome wins; retries observe it.
    let settlements = Dictionary<(TenantId * SessionId * TurnId), TurnStatus>()

    // Usage checkpoints per (tenant, session id, turn id): last write wins.
    let usageCheckpoints = Dictionary<(TenantId * SessionId * TurnId), UsageSummary>()

    // The current turn id per (tenant, session id): set on claim, cleared
    // on settle/abort, the input of CurrentTurnId-based state rules.
    let currentTurnIds = Dictionary<(TenantId * SessionId), TurnId>()

    // The structured outcome recorded at settlement, per settled turn.
    let outcomes = Dictionary<(TenantId * SessionId * TurnId), TurnOutcome | null>()

    // Journals per (tenant, session id): the ordered events and the flag
    // telling replay the journal was archived away.
    let journals = Dictionary<(TenantId * SessionId), JournalRow>()

    // Cleanup leases per (tenant, session id).
    let cleanupLeases = Dictionary<(TenantId * SessionId), EventCleanupClaim>()

    // Agents keyed by (tenant, agent id).
    let agents = Dictionary<(TenantId * AgentId), Agent>()

    // Custom tools per (tenant, agent id), keyed by tool name.
    let customTools =
        Dictionary<(TenantId * AgentId), Dictionary<string, AgentCustomTool>>()

    // Blobs keyed by the validated blob key.
    let blobs = Dictionary<string, BlobRow>()

    // Agent packages per (tenant, agent id).
    let packages = Dictionary<(TenantId * AgentId), PackageRow>()

    /// Constructs the database with the system clock and default (unbounded)
    /// options.
    new() = InMemoryDatabase(TimeProvider.System, InMemoryStoreOptions())

    /// Constructs the database with the given clock and default (unbounded)
    /// options.
    /// <param name="timeProvider">The clock lease expiry reads.</param>
    new(timeProvider: TimeProvider) = InMemoryDatabase(timeProvider, InMemoryStoreOptions())
    /// The one gate every store locks. Stores take it for multi-table
    /// transitions; single-table calls may rely on the stores' own locking
    /// discipline that funnels through this same gate.
    member internal _.Gate = gate

    /// The session rows, keyed by (tenant, session id). Internal: stores
    /// mutate through the gate only.
    member internal _.Sessions = sessions

    /// The inbox rows per (tenant, session id), in append order.
    member internal _.Inboxes = inboxes

    /// The next inbox position per (tenant, session id).
    member internal _.InboxPositions = inboxPositions

    /// The open turns per (tenant, session id).
    member internal _.OpenTurns = openTurns

    /// The live claim per (tenant, session id).
    member internal _.LiveClaims = liveClaims

    /// The terminal settlement per (tenant, session id, turn id).
    member internal _.Settlements = settlements

    /// The last usage checkpoint per (tenant, session id, turn id).
    member internal _.UsageCheckpoints = usageCheckpoints

    /// The current turn id per (tenant, session id).
    member internal _.CurrentTurnIds = currentTurnIds

    /// The structured outcome recorded at settlement, per settled turn.
    member internal _.Outcomes = outcomes

    /// The journals per (tenant, session id).
    member internal _.Journals = journals

    /// The cleanup leases per (tenant, session id).
    member internal _.CleanupLeases = cleanupLeases

    /// The agent rows keyed by (tenant, agent id).
    member internal _.Agents = agents

    /// The custom tools per (tenant, agent id), name-keyed.
    member internal _.CustomTools = customTools

    /// The blob rows keyed by validated key.
    member internal _.Blobs = blobs

    /// The agent packages per (tenant, agent id).
    member internal _.Packages = packages

    /// The clock lease expiry reads.
    member _.TimeProvider = timeProvider

    /// The journal limits the event store enforces.
    member _.Options = options

    /// The instant the stores read now from; exposed so stores on the same
    /// database stamp with one clock.
    member db.UtcNow = db.TimeProvider.GetUtcNow()

/// One open turn row: the claim model's minting state. A turn is open from
/// the claim that consumed its message until the settle or abort that
/// settles it.
and internal OpenTurnRow(turnId: TurnId, attempt: int) =

    /// The turn id.
    member val TurnId = turnId with get, set

    /// The attempt number the next claim of this turn carries.
    member val Attempt = attempt with get, set

/// One journal row: the ordered events, the archived flag, and the running
/// byte total.
and internal JournalRow(events: List<SessionEvent>, archived: bool, totalBytes: int64) =

    /// The journal's events in sequence order.
    member val Events = events with get, set

    /// Whether the journal was archived away by cleanup.
    member val Archived = archived with get, set

    /// The running byte total of the journaled events.
    member val TotalBytes = totalBytes with get, set

/// One blob row: the content and the metadata the store reports.
and internal BlobRow(content: BlobContent, metadata: BlobMetadata) =

    /// The blob's content.
    member internal _.Content = content

    /// The blob's metadata.
    member internal _.Metadata = metadata

/// One agent package row: the stored versions, the active pointer, and the
/// host-supplied source label of the last write.
and internal PackageRow(versions: Dictionary<string, StoredVersion>, active: string | null) =

    /// The stored versions keyed by version string.
    member val Versions = versions with get, set

    /// The active version string, or null when none.
    member val Active: string | null = active with get, set

    /// The host-supplied opaque source label of the package's last write.
    member val Source: string | null = null with get, set

/// One stored package version: its files and its creation stamp.
and internal StoredVersion(files: Dictionary<string, byte[]>, createdAt: DateTimeOffset) =

    /// The version's files keyed by normalised path.
    member val Files = files with get, set

    /// When the version was first published.
    member val CreatedAt = createdAt with get, set
