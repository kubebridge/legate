// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

// Transcript replay (issue 50): the bounded paging loop over
// ISessionEventStore.Replay feeding the pure TranscriptReader. Appends
// stay on JournalWriter as the sole write path; this module only reads.
// Outcome handling follows the store contract: unknown session, expired
// journal, and end of stream are settled tails, so the read returns the
// cells derived from the events it saw (none on a fresh unknown or
// expired journal) instead of throwing. Callers needing a
// control-plane precondition (an unknown-session failure) enforce it
// before calling.

/// Pages one session's event journal into transcript cells.
module internal Transcripts =

    /// Reads one session's transcript: replays the whole journal through
    /// bounded pages and derives the cells with
    /// <see cref="T:Legate.TranscriptReader" />, so the result is
    /// identical for every page size (chunking invariance is pinned by
    /// test, not by code: pages only transport, the pure read folds the
    /// concatenated journal).
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose transcript to read.</param>
    /// <param name="options">The read knobs.</param>
    /// <param name="pageSize">The replay page size. Must be positive.</param>
    /// <param name="cancellationToken">Token that abandons the replay.</param>
    /// <returns>The derived cells, in transcript order.</returns>
    let readTranscript
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (options: ReadTranscriptOptions)
        (pageSize: int)
        (cancellationToken: CancellationToken)
        : Task<IReadOnlyList<SessionCell>> =
        if isNull (box eventStore) then
            raise (ArgumentNullException(nameof eventStore))

        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        if pageSize <= 0 then
            raise (ArgumentOutOfRangeException(nameof pageSize, "PageSize must be positive."))

        task {
            let journal = ResizeArray<SessionEvent>()
            let mutable cursor = 0L
            let mutable paging = true

            while paging do
                cancellationToken.ThrowIfCancellationRequested()

                let! outcome = eventStore.Replay(tenant, sessionId, cursor, pageSize, cancellationToken)

                match outcome with
                | :? EventReplayPage as page when not (isNull (box page)) ->
                    let hadEvents = not (isNull (box page.Events)) && page.Events.Count > 0

                    if hadEvents then
                        for event in page.Events do
                            if not (isNull (box event)) then
                                journal.Add(event)

                    // Pages only past a non-empty page with a cursor: an
                    // empty page resolves to the asked-for cursor, so
                    // paging on would never advance.
                    if hadEvents && page.NextCursor.HasValue then
                        cursor <- page.NextCursor.Value
                    else
                        paging <- false
                | _ -> paging <- false

            return TranscriptReader.Read(journal :> IReadOnlyList<SessionEvent>, options)
        }
