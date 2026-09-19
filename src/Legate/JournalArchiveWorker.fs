// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.IO
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open Microsoft.Extensions.Options

// Leased journal archive worker (issue 111). Closed sessions whose
// ClosedAt plus the retention delay is past on the injected clock get
// their journal archived: the worker claims the journal cleanup lease,
// replays every page from cursor 0, serialises each SessionEvent as one
// System.Text.Json line (the same polymorphic $type wire shape the
// journal stores carry), atomically writes
// <ArchiveDirectory>/<tenant>_<session>/events.jsonl (temp file plus
// move, write-through plus flush), reads the file back and byte-compares
// it before completing the cleanup with the archive location; any
// mismatch or I/O failure defers with backoff on the ILlmDelay seam and
// never deletes. Tenant enumeration mirrors the session-expiry sweeper
// (issue 110): the facade client's tenant when one is registered, else
// the default single-tenant id, resolved lazily from DI at loop start, so
// no new store API is needed. No external services: only ISessionStore,
// ISessionEventStore, the TimeProvider clock, and the delay/random seams,
// so Legate still builds and tests with no Redis, Docker, Postgres, or
// Kubernetes.

// ──────────────────────────────────────────────────────────────────────────
// Archive files: pure path mapping plus streaming write and verify

/// Path mapping and file IO for archived journals. The path helpers stay
/// pure so unit tests pin the sanitisation; the write and verify stream
/// line by line, so a large journal never sits whole in memory.
module internal JournalArchiveFiles =

    /// The serialiser the archive lines use: the defaults, so every line
    /// carries the same polymorphic <c>$type</c> wire shape the journal
    /// stores persist.
    let jsonOptions = JsonSerializerOptions()

    /// Whether the character survives into an archive file or directory
    /// name unchanged: letters, digits, dash, and underscore. Anything
    /// else (notably separators and parent traversals) maps to an
    /// underscore, so tenant and session ids can never escape the archive
    /// root.
    /// <param name="value">The character to test.</param>
    /// <returns>True when the character is kept verbatim.</returns>
    let isSafeSegmentChar (value: char) : bool =
        Char.IsLetterOrDigit value || value = '-' || value = '_'

    /// Maps an identifier to its archive-name segment: safe characters
    /// kept, everything else an underscore. Host-controlled tenant ids may
    /// carry separators; session ids are ULIDs but map through the same
    /// rule so the invariant holds by construction, not by charset luck.
    /// <param name="value">The identifier to map. Must not be null.</param>
    /// <returns>The sanitised segment, never empty and never carrying a separator.</returns>
    let sanitizeSegment (value: string) : string =
        ArgumentNullException.ThrowIfNull(value)

        let mapped =
            value
            |> Seq.map (fun c -> if isSafeSegmentChar c then c else '_')
            |> Seq.toArray

        if mapped.Length = 0 then "_" else new String(mapped)

    /// Maps a session to its archive file path:
    /// <c>&lt;root&gt;/&lt;tenant&gt;_&lt;session&gt;/events.jsonl</c>,
    /// with both ids sanitised and the result proven inside the root: a
    /// resolved path escaping the root fails instead of writing.
    /// <param name="archiveRoot">The archive directory. Must not be null or whitespace.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session being archived.</param>
    /// <returns>The absolute archive file path.</returns>
    let archivePath (archiveRoot: string) (tenant: TenantId) (sessionId: SessionId) : string =
        if String.IsNullOrWhiteSpace archiveRoot then
            raise (ArgumentException("The archive directory must be a non-empty path.", nameof archiveRoot))

        let root = Path.GetFullPath archiveRoot

        let directory =
            sprintf "%s_%s" (sanitizeSegment tenant.Value) (sanitizeSegment sessionId.Value)

        let path = Path.GetFullPath(Path.Combine(root, directory, "events.jsonl"))

        let rooted = root + string Path.DirectorySeparatorChar

        if not (path.StartsWith(rooted, StringComparison.Ordinal)) then
            raise (
                InvalidOperationException(
                    sprintf "The archive path for session %O escapes the archive directory." sessionId
                )
            )

        path

    /// Deletes a path when it exists, swallowing every failure: archive
    /// hygiene must never fail a pass (the stores stay the source of
    /// truth; stray files carry no pointer until a cleanup completes).
    /// <param name="path">The file to remove.</param>
    let deleteQuietly (path: string) : unit =
        try
            File.Delete(path)
        with _ ->
            ()

    /// Streams one replay page's events to the writer, one JSON line per
    /// event with a line feed terminator (never the platform newline, so
    /// the bytes verify deterministically).
    /// <param name="writer">The open archive writer.</param>
    /// <param name="events">The page's events, in sequence order.</param>
    let private writeLines (writer: StreamWriter) (events: Collections.Generic.IReadOnlyList<SessionEvent>) : unit =
        ArgumentNullException.ThrowIfNull(writer)
        ArgumentNullException.ThrowIfNull(events)

        for event in events do
            if isNull (box event) then
                raise (InvalidOperationException("The journal replay returned a null event."))

            writer.Write(JsonSerializer.Serialize(event, jsonOptions))
            writer.Write("\n")

    /// Writes one journal to a temp file beside its final path: replays
    /// every page from cursor 0 and streams the lines with write-through,
    /// then flushes through the OS buffers. Returns false (writing
    /// nothing) when the journal cannot be archived as-is: unknown
    /// session, an already-expired journal, or end of stream before any
    /// page.
    /// <param name="eventStore">The journal source.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to archive.</param>
    /// <param name="tempPath">The temp file to write.</param>
    /// <param name="pageSize">The replay page size; must be positive.</param>
    /// <param name="cancellationToken">Abandons the write.</param>
    /// <returns>True when the temp file holds the whole journal.</returns>
    let private writeTempAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (tempPath: string)
        (pageSize: int)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        task {
            let mutable cursor = 0L
            let mutable more = true
            let mutable writable = true
            let mutable sawPage = false

            use stream =
                new FileStream(
                    tempPath,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.None,
                    8192,
                    FileOptions.WriteThrough
                )

            // No BOM: the verify re-serialises plain lines, so a preamble
            // would fail every compare.
            use writer = new StreamWriter(stream, new UTF8Encoding(false))

            while more && writable do
                let! outcome = eventStore.Replay(tenant, sessionId, cursor, pageSize, cancellationToken)

                match outcome with
                | :? EventReplayPage as page when not (isNull (box page)) && not (isNull (box page.Events)) ->
                    sawPage <- true
                    writeLines writer page.Events

                    if page.NextCursor.HasValue then
                        cursor <- page.NextCursor.Value
                    else
                        more <- false
                | :? EventReplayEndOfStream when sawPage -> more <- false
                | :? EventReplayEndOfStream -> writable <- false
                | _ -> writable <- false

            if writable then
                writer.Flush()
                stream.Flush(true)

            return writable
        }

    /// Reads exactly the expected bytes from the archive stream: false on
    /// end of stream, so a short file never compares equal.
    /// <param name="stream">The open archive file.</param>
    /// <param name="expected">The bytes to match, filled from the stream.</param>
    /// <returns>True when the stream held that many bytes.</returns>
    let private readExact (stream: FileStream) (expected: byte[]) : bool =
        let mutable offset = 0
        let mutable ended = false

        while offset < expected.Length && not ended do
            let read = stream.Read(expected, offset, expected.Length - offset)

            if read <= 0 then ended <- true else offset <- offset + read

        not ended

    /// Reads the archive file back and byte-compares it against a fresh
    /// replay from cursor 0: every line must match exactly and the file
    /// must end where the journal does. A journal that changed between the
    /// write and this read (a concurrent append under a live turn claim)
    /// fails the compare instead of archiving a half-old journal.
    /// <param name="eventStore">The journal source.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session that was archived.</param>
    /// <param name="archivePath">The published archive file.</param>
    /// <param name="pageSize">The replay page size; must be positive.</param>
    /// <param name="cancellationToken">Abandons the verify.</param>
    /// <returns>True when the file holds exactly the journal's bytes.</returns>
    let private verifyFileAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (archivePath: string)
        (pageSize: int)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        task {
            use stream =
                new FileStream(
                    archivePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    8192,
                    FileOptions.SequentialScan
                )

            let mutable cursor = 0L
            let mutable more = true
            let mutable matched = true

            while more && matched do
                let! outcome = eventStore.Replay(tenant, sessionId, cursor, pageSize, cancellationToken)

                match outcome with
                | :? EventReplayPage as page when not (isNull (box page)) && not (isNull (box page.Events)) ->
                    for event in page.Events do
                        if matched then
                            if isNull (box event) then
                                raise (InvalidOperationException("The journal replay returned a null event."))

                            let expected =
                                Encoding.UTF8.GetBytes(JsonSerializer.Serialize(event, jsonOptions) + "\n")

                            let actual = Array.zeroCreate expected.Length

                            matched <- readExact stream actual && expected.AsSpan().SequenceEqual(actual.AsSpan())

                    if matched then
                        if page.NextCursor.HasValue then
                            cursor <- page.NextCursor.Value
                        else
                            matched <- stream.ReadByte() = -1
                            more <- false
                | :? EventReplayEndOfStream ->
                    matched <- stream.ReadByte() = -1
                    more <- false
                | _ ->
                    matched <- false
                    more <- false

            return matched
        }

    /// Archives one journal to its file: replays every page from cursor 0
    /// into a temp file, atomically moves it over the final path, then
    /// reads the final file back and byte-compares it. A mismatch deletes
    /// the unverified file and reports false; I/O failures throw. Neither
    /// path touches the stores: completing or deferring stays the pass's
    /// job.
    /// <param name="eventStore">The journal source.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to archive.</param>
    /// <param name="archivePath">The final archive file path.</param>
    /// <param name="pageSize">The replay page size; must be positive.</param>
    /// <param name="cancellationToken">Abandons the archive.</param>
    /// <returns>True when the final file holds exactly the journal's bytes.</returns>
    let writeAndVerifyAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (archivePath: string)
        (pageSize: int)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        task {
            ArgumentNullException.ThrowIfNull(eventStore)

            if String.IsNullOrWhiteSpace archivePath then
                raise (ArgumentException("The archive path must be a non-empty path.", nameof archivePath))

            if pageSize <= 0 then
                raise (ArgumentOutOfRangeException(nameof pageSize, "The replay page size must be positive."))

            match Path.GetDirectoryName archivePath with
            | null -> raise (ArgumentException("The archive path must carry a directory.", nameof archivePath))
            | directory -> Directory.CreateDirectory directory |> ignore

            let tempPath = sprintf "%s.tmp-%s" archivePath (Guid.NewGuid().ToString("N"))

            try
                let! writable = writeTempAsync eventStore tenant sessionId tempPath pageSize cancellationToken

                if not writable then
                    deleteQuietly tempPath
                    return false
                else
                    File.Move(tempPath, archivePath, true)

                    let! verified = verifyFileAsync eventStore tenant sessionId archivePath pageSize cancellationToken

                    if verified then
                        return true
                    else
                        deleteQuietly archivePath
                        return false
            with ex ->
                deleteQuietly tempPath
                return raise ex
        }

// ──────────────────────────────────────────────────────────────────────────
// One pass

/// One archive pass over closed sessions. Internal so no store type ever
/// crosses the public API; tests drive <c>passOnceAsync</c> directly under
/// virtual time.
module internal JournalArchive =

    /// How many sessions one ListSessions page holds at most: bounded pages
    /// keep a large closed backlog from growing the pass without bound.
    [<Literal>]
    let MaxBatchSize = 50

    /// How many events one archive replay page holds at most: bounded pages
    /// stream large journals line by line instead of buffering them whole.
    [<Literal>]
    let MaxReplayPageSize = 100

    /// The lease owner prefix this worker's claims stamp: one Guid-suffixed
    /// owner per worker, so competing workers never share an identity.
    [<Literal>]
    let OwnerPrefix = "journal-archive-"

    /// Whether the session's journal is due for archival on the clock:
    /// closed with a stamp, and the stamp plus the retention delay reached.
    /// Sessions that never stamped ClosedAt never archive (a store quirk
    /// skips instead of failing the pass).
    /// <param name="session">The session to test.</param>
    /// <param name="retention">How long a closed journal waits before archival.</param>
    /// <param name="now">The instant to measure retention against.</param>
    /// <returns>True when the journal is due.</returns>
    let isDue (session: Session) (retention: TimeSpan) (now: DateTimeOffset) : bool =
        if isNull (box session) then false
        elif session.State <> SessionState.Closed then false
        elif not session.ClosedAt.HasValue then false
        else session.ClosedAt.Value + retention <= now

    /// The backoff wait after an archive failure: the base plus up to one
    /// more base of jitter, so competing workers do not retry in lockstep.
    /// <param name="random">The jitter source.</param>
    /// <param name="backoff">The configured base backoff; zero or negative waits nothing.</param>
    /// <returns>The wait, within [backoff, 2 * backoff).</returns>
    let backoffWithJitter (random: ILlmRandom) (backoff: TimeSpan) : TimeSpan =
        ArgumentNullException.ThrowIfNull(random)

        if backoff <= TimeSpan.Zero then
            TimeSpan.Zero
        else
            backoff + TimeSpan.FromTicks(int64 (random.NextDouble() * float backoff.Ticks))

    /// Defers the journal back to claimable and waits the jittered backoff
    /// on the delay seam: the failure path that never deletes. Cancellation
    /// propagates (a cancelled pass is not a deferral); other defer or
    /// delay failures stay silent so one poisoned journal cannot fail the
    /// pass.
    /// <param name="eventStore">The journal source.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session that failed to archive.</param>
    /// <param name="claimToken">The cleanup claim token to release.</param>
    /// <param name="delay">The delay seam the backoff waits on.</param>
    /// <param name="random">The backoff jitter source.</param>
    /// <param name="backoff">The configured base backoff.</param>
    /// <param name="cancellationToken">Abandons the deferral.</param>
    /// <returns>A task that completes once the backoff elapsed.</returns>
    let deferWithBackoffAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (claimToken: string)
        (delay: ILlmDelay)
        (random: ILlmRandom)
        (backoff: TimeSpan)
        (cancellationToken: CancellationToken)
        : Task =
        task {
            try
                let! _ = eventStore.DeferCleanup(tenant, sessionId, claimToken, cancellationToken)
                ()
            with
            | :? OperationCanceledException as cancel -> return raise cancel
            | _ -> ()

            try
                do! delay.Delay(backoffWithJitter random backoff, cancellationToken)
            with
            | :? OperationCanceledException as cancel -> return raise cancel
            | _ -> ()
        }

    /// Archives one claimed journal: writes and verifies its file, then
    /// completes the cleanup with the archive location under the claim's
    /// fence. A losing token completes nothing and leaves no unverified
    /// file behind; a verify failure or I/O error defers with backoff. The
    /// journal rows are deleted only by the fenced complete, never here.
    /// <param name="eventStore">The journal source.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to archive.</param>
    /// <param name="claimToken">The cleanup claim token fencing the settlement.</param>
    /// <param name="archiveRoot">The archive directory root.</param>
    /// <param name="writeVerify">Writes and verifies the journal at the given path.</param>
    /// <param name="delay">The delay seam the failure backoff waits on.</param>
    /// <param name="random">The backoff jitter source.</param>
    /// <param name="backoff">The configured base backoff.</param>
    /// <param name="cancellationToken">Abandons the archive.</param>
    /// <returns>True when the journal archived and cleaned up.</returns>
    let archiveOneAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (claimToken: string)
        (archiveRoot: string)
        (writeVerify: string -> Task<bool>)
        (delay: ILlmDelay)
        (random: ILlmRandom)
        (backoff: TimeSpan)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        task {
            ArgumentNullException.ThrowIfNull(eventStore)
            ArgumentNullException.ThrowIfNull(claimToken)
            ArgumentNullException.ThrowIfNull(writeVerify)
            ArgumentNullException.ThrowIfNull(delay)
            ArgumentNullException.ThrowIfNull(random)

            let path = JournalArchiveFiles.archivePath archiveRoot tenant sessionId

            try
                let! verified = writeVerify path

                if not verified then
                    do!
                        deferWithBackoffAsync
                            eventStore
                            tenant
                            sessionId
                            claimToken
                            delay
                            random
                            backoff
                            cancellationToken

                    return false
                else
                    let! settlement = eventStore.CompleteCleanup(tenant, sessionId, claimToken, path, cancellationToken)

                    match settlement with
                    | :? EventCleanupApplied as applied when not (isNull (box applied)) && applied.Completed ->
                        return true
                    | _ ->
                        // The lease lapsed or another worker settled first:
                        // this attempt's file is unverified under any live
                        // claim, so remove it instead of orphaning it.
                        JournalArchiveFiles.deleteQuietly path
                        return false
            with
            | :? OperationCanceledException as cancel -> return raise cancel
            | _ ->
                do! deferWithBackoffAsync eventStore tenant sessionId claimToken delay random backoff cancellationToken

                return false
        }

    /// Archives one claimed session through the real writer: replays its
    /// journal from cursor 0 into its archive file, verifies, and settles.
    /// <param name="eventStore">The journal source.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to archive.</param>
    /// <param name="claimToken">The cleanup claim token fencing the settlement.</param>
    /// <param name="archiveRoot">The archive directory root.</param>
    /// <param name="delay">The delay seam the failure backoff waits on.</param>
    /// <param name="random">The backoff jitter source.</param>
    /// <param name="options">The archive knobs carrying the backoff.</param>
    /// <param name="cancellationToken">Abandons the archive.</param>
    /// <returns>True when the journal archived and cleaned up.</returns>
    let private archiveSessionAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (claimToken: string)
        (archiveRoot: string)
        (delay: ILlmDelay)
        (random: ILlmRandom)
        (options: JournalArchiveOptions)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        let writeVerify path =
            JournalArchiveFiles.writeAndVerifyAsync eventStore tenant sessionId path MaxReplayPageSize cancellationToken

        archiveOneAsync
            eventStore
            tenant
            sessionId
            claimToken
            archiveRoot
            writeVerify
            delay
            random
            options.VerifyBackoff
            cancellationToken

    /// Runs one full pass: pages the tenant's Closed sessions in bounded
    /// batches and archives every journal past the retention delay. An
    /// unset archive directory archives nothing; a non-positive lease
    /// duration archives nothing (configuration validation rejects it at
    /// startup, so the pass treats it as disabled rather than throwing on
    /// every claim). Claimed-by-another journals are skipped: the lease is
    /// single-winner and the next pass retries.
    /// <param name="sessionStore">The session source.</param>
    /// <param name="eventStore">The journal source.</param>
    /// <param name="tenant">The tenant whose sessions to sweep.</param>
    /// <param name="options">The archive knobs.</param>
    /// <param name="clock">The clock retention reads.</param>
    /// <param name="delay">The delay seam the failure backoff waits on.</param>
    /// <param name="random">The backoff jitter source.</param>
    /// <param name="owner">This worker's lease owner identity.</param>
    /// <param name="cancellationToken">Abandons the pass.</param>
    /// <returns>How many journals were archived and cleaned up.</returns>
    let passOnceAsync
        (sessionStore: ISessionStore)
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (options: JournalArchiveOptions)
        (clock: TimeProvider)
        (delay: ILlmDelay)
        (random: ILlmRandom)
        (owner: string)
        (cancellationToken: CancellationToken)
        : Task<int> =
        task {
            ArgumentNullException.ThrowIfNull(sessionStore)
            ArgumentNullException.ThrowIfNull(eventStore)
            ArgumentNullException.ThrowIfNull(options)
            ArgumentNullException.ThrowIfNull(clock)
            ArgumentNullException.ThrowIfNull(delay)
            ArgumentNullException.ThrowIfNull(random)
            ArgumentNullException.ThrowIfNull(owner)

            // The conversion narrows the nullability the whitespace guard
            // below proves: an unset directory stays disabled, a set one
            // resolves here.
            let directory: string = string options.ArchiveDirectory

            if String.IsNullOrWhiteSpace directory then
                return 0
            elif options.LeaseDuration <= TimeSpan.Zero then
                return 0
            else
                let root = Path.GetFullPath directory
                Directory.CreateDirectory root |> ignore

                let now = clock.GetUtcNow()
                let mutable archived = 0
                let mutable continuation: string | null = null
                let mutable more = true

                while more do
                    let! page =
                        sessionStore.ListSessions(
                            tenant,
                            Nullable(SessionState.Closed),
                            Nullable(),
                            Nullable(),
                            Nullable(),
                            MaxBatchSize,
                            continuation,
                            cancellationToken
                        )

                    if isNull (box page) || isNull (box page.Items) then
                        more <- false
                    else
                        for session in page.Items do
                            if not (isNull (box session)) && isDue session options.RetentionDelay now then
                                let! state =
                                    eventStore.TryClaimCleanup(
                                        tenant,
                                        session.Id,
                                        owner,
                                        options.LeaseDuration,
                                        cancellationToken
                                    )

                                match state with
                                | :? EventCleanupClaimed as granted when
                                    not (isNull (box granted)) && not (isNull (box granted.Claim))
                                    ->
                                    let! finished =
                                        archiveSessionAsync
                                            eventStore
                                            tenant
                                            session.Id
                                            granted.Claim.Token
                                            root
                                            delay
                                            random
                                            options
                                            cancellationToken

                                    if finished then
                                        archived <- archived + 1
                                | _ -> ()

                        continuation <- page.Continuation
                        more <- not (isNull (box page.Continuation))

                return archived
        }

// ──────────────────────────────────────────────────────────────────────────
// Hosted service

/// Singleton hosted service owning the journal archive loop: one archive
/// pass every tick through
/// <see cref="M:Legate.JournalArchive.passOnceAsync*" /> over the injected
/// clock and delay seams, so virtual-time tests advance it without
/// sleeping. A pass failure is retried next interval; host stop cancels
/// the wait and the in-flight pass. The stores and the sweep tenant
/// resolve lazily from the provider at loop start (never at construction),
/// so an empty host still fails startup with the required-registration
/// message instead of a resolution error. Operational visibility rides the
/// runtime's own logs: like LocalActorSystemService this service takes no
/// logger, so it stays constructible on containers without logging.
type internal JournalArchiveWorker
    (
        serviceProvider: IServiceProvider,
        options: IOptions<JournalArchiveOptions>,
        timeProvider: TimeProvider,
        delay: ILlmDelay,
        random: ILlmRandom
    ) =

    do
        ArgumentNullException.ThrowIfNull(serviceProvider)
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(timeProvider)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(random)

    let log: ILogger = NullLogger.Instance :> ILogger
    let owner = JournalArchive.OwnerPrefix + Guid.NewGuid().ToString("N")
    let lifetime = new CancellationTokenSource()
    let mutable loop: Task | null = null

    /// This worker's lease owner identity: what its claims stamp.
    member _.Owner: string = owner

    /// Resolves the locally scoped sweep tenant: the facade client's tenant
    /// when one is registered, else the default single-tenant id. The sweep
    /// stays tenant-scoped with no new store API by covering exactly the
    /// tenant this node serves, the same source the session-expiry sweeper
    /// (issue 110) uses.
    /// <returns>The tenant whose sessions to sweep.</returns>
    member private _.SweepTenant() : TenantId =
        match serviceProvider.GetService<SessionClientOptions>() with
        | null -> TenantId.Default
        | clientOptions -> clientOptions.Tenant

    /// The tick interval for the loop: the configured poll interval, with a
    /// ten-minute fallback so a misconfigured host can never busy-loop;
    /// configuration validation rejects such values at startup.
    /// <returns>How long the loop waits between passes.</returns>
    member private _.TickInterval() : TimeSpan =
        let archiveOptions = options.Value
        let fallback = TimeSpan.FromMinutes 10.0

        if isNull (box archiveOptions) then
            fallback
        elif archiveOptions.PollInterval > TimeSpan.Zero then
            archiveOptions.PollInterval
        else
            fallback

    /// Runs one full pass: archives every due journal of the sweep tenant.
    /// <param name="cancellationToken">Abandons the pass.</param>
    /// <returns>How many journals were archived and cleaned up.</returns>
    member internal this.RunOnceAsync(cancellationToken: CancellationToken) : Task<int> =
        task {
            let sessionStore = serviceProvider.GetRequiredService<ISessionStore>()
            let eventStore = serviceProvider.GetRequiredService<ISessionEventStore>()
            let archiveOptions = options.Value
            let tenant = this.SweepTenant()

            return!
                JournalArchive.passOnceAsync
                    sessionStore
                    eventStore
                    tenant
                    archiveOptions
                    timeProvider
                    delay
                    random
                    owner
                    cancellationToken
        }

    /// Runs the poll loop until host stop.
    /// <returns>A task that completes once the loop exits.</returns>
    member private this.RunAsync() : Task =
        task {
            let mutable running = true

            while running && not lifetime.Token.IsCancellationRequested do
                try
                    let! _ = this.RunOnceAsync(lifetime.Token)
                    ()
                with
                | :? OperationCanceledException -> running <- false
                | failed ->
                    log.LogWarning("Journal archive pass failed and will retry next interval: {Reason}", failed.Message)

                if running && not lifetime.Token.IsCancellationRequested then
                    try
                        do! delay.Delay(this.TickInterval(), lifetime.Token)
                    with :? OperationCanceledException ->
                        running <- false
        }

    interface IHostedService with
        member this.StartAsync(_cancellationToken: CancellationToken) =
            loop <- Task.Run(Func<Task>(fun () -> this.RunAsync()), lifetime.Token)
            Task.CompletedTask

        member _.StopAsync(cancellationToken: CancellationToken) =
            task {
                lifetime.Cancel()

                match loop with
                | null -> ()
                | running ->
                    try
                        do! running.WaitAsync(cancellationToken)
                    with
                    | :? OperationCanceledException -> ()
                    | :? TimeoutException -> ()
            }
            :> Task

// ──────────────────────────────────────────────────────────────────────────
// Registration

/// Registers the journal archive options pipeline and hosted service.
module internal JournalArchiveRegistration =

    /// Registers the standalone <c>Legate:Archive</c> options binding (a
    /// host without configuration keeps the defaults, and a host callback
    /// registered later still wins), start-time validation, and the archive
    /// worker as a singleton hosted service, only when the host has not
    /// already supplied its own registration for the same implementation.
    /// <param name="services">The container to add the archive to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        // Deferred standalone bind: the section resolves when the options
        // do, so hosts that configure Legate:Archive land there and hosts
        // without an IConfiguration keep the option defaults. A Configure
        // callback the host registers afterwards still runs later and wins.
        services
            .AddOptions<JournalArchiveOptions>()
            .Configure<IServiceProvider>(
                Action<JournalArchiveOptions, IServiceProvider>(fun archive provider ->
                    match provider.GetService<IConfiguration>() with
                    | null -> ()
                    | configuration ->
                        configuration.GetSection(JournalArchiveOptions.ConfigurationSectionPath).Bind(archive))
            )
            .ValidateOnStart()
        |> ignore

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<JournalArchiveOptions>, JournalArchiveOptionsValidation>()
        )
        |> ignore

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, JournalArchiveWorker>())
        |> ignore
