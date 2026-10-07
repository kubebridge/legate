// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging

// Session actor: the Akka.FSharp child owning one session's
// Idle -> Running -> (Idle | WaitingForInput | Closed) state machine with a
// durable ISessionStore inbox, driving TurnLoop.runAsync for Queue delivery
// only. The LocalActorSystem router spawns one of these per session id.
//
// Store-first persistence: Prompt appends the inbox entry and persists the
// Running state before the turn starts; settle consumes the completed entry
// before draining the next one or returning to Idle. Consumption at settle
// (not at start) makes a mid-turn crash redeliver: the entry is still
// pending, so restart rebuilds Idle with the work intact (at-least-once).
// An observed in-process turn fault consumes its entry instead, so a poison
// message cannot hot-loop; retry policy belongs to later issues.
//
// Threading: the Akka.FSharp actor computation can only receive messages,
// never await tasks, so the fast store metadata ops are awaited
// synchronously on the actor thread. That preserves the actor's
// single-threaded sequencing guarantee (a Close can never slip between a
// settle's consume and its Idle write), which piped follow-up tasks would
// lose. Only the unbounded turn execution leaves the thread: it runs
// fire-and-forget and pipes its outcome back as a message, per the actor
// rules. A future clustered or high-throughput redesign can move the
// metadata ops to stashed follow-ups with epoch fencing.
//
// Out of scope here, owned elsewhere: claim tokens and fencing (#33),
// Inject/Interrupt delivery (#34), and suspend triggers with reply matching
// (#36: WaitingForInput is modelled and preserved, never entered). Abort is
// a first-class turn-level verb here (#35): AbortSession carries a typed
// stop cause, exactly one of settlement and stop wins per turn, and Close
// stays lifecycle-only (it wires turn cancellation without recording a
// stop cause).

// ──────────────────────────────────────────────────────────────────────────
// Protocol

/// How the session actor answers a prompt. The actor never throws
/// InvalidSessionStateException: a Closed-state prompt is rejected with the
/// current state and the client boundary maps it to the exception.
type internal SessionPromptReply =

    /// The prompt was accepted: the entry was appended to the durable inbox
    /// (and a turn started when the session was Idle).
    | PromptAccepted of entry: InboxEntry

    /// The prompt arrived while the session was Closed; nothing was
    /// appended. Carries the state the session was in.
    | PromptRejected of state: SessionState

/// How the session actor answers an on-demand compaction. The actor never
/// throws InvalidSessionStateException: a Closed-state compact is rejected
/// with the current state and the client boundary maps it to the exception.
type internal SessionCompactReply =

    /// The Idle session compacted now without starting a turn: the
    /// before/after estimates from the CompactionOutcome.
    | CompactCompleted of beforeEstimate: int64 * afterEstimate: int64

    /// Nothing compacted and no summariser call ran: the session was Idle
    /// but under threshold (or had nothing replaceable), WaitingForInput (a
    /// suspended turn owns the history), in an out-of-range state,
    /// unconfigured, cancelled, or the summariser failed and the session
    /// continues uncompacted (the CompactionFailedEvent carries the reason).
    | CompactNotNeeded

    /// The session was Running: the one-shot force flag is armed and the
    /// running turn's force-aware boundary hook compacts at the next
    /// iteration boundary, bypassing the threshold once.
    | CompactDeferred

    /// The actor lost its claim before the journal write landed, so it
    /// journaled nothing: the takeover winner owns the session.
    | CompactFenced

    /// The compact arrived while the session was Closed; nothing ran.
    /// Carries the state the session was in.
    | CompactRejected of state: SessionState

/// The session actor protocol. QueuePrompt, InjectPrompt,
/// InterruptPrompt, CloseSession, and GetSnapshot
/// are answered to the sender; the SessionTurnSettled and SessionTurnFaulted completions
/// are one-way (Tell) from the turn task back to the actor.
type internal SessionActorMessage =

    /// Queue a user message: appends to the durable inbox, starts a turn
    /// when Idle, waits when Running or WaitingForInput, rejects when
    /// Closed. Carries Queue delivery only; Inject and Interrupt delivery
    /// arrive through InjectPrompt and InterruptPrompt. Answered with
    /// <see cref="T:Legate.SessionPromptReply" />.
    | QueuePrompt of payload: InboxPayload * cancellationToken: CancellationToken

    /// Inject a user message into the running turn: appends to the durable
    /// inbox with Inject delivery, starts a turn when Idle, folds at the
    /// next iteration boundary when Running through the runner's drain
    /// hooks, waits when WaitingForInput, rejects when Closed. Never
    /// aborts the running turn. Answered with
    /// <see cref="T:Legate.SessionPromptReply" />.
    | InjectPrompt of payload: InboxPayload * cancellationToken: CancellationToken

    /// Interrupt the running turn with a user message: appends to the
    /// durable inbox with Interrupt delivery, starts a turn when Idle,
    /// aborts the running turn under ExplicitAbort and drains the
    /// Interrupt entry first when Running, waits when WaitingForInput,
    /// rejects when Closed. Answered with
    /// <see cref="T:Legate.SessionPromptReply" />.
    | InterruptPrompt of payload: InboxPayload * cancellationToken: CancellationToken

    /// Close the session: aborts the running turn first through
    /// cancellation only, then closes idempotently in the store. Valid in
    /// every state. Answered with the stored session.
    | CloseSession of cancellationToken: CancellationToken

    /// Abort the running turn under a typed stop cause: records the pending
    /// stop, cancels the turn, and settles Aborted under the winning cause
    /// when the turn reports back. Exactly one of settlement and stop wins:
    /// a completion that landed first stands and the abort is a no-op, a
    /// stop that landed first maps even a successful completion to Aborted,
    /// and a second abort keeps the first cause. Idle is a no-op returning
    /// the current state, as is WaitingForInput (suspended turns belong to
    /// issue 36: nothing runs to abort). Only abort-family causes
    /// (ExplicitAbort, HostShutdown) act; anything else is a no-op. Answered
    /// with <see cref="T:Legate.SessionSnapshot" />.
    | AbortSession of cause: StopCause * reason: string * cancellationToken: CancellationToken
    /// Exact-target durable-intent wake hint. It never authorizes stop without provider evidence.
    | ObserveHostAbort of tenant: TenantId * sessionId: SessionId * targetTurnId: TurnId

    /// Compact the session on demand: Idle replays the journal into a
    /// history and compacts now without starting a turn, Running arms the
    /// one-shot force flag the turn's force-aware boundary hook honors at
    /// the next iteration (threshold bypassed, single-pass guard kept),
    /// WaitingForInput is a no-op (suspended turns belong to issue 36:
    /// nothing runs to compact), and Closed rejects. Answered with
    /// <see cref="T:Legate.SessionCompactReply" />.
    | CompactSession of cancellationToken: CancellationToken

    /// Reads the actor's current state plus the store's pending inbox count.
    /// Answered with <see cref="T:Legate.SessionSnapshot" />.
    | GetSnapshot

    /// The running turn settled with a TurnResult. Consumes the completed
    /// entry, then drains the next pending Queue entry or returns to Idle.
    /// Stale completions (arrived after Close) are ignored.
    | SessionTurnSettled of entry: InboxEntry * result: TurnResult

    /// The running turn faulted (cancellation, or a runner failure).
    /// Consumes the faulted entry so a poison message cannot hot-loop, then
    /// drains the next pending Queue entry or returns to Idle. Faults
    /// arriving after Close are ignored.
    | SessionTurnFaulted of entry: InboxEntry * error: Exception

    /// One piped lifecycle store wait finished (issue 390). The outcome is
    /// the boxed LifecyclePipe.StoreOpResult the wait produced; only the
    /// outstanding wait's op id plus the pipe incarnation resumes, so
    /// delayed, duplicate, reordered, and pre-restart completions are
    /// discarded with zero effects. Internal to the actor loop, never
    /// crossing node boundaries.
    | LifecycleStoreCompleted of opId: int64 * incarnation: Guid * outcome: obj

    /// One piped lifecycle store wait outran its bound (issue 390). The
    /// resumption fails with DeadlineExceededException; the late real
    /// completion is discarded by op id with zero effects. Internal to the
    /// actor loop, never crossing node boundaries.
    | LifecycleStoreTimeout of opId: int64 * incarnation: Guid

/// The actor's observable state: its in-memory lifecycle state, the store's
/// pending inbox count, and the running turn's entry position when a turn is
/// in flight.
type internal SessionSnapshot =
    {
        /// The session the snapshot was taken for.
        SessionId: SessionId
        /// The actor's current lifecycle state.
        State: SessionState
        /// How many inbox entries the store reports pending.
        PendingCount: int
        /// The position of the entry the running turn executes, or None
        /// when no turn is in flight.
        RunningPosition: int64 option
        /// The pending suspend request id while WaitingForInput, or null
        /// when no turn is suspended. The journal is the normative resume
        /// source; this is the observable projection of it.
        PendingRequestId: string | null
    }

/// A turn in flight: the inbox entry it executes plus the source Close
/// cancels to abort it. The source is cancelled but never disposed on the
/// abort path: disposal races in-flight token observations, and the source
/// is reclaimed by the GC instead.
type private RunningTurn =
    {
        /// The inbox entry the turn executes.
        Entry: InboxEntry
        /// The turn the attempt runs as (issue 289): the CurrentTurnId
        /// snapshot at start, or a fresh mint when the row carries none.
        /// The settle choke points journal the terminal event under it.
        TurnId: TurnId
        /// The source Close cancels to abort it.
        Cts: CancellationTokenSource
    }

/// Whether the base loop finished its store recovery (issue 390): the
/// recover chain runs piped before the first lifecycle message is handled,
/// and everything received meanwhile waits bounded behind it in arrival
/// order.
type private BehaviorActivation =
    /// Recovery landed: lifecycle messages handle normally.
    | Ready
    /// The recover chain is in flight: non-completion messages defer.
    | Recovering

/// The base-loop state threaded through every message (issue 390): the
/// lifecycle state plus the bounded pipe state, the pending inbox count
/// cache snapshots answer from, and the close senders waiting on the
/// durable close write.
type private BehaviorLoopArgs =
    {
        /// The actor's current lifecycle state.
        State: SessionState
        /// The turn in flight, or None.
        Running: RunningTurn option
        /// The stop arbitration cell.
        Arbitration: StopArbitration.ArbitrationState
        /// The recorded stop, or None.
        PendingStop: (StopCause * string) option
        /// Whether the recover chain landed.
        Activation: BehaviorActivation
        /// The store's pending inbox count as of the last inbox read or
        /// mutation the actor applied: snapshots answer from memory while a
        /// store wait is outstanding.
        PendingCount: int
        /// Close senders waiting on the durable close write: empty when no
        /// close is outstanding. A requested close behaves Closed for new
        /// lifecycle work while its write is outstanding.
        Closing: (IActorRef * CancellationToken) list
    }

/// The bounded pipe state the base loop threads.
type private BehaviorPipe = LifecyclePipe.PipeState<SessionActorMessage, BehaviorLoopArgs>

/// A base-loop continuation: the loop state plus the pipe state it resumes with.
type private BehaviorCont = BehaviorLoopArgs -> BehaviorPipe -> Cont<SessionActorMessage, unit>

/// What an on-demand compact needs outside a turn (issue 46): the same
/// summariser wiring a per-turn CompactionWiring carries, plus the journal
/// to replay and the one-shot force cell the turn's force-aware boundary
/// hooks share. The Idle path replays the journal into a history through
/// Transcripts.readTranscript and Compaction.messagesFromCells, then runs
/// the merged runner; the Running path only arms Force.
type internal CompactDeps =
    {
        /// LLM settings carrying the Compaction model override and the
        /// CompactionKeepMessages tail. Never null.
        Llm: LlmOptions
        /// The tokens held back beyond the reserved output when deriving
        /// the threshold.
        ReservedBufferTokens: int
        /// The session's model: the threshold lookup and default
        /// summariser model.
        SessionModel: ModelReference
        /// Resolves the session model to its catalog entry, or null
        /// when the host keeps no catalog (the threshold falls back to
        /// the catalog defaults).
        Catalog: ILlmModelCatalog | null
        /// The chat client the summariser call runs against. Never null.
        Client: IChatClient
        /// Receives the summariser usage checkpoint, or null.
        Observer: IUsageObserver | null
        /// Authorises the summariser model call, or null.
        Policy: IModelPolicy | null
        /// The journal the Idle path replays and appends to. Never null.
        EventStore: ISessionEventStore
        /// The claim token fencing the journal appends. Never null.
        JournalToken: string
        /// The one-shot force cell the turn's force-aware boundary hooks
        /// share: arming fires compaction at the next boundary exactly
        /// once. Never null.
        Force: Compaction.CompactForce
    }

/// What a session actor is built from: the durable store, the tenant and
/// session it owns, and the turn runner it drives per Queue entry. The
/// default runner (<see cref="M:Legate.SessionActor.createTurnRunner" />)
/// calls TurnLoop.runAsync; tests inject scripted runners.
type internal SessionActorProps =
    {
        /// The durable store the inbox and lifecycle state persist through.
        Store: ISessionStore
        /// The container-registered atomic settlement capability the
        /// suspendable settle path commits through (issue 383): split
        /// compositions (SQLite, Postgres) register it as a separate
        /// service, while unified compositions (InMemory) expose it on the
        /// store itself. None keeps the store-cast fallback below, so
        /// direct test constructions over a unified store behave
        /// unchanged.
        Settlement: ISessionSettlementStore option
        /// The tenant the session belongs to.
        Tenant: TenantId
        /// The session the actor owns.
        SessionId: SessionId
        /// Runs one turn for an inbox entry. Never null.
        RunTurn: InboxEntry -> CancellationToken -> Task<TurnResult>
        /// Observes each settled turn result (the carried result, or the
        /// abort-mapped Aborted result when a stop won) on the actor thread,
        /// or None for no observation. Guarded: a throwing observer never
        /// kills the actor.
        OnTurnSettled: (TurnResult -> unit) option
        /// Observes each Inject user message the running turn folds, as a
        /// UserMessageEvent carrying the running (injecting) turn's id, or
        /// None for no observation. The runner calls it once per folded
        /// entry, after the message is appended to history and before the
        /// consume hook. Guarded: a throwing observer never kills the turn.
        OnInjectJournaled: (UserMessageEvent -> unit) option
        /// Carries the on-demand compaction wiring (summariser client,
        /// model, catalog, journal, and the one-shot force cell the turn's
        /// force-aware boundary hooks share), or None when the host did not
        /// configure compaction: CompactSession then answers without
        /// compacting and never starts a turn.
        Compact: CompactDeps option
        /// The logger the actor reports prompt/reply/settle/suspend points
        /// to, or null for no logging (the CustomToolSource precedent: a
        /// null logger resolves to the NullLogger). Internal-only wiring.
        Logger: Microsoft.Extensions.Logging.ILogger | null
        /// How the actor bounds piped lifecycle store waits (issue 390):
        /// the clock deadline timers register on plus how long one wait may
        /// stay outstanding before its resumption fails with
        /// DeadlineExceededException. None selects the system clock with the
        /// default bound. Internal-only wiring.
        StorePipe: LifecyclePipe.StorePipeConfig option
    }

// ────────────────── Piped lifecycle store waits (issue 390) ──────────────────

/// The pared-down pipe context one actor loop builds once and threads into
/// every piped wait: which actor completions Tell, which clock and bound
/// they carry, and how completion/timeout messages pack for the loop's
/// protocol. Lets the shared helpers below stay generic over both loops.
type private PipeStarter<'M, 'A> =
    {
        /// The actor completions Tell.
        Self: IActorRef
        /// The clock deadline timers register on.
        Clock: TimeProvider
        /// How long one wait may stay outstanding.
        Timeout: TimeSpan
        /// Builds the loop protocol's completion message.
        PackCompleted: int64 * Guid * obj -> 'M
        /// Builds the loop protocol's timeout message.
        PackTimeout: int64 * Guid -> 'M
    }

// ──────────────────────────────────────────────────────────────────────────
// Behaviour

/// The fenced journal appends for facade-driven turns (issue 321):
/// testable wrappers around the fenced writer mirroring
/// journalTurnStartedAsync. Extracted so wrapper-level fencing tests drive
/// the same mapping the live actor uses: a stale-token rejection raises
/// TurnLeaseLostException so the takeover loser stops with zero effects; a
/// failed write returns silently and the turn proceeds (best-effort
/// observability).
module internal SessionJournal =

    /// Journals one cumulative usage checkpoint for a facade-driven turn
    /// (issue 321): a UsageEvent carrying the turn's accumulated totals
    /// under the given journal token through the fenced writer.
    /// <param name="eventStore">The journal the checkpoint appends to.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="token">The journal token snapshot fencing the write.</param>
    /// <param name="turnId">The turn the usage was checkpointed for.</param>
    /// <param name="inputTokens">The input tokens the turn had consumed.</param>
    /// <param name="outputTokens">The output tokens the turn had produced.</param>
    /// <param name="cancellationToken">Abandons the append.</param>
    let journalUsageAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (token: string)
        (turnId: TurnId)
        (inputTokens: int64)
        (outputTokens: int64)
        (cancellationToken: CancellationToken)
        : Task<unit> =
        task {
            let checkpoint =
                UsageEvent(
                    sessionId,
                    turnId,
                    Unchecked.defaultof<Nullable<int64>>,
                    DateTimeOffset.UtcNow,
                    inputTokens,
                    outputTokens
                )
                :> SessionEvent

            let batch =
                ResizeArray<SessionEvent>([| checkpoint |]) :> IReadOnlyList<SessionEvent>

            match! JournalWriter.appendWithTokenAsync eventStore tenant sessionId token batch cancellationToken with
            | JournalWriter.JournalAppended _ -> ()
            | JournalWriter.JournalRejected _ -> return raise (TurnLoop.TurnLeaseLostException())
            | JournalWriter.JournalFailed _ -> ()
        }

    /// Journals one skill load for a facade-driven turn (issue 321): the
    /// SkillLoadedEvent the SkillTool's onLoaded carried, re-keyed under
    /// the running turn when the tool was bound with a stale id, through
    /// the fenced writer.
    /// <param name="eventStore">The journal the event appends to.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="token">The journal token snapshot fencing the write.</param>
    /// <param name="turnId">The running turn the load ran inside.</param>
    /// <param name="loaded">The loaded event the tool reported.</param>
    /// <param name="cancellationToken">Abandons the append.</param>
    let journalSkillLoadedAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (token: string)
        (turnId: TurnId)
        (loaded: SkillLoadedEvent)
        (cancellationToken: CancellationToken)
        : Task<unit> =
        task {
            let companions =
                if isNull (box loaded) || isNull (box loaded.Companions) then
                    ResizeArray<string>() :> IReadOnlyList<string>
                else
                    loaded.Companions

            let skillName =
                if isNull (box loaded) || isNull (box loaded.SkillName) then
                    ""
                else
                    loaded.SkillName

            let event =
                SkillLoadedEvent(
                    sessionId,
                    turnId,
                    Unchecked.defaultof<Nullable<int64>>,
                    DateTimeOffset.UtcNow,
                    skillName,
                    companions
                )
                :> SessionEvent

            let batch = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

            match! JournalWriter.appendWithTokenAsync eventStore tenant sessionId token batch cancellationToken with
            | JournalWriter.JournalAppended _ -> ()
            | JournalWriter.JournalRejected _ -> return raise (TurnLoop.TurnLeaseLostException())
            | JournalWriter.JournalFailed _ -> ()
        }

/// Bounded per-attempt streaming journaler (issue 379): coalesces the
/// production suspendable path's progressive assistant-text and
/// provider-surfaced reasoning deltas into bounded journal appends. Only
/// surfaced chunks are journaled: nothing is inferred or fabricated, and
/// reasoning never reaches AssistantText (that stays TurnLoop's shape).
/// Every flush routes through the fenced
/// JournalWriter.appendWithTokenAsync under the attempt's claim token with
/// the real turn id: landed batches publish to live subscribers before
/// returning (replayable through the same journal), rejections raise
/// TurnLeaseLostException (zero writes, zero publication), and failures
/// raise with the writer's typed reason (explicit, never silent loss or a
/// false durability claim). Storage backpressure stays on the turn thread
/// as bounded append work; subscriber lag stays on the SessionEventBus,
/// which never blocks the publisher. Single-threaded turn ownership: all
/// members run on the running attempt's thread, never the actor thread.
module internal SessionStreaming =

    /// Bounded per-attempt streaming journaler tuning, resolved from
    /// TurnsOptions so production honors the configured bounds.
    type StreamingBounds =
        {
            /// The coalesced delta events buffered before forcing a flush.
            MaxPendingEvents: int
            /// The coalesced delta bytes (UTF-8) buffered before forcing a
            /// flush. Whichever pending cap trips first flushes.
            MaxPendingBytes: int
            /// The most delta events one journal append carries. Larger
            /// buffers split across bounded appends.
            MaxBatchEvents: int
            /// The most chars one delta event carries. Larger buffers split
            /// into several events within MaxBatchEvents.
            MaxBatchChars: int
        }

    /// Default tuning mirroring the TurnsOptions defaults.
    let defaultBounds: StreamingBounds =
        {
            MaxPendingEvents = 64
            MaxPendingBytes = 65536
            MaxBatchEvents = 8
            MaxBatchChars = 8192
        }

    /// Resolves the streaming bounds from the Turns snapshot,
    /// re-checking the positive bound at use like
    /// ClaimHeartbeat.fromSessions. A null snapshot reads as defaults.
    /// <param name="turns">The configured turn defaults, or null for defaults.</param>
    /// <returns>The bounds the per-attempt journaler enforces.</returns>
    let boundsFromTurns (turns: TurnsOptions) : StreamingBounds =
        if isNull (box turns) then
            defaultBounds
        else
            if turns.MaxStreamingPendingEvents < 1 then
                raise (
                    ArgumentOutOfRangeException(
                        nameof turns,
                        "TurnsOptions.MaxStreamingPendingEvents must be at least 1."
                    )
                )

            if turns.MaxStreamingPendingBytes < 1 then
                raise (
                    ArgumentOutOfRangeException(
                        nameof turns,
                        "TurnsOptions.MaxStreamingPendingBytes must be at least 1."
                    )
                )

            if turns.MaxStreamingAppendBatchEvents < 1 then
                raise (
                    ArgumentOutOfRangeException(
                        nameof turns,
                        "TurnsOptions.MaxStreamingAppendBatchEvents must be at least 1."
                    )
                )

            if turns.MaxStreamingAppendBatchChars < 1 then
                raise (
                    ArgumentOutOfRangeException(
                        nameof turns,
                        "TurnsOptions.MaxStreamingAppendBatchChars must be at least 1."
                    )
                )

            {
                MaxPendingEvents = turns.MaxStreamingPendingEvents
                MaxPendingBytes = turns.MaxStreamingPendingBytes
                MaxBatchEvents = turns.MaxStreamingAppendBatchEvents
                MaxBatchChars = turns.MaxStreamingAppendBatchChars
            }

    /// One per-attempt coalescing journaler: buffers text and reasoning
    /// chunks separately (each kind keeps chunk order; text precedes
    /// reasoning within one flush), starts a chained append when a pending
    /// cap trips, and lands the remainder on FlushAsync. The chain
    /// serializes appends in creation order, so committed output stays
    /// ordered before whatever terminal event the actor journals next, and
    /// every flush observes its predecessors: a rejected or failed
    /// predecessor faults its successors with zero further writes. Empty
    /// buffers never touch storage. Callbacks never block: they buffer and
    /// chain only, so a slow store stalls neither the turn thread (which
    /// awaits the chain at the attempt boundary) nor the actor thread
    /// (which the turn never runs inline past its first incomplete await).
    /// A flush that lands publishes its stamped events before returning; a
    /// rejected flush raises TurnLeaseLostException with zero writes and
    /// zero publication; a failed flush raises InvalidOperationException
    /// with the writer's typed reason. Prior landed batches survive every
    /// later outcome, so partial committed output stays ordered before
    /// whatever terminal event the actor journals next.
    type StreamingJournaler
        (
            eventStore: ISessionEventStore,
            tenant: TenantId,
            sessionId: SessionId,
            turnId: TurnId,
            token: string,
            bounds: StreamingBounds
        ) =

        do ArgumentNullException.ThrowIfNull(eventStore)

        do
            if isNull (box token) then
                raise (ArgumentNullException(nameof token))

        do
            if bounds.MaxPendingEvents < 1 then
                raise (ArgumentOutOfRangeException(nameof bounds, "MaxPendingEvents must be at least 1."))

        do
            if bounds.MaxPendingBytes < 1 then
                raise (ArgumentOutOfRangeException(nameof bounds, "MaxPendingBytes must be at least 1."))

        do
            if bounds.MaxBatchEvents < 1 then
                raise (ArgumentOutOfRangeException(nameof bounds, "MaxBatchEvents must be at least 1."))

        do
            if bounds.MaxBatchChars < 1 then
                raise (ArgumentOutOfRangeException(nameof bounds, "MaxBatchChars must be at least 1."))

        let textChunks = ResizeArray<string>()
        let reasoningChunks = ResizeArray<string>()
        let mutable textChars = 0
        let mutable reasoningChars = 0
        let mutable pendingBytes = 0
        let mutable totalAppends = 0
        let mutable totalEvents = 0
        let mutable maxPendingChars = 0
        let mutable maxPendingBytes = 0

        // The serialized flush chain: every flush awaits its predecessor,
        // so appends land in creation order and a rejected or failed flush
        // faults its successors with zero further writes. Mutated only on
        // the turn thread (buffering and FlushAsync); awaited from the
        // chain tasks and the attempt boundary.
        let mutable flushChain: Task = Task.CompletedTask

        /// The pending delta events the buffer would produce, split at the
        /// batch-char bound: text pieces plus reasoning pieces.
        let pendingEventCount () =
            let pieces chars =
                if chars <= 0 then
                    0
                else
                    (chars + bounds.MaxBatchChars - 1) / bounds.MaxBatchChars

            pieces textChars + pieces reasoningChars

        /// Splits buffered text into MaxBatchChars pieces in order.
        let splitPieces (value: string) : string list =
            if String.IsNullOrEmpty value then
                []
            else
                let mutable pieces = []
                let mutable index = 0

                while index < value.Length do
                    let take = min bounds.MaxBatchChars (value.Length - index)
                    pieces <- value.Substring(index, take) :: pieces
                    index <- index + take

                List.rev pieces

        /// Builds the flush events in journal order: text pieces first,
        /// then reasoning pieces, each carrying the real session and turn
        /// ids with an empty in-flight sequence the store stamps.
        let buildEvents (text: string) (reasoning: string) : SessionEvent list =
            [
                for piece in splitPieces text do
                    yield
                        TextDeltaEvent(
                            sessionId,
                            turnId,
                            Unchecked.defaultof<Nullable<int64>>,
                            DateTimeOffset.UtcNow,
                            piece
                        )
                        :> SessionEvent

                for piece in splitPieces reasoning do
                    yield
                        ReasoningDeltaEvent(
                            sessionId,
                            turnId,
                            Unchecked.defaultof<Nullable<int64>>,
                            DateTimeOffset.UtcNow,
                            piece
                        )
                        :> SessionEvent
            ]

        /// Appends the given events in bounded batches, awaiting the
        /// predecessor first: the chain primitive behind every flush. A
        /// landed batch stays observable and replayable; rejected and
        /// failed batches publish nothing and fault the chain.
        /// <param name="events">The events to append, in journal order. May be empty (a pure ordering barrier).</param>
        /// <returns>The chained flush, awaitable to observe predecessors and this flush.</returns>
        let chainFlush (events: SessionEvent list) : Task<unit> =
            let previous = flushChain

            let chained =
                task {
                    do! previous

                    for batch in events |> List.chunkBySize bounds.MaxBatchEvents do
                        let prepared = ResizeArray<SessionEvent>(batch) :> IReadOnlyList<SessionEvent>

                        match!
                            JournalWriter.appendWithTokenAsync
                                eventStore
                                tenant
                                sessionId
                                token
                                prepared
                                CancellationToken.None
                        with
                        | JournalWriter.JournalAppended stamped ->
                            totalAppends <- totalAppends + 1
                            totalEvents <- totalEvents + (if isNull (box stamped) then 0 else stamped.Count)
                        | JournalWriter.JournalRejected _ -> return raise (TurnLoop.TurnLeaseLostException())
                        | JournalWriter.JournalFailed reason -> return raise (InvalidOperationException(reason))
                }

            flushChain <- chained
            chained

        /// Snapshots the buffered remainder into events and clears the
        /// buffers. Runs on the turn thread only.
        /// <returns>The buffered events, in journal order.</returns>
        let takeBuffered () : SessionEvent list =
            let text = String.Concat(textChunks)
            let reasoning = String.Concat(reasoningChunks)
            textChunks.Clear()
            reasoningChunks.Clear()
            textChars <- 0
            reasoningChars <- 0
            pendingBytes <- 0
            buildEvents text reasoning

        /// The journal appends the turn spent on streaming deltas so far.
        member this.TotalAppends = totalAppends

        /// The delta events journaled so far.
        member this.TotalEvents = totalEvents

        /// The largest pending char count observed before a flush.
        member this.MaxPendingChars = maxPendingChars

        /// The largest pending byte count observed before a flush.
        member this.MaxPendingBytes = maxPendingBytes

        /// Buffers one assistant-text chunk, chaining a flush first when a
        /// pending cap already trips. Never blocks: the flush runs chained
        /// in the background while buffering continues. Null and empty
        /// chunks are provider keepalives, never deltas.
        /// <param name="text">The text chunk that arrived.</param>
        member this.AppendText(text: string) : unit =
            if not (String.IsNullOrEmpty text) then
                if
                    pendingEventCount () >= bounds.MaxPendingEvents
                    || pendingBytes >= bounds.MaxPendingBytes
                then
                    takeBuffered () |> chainFlush |> ignore

                textChunks.Add(text)
                textChars <- textChars + text.Length
                pendingBytes <- pendingBytes + System.Text.Encoding.UTF8.GetByteCount(text)
                maxPendingChars <- max maxPendingChars (textChars + reasoningChars)
                maxPendingBytes <- max maxPendingBytes pendingBytes

        /// Buffers one provider-surfaced reasoning chunk, like AppendText.
        /// <param name="text">The reasoning chunk that arrived.</param>
        member this.AppendReasoning(text: string) : unit =
            if not (String.IsNullOrEmpty text) then
                if
                    pendingEventCount () >= bounds.MaxPendingEvents
                    || pendingBytes >= bounds.MaxPendingBytes
                then
                    takeBuffered () |> chainFlush |> ignore

                reasoningChunks.Add(text)
                reasoningChars <- reasoningChars + text.Length
                pendingBytes <- pendingBytes + System.Text.Encoding.UTF8.GetByteCount(text)
                maxPendingChars <- max maxPendingChars (textChars + reasoningChars)
                maxPendingBytes <- max maxPendingBytes pendingBytes

        /// Lands the buffered remainder in bounded appends and awaits the
        /// whole chain: at most MaxBatchEvents events per storage
        /// operation, so a sustained stream never needs a synchronous
        /// storage operation per token and never grows one unbounded event.
        /// Awaits outstanding mid-stream flushes even with nothing new, so
        /// the attempt boundary observes every predecessor before the actor
        /// journals suspension or settlement.
        /// <returns>The attempt's streaming flush, awaitable to completion.</returns>
        member this.FlushAsync() : Task<unit> =
            let buffered = takeBuffered ()

            task { do! chainFlush buffered }

    /// Execution-context-scoped per-attempt streaming journaler (issue
    /// 379): the actor enters it before invoking the runner (alongside
    /// FencedClaimScope, ControlAdmission, and LeaseAdmission); the
    /// runner's delta hooks read it to buffer under the running attempt,
    /// so nested continuations inherit it and resumed attempts observe the
    /// fresh journaler. AsyncLocal, None outside a streaming turn.
    module StreamingScope =

        let private current = AsyncLocal<StreamingJournaler option>()

        /// Reads the running attempt's streaming journaler, or None outside
        /// a streaming turn.
        /// <returns>The journaler, or None when no streaming turn is running.</returns>
        let currentJournaler () : StreamingJournaler option = current.Value

        /// Enters the scope for one running attempt.
        /// <param name="journaler">The attempt's journaler, or None for unclaimed shells.</param>
        /// <returns>The scope to dispose when the attempt reports.</returns>
        let enter (journaler: StreamingJournaler option) =
            let previous = current.Value
            current.Value <- journaler

            { new IDisposable with
                member _.Dispose() = current.Value <- previous
            }

/// The session actor behaviour and its client boundary. Internal so no Akka
/// type ever crosses the public API.
module internal SessionActor =

    /// How long a client-boundary Ask waits for the actor's reply before
    /// the call fails. The actor answers from memory plus fast store reads,
    /// so this only fires when the system is wedged.
    let askTimeout = TimeSpan.FromSeconds 10.0

    /// Reason carried by TurnAborted when an Interrupt prompt pre-empts the
    /// running turn. Never contains secrets or tool arguments.
    [<Literal>]
    let InterruptReason = "The turn was interrupted by a new message."

    /// Awaits a fast store metadata task synchronously on the actor thread.
    /// The actor computation cannot bind tasks (only mailbox receives), and
    /// blocking here preserves the single-threaded sequencing the
    /// store-first transitions rely on. The unbounded turn execution never
    /// blocks: it runs fire-and-forget and pipes its outcome back.
    /// <param name="task">The store task to await.</param>
    /// <returns>The task's result.</returns>
    let private awaitTask<'T> (task: Task<'T>) : 'T = task.GetAwaiter().GetResult()

    /// Selects the drainable entries from a pending read in position order,
    /// in two tiers: Interrupt user messages first in position order, then
    /// Queue-plus-Inject user messages in position order. Reply payloads
    /// never drain to a new turn: they resume a suspended turn instead, so
    /// they stay pending for their owning issue, as does anything that is
    /// not a user message. Null entries and a null read result are treated
    /// as empty.
    /// <param name="entries">The pending read result.</param>
    /// <returns>The drainable entries, Interrupt tier first.</returns>
    let private selectDrainableEntries (entries: IReadOnlyList<InboxEntry>) : InboxEntry list =
        if isNull (box entries) then
            []
        else
            let userMessages =
                entries
                |> Seq.filter (fun entry -> not (isNull (box entry)) && (entry.Payload :? UserMessagePayload))
                |> Seq.sortBy (fun entry -> entry.Position)
                |> List.ofSeq

            let interrupts =
                userMessages
                |> List.filter (fun entry -> entry.Delivery = DeliveryMode.Interrupt)

            let queued =
                userMessages
                |> List.filter (fun entry ->
                    entry.Delivery = DeliveryMode.Queue || entry.Delivery = DeliveryMode.Inject)

            interrupts @ queued

    /// Store-backed Inject fold wiring for one session: what the
    /// Inject-aware turn runner closes over to fold Inject entries at
    /// iteration boundaries (issue 34 over issue 41's drain hooks). The
    /// drain reads the session's pending inbox, the journal sink observes
    /// one UserMessageEvent per folded entry under the running turn's id,
    /// and the consume marks each folded entry so it never refolds.
    type internal InjectFoldWiring =
        {
            /// The durable store the inbox persists through.
            Store: ISessionStore
            /// The tenant the session belongs to.
            Tenant: TenantId
            /// The session the wiring drains and consumes for.
            SessionId: SessionId
            /// Observes each folded Inject entry as a UserMessageEvent, or
            /// None for no observation. Guarded by the runner: a throwing
            /// observer never kills the turn.
            JournalEvent: (UserMessageEvent -> unit) option
        }

    /// Builds a Queue turn runner over TurnLoop.runAsync: one ChatRole.User
    /// history message from the entry's parts and no Inject fold, running
    /// under the given lease hook and the given deadline seam. With an
    /// Inject wiring the runner instead calls
    /// TurnLoop.runAsyncWithInjects: the turn mints one TurnId at
    /// invocation (the invocation runs synchronously inside the actor's
    /// startTurn, so the id is the running turn's), folds pending Inject
    /// entries at each iteration boundary, journals each folded entry once
    /// as a UserMessageEvent under that id, and marks it consumed before
    /// the next provider call. The drain and consume block the turn thread
    /// on the store (the hooks are synchronous): the consume must land
    /// before the turn reports back, or the settle drain would redeliver
    /// the folded entry as a new turn and journal it twice. The
    /// would-complete pending signal is discarded: the settle drain
    /// re-reads the store and starts the Inject new turn implicitly.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <param name="isLeaseValid">The lease hook the loop checks. Must not be null.</param>
    /// <param name="inject">The store-backed Inject fold wiring, or None for a Queue-only turn with no fold.</param>
    /// <param name="getSystemPrompt">The composed system prompt hook (issue 66), or None to run with no system message.</param>
    /// <returns>A runner executing one inbox entry per turn.</returns>
    let private runnerFor
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        (isLeaseValid: unit -> bool)
        (inject: InjectFoldWiring option)
        (getSystemPrompt: PromptComposition.GetTurnSystemPrompt option)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(tools)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(isLeaseValid)

        fun entry cancellationToken ->
            task {
                let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

                // Package-load step (issue 66): resolve the composed
                // system prompt and lead the history with it. A
                // null/empty resolution keeps today's user-only shape.
                match getSystemPrompt with
                | Some resolve ->
                    let! systemPrompt = resolve entry cancellationToken
                    PromptComposition.prependSystemPrompt history systemPrompt
                | None -> ()

                match entry.Payload with
                | :? UserMessagePayload as userMessage when
                    not (isNull (box userMessage))
                    && not (isNull (box userMessage.Message))
                    && not (isNull (box userMessage.Message.Parts))
                    ->
                    let parts = ResizeArray<AIContent>()

                    for part in userMessage.Message.Parts do
                        if not (isNull (box part)) then
                            parts.Add(part)

                    history.Add(ChatMessage(ChatRole.User, parts :> IList<AIContent>))
                | _ -> history.Add(ChatMessage(ChatRole.User, ""))

                match inject with
                | None -> return! TurnLoop.runAsync client history tools options delay cancellationToken isLeaseValid
                | Some wiring ->
                    // Per-turn mint: this closure runs synchronously inside
                    // the actor's startTurn, once per turn, so the id is
                    // the running (injecting) turn's for every entry this
                    // turn folds. The initial entry journals once here as
                    // the turn's own evidence; folded Inject entries journal
                    // once each at their fold boundary below. Pending inbox
                    // entries with no event are queued-but-not-executed;
                    // rejected prompts never reach the runner and journal
                    // nothing, so applied vs queued vs rejected stay
                    // distinguishable.
                    let turnId = TurnId.New()

                    let drainInjected () : IReadOnlyList<InboxEntry> =
                        // Cancellable Inject read (issue 391): the attempt
                        // token reaches the store, so Abort and shutdown
                        // during the fold propagate truthfully instead of
                        // hiding as empty. A failed read propagates instead
                        // of hiding as no Injects; pending input stays
                        // pending, never lost nor fabricated.
                        let pending =
                            wiring.Store
                                .ReadPendingInbox(wiring.Tenant, wiring.SessionId, cancellationToken)
                                .GetAwaiter()
                                .GetResult()

                        if isNull (box pending) then
                            ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>
                        else
                            // The running entry stays pending until settle:
                            // exclude it so an Inject start never refolds
                            // itself and journals twice.
                            pending
                            |> Seq.filter (fun candidate ->
                                isNull (box candidate) |> not && candidate.Position <> entry.Position)
                            |> ResizeArray
                            :> IReadOnlyList<InboxEntry>

                    let observeUserEvidence (sessionId: SessionId) (message: UserMessage) : unit =
                        if not (isNull (box message)) then
                            if not (isLeaseValid ()) then
                                raise (TurnLoop.TurnLeaseLostException())

                            let event =
                                UserMessageEvent(sessionId, turnId, Nullable<int64>(), DateTimeOffset.UtcNow, message)

                            // Route the fold through the journal writer:
                            // the observer sees the redacted shape, so a
                            // downstream journal carries no secrets. The
                            // writer never drops: kind and ids survive,
                            // only secret shapes are replaced. A fenced-out
                            // loser raises above instead of observing, so
                            // it journals nothing.
                            let redacted = JournalWriter.sanitizeEvent event :?> UserMessageEvent

                            match wiring.JournalEvent with
                            | Some observe ->
                                try
                                    observe redacted
                                with _ ->
                                    ()
                            | None -> ()

                    let journalInjected (injected: InboxEntry) : unit =
                        if not (isNull (box injected)) then
                            match injected.Payload with
                            | :? UserMessagePayload as payload when
                                not (isNull (box payload)) && not (isNull (box payload.Message))
                                ->
                                observeUserEvidence injected.SessionId payload.Message
                            | _ -> ()

                    let consumeInjected (injected: InboxEntry) : unit =
                        // Truthful Inject consume (issue 391): the attempt
                        // token reaches the store; cancellation and store
                        // failures propagate instead of claiming success.
                        // A fenced-out loser raises TurnLeaseLostException
                        // with zero effects; pending input stays pending.
                        if not (isNull (box injected)) then
                            let positions = [| injected.Position |] :> IReadOnlyList<int64>

                            try
                                match FencedClaimScope.currentClaim () with
                                | Some claim when not (isNull (box claim)) ->
                                    let landed =
                                        ClaimFence.consumeInboxAsync
                                            wiring.Store
                                            wiring.Tenant
                                            claim
                                            wiring.SessionId
                                            positions
                                            cancellationToken
                                        |> fun task -> task.GetAwaiter().GetResult()

                                    if not landed then
                                        raise (TurnLoop.TurnLeaseLostException())
                                | _ ->
                                    wiring.Store
                                        .MarkInboxConsumed(
                                            wiring.Tenant,
                                            wiring.SessionId,
                                            positions,
                                            cancellationToken
                                        )
                                        .GetAwaiter()
                                        .GetResult()
                                    |> ignore
                            with
                            | :? TurnLoop.TurnLeaseLostException -> reraise ()
                            | :? OperationCanceledException -> reraise ()
                            | _ -> reraise ()

                    // Initial evidence, once per delivery semantics: the
                    // turn's own entry journals exactly once under the
                    // running turn's id before the first provider call, so
                    // the transcript holds the actual user content even when
                    // no Inject ever folds. A fenced-out loser raises here
                    // with zero provider effects.
                    match entry.Payload with
                    | :? UserMessagePayload as initial when
                        not (isNull (box initial)) && not (isNull (box initial.Message))
                        ->
                        observeUserEvidence entry.SessionId initial.Message
                    | _ -> ()

                    let! completion =
                        TurnLoop.runAsyncWithInjects
                            client
                            history
                            tools
                            options
                            delay
                            cancellationToken
                            isLeaseValid
                            drainInjected
                            journalInjected
                            consumeInjected

                    return completion.Result
            }

    /// Builds the default turn runner over TurnLoop.runAsync for Queue
    /// delivery: one ChatRole.User history message from the entry's parts,
    /// no Inject fold, and an always-live lease hook with no claim fence.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <returns>A runner executing one Queue inbox entry per turn.</returns>
    let createTurnRunner
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        runnerFor client tools options delay (fun () -> true) None None

    /// Builds the composed turn runner over TurnLoop.runAsync for Queue
    /// delivery (issue 66): the same history shape as
    /// <see cref="M:Legate.SessionActor.createTurnRunner" />, but the
    /// turn's composed system prompt leads the history as a
    /// ChatRole.System message. A null or empty resolution runs the
    /// user-only shape, so an uncomposed turn behaves exactly like the
    /// default runner.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <param name="getSystemPrompt">Resolves the turn's composed system prompt. Must not be null.</param>
    /// <returns>A runner executing one Queue inbox entry per turn with the composed system message first.</returns>
    let createComposedTurnRunner
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        (getSystemPrompt: PromptComposition.GetTurnSystemPrompt)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        if isNull (box getSystemPrompt) then
            raise (ArgumentNullException(nameof getSystemPrompt))

        runnerFor client tools options delay (fun () -> true) None (Some getSystemPrompt)

    /// Builds the Inject-aware turn runner over
    /// TurnLoop.runAsyncWithInjects (issue 34): the same history shape as
    /// <see cref="M:Legate.SessionActor.createTurnRunner" />, folding
    /// pending Inject entries at each iteration boundary through the
    /// store-backed wiring and journaling each folded entry once as a
    /// UserMessageEvent under the running turn's id. The would-complete
    /// pending signal is discarded: the settle drain re-reads the store
    /// and starts the Inject new turn implicitly.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <param name="wiring">The store-backed Inject fold wiring for the session.</param>
    /// <returns>A runner executing one inbox entry per turn with the Inject fold.</returns>
    let createInjectFoldRunner
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        (wiring: InjectFoldWiring)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        if isNull (box wiring) then
            raise (ArgumentNullException(nameof wiring))

        runnerFor client tools options delay (fun () -> true) (Some wiring) None

    /// Builds the claimed turn runner over TurnLoop.runAsync for Queue
    /// delivery (issue 33): the same history shape as
    /// <see cref="M:Legate.SessionActor.createTurnRunner" />, running under
    /// the claim's heartbeat-backed lease hook with the options-carried
    /// per-tool fence, so a fenced loser stops before its next provider
    /// call and never invokes a tool.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <param name="isLeaseValid">The heartbeat-backed lease hook (ClaimHeartbeat.ClaimLeaseView.IsValid). Must not be null.</param>
    /// <param name="verifyClaim">The last-moment per-tool fence (ClaimFence.checkBeforeCallAsync), or None for no fence.</param>
    /// <returns>A runner executing one Queue inbox entry per turn under the claim.</returns>
    let createClaimedTurnRunner
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        (isLeaseValid: unit -> bool)
        (verifyClaim: (unit -> Task<bool>) option)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        runnerFor
            client
            tools
            { options with
                VerifyClaim = verifyClaim
            }
            delay
            isLeaseValid
            None
            None

    // ────────────────── Compaction wiring (issue 45) ──────────────────

    /// Store-backed compaction wiring for one turn: what the TurnLoop
    /// iteration-boundary hook closes over to compact at each boundary
    /// (issue 45 over issue 44's threshold). The hook resolves the catalog
    /// entry for the session model once here, journals through
    /// JournalWriter.appendWithTokenAsync under the wiring's journal token,
    /// and checks the lease hook at the last moment before every journal
    /// write, so the takeover loser journals nothing. Sub-agent turns share
    /// the TurnLoop entry and its options-carried hook, so they inherit the
    /// same mechanism with no separate code.
    type internal CompactionWiring =
        {
            /// LLM settings carrying the Compaction model override and the
            /// CompactionKeepMessages tail. Never null.
            Llm: LlmOptions
            /// The tokens held back beyond the reserved output when deriving
            /// the threshold.
            ReservedBufferTokens: int
            /// The session's model: the threshold lookup and default
            /// summariser model.
            SessionModel: ModelReference
            /// Resolves the session model to its catalog entry, or null
            /// when the host keeps no catalog (the threshold falls back to
            /// the catalog defaults).
            Catalog: ILlmModelCatalog | null
            /// The chat client the turn runs against. Never null.
            Client: IChatClient
            /// Receives the summariser usage checkpoint, or null.
            Observer: IUsageObserver | null
            /// Authorises the summariser model call, or null.
            Policy: IModelPolicy | null
            /// The tenant the session belongs to.
            Tenant: TenantId
            /// The session the turn runs in.
            SessionId: SessionId
            /// The turn compacting.
            TurnId: TurnId
            /// The 1-based attempt the turn runs under.
            Attempt: int
            /// The journal the compaction events append to. Never null.
            EventStore: ISessionEventStore
            /// The claim token fencing the journal appends. Never null.
            JournalToken: string
            /// The last-moment claim fence the journal writes check. Never
            /// null.
            IsLeaseValid: unit -> bool
        }

    /// Resolves the session model's catalog entry: the wiring's catalog, or
    /// null when the host keeps no catalog (the threshold falls back to the
    /// catalog defaults).
    /// <param name="catalog">The model catalog, or null for the fallback threshold.</param>
    /// <param name="sessionModel">The session's model.</param>
    /// <returns>The catalog entry, or null when the model is unknown.</returns>
    let private catalogEntryOf
        (catalog: ILlmModelCatalog | null)
        (sessionModel: ModelReference)
        : ModelCatalogEntry | null =
        match box catalog with
        | null -> Unchecked.defaultof<ModelCatalogEntry>
        | :? ILlmModelCatalog as live -> live.GetEntry(sessionModel)
        | _ -> Unchecked.defaultof<ModelCatalogEntry>

    /// Journals one event under a claim token: the sink every compaction
    /// path shares, fenced store-side so a takeover loser journals nothing.
    /// <param name="eventStore">The journal the event appends to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session compacting.</param>
    /// <param name="token">The claim token fencing the append. Must not be null.</param>
    /// <returns>The single-event journal sink.</returns>
    let private journalSink
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (token: string)
        : SessionEvent -> Task<JournalWriter.JournalWriteResult> =
        fun event ->
            let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

            JournalWriter.appendWithTokenAsync eventStore tenant sessionId token events CancellationToken.None

    /// Validates the store-backed compaction wiring shared by the hook
    /// builders: every reference the boundary dereferences must be set.
    /// <param name="wiring">The wiring to validate. Must not be null.</param>
    let private requireWiring (wiring: CompactionWiring) : unit =
        if isNull (box wiring) then
            raise (ArgumentNullException(nameof wiring))

        ArgumentNullException.ThrowIfNull(wiring.Llm)
        ArgumentNullException.ThrowIfNull(wiring.Client)
        ArgumentNullException.ThrowIfNull(wiring.EventStore)
        ArgumentNullException.ThrowIfNull(wiring.JournalToken)
        ArgumentNullException.ThrowIfNull(wiring.IsLeaseValid)

    /// Builds the per-turn hook dependencies from the store-backed wiring.
    /// <param name="wiring">The store-backed compaction wiring for the turn. Must be validated.</param>
    /// <param name="turnId">The turn compacting.</param>
    /// <param name="attempt">The 1-based attempt the turn runs under.</param>
    /// <param name="journalAsync">The fenced single-event journal sink. Must not be null.</param>
    /// <param name="isLeaseValid">The last-moment claim fence. Must not be null.</param>
    /// <returns>The boundary dependencies.</returns>
    let private hookDepsOf
        (wiring: CompactionWiring)
        (turnId: TurnId)
        (attempt: int)
        (journalAsync: SessionEvent -> Task<JournalWriter.JournalWriteResult>)
        (isLeaseValid: unit -> bool)
        : Compaction.CompactionHookDeps =
        {
            Client = wiring.Client
            SessionModel = wiring.SessionModel
            CompactionModel = wiring.Llm.Compaction
            KeepMessages = wiring.Llm.CompactionKeepMessages
            CatalogEntry = catalogEntryOf wiring.Catalog wiring.SessionModel
            ReservedBufferTokens = wiring.ReservedBufferTokens
            Observer = wiring.Observer
            ModelPolicy = wiring.Policy
            Tenant = wiring.Tenant
            SessionId = wiring.SessionId
            TurnId = turnId
            Attempt = attempt
            JournalAsync = journalAsync
            IsLeaseValid = isLeaseValid
        }

    /// Builds the per-turn compaction hook from the store-backed wiring:
    /// one summarise-and-rewrite pass per iteration boundary, journaled
    /// under the wiring's token and fenced by its lease hook.
    /// <param name="wiring">The store-backed compaction wiring for the turn.</param>
    /// <returns>The boundary hook for TurnLoopOptions.</returns>
    let buildCompactionHook (wiring: CompactionWiring) : TurnLoop.CompactionHook =
        requireWiring wiring

        let journalAsync =
            journalSink wiring.EventStore wiring.Tenant wiring.SessionId wiring.JournalToken

        Compaction.createHook (hookDepsOf wiring wiring.TurnId wiring.Attempt journalAsync wiring.IsLeaseValid)

    /// Builds the force-aware per-turn compaction hook from the store-backed
    /// wiring (issue 46): each boundary takes the shared one-shot force
    /// cell, so an armed Compact fires one threshold-bypassed pass at the
    /// next boundary exactly once while unforced boundaries behave like
    /// <see cref="M:Legate.SessionActor.buildCompactionHook" />. The host
    /// shares the cell instance with the CompactDeps it passes in props.
    /// <param name="force">The one-shot cell the session actor arms. Must not be null.</param>
    /// <param name="wiring">The store-backed compaction wiring for the turn.</param>
    /// <returns>The force-aware boundary hook for TurnLoopOptions.</returns>
    let buildForcedCompactionHook
        (force: Compaction.CompactForce)
        (wiring: CompactionWiring)
        : TurnLoop.CompactionHook =
        ArgumentNullException.ThrowIfNull(force)
        requireWiring wiring

        let journalAsync =
            journalSink wiring.EventStore wiring.Tenant wiring.SessionId wiring.JournalToken

        Compaction.createForceHook
            force
            (hookDepsOf wiring wiring.TurnId wiring.Attempt journalAsync wiring.IsLeaseValid)

    /// Validates the on-demand compaction dependencies: every reference the
    /// Idle path dereferences must be set.
    /// <param name="compact">The dependencies to validate. Must not be null.</param>
    let private requireCompactDeps (compact: CompactDeps) : unit =
        if isNull (box compact) then
            raise (ArgumentNullException(nameof compact))

        ArgumentNullException.ThrowIfNull(compact.Llm)
        ArgumentNullException.ThrowIfNull(compact.Client)
        ArgumentNullException.ThrowIfNull(compact.EventStore)
        ArgumentNullException.ThrowIfNull(compact.JournalToken)
        ArgumentNullException.ThrowIfNull(compact.Force)

    /// Starts one piped lifecycle store wait through a starter: the task
    /// factory runs on the actor thread without blocking it (a synchronously
    /// throwing factory reads as a faulted wait, exactly like an awaited
    /// throw); a completed wait runs its resumption inline (fast path,
    /// today's sequencing), otherwise the outcome pipes back as a one-way
    /// message and the loop suspends with the wait outstanding. The suspension
    /// always re-enters the loop with the state current at the wait's start;
    /// resumptions receive the state current at the wait's settle, so control
    /// recorded meanwhile is never lost.
    let private startPipedWait<'T, 'M, 'A>
        (starter: PipeStarter<'M, 'A>)
        (taskFactory: unit -> Task<'T>)
        (label: string)
        (resume: 'A -> LifecyclePipe.PipeState<'M, 'A> -> Result<'T, exn> -> Cont<'M, unit>)
        (suspendWith: 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (args: 'A)
        (pipe: LifecyclePipe.PipeState<'M, 'A>)
        : Cont<'M, unit> =
        let task =
            try
                let started = taskFactory ()

                if isNull (box started) then
                    Task.FromException<'T>(ArgumentNullException("taskFactory") :> exn)
                else
                    started
            with ex ->
                Task.FromException<'T>(ex)

        LifecyclePipe.start
            starter.Self
            starter.Clock
            starter.Timeout
            label
            task
            resume
            starter.PackCompleted
            starter.PackTimeout
            (suspendWith args)
            args
            pipe

    /// Starts one non-generic piped lifecycle store wait through a starter.
    /// Same contract as startPipedWait.
    let private startPipedWaitUnit<'M, 'A>
        (starter: PipeStarter<'M, 'A>)
        (taskFactory: unit -> Task)
        (label: string)
        (resume: 'A -> LifecyclePipe.PipeState<'M, 'A> -> Result<unit, exn> -> Cont<'M, unit>)
        (suspendWith: 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (args: 'A)
        (pipe: LifecyclePipe.PipeState<'M, 'A>)
        : Cont<'M, unit> =
        let task =
            try
                let started = taskFactory ()

                if isNull (box started) then
                    Task.FromException(ArgumentNullException("taskFactory") :> exn)
                else
                    started
            with ex ->
                Task.FromException(ex)

        LifecyclePipe.startUnit
            starter.Self
            starter.Clock
            starter.Timeout
            label
            task
            resume
            starter.PackCompleted
            starter.PackTimeout
            (suspendWith args)
            args
            pipe

    /// Replies Status.Failure when a lifecycle message arrives past the bounded
    /// deferred queue: the store dependency is delayed and the actor refuses to
    /// grow without bound. Callers pass the Ask sender; one-way senders absorb
    /// it as dead letters.
    let private replyPipeOverflow (sender: IActorRef) : unit =
        sender
        <! Status.Failure(
            DeadlineExceededException(
                "session-lifecycle-pipe",
                sprintf
                    "The session actor deferred more than %d lifecycle messages behind a delayed store dependency."
                    LifecyclePipe.MaxDeferredMessages
            )
            :> Exception
        )

    /// Builds one base loop's pipe starter: completions Tell the loop's own
    /// actor and pack for the base protocol.
    /// <param name="self">The base loop's own actor.</param>
    /// <param name="config">The resolved pipe configuration.</param>
    /// <returns>The starter the loop's waits run through.</returns>
    let private behaviorStarter
        (self: IActorRef)
        (config: LifecyclePipe.StorePipeConfig)
        : PipeStarter<SessionActorMessage, BehaviorLoopArgs> =
        {
            Self = self
            Clock = config.Clock
            Timeout = config.Timeout
            PackCompleted = fun (opId, incarnation, outcome) -> LifecycleStoreCompleted(opId, incarnation, outcome)
            PackTimeout = fun (opId, incarnation) -> LifecycleStoreTimeout(opId, incarnation)
        }

    /// Compacts an Idle session now without starting a turn: replays the
    /// journal through the shared compacted-base builder (issue 387),
    /// then runs the forced core (threshold bypass, replaceable guard
    /// kept) without a synthetic user turn or execution claim. Under
    /// threshold (or with nothing replaceable) no summariser call runs;
    /// a summariser failure, failed persistence, or recovery rejection
    /// journals no success (a failure event at most) while the session
    /// continues uncompacted. The mailbox serializes Idle work, so the
    /// lease hook is actor-owned constant-true; the store-side host fence
    /// still fences a takeover loser into TurnLeaseLostException before
    /// anything journals. Truthful idle-operation attribution: the
    /// journaled CompactedEvent carries the default TurnId sentinel and
    /// zero turn totals, never an executing turn. Every wait runs piped
    /// (issue 390): the dispatcher thread never blocks on the replay, the
    /// stamp read, or the summariser call.
    /// <param name="starter">The loop's pipe starter.</param>
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="compact">The on-demand compaction wiring. Must be validated.</param>
    /// <param name="cancellationToken">Abandons the replay and the summariser call.</param>
    /// <param name="cont">Continues with the compact reply.</param>
    /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
    /// <param name="args">The current loop state.</param>
    /// <param name="pipe">The current pipe state.</param>
    /// <returns>The actor computation.</returns>
    let private compactIdleNowPiped<'M, 'A>
        (starter: PipeStarter<'M, 'A>)
        (props: SessionActorProps)
        (compact: CompactDeps)
        (cancellationToken: CancellationToken)
        (cont: SessionCompactReply -> 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (suspendWith: 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (args: 'A)
        (pipe: LifecyclePipe.PipeState<'M, 'A>)
        : Cont<'M, unit> =
        let runCore
            (history: IList<ChatMessage>)
            (expectedStamp: DateTimeOffset option)
            (args2: 'A)
            (pipe2: LifecyclePipe.PipeState<'M, 'A>)
            : Cont<'M, unit> =
            let journalAsync =
                match expectedStamp with
                | Some stamp ->
                    fun (event: SessionEvent) ->
                        let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

                        JournalWriter.appendHostAsync
                            compact.EventStore
                            props.Tenant
                            props.SessionId
                            stamp
                            events
                            CancellationToken.None
                | None -> journalSink compact.EventStore props.Tenant props.SessionId compact.JournalToken

            let request: Compaction.CompactionRequest =
                {
                    Client = compact.Client
                    History = history
                    SessionModel = compact.SessionModel
                    CompactionModel = compact.Llm.Compaction
                    KeepMessages = compact.Llm.CompactionKeepMessages
                    CatalogEntry = catalogEntryOf compact.Catalog compact.SessionModel
                    ReservedBufferTokens = compact.ReservedBufferTokens
                    Observer = compact.Observer
                    ModelPolicy = compact.Policy
                    Tenant = props.Tenant
                    SessionId = props.SessionId
                    // The host-operation sentinel (issue 373): the idle
                    // compact is host authority, not execution, so the
                    // journaled CompactedEvent carries the default TurnId.
                    TurnId = Unchecked.defaultof<TurnId>
                    Attempt = 1
                    InputTokens = 0L
                    OutputTokens = 0L
                    JournalAsync = journalAsync
                    IsLeaseValid = (fun () -> true)
                    CancellationToken = cancellationToken
                }

            // Forced core (issue 387): explicit idle requests compact
            // eligible context below the automatic threshold, while the
            // replaceable guard still truthfully no-ops when nothing
            // can be replaced. No synthetic user turn, no execution
            // claim, idle-operation attribution via the sentinel above.
            startPipedWait
                starter
                (fun () -> Compaction.tryCompactCoreAsync true request)
                "compact-core"
                (fun args3 pipe3 ->
                    function
                    | Ok Compaction.NotNeeded -> cont CompactNotNeeded args3 pipe3
                    | Ok(Compaction.Compacted(beforeEstimate, afterEstimate, _, _)) ->
                        cont (CompactCompleted(beforeEstimate, afterEstimate)) args3 pipe3
                    | Ok(Compaction.FailedContinue _) -> cont CompactNotNeeded args3 pipe3
                    | Error(:? TurnLoop.TurnLeaseLostException) -> cont CompactFenced args3 pipe3
                    | Error(:? OperationCanceledException) -> cont CompactNotNeeded args3 pipe3
                    | Error error -> raise error)
                suspendWith
                args2
                pipe2

        let readStamp
            (history: IList<ChatMessage>)
            (args2: 'A)
            (pipe2: LifecyclePipe.PipeState<'M, 'A>)
            : Cont<'M, unit> =
            // The host fence stamp for the idle compact (issue 373): read
            // before the summariser call. A concurrent idle writer moves
            // the stamp and the host append below fences as CompactFenced.
            // A missing row falls back to the primed token sink, so the
            // compact stays fenced either way; the prime itself is untouched.
            // Any read failure reads as no stamp, exactly like before.
            startPipedWait
                starter
                (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                "compact-stamp"
                (fun args3 pipe3 ->
                    function
                    | Ok session ->
                        match session with
                        | null -> runCore history None args3 pipe3
                        | s -> runCore history (Some s.UpdatedAt) args3 pipe3
                    | Error _ -> runCore history None args3 pipe3)
                suspendWith
                args2
                pipe2

        startPipedWait
            starter
            (fun () ->
                BoundedReplay.readSuffixWithBaseAsync
                    compact.EventStore
                    props.Tenant
                    props.SessionId
                    100
                    cancellationToken)
            "compact-replay"
            (fun args2 pipe2 ->
                function
                | Error(:? TurnLoop.TurnLeaseLostException) -> cont CompactFenced args2 pipe2
                | Error(:? OperationCanceledException) -> cont CompactNotNeeded args2 pipe2
                | Error error -> raise error
                | Ok suffixRead ->
                    // Resolve the idle history through the shared
                    // checkpoint-resumed compacted base (issue 389), exactly
                    // like before: pure before the next piped wait.
                    try
                        let collected = BoundedReplay.recoveryInputOf suffixRead

                        let baseResolution = ConversationRecovery.tryRecoverCompacted collected

                        match baseResolution with
                        | Error _ when collected.Count > 0 ->
                            // Unsupported or incomplete compacted state:
                            // truthful no-op with no success published and
                            // no journal write.
                            cont CompactNotNeeded args2 pipe2
                        | _ ->
                            let history =
                                match baseResolution with
                                | Error _ -> ResizeArray<ChatMessage>() :> IList<ChatMessage>
                                | Ok resolved -> ResizeArray<ChatMessage>(resolved) :> IList<ChatMessage>

                            readStamp history args2 pipe2
                    with
                    | :? TurnLoop.TurnLeaseLostException -> cont CompactFenced args2 pipe2
                    | :? OperationCanceledException -> cont CompactNotNeeded args2 pipe2)
            suspendWith
            args
            pipe

    // ────────────────── AutoClose (issue 82) ──────────────────

    /// Reads whether the session closes itself after its first completed
    /// turn: the AutoClose snapshot the session was opened with. A missing
    /// row, missing options, or a store read failure reads as false, so the
    /// close never fires spuriously. The read runs piped (issue 390).
    /// <param name="starter">The loop's pipe starter.</param>
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="cont">Continues with whether AutoClose is enabled.</param>
    /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
    /// <param name="args">The current loop state.</param>
    /// <param name="pipe">The current pipe state.</param>
    /// <returns>The actor computation.</returns>
    let private withAutoCloseEnabled<'M, 'A>
        (starter: PipeStarter<'M, 'A>)
        (props: SessionActorProps)
        (cont: bool -> 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (suspendWith: 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (args: 'A)
        (pipe: LifecyclePipe.PipeState<'M, 'A>)
        : Cont<'M, unit> =
        startPipedWait
            starter
            (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
            "auto-close-read"
            (fun args2 pipe2 ->
                function
                | Ok session ->
                    match session with
                    | null -> cont false args2 pipe2
                    | s when isNull (box s.Options) -> cont false args2 pipe2
                    | s -> cont s.Options.AutoClose args2 pipe2
                | Error(:? SessionNotFoundException) -> cont false args2 pipe2
                | Error error -> raise error)
            suspendWith
            args
            pipe

    /// Consumes the settled entry and closes the session store-first for an
    /// AutoClose turn: the entry leaves the pending set before the Closed
    /// write lands, so a restart never redelivers a turn the close already
    /// answered. CloseSession is idempotent, and the single-flight pipe
    /// keeps a second prompt from slipping between the consume and the
    /// close. Both writes run piped (issue 390). The caller adjusts its
    /// pending-count cache in the continuation: this helper stays generic
    /// over both loops.
    /// <param name="starter">The loop's pipe starter.</param>
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="entry">The entry the AutoClose turn executed.</param>
    /// <param name="cont">Continues once the close landed.</param>
    /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
    /// <param name="args">The current loop state.</param>
    /// <param name="pipe">The current pipe state.</param>
    /// <returns>The actor computation.</returns>
    let private withConsumeAndClose<'M, 'A>
        (starter: PipeStarter<'M, 'A>)
        (props: SessionActorProps)
        (entry: InboxEntry)
        (cont: unit -> 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (suspendWith: 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (args: 'A)
        (pipe: LifecyclePipe.PipeState<'M, 'A>)
        : Cont<'M, unit> =
        let positions = [| entry.Position |] :> IReadOnlyList<int64>

        startPipedWaitUnit
            starter
            (fun () -> props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, CancellationToken.None))
            "consume-and-close/consume"
            (fun args2 pipe2 ->
                function
                | Error error -> raise error
                | Ok() ->
                    startPipedWaitUnit
                        starter
                        (fun () -> props.Store.CloseSession(props.Tenant, props.SessionId, CancellationToken.None))
                        "consume-and-close/close"
                        (fun args3 pipe3 ->
                            function
                            | Ok() ->
                                // Bounded transient state (issue 384): the
                                // durable close landed, so the session's hub
                                // and live hints release. Transient-only.
                                PromptWaitHubs.ReleaseSession props.Tenant props.SessionId |> ignore
                                cont () args3 pipe3
                            | Error error -> raise error)
                        suspendWith
                        args2
                        pipe2)
            suspendWith
            args
            pipe
    // ────────────────── Completion outbox (issue 84) ──────────────────

    /// Mints the stable idempotency key one settlement shares between its
    /// outbox row and its inline Notify: random 32-hex per settlement, so
    /// an inline delivery overlapping a re-drive deduplicates on the
    /// receiver's Idempotency-Key.
    /// <returns>A fresh stable key for one settlement.</returns>
    let private mintCompletionKey () : string = Guid.NewGuid().ToString("N")

    /// Entry-recovery-only synchronous completion enqueue (issue 390 keeps
    /// entry recovery synchronous): same best-effort guarded semantics as
    /// the piped path, for crash-recovery fail paths that run on the
    /// spawning thread before any loop exists.
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="result">The settled turn result to deliver.</param>
    /// <returns>The stored completion, or None when sinkless or best-effort failed.</returns>
    let private dispatchCompletionNow (props: SessionActorProps) (result: TurnResult) : SessionCompletion option =
        try
            if isNull (box result) then
                None
            else
                match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> None
                | session when isNull (box session.Options) -> None
                | session ->
                    match session.Options.CompletionDestinationId with
                    | null -> None
                    | destinationId ->
                        let completion =
                            {
                                SessionId = props.SessionId
                                TurnResult = result
                                Metadata = session.Options.Metadata
                                IdempotencyKey = mintCompletionKey ()
                            }

                        let stored =
                            try
                                awaitTask (
                                    props.Store.EnqueueCompletionOutbox(
                                        props.Tenant,
                                        destinationId,
                                        completion,
                                        CancellationToken.None
                                    )
                                )
                                |> Some
                            with _ ->
                                None

                        match stored with
                        | None -> None
                        | Some row -> Some row.Completion
        with _ ->
            None

    /// Enqueues the settlement's immutable route-snapshot completion row and
    /// continues with the stored completion: the durable redriver is the sole
    /// delivery path, so this step performs no inline notification. Only
    /// sessions carrying a completion destination id enqueue: sinkless
    /// sessions store nothing. Best-effort and guarded: a store failure
    /// stores nothing, and the single-flight pipe sequencing is the fence.
    /// Both reads run piped (issue 390).
    /// <param name="starter">The loop's pipe starter.</param>
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="result">The settled turn result to deliver.</param>
    /// <param name="cont">Continues with the stored completion, or None when sinkless or best-effort failed.</param>
    /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
    /// <param name="args">The current loop state.</param>
    /// <param name="pipe">The current pipe state.</param>
    /// <returns>The actor computation.</returns>
    let private withDispatchCompletion<'M, 'A>
        (starter: PipeStarter<'M, 'A>)
        (props: SessionActorProps)
        (result: TurnResult)
        (cont: SessionCompletion option -> 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (suspendWith: 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (args: 'A)
        (pipe: LifecyclePipe.PipeState<'M, 'A>)
        : Cont<'M, unit> =
        if isNull (box result) then
            cont None args pipe
        else
            startPipedWait
                starter
                (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                "dispatch-completion/read"
                (fun args2 pipe2 ->
                    function
                    | Error _ -> cont None args2 pipe2
                    | Ok session ->
                        match session with
                        | null -> cont None args2 pipe2
                        | s when isNull (box s.Options) -> cont None args2 pipe2
                        | s ->
                            match s.Options.CompletionDestinationId with
                            | null -> cont None args2 pipe2
                            | destinationId ->
                                let completion =
                                    {
                                        SessionId = props.SessionId
                                        TurnResult = result
                                        Metadata = s.Options.Metadata
                                        IdempotencyKey = mintCompletionKey ()
                                    }

                                startPipedWait
                                    starter
                                    (fun () ->
                                        props.Store.EnqueueCompletionOutbox(
                                            props.Tenant,
                                            destinationId,
                                            completion,
                                            CancellationToken.None
                                        ))
                                    "dispatch-completion/enqueue"
                                    (fun args3 pipe3 ->
                                        function
                                        | Ok row when not (isNull (box row)) -> cont (Some row.Completion) args3 pipe3
                                        | Ok _ -> cont None args3 pipe3
                                        | Error _ -> cont None args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2)
                suspendWith
                args
                pipe

    /// Writes the durable close and continues with the stored session,
    /// piped (issue 390): the hub and live hints release only once the
    /// write landed.
    /// <param name="starter">The loop's pipe starter.</param>
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="cancellationToken">Abandons the close.</param>
    /// <param name="cont">Continues with the closed session.</param>
    /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
    /// <param name="args">The current loop state.</param>
    /// <param name="pipe">The current pipe state.</param>
    /// <returns>The actor computation.</returns>
    let private withCloseWriteSession<'M, 'A>
        (starter: PipeStarter<'M, 'A>)
        (props: SessionActorProps)
        (cancellationToken: CancellationToken)
        (cont: Session -> 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (suspendWith: 'A -> LifecyclePipe.PipeState<'M, 'A> -> Cont<'M, unit>)
        (args: 'A)
        (pipe: LifecyclePipe.PipeState<'M, 'A>)
        : Cont<'M, unit> =
        startPipedWait
            starter
            (fun () -> props.Store.CloseSession(props.Tenant, props.SessionId, cancellationToken))
            "close-session"
            (fun args2 pipe2 ->
                function
                | Error error -> raise error
                | Ok closed ->
                    // Bounded transient state (issue 384): the durable
                    // close landed, so the session's hub and live hints
                    // release. Transient-only.
                    PromptWaitHubs.ReleaseSession props.Tenant props.SessionId |> ignore
                    cont closed args2 pipe2)
            suspendWith
            args
            pipe

    /// The session actor: recovers from the store, then owns the state
    /// machine. The mailbox parameter is injected by the spawn functions;
    /// one message is processed fully before the next is received, so the
    /// store-first sequences never interleave.
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="mailbox">The actor mailbox, injected by spawn.</param>
    /// <returns>The Akka.FSharp actor computation to spawn.</returns>
    let behavior (props: SessionActorProps) (mailbox: Actor<SessionActorMessage>) =
        if isNull (box props.Store) then
            raise (ArgumentNullException(nameof props))

        if isNull (box props.RunTurn) then
            raise (ArgumentNullException(nameof props))

        match props.Compact with
        | Some compact -> requireCompactDeps compact
        | None -> ()

        let self = mailbox.Self
        let pipeConfig = LifecyclePipe.resolveConfig props.StorePipe
        let starter = behaviorStarter self pipeConfig

        let log = LoggingScopes.resolveLogger props.Logger

        /// Logs one actor point under the six canonical scopes, scoped to
        /// the synchronous handler block only (never across awaits). Text
        /// travels redacted, so a prompt carrying a secret shape never
        /// lands verbatim in the log.
        /// <param name="turnId">The turn, or null for prompt receipt.</param>
        /// <param name="message">The fixed message template.</param>
        let logScoped (turnId: string | null) (message: string) : unit =
            let scope =
                LoggingScopes.createScope (props.Tenant.ToString()) (props.SessionId.ToString()) turnId null 0 null

            use _scope = LoggingScopes.beginScope log scope
            log.LogInformation("{Message}", LoggingScopes.redactForLog message)

        /// Builds the observable snapshot from the loop state: the
        /// lifecycle state, the cached pending inbox count, and the running
        /// entry position. Pure memory, so Abort and GetSnapshot answer
        /// while a store wait is outstanding.
        /// <param name="args">The current loop state.</param>
        /// <returns>The actor's current snapshot.</returns>
        let takeSnapshot (args: BehaviorLoopArgs) : SessionSnapshot =
            {
                SessionId = props.SessionId
                State = args.State
                PendingCount = args.PendingCount
                RunningPosition = args.Running |> Option.map (fun inFlight -> inFlight.Entry.Position)
                PendingRequestId = null
            }

        /// Reads the live turn one turn runs as (issue 289): the claimed
        /// turn id the settle choke points journal under. A missing row or
        /// an empty snapshot mints fresh, preserving the pre-plumbing
        /// shape. Any read failure mints fresh, exactly like before. The
        /// read runs piped.
        /// <param name="cont">Continues with the turn id.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withTurnIdSnapshot
            (cont: TurnId -> BehaviorCont)
            (suspendWith: BehaviorCont)
            (args: BehaviorLoopArgs)
            (pipe: BehaviorPipe)
            : Cont<SessionActorMessage, unit> =
            startPipedWait
                starter
                (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                "start-turn-snapshot"
                (fun args2 pipe2 ->
                    function
                    | Ok session ->
                        match session with
                        | null -> cont (TurnId.New()) args2 pipe2
                        | s when s.CurrentTurnId.HasValue -> cont s.CurrentTurnId.Value args2 pipe2
                        | _ -> cont (TurnId.New()) args2 pipe2
                    | Error _ -> cont (TurnId.New()) args2 pipe2)
                suspendWith
                args
                pipe

        /// Starts a turn for an inbox entry under an already-resolved turn
        /// id: guards the runner call itself (a synchronously throwing or
        /// null-returning runner faults the turn, never the actor), then
        /// pipes the outcome back as a one-way message without blocking the
        /// actor thread. Runs on the actor thread, like before.
        /// <param name="entry">The inbox entry the turn executes.</param>
        /// <param name="turnId">The turn the attempt runs as.</param>
        /// <returns>The in-flight turn handle.</returns>
        let startTurnNow (entry: InboxEntry) (turnId: TurnId) : RunningTurn =
            let cts = new CancellationTokenSource()

            let runTask =
                try
                    let started = props.RunTurn entry cts.Token

                    if isNull (box started) then
                        Task.FromException<TurnResult>(
                            InvalidOperationException("The session turn runner returned null.")
                        )
                    else
                        started
                with ex ->
                    Task.FromException<TurnResult>(ex)

            runTask.ContinueWith(fun (completed: Task<TurnResult>) ->
                if completed.IsCanceled then
                    self.Tell(SessionTurnFaulted(entry, OperationCanceledException("The session turn was aborted.")))
                elif completed.IsFaulted then
                    let error =
                        match completed.Exception with
                        | null ->
                            InvalidOperationException("The session turn faulted without an exception.") :> Exception
                        | aggregate -> aggregate.GetBaseException()

                    self.Tell(SessionTurnFaulted(entry, error))
                else
                    self.Tell(SessionTurnSettled(entry, completed.Result)))
            |> ignore

            {
                Entry = entry
                TurnId = turnId
                Cts = cts
            }

        /// Settles a finished turn attempt: consumes the attempt's entry
        /// store-first, then drains the next pending entry into a new turn
        /// or returns the session to Idle. The drain tiers Interrupt user
        /// messages first in position order, then Queue-plus-Inject user
        /// messages in position order; Reply payloads never start a turn.
        /// A final-iteration Inject the loop left pending (its
        /// would-complete signal is discarded across the runner boundary)
        /// starts its new turn here, implicitly. Every wait runs piped.
        /// <param name="entry">The entry the finished attempt executed.</param>
        /// <param name="cancellationToken">Abandons the settle reads.</param>
        /// <param name="cont">Continues with the next loop state and in-flight turn.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withSettle
            (entry: InboxEntry)
            (cancellationToken: CancellationToken)
            (cont: (SessionState * RunningTurn option) -> BehaviorCont)
            (suspendWith: BehaviorCont)
            (args: BehaviorLoopArgs)
            (pipe: BehaviorPipe)
            : Cont<SessionActorMessage, unit> =
            let positions = [| entry.Position |] :> IReadOnlyList<int64>

            startPipedWaitUnit
                starter
                (fun () -> props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, cancellationToken))
                "settle-consume"
                (fun args2 pipe2 ->
                    function
                    | Error error -> raise error
                    | Ok() ->
                        Telemetry.addQueueDepth -1

                        let args2c =
                            { args2 with
                                PendingCount = max 0 (args2.PendingCount - 1)
                            }

                        startPipedWait
                            starter
                            (fun () -> props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))
                            "settle-drain"
                            (fun args3 pipe3 ->
                                function
                                | Error error -> raise error
                                | Ok pending ->
                                    let args4 =
                                        { args3 with
                                            PendingCount = if isNull (box pending) then 0 else pending.Count
                                        }

                                    match selectDrainableEntries pending with
                                    | next :: _ ->
                                        withTurnIdSnapshot
                                            (fun turnId args5 pipe5 ->
                                                let running = startTurnNow next turnId
                                                cont (SessionState.Running, Some running) args5 pipe5)
                                            suspendWith
                                            args4
                                            pipe3
                                    | [] ->
                                        startPipedWaitUnit
                                            starter
                                            (fun () ->
                                                props.Store.UpdateSessionState(
                                                    props.Tenant,
                                                    props.SessionId,
                                                    SessionState.Idle,
                                                    cancellationToken
                                                ))
                                            "settle-idle"
                                            (fun args5 pipe5 ->
                                                function
                                                | Ok() -> cont (SessionState.Idle, None) args5 pipe5
                                                | Error error -> raise error)
                                            suspendWith
                                            args4
                                            pipe3)
                            suspendWith
                            args2c
                            pipe2)
                suspendWith
                args
                pipe

        /// Observes a settled turn result through the props hook. Guarded: a
        /// throwing observer never kills the actor.
        /// <param name="result">The settled (or abort-mapped) turn result.</param>
        let notifySettled (result: TurnResult) : unit =
            match props.OnTurnSettled with
            | Some observe ->
                try
                    observe result
                with _ ->
                    ()
            | None -> ()

        /// Maps a reported result to the Aborted result a won stop settles:
        /// the stop cause wins over whatever the turn reported, even a
        /// success, so settlement and stop stay mutually exclusive. Falls
        /// back to the carried result when the cause maps to no settlement
        /// (only abort-family causes reach the pending stop, so this never
        /// fires).
        /// <param name="cause">The stop cause that won.</param>
        /// <param name="reason">Why the turn stopped.</param>
        /// <param name="result">The result the turn reported.</param>
        /// <returns>The result the actor settles.</returns>
        let mapAborted (cause: StopCause) (reason: string) (result: TurnResult) : TurnResult =
            match StopArbitration.settlementFor cause reason with
            | Some(status, outcome) ->
                { result with
                    Status = status
                    Outcome = outcome
                }
            | None -> result

        /// Builds the Aborted result a won stop settles when the turn left
        /// no result behind (a faulted attempt): zero iterations and usage,
        /// the abort-family outcome carrying who and why. Falls back to a
        /// Failed result when the cause maps to no settlement (only
        /// abort-family causes reach the pending stop, so this never fires).
        /// <param name="cause">The stop cause that won.</param>
        /// <param name="reason">Why the turn stopped.</param>
        /// <returns>The result the actor settles.</returns>
        let abortedResult (cause: StopCause) (reason: string) : TurnResult =
            match StopArbitration.settlementFor cause reason with
            | Some(status, outcome) ->
                {
                    AssistantText = ""
                    Status = status
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = outcome
                }
            | None ->
                {
                    AssistantText = ""
                    Status = TurnStatus.Failed
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = TurnFailed(reason) :> TurnOutcome
                }

        /// Appends a prompt entry without starting a turn: the entry waits
        /// for the settle drain (Running), for the Reply resume
        /// (WaitingForInput), or stays durable with nothing new starting
        /// (an out-of-range stored state). The append runs piped.
        /// <param name="payload">What the entry carries: a user message.</param>
        /// <param name="delivery">How the message was delivered.</param>
        /// <param name="cancellationToken">Abandons the append.</param>
        /// <param name="cont">Continues with the appended inbox entry.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withAppend
            (payload: InboxPayload)
            (delivery: DeliveryMode)
            (cancellationToken: CancellationToken)
            (cont: InboxEntry -> BehaviorCont)
            (suspendWith: BehaviorCont)
            (args: BehaviorLoopArgs)
            (pipe: BehaviorPipe)
            : Cont<SessionActorMessage, unit> =
            startPipedWait
                starter
                (fun () ->
                    props.Store.AppendInboxMessage(props.Tenant, props.SessionId, payload, delivery, cancellationToken))
                "append-inbox"
                (fun args2 pipe2 ->
                    function
                    | Error error -> raise error
                    | Ok appended ->
                        Telemetry.addQueueDepth 1

                        cont
                            appended
                            { args2 with
                                PendingCount = args2.PendingCount + 1
                            }
                            pipe2)
                suspendWith
                args
                pipe

        /// Appends a prompt entry while Idle and starts its turn: persists
        /// Running store-first, then drains tier-first (Interrupt first,
        /// then Queue-plus-Inject in position order), so an older entry
        /// orphaned by a restart wins over the just-appended one. The
        /// appended entry is the fallback when nothing else is drainable.
        /// Every wait runs piped.
        /// <param name="payload">What the entry carries: a user message.</param>
        /// <param name="delivery">How the message was delivered.</param>
        /// <param name="cancellationToken">Abandons the append.</param>
        /// <param name="cont">Continues with the appended entry and the in-flight turn.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withStartIdleTurn
            (payload: InboxPayload)
            (delivery: DeliveryMode)
            (cancellationToken: CancellationToken)
            (cont: (InboxEntry * RunningTurn) -> BehaviorCont)
            (suspendWith: BehaviorCont)
            (args: BehaviorLoopArgs)
            (pipe: BehaviorPipe)
            : Cont<SessionActorMessage, unit> =
            withAppend
                payload
                delivery
                cancellationToken
                (fun appended args2 pipe2 ->
                    startPipedWaitUnit
                        starter
                        (fun () ->
                            props.Store.UpdateSessionState(
                                props.Tenant,
                                props.SessionId,
                                SessionState.Running,
                                cancellationToken
                            ))
                        "start-idle-turn/running"
                        (fun args3 pipe3 ->
                            function
                            | Error error -> raise error
                            | Ok() ->
                                startPipedWait
                                    starter
                                    (fun () ->
                                        props.Store.ReadPendingInbox(
                                            props.Tenant,
                                            props.SessionId,
                                            cancellationToken
                                        ))
                                    "start-idle-turn/drain"
                                    (fun args4 pipe4 ->
                                        function
                                        | Error error -> raise error
                                        | Ok pending ->
                                            let args5 =
                                                { args4 with
                                                    PendingCount = if isNull (box pending) then 0 else pending.Count
                                                }

                                            let first =
                                                selectDrainableEntries pending
                                                |> List.tryHead
                                                |> Option.defaultValue appended

                                            withTurnIdSnapshot
                                                (fun turnId args6 pipe6 ->
                                                    let next = startTurnNow first turnId
                                                    cont (appended, next) args6 pipe6)
                                                suspendWith
                                                args5
                                                pipe4)
                                    suspendWith
                                    args3
                                    pipe3)
                        suspendWith
                        args2
                        pipe2)
                suspendWith
                args
                pipe

        /// Writes the durable close and continues with the stored session:
        /// the hub and live hints release only once the write landed.
        /// <param name="cancellationToken">Abandons the close.</param>
        /// <param name="cont">Continues with the closed session.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withCloseWrite
            (cancellationToken: CancellationToken)
            (cont: Session -> BehaviorCont)
            (suspendWith: BehaviorCont)
            (args: BehaviorLoopArgs)
            (pipe: BehaviorPipe)
            : Cont<SessionActorMessage, unit> =
            withCloseWriteSession starter props cancellationToken cont suspendWith args pipe

        /// Starts the recover chain: the stored lifecycle state plus the
        /// pending inbox count, releasing a stored Running back to Idle so
        /// the still-pending entry redelivers on the next drain. A missing
        /// session row starts as an empty Idle shell. Every wait runs
        /// piped; the loop enters Ready only once the chain lands.
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let startRecover
            (suspendWith: BehaviorCont)
            (args: BehaviorLoopArgs)
            (pipe: BehaviorPipe)
            : Cont<SessionActorMessage, unit> =
            let readInbox
                (state: SessionState)
                (args2: BehaviorLoopArgs)
                (pipe2: BehaviorPipe)
                : Cont<SessionActorMessage, unit> =
                startPipedWait
                    starter
                    (fun () -> props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None))
                    "recover-inbox"
                    (fun args3 pipe3 ->
                        function
                        | Ok pending ->
                            suspendWith
                                { args3 with
                                    State = state
                                    Activation = Ready
                                    PendingCount = if isNull (box pending) then 0 else pending.Count
                                }
                                pipe3
                        | Error(:? SessionNotFoundException) ->
                            suspendWith
                                { args3 with
                                    State = state
                                    Activation = Ready
                                    PendingCount = 0
                                }
                                pipe3
                        | Error error -> raise error)
                    suspendWith
                    args2
                    pipe2

            startPipedWait
                starter
                (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                "recover-session"
                (fun args2 pipe2 ->
                    function
                    | Error error -> raise error
                    | Ok found ->
                        match found with
                        | null -> readInbox SessionState.Idle args2 pipe2
                        | session ->
                            match session.State with
                            | SessionState.Running ->
                                startPipedWaitUnit
                                    starter
                                    (fun () ->
                                        props.Store.UpdateSessionState(
                                            props.Tenant,
                                            props.SessionId,
                                            SessionState.Idle,
                                            CancellationToken.None
                                        ))
                                    "recover-release-running"
                                    (fun args3 pipe3 ->
                                        function
                                        | Ok() -> readInbox SessionState.Idle args3 pipe3
                                        | Error error -> raise error)
                                    suspendWith
                                    args2
                                    pipe2
                            | SessionState.Idle -> readInbox SessionState.Idle args2 pipe2
                            | SessionState.WaitingForInput -> readInbox SessionState.WaitingForInput args2 pipe2
                            | SessionState.Closed -> readInbox SessionState.Closed args2 pipe2
                            | unknown -> readInbox unknown args2 pipe2)
                suspendWith
                args
                pipe

        let rec loop (args: BehaviorLoopArgs) (pipe: BehaviorPipe) : Cont<SessionActorMessage, unit> =
            match args.Closing with
            | (_, token) :: _ when not (LifecyclePipe.isBusy pipe) ->
                // A requested close owns the durable write now that the
                // pipe drains: every recorded sender shares the one write
                // and observes the same stored session.
                withCloseWrite
                    token
                    (fun closed args2 pipe2 ->
                        actor {
                            for sender, _ in args2.Closing do
                                sender <! closed

                            return!
                                loop
                                    { args2 with
                                        State = SessionState.Closed
                                        Running = None
                                        Arbitration = StopArbitration.Undecided
                                        PendingStop = None
                                        Closing = []
                                    }
                                    pipe2
                        })
                    suspendWith
                    args
                    pipe
            | _ ->
                match LifecyclePipe.tryTakeDeferred pipe with
                | Some((message, sender), pipe') -> handleMessage message sender args pipe'
                | None ->
                    actor {
                        let! message = mailbox.Receive()
                        return! handleMessage message (mailbox.Sender()) args pipe
                    }

        and handleMessage
            (message: SessionActorMessage)
            (sender: IActorRef)
            (args: BehaviorLoopArgs)
            (pipe: BehaviorPipe)
            : Cont<SessionActorMessage, unit> =
            actor {
                match message with
                | LifecycleStoreCompleted(opId, incarnation, outcome) ->
                    match LifecyclePipe.tryComplete pipe opId incarnation with
                    | Some(outstanding, pipe') -> return! outstanding.Resume args pipe' outcome
                    | None -> return! loop args pipe
                | LifecycleStoreTimeout(opId, incarnation) ->
                    match LifecyclePipe.tryComplete pipe opId incarnation with
                    | Some(outstanding, pipe') -> return! outstanding.Resume args pipe' outstanding.TimeoutOutcome
                    | None -> return! loop args pipe
                | _ when args.Activation = Recovering ->
                    // The recover chain is in flight: everything waits
                    // bounded behind it in arrival order.
                    match LifecyclePipe.defer pipe message sender with
                    | pipe', true -> return! loop args pipe'
                    | _, false ->
                        replyPipeOverflow sender
                        return! loop args pipe
                | AbortSession(cause, reason, _) ->
                    match args.Closing with
                    | _ :: _ ->
                        // A requested close owns the turn's cancellation
                        // already: the abort no-ops returning the current
                        // snapshot, like post-close.
                        sender <! takeSnapshot args
                        return! loop args pipe
                    | [] ->
                        match args.State, args.Running with
                        | SessionState.Running, Some inFlight when
                            cause = StopCause.ExplicitAbort || cause = StopCause.HostShutdown
                            ->
                            let nextArbitration, won = StopArbitration.applyStop args.Arbitration cause

                            let nextStop = if won then Some(cause, reason) else args.PendingStop

                            if won then
                                inFlight.Cts.Cancel()

                            let args2 =
                                { args with
                                    Arbitration = nextArbitration
                                    PendingStop = nextStop
                                }

                            sender <! takeSnapshot args2
                            return! loop args2 pipe
                        | _ ->
                            // Idle, WaitingForInput (suspended turns belong to
                            // issue 36: nothing runs to abort), Closed, unknown
                            // states, and non-abort-family causes: a no-op
                            // returning the current state.
                            sender <! takeSnapshot args
                            return! loop args pipe
                | CloseSession cancellationToken ->
                    match args.Running with
                    | Some inFlight -> inFlight.Cts.Cancel()
                    | None -> ()

                    // The turn cancellation applies now; the durable write
                    // lands through the loop entry once the pipe drains, so
                    // shutdown stays responsive behind a delayed dependency.
                    return!
                        loop
                            { args with
                                Closing = args.Closing @ [ sender, cancellationToken ]
                            }
                            pipe
                | GetSnapshot ->
                    sender <! takeSnapshot args
                    return! loop args pipe
                | _ when args.Closing <> [] ->
                    // A requested close behaves Closed for new lifecycle
                    // work: it waits bounded behind the close write and is
                    // then answered as Closed, in order.
                    match LifecyclePipe.defer pipe message sender with
                    | pipe', true -> return! loop args pipe'
                    | _, false ->
                        replyPipeOverflow sender
                        return! loop args pipe
                | _ when LifecyclePipe.isBusy pipe ->
                    // A store wait is outstanding: Abort, Close, and
                    // GetSnapshot answered from memory above; everything
                    // else waits its turn behind the wait.
                    match LifecyclePipe.defer pipe message sender with
                    | pipe', true -> return! loop args pipe'
                    | _, false ->
                        replyPipeOverflow sender
                        return! loop args pipe
                | QueuePrompt(payload, cancellationToken) ->
                    match args.State with
                    | SessionState.Closed ->
                        logScoped null "The session rejected a prompt: the session is closed."
                        sender <! PromptRejected SessionState.Closed
                        return! loop args pipe
                    | SessionState.Idle ->
                        return!
                            withStartIdleTurn
                                payload
                                DeliveryMode.Queue
                                cancellationToken
                                (fun (appended, next) args2 pipe2 ->
                                    actor {
                                        sender <! PromptAccepted appended
                                        logScoped null "The session accepted a prompt and started a turn."

                                        return!
                                            loop
                                                { args2 with
                                                    State = SessionState.Running
                                                    Running = Some next
                                                    Arbitration = StopArbitration.Undecided
                                                    PendingStop = None
                                                }
                                                pipe2
                                    })
                                suspendWith
                                args
                                pipe
                    | SessionState.Running
                    | SessionState.WaitingForInput ->
                        return!
                            withAppend
                                payload
                                DeliveryMode.Queue
                                cancellationToken
                                (fun appended args2 pipe2 ->
                                    actor {
                                        sender <! PromptAccepted appended
                                        logScoped null "The session accepted a prompt while busy."
                                        return! loop args2 pipe2
                                    })
                                suspendWith
                                args
                                pipe
                    | _ ->
                        // Out-of-range stored state: stay durable but start
                        // nothing new.
                        return!
                            withAppend
                                payload
                                DeliveryMode.Queue
                                cancellationToken
                                (fun appended args2 pipe2 ->
                                    actor {
                                        sender <! PromptAccepted appended
                                        logScoped null "The session accepted a prompt while out of range."
                                        return! loop args2 pipe2
                                    })
                                suspendWith
                                args
                                pipe
                | InjectPrompt(payload, cancellationToken) ->
                    match args.State with
                    | SessionState.Closed ->
                        logScoped null "The session rejected an injected prompt: the session is closed."
                        sender <! PromptRejected SessionState.Closed
                        return! loop args pipe
                    | SessionState.Idle ->
                        return!
                            withStartIdleTurn
                                payload
                                DeliveryMode.Inject
                                cancellationToken
                                (fun (appended, next) args2 pipe2 ->
                                    actor {
                                        sender <! PromptAccepted appended
                                        logScoped null "The session accepted an injected prompt and started a turn."

                                        return!
                                            loop
                                                { args2 with
                                                    State = SessionState.Running
                                                    Running = Some next
                                                    Arbitration = StopArbitration.Undecided
                                                    PendingStop = None
                                                }
                                                pipe2
                                    })
                                suspendWith
                                args
                                pipe
                    | SessionState.Running
                    | SessionState.WaitingForInput ->
                        // Append-and-wait: the running turn folds the entry
                        // at its next iteration boundary, and a suspended
                        // turn leaves it for the settle drain. Never aborts.
                        return!
                            withAppend
                                payload
                                DeliveryMode.Inject
                                cancellationToken
                                (fun appended args2 pipe2 ->
                                    actor {
                                        sender <! PromptAccepted appended
                                        logScoped null "The session accepted an injected prompt while busy."
                                        return! loop args2 pipe2
                                    })
                                suspendWith
                                args
                                pipe
                    | _ ->
                        return!
                            withAppend
                                payload
                                DeliveryMode.Inject
                                cancellationToken
                                (fun appended args2 pipe2 ->
                                    actor {
                                        sender <! PromptAccepted appended
                                        logScoped null "The session accepted an injected prompt while out of range."
                                        return! loop args2 pipe2
                                    })
                                suspendWith
                                args
                                pipe
                | InterruptPrompt(payload, cancellationToken) ->
                    match args.State with
                    | SessionState.Closed ->
                        logScoped null "The session rejected an interrupt prompt: the session is closed."
                        sender <! PromptRejected SessionState.Closed
                        return! loop args pipe
                    | SessionState.Idle ->
                        return!
                            withStartIdleTurn
                                payload
                                DeliveryMode.Interrupt
                                cancellationToken
                                (fun (appended, next) args2 pipe2 ->
                                    actor {
                                        sender <! PromptAccepted appended
                                        logScoped null "The session accepted an interrupt prompt and started a turn."

                                        return!
                                            loop
                                                { args2 with
                                                    State = SessionState.Running
                                                    Running = Some next
                                                    Arbitration = StopArbitration.Undecided
                                                    PendingStop = None
                                                }
                                                pipe2
                                    })
                                suspendWith
                                args
                                pipe
                    | SessionState.Running ->
                        // Pre-empt through the abort verb: the entry joins
                        // the inbox first so the settle drain finds it, then
                        // the running turn aborts under ExplicitAbort through
                        // the same arbitration cell AbortSession uses. The
                        // settle consumes the aborted entry and drains the
                        // Interrupt entry first with the rest of the inbox
                        // intact. A stop that already won keeps the first
                        // cause; the new entry still drains after the
                        // settle.
                        return!
                            withAppend
                                payload
                                DeliveryMode.Interrupt
                                cancellationToken
                                (fun appended args2 pipe2 ->
                                    actor {
                                        match args2.Running with
                                        | Some inFlight ->
                                            let nextArbitration, won =
                                                StopArbitration.applyStop args2.Arbitration StopCause.ExplicitAbort

                                            let nextStop =
                                                if won then
                                                    Some(StopCause.ExplicitAbort, InterruptReason)
                                                else
                                                    args2.PendingStop

                                            if won then
                                                inFlight.Cts.Cancel()

                                            sender <! PromptAccepted appended

                                            logScoped
                                                null
                                                "The session accepted an interrupt prompt and pre-empted the running turn."

                                            return!
                                                loop
                                                    { args2 with
                                                        Arbitration = nextArbitration
                                                        PendingStop = nextStop
                                                    }
                                                    pipe2
                                        | None ->
                                            sender <! PromptAccepted appended

                                            logScoped
                                                null
                                                "The session accepted an interrupt prompt with no turn in flight."

                                            return! loop args2 pipe2
                                    })
                                suspendWith
                                args
                                pipe
                    | SessionState.WaitingForInput ->
                        // Append-and-wait: suspended turns belong to issue
                        // 36, so nothing runs to abort and Reply still
                        // resumes the suspended turn.
                        return!
                            withAppend
                                payload
                                DeliveryMode.Interrupt
                                cancellationToken
                                (fun appended args2 pipe2 ->
                                    actor {
                                        sender <! PromptAccepted appended
                                        logScoped null "The session accepted an interrupt prompt while suspended."
                                        return! loop args2 pipe2
                                    })
                                suspendWith
                                args
                                pipe
                    | _ ->
                        return!
                            withAppend
                                payload
                                DeliveryMode.Interrupt
                                cancellationToken
                                (fun appended args2 pipe2 ->
                                    actor {
                                        sender <! PromptAccepted appended
                                        return! loop args2 pipe2
                                    })
                                suspendWith
                                args
                                pipe
                | ObserveHostAbort(tenant, sessionId, targetTurnId) ->
                    match args.State, args.Running, props.Store with
                    | SessionState.Running, Some inFlight, (:? ISessionAbortControlStore as control) when
                        tenant = props.Tenant
                        && sessionId = props.SessionId
                        && inFlight.TurnId = targetTurnId
                        ->
                        return!
                            startPipedWait
                                starter
                                (fun () -> control.ReadAbortTarget(tenant, sessionId, CancellationToken.None))
                                "observe-host-abort"
                                (fun args2 pipe2 ->
                                    function
                                    | Error error -> raise error
                                    | Ok target ->
                                        match target with
                                        | null -> loop args2 pipe2
                                        | t when t.TurnId = targetTurnId ->
                                            match t.Stop with
                                            | null -> loop args2 pipe2
                                            | stop ->
                                                let next, won =
                                                    StopArbitration.applyStop args2.Arbitration stop.Cause.Value

                                                if won then
                                                    inFlight.Cts.Cancel()

                                                let selected =
                                                    if won then
                                                        Some(
                                                            stop.Cause.Value,
                                                            stop.Reason |> Option.ofObj |> Option.defaultValue ""
                                                        )
                                                    else
                                                        args2.PendingStop

                                                loop
                                                    { args2 with
                                                        Arbitration = next
                                                        PendingStop = selected
                                                    }
                                                    pipe2
                                        | _ -> loop args2 pipe2)
                                suspendWith
                                args
                                pipe
                    | _ -> return! loop args pipe
                | CompactSession cancellationToken ->
                    match args.State with
                    | SessionState.Closed ->
                        sender <! CompactRejected SessionState.Closed
                        return! loop args pipe
                    | SessionState.Idle ->
                        match props.Compact with
                        | None ->
                            sender <! CompactNotNeeded
                            return! loop args pipe
                        | Some compact ->
                            return!
                                compactIdleNowPiped
                                    starter
                                    props
                                    compact
                                    cancellationToken
                                    (fun reply args2 pipe2 ->
                                        actor {
                                            sender <! reply
                                            return! loop args2 pipe2
                                        })
                                    suspendWith
                                    args
                                    pipe
                    | SessionState.Running ->
                        match props.Compact with
                        | Some compact when not (isNull (box compact.Force)) ->
                            compact.Force.Request()
                            sender <! CompactDeferred
                            return! loop args pipe
                        | _ ->
                            // Unconfigured: no boundary hook shares the
                            // one-shot cell, so nothing can fire later.
                            sender <! CompactNotNeeded
                            return! loop args pipe
                    | SessionState.WaitingForInput ->
                        // Suspended turns belong to issue 36: their history
                        // is parked, so an on-demand compact no-ops.
                        sender <! CompactNotNeeded
                        return! loop args pipe
                    | _ ->
                        // Out-of-range stored state: stay durable but
                        // compact nothing.
                        sender <! CompactNotNeeded
                        return! loop args pipe
                | SessionTurnSettled(entry, result) ->
                    match args.State, args.Running with
                    | SessionState.Running, Some inFlight when inFlight.Entry.Position = entry.Position ->
                        match args.Arbitration with
                        | StopArbitration.Undecided ->
                            // Settlement wins: the carried result stands.
                            inFlight.Cts.Dispose()
                            notifySettled result
                            logScoped null "The session settled a turn."

                            return!
                                withDispatchCompletion
                                    starter
                                    props
                                    result
                                    (fun _ args2 pipe2 ->
                                        actor {
                                            if result.Status = TurnStatus.Completed then
                                                return!
                                                    withAutoCloseEnabled
                                                        starter
                                                        props
                                                        (fun enabled args3 pipe3 ->
                                                            actor {
                                                                if enabled then
                                                                    // AutoClose (issue 82): the first Completed
                                                                    // turn closes the session store-first instead
                                                                    // of draining. Aborted and Failed results
                                                                    // never take this path, so failed runs stay
                                                                    // open for inspection.
                                                                    return!
                                                                        withConsumeAndClose
                                                                            starter
                                                                            props
                                                                            entry
                                                                            (fun () args4 pipe4 ->
                                                                                actor {
                                                                                    return!
                                                                                        loop
                                                                                            { args4 with
                                                                                                State =
                                                                                                    SessionState.Closed
                                                                                                Running = None
                                                                                                Arbitration =
                                                                                                    StopArbitration.Undecided
                                                                                                PendingStop = None
                                                                                                PendingCount =
                                                                                                    max
                                                                                                        0
                                                                                                        (args4.PendingCount
                                                                                                         - 1)
                                                                                            }
                                                                                            pipe4
                                                                                })
                                                                            suspendWith
                                                                            args3
                                                                            pipe3
                                                                else
                                                                    return!
                                                                        withSettle
                                                                            entry
                                                                            CancellationToken.None
                                                                            (fun (nextState, nextRunning) args4 pipe4 ->
                                                                                actor {
                                                                                    return!
                                                                                        loop
                                                                                            { args4 with
                                                                                                State = nextState
                                                                                                Running = nextRunning
                                                                                                Arbitration =
                                                                                                    StopArbitration.Undecided
                                                                                                PendingStop = None
                                                                                            }
                                                                                            pipe4
                                                                                })
                                                                            suspendWith
                                                                            args3
                                                                            pipe3
                                                            })
                                                        suspendWith
                                                        args2
                                                        pipe2
                                            else
                                                return!
                                                    withSettle
                                                        entry
                                                        CancellationToken.None
                                                        (fun (nextState, nextRunning) args3 pipe3 ->
                                                            actor {
                                                                return!
                                                                    loop
                                                                        { args3 with
                                                                            State = nextState
                                                                            Running = nextRunning
                                                                            Arbitration = StopArbitration.Undecided
                                                                            PendingStop = None
                                                                        }
                                                                        pipe3
                                                            })
                                                        suspendWith
                                                        args2
                                                        pipe2
                                        })
                                    suspendWith
                                    args
                                    pipe
                        | StopArbitration.Decided(StopArbitration.StopWins cause) ->
                            // The stop landed first, so it wins even over a
                            // success: map to Aborted under the winning
                            // cause, then run the settle bookkeeping once.
                            inFlight.Cts.Dispose()

                            let reason = args.PendingStop |> Option.map snd |> Option.defaultValue ""

                            let settled = mapAborted cause reason result
                            notifySettled settled
                            logScoped null "The session settled a turn under a stop cause."

                            return!
                                withDispatchCompletion
                                    starter
                                    props
                                    settled
                                    (fun _ args2 pipe2 ->
                                        actor {
                                            return!
                                                withSettle
                                                    entry
                                                    CancellationToken.None
                                                    (fun (nextState, nextRunning) args3 pipe3 ->
                                                        actor {
                                                            return!
                                                                loop
                                                                    { args3 with
                                                                        State = nextState
                                                                        Running = nextRunning
                                                                        Arbitration = StopArbitration.Undecided
                                                                        PendingStop = None
                                                                    }
                                                                    pipe3
                                                        })
                                                    suspendWith
                                                    args2
                                                    pipe2
                                        })
                                    suspendWith
                                    args
                                    pipe
                        | StopArbitration.Decided StopArbitration.SettlementWins ->
                            // Stale: the turn already settled, so this
                            // completion produces zero effects.
                            return! loop args pipe
                    | _ -> return! loop args pipe
                | SessionTurnFaulted(entry, _) ->
                    match args.State, args.Running with
                    | SessionState.Running, Some inFlight when inFlight.Entry.Position = entry.Position ->
                        match args.Arbitration with
                        | StopArbitration.Undecided ->
                            inFlight.Cts.Dispose()
                            logScoped null "The session turn faulted and its entry was consumed."

                            return!
                                withSettle
                                    entry
                                    CancellationToken.None
                                    (fun (nextState, nextRunning) args2 pipe2 ->
                                        actor {
                                            return!
                                                loop
                                                    { args2 with
                                                        State = nextState
                                                        Running = nextRunning
                                                        Arbitration = StopArbitration.Undecided
                                                        PendingStop = None
                                                    }
                                                    pipe2
                                        })
                                    suspendWith
                                    args
                                    pipe
                        | StopArbitration.Decided(StopArbitration.StopWins cause) ->
                            // The stop arrived first, so it wins even over
                            // a real fault: settle Aborted under the cause.
                            inFlight.Cts.Dispose()

                            let reason = args.PendingStop |> Option.map snd |> Option.defaultValue ""

                            notifySettled (abortedResult cause reason)
                            logScoped null "The session turn faulted under a stop cause."

                            return!
                                withSettle
                                    entry
                                    CancellationToken.None
                                    (fun (nextState, nextRunning) args2 pipe2 ->
                                        actor {
                                            return!
                                                loop
                                                    { args2 with
                                                        State = nextState
                                                        Running = nextRunning
                                                        Arbitration = StopArbitration.Undecided
                                                        PendingStop = None
                                                    }
                                                    pipe2
                                        })
                                    suspendWith
                                    args
                                    pipe
                        | StopArbitration.Decided StopArbitration.SettlementWins ->
                            // Stale: the turn already settled, so this fault
                            // produces zero effects.
                            return! loop args pipe
                    | _ -> return! loop args pipe
            }

        and suspendWith (args: BehaviorLoopArgs) (pipe: BehaviorPipe) : Cont<SessionActorMessage, unit> = loop args pipe

        let initialArgs =
            {
                State = SessionState.Idle
                Running = None
                Arbitration = StopArbitration.Undecided
                PendingStop = None
                Activation = Recovering
                PendingCount = 0
                Closing = []
            }

        startRecover suspendWith initialArgs (LifecyclePipe.empty ())

    /// Builds the child-spawn factory the session router uses: parses the
    /// router's string id into a SessionId and spawns the session actor,
    /// falling back to the legacy identity-only child for ids that do not
    /// parse (the router historically accepted any non-blank string).
    /// <param name="store">The durable store session actors persist through.</param>
    /// <param name="tenant">The tenant router-spawned sessions belong to.</param>
    /// <param name="runTurn">The turn runner session actors drive per Queue entry.</param>
    /// <returns>A factory mapping a session id string to a child spawn.</returns>
    let spawnFactory
        (store: ISessionStore)
        (tenant: TenantId)
        (runTurn: InboxEntry -> CancellationToken -> Task<TurnResult>)
        : (string -> IActorContext -> string -> IActorRef) =
        ArgumentNullException.ThrowIfNull(store)

        if isNull (box runTurn) then
            raise (ArgumentNullException(nameof runTurn))

        fun sessionId context name ->
            let mutable parsed = Unchecked.defaultof<SessionId>

            if SessionId.TryParse(sessionId, &parsed) then
                let captured = parsed

                let props =
                    {
                        Store = store
                        Settlement = None
                        Tenant = tenant
                        SessionId = captured
                        RunTurn = runTurn
                        OnTurnSettled = Some(fun result -> PromptWaitHubs.ObserveSettledScoped tenant captured result)
                        OnInjectJournaled = None
                        Compact = None
                        Logger = null
                        StorePipe = None
                    }

                spawn context name (behavior props)
            else
                spawn context name (actorOf (fun (_: obj) -> ()))

    /// Asks the actor with the shared timeout, honouring the caller's
    /// cancellation. The timeout bounds the wait while the token honours
    /// the caller.
    /// <param name="session">The session actor.</param>
    /// <param name="message">The message to ask with.</param>
    /// <param name="cancellationToken">Cancels the ask.</param>
    /// <returns>The actor's reply.</returns>
    let private askAsync<'Reply>
        (session: IActorRef)
        (message: SessionActorMessage)
        (cancellationToken: CancellationToken)
        : Task<'Reply> =
        task {
            use timeoutCts = new CancellationTokenSource(askTimeout)

            use linkedCts =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)

            let! reply = session.Ask<'Reply>(message, linkedCts.Token)
            return reply
        }

    /// Reads the session row or throws the boundary precondition failure.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to require.</param>
    /// <param name="cancellationToken">Abandons the read.</param>
    /// <returns>The stored session.</returns>
    let private requireSessionAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (cancellationToken: CancellationToken)
        : Task<Session> =
        task {
            let! found = store.GetSession(tenant, sessionId, cancellationToken)

            match found with
            | null -> return raise (SessionNotFoundException(sessionId, "The session does not exist."))
            | session -> return session
        }

    /// Prompts a session with a delivery mode: the client boundary. Validates
    /// the session is present and not Closed before touching the actor, so
    /// invalid transitions throw here, never inside the actor. A rejection
    /// that still races through (Closed between the check and the actor)
    /// maps to the same exception. Queue appends and acts on the message
    /// once the running turn (if any) finishes; Inject appends and folds
    /// into the running turn at its next iteration boundary without
    /// interrupting it; Interrupt appends and pre-empts the running turn,
    /// settling it as Aborted under ExplicitAbort before starting the new
    /// turn. While WaitingForInput every mode appends and waits (Reply
    /// still resumes the suspended turn), and while Idle every mode starts
    /// a turn normally through the tiered drain.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="delivery">How the message is delivered to a running turn.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let promptAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (delivery: DeliveryMode)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box message) then
            raise (ArgumentNullException(nameof message))

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no further prompts."
                    )
                )

            let payload = UserMessagePayload(message) :> InboxPayload

            let prompt =
                match delivery with
                | DeliveryMode.Queue -> QueuePrompt(payload, cancellationToken)
                | DeliveryMode.Inject -> InjectPrompt(payload, cancellationToken)
                | DeliveryMode.Interrupt -> InterruptPrompt(payload, cancellationToken)
                | unknown ->
                    raise (
                        ArgumentOutOfRangeException(
                            nameof delivery,
                            sprintf "Unknown delivery mode: %O. Expected Queue, Inject, or Interrupt." unknown
                        )
                    )

            let! reply = askAsync<SessionPromptReply> session prompt cancellationToken

            match reply with
            | PromptAccepted entry -> return entry
            | PromptRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the prompt was accepted."
                        )
                    )
        }

    /// Queues a user message on a session: the client boundary. Validates
    /// the session is present and not Closed before touching the actor, so
    /// invalid transitions throw here, never inside the actor. A rejection
    /// that still races through (Closed between the check and the actor)
    /// maps to the same exception.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let promptQueueAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        promptAsync store tenant sessionId session message DeliveryMode.Queue cancellationToken

    /// Closes a session: the client boundary. Valid in every state and
    /// idempotent; on a Running session the actor aborts the turn first
    /// through cancellation only. Unknown sessions throw
    /// SessionNotFoundException before touching the actor.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to close.</param>
    /// <param name="session">The session actor.</param>
    /// <param name="cancellationToken">Cancels the close.</param>
    /// <returns>The stored session after the close.</returns>
    let closeAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cancellationToken: CancellationToken)
        : Task<Session> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! _ = requireSessionAsync store tenant sessionId cancellationToken
            let! closed = askAsync<Session> session (CloseSession cancellationToken) cancellationToken
            return closed
        }

    /// Reads the session actor's snapshot: the client boundary read.
    /// <param name="session">The session actor.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The actor's current snapshot.</returns>
    let getSnapshotAsync (session: IActorRef) (cancellationToken: CancellationToken) : Task<SessionSnapshot> =
        ArgumentNullException.ThrowIfNull(session)
        askAsync<SessionSnapshot> session GetSnapshot cancellationToken

    /// Aborts the turn running in a session: the client boundary turn-level
    /// verb. Idle is a no-op returning the current snapshot, as is
    /// WaitingForInput (suspended turns belong to issue 36: nothing runs to
    /// abort). Running records the pending stop under the typed cause,
    /// cancels the turn, and settles Aborted under the winning cause when
    /// the turn reports back; settlement and stop stay mutually exclusive
    /// and a second abort keeps the first cause. Close stays lifecycle-only
    /// and never records a stop cause. Unknown sessions throw
    /// SessionNotFoundException and Closed sessions throw
    /// InvalidSessionStateException before touching the actor; a Close
    /// racing the abort maps to the same exception.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to abort the turn in.</param>
    /// <param name="session">The session actor.</param>
    /// <param name="cause">Which abort-family stop cause wins: ExplicitAbort or HostShutdown.</param>
    /// <param name="reason">Why the turn stops. Must not be null. Never contains secrets or tool arguments.</param>
    /// <param name="cancellationToken">Cancels the abort.</param>
    /// <returns>The actor's snapshot after the abort was accepted.</returns>
    let abortAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cause: StopCause)
        (reason: string)
        (cancellationToken: CancellationToken)
        : Task<SessionSnapshot> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box reason) then
            raise (ArgumentNullException(nameof reason))

        if cause <> StopCause.ExplicitAbort && cause <> StopCause.HostShutdown then
            raise (
                ArgumentOutOfRangeException(
                    nameof cause,
                    "Only ExplicitAbort and HostShutdown abort a turn: the deadline arrives through the turn loop and lease loss through the claim fence."
                )
            )

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no abort."
                    )
                )

            let! snapshot =
                askAsync<SessionSnapshot> session (AbortSession(cause, reason, cancellationToken)) cancellationToken

            if snapshot.State = SessionState.Closed then
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            snapshot.State.ToString(),
                            "The session closed before the abort was accepted."
                        )
                    )
            else
                return snapshot
        }

    /// Compacts a session on demand: the client boundary. Idle replays the
    /// journal and compacts now without starting a turn, answering the
    /// before/after estimates; Running arms the one-shot force flag the
    /// turn's force-aware hook honors at the next boundary;
    /// WaitingForInput no-ops (suspended turns belong to issue 36).
    /// Unknown sessions throw SessionNotFoundException and Closed sessions
    /// throw InvalidSessionStateException before touching the actor; a
    /// Close racing the compact maps to the same exception. The future
    /// public ILegateClient wrapper stays a follow-up: this boundary is
    /// internal.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to compact.</param>
    /// <param name="session">The session actor.</param>
    /// <param name="cancellationToken">Cancels the compact.</param>
    /// <returns>The actor's compact reply.</returns>
    let compactAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cancellationToken: CancellationToken)
        : Task<SessionCompactReply> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no compact."
                    )
                )

            let! reply = askAsync<SessionCompactReply> session (CompactSession cancellationToken) cancellationToken

            match reply with
            | CompactRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the compact was accepted."
                        )
                    )
            | _ -> return reply
        }

    // ────────────────── Suspend and resume (issue 36) ──────────────────

    /// How a suspendable turn runs: the first run carries no cursor and no
    /// reply, a resume carries both. Attempt is 1-based and incremented on
    /// every resume, so a resumed run continues the same turn id. The
    /// session memory of AllowForSession decisions travels with the turn.
    /// The crash seed carries the in-memory rehydrated history (with the
    /// resumption note) on crash-resume activation and is None elsewhere:
    /// a seeded run leads its runner history input with the seed instead
    /// of the entry-derived message. The in-call marker hook (issue 284)
    /// rides fourth to last: Some journals the fenced TurnStartedEvent at the first
    /// provider-call entry, None journals nothing (resumes already marked
    /// before they suspended). The usage-checkpoint hook (issue 321) rides
    /// third to last and the skill-load hook second to last: Some journals
    /// the fenced UsageEvent at iteration boundaries plus settle and the
    /// fenced SkillLoadedEvent per successful load, None journals nothing.
    /// Resumes carry usage and skill hooks (post-resume work checkpoints)
    /// while OnTurnStarted stays None there. The turn id rides last (issue
    /// 289): the actor-supplied loop-run id the marker, the completion, and
    /// the settle choke points all key on. Tests inject scripted runners; the
    /// TurnLoop-backed runner wires
    /// TurnLoop.runSuspendableAsync plus the resume continuations.
    type SuspendableRunner =
        InboxEntry
            -> int
            -> HashSet<string>
            -> TurnLoop.TurnLoopSuspension option
            -> Reply option
            -> IList<ChatMessage> option
            -> CancellationToken
            -> TurnLoop.TurnStartedHook option
            -> TurnLoop.UsageCheckpointHook option
            -> TurnLoop.SkillLoadedHook option
            -> TurnId
            -> Task<TurnLoop.TurnLoopCompletion>

    /// What a suspendable session actor is built from: the base actor
    /// dependencies plus the journal, the AskTimeout seam, the journal
    /// token, and the suspendable runner. The journal token fences journal
    /// appends; tests prime it with ISessionStore.ClaimNextTurn so the
    /// in-memory journal lands, and a takeover re-claims so the loser
    /// appends nothing. The heartbeat is not cancelled on suspend: renewal
    /// continues while suspended under the same claim, bounded by lease
    /// expiry and AskTimeout. The behavior copies the token into a mutable
    /// cell a SetAgent swap replaces, so every journal write below reads
    /// the cell, never this initial value, after the first swap.
    type SuspendDeps =
        {
            /// The journal suspend and resolve events append to.
            EventStore: ISessionEventStore
            /// The seam the AskTimeout deadline fires off. Never the clock.
            Delay: ILlmDelay
            /// How long a suspension waits for its Reply before settling Failed.
            AskTimeout: TimeSpan
            /// The claim token fencing journal appends.
            JournalToken: string
            /// Genuine captured journal-prime authority. None only for internal unclaimed test shells.
            PrimeClaim: TurnClaim option
            /// Provider-fenced original entry for unstopped current-format recovery; null on fresh activation.
            Recovery: ControlTargetRecovery | null
            /// Runs one suspendable attempt. Never null.
            RunSuspendable: SuspendableRunner
            /// Re-primes the journal after the actor settles its primed
            /// claim: appends a bootstrap entry and claims it, returning the
            /// live claim, or None when no turn is claimable (a live claim
            /// is held) or the prime failed. The task starts without
            /// blocking the caller (issue 390): the loop pipes the wait
            /// instead of awaiting it. The SetAgent swap calls it after
            /// settling the old prime and again to restore the live prime;
            /// a quiescent boundary that finds a recorded rebind retries
            /// through it until it succeeds. None when the host never
            /// re-primes (direct test constructions): a recorded rebind
            /// then stays pending.
            ReprimeJournal: (unit -> Task<TurnClaim option>) option
            /// Rebuilds the on-demand compaction wiring for a fresh journal
            /// token after a SetAgent swap, or None when the host drives
            /// Compact directly. The factory supplies the spawn-time
            /// compactFor; without one the swap keeps the wiring and only
            /// refreshes its token, so later Compacts stay live either way.
            RefreshCompact: (string -> CompactDeps option) option
            /// The agent catalog the per-turn authority gate reads, or null
            /// when the host runs without one: the gate is skipped then (the
            /// SetAgent facade validation precedent). Never drives tool
            /// resolution.
            AgentStore: IAgentStore | null
            /// Reads the completion era (issue 289): true once Open or Fork
            /// marked the session, false for pre-era sessions. The
            /// entity-start predicate consults it exactly like the journal.
            /// Never null.
            EraMarked: CompletionEra.CompletionEraReader
        }

    /// A rebuilt pending request from the journal: the crash path carries
    /// no in-memory cursor (history, tool call), so the matching Reply
    /// retries the turn from its inbox entry with attempt plus 1 instead of
    /// resuming from the cursor. The journal is the normative resume source.
    type RebuiltPending =
        {
            /// The pending request or question id the Reply must carry.
            RequestId: string
            /// The tool whose call raised the request.
            ToolName: string
            /// Which reply resumes the turn.
            Kind: TurnLoop.SuspensionKind
            /// The question text, or empty for permission suspensions.
            QuestionText: string
        }

    /// A suspended turn: the inbox entry it parked on, either the live
    /// cursor (in-memory resume) or the rebuilt pending (crash retry), the
    /// session AllowForSession memory, the attempt the parked run used, and
    /// the source the AskTimeout delay pipes through. The source is
    /// cancelled on Reply and never disposed on the timeout path.
    type private SuspendedTurn =
        {
            /// The inbox entry the parked turn executes.
            Entry: InboxEntry
            /// The turn the parked run executes (issue 289): captured at
            /// suspend from the completion, or the live-turn snapshot for a
            /// crash rebuild. The AskTimeout journal carries it.
            TurnId: TurnId
            /// The live cursor, or None after a crash rebuild.
            Cursor: TurnLoop.TurnLoopSuspension option
            /// The rebuilt pending, or None for a live suspension.
            Rebuilt: RebuiltPending option
            /// Tool names the host already allowed for the session.
            Allowed: HashSet<string>
            /// The 1-based attempt the parked run used.
            Attempt: int
            /// The source the AskTimeout delay cancels through.
            TimeoutCts: CancellationTokenSource
        }

    /// How the suspendable actor answers a Reply. The actor never throws
    /// ReplyMismatchException: an unknown or already-resolved request id is
    /// rejected with the exception and the client boundary throws it.
    type internal SessionReplyReply =

        /// The reply matched the pending request: carries the appended Reply
        /// inbox entry. The turn resumes under attempt plus 1.
        | ReplyAccepted of entry: InboxEntry

        /// The reply answered nothing pending: unknown or already-resolved.
        /// Carries the typed error the boundary throws.
        | ReplyRejected of error: ReplyMismatchException

    /// How the suspendable actor answers a SetAgent. The actor never throws
    /// InvalidSessionStateException: a Closed-state rebind is rejected with
    /// the current state and the client boundary maps it to the exception.
    /// Agent existence and enablement are enforced at the facade boundary
    /// (which holds the agent store); the actor applies the rebound id
    /// verbatim, at once when quiescent and at the next quiescent boundary
    /// otherwise.
    type internal SessionSetAgentReply =

        /// The rebind applied at once: the session converses with the new
        /// agent from the next turn. Carries the stored session after the
        /// rebind.
        | SetAgentApplied of session: Session

        /// The rebind was recorded and applies at the next quiescent
        /// boundary: a turn is running or entries are queued. Carries the
        /// stored session as it stands, still conversing with the previous
        /// agent.
        | SetAgentPending of session: Session

        /// The rebind arrived while the session was Closed; nothing was
        /// recorded. Carries the state the session was in.
        | SetAgentRejected of state: SessionState

    /// The suspendable session actor protocol extension. QueuePrompt,
    /// CloseSession, AbortSession, and GetSnapshot keep their base meaning;
    /// the completions below carry suspendable outcomes back to the actor.
    type internal SuspendableActorMessage =

        /// Queue a user message on the suspendable actor: appends to the
        /// durable inbox, starts a suspendable turn when Idle, waits when
        /// Running or WaitingForInput. Answered with SessionPromptReply.
        | SuspendableQueuePrompt of payload: InboxPayload * cancellationToken: CancellationToken

        /// Inject a user message into the running suspendable turn: appends
        /// to the durable inbox with Inject delivery, starts a suspendable
        /// turn when Idle, folds at the next iteration boundary when Running
        /// through the runner's drain hooks, waits when WaitingForInput,
        /// rejects when Closed. Never aborts the running turn. Answered with
        /// <see cref="T:Legate.SessionPromptReply" />.
        | SuspendableInjectPrompt of payload: InboxPayload * cancellationToken: CancellationToken

        /// Interrupt the running suspendable turn with a user message:
        /// appends to the durable inbox with Interrupt delivery, starts a
        /// suspendable turn when Idle, records the ExplicitAbort pending stop
        /// and drains the Interrupt entry first when Running, waits when
        /// WaitingForInput, rejects when Closed. The detached suspendable
        /// turn runs un-cancellable, so a recorded stop wins at its next
        /// report. Answered with <see cref="T:Legate.SessionPromptReply" />.
        | SuspendableInterruptPrompt of payload: InboxPayload * cancellationToken: CancellationToken

        /// The suspendable turn finished: settled (Suspension None) or
        /// suspended (Suspension Some). One-way from the turn task.
        | SuspendableFinished of
            entry: InboxEntry *
            completion: TurnLoop.TurnLoopCompletion *
            attempt: int *
            allowed: HashSet<string>

        /// The suspendable turn faulted. One-way from the turn task. The
        /// turn id rides last (issue 289): Some when the fault arrived
        /// from a running attempt, None when the runner never started
        /// (a miswired or null runner faults before any mint).
        | SuspendableFaulted of entry: InboxEntry * error: Exception * attempt: int * turnId: TurnId option

        /// A Reply inbox entry arrived for the suspended turn. Answered with
        /// SessionReplyReply; a mismatch rejects with ReplyMismatchException.
        | ReplyEntry of entry: InboxEntry

        /// Internal direct-actor reply ingress used by the unit-level actor
        /// seam. Routed production replies are admitted and appended by the
        /// scoped route gate before it sends ReplyEntry to this actor.
        | SessionReplyPayload of reply: Reply

        /// Reads the suspendable actor's snapshot. Answered with SessionSnapshot.
        | SuspendableGetSnapshot

        /// The AskTimeout deadline fired for the pending request id.
        /// One-way from the delay seam.
        | SuspendTimedOut of requestId: string

        /// Closes the suspendable session: the store row closes (evicting
        /// the grant memory) and the actor drops to Closed. Answered with
        /// the stored session.
        | SuspendableCloseSession of cancellationToken: CancellationToken

        /// Abort the turn running in a suspendable session under a typed
        /// stop cause: Idle and WaitingForInput (a suspended turn owns
        /// nothing running to abort) no-op returning the current snapshot;
        /// Running records the pending stop with first-cause-wins and answers
        /// the snapshot. The detached suspendable turn runs un-cancellable,
        /// so the recorded stop wins at its next report, mapping even a
        /// success to Aborted. Only abort-family causes
        /// (ExplicitAbort, HostShutdown) act; anything else is a no-op.
        /// Answered with <see cref="T:Legate.SessionSnapshot" />.
        | SuspendableAbortSession of cause: StopCause * reason: string * cancellationToken: CancellationToken
        /// Exact-target wake hint; receiving actors read persisted intent and validate tenant/session.
        | SuspendableObserveHostAbort of tenant: TenantId * sessionId: SessionId * targetTurnId: TurnId

        /// Compact the suspendable session on demand: Idle replays the
        /// journal and compacts now without starting a turn, Running arms
        /// the one-shot force flag the turn's force-aware boundary hook
        /// honors at the next iteration, WaitingForInput no-ops (a suspended
        /// turn owns the history), and Closed rejects. Answered with
        /// <see cref="T:Legate.SessionCompactReply" />.
        | SuspendableCompactSession of cancellationToken: CancellationToken

        /// Wakes the suspendable session for the polling dispatcher: when
        /// the actor is Idle and the durable inbox holds a drainable entry,
        /// starts a suspendable turn on the oldest one through the existing
        /// start path, otherwise a no-op. Never appends: dispatch routes
        /// through the prompt wire would duplicate pending work, so the
        /// wake only starts what is already stored. Idempotent by mailbox
        /// serialization plus the in-handler Idle re-check: a wake racing a
        /// prompt or a second wake collapses to a no-op or ordered
        /// queueing, never a duplicate or out-of-turn start. One-way from
        /// the dispatcher: no reply.
        | SuspendableCheckInbox

        /// Rebind the agent the session converses with: applies at once
        /// through the 6-step quiescent protocol when no turn is live and
        /// the pending inbox is empty, records the rebind as pending
        /// otherwise (Running, WaitingForInput, or queued entries), and
        /// rejects when Closed. A pending rebind applies at the next
        /// quiescent boundary: the settle drain, the faulted path, or the
        /// Idle handler before draining. Answered with
        /// <see cref="T:Legate.SessionSetAgentReply" />.
        | SuspendableSetAgent of agentId: AgentId * cancellationToken: CancellationToken

        /// One piped lifecycle store wait finished (issue 390). Same
        /// contract as the base protocol's LifecycleStoreCompleted:
        /// only the outstanding wait's op id plus the pipe incarnation
        /// resumes, everything else is discarded with zero effects.
        /// Internal to the actor loop, never crossing node boundaries.
        | SuspendableStoreCompleted of opId: int64 * incarnation: Guid * outcome: obj

        /// One piped lifecycle store wait outran its bound (issue 390).
        /// Same contract as the base protocol's LifecycleStoreTimeout.
        /// Internal to the actor loop, never crossing node boundaries.
        | SuspendableStoreTimeout of opId: int64 * incarnation: Guid

    /// The suspendable-loop state threaded through every message (issue
    /// 390): the lifecycle state plus the bounded pipe state, the pending
    /// inbox count cache snapshots answer from, the close senders waiting
    /// on the durable close write, and the one-shot inbox-count seeding
    /// after entry recovery.
    type private SuspendLoopArgs =
        {
            /// The actor's current lifecycle state.
            State: SessionState
            /// The parked turn, or None.
            Suspended: SuspendedTurn option
            /// Request ids already resolved.
            Resolved: HashSet<string>
            /// The store's pending inbox count as of the last inbox read or
            /// mutation the actor applied: snapshots answer from memory
            /// while a store wait is outstanding.
            PendingCount: int
            /// Close senders waiting on the durable close write: empty when
            /// no close is outstanding. A requested close behaves Closed for
            /// new lifecycle work while its write is outstanding.
            Closing: (IActorRef * CancellationToken) list
            /// True until the entry inbox-count seeding wait lands: received
            /// lifecycle work waits bounded behind it in arrival order.
            Seeding: bool
            /// A crash-resume turn start deferred to the loop: the entry,
            /// attempt, grants, and crash seed the interrupted turn
            /// restarts with once the seeding read landed. None afterwards.
            PendingResume: (InboxEntry * int * HashSet<string> * IList<ChatMessage> option) option
        }

    /// The bounded pipe state the suspendable loop threads.
    type private SuspendPipe = LifecyclePipe.PipeState<SuspendableActorMessage, SuspendLoopArgs>

    /// A suspendable-loop continuation: the loop state plus the pipe state it resumes with.
    type private SuspendCont = SuspendLoopArgs -> SuspendPipe -> Cont<SuspendableActorMessage, unit>

    /// Builds one suspendable loop's pipe starter: completions Tell the
    /// loop's own actor and pack for the suspendable protocol.
    /// <param name="self">The suspendable loop's own actor.</param>
    /// <param name="config">The resolved pipe configuration.</param>
    /// <returns>The starter the loop's waits run through.</returns>
    let private suspendableStarter
        (self: IActorRef)
        (config: LifecyclePipe.StorePipeConfig)
        : PipeStarter<SuspendableActorMessage, SuspendLoopArgs> =
        {
            Self = self
            Clock = config.Clock
            Timeout = config.Timeout
            PackCompleted = fun (opId, incarnation, outcome) -> SuspendableStoreCompleted(opId, incarnation, outcome)
            PackTimeout = fun (opId, incarnation) -> SuspendableStoreTimeout(opId, incarnation)
        }

    /// Reason carried by TurnFailed when AskTimeout fires while suspended.
    /// Never contains secrets or tool arguments.
    [<Literal>]
    let AskTimeoutReason = "The suspended turn timed out waiting for a host reply."

    /// Extracts the request id a Reply answers: the permission request id
    /// or the question id. Returns None when the reply carries no id.
    /// <param name="reply">The reply.</param>
    /// <returns>The id the reply answers, or None.</returns>
    let private replyRequestId (reply: Reply) : string option =
        match reply with
        | :? PermissionDecision as decision when not (isNull (box decision)) ->
            if isNull (box decision.RequestId) then
                None
            else
                Some decision.RequestId
        | :? QuestionAnswer as answer when not (isNull (box answer)) ->
            if isNull (box answer.QuestionId) then
                None
            else
                Some answer.QuestionId
        | _ -> None

    // ────────────────── Bounded tail probes (issue 389) ──────────────────
    //
    // Checkpoint-resumed tail probes: each probe keeps its own per-session
    // (cursor, state) checkpoint and replays only the post-checkpoint
    // suffix through the shared hardened consume, merging into the cached
    // state. The one full read with no checkpoint is the documented
    // initial reconstruction; a stale checkpoint (a gap between the cached
    // cursor and the first suffix sequence) falls back to explicit
    // reconstruction from cursor 0, never silent truncation. Probes retain
    // only small folded state (one pending option, one turn option, one
    // flag, or the turn-proportional marker sets for the orphan probe),
    // never the raw journal.

    /// Per-session probe checkpoints, keyed by "tenant|session|probe".
    let private probeCursors = ConcurrentDictionary<string, int64>()
    let private pendingStates = ConcurrentDictionary<string, RebuiltPending option>()
    let private lastTurnStates = ConcurrentDictionary<string, TurnId option>()
    let private tailFlagStates = ConcurrentDictionary<string, bool>()
    let private orphanMarkers = ConcurrentDictionary<string, ResizeArray<TurnId>>()
    let private orphanTerminals = ConcurrentDictionary<string, HashSet<TurnId>>()

    /// Clears all tail-probe checkpoints. Tests only.
    let clearProbeCheckpoints () : unit =
        probeCursors.Clear()
        pendingStates.Clear()
        lastTurnStates.Clear()
        tailFlagStates.Clear()
        orphanMarkers.Clear()
        orphanTerminals.Clear()

    /// Builds one probe's checkpoint key. Tenant and session scope the
    /// checkpoint so cached probe state never crosses isolation boundaries.
    let private probeKey (probe: string) (tenant: TenantId) (sessionId: SessionId) : string =
        sprintf "%O|%O|%s" tenant sessionId probe

    /// Consumes one probe's suffix: the post-checkpoint pages through the
    /// shared hardened consume, or the full journal from cursor 0 when no
    /// checkpoint exists (the documented initial reconstruction). A stale
    /// checkpoint (the first suffix sequence skips past the cached cursor
    /// plus one) falls back to explicit reconstruction from cursor 0.
    /// Unstamped sequences skip gap validation.
    /// <param name="probe">The probe name scoping the checkpoint.</param>
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to consume.</param>
    /// <param name="cancellationToken">Token that abandons the consume.</param>
    /// <returns>The suffix events in sequence order with the end cursor and whether a fallback ran.</returns>
    let private consumeProbeSuffixAsync
        (probe: string)
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (cancellationToken: CancellationToken)
        : Task<IReadOnlyList<SessionEvent> * int64 * bool> =
        task {
            let key = probeKey probe tenant sessionId

            let cached =
                match probeCursors.TryGetValue(key) with
                | true, cursor -> Some cursor
                | false, _ -> None

            match cached with
            | None ->
                let suffix = ResizeArray<SessionEvent>()

                let! stats =
                    BoundedReplay.consumePagesAsync eventStore tenant sessionId 0L 100 cancellationToken suffix.Add

                probeCursors[key] <- stats.LastCursor
                return (suffix :> IReadOnlyList<SessionEvent>), stats.LastCursor, false
            | Some resumeFrom ->
                let suffix = ResizeArray<SessionEvent>()

                let! stats =
                    BoundedReplay.consumePagesAsync
                        eventStore
                        tenant
                        sessionId
                        resumeFrom
                        100
                        cancellationToken
                        suffix.Add

                let gap =
                    if suffix.Count = 0 then
                        false
                    else
                        let first = suffix[0]

                        if isNull (box first) then
                            false
                        elif first.Sequence.HasValue then
                            first.Sequence.Value <> resumeFrom + 1L
                        else
                            false

                if gap then
                    let full = ResizeArray<SessionEvent>()

                    let! fullStats =
                        BoundedReplay.consumePagesAsync eventStore tenant sessionId 0L 100 cancellationToken full.Add

                    probeCursors[key] <- fullStats.LastCursor
                    return (full :> IReadOnlyList<SessionEvent>), fullStats.LastCursor, true
                else
                    probeCursors[key] <- stats.LastCursor
                    return (suffix :> IReadOnlyList<SessionEvent>), stats.LastCursor, false
        }

    /// Rebuilds the pending request from the journal (issue 389): replays
    /// only the post-checkpoint suffix through the shared hardened consume
    /// and merges into the cached pending, so successive probes add no
    /// repeated full-prefix scan. The one full read with no checkpoint is
    /// the documented initial reconstruction; a stale checkpoint falls back
    /// to explicit reconstruction. Returns the latest PermissionRequested
    /// or QuestionAsked with no matching resolve after it. A resolve
    /// matches when its request id equals the ask id. Returns None when
    /// nothing is pending.
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to rebuild.</param>
    /// <param name="cancellationToken">Token that abandons the replay.</param>
    /// <returns>The rebuilt pending, or None.</returns>
    let rebuildPendingFromJournal
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (cancellationToken: CancellationToken)
        : RebuiltPending option =
        ArgumentNullException.ThrowIfNull(eventStore)

        let foldOne (current: RebuiltPending option) (event: SessionEvent) : RebuiltPending option =
            if isNull (box event) then
                current
            else
                match event with
                | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
                    Some
                        {
                            RequestId = asked.RequestId
                            ToolName = asked.ToolName
                            Kind = TurnLoop.PermissionSuspension
                            QuestionText = ""
                        }
                | :? QuestionAskedEvent as asked when not (isNull (box asked)) ->
                    Some
                        {
                            RequestId = asked.QuestionId
                            ToolName = TurnLoop.AskUserToolName
                            Kind = TurnLoop.QuestionSuspension
                            QuestionText = asked.Question
                        }
                | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) ->
                    match current with
                    | Some awaiting when String.Equals(awaiting.RequestId, resolved.RequestId, StringComparison.Ordinal) ->
                        None
                    | _ -> current
                | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) ->
                    match current with
                    | Some awaiting when
                        String.Equals(awaiting.RequestId, answered.QuestionId, StringComparison.Ordinal)
                        ->
                        None
                    | _ -> current
                | _ -> current

        let key = probeKey "pending" tenant sessionId

        let seed =
            match pendingStates.TryGetValue(key) with
            | true, cached -> cached
            | false, _ -> None

        let suffix, _, hadFallback =
            awaitTask (consumeProbeSuffixAsync "pending" eventStore tenant sessionId cancellationToken)

        let start = if hadFallback then None else seed

        let mutable current = start

        if not (isNull (box suffix)) then
            for event in suffix do
                current <- foldOne current event

        pendingStates[key] <- current
        current

    /// In-memory resumption note appended to a crash-rehydrated history.
    /// Never journaled, so no wire-contract change and no duplication
    /// across repeated restarts. Never contains secrets or tool arguments.
    [<Literal>]
    let CrashResumptionNote =
        "The previous attempt was interrupted by a restart. Tool calls from the interrupted attempt were not replayed."

    /// Client-safe reason carried by TurnFailed when OnCrashResume fails the
    /// interrupted turn instead of resuming it. Never contains secrets or
    /// tool arguments.
    [<Literal>]
    let CrashFailReason = "The turn was interrupted by a restart."

    /// Rebuilds a crash-rehydrated LLM history from transcript cells: drops
    /// the interrupted attempt's ToolCall and ToolResult cells (completed
    /// exchanges survive as text, never replayed as calls), keeps every
    /// other cell in order through the shared estimate mapping, and appends
    /// the in-memory resumption note. Pure: reads the cells, returns fresh
    /// messages, performs no I/O.
    /// <param name="cells">The transcript cells, in order. Must not be null and must not contain null.</param>
    /// <param name="interruptedTurn">The interrupted turn whose tool cells drop.</param>
    /// <returns>The rehydrated history with the resumption note.</returns>
    let rehydrateHistoryFromCells (cells: IReadOnlyList<SessionCell>) (interruptedTurn: TurnId) : IList<ChatMessage> =
        if isNull (box cells) then
            raise (ArgumentNullException(nameof cells))

        let kept = ResizeArray<SessionCell>(cells.Count)

        for cell in cells do
            if isNull (box cell) then
                raise (ArgumentNullException(nameof cells))

            let isInterruptedTool =
                cell.TurnId = interruptedTurn
                && (cell.Kind = SessionCellKind.ToolCall || cell.Kind = SessionCellKind.ToolResult)

            if not isInterruptedTool then
                kept.Add(cell)

        let history = Compaction.messagesFromCells (kept :> IReadOnlyList<SessionCell>)
        history.Add(ChatMessage(ChatRole.System, CrashResumptionNote))
        history

    /// Rehydrates a crash-interrupted turn's history from the journal (issue
    /// 389): pages through the shared incremental transcript read (which
    /// folds every turn through SessionCellDeriver.Fold with no full raw
    /// retention beside the output, under the #388 cursor/cancellation
    /// contract), drops the interrupted turn's tool cells, and appends the
    /// in-memory resumption note. Crash recovery runs once per restart, so
    /// this full read is the documented exceptional initial
    /// reconstruction; ordinary continuation never repeats it (it resumes
    /// from the BoundedReplay checkpoint instead). The journal is
    /// append-only: old-attempt events stay, nothing journals. Unknown
    /// session, expired journal, and end of stream read as the note alone,
    /// so recovery never fails spuriously. Cancellation is observed between
    /// bounded units: a cancelled rehydrate raises instead of advertising
    /// complete context.
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to rehydrate.</param>
    /// <param name="interruptedTurn">The interrupted turn whose tool cells drop.</param>
    /// <param name="cancellationToken">Token that abandons the replay.</param>
    /// <returns>The rehydrated history with the resumption note.</returns>
    let rehydrateCrashHistory
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (interruptedTurn: TurnId)
        (cancellationToken: CancellationToken)
        : IList<ChatMessage> =
        ArgumentNullException.ThrowIfNull(eventStore)

        let options = ReadTranscriptOptions()

        let cells =
            awaitTask (Transcripts.readTranscript eventStore tenant sessionId options 100 cancellationToken)

        rehydrateHistoryFromCells cells interruptedTurn

    /// Reads the interrupted turn id as the journal's most recent turn
    /// (issue 389): the last event's turn in sequence order, checkpoint
    /// resumed so successive probes add no repeated full-prefix scan. The
    /// one full read with no checkpoint is the documented initial
    /// reconstruction; a stale checkpoint falls back to explicit
    /// reconstruction. Returns None when the journal carries no events, so
    /// the caller falls back to a fresh turn id.
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to inspect.</param>
    /// <param name="cancellationToken">Token that abandons the replay.</param>
    /// <returns>The most recent turn id, or None on an empty journal.</returns>
    let lastJournalTurnId
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (cancellationToken: CancellationToken)
        : TurnId option =
        ArgumentNullException.ThrowIfNull(eventStore)

        let key = probeKey "lastTurn" tenant sessionId

        let seed =
            match lastTurnStates.TryGetValue(key) with
            | true, cached -> cached
            | false, _ -> None

        let suffix, _, hadFallback =
            awaitTask (consumeProbeSuffixAsync "lastTurn" eventStore tenant sessionId cancellationToken)

        let mutable current = if hadFallback then None else seed

        // Track whether the (re)read saw any event at all: a fallback full
        // scan with no events reads as None like the empty journal.
        let mutable observed = false

        if not (isNull (box suffix)) then
            for event in suffix do
                if not (isNull (box event)) then
                    observed <- true
                    current <- Some event.TurnId

        if hadFallback && not observed then
            current <- None

        lastTurnStates[key] <- current
        current

    /// Reads whether the journal tail holds an unterminated turn (issue
    /// 287, bounded by 389): the last TurnStartedEvent with no terminal
    /// after it (TurnCompleted, TurnFailed, TurnAborted, or SessionClosed),
    /// checkpoint resumed so successive probes add no repeated full-prefix
    /// scan. A marker-only journal (a single TurnStartedEvent, the
    /// mid-LLM-call kill shape) reads as true; an empty journal reads as
    /// false, so a Running row with no work to recover still idles instead
    /// of failing spuriously. Completion journals nothing, so this check
    /// runs only for Running rows with an empty inbox: Idle rows (including
    /// healthy completions, which also end marker-only) never consult it,
    /// and a settled orphan flips to Idle with its TurnFailedEvent
    /// terminal, so a later restart reads false and stays quiet. Read-only:
    /// never appends. Cancellation is observed between bounded units.
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to inspect.</param>
    /// <param name="cancellationToken">Token that abandons the replay.</param>
    /// <returns>True when the tail shows an unterminated turn.</returns>
    let hasUnterminatedTurnTail
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (cancellationToken: CancellationToken)
        : bool =
        ArgumentNullException.ThrowIfNull(eventStore)

        let key = probeKey "tailFlag" tenant sessionId

        let seed =
            match tailFlagStates.TryGetValue(key) with
            | true, cached -> cached
            | false, _ -> false

        let suffix, _, hadFallback =
            awaitTask (consumeProbeSuffixAsync "tailFlag" eventStore tenant sessionId cancellationToken)

        let mutable current = if hadFallback then false else seed

        if not (isNull (box suffix)) then
            for event in suffix do
                if not (isNull (box event)) then
                    match event with
                    | :? TurnStartedEvent -> current <- true
                    | :? TurnCompletedEvent -> current <- false
                    | :? TurnFailedEvent -> current <- false
                    | :? TurnAbortedEvent -> current <- false
                    | :? SessionClosedEvent -> current <- false
                    | _ -> ()

        tailFlagStates[key] <- current
        current

    /// The suspendable session actor: like behavior but driving the
    /// suspendable runner, entering WaitingForInput store-first on suspend,
    /// matching Reply ids with the typed error, resuming from the cursor
    /// with attempt plus 1, settling Failed with TurnFailed on AskTimeout,
    /// and rebuilding the pending request from the journal on restart. The
    /// claim heartbeat is never cancelled on suspend: renewal continues
    /// while suspended under the same claim (proven by test; no new
    /// background work, bounded by lease expiry and AskTimeout). Reply
    /// never starts a turn: it only resumes the suspended one. Inject folds
    /// at the next iteration boundary through the runner's drain hooks and
    /// Interrupt pre-empts through the ExplicitAbort pending stop; the
    /// detached suspendable turn runs un-cancellable, so a recorded stop
    /// wins at its next report. Abort on WaitingForInput stays a no-op per
    /// #35, as does Compact there.
    /// <param name="props">The base session actor dependencies.</param>
    /// <param name="suspend">The suspend dependencies.</param>
    /// <param name="mailbox">The actor mailbox, injected by spawn.</param>
    /// <returns>The Akka.FSharp actor computation to spawn.</returns>
    let behaviorWithSuspendRouted
        (validateRoute: unit -> unit)
        (checkRoute: Session -> unit)
        (props: SessionActorProps)
        (suspend: SuspendDeps)
        (clock: TimeProvider)
        (heartbeatOptions: ClaimHeartbeat.ClaimHeartbeatOptions option)
        (initialDeferred: (SuspendableActorMessage * IActorRef) list)
        (mailbox: Actor<SuspendableActorMessage>)
        =
        ArgumentNullException.ThrowIfNull(clock)
        ArgumentNullException.ThrowIfNull(checkRoute)
        validateRoute ()

        if isNull (box props.Store) then
            raise (ArgumentNullException(nameof props))

        if isNull (box props.RunTurn) then
            raise (ArgumentNullException(nameof props))

        if isNull (box suspend.EventStore) then
            raise (ArgumentNullException(nameof suspend))

        if isNull (box suspend.Delay) then
            raise (ArgumentNullException(nameof suspend))

        if isNull (box suspend.JournalToken) then
            raise (ArgumentNullException(nameof suspend))

        if isNull (box suspend.RunSuspendable) then
            raise (ArgumentNullException(nameof suspend))

        if isNull (box suspend.EraMarked) then
            raise (ArgumentNullException(nameof suspend))

        if suspend.AskTimeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof suspend, "SuspendDeps.AskTimeout must be positive."))

        match props.Compact with
        | Some compact -> requireCompactDeps compact
        | None -> ()

        let suspendSelf = mailbox.Self

        /// Reads the session's persisted AllowForSession memory: the grant
        /// tool names the store row carries. A missing row or a null grant
        /// list reads as empty, so a stored row from before grants existed
        /// resumes with no memory and the persisted set always wins over the
        /// actor's in-memory copy on rebuild.
        /// <returns>The granted tool names.</returns>
        let readGrantsNow () : HashSet<string> =
            try
                match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> HashSet<string>()
                | session when isNull (box session.PermissionGrants) -> HashSet<string>()
                | session -> HashSet<string>(session.PermissionGrants :> seq<string>)
            with :? SessionNotFoundException ->
                HashSet<string>()

        let crashKnobOf (session: Session) : OnCrashResume =
            if isNull (box session) || isNull (box session.Options) then
                OnCrashResume.FailAttempt
            else
                session.Options.OnCrashResume

        // The journal token fencing this actor's journal appends: the
        // spawn-primed token at first, replaced by the SetAgent swap with
        // each re-prime. Every journal write below reads this cell, never
        // SuspendDeps.JournalToken, so post-swap writes stay live. A cell
        // rather than a loop parameter, like pendingStop below.
        let mutable journalToken = suspend.JournalToken
        let mutable controlPrime = suspend.PrimeClaim
        let controlReports = Dictionary<int64, TurnId * TurnClaim * string>()
        let completedControlReports = HashSet<string>()

        // Production turn-ownership heartbeat (issue 375): the actor-owned
        // per-attempt renewal over the spawn-time prime. The view backs the
        // ambient LeaseAdmission scope the runner reads; the CTS cancels on
        // every terminal path. Suspended renewal survives WaitingForInput
        // under the same claim without consuming replies or resuming work.
        let mutable heartbeatCts: CancellationTokenSource option = None
        let mutable heartbeatView: ClaimHeartbeat.ClaimLeaseView option = None

        let cancelHeartbeat () : unit =
            match heartbeatCts with
            | Some cts ->
                try
                    cts.Cancel()
                with _ ->
                    ()

                heartbeatCts <- None
                heartbeatView <- None
            | None -> ()

        /// Starts the production heartbeat for one attempt over its bound
        /// prime claim: fire-and-forget renewal that never touches inbox or
        /// lifecycle state, so detached or terminal loops stay inert. Loss
        /// pipes back through the existing SuspendableFaulted path with the
        /// lease-loss cause. The actor thread never awaits the loop.
        let startHeartbeat (entry: InboxEntry) (attempt: int) (runId: TurnId) (claim: TurnClaim) : unit =
            match heartbeatOptions with
            | None -> ()
            | Some hbOptions ->
                if isNull (box claim) then
                    ()
                else
                    cancelHeartbeat ()

                    let view = ClaimHeartbeat.ClaimLeaseView(clock, claim)
                    let cts = new CancellationTokenSource()
                    heartbeatView <- Some view
                    heartbeatCts <- Some cts

                    let loopTask =
                        ClaimHeartbeat.runWithStoreAsync
                            props.Store
                            props.Tenant
                            claim
                            hbOptions
                            clock
                            suspend.Delay
                            (fun () -> false)
                            (Some view)
                            cts.Token

                    loopTask.ContinueWith(fun (completed: Task<ClaimHeartbeat.ClaimHeartbeatDecision>) ->
                        if completed.IsCompletedSuccessfully then
                            match completed.Result with
                            | ClaimHeartbeat.StopLeaseLost ->
                                try
                                    suspendSelf.Tell(
                                        SuspendableFaulted(
                                            entry,
                                            TurnLoop.TurnLeaseLostException() :> Exception,
                                            attempt,
                                            Some runId
                                        )
                                    )
                                with _ ->
                                    ()
                            | ClaimHeartbeat.StopCancelled -> ()
                            | _ -> ())
                    |> ignore

        let controlStore =
            match props.Store with
            | :? ISessionAbortControlStore as control -> Some control
            | _ when suspend.PrimeClaim.IsNone -> None
            | _ -> raise (InvalidOperationException("ISessionAbortControlStore is required."))

        let controlAdmission turn position claim () =
            match controlStore with
            | None -> true
            | Some control ->
                let checkedTarget =
                    awaitTask (
                        control.CheckControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            position,
                            claim,
                            CancellationToken.None
                        )
                    )

                checkedTarget.Outcome = ControlOperationOutcome.Applied

        let decideControl (entry: InboxEntry) (candidate: TurnResult) =
            match controlStore, controlReports.TryGetValue entry.Position with
            | Some control, (true, (turn, claim, id)) ->
                if completedControlReports.Contains id then
                    raise (
                        InvalidSessionStateException(
                            props.SessionId,
                            "duplicateControlReport",
                            "A duplicate report cannot publish or drain."
                        )
                    )

                let cause, reason =
                    match candidate.Outcome with
                    | :? TurnAborted as stop -> Nullable stop.Cause, (stop.Reason: string | null)
                    | _ when candidate.Status = TurnStatus.Aborted ->
                        Nullable StopCause.ExplicitAbort, (InterruptReason: string | null)
                    | _ -> Nullable(), null

                let decided =
                    awaitTask (
                        control.TryDecideControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            entry.Position,
                            claim,
                            id,
                            candidate.Status,
                            cause,
                            reason,
                            CancellationToken.None
                        )
                    )

                match decided.Outcome, decided.Decision with
                | ControlOperationOutcome.Applied, evidence ->
                    match evidence with
                    | null -> raise (InvalidOperationException("Applied control decision has no evidence."))
                    | evidence when evidence.Status = TurnStatus.Aborted ->
                        { candidate with
                            Status = TurnStatus.Aborted
                            Outcome =
                                TurnAborted(
                                    evidence.Cause.Value,
                                    evidence.Reason |> Option.ofObj |> Option.defaultValue ""
                                )
                                :> TurnOutcome
                        }
                    | evidence ->
                        { candidate with
                            Status = evidence.Status
                        }
                | _ ->
                    raise (
                        InvalidSessionStateException(
                            props.SessionId,
                            "controlPending",
                            "Control decision refused this report; no downstream effects are authorized."
                        )
                    )
            | _ -> candidate

        let retireControl (entry: InboxEntry) =
            match controlStore, controlReports.TryGetValue entry.Position with
            | Some control, (true, (turn, claim, id)) ->
                let retired =
                    awaitTask (
                        control.RetireControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            entry.Position,
                            claim,
                            id,
                            CancellationToken.None
                        )
                    )

                if retired.Outcome <> ControlOperationOutcome.Applied then
                    raise (
                        InvalidSessionStateException(
                            props.SessionId,
                            "controlPending",
                            "Control retirement refused further settlement or queue drain."
                        )
                    )

                completedControlReports.Add id |> ignore
            | _ -> ()

        // The on-demand compaction wiring the Idle compact path runs: the
        // spawn wiring at first, refreshed by the SetAgent swap with each
        // re-prime, so later Compacts journal under the live token.
        let mutable currentCompact = props.Compact

        // The turn id the running attempt executes as (issue 289): set by
        // startSuspendable and resumeSuspendable, cleared when the attempt
        // reports. The SuspendableFinished/Faulted handlers resolve the
        // settling id from the carried id, then this cell, then the
        // CurrentTurnId snapshot, else journal nothing.
        let mutable runningTurnId: TurnId option = None

        /// Snapshots the session row's live turn, or None when the row is
        /// missing, the turn settled to null, or the read fails.
        /// <returns>The live turn id, or None.</returns>
        let currentTurnSnapshot () : TurnId option =
            try
                match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> None
                | session when session.CurrentTurnId.HasValue -> Some session.CurrentTurnId.Value
                | _ -> None
            with _ ->
                None

        /// Re-primes the journal through the spawn wiring: a fresh bootstrap
        /// plus ClaimNextTurn, or None when the host never re-primes, the
        /// prime fails, or a live claim is held (takeover, or a restart
        /// inside the old prime's lease). Total: a throwing prime reads as
        /// None and the recorded rebind retries at the next boundary.
        /// Entry-recovery only: the loop pipes ReprimeJournal through
        /// withReprime instead of blocking on it.
        /// <returns>The live claim, or None.</returns>
        let reprimeNow () : TurnClaim option =
            match suspend.ReprimeJournal with
            | None -> None
            | Some reprime ->
                try
                    awaitTask (reprime ())
                with _ ->
                    None

        /// Settles a claim Completed with a null outcome, best-effort: the
        /// first settle wins and a retry of the same outcome observes it, so
        /// only the settled/already-settled outcomes read as settled. Total:
        /// a rejected or faulted settle reads as false.
        /// <param name="claim">The claim fencing the settlement. Must not be null.</param>
        /// <returns>True when the turn settled.</returns>
        let settleTurnQuiet (claim: TurnClaim) : bool =
            try
                match
                    awaitTask (
                        props.Store.SettleTurn(props.Tenant, claim, TurnStatus.Completed, null, CancellationToken.None)
                    )
                with
                | :? TurnSettled -> true
                | :? TurnAlreadySettled -> true
                | _ -> false
            with _ ->
                false

        /// Explicit graph settlement takes priority over the unified-store fallback.
        let settlementStore: ISessionSettlementStore option =
            match props.Settlement with
            | Some capable -> Some capable
            | None ->
                match props.Store with
                | :? ISessionSettlementStore as capable -> Some capable
                | _ -> None

        let notifySettled (result: TurnResult) : unit =
            match props.OnTurnSettled with
            | Some observe ->
                try
                    observe result
                with _ ->
                    ()
            | None -> ()

        let notifyPosition (position: int64) : unit =
            try
                PromptWaitHubs.NotifyPositionScoped props.Tenant props.SessionId position
            with _ ->
                ()

        /// Startup failure uses the same original association and renewed prime
        /// as resume. No fault or refusal permits the legacy cleanup path.
        let failRecoveredTurn (entry: InboxEntry) : SessionSettlementOutcome =
            let refuse () =
                raise (
                    InvalidSessionStateException(
                        props.SessionId,
                        "recoverySettlementUnavailable",
                        "Crash settlement refused authority or committed disposition."
                    )
                )

            let safe operation =
                try
                    operation ()
                with _ ->
                    refuse ()

            match suspend.Recovery, settlementStore, controlStore with
            | null, _, _
            | _, None, _
            | _, _, None -> refuse ()
            | recovery, Some capable, Some control ->
                let claim = recovery.Claim
                let target = recovery.Target

                if isNull (box claim) || isNull (box target) || isNull (box recovery.Entry) then
                    refuse ()

                let claim = unbox<TurnClaim> (box claim)
                let target = unbox<AbortTarget> (box target)
                let original = unbox<InboxEntry> (box recovery.Entry)

                if
                    recovery.Outcome <> ControlOperationOutcome.Applied
                    || original.SessionId <> props.SessionId
                    || target.SessionId <> props.SessionId
                    || original.Position <> entry.Position
                    || target.InboxPosition <> entry.Position
                    || target.TurnId <> entry.TurnId
                then
                    refuse ()

                if
                    not (
                        safe (fun () ->
                            awaitTask (
                                capable.AdmitExecution(
                                    props.Tenant,
                                    props.SessionId,
                                    entry.Position,
                                    claim,
                                    CancellationToken.None
                                )
                            ))
                    )
                then
                    refuse ()

                let candidate =
                    {
                        AssistantText = ""
                        Status = TurnStatus.Failed
                        Iterations = 0
                        Usage = { InputTokens = 0L; OutputTokens = 0L }
                        Outcome = TurnFailed(CrashFailReason) :> TurnOutcome
                    }

                let _, _, decisionId = controlReports[entry.Position]

                let holdsAuthority () =
                    try
                        match awaitTask (props.Store.VerifyClaim(props.Tenant, claim, CancellationToken.None)) with
                        | :? TurnLeaseHeld
                        | :? TurnLeaseRenewed -> true
                        | _ -> false
                    with _ ->
                        false

                let decide () =
                    awaitTask (
                        control.TryDecideControlTarget(
                            props.Tenant,
                            props.SessionId,
                            target.TurnId,
                            entry.Position,
                            claim,
                            decisionId,
                            candidate.Status,
                            Nullable(),
                            null,
                            CancellationToken.None
                        )
                    )

                let decided =
                    try
                        decide ()
                    with _ ->
                        if holdsAuthority () then safe decide else refuse ()

                let selected =
                    match decided.Outcome, decided.Decision with
                    | (ControlOperationOutcome.Applied | ControlOperationOutcome.AlreadyDecided), evidence when
                        not (isNull (box evidence))
                        ->
                        let evidence = unbox<ControlTargetDecision> (box evidence)

                        if
                            evidence.SessionId <> props.SessionId
                            || evidence.TurnId <> target.TurnId
                            || evidence.InboxPosition <> entry.Position
                            || evidence.DecisionId <> decisionId
                            || evidence.ProposedStatus <> candidate.Status
                            || evidence.ProposedCause.HasValue
                            || not (isNull evidence.ProposedReason)
                            || evidence.Retired
                        then
                            refuse ()

                        if evidence.Status = TurnStatus.Aborted && evidence.Cause.HasValue then
                            { candidate with
                                Status = TurnStatus.Aborted
                                Outcome =
                                    TurnAborted(
                                        evidence.Cause.Value,
                                        evidence.Reason |> Option.ofObj |> Option.defaultValue ""
                                    )
                                    :> TurnOutcome
                            }
                        elif evidence.Status = TurnStatus.Failed then
                            candidate
                        else
                            refuse ()
                    | _ -> refuse ()

                let terminal: SessionEvent =
                    match selected.Outcome with
                    | :? TurnAborted as stop ->
                        TurnAbortedEvent(
                            props.SessionId,
                            target.TurnId,
                            Nullable(),
                            clock.GetUtcNow(),
                            stop.Cause,
                            stop.Reason
                        )
                    | _ ->
                        TurnFailedEvent(props.SessionId, target.TurnId, Nullable(), clock.GetUtcNow(), CrashFailReason)

                let request =
                    SessionSettlementRequest(
                        props.SessionId,
                        entry.Position,
                        claim,
                        Nullable target.TurnId,
                        selected,
                        mintCompletionKey (),
                        terminal
                    )

                // A lost response may conceal a commit. Retry only this exact
                // request, once, while the captured prime still holds authority.
                let outcome =
                    try
                        awaitTask (capable.SettleExecution(props.Tenant, request, CancellationToken.None))
                    with _ ->
                        if holdsAuthority () then
                            safe (fun () ->
                                awaitTask (capable.SettleExecution(props.Tenant, request, CancellationToken.None)))
                        else
                            refuse ()

                if
                    (outcome.Status <> SessionSettlementStatus.Applied
                     && outcome.Status <> SessionSettlementStatus.AlreadyApplied)
                    || isNull (box outcome.Result)
                then
                    refuse ()

                let winner = unbox<TurnResult> (box outcome.Result)

                if outcome.Status = SessionSettlementStatus.Applied then
                    JournalWriter.notifyPublished props.Tenant props.SessionId outcome.Events
                    notifySettled winner
                    notifyPosition entry.Position

                    match outcome.JournalReason, props.Logger with
                    | null, _
                    | _, null -> ()
                    | _, logger -> logger.LogWarning("Crash settlement committed without its optional journal event.")

                // Settlement keeps the decided prime until retirement. A read
                // of a receipt alone never authorizes this cleanup.
                let retired =
                    safe (fun () ->
                        awaitTask (
                            control.RetireControlTarget(
                                props.Tenant,
                                props.SessionId,
                                target.TurnId,
                                entry.Position,
                                claim,
                                decisionId,
                                CancellationToken.None
                            )
                        ))

                if retired.Outcome <> ControlOperationOutcome.Applied then
                    refuse ()

                completedControlReports.Add decisionId |> ignore

                if isNull (box outcome.Following) then
                    match
                        safe (fun () ->
                            awaitTask (
                                props.Store.SettleTurn(
                                    props.Tenant,
                                    claim,
                                    winner.Status,
                                    winner.Outcome,
                                    CancellationToken.None
                                )
                            ))
                    with
                    | :? TurnSettled -> controlPrime <- None
                    | _ -> refuse ()

                outcome

        let failInterruptedTurn (liveId: TurnId option) (entryOpt: InboxEntry option) : unit =
            let candidate =
                {
                    AssistantText = ""
                    Status = TurnStatus.Failed
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = TurnFailed(CrashFailReason) :> TurnOutcome
                }

            let fenced =
                entryOpt
                |> Option.exists (fun entry -> controlReports.ContainsKey entry.Position)

            let result =
                match entryOpt with
                | Some entry when fenced -> decideControl entry candidate
                | _ -> candidate

            if fenced then
                match entryOpt with
                | Some entry ->
                    awaitTask (
                        props.Store.MarkInboxConsumed(
                            props.Tenant,
                            props.SessionId,
                            [| entry.Position |],
                            CancellationToken.None
                        )
                    )
                    |> ignore
                | None -> ()

            match props.OnTurnSettled with
            | Some observe ->
                try
                    observe result
                with _ ->
                    ()
            | None -> ()

            dispatchCompletionNow props result |> ignore

            match entryOpt with
            | Some _ when fenced -> ()
            | Some entry ->
                let positions = [| entry.Position |] :> IReadOnlyList<int64>

                try
                    awaitTask (
                        props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, CancellationToken.None)
                    )
                    |> ignore
                with _ ->
                    ()
            | None -> ()

            if fenced then
                match liveId with
                | Some turnId ->
                    let event: SessionEvent =
                        match result.Outcome with
                        | :? TurnAborted as stop ->
                            TurnAbortedEvent(
                                props.SessionId,
                                turnId,
                                Nullable(),
                                DateTimeOffset.UtcNow,
                                stop.Cause,
                                stop.Reason
                            )
                        | _ ->
                            TurnFailedEvent(props.SessionId, turnId, Nullable(), DateTimeOffset.UtcNow, CrashFailReason)

                    awaitTask (
                        JournalWriter.appendWithTokenAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            journalToken
                            [| event |]
                            CancellationToken.None
                    )
                    |> ignore
                | None -> ()

                match entryOpt with
                | Some entry -> retireControl entry
                | None -> ()

                awaitTask (
                    props.Store.UpdateSessionState(
                        props.Tenant,
                        props.SessionId,
                        SessionState.Idle,
                        CancellationToken.None
                    )
                )
                |> ignore
            else
                try
                    awaitTask (
                        props.Store.UpdateSessionState(
                            props.Tenant,
                            props.SessionId,
                            SessionState.Idle,
                            CancellationToken.None
                        )
                    )
                    |> ignore
                with _ ->
                    ()

            // The crash-path terminal (issue 289): the CurrentTurnId
            // snapshot the crash path never ran a loop for. Fenced under
            // the live journal token; a settled-NULL turn (None) journals
            // nothing. Stays on the prime (issue 373): the write terminates
            // an execution turn, so only the claim fence proves the writer
            // is no takeover loser; the host path cannot tell a stale
            // generation from the live one. Re-anchoring belongs to #400.
            match liveId with
            | Some _ when fenced -> ()
            | Some turnId ->
                try
                    let failedEvent =
                        TurnFailedEvent(
                            props.SessionId,
                            turnId,
                            Nullable<int64>(),
                            DateTimeOffset.UtcNow,
                            CrashFailReason
                        )
                        :> SessionEvent

                    let events =
                        ResizeArray<SessionEvent>([| failedEvent |]) :> IReadOnlyList<SessionEvent>

                    awaitTask (
                        JournalWriter.appendWithTokenAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            journalToken
                            events
                            CancellationToken.None
                    )
                    |> ignore
                with _ ->
                    ()
            | None -> ()

        /// Reads the journal state for one turn id (issue 289): whether the
        /// journal holds the TurnStartedEvent marker for the id, whether
        /// any terminal row (TurnCompleted, TurnFailed, TurnAborted,
        /// SessionClosed) carries it, and the latest marker id with no
        /// terminal row (the journal-derived orphan). Read-only: never
        /// appends. A failed replay reads as no marker, so the probe stays
        /// quiet.
        ///
        /// The fallback exists because a winning prime replaces the row's
        /// turn id: after the dispatcher's poke-claim (or the spawn prime)
        /// wins post-expiry, CurrentTurnId names the prime, never the
        /// orphan, so marker-for-live-id alone would stay quiet forever.
        /// The orphan id survives only in the journal.
        /// <param name="liveId">The live turn to inspect.</param>
        /// <param name="cancellationToken">Token that abandons the replay.</param>
        /// <returns>The marker flag and terminal flag for the live id, plus the latest unterminated marker id.</returns>
        let journalOrphanState (liveId: TurnId) (cancellationToken: CancellationToken) : bool * bool * TurnId option =
            let key = sprintf "%O|%O|orphan" props.Tenant props.SessionId

            let markers =
                match orphanMarkers.TryGetValue(key) with
                | true, cached when not (isNull (box cached)) -> cached
                | _ ->
                    let created = ResizeArray<TurnId>()
                    orphanMarkers[key] <- created
                    created

            let terminals =
                match orphanTerminals.TryGetValue(key) with
                | true, cached when not (isNull (box cached)) -> cached
                | _ ->
                    let created = HashSet<TurnId>()
                    orphanTerminals[key] <- created
                    created

            let suffix, _, hadFallback =
                awaitTask (
                    consumeProbeSuffixAsync "orphan" suspend.EventStore props.Tenant props.SessionId cancellationToken
                )

            if hadFallback then
                // Explicit reconstruction: the cached sets describe a
                // skipped prefix, so rebuild them from the full suffix.
                markers.Clear()
                terminals.Clear()

            if not (isNull (box suffix)) then
                for event in suffix do
                    if not (isNull (box event)) then
                        match event with
                        | :? TurnStartedEvent -> markers.Add(event.TurnId)
                        | :? TurnCompletedEvent
                        | :? TurnFailedEvent
                        | :? TurnAbortedEvent
                        | :? SessionClosedEvent -> terminals.Add(event.TurnId) |> ignore
                        | _ -> ()

            let mutable foundMarker = false
            let mutable foundTerminal = false

            for marker in markers do
                if marker.Equals(liveId) then
                    foundMarker <- true

            if foundMarker then
                foundTerminal <- terminals.Contains(liveId)

            let fallback =
                markers
                |> Seq.filter (fun candidate -> not (terminals.Contains(candidate)))
                |> Seq.tryLast

            foundMarker, foundTerminal, fallback

        /// Reads whether the live turn is an orphan (issue 289), in order:
        /// the settling id (the live id when its marker stands unterminated,
        /// else the journal-derived orphan the prime replaced), an
        /// era-marked session, and a won re-prime (a live lease held
        /// elsewhere reads quiet). The won claim's token is adopted into
        /// the journal cell so fenced writes land; the caller settles the
        /// prime quietly after failing the orphan (a never-ran prime
        /// journals nothing).
        /// <param name="liveId">The live turn to probe.</param>
        /// <param name="cancellationToken">Token that abandons the journal probe.</param>
        /// <returns>The settling turn id with the won prime claim, or None when the session stays quiet.</returns>
        let probeOrphanTurn (liveId: TurnId) (cancellationToken: CancellationToken) : (TurnId * TurnClaim) option =
            let marker, terminal, fallback = journalOrphanState liveId cancellationToken

            let settling = if marker && not terminal then Some liveId else fallback

            match settling with
            | None -> None
            | Some settlingId ->
                let marked =
                    try
                        awaitTask (suspend.EraMarked props.Tenant props.SessionId CancellationToken.None)
                    with _ ->
                        false

                if not marked then
                    None
                else
                    match reprimeNow () with
                    | None -> None
                    | Some prime ->
                        journalToken <- prime.Token
                        controlPrime <- Some prime
                        Some(settlingId, prime)

        let originalRecoveryEntry () =
            match suspend.Recovery with
            | null ->
                try
                    awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None))
                    |> selectDrainableEntries
                    |> List.tryHead
                with _ ->
                    None
            | recovery -> recovery.Entry |> Option.ofObj

        let initialRecovered: SessionState * RebuiltPending option * InboxEntry option =
            match controlStore, controlPrime with
            | Some control, Some _ ->
                match awaitTask (control.ReadAbortTarget(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> ()
                | target ->
                    match target, suspend.Recovery with
                    | _, null ->
                        raise (
                            InvalidSessionStateException(
                                props.SessionId,
                                "controlPending",
                                "Recovery requires a provider-fenced original association."
                            )
                        )
                    | target, recovery when target.State = ControlTargetState.Active && isNull (box target.Stop) ->
                        match recovery.Claim, recovery.Entry with
                        | null, _
                        | _, null ->
                            raise (
                                InvalidSessionStateException(
                                    props.SessionId,
                                    "controlPending",
                                    "Recovery has no original association or authority."
                                )
                            )
                        | claim, _ ->
                            if not (controlAdmission target.TurnId target.InboxPosition claim ()) then
                                raise (
                                    InvalidSessionStateException(
                                        props.SessionId,
                                        "controlPending",
                                        "Recovery admission lost its exact authority or observed stop."
                                    )
                                )

                            controlReports[target.InboxPosition] <- target.TurnId, claim, Guid.NewGuid().ToString("N")
                    | _ ->
                        raise (
                            InvalidSessionStateException(
                                props.SessionId,
                                "controlPending",
                                "Persisted attribution blocks fresh recovery; no synthetic prime or settlement is authorized."
                            )
                        )
            | _ -> ()

            let found =
                awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))

            match found with
            | null -> SessionState.Idle, None, None
            | session ->
                // The crash-path terminal id (issue 289): the live turn
                // the crash interrupted, or None for a settled-NULL row
                // (which journals nothing).
                let liveId =
                    match suspend.Recovery with
                    | null when session.CurrentTurnId.HasValue -> Some session.CurrentTurnId.Value
                    | null -> None
                    | recovery -> recovery.Target |> Option.ofObj |> Option.map _.TurnId

                match session.State with
                | SessionState.Running ->
                    let drainable = originalRecoveryEntry ()

                    match crashKnobOf session with
                    | OnCrashResume.FailAttempt ->
                        match drainable with
                        | Some entry when not (isNull (box suspend.Recovery)) ->
                            let committed = failRecoveredTurn entry
                            committed.State, None, (committed.Following |> Option.ofObj)
                        | _ when suspend.PrimeClaim.IsSome ->
                            raise (
                                InvalidSessionStateException(
                                    props.SessionId,
                                    "recoveryAssociationUnavailable",
                                    "Crash settlement requires its original provider association."
                                )
                            )
                        | Some _ ->
                            failInterruptedTurn liveId drainable
                            SessionState.Idle, None, None
                        | None ->
                            // Marker-only orphan (issue 287): the claim
                            // consumed the inbox before the mid-LLM-call
                            // kill, so no drainable remains but the journal
                            // tail shows an unterminated turn. Fail fenced
                            // under the fresh primed token. An empty tail
                            // (no TurnStarted) idles instead of failing
                            // spuriously.
                            let orphaned =
                                try
                                    hasUnterminatedTurnTail
                                        suspend.EventStore
                                        props.Tenant
                                        props.SessionId
                                        CancellationToken.None
                                with _ ->
                                    false

                            if orphaned then
                                failInterruptedTurn liveId None
                                SessionState.Idle, None, None
                            else
                                awaitTask (
                                    props.Store.UpdateSessionState(
                                        props.Tenant,
                                        props.SessionId,
                                        SessionState.Idle,
                                        CancellationToken.None
                                    )
                                )
                                |> ignore

                                SessionState.Idle, None, None
                    | _ ->
                        match drainable with
                        | Some entry -> SessionState.Running, None, Some entry
                        | None ->
                            // Resume knob with an empty inbox and an
                            // unterminated tail cannot restart: the consumed
                            // claim took the only message and the
                            // marker-only journal carries no UserMessage to
                            // retry from, so a seedless restart has no entry
                            // to run. Fail fenced to settle the orphan
                            // (the smoke default Fail terminal); restart
                            // stays available whenever a drainable entry
                            // exists. An empty tail idles as before.
                            let orphaned =
                                try
                                    hasUnterminatedTurnTail
                                        suspend.EventStore
                                        props.Tenant
                                        props.SessionId
                                        CancellationToken.None
                                with _ ->
                                    false

                            if orphaned then
                                failInterruptedTurn liveId None
                                SessionState.Idle, None, None
                            else
                                awaitTask (
                                    props.Store.UpdateSessionState(
                                        props.Tenant,
                                        props.SessionId,
                                        SessionState.Idle,
                                        CancellationToken.None
                                    )
                                )
                                |> ignore

                                SessionState.Idle, None, None
                | SessionState.WaitingForInput ->
                    let rebuilt =
                        rebuildPendingFromJournal suspend.EventStore props.Tenant props.SessionId CancellationToken.None

                    SessionState.WaitingForInput, rebuilt, None
                | SessionState.Idle ->
                    // Live-turn orphan probe (issue 289): an Idle row still
                    // carrying a live turn whose journal holds an
                    // unterminated marker — for the live id itself, or for
                    // the orphan id a won prime replaced (the marker is the
                    // only surviving record of it) — on an era-marked
                    // session with no live lease elsewhere, enters the
                    // Running-branch shape so entity start runs the
                    // journal-tail recovery. Settled-NULL, terminated,
                    // empty-tail, live-lease, and pre-era sessions all stay
                    // quiet with the turn preserved; the losing branch
                    // journals nothing (loser-zero-effects).
                    if session.CurrentTurnId.HasValue then
                        let liveId = session.CurrentTurnId.Value

                        // The actor thread carries no cancellation token:
                        // the probe still bounds through hardening plus its
                        // checkpoint, and stays quiet when the journal will
                        // not read.
                        let orphan =
                            try
                                probeOrphanTurn liveId CancellationToken.None
                            with _ ->
                                None

                        match orphan with
                        | None -> SessionState.Idle, None, None
                        | Some(settlingId, prime) ->
                            let drainable =
                                try
                                    let pending =
                                        awaitTask (
                                            props.Store.ReadPendingInbox(
                                                props.Tenant,
                                                props.SessionId,
                                                CancellationToken.None
                                            )
                                        )

                                    selectDrainableEntries pending |> List.tryHead
                                with _ ->
                                    None

                            match drainable with
                            | Some entry when crashKnobOf session <> OnCrashResume.FailAttempt ->
                                SessionState.Running, None, Some entry
                            | _ ->
                                failInterruptedTurn (Some settlingId) drainable
                                settleTurnQuiet prime |> ignore
                                SessionState.Idle, None, None
                    else
                        SessionState.Idle, None, None
                | SessionState.Closed -> SessionState.Closed, None, None
                | unknown -> unknown, None, None

        let initialState, initialRebuilt, initialResumeEntry = initialRecovered

        let initialSuspended: SuspendedTurn option =
            match initialState, initialRebuilt with
            | SessionState.WaitingForInput, Some rebuilt ->
                // Crash rebuild: no cursor and no running task; the matching
                // Reply retries from the oldest drainable entry. The
                // timeout is not restarted here: the AskTimeout bound restarts
                // when the retried turn suspends again, so a restarted host
                // never inherits a fired deadline.
                let queueEntry = originalRecoveryEntry ()

                match queueEntry with
                | Some entry ->
                    Some
                        {
                            Entry = entry
                            TurnId =
                                match suspend.Recovery with
                                | null -> currentTurnSnapshot () |> Option.defaultValue Unchecked.defaultof<TurnId>
                                | recovery ->
                                    recovery.Target
                                    |> Option.ofObj
                                    |> Option.map _.TurnId
                                    |> Option.defaultValue Unchecked.defaultof<TurnId>
                            Cursor = None
                            Rebuilt = Some rebuilt
                            Allowed = readGrantsNow ()
                            Attempt = controlPrime |> Option.map _.Attempt |> Option.defaultValue 1
                            TimeoutCts = new CancellationTokenSource()
                        }
                | None -> None
            | _ -> None

        /// Maps a reported result to the Aborted result a won stop settles:
        /// the stop cause wins over whatever the detached turn reported,
        /// even a success, so settlement and stop stay mutually exclusive.
        /// Falls back to the carried result when the cause maps to no
        /// settlement (only abort-family causes reach the pending stop, so
        /// this never fires).
        /// <param name="cause">The stop cause that won.</param>
        /// <param name="reason">Why the turn stopped.</param>
        /// <param name="result">The result the turn reported.</param>
        /// <returns>The result the actor settles.</returns>
        let mapSuspendAborted (cause: StopCause) (reason: string) (result: TurnResult) : TurnResult =
            match StopArbitration.settlementFor cause reason with
            | Some(status, outcome) ->
                { result with
                    Status = status
                    Outcome = outcome
                }
            | None -> result

        /// Builds the Aborted result a won stop settles when the turn left
        /// no result behind (a faulted attempt): zero iterations and usage,
        /// the abort-family outcome carrying who and why. Falls back to a
        /// Failed result when the cause maps to no settlement (only
        /// abort-family causes reach the pending stop, so this never fires).
        /// <param name="cause">The stop cause that won.</param>
        /// <param name="reason">Why the turn stopped.</param>
        /// <returns>The result the actor settles.</returns>
        let abortedSuspendResult (cause: StopCause) (reason: string) : TurnResult =
            match StopArbitration.settlementFor cause reason with
            | Some(status, outcome) ->
                {
                    AssistantText = ""
                    Status = status
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = outcome
                }
            | None ->
                {
                    AssistantText = ""
                    Status = TurnStatus.Failed
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = TurnFailed(reason) :> TurnOutcome
                }

        /// Journals the in-call marker for one turn entering its first
        /// provider call (issue 284): a TurnStartedEvent under the given
        /// journal token through the fenced writer. The caller snapshots
        /// the live token at turn start and passes the snapshot, so a
        /// takeover between snapshot and append still fences out: the store
        /// rejects the stale token. Appended proceeds to the provider call;
        /// a stale-token rejection raises TurnLeaseLostException so the
        /// takeover loser stops before the provider call with zero effects;
        /// a failed write (a persistent store fault past the bounded
        /// retries) returns silently and the turn proceeds unmarked:
        /// best-effort observability, since completion journals nothing and
        /// stays valid without the marker.
        /// <param name="eventStore">The journal the marker appends to.</param>
        /// <param name="tenant">The tenant the session belongs to.</param>
        /// <param name="sessionId">The session whose journal appends.</param>
        /// <param name="token">The journal token snapshot fencing the write.</param>
        /// <param name="turnId">The turn entering its first provider call.</param>
        /// <param name="cancellationToken">Abandons the append.</param>
        let journalTurnStartedAsync
            (eventStore: ISessionEventStore)
            (tenant: TenantId)
            (sessionId: SessionId)
            (token: string)
            (turnId: TurnId)
            (cancellationToken: CancellationToken)
            : Task<unit> =
            task {
                let marker =
                    TurnStartedEvent(sessionId, turnId, Unchecked.defaultof<Nullable<int64>>, DateTimeOffset.UtcNow)
                    :> SessionEvent

                let batch = ResizeArray<SessionEvent>([| marker |]) :> IReadOnlyList<SessionEvent>

                match! JournalWriter.appendWithTokenAsync eventStore tenant sessionId token batch cancellationToken with
                | JournalWriter.JournalAppended _ -> ()
                | JournalWriter.JournalRejected _ -> return raise (TurnLoop.TurnLeaseLostException())
                | JournalWriter.JournalFailed _ -> ()
            }

        /// Reads the failure reason to journal for a Failed result: the
        /// typed outcome reason when present, else the assistant text,
        /// else a fixed fallback. Never synthesizes secrets: both sources
        /// are contract-bound to never carry them.
        /// <param name="result">The Failed result.</param>
        /// <returns>The reason the terminal event carries.</returns>
        let failedReasonOf (result: TurnResult) : string =
            match result.Outcome with
            | :? TurnFailed as failed when not (isNull (box failed)) && not (String.IsNullOrEmpty failed.Reason) ->
                failed.Reason
            | :? TurnAgentRejected as rejected when
                not (isNull (box rejected)) && not (String.IsNullOrEmpty rejected.Reason)
                ->
                rejected.Reason
            | _ when not (String.IsNullOrEmpty result.AssistantText) -> result.AssistantText
            | _ -> "The turn failed."

        /// Builds the Failed result an authority refusal settles: zero
        /// iterations and usage, the typed rejection carrying which branch
        /// refused and why.
        /// <param name="failure">Which authority branch refused the turn.</param>
        /// <param name="reason">Why the turn refused to run. Never contains secrets or tool arguments.</param>
        /// <returns>The result the actor settles.</returns>
        let authorityRefusalResult (failure: AgentAuthorityFailure) (reason: string) : TurnResult =
            {
                AssistantText = ""
                Status = TurnStatus.Failed
                Iterations = 0
                Usage = { InputTokens = 0L; OutputTokens = 0L }
                Outcome = TurnAgentRejected(failure, reason) :> TurnOutcome
            }

        /// Journals an authority refusal as nothing (issue 289): the runner
        /// never ran, so no turn id ever existed for the entry and
        /// borrowing the prime id would misattribute. The entry is
        /// consumed, the session returns Idle, and the absent marker keeps
        /// the orphan trigger quiet. Kept as a named step so the refusal
        /// path reads explicitly.
        /// <param name="reason">Why the turn refused to run. Never contains secrets or tool arguments.</param>
        let journalAuthorityFailure (_reason: string) : unit = ()

        let armTimeout (requestId: string) (timeoutCts: CancellationTokenSource) : unit =
            let delayTask =
                try
                    suspend.Delay.Delay(suspend.AskTimeout, timeoutCts.Token)
                with ex ->
                    Task.FromException(ex)

            delayTask.ContinueWith(fun (elapsed: Task) ->
                if elapsed.Status = TaskStatus.RanToCompletion then
                    suspendSelf.Tell(SuspendTimedOut requestId))
            |> ignore

        let timeoutResult () : TurnResult =
            {
                AssistantText = ""
                Status = TurnStatus.Failed
                Iterations = 0
                Usage = { InputTokens = 0L; OutputTokens = 0L }
                Outcome = TurnFailed(AskTimeoutReason) :> TurnOutcome
            }

        // The pending stop an Abort (or an Interrupt pre-empt) recorded
        // while Running: first cause wins, cleared on every settle. A cell
        // rather than a loop parameter: the actor processes one message
        // fully before the next, so the actor-thread read-modify-write never
        // races, mirroring the base loop's first-cause-wins arbitration.
        let mutable pendingStop: (StopCause * string) option = None

        // The agent rebind a SetAgent recorded while the session was not
        // quiescent (a turn running, or entries queued): applied at the
        // next quiescent boundary, cleared when it lands or the session
        // closes. A crash before apply loses it (in-memory only): the host
        // sees the unchanged AgentId on GetSession and retries.
        let mutable pendingAgent: AgentId option = None

        /// Adopts a fresh journal claim into the mutable cells: the token
        /// every journal write below reads, plus the on-demand compaction
        /// wiring rebuilt for it (or the kept wiring with only its token
        /// refreshed when the host drives Compact directly), so later
        /// Compacts stay live. Called the moment a fresh claim is held, so
        /// an abort below still leaves the cells fencing live writes.
        /// <param name="fresh">The live journal claim to adopt.</param>
        let swapJournal (fresh: TurnClaim) : unit =
            journalToken <- fresh.Token
            controlPrime <- Some fresh

            match suspend.RefreshCompact with
            | Some refresh ->
                try
                    currentCompact <- refresh fresh.Token
                with _ ->
                    currentCompact <-
                        currentCompact
                        |> Option.map (fun wiring ->
                            { wiring with
                                JournalToken = fresh.Token
                            })
            | None ->
                currentCompact <-
                    currentCompact
                    |> Option.map (fun wiring ->
                        { wiring with
                            JournalToken = fresh.Token
                        })

        /// Reads the agent the session converses with now, for the switch
        /// Resolves the captured claim authority one suspendable entry
        /// executes under (issue 363): the per-entry bound claim when the
        /// control target bound it, else the primed claim. Local
        /// correlation only; the store validates it as durable authority.
        /// <param name="entry">The entry the attempt executed.</param>
        /// <returns>The captured claim, or None for unclaimed shells.</returns>
        let settlementClaimFor (entry: InboxEntry) : TurnClaim option =
            match controlReports.TryGetValue entry.Position with
            | true, (_, claim, _) when not (isNull (box claim)) -> Some claim
            | _ ->
                match controlPrime with
                | Some claim when not (isNull (box claim)) -> Some claim
                | _ -> None

        let pipeConfig = LifecyclePipe.resolveConfig props.StorePipe
        let starter = suspendableStarter suspendSelf pipeConfig

        /// Builds the observable snapshot from the loop state: the
        /// lifecycle state, the cached pending inbox count, and the parked
        /// request id. Pure memory, so Abort and GetSnapshot answer while a
        /// store wait is outstanding.
        /// <param name="args">The current loop state.</param>
        /// <returns>The actor's current snapshot.</returns>
        let takeSuspendSnapshot (args: SuspendLoopArgs) : SessionSnapshot =
            let pendingId: string | null =
                match args.Suspended with
                | Some parked ->
                    match parked.Cursor with
                    | Some cursor -> cursor.RequestId
                    | None ->
                        match parked.Rebuilt with
                        | Some rebuilt -> rebuilt.RequestId
                        | None -> null
                | None -> null

            {
                SessionId = props.SessionId
                State = args.State
                PendingCount = args.PendingCount
                RunningPosition = None
                PendingRequestId = pendingId
            }

        /// Validates the session route for one message, piped: the row read
        /// runs through the pipe and the pure checks run on the actor
        /// thread, exactly like the synchronous loop-head check.
        /// <param name="cont">Continues with Choice1 on success or Choice2 with the routing refusal.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withValidateRoute
            (cont: Choice<unit, CompletionRoutingException> -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            startPipedWait
                starter
                (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                "validate-route"
                (fun args2 pipe2 ->
                    function
                    | Error error -> raise error
                    | Ok session ->
                        match session with
                        | null -> raise (SessionNotFoundException(props.SessionId, "The session does not exist."))
                        | s ->
                            try
                                checkRoute s
                                cont (Choice1Of2()) args2 pipe2
                            with :? CompletionRoutingException as error ->
                                cont (Choice2Of2 error) args2 pipe2)
                suspendWith
                args
                pipe

        /// Reads the durable stop for the session, piped. No claim means
        /// no stop, without touching the store.
        /// <param name="cont">Continues with the recorded stop, or None.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withDurableStop
            (cont: (StopCause * string) option -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match controlStore, controlPrime with
            | Some control, Some _ ->
                startPipedWait
                    starter
                    (fun () -> control.ReadAbortTarget(props.Tenant, props.SessionId, CancellationToken.None))
                    "durable-stop"
                    (fun args2 pipe2 ->
                        function
                        | Error error -> raise error
                        | Ok target ->
                            match target with
                            | null -> cont None args2 pipe2
                            | t ->
                                match t.Stop with
                                | null -> cont None args2 pipe2
                                | stop ->
                                    cont
                                        (Some(stop.Cause.Value, stop.Reason |> Option.ofObj |> Option.defaultValue ""))
                                        args2
                                        pipe2)
                    suspendWith
                    args
                    pipe
            | _ -> cont None args pipe

        /// Snapshots the session row's live turn, piped, or None when the
        /// row is missing, the turn settled to null, or the read fails.
        /// <param name="cont">Continues with the live turn id, or None.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withCurrentTurnSnapshot
            (cont: TurnId option -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            startPipedWait
                starter
                (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                "current-turn-snapshot"
                (fun args2 pipe2 ->
                    function
                    | Ok session ->
                        match session with
                        | null -> cont None args2 pipe2
                        | s when s.CurrentTurnId.HasValue -> cont (Some s.CurrentTurnId.Value) args2 pipe2
                        | _ -> cont None args2 pipe2
                    | Error _ -> cont None args2 pipe2)
                suspendWith
                args
                pipe

        /// Reads the session's persisted AllowForSession memory, piped: the
        /// grant tool names the store row carries. A missing row or a null
        /// grant list reads as empty; a missing session reads as empty, and
        /// any other read failure propagates, exactly like before.
        /// <param name="cont">Continues with the granted tool names.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withReadGrants
            (cont: HashSet<string> -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            startPipedWait
                starter
                (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                "read-grants"
                (fun args2 pipe2 ->
                    function
                    | Ok session ->
                        match session with
                        | null -> cont (HashSet<string>()) args2 pipe2
                        | s when isNull (box s.PermissionGrants) -> cont (HashSet<string>()) args2 pipe2
                        | s -> cont (HashSet<string>(s.PermissionGrants :> seq<string>)) args2 pipe2
                    | Error(:? SessionNotFoundException) -> cont (HashSet<string>()) args2 pipe2
                    | Error error -> raise error)
                suspendWith
                args
                pipe

        /// Reads the session's current agent for the switch audit, piped: a
        /// missing row or a read failure reads as the fallback, like before.
        /// <param name="fallback">The agent to report when the row cannot be read.</param>
        /// <param name="cont">Continues with the session's current agent.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withPreviousAgent
            (fallback: AgentId)
            (cont: AgentId -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            startPipedWait
                starter
                (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                "previous-agent"
                (fun args2 pipe2 ->
                    function
                    | Ok session ->
                        match session with
                        | null -> cont fallback args2 pipe2
                        | s -> cont s.AgentId args2 pipe2
                    | Error _ -> cont fallback args2 pipe2)
                suspendWith
                args
                pipe

        /// Checks the per-turn execution authority for a fresh turn start,
        /// piped: re-reads the session's agent from the store. Missing,
        /// disabled, or tenant-mismatched agents refuse without ever
        /// invoking the runner. A null agent catalog, a missing session
        /// row, or a store failure authorizes, exactly like before.
        /// <param name="cont">Continues with the refusal branch and reason, or None when authorized.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withCheckAgentAuthority
            (cont: (AgentAuthorityFailure * string) option -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match suspend.AgentStore with
            | null -> cont None args pipe
            | agentStore ->
                startPipedWait
                    starter
                    (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                    "check-agent-authority/session"
                    (fun args2 pipe2 ->
                        function
                        | Error _ -> cont None args2 pipe2
                        | Ok session ->
                            match session with
                            | null -> cont None args2 pipe2
                            | s ->
                                let agentId = s.AgentId

                                startPipedWait
                                    starter
                                    (fun () -> agentStore.GetAgent(props.Tenant, agentId, CancellationToken.None))
                                    "check-agent-authority/agent"
                                    (fun args3 pipe3 ->
                                        function
                                        | Error _ -> cont None args3 pipe3
                                        | Ok agent ->
                                            match agent with
                                            | null ->
                                                cont
                                                    (Some(
                                                        AgentAuthorityFailure.NotFound,
                                                        sprintf "No agent %O exists in this tenant." agentId
                                                    ))
                                                    args3
                                                    pipe3
                                            | a when not a.Enabled ->
                                                cont
                                                    (Some(
                                                        AgentAuthorityFailure.Disabled,
                                                        sprintf "Agent %O is disabled." agentId
                                                    ))
                                                    args3
                                                    pipe3
                                            | a when not (a.Tenant.Equals(props.Tenant)) ->
                                                cont
                                                    (Some(
                                                        AgentAuthorityFailure.TenantMismatch,
                                                        sprintf "Agent %O belongs to another tenant." agentId
                                                    ))
                                                    args3
                                                    pipe3
                                            | _ -> cont None args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2)
                    suspendWith
                    args
                    pipe

        /// Settles an authority refusal as Failed with the typed outcome,
        /// piped: observes and dispatches the result, consumes the entry and
        /// returns the session to Idle, both best-effort, exactly like
        /// before.
        /// <param name="entry">The turn's inbox entry to consume.</param>
        /// <param name="failure">Which authority branch refused the turn.</param>
        /// <param name="reason">Why the turn refused to run.</param>
        /// <param name="cont">Continues once the refusal settled.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withSettleAuthorityRefusal
            (entry: InboxEntry)
            (failure: AgentAuthorityFailure)
            (reason: string)
            (cont: unit -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            journalAuthorityFailure reason
            let result = authorityRefusalResult failure reason
            notifySettled result
            notifyPosition entry.Position

            let positions = [| entry.Position |] :> IReadOnlyList<int64>

            withDispatchCompletion
                starter
                props
                result
                (fun _ args2 pipe2 ->
                    startPipedWaitUnit
                        starter
                        (fun () ->
                            props.Store.MarkInboxConsumed(
                                props.Tenant,
                                props.SessionId,
                                positions,
                                CancellationToken.None
                            ))
                        "settle-authority-refusal/consume"
                        (fun args3 pipe3 _ ->
                            startPipedWaitUnit
                                starter
                                (fun () ->
                                    props.Store.UpdateSessionState(
                                        props.Tenant,
                                        props.SessionId,
                                        SessionState.Idle,
                                        CancellationToken.None
                                    ))
                                "settle-authority-refusal/idle"
                                (fun args4 pipe4 _ -> cont () args4 pipe4)
                                suspendWith
                                args3
                                pipe3)
                        suspendWith
                        args2
                        pipe2)
                suspendWith
                args
                pipe

        /// Settles the primed journal claim the spawn (or a re-prime)
        /// holds, piped and best-effort: synthesizes the claim from the
        /// row's CurrentTurnId stamp plus the token cell. Every failure is
        /// swallowed, exactly like before.
        /// <param name="cont">Continues once the settle attempt finished.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withSettlePrimed
            (cont: unit -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            startPipedWait
                starter
                (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                "settle-primed/read"
                (fun args2 pipe2 ->
                    function
                    | Ok session ->
                        match session with
                        | null -> cont () args2 pipe2
                        | s when not s.CurrentTurnId.HasValue -> cont () args2 pipe2
                        | s ->
                            let claim =
                                {
                                    TurnId = s.CurrentTurnId.Value
                                    Token = journalToken
                                    Owner = ""
                                    ExpiresAt = DateTimeOffset.MinValue
                                    Attempt = 1
                                }

                            startPipedWait
                                starter
                                (fun () ->
                                    props.Store.SettleTurn(
                                        props.Tenant,
                                        claim,
                                        TurnStatus.Completed,
                                        null,
                                        CancellationToken.None
                                    ))
                                "settle-primed/settle"
                                (fun args3 pipe3 _ -> cont () args3 pipe3)
                                suspendWith
                                args2
                                pipe2
                    | Error _ -> cont () args2 pipe2)
                suspendWith
                args
                pipe

        /// Settles a claim Completed with a null outcome, piped and
        /// best-effort: the first settle wins and a retry of the same
        /// outcome observes it; a rejected or faulted settle reads as
        /// false, exactly like before.
        /// <param name="claim">The claim fencing the settlement. Must not be null.</param>
        /// <param name="cont">Continues with whether the turn settled.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withSettleTurnQuiet
            (claim: TurnClaim)
            (cont: bool -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            startPipedWait
                starter
                (fun () ->
                    props.Store.SettleTurn(props.Tenant, claim, TurnStatus.Completed, null, CancellationToken.None))
                "settle-turn-quiet"
                (fun args2 pipe2 ->
                    function
                    | Ok(:? TurnSettled) -> cont true args2 pipe2
                    | Ok(:? TurnAlreadySettled) -> cont true args2 pipe2
                    | Ok _ -> cont false args2 pipe2
                    | Error _ -> cont false args2 pipe2)
                suspendWith
                args
                pipe

        /// Re-primes the journal through the spawn wiring, piped: a fresh
        /// bootstrap plus ClaimNextTurn, or None when the host never
        /// re-primes, the prime fails, or a live claim is held. The
        /// pending-count cache refreshes exactly when the trailing read
        /// lands.
        /// <param name="cont">Continues with the live claim (or None) plus the refreshed count (or None).</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withReprime
            (cont: TurnClaim option -> int option -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match suspend.ReprimeJournal with
            | None -> cont None (Some args.PendingCount) args pipe
            | Some reprime ->
                startPipedWait
                    starter
                    (fun () -> reprime ())
                    "reprime-journal"
                    (fun args2 pipe2 ->
                        function
                        | Error _ -> cont None None args2 pipe2
                        | Ok fresh ->
                            startPipedWait
                                starter
                                (fun () ->
                                    props.Store.ReadPendingInbox(
                                        props.Tenant,
                                        props.SessionId,
                                        CancellationToken.None
                                    ))
                                "reprime-refresh"
                                (fun args3 pipe3 ->
                                    function
                                    | Ok pending when not (isNull (box pending)) ->
                                        cont fresh (Some pending.Count) args3 pipe3
                                    | Ok _ -> cont fresh (Some 0) args3 pipe3
                                    | Error _ -> cont fresh None args3 pipe3)
                                suspendWith
                                args2
                                pipe2)
                    suspendWith
                    args
                    pipe

        /// Reads whether the session's pending inbox is empty, piped. A
        /// missing row or a store failure reads as non-empty, so a recorded
        /// rebind stays pending rather than applying half-informed, exactly
        /// like before.
        /// <param name="cont">Continues with whether the inbox is empty.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withInboxEmpty
            (cont: bool -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            startPipedWait
                starter
                (fun () -> props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None))
                "inbox-empty"
                (fun args2 pipe2 ->
                    function
                    | Ok pending when not (isNull (box pending)) -> cont (pending.Count = 0) args2 pipe2
                    | Ok _ -> cont true args2 pipe2
                    | Error _ -> cont false args2 pipe2)
                suspendWith
                args
                pipe

        /// Reads the host fence stamp and journals one host batch under
        /// it, piped: a missing row or any failed wait reads as no
        /// outcome, exactly like before.
        /// <param name="batch">The host events to journal.</param>
        /// <param name="cont">Continues with the journal outcome, or None when skipped or failed.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withExpectedStampAndJournal
            (batch: IReadOnlyList<SessionEvent>)
            (cont: JournalWriter.JournalWriteResult option -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            // The host fence reads the version stamp after the re-prime
            // (settle and claim both stamp the row), so the exact
            // equality compares against the current row.
            startPipedWait
                starter
                (fun () -> props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                "apply-agent/stamp"
                (fun args2 pipe2 ->
                    function
                    | Ok session ->
                        match session with
                        | null -> cont None args2 pipe2
                        | s ->
                            startPipedWait
                                starter
                                (fun () ->
                                    JournalWriter.appendHostAsync
                                        suspend.EventStore
                                        props.Tenant
                                        props.SessionId
                                        s.UpdatedAt
                                        batch
                                        CancellationToken.None)
                                "apply-agent/journal"
                                (fun args3 pipe3 ->
                                    function
                                    | Ok outcome -> cont (Some outcome) args3 pipe3
                                    | Error _ -> cont None args3 pipe3)
                                suspendWith
                                args2
                                pipe2
                    | Error _ -> cont None args2 pipe2)
                suspendWith
                args
                pipe

        /// Applies the recorded agent rebind through the 6-step quiescent
        /// protocol, piped: settle the primed claim, re-prime, journal the
        /// switch under the fresh token, settle the fresh claim, rebind the
        /// row, re-prime to restore the live prime. Every write stays
        /// claim-checked last-moment by the stores. A throwing apply reads
        /// as not landed, exactly like before.
        /// <param name="target">The agent the session converses with from now on.</param>
        /// <param name="cont">Continues with whether the rebind landed.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withApplyPendingAgent
            (target: AgentId)
            (cont: bool -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            withSettlePrimed
                (fun () args2 pipe2 ->
                    withReprime
                        (fun fresh count args3 pipe3 ->
                            let args3c =
                                match count with
                                | Some c -> { args3 with PendingCount = c }
                                | None -> args3

                            match fresh with
                            | None -> cont false args3c pipe3
                            | Some live ->
                                swapJournal live

                                withPreviousAgent
                                    target
                                    (fun previous args4 pipe4 ->
                                        let switched =
                                            AgentSwitchedEvent(
                                                props.SessionId,
                                                Unchecked.defaultof<TurnId>,
                                                Unchecked.defaultof<Nullable<int64>>,
                                                DateTimeOffset.UtcNow,
                                                previous,
                                                target
                                            )
                                            :> SessionEvent

                                        let batch =
                                            ResizeArray<SessionEvent>([| switched |]) :> IReadOnlyList<SessionEvent>

                                        withExpectedStampAndJournal
                                            batch
                                            (fun hostOutcome args5 pipe5 ->
                                                match hostOutcome with
                                                | Some(JournalWriter.JournalAppended _) ->
                                                    withSettleTurnQuiet
                                                        live
                                                        (fun settled args6 pipe6 ->
                                                            if not settled then
                                                                cont false args6 pipe6
                                                            else
                                                                startPipedWaitUnit
                                                                    starter
                                                                    (fun () ->
                                                                        props.Store.SetSessionAgent(
                                                                            props.Tenant,
                                                                            props.SessionId,
                                                                            target,
                                                                            CancellationToken.None
                                                                        ))
                                                                    "apply-agent/rebind"
                                                                    (fun args7 pipe7 ->
                                                                        function
                                                                        | Ok() ->
                                                                            withReprime
                                                                                (fun restored count args8 pipe8 ->
                                                                                    let args8c =
                                                                                        match count with
                                                                                        | Some c ->
                                                                                            { args8 with
                                                                                                PendingCount = c
                                                                                            }
                                                                                        | None -> args8

                                                                                    match restored with
                                                                                    | Some fresh2 ->
                                                                                        swapJournal fresh2
                                                                                        cont true args8c pipe8
                                                                                    | None ->
                                                                                        cont false args8c pipe8)
                                                                                suspendWith
                                                                                args7
                                                                                pipe7
                                                                        | Error _ ->
                                                                            withReprime
                                                                                (fun restored count args8 pipe8 ->
                                                                                    let args8c =
                                                                                        match count with
                                                                                        | Some c ->
                                                                                            { args8 with
                                                                                                PendingCount = c
                                                                                            }
                                                                                        | None -> args8

                                                                                    match restored with
                                                                                    | Some fresh2 ->
                                                                                        swapJournal fresh2
                                                                                    | None -> ()

                                                                                    cont false args8c pipe8)
                                                                                suspendWith
                                                                                args7
                                                                                pipe7)
                                                                    suspendWith
                                                                    args6
                                                                    pipe6)
                                                        suspendWith
                                                        args5
                                                        pipe5
                                                | _ ->
                                                    withSettleTurnQuiet
                                                        live
                                                        (fun _ args6 pipe6 -> cont false args6 pipe6)
                                                        suspendWith
                                                        args5
                                                        pipe5)
                                            suspendWith
                                            args4
                                            pipe4)
                                    suspendWith
                                    args3c
                                    pipe3)
                        suspendWith
                        args2
                        pipe2)
                suspendWith
                args
                pipe

        /// Applies the recorded agent rebind when the inbox is empty, piped.
        /// The caller guarantees no live turn task. A throwing apply keeps
        /// the rebind pending for the next boundary, exactly like before.
        /// <param name="cont">Continues once the apply attempt finished.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withTryApplyPendingWhenIdle
            (cont: unit -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match pendingAgent with
            | None -> cont () args pipe
            | Some target ->
                withInboxEmpty
                    (fun empty args2 pipe2 ->
                        if not empty then
                            cont () args2 pipe2
                        else
                            try
                                withApplyPendingAgent
                                    target
                                    (fun applied args3 pipe3 ->
                                        if applied then
                                            pendingAgent <- None

                                        cont () args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2
                            with _ ->
                                cont () args2 pipe2)
                    suspendWith
                    args
                    pipe

        /// Restores the live journal prime when quiescence settled it
        /// (issue 313), piped: the fresh turn re-primes while the inbox is
        /// still empty, adopting the fresh token before appending. Skips
        /// when a live turn is snapshotted, when the inbox is non-empty,
        /// and when the re-prime fails, exactly like before.
        /// <param name="cont">Continues once the prime check finished.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withEnsurePrimed
            (cont: unit -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            withCurrentTurnSnapshot
                (fun snapshot args2 pipe2 ->
                    match snapshot with
                    | Some _ -> cont () args2 pipe2
                    | None ->
                        withInboxEmpty
                            (fun empty args3 pipe3 ->
                                if not empty then
                                    cont () args3 pipe3
                                else
                                    withReprime
                                        (fun fresh count args4 pipe4 ->
                                            let args4c =
                                                match count with
                                                | Some c -> { args4 with PendingCount = c }
                                                | None -> args4

                                            match fresh with
                                            | None -> cont () args4c pipe4
                                            | Some live ->
                                                swapJournal live
                                                cont () args4c pipe4)
                                        suspendWith
                                        args3
                                        pipe3)
                            suspendWith
                            args2
                            pipe2)
                suspendWith
                args
                pipe

        /// Binds one entry's execution to its control target, piped:
        /// the durable TurnId stamped at accept owns execution. A refused
        /// bind or a failed wait reports through onError, exactly like the
        /// synchronous raise did.
        /// <param name="entry">The entry to bind.</param>
        /// <param name="cont">Continues with the bound turn id, or None when unbound.</param>
        /// <param name="onError">Continues when the bind refuses or fails.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withBindControl
            (entry: InboxEntry)
            (cont: TurnId option -> SuspendCont)
            (onError: exn -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match controlStore, controlPrime with
            | Some control, Some claim ->
                // Real-turn identity (issue 374): the durable TurnId stamped
                // at accept owns execution; the in-memory report reuses it so
                // restart and recovery agree with the store. Legacy entries
                // without a stamped identity mint once here.
                let turn =
                    match controlReports.TryGetValue entry.Position with
                    | true, (bound, _, _) -> bound
                    | _ ->
                        if isNull (box entry.TurnId.Value) then
                            TurnId.New()
                        else
                            entry.TurnId

                startPipedWait
                    starter
                    (fun () ->
                        control.BindControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            entry.Position,
                            claim,
                            CancellationToken.None
                        ))
                    "bind-control"
                    (fun args2 pipe2 ->
                        function
                        | Error error -> onError error args2 pipe2
                        | Ok bound ->
                            if bound.Outcome <> ControlOperationOutcome.Applied then
                                onError
                                    (InvalidSessionStateException(
                                        props.SessionId,
                                        "controlPending",
                                        "Current target refused execution admission."
                                    ))
                                    args2
                                    pipe2
                            else
                                controlReports[entry.Position] <- turn, claim, Guid.NewGuid().ToString("N")
                                cont (Some turn) args2 pipe2)
                    suspendWith
                    args
                    pipe
            | _ -> cont None args pipe

        /// Admits one bound turn through its control target, piped: the
        /// actor-thread half of the admission check the runner re-checks on
        /// the turn thread. A refused or failed admission reports through
        /// onError, like the synchronous raise did.
        /// <param name="turn">The bound turn id.</param>
        /// <param name="position">The entry position.</param>
        /// <param name="claim">The claim fencing the check.</param>
        /// <param name="cont">Continues once the turn is admitted.</param>
        /// <param name="onError">Continues when the admission refuses or fails.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withControlAdmitted
            (turn: TurnId)
            (position: int64)
            (claim: TurnClaim)
            (cont: unit -> SuspendCont)
            (onError: exn -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match controlStore with
            | None -> cont () args pipe
            | Some control ->
                startPipedWait
                    starter
                    (fun () ->
                        control.CheckControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            position,
                            claim,
                            CancellationToken.None
                        ))
                    "control-admission"
                    (fun args2 pipe2 ->
                        function
                        | Ok verified when verified.Outcome = ControlOperationOutcome.Applied -> cont () args2 pipe2
                        | Ok _ -> onError (TurnLoop.TurnLeaseLostException()) args2 pipe2
                        | Error error -> onError error args2 pipe2)
                    suspendWith
                    args
                    pipe

        /// Starts one suspendable turn for an entry, piped: binds control,
        /// resolves the loop-run id, fences the Running write, renews the
        /// heartbeat, and invokes the runner fire-and-forget exactly like
        /// before. The runner outcome pipes back through the existing
        /// SuspendableFinished/Faulted path. Start-chain failures report
        /// through onError (the Idle arms answer Status.Failure, like the
        /// synchronous try did); anything else faults, like before.
        /// <param name="entry">The entry to run.</param>
        /// <param name="attempt">The 1-based attempt number.</param>
        /// <param name="allowed">The session AllowForSession memory.</param>
        /// <param name="seed">The crash seed, or None.</param>
        /// <param name="cont">Continues once the runner started.</param>
        /// <param name="onError">Continues when the start chain refuses or fails.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withStartSuspendable
            (entry: InboxEntry)
            (attempt: int)
            (allowed: HashSet<string>)
            (seed: IList<ChatMessage> option)
            (cont: unit -> SuspendCont)
            (onError: exn -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            // Snapshot the live journal token for the turn's in-call
            // marker (issue 284): the marker presents this snapshot, so a
            // takeover between snapshot and append still fences out.
            let markerToken = journalToken

            let startRunner
                (runTurnId: TurnId)
                (args2: SuspendLoopArgs)
                (pipe2: SuspendPipe)
                : Cont<SuspendableActorMessage, unit> =
                let onTurnStarted: TurnLoop.TurnStartedHook option =
                    Some(fun turnId cancellationToken ->
                        journalTurnStartedAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            markerToken
                            turnId
                            cancellationToken)

                let onUsageCheckpoint: TurnLoop.UsageCheckpointHook option =
                    Some(fun inputTokens outputTokens cancellationToken ->
                        SessionJournal.journalUsageAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            markerToken
                            runTurnId
                            inputTokens
                            outputTokens
                            cancellationToken)

                let onSkillLoaded: TurnLoop.SkillLoadedHook option =
                    Some(fun loaded cancellationToken ->
                        SessionJournal.journalSkillLoadedAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            markerToken
                            runTurnId
                            loaded
                            cancellationToken)

                // None when the runner never started (a synchronously throwing
                // or null-returning runner faults before any mint): the fault
                // handler then falls back to the turn cell and the snapshot.
                let mutable faultTurnId: TurnId option = Some runTurnId

                // The admission the actor thread verifies, piped: bound
                // turns check through the contract, unbound turns proceed.
                // The scope below still enters for the turn thread, exactly
                // like before.
                let admitAndRun (argsX: SuspendLoopArgs) (pipeX: SuspendPipe) : Cont<SuspendableActorMessage, unit> =
                    try
                        use _controlScope =
                            match controlReports.TryGetValue entry.Position with
                            | true, (turn, claim, _) ->
                                ControlAdmission.enter (controlAdmission turn entry.Position claim)
                            | _ -> ControlAdmission.enter (fun () -> true)

                        use _leaseScope =
                            match heartbeatView with
                            | Some view -> LeaseAdmission.enter (view.IsValid)
                            | None -> LeaseAdmission.enter (fun () -> true)

                        use _fenceScope =
                            match controlReports.TryGetValue entry.Position with
                            | true, (_, claim, _) when not (isNull (box claim)) -> FencedClaimScope.enter (Some claim)
                            | _ ->
                                match controlPrime with
                                | Some claim when not (isNull (box claim)) -> FencedClaimScope.enter (Some claim)
                                | _ -> FencedClaimScope.enter None

                        if not (LeaseAdmission.check ()) then
                            raise (TurnLoop.TurnLeaseLostException())

                        let runTask =
                            try
                                let started =
                                    suspend.RunSuspendable
                                        entry
                                        attempt
                                        allowed
                                        None
                                        None
                                        seed
                                        CancellationToken.None
                                        onTurnStarted
                                        onUsageCheckpoint
                                        onSkillLoaded
                                        runTurnId

                                if isNull (box started) then
                                    faultTurnId <- None

                                    Task.FromException<TurnLoop.TurnLoopCompletion>(
                                        InvalidOperationException("The suspendable turn runner returned null.")
                                    )
                                else
                                    started
                            with ex ->
                                faultTurnId <- None
                                Task.FromException<TurnLoop.TurnLoopCompletion>(ex)

                        runTask.ContinueWith(fun (completed: Task<TurnLoop.TurnLoopCompletion>) ->
                            if completed.IsCanceled then
                                suspendSelf.Tell(
                                    SuspendableFaulted(
                                        entry,
                                        OperationCanceledException("The suspendable turn was aborted."),
                                        attempt,
                                        faultTurnId
                                    )
                                )
                            elif completed.IsFaulted then
                                let error =
                                    match completed.Exception with
                                    | null ->
                                        InvalidOperationException("The suspendable turn faulted without an exception.")
                                        :> Exception
                                    | aggregate -> aggregate.GetBaseException()

                                suspendSelf.Tell(SuspendableFaulted(entry, error, attempt, faultTurnId))
                            else
                                suspendSelf.Tell(SuspendableFinished(entry, completed.Result, attempt, allowed)))
                        |> ignore

                        cont () argsX pipeX
                    with ex ->
                        onError ex argsX pipeX

                match controlReports.TryGetValue entry.Position with
                | true, (turn, claim, _) ->
                    withControlAdmitted
                        turn
                        entry.Position
                        claim
                        (fun () args3 pipe3 -> admitAndRun args3 pipe3)
                        onError
                        suspendWith
                        args2
                        pipe2
                | _ -> admitAndRun args2 pipe2

            let invokeRunner
                (runTurnId: TurnId)
                (args2: SuspendLoopArgs)
                (pipe2: SuspendPipe)
                : Cont<SuspendableActorMessage, unit> =
                // Execution-owned Running (issue 377): fenced under the bound
                // claim when one exists, so a takeover between verification
                // and the write rejects with zero effects. Unclaimed shells
                // keep the unfenced update. Runs piped; failures report
                // through onError.
                let proceed (argsX: SuspendLoopArgs) (pipeX: SuspendPipe) : Cont<SuspendableActorMessage, unit> =
                    runningTurnId <- Some runTurnId

                    // Production heartbeat (issue 375): renew the bound prime
                    // in place for the whole attempt.
                    match controlReports.TryGetValue entry.Position with
                    | true, (_, boundClaim, _) when not (isNull (box boundClaim)) ->
                        startHeartbeat entry attempt runTurnId boundClaim
                    | _ ->
                        match controlPrime with
                        | Some prime when not (isNull (box prime)) -> startHeartbeat entry attempt runTurnId prime
                        | _ -> ()

                    startRunner runTurnId argsX pipeX

                let startClaim =
                    match controlReports.TryGetValue entry.Position with
                    | true, (_, claim, _) when not (isNull (box claim)) -> Some claim
                    | _ ->
                        match controlPrime with
                        | Some claim when not (isNull (box claim)) -> Some claim
                        | _ -> None

                match startClaim with
                | Some claim ->
                    startPipedWait
                        starter
                        (fun () ->
                            ClaimFence.updateSessionStateAsync
                                props.Store
                                props.Tenant
                                claim
                                props.SessionId
                                SessionState.Running
                                CancellationToken.None)
                        "start-running-fenced"
                        (fun args3 pipe3 ->
                            function
                            | Ok true -> proceed args3 pipe3
                            | Ok false -> onError (TurnLoop.TurnLeaseLostException()) args3 pipe3
                            | Error error -> onError error args3 pipe3)
                        suspendWith
                        args2
                        pipe2
                | None ->
                    startPipedWaitUnit
                        starter
                        (fun () ->
                            props.Store.UpdateSessionState(
                                props.Tenant,
                                props.SessionId,
                                SessionState.Running,
                                CancellationToken.None
                            ))
                        "start-running"
                        (fun args3 pipe3 ->
                            function
                            | Ok() -> proceed args3 pipe3
                            | Error error -> onError error args3 pipe3)
                        suspendWith
                        args2
                        pipe2

            withBindControl
                entry
                (fun boundTurn args2 pipe2 ->
                    withCurrentTurnSnapshot
                        (fun snapshot args3 pipe3 ->
                            let runTurnId =
                                match boundTurn |> Option.orElse snapshot with
                                | Some live -> live
                                | None -> TurnId.New()

                            invokeRunner runTurnId args3 pipe3)
                        suspendWith
                        args2
                        pipe2)
                onError
                suspendWith
                args
                pipe

        /// Resumes one parked turn for a reply, piped: admits the resume
        /// through its control target, then invokes the runner
        /// fire-and-forget exactly like before. The runner outcome pipes
        /// back through the existing SuspendableFinished/Faulted path.
        /// <param name="parked">The parked turn to resume.</param>
        /// <param name="reply">The reply to resume with.</param>
        /// <param name="nextAttempt">The 1-based attempt the resume runs as.</param>
        /// <param name="cont">Continues once the runner started.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withResumeSuspendable
            (parked: SuspendedTurn)
            (reply: Reply)
            (nextAttempt: int)
            (cont: unit -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            let runResumed (argsX: SuspendLoopArgs) (pipeX: SuspendPipe) : Cont<SuspendableActorMessage, unit> =
                // Suspended renewal continues under the same claim/view (issue
                // 375): no new heartbeat, no reply consumption by the renewal
                // itself. The ambient lease scope below carries the live view
                // into the resumed runner, so a lost view faults before any
                // provider call and stale execution never authorizes.
                use _controlScope =
                    match controlReports.TryGetValue parked.Entry.Position with
                    | true, (turn, claim, _) ->
                        ControlAdmission.enter (controlAdmission turn parked.Entry.Position claim)
                    | _ -> ControlAdmission.enter (fun () -> true)

                use _leaseScope =
                    match heartbeatView with
                    | Some view -> LeaseAdmission.enter (view.IsValid)
                    | None -> LeaseAdmission.enter (fun () -> true)

                // Fenced post-resume Inject consumption (issue 377): the
                // parked claim rides the scope into the resumed runner's fold
                // hooks.
                use _fenceScope =
                    match controlReports.TryGetValue parked.Entry.Position with
                    | true, (_, claim, _) when not (isNull (box claim)) -> FencedClaimScope.enter (Some claim)
                    | _ ->
                        match controlPrime with
                        | Some claim when not (isNull (box claim)) -> FencedClaimScope.enter (Some claim)
                        | _ -> FencedClaimScope.enter None

                let cursor =
                    match parked.Cursor with
                    | Some live -> Some live
                    | None -> None

                // Resumes continue the parked turn id (issue 289): the origin
                // id the suspend carried, so the settled completion keys on
                // the same id the marker journaled.
                runningTurnId <- Some parked.TurnId

                // Resumes already marked before they suspended (no marker), but
                // post-resume iteration boundaries and settles still checkpoint
                // usage and post-resume skill loads still journal (issue 321),
                // fenced under the live token so a takeover loser journals
                // nothing.
                let resumeToken = journalToken

                let onUsageResumed: TurnLoop.UsageCheckpointHook option =
                    Some(fun inputTokens outputTokens cancellationToken ->
                        SessionJournal.journalUsageAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            resumeToken
                            parked.TurnId
                            inputTokens
                            outputTokens
                            cancellationToken)

                let onSkillResumed: TurnLoop.SkillLoadedHook option =
                    Some(fun loaded cancellationToken ->
                        SessionJournal.journalSkillLoadedAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            resumeToken
                            parked.TurnId
                            loaded
                            cancellationToken)

                let mutable faultTurnId: TurnId option = Some parked.TurnId

                let runTask =
                    try
                        if not (LeaseAdmission.check ()) then
                            raise (TurnLoop.TurnLeaseLostException())

                        let started =
                            suspend.RunSuspendable
                                parked.Entry
                                nextAttempt
                                parked.Allowed
                                cursor
                                (Some reply)
                                None
                                CancellationToken.None
                                // A resumed turn already marked before it
                                // suspended: no marker on resume.
                                None
                                onUsageResumed
                                onSkillResumed
                                parked.TurnId

                        if isNull (box started) then
                            faultTurnId <- None

                            Task.FromException<TurnLoop.TurnLoopCompletion>(
                                InvalidOperationException("The suspendable turn runner returned null.")
                            )
                        else
                            started
                    with ex ->
                        faultTurnId <- None
                        Task.FromException<TurnLoop.TurnLoopCompletion>(ex)

                runTask.ContinueWith(fun (completed: Task<TurnLoop.TurnLoopCompletion>) ->
                    if completed.IsCanceled then
                        suspendSelf.Tell(
                            SuspendableFaulted(
                                parked.Entry,
                                OperationCanceledException("The resumed turn was aborted."),
                                nextAttempt,
                                faultTurnId
                            )
                        )
                    elif completed.IsFaulted then
                        let error =
                            match completed.Exception with
                            | null ->
                                InvalidOperationException("The resumed turn faulted without an exception.")
                                :> Exception
                            | aggregate -> aggregate.GetBaseException()

                        suspendSelf.Tell(SuspendableFaulted(parked.Entry, error, nextAttempt, faultTurnId))
                    else
                        suspendSelf.Tell(
                            SuspendableFinished(parked.Entry, completed.Result, nextAttempt, parked.Allowed)
                        ))
                |> ignore

                cont () argsX pipeX

            match controlReports.TryGetValue parked.Entry.Position, controlStore with
            | (true, (turn, claim, _)), Some control ->
                startPipedWait
                    starter
                    (fun () ->
                        control.CheckControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            parked.Entry.Position,
                            claim,
                            CancellationToken.None
                        ))
                    "resume-admission"
                    (fun args2 pipe2 ->
                        function
                        | Ok verified when verified.Outcome = ControlOperationOutcome.Applied -> runResumed args2 pipe2
                        | Ok _ ->
                            raise (
                                InvalidSessionStateException(
                                    props.SessionId,
                                    "controlPending",
                                    "Durable control forbids resume."
                                )
                            )
                        | Error error -> raise error)
                    suspendWith
                    args
                    pipe
            | _ -> runResumed args pipe

        /// Drains the next fresh turn after an authority refusal, piped:
        /// applies a recorded rebind when quiescent, then gates the oldest
        /// drainable entry. Authorized entries start (the session returns to
        /// Running); refused entries settle Failed and the drain recurses;
        /// an empty inbox returns the session to Idle.
        /// <param name="cont">Continues with the next loop state.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let rec withDrainAfterRefusal
            (cont: SessionState -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            withValidateRoute
                (fun choice args2 pipe2 ->
                    match choice with
                    | Choice2Of2 error -> raise error
                    | Choice1Of2() ->
                        withTryApplyPendingWhenIdle
                            (fun () args3 pipe3 ->
                                startPipedWait
                                    starter
                                    (fun () ->
                                        props.Store.ReadPendingInbox(
                                            props.Tenant,
                                            props.SessionId,
                                            CancellationToken.None
                                        ))
                                    "drain-after-refusal/read"
                                    (fun args4 pipe4 ->
                                        function
                                        | Error error -> raise error
                                        | Ok pending ->
                                            let args4c =
                                                { args4 with
                                                    PendingCount = if isNull (box pending) then 0 else pending.Count
                                                }

                                            match selectDrainableEntries pending with
                                            | following :: _ ->
                                                withCheckAgentAuthority
                                                    (fun authority args5 pipe5 ->
                                                        match authority with
                                                        | None when args5.Closing <> [] ->
                                                            // A requested close owns the session now:
                                                            // the entry stays durable, but no turn
                                                            // starts into the close.
                                                            cont SessionState.Idle args5 pipe5
                                                        | None ->
                                                            startPipedWaitUnit
                                                                starter
                                                                (fun () ->
                                                                    props.Store.UpdateSessionState(
                                                                        props.Tenant,
                                                                        props.SessionId,
                                                                        SessionState.Running,
                                                                        CancellationToken.None
                                                                    ))
                                                                "drain-after-refusal/running"
                                                                (fun args6 pipe6 ->
                                                                    function
                                                                    | Error error -> raise error
                                                                    | Ok() ->
                                                                        withReadGrants
                                                                            (fun grants args7 pipe7 ->
                                                                                withStartSuspendable
                                                                                    following
                                                                                    1
                                                                                    grants
                                                                                    None
                                                                                    (fun () args8 pipe8 ->
                                                                                        cont
                                                                                            SessionState.Running
                                                                                            args8
                                                                                            pipe8)
                                                                                    (fun error _ _ -> raise error)
                                                                                    suspendWith
                                                                                    args7
                                                                                    pipe7)
                                                                            suspendWith
                                                                            args6
                                                                            pipe6)
                                                                suspendWith
                                                                args5
                                                                pipe5
                                                        | Some(failure, reason) ->
                                                            withSettleAuthorityRefusal
                                                                following
                                                                failure
                                                                reason
                                                                (fun () args6 pipe6 ->
                                                                    withDrainAfterRefusal
                                                                        cont
                                                                        suspendWith
                                                                        args6
                                                                        pipe6)
                                                                suspendWith
                                                                args5
                                                                pipe5)
                                                    suspendWith
                                                    args4c
                                                    pipe4
                                            | [] ->
                                                startPipedWaitUnit
                                                    starter
                                                    (fun () ->
                                                        props.Store.UpdateSessionState(
                                                            props.Tenant,
                                                            props.SessionId,
                                                            SessionState.Idle,
                                                            CancellationToken.None
                                                        ))
                                                    "drain-after-refusal/idle"
                                                    (fun args5 pipe5 ->
                                                        function
                                                        | Ok() -> cont SessionState.Idle args5 pipe5
                                                        | Error error -> raise error)
                                                    suspendWith
                                                    args4c
                                                    pipe4)
                                    suspendWith
                                    args3
                                    pipe3)
                            suspendWith
                            args2
                            pipe2)
                suspendWith
                args
                pipe

        /// Journals one suspension event under the current attribution,
        /// piped: a missing control target raises, exactly like before.
        /// The caller branches the result instead of resuming on a write
        /// that never landed.
        /// <param name="suspension">The suspension to journal.</param>
        /// <param name="cont">Continues with the journal outcome.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withJournalSuspend
            (suspension: TurnLoop.TurnLoopSuspension)
            (cont: JournalWriter.JournalWriteResult -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            let appendFor (turnId: TurnId) : SuspendCont =
                fun argsX pipeX ->
                    let stamp = DateTimeOffset.UtcNow

                    let event =
                        match suspension.Kind with
                        | TurnLoop.PermissionSuspension ->
                            PermissionRequestedEvent(
                                props.SessionId,
                                turnId,
                                Nullable<int64>(),
                                stamp,
                                suspension.RequestId,
                                suspension.ToolName
                            )
                            :> SessionEvent
                        | TurnLoop.QuestionSuspension ->
                            QuestionAskedEvent(
                                props.SessionId,
                                turnId,
                                Nullable<int64>(),
                                stamp,
                                suspension.RequestId,
                                suspension.QuestionText
                            )
                            :> SessionEvent

                    let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

                    // Through the journal writer: sanitized, bounded, fenced on the
                    // journal token with bounded retries. The caller branches the
                    // result: Appended parks the turn, Rejected/Failed settle it
                    // Failed with the typed reason instead.
                    startPipedWait
                        starter
                        (fun () ->
                            JournalWriter.appendWithTokenAsync
                                suspend.EventStore
                                props.Tenant
                                props.SessionId
                                journalToken
                                events
                                CancellationToken.None)
                        "journal-suspend"
                        (fun args2 pipe2 ->
                            function
                            | Ok outcome -> cont outcome args2 pipe2
                            | Error error -> raise error)
                        suspendWith
                        argsX
                        pipeX

            match controlStore, controlPrime with
            | Some control, Some _ ->
                startPipedWait
                    starter
                    (fun () -> control.ReadAbortTarget(props.Tenant, props.SessionId, CancellationToken.None))
                    "journal-suspend/target"
                    (fun args2 pipe2 ->
                        function
                        | Error error -> raise error
                        | Ok target ->
                            match target with
                            | null ->
                                raise (
                                    InvalidSessionStateException(
                                        props.SessionId,
                                        "missingControlTarget",
                                        "Suspension requires current attribution."
                                    )
                                )
                            | t -> appendFor t.TurnId args2 pipe2)
                    suspendWith
                    args
                    pipe
            | _ -> appendFor (TurnId.New()) args pipe

        /// Journals one reply resolution event under the current
        /// attribution, piped: a missing control target raises, exactly
        /// like before. Replies with no journal shape resolve on an empty
        /// applied result without touching the store.
        /// <param name="reply">The reply to journal.</param>
        /// <param name="cont">Continues with the journal outcome.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withJournalResolve
            (reply: Reply)
            (cont: JournalWriter.JournalWriteResult -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            let appendFor (turnId: TurnId) : SuspendCont =
                fun argsX pipeX ->
                    let stamp = DateTimeOffset.UtcNow

                    let eventOpt: SessionEvent option =
                        match reply with
                        | :? PermissionDecision as decision when not (isNull (box decision)) ->
                            PermissionResolvedEvent(
                                props.SessionId,
                                turnId,
                                Nullable<int64>(),
                                stamp,
                                decision.RequestId,
                                decision.Decision
                            )
                            :> SessionEvent
                            |> Some
                        | :? QuestionAnswer as answer when not (isNull (box answer)) ->
                            QuestionAnsweredEvent(
                                props.SessionId,
                                turnId,
                                Nullable<int64>(),
                                stamp,
                                answer.QuestionId,
                                answer.Answer
                            )
                            :> SessionEvent
                            |> Some
                        | _ -> None

                    match eventOpt with
                    | None ->
                        // No journal shape for this reply kind: nothing to
                        // append, so the resume proceeds on an empty applied
                        // result.
                        cont
                            (JournalWriter.JournalAppended(ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>))
                            argsX
                            pipeX
                    | Some event ->
                        let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

                        startPipedWait
                            starter
                            (fun () ->
                                JournalWriter.appendWithTokenAsync
                                    suspend.EventStore
                                    props.Tenant
                                    props.SessionId
                                    journalToken
                                    events
                                    CancellationToken.None)
                            "journal-resolve"
                            (fun args2 pipe2 ->
                                function
                                | Ok outcome -> cont outcome args2 pipe2
                                | Error error -> raise error)
                            suspendWith
                            argsX
                            pipeX

            match controlStore, controlPrime with
            | Some control, Some _ ->
                startPipedWait
                    starter
                    (fun () -> control.ReadAbortTarget(props.Tenant, props.SessionId, CancellationToken.None))
                    "journal-resolve/target"
                    (fun args2 pipe2 ->
                        function
                        | Error error -> raise error
                        | Ok target ->
                            match target with
                            | null ->
                                raise (
                                    InvalidSessionStateException(
                                        props.SessionId,
                                        "missingControlTarget",
                                        "Reply resolution requires current attribution."
                                    )
                                )
                            | t -> appendFor t.TurnId args2 pipe2)
                    suspendWith
                    args
                    pipe
            | _ -> appendFor (TurnId.New()) args pipe

        /// Journals the terminal completion event for one settled turn,
        /// piped and best-effort (issue 289): verdict-first, so a rejected
        /// or failed write carries no further turn to fail. Failures are
        /// swallowed, exactly like before.
        /// <param name="turnId">The settling turn's id.</param>
        /// <param name="result">The settled (possibly abort-mapped) result.</param>
        /// <param name="cont">Continues once the journal attempt finished.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withJournalSettledCompletion
            (turnId: TurnId)
            (result: TurnResult)
            (cont: unit -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            let stamp = DateTimeOffset.UtcNow

            let eventOpt: SessionEvent option =
                match result.Status with
                | TurnStatus.Completed ->
                    TurnCompletedEvent(props.SessionId, turnId, Nullable<int64>(), stamp) :> SessionEvent
                    |> Some
                | TurnStatus.Aborted ->
                    match result.Outcome with
                    | :? TurnAborted as aborted when not (isNull (box aborted)) ->
                        TurnAbortedEvent(
                            props.SessionId,
                            turnId,
                            Nullable<int64>(),
                            stamp,
                            aborted.Cause,
                            aborted.Reason
                        )
                        :> SessionEvent
                        |> Some
                    | _ ->
                        TurnAbortedEvent(
                            props.SessionId,
                            turnId,
                            Nullable<int64>(),
                            stamp,
                            StopCause.ExplicitAbort,
                            InterruptReason
                        )
                        :> SessionEvent
                        |> Some
                | TurnStatus.Failed ->
                    TurnFailedEvent(props.SessionId, turnId, Nullable<int64>(), stamp, failedReasonOf result)
                    :> SessionEvent
                    |> Some
                | _ -> None

            match eventOpt with
            | None -> cont () args pipe
            | Some event ->
                let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

                startPipedWait
                    starter
                    (fun () ->
                        JournalWriter.appendWithTokenAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            journalToken
                            events
                            CancellationToken.None)
                    "journal-settled-completion"
                    (fun args2 pipe2 _ -> cont () args2 pipe2)
                    suspendWith
                    args
                    pipe

        /// Journals the AskTimeout terminal event, piped and best-effort:
        /// the turn already settles Failed, so a rejected or failed write
        /// carries no further turn to fail. A faulted write propagates,
        /// exactly like before.
        /// <param name="turnId">The parked turn's id.</param>
        /// <param name="cont">Continues once the journal attempt finished.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withJournalTimeout
            (turnId: TurnId)
            (cont: unit -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            // The parked turn id (issue 289): the default id (a cursor
            // that never carried one) journals nothing, while the settle
            // effects below run unchanged.
            if turnId.Equals(Unchecked.defaultof<TurnId>) then
                cont () args pipe
            else
                let stamp = DateTimeOffset.UtcNow

                let event =
                    TurnFailedEvent(props.SessionId, turnId, Nullable<int64>(), stamp, AskTimeoutReason) :> SessionEvent

                let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

                startPipedWait
                    starter
                    (fun () ->
                        JournalWriter.appendWithTokenAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            journalToken
                            events
                            CancellationToken.None)
                    "journal-timeout"
                    (fun args2 pipe2 ->
                        function
                        | Ok _ -> cont () args2 pipe2
                        | Error error -> raise error)
                    suspendWith
                    args
                    pipe

        /// Decides one report through the bound control target, piped:
        /// validates the captured claim authority and maps the decided
        /// terminal result. A duplicate report or a refused decision raises,
        /// exactly like before.
        /// <param name="entry">The entry the attempt executed.</param>
        /// <param name="candidate">The result the turn reported.</param>
        /// <param name="cont">Continues with the decided result.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withDecideControl
            (entry: InboxEntry)
            (candidate: TurnResult)
            (cont: TurnResult -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match controlStore, controlReports.TryGetValue entry.Position with
            | Some control, (true, (turn, claim, id)) ->
                if completedControlReports.Contains id then
                    raise (
                        InvalidSessionStateException(
                            props.SessionId,
                            "duplicateControlReport",
                            "A duplicate report cannot publish or drain."
                        )
                    )

                let cause, reason =
                    match candidate.Outcome with
                    | :? TurnAborted as stop -> Nullable stop.Cause, (stop.Reason: string | null)
                    | _ when candidate.Status = TurnStatus.Aborted ->
                        Nullable StopCause.ExplicitAbort, (InterruptReason: string | null)
                    | _ -> Nullable(), null

                startPipedWait
                    starter
                    (fun () ->
                        control.TryDecideControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            entry.Position,
                            claim,
                            id,
                            candidate.Status,
                            cause,
                            reason,
                            CancellationToken.None
                        ))
                    "decide-control"
                    (fun args2 pipe2 ->
                        function
                        | Error error -> raise error
                        | Ok decided ->
                            match decided.Outcome, decided.Decision with
                            | ControlOperationOutcome.Applied, evidence ->
                                match evidence with
                                | null ->
                                    raise (InvalidOperationException("Applied control decision has no evidence."))
                                | evidence when evidence.Status = TurnStatus.Aborted ->
                                    cont
                                        { candidate with
                                            Status = TurnStatus.Aborted
                                            Outcome =
                                                TurnAborted(
                                                    evidence.Cause.Value,
                                                    evidence.Reason |> Option.ofObj |> Option.defaultValue ""
                                                )
                                                :> TurnOutcome
                                        }
                                        args2
                                        pipe2
                                | evidence ->
                                    cont
                                        { candidate with
                                            Status = evidence.Status
                                        }
                                        args2
                                        pipe2
                            | _ ->
                                raise (
                                    InvalidSessionStateException(
                                        props.SessionId,
                                        "controlPending",
                                        "Control decision refused this report; no downstream effects are authorized."
                                    )
                                ))
                    suspendWith
                    args
                    pipe
            | _ -> cont candidate args pipe

        /// Retires one bound control target, piped: a refused retirement
        /// raises, exactly like before.
        /// <param name="entry">The entry the attempt executed.</param>
        /// <param name="cont">Continues once the target retired.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withRetireControl
            (entry: InboxEntry)
            (cont: unit -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match controlStore, controlReports.TryGetValue entry.Position with
            | Some control, (true, (turn, claim, id)) ->
                startPipedWait
                    starter
                    (fun () ->
                        control.RetireControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            entry.Position,
                            claim,
                            id,
                            CancellationToken.None
                        ))
                    "retire-control"
                    (fun args2 pipe2 ->
                        function
                        | Error error -> raise error
                        | Ok retired ->
                            if retired.Outcome <> ControlOperationOutcome.Applied then
                                raise (
                                    InvalidSessionStateException(
                                        props.SessionId,
                                        "controlPending",
                                        "Control retirement refused further settlement or queue drain."
                                    )
                                )

                            completedControlReports.Add id |> ignore
                            cont () args2 pipe2)
                    suspendWith
                    args
                    pipe
            | _ -> cont () args pipe

        /// Settles a turn whose suspend/resolve journal write never landed
        /// as Failed with the typed reason, piped: consumes the entry and
        /// returns the session to Idle with the Failed result observed,
        /// mirroring the AskTimeout settle.
        /// <param name="entry">The turn's inbox entry to consume.</param>
        /// <param name="reason">Why the turn failed.</param>
        /// <param name="cont">Continues once the failure settled.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withSettleJournalFailure
            (entry: InboxEntry)
            (reason: string)
            (cont: unit -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            let result =
                {
                    AssistantText = ""
                    Status = TurnStatus.Failed
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = TurnFailed(reason) :> TurnOutcome
                }

            let positions = [| entry.Position |] :> IReadOnlyList<int64>

            withDecideControl
                entry
                result
                (fun decided args2 pipe2 ->
                    startPipedWaitUnit
                        starter
                        (fun () ->
                            props.Store.MarkInboxConsumed(
                                props.Tenant,
                                props.SessionId,
                                positions,
                                CancellationToken.None
                            ))
                        "settle-journal-failure/consume"
                        (fun args3 pipe3 ->
                            function
                            | Ok() ->
                                let args3c =
                                    { args3 with
                                        PendingCount = max 0 (args3.PendingCount - 1)
                                    }

                                notifySettled decided
                                notifyPosition entry.Position

                                withDispatchCompletion
                                    starter
                                    props
                                    decided
                                    (fun _ args4 pipe4 ->
                                        withRetireControl
                                            entry
                                            (fun () args5 pipe5 ->
                                                startPipedWaitUnit
                                                    starter
                                                    (fun () ->
                                                        props.Store.UpdateSessionState(
                                                            props.Tenant,
                                                            props.SessionId,
                                                            SessionState.Idle,
                                                            CancellationToken.None
                                                        ))
                                                    "settle-journal-failure/idle"
                                                    (fun args6 pipe6 ->
                                                        function
                                                        | Ok() -> cont () args6 pipe6
                                                        | Error error -> raise error)
                                                    suspendWith
                                                    args5
                                                    pipe5)
                                            suspendWith
                                            args4
                                            pipe4)
                                    suspendWith
                                    args3c
                                    pipe3
                            | Error error -> raise error)
                        suspendWith
                        args2
                        pipe2)
                suspendWith
                args
                pipe

        /// Settles the primed journal claim at quiescence for a Completed
        /// turn, piped and best-effort (issue 313): every failure reads as
        /// unsettled, exactly like before.
        /// <param name="cont">Continues once the settle attempt finished.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withSettleCompletedPrime
            (cont: unit -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            withCurrentTurnSnapshot
                (fun snapshot args2 pipe2 ->
                    match snapshot with
                    | None -> cont () args2 pipe2
                    | Some turnId ->
                        let claim =
                            {
                                TurnId = turnId
                                Token = journalToken
                                Owner = ""
                                ExpiresAt = DateTimeOffset.MinValue
                                Attempt = 1
                            }

                        startPipedWait
                            starter
                            (fun () ->
                                ClaimFence.settleTurnAsync
                                    props.Store
                                    props.Tenant
                                    claim
                                    TurnStatus.Completed
                                    null
                                    CancellationToken.None)
                            "settle-completed-prime"
                            (fun args3 pipe3 _ -> cont () args3 pipe3)
                            suspendWith
                            args2
                            pipe2)
                suspendWith
                args
                pipe

        /// Resolves the settling turn id, piped (issue 289): the
        /// completion-carried id, then the running attempt's cell, then the
        /// CurrentTurnId snapshot, else None (journal nothing).
        /// <param name="carried">The completion-carried turn id.</param>
        /// <param name="cont">Continues with the settling turn id, or None.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withSettlingTurnId
            (carried: TurnId)
            (cont: TurnId option -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            if not (carried.Equals(Unchecked.defaultof<TurnId>)) then
                cont (Some carried) args pipe
            else
                match runningTurnId with
                | Some _ as resolved -> cont resolved args pipe
                | None ->
                    withCurrentTurnSnapshot
                        (fun snapshot args2 pipe2 -> cont snapshot args2 pipe2)
                        suspendWith
                        args
                        pipe

        /// Drains the committed settlement's authoritative following entry,
        /// piped (issue 363): the provider-selected runnable candidate,
        /// never an actor-computed inbox snapshot. Authorized entries
        /// start; refused ones settle through the existing refusal path and
        /// the drain recurses.
        /// <param name="following">The authoritative following entry. Never null.</param>
        /// <param name="cont">Continues with the next loop state.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withDrainSettledFollowing
            (following: InboxEntry)
            (cont: SessionState -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            withTryApplyPendingWhenIdle
                (fun () args2 pipe2 ->
                    withCheckAgentAuthority
                        (fun authority args3 pipe3 ->
                            match authority with
                            | None when args3.Closing <> [] ->
                                // A requested close owns the session now:
                                // the entry stays durable, but no turn
                                // starts into the close.
                                cont SessionState.Idle args3 pipe3
                            | None ->
                                withReadGrants
                                    (fun grants args4 pipe4 ->
                                        withStartSuspendable
                                            following
                                            1
                                            grants
                                            None
                                            (fun () args5 pipe5 -> cont SessionState.Running args5 pipe5)
                                            (fun error _ _ -> raise error)
                                            suspendWith
                                            args4
                                            pipe4)
                                    suspendWith
                                    args3
                                    pipe3
                            | Some(failure, reason) ->
                                withSettleAuthorityRefusal
                                    following
                                    failure
                                    reason
                                    (fun () args4 pipe4 -> withDrainAfterRefusal cont suspendWith args4 pipe4)
                                    suspendWith
                                    args3
                                    pipe3)
                        suspendWith
                        args2
                        pipe2)
                suspendWith
                args
                pipe

        /// Settles one suspendable attempt through the atomic capability,
        /// piped (issue 363): validates the captured claim authority and
        /// commits under the same takeover-serializing boundary. Returns
        /// None when no capability or claim is available, or when the call
        /// itself faults, exactly like before. A Rejected outcome is Some,
        /// never None.
        /// <param name="entry">The entry the attempt executed.</param>
        /// <param name="result">The decided terminal result.</param>
        /// <param name="executionId">The settling turn id, or None when no loop id ever existed.</param>
        /// <param name="cont">Continues with the committed disposition, or None when unsettleable here.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withTrySettleSuspendable
            (entry: InboxEntry)
            (result: TurnResult)
            (executionId: TurnId option)
            (cont: SessionSettlementOutcome option -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match settlementStore, settlementClaimFor entry with
            | Some capable, Some claim ->
                startPipedWaitUnit
                    starter
                    (fun () ->
                        capable.AdmitExecution(
                            props.Tenant,
                            props.SessionId,
                            entry.Position,
                            claim,
                            CancellationToken.None
                        ))
                    "settle/admit"
                    (fun args2 pipe2 _ ->

                        let execution =
                            match executionId with
                            | Some id -> Nullable id
                            | None -> Nullable()

                        let request =
                            SessionSettlementRequest(
                                props.SessionId,
                                entry.Position,
                                claim,
                                execution,
                                result,
                                mintCompletionKey (),
                                null
                            )

                        startPipedWait
                            starter
                            (fun () -> capable.SettleExecution(props.Tenant, request, CancellationToken.None))
                            "settle/execution"
                            (fun args3 pipe3 ->
                                function
                                | Ok outcome -> cont (Some outcome) args3 pipe3
                                | Error _ -> cont None args3 pipe3)
                            suspendWith
                            args2
                            pipe2)
                    suspendWith
                    args
                    pipe
            | _ -> cont None args pipe

        /// Consumes inbox positions atomically under the entry's captured
        /// claim, piped (issue 377). Unclaimed shells fall back to the
        /// unfenced consume; a fenced rejection returns false with zero
        /// effects. Failures propagate, exactly like before.
        /// <param name="entry">The entry carrying the captured claim.</param>
        /// <param name="positions">The positions to consume.</param>
        /// <param name="cont">Continues with whether the consume landed.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withConsumeUnderClaim
            (entry: InboxEntry)
            (positions: IReadOnlyList<int64>)
            (cont: bool -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match settlementClaimFor entry with
            | Some claim ->
                startPipedWait
                    starter
                    (fun () ->
                        ClaimFence.consumeInboxAsync
                            props.Store
                            props.Tenant
                            claim
                            props.SessionId
                            positions
                            CancellationToken.None)
                    "consume-under-claim"
                    (fun args2 pipe2 ->
                        function
                        | Ok landed -> cont landed args2 pipe2
                        | Error error -> raise error)
                    suspendWith
                    args
                    pipe
            | None ->
                startPipedWaitUnit
                    starter
                    (fun () ->
                        props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, CancellationToken.None))
                    "consume-legacy"
                    (fun args2 pipe2 ->
                        function
                        | Ok() -> cont true args2 pipe2
                        | Error error -> raise error)
                    suspendWith
                    args
                    pipe

        /// Updates the execution-owned lifecycle atomically under the
        /// entry's captured claim, piped (issue 377). Unclaimed shells fall
        /// back to the unfenced update; a fenced rejection returns false
        /// with zero effects. Failures propagate, exactly like before.
        /// <param name="entry">The entry carrying the captured claim.</param>
        /// <param name="state">The new execution-owned lifecycle state.</param>
        /// <param name="cont">Continues with whether the update landed.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withUpdateStateUnderClaim
            (entry: InboxEntry)
            (state: SessionState)
            (cont: bool -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match settlementClaimFor entry with
            | Some claim ->
                startPipedWait
                    starter
                    (fun () ->
                        ClaimFence.updateSessionStateAsync
                            props.Store
                            props.Tenant
                            claim
                            props.SessionId
                            state
                            CancellationToken.None)
                    "update-state-under-claim"
                    (fun args2 pipe2 ->
                        function
                        | Ok landed -> cont landed args2 pipe2
                        | Error error -> raise error)
                    suspendWith
                    args
                    pipe
            | None ->
                startPipedWaitUnit
                    starter
                    (fun () ->
                        props.Store.UpdateSessionState(props.Tenant, props.SessionId, state, CancellationToken.None))
                    "update-state-legacy"
                    (fun args2 pipe2 ->
                        function
                        | Ok() -> cont true args2 pipe2
                        | Error error -> raise error)
                    suspendWith
                    args
                    pipe

        /// Applies the resume's fenced writes in order under the parked
        /// entry's captured claim, piped (issue 377): execution Running
        /// first, then the reply's AllowForSession grant. Short-circuits on
        /// the first rejection with zero further effects, exactly like
        /// before.
        /// <param name="parkedEntry">The parked entry carrying the captured claim.</param>
        /// <param name="allowed">The session AllowForSession memory.</param>
        /// <param name="cursorTool">The suspending tool name, or empty.</param>
        /// <param name="rebuiltTool">The rebuilt tool name, or empty.</param>
        /// <param name="reply">The reply to inspect.</param>
        /// <param name="cont">Continues with whether both writes landed or were unneeded.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withResumeFencedWrites
            (parkedEntry: InboxEntry)
            (allowed: HashSet<string>)
            (cursorTool: string)
            (rebuiltTool: string)
            (reply: Reply)
            (cont: bool -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            withUpdateStateUnderClaim
                parkedEntry
                SessionState.Running
                (fun running args2 pipe2 ->
                    if not running then
                        cont false args2 pipe2
                    else
                        match reply with
                        | :? PermissionDecision as decision when
                            not (isNull (box decision))
                            && decision.Decision = PermissionDecisionKind.AllowForSession
                            ->
                            let toolName =
                                if not (String.IsNullOrEmpty cursorTool) then cursorTool
                                elif not (String.IsNullOrEmpty rebuiltTool) then rebuiltTool
                                else ""

                            if String.IsNullOrEmpty toolName then
                                cont true args2 pipe2
                            else
                                allowed.Add(toolName) |> ignore

                                match settlementClaimFor parkedEntry with
                                | Some claim ->
                                    startPipedWait
                                        starter
                                        (fun () ->
                                            ClaimFence.grantSessionToolAsync
                                                props.Store
                                                props.Tenant
                                                claim
                                                props.SessionId
                                                toolName
                                                CancellationToken.None)
                                        "grant-under-claim"
                                        (fun args3 pipe3 ->
                                            function
                                            | Ok granted -> cont granted args3 pipe3
                                            | Error error -> raise error)
                                        suspendWith
                                        args2
                                        pipe2
                                | None ->
                                    startPipedWaitUnit
                                        starter
                                        (fun () ->
                                            props.Store.GrantSessionTool(
                                                props.Tenant,
                                                props.SessionId,
                                                toolName,
                                                CancellationToken.None
                                            ))
                                        "grant-legacy"
                                        (fun args3 pipe3 ->
                                            function
                                            | Ok() -> cont true args3 pipe3
                                            | Error error -> raise error)
                                        suspendWith
                                        args2
                                        pipe2
                        | _ -> cont true args2 pipe2)
                suspendWith
                args
                pipe

        let mutable replyInFlight = false

        /// Replies a routing refusal for one message: the session's
        /// completion destination is unknown or unsupported.
        /// <param name="sender">The message sender.</param>
        /// <param name="error">The routing refusal.</param>
        let replyRouteRefusal (sender: IActorRef) (error: CompletionRoutingException) : unit =
            sender
            <! {
                   Tenant = props.Tenant
                   SessionId = props.SessionId
                   DestinationId = error.DestinationId
                   Reason = error.Reason
               }

        /// Appends one inbox entry, piped, refreshing the pending-count
        /// cache: the entry the prompt and reply arms durable store.
        /// <param name="payload">What the entry carries.</param>
        /// <param name="delivery">How the message was delivered.</param>
        /// <param name="cancellationToken">Abandons the append.</param>
        /// <param name="cont">Continues with the appended entry.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withAppendInbox
            (payload: InboxPayload)
            (delivery: DeliveryMode)
            (cancellationToken: CancellationToken)
            (cont: InboxEntry -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            startPipedWait
                starter
                (fun () ->
                    props.Store.AppendInboxMessage(props.Tenant, props.SessionId, payload, delivery, cancellationToken))
                "prompt-append"
                (fun args2 pipe2 ->
                    function
                    | Error error -> raise error
                    | Ok appended ->
                        cont
                            appended
                            { args2 with
                                PendingCount = args2.PendingCount + 1
                            }
                            pipe2)
                suspendWith
                args
                pipe

        /// Reads the pending inbox for a drain, piped, refreshing the
        /// pending-count cache exactly. Failures propagate, like before.
        /// <param name="cancellationToken">Abandons the read.</param>
        /// <param name="cont">Continues with the pending entries.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at the wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let withReadPending
            (cancellationToken: CancellationToken)
            (cont: IReadOnlyList<InboxEntry> -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            startPipedWait
                starter
                (fun () -> props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))
                "prompt-drain"
                (fun args2 pipe2 ->
                    function
                    | Error error -> raise error
                    | Ok pending ->
                        cont
                            pending
                            { args2 with
                                PendingCount = if isNull (box pending) then 0 else pending.Count
                            }
                            pipe2)
                suspendWith
                args
                pipe

        /// Runs one Idle prompt arm end to end, piped (route already
        /// validated by the caller): applies a recorded rebind, restores
        /// the prime, appends, drains tier-first, and gates authority.
        /// Authorized entries start (the session returns to Running);
        /// refused ones settle Failed and the drain moves on.
        /// <param name="payload">What the entry carries.</param>
        /// <param name="delivery">How the message was delivered.</param>
        /// <param name="cancellationToken">Abandons the append and reads.</param>
        /// <param name="sender">The prompt sender.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let idlePromptArm
            (payload: InboxPayload)
            (delivery: DeliveryMode)
            (cancellationToken: CancellationToken)
            (sender: IActorRef)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            // A recorded rebind applies before draining when
            // the inbox is still empty (a quiescent boundary
            // without its own hook settled here): the protocol
            // leaves the inbox as found, so the drain below
            // sees only real entries.
            withTryApplyPendingWhenIdle
                (fun () args3 pipe3 ->
                    // A prime settled at quiescence is restored before
                    // appending (issue 313): the fresh turn re-primes
                    // while the inbox is still empty, so its marker and
                    // suspend/resolve writes fence live.
                    withEnsurePrimed
                        (fun () args4 pipe4 ->
                            withAppendInbox
                                payload
                                delivery
                                cancellationToken
                                (fun appended args5 pipe5 ->
                                    withReadPending
                                        cancellationToken
                                        (fun pending args6 pipe6 ->
                                            let first =
                                                selectDrainableEntries pending
                                                |> List.tryHead
                                                |> Option.defaultValue appended

                                            // The per-turn authority gate runs at this fresh-turn
                                            // boundary only: authorized entries run, refused ones
                                            // settle Failed without ever invoking the runner and
                                            // the drain moves on.
                                            withCheckAgentAuthority
                                                (fun authority args7 pipe7 ->
                                                    match authority with
                                                    | None when args7.Closing <> [] ->
                                                        // A requested close owns the session now:
                                                        // the appended entry stays durable, but no
                                                        // turn starts into the close.
                                                        sender <! PromptAccepted appended
                                                        suspendWith args7 pipe7
                                                    | None ->
                                                        withReadGrants
                                                            (fun grants args8 pipe8 ->
                                                                withStartSuspendable
                                                                    first
                                                                    1
                                                                    grants
                                                                    None
                                                                    (fun () args9 pipe9 ->
                                                                        sender <! PromptAccepted appended

                                                                        suspendWith
                                                                            { args9 with
                                                                                State = SessionState.Running
                                                                                Suspended = None
                                                                            }
                                                                            pipe9)
                                                                    (fun error args9 pipe9 ->
                                                                        sender <! Status.Failure(error)
                                                                        suspendWith args9 pipe9)
                                                                    suspendWith
                                                                    args8
                                                                    pipe8)
                                                            suspendWith
                                                            args7
                                                            pipe7
                                                    | Some(failure, reason) ->
                                                        withSettleAuthorityRefusal
                                                            first
                                                            failure
                                                            reason
                                                            (fun () args8 pipe8 ->
                                                                withDrainAfterRefusal
                                                                    (fun next args9 pipe9 ->
                                                                        sender <! PromptAccepted appended

                                                                        suspendWith
                                                                            { args9 with
                                                                                State = next
                                                                                Suspended = None
                                                                            }
                                                                            pipe9)
                                                                    suspendWith
                                                                    args8
                                                                    pipe8)
                                                            suspendWith
                                                            args7
                                                            pipe7)
                                                suspendWith
                                                args6
                                                pipe6)
                                        suspendWith
                                        args5
                                        pipe5)
                                suspendWith
                                args4
                                pipe4)
                        suspendWith
                        args3
                        pipe3)
                suspendWith
                args
                pipe

        /// Settles one reported Finished attempt, piped: consumes the
        /// entry, observes the (possibly abort-mapped) result, then
        /// AutoCloses on the first Completed turn or drains the next
        /// drainable entry into a new turn, else returns to Idle. The
        /// caller clears the parked suspension: every exit rests
        /// unparked. A final-iteration Inject the loop left pending starts
        /// its new turn here, implicitly.
        /// <param name="entry">The entry the finished attempt executed.</param>
        /// <param name="result">The result the actor settles.</param>
        /// <param name="turnId">The settling turn's id, or None when no loop id ever existed.</param>
        /// <param name="cont">Continues with the next loop state.</param>
        /// <param name="suspendWith">Re-enters the loop with the state current at each wait's start.</param>
        /// <param name="args">The current loop state.</param>
        /// <param name="pipe">The current pipe state.</param>
        /// <returns>The actor computation.</returns>
        let settleEntryNowChain
            (entry: InboxEntry)
            (result: TurnResult)
            (turnId: TurnId option)
            (cont: SessionState -> SuspendCont)
            (suspendWith: SuspendCont)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            let positions = [| entry.Position |] :> IReadOnlyList<int64>

            withDecideControl
                entry
                result
                (fun decided args2 pipe2 ->
                    withTrySettleSuspendable
                        entry
                        decided
                        turnId
                        (fun outcomeOpt args3 pipe3 ->
                            match outcomeOpt with
                            | Some outcome when outcome.Status = SessionSettlementStatus.Applied ->
                                // Committed winner (issue 363): the atomic
                                // boundary already consumed the entry, chose
                                // the lifecycle disposition and the queued
                                // candidate, enqueued the completion under
                                // the stable key, and released the prime at
                                // quiescence. Publish only this winner:
                                // observe once, journal the terminal event
                                // best-effort, then drain the authoritative
                                // following entry or rest at quiescence. No
                                // unfenced execution cleanup runs here.
                                pendingStop <- None
                                runningTurnId <- None
                                notifySettled decided
                                notifyPosition entry.Position

                                let afterJournal args4 pipe4 =
                                    withRetireControl
                                        entry
                                        (fun () args5 pipe5 ->
                                            if outcome.State = SessionState.Closed then
                                                pendingAgent <- None
                                                cont SessionState.Closed args5 pipe5
                                            elif outcome.State = SessionState.Running then
                                                match outcome.Following with
                                                | null ->
                                                    withTryApplyPendingWhenIdle
                                                        (fun () args6 pipe6 ->
                                                            if decided.Status = TurnStatus.Completed then
                                                                withSettleCompletedPrime
                                                                    (fun () args7 pipe7 ->
                                                                        cont SessionState.Idle args7 pipe7)
                                                                    suspendWith
                                                                    args6
                                                                    pipe6
                                                            else
                                                                cont SessionState.Idle args6 pipe6)
                                                        suspendWith
                                                        args5
                                                        pipe5
                                                | following ->
                                                    withDrainSettledFollowing following cont suspendWith args5 pipe5
                                            else
                                                // The settled entry is consumed: an empty
                                                // inbox is quiescent, so a recorded
                                                // rebind applies before resting, and the
                                                // Completed-turn prime settle below keeps
                                                // the facade prime releasable. The
                                                // lifecycle write already landed in the
                                                // atomic boundary.
                                                withTryApplyPendingWhenIdle
                                                    (fun () args6 pipe6 ->
                                                        if decided.Status = TurnStatus.Completed then
                                                            withSettleCompletedPrime
                                                                (fun () args7 pipe7 ->
                                                                    cont SessionState.Idle args7 pipe7)
                                                                suspendWith
                                                                args6
                                                                pipe6
                                                        else
                                                            cont SessionState.Idle args6 pipe6)
                                                    suspendWith
                                                    args5
                                                    pipe5)
                                        suspendWith
                                        args4
                                        pipe4

                                // Terminal completion event (issue 289):
                                // verdict-first (the committed settle above
                                // decided the terminal kind), journaled
                                // best-effort under the live token. A prime
                                // that never ran never reaches here, and a
                                // fault before any mint (None) journals
                                // nothing.
                                match turnId with
                                | Some tid ->
                                    withJournalSettledCompletion
                                        tid
                                        decided
                                        (fun () args4 pipe4 -> afterJournal args4 pipe4)
                                        suspendWith
                                        args3
                                        pipe3
                                | None -> afterJournal args3 pipe3
                            | Some outcome when outcome.Status = SessionSettlementStatus.AlreadyApplied ->
                                // Identical retry already committed: suppress
                                // every duplicate effect (no second
                                // observation, journal, retirement, or
                                // drain) and honor the recorded disposition
                                // as the loop state only.
                                pendingStop <- None
                                runningTurnId <- None

                                if outcome.State = SessionState.Closed then
                                    pendingAgent <- None
                                    cont SessionState.Closed args3 pipe3
                                elif outcome.State = SessionState.Running then
                                    cont SessionState.Running args3 pipe3
                                else
                                    cont SessionState.Idle args3 pipe3
                            | Some _ ->
                                // Rejected: a takeover winner owns the turn
                                // now. Zero effects from this loser: no
                                // observation, no journal, no completion, no
                                // control retirement, no lifecycle write.
                                runningTurnId <- None
                                cont args3.State args3 pipe3
                            | None ->
                                // No capability or claim (unclaimed test
                                // shells) or a faulted settlement call: the
                                // legacy store-first path below.
                                pendingStop <- None
                                runningTurnId <- None

                                startPipedWaitUnit
                                    starter
                                    (fun () ->
                                        props.Store.MarkInboxConsumed(
                                            props.Tenant,
                                            props.SessionId,
                                            positions,
                                            CancellationToken.None
                                        ))
                                    "settle-entry/consume"
                                    (fun args4 pipe4 ->
                                        function
                                        | Error error -> raise error
                                        | Ok() ->
                                            let args4c =
                                                { args4 with
                                                    PendingCount = max 0 (args4.PendingCount - 1)
                                                }

                                            notifySettled decided
                                            notifyPosition entry.Position

                                            withDispatchCompletion
                                                starter
                                                props
                                                decided
                                                (fun _ args5 pipe5 ->
                                                    let rec afterJournalLegacy args6 pipe6 =
                                                        withRetireControl
                                                            entry
                                                            (fun () args7 pipe7 ->
                                                                if decided.Status = TurnStatus.Completed then
                                                                    withAutoCloseEnabled
                                                                        starter
                                                                        props
                                                                        (fun enabled args8 pipe8 ->
                                                                            if enabled then
                                                                                // AutoClose (issue 82): the first Completed
                                                                                // turn closes the session store-first instead
                                                                                // of draining; the entry is already consumed
                                                                                // above. Aborted and Failed results never take
                                                                                // this path, so failed runs stay open for
                                                                                // inspection. A recorded rebind dies with
                                                                                // the session: Closed rejects it.
                                                                                startPipedWaitUnit
                                                                                    starter
                                                                                    (fun () ->
                                                                                        props.Store.CloseSession(
                                                                                            props.Tenant,
                                                                                            props.SessionId,
                                                                                            CancellationToken.None
                                                                                        ))
                                                                                    "settle-entry/autoclose"
                                                                                    (fun args9 pipe9 ->
                                                                                        function
                                                                                        | Error error ->
                                                                                            raise error
                                                                                        | Ok() ->
                                                                                            let args9c =
                                                                                                { args9 with
                                                                                                    PendingCount =
                                                                                                        max
                                                                                                            0
                                                                                                            (args9.PendingCount
                                                                                                             - 1)
                                                                                                }

                                                                                            pendingAgent <- None

                                                                                            cont
                                                                                                SessionState.Closed
                                                                                                args9c
                                                                                                pipe9)
                                                                                    suspendWith
                                                                                    args8
                                                                                    pipe8
                                                                            else
                                                                                legacyDrain args8 pipe8)
                                                                        suspendWith
                                                                        args7
                                                                        pipe7
                                                                else
                                                                    legacyDrain args7 pipe7)
                                                            suspendWith
                                                            args6
                                                            pipe6

                                                    and legacyDrain args6 pipe6 =
                                                        // The settled entry is consumed: an empty
                                                        // inbox is quiescent (the reporting task is
                                                        // done and no new turn started), so a
                                                        // recorded rebind applies before draining
                                                        // next, and stays pending while entries
                                                        // remain.
                                                        withTryApplyPendingWhenIdle
                                                            (fun () args7 pipe7 ->
                                                                withReadPending
                                                                    CancellationToken.None
                                                                    (fun pending args8 pipe8 ->
                                                                        match selectDrainableEntries pending with
                                                                        | following :: _ ->
                                                                            // The per-turn authority gate runs at this
                                                                            // settle-drain boundary only: authorized
                                                                            // entries run, refused ones settle Failed
                                                                            // without ever invoking the runner and
                                                                            // the drain moves on.
                                                                            withCheckAgentAuthority
                                                                                (fun authority args9 pipe9 ->
                                                                                    match authority with
                                                                                    | None when
                                                                                        args9.Closing <> []
                                                                                        ->
                                                                                        // A requested close owns the session now:
                                                                                        // the entry stays durable, but no turn
                                                                                        // starts into the close.
                                                                                        cont
                                                                                            SessionState.Idle
                                                                                            args9
                                                                                            pipe9
                                                                                    | None ->
                                                                                        withReadGrants
                                                                                            (fun
                                                                                                grants
                                                                                                args10
                                                                                                pipe10 ->
                                                                                                withStartSuspendable
                                                                                                    following
                                                                                                    1
                                                                                                    grants
                                                                                                    None
                                                                                                    (fun
                                                                                                        ()
                                                                                                        args11
                                                                                                        pipe11 ->
                                                                                                        cont
                                                                                                            SessionState.Running
                                                                                                            args11
                                                                                                            pipe11)
                                                                                                    (fun
                                                                                                        error
                                                                                                        _
                                                                                                        _ ->
                                                                                                        raise
                                                                                                            error)
                                                                                                    suspendWith
                                                                                                    args10
                                                                                                    pipe10)
                                                                                            suspendWith
                                                                                            args9
                                                                                            pipe9
                                                                                    | Some(failure, reason) ->
                                                                                        withSettleAuthorityRefusal
                                                                                            following
                                                                                            failure
                                                                                            reason
                                                                                            (fun () args10 pipe10 ->
                                                                                                withDrainAfterRefusal
                                                                                                    cont
                                                                                                    suspendWith
                                                                                                    args10
                                                                                                    pipe10)
                                                                                            suspendWith
                                                                                            args9
                                                                                            pipe9)
                                                                                suspendWith
                                                                                args8
                                                                                pipe8
                                                                        | [] ->
                                                                            // Completed-turn prime settle (issue
                                                                            // 313): release the facade prime exactly
                                                                            // once at quiescence through the fenced
                                                                            // settle, so a later prompt (or a
                                                                            // respawn prime after a restart) claims
                                                                            // anew instead of observing
                                                                            // TurnLeaseMissing. Aborted and Failed
                                                                            // results keep their prime, like the
                                                                            // fault path; a stale (taken-over) token
                                                                            // settles nothing. The terminal event
                                                                            // above already journaled under the live
                                                                            // token, so the settle lands after it.
                                                                            if
                                                                                decided.Status = TurnStatus.Completed
                                                                            then
                                                                                withSettleCompletedPrime
                                                                                    (fun () args9 pipe9 ->
                                                                                        idleWrite args9 pipe9)
                                                                                    suspendWith
                                                                                    args8
                                                                                    pipe8
                                                                            else
                                                                                idleWrite args8 pipe8)
                                                                    suspendWith
                                                                    args7
                                                                    pipe7)
                                                            suspendWith
                                                            args6
                                                            pipe6

                                                    and idleWrite args8 pipe8 =
                                                        startPipedWaitUnit
                                                            starter
                                                            (fun () ->
                                                                props.Store.UpdateSessionState(
                                                                    props.Tenant,
                                                                    props.SessionId,
                                                                    SessionState.Idle,
                                                                    CancellationToken.None
                                                                ))
                                                            "settle-entry/idle"
                                                            (fun args9 pipe9 ->
                                                                function
                                                                | Ok() -> cont SessionState.Idle args9 pipe9
                                                                | Error error -> raise error)
                                                            suspendWith
                                                            args8
                                                            pipe8

                                                    // Terminal completion event (issue 289):
                                                    // verdict-first (the store-first settle above
                                                    // decided the terminal kind), journaled
                                                    // best-effort under the live token. A prime
                                                    // that never ran never reaches here, and a
                                                    // fault before any mint (None) journals
                                                    // nothing.
                                                    match turnId with
                                                    | Some tid ->
                                                        withJournalSettledCompletion
                                                            tid
                                                            decided
                                                            (fun () args6 pipe6 -> afterJournalLegacy args6 pipe6)
                                                            suspendWith
                                                            args5
                                                            pipe5
                                                    | None -> afterJournalLegacy args5 pipe5)
                                                suspendWith
                                                args4c
                                                pipe4)
                                    suspendWith
                                    args3
                                    pipe3)
                        suspendWith
                        args2
                        pipe2)
                suspendWith
                args
                pipe

        let rec loop (args: SuspendLoopArgs) (pipe: SuspendPipe) : Cont<SuspendableActorMessage, unit> =
            match args.Closing with
            | (_, token) :: _ when not (LifecyclePipe.isBusy pipe) ->
                // A requested close owns the durable write now that the
                // pipe drains: every recorded sender shares the one write
                // and observes the same stored session.
                withCloseWriteSession
                    starter
                    props
                    token
                    (fun closed args2 pipe2 ->
                        for sender, _ in args2.Closing do
                            sender <! closed

                        loop
                            { args2 with
                                State = SessionState.Closed
                                Suspended = None
                                Closing = []
                            }
                            pipe2)
                    suspendWith
                    args
                    pipe
            | _ ->
                match args.PendingResume with
                | Some(entry, attempt, allowed, seed) when not (LifecyclePipe.isBusy pipe) && not args.Seeding ->
                    // Crash-resume start deferred to the loop (issue 390):
                    // the interrupted turn restarts as a new attempt only
                    // once the seeding read landed and no wait is
                    // outstanding, so the start chain never blocks a
                    // dispatcher thread.
                    withStartSuspendable
                        entry
                        attempt
                        allowed
                        seed
                        (fun () args2 pipe2 ->
                            loop
                                { args2 with
                                    State = SessionState.Running
                                    Suspended = None
                                    PendingResume = None
                                }
                                pipe2)
                        (fun error _ _ -> raise error)
                        suspendWith
                        args
                        pipe
                | _ ->
                    match LifecyclePipe.tryTakeDeferred pipe with
                    | Some((message, sender), pipe') -> handleMessage message sender args pipe'
                    | None ->
                        actor {
                            let! message = mailbox.Receive()
                            return! handleMessage message (mailbox.Sender()) args pipe
                        }

        and handleMessage
            (message: SuspendableActorMessage)
            (sender: IActorRef)
            (args: SuspendLoopArgs)
            (pipe: SuspendPipe)
            : Cont<SuspendableActorMessage, unit> =
            match message with
            | SuspendableStoreCompleted(opId, incarnation, outcome) ->
                match LifecyclePipe.tryComplete pipe opId incarnation with
                | Some(outstanding, pipe') -> outstanding.Resume args pipe' outcome
                | None -> loop args pipe
            | SuspendableStoreTimeout(opId, incarnation) ->
                match LifecyclePipe.tryComplete pipe opId incarnation with
                | Some(outstanding, pipe') -> outstanding.Resume args pipe' outstanding.TimeoutOutcome
                | None -> loop args pipe
            | _ when args.Seeding ->
                // The entry inbox-count seeding read is in flight:
                // everything waits bounded behind it in arrival order.
                match LifecyclePipe.defer pipe message sender with
                | pipe', true -> loop args pipe'
                | _, false ->
                    replyPipeOverflow sender
                    loop args pipe
            | SuspendableAbortSession(cause, reason, _) ->
                match args.Closing with
                | _ :: _ ->
                    // A requested close owns the stop already: the abort
                    // no-ops returning the current snapshot, like
                    // post-close.
                    sender <! takeSuspendSnapshot args
                    loop args pipe
                | [] ->
                    match args.State, args.Suspended with
                    | SessionState.Running, None when cause = StopCause.ExplicitAbort || cause = StopCause.HostShutdown ->
                        // First cause wins: the detached suspendable turn
                        // runs un-cancellable, so the recorded stop wins at
                        // its next report.
                        if pendingStop.IsNone then
                            pendingStop <- Some(cause, reason)

                        sender <! takeSuspendSnapshot args
                        loop args pipe
                    | _ ->
                        // Idle, WaitingForInput (a suspended turn owns
                        // nothing running to abort), Closed, unknown states,
                        // and non-abort-family causes: a no-op returning the
                        // current state.
                        sender <! takeSuspendSnapshot args
                        loop args pipe
            | SuspendableCloseSession cancellationToken ->
                match args.Suspended with
                | Some parked ->
                    try
                        parked.TimeoutCts.Cancel()
                    with _ ->
                        ()
                | None -> ()

                cancelHeartbeat ()

                // A recorded stop dies with the session: Closed settles
                // nothing further. A recorded rebind dies with it too:
                // Closed rejects it.
                pendingStop <- None
                pendingAgent <- None

                // The durable write lands through the loop entry once the
                // pipe drains, so shutdown stays responsive behind a
                // delayed dependency.
                loop
                    { args with
                        Closing = args.Closing @ [ sender, cancellationToken ]
                    }
                    pipe
            | SuspendableGetSnapshot ->
                sender <! takeSuspendSnapshot args
                loop args pipe
            | _ when args.Closing <> [] ->
                // A requested close behaves Closed for new lifecycle work:
                // it waits bounded behind the close write and is then
                // answered as Closed, in order.
                match LifecyclePipe.defer pipe message sender with
                | pipe', true -> loop args pipe'
                | _, false ->
                    replyPipeOverflow sender
                    loop args pipe
            | _ when LifecyclePipe.isBusy pipe ->
                // A store wait is outstanding: Abort, Close, and
                // GetSnapshot answered from memory above; everything else
                // waits its turn behind the wait.
                match LifecyclePipe.defer pipe message sender with
                | pipe', true -> loop args pipe'
                | _, false ->
                    replyPipeOverflow sender
                    loop args pipe
            | SessionReplyPayload reply ->
                let reject requestId message =
                    sender
                    <! ReplyRejected(ReplyMismatchException(props.SessionId, requestId, message))

                let expectedRequestId (parked: SuspendedTurn) : string option =
                    match parked.Cursor with
                    | Some cursor -> Some cursor.RequestId
                    | None ->
                        match parked.Rebuilt with
                        | Some rebuilt -> Some rebuilt.RequestId
                        | None -> None

                withValidateRoute
                    (fun choice args2 pipe2 ->
                        match choice with
                        | Choice2Of2 error ->
                            replyRouteRefusal sender error
                            loop args2 pipe2
                        | Choice1Of2() ->
                            match args2.State, args2.Suspended, replyRequestId reply with
                            | _, _, _ when replyInFlight ->
                                reject
                                    (replyRequestId reply |> Option.defaultValue "")
                                    "A reply is already being consumed."

                                loop args2 pipe2
                            | SessionState.WaitingForInput, Some _, Some requestId ->
                                withDurableStop
                                    (fun stopOpt args3 pipe3 ->
                                        match stopOpt with
                                        | Some _ ->
                                            reject requestId "Accepted stop forbids reply consumption or resume."
                                            loop args3 pipe3
                                        | None ->
                                            match args3.Suspended with
                                            | Some parked ->
                                                match expectedRequestId parked with
                                                | Some expected when
                                                    String.Equals(requestId, expected, StringComparison.Ordinal)
                                                    ->
                                                    // The actor mailbox is the ordinary scoped reply
                                                    // gate: validate and append in one serialized
                                                    // turn, so concurrent answers cannot both pass a
                                                    // snapshot-before-append check.
                                                    startPipedWait
                                                        starter
                                                        (fun () ->
                                                            props.Store.AppendInboxMessage(
                                                                props.Tenant,
                                                                props.SessionId,
                                                                ReplyPayload(reply),
                                                                DeliveryMode.Queue,
                                                                CancellationToken.None
                                                            ))
                                                        "reply-append"
                                                        (fun args4 pipe4 ->
                                                            function
                                                            | Error error -> raise error
                                                            | Ok appended ->
                                                                replyInFlight <- true
                                                                suspendSelf.Tell(ReplyEntry appended, sender)

                                                                loop
                                                                    { args4 with
                                                                        PendingCount = args4.PendingCount + 1
                                                                    }
                                                                    pipe4)
                                                        suspendWith
                                                        args3
                                                        pipe3
                                                | _ ->
                                                    reject requestId "The reply answered no pending request."
                                                    loop args3 pipe3
                                            | None ->
                                                reject requestId "The reply answered no pending request."
                                                loop args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2
                            | SessionState.WaitingForInput, Some _, None ->
                                reject "" "The reply carried no answer for the pending request."
                                loop args2 pipe2
                            | _ ->
                                let requestId = replyRequestId reply |> Option.defaultValue ""
                                reject requestId "The session has no pending request for the reply."
                                loop args2 pipe2)
                    suspendWith
                    args
                    pipe
            | SuspendableQueuePrompt(payload, cancellationToken) ->
                withValidateRoute
                    (fun choice args2 pipe2 ->
                        match choice with
                        | Choice2Of2 error ->
                            replyRouteRefusal sender error
                            loop args2 pipe2
                        | Choice1Of2() ->
                            match args2.State with
                            | SessionState.Closed ->
                                sender <! PromptRejected SessionState.Closed
                                loop args2 pipe2
                            | SessionState.Idle ->
                                idlePromptArm
                                    payload
                                    DeliveryMode.Queue
                                    cancellationToken
                                    sender
                                    suspendWith
                                    args2
                                    pipe2
                            | SessionState.Running
                            | SessionState.WaitingForInput ->
                                withAppendInbox
                                    payload
                                    DeliveryMode.Queue
                                    cancellationToken
                                    (fun appended args3 pipe3 ->
                                        sender <! PromptAccepted appended
                                        loop args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2
                            | _ ->
                                withAppendInbox
                                    payload
                                    DeliveryMode.Queue
                                    cancellationToken
                                    (fun appended args3 pipe3 ->
                                        sender <! PromptAccepted appended
                                        loop args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2)
                    suspendWith
                    args
                    pipe
            | SuspendableInjectPrompt(payload, cancellationToken) ->
                withValidateRoute
                    (fun choice args2 pipe2 ->
                        match choice with
                        | Choice2Of2 error ->
                            replyRouteRefusal sender error
                            loop args2 pipe2
                        | Choice1Of2() ->
                            match args2.State with
                            | SessionState.Closed ->
                                sender <! PromptRejected SessionState.Closed
                                loop args2 pipe2
                            | SessionState.Idle ->
                                idlePromptArm
                                    payload
                                    DeliveryMode.Inject
                                    cancellationToken
                                    sender
                                    suspendWith
                                    args2
                                    pipe2
                            | SessionState.Running
                            | SessionState.WaitingForInput ->
                                // Append-and-wait: the running turn folds the entry
                                // at its next iteration boundary through the runner's
                                // drain hooks, and a suspended turn leaves it for the
                                // settle drain. Never aborts.
                                withAppendInbox
                                    payload
                                    DeliveryMode.Inject
                                    cancellationToken
                                    (fun appended args3 pipe3 ->
                                        sender <! PromptAccepted appended
                                        loop args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2
                            | _ ->
                                withAppendInbox
                                    payload
                                    DeliveryMode.Inject
                                    cancellationToken
                                    (fun appended args3 pipe3 ->
                                        sender <! PromptAccepted appended
                                        loop args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2)
                    suspendWith
                    args
                    pipe
            | SuspendableInterruptPrompt(payload, cancellationToken) ->
                withValidateRoute
                    (fun choice args2 pipe2 ->
                        match choice with
                        | Choice2Of2 error ->
                            replyRouteRefusal sender error
                            loop args2 pipe2
                        | Choice1Of2() ->
                            match args2.State with
                            | SessionState.Closed ->
                                sender <! PromptRejected SessionState.Closed
                                loop args2 pipe2
                            | SessionState.Idle ->
                                idlePromptArm
                                    payload
                                    DeliveryMode.Interrupt
                                    cancellationToken
                                    sender
                                    suspendWith
                                    args2
                                    pipe2
                            | SessionState.Running ->
                                // Pre-empt through the abort verb: the entry joins
                                // the inbox first so the settle drain finds it
                                // first, then the pending stop records
                                // ExplicitAbort. The detached suspendable turn runs
                                // un-cancellable, so the stop wins at its next
                                // report and the settle drains the Interrupt entry
                                // first. A stop that already won keeps the first
                                // cause; the new entry still drains after the settle.
                                withAppendInbox
                                    payload
                                    DeliveryMode.Interrupt
                                    cancellationToken
                                    (fun appended args3 pipe3 ->
                                        if pendingStop.IsNone then
                                            pendingStop <- Some(StopCause.ExplicitAbort, InterruptReason)

                                        sender <! PromptAccepted appended
                                        loop args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2
                            | SessionState.WaitingForInput ->
                                // Append-and-wait: nothing runs to pre-empt and
                                // Reply still resumes the suspended turn.
                                withAppendInbox
                                    payload
                                    DeliveryMode.Interrupt
                                    cancellationToken
                                    (fun appended args3 pipe3 ->
                                        sender <! PromptAccepted appended
                                        loop args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2
                            | _ ->
                                withAppendInbox
                                    payload
                                    DeliveryMode.Interrupt
                                    cancellationToken
                                    (fun appended args3 pipe3 ->
                                        sender <! PromptAccepted appended
                                        loop args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2)
                    suspendWith
                    args
                    pipe
            | SuspendableFinished(entry, completion, attempt, allowed) ->
                match args.State, args.Suspended with
                | SessionState.Running, None when
                    controlReports.ContainsKey entry.Position
                    && (let _, _, id = controlReports[entry.Position] in completedControlReports.Contains id)
                    ->
                    loop args pipe
                | SessionState.Running, None ->
                    withDurableStop
                        (fun durableOpt args2 pipe2 ->
                            // A recorded stop wins over whatever the detached
                            // turn reported, even a success or a suspension: map
                            // to Aborted and clear the cell. Settlement already
                            // won when the cell is empty.
                            let stop = durableOpt |> Option.orElse pendingStop

                            let carried =
                                match stop with
                                | Some(cause, reason) -> mapSuspendAborted cause reason completion.Result
                                | None -> completion.Result

                            match completion.Suspension, stop with
                            | Some cursor, None ->
                                // Execution-owned WaitingForInput (issue 377):
                                // fenced under the captured claim. A rejection
                                // means a takeover winner owns the turn: zero
                                // effects, no park, no journal.
                                withUpdateStateUnderClaim
                                    entry
                                    SessionState.WaitingForInput
                                    (fun landed args3 pipe3 ->
                                        if not landed then
                                            cancelHeartbeat ()
                                            runningTurnId <- None
                                            loop { args3 with Suspended = None } pipe3
                                        else
                                            withJournalSuspend
                                                cursor
                                                (fun journalOutcome args4 pipe4 ->
                                                    match journalOutcome with
                                                    | JournalWriter.JournalAppended _ ->
                                                        let timeoutCts = new CancellationTokenSource()

                                                        let carriedAllowed =
                                                            if isNull (box allowed) then
                                                                HashSet<string>()
                                                            else
                                                                allowed

                                                        // No running attempt remains once parked:
                                                        // the parked turn id carries the settle
                                                        // identity from here on.
                                                        runningTurnId <- None

                                                        withSettlingTurnId
                                                            completion.TurnId
                                                            (fun settlingId args5 pipe5 ->
                                                                let parked =
                                                                    {
                                                                        Entry = entry
                                                                        TurnId =
                                                                            match settlingId with
                                                                            | Some live -> live
                                                                            | None -> Unchecked.defaultof<TurnId>
                                                                        Cursor = Some cursor
                                                                        Rebuilt = None
                                                                        Allowed = carriedAllowed
                                                                        Attempt = attempt
                                                                        TimeoutCts = timeoutCts
                                                                    }

                                                                armTimeout cursor.RequestId timeoutCts

                                                                loop
                                                                    { args5 with
                                                                        State = SessionState.WaitingForInput
                                                                        Suspended = Some parked
                                                                    }
                                                                    pipe5)
                                                            suspendWith
                                                            args4
                                                            pipe4
                                                    | JournalWriter.JournalRejected rejection ->
                                                        // The suspend event never landed: parking
                                                        // would strand the turn on a missing journal
                                                        // entry, so the turn fails with the typed
                                                        // reason instead.
                                                        cancelHeartbeat ()

                                                        withSettleJournalFailure
                                                            entry
                                                            (sprintf "The journal append was rejected: %s." rejection)
                                                            (fun () args5 pipe5 ->
                                                                loop
                                                                    { args5 with
                                                                        State = SessionState.Idle
                                                                        Suspended = None
                                                                    }
                                                                    pipe5)
                                                            suspendWith
                                                            args4
                                                            pipe4
                                                    | JournalWriter.JournalFailed failure ->
                                                        cancelHeartbeat ()

                                                        withSettleJournalFailure
                                                            entry
                                                            failure
                                                            (fun () args5 pipe5 ->
                                                                loop
                                                                    { args5 with
                                                                        State = SessionState.Idle
                                                                        Suspended = None
                                                                    }
                                                                    pipe5)
                                                            suspendWith
                                                            args4
                                                            pipe4)
                                                suspendWith
                                                args3
                                                pipe3)
                                    suspendWith
                                    args2
                                    pipe2
                            | _ ->
                                // Settled, or suspended after a stop won: the
                                // stop settles Aborted with no suspend event
                                // journaled and nothing parked for a Reply. The
                                // settling id resolves completion-carried, then
                                // turn-cell, then snapshot, else the terminal
                                // journals nothing.
                                withSettlingTurnId
                                    completion.TurnId
                                    (fun settling args3 pipe3 ->
                                        // End the finishing attempt's heartbeat before
                                        // settling: a drained following turn starts its
                                        // own heartbeat after this cancel.
                                        cancelHeartbeat ()

                                        settleEntryNowChain
                                            entry
                                            carried
                                            settling
                                            (fun next args4 pipe4 ->
                                                loop
                                                    { args4 with
                                                        State = next
                                                        Suspended = None
                                                    }
                                                    pipe4)
                                            suspendWith
                                            args3
                                            pipe3)
                                    suspendWith
                                    args2
                                    pipe2)
                        suspendWith
                        args
                        pipe
                | SessionState.WaitingForInput, Some parked when parked.Cursor.IsNone && parked.Rebuilt.IsSome ->
                    // Crash-retry path should never produce a running
                    // finish while still parked; ignore stale completions.
                    loop args pipe
                | _ -> loop args pipe
            | SuspendableFaulted(entry, error, _, faultTurnId) ->
                match args.State, args.Suspended with
                | SessionState.Running, None when
                    controlReports.ContainsKey entry.Position
                    && (let _, _, id = controlReports[entry.Position] in completedControlReports.Contains id)
                    ->
                    loop args pipe
                | SessionState.Running, None ->
                    withDurableStop
                        (fun durableOpt args2 pipe2 ->
                            // A recorded stop wins even over a real fault:
                            // settle Aborted under the cause instead of failing
                            // silently. The cell clears on every fault settle.
                            let stop = durableOpt |> Option.orElse pendingStop

                            // The fault's settling id (issue 289): the
                            // message-carried id, then the turn cell, then the
                            // snapshot; None (fault before any mint) journals
                            // nothing while the settle effects run unchanged.
                            let resolveSettling (cont: TurnId option -> SuspendCont) : SuspendCont =
                                fun argsX pipeX ->
                                    match faultTurnId with
                                    | Some _ as resolved -> cont resolved argsX pipeX
                                    | None ->
                                        match runningTurnId with
                                        | Some _ as resolved -> cont resolved argsX pipeX
                                        | None ->
                                            withCurrentTurnSnapshot
                                                (fun snapshot argsY pipeY -> cont snapshot argsY pipeY)
                                                suspendWith
                                                argsX
                                                pipeX

                            resolveSettling
                                (fun settling args3 pipe3 ->
                                    let candidate =
                                        match stop with
                                        | Some(cause, reason) -> abortedSuspendResult cause reason
                                        | None ->
                                            {
                                                AssistantText = ""
                                                Status = TurnStatus.Failed
                                                Iterations = 0
                                                Usage = { InputTokens = 0L; OutputTokens = 0L }
                                                Outcome =
                                                    TurnFailed(ProviderFailureReason.formatFault error) :> TurnOutcome
                                            }

                                    withDecideControl
                                        entry
                                        candidate
                                        (fun selected args4 pipe4 ->
                                            withTrySettleSuspendable
                                                entry
                                                selected
                                                settling
                                                (fun outcomeOpt args5 pipe5 ->
                                                    let positions = [| entry.Position |] :> IReadOnlyList<int64>

                                                    match outcomeOpt with
                                                    | Some outcome when
                                                        outcome.Status = SessionSettlementStatus.Applied
                                                        ->
                                                        // Committed winner (issue 363): the atomic
                                                        // boundary already consumed the entry, chose the
                                                        // lifecycle disposition and the queued candidate,
                                                        // enqueued the completion under the stable key,
                                                        // and released the prime at quiescence. Publish
                                                        // only this winner: observe once, journal the
                                                        // terminal event best-effort, then drain the
                                                        // authoritative following entry or rest at
                                                        // quiescence. No unfenced execution cleanup runs
                                                        // here.
                                                        pendingStop <- None
                                                        runningTurnId <- None
                                                        cancelHeartbeat ()
                                                        notifySettled selected
                                                        notifyPosition entry.Position

                                                        let afterJournal args6 pipe6 =
                                                            withRetireControl
                                                                entry
                                                                (fun () args7 pipe7 ->
                                                                    if outcome.State = SessionState.Closed then
                                                                        pendingAgent <- None

                                                                        loop
                                                                            { args7 with
                                                                                State = SessionState.Closed
                                                                                Suspended = None
                                                                            }
                                                                            pipe7
                                                                    elif outcome.State = SessionState.Running then
                                                                        match outcome.Following with
                                                                        | null ->
                                                                            withTryApplyPendingWhenIdle
                                                                                (fun () args8 pipe8 ->
                                                                                    loop
                                                                                        { args8 with
                                                                                            State =
                                                                                                SessionState.Idle
                                                                                            Suspended = None
                                                                                        }
                                                                                        pipe8)
                                                                                suspendWith
                                                                                args7
                                                                                pipe7
                                                                        | following ->
                                                                            withDrainSettledFollowing
                                                                                following
                                                                                (fun next args8 pipe8 ->
                                                                                    loop
                                                                                        { args8 with
                                                                                            State = next
                                                                                            Suspended = None
                                                                                        }
                                                                                        pipe8)
                                                                                suspendWith
                                                                                args7
                                                                                pipe7
                                                                    else
                                                                        // The faulted entry is consumed and the
                                                                        // lifecycle write already landed in the
                                                                        // atomic boundary: an empty inbox is
                                                                        // quiescent, so a recorded rebind applies
                                                                        // here; entries remaining were drained above.
                                                                        withTryApplyPendingWhenIdle
                                                                            (fun () args8 pipe8 ->
                                                                                loop
                                                                                    { args8 with
                                                                                        State = SessionState.Idle
                                                                                        Suspended = None
                                                                                    }
                                                                                    pipe8)
                                                                            suspendWith
                                                                            args7
                                                                            pipe7)
                                                                suspendWith
                                                                args6
                                                                pipe6

                                                        match settling with
                                                        | Some tid ->
                                                            withJournalSettledCompletion
                                                                tid
                                                                selected
                                                                (fun () args6 pipe6 -> afterJournal args6 pipe6)
                                                                suspendWith
                                                                args5
                                                                pipe5
                                                        | None -> afterJournal args5 pipe5
                                                    | Some outcome when
                                                        outcome.Status = SessionSettlementStatus.AlreadyApplied
                                                        ->
                                                        // Identical retry already committed: suppress
                                                        // every duplicate effect and honor the recorded
                                                        // disposition as the loop state only.
                                                        pendingStop <- None
                                                        runningTurnId <- None
                                                        cancelHeartbeat ()

                                                        if outcome.State = SessionState.Closed then
                                                            pendingAgent <- None

                                                            loop
                                                                { args5 with
                                                                    State = SessionState.Closed
                                                                    Suspended = None
                                                                }
                                                                pipe5
                                                        elif outcome.State = SessionState.Running then
                                                            loop
                                                                { args5 with
                                                                    State = SessionState.Running
                                                                    Suspended = None
                                                                }
                                                                pipe5
                                                        else
                                                            loop
                                                                { args5 with
                                                                    State = SessionState.Idle
                                                                    Suspended = None
                                                                }
                                                                pipe5
                                                    | Some _ ->
                                                        // Rejected: a takeover winner owns the turn now.
                                                        // Zero effects from this loser.
                                                        runningTurnId <- None
                                                        cancelHeartbeat ()
                                                        loop { args5 with Suspended = None } pipe5
                                                    | None ->
                                                        // No capability or claim (unclaimed test shells)
                                                        // or a faulted settlement call: the legacy
                                                        // store-first path below.
                                                        startPipedWaitUnit
                                                            starter
                                                            (fun () ->
                                                                props.Store.MarkInboxConsumed(
                                                                    props.Tenant,
                                                                    props.SessionId,
                                                                    positions,
                                                                    CancellationToken.None
                                                                ))
                                                            "faulted/consume"
                                                            (fun args6 pipe6 ->
                                                                function
                                                                | Error error -> raise error
                                                                | Ok() ->
                                                                    let args6c =
                                                                        { args6 with
                                                                            PendingCount =
                                                                                max 0 (args6.PendingCount - 1)
                                                                        }

                                                                    pendingStop <- None
                                                                    runningTurnId <- None
                                                                    cancelHeartbeat ()
                                                                    notifySettled selected
                                                                    notifyPosition entry.Position

                                                                    withDispatchCompletion
                                                                        starter
                                                                        props
                                                                        selected
                                                                        (fun _ args7 pipe7 ->
                                                                            let afterJournalLegacy args8 pipe8 =
                                                                                withRetireControl
                                                                                    entry
                                                                                    (fun () args9 pipe9 ->
                                                                                        startPipedWaitUnit
                                                                                            starter
                                                                                            (fun () ->
                                                                                                props
                                                                                                    .Store
                                                                                                    .UpdateSessionState(
                                                                                                        props.Tenant,
                                                                                                        props.SessionId,
                                                                                                        SessionState.Idle,
                                                                                                        CancellationToken.None
                                                                                                    ))
                                                                                            "faulted/idle"
                                                                                            (fun args10 pipe10 ->
                                                                                                function
                                                                                                | Ok() ->
                                                                                                    withTryApplyPendingWhenIdle
                                                                                                        (fun
                                                                                                            ()
                                                                                                            args11
                                                                                                            pipe11 ->
                                                                                                            // The faulted entry is consumed and no turn runs:
                                                                                                            // an empty inbox is quiescent, so a recorded rebind
                                                                                                            // applies here; entries remaining keep it pending
                                                                                                            // for the Idle handler.
                                                                                                            loop
                                                                                                                { args11 with
                                                                                                                    State =
                                                                                                                        SessionState.Idle
                                                                                                                    Suspended =
                                                                                                                        None
                                                                                                                }
                                                                                                                pipe11)
                                                                                                        suspendWith
                                                                                                        args10
                                                                                                        pipe10
                                                                                                | Error error ->
                                                                                                    raise error)
                                                                                            suspendWith
                                                                                            args9
                                                                                            pipe9)
                                                                                    suspendWith
                                                                                    args8
                                                                                    pipe8

                                                                            match settling with
                                                                            | Some tid ->
                                                                                withJournalSettledCompletion
                                                                                    tid
                                                                                    selected
                                                                                    (fun () args8 pipe8 ->
                                                                                        afterJournalLegacy
                                                                                            args8
                                                                                            pipe8)
                                                                                    suspendWith
                                                                                    args7
                                                                                    pipe7
                                                                            | None ->
                                                                                afterJournalLegacy args7 pipe7)
                                                                        suspendWith
                                                                        args6c
                                                                        pipe6)
                                                            suspendWith
                                                            args5
                                                            pipe5)
                                                suspendWith
                                                args4
                                                pipe4)
                                        suspendWith
                                        args3
                                        pipe3)
                                args2
                                pipe2)
                        suspendWith
                        args
                        pipe
                | _ -> loop args pipe
            | ReplyEntry replyEntry ->
                replyInFlight <- false

                withValidateRoute
                    (fun choice args2 pipe2 ->
                        match choice with
                        | Choice2Of2 error ->
                            replyRouteRefusal sender error
                            loop args2 pipe2
                        | Choice1Of2() ->
                            match args2.State, args2.Suspended with
                            | SessionState.WaitingForInput, Some _ ->
                                withDurableStop
                                    (fun stopOpt args3 pipe3 ->
                                        match stopOpt with
                                        | Some _ ->
                                            sender
                                            <! Status.Failure(
                                                InvalidSessionStateException(
                                                    props.SessionId,
                                                    "controlPending",
                                                    "Accepted stop forbids reply consumption or resume."
                                                )
                                            )

                                            loop args3 pipe3
                                        | None ->
                                            match args3.Suspended with
                                            | Some parked ->
                                                let replyOpt: Reply option =
                                                    match replyEntry.Payload with
                                                    | :? ReplyPayload as payload when not (isNull (box payload)) ->
                                                        if isNull (box payload.Reply) then
                                                            None
                                                        else
                                                            Some payload.Reply
                                                    | _ -> None

                                                match replyOpt with
                                                | None ->
                                                    let error =
                                                        ReplyMismatchException(
                                                            props.SessionId,
                                                            "",
                                                            "The reply carried no answer for the pending request."
                                                        )

                                                    sender <! ReplyRejected error
                                                    loop args3 pipe3
                                                | Some reply ->
                                                    let requestOpt = replyRequestId reply

                                                    let expectedOpt: string option =
                                                        match parked.Cursor with
                                                        | Some cursor -> Some cursor.RequestId
                                                        | None ->
                                                            match parked.Rebuilt with
                                                            | Some rebuilt -> Some rebuilt.RequestId
                                                            | None -> None

                                                    match requestOpt, expectedOpt with
                                                    | Some requestId, Some expected when
                                                        String.Equals(requestId, expected, StringComparison.Ordinal)
                                                        ->
                                                        if args3.Resolved.Contains(requestId) then
                                                            let error =
                                                                ReplyMismatchException(
                                                                    props.SessionId,
                                                                    requestId,
                                                                    "The reply answers an already-resolved request."
                                                                )

                                                            sender <! ReplyRejected error
                                                            loop args3 pipe3
                                                        else
                                                            try
                                                                parked.TimeoutCts.Cancel()
                                                            with _ ->
                                                                ()

                                                            let positions =
                                                                [| replyEntry.Position |] :> IReadOnlyList<int64>

                                                            // Reply consumption (issue 377): fenced
                                                            // under the parked claim. A rejection
                                                            // means a takeover winner owns the turn:
                                                            // zero effects, host retries.
                                                            withConsumeUnderClaim
                                                                parked.Entry
                                                                positions
                                                                (fun consumed args4 pipe4 ->
                                                                    if not consumed then
                                                                        let error =
                                                                            ReplyMismatchException(
                                                                                props.SessionId,
                                                                                requestId,
                                                                                "The reply arrived after a takeover and was not consumed."
                                                                            )

                                                                        sender <! ReplyRejected error
                                                                        loop args4 pipe4
                                                                    else
                                                                        let args4c =
                                                                            { args4 with
                                                                                PendingCount =
                                                                                    max 0 (args4.PendingCount - 1)
                                                                            }

                                                                        withJournalResolve
                                                                            reply
                                                                            (fun journalOutcome args5 pipe5 ->
                                                                                match journalOutcome with
                                                                                | JournalWriter.JournalAppended _ ->
                                                                                    args5.Resolved.Add(requestId)
                                                                                    |> ignore

                                                                                    let cursorTool =
                                                                                        match parked.Cursor with
                                                                                        | Some cursor ->
                                                                                            cursor.ToolName
                                                                                        | None -> ""

                                                                                    let rebuiltTool =
                                                                                        match parked.Rebuilt with
                                                                                        | Some rebuilt ->
                                                                                            rebuilt.ToolName
                                                                                        | None -> ""

                                                                                    // Resume Running plus grant
                                                                                    // (issue 377): fenced under the
                                                                                    // parked claim with a single
                                                                                    // branch. A rejection after a
                                                                                    // landed consume and journal keeps
                                                                                    // those pre-takeover commits and
                                                                                    // stops without resuming.
                                                                                    withResumeFencedWrites
                                                                                        parked.Entry
                                                                                        parked.Allowed
                                                                                        cursorTool
                                                                                        rebuiltTool
                                                                                        reply
                                                                                        (fun resumed args6 pipe6 ->
                                                                                            if not resumed then
                                                                                                cancelHeartbeat ()

                                                                                                loop
                                                                                                    { args6 with
                                                                                                        Suspended =
                                                                                                            args6.Suspended
                                                                                                    }
                                                                                                    pipe6
                                                                                            else
                                                                                                sender
                                                                                                <! ReplyAccepted
                                                                                                    replyEntry

                                                                                                let nextAttempt =
                                                                                                    parked.Attempt
                                                                                                    + 1

                                                                                                match
                                                                                                    parked.Cursor
                                                                                                with
                                                                                                | Some _ ->
                                                                                                    if
                                                                                                        args6.Closing
                                                                                                        <> []
                                                                                                    then
                                                                                                        // A requested close owns the session now:
                                                                                                        // the consumed reply still acks Accepted,
                                                                                                        // but no resumed turn starts into the close.
                                                                                                        sender
                                                                                                        <! ReplyAccepted
                                                                                                            replyEntry

                                                                                                        loop
                                                                                                            args6
                                                                                                            pipe6
                                                                                                    else
                                                                                                        withResumeSuspendable
                                                                                                            parked
                                                                                                            reply
                                                                                                            nextAttempt
                                                                                                            (fun
                                                                                                                ()
                                                                                                                args7
                                                                                                                pipe7 ->
                                                                                                                loop
                                                                                                                    { args7 with
                                                                                                                        State =
                                                                                                                            SessionState.Running
                                                                                                                        Suspended =
                                                                                                                            None
                                                                                                                    }
                                                                                                                    pipe7)
                                                                                                            suspendWith
                                                                                                            args6
                                                                                                            pipe6
                                                                                                | None ->
                                                                                                    if
                                                                                                        args6.Closing
                                                                                                        <> []
                                                                                                    then
                                                                                                        sender
                                                                                                        <! ReplyAccepted
                                                                                                            replyEntry

                                                                                                        loop
                                                                                                            args6
                                                                                                            pipe6
                                                                                                    else
                                                                                                        withStartSuspendable
                                                                                                            parked.Entry
                                                                                                            nextAttempt
                                                                                                            parked.Allowed
                                                                                                            None
                                                                                                            (fun
                                                                                                                ()
                                                                                                                args8
                                                                                                                pipe8 ->
                                                                                                                loop
                                                                                                                    { args8 with
                                                                                                                        State =
                                                                                                                            SessionState.Running
                                                                                                                        Suspended =
                                                                                                                            None
                                                                                                                    }
                                                                                                                    pipe8)
                                                                                                            (fun
                                                                                                                error
                                                                                                                _
                                                                                                                _ ->
                                                                                                                raise
                                                                                                                    error)
                                                                                                            suspendWith
                                                                                                            args6
                                                                                                            pipe6)
                                                                                        suspendWith
                                                                                        args5
                                                                                        pipe5
                                                                                | JournalWriter.JournalRejected rejection ->
                                                                                    // The resolve event never landed: resuming
                                                                                    // would strand the turn on a missing
                                                                                    // journal entry, so the turn fails with
                                                                                    // the typed reason instead. The reply
                                                                                    // matched and is consumed, so it still
                                                                                    // acks Accepted, and the request id is
                                                                                    // recorded so a redelivery replays
                                                                                    // Accepted instead of ReplyMismatch.
                                                                                    args5.Resolved.Add(requestId)
                                                                                    |> ignore

                                                                                    withSettleJournalFailure
                                                                                        parked.Entry
                                                                                        (sprintf
                                                                                            "The journal append was rejected: %s."
                                                                                            rejection)
                                                                                        (fun () args6 pipe6 ->
                                                                                            sender
                                                                                            <! ReplyAccepted
                                                                                                replyEntry

                                                                                            loop
                                                                                                { args6 with
                                                                                                    State =
                                                                                                        SessionState.Idle
                                                                                                    Suspended =
                                                                                                        None
                                                                                                }
                                                                                                pipe6)
                                                                                        suspendWith
                                                                                        args5
                                                                                        pipe5
                                                                                | JournalWriter.JournalFailed failure ->
                                                                                    args5.Resolved.Add(requestId)
                                                                                    |> ignore

                                                                                    withSettleJournalFailure
                                                                                        parked.Entry
                                                                                        failure
                                                                                        (fun () args6 pipe6 ->
                                                                                            sender
                                                                                            <! ReplyAccepted
                                                                                                replyEntry

                                                                                            loop
                                                                                                { args6 with
                                                                                                    State =
                                                                                                        SessionState.Idle
                                                                                                    Suspended =
                                                                                                        None
                                                                                                }
                                                                                                pipe6)
                                                                                        suspendWith
                                                                                        args5
                                                                                        pipe5)
                                                                            suspendWith
                                                                            args4c
                                                                            pipe4)
                                                                suspendWith
                                                                args3
                                                                pipe3
                                                    | Some requestId, _ ->
                                                        let error =
                                                            ReplyMismatchException(
                                                                props.SessionId,
                                                                requestId,
                                                                "The reply answered no pending request."
                                                            )

                                                        sender <! ReplyRejected error
                                                        loop args3 pipe3
                                                    | None, _ ->
                                                        let error =
                                                            ReplyMismatchException(
                                                                props.SessionId,
                                                                "",
                                                                "The reply carried no answer for the pending request."
                                                            )

                                                        sender <! ReplyRejected error
                                                        loop args3 pipe3
                                            | None ->
                                                let error =
                                                    ReplyMismatchException(
                                                        props.SessionId,
                                                        "",
                                                        "The session has no pending request for the reply."
                                                    )

                                                sender <! ReplyRejected error
                                                loop args3 pipe3)
                                    suspendWith
                                    args2
                                    pipe2
                            | _ ->
                                let requestId: string =
                                    match replyEntry.Payload with
                                    | :? ReplyPayload as payload when not (isNull (box payload)) ->
                                        match replyRequestId payload.Reply with
                                        | Some id -> id
                                        | None -> ""
                                    | _ -> ""

                                let error =
                                    ReplyMismatchException(
                                        props.SessionId,
                                        requestId,
                                        "The session has no pending request for the reply."
                                    )

                                sender <! ReplyRejected error
                                loop args2 pipe2)
                    suspendWith
                    args
                    pipe
            | SuspendTimedOut requestId ->
                match args.State, args.Suspended with
                | SessionState.WaitingForInput, Some parked ->
                    let expectedOpt: string option =
                        match parked.Cursor with
                        | Some cursor -> Some cursor.RequestId
                        | None ->
                            match parked.Rebuilt with
                            | Some rebuilt -> Some rebuilt.RequestId
                            | None -> None

                    match expectedOpt with
                    | Some expected when String.Equals(requestId, expected, StringComparison.Ordinal) ->
                        try
                            parked.TimeoutCts.Cancel()
                        with _ ->
                            ()

                        let positions = [| parked.Entry.Position |] :> IReadOnlyList<int64>

                        withDecideControl
                            parked.Entry
                            (timeoutResult ())
                            (fun decided args2 pipe2 ->
                                // The timeout settled the turn Failed: a
                                // recorded stop loses to the settlement.
                                pendingStop <- None

                                startPipedWaitUnit
                                    starter
                                    (fun () ->
                                        props.Store.MarkInboxConsumed(
                                            props.Tenant,
                                            props.SessionId,
                                            positions,
                                            CancellationToken.None
                                        ))
                                    "suspend-timeout/consume"
                                    (fun args3 pipe3 ->
                                        function
                                        | Error error -> raise error
                                        | Ok() ->
                                            let args3c =
                                                { args3 with
                                                    PendingCount = max 0 (args3.PendingCount - 1)
                                                }

                                            withJournalTimeout
                                                parked.TurnId
                                                (fun () args4 pipe4 ->
                                                    notifySettled decided
                                                    notifyPosition parked.Entry.Position

                                                    withDispatchCompletion
                                                        starter
                                                        props
                                                        decided
                                                        (fun _ args5 pipe5 ->
                                                            withRetireControl
                                                                parked.Entry
                                                                (fun () args6 pipe6 ->
                                                                    cancelHeartbeat ()

                                                                    startPipedWaitUnit
                                                                        starter
                                                                        (fun () ->
                                                                            props.Store.UpdateSessionState(
                                                                                props.Tenant,
                                                                                props.SessionId,
                                                                                SessionState.Idle,
                                                                                CancellationToken.None
                                                                            ))
                                                                        "suspend-timeout/idle"
                                                                        (fun args7 pipe7 ->
                                                                            function
                                                                            | Ok() ->
                                                                                loop
                                                                                    { args7 with
                                                                                        State = SessionState.Idle
                                                                                        Suspended = None
                                                                                    }
                                                                                    pipe7
                                                                            | Error error -> raise error)
                                                                        suspendWith
                                                                        args6
                                                                        pipe6)
                                                                suspendWith
                                                                args5
                                                                pipe5)
                                                        suspendWith
                                                        args4
                                                        pipe4)
                                                suspendWith
                                                args3c
                                                pipe3)
                                    suspendWith
                                    args2
                                    pipe2)
                            suspendWith
                            args
                            pipe
                    | _ -> loop args pipe
                | _ -> loop args pipe
            | SuspendableObserveHostAbort(tenant, sessionId, turn) ->
                if
                    tenant = props.Tenant
                    && sessionId = props.SessionId
                    && runningTurnId = Some turn
                then
                    withDurableStop
                        (fun stopOpt args2 pipe2 ->
                            match stopOpt with
                            | Some stop -> pendingStop <- Some stop
                            | None -> ()

                            loop args2 pipe2)
                        suspendWith
                        args
                        pipe
                else
                    loop args pipe
            | SuspendableCompactSession cancellationToken ->
                match args.State with
                | SessionState.Closed ->
                    sender <! CompactRejected SessionState.Closed
                    loop args pipe
                | SessionState.Idle ->
                    match currentCompact with
                    | None ->
                        sender <! CompactNotNeeded
                        loop args pipe
                    | Some compact ->
                        compactIdleNowPiped
                            starter
                            props
                            compact
                            cancellationToken
                            (fun reply args2 pipe2 ->
                                sender <! reply
                                loop args2 pipe2)
                            suspendWith
                            args
                            pipe
                | SessionState.Running ->
                    match currentCompact with
                    | Some compact when not (isNull (box compact.Force)) ->
                        compact.Force.Request()
                        sender <! CompactDeferred
                        loop args pipe
                    | _ ->
                        // Unconfigured: no boundary hook shares the
                        // one-shot cell, so nothing can fire later.
                        sender <! CompactNotNeeded
                        loop args pipe
                | SessionState.WaitingForInput ->
                    // A suspended turn owns the history, so an on-demand
                    // compact no-ops.
                    sender <! CompactNotNeeded
                    loop args pipe
                | _ ->
                    // Out-of-range stored state: stay durable but
                    // compact nothing.
                    sender <! CompactNotNeeded
                    loop args pipe
            | SuspendableSetAgent(agentId, cancellationToken) ->
                match args.State with
                | SessionState.Closed ->
                    sender <! SetAgentRejected SessionState.Closed
                    loop args pipe
                | _ ->
                    pendingAgent <- Some agentId

                    let afterApply args2 pipe2 =
                        startPipedWait
                            starter
                            (fun () -> requireSessionAsync props.Store props.Tenant props.SessionId cancellationToken)
                            "set-agent/read"
                            (fun args3 pipe3 ->
                                function
                                | Error error -> raise error
                                | Ok current ->
                                    match pendingAgent with
                                    | None -> sender <! SetAgentApplied current
                                    | Some _ -> sender <! SetAgentPending current

                                    loop args3 pipe3)
                            suspendWith
                            args2
                            pipe2

                    if args.State = SessionState.Idle || args.State = SessionState.WaitingForInput then
                        // No turn task runs in these states: an empty
                        // inbox applies the rebind at once, queued
                        // entries keep it pending. (A parked suspension
                        // keeps its entry pending, so a Waiting session
                        // applies at the post-resume settle boundary,
                        // never mid-suspension: claiming there would
                        // steal the parked entry and leak the protocol
                        // bootstrap as a turn.)
                        withTryApplyPendingWhenIdle (fun () args2 pipe2 -> afterApply args2 pipe2) suspendWith args pipe
                    else
                        afterApply args pipe
            | SuspendableCheckInbox ->
                withValidateRoute
                    (fun choice args2 pipe2 ->
                        match choice with
                        | Choice2Of2 error ->
                            replyRouteRefusal sender error
                            loop args2 pipe2
                        | Choice1Of2() ->
                            match args2.State with
                            | SessionState.Idle ->
                                // The dispatch wake: the mailbox serializes this
                                // against prompts, and the in-memory Idle re-check
                                // above is the last word, so a wake racing a prompt
                                // or a second wake collapses to a no-op or ordered
                                // queueing. Never appends: only the oldest drainable
                                // entry already stored starts, through the same
                                // start path the Idle prompt arms use.
                                startPipedWait
                                    starter
                                    (fun () ->
                                        props.Store.ReadPendingInbox(
                                            props.Tenant,
                                            props.SessionId,
                                            CancellationToken.None
                                        ))
                                    "check-inbox/read"
                                    (fun args3 pipe3 ->
                                        function
                                        | Error(:? SessionNotFoundException) -> loop args3 pipe3
                                        | Error error -> raise error
                                        | Ok pending ->
                                            let args3c =
                                                { args3 with
                                                    PendingCount = if isNull (box pending) then 0 else pending.Count
                                                }

                                            match selectDrainableEntries pending with
                                            | first :: _ ->
                                                // A settled Idle child has released its prime. Claim
                                                // the pending entry through the existing provider path,
                                                // retaining this original envelope for the start below.
                                                let reprimeThenStart args4 pipe4 =
                                                    withCheckAgentAuthority
                                                        (fun authority args5 pipe5 ->
                                                            match authority with
                                                            | None when
                                                                suspend.ReprimeJournal.IsSome && controlPrime.IsNone
                                                                ->
                                                                loop args5 pipe5
                                                            | None ->
                                                                startPipedWaitUnit
                                                                    starter
                                                                    (fun () ->
                                                                        props.Store.UpdateSessionState(
                                                                            props.Tenant,
                                                                            props.SessionId,
                                                                            SessionState.Running,
                                                                            CancellationToken.None
                                                                        ))
                                                                    "check-inbox/running"
                                                                    (fun args6 pipe6 ->
                                                                        function
                                                                        | Error error -> raise error
                                                                        | Ok() ->
                                                                            withReadGrants
                                                                                (fun grants args7 pipe7 ->
                                                                                    if args7.Closing <> [] then
                                                                                        loop args7 pipe7
                                                                                    else
                                                                                        withStartSuspendable
                                                                                            first
                                                                                            1
                                                                                            grants
                                                                                            None
                                                                                            (fun () args8 pipe8 ->
                                                                                                loop
                                                                                                    { args8 with
                                                                                                        State =
                                                                                                            SessionState.Running
                                                                                                        Suspended =
                                                                                                            None
                                                                                                    }
                                                                                                    pipe8)
                                                                                            (fun error _ _ ->
                                                                                                raise error)
                                                                                            suspendWith
                                                                                            args7
                                                                                            pipe7)
                                                                                suspendWith
                                                                                args6
                                                                                pipe6)
                                                                    suspendWith
                                                                    args5
                                                                    pipe5
                                                            | Some(failure, reason) ->
                                                                withSettleAuthorityRefusal
                                                                    first
                                                                    failure
                                                                    reason
                                                                    (fun () args6 pipe6 ->
                                                                        withDrainAfterRefusal
                                                                            (fun next args7 pipe7 ->
                                                                                loop
                                                                                    { args7 with
                                                                                        State = next
                                                                                        Suspended = None
                                                                                    }
                                                                                    pipe7)
                                                                            suspendWith
                                                                            args6
                                                                            pipe6)
                                                                    suspendWith
                                                                    args5
                                                                    pipe5)
                                                        suspendWith
                                                        args4
                                                        pipe4

                                                withCurrentTurnSnapshot
                                                    (fun snapshot args4 pipe4 ->
                                                        match snapshot with
                                                        | None when suspend.ReprimeJournal.IsSome ->
                                                            withReprime
                                                                (fun fresh count args5 pipe5 ->
                                                                    let args5c =
                                                                        match count with
                                                                        | Some c -> { args5 with PendingCount = c }
                                                                        | None -> args5

                                                                    match fresh with
                                                                    | Some live -> swapJournal live
                                                                    | None -> ()

                                                                    reprimeThenStart args5c pipe5)
                                                                suspendWith
                                                                args4
                                                                pipe4
                                                        | _ -> reprimeThenStart args4 pipe4)
                                                    suspendWith
                                                    args3c
                                                    pipe3
                                            | [] -> loop args3c pipe3)
                                    suspendWith
                                    args2
                                    pipe2
                            | _ -> loop args2 pipe2)
                    suspendWith
                    args
                    pipe

        and suspendWith (args: SuspendLoopArgs) (pipe: SuspendPipe) : Cont<SuspendableActorMessage, unit> =
            loop args pipe

        // Crash-resume starts deferred to the loop (issue 390): the entry
        // decision stays synchronous on the spawning thread, but the
        // store-touching start chain runs piped once the seeding read
        // lands, so activation never blocks a dispatcher thread.
        let pendingResume: (InboxEntry * int * HashSet<string> * IList<ChatMessage> option) option =
            match initialState, initialResumeEntry with
            | SessionState.Running, Some entry when
                not (isNull (box suspend.Recovery))
                && entry.Position
                   <> (unbox<InboxEntry> (box (unbox<ControlTargetRecovery> (box suspend.Recovery)).Entry)).Position
                ->
                // Atomic crash failure chose fresh following work, not a replay
                // of the failed entry. Existing admission owns its real turn.
                Some(entry, 1, readGrantsNow (), None)
            | SessionState.Running, Some entry ->
                // Crash resume: the interrupted turn restarts as a new attempt
                // under the fresh spawn-primed journal token (old-attempt events
                // stay since the journal is append-only). The rehydrated history
                // seeds the resumed run's runner input in-memory (never
                // journaled, so replay cursors stay untouched); a rehydration
                // failure falls back to a seedless retry from the inbox entry
                // per the existing crash-retry precedent. The spawn thread
                // carries no cancellation token: the probes still bound through
                // hardening plus their checkpoints, and the guard below keeps a
                // failed probe from failing the resume.
                let crashSeed: IList<ChatMessage> option =
                    try
                        match
                            lastJournalTurnId suspend.EventStore props.Tenant props.SessionId CancellationToken.None
                        with
                        | Some interrupted ->
                            Some(
                                rehydrateCrashHistory
                                    suspend.EventStore
                                    props.Tenant
                                    props.SessionId
                                    interrupted
                                    CancellationToken.None
                            )
                        | None -> None
                    with _ ->
                        None

                Some(
                    entry,
                    (controlPrime |> Option.map _.Attempt |> Option.defaultValue 2),
                    (readGrantsNow ()),
                    crashSeed
                )
            | _ -> None

        let initialArgs: SuspendLoopArgs =
            {
                State = initialState
                Suspended = initialSuspended
                Resolved = HashSet<string>()
                PendingCount = 0
                Closing = []
                Seeding = true
                PendingResume = pendingResume
            }

        let initialPipe: SuspendPipe =
            { LifecyclePipe.empty () with
                Deferred = initialDeferred
            }

        startPipedWait
            starter
            (fun () -> props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None))
            "seed-inbox-count"
            (fun args2 pipe2 ->
                function
                | Ok pending ->
                    loop
                        { args2 with
                            Seeding = false
                            PendingCount = if isNull (box pending) then 0 else pending.Count
                        }
                        pipe2
                | Error(:? SessionNotFoundException) ->
                    loop
                        { args2 with
                            Seeding = false
                            PendingCount = 0
                        }
                        pipe2
                | Error error -> raise error)
            suspendWith
            initialArgs
            initialPipe

    let behaviorWithSuspend props suspend mailbox =
        behaviorWithSuspendRouted (fun () -> ()) (fun _ -> ()) props suspend TimeProvider.System None [] mailbox

    /// Asks a suspendable actor with the shared timeout, honouring the
    /// caller's cancellation. Mirrors askAsync for the suspendable protocol.
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="message">The message to ask with.</param>
    /// <param name="cancellationToken">Cancels the ask.</param>
    /// <returns>The actor's reply.</returns>
    let private askSuspendableAsync<'Reply>
        (session: IActorRef)
        (message: SuspendableActorMessage)
        (cancellationToken: CancellationToken)
        : Task<'Reply> =
        task {
            use timeoutCts = new CancellationTokenSource(askTimeout)

            use linkedCts =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)

            let! reply = session.Ask<obj>(message, linkedCts.Token)

            match reply with
            | :? CompletionRoutingRefused as refusal -> return raise refusal.Exception
            | _ -> return unbox<'Reply> reply
        }

    /// Prompts a suspendable session actor: appends the Queue inbox entry
    /// and starts a suspendable turn when Idle. Validates the session is
    /// present and not Closed before touching the actor.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let promptSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box message) then
            raise (ArgumentNullException(nameof message))

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no further prompts."
                    )
                )

            // The suspendable actor owns its queue wire: the suspendable
            // behavior starts its suspendable runner for it.
            let payload = UserMessagePayload(message) :> InboxPayload

            let! reply =
                askSuspendableAsync<SessionPromptReply>
                    session
                    (SuspendableQueuePrompt(payload, cancellationToken))
                    cancellationToken

            match reply with
            | PromptAccepted entry -> return entry
            | PromptRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the prompt was accepted."
                        )
                    )
        }

    /// Prompts a suspendable session actor through one delivery wire: the
    /// shared validation and Closed rejection behind the Inject and
    /// Interrupt wires. Validates the session is present and not Closed
    /// before touching the actor, so invalid transitions throw here, never
    /// inside the actor.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="ask">Builds the suspendable prompt message for the delivery.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let private promptSuspendableCore
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (ask: InboxPayload -> CancellationToken -> SuspendableActorMessage)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box message) then
            raise (ArgumentNullException(nameof message))

        if isNull (box ask) then
            raise (ArgumentNullException(nameof ask))

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no further prompts."
                    )
                )

            let payload = UserMessagePayload(message) :> InboxPayload

            let! reply =
                askSuspendableAsync<SessionPromptReply> session (ask payload cancellationToken) cancellationToken

            match reply with
            | PromptAccepted entry -> return entry
            | PromptRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the prompt was accepted."
                        )
                    )
        }

    /// Injects a user message into a suspendable session actor: appends with
    /// Inject delivery and folds into the running turn at its next iteration
    /// boundary without interrupting it. Validates the session is present
    /// and not Closed before touching the actor.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let injectSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        promptSuspendableCore
            store
            tenant
            sessionId
            session
            message
            (fun payload token -> SuspendableInjectPrompt(payload, token))
            cancellationToken

    /// Interrupts a suspendable session actor with a user message: appends
    /// with Interrupt delivery, records the ExplicitAbort pending stop when a
    /// turn runs, and drains the Interrupt entry first after the settle.
    /// Validates the session is present and not Closed before touching the
    /// actor.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let interruptSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        promptSuspendableCore
            store
            tenant
            sessionId
            session
            message
            (fun payload token -> SuspendableInterruptPrompt(payload, token))
            cancellationToken

    /// Replies to a suspended turn: appends the Reply inbox entry, matches
    /// it against the pending request id, and resumes from the cursor with
    /// attempt plus 1. An unknown or already-resolved request id throws the
    /// typed ReplyMismatchException; a matching one consumes the Reply entry
    /// and resumes. Reply never starts a turn.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to reply to.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="reply">The host reply. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the reply.</param>
    /// <returns>The consumed Reply inbox entry.</returns>
    let replyAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (reply: Reply)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box reply) then
            raise (ArgumentNullException(nameof reply))

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no reply."
                    )
                )

            let! answer = session.Ask<obj>(SessionReplyPayload reply, TimeSpan.FromSeconds 30.0, cancellationToken)

            match answer with
            | :? CompletionRoutingRefused as refusal -> return raise refusal.Exception
            | :? SessionReplyReply as replyReply ->
                match replyReply with
                | ReplyAccepted entry -> return entry
                | ReplyRejected error -> return raise error
            | _ -> return raise (InvalidOperationException("The session actor returned an unexpected reply."))
        }

    /// Closes a suspendable session: the client boundary. Valid in every
    /// state and idempotent; the store close evicts the session's grant
    /// memory. Unknown sessions throw SessionNotFoundException before
    /// touching the actor. A turn running while the session closes keeps
    /// its detached task, but its journal writes fence on the claim and its
    /// finish is ignored once the actor is Closed.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to close.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cancellationToken">Cancels the close.</param>
    /// <returns>The stored session after the close.</returns>
    let closeSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cancellationToken: CancellationToken)
        : Task<Session> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! _ = requireSessionAsync store tenant sessionId cancellationToken

            let! closed =
                askSuspendableAsync<Session> session (SuspendableCloseSession cancellationToken) cancellationToken

            return closed
        }

    /// Reads a suspendable actor's snapshot: its lifecycle state, the
    /// store's pending inbox count, and the pending suspend request id.
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The actor's current snapshot.</returns>
    let getSuspendSnapshotAsync (session: IActorRef) (cancellationToken: CancellationToken) : Task<SessionSnapshot> =
        ArgumentNullException.ThrowIfNull(session)
        askSuspendableAsync<SessionSnapshot> session SuspendableGetSnapshot cancellationToken

    /// Wakes a suspendable session actor for the polling dispatcher: tells
    /// the idempotent check-inbox message, which starts a suspendable turn
    /// on the oldest drainable entry when the actor is Idle and no-ops
    /// otherwise. Validates the session is present and not Closed before
    /// touching the actor, so invalid transitions skip here, never inside
    /// the actor. One-way: the Tell carries no reply, so the dispatcher
    /// never blocks on the actor. Never appends and never claims a turn:
    /// claim ownership and fencing stay with the actor's start path.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to wake.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cancellationToken">Cancels the wake.</param>
    /// <returns>A task that completes once the wake was told.</returns>
    let checkInboxAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cancellationToken: CancellationToken)
        : Task =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State <> SessionState.Closed then
                session.Tell(SuspendableCheckInbox)
        }

    /// Aborts the turn running in a suspendable session: the client boundary
    /// turn-level verb. Idle is a no-op returning the current snapshot, as
    /// is WaitingForInput (a suspended turn owns nothing running to abort).
    /// Running records the pending stop under the typed cause: the detached
    /// suspendable turn runs un-cancellable, so the stop wins at its next
    /// report and a second abort keeps the first cause. Unknown sessions
    /// throw SessionNotFoundException and Closed sessions throw
    /// InvalidSessionStateException before touching the actor; a Close
    /// racing the abort maps to the same exception.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to abort the turn in.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cause">Which abort-family stop cause wins: ExplicitAbort or HostShutdown.</param>
    /// <param name="reason">Why the turn stops. Must not be null. Never contains secrets or tool arguments.</param>
    /// <param name="cancellationToken">Cancels the abort.</param>
    /// <returns>The actor's snapshot after the abort was accepted.</returns>
    let abortSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cause: StopCause)
        (reason: string)
        (cancellationToken: CancellationToken)
        : Task<SessionSnapshot> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box reason) then
            raise (ArgumentNullException(nameof reason))

        if cause <> StopCause.ExplicitAbort && cause <> StopCause.HostShutdown then
            raise (
                ArgumentOutOfRangeException(
                    nameof cause,
                    "Only ExplicitAbort and HostShutdown abort a turn: the deadline arrives through the turn loop and lease loss through the claim fence."
                )
            )

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no abort."
                    )
                )

            let! snapshot =
                askSuspendableAsync<SessionSnapshot>
                    session
                    (SuspendableAbortSession(cause, reason, cancellationToken))
                    cancellationToken

            if snapshot.State = SessionState.Closed then
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            snapshot.State.ToString(),
                            "The session closed before the abort was accepted."
                        )
                    )
            else
                return snapshot
        }

    /// Compacts a suspendable session on demand: the client boundary. Idle
    /// replays the journal and compacts now without starting a turn,
    /// answering the before/after estimates; Running arms the one-shot force
    /// flag the turn's force-aware hook honors at the next boundary;
    /// WaitingForInput no-ops (a suspended turn owns the history). Unknown
    /// sessions throw SessionNotFoundException and Closed sessions throw
    /// InvalidSessionStateException before touching the actor; a Close
    /// racing the compact maps to the same exception.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to compact.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cancellationToken">Cancels the compact.</param>
    /// <returns>The actor's compact reply.</returns>
    let compactSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cancellationToken: CancellationToken)
        : Task<SessionCompactReply> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no compact."
                    )
                )

            let! reply =
                askSuspendableAsync<SessionCompactReply>
                    session
                    (SuspendableCompactSession cancellationToken)
                    cancellationToken

            match reply with
            | CompactRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the compact was accepted."
                        )
                    )
            | _ -> return reply
        }

    /// Rebinds the agent a suspendable session converses with: the client
    /// boundary. Applies at once when the session is quiescent and records
    /// the rebind as pending otherwise; the recorded rebind applies at the
    /// next quiescent boundary. Unknown sessions throw
    /// SessionNotFoundException and Closed sessions throw
    /// InvalidSessionStateException before touching the actor; a Close
    /// racing the rebind maps to the same exception. Agent existence and
    /// enablement are enforced by the facade (which holds the agent store),
    /// never here: the actor applies the rebound id verbatim.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to rebind.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="agentId">The agent the session converses with from now on.</param>
    /// <param name="cancellationToken">Cancels the rebind.</param>
    /// <returns>The stored session: rebound when the rebind applied at once, unchanged while pending.</returns>
    let setAgentSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (agentId: AgentId)
        (cancellationToken: CancellationToken)
        : Task<Session> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no agent change."
                    )
                )

            let! reply =
                askSuspendableAsync<SessionSetAgentReply>
                    session
                    (SuspendableSetAgent(agentId, cancellationToken))
                    cancellationToken

            match reply with
            | SetAgentApplied rebound -> return rebound
            | SetAgentPending unchanged -> return unchanged
            | SetAgentRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the agent change was accepted."
                        )
                    )
        }

    /// Builds the production child-spawn factory the session router uses:
    /// like <see cref="M:Legate.SessionActor.spawnFactory" /> but spawning
    /// the suspendable behavior (<see cref="M:Legate.SessionActor.behaviorWithSuspend" />),
    /// so live sessions suspend on Ask instead of running the base loop.
    /// The suspendable runner carries the DI-resolved
    /// <see cref="T:Legate.IPermissionPolicy" />: the caller builds it (see
    /// SessionPermissions.createRunner) with the policy the container
    /// resolved, and this factory threads it into every child it spawns.
    /// The journal token is primed per session at spawn: when the session
    /// row exists the factory appends a bootstrap inbox entry and claims it,
    /// so the suspend and resolve journal writes fence on a live claim
    /// (mirroring the harness prime); the bootstrap entry is consumed by the
    /// claim, so real prompts still drain first. When the row is missing the
    /// child starts as an empty Idle shell over a fallback token and the
    /// client boundary rejects its mutations, exactly like the base shell.
    /// Claim renewal while suspended belongs to the dispatcher and heartbeat
    /// cycle: the primed lease covers the configured AskTimeout window, and
    /// a lapsed lease settles the turn Failed with the typed reason instead
    /// of journaling half a suspension. The on-demand compaction wiring
    /// comes from compactFor per spawned child, sharing the primed token.
    /// <param name="store">The durable store session actors persist through.</param>
    /// <param name="tenant">The tenant router-spawned sessions belong to.</param>
    /// <param name="eventStore">The journal suspend and resolve events append to.</param>
    /// <param name="delay">The seam the AskTimeout deadline fires off.</param>
    /// <param name="askTimeout">How long a suspension waits for its Reply before settling Failed. Must be positive.</param>
    /// <param name="claimOwner">The claim owner identity the journal prime claims under. Must not be null.</param>
    /// <param name="leaseDuration">How long the primed journal claim lasts. Must be positive.</param>
    /// <param name="runSuspendable">Runs one suspendable attempt, bound to the DI-resolved policy. Never null.</param>
    /// <param name="compactFor">Builds the on-demand compaction wiring for one session from its primed journal token, or None when the host compacts nothing. Must not be null; return None to answer CompactNotNeeded.</param>
    /// <param name="agentStore">The agent catalog the per-turn authority gate reads, or null when the host runs without one: the gate is skipped then.</param>
    /// <param name="eraMarked">Reads the completion era the entity-start probe consults (issue 289). Never null.</param>
    /// <param name="settlement">The container-registered atomic settlement capability the suspendable settle commits through, or None when the host registered none: the actor then falls back to the store itself when it implements the capability, else the legacy store-first path. Split compositions (SQLite, Postgres) must pass theirs, or receipt-bound waits never observe a committed winner.</param>
    /// <returns>A factory mapping a session id string to a suspendable child spawn.</returns>
    /// Where one suspendable activation stands (issue 390): the
    /// validate/recover/prime chain runs piped before the real behavior
    /// starts, and everything received meanwhile waits bounded behind it
    /// in arrival order.
    type private ActivationStep =
        /// The route validation read is outstanding.
        | ValidateRouteStep
        /// The route validated: the control-target recovery read is outstanding.
        | RecoverTargetStep
        /// The recovery resolved: the journal prime is outstanding.
        | PrimeClaimStep of ControlTargetRecovery option

    /// The activation-loop state: which chain step is outstanding.
    type private ActivationArgs =
        {
            /// The chain step in flight.
            Step: ActivationStep
        }

    /// The bounded pipe state the activation loop threads.
    type private ActivationPipe = LifecyclePipe.PipeState<SuspendableActorMessage, ActivationArgs>

    /// An activation-loop continuation.
    type private ActivationCont = ActivationArgs -> ActivationPipe -> Cont<SuspendableActorMessage, unit>

    /// What one suspendable activation decides, once (issue 390): the
    /// chain runs once as an async task; a synchronously completed chain
    /// interprets inline (the historical synchronous factory behavior,
    /// including throws), otherwise an activating actor pipes the same
    /// task without blocking the spawning thread.
    type private ActivationOutcome =
        /// The session validated, recovered, and primed: the routed
        /// behavior's dependencies plus its row checker.
        | Activated of props: SessionActorProps * suspend: SuspendDeps * checkRoute: (Session -> unit)
        /// The route refused: the session answers the refusal.
        | Refused of error: CompletionRoutingRefused
        /// Activation proved impossible: the spawn fails (sync) or the
        /// actor fails its Asks (async) with this error.
        | Failed of error: exn

    let spawnSuspendFactoryRouted
        (routes: CompletionDestinations option)
        (store: ISessionStore)
        (tenant: TenantId)
        (eventStore: ISessionEventStore)
        (delay: ILlmDelay)
        (askTimeout: TimeSpan)
        (claimOwner: string)
        (leaseDuration: TimeSpan)
        (runSuspendable: SuspendableRunner)
        (compactFor: SessionId -> string -> CompactDeps option)
        (agentStore: IAgentStore | null)
        (eraMarked: CompletionEra.CompletionEraReader)
        (clock: TimeProvider)
        (heartbeatOptions: ClaimHeartbeat.ClaimHeartbeatOptions option)
        (settlement: ISessionSettlementStore option)
        : (string -> IActorContext -> string -> IActorRef) =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(eventStore)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(clock)

        if String.IsNullOrWhiteSpace claimOwner then
            raise (ArgumentException("The claim owner must be a non-empty string.", nameof claimOwner))

        if askTimeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof askTimeout, "AskTimeout must be positive."))

        if leaseDuration <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

        if isNull (box runSuspendable) then
            raise (ArgumentNullException(nameof runSuspendable))

        if isNull (box eraMarked) then
            raise (ArgumentNullException(nameof eraMarked))

        if isNull (box compactFor) then
            raise (ArgumentNullException(nameof compactFor))

        /// The base turn runner never runs on a suspendable child: the
        /// suspend behavior drives RunSuspendable only. It stays non-null
        /// because the props contract requires it.
        let unusedRunTurn (_: InboxEntry) (_: CancellationToken) : Task<TurnResult> =
            Task.FromException<TurnResult>(
                InvalidOperationException("A suspendable session actor never runs its base turn runner.")
            )

        /// Primes the journal claim for one session, started without
        /// blocking the caller (issue 390): appends a bootstrap entry and
        /// claims it, returning the live claim. A missing session row, a
        /// validation or route refusal, or any prime failure primes
        /// nothing. The loop pipes the returned task instead of awaiting
        /// it, so the dispatcher thread never blocks on the prime.
        /// <param name="id">The session to prime.</param>
        /// <returns>The live claim, or None.</returns>
        let primeClaimTask (id: SessionId) : Task<TurnClaim option> =
            task {
                try
                    let! found = store.GetSession(tenant, id, CancellationToken.None)

                    match found with
                    | null -> return None
                    | session when isNull (box session.Options) ->
                        raise (
                            CompletionRoutingException(
                                Nullable tenant,
                                Nullable id,
                                null,
                                CompletionRoutingReason.UnsupportedFormat
                            )
                        )

                        return None
                    | session ->
                        session.Options.ValidatePersistence()

                        match routes, session.Options.CompletionDestinationId with
                        | Some registry, _ -> registry.Validate session
                        | None, null -> ()
                        | None, routeId ->
                            raise (
                                CompletionRoutingException(
                                    Nullable tenant,
                                    Nullable id,
                                    routeId,
                                    CompletionRoutingReason.Unknown
                                )
                            )

                        let bootstrap =
                            UserMessagePayload(UserMessage.Text "legate journal prime") :> InboxPayload

                        let! pending = store.ReadPendingInbox(tenant, id, CancellationToken.None)

                        if isNull (box pending) || pending.Count = 0 then
                            let! _ =
                                store.AppendInboxMessage(
                                    tenant,
                                    id,
                                    bootstrap,
                                    DeliveryMode.Queue,
                                    CancellationToken.None
                                )

                            ()

                        let! claimed =
                            store.ClaimNextTurn(tenant, id, claimOwner, leaseDuration, CancellationToken.None)

                        match claimed with
                        | :? TurnLeaseRenewed as renewed when not (isNull (box renewed)) -> return Some renewed.Claim
                        | :? TurnLeaseHeld as held when not (isNull (box held)) -> return Some held.Claim
                        | :? TurnLeaseExpiring as expiring when not (isNull (box expiring)) ->
                            return Some expiring.Claim
                        | _ -> return None
                with _ ->
                    return None
            }

        /// Checks one session row for route activation, synchronously
        /// (pure validation, never a wait): raises the routing refusal the
        /// loop-head and activation chains answer with, or SessionNotFound
        /// for a missing row. Shared by the activation chain and the
        /// loop-head piped validation.
        /// <param name="id">The session the row belongs to.</param>
        /// <param name="session">The row to check. Never null.</param>
        let checkRoute (id: SessionId) (session: Session) : unit =
            if isNull (box session.Options) then
                raise (
                    CompletionRoutingException(
                        Nullable tenant,
                        Nullable id,
                        null,
                        CompletionRoutingReason.UnsupportedFormat
                    )
                )

            session.Options.ValidatePersistence()

            match routes, session.Options.CompletionDestinationId with
            | Some registry, _ -> registry.Validate session
            | None, null -> ()
            | None, routeId ->
                raise (
                    CompletionRoutingException(Nullable tenant, Nullable id, routeId, CompletionRoutingReason.Unknown)
                )

        /// Answers every message with its failure once activation proved
        /// impossible: the spawn would have thrown, so the actor exists
        /// only to fail its Asks instead of faulting the router.
        /// <param name="error">The activation failure.</param>
        /// <param name="mailbox">The actor mailbox, injected by spawn.</param>
        /// <returns>The Akka.FSharp actor computation to spawn.</returns>
        let failedActivation (error: exn) (mailbox: Actor<SuspendableActorMessage>) =
            let rec loopF () =
                actor {
                    let! _ = mailbox.Receive()
                    mailbox.Sender() <! Status.Failure(error)
                    return! loopF ()
                }

            loopF ()

        /// Answers the routing refusal to every message plus snapshots
        /// from the store, piped: a refused session never runs, but its
        /// observable state stays readable without blocking a dispatcher
        /// thread.
        /// <param name="error">The routing refusal.</param>
        /// <param name="captured">The refused session id.</param>
        /// <param name="mailbox">The actor mailbox, injected by spawn.</param>
        /// <returns>The Akka.FSharp actor computation to spawn.</returns>
        let blockedPiped
            (error: CompletionRoutingRefused)
            (captured: SessionId)
            (mailbox: Actor<SuspendableActorMessage>)
            =
            let self = mailbox.Self

            let starter: PipeStarter<SuspendableActorMessage, unit> =
                {
                    Self = self
                    Clock = clock
                    Timeout = LifecyclePipe.defaultStoreOpTimeout
                    PackCompleted =
                        fun (opId, incarnation, outcome) -> SuspendableStoreCompleted(opId, incarnation, outcome)
                    PackTimeout = fun (opId, incarnation) -> SuspendableStoreTimeout(opId, incarnation)
                }

            let rec suspendB (_: unit) (pipe: LifecyclePipe.PipeState<SuspendableActorMessage, unit>) = loopB () pipe

            and loopB () (pipe: LifecyclePipe.PipeState<SuspendableActorMessage, unit>) =
                actor {
                    let! message = mailbox.Receive()

                    match message with
                    | SuspendableStoreCompleted(opId, incarnation, outcome) ->
                        match LifecyclePipe.tryComplete pipe opId incarnation with
                        | Some(outstanding, pipe') -> return! outstanding.Resume () pipe' outcome
                        | None -> return! loopB () pipe
                    | SuspendableStoreTimeout(opId, incarnation) ->
                        match LifecyclePipe.tryComplete pipe opId incarnation with
                        | Some(outstanding, pipe') -> return! outstanding.Resume () pipe' outstanding.TimeoutOutcome
                        | None -> return! loopB () pipe
                    | SuspendableGetSnapshot when not (LifecyclePipe.isBusy pipe) ->
                        let sender = mailbox.Sender()

                        return!
                            startPipedWait
                                starter
                                (fun () -> requireSessionAsync store tenant captured CancellationToken.None)
                                "blocked/snapshot-session"
                                (fun () pipe2 ->
                                    function
                                    | Error e -> raise e
                                    | Ok session ->
                                        startPipedWait
                                            starter
                                            (fun () ->
                                                store.ReadPendingInbox(tenant, captured, CancellationToken.None))
                                            "blocked/snapshot-inbox"
                                            (fun () pipe3 ->
                                                function
                                                | Error e -> raise e
                                                | Ok pending ->
                                                    sender
                                                    <! {
                                                           SessionId = captured
                                                           State = session.State
                                                           PendingCount =
                                                               if isNull (box pending) then 0 else pending.Count
                                                           RunningPosition = None
                                                           PendingRequestId = null
                                                       }

                                                    loopB () pipe3)
                                            suspendB
                                            ()
                                            pipe2)
                                suspendB
                                ()
                                pipe
                    | _ ->
                        // Every other message is refused without touching
                        // the store; a snapshot arriving behind the read
                        // is refused too rather than growing a queue on a
                        // refused session.
                        mailbox.Sender() <! error

                        return! loopB () pipe
                }

            loopB () (LifecyclePipe.empty ())

        /// Runs the validate/recover/prime activation chain once, without
        /// blocking the caller (issue 390): every wait is an async task
        /// the caller pipes instead of awaiting. A synchronously completed
        /// chain interprets inline through the historical synchronous
        /// factory behavior.
        /// <param name="captured">The session to activate.</param>
        /// <returns>The activation outcome.</returns>
        let runActivationChainAsync (captured: SessionId) : Task<ActivationOutcome> =
            let primeBranch (recovery: ControlTargetRecovery | null) : Task<ActivationOutcome> =
                task {
                    try
                        let! primed =
                            match recovery with
                            | null -> primeClaimTask captured
                            | r -> Task.FromResult(r.Claim |> Option.ofObj)

                        if primed.IsNone then
                            return
                                Failed(
                                    InvalidSessionStateException(
                                        captured,
                                        "executionAuthorityUnavailable",
                                        "Activation acquired no genuine prime authority; no runner may start."
                                    )
                                )
                        else
                            let token =
                                match primed with
                                | Some claim -> claim.Token
                                | None -> Guid.NewGuid().ToString("N")

                            let props: SessionActorProps =
                                {
                                    Store = store
                                    Settlement = settlement
                                    Tenant = tenant
                                    SessionId = captured
                                    RunTurn = unusedRunTurn
                                    OnTurnSettled =
                                        Some(fun result -> PromptWaitHubs.ObserveSettledScoped tenant captured result)
                                    OnInjectJournaled = None
                                    Compact = compactFor captured token
                                    Logger = null
                                    StorePipe = None
                                }

                            let suspend: SuspendDeps =
                                {
                                    EventStore = eventStore
                                    Delay = delay
                                    AskTimeout = askTimeout
                                    JournalToken = token
                                    PrimeClaim = primed
                                    Recovery = recovery
                                    RunSuspendable = runSuspendable
                                    ReprimeJournal = Some(fun () -> primeClaimTask captured)
                                    RefreshCompact = Some(compactFor captured)
                                    AgentStore = agentStore
                                    EraMarked = eraMarked
                                }

                            return Activated(props, suspend, checkRoute captured)
                    with ex ->
                        return Failed ex
                }

            task {
                try
                    let! session = store.GetSession(tenant, captured, CancellationToken.None)

                    match session with
                    | null -> return Failed(SessionNotFoundException(captured, "The session does not exist."))
                    | s ->
                        let routeOutcome =
                            try
                                checkRoute captured s
                                Ok()
                            with
                            | :? CompletionRoutingException as error -> Error(Choice1Of2 error)
                            | failure -> Error(Choice2Of2 failure)

                        match routeOutcome with
                        | Error(Choice1Of2 error) ->
                            return
                                Refused
                                    {
                                        Tenant = tenant
                                        SessionId = captured
                                        DestinationId = error.DestinationId
                                        Reason = error.Reason
                                    }
                        | Error(Choice2Of2 failure) -> return Failed failure
                        | Ok() ->

                            match store with
                            | :? ISessionAbortControlStore as control ->
                                try
                                    let! target = control.ReadAbortTarget(tenant, captured, CancellationToken.None)

                                    match target with
                                    | null ->
                                        let! rejected = primeBranch null
                                        return rejected
                                    | t when t.State = ControlTargetState.Active && isNull (box t.Stop) ->
                                        try
                                            let! result =
                                                control.TryRecoverControlTarget(
                                                    tenant,
                                                    captured,
                                                    t.TurnId,
                                                    claimOwner,
                                                    leaseDuration,
                                                    CancellationToken.None
                                                )

                                            if result.Outcome <> ControlOperationOutcome.Applied then
                                                let category =
                                                    if result.Outcome = ControlOperationOutcome.Stopped then
                                                        "controlPending"
                                                    else
                                                        "executionAuthorityUnavailable"

                                                return
                                                    Failed(
                                                        InvalidSessionStateException(
                                                            captured,
                                                            category,
                                                            "Recovery cannot acquire genuine authority for this exact unstopped target."
                                                        )
                                                    )
                                            else
                                                let! recovered = primeBranch result
                                                return recovered
                                        with ex ->
                                            return Failed ex
                                    | _ ->
                                        return
                                            Failed(
                                                InvalidSessionStateException(
                                                    captured,
                                                    "controlPending",
                                                    "Accepted stop or pending control decision forbids activation."
                                                )
                                            )
                                with ex ->
                                    return Failed ex
                            | _ ->
                                return
                                    Failed(
                                        InvalidOperationException(
                                            "ISessionAbortControlStore is required before session activation."
                                        )
                                    )
                with ex ->
                    return Failed ex
            }

        /// Pipes one already-started activation chain without blocking the
        /// spawning thread (issue 390): becomes the routed behavior, the
        /// refusing behavior, or the failing behavior once the chain
        /// settles. Everything received meanwhile waits bounded behind it
        /// in arrival order.
        /// <param name="captured">The session to activate.</param>
        /// <param name="chainTask">The started activation chain.</param>
        /// <param name="mailbox">The actor mailbox, injected by spawn.</param>
        /// <returns>The Akka.FSharp actor computation to spawn.</returns>
        let activatingWithTask
            (captured: SessionId)
            (chainTask: Task<ActivationOutcome>)
            (mailbox: Actor<SuspendableActorMessage>)
            =
            let self = mailbox.Self

            let starter: PipeStarter<SuspendableActorMessage, ActivationArgs> =
                {
                    Self = self
                    Clock = clock
                    Timeout = LifecyclePipe.defaultStoreOpTimeout
                    PackCompleted =
                        fun (opId, incarnation, outcome) -> SuspendableStoreCompleted(opId, incarnation, outcome)
                    PackTimeout = fun (opId, incarnation) -> SuspendableStoreTimeout(opId, incarnation)
                }

            let rec loopA (args: ActivationArgs) (pipe: ActivationPipe) =
                match LifecyclePipe.tryTakeDeferred pipe with
                | Some((message, sender), pipe') -> handleA message sender args pipe'
                | None ->
                    actor {
                        let! message = mailbox.Receive()
                        return! handleA message (mailbox.Sender()) args pipe
                    }

            and handleA
                (message: SuspendableActorMessage)
                (sender: IActorRef)
                (args: ActivationArgs)
                (pipe: ActivationPipe)
                : Cont<SuspendableActorMessage, unit> =
                match message with
                | SuspendableStoreCompleted(opId, incarnation, outcome) ->
                    match LifecyclePipe.tryComplete pipe opId incarnation with
                    | Some(outstanding, pipe') -> outstanding.Resume args pipe' outcome
                    | None -> loopA args pipe
                | SuspendableStoreTimeout(opId, incarnation) ->
                    match LifecyclePipe.tryComplete pipe opId incarnation with
                    | Some(outstanding, pipe') -> outstanding.Resume args pipe' outstanding.TimeoutOutcome
                    | None -> loopA args pipe
                | _ ->
                    match LifecyclePipe.defer pipe message sender with
                    | pipe', true -> loopA args pipe'
                    | _, false ->
                        replyPipeOverflow sender
                        loopA args pipe

            and suspendA (args: ActivationArgs) (pipe: ActivationPipe) = loopA args pipe

            let becomeRouted
                (props: SessionActorProps)
                (suspend: SuspendDeps)
                (check: Session -> unit)
                (pipe: ActivationPipe)
                =
                behaviorWithSuspendRouted
                    (fun () -> ())
                    check
                    props
                    suspend
                    clock
                    heartbeatOptions
                    pipe.Deferred
                    mailbox

            let becomeBlocked (error: CompletionRoutingRefused) (pipe: ActivationPipe) =
                for message, sender in pipe.Deferred do
                    try
                        self.Tell(message, sender) |> ignore
                    with _ ->
                        ()

                blockedPiped error captured mailbox

            let becomeFailed (failure: exn) (pipe: ActivationPipe) =
                for message, sender in pipe.Deferred do
                    try
                        self.Tell(message, sender) |> ignore
                    with _ ->
                        ()

                failedActivation failure mailbox

            startPipedWait
                starter
                (fun () -> chainTask)
                "activate/chain"
                (fun _ pipe2 ->
                    function
                    | Error failure -> becomeFailed failure pipe2
                    | Ok outcome ->
                        match outcome with
                        | Activated(props, suspend, check) -> becomeRouted props suspend check pipe2
                        | Refused error -> becomeBlocked error pipe2
                        | Failed failure -> becomeFailed failure pipe2)
                suspendA
                { Step = ValidateRouteStep }
                (LifecyclePipe.empty ())

        fun sessionId context name ->
            let mutable parsed = Unchecked.defaultof<SessionId>

            if SessionId.TryParse(sessionId, &parsed) then
                let captured = parsed
                let chainTask = runActivationChainAsync captured

                if chainTask.IsCompletedSuccessfully then
                    // Synchronous stores interpret inline: the historical
                    // factory behavior, including synchronous throws and
                    // the refusing child, with no dispatcher wait.
                    match chainTask.Result with
                    | Activated(props, suspend, check) ->
                        spawn
                            context
                            name
                            (behaviorWithSuspendRouted (fun () -> ()) check props suspend clock heartbeatOptions [])
                    | Refused error -> spawn context name (blockedPiped error captured)
                    | Failed failure -> raise failure
                else
                    spawn context name (activatingWithTask captured chainTask)
            else
                spawn context name (actorOf (fun (_: obj) -> ()))

    let spawnSuspendFactory
        store
        tenant
        eventStore
        delay
        askTimeout
        claimOwner
        leaseDuration
        runSuspendable
        compactFor
        agentStore
        eraMarked
        settlement
        =
        spawnSuspendFactoryRouted
            None
            store
            tenant
            eventStore
            delay
            askTimeout
            claimOwner
            leaseDuration
            runSuspendable
            compactFor
            agentStore
            eraMarked
            TimeProvider.System
            None
            settlement
