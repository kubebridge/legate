// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"
module Legate.LifecyclePipe

open System
open System.Threading
open System.Threading.Tasks
open Akka
open Akka.Actor
open Akka.FSharp

// Non-blocking lifecycle store waits for session actors (issue 390): every
// Task a lifecycle path starts on the actor thread is piped back as a
// one-way message instead of awaited, so a delayed dependency never occupies
// the dispatcher thread. The actor holds one outstanding wait plus a bounded
// deferred-message queue in its loop state; control messages are still
// processed from memory while a wait is outstanding, and durable effects
// apply only when the awaited wait genuinely completes for the current
// incarnation. All domain computation stays on the actor thread: only the
// outcome boxing (ContinueWith) and the deadline callback (timer) run
// elsewhere, and both only Tell the actor.
//
// Contract notes:
// - One outstanding wait per actor at a time. Starting a second wait while
//   one is outstanding raises: handlers defer store-touching messages until
//   the pipe drains, preserving the mailbox order the blocking code relied
//   on.
// - A wait that already completed runs its resumption inline (fast path),
//   so synchronous stores observe bit-for-bit today's sequencing.
// - A wait that outruns its deadline fails its resumption with
//   DeadlineExceededException and the late completion is discarded by op id.
//   The deadline bounds the wait, never the store task itself: a task that
//   cannot cooperate with cancellation still runs, but its late result
//   produces zero effects.
// - Completions carry the pipe incarnation (fresh per behavior entry), so a
//   pre-restart wait arriving after a restart is discarded even when its op
//   id was reused.

/// How long a piped lifecycle store wait may stay outstanding before the
/// actor abandons it with DeadlineExceededException. Bounds the wait, never
/// the store task: non-cooperative tasks still run, but their late results
/// are discarded by op id.
let defaultStoreOpTimeout = TimeSpan.FromSeconds 30.0

/// How many lifecycle messages may wait behind an outstanding store wait
/// before the actor refuses more. Bounds the pending state; overflow replies
/// Status.Failure to the sender instead of growing without limit.
[<Literal>]
let MaxDeferredMessages = 100

/// Per-actor configuration for piped lifecycle store waits.
type internal StorePipeConfig =
    {
        /// The clock deadline timers register on. Tests pass a
        /// FakeTimeProvider for deterministic deadline control.
        Clock: TimeProvider
        /// How long one piped wait may stay outstanding. Zero or negative
        /// waits without bound, like the blocking code before.
        Timeout: TimeSpan
    }

/// Resolves the effective pipe configuration: the props-supplied config, or
/// the system-clock default when the host constructed none.
let internal resolveConfig (configured: StorePipeConfig option) : StorePipeConfig =
    match configured with
    | Some config when not (isNull (box config.Clock)) && config.Timeout >= TimeSpan.Zero -> config
    | Some config when not (isNull (box config.Clock)) ->
        {
            Clock = config.Clock
            Timeout = defaultStoreOpTimeout
        }
    | _ ->
        {
            Clock = TimeProvider.System
            Timeout = defaultStoreOpTimeout
        }

/// One outstanding piped store wait.
type OutstandingOp<'M, 'A> =
    {
        /// The wait's id, unique within the pipe incarnation.
        OpId: int64
        /// Fixed label for logs and the deadline exception, never secrets.
        Label: string
        /// Runs the wait's outcome against the loop state and pipe state
        /// current when the completion is processed. The outcome argument
        /// is the boxed StoreOpResult the start call built; unboxing is
        /// safe by construction because only this wait's completion carries
        /// this id.
        Resume: 'A -> PipeState<'M, 'A> -> obj -> Cont<'M, unit>
        /// The pre-boxed StoreError resumption the timeout branch runs when
        /// the wait outruns its bound. Boxed here because the result type
        /// parameter is erased on this record.
        TimeoutOutcome: obj
        /// The deadline registration, disposed when the wait settles.
        Timer: ITimer | null
    }

/// The bounded pending state one actor loop threads through: at most one
/// outstanding store wait plus a bounded queue of lifecycle messages
/// deferred behind it.
and PipeState<'M, 'A> =
    {
        /// Fresh per behavior entry: pre-restart completions arriving after
        /// a restart never match, even when their op id was reused.
        Incarnation: Guid
        /// The next wait id to issue.
        NextOpId: int64
        /// The wait in flight, or None when the pipe drains.
        Outstanding: OutstandingOp<'M, 'A> option
        /// Lifecycle messages received while a wait is outstanding, in
        /// arrival order with their senders, reprocessed head-first once
        /// the pipe drains. Bounded by MaxDeferredMessages.
        Deferred: ('M * IActorRef) list
    }

/// A fresh pipe for one behavior entry.
let empty<'M, 'A> () : PipeState<'M, 'A> =
    {
        Incarnation = Guid.NewGuid()
        NextOpId = 0L
        Outstanding = None
        Deferred = []
    }

/// True when a store wait is outstanding: store-touching messages defer and
/// control answers from memory.
let isBusy (pipe: PipeState<'M, 'A>) : bool = pipe.Outstanding.IsSome

/// Takes the head deferred message when the pipe drains. Returns None while
/// a wait is outstanding or when nothing is deferred.
let tryTakeDeferred (pipe: PipeState<'M, 'A>) : (('M * IActorRef) * PipeState<'M, 'A>) option =
    match pipe.Outstanding, pipe.Deferred with
    | None, (head :: tail) -> Some(head, { pipe with Deferred = tail })
    | _ -> None

/// Defers a lifecycle message behind the outstanding wait, preserving
/// arrival order. Returns the updated pipe and whether the message was
/// accepted: past MaxDeferredMessages the message is refused and the caller
/// must fail it (Status.Failure) instead of growing without bound.
let defer (pipe: PipeState<'M, 'A>) (message: 'M) (sender: IActorRef) : PipeState<'M, 'A> * bool =
    if pipe.Deferred.Length >= MaxDeferredMessages then
        pipe, false
    else
        { pipe with Deferred = pipe.Deferred @ [ message, sender ] }, true

/// Matches a completion against the outstanding wait. On a hit the timer is
/// disposed, the slot clears, and the caller runs the resumption against
/// the current loop state. On a miss (stale, duplicate, reordered, or
/// pre-restart completion) the pipe is untouched and the caller discards.
let tryComplete
    (pipe: PipeState<'M, 'A>)
    (opId: int64)
    (incarnation: Guid)
    : (OutstandingOp<'M, 'A> * PipeState<'M, 'A>) option =
    match pipe.Outstanding with
    | Some outstanding when outstanding.OpId = opId && pipe.Incarnation = incarnation ->
        match box outstanding.Timer with
        | null -> ()
        | :? IDisposable as disposable ->
            try
                disposable.Dispose()
            with _ ->
                ()
        | _ -> ()

        Some(outstanding, { pipe with Outstanding = None })
    | _ -> None

/// Maps a finished task to the outcome resumptions match on. Mirrors what
/// GetAwaiter().GetResult() would have thrown: the base exception for a
/// fault, a TaskCanceledException for cancellation. Accessing the task
/// observes it either way.
type internal StoreOpResult<'T> =
    /// The wait completed with a value (possibly null from the store).
    | StoreOk of 'T
    /// The wait faulted or was cancelled.
    | StoreError of exn

/// Maps a finished task to a boxed-crossable outcome.
let private toOutcome<'T> (finished: Task<'T>) : StoreOpResult<'T> =
    if finished.IsCompletedSuccessfully then
        StoreOk finished.Result
    elif finished.IsCanceled then
        StoreError(TaskCanceledException(finished) :> exn)
    else
        match finished.Exception with
        | null -> StoreError(InvalidOperationException("The lifecycle store task finished without a result.") :> exn)
        | aggregate -> StoreError(aggregate.GetBaseException())

/// Unboxes one wait's outcome back to the Result sites match on. Safe by
/// construction: only this wait's completion carries this op id.
let private toPublic<'T> (outcome: obj) : Result<'T, exn> =
    match unbox<StoreOpResult<'T>> outcome with
    | StoreOk value -> Ok value
    | StoreError error -> Error error

/// The DeadlineExceededException a wait that outran its bound fails its
/// resumption with. The store task itself keeps running when it cannot
/// cooperate with cancellation; its late completion is discarded by op id
/// with zero effects.
let timeoutError (label: string) (timeout: TimeSpan) : exn =
    DeadlineExceededException(
        label,
        sprintf "The lifecycle store wait '%s' stayed outstanding past its %g-second bound." label timeout.TotalSeconds
    )
    :> exn


/// Starts a lifecycle store wait the caller already started on the actor
/// thread: a completed wait runs its resumption inline (fast path, today's
/// sequencing), otherwise the outcome pipes back as a one-way completion
/// message and the loop suspends with the wait outstanding. Raises when a
/// wait is already outstanding: handlers must defer first.
/// <param name="self">The actor completions Tell.</param>
/// <param name="clock">The clock the deadline timer registers on.</param>
/// <param name="timeout">How long the wait may stay outstanding. Zero or negative waits without bound.</param>
/// <param name="label">Fixed label for logs and the deadline exception, never secrets.</param>
/// <param name="task">The store task, already started on the actor thread. Never null.</param>
/// <param name="resume">Runs the outcome against the loop and pipe state current when it settles.</param>
/// <param name="packCompleted">Builds the protocol's completion message.</param>
/// <param name="packTimeout">Builds the protocol's timeout message.</param>
/// <param name="suspend">Re-enters the loop with the wait outstanding.</param>
/// <param name="args">The current loop state for the fast path.</param>
/// <param name="pipe">The current pipe state.</param>
/// <returns>The actor computation: the inline resumption or the suspended loop.</returns>
let start<'T, 'M, 'A>
    (self: IActorRef)
    (clock: TimeProvider)
    (timeout: TimeSpan)
    (label: string)
    (task: Task<'T>)
    (resume: 'A -> PipeState<'M, 'A> -> Result<'T, exn> -> Cont<'M, unit>)
    (packCompleted: int64 * Guid * obj -> 'M)
    (packTimeout: int64 * Guid -> 'M)
    (suspend: PipeState<'M, 'A> -> Cont<'M, unit>)
    (args: 'A)
    (pipe: PipeState<'M, 'A>)
    : Cont<'M, unit> =
    ArgumentNullException.ThrowIfNull(self)
    ArgumentNullException.ThrowIfNull(task)

    if pipe.Outstanding.IsSome then
        raise (InvalidOperationException("A lifecycle store wait is already outstanding; defer first."))

    if task.IsCompleted then
        match toOutcome task with
        | StoreOk value -> resume args pipe (Ok value)
        | StoreError error -> resume args pipe (Error error)
    else
        let opId = pipe.NextOpId
        let incarnation = pipe.Incarnation

        let boxedResume (current: 'A) (currentPipe: PipeState<'M, 'A>) (outcome: obj) : Cont<'M, unit> =
            resume current currentPipe (toPublic<'T> outcome)

        let timeoutOutcome: obj = box (StoreError(timeoutError label timeout))

        let timer: ITimer | null =
            if timeout > TimeSpan.Zero then
                clock.CreateTimer(
                    TimerCallback(fun _ ->
                        try
                            self.Tell(packTimeout (opId, incarnation)) |> ignore
                        with _ ->
                            ()),
                    box (),
                    timeout,
                    Timeout.InfiniteTimeSpan
                )
            else
                null

        task.ContinueWith(fun (finished: Task<'T>) ->
            try
                self.Tell(packCompleted (opId, incarnation, box (toOutcome finished)))
                |> ignore
            with _ ->
                ())
        |> ignore

        suspend
            {
                pipe with
                    NextOpId = opId + 1L
                    Outstanding =
                        Some
                            {
                                OpId = opId
                                Label = label
                                Resume = boxedResume
                                TimeoutOutcome = timeoutOutcome
                                Timer = timer
                            }
            }

/// Starts a non-generic lifecycle store wait by projecting it onto
/// Task&lt;unit&gt; without losing the outcome: a faulted wait projects to
/// the base exception (what GetAwaiter().GetResult() would have thrown), a
/// cancelled wait to TaskCanceledException. Same contract as start.
/// <param name="self">The actor completions Tell.</param>
/// <param name="clock">The clock the deadline timer registers on.</param>
/// <param name="timeout">How long the wait may stay outstanding. Zero or negative waits without bound.</param>
/// <param name="label">Fixed label for logs and the deadline exception, never secrets.</param>
/// <param name="antecedent">The store task, already started on the actor thread. Never null.</param>
/// <param name="resume">Runs the outcome against the loop and pipe state current when it settles.</param>
/// <param name="packCompleted">Builds the protocol's completion message.</param>
/// <param name="packTimeout">Builds the protocol's timeout message.</param>
/// <param name="suspend">Re-enters the loop with the wait outstanding.</param>
/// <param name="args">The current loop state for the fast path.</param>
/// <param name="pipe">The current pipe state.</param>
/// <returns>The actor computation: the inline resumption or the suspended loop.</returns>
let startUnit<'M, 'A>
    (self: IActorRef)
    (clock: TimeProvider)
    (timeout: TimeSpan)
    (label: string)
    (antecedent: Task)
    (resume: 'A -> PipeState<'M, 'A> -> Result<unit, exn> -> Cont<'M, unit>)
    (packCompleted: int64 * Guid * obj -> 'M)
    (packTimeout: int64 * Guid -> 'M)
    (suspend: PipeState<'M, 'A> -> Cont<'M, unit>)
    (args: 'A)
    (pipe: PipeState<'M, 'A>)
    : Cont<'M, unit> =
    ArgumentNullException.ThrowIfNull(antecedent)

    // Awaiting never blocks: the builder returns an incomplete task when
    // the antecedent is incomplete, and faults or cancels exactly like it.
    let projected: Task<unit> =
        task {
            do! antecedent
            return ()
        }

    start self clock timeout label projected resume packCompleted packTimeout suspend args pipe
