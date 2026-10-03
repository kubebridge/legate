// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks

module internal NodeBoundedWait =
    let awaitTask
        (operationName: string)
        (operation: Task)
        (bound: TimeSpan)
        (timeProvider: TimeProvider)
        (cancellationToken: CancellationToken)
        : Task =
        ArgumentNullException.ThrowIfNull(operation)
        ArgumentNullException.ThrowIfNull(timeProvider)

        task {
            if operation.IsCompleted then
                do! operation
            elif cancellationToken.IsCancellationRequested then
                return raise (OperationCanceledException(cancellationToken))
            elif bound <= TimeSpan.Zero then
                return
                    raise (
                        DeadlineExceededException(
                            operationName,
                            $"The {operationName} operation exceeded its bounded shutdown wait."
                        )
                    )
            else
                let cancellation =
                    TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)

                let registration =
                    cancellationToken.Register(fun () -> cancellation.TrySetResult(true) |> ignore)

                try
                    let timeout = Task.Delay(bound, timeProvider, CancellationToken.None)
                    let! winner = Task.WhenAny(operation, timeout, cancellation.Task)

                    if Object.ReferenceEquals(winner, operation) then
                        do! operation
                    elif Object.ReferenceEquals(winner, cancellation.Task) then
                        return raise (OperationCanceledException(cancellationToken))
                    else
                        return
                            raise (
                                DeadlineExceededException(
                                    operationName,
                                    $"The {operationName} operation exceeded its bounded shutdown wait."
                                )
                            )
                finally
                    registration.Dispose()
        }

/// Tracks execution work owned by one node/context without taking disposal
/// ownership of any borrowed provider dependency. Admission closes before a
/// node begins draining, so work that was not admitted cannot touch a store,
/// provider, tool, or subscription.
type internal ExecutionWorkTracker() =
    let gate = obj ()
    let mutable drained: Task = Task.CompletedTask
    let mutable drainedSource: TaskCompletionSource<unit> option = None
    let mutable admitted = 0
    let mutable closed = 0

    member _.RunningCount = lock gate (fun () -> admitted)

    member _.IsClosed = Volatile.Read(&closed) = 1

    member _.CloseAdmission() : unit =
        lock gate (fun () ->
            if Interlocked.Exchange(&closed, 1) = 0 then
                if admitted = 0 then
                    drained <- Task.CompletedTask
                    drainedSource <- None)

    member _.Track<'T>(work: unit -> Task<'T>) : Task<'T> =
        ArgumentNullException.ThrowIfNull(work)

        let completion =
            TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)

        lock gate (fun () ->
            if Volatile.Read(&closed) = 1 then
                raise (SessionScopeRejectedException(SessionScopeRejectionReason.NodeStopping))

            if admitted = 0 then
                let source =
                    TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

                drainedSource <- Some source
                drained <- source.Task

            admitted <- admitted + 1)

        let release () =
            lock gate (fun () ->
                admitted <- admitted - 1

                if admitted = 0 then
                    drainedSource |> Option.iter (fun pending -> pending.TrySetResult(()) |> ignore)
                    drainedSource <- None
                    drained <- Task.CompletedTask)

        try
            let started = work ()

            if isNull (box started) then
                raise (InvalidOperationException("Tracked execution work returned no task."))

            started.ContinueWith(
                (fun (completed: Task<'T>) ->
                    try
                        if completed.IsCanceled then
                            completion.TrySetCanceled() |> ignore
                        elif completed.IsFaulted then
                            match completed.Exception with
                            | null ->
                                completion.TrySetException(
                                    InvalidOperationException("Tracked work faulted without an exception.")
                                )
                                |> ignore
                            | aggregate -> completion.TrySetException(aggregate.GetBaseException()) |> ignore
                        else
                            completion.TrySetResult(completed.Result) |> ignore
                    finally
                        release ()),
                TaskScheduler.Default
            )
            |> ignore
        with error ->
            completion.TrySetException(error) |> ignore
            release ()

        completion.Task

    member this.DrainAsync
        (operationName: string)
        (bound: TimeSpan)
        (timeProvider: TimeProvider)
        (cancellationToken: CancellationToken)
        : Task =
        let pending = lock gate (fun () -> drained)
        NodeBoundedWait.awaitTask operationName pending bound timeProvider cancellationToken
