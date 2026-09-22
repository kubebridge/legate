// SPDX-License-Identifier: Apache-2.0
namespace Legate.Coordination.Redis

open System
open System.Threading
open System.Threading.Tasks
open StackExchange.Redis

// StackExchange.Redis client behind IDistributedLlmAdmission. Every mutation
// runs one atomic Lua script (see RedisScripts): acquire prunes expired
// leases and waiters before admitting, renew/release/complete fence on the
// owner field, and cooldown short-circuits acquire. A script call whose
// reply is lost after a connection failure resolves through the
// CompensationLedger exactly once: the probe checks the lease, the
// compensation removes a landed lease, and repeats never re-apply. Raw
// connection values never enter keys, logs, or messages.

// ────────────────── Outcome parsing (internal, faked in unit tests) ──

/// <summary>
/// Maps raw acquire script outputs to outcomes. Internal: unit tests drive
/// this with fakes.
/// </summary>
module internal AdmissionParsing =

    /// <summary>
    /// Maps a script status and rank to an outcome.
    /// </summary>
    /// <param name="status">The script status: acquired, queued, or cooldown.</param>
    /// <param name="rank">The queue rank when queued.</param>
    /// <returns>The admission outcome; unknown statuses map to cooldown (fail-closed).</returns>
    let toOutcome (status: string) (rank: int) : DistributedAdmissionOutcome =
        let normalized =
            if String.IsNullOrWhiteSpace status then
                ""
            else
                status.Trim().ToLowerInvariant()

        match normalized with
        | "acquired" -> DistributedAdmissionOutcome.Acquired()
        | "queued" -> DistributedAdmissionOutcome.Queued(max 0 rank)
        | "cooldown" -> DistributedAdmissionOutcome.CooldownActive()
        | _ -> DistributedAdmissionOutcome.CooldownActive()

// ────────────────── Client ────────────────────────────────────────────

/// <summary>
/// Distributed LLM admission over one shared Redis instance on
/// StackExchange.Redis. Processes sharing the instance respect one
/// combined concurrency limit with FIFO waiters, waiter TTL, and owner
/// fencing. Sealed; hosts resolve it through
/// <see cref="T:Legate.Coordination.Redis.IDistributedLlmAdmission" />.
/// </summary>
/// <param name="options">The validated coordination options. Must not be null.</param>
/// <param name="getDatabase">The database factory. Must not be null.</param>
[<Sealed>]
type RedisDistributedLlmAdmission private (options: DistributedCoordinationOptions, getDatabase: Func<IDatabase>) =

    do ArgumentNullException.ThrowIfNull(options)
    do ArgumentNullException.ThrowIfNull(getDatabase)

    let ledger = CompensationLedger()
    let prefix = options.KeyPrefix

    /// <summary>
    /// Creates distributed admission over the given options and connection.
    /// </summary>
    /// <param name="options">The validated coordination options. Must not be null.</param>
    /// <param name="multiplexer">The shared Redis connection. Must not be null.</param>
    new(options: DistributedCoordinationOptions, multiplexer: IConnectionMultiplexer) =
        ArgumentNullException.ThrowIfNull(multiplexer)
        RedisDistributedLlmAdmission(options, Func<IDatabase>(fun () -> multiplexer.GetDatabase()))

    /// <summary>
    /// The deployment key prefix isolating this client's keys.
    /// </summary>
    member _.KeyPrefix: string = prefix

    /// <summary>
    /// Creates a client over a database factory for tests. Internal: unit
    /// tests prove argument validation without a Redis connection; the
    /// factory must not be invoked when validation fails.
    /// </summary>
    /// <param name="options">The coordination options. Must not be null.</param>
    /// <param name="getDatabase">The database factory. Must not be null.</param>
    /// <returns>A client over the factory.</returns>
    static member internal CreateForTests
        (options: DistributedCoordinationOptions, getDatabase: Func<IDatabase>)
        : RedisDistributedLlmAdmission =
        RedisDistributedLlmAdmission(options, getDatabase)

    static member private RequireText (value: string) (name: string) : string =
        if String.IsNullOrWhiteSpace value then
            raise (ArgumentException($"{name} must be non-empty.", name))

        value

    static member private RequirePositive (value: TimeSpan) (name: string) : TimeSpan =
        if value <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(name, $"{name} must be positive."))

        value

    member private _.Database() : IDatabase = getDatabase.Invoke()

    member private _.NowMillis() : int64 =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

    member private _.Keys(identity: string) : RedisKey[] * string * string * string =
        let active = RedisScripts.activeKey prefix identity
        let queue = RedisScripts.queueKey prefix identity
        let cooldown = RedisScripts.cooldownKey prefix identity

        ([|
            RedisKey.op_Implicit active
            RedisKey.op_Implicit queue
            RedisKey.op_Implicit cooldown
         |],
         active,
         queue,
         cooldown)

    member private this.ParseAcquire(result: RedisResult) : DistributedAdmissionOutcome =
        try
            let status = result.[0].ToString()
            let rankText = result.[1].ToString()

            let rank =
                match Int32.TryParse rankText with
                | true, value -> value
                | false, _ -> 0

            AdmissionParsing.toOutcome status rank
        with _ ->
            DistributedAdmissionOutcome.CooldownActive()

    static member private ParseBool(result: RedisResult) : bool =
        try
            match Int32.TryParse(result.ToString()) with
            | true, value -> value = 1
            | false, _ -> String.Equals(result.ToString(), "1", StringComparison.Ordinal)
        with _ ->
            false

    interface IDistributedLlmAdmission with

        member this.AcquireAsync(identity, ownerId, maxConcurrency, leaseTtl, waiterTtl, cancellationToken) =
            RedisDistributedLlmAdmission.RequireText identity "identity" |> ignore
            RedisDistributedLlmAdmission.RequireText ownerId "ownerId" |> ignore

            if maxConcurrency < 1 then
                raise (ArgumentOutOfRangeException("maxConcurrency", "maxConcurrency must be at least 1."))

            RedisDistributedLlmAdmission.RequirePositive leaseTtl "leaseTtl" |> ignore
            RedisDistributedLlmAdmission.RequirePositive waiterTtl "waiterTtl" |> ignore
            cancellationToken.ThrowIfCancellationRequested()

            task {
                let keys, activeKey, _, _ = this.Keys identity
                let nowMs = this.NowMillis()
                let operationId = $"acquire:{identity}:{ownerId}:{nowMs}"

                let values: RedisValue[] =
                    [|
                        RedisValue.op_Implicit ownerId
                        RedisValue.op_Implicit maxConcurrency
                        RedisValue.op_Implicit leaseTtl.TotalMilliseconds
                        RedisValue.op_Implicit waiterTtl.TotalMilliseconds
                        RedisValue.op_Implicit nowMs
                    |]

                try
                    let! result = this.Database().ScriptEvaluateAsync(RedisScripts.AcquireScript, keys, values)
                    return this.ParseAcquire result
                with :? RedisConnectionException as ex ->
                    // Unknown outcome: the script may have landed. Probe
                    // once and compensate a landed lease exactly once, then
                    // surface rather than silently dropping.
                    let probe =
                        Func<bool>(fun () ->
                            try
                                this
                                    .Database()
                                    .HashExists(RedisKey.op_Implicit activeKey, RedisValue.op_Implicit ownerId)
                            with _ ->
                                true)

                    let compensate =
                        Action(fun () ->
                            try
                                let db = this.Database()

                                db.HashDelete(RedisKey.op_Implicit activeKey, RedisValue.op_Implicit ownerId)
                                |> ignore
                            with _ ->
                                ())

                    ledger.Resolve(operationId, probe, compensate) |> ignore

                    raise (
                        InvalidOperationException(
                            $"Redis acquire outcome unknown and compensated for '{identity}'.",
                            ex
                        )
                    )

                    return DistributedAdmissionOutcome.CooldownActive()
            }

        member this.RenewAsync(identity, ownerId, leaseTtl, cancellationToken) =
            RedisDistributedLlmAdmission.RequireText identity "identity" |> ignore
            RedisDistributedLlmAdmission.RequireText ownerId "ownerId" |> ignore
            RedisDistributedLlmAdmission.RequirePositive leaseTtl "leaseTtl" |> ignore
            cancellationToken.ThrowIfCancellationRequested()

            task {
                let active = RedisScripts.activeKey prefix identity
                let keys = [| RedisKey.op_Implicit active |]
                let nowMs = this.NowMillis()

                let values: RedisValue[] =
                    [|
                        RedisValue.op_Implicit ownerId
                        RedisValue.op_Implicit leaseTtl.TotalMilliseconds
                        RedisValue.op_Implicit nowMs
                    |]

                try
                    let! result = this.Database().ScriptEvaluateAsync(RedisScripts.RenewScript, keys, values)
                    return RedisDistributedLlmAdmission.ParseBool result
                with :? RedisConnectionException as ex ->
                    let operationId = $"renew:{identity}:{ownerId}:{nowMs}"

                    let probe = Func<bool>(fun () -> false)

                    let compensate = Action(fun () -> ())

                    ledger.Resolve(operationId, probe, compensate) |> ignore
                    raise (InvalidOperationException($"Redis renew outcome unknown for '{identity}'.", ex))
                    return false
            }

        member this.ReleaseAsync(identity, ownerId, cancellationToken) =
            RedisDistributedLlmAdmission.RequireText identity "identity" |> ignore
            RedisDistributedLlmAdmission.RequireText ownerId "ownerId" |> ignore
            cancellationToken.ThrowIfCancellationRequested()

            task {
                let active = RedisScripts.activeKey prefix identity
                let queue = RedisScripts.queueKey prefix identity

                let keys =
                    [|
                        RedisKey.op_Implicit active
                        RedisKey.op_Implicit queue
                    |]

                let values: RedisValue[] = [| RedisValue.op_Implicit ownerId |]

                try
                    let! result = this.Database().ScriptEvaluateAsync(RedisScripts.ReleaseScript, keys, values)
                    return RedisDistributedLlmAdmission.ParseBool result
                with :? RedisConnectionException as ex ->
                    let nowMs = this.NowMillis()
                    let operationId = $"release:{identity}:{ownerId}:{nowMs}"

                    let probe = Func<bool>(fun () -> false)

                    let compensate = Action(fun () -> ())

                    ledger.Resolve(operationId, probe, compensate) |> ignore
                    raise (InvalidOperationException($"Redis release outcome unknown for '{identity}'.", ex))
                    return false
            }

        member this.CompleteAsync(identity, ownerId, cancellationToken) =
            RedisDistributedLlmAdmission.RequireText identity "identity" |> ignore
            RedisDistributedLlmAdmission.RequireText ownerId "ownerId" |> ignore
            cancellationToken.ThrowIfCancellationRequested()

            task {
                let active = RedisScripts.activeKey prefix identity
                let queue = RedisScripts.queueKey prefix identity

                let keys =
                    [|
                        RedisKey.op_Implicit active
                        RedisKey.op_Implicit queue
                    |]

                let values: RedisValue[] = [| RedisValue.op_Implicit ownerId |]

                try
                    let! result = this.Database().ScriptEvaluateAsync(RedisScripts.CompleteScript, keys, values)
                    return RedisDistributedLlmAdmission.ParseBool result
                with :? RedisConnectionException as ex ->
                    let nowMs = this.NowMillis()
                    let operationId = $"complete:{identity}:{ownerId}:{nowMs}"

                    let probe = Func<bool>(fun () -> false)

                    let compensate = Action(fun () -> ())

                    ledger.Resolve(operationId, probe, compensate) |> ignore
                    raise (InvalidOperationException($"Redis complete outcome unknown for '{identity}'.", ex))
                    return false
            }

        member this.StartCooldownAsync(identity, cooldown, cancellationToken) =
            RedisDistributedLlmAdmission.RequireText identity "identity" |> ignore
            RedisDistributedLlmAdmission.RequirePositive cooldown "cooldown" |> ignore
            cancellationToken.ThrowIfCancellationRequested()

            task {
                let key = RedisScripts.cooldownKey prefix identity
                let keys = [| RedisKey.op_Implicit key |]

                let values: RedisValue[] =
                    [|
                        RedisValue.op_Implicit cooldown.TotalMilliseconds
                    |]

                let! _ = this.Database().ScriptEvaluateAsync(RedisScripts.CooldownScript, keys, values)
                return ()
            }
