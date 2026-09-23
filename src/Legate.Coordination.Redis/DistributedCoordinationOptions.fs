// SPDX-License-Identifier: Apache-2.0
namespace Legate.Coordination.Redis

open System

// Settings for distributed LLM admission, bound from the
// Legate:Llm:DistributedCoordination configuration section. A plain class
// with mutable properties and defaults, so hosts set properties before
// registering and the configuration binder only overrides the keys the host
// sets. The connection string carries no secret beyond the Redis endpoint
// itself and is never logged: exception messages name the section, never
// the connection value.

/// <summary>
/// How distributed LLM admission coordinates: local-only or through one
/// shared Redis instance. Bound from configuration; unknown values fail
/// binding.
/// </summary>
type DistributedCoordinationMode =

    /// <summary>
    /// No distributed coordination: the local coordinator admits in
    /// process. The default.
    /// </summary>
    | Disabled = 0

    /// <summary>
    /// Admit through one shared Redis instance: processes sharing the
    /// instance respect one combined concurrency limit.
    /// </summary>
    | Redis = 1

/// <summary>
/// Where distributed LLM admission lives and how it fails: the mode, the
/// Redis connection, the key prefix isolating one deployment's keys, and
/// the startup and failure switches. Bound from the
/// <c>Legate:Llm:DistributedCoordination</c> configuration section.
/// </summary>
type DistributedCoordinationOptions() =

    /// <summary>
    /// The configuration section the client binds from:
    /// <c>Legate:Llm:DistributedCoordination</c>.
    /// </summary>
    static member ConfigurationSectionPath = "Legate:Llm:DistributedCoordination"

    /// <summary>
    /// How admission coordinates. Defaults to
    /// <see cref="F:Legate.Coordination.Redis.DistributedCoordinationMode.Disabled" />:
    /// a single node coordinates locally.
    /// </summary>
    member val Mode: DistributedCoordinationMode = DistributedCoordinationMode.Disabled with get, set

    /// <summary>
    /// The Redis connection string, for example
    /// <c>127.0.0.1:6379</c>. Required when
    /// <see cref="P:Legate.Coordination.Redis.DistributedCoordinationOptions.Mode" />
    /// is Redis; ignored otherwise. Never logged.
    /// </summary>
    member val ConnectionString: string = "" with get, set

    /// <summary>
    /// The key prefix isolating one deployment's admission keys, for
    /// example <c>legate:llm</c>. Every script builds its keys as
    /// <c>{prefix}:v1:...</c> so a rolling upgrade never mixes script
    /// generations under one prefix. Must be non-empty.
    /// </summary>
    member val KeyPrefix: string = "legate:llm" with get, set

    /// <summary>
    /// Whether startup must prove Redis before reporting ready. Defaults
    /// to true: the startup canary (owned by the wiring issue) gates
    /// readiness. Kept here so the canary and the client read one set of
    /// options.
    /// </summary>
    member val StartupRequired: bool = true with get, set

    /// <summary>
    /// Whether admission fails closed when Redis is unavailable. Defaults
    /// to true: new work is rejected. When false the coordinator admits
    /// locally under bounded emergency limits (owned by the wiring
    /// issue). Kept here so the fail policy and the client read one set
    /// of options.
    /// </summary>
    member val FailClosed: bool = true with get, set

    /// <summary>
    /// Checks the options: the mode must be defined, the prefix
    /// non-empty without whitespace or wildcards, and the connection
    /// string non-empty when the mode is Redis.
    /// </summary>
    /// <returns>Null when the options are valid; otherwise the reason they are not.</returns>
    member this.Validate() : string | null =
        if
            this.Mode <> DistributedCoordinationMode.Disabled
            && this.Mode <> DistributedCoordinationMode.Redis
        then
            "DistributedCoordinationOptions.Mode must be Disabled or Redis."
        elif String.IsNullOrWhiteSpace this.KeyPrefix then
            "DistributedCoordinationOptions.KeyPrefix must be a non-empty key prefix."
        elif
            this.KeyPrefix.Contains(" ")
            || this.KeyPrefix.Contains("*")
            || this.KeyPrefix.Contains(":v1:")
        then
            "DistributedCoordinationOptions.KeyPrefix must not contain spaces, wildcards, or the :v1: generation tag."
        elif isNull (box this.ConnectionString) then
            "DistributedCoordinationOptions.ConnectionString must not be null: use the empty string when disabled."
        elif
            this.Mode = DistributedCoordinationMode.Redis
            && String.IsNullOrWhiteSpace this.ConnectionString
        then
            "DistributedCoordinationOptions.ConnectionString must be non-empty when Mode is Redis."
        else
            null
