// SPDX-License-Identifier: Apache-2.0
namespace Legate.Coordination.Redis

// Atomic Lua scripts behind the distributed admission client. Every key is
// built as {prefix}:v1:{name}:{identity} so a rolling upgrade never mixes
// script generations under one prefix: the v1 generation tag is baked into
// the key builder and echoed in every script body, and the unit tests assert
// both. All scripts are atomic (single EVAL): the acquire prunes expired
// leases and waiters before admitting, renew/release/complete fence on the
// owner field, and cooldown short-circuits acquire while set.

/// <summary>
/// Key layout and Lua bodies for distributed admission. Internal: the
/// client calls through these; hosts see only
/// <see cref="T:Legate.Coordination.Redis.IDistributedLlmAdmission" />.
/// </summary>
module internal RedisScripts =

    /// <summary>
    /// The script generation baked into every key and every script body:
    /// a rolling upgrade under a fresh prefix never meets an older
    /// generation's keys.
    /// </summary>
    [<Literal>]
    let Generation = "v1"

    /// <summary>
    /// Builds the lease hash key for one identity:
    /// <c>{prefix}:v1:active:{identity}</c>. Holds owner to expiry-millis
    /// fields for the currently admitted leases.
    /// </summary>
    /// <param name="prefix">The deployment key prefix. Must not be null.</param>
    /// <param name="identity">The coordination identity. Must not be null.</param>
    /// <returns>The active-lease key.</returns>
    let activeKey (prefix: string) (identity: string) : string =
        $"{prefix}:{Generation}:active:{identity}"

    /// <summary>
    /// Builds the waiter queue key for one identity:
    /// <c>{prefix}:v1:queue:{identity}</c>. A sorted set of waiter to
    /// enqueue-millis entries in FIFO order.
    /// </summary>
    /// <param name="prefix">The deployment key prefix. Must not be null.</param>
    /// <param name="identity">The coordination identity. Must not be null.</param>
    /// <returns>The waiter-queue key.</returns>
    let queueKey (prefix: string) (identity: string) : string =
        $"{prefix}:{Generation}:queue:{identity}"

    /// <summary>
    /// Builds the cooldown key for one identity:
    /// <c>{prefix}:v1:cooldown:{identity}</c>. Present while the identity
    /// refuses new work after an HTTP 429.
    /// </summary>
    /// <param name="prefix">The deployment key prefix. Must not be null.</param>
    /// <param name="identity">The coordination identity. Must not be null.</param>
    /// <returns>The cooldown key.</returns>
    let cooldownKey (prefix: string) (identity: string) : string =
        $"{prefix}:{Generation}:cooldown:{identity}"

    /// <summary>
    /// Admits one waiter or queues it: prunes expired leases and expired
    /// waiters, refuses while the cooldown key exists, re-admits a holder,
    /// admits under the limit, otherwise enqueues in FIFO order.
    /// Keys: active, queue, cooldown. Args: owner, limit, leaseTtlMs,
    /// waiterTtlMs, nowMs. Returns a two-element array: the outcome
    /// (acquired, queued, or cooldown) and the queue rank (0 when
    /// acquired or cooling down). Generation v1.
    /// </summary>
    let AcquireScript =
        """
-- Generation v1: keys are {prefix}:v1:active:{identity}, {prefix}:v1:queue:{identity}, {prefix}:v1:cooldown:{identity}.
if redis.call("EXISTS", KEYS[3]) == 1 then
  return {"cooldown", 0}
end
local nowMs = tonumber(ARGV[5])
local leaseTtlMs = tonumber(ARGV[3])
local waiterTtlMs = tonumber(ARGV[4])
local limit = tonumber(ARGV[2])
local owner = ARGV[1]
local entries = redis.call("HGETALL", KEYS[1])
for i = 1, #entries, 2 do
  local expiry = tonumber(entries[i + 1])
  if expiry ~= nil and nowMs > expiry then
    redis.call("HDEL", KEYS[1], entries[i])
  end
end
redis.call("ZREMRANGEBYSCORE", KEYS[2], 0, nowMs - waiterTtlMs)
if redis.call("HEXISTS", KEYS[1], owner) == 1 then
  return {"acquired", 0}
end
local count = redis.call("HLEN", KEYS[1])
if count < limit then
  redis.call("ZREM", KEYS[2], owner)
  redis.call("HSET", KEYS[1], owner, nowMs + leaseTtlMs)
  return {"acquired", 0}
else
  redis.call("ZADD", KEYS[2], nowMs, owner)
  local rank = redis.call("ZRANK", KEYS[2], owner)
  return {"queued", rank}
end
"""

    /// <summary>
    /// Renews one owner's lease: extends the expiry only when the owner
    /// field exists, so another owner can never renew a foreign lease.
    /// Keys: active. Args: owner, leaseTtlMs, nowMs. Returns 1 when
    /// renewed, 0 when fenced or missing. Generation v1.
    /// </summary>
    let RenewScript =
        """
-- Generation v1: keys are {prefix}:v1:active:{identity}.
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
  return 0
end
local nowMs = tonumber(ARGV[3])
local leaseTtlMs = tonumber(ARGV[2])
redis.call("HSET", KEYS[1], ARGV[1], nowMs + leaseTtlMs)
return 1
"""

    /// <summary>
    /// Releases one owner's slot: deletes the lease field and dequeues
    /// the waiter entry, so the next waiter can acquire. A foreign owner
    /// removes only its own queue entry when present. Keys: active,
    /// queue. Args: owner. Returns 1 when a lease or queue entry was
    /// removed, 0 when fenced. Generation v1.
    /// </summary>
    let ReleaseScript =
        """
-- Generation v1: keys are {prefix}:v1:active:{identity}, {prefix}:v1:queue:{identity}.
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
  local removed = redis.call("ZREM", KEYS[2], ARGV[1])
  if removed == 1 then
    return 1
  else
    return 0
  end
end
redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
return 1
"""

    /// <summary>
    /// Completes one owner's lease: same fencing as release, kept as its
    /// own script so usage settlement can ride along later without
    /// callers changing keys. Keys: active, queue. Args: owner. Returns
    /// 1 when a lease or queue entry was removed, 0 when fenced.
    /// Generation v1.
    /// </summary>
    let CompleteScript =
        """
-- Generation v1: keys are {prefix}:v1:active:{identity}, {prefix}:v1:queue:{identity}.
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
  local removed = redis.call("ZREM", KEYS[2], ARGV[1])
  if removed == 1 then
    return 1
  else
    return 0
  end
end
redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
return 1
"""

    /// <summary>
    /// Starts a cooldown: sets the cooldown key with a millisecond TTL so
    /// acquire short-circuits while it exists. Keys: cooldown. Args:
    /// ttlMs. Returns 1. Generation v1.
    /// </summary>
    let CooldownScript =
        """
-- Generation v1: keys are {prefix}:v1:cooldown:{identity}.
redis.call("PSETEX", KEYS[1], tonumber(ARGV[1]), "1")
return 1
"""
