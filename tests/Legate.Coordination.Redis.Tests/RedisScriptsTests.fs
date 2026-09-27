// SPDX-License-Identifier: Apache-2.0
module Legate.Coordination.Redis.Tests.RedisScriptsTests

open FsUnit.Xunit
open Legate.Coordination.Redis
open Xunit

[<Fact>]
let ``Generation is v1`` () =
    RedisScripts.Generation |> should equal "v1"

[<Fact>]
let ``Key builders tag every key with the generation`` () =
    let prefix = "legate:llm"
    let identity = "openai/static"

    RedisScripts.activeKey prefix identity
    |> should equal "legate:llm:v1:active:openai/static"

    RedisScripts.queueKey prefix identity
    |> should equal "legate:llm:v1:queue:openai/static"

    RedisScripts.cooldownKey prefix identity
    |> should equal "legate:llm:v1:cooldown:openai/static"

[<Fact>]
let ``Key builders isolate prefixes`` () =
    let first = RedisScripts.activeKey "legate:a" "id"
    let second = RedisScripts.activeKey "legate:b" "id"
    first |> should not' (equal second)

[<Theory>]
[<InlineData("acquire")>]
[<InlineData("renew")>]
[<InlineData("release")>]
[<InlineData("complete")>]
[<InlineData("cooldown")>]
let ``Every script carries the generation tag`` (name: string) =
    let script =
        match name with
        | "acquire" -> RedisScripts.AcquireScript
        | "renew" -> RedisScripts.RenewScript
        | "release" -> RedisScripts.ReleaseScript
        | "complete" -> RedisScripts.CompleteScript
        | _ -> RedisScripts.CooldownScript

    script |> should not' (equal "")
    script.Contains(":v1:") |> should equal true

[<Fact>]
let ``Acquire prunes before admitting and fences cooldown`` () =
    RedisScripts.AcquireScript.Contains("ZREMRANGEBYSCORE") |> should equal true
    RedisScripts.AcquireScript.Contains("HGETALL") |> should equal true
    RedisScripts.AcquireScript.Contains("cooldown") |> should equal true

[<Fact>]
let ``Renew fences on the owner field`` () =
    RedisScripts.RenewScript.Contains("HEXISTS") |> should equal true

[<Fact>]
let ``Release and complete dequeue the waiter`` () =
    RedisScripts.ReleaseScript.Contains("ZREM") |> should equal true
    RedisScripts.CompleteScript.Contains("ZREM") |> should equal true

[<Fact>]
let ``Cooldown sets a millisecond TTL`` () =
    RedisScripts.CooldownScript.Contains("PSETEX") |> should equal true
