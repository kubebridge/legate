// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open Legate.Testing
open Xunit

module TestClockTests =

    [<Fact>]
    let ``Advance moves the instant forward`` () =
        let clock = TestClock(DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))

        clock.Advance(TimeSpan.FromMinutes 30.)

        Assert.Equal(DateTimeOffset(2026, 1, 1, 0, 30, 0, TimeSpan.Zero), clock.Instant)

    [<Fact>]
    let ``Advance rejects moving backwards`` () =
        let clock = TestClock()

        Assert.Throws<ArgumentOutOfRangeException>(fun () -> clock.Advance(TimeSpan.FromMinutes -1.) |> ignore)
        |> ignore

    [<Fact>]
    let ``The TimeProvider surface reads the same instant`` () =
        let clock = TestClock()
        let provider = clock :> TimeProvider

        clock.Advance(TimeSpan.FromHours 1.)

        Assert.Equal(clock.Instant, provider.GetUtcNow())
