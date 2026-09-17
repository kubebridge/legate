// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.Packages
open Legate.Testing
open Microsoft.Extensions.DependencyInjection
open Xunit

// Lease behavior tests for the shared AgentPackageLeaseService (issue
// 107): renew-interval observation, lost and over-timeout renewal
// cancellation, release on fault paths, and foreign-token fencing with
// zero loser effects. Virtual time throughout: RecordingDelay observes
// the waits and FakeClock lapses the leases, never sleeps.

/// The public service over a manual clock: acquire, renew, verify,
// release, and WithLease success, fault, conflict, and mid-run lapse.
module PackageLeaseServiceTests =

    let tenant = TenantId.Create "acme"
    let agent = AgentId.New()

    let freshService (clock: FakeClock) =
        let delay = RecordingDelay()
        (AgentPackageLeaseService(clock, delay) :> IAgentPackageLeaseService, delay)

    let testOptions () =
        let options = PackageLeaseOptions()
        options.RenewInterval <- TimeSpan.FromSeconds 10.
        options.RenewalTimeout <- TimeSpan.FromSeconds 5.
        options

    [<Fact>]
    let ``Acquire grants, renew mints a fresh token, verify and release follow`` () =
        task {
            let clock = FakeClock()
            let service, _ = freshService clock

            let! first = service.Acquire(tenant, agent, "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)

            let lease =
                match first with
                | :? PackageLeaseAcquired as acquired -> acquired.Lease
                | _ -> failwith "expected PackageLeaseAcquired"

            Assert.Equal("worker-a", lease.Owner)

            let! held = service.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

            match held with
            | :? PackageLeaseUnavailable as unavailable -> Assert.Equal("leaseHeld", unavailable.Reason)
            | _ -> failwith "expected PackageLeaseUnavailable"

            let! renewed = service.Renew(lease, TimeSpan.FromMinutes 5., CancellationToken.None)

            let fresh =
                match renewed with
                | :? PackageLeaseRenewed as ok -> ok.Lease
                | _ -> failwith "expected PackageLeaseRenewed"

            Assert.NotEqual<string>(lease.Token, fresh.Token)

            let! stale = service.Renew(lease, TimeSpan.FromMinutes 5., CancellationToken.None)

            match stale with
            | :? PackageLeaseLost as lost -> Assert.Equal("staleToken", lost.Reason)
            | _ -> failwith "expected PackageLeaseLost for the superseded token"

            let! verified = service.Verify(fresh, CancellationToken.None)
            Assert.True(verified)

            let! released = service.Release(fresh, CancellationToken.None)
            Assert.True(released)

            let! after = service.Verify(fresh, CancellationToken.None)
            Assert.False(after)
        }

    [<Fact>]
    let ``An expired lease frees the package for the next writer`` () =
        task {
            let clock = FakeClock()
            let service, _ = freshService clock

            let! first = service.Acquire(tenant, agent, "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)
            let lease = (first :?> PackageLeaseAcquired).Lease

            clock.Advance(TimeSpan.FromMinutes 6.)

            let! expired = service.Verify(lease, CancellationToken.None)
            Assert.False(expired)

            let! second = service.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

            match second with
            | :? PackageLeaseAcquired -> ()
            | _ -> failwith "expected PackageLeaseAcquired after expiry"
        }

    [<Fact>]
    let ``A foreign token renews and releases nothing`` () =
        task {
            let clock = FakeClock()
            let service, _ = freshService clock

            let! first = service.Acquire(tenant, agent, "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)
            let heldLease = (first :?> PackageLeaseAcquired).Lease

            let forged =
                AgentPackageLease(tenant, agent, "attacker", "forged", heldLease.ExpiresAt)

            let! lost = service.Renew(forged, TimeSpan.FromMinutes 5., CancellationToken.None)

            match lost with
            | :? PackageLeaseLost as failure -> Assert.Equal("staleToken", failure.Reason)
            | _ -> failwith "expected PackageLeaseLost for the forged token"

            let! released = service.Release(forged, CancellationToken.None)
            Assert.False(released)

            // Zero loser effects: the held lease is untouched.
            let! stillHeld = service.Verify(heldLease, CancellationToken.None)
            Assert.True(stillHeld)

            let! freed = service.Release(heldLease, CancellationToken.None)
            Assert.True(freed)
        }

    [<Fact>]
    let ``A loser after takeover changes nothing`` () =
        task {
            let clock = FakeClock()
            let service, _ = freshService clock

            let! first = service.Acquire(tenant, agent, "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)
            let loser = (first :?> PackageLeaseAcquired).Lease

            clock.Advance(TimeSpan.FromMinutes 6.)

            let! second = service.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

            let winner =
                match second with
                | :? PackageLeaseAcquired as acquired -> acquired.Lease
                | _ -> failwith "expected the winner to acquire after expiry"

            let! lost = service.Renew(loser, TimeSpan.FromMinutes 5., CancellationToken.None)

            match lost with
            | :? PackageLeaseLost -> ()
            | _ -> failwith "expected PackageLeaseLost for the loser"

            let! loserRelease = service.Release(loser, CancellationToken.None)
            Assert.False(loserRelease)

            let! winnerHeld = service.Verify(winner, CancellationToken.None)
            Assert.True(winnerHeld)
        }

    [<Fact>]
    let ``WithLease runs the work and releases on success and on fault`` () =
        task {
            let clock = FakeClock()
            let service, _ = freshService clock
            let options = testOptions ()

            let! result =
                service.WithLease(
                    tenant,
                    agent,
                    "worker-a",
                    TimeSpan.FromMinutes 5.,
                    options,
                    Func<CancellationToken, Task<int>>(fun _ -> Task.FromResult 42),
                    CancellationToken.None
                )

            Assert.Equal(42, result)

            let! after = service.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

            let holding =
                match after with
                | :? PackageLeaseAcquired as acquired -> acquired.Lease
                | _ -> failwith "expected the lease to be released after success"

            let! releasedHolding = service.Release(holding, CancellationToken.None)
            Assert.True(releasedHolding)

            try
                let! _ =
                    service.WithLease(
                        tenant,
                        agent,
                        "worker-a",
                        TimeSpan.FromMinutes 5.,
                        options,
                        Func<CancellationToken, Task<int>>(fun _ ->
                            Task.FromException<int>(InvalidOperationException("boom"))),
                        CancellationToken.None
                    )

                failwith "expected InvalidOperationException"
            with :? InvalidOperationException ->
                ()

            let! freed = service.Acquire(tenant, agent, "worker-c", TimeSpan.FromMinutes 5., CancellationToken.None)

            match freed with
            | :? PackageLeaseAcquired -> ()
            | _ -> failwith "expected the lease to be released after the fault"
        }

    [<Fact>]
    let ``WithLease runs unit work and releases`` () =
        task {
            let clock = FakeClock()
            let service, _ = freshService clock
            let mutable ran = false

            do!
                service.WithLease(
                    tenant,
                    agent,
                    "worker-a",
                    TimeSpan.FromMinutes 5.,
                    null,
                    Func<CancellationToken, Task>(fun _ -> task { ran <- true } :> Task),
                    CancellationToken.None
                )

            Assert.True(ran)

            let! after = service.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

            match after with
            | :? PackageLeaseAcquired -> ()
            | _ -> failwith "expected the lease to be released after unit work"
        }

    [<Fact>]
    let ``WithLease throws PackageLeaseException when another writer holds the lease`` () =
        task {
            let clock = FakeClock()
            let service, _ = freshService clock

            let! _ = service.Acquire(tenant, agent, "holder", TimeSpan.FromMinutes 5., CancellationToken.None)

            try
                let! _ =
                    service.WithLease(
                        tenant,
                        agent,
                        "worker-a",
                        TimeSpan.FromMinutes 5.,
                        null,
                        Func<CancellationToken, Task<int>>(fun _ -> Task.FromResult 1),
                        CancellationToken.None
                    )

                failwith "expected PackageLeaseException"
            with :? PackageLeaseException as exn ->
                Assert.Equal("acquire", exn.Operation)
                Assert.Equal(agent, exn.AgentId)
                Assert.Equal("worker-a", exn.Owner)
        }

    [<Fact>]
    let ``WithLease cancels the work when the lease lapses mid-run`` () =
        task {
            let clock = FakeClock()
            let service, _ = freshService clock
            let options = testOptions ()
            let mutable observedCancel = false

            let work (token: CancellationToken) =
                task {
                    use _registration = token.Register(Action(fun () -> observedCancel <- true))

                    // Lapse the lease before parking: the next renewal
                    // lands lost and must cancel this token. The park
                    // itself waits on the system clock: the manual clock
                    // above exists only to lapse the lease.
                    clock.Advance(TimeSpan.FromMinutes 10.)
                    do! Task.Delay(Timeout.InfiniteTimeSpan, token)
                    return 0
                }

            try
                let! _ =
                    service.WithLease(
                        tenant,
                        agent,
                        "worker-a",
                        TimeSpan.FromMinutes 5.,
                        options,
                        Func<CancellationToken, Task<int>>(fun token -> work token),
                        CancellationToken.None
                    )

                failwith "expected PackageLeaseException"
            with :? PackageLeaseException as exn ->
                Assert.Equal("renew", exn.Operation)
                Assert.Equal(agent, exn.AgentId)
                Assert.Equal("worker-a", exn.Owner)

            Assert.True(observedCancel)

            // The failed run released best-effort: the package leases again.
            let! after = service.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

            match after with
            | :? PackageLeaseAcquired -> ()
            | _ -> failwith "expected the lease to be free after the lost renewal"
        }

/// The internal renew loop over scripted delegates: the interval the
/// RecordingDelay observes, and lost and over-timeout renewals cancelling
/// parked work. The public lapse fact above proves the same paths
/// surface PackageLeaseException through WithLease.
module PackageLeaseLoopTests =

    let tenant = TenantId.Create "acme"
    let agent = AgentId.New()

    let leaseAt (clock: FakeClock) =
        AgentPackageLease(tenant, agent, "worker-a", "token-1", clock.GetUtcNow().Add(TimeSpan.FromMinutes 5.))

    // Parks the work on the loop's token and observes cancellation through
    // the work's own path: the OperationCanceledException handler signals
    // the TCS synchronously before the work task completes, so awaiting the
    // loop outcome (which awaited the work) makes the assertion below
    // deterministic with no sleeps. A BCL Register callback is intentionally
    // not used: its invocation ordering relative to the awaiting
    // continuation differs across SDK bands and raced on CI.
    let parkedWork (cancelled: TaskCompletionSource<unit>) (token: CancellationToken) =
        task {
            try
                do! Task.Delay(Timeout.InfiniteTimeSpan, token)
                return 0
            with :? OperationCanceledException ->
                cancelled.TrySetResult(()) |> ignore
                return raise (OperationCanceledException(token))
        }

    [<Fact>]
    let ``The renew loop waits the interval and carries the fresh lease`` () =
        task {
            let clock = FakeClock()
            let delay = RecordingDelay()
            let renewed = TaskCompletionSource<AgentPackageLease>()

            let renew (current: AgentPackageLease) (_: TimeSpan) (_: CancellationToken) =
                task {
                    renewed.TrySetResult(current) |> ignore
                    return PackageLeaseRenewed(current) :> PackageLeaseRenewal
                }

            let work (_: CancellationToken) =
                task {
                    let! _ = renewed.Task
                    return 7
                }

            let! outcome =
                PackageLeaseLoop.runAsync
                    renew
                    (leaseAt clock)
                    (TimeSpan.FromMinutes 5.)
                    (TimeSpan.FromSeconds 10.)
                    (TimeSpan.FromSeconds 5.)
                    clock
                    delay
                    work
                    CancellationToken.None

            match outcome with
            | PackageLeaseLoop.Completed(result, _) -> Assert.Equal(7, result)
            | PackageLeaseLoop.Failed(_, reason) -> failwith $"expected completion, got failure {reason}"

            Assert.Contains(TimeSpan.FromSeconds 10., delay.Recorded)
        }

    [<Fact>]
    let ``A lost renewal cancels the work and reports the reason`` () =
        task {
            let clock = FakeClock()
            let delay = RecordingDelay()
            let cancelled = TaskCompletionSource<unit>()

            let renew (_: AgentPackageLease) (_: TimeSpan) (_: CancellationToken) =
                Task.FromResult(PackageLeaseLost("staleToken") :> PackageLeaseRenewal)

            let! outcome =
                PackageLeaseLoop.runAsync
                    renew
                    (leaseAt clock)
                    (TimeSpan.FromMinutes 5.)
                    (TimeSpan.FromSeconds 10.)
                    (TimeSpan.FromSeconds 5.)
                    clock
                    delay
                    (parkedWork cancelled)
                    CancellationToken.None

            match outcome with
            | PackageLeaseLoop.Failed(_, reason) -> Assert.Equal("lost:staleToken", reason)
            | PackageLeaseLoop.Completed _ -> failwith "expected the lost renewal to fail the loop"

            Assert.True(cancelled.Task.IsCompleted)
            Assert.Contains(TimeSpan.FromSeconds 10., delay.Recorded)
        }

    [<Fact>]
    let ``An over-timeout renewal cancels the work even when it lands`` () =
        task {
            let clock = FakeClock()
            let delay = RecordingDelay()
            let cancelled = TaskCompletionSource<unit>()

            let renew (current: AgentPackageLease) (_: TimeSpan) (_: CancellationToken) =
                task {
                    // A slow renewal the clock can see: six seconds
                    // against a five-second budget.
                    clock.Advance(TimeSpan.FromSeconds 6.)
                    return PackageLeaseRenewed(current) :> PackageLeaseRenewal
                }

            let! outcome =
                PackageLeaseLoop.runAsync
                    renew
                    (leaseAt clock)
                    (TimeSpan.FromMinutes 5.)
                    (TimeSpan.FromSeconds 10.)
                    (TimeSpan.FromSeconds 5.)
                    clock
                    delay
                    (parkedWork cancelled)
                    CancellationToken.None

            match outcome with
            | PackageLeaseLoop.Failed(_, reason) -> Assert.Equal("timeout", reason)
            | PackageLeaseLoop.Completed _ -> failwith "expected the slow renewal to fail the loop"

            Assert.True(cancelled.Task.IsCompleted)
        }

/// The DI surface: the service resolves over the container's clock and
/// delay seam, and over the machine clock when the container holds
/// neither.
module PackageLeaseRegistrationTests =

    [<Fact>]
    let ``AddAgentPackageLeaseService resolves over the container seams`` () =
        task {
            let services = ServiceCollection()
            let clock = FakeClock()
            services.AddSingleton<TimeProvider>(clock) |> ignore
            services.AddSingleton<ILlmDelay>(RecordingDelay() :> ILlmDelay) |> ignore
            PackageLeaseServiceExtensions.AddAgentPackageLeaseService(services) |> ignore

            use provider = services.BuildServiceProvider()
            let service = provider.GetRequiredService<IAgentPackageLeaseService>()

            let tenant = TenantId.Create "acme"

            let! first =
                service.Acquire(tenant, AgentId.New(), "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)

            match first with
            | :? PackageLeaseAcquired -> ()
            | _ -> failwith "expected PackageLeaseAcquired from the registered service"
        }

    [<Fact>]
    let ``AddAgentPackageLeaseService falls back to the machine clock`` () =
        task {
            let services = ServiceCollection()

            PackageLeaseServiceCollectionExtensions.AddAgentPackageLeaseService(services)
            |> ignore

            use provider = services.BuildServiceProvider()
            let service = provider.GetRequiredService<IAgentPackageLeaseService>()

            let tenant = TenantId.Create "acme"

            let! first =
                service.Acquire(tenant, AgentId.New(), "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)

            match first with
            | :? PackageLeaseAcquired -> ()
            | _ -> failwith "expected PackageLeaseAcquired from the fallback service"
        }
