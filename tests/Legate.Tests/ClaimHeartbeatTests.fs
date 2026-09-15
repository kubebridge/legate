// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ClaimHeartbeatTests

open System
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Xunit

// Turn-claim lease heartbeat (issue 33): the renew step branches every
// lease outcome over scripted renew delegates, the loop waits on the
// injected ILlmDelay seam (an expiring lease renews immediately instead of
// waiting), and a TestClock-backed in-memory store proves the loop stops
// with lease loss once the clock passes expiry.

// ──────────────────────────────────────────────────────────────────────────
// Doubles

let tenant = TenantId.Create "acme"

let private sampleClaim (token: string) (attempt: int) (expiresAt: DateTimeOffset) : TurnClaim =
    {
        TurnId = TurnId.New()
        Token = token
        Owner = "owner-a"
        ExpiresAt = expiresAt
        Attempt = attempt
    }

let private liveClaim () : TurnClaim =
    sampleClaim "token-a" 1 (DateTimeOffset(2026, 2, 1, 0, 0, 0, TimeSpan.Zero))

/// A renew delegate scripted to a single outcome.
let private scriptRenew (outcome: TurnLeaseState) : TurnClaim -> CancellationToken -> Task<TurnLeaseState> =
    fun _ _ -> Task.FromResult outcome

let private neverCancelled () = false

let private cancelled () = true

/// Runs one heartbeat step to completion, blocking.
let private step
    (renew: TurnClaim -> CancellationToken -> Task<TurnLeaseState>)
    (claim: TurnClaim)
    (isCancelled: unit -> bool)
    : ClaimHeartbeat.ClaimHeartbeatDecision =
    ClaimHeartbeat.stepAsync renew claim isCancelled CancellationToken.None
    |> fun task -> task.GetAwaiter().GetResult()

/// An ILlmDelay logging every requested wait and advancing the test clock
/// by the scripted span, so loop tests assert exact renewal timings
/// without sleeping.
type LoggingDelay(log: ResizeArray<string>, advance: TimeSpan -> unit) =
    interface ILlmDelay with
        member _.Delay(delay, _) =
            log.Add($"delay:{delay.TotalSeconds}")
            advance delay
            Task.CompletedTask

// ──────────────────────────────────────────────────────────────────────────
// Step: every renewal outcome

[<Fact>]
let ``Renewed continues under the renewed claim`` () =
    let renewed = { liveClaim () with Token = "token-a2" }

    let decision =
        step (scriptRenew (TurnLeaseRenewed(renewed) :> TurnLeaseState)) (liveClaim ()) neverCancelled

    match decision with
    | ClaimHeartbeat.Continue claim -> claim.Token |> should equal "token-a2"
    | _ -> failwith "Expected Continue."

[<Fact>]
let ``Held continues under the held claim`` () =
    let decision =
        step (scriptRenew (TurnLeaseHeld(liveClaim ()) :> TurnLeaseState)) (liveClaim ()) neverCancelled

    match decision with
    | ClaimHeartbeat.Continue claim -> claim.Token |> should equal "token-a"
    | _ -> failwith "Expected Continue."

[<Fact>]
let ``Expiring renews now under the current claim`` () =
    let decision =
        step (scriptRenew (TurnLeaseExpiring(liveClaim ()) :> TurnLeaseState)) (liveClaim ()) neverCancelled

    match decision with
    | ClaimHeartbeat.RenewNow claim -> claim.Token |> should equal "token-a"
    | _ -> failwith "Expected RenewNow."

[<Fact>]
let ``Expired stops with lease loss`` () =
    let claim = liveClaim ()

    let decision =
        step (scriptRenew (TurnLeaseLost(claim.TurnId, "expired") :> TurnLeaseState)) claim neverCancelled

    decision |> should equal ClaimHeartbeat.StopLeaseLost

[<Fact>]
let ``Taken over stops with lease loss`` () =
    let claim = liveClaim ()

    let decision =
        step (scriptRenew (TurnLeaseLost(claim.TurnId, "takenOver") :> TurnLeaseState)) claim neverCancelled

    decision |> should equal ClaimHeartbeat.StopLeaseLost

[<Fact>]
let ``Missing stops with lease loss`` () =
    let claim = liveClaim ()

    let decision =
        step (scriptRenew (TurnLeaseMissing(claim.TurnId) :> TurnLeaseState)) claim neverCancelled

    decision |> should equal ClaimHeartbeat.StopLeaseLost

[<Fact>]
let ``Observed cancellation stops with cancellation on a live lease`` () =
    // Issue 35 owns the cancellation-request carrier behind
    // ObserveAndRenewClaim; until then the injected observer is the
    // branch, covered here in its stubbed form.
    let decision =
        step (scriptRenew (TurnLeaseRenewed(liveClaim ()) :> TurnLeaseState)) (liveClaim ()) cancelled

    decision |> should equal ClaimHeartbeat.StopCancelled

[<Fact>]
let ``Step rejects a null renewal`` () =
    let noRenew: TurnClaim -> CancellationToken -> Task<TurnLeaseState> =
        Unchecked.defaultof<_>

    let run () =
        ClaimHeartbeat.stepAsync noRenew (liveClaim ()) neverCancelled CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()
        |> ignore

    (fun () -> run ()) |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``Step fails loudly on a null lease state`` () =
    let decision () =
        step (scriptRenew (Unchecked.defaultof<TurnLeaseState>)) (liveClaim ()) neverCancelled
        |> ignore

    (fun () -> decision ()) |> should throw typeof<InvalidOperationException>

// ──────────────────────────────────────────────────────────────────────────
// Lease view

[<Fact>]
let ``View stays valid while live and flips on stop`` () =
    let clock = TestClock(DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))
    let claim = sampleClaim "token-a" 1 (clock.Instant.AddMinutes 2.0)
    let view = ClaimHeartbeat.ClaimLeaseView(clock, claim)

    view.IsValid() |> should equal true

    let renewed = { claim with Token = "token-a2" }
    view.Observe(ClaimHeartbeat.Continue renewed)
    view.Current.Token |> should equal "token-a2"
    view.IsValid() |> should equal true

    view.Observe(ClaimHeartbeat.StopLeaseLost)
    view.IsValid() |> should equal false

[<Fact>]
let ``View reads invalid once the clock passes expiry`` () =
    let clock = TestClock(DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))
    let claim = sampleClaim "token-a" 1 (clock.Instant.AddSeconds 30.0)
    let view = ClaimHeartbeat.ClaimLeaseView(clock, claim)

    view.IsValid() |> should equal true
    clock.Advance(TimeSpan.FromSeconds 31.0)
    view.IsValid() |> should equal false

// ──────────────────────────────────────────────────────────────────────────
// Loop over the delay seam

let private heartbeatOptions () : ClaimHeartbeat.ClaimHeartbeatOptions =
    {
        LeaseDuration = TimeSpan.FromSeconds 120.0
        RenewalInterval = TimeSpan.FromSeconds 30.0
    }

/// Runs the heartbeat loop over a scripted renew queue to completion,
/// logging every delay and renewal in order.
let private runScripted (outcomes: TurnLeaseState list) (isCancelled: unit -> bool) =
    let log = ResizeArray<string>()
    let remaining = ResizeArray<TurnLeaseState>(outcomes)

    let renew _ _ =
        log.Add("renew")

        if remaining.Count = 0 then
            failwith "The heartbeat renewed more often than scripted."

        let next = remaining[0]
        remaining.RemoveAt(0)
        Task.FromResult next

    let delay = LoggingDelay(log, ignore) :> ILlmDelay

    let clock = TestClock()

    let decision =
        ClaimHeartbeat.runAsync
            renew
            (liveClaim ())
            (heartbeatOptions ())
            clock
            delay
            isCancelled
            None
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    (decision, log |> List.ofSeq)

[<Fact>]
let ``Loop waits the interval between renewals and stops with lease loss`` () =
    let claim = liveClaim ()

    let outcomes =
        [
            TurnLeaseRenewed(claim) :> TurnLeaseState
            TurnLeaseRenewed(claim) :> TurnLeaseState
            TurnLeaseMissing(claim.TurnId) :> TurnLeaseState
        ]

    let decision, log = runScripted outcomes neverCancelled

    decision |> should equal ClaimHeartbeat.StopLeaseLost

    log
    |> should
        equal
        [
            "delay:30"
            "renew"
            "delay:30"
            "renew"
            "delay:30"
            "renew"
        ]

[<Fact>]
let ``Loop renews immediately on expiring without waiting`` () =
    let claim = liveClaim ()

    let outcomes =
        [
            TurnLeaseExpiring(claim) :> TurnLeaseState
            TurnLeaseRenewed(claim) :> TurnLeaseState
            TurnLeaseMissing(claim.TurnId) :> TurnLeaseState
        ]

    let decision, log = runScripted outcomes neverCancelled

    decision |> should equal ClaimHeartbeat.StopLeaseLost

    // No delay between the expiring renewal and its immediate re-renewal.
    log
    |> should
        equal
        [
            "delay:30"
            "renew"
            "renew"
            "delay:30"
            "renew"
        ]

[<Fact>]
let ``Loop stops with cancellation when the observer fires`` () =
    let claim = liveClaim ()

    let outcomes =
        [
            TurnLeaseRenewed(claim) :> TurnLeaseState
        ]

    let decision, _ = runScripted outcomes cancelled
    decision |> should equal ClaimHeartbeat.StopCancelled

[<Fact>]
let ``Loop rejects a renewal interval at half the lease`` () =
    let bad: ClaimHeartbeat.ClaimHeartbeatOptions =
        {
            LeaseDuration = TimeSpan.FromSeconds 60.0
            RenewalInterval = TimeSpan.FromSeconds 30.0
        }

    let run () =
        ClaimHeartbeat.runAsync
            (scriptRenew (TurnLeaseHeld(liveClaim ()) :> TurnLeaseState))
            (liveClaim ())
            bad
            (TestClock())
            (LoggingDelay(ResizeArray(), ignore) :> ILlmDelay)
            neverCancelled
            None
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()
        |> ignore

    (fun () -> run ()) |> should throw typeof<ArgumentOutOfRangeException>

// ──────────────────────────────────────────────────────────────────────────
// Loop against the real store under virtual time

/// A minimal Idle session row, mirroring the session actor suite sample.
let private sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "checkout"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
    }

[<Fact>]
let ``Loop stops with lease loss once the clock passes expiry`` () =
    let clock = TestClock(DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))
    let database = InMemoryDatabase(clock)
    let store = InMemorySessionStore(database) :> ISessionStore

    let session =
        store.CreateSession(tenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

    let payload = UserMessagePayload(UserMessage.Text("hi")) :> InboxPayload

    store.AppendInboxMessage(tenant, session.Id, payload, DeliveryMode.Queue, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()
    |> ignore

    let claim =
        match
            store.ClaimNextTurn(tenant, session.Id, "owner-a", TimeSpan.FromSeconds 120.0, CancellationToken.None)
            |> fun task -> task.GetAwaiter().GetResult()
        with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | _ -> failwith "Expected the claim to be granted."

    // The first wait jumps past the lease in one step (a healthy
    // heartbeat would renew forever, each renewal extending the lease),
    // so the renewal observes expiry and the loop stops, flipping the
    // backed view invalid.
    let log = ResizeArray<string>()

    let jumpPastLease (_: TimeSpan) =
        clock.Advance(TimeSpan.FromSeconds 200.0)

    let delay = LoggingDelay(log, jumpPastLease) :> ILlmDelay
    let view = ClaimHeartbeat.ClaimLeaseView(clock, claim)

    let decision =
        ClaimHeartbeat.runWithStoreAsync
            store
            tenant
            claim
            (heartbeatOptions ())
            clock
            delay
            neverCancelled
            (Some view)
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    decision |> should equal ClaimHeartbeat.StopLeaseLost
    log |> List.ofSeq |> should equal [ "delay:30" ]
    view.IsValid() |> should equal false

// ──────────────────────────────────────────────────────────────────────────
// Options from configuration

[<Fact>]
let ``Configured defaults satisfy the renewal bound`` () =
    // SessionsOptions carries the merged lease tuning (60 s lease, 15 s
    // renewal); the heartbeat derives from it so the under-half bound the
    // configuration validates always holds on the wire too.
    let sessions = SessionsOptions()
    sessions.Validate() |> should equal null

    let options = ClaimHeartbeat.fromSessions sessions
    options.LeaseDuration |> should equal sessions.LeaseDuration
    options.RenewalInterval |> should equal sessions.LeaseRenewalInterval

    (options.RenewalInterval < options.LeaseDuration.Divide 2.0)
    |> should equal true

[<Fact>]
let ``Options reject a renewal interval at half the lease`` () =
    let sessions = SessionsOptions()
    sessions.LeaseDuration <- TimeSpan.FromSeconds 60.0
    sessions.LeaseRenewalInterval <- TimeSpan.FromSeconds 30.0

    (fun () -> ClaimHeartbeat.fromSessions sessions |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

// ──────────────────────────────────────────────────────────────────────────
// Renewal while suspended (issue 36)

/// Reads a session's stored lifecycle state, blocking. Tests only read rows
/// they created, so a missing row is a test bug.
let private storedStateOf (store: ISessionStore) (sessionId: SessionId) : SessionState =
    match store.GetSession(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult() with
    | null -> failwith "Expected the session row to exist."
    | session -> session.State

[<Fact>]
let ``Renewal continues while the session is suspended`` () =
    // The heartbeat never consults session state: a session that suspended
    // to WaitingForInput keeps renewing under the same claim, so no cancel
    // fires on suspend. The loop below renews twice past a suspended row
    // before the scripted missing branch stops it.
    let clock = TestClock(DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))
    let database = InMemoryDatabase(clock)
    let store = InMemorySessionStore(database) :> ISessionStore

    let session =
        store.CreateSession(tenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

    store.UpdateSessionState(tenant, session.Id, SessionState.WaitingForInput, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()
    |> ignore

    let payload = UserMessagePayload(UserMessage.Text("run")) :> InboxPayload

    store.AppendInboxMessage(tenant, session.Id, payload, DeliveryMode.Queue, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()
    |> ignore

    // A suspended session still holds pending inbox work; the heartbeat's
    // renewals below land while the row reads WaitingForInput.
    let claim =
        match
            store.ClaimNextTurn(tenant, session.Id, "owner-a", TimeSpan.FromSeconds 120.0, CancellationToken.None)
            |> fun task -> task.GetAwaiter().GetResult()
        with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | :? TurnLeaseHeld as held -> held.Claim
        | _ -> failwith "Expected the suspend claim to be granted."

    let log = ResizeArray<string>()

    let renewedState = TurnLeaseRenewed(claim) :> TurnLeaseState
    let remaining = ResizeArray<TurnLeaseState>([ renewedState; renewedState ])

    let renew _ _ =
        log.Add("renew")

        if remaining.Count = 0 then
            Task.FromResult(TurnLeaseMissing(claim.TurnId) :> TurnLeaseState)
        else
            let next = remaining[0]
            remaining.RemoveAt(0)
            Task.FromResult(next)

    let delay = LoggingDelay(log, ignore) :> ILlmDelay

    let decision =
        ClaimHeartbeat.runAsync renew claim (heartbeatOptions ()) clock delay neverCancelled None CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    decision |> should equal ClaimHeartbeat.StopLeaseLost

    // Two renewals landed while the session stayed suspended, then the
    // missing branch stopped the loop: suspension never cancels renewal.
    (storedStateOf store session.Id) |> should equal SessionState.WaitingForInput

    log
    |> List.ofSeq
    |> should
        equal
        [
            "delay:30"
            "renew"
            "delay:30"
            "renew"
            "delay:30"
            "renew"
        ]
