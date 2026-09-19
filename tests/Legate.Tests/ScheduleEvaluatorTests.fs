// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ScheduleEvaluatorTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.Configuration
open Xunit

// Agent schedules (issue 121): the Schedules options knob and its binding,
// the shared cron/time-zone validator, and the evaluator pass over agents
// with enabled schedules under virtual time (FakeTimeProvider-free: the
// TestClock the in-memory database reads, never sleeps), including the
// two-instance exactly-once race with no external services.

let tenant = TenantId.Create "acme"

// Unchecked.defaultof<string> rather than a bare null literal: under
// Nullable=enable the literal trips F# nullness checking on string-typed
// parameters. Deliberate: this is the null string value for rejection cases.
let nullString = Unchecked.defaultof<string>

/// Builds an agent carrying an enabled schedule with the given cron, time
/// zone, and message.
let scheduledAgent (cron: string) (timeZone: string) (message: string) =
    {
        Id = AgentId.New()
        Tenant = tenant
        Name = "scheduled"
        Description = null
        Model = ModelReference.Parse("test/model-a")
        SystemPrompt = "You are on schedule."
        EnvironmentVariables = null
        PermissionDefaults = null
        ToolSelection = null
        PackageReference = null
        Enabled = true
        Schedule =
            {
                AgentSchedule.Cron = cron
                TimeZone = timeZone
                Message = message
                Enabled = true
            }
        RowVersion = 0UL
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
    }

/// A thread-safe recording fire callback standing in for the production
/// open-session-plus-prompt wiring: records every won firing with its
/// schedule message so tests assert exactly-once firing and zero loser
/// effects at the seam.
type RecordingFire() =
    let gate = obj ()
    let fires = ResizeArray<Agent * AgentSchedule * DateTimeOffset>()

    /// Fires once per won occurrence, recording the agent, schedule, and
    /// occurrence instant. Curried so tests pass it directly as the
    /// evaluator's fire callback.
    member _.Fire agent schedule occurrenceUtc (_: CancellationToken) : Task =
        task { lock gate (fun () -> fires.Add(agent, schedule, occurrenceUtc)) }

    /// The recorded firings in firing order.
    member _.Fires: IReadOnlyList<Agent * AgentSchedule * DateTimeOffset> =
        lock gate (fun () -> fires :> IReadOnlyList<Agent * AgentSchedule * DateTimeOffset>)

/// A fresh in-memory agent store over a TestClock plus the clock.
let createStore () =
    let clock = TestClock()
    let database = InMemoryDatabase(clock)
    InMemoryStoreFactory.agentStore database, clock

/// Saves the agent and returns the stored row.
let saveAgent (store: IAgentStore) (agent: Agent) =
    task {
        let! outcome = store.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

        match outcome with
        | :? AgentUpdated as updated -> return updated.Agent
        | _ -> return failwith "The agent save should have applied."
    }

/// Builds the Legate section from in-memory pairs, mirroring the binding
/// tests' helper.
let private buildSection (pairs: (string * string) seq) : IConfigurationSection =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build().GetSection("Legate")

// ──────────────────────────────────────────────────────────────────────────
// Schedule options

[<Fact>]
let ``Schedules default to a 60 second poll`` () =
    let options = ScheduleOptions()
    options.PollInterval |> should equal (TimeSpan.FromSeconds 60.0)
    options.Validate() |> should equal null
    LegateOptions().Schedules.Validate() |> should equal null

[<Fact>]
let ``Schedules reject zero and negative poll intervals`` () =
    ScheduleOptions(PollInterval = TimeSpan.Zero).Validate()
    |> should equal "PollInterval must be positive."

    ScheduleOptions(PollInterval = TimeSpan.FromSeconds -1.0).Validate()
    |> should equal "PollInterval must be positive."

[<Fact>]
let ``Binds Schedules PollInterval from configuration`` () =
    let section =
        buildSection
            [
                "Legate:Schedules:PollInterval", "90s"
            ]

    let options = LegateOptionsBinding.bind section
    options.Schedules.PollInterval |> should equal (TimeSpan.FromSeconds 90.0)
    options.Validate() |> should equal null

[<Fact>]
let ``Invalid Schedules PollInterval fails binding with its section path`` () =
    let section =
        buildSection
            [
                "Legate:Schedules:PollInterval", "0s"
            ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Schedules: PollInterval must be positive.")
    |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Cron and time-zone validation

[<Fact>]
let ``Valid 5-field crons validate`` () =
    for cron in
        [
            "* * * * *"
            "0 9 * * 1-5"
            "*/15 0 1,15 * 1-5"
            "0 0 29 2 *"
        ] do
        AgentScheduleRules.TryValidateCron cron |> should equal true
        AgentScheduleRules.ValidateCron cron |> should equal cron

[<Fact>]
let ``Invalid crons are rejected`` () =
    for cron in
        [
            "not a cron"
            "* * * * * *"
            "61 * * * *"
            ""
            "   "
        ] do
        AgentScheduleRules.TryValidateCron cron |> should equal false

    AgentScheduleRules.TryValidateCron nullString |> should equal false

    (fun () -> AgentScheduleRules.ValidateCron "not a cron" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> AgentScheduleRules.ValidateCron nullString |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``IANA and Windows-fallback time zones validate`` () =
    for zone in
        [
            "UTC"
            "Europe/Berlin"
            "America/New_York"
            "Eastern Standard Time"
            "W. Europe Standard Time"
        ] do
        AgentScheduleRules.TryValidateTimeZone zone |> should equal true
        AgentScheduleRules.ValidateTimeZone zone |> should equal zone
        AgentScheduleRules.ResolveTimeZone zone |> should not' (equal null)

[<Fact>]
let ``Unknown time zones are rejected`` () =
    for zone in [ "Mars/Olympus_Mons"; ""; "   " ] do
        AgentScheduleRules.TryValidateTimeZone zone |> should equal false

    AgentScheduleRules.TryValidateTimeZone nullString |> should equal false

    (fun () -> AgentScheduleRules.ValidateTimeZone "Mars/Olympus_Mons" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> AgentScheduleRules.ValidateTimeZone nullString |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``ValidateSchedule accepts null and rejects bad schedules with the typed error`` () =
    AgentScheduleRules.ValidateSchedule(null, AgentId.New())

    let agentId = AgentId.New()

    let badCron =
        {
            AgentSchedule.Cron = "bogus"
            TimeZone = "UTC"
            Message = "tick"
            Enabled = true
        }

    try
        AgentScheduleRules.ValidateSchedule(badCron, agentId)
        Assert.Fail("The bad cron should have thrown.")
    with :? InvalidAgentScheduleException as rejected ->
        rejected.AgentId |> should equal agentId
        rejected.Cron |> should equal "bogus"

    let badZone =
        {
            AgentSchedule.Cron = "* * * * *"
            TimeZone = "Mars/Olympus_Mons"
            Message = "tick"
            Enabled = true
        }

    try
        AgentScheduleRules.ValidateSchedule(badZone, agentId)
        Assert.Fail("The bad time zone should have thrown.")
    with :? InvalidAgentScheduleException as rejected ->
        rejected.AgentId |> should equal agentId
        rejected.TimeZone |> should equal "Mars/Olympus_Mons"

// ──────────────────────────────────────────────────────────────────────────
// Evaluator passes under virtual time

[<Fact>]
let ``Due occurrence fires once with the schedule message`` () =
    task {
        let store, clock = createStore ()
        let! stored = saveAgent store (scheduledAgent "* * * * *" "UTC" "standup")
        let fire = RecordingFire()

        let! fired = ScheduleEvaluator.passOnceAsync store tenant clock fire.Fire CancellationToken.None

        fired |> should equal 1
        fire.Fires.Count |> should equal 1

        let _, schedule, _ = fire.Fires[0]
        schedule.Message |> should equal "standup"

        // The won occurrence is consumed: a second pass fires nothing.
        let! again = ScheduleEvaluator.passOnceAsync store tenant clock fire.Fire CancellationToken.None
        again |> should equal 0
        fire.Fires.Count |> should equal 1

        return stored.Id |> ignore
    }

[<Fact>]
let ``Only the latest missed occurrence fires while older are consumed`` () =
    task {
        let store, clock = createStore ()
        let! stored = saveAgent store (scheduledAgent "* * * * *" "UTC" "tick")

        // Three occurrences pass with no sweep.
        clock.Advance(TimeSpan.FromMinutes 3.0)

        let fire = RecordingFire()

        let! fired = ScheduleEvaluator.passOnceAsync store tenant clock fire.Fire CancellationToken.None

        fired |> should equal 1
        fire.Fires.Count |> should equal 1

        let midnight = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

        let keyAt (instant: DateTimeOffset) =
            ScheduleEvaluator.occurrenceKey stored.Id "* * * * *" instant

        let consume key =
            store.TryConsumeScheduleOccurrence(tenant, stored.Id, key, midnight, CancellationToken.None)

        // The latest fired...
        let! latest = consume (keyAt (midnight.AddMinutes 3.0))
        latest :? ScheduleOccurrenceAlreadyConsumed |> should equal true

        // ...and every older missed occurrence was consumed without firing.
        for minutes in [ 1.0; 2.0 ] do
            let! older = consume (keyAt (midnight.AddMinutes minutes))
            older :? ScheduleOccurrenceAlreadyConsumed |> should equal true
    }

[<Fact>]
let ``Disabled schedules and schedule-less agents never fire`` () =
    task {
        let store, clock = createStore ()

        let disabledBase = scheduledAgent "* * * * *" "UTC" "tock"

        let disabled =
            { disabledBase with
                Schedule =
                    {
                        AgentSchedule.Cron = "* * * * *"
                        TimeZone = "UTC"
                        Message = "tock"
                        Enabled = false
                    }
            }

        let! _ = saveAgent store disabled

        let onDemandBase = scheduledAgent "* * * * *" "UTC" "tick"
        let! _ = saveAgent store { onDemandBase with Schedule = null }

        clock.Advance(TimeSpan.FromMinutes 5.0)

        let fire = RecordingFire()

        let! fired = ScheduleEvaluator.passOnceAsync store tenant clock fire.Fire CancellationToken.None

        fired |> should equal 0
        fire.Fires.Count |> should equal 0
    }

[<Fact>]
let ``Nothing due fires nothing`` () =
    task {
        // 2026 is not a leap year and the last Feb 29 sits beyond every
        // lookback window, so no occurrence is due.
        let store, clock = createStore ()
        let! _ = saveAgent store (scheduledAgent "0 0 29 2 *" "UTC" "leap")

        let fire = RecordingFire()

        let! fired = ScheduleEvaluator.passOnceAsync store tenant clock fire.Fire CancellationToken.None

        fired |> should equal 0
        fire.Fires.Count |> should equal 0
    }

/// An agent store serving agents the save path could never persist (they
/// predate validation): the evaluator must skip them without consuming.
type InvalidScheduleStore(agents: IReadOnlyList<Agent>) =
    let gate = obj ()
    let mutable consumes = 0

    /// How many consume calls the sweep attempted.
    member _.Consumes = lock gate (fun () -> consumes)

    interface IAgentStore with
        member _.GetAgent(_, _, _) =
            Task.FromResult Unchecked.defaultof<Agent>

        member _.ListAgents(_, _) =
            Task.FromResult(ResizeArray<Agent>() :> IReadOnlyList<Agent>)

        member _.UpdateIfUnchanged(_, _, _, _) =
            Task.FromException<AgentUpdateOutcome>(InvalidOperationException("The stub store is read-only."))

        member _.DeleteAgent(_, _, _) = Task.FromResult false

        member _.ListAgentsWithEnabledSchedules(_, _) = Task.FromResult agents

        member _.TryConsumeScheduleOccurrence(_, _, key, _, _) =
            lock gate (fun () -> consumes <- consumes + 1)

            Task.FromResult(ScheduleOccurrenceConsumed key :> ScheduleOccurrenceOutcome)

[<Fact>]
let ``Agents with invalid schedules are skipped without consuming`` () =
    task {
        let badCron = scheduledAgent "bogus" "UTC" "tick"
        let badZone = scheduledAgent "* * * * *" "Mars/Olympus_Mons" "tock"
        let listed = ResizeArray<Agent>([| badCron; badZone |]) :> IReadOnlyList<Agent>
        let stub = InvalidScheduleStore(listed)

        let fire = RecordingFire()
        let clock = TestClock()
        clock.Advance(TimeSpan.FromMinutes 5.0)

        let! fired = ScheduleEvaluator.passOnceAsync (stub :> IAgentStore) tenant clock fire.Fire CancellationToken.None

        fired |> should equal 0
        fire.Fires.Count |> should equal 0
        stub.Consumes |> should equal 0
    }

// ──────────────────────────────────────────────────────────────────────────
// Two-instance exactly-once race with no external services

[<Fact>]
let ``Two instances racing one occurrence fire exactly once`` () =
    task {
        let store, clock = createStore ()
        let! stored = saveAgent store (scheduledAgent "* * * * *" "UTC" "tick")

        clock.Advance(TimeSpan.FromMinutes 2.0)

        let fire = RecordingFire()

        let first =
            ScheduleEvaluator.passOnceAsync store tenant clock fire.Fire CancellationToken.None

        let second =
            ScheduleEvaluator.passOnceAsync store tenant clock fire.Fire CancellationToken.None

        let! results = Task.WhenAll(first, second)

        (results[0] + results[1]) |> should equal 1
        fire.Fires.Count |> should equal 1

        // The loser produced zero effects: exactly one firing was recorded
        // and the won occurrence key stays consumed.
        let _, _, won = fire.Fires[0]
        let key = ScheduleEvaluator.occurrenceKey stored.Id "* * * * *" won

        let! observed = store.TryConsumeScheduleOccurrence(tenant, stored.Id, key, won, CancellationToken.None)

        observed :? ScheduleOccurrenceAlreadyConsumed |> should equal true
    }
