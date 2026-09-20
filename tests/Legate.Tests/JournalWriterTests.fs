// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.JournalWriterTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// Journal writer (issue 48): the sole append path sanitizes every event
// (table-driven secret shapes to [REDACTED]) and bounds it deterministically
// (32K text chars with a marker, 64 KiB per-event estimate, 64 metadata
// fields, never a drop), then appends under the claim fence with bounded
// retries and a typed write result. The takeover facts prove the fenced-out
// loser appends nothing at the writer level and through the session actor,
// while the winner's writes land afterward.

// ──────────────────────────────────────────────────────────────────────────
// Fixtures

let tenant = TenantId.Create "acme"

let private startInstant = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

let private lease = TimeSpan.FromSeconds 120.0

let private noSequence = Nullable<int64>()

/// Fresh clock-backed stores; every fact owns its database.
let private createStores () =
    let clock = TestClock(startInstant)
    let database = InMemoryDatabase(clock)
    let sessions = InMemorySessionStore(database) :> ISessionStore
    let events = InMemorySessionEventStore(database) :> ISessionEventStore
    (clock, sessions, events)

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
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private createSession (store: ISessionStore) : Session =
    store.CreateSession(tenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

let private appendUser (store: ISessionStore) (sessionId: SessionId) (text: string) : unit =
    let payload = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload

    store.AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()
    |> ignore

/// Claims the next turn, failing the test unless the store grants it.
let private claimTurn (store: ISessionStore) (sessionId: SessionId) (owner: string) : TurnClaim =
    match
        store.ClaimNextTurn(tenant, sessionId, owner, lease, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnLeaseRenewed as renewed -> renewed.Claim
    | state -> failwith $"Expected a granted claim, observed %s{state.GetType().Name}."

let private textEvent (sessionId: SessionId) (turnId: TurnId) (text: string) : SessionEvent =
    TextDeltaEvent(sessionId, turnId, noSequence, startInstant, text) :> SessionEvent

let private batchOf (event: SessionEvent) : IReadOnlyList<SessionEvent> =
    ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

let private storedState (store: ISessionStore) (sessionId: SessionId) : SessionState =
    match box (store.GetSession(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult()) with
    | :? Session as session -> session.State
    | _ -> failwith "Expected the session row to exist."

/// Counts Append calls, delegating everything to the inner store.
type private CountingEventStore(inner: ISessionEventStore) =
    let mutable calls = 0

    /// How many Append calls reached the store.
    member _.AppendCalls = calls

    interface ISessionEventStore with
        member _.Append(tenant, sessionId, token, events, cancellationToken) =
            calls <- calls + 1
            inner.Append(tenant, sessionId, token, events, cancellationToken)

        member _.Replay(tenant, sessionId, cursor, limit, cancellationToken) =
            inner.Replay(tenant, sessionId, cursor, limit, cancellationToken)

        member _.TryClaimCleanup(tenant, sessionId, owner, duration, cancellationToken) =
            inner.TryClaimCleanup(tenant, sessionId, owner, duration, cancellationToken)

        member _.CompleteCleanup(tenant, sessionId, token, archiveLocation, cancellationToken) =
            inner.CompleteCleanup(tenant, sessionId, token, archiveLocation, cancellationToken)

        member _.DeferCleanup(tenant, sessionId, token, cancellationToken) =
            inner.DeferCleanup(tenant, sessionId, token, cancellationToken)

/// Fails the first N appends with the given exception, then delegates.
type private FlakyEventStore(inner: ISessionEventStore, failures: int, failure: Exception) =
    let mutable calls = 0

    /// How many Append calls arrived, failed or not.
    member _.AppendCalls = calls

    interface ISessionEventStore with
        member _.Append(tenant, sessionId, token, events, cancellationToken) =
            calls <- calls + 1

            if calls <= failures then
                Task.FromException<EventAppendOutcome>(failure)
            else
                inner.Append(tenant, sessionId, token, events, cancellationToken)

        member _.Replay(tenant, sessionId, cursor, limit, cancellationToken) =
            inner.Replay(tenant, sessionId, cursor, limit, cancellationToken)

        member _.TryClaimCleanup(tenant, sessionId, owner, duration, cancellationToken) =
            inner.TryClaimCleanup(tenant, sessionId, owner, duration, cancellationToken)

        member _.CompleteCleanup(tenant, sessionId, token, archiveLocation, cancellationToken) =
            inner.CompleteCleanup(tenant, sessionId, token, archiveLocation, cancellationToken)

        member _.DeferCleanup(tenant, sessionId, token, cancellationToken) =
            inner.DeferCleanup(tenant, sessionId, token, cancellationToken)

/// A store honouring cancellation on Append: the writer must propagate the
/// cancellation instead of settling it into a JournalFailed result.
type private CancelHonoringStore() =
    interface ISessionEventStore with
        member _.Append(_, _, _, _, cancellationToken) =
            task {
                cancellationToken.ThrowIfCancellationRequested()
                return Unchecked.defaultof<EventAppendOutcome>
            }

        member _.Replay(_, sessionId, _, _, _) =
            Task.FromResult(EventReplayEndOfStream sessionId :> EventReplayOutcome)

        member _.TryClaimCleanup(_, sessionId, _, _, _) =
            Task.FromResult(EventCleanupNotClaimable(sessionId, "notSupported") :> EventCleanupState)

        member _.CompleteCleanup(_, sessionId, _, _, _) =
            Task.FromResult(EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)

        member _.DeferCleanup(_, sessionId, _, _) =
            Task.FromResult(EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)

// ──────────────────────────────────────────────────────────────────────────
// Redaction shapes

[<Fact>]
let ``Provider keys redact to REDACTED`` () =
    let openai = JournalWriter.redactText "key sk-abcdefgh12345678 here"
    openai |> should equal "key [REDACTED] here"

    let anthropic = JournalWriter.redactText "key sk-ant-xyz1234567890abcdef here"
    anthropic |> should equal "key [REDACTED] here"

[<Fact>]
let ``GitHub and AWS shapes redact`` () =
    let github = JournalWriter.redactText "token ghp_abcdefgh1234567890 here"
    github |> should equal "token [REDACTED] here"

    let aws = JournalWriter.redactText "id AKIAIOSFODNN7EXAMPLE here"
    aws |> should equal "id [REDACTED] here"

[<Fact>]
let ``Bearer tokens redact`` () =
    let bearer =
        JournalWriter.redactText "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.c2lnbmF0dXJl"

    (bearer.Contains "[REDACTED]") |> should equal true
    (bearer.Contains "eyJhbGciOiJIUzI1NiJ9") |> should equal false

[<Fact>]
let ``Quoted-label JSON secrets redact`` () =
    // Round-1 review: the labelled rule required [:=] immediately after the
    // label, so JSON shapes ("label": "value") passed through verbatim.
    let doubleSpaced = JournalWriter.redactText "{\"password\": \"hunter2\"}"
    (doubleSpaced.Contains "[REDACTED]") |> should equal true
    (doubleSpaced.Contains "hunter2") |> should equal false

    let doubleTight = JournalWriter.redactText "{\"secret\":\"hunter2\"}"
    (doubleTight.Contains "[REDACTED]") |> should equal true
    (doubleTight.Contains "hunter2") |> should equal false

    let singleQuoted = JournalWriter.redactText "{ 'token': 'abc123xyz' }"
    (singleQuoted.Contains "[REDACTED]") |> should equal true
    (singleQuoted.Contains "abc123xyz") |> should equal false

    let tokenSpaced = JournalWriter.redactText "{ \"token\": \"abc123xyz\" }"
    (tokenSpaced.Contains "[REDACTED]") |> should equal true
    (tokenSpaced.Contains "abc123xyz") |> should equal false

[<Fact>]
let ``Basic auth headers and URI credentials redact`` () =
    let basic = JournalWriter.redactText "Authorization: Basic dXNlcjpwYXNz"
    (basic.Contains "[REDACTED]") |> should equal true
    (basic.Contains "dXNlcjpwYXNz") |> should equal false

    let uri = JournalWriter.redactText "connect mongodb://user:p%40ssword@host/db now"
    (uri.Contains "[REDACTED]") |> should equal true
    (uri.Contains "p%40ssword") |> should equal false

    let bareHost = JournalWriter.redactText "connect mongodb://host/db now"
    bareHost |> should equal "connect mongodb://host/db now"

[<Fact>]
let ``Labelled secrets redact with mixed case and quotes`` () =
    JournalWriter.redactText "api_key=hunter2" |> should equal "[REDACTED]"
    JournalWriter.redactText "API_KEY: \"hunter2\"" |> should equal "[REDACTED]"
    JournalWriter.redactText "password=hunter2" |> should equal "[REDACTED]"

    JournalWriter.redactText "Claim Token: 01J9Z8X7C6V5B4N3M2L1K0PQ"
    |> should equal "[REDACTED]"

    JournalWriter.redactText "session-token=abc123" |> should equal "[REDACTED]"

[<Fact>]
let ``PEM blocks redact`` () =
    let key =
        "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA7b\n-----END RSA PRIVATE KEY-----"

    JournalWriter.redactText $"header {key} trailer"
    |> should equal "header [REDACTED] trailer"

[<Fact>]
let ``Environment assignments redact while harmless lines pass`` () =
    let text = "host=db-1\nSTRIPE_SECRET_KEY=rk_live_abc123XYZ\nPATH=/usr/bin:/bin"

    JournalWriter.redactText text
    |> should equal "host=db-1\n[REDACTED]\nPATH=/usr/bin:/bin"

[<Fact>]
let ``Connection-string passwords redact`` () =
    JournalWriter.redactText "Server=db-1;Password=hunter2;Database=app"
    |> should equal "Server=db-1;[REDACTED];Database=app"

[<Fact>]
let ``Ordinary text and bare identifiers pass through`` () =
    // Bare words without a label/value shape are not secrets; bare ULIDs
    // share the claim-token shape, so only a labelled token redacts.
    let text =
        "The meeting is at noon. Please reset your password before Friday. The token expired."

    JournalWriter.redactText text |> should equal text

    let ulid = "01J9Z8X7C6V5B4N3M2L1K0PQ"

    JournalWriter.redactText $"turn {ulid} settled"
    |> should equal $"turn {ulid} settled"

    JournalWriter.redactText "prefix sk-x" |> should equal "prefix sk-x"

[<Fact>]
let ``Every text-bearing kind redacts`` () =
    let sessionId = SessionId.New()
    let turnId = TurnId.New()

    let kinds: (string * SessionEvent * (SessionEvent -> string | null)) list =
        [
            ("Text",
             TextDeltaEvent(sessionId, turnId, noSequence, startInstant, "pwd=hunter2") :> SessionEvent,
             fun event -> (event :?> TextDeltaEvent).Text)
            ("Reasoning",
             ReasoningDeltaEvent(sessionId, turnId, noSequence, startInstant, "pwd=hunter2") :> SessionEvent,
             fun event -> (event :?> ReasoningDeltaEvent).Text)
            ("Output",
             ToolCallOutputEvent(sessionId, turnId, noSequence, startInstant, "c1", "pwd=hunter2") :> SessionEvent,
             fun event -> (event :?> ToolCallOutputEvent).Output)
            ("Error",
             ToolCallCompletedEvent(sessionId, turnId, noSequence, startInstant, "c1", "pwd=hunter2") :> SessionEvent,
             fun event -> (event :?> ToolCallCompletedEvent).Error)
            ("Question",
             QuestionAskedEvent(sessionId, turnId, noSequence, startInstant, "q1", "pwd=hunter2") :> SessionEvent,
             fun event -> (event :?> QuestionAskedEvent).Question)
            ("Answer",
             QuestionAnsweredEvent(sessionId, turnId, noSequence, startInstant, "q1", "pwd=hunter2") :> SessionEvent,
             fun event -> (event :?> QuestionAnsweredEvent).Answer)
            ("AbortReason",
             TurnAbortedEvent(sessionId, turnId, noSequence, startInstant, StopCause.ExplicitAbort, "pwd=hunter2")
             :> SessionEvent,
             fun event -> (event :?> TurnAbortedEvent).Reason)
            ("FailReason",
             TurnFailedEvent(sessionId, turnId, noSequence, startInstant, "pwd=hunter2") :> SessionEvent,
             fun event -> (event :?> TurnFailedEvent).Reason)
            ("SkipReason",
             SkillInvalidEvent(sessionId, turnId, noSequence, startInstant, "deploy", "pwd=hunter2") :> SessionEvent,
             fun event -> (event :?> SkillInvalidEvent).Reason)
        ]

    for _, event, field in kinds do
        let redacted = JournalWriter.sanitizeEvent event

        match box (field redacted) with
        | :? string as value ->
            (value.Contains "[REDACTED]") |> should equal true
            (value.Contains "hunter2") |> should equal false
        | _ -> failwith "Expected the redacted text field."

    // Message parts redact through the same table.
    let parts =
        ResizeArray<AIContent>(
            [|
                TextContent("pwd=hunter2") :> AIContent
            |]
        )
        :> IReadOnlyList<AIContent>

    let messageEvent =
        UserMessageEvent(sessionId, turnId, noSequence, startInstant, UserMessage(parts, null))

    let redactedMessage = JournalWriter.sanitizeEvent messageEvent :?> UserMessageEvent
    let redactedPart = redactedMessage.Message.Parts[0] :?> TextContent
    (redactedPart.Text.Contains "[REDACTED]") |> should equal true
    (redactedPart.Text.Contains "hunter2") |> should equal false

    // Non-text fields are never text-bearing: ids, names, and counts pass.
    let usage =
        UsageEvent(sessionId, turnId, noSequence, startInstant, 3L, 5L) :> SessionEvent

    let kept = JournalWriter.sanitizeEvent usage :?> UsageEvent
    kept.InputTokens |> should equal 3L
    kept.OutputTokens |> should equal 5L

    let started =
        ToolCallStartedEvent(sessionId, turnId, noSequence, startInstant, "c1", "exec") :> SessionEvent

    let keptStarted = JournalWriter.sanitizeEvent started :?> ToolCallStartedEvent
    keptStarted.ToolName |> should equal "exec"

    // The skip reason redacts like any failure reason; the skill name is
    // an identifier and passes through verbatim.
    let skipped =
        SkillInvalidEvent(sessionId, turnId, noSequence, startInstant, "deploy", "pwd=hunter2") :> SessionEvent

    let keptSkipped = JournalWriter.sanitizeEvent skipped :?> SkillInvalidEvent
    keptSkipped.SkillName |> should equal "deploy"

[<Fact>]
let ``Null text fields stay null`` () =
    let sessionId = SessionId.New()
    let turnId = TurnId.New()

    let nullText = Unchecked.defaultof<string>

    let delta =
        TextDeltaEvent(sessionId, turnId, noSequence, startInstant, nullText) :> SessionEvent

    let kept = JournalWriter.sanitizeEvent delta :?> TextDeltaEvent
    (isNull (box kept.Text)) |> should equal true

    let completed =
        ToolCallCompletedEvent(sessionId, turnId, noSequence, startInstant, "c1", nullText) :> SessionEvent

    let keptCompleted = JournalWriter.sanitizeEvent completed :?> ToolCallCompletedEvent
    (isNull (box keptCompleted.Error)) |> should equal true

    JournalWriter.redactText Unchecked.defaultof<string> |> should equal null

[<Fact>]
let ``User metadata stays verbatim`` () =
    // #42 boundary: untrusted tagging is prompt-render-only, so the journal
    // keeps metadata keys and values untouched; only secret shapes inside
    // text fields redact, and reserved keys are never stripped here.
    let sessionId = SessionId.New()
    let turnId = TurnId.New()

    let metadata =
        Dictionary<string, string>(
            dict
                [
                    ("api_key", "hunter2")
                    ("legate.shadow", "x")
                ]
        )
        :> IReadOnlyDictionary<string, string>

    let parts =
        ResizeArray<AIContent>([| TextContent("hello") :> AIContent |]) :> IReadOnlyList<AIContent>

    let event =
        UserMessageEvent(sessionId, turnId, noSequence, startInstant, UserMessage(parts, metadata))

    let kept = JournalWriter.sanitizeEvent event :?> UserMessageEvent

    match box kept.Message.Metadata with
    | :? IReadOnlyDictionary<string, string> as metadata ->
        metadata["api_key"] |> should equal "hunter2"
        metadata["legate.shadow"] |> should equal "x"
        metadata.Count |> should equal 2
    | _ -> failwith "Expected user metadata to survive sanitizing."

// ──────────────────────────────────────────────────────────────────────────
// Bounds

[<Fact>]
let ``Long text truncates with the marker`` () =
    let sessionId = SessionId.New()
    let turnId = TurnId.New()
    let text = String('a', 40000)

    let bounded =
        JournalWriter.boundEvent (textEvent sessionId turnId text) :?> TextDeltaEvent

    bounded.Text.Length
    |> should equal (JournalWriter.MaxTextChars + JournalWriter.TruncationMarker.Length)

    bounded.Text.EndsWith(JournalWriter.TruncationMarker, StringComparison.Ordinal)
    |> should equal true

    bounded.Text.Substring(0, 100) |> should equal (String('a', 100))
    bounded.SessionId |> should equal sessionId
    bounded.TurnId |> should equal turnId

[<Fact>]
let ``Exactly-at-limit text passes through`` () =
    let text = String('b', JournalWriter.MaxTextChars)

    let bounded =
        JournalWriter.boundEvent (textEvent (SessionId.New()) (TurnId.New()) text) :?> TextDeltaEvent

    bounded.Text |> should equal text
    (bounded.Text.Contains JournalWriter.TruncationMarker) |> should equal false

[<Fact>]
let ``Metadata trims to 64 entries deterministically`` () =
    let metadata =
        Dictionary<string, string>(
            dict
                [
                    for i in 0..69 -> $"m%02d{i}", $"v%02d{i}"
                ]
        )
        :> IReadOnlyDictionary<string, string>

    let parts =
        ResizeArray<AIContent>([| TextContent("hi") :> AIContent |]) :> IReadOnlyList<AIContent>

    let event =
        UserMessageEvent(SessionId.New(), TurnId.New(), noSequence, startInstant, UserMessage(parts, metadata))

    let bounded = JournalWriter.boundEvent event :?> UserMessageEvent

    match box bounded.Message.Metadata with
    | :? IReadOnlyDictionary<string, string> as metadata ->
        metadata.Count |> should equal JournalWriter.MaxMetadataFields
        metadata["m00"] |> should equal "v00"
        metadata["m63"] |> should equal "v63"
        (metadata.ContainsKey "m64") |> should equal false
    | _ -> failwith "Expected trimmed metadata to survive bounding."

[<Fact>]
let ``UserMessage text parts bound below 64 KiB`` () =
    // Round-1 review: boundEvent never reduced UserMessageEvent text parts,
    // so a 100K-char part exited at ~100270 bytes. Parts now truncate and
    // shrink symmetric with every other text-bearing kind.
    let parts =
        ResizeArray<AIContent>(
            [|
                TextContent(String('c', 100000)) :> AIContent
            |]
        )
        :> IReadOnlyList<AIContent>

    let event =
        UserMessageEvent(SessionId.New(), TurnId.New(), noSequence, startInstant, UserMessage(parts, null))

    let bounded = JournalWriter.boundEvent event :?> UserMessageEvent

    JournalWriter.estimateEventBytes bounded <= JournalWriter.MaxEventBytes
    |> should equal true

    let part = bounded.Message.Parts[0] :?> TextContent

    (part.Text.EndsWith(JournalWriter.TruncationMarker, StringComparison.Ordinal))
    |> should equal true

    (part.Text.Length < 100000) |> should equal true

[<Fact>]
let ``Escape-heavy text shrinks below 64 KiB without dropping`` () =
    // 20K control chars serialise to ~120 KiB of \u00XX escapes: under the
    // char bound but over the byte bound, so the shrink loop engages.
    let text = String(char 0x1f, 20000)

    let bounded =
        JournalWriter.boundEvent (textEvent (SessionId.New()) (TurnId.New()) text) :?> TextDeltaEvent

    JournalWriter.estimateEventBytes bounded <= JournalWriter.MaxEventBytes
    |> should equal true

    (bounded.Text.EndsWith(JournalWriter.TruncationMarker, StringComparison.Ordinal))
    |> should equal true

    (bounded.Text.Length > 0) |> should equal true
    (bounded.Text.Length < 20000) |> should equal true

[<Fact>]
let ``Bounded events sit strictly below the store limit`` () =
    // The writer and the store share the UTF-8 JSON estimator: a bounded
    // event lands under a 64 KiB store limit that rejects the raw shape.
    let clock = TestClock(startInstant)
    let options = InMemoryStoreOptions()
    options.MaxEventBytes <- JournalWriter.MaxEventBytes
    let database = InMemoryDatabase(clock, options)
    let store = InMemorySessionStore(database) :> ISessionStore
    let events = InMemorySessionEventStore(database) :> ISessionEventStore

    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"

    let raw = textEvent session.Id claim.TurnId (String('z', 100000))

    Assert.Throws<EventLimitExceededException>(fun () ->
        events.Append(tenant, session.Id, claim.Token, batchOf raw, CancellationToken.None).GetAwaiter().GetResult()
        |> ignore)
    |> ignore

    let result =
        JournalWriter.appendAsync store events tenant session.Id claim (batchOf raw) CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    match result with
    | JournalWriter.JournalAppended stamped ->
        stamped.Count |> should equal 1
        let landed = stamped[0] :?> TextDeltaEvent

        (landed.Text.EndsWith(JournalWriter.TruncationMarker, StringComparison.Ordinal))
        |> should equal true

        (JournalWriter.estimateEventBytes landed <= JournalWriter.MaxEventBytes)
        |> should equal true
    | _ -> failwith "Expected the bounded append to land."

// ──────────────────────────────────────────────────────────────────────────
// Append outcomes and bounded retries

[<Fact>]
let ``Live claim appends stamped and redacted events`` () =
    let _, store, events = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"

    let result =
        JournalWriter.appendAsync
            store
            events
            tenant
            session.Id
            claim
            (batchOf (textEvent session.Id claim.TurnId "leaked api_key=hunter2 here"))
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    match result with
    | JournalWriter.JournalAppended stamped ->
        stamped.Count |> should equal 1
        stamped[0].Sequence.Value |> should equal 1L
        let landed = stamped[0] :?> TextDeltaEvent
        (landed.Text.Contains "[REDACTED]") |> should equal true
        (landed.Text.Contains "hunter2") |> should equal false
    | _ -> failwith "Expected the append to land."

[<Fact>]
let ``Fenced-out claim writes nothing and never calls the store`` () =
    let clock, store, inner = createStores ()
    let counting = CountingEventStore(inner)
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"
    clock.Advance(TimeSpan.FromSeconds 121.0)

    let result =
        JournalWriter.appendAsync
            store
            (counting :> ISessionEventStore)
            tenant
            session.Id
            claim
            (batchOf (textEvent session.Id claim.TurnId "hello"))
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    match result with
    | JournalWriter.JournalRejected reason -> reason |> should equal JournalWriter.StaleClaimReason
    | _ -> failwith "Expected the fenced-out append to reject."

    counting.AppendCalls |> should equal 0

    let replay =
        inner.Replay(tenant, session.Id, 0L, 10, CancellationToken.None).GetAwaiter().GetResult()

    match replay with
    | :? EventReplayEndOfStream -> ()
    | :? EventReplayPage as page when page.Events.Count = 0 -> ()
    | _ -> failwith "Expected an empty journal after the fenced-out append."

[<Fact>]
let ``Stale token rejects without retry`` () =
    let _, store, inner = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let counting = CountingEventStore(inner)

    let result =
        JournalWriter.appendWithTokenAsync
            (counting :> ISessionEventStore)
            tenant
            session.Id
            "bogus-token"
            (batchOf (textEvent session.Id (TurnId.New()) "hello"))
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    // The store fences the unknown token before anything lands: one call,
    // no retry, no write.
    match result with
    | JournalWriter.JournalRejected reason -> reason |> should equal JournalWriter.StaleClaimReason
    | _ -> failwith "Expected the stale token to reject."

    counting.AppendCalls |> should equal 1

    let replay =
        inner.Replay(tenant, session.Id, 0L, 10, CancellationToken.None).GetAwaiter().GetResult()

    match replay with
    | :? EventReplayEndOfStream -> ()
    | :? EventReplayPage as page when page.Events.Count = 0 -> ()
    | _ -> failwith "Expected an empty journal after the rejected append."

[<Fact>]
let ``Transient failures retry bounded then land`` () =
    let _, store, inner = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"
    let flaky = FlakyEventStore(inner, 2, InvalidOperationException("transient blip"))

    let result =
        JournalWriter.appendAsync
            store
            (flaky :> ISessionEventStore)
            tenant
            session.Id
            claim
            (batchOf (textEvent session.Id claim.TurnId "hello"))
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    match result with
    | JournalWriter.JournalAppended stamped -> stamped.Count |> should equal 1
    | _ -> failwith "Expected the retried append to land."

    flaky.AppendCalls |> should equal 3

[<Fact>]
let ``Exhausted retries fail with the typed reason and no partial write`` () =
    let _, store, inner = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"

    let flaky =
        FlakyEventStore(inner, 10, InvalidOperationException("persistent outage"))

    let result =
        JournalWriter.appendAsync
            store
            (flaky :> ISessionEventStore)
            tenant
            session.Id
            claim
            (batchOf (textEvent session.Id claim.TurnId "hello"))
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    match result with
    | JournalWriter.JournalFailed reason -> reason |> should equal "The journal append failed after 3 attempts."
    | _ -> failwith "Expected the exhausted append to fail typed."

    flaky.AppendCalls |> should equal JournalWriter.MaxAppendAttempts

    let replay =
        inner.Replay(tenant, session.Id, 0L, 10, CancellationToken.None).GetAwaiter().GetResult()

    match replay with
    | :? EventReplayEndOfStream -> ()
    | :? EventReplayPage as page when page.Events.Count = 0 -> ()
    | _ -> failwith "Expected no partial write after the exhausted append."

[<Fact>]
let ``Limit breach fails immediately without retry`` () =
    let _, store, inner = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"

    let breach =
        EventLimitExceededException("perEventBytes", 65536L, 100000L, "An event exceeds the limit.")

    let flaky = FlakyEventStore(inner, 10, breach)

    let result =
        JournalWriter.appendAsync
            store
            (flaky :> ISessionEventStore)
            tenant
            session.Id
            claim
            (batchOf (textEvent session.Id claim.TurnId "hello"))
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    // Deterministic refusals never burn the retry budget.
    match result with
    | JournalWriter.JournalFailed reason ->
        reason
        |> should equal "The journal append was rejected by the event store: perEventBytes."
    | _ -> failwith "Expected the limit breach to fail typed."

    flaky.AppendCalls |> should equal 1

[<Fact>]
let ``Null and empty batches raise before touching the store`` () =
    let _, store, inner = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"
    let counting = CountingEventStore(inner)

    Assert.Throws<ArgumentNullException>(fun () ->
        JournalWriter.appendAsync
            store
            (counting :> ISessionEventStore)
            tenant
            session.Id
            claim
            (Unchecked.defaultof<IReadOnlyList<SessionEvent>>)
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()
        |> ignore)
    |> ignore

    Assert.Throws<ArgumentException>(fun () ->
        JournalWriter.appendAsync
            store
            (counting :> ISessionEventStore)
            tenant
            session.Id
            claim
            (ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>)
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()
        |> ignore)
    |> ignore

    counting.AppendCalls |> should equal 0

[<Fact>]
let ``Cancellation propagates instead of failing typed`` () =
    use cancelled = new CancellationTokenSource()
    cancelled.Cancel()

    Assert.Throws<OperationCanceledException>(fun () ->
        JournalWriter.appendWithTokenAsync
            (CancelHonoringStore() :> ISessionEventStore)
            tenant
            (SessionId.New())
            "token"
            (batchOf (textEvent (SessionId.New()) (TurnId.New()) "hello"))
            cancelled.Token
        |> fun task -> task.GetAwaiter().GetResult()
        |> ignore)
    |> ignore

// ──────────────────────────────────────────────────────────────────────────
// Takeover: the loser appends nothing

[<Fact>]
let ``Takeover loser appends nothing while the winner lands`` () =
    let clock, store, events = createStores ()
    let session = createSession store
    appendUser store session.Id "first"
    appendUser store session.Id "second"

    // Owner A holds the first turn; the clock lapses its lease and owner B
    // takes over on the second message.
    let loser = claimTurn store session.Id "owner-a"
    clock.Advance(TimeSpan.FromSeconds 121.0)
    let winner = claimTurn store session.Id "owner-b"

    winner.TurnId |> should not' (equal loser.TurnId)

    let loserResult =
        JournalWriter.appendAsync
            store
            events
            tenant
            session.Id
            loser
            (batchOf (textEvent session.Id loser.TurnId "leaked api_key=hunter2"))
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    match loserResult with
    | JournalWriter.JournalRejected reason -> reason |> should equal JournalWriter.StaleClaimReason
    | _ -> failwith "Expected the loser to reject."

    // Zero journal effects from the loser: the leak never landed either.
    let replay =
        events.Replay(tenant, session.Id, 0L, 10, CancellationToken.None).GetAwaiter().GetResult()

    match replay with
    | :? EventReplayEndOfStream -> ()
    | :? EventReplayPage as page when page.Events.Count = 0 -> ()
    | _ -> failwith "Expected an empty journal after the loser's append."

    let winnerResult =
        JournalWriter.appendAsync
            store
            events
            tenant
            session.Id
            winner
            (batchOf (textEvent session.Id winner.TurnId "winner hi"))
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    match winnerResult with
    | JournalWriter.JournalAppended stamped -> stamped[0].Sequence.Value |> should equal 1L
    | _ -> failwith "Expected the winner to land."

// ──────────────────────────────────────────────────────────────────────────
// Through the session actor

/// Starts a local actor system for one test.
let private createSystem () : ActorSystem = LocalActorSystem.createSystem ()

/// Terminates a test system, bounding the drain.
let private stopSystem (system: ActorSystem) : unit =
    system.Terminate() |> ignore
    system.WhenTerminated.Wait(TimeSpan.FromSeconds 10.0) |> ignore

/// Spins until the condition holds or the timeout elapses.
let private waitUntil (timeout: TimeSpan) (condition: unit -> bool) : bool =
    let deadline = DateTimeOffset.UtcNow + timeout
    let mutable satisfied = condition ()

    while not satisfied && DateTimeOffset.UtcNow < deadline do
        Thread.Sleep(25)
        satisfied <- condition ()

    satisfied

/// Builds one question-suspension cursor for the scripted runner.
let private testCursor (requestId: string) (question: string) : TurnLoop.TurnLoopSuspension =
    let args = Dictionary<string, obj>() :> IDictionary<string, obj>

    {
        RequestId = requestId
        ToolName = TurnLoop.AskUserToolName
        ToolCallId = "c1"
        Kind = TurnLoop.QuestionSuspension
        QuestionText = question
        QuestionOptions = []
        HistorySnapshot = ResizeArray<ChatMessage>() :> IList<ChatMessage>
        InputTokens = 3L
        OutputTokens = 5L
        Iterations = 1
        PendingCall = FunctionCallContent("c1", TurnLoop.AskUserToolName, args)
        Nested = None
    }

/// A Suspended completion parking on the given cursor.
let private suspendedOn (cursor: TurnLoop.TurnLoopSuspension) : TurnLoop.TurnLoopCompletion =
    {
        Result =
            {
                AssistantText = ""
                Status = TurnStatus.Suspended
                Iterations = cursor.Iterations
                Usage =
                    {
                        InputTokens = cursor.InputTokens
                        OutputTokens = cursor.OutputTokens
                    }
                Outcome = null
            }
        HasPendingInjects = false
        Suspension = Some cursor
    }

/// Spawns a suspendable actor over the given stores with the given journal
/// token, running the scripted completion once.
let private spawnWriterActor
    (system: ActorSystem)
    (store: ISessionStore)
    (journal: ISessionEventStore)
    (sessionId: SessionId)
    (token: string)
    (completion: TurnLoop.TurnLoopCompletion)
    (settled: ResizeArray<TurnResult>)
    : IActorRef =
    let unusedResult =
        {
            AssistantText = "unused"
            Status = TurnStatus.Completed
            Iterations = 0
            Usage = { InputTokens = 0L; OutputTokens = 0L }
            Outcome = null
        }

    let baseProps: SessionActorProps =
        {
            Store = store
            Tenant = tenant
            SessionId = sessionId
            RunTurn = (fun _ _ -> Task.FromResult(unusedResult))
            OnTurnSettled = Some(fun result -> lock settled (fun () -> settled.Add(result)))
            OnInjectJournaled = None
            Logger = null
            Compact = None
        }

    let runner: SessionActor.SuspendableRunner =
        fun _ _ _ _ _ _ _ -> Task.FromResult(completion)

    let deps: SessionActor.SuspendDeps =
        {
            EventStore = journal
            Delay = TurnLoopTests.NeverDelay() :> ILlmDelay
            AskTimeout = TimeSpan.FromMinutes 5.0
            JournalToken = token
            RunSuspendable = runner
            ReprimeJournal = None
            RefreshCompact = None
            AgentStore = null
        }

    spawn system $"journal-{Guid.NewGuid():N}" (SessionActor.behaviorWithSuspend baseProps deps)

[<Fact>]
let ``Suspended turn journals the redacted suspend event`` () =
    use system = createSystem ()
    let _, store, journal = createStores ()
    let session = createSession store
    appendUser store session.Id "first"
    let claim = claimTurn store session.Id "owner-a"
    let settled = ResizeArray<TurnResult>()

    let actor =
        spawnWriterActor
            system
            store
            journal
            session.Id
            claim.Token
            (suspendedOn (testCursor "req-live" "Which region? api_key=hunter2"))
            settled

    try
        SessionActor.promptSuspendableAsync
            store
            tenant
            session.Id
            actor
            (UserMessage.Text "run")
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()
        |> ignore

        let journaled =
            waitUntil (TimeSpan.FromSeconds 10.0) (fun () ->
                match journal.Replay(tenant, session.Id, 0L, 10, CancellationToken.None).GetAwaiter().GetResult() with
                | :? EventReplayPage as page when page.Events.Count = 1 -> true
                | _ -> false)

        journaled |> should equal true
        storedState store session.Id |> should equal SessionState.WaitingForInput
        settled.Count |> should equal 0

        let replay =
            journal.Replay(tenant, session.Id, 0L, 10, CancellationToken.None).GetAwaiter().GetResult()

        match replay with
        | :? EventReplayPage as page ->
            page.Events.Count |> should equal 1
            let asked = page.Events[0] :?> QuestionAskedEvent
            asked.QuestionId |> should equal "req-live"
            (asked.Question.Contains "[REDACTED]") |> should equal true
            (asked.Question.Contains "hunter2") |> should equal false
        | _ -> failwith "Expected the suspend event in the journal."
    finally
        stopSystem system

[<Fact>]
let ``Takeover loser suspends with zero journal effects and a typed failure`` () =
    use system = createSystem ()
    let clock, store, journal = createStores ()
    let session = createSession store
    appendUser store session.Id "first"
    appendUser store session.Id "second"

    let loser = claimTurn store session.Id "owner-a"
    clock.Advance(TimeSpan.FromSeconds 121.0)
    let _winner = claimTurn store session.Id "owner-b"

    let settled = ResizeArray<TurnResult>()

    let actor =
        spawnWriterActor
            system
            store
            journal
            session.Id
            loser.Token
            (suspendedOn (testCursor "req-loser" "Which region?"))
            settled

    try
        SessionActor.promptSuspendableAsync
            store
            tenant
            session.Id
            actor
            (UserMessage.Text "run")
            CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()
        |> ignore

        // The settle observes first, then consumes and idles on the same
        // actor thread: wait for both, so neither the result nor the
        // state is observed mid-flight.
        let finished =
            waitUntil (TimeSpan.FromSeconds 10.0) (fun () ->
                (lock settled (fun () -> settled.Count) = 1)
                && storedState store session.Id = SessionState.Idle)

        finished |> should equal true
        settled[0].Status |> should equal TurnStatus.Failed

        match settled[0].Outcome with
        | :? TurnFailed as failed -> failed.Reason |> should equal "The journal append was rejected: staleClaim."
        | _ -> failwith "Expected the typed TurnFailed outcome."

        let replay =
            journal.Replay(tenant, session.Id, 0L, 10, CancellationToken.None).GetAwaiter().GetResult()

        match replay with
        | :? EventReplayEndOfStream -> ()
        | :? EventReplayPage as page when page.Events.Count = 0 -> ()
        | _ -> failwith "Expected an empty journal after the loser's suspend."
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Logging scopes (issue 93)

/// One captured log line with the scopes active when it logged.
type private LoggedLine =
    {
        Level: string
        Text: string
        Scopes: (string * obj) list
    }

/// An ILogger capturing every entry with the scopes active at log time.
type private ScopeCapturingLogger() =
    let gate = obj ()
    let entries = ResizeArray<LoggedLine>()
    let stack = ResizeArray<(string * obj) list>()

    let toPairs (state: obj | null) : (string * obj) list =
        if isNull (box state) then
            []
        else
            match state with
            | :? IReadOnlyList<KeyValuePair<string, obj>> as kvs ->
                kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> List.ofSeq
            | :? IEnumerable<KeyValuePair<string, obj>> as kvs ->
                kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> List.ofSeq
            | _ -> []

    interface Microsoft.Extensions.Logging.ILogger with
        member _.BeginScope<'TState when 'TState: not null>(state: 'TState) : IDisposable =
            let pairs = toPairs (box state)
            lock gate (fun () -> stack.Add(pairs))

            { new IDisposable with
                member _.Dispose() =
                    lock gate (fun () ->
                        if stack.Count > 0 then
                            stack.RemoveAt(stack.Count - 1))
            }

        member _.IsEnabled(_) = true

        member _.Log<'TState>
            (
                logLevel: Microsoft.Extensions.Logging.LogLevel,
                _eventId: Microsoft.Extensions.Logging.EventId,
                state: 'TState,
                ex: exn,
                formatter: Func<'TState, exn, string>
            ) : unit =
            let text = formatter.Invoke(state, ex)
            let scopes = lock gate (fun () -> stack |> Seq.concat |> List.ofSeq)

            lock gate (fun () ->
                entries.Add(
                    {
                        Level = logLevel.ToString()
                        Text = text
                        Scopes = scopes
                    }
                ))

    /// Every captured line, oldest first.
    member _.Entries: LoggedLine list = lock gate (fun () -> entries |> List.ofSeq)

[<Fact>]
let ``Scoped append outcomes carry all six scopes`` () =
    let _, store, events = createStores ()
    let session = createSession store
    appendUser store session.Id "hi"
    let claim = claimTurn store session.Id "owner-a"
    let logger = ScopeCapturingLogger()

    let scope =
        LoggingScopes.createScope
            (tenant.ToString())
            (session.Id.ToString())
            (claim.TurnId.ToString())
            null
            1
            claim.Owner

    let result =
        JournalWriter.appendAsyncWithLogger
            store
            events
            tenant
            session.Id
            claim
            (batchOf (textEvent session.Id claim.TurnId "hello"))
            CancellationToken.None
            (logger :> Microsoft.Extensions.Logging.ILogger)
            scope
        |> fun task -> task.GetAwaiter().GetResult()

    match result with
    | JournalWriter.JournalAppended _ -> ()
    | _ -> failwith "Expected the scoped append to land."

    let entries = logger.Entries
    entries |> should not' (equal [])

    for entry in entries do
        for key in
            [
                LoggingScopes.SessionIdKey
                LoggingScopes.TurnIdKey
                LoggingScopes.AgentIdKey
                LoggingScopes.TenantIdKey
                LoggingScopes.AttemptKey
                LoggingScopes.ClaimOwnerKey
            ] do
            entry.Scopes |> List.exists (fun (name, _) -> name = key) |> should equal true

[<Fact>]
let ``Scoped token append redacts fixture secrets from captured logs`` () =
    let sessionId = SessionId.New()
    let turnId = TurnId.New()
    let logger = ScopeCapturingLogger()
    let secret = "sk-ant-journal-secret-33333333"

    let scope =
        LoggingScopes.createScope (tenant.ToString()) (sessionId.ToString()) (turnId.ToString()) null 1 "owner-a"

    JournalWriter.reportOutcome
        (logger :> Microsoft.Extensions.Logging.ILogger)
        scope
        (JournalWriter.JournalFailed $"The journal append failed after 3 attempts holding {secret}.")

    let entries = logger.Entries
    entries |> should not' (equal [])

    for entry in entries do
        entry.Text.Contains(secret) |> should equal false
        entry.Text.Contains(JournalWriter.RedactedText) |> should equal true
