// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.WireSerializationGoldenTests

open System
open System.Collections.Generic
open System.IO
open System.Threading
open Akka.Actor
open Akka.Configuration
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Xunit

// Wire compatibility goldens (issue 131): one committed serialized message
// per registered manifest under TestData/Wire/v0, pinned at the issue 130
// merge code state. Two properties hold: (a) the manifest registry and the
// fixture directory agree exactly in both directions, so adding a wire case
// without its golden file (or leaving an orphaned golden behind) fails CI;
// (b) every golden still deserialises through the current serializer, so an
// envelope or payload break against the pinned baseline fails CI. Goldens
// are byte-exact ToBinary outputs; they are never regenerated in CI.

// ────────────────── Deterministic representatives ──────────────────

// Fixed ULIDs so the committed bytes are stable and reviewable. Any valid
// 26-character Crockford ULID parses; these differ only in trailing chars.
let private fixedSessionId = SessionId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FAV")
let private fixedAgentId = AgentId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FAW")

let private fixedTimestamp = DateTimeOffset(2026, 9, 20, 12, 0, 0, TimeSpan.Zero)

/// Builds a Queue inbox entry carrying one text user message, with fixed
/// ids and timestamps so the golden bytes are stable.
let private goldenEntry (text: string) (position: int64) : InboxEntry =
    {
        SessionId = fixedSessionId
        Position = position
        Payload = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload
        Delivery = DeliveryMode.Queue
        Consumed = false
        AppendedAt = fixedTimestamp
    }

/// Builds a stored session with fixed ids and timestamps.
let private goldenSession () : Session =
    {
        Id = fixedSessionId
        Tenant = TenantId.Default
        AgentId = fixedAgentId
        Title = "wire golden"
        State = SessionState.Idle
        CurrentTurnId = Nullable<TurnId>()
        CreatedAt = fixedTimestamp
        UpdatedAt = fixedTimestamp
        ClosedAt = Nullable<DateTimeOffset>()
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// Builds a Completed turn result.
let private goldenResult () : TurnResult =
    {
        AssistantText = "done"
        Status = TurnStatus.Completed
        Iterations = 1
        Usage = { InputTokens = 10L; OutputTokens = 5L }
        Outcome = TurnFinished("done") :> TurnOutcome
    }

/// Builds a settled suspendable completion (no suspension).
let private goldenCompletion () : TurnLoop.TurnLoopCompletion =
    {
        Result = goldenResult ()
        HasPendingInjects = false
        Suspension = None
    }

/// Builds a live suspend cursor over one tool call, with no nested resume.
let private goldenCursor () : TurnLoop.TurnLoopSuspension =
    let args = Dictionary<string, obj>() :> IDictionary<string, obj>

    let history =
        ResizeArray<ChatMessage>([| ChatMessage(ChatRole.User, "go") |]) :> IList<ChatMessage>

    {
        RequestId = "req-1"
        ToolName = "probe-tool"
        ToolCallId = "call-1"
        Kind = TurnLoop.SuspensionKind.PermissionSuspension
        QuestionText = ""
        QuestionOptions = []
        HistorySnapshot = history
        InputTokens = 3L
        OutputTokens = 7L
        Iterations = 2
        PendingCall = FunctionCallContent("call-1", "probe-tool", args)
        Nested = None
    }

/// Builds every live wire message: all nine SessionActorMessage cases,
/// all thirteen SuspendableActorMessage cases, every reply case, the
/// snapshot, a stored session, the router message, and the four cross-node
/// subscription cases (issue 133). Mirrors the 130 serializer test's
/// coverage so every registered DTO has a representative.
let private everyGoldenMessage () : obj list =
    let entry = goldenEntry "wire golden" 7L
    let session = goldenSession ()
    let error = InvalidOperationException("boom") :> Exception

    let mismatch =
        ReplyMismatchException(session.Id, "req-9", "No pending request 'req-9'.")

    let allowed = HashSet<string>([| "probe-tool" |])

    let subscribeRequest: CrossNodeSubscriptions.CrossNodeSubscribeRequest =
        {
            Tenant = TenantId.Default
            SessionId = session.Id
            FromSequence = 0L
            SubscriberToken = "wire-subscriber"
        }

    let unsubscribeRequest: CrossNodeSubscriptions.CrossNodeUnsubscribe =
        {
            Tenant = TenantId.Default
            SessionId = session.Id
            SubscriberToken = "wire-subscriber"
        }

    let goldenEvent: SessionEvent =
        TextDeltaEvent(
            session.Id,
            TurnId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FAX"),
            Nullable<int64>(7L),
            fixedTimestamp,
            "wire event"
        )
        :> SessionEvent

    let eventBatch: CrossNodeSubscriptions.CrossNodeEventBatch =
        {
            SessionId = session.Id
            Events = [| goldenEvent |] :> IReadOnlyList<SessionEvent>
            NextCursor = 7L
            EndOfStream = true
        }

    let finished =
        SessionActor.SuspendableFinished(entry, goldenCompletion (), 1, allowed)

    let suspended =
        SessionActor.SuspendableFinished(
            entry,
            {
                Result = goldenResult ()
                HasPendingInjects = true
                Suspension = Some(goldenCursor ())
            },
            2,
            allowed
        )

    [
        QueuePrompt(entry.Payload, CancellationToken.None) :> obj
        InjectPrompt(entry.Payload, CancellationToken.None) :> obj
        InterruptPrompt(entry.Payload, CancellationToken.None) :> obj
        CloseSession(CancellationToken.None) :> obj
        AbortSession(StopCause.ExplicitAbort, "host abort", CancellationToken.None) :> obj
        CompactSession(CancellationToken.None) :> obj
        GetSnapshot :> obj
        SessionTurnSettled(entry, goldenResult ()) :> obj
        SessionTurnFaulted(entry, error) :> obj
        SessionActor.SuspendableQueuePrompt(entry.Payload, CancellationToken.None) :> obj
        SessionActor.SuspendableInjectPrompt(entry.Payload, CancellationToken.None) :> obj
        SessionActor.SuspendableInterruptPrompt(entry.Payload, CancellationToken.None) :> obj
        finished :> obj
        suspended :> obj
        SessionActor.SuspendableFaulted(entry, error, 1) :> obj
        SessionActor.ReplyEntry(entry) :> obj
        SessionActor.SuspendableGetSnapshot :> obj
        SessionActor.SuspendTimedOut("req-1") :> obj
        SessionActor.SuspendableCloseSession(CancellationToken.None) :> obj
        SessionActor.SuspendableAbortSession(StopCause.HostShutdown, "shutting down", CancellationToken.None) :> obj
        SessionActor.SuspendableCompactSession(CancellationToken.None) :> obj
        SessionActor.SuspendableCheckInbox :> obj
        SessionActor.SuspendableSetAgent(fixedAgentId, CancellationToken.None) :> obj
        PromptAccepted(entry) :> obj
        PromptRejected(SessionState.Closed) :> obj
        CompactCompleted(100L, 40L) :> obj
        CompactNotNeeded :> obj
        CompactDeferred :> obj
        CompactFenced :> obj
        CompactRejected(SessionState.Closed) :> obj
        SessionActor.ReplyAccepted(entry) :> obj
        SessionActor.ReplyRejected(mismatch) :> obj
        SessionActor.SetAgentApplied(session) :> obj
        SessionActor.SetAgentPending(session) :> obj
        SessionActor.SetAgentRejected(SessionState.Closed) :> obj
        {
            SessionId = session.Id
            State = SessionState.Running
            PendingCount = 2
            RunningPosition = Some 7L
            PendingRequestId = "req-1"
        }
        :> obj
        session :> obj
        SessionRouterMessage.ResolveSession(session.Id.ToString()) :> obj
        subscribeRequest :> obj
        unsubscribeRequest :> obj
        eventBatch :> obj
        goldenEvent :> obj
    ]

// ────────────────── Fixture location ──────────────────

/// Locates the committed v0 golden directory as shipped to the test output.
let private goldenDir () : string =
    let dir = Path.Combine(AppContext.BaseDirectory, "TestData", "Wire", "v0")

    if Directory.Exists(dir) then
        dir
    else
        failwith
            $"Expected the wire golden directory at '{dir}'. Did the TestData None entry copy it to the test output?"

/// Strips the trailing .golden suffix back to the manifest string.
let private manifestOfFile (path: string) : string =
    match Path.GetFileName(path) with
    | null -> failwith $"The golden path '{path}' has no file name."
    | name ->
        if name.EndsWith(".golden", StringComparison.Ordinal) then
            name.Substring(0, name.Length - ".golden".Length)
        else
            failwith $"Expected a '.golden' file but found '{name}'."

// ────────────────── Actor system ──────────────────

/// Creates a local actor system with the wire HOCON fragment inlined. The
/// caller terminates the system.
let private createGoldenSystem () : ActorSystem =
    let hocon =
        """
        akka {
          actor {
            provider = "local"
          }
          loglevel = "WARNING"
          stdout-loglevel = "WARNING"
        }
        """
        + WireSerialization.hoconFragment WireManifests.DefaultMaxWirePayloadBytes

    ActorSystem.Create("legate-wire-golden-test", ConfigurationFactory.ParseString(hocon))

/// Resolves the envelope serializer for a prompt message.
let private goldenSerializerOf (system: ActorSystem) : WireSerializer =
    let message =
        QueuePrompt(UserMessagePayload(UserMessage.Text("probe")) :> InboxPayload, CancellationToken.None)

    match system.Serialization.FindSerializerFor(message) with
    | :? WireSerializer as wire -> wire
    | other ->
        failwith $"Expected the wire serializer but resolved '{other.GetType().FullName}'."
        Unchecked.defaultof<WireSerializer>

// ────────────────── Golden seed helper ──────────────────

/// One-off seed helper: serialises one representative message per
/// registered manifest into targetDir. Used once to create the committed
/// v0 baseline at the 130-merge code state; never called from CI.
module internal GoldenSeed =

    /// Writes one <manifest>.golden file per registered manifest.
    /// <param name="serializer">The envelope serializer.</param>
    /// <param name="targetDir">The v0 directory; created when missing.</param>
    /// <returns>How many golden files were written.</returns>
    let writeGoldens (serializer: WireSerializer) (targetDir: string) : int =
        ArgumentNullException.ThrowIfNull(serializer)
        ArgumentNullException.ThrowIfNull(targetDir)
        Directory.CreateDirectory(targetDir) |> ignore

        let mutable seen = Set.empty<string>
        let mutable written = 0

        for message in everyGoldenMessage () do
            let manifest = serializer.Manifest(message)

            if not (seen.Contains(manifest)) then
                seen <- seen.Add(manifest)
                let bytes = serializer.ToBinary(message)
                File.WriteAllBytes(Path.Combine(targetDir, manifest + ".golden"), bytes)
                written <- written + 1

        written

// ────────────────── Compatibility tests ──────────────────

[<Fact>]
let ``Registry and golden directory agree in both directions`` () =
    let registered =
        WireManifests.cases |> List.map WireManifests.manifestOf |> Set.ofList

    registered.IsEmpty |> should equal false

    let onDisk =
        Directory.GetFiles(goldenDir (), "*.golden")
        |> Array.map manifestOfFile
        |> Set.ofArray

    let missing = Set.difference registered onDisk
    let orphaned = Set.difference onDisk registered
    let separator = ", "

    if not missing.IsEmpty then
        failwith
            $"The wire golden directory is missing files for registered manifests: {String.Join(separator, missing)}. Add one <manifest>.golden per manifest."

    if not orphaned.IsEmpty then
        failwith
            $"The wire golden directory holds goldens for no registered manifest: {String.Join(separator, orphaned)}. Remove or re-register them."

[<Fact>]
let ``Every golden deserialises through the current serializer`` () =
    let system = createGoldenSystem ()

    try
        let serializer = goldenSerializerOf system
        let failures = ResizeArray<string>()

        for path in Directory.GetFiles(goldenDir (), "*.golden") do
            let manifest = manifestOfFile path

            try
                match WireManifests.tryParseManifest manifest with
                | None -> failwith $"The golden name parses as no legate manifest."
                | Some(family, name, _) ->
                    match WireManifests.tryFindCase family name with
                    | None -> failwith $"The golden names no registered wire case."
                    | Some wireCase ->
                        let bytes = File.ReadAllBytes(path)

                        if bytes.Length = 0 then
                            failwith $"The golden file is empty."

                        let back = serializer.FromBinary(bytes, manifest)
                        let dto = WireDtos.toWire back

                        if dto.GetType() <> wireCase.DtoType then
                            failwith
                                $"The golden deserialised to '{dto.GetType().FullName}' but the manifest registers '{wireCase.DtoType.FullName}'."
            with ex ->
                failures.Add($"{manifest}: {ex.Message}")

        if failures.Count > 0 then
            failwith
                $"The wire goldens refused to deserialise:{Environment.NewLine}{String.Join(Environment.NewLine, failures)}"
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore
