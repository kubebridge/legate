// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Xunit
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Microsoft.Extensions.AI

// Prompt composition tests (issue 66): the pure order/separator/omission
// rules, the lowercase-wins casing probe (pure and against the store), the
// version-keyed per-session cache (hit, change-miss, null-miss), the
// SessionOptions host-file surface (ordering, skips, round-trip), and the
// turn-step wiring proving the composed runner leads history with the
// system message.
module PromptCompositionTests =

    let private textEntries (pairs: (string * string) list) : IAsyncEnumerable<AgentPackageEntry> =
        let prepared =
            pairs
            |> List.map (fun (path, text) -> AgentPackageEntry(path, new MemoryStream(Encoding.UTF8.GetBytes text)))
            |> List.toArray

        { new IAsyncEnumerable<AgentPackageEntry> with
            member _.GetAsyncEnumerator(_: CancellationToken) =
                let mutable index = -1

                { new IAsyncEnumerator<AgentPackageEntry> with
                    member _.MoveNextAsync() =
                        index <- index + 1
                        ValueTask<bool>(index < prepared.Length)

                    member _.Current: AgentPackageEntry = prepared[index]

                    member _.DisposeAsync() : ValueTask = ValueTask()
                }
        }

    let private upload
        (store: IAgentPackageStore)
        (agentId: AgentId)
        (version: string)
        (pairs: (string * string) list)
        =
        store.UploadPackage(
            TenantId.Create "prompt-composition",
            agentId,
            version,
            "prompt-tests",
            textEntries pairs,
            CancellationToken.None
        )

    let private composeRequest
        (store: IAgentPackageStore)
        (agentId: AgentId)
        (cache: PromptComposition.SessionPromptCache | null)
        : PromptComposition.TurnPromptRequest =
        {
            PackageStore = store
            Tenant = TenantId.Create "prompt-composition"
            AgentId = agentId
            AgentSystemPrompt = "agent prompt"
            SkillsBlock = "<available_skills>\n</available_skills>"
            HostFilePaths = null
            Cache = cache
        }

    /// An IChatClient recording every history it is called with and
    /// answering done: the turn-step tests prove the composed system
    /// message led the model context. Streaming is unimplemented like the
    /// shared scripted clients: the loop falls back to a single delta.
    type private HistoryCapturingClient() =
        let seen = ResizeArray<ChatMessage>()

        interface IChatClient with
            member _.GetResponseAsync(messages, _, _) =
                for message in messages do
                    seen.Add message

                Task.FromResult(TurnLoopTests.textResponse "done")

            member _.GetStreamingResponseAsync(_, _, _) = raise (NotImplementedException())

            member _.GetService(_, _) = null
            member _.Dispose() = ()

        /// Every history message seen, in call order.
        member _.Seen = seen |> List.ofSeq

    let private messageText (message: ChatMessage) : string =
        if isNull (box message) || isNull (box message.Contents) then
            ""
        else
            message.Contents
            |> Seq.choose (fun content ->
                match content with
                | :? TextContent as text when not (isNull (box text)) && not (isNull (box text.Text)) -> Some text.Text
                | _ -> None)
            |> String.concat ""

    let private noTools () : IReadOnlyDictionary<string, AITool> =
        Dictionary<string, AITool>() :> IReadOnlyDictionary<string, AITool>

    let private turnEntry () : InboxEntry =
        {
            SessionId = SessionId.New()
            Position = 0L
            Payload = UserMessagePayload(UserMessage.Text "hello") :> InboxPayload
            Delivery = DeliveryMode.Queue
            Consumed = false
            AppendedAt = DateTimeOffset.UtcNow
        }

    let private hookReturning (text: string | null) : PromptComposition.GetTurnSystemPrompt =
        fun _ _ -> task { return text }

    // ───────────────────────────────────────────────────────────────────
    // Pure composition: order, separators, omission

    [<Fact>]
    let ``Compose pins the fixed order with the fixed separator`` () =
        let skills = "<available_skills>\n</available_skills>"

        PromptComposition.compose
            "package"
            "agent"
            skills
            (ResizeArray<string>([| "host-a"; "host-b" |]) :> IReadOnlyList<string>)
        |> should
            equal
            "package\n\n---\n\nagent\n\n---\n\n<available_skills>\n</available_skills>\n\n---\n\nhost-a\n\n---\n\nhost-b"

    [<Fact>]
    let ``Compose omits each empty part with no dangling separator`` () =
        PromptComposition.compose null "agent" null null |> should equal "agent"
        PromptComposition.compose "package" null null null |> should equal "package"

        PromptComposition.compose null null "<available_skills>\n</available_skills>" null
        |> should equal "<available_skills>\n</available_skills>"

        PromptComposition.compose "" "  " null null |> should equal ""

    [<Fact>]
    let ``Compose skips blank host entries but keeps the surviving order`` () =
        let files = ResizeArray<string>()
        files.Add "host-a"
        files.Add(Unchecked.defaultof<string>)
        files.Add "  "
        files.Add "host-b"

        PromptComposition.compose null null null files
        |> should equal "host-a\n\n---\n\nhost-b"

    // ───────────────────────────────────────────────────────────────────
    // Casing: lowercase wins

    [<Fact>]
    let ``ResolveInstructions prefers the lowercase casing`` () =
        PromptComposition.resolveInstructions "canonical" "lowercase"
        |> should equal "lowercase"

        PromptComposition.resolveInstructions "canonical" null
        |> should equal "canonical"

        PromptComposition.resolveInstructions null "lowercase"
        |> should equal "lowercase"

        PromptComposition.resolveInstructions null null |> should equal null

    [<Fact>]
    let ``ResolveInstructions treats blank text as absent`` () =
        PromptComposition.resolveInstructions "canonical" "  "
        |> should equal "canonical"

        PromptComposition.resolveInstructions "" null |> should equal null

    [<Fact>]
    let ``Store probe prefers lowercase agents-md when both casings exist`` () =
        task {
            let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
            let agentId = AgentId.New()

            let! _ =
                upload
                    store
                    agentId
                    "1.0.0"
                    [
                        ("AGENTS.md", "canonical")
                        ("agents.md", "lowercase")
                    ]

            let! probed =
                PromptComposition.readPackageInstructionsAsync
                    store
                    (TenantId.Create "prompt-composition")
                    agentId
                    "1.0.0"
                    CancellationToken.None

            probed |> should equal "lowercase"
        }

    [<Fact>]
    let ``Store probe falls back to the canonical casing`` () =
        task {
            let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
            let agentId = AgentId.New()

            let! _ = upload store agentId "1.0.0" [ ("AGENTS.md", "canonical") ]

            let! probed =
                PromptComposition.readPackageInstructionsAsync
                    store
                    (TenantId.Create "prompt-composition")
                    agentId
                    "1.0.0"
                    CancellationToken.None

            probed |> should equal "canonical"
        }

    // ───────────────────────────────────────────────────────────────────
    // Version-keyed per-session cache

    [<Fact>]
    let ``Cache hits on the same version and misses on change or null`` () =
        let cache = PromptComposition.SessionPromptCache()
        let tenant = TenantId.Create "prompt-composition"
        let agentId = AgentId.New()

        cache.TryGet(tenant, agentId, "1.0.0") |> should equal None

        cache.Store(tenant, agentId, "1.0.0", "instructions")
        cache.TryGet(tenant, agentId, "1.0.0") |> should equal (Some "instructions")
        cache.TryGet(tenant, agentId, "2.0.0") |> should equal None
        cache.TryGet(tenant, agentId, null) |> should equal None
        cache.TryGet(TenantId.Create "other", agentId, "1.0.0") |> should equal None

    [<Fact>]
    let ``Cache distinguishes stored null from a miss`` () =
        let cache = PromptComposition.SessionPromptCache()
        let tenant = TenantId.Create "prompt-composition"
        let agentId = AgentId.New()

        cache.Store(tenant, agentId, "1.0.0", null)

        match cache.TryGet(tenant, agentId, "1.0.0") with
        | Some instructions -> Assert.Null(instructions)
        | None -> failwith "expected a hit carrying null instructions"

    [<Fact>]
    let ``Cache rejects a null version on store`` () =
        let cache = PromptComposition.SessionPromptCache()

        (fun () ->
            cache.Store(
                TenantId.Create "prompt-composition",
                AgentId.New(),
                Unchecked.defaultof<string>,
                "instructions"
            )
            |> ignore)
        |> should throw typeof<ArgumentNullException>

    [<Fact>]
    let ``Turn composition misses the cache on version change and on active-version delete`` () =
        task {
            let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
            let agentId = AgentId.New()
            let cache = PromptComposition.SessionPromptCache()

            let! _ = upload store agentId "1.0.0" [ ("AGENTS.md", "first instructions") ]

            let! first =
                PromptComposition.composeTurnSystemPromptAsync
                    (composeRequest store agentId cache)
                    CancellationToken.None

            first
            |> should
                equal
                "first instructions\n\n---\n\nagent prompt\n\n---\n\n<available_skills>\n</available_skills>"

            let! _ = upload store agentId "2.0.0" [ ("AGENTS.md", "second instructions") ]

            let! second =
                PromptComposition.composeTurnSystemPromptAsync
                    (composeRequest store agentId cache)
                    CancellationToken.None

            second
            |> should
                equal
                "second instructions\n\n---\n\nagent prompt\n\n---\n\n<available_skills>\n</available_skills>"

            let! deleted =
                store.DeletePackageVersion(
                    TenantId.Create "prompt-composition",
                    agentId,
                    "2.0.0",
                    CancellationToken.None
                )

            deleted |> should equal true

            let! cleared =
                PromptComposition.composeTurnSystemPromptAsync
                    (composeRequest store agentId cache)
                    CancellationToken.None

            cleared
            |> should equal "agent prompt\n\n---\n\n<available_skills>\n</available_skills>"
        }

    [<Fact>]
    let ``Turn composition works with no cache and no package`` () =
        task {
            let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
            let agentId = AgentId.New()

            let! composed =
                PromptComposition.composeTurnSystemPromptAsync
                    (composeRequest store agentId null)
                    CancellationToken.None

            composed
            |> should equal "agent prompt\n\n---\n\n<available_skills>\n</available_skills>"
        }

    // ───────────────────────────────────────────────────────────────────
    // Host files: SessionOptions surface, ordering, skips

    [<Fact>]
    let ``SessionOptions defaults HostInstructionFiles to null`` () =
        let options = SessionOptions()
        Assert.Null(options.HostInstructionFiles)

    [<Fact>]
    let ``SessionOptions carries host files through a JSON round-trip`` () =
        let options = SessionOptions()

        options.HostInstructionFiles <- ResizeArray<string>([| "AGENTS.md"; "docs/notes.md" |]) :> IReadOnlyList<string>

        let json = JsonSerializer.Serialize options

        match JsonSerializer.Deserialize<SessionOptions> json with
        | null -> failwith "expected the SessionOptions JSON to deserialize"
        | revived ->
            match revived.HostInstructionFiles with
            | null -> failwith "expected HostInstructionFiles to survive the round-trip"
            | files -> files |> List.ofSeq |> should equal [ "AGENTS.md"; "docs/notes.md" ]

    [<Fact>]
    let ``Host files append last in the listed order`` () =
        task {
            let root = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"))
            Directory.CreateDirectory root |> ignore

            try
                let first = Path.Combine(root, "first.md")
                let second = Path.Combine(root, "second.md")
                File.WriteAllText(first, "first file")
                File.WriteAllText(second, "second file")

                let! texts =
                    PromptComposition.readHostFilesAsync
                        (ResizeArray<string>([| second; first |]) :> IReadOnlyList<string>)
                        CancellationToken.None

                texts |> List.ofSeq |> should equal [ "second file"; "first file" ]

                PromptComposition.compose "package" "agent" null texts
                |> should equal "package\n\n---\n\nagent\n\n---\n\nsecond file\n\n---\n\nfirst file"
            finally
                Directory.Delete(root, true)
        }

    [<Fact>]
    let ``Host files skip missing paths`` () =
        task {
            let root = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"))
            Directory.CreateDirectory root |> ignore

            try
                let present = Path.Combine(root, "present.md")
                File.WriteAllText(present, "present file")

                let! texts =
                    PromptComposition.readHostFilesAsync
                        (ResizeArray<string>(
                            [|
                                Path.Combine(root, "absent.md")
                                present
                            |]
                        )
                        :> IReadOnlyList<string>)
                        CancellationToken.None

                texts |> List.ofSeq |> should equal [ "present file" ]
            finally
                Directory.Delete(root, true)
        }

    [<Fact>]
    let ``Host files skip content past the shared discovery bound`` () =
        task {
            let root = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"))
            Directory.CreateDirectory root |> ignore

            try
                let oversized = Path.Combine(root, "oversized.md")

                use stream =
                    new FileStream(oversized, FileMode.Create, FileAccess.Write, FileShare.None, 8192, true)

                let chunk = Array.zeroCreate<byte> 8192

                for _ in 1 .. (SkillDiscovery.MaxSkillFileBytes / chunk.Length) + 1 do
                    do! stream.WriteAsync(chunk, 0, chunk.Length, CancellationToken.None)

                let! texts =
                    PromptComposition.readHostFilesAsync
                        (ResizeArray<string>([| oversized |]) :> IReadOnlyList<string>)
                        CancellationToken.None

                texts |> List.ofSeq |> should equal ([]: string list)
            finally
                Directory.Delete(root, true)
        }

    // ───────────────────────────────────────────────────────────────────
    // Turn wiring: the composed runner leads with the system message

    /// Asserts the history led with the resolved system prompt. Pure so
    /// the resumable test stays a straight-line await plus a return.
    let private checkSystemLedHistory (seen: ChatMessage list) =
        match seen with
        | [ system; user ] ->
            system.Role |> should equal ChatRole.System
            messageText system |> should equal "composed system"
            user.Role |> should equal ChatRole.User
            messageText user |> should equal "hello"
        | seen -> failwith (sprintf "expected a system message plus the user message, saw %d messages" seen.Length)

    /// Asserts the history kept the user-only shape. Pure so the
    /// resumable test stays a straight-line await plus a return.
    let private checkUserOnlyHistory (seen: ChatMessage list) =
        match seen with
        | [ user ] ->
            user.Role |> should equal ChatRole.User
            messageText user |> should equal "hello"
        | seen -> failwith (sprintf "expected only the user message, saw %d messages" seen.Length)

    [<Fact>]
    let ``Composed runner leads history with the resolved system prompt`` () =
        task {
            let client = new HistoryCapturingClient()

            let runner =
                SessionActor.createComposedTurnRunner
                    (client :> IChatClient)
                    (noTools ())
                    TurnLoop.TurnLoopOptions.Default
                    (TurnLoopTests.NeverDelay() :> ILlmDelay)
                    (hookReturning "composed system")

            let! result = runner (turnEntry ()) CancellationToken.None

            result.Status |> should equal TurnStatus.Completed

            checkSystemLedHistory client.Seen
        }

    [<Fact>]
    let ``Composed runner keeps the user-only shape when nothing resolves`` () =
        task {
            let client = new HistoryCapturingClient()

            let runner =
                SessionActor.createComposedTurnRunner
                    (client :> IChatClient)
                    (noTools ())
                    TurnLoop.TurnLoopOptions.Default
                    (TurnLoopTests.NeverDelay() :> ILlmDelay)
                    (hookReturning null)

            let! result = runner (turnEntry ()) CancellationToken.None

            result.Status |> should equal TurnStatus.Completed

            checkUserOnlyHistory client.Seen
        }

    [<Fact>]
    let ``PrependSystemPrompt inserts once and ignores empty resolutions`` () =
        let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
        history.Add(ChatMessage(ChatRole.User, "hello"))

        PromptComposition.prependSystemPrompt history "composed system"
        history.Count |> should equal 2
        history[0].Role |> should equal ChatRole.System
        messageText history[0] |> should equal "composed system"

        PromptComposition.prependSystemPrompt history null
        PromptComposition.prependSystemPrompt history "  "
        history.Count |> should equal 2
