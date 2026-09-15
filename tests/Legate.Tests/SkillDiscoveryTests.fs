// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Xunit
open FsUnit.Xunit
open FsCheck
open Legate
open Legate.Storage.InMemory

// Skill discovery tests (issue 67): hand-written fixtures pin the YAML
// shapes the issue calls out (multi-line and quoted frontmatter, a missing
// name, invalid YAML), the orchestrator pins staging through IWorkspace at
// package-relative paths with overwrite, and FsCheck properties prove the
// parser is total and the block carries every valid skill.
module SkillDiscoveryTests =

    let private sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    let private turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FBV"
    let private stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)

    let private entries (pairs: (string * string) list) : IAsyncEnumerable<AgentPackageEntry> =
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

    /// An in-memory workspace recording every write: staging lands here at
    /// package-relative paths, and a pre-seeded path proves overwrite.
    type private FakeWorkspace() =
        let files = Dictionary<string, byte[]>(StringComparer.Ordinal)

        member _.Text(path: string) =
            match files.TryGetValue path with
            | true, bytes -> Encoding.UTF8.GetString bytes
            | false, _ -> failwith (sprintf "the workspace has no file '%s'" path)

        interface IWorkspace with
            member _.Root = WorkspaceRoot("skill-discovery-tests", null)

            member _.Exec(_, _, _, _) =
                Task.FromException<WorkspaceExecResult>(NotImplementedException("not used"))

            member _.Exists(path, _) = Task.FromResult(files.ContainsKey path)

            member _.ReadFile(path, _) =
                match files.TryGetValue path with
                | true, bytes -> Task.FromResult<Stream>(new MemoryStream(bytes, false) :> Stream)
                | false, _ -> Task.FromException<Stream>(FileNotFoundException path)

            member _.WriteFile(path, content, _) =
                task {
                    files[path] <- Array.copy content
                    return ()
                }

            member _.DeleteFile(path, _) = Task.FromResult(files.Remove path)

            member _.DisposeAsync() = ValueTask.CompletedTask

    let private discover
        (store: IAgentPackageStore)
        (tenant: TenantId)
        (agentId: AgentId)
        (workspace: IWorkspace)
        : Task<SkillDiscovery.SkillDiscoveryResult> =
        SkillDiscovery.discoverAndStage
            {
                Store = store
                Tenant = tenant
                AgentId = agentId
                Workspace = workspace
                SessionId = sessionId
                TurnId = turnId
                Timestamp = stamp
            }
            CancellationToken.None

    // ───────────────────────────────────────────────────────────────────
    // Fixtures: the YAML shapes the issue calls out

    [<Fact>]
    let ``A multi-line block description parses verbatim`` () =
        let text =
            "---\nname: deploy\ndescription: |\n  Deploys the service\n  across regions.\n---\n# Deploy\n"

        match SkillDiscovery.parseSkillDocument text with
        | Error reason -> failwith (sprintf "expected a skill, got invalid: %s" reason)
        | Ok summary ->
            summary.Name |> should equal "deploy"
            summary.Description |> should equal "Deploys the service\nacross regions."

    [<Fact>]
    let ``Quoted values parse with their quoting removed`` () =
        let text =
            "---\nname: \"deploy:canary\"\ndescription: 'Deploys the \"canary\" build.'\n---\n# Deploy\n"

        match SkillDiscovery.parseSkillDocument text with
        | Error reason -> failwith (sprintf "expected a skill, got invalid: %s" reason)
        | Ok summary ->
            summary.Name |> should equal "deploy:canary"
            summary.Description |> should equal "Deploys the \"canary\" build."

    [<Fact>]
    let ``A missing name is invalid with a name reason`` () =
        let text = "---\ndescription: Nameless.\n---\n# Nothing\n"

        match SkillDiscovery.parseSkillDocument text with
        | Ok summary -> failwith (sprintf "expected invalid, got skill '%s'" summary.Name)
        | Error reason -> reason.Contains "name" |> should equal true

    [<Fact>]
    let ``Invalid YAML is invalid rather than thrown`` () =
        let text = "---\nname: [unclosed\n---\n# Broken\n"

        match SkillDiscovery.parseSkillDocument text with
        | Ok summary -> failwith (sprintf "expected invalid, got skill '%s'" summary.Name)
        | Error reason -> reason.Contains "valid YAML" |> should equal true

    [<Fact>]
    let ``A document without frontmatter is invalid`` () =
        match SkillDiscovery.parseSkillDocument "# Just a heading\n" with
        | Ok summary -> failwith (sprintf "expected invalid, got skill '%s'" summary.Name)
        | Error reason -> reason.Contains "frontmatter" |> should equal true

    [<Fact>]
    let ``Unclosed frontmatter is invalid`` () =
        match SkillDiscovery.parseSkillDocument "---\nname: deploy\n" with
        | Ok summary -> failwith (sprintf "expected invalid, got skill '%s'" summary.Name)
        | Error reason -> reason.Contains "closing" |> should equal true

    [<Fact>]
    let ``Allowed-tools travel as advisory text in both shapes`` () =
        let scalar = "---\nname: a\ndescription: A.\nallowed-tools: read_file, exec\n---\n"

        let sequence =
            "---\nname: b\ndescription: B.\nallowed-tools:\n  - read_file\n  - exec\n---\n"

        match SkillDiscovery.parseSkillDocument scalar, SkillDiscovery.parseSkillDocument sequence with
        | Ok first, Ok second ->
            first.AllowedTools |> should equal "read_file, exec"
            second.AllowedTools |> should equal "read_file, exec"

            let block = SkillDiscovery.buildAvailableSkillsBlock [ first; second ]

            block.Contains "allowed-tools=\"read_file, exec\"" |> should equal true
        | _ -> failwith "expected both skills to parse"

    [<Fact>]
    let ``The block is golden over two skills in ordinal order`` () =
        let summaries: SkillDiscovery.SkillSummary list =
            [
                {
                    Name = "zeta"
                    Description = "Last."
                    AllowedTools = Unchecked.defaultof<string>
                }
                {
                    Name = "alpha"
                    Description = "First & <foremost>."
                    AllowedTools = "read_file"
                }
            ]

        let block = SkillDiscovery.buildAvailableSkillsBlock summaries

        let expected =
            "<available_skills>\n"
            + "<skill name=\"alpha\" description=\"First &amp; &lt;foremost&gt;.\" allowed-tools=\"read_file\" />\n"
            + "<skill name=\"zeta\" description=\"Last.\" />\n"
            + "</available_skills>"

        block |> should equal expected

    // ───────────────────────────────────────────────────────────────────
    // Orchestrator: discovery plus staging over the active version

    [<Fact>]
    let ``Discovery stages companions at package-relative paths and reports invalid skills`` () =
        task {
            let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
            let tenant = TenantId.Create "discovery"
            let agentId = AgentId.New()
            let workspace = FakeWorkspace()

            // A stale companion proves staging overwrites: WriteFile has
            // no create-only mode.
            do!
                (workspace :> IWorkspace)
                    .WriteFile(
                        ".agent/skills/deploy/refs/api.md",
                        Encoding.UTF8.GetBytes "stale",
                        CancellationToken.None
                    )

            let! _ =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "1.0.0",
                    "discovery-tests",
                    entries
                        [
                            ".agent/skills/deploy/SKILL.md",
                            "---\nname: deploy\ndescription: Deploys things.\nallowed-tools: read_file, exec\n---\n# Deploy\n"
                            ".agent/skills/deploy/refs/api.md", "api notes"
                            ".agent/skills/broken/SKILL.md", "---\ndescription: Nameless.\n---\n# Nothing\n"
                            ".agent/skills/garbled/SKILL.md", "---\nname: [unclosed\n---\n# Broken\n"
                        ],
                    CancellationToken.None
                )

            let! result = discover store tenant agentId workspace

            // The valid skill is the whole block: advisory tools present,
            // invalid skills excluded.
            result.AvailableSkillsBlock.Contains "name=\"deploy\"" |> should equal true

            result.AvailableSkillsBlock.Contains "allowed-tools=\"read_file, exec\""
            |> should equal true

            result.AvailableSkillsBlock.Contains "broken" |> should equal false
            result.AvailableSkillsBlock.Contains "garbled" |> should equal false

            // The companion landed at its package-relative path with fresh
            // content; SKILL.md itself is never staged.
            result.StagedCompanions |> should equal [ ".agent/skills/deploy/refs/api.md" ]
            workspace.Text ".agent/skills/deploy/refs/api.md" |> should equal "api notes"

            let! skillStaged = (workspace :> IWorkspace).Exists(".agent/skills/deploy/SKILL.md", CancellationToken.None)

            skillStaged |> should equal false

            // One diagnostic per invalid skill, naming the skill.
            let diagnostics = result.Diagnostics |> List.ofSeq

            diagnostics.Length |> should equal 2

            let byName =
                diagnostics
                |> List.map (fun event ->
                    match event with
                    | :? SkillInvalidEvent as invalid -> invalid.SkillName, invalid.Reason
                    | _ -> failwith "expected SkillInvalidEvent diagnostics")
                |> Map.ofList

            byName.ContainsKey "broken" |> should equal true
            byName.ContainsKey "garbled" |> should equal true
            byName.["broken"].Contains "name" |> should equal true
        }

    [<Fact>]
    let ``Discovery over an unpackaged agent is empty without diagnostics`` () =
        task {
            let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
            let workspace = FakeWorkspace()

            let! result = discover store (TenantId.Create "discovery") (AgentId.New()) workspace

            result.AvailableSkillsBlock
            |> should equal "<available_skills>\n</available_skills>"

            Assert.Equal<string list>([], result.StagedCompanions)
            Assert.Equal<SessionEvent list>([], result.Diagnostics)
        }

    // ───────────────────────────────────────────────────────────────────
    // Properties: totality, round-trips, and block coverage

    [<FsCheck.Xunit.Property>]
    let ``Parsing never throws on arbitrary text`` (text: string) =
        try
            SkillDiscovery.parseSkillDocument text |> ignore
            true
        with _ ->
            false

    let private wordGen: Gen<string> =
        gen {
            let! length = Gen.choose (1, 12)

            let! chars = Gen.arrayOfLength length (Gen.elements ([ 'a' .. 'z' ] @ [ 'A' .. 'Z' ] @ [ '0' .. '9' ]))

            return String chars
        }

    let private nameGen: Gen<string> =
        gen {
            let! first = Gen.elements ([ 'a' .. 'z' ] @ [ 'A' .. 'Z' ])
            let! length = Gen.choose (0, 20)

            let! rest =
                Gen.arrayOfLength
                    length
                    (Gen.elements ([ 'a' .. 'z' ] @ [ 'A' .. 'Z' ] @ [ '0' .. '9' ] @ [ '-'; '_' ]))

            return String(Array.append [| first |] rest)
        }

    let private descriptionGen: Gen<string> =
        gen {
            let! count = Gen.choose (1, 5)
            let! words = Gen.arrayOfLength count wordGen
            return String.Join(" ", words)
        }

    [<FsCheck.Xunit.Property>]
    let ``Plain scalar names and descriptions round-trip`` () =
        let pairGen =
            gen {
                let! name = nameGen
                let! description = descriptionGen
                return name, description
            }

        Prop.forAll (Arb.fromGen pairGen) (fun (name, description) ->
            let text =
                sprintf "---\nname: %s\ndescription: %s\n---\nBody here." name description

            match SkillDiscovery.parseSkillDocument text with
            | Ok summary ->
                summary.Name = name
                && summary.Description = description
                && isNull (box summary.AllowedTools)
            | Error _ -> false)

    [<FsCheck.Xunit.Property>]
    let ``The block carries every valid skill line in ordinal order`` () =
        let pairGen =
            gen {
                let! name = nameGen
                let! description = descriptionGen
                return name, description
            }

        Prop.forAll (Arb.fromGen (Gen.listOf pairGen)) (fun pairs ->
            let summaries: SkillDiscovery.SkillSummary list =
                pairs
                |> List.distinct
                |> List.map (fun (name, description) ->
                    {
                        Name = name
                        Description = description
                        AllowedTools = Unchecked.defaultof<string>
                    })

            let block = SkillDiscovery.buildAvailableSkillsBlock summaries

            // Full lines, not bare names: a short name can also occur
            // inside another skill's description, so order is pinned on
            // the lines the builder wrote, in ordinal name order.
            let orderedLines =
                summaries
                |> List.sortWith (fun left right -> String.CompareOrdinal(left.Name, right.Name))
                |> List.map (fun summary ->
                    sprintf "<skill name=\"%s\" description=\"%s\" />" summary.Name summary.Description)

            let carried = orderedLines |> List.forall block.Contains

            let ordered =
                orderedLines
                |> List.map block.IndexOf
                |> List.pairwise
                |> List.forall (fun (earlier, later) -> earlier >= 0 && earlier < later)

            carried && ordered)

    [<FsCheck.Xunit.Property>]
    let ``Allowed-tools sequences join with a comma and a space`` () =
        Prop.forAll (Arb.fromGen (Gen.listOf wordGen)) (fun tools ->
            let toolsLine =
                if List.isEmpty tools then
                    "allowed-tools: []"
                else
                    "allowed-tools:\n"
                    + (tools |> List.map (sprintf "  - %s") |> String.concat "\n")

            let text = sprintf "---\nname: deploy\ndescription: Deploys.\n%s\n---\n" toolsLine

            match SkillDiscovery.parseSkillDocument text with
            | Ok summary -> String.Equals(summary.AllowedTools, String.Join(", ", tools), StringComparison.Ordinal)
            | Error _ -> false)
