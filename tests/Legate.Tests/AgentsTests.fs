// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.AgentsTests

open System
open System.Collections.Generic
open System.Text.Json
open FsUnit.Xunit
open Legate
open Xunit

/// The generic Deserialize<'T> overload is annotated to return 'T | null,
/// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

// Unchecked.defaultof<string> rather than a bare null literal: under
// Nullable=enable the literal trips F# nullness checking on string-typed
// parameters. Deliberate: this is the null string value for rejection cases.
let nullString = Unchecked.defaultof<string>

/// Builds an agent with every field set, the common shape for round-trip
/// and construction tests.
let sampleAgent () =
    {
        Id = AgentId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        Tenant = TenantId.Create "acme"
        Name = "checkout"
        Description = "Handles checkout questions"
        Model = ModelReference.Parse "anthropic/claude-sonnet"
        SystemPrompt = "You help with checkout."
        EnvironmentVariables =
            let table = Dictionary<string, string>()
            table["API_BASE"] <- "https://127.0.0.1:8443"
            table :> IReadOnlyDictionary<string, string>
        PermissionDefaults =
            PermissionDefaults(DefaultDecision = PermissionDecisionKind.AllowForSession, AskWhenUnresolved = false)
        ToolSelection =
            let selection = ToolSelection()
            selection.BuiltIns <- [ "read_file" ] :> IReadOnlyList<string>
            selection.ToolSources <- [ "mcp-main" ] :> IReadOnlyList<string>
            selection
        PackageReference = "package.zip"
        Enabled = false
        Schedule =
            {
                Cron = "0 9 * * 1-5"
                TimeZone = "Europe/Berlin"
                Message = "Standup summary"
                Enabled = true
            }
        RowVersion = 7UL
        CreatedAt = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
        UpdatedAt = DateTimeOffset(2024, 6, 7, 8, 9, 10, TimeSpan.Zero)
    }

/// Builds a custom tool with every field set, the common shape for
/// construction and round-trip tests.
let sampleCustomTool () =
    {
        Tenant = TenantId.Create "acme"
        AgentId = AgentId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        Name = "lookup_order"
        Description = "Looks up an order by id"
        Endpoint = Uri "https://orders.internal.example/api/orders"
        InputSchema = """{"type":"object","properties":{"orderId":{"type":"string"}}}"""
        Headers =
            let table = Dictionary<string, string>()
            table["X-Trace-Source"] <- "legate"
            table :> IReadOnlyDictionary<string, string>
        SigningSecret = [| 1uy; 2uy; 3uy; 4uy |]
        Enabled = true
        RowVersion = 3UL
        CreatedAt = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
        UpdatedAt = DateTimeOffset(2024, 6, 7, 8, 9, 10, TimeSpan.Zero)
    }

// ───────────────────────────────────────────────────────────────────────────
// AgentEnvironmentKeys

[<Fact>]
let ``Pattern matches the documented shape`` () =
    AgentEnvironmentKeys.Pattern |> should equal @"\A[A-Z_][A-Z0-9_]{0,63}\z"

[<Fact>]
let ``Validate accepts the documented shapes`` () =
    AgentEnvironmentKeys.Validate "A" |> should equal "A"
    AgentEnvironmentKeys.Validate "_X" |> should equal "_X"
    AgentEnvironmentKeys.Validate "MY_KEY_1" |> should equal "MY_KEY_1"

    AgentEnvironmentKeys.Validate(String.replicate 64 "A")
    |> should equal (String.replicate 64 "A")

[<Fact>]
let ``TryValidate mirrors Validate for accepted keys`` () =
    AgentEnvironmentKeys.TryValidate "A" |> should equal true
    AgentEnvironmentKeys.TryValidate "_X" |> should equal true
    AgentEnvironmentKeys.TryValidate "MY_KEY_1" |> should equal true

    AgentEnvironmentKeys.TryValidate(String.replicate 64 "A") |> should equal true

[<Fact>]
let ``Validate rejects malformed keys`` () =
    (fun () -> AgentEnvironmentKeys.Validate nullString |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> AgentEnvironmentKeys.Validate "" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> AgentEnvironmentKeys.Validate "key" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> AgentEnvironmentKeys.Validate "1KEY" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> AgentEnvironmentKeys.Validate(String.replicate 65 "A") |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> AgentEnvironmentKeys.Validate "A-B" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> AgentEnvironmentKeys.Validate "HAS SPACE" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> AgentEnvironmentKeys.Validate "KEY\u00E9" |> ignore)
    |> should throw typeof<ArgumentException>

    // $ without \z also matches immediately before a trailing line feed,
    // which would admit a newline-suffixed key and contradict the doc.
    (fun () -> AgentEnvironmentKeys.Validate "KEY\n" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> AgentEnvironmentKeys.Validate "KEY\n " |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``TryValidate returns false for malformed keys without throwing`` () =
    AgentEnvironmentKeys.TryValidate nullString |> should equal false
    AgentEnvironmentKeys.TryValidate "" |> should equal false
    AgentEnvironmentKeys.TryValidate "key" |> should equal false
    AgentEnvironmentKeys.TryValidate "1KEY" |> should equal false

    AgentEnvironmentKeys.TryValidate(String.replicate 65 "A") |> should equal false

    AgentEnvironmentKeys.TryValidate "A-B" |> should equal false
    AgentEnvironmentKeys.TryValidate "HAS SPACE" |> should equal false
    AgentEnvironmentKeys.TryValidate "KEY\u00E9" |> should equal false
    AgentEnvironmentKeys.TryValidate "KEY\n" |> should equal false
    AgentEnvironmentKeys.TryValidate "KEY\n " |> should equal false

[<Fact>]
let ``Validate performs no trimming`` () =
    (fun () -> AgentEnvironmentKeys.Validate " KEY " |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``Validate compares ordinally across the Unicode class`` () =
    // A fullwidth letter is not [A-Z] under an ordinal, culture-invariant
    // match; culture-sensitive matching could accept it.
    (fun () -> AgentEnvironmentKeys.Validate "ＫＥＹ" |> ignore)
    |> should throw typeof<ArgumentException>

// ───────────────────────────────────────────────────────────────────────────
// PermissionDefaults and ToolSelection

[<Fact>]
let ``PermissionDefaults keeps safe defaults when properties are omitted`` () =
    let defaults = PermissionDefaults()

    defaults.DefaultDecision |> should equal PermissionDecisionKind.Deny
    defaults.AskWhenUnresolved |> should equal true

[<Fact>]
let ``PermissionDefaults accepts configured decisions`` () =
    let defaults =
        PermissionDefaults(DefaultDecision = PermissionDecisionKind.AllowForSession, AskWhenUnresolved = false)

    defaults.DefaultDecision |> should equal PermissionDecisionKind.AllowForSession
    defaults.AskWhenUnresolved |> should equal false

[<Fact>]
let ``ToolSelection defaults to empty selections`` () =
    let selection = ToolSelection()

    Seq.isEmpty selection.BuiltIns |> should equal true
    Seq.isEmpty selection.ToolSources |> should equal true

[<Fact>]
let ``ToolSelection accepts configured lists`` () =
    let selection =
        ToolSelection(
            BuiltIns = ([ "read_file"; "list_directory" ] :> IReadOnlyList<string>),
            ToolSources = ([ "mcp-main" ] :> IReadOnlyList<string>)
        )

    selection.BuiltIns
    |> Seq.toList
    |> should equal [ "read_file"; "list_directory" ]

    selection.ToolSources |> Seq.toList |> should equal [ "mcp-main" ]

// ───────────────────────────────────────────────────────────────────────────
// Agent construction and JSON round-trip

[<Fact>]
let ``Agent record constructs with F# syntax and compares structurally`` () =
    let agent = sampleAgent ()

    let changed =
        { agent with
            Description = "Different description"
        }

    changed |> should not' (equal agent)

    let copy =
        { agent with
            Description = "Handles checkout questions"
        }

    copy |> should equal agent

[<Fact>]
let ``Agent CLIMutable setters drive object-initialiser construction`` () =
    let agent = sampleAgent ()

    agent.Name |> should equal "checkout"
    agent.Enabled |> should equal false
    agent.RowVersion |> should equal 7UL

    let defaults = agent.PermissionDefaults |> Option.ofObj
    defaults.Value.AskWhenUnresolved |> should equal false

    let selection = agent.ToolSelection |> Option.ofObj
    selection.Value.BuiltIns |> Seq.toList |> should equal [ "read_file" ]

[<Fact>]
let ``Agent JSON round-trip preserves every field with default options`` () =
    let agent = sampleAgent ()

    let json = JsonSerializer.Serialize agent
    let roundTripped = deserialize<Agent> json

    roundTripped.Id |> should equal agent.Id
    roundTripped.Tenant |> should equal agent.Tenant
    roundTripped.Name |> should equal agent.Name
    roundTripped.Description |> should equal agent.Description
    roundTripped.Model |> should equal agent.Model
    roundTripped.SystemPrompt |> should equal agent.SystemPrompt

    let environmentVariables = roundTripped.EnvironmentVariables |> Option.ofObj
    environmentVariables.Value["API_BASE"] |> should equal "https://127.0.0.1:8443"

    let defaults = roundTripped.PermissionDefaults |> Option.ofObj

    defaults.Value.DefaultDecision
    |> should equal PermissionDecisionKind.AllowForSession

    defaults.Value.AskWhenUnresolved |> should equal false

    let selection = roundTripped.ToolSelection |> Option.ofObj
    selection.Value.BuiltIns |> Seq.toList |> should equal [ "read_file" ]
    selection.Value.ToolSources |> Seq.toList |> should equal [ "mcp-main" ]

    roundTripped.PackageReference |> should equal agent.PackageReference
    roundTripped.Enabled |> should equal agent.Enabled

    let schedule = roundTripped.Schedule |> Option.ofObj
    schedule.Value |> should equal agent.Schedule |> ignore
    roundTripped.Schedule |> should equal agent.Schedule

    roundTripped.RowVersion |> should equal agent.RowVersion
    roundTripped.CreatedAt |> should equal agent.CreatedAt
    roundTripped.UpdatedAt |> should equal agent.UpdatedAt

[<Fact>]
let ``Agent JSON serialises TenantId and ModelReference as plain strings`` () =
    let document = JsonSerializer.Serialize(sampleAgent ()) |> JsonDocument.Parse

    document.RootElement.GetProperty("Tenant").GetString() |> should equal "acme"

    document.RootElement.GetProperty("Model").GetString()
    |> should equal "anthropic/claude-sonnet"

[<Fact>]
let ``Agent JSON deserialises absent optional properties as null`` () =
    let json =
        """{"Id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Tenant":"acme","Name":"checkout","Model":"anthropic/claude-sonnet","SystemPrompt":"You help.","Enabled":true,"RowVersion":0,"CreatedAt":"2024-01-02T03:04:05+00:00","UpdatedAt":"2024-01-02T03:04:05+00:00"}"""

    let agent = deserialize<Agent> json

    agent.Description |> should equal null
    agent.EnvironmentVariables |> should equal null
    agent.PermissionDefaults |> should equal null
    agent.ToolSelection |> should equal null
    agent.PackageReference |> should equal null
    agent.Schedule |> should equal null
    agent.RowVersion |> should equal 0UL
    agent.Tenant.Value |> should equal "acme"
    agent.Model.Value |> should equal "anthropic/claude-sonnet"

[<Fact>]
let ``Agent JSON deserialises configured PermissionDefaults and ToolSelection`` () =
    let json =
        """{"Id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Tenant":"acme","Name":"checkout","Model":"openai/gpt-4o","SystemPrompt":"You help.","PermissionDefaults":{"DefaultDecision":1,"AskWhenUnresolved":false},"ToolSelection":{"BuiltIns":["read_file"],"ToolSources":["mcp-main"]},"Enabled":true,"RowVersion":3,"CreatedAt":"2024-01-02T03:04:05+00:00","UpdatedAt":"2024-01-02T03:04:05+00:00"}"""

    let agent = deserialize<Agent> json

    let defaults = agent.PermissionDefaults |> Option.ofObj

    defaults.Value.DefaultDecision
    |> should equal PermissionDecisionKind.AllowForSession

    defaults.Value.AskWhenUnresolved |> should equal false

    let selection = agent.ToolSelection |> Option.ofObj
    selection.Value.BuiltIns |> Seq.toList |> should equal [ "read_file" ]
    selection.Value.ToolSources |> Seq.toList |> should equal [ "mcp-main" ]

[<Fact>]
let ``Agent JSON rejects invalid tenant and model payloads`` () =
    let blankTenant =
        """{"Id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Tenant":"","Name":"checkout","Model":"openai/gpt-4o","SystemPrompt":"p","Enabled":true,"RowVersion":0,"CreatedAt":"2024-01-02T03:04:05+00:00","UpdatedAt":"2024-01-02T03:04:05+00:00"}"""

    (fun () -> deserialize<Agent> blankTenant |> ignore)
    |> should throw typeof<JsonException>

    let invalidModel =
        """{"Id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Tenant":"acme","Name":"checkout","Model":"noglash","SystemPrompt":"p","Enabled":true,"RowVersion":0,"CreatedAt":"2024-01-02T03:04:05+00:00","UpdatedAt":"2024-01-02T03:04:05+00:00"}"""

    (fun () -> deserialize<Agent> invalidModel |> ignore)
    |> should throw typeof<LegateIdentifierException>

// ───────────────────────────────────────────────────────────────────────────
// AgentSchedule and AgentCustomTool

[<Fact>]
let ``AgentSchedule carries the opaque strings and the enabled flag`` () =
    let schedule =
        {
            Cron = "30 2 * * *"
            TimeZone = "America/New_York"
            Message = "Nightly report"
            Enabled = false
        }

    schedule.Cron |> should equal "30 2 * * *"
    schedule.TimeZone |> should equal "America/New_York"
    schedule.Message |> should equal "Nightly report"
    schedule.Enabled |> should equal false

[<Fact>]
let ``AgentSchedule JSON round-trip preserves every field`` () =
    let schedule =
        {
            Cron = "0 9 * * 1-5"
            TimeZone = "Europe/Berlin"
            Message = "Standup summary"
            Enabled = true
        }

    let json = JsonSerializer.Serialize schedule
    let roundTripped = deserialize<AgentSchedule> json

    roundTripped |> should equal schedule

[<Fact>]
let ``Agent JSON deserialises a configured schedule`` () =
    let json =
        """{"Id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Tenant":"acme","Name":"checkout","Model":"openai/gpt-4o","SystemPrompt":"You help.","Enabled":true,"Schedule":{"Cron":"0 9 * * 1-5","TimeZone":"Europe/Berlin","Message":"Standup summary","Enabled":true},"RowVersion":1,"CreatedAt":"2024-01-02T03:04:05+00:00","UpdatedAt":"2024-01-02T03:04:05+00:00"}"""

    let agent = deserialize<Agent> json

    let schedule = agent.Schedule |> Option.ofObj
    schedule.Value.Cron |> should equal "0 9 * * 1-5"
    schedule.Value.TimeZone |> should equal "Europe/Berlin"
    schedule.Value.Message |> should equal "Standup summary"
    schedule.Value.Enabled |> should equal true

[<Fact>]
let ``AgentCustomTool record constructs with F# syntax and compares structurally`` () =
    let tool = sampleCustomTool ()

    let changed = { tool with Name = "lookup_invoice" }

    changed |> should not' (equal tool)

    let copy = { tool with Name = "lookup_order" }

    copy |> should equal tool

[<Fact>]
let ``AgentCustomTool CLIMutable setters drive property access`` () =
    let tool = sampleCustomTool ()

    tool.Name |> should equal "lookup_order"
    tool.Enabled |> should equal true
    tool.RowVersion |> should equal 3UL
    tool.Endpoint |> should equal (Uri "https://orders.internal.example/api/orders")
    tool.SigningSecret |> should equal [| 1uy; 2uy; 3uy; 4uy |]

    let headers = tool.Headers |> Option.ofObj
    headers.Value["X-Trace-Source"] |> should equal "legate"

[<Fact>]
let ``AgentCustomTool JSON round-trip preserves every field`` () =
    let tool = sampleCustomTool ()

    let json = JsonSerializer.Serialize tool
    let roundTripped = deserialize<AgentCustomTool> json

    roundTripped.Tenant |> should equal tool.Tenant
    roundTripped.AgentId |> should equal tool.AgentId
    roundTripped.Name |> should equal tool.Name
    roundTripped.Description |> should equal tool.Description
    roundTripped.Endpoint |> should equal tool.Endpoint
    roundTripped.InputSchema |> should equal tool.InputSchema

    // Deserialisation builds a fresh dictionary, so the header table is
    // compared by content, not by record equality.
    let roundTrippedHeaders = roundTripped.Headers |> Option.ofObj
    let originalHeaders = tool.Headers |> Option.ofObj
    roundTrippedHeaders.Value.Count |> should equal originalHeaders.Value.Count

    roundTrippedHeaders.Value["X-Trace-Source"]
    |> should equal originalHeaders.Value["X-Trace-Source"]

    roundTripped.SigningSecret |> should equal tool.SigningSecret
    roundTripped.Enabled |> should equal tool.Enabled
    roundTripped.RowVersion |> should equal tool.RowVersion
    roundTripped.CreatedAt |> should equal tool.CreatedAt
    roundTripped.UpdatedAt |> should equal tool.UpdatedAt

[<Fact>]
let ``AgentCustomTool JSON deserialises absent optional properties as null`` () =
    let json =
        """{"Tenant":"acme","AgentId":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Name":"lookup_order","Endpoint":"https://orders.internal.example/api/orders","SigningSecret":"AQIDBA==","Enabled":true,"RowVersion":1,"CreatedAt":"2024-01-02T03:04:05+00:00","UpdatedAt":"2024-01-02T03:04:05+00:00"}"""

    let tool = deserialize<AgentCustomTool> json

    tool.Description |> should equal null
    tool.InputSchema |> should equal null
    tool.Headers |> should equal null
    tool.SigningSecret |> should equal [| 1uy; 2uy; 3uy; 4uy |]
