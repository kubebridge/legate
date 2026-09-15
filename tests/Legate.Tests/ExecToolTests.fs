// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ExecToolTests

open System
open System.Collections.Generic
open System.IO
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// Tests for the built-in exec tool (issue 59): the AIFunction definition
// mapping verbatim onto IWorkspace.Exec plus its wiring through the
// existing TurnLoop permission and claim-fence path. The process-tree kill
// itself belongs to the primitive and is covered by ProcessWorkspaceTests;
// here a recording fake proves the tool maps timeout, truncation,
// allowlist env, result shape, and redaction correctly, using harmless
// canned outputs only (no processes spawn).

// ───────────────────────────────────────────────────────────────────────────
// Doubles

/// Recording IWorkspace fake: answers Exec with the canned result and
/// records every call's command, timeout, and env for mapping assertions.
type private RecordingWorkspace(result: WorkspaceExecResult) =
    let commands = ResizeArray<string>()
    let timeouts = ResizeArray<Nullable<TimeSpan>>()
    let envs = ResizeArray<IReadOnlyDictionary<string, string> | null>()

    interface IWorkspace with
        member _.Root = WorkspaceRoot("test-runtime", Path.GetTempPath())

        member _.Exec(command, timeout, env, _cancellationToken) =
            commands.Add(command)
            timeouts.Add(timeout)
            envs.Add(env)
            Task.FromResult(result)

        member _.Exists(_, _) = Task.FromResult(false)

        member _.ReadFile(_, _) : Task<Stream> = Task.FromResult(Stream.Null)

        member _.WriteFile(_, _, _) = Task.CompletedTask

        member _.DeleteFile(_, _) = Task.FromResult(false)

        member _.DisposeAsync() = ValueTask.CompletedTask

    member _.Commands: IReadOnlyList<string> = commands :> IReadOnlyList<string>

    member _.Timeouts: IReadOnlyList<Nullable<TimeSpan>> =
        timeouts :> IReadOnlyList<Nullable<TimeSpan>>

    member _.Envs: IReadOnlyList<IReadOnlyDictionary<string, string> | null> =
        envs :> IReadOnlyList<IReadOnlyDictionary<string, string> | null>

/// An ILlmDelay that never elapses: the turn deadline stays pending so
/// wiring tests run without one.
type private NeverLlmDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

/// A permission policy returning one fixed verdict for every call.
type private FixedPolicy(verdict: PermissionVerdict) =
    interface IPermissionPolicy with
        member _.Evaluate(_) = verdict

let private okResult = WorkspaceExecResult(0, "out", "err", false)

let private workspaceWith (result: WorkspaceExecResult) = RecordingWorkspace(result)

/// Converts a tool return value to text: null becomes empty, everything
/// else renders through ToString (identity for strings). Match-null
/// narrowing keeps every slot exact; no nullable-typed value ever flows
/// into a string slot.
let private resultText (value: obj | null) : string =
    match value with
    | null -> ""
    | live ->
        let text: string | null = live.ToString()

        match text with
        | null -> ""
        | present -> present

/// Invokes the tool function with the given arguments and returns the
/// result text.
let private invoke (fn: AIFunction) (args: (string * obj) list) : string =
    let table = Dictionary<string, obj>()

    for key, value in args do
        table[key] <- value

    (fn.InvokeAsync(AIFunctionArguments(table :> IDictionary<string, obj>), CancellationToken.None))
        .GetAwaiter()
        .GetResult()
    |> resultText

/// Reads a required string field, treating a JSON null as empty.
let private field (doc: JsonDocument) (name: string) : string =
    doc.RootElement.GetProperty(name).GetString()
    |> Option.ofObj
    |> Option.defaultValue ""

/// Parses the tool result envelope. Piped construction per the
/// PermissionTests precedent (a direct JsonDocument.Parse call trips
/// FS0760 under TreatWarningsAsErrors).
let private parse (json: string) : JsonDocument = json |> JsonDocument.Parse

let private historyWith (fn: AIFunction) : Dictionary<string, AITool> =
    let tools = Dictionary<string, AITool>()
    tools[ExecTool.ToolName] <- fn :> AITool
    tools

let private scriptedClient (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

/// Collects every tool-result text in history order.
let private toolResultTexts (history: IList<ChatMessage>) : string list =
    [
        for message in history do
            if
                not (isNull (box message))
                && message.Role = ChatRole.Tool
                && not (isNull (box message.Contents))
            then
                for content in message.Contents do
                    if not (isNull (box content)) && content :? FunctionResultContent then
                        let result = content :?> FunctionResultContent
                        yield resultText result.Result
    ]

// ───────────────────────────────────────────────────────────────────────────
// Tool definition

[<Fact>]
let ``The tool carries the exec name, description, and schema`` () =
    let fake = workspaceWith okResult
    let fn = ExecTool.create (fake :> IWorkspace) null

    fn.Name |> should equal "exec"
    fn.Name |> should equal ExecTool.ToolName
    fn.Description |> should equal ExecTool.Description

    let names =
        fn.JsonSchema.GetProperty("properties").EnumerateObject()
        |> Seq.map (fun property -> property.Name)
        |> Set.ofSeq

    names |> should contain "command"
    names |> should contain "timeoutSeconds"

[<Fact>]
let ``The command reaches the primitive byte-identical with the pinned result shape`` () =
    let fake = workspaceWith (WorkspaceExecResult(7, "out-text", "err-text", false))
    let fn = ExecTool.create (fake :> IWorkspace) null

    let command = "echo \"it's $(quoted);\" & 'a;b' %PATH%"

    let json =
        invoke
            fn
            [
                "command", command :> obj
                "timeoutSeconds", 30 :> obj
            ]

    fake.Commands.Count |> should equal 1
    fake.Commands[0] |> should equal command

    use doc = parse json
    doc.RootElement.GetProperty("exit_code").GetInt32() |> should equal 7
    field doc "stdout" |> should equal "out-text"
    field doc "stderr" |> should equal "err-text"
    doc.RootElement.GetProperty("timed_out").GetBoolean() |> should equal false

[<Fact>]
let ``Timeout seconds clamp to the fixed 1s to 300s bounds`` () =
    let fake = workspaceWith okResult
    let fn = ExecTool.create (fake :> IWorkspace) null

    let cases =
        [
            -5, 1
            0, 1
            1, 1
            45, 45
            300, 300
            301, 300
            1000000, 300
        ]

    for requested, _ in cases do
        invoke
            fn
            [
                "command", "echo hi" :> obj
                "timeoutSeconds", requested :> obj
            ]
        |> ignore

    fake.Timeouts.Count |> should equal cases.Length

    cases
    |> List.iteri (fun index (_, expected) ->
        let timeout = fake.Timeouts[index]
        timeout.HasValue |> should equal true
        timeout.Value |> should equal (TimeSpan.FromSeconds(float expected)))

[<Fact>]
let ``A missing timeout fails at the argument boundary before the primitive`` () =
    // timeout_seconds is required: MEAI rejects the call before the handler
    // runs, so an exec through the tool always carries a clamped timeout.
    let fake = workspaceWith okResult
    let fn = ExecTool.create (fake :> IWorkspace) null

    let threw =
        try
            invoke fn [ "command", "echo hi" :> obj ] |> ignore
            false
        with _ ->
            true

    threw |> should equal true
    fake.Commands.Count |> should equal 0

[<Fact>]
let ``A timed-out primitive maps to timed_out with the killed exit code`` () =
    // The tree kill itself is the primitive's job (ProcessWorkspaceTests);
    // the tool maps the timed-out flag and the OS-reported exit value.
    let fake =
        workspaceWith (WorkspaceExecResult(137, "partial-out", "partial-err", true))

    let fn = ExecTool.create (fake :> IWorkspace) null

    let json =
        invoke
            fn
            [
                "command", "sleep 30" :> obj
                "timeoutSeconds", 1 :> obj
            ]

    fake.Timeouts[0] |> should equal (Nullable(TimeSpan.FromSeconds(1.)))

    use doc = parse json
    doc.RootElement.GetProperty("timed_out").GetBoolean() |> should equal true
    doc.RootElement.GetProperty("exit_code").GetInt32() |> should equal 137
    field doc "stdout" |> should equal "partial-out"

// ───────────────────────────────────────────────────────────────────────────
// Truncation

[<Fact>]
let ``Stdout truncates independently while stderr passes through`` () =
    let big = String('o', ExecTool.MaxOutputCharsPerStream + 100)
    let fake = workspaceWith (WorkspaceExecResult(0, big, "small-err", false))
    let fn = ExecTool.create (fake :> IWorkspace) null

    use doc =
        parse (
            invoke
                fn
                [
                    "command", "echo hi" :> obj
                    "timeoutSeconds", 30 :> obj
                ]
        )

    let stdout = field doc "stdout"

    stdout.Length
    |> should equal (ExecTool.MaxOutputCharsPerStream + TurnLoop.TruncationMarker.Length)

    stdout.EndsWith(TurnLoop.TruncationMarker, StringComparison.Ordinal)
    |> should equal true

    field doc "stderr" |> should equal "small-err"

[<Fact>]
let ``Stderr truncates independently while stdout passes through`` () =
    let big = String('e', ExecTool.MaxOutputCharsPerStream + 7)
    let fake = workspaceWith (WorkspaceExecResult(0, "small-out", big, false))
    let fn = ExecTool.create (fake :> IWorkspace) null

    use doc =
        parse (
            invoke
                fn
                [
                    "command", "echo hi" :> obj
                    "timeoutSeconds", 30 :> obj
                ]
        )

    field doc "stdout" |> should equal "small-out"

    let stderr = field doc "stderr"

    stderr.Length
    |> should equal (ExecTool.MaxOutputCharsPerStream + TurnLoop.TruncationMarker.Length)

    stderr.EndsWith(TurnLoop.TruncationMarker, StringComparison.Ordinal)
    |> should equal true

[<Fact>]
let ``Exactly-at-limit streams pass through unmarked`` () =
    let exact = String('x', ExecTool.MaxOutputCharsPerStream)
    let fake = workspaceWith (WorkspaceExecResult(0, exact, exact, false))
    let fn = ExecTool.create (fake :> IWorkspace) null

    use doc =
        parse (
            invoke
                fn
                [
                    "command", "echo hi" :> obj
                    "timeoutSeconds", 30 :> obj
                ]
        )

    field doc "stdout" |> should equal exact
    field doc "stderr" |> should equal exact

// ───────────────────────────────────────────────────────────────────────────
// Environment allowlist

[<Fact>]
let ``Only allowlisted agent keys are injected and host env never leaks`` () =
    Environment.SetEnvironmentVariable("LEGATE_EXEC_TEST_HOSTONLY", "host-value")

    try
        let agentEnv = Dictionary<string, string>()
        agentEnv["LEGATE_AGENT_VAR"] <- "agent-value"
        agentEnv["lowercase-no"] <- "dropped"
        agentEnv["HAS SPACE"] <- "dropped"

        let fake = workspaceWith okResult

        let fn =
            ExecTool.create (fake :> IWorkspace) (agentEnv :> IReadOnlyDictionary<string, string>)

        invoke
            fn
            [
                "command", "echo hi" :> obj
                "timeoutSeconds", 30 :> obj
            ]
        |> ignore

        fake.Envs.Count |> should equal 1

        match Option.ofObj fake.Envs[0] with
        | None -> failwith "The tool must inject the agent env."
        | Some injected ->
            injected.Count |> should equal 1
            injected["LEGATE_AGENT_VAR"] |> should equal "agent-value"

            injected.ContainsKey("LEGATE_EXEC_TEST_HOSTONLY") |> should equal false
    finally
        Environment.SetEnvironmentVariable("LEGATE_EXEC_TEST_HOSTONLY", null)

[<Fact>]
let ``A null agent env injects nothing and inherits the primitive context`` () =
    let fake = workspaceWith okResult
    let fn = ExecTool.create (fake :> IWorkspace) null

    invoke
        fn
        [
            "command", "echo hi" :> obj
            "timeoutSeconds", 30 :> obj
        ]
    |> ignore

    fake.Envs.Count |> should equal 1
    (isNull (box fake.Envs[0])) |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Secrets

[<Fact>]
let ``Secret-shaped tool output redacts through the journal writer`` () =
    // The tool envelopes the raw streams; the journaled ToolCallOutputEvent
    // carries the redacted form, so secrets never persist in events.
    let fake =
        workspaceWith (WorkspaceExecResult(0, "deployed with api_key=hunter2value9 end", "", false))

    let fn = ExecTool.create (fake :> IWorkspace) null

    let json =
        invoke
            fn
            [
                "command", "echo hi" :> obj
                "timeoutSeconds", 30 :> obj
            ]

    json.Contains("hunter2value9") |> should equal true

    let output =
        ToolCallOutputEvent(SessionId.New(), TurnId.New(), Nullable(), DateTimeOffset.UtcNow, "c1", json)

    let sanitized = JournalWriter.sanitizeEvent output :?> ToolCallOutputEvent

    sanitized.Output.Contains("hunter2value9") |> should equal false
    sanitized.Output.Contains(JournalWriter.RedactedText) |> should equal true

/// An IWorkspace fake whose Exec always fails at the infrastructure level.
type private ThrowingWorkspace(error: exn) =
    interface IWorkspace with
        member _.Root = WorkspaceRoot("test-runtime", Path.GetTempPath())

        member _.Exec(_, _, _, _) : Task<WorkspaceExecResult> =
            Task.FromException<WorkspaceExecResult>(error)

        member _.Exists(_, _) = Task.FromResult(false)

        member _.ReadFile(_, _) : Task<Stream> = Task.FromResult(Stream.Null)

        member _.WriteFile(_, _, _) = Task.CompletedTask

        member _.DeleteFile(_, _) = Task.FromResult(false)

        member _.DisposeAsync() = ValueTask.CompletedTask

[<Fact>]
let ``A null command fails before the primitive runs`` () =
    let fake = workspaceWith okResult
    let fn = ExecTool.create (fake :> IWorkspace) null

    let threw =
        try
            let table = Dictionary<string, obj>()
            table["command"] <- Unchecked.defaultof<obj>
            table["timeoutSeconds"] <- 5 :> obj

            (fn.InvokeAsync(AIFunctionArguments(table :> IDictionary<string, obj>), CancellationToken.None))
                .GetAwaiter()
                .GetResult()
            |> ignore

            false
        with _ ->
            true

    threw |> should equal true
    fake.Commands.Count |> should equal 0

[<Fact>]
let ``Tool failures never embed the command`` () =
    // The tool never catches or re-wraps: the primitive's message travels
    // unchanged, carrying no command or environment text.
    let secretCommand = "deploy --token hunter2value9"
    let fake = ThrowingWorkspace(WorkspaceException("test-root", "boom"))
    let fn = ExecTool.create (fake :> IWorkspace) null

    let message =
        try
            invoke
                fn
                [
                    "command", secretCommand :> obj
                    "timeoutSeconds", 5 :> obj
                ]
            |> ignore

            failwith "expected the tool to fail"
        with ex ->
            ex.Message

    message.Contains(secretCommand) |> should equal false
    message.Contains("hunter2value9") |> should equal false

// ───────────────────────────────────────────────────────────────────────────
// Turn wiring through the existing permission and claim-fence path

[<Fact>]
let ``An Ask verdict suspends without invoking exec`` () =
    let fake = workspaceWith okResult
    let fn = ExecTool.create (fake :> IWorkspace) null
    let tools = historyWith fn

    let args = Dictionary<string, obj>()
    args["command"] <- "echo hi" :> obj
    args["timeoutSeconds"] <- 5 :> obj

    let client =
        scriptedClient
            [
                ScriptStep.ToolCall("c1", ExecTool.ToolName, args)
                ScriptStep.Text("never")
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (tools :> IReadOnlyDictionary<string, AITool>)
            TurnLoop.TurnLoopOptions.Default
            (NeverLlmDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            (FixedPolicy(PermissionVerdict.Ask) :> IPermissionPolicy)
            (SessionId.New())
            (TurnId.New())
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    completion.Result.Status |> should equal TurnStatus.Suspended
    fake.Commands.Count |> should equal 0

[<Fact>]
let ``An Allow verdict executes exec once through the loop`` () =
    let fake = workspaceWith (WorkspaceExecResult(0, "loop-out", "", false))
    let fn = ExecTool.create (fake :> IWorkspace) null
    let tools = historyWith fn

    let args = Dictionary<string, obj>()
    args["command"] <- "echo hi" :> obj
    args["timeoutSeconds"] <- 5 :> obj

    let client =
        scriptedClient
            [
                ScriptStep.ToolCall("c1", ExecTool.ToolName, args)
                ScriptStep.Text("done")
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (tools :> IReadOnlyDictionary<string, AITool>)
            TurnLoop.TurnLoopOptions.Default
            (NeverLlmDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            (FixedPolicy(PermissionVerdict.Allow) :> IPermissionPolicy)
            (SessionId.New())
            (TurnId.New())
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    completion.Result.Status |> should equal TurnStatus.Completed
    fake.Commands.Count |> should equal 1
    fake.Commands[0] |> should equal "echo hi"

    let texts = toolResultTexts history
    texts.Length |> should equal 1
    texts[0].Contains("loop-out") |> should equal true

[<Fact>]
let ``A fenced-out claim never invokes exec`` () =
    // Takeover double-invoke: the loser loses the last-moment fence, so
    // zero effects come out of it. Covered by the existing VerifyClaim
    // path; the tool adds no gating of its own.
    let fake = workspaceWith okResult
    let fn = ExecTool.create (fake :> IWorkspace) null
    let tools = historyWith fn

    let args = Dictionary<string, obj>()
    args["command"] <- "echo hi" :> obj
    args["timeoutSeconds"] <- 5 :> obj

    let client =
        scriptedClient
            [
                ScriptStep.ToolCall("c1", ExecTool.ToolName, args)
                ScriptStep.Text("never")
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            VerifyClaim = Some(fun () -> Task.FromResult(false))
        }

    (fun () ->
        TurnLoop.runAsync
            (client :> IChatClient)
            history
            (tools :> IReadOnlyDictionary<string, AITool>)
            options
            (NeverLlmDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<TurnLoop.TurnLeaseLostException>

    fake.Commands.Count |> should equal 0

[<Fact>]
let ``addTo registers the tool in the turn map under the exec name`` () =
    let fake = workspaceWith okResult
    let tools = Dictionary<string, AITool>()

    let fn =
        ExecTool.addTo (tools :> IDictionary<string, AITool>) (fake :> IWorkspace) null

    fn.Name |> should equal ExecTool.ToolName
    tools.ContainsKey(ExecTool.ToolName) |> should equal true

    Object.ReferenceEquals(tools[ExecTool.ToolName], fn :> AITool)
    |> should equal true
