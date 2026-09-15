// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// The ask_user built-in tool: the model asks the host a question instead
// of executing. The tool carries the schema (question required, options an
// optional string-array hint; absent or empty means free text, present
// means a multiple-choice hint, and the answer is always a plain string),
// but the turn loop never invokes it: a call to this tool name suspends
// with a QuestionSuspension through the existing resumeQuestionAsync path,
// and the matching QuestionAnswer resumes with the answer as the tool
// result. Direct invocation is out of contract and raises: answering
// without the loop would hallucinate user input. Headless answers (the
// AskUserOptions Fail/AnswerWith policy) are applied by the loop around
// the suspension, never by this function.

/// The tool's identity: its model-facing name and description, kept in
/// one internal module so the function type and the factory serve the
/// same literals. The name is the TurnLoop ask_user constant itself, so
/// the schema the model reads and the suspension the loop parks can never
/// drift apart.
module internal AskUserIdentity =

    /// The tool's name as the model calls it: the question-suspension path.
    let name = TurnLoop.AskUserToolName

    /// The tool's description as the model reads it: what the tool does,
    /// its arguments, and how the answer comes back.
    let description =
        "Asks the host a question (ask_user). "
        + "Pass what the agent asks in 'question' (required) and an optional 'options' hint "
        + "with candidate answers; absent or empty options mean free text. "
        + "The turn suspends until the host answers with a plain string, "
        + "which resumes as this call's result."

/// The tool's argument schema, kept in one internal module so the schema
/// tests assert on the same JSON the tool serves. Option-hint extraction
/// lives with the turn loop (TurnLoop.extractOptions): the loop is the
/// only executor, so the parse exists exactly once.
module internal AskUserSchema =

    /// The JSON schema served on the tool's JsonSchema: question is a
    /// required string, options is an optional array of strings (a
    /// multiple-choice hint; absent or empty means free text).
    let json =
        """{"type":"object","description":"Arguments for the ask_user tool.","properties":{"question":{"type":"string","description":"The question the agent asks the host. Required."},"options":{"type":"array","items":{"type":"string"},"description":"An optional multiple-choice hint: candidate answers the host may pick from. Absent or empty means free text; the answer is always a plain string."}},"required":["question"],"additionalProperties":false}"""

    /// The single parsed schema document backing every tool instance; the
    /// JsonSchema elements borrow it, so it lives as long as the process.
    let document = JsonDocument.Parse json

    /// Reads one raw argument by name; missing reads as null.
    /// <param name="args">The call's arguments.</param>
    /// <param name="name">The argument to read.</param>
    /// <returns>The raw value, or null when the argument is missing.</returns>
    let argument (args: AIFunctionArguments) (name: string) : obj | null =
        let mutable value: obj | null = null

        if args.TryGetValue(name, &value) then value else null

    /// Reads one argument value as text: plain strings and JSON string
    /// elements; anything else is not text.
    /// <param name="value">The raw argument value.</param>
    /// <returns>The text, or null when the value is not text.</returns>
    let asText (value: obj | null) : string | null =
        match value with
        | null -> null
        | :? string as text -> text
        | :? JsonElement as element when element.ValueKind = JsonValueKind.String -> element.GetString()
        | _ -> null

/// The <c>ask_user</c> function served to the model. Internal: hosts hold
/// the <see cref="T:Microsoft.Extensions.AI.AIFunction" /> that
/// <see cref="T:Legate.AskUserTool" /> hands back, never this type. The
/// function validates the question and then raises: the turn loop
/// intercepts ask_user calls into the question-suspension path before any
/// invocation, so a direct call is out of contract.
[<Sealed>]
type internal AskUserFunction() =
    inherit AIFunction()

    /// This tool's name for error text.
    override _.Name = AskUserIdentity.name

    /// This tool's description for the model.
    override _.Description = AskUserIdentity.description

    /// This tool's argument schema.
    override _.JsonSchema = AskUserSchema.document.RootElement

    /// Validates the call's question, then raises: execution belongs to
    /// the turn loop's question suspension (suspend on the call, resume
    /// with the host's answer as the tool result), never to this
    /// function. A missing or blank question raises
    /// <see cref="T:Legate.ToolException" />; a well-formed direct call
    /// raises <see cref="T:System.InvalidOperationException" />.
    /// Cancellation propagates as-is.
    override _.InvokeCoreAsync
        (args: AIFunctionArguments, cancellationToken: CancellationToken)
        : ValueTask<obj | null> =
        ValueTask<obj | null>(
            task {
                cancellationToken.ThrowIfCancellationRequested()

                let args = if isNull (box args) then AIFunctionArguments() else args

                match Option.ofObj (AskUserSchema.asText (AskUserSchema.argument args "question")) with
                | Some question when not (String.IsNullOrWhiteSpace question) ->
                    return
                        raise (
                            InvalidOperationException(
                                "The ask_user tool runs through the turn loop's question suspension: invoke it through a turn, never directly."
                            )
                        )
                | _ ->
                    return
                        raise (
                            ToolException(
                                AskUserIdentity.name,
                                "The ask_user tool needs a question: pass what the agent asks the host in 'question'."
                            )
                        )
            }
        )

/// Resolves the effective ask_user headless policy: the session's
/// override wins when set, otherwise the configured runtime default.
/// Internal: the session pipeline (#62) applies it; tests assert the
/// precedence here.
module internal AskUserPolicy =

    /// Resolves the policy a turn answers questions with: the session
    /// override when the session carries one, otherwise the configured
    /// default (Fail when the configuration carries none).
    /// <param name="configured">The bound runtime options. Must not be null.</param>
    /// <param name="session">The session's open options, or null for the configured default.</param>
    /// <returns>The effective policy, never null.</returns>
    let resolve (configured: LegateOptions) (session: SessionOptions | null) : AskUserOptions =
        ArgumentNullException.ThrowIfNull(configured)

        let configuredPolicy =
            if isNull (box configured.AskUser) then
                AskUserOptions()
            else
                configured.AskUser

        match Option.ofObj session with
        | None -> configuredPolicy
        | Some live ->
            match Option.ofObj live.AskUser with
            | Some policy -> policy
            | None -> configuredPolicy

/// Builds the <c>ask_user</c> built-in tool: the schema the model asks
/// questions with. The tool never executes: the turn loop intercepts its
/// calls into the existing question-suspension path, and the headless
/// policy answers where no host can.
type AskUserTool private () =

    /// The tool's name as the model calls it: the question-suspension
    /// path. Validated against <see cref="T:Legate.ToolNameRules" /> when
    /// the tool is built.
    static member ToolName: string = AskUserIdentity.name

    /// The tool's description as the model reads it: what the tool does,
    /// its arguments, and how the answer comes back.
    static member Description: string = AskUserIdentity.description

    /// Builds the tool: the ask_user schema with no execution behind it.
    /// <returns>The tool to offer to the model.</returns>
    static member Create() : AIFunction =
        ToolNameRules.Validate(AskUserTool.ToolName) |> ignore
        AskUserFunction() :> AIFunction
