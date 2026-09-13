// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text.Json.Serialization
open Microsoft.Extensions.AI

// Inbound message contracts. A UserMessage is what a host sends into a
// session and the only thing that starts a turn; a Reply is what the host
// answers while a turn is suspended. All types serialise with
// System.Text.Json: parts reuse the AIContent polymorphic contract shipped
// by Microsoft.Extensions.AI.Abstractions, and Reply carries stable $type
// discriminators. Public signatures stay BCL-only (no option, list, or DU).

/// How a message passed to <c>Prompt</c> is delivered to a session that may
/// already be running a turn.
type DeliveryMode =

    /// Appended to the session inbox and acted on once the turn currently
    /// running (if any) finishes. The ordinary delivery mode.
    | Queue = 0

    /// Folded into the turn currently running at the next model-iteration
    /// boundary without interrupting it. Use for steering context that
    /// should shape the turn in progress.
    | Inject = 1

    /// Pre-empts the turn currently running: the session suspends that turn
    /// and starts a new turn on this message.
    | Interrupt = 2

/// What the user sent: an ordered list of parts
/// (<see cref="T:Microsoft.Extensions.AI.AIContent" />: text, file, image).
/// The only message that starts a turn; the runtime journals it before
/// acting on it. Parts and metadata are copied on construction, so later
/// changes to the caller's collections never reach the message.
/// <param name="parts">The parts of the message, in order. Must not be null.</param>
/// <param name="metadata">Host metadata carried with the message, or null when there is none.</param>
type UserMessage(parts: IReadOnlyList<AIContent>, metadata: IReadOnlyDictionary<string, string> | null) =

    do
        if box parts |> isNull then
            raise (ArgumentNullException(nameof parts))

    let partsList = ResizeArray<AIContent>(parts)

    let metadataSnapshot =
        match metadata with
        | null -> None
        | value ->
            let snapshot = Dictionary<string, string>(value.Count)

            for entry in value do
                snapshot[entry.Key] <- entry.Value

            Some(snapshot :> IReadOnlyDictionary<string, string>)

    /// The ordered parts of the message.
    member _.Parts: IReadOnlyList<AIContent> = partsList :> IReadOnlyList<AIContent>

    /// Host metadata carried with the message (for example where it came
    /// from), or null when the message has none.
    member _.Metadata: IReadOnlyDictionary<string, string> | null =
        match metadataSnapshot with
        | Some value -> value
        | None -> null

    /// Creates a message with a single text part.
    /// <param name="text">The text of the message. Must be a non-empty string.</param>
    /// <returns>A message carrying one <see cref="T:Microsoft.Extensions.AI.TextContent" /> part.</returns>
    static member Text(text: string) =
        if String.IsNullOrWhiteSpace text then
            raise (ArgumentException("A text part must be a non-empty string.", nameof text))

        let parts = [ TextContent(text) :> AIContent ]

        UserMessage(parts, null)

    /// Creates a message with a single file part.
    /// <param name="data">The file contents.</param>
    /// <param name="mediaType">The IANA media type of the file, for example "application/pdf". Must be a non-empty string.</param>
    /// <returns>A message carrying one <see cref="T:Microsoft.Extensions.AI.DataContent" /> part.</returns>
    static member WithFile(data: ReadOnlyMemory<byte>, mediaType: string) =
        if String.IsNullOrWhiteSpace mediaType then
            raise (ArgumentException("A file part must declare a non-empty media type.", nameof mediaType))

        let parts =
            [
                DataContent(data, mediaType) :> AIContent
            ]

        UserMessage(parts, null)

/// What the host decided about a permission request the agent raised.
type PermissionDecisionKind =

    /// The request is allowed for this call only; an identical later request
    /// asks again.
    | AllowOnce = 0

    /// The request is allowed for the rest of the session.
    | AllowForSession = 1

    /// The request is denied; the turn continues without the tool's effect.
    | Deny = 2

/// The host's answer to something the agent is waiting on: a
/// <see cref="T:Legate.PermissionDecision" /> or a
/// <see cref="T:Legate.QuestionAnswer" />. Resumes a suspended turn; never
/// starts one. Serialises polymorphically: every concrete reply carries a
/// stable <c>$type</c> discriminator on the wire.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<PermissionDecision>, "permissionDecision")>]
[<JsonDerivedType(typeof<QuestionAnswer>, "questionAnswer")>]
type Reply() = class end

/// The host's decision on a permission request raised by a tool call.
/// <param name="requestId">The id of the permission request being answered.</param>
/// <param name="decision">What the host decided.</param>
and [<Sealed>] PermissionDecision(requestId: string, decision: PermissionDecisionKind) =
    inherit Reply()

    /// The id of the permission request being answered.
    member _.RequestId = requestId

    /// What the host decided.
    member _.Decision = decision

/// The host's answer to a question the agent asked.
/// <param name="questionId">The id of the question being answered.</param>
/// <param name="answer">The host's answer, verbatim.</param>
and [<Sealed>] QuestionAnswer(questionId: string, answer: string) =
    inherit Reply()

    /// The id of the question being answered.
    member _.QuestionId = questionId

    /// The host's answer, verbatim.
    member _.Answer = answer
