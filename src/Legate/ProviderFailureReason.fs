// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System

// Pure ProviderException-property reason shaping shared by the TurnLoop
// provider-call failure path and the SessionActor fault fallback (issue
// 349). Reads the structured properties only (provider id, HTTP status,
// message), never raw bodies or exception text, so reasons and the
// redacted log lines that carry them stay secret-free by construction:
// ProviderException messages are contract-bound to never embed secrets
// or tool arguments (see Docs/agents/code-rules.md § Error handling).
// The message portion is bounded so reasons stay far below the journal
// per-field limit; failedReasonOf passes the shaped reason through
// untouched.
module internal ProviderFailureReason =

    /// Status text when the failure carries no HTTP status (a network
    /// failure the coordinator wrapped without one).
    [<Literal>]
    let NoStatusText = "no-status"

    /// Maximum provider-message chars a shaped reason carries before
    /// truncation with the marker. Far below the journal per-field
    /// bound, so a chatty provider message never grows the journal
    /// event past its limits.
    [<Literal>]
    let MaxMessageChars = 1000

    /// Bounds one provider message for reason shaping: null and empty
    /// pass through as empty, longer text truncates with the marker.
    /// <param name="message">The provider message, or null.</param>
    /// <returns>The bounded message, or empty when there was none.</returns>
    let private boundMessage (message: string) : string =
        if String.IsNullOrEmpty message then
            ""
        elif message.Length > MaxMessageChars then
            message.Substring(0, MaxMessageChars) + JournalWriter.TruncationMarker
        else
            message

    /// Shapes one provider failure into a turn-failure reason carrying
    /// the provider id, the HTTP status (or no-status for network
    /// failures), and the provider message: enough to distinguish a
    /// 401 bad key from a 404 bad model from a network failure.
    /// <param name="failure">The provider failure. Must not be null.</param>
    /// <returns>The secret-free reason the turn failure carries.</returns>
    let formatProviderFailure (failure: ProviderException) : string =
        ArgumentNullException.ThrowIfNull(failure)

        let providerId =
            if String.IsNullOrWhiteSpace failure.ProviderId then
                "unknown"
            else
                failure.ProviderId.Trim()

        let statusText =
            if failure.Status.HasValue then
                sprintf "HTTP %d" failure.Status.Value
            else
                NoStatusText

        match boundMessage failure.Message with
        | "" -> sprintf "The '%s' provider call failed with %s." providerId statusText
        | message -> sprintf "The '%s' provider call failed with %s: %s" providerId statusText message

    /// Shapes one faulted-turn error into the fallback reason: provider
    /// faults carry the detailed provider shape, every other fault keeps
    /// the legacy type-name shape, and a null error reads as Exception.
    /// <param name="error">The fault that escaped the turn, or null.</param>
    /// <returns>The secret-free reason the turn failure carries.</returns>
    let formatFault (error: exn) : string =
        match error with
        | :? ProviderException as providerFailure when not (isNull (box providerFailure)) ->
            formatProviderFailure providerFailure
        | _ when isNull (box error) -> "The turn faulted: Exception."
        | _ -> sprintf "The turn faulted: %s." (error.GetType().Name)
