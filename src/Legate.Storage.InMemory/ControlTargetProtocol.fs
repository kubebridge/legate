// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage

open System
open System.Collections.Generic
open System.Text.Json
open Legate

// Linked into each provider. The caller supplies a transaction-consistent minimal
// projection and persists the returned state in that same serialization boundary.
[<CLIMutable; NoComparison>]
type internal ControlState =
    {
        Version: int
        Binding: AbortTarget | null
        Receipts: HostAbortReceipt array
        Decisions: ControlTargetDecision array
    }

type internal ControlContext =
    {
        SessionId: SessionId
        Lifecycle: SessionState
        Now: DateTimeOffset
        Prime: TurnClaim option
        Entries: Map<int64, bool>
    }

module internal ControlTargetProtocol =
    let jsonOptions =
        JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase)

    let fresh () =
        {
            Version = 1
            Binding = null
            Receipts = [||]
            Decisions = [||]
        }

    let encode (state: ControlState) =
        let fields = Dictionary<string, obj | null>()
        fields["version"] <- box state.Version

        fields["binding"] <-
            match state.Binding with
            | null -> null
            | target -> box target

        fields["receipts"] <- box state.Receipts
        fields["decisions"] <- box state.Decisions
        JsonSerializer.Serialize(fields, jsonOptions)

    let validate sessionId (state: ControlState) =
        if
            isNull (box state)
            || state.Version <> 1
            || isNull (box state.Receipts)
            || isNull (box state.Decisions)
        then
            raise (
                InvalidSessionStateException(
                    sessionId,
                    "unsupportedControlFormat",
                    "Unsupported control data. Start a new session; legacy control attribution cannot be inferred."
                )
            )

        match state.Binding with
        | null -> ()
        | target when
            target.FormatVersion <> 1
            || target.SessionId <> sessionId
            || target.InboxPosition <= 0L
            ->
            raise (
                InvalidSessionStateException(
                    sessionId,
                    "unsupportedControlFormat",
                    "Invalid control association. Start a new session."
                )
            )
        | _ -> ()

        if
            state.Receipts
            |> Array.exists (fun receipt ->
                isNull (box receipt)
                || receipt.FormatVersion <> 1
                || receipt.SessionId <> sessionId
                || receipt.Outcome <> HostAbortOutcome.Accepted
                || not receipt.AcceptedAt.HasValue
                || not receipt.Cause.HasValue
                || (receipt.Cause.Value <> StopCause.ExplicitAbort
                    && receipt.Cause.Value <> StopCause.HostShutdown)
                || isNull (box receipt.Reason))
            || state.Decisions
               |> Array.exists (fun decision ->
                   isNull (box decision)
                   || decision.FormatVersion <> 1
                   || decision.SessionId <> sessionId
                   || String.IsNullOrWhiteSpace decision.DecisionId
                   || (decision.Status <> TurnStatus.Completed
                       && decision.Status <> TurnStatus.Failed
                       && decision.Status <> TurnStatus.Aborted)
                   || (decision.ProposedStatus <> TurnStatus.Completed
                       && decision.ProposedStatus <> TurnStatus.Failed
                       && decision.ProposedStatus <> TurnStatus.Aborted))
        then
            raise (
                InvalidSessionStateException(
                    sessionId,
                    "unsupportedControlFormat",
                    "Unsupported control evidence. Start a new session."
                )
            )

    let private decodeCore sessionId (text: string) =
        use document = JsonDocument.Parse(text)
        let root = document.RootElement
        let mutable version = Unchecked.defaultof<JsonElement>

        if
            not (root.TryGetProperty("version", &version))
            || version.ValueKind <> JsonValueKind.Number
            || version.GetInt32() <> 1
        then
            raise (
                InvalidSessionStateException(
                    sessionId,
                    "unsupportedControlFormat",
                    "Unsupported control data. Start a new session."
                )
            )

        let state =
            {
                Version = version.GetInt32()
                Binding = JsonSerializer.Deserialize<AbortTarget>(root.GetProperty("binding"), jsonOptions)
                Receipts =
                    JsonSerializer.Deserialize<HostAbortReceipt array>(root.GetProperty("receipts"), jsonOptions)
                    |> Option.ofObj
                    |> Option.defaultWith (fun () ->
                        raise (
                            InvalidSessionStateException(
                                sessionId,
                                "unsupportedControlFormat",
                                "Missing receipt data."
                            )
                        ))
                Decisions =
                    JsonSerializer.Deserialize<ControlTargetDecision array>(root.GetProperty("decisions"), jsonOptions)
                    |> Option.ofObj
                    |> Option.defaultWith (fun () ->
                        raise (
                            InvalidSessionStateException(
                                sessionId,
                                "unsupportedControlFormat",
                                "Missing decision data."
                            )
                        ))
            }

        validate sessionId state
        state

    let decode sessionId text =
        try
            decodeCore sessionId text
        with
        | :? JsonException
        | :? KeyNotFoundException
        | :? FormatException
        | :? InvalidOperationException ->
            raise (
                InvalidSessionStateException(
                    sessionId,
                    "unsupportedControlFormat",
                    "Unsupported control data. Start a new session; no legacy attribution is inferred."
                )
            )

    let reason (text: string) =
        if isNull (box text) then
            raise (ArgumentNullException("reason"))

        if text.Length > 512 || text |> Seq.exists Char.IsControl then
            raise (ArgumentException("A reason must have at most 512 non-control characters.", "reason"))

    let targetId (id: TurnId) =
        if id = Unchecked.defaultof<TurnId> then
            raise (ArgumentException("An exact nonempty target is required.", "targetTurnId"))

    let authority (context: ControlContext) (claim: TurnClaim) =
        if isNull (box claim) then
            raise (ArgumentNullException("capturedPrimeClaim"))

        match context.Prime with
        | Some live ->
            live.TurnId = claim.TurnId
            && live.Token = claim.Token
            && live.Owner = claim.Owner
            && live.Attempt = claim.Attempt
            && live.ExpiresAt > context.Now
        | None -> false

    let result outcome (target: AbortTarget | null) (decision: ControlTargetDecision | null) =
        {
            Outcome = outcome
            Target = target
            Decision = decision
        }

    let associated turn position (target: AbortTarget) =
        target.TurnId = turn && target.InboxPosition = position

    let receipt turn state =
        state.Receipts |> Array.tryFind (fun receipt -> receipt.TurnId = turn)

    let decision turn state =
        state.Decisions |> Array.tryFind (fun decision -> decision.TurnId = turn)

    let requireTransition sessionId lifecycle state =
        validate sessionId state

        match state.Binding with
        | null -> ()
        | target when
            lifecycle = SessionState.WaitingForInput
            && (receipt target.TurnId state |> Option.isSome)
            ->
            raise (InvalidSessionStateException(sessionId, "controlPending", "Accepted stop forbids a new suspension."))
        | _ -> ()

    let read (context: ControlContext) state : AbortTarget | null =
        validate context.SessionId state

        match state.Binding with
        | null when
            context.Lifecycle = SessionState.Running
            && (state.Decisions |> Array.exists _.Retired)
            ->
            raise (
                InvalidSessionStateException(
                    context.SessionId,
                    "controlPending",
                    "Control retirement committed but the running lifecycle has not reached its next boundary; fresh activation cannot prime or infer new work."
                )
            )
        | null when context.Lifecycle = SessionState.Running ->
            raise (
                InvalidSessionStateException(
                    context.SessionId,
                    "missingControlTarget",
                    "Running work has no supported target. Start a new session."
                )
            )
        | null -> null
        | target ->
            if not (context.Entries.ContainsKey target.InboxPosition) then
                raise (
                    InvalidSessionStateException(
                        context.SessionId,
                        "missingControlAssociation",
                        "Current work has no provable original entry. Start a new session."
                    )
                )

            { target with
                Stop = receipt target.TurnId state |> Option.toObj
            }

    // Recovery changes only the existing prime fence. It never binds a new target,
    // selects another entry, or treats a stop receipt as authority to settle.
    let recover context state turn owner leaseDuration =
        targetId turn

        if String.IsNullOrWhiteSpace owner then
            raise (ArgumentException("A recovery owner is required.", "owner"))

        if leaseDuration <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException("leaseDuration"))

        let target = read context state

        match target with
        | null -> ControlOperationOutcome.TargetChanged, None
        | target when target.TurnId <> turn -> ControlOperationOutcome.TargetChanged, None
        | target when target.State <> ControlTargetState.Active || not (isNull (box target.Stop)) ->
            ControlOperationOutcome.Stopped, None
        | _ ->
            match context.Prime with
            | Some prime when prime.ExpiresAt <= context.Now ->
                ControlOperationOutcome.Applied,
                Some
                    { prime with
                        Token = Guid.NewGuid().ToString("N")
                        Owner = owner
                        ExpiresAt = context.Now + leaseDuration
                        Attempt = prime.Attempt + 1
                    }
            | _ -> ControlOperationOutcome.LostAuthority, None

    let request context state turn cause text =
        targetId turn
        reason text

        if cause <> StopCause.ExplicitAbort && cause <> StopCause.HostShutdown then
            raise (ArgumentOutOfRangeException("cause", "Host stop requires ExplicitAbort or HostShutdown."))

        validate context.SessionId state

        let refusal outcome terminal =
            {
                FormatVersion = 1
                SessionId = context.SessionId
                TurnId = turn
                Outcome = outcome
                AcceptedAt = Nullable()
                Cause = Nullable()
                Reason = null
                TerminalStatus = terminal
            },
            state

        match receipt turn state with
        | Some original ->
            { original with
                Outcome = HostAbortOutcome.AlreadyAccepted
            },
            state
        | None ->
            match decision turn state with
            | Some known -> refusal HostAbortOutcome.AlreadyTerminal (Nullable known.Status)
            | None ->
                if
                    context.Lifecycle = SessionState.Closed
                    || context.Lifecycle = SessionState.WaitingForInput
                then
                    raise (
                        InvalidSessionStateException(
                            context.SessionId,
                            context.Lifecycle.ToString(),
                            "This lifecycle does not accept a new host stop request."
                        )
                    )

                let target = read context state

                match target with
                | null -> refusal HostAbortOutcome.NoCurrentTurn (Nullable())
                | target when target.TurnId <> turn -> refusal HostAbortOutcome.TargetChanged (Nullable())
                | target when target.State <> ControlTargetState.Active ->
                    refusal HostAbortOutcome.TargetChanged (Nullable())
                | _ when context.Lifecycle <> SessionState.Running ->
                    refusal HostAbortOutcome.NoCurrentTurn (Nullable())
                | _ ->
                    let accepted =
                        {
                            FormatVersion = 1
                            SessionId = context.SessionId
                            TurnId = turn
                            Outcome = HostAbortOutcome.Accepted
                            AcceptedAt = Nullable context.Now
                            Cause = Nullable cause
                            Reason = text
                            TerminalStatus = Nullable()
                        }

                    accepted,
                    { state with
                        Receipts = Array.append state.Receipts [| accepted |]
                    }

    let check context state turn position claim =
        validate context.SessionId state
        let target = read context state

        if not (authority context claim) then
            result ControlOperationOutcome.LostAuthority target null
        else
            match target with
            | null -> result ControlOperationOutcome.TargetChanged null null
            | target when not (associated turn position target) ->
                result ControlOperationOutcome.TargetChanged target null
            | target when target.State <> ControlTargetState.Active || not (isNull (box target.Stop)) ->
                result ControlOperationOutcome.Stopped target (decision turn state |> Option.toObj)
            | target -> result ControlOperationOutcome.Applied target null

    let bind context state turn position claim =
        targetId turn
        validate context.SessionId state

        if not (authority context claim) then
            result ControlOperationOutcome.LostAuthority state.Binding null, state
        elif
            context.Lifecycle = SessionState.Closed
            || not (context.Entries.ContainsKey position)
        then
            result ControlOperationOutcome.TargetChanged state.Binding null, state
        else
            match state.Binding with
            | null when receipt turn state |> Option.isSome || decision turn state |> Option.isSome ->
                result ControlOperationOutcome.Conflict null null, state
            | null ->
                let target =
                    {
                        FormatVersion = 1
                        SessionId = context.SessionId
                        TurnId = turn
                        InboxPosition = position
                        State = ControlTargetState.Active
                        Stop = null
                    }

                result ControlOperationOutcome.Applied target null, { state with Binding = target }
            | _ -> check context state turn position claim, state

    let decide context state turn position claim id status (cause: Nullable<StopCause>) (text: string | null) =
        targetId turn

        if String.IsNullOrWhiteSpace id || id.Length > 128 then
            raise (ArgumentException("A bounded stable decision identifier is required.", "decisionId"))

        if
            status <> TurnStatus.Completed
            && status <> TurnStatus.Failed
            && status <> TurnStatus.Aborted
        then
            raise (ArgumentOutOfRangeException("proposedTerminalStatus"))

        if status = TurnStatus.Aborted then
            if not cause.HasValue || not (Enum.IsDefined(typeof<StopCause>, cause.Value)) then
                raise (ArgumentException("Aborted requires a valid stop cause.", "proposedCause"))

            match text with
            | null -> raise (ArgumentNullException("proposedReason"))
            | text -> reason text
        elif cause.HasValue || not (isNull text) then
            raise (ArgumentException("Only Aborted proposals carry stop metadata."))

        validate context.SessionId state

        match decision turn state with
        | Some known ->
            let outcome =
                if known.DecisionId <> id then
                    ControlOperationOutcome.DecisionConflict
                elif
                    known.InboxPosition <> position
                    || known.ProposedStatus <> status
                    || known.ProposedCause <> cause
                    || known.ProposedReason <> text
                then
                    ControlOperationOutcome.Conflict
                else
                    ControlOperationOutcome.AlreadyDecided

            result outcome state.Binding known, state
        | None ->
            if state.Decisions |> Array.exists (fun known -> known.DecisionId = id) then
                result ControlOperationOutcome.Conflict state.Binding null, state
            elif not (authority context claim) then
                result ControlOperationOutcome.LostAuthority state.Binding null, state
            else
                match state.Binding with
                | null -> result ControlOperationOutcome.TargetChanged null null, state
                | target when
                    not (associated turn position target)
                    || not (context.Entries.ContainsKey position)
                    ->
                    result ControlOperationOutcome.TargetChanged target null, state
                | target ->
                    let selected, stopCause, stopReason =
                        match receipt turn state with
                        | Some stop -> TurnStatus.Aborted, stop.Cause, stop.Reason
                        | None -> status, cause, text

                    let evidence =
                        {
                            FormatVersion = 1
                            SessionId = context.SessionId
                            TurnId = turn
                            InboxPosition = position
                            DecisionId = id
                            ProposedStatus = status
                            ProposedCause = cause
                            ProposedReason = text
                            Status = selected
                            Cause = stopCause
                            Reason = stopReason
                            DecidedAt = context.Now
                            Retired = false
                        }

                    let pending =
                        { target with
                            State = ControlTargetState.TerminalPendingRetirement
                        }

                    result ControlOperationOutcome.Applied pending evidence,
                    { state with
                        Binding = pending
                        Decisions = Array.append state.Decisions [| evidence |]
                    }

    let retire context state turn position claim id =
        validate context.SessionId state

        match decision turn state with
        | Some known when known.DecisionId <> id || known.InboxPosition <> position ->
            result ControlOperationOutcome.Conflict state.Binding known, state
        | Some known when known.Retired -> result ControlOperationOutcome.AlreadyRetired state.Binding known, state
        | Some known ->
            if not (authority context claim) then
                result ControlOperationOutcome.LostAuthority state.Binding known, state
            else
                match state.Binding, context.Entries.TryFind position with
                | null, _ -> result ControlOperationOutcome.TargetChanged null known, state
                | target, Some true when
                    associated turn position target
                    && target.State = ControlTargetState.TerminalPendingRetirement
                    ->
                    let retired = { known with Retired = true }

                    result ControlOperationOutcome.Applied null retired,
                    { state with
                        Binding = null
                        Decisions =
                            state.Decisions
                            |> Array.map (fun item -> if item.TurnId = turn then retired else item)
                    }
                | target, Some false when associated turn position target ->
                    result ControlOperationOutcome.NotReady target known, state
                | _ -> result ControlOperationOutcome.TargetChanged state.Binding known, state
        | None -> result ControlOperationOutcome.TargetChanged state.Binding null, state
