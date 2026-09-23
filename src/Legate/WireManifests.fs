// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System

// The versioned-envelope manifest table (issue 130): one entry per wire
// case, mapping each legate.<family>.<MessageName>.v<version> manifest onto
// its DTO type, current version, and per-case byte bound. The table is the
// single source the serializer, the HOCON bindings, the tests, and the
// Docs/ARCHITECTURE.md wire-contract listing all read from: adding a wire
// case means adding a row here (plus its DTO in WireDtos and, with issue
// 131, its golden file).
module internal WireManifests =

    /// Manifest family: the base session actor protocol.
    [<Literal>]
    let ActorFamily = "actor"

    /// Manifest family: the session router protocol.
    [<Literal>]
    let RouterFamily = "router"

    /// Manifest family: the suspendable entity protocol the sharded region
    /// speaks (issue 126).
    [<Literal>]
    let EntityFamily = "entity"

    /// Manifest family: the cross-node subscription protocol (issue 133).
    /// Subscribe routes through the session shard region; EventBatch
    /// streams bounded pages back to the subscribing node.
    [<Literal>]
    let SubscriptionFamily = "subscription"

    /// Manifest family: the session event stream (issue 133). Single
    /// journaled events stream from the owning entity to the subscribing
    /// node; batches cross as subscription EventBatch pages.
    [<Literal>]
    let EventFamily = "event"

    /// Byte bound for control DTOs: replies without entries, snapshots of
    /// state, timeouts, and resolve markers.
    [<Literal>]
    let SmallWireBytes = 32768

    /// Byte bound for data-carrying DTOs: prompts, entries, results,
    /// sessions, and suspendable completions.
    [<Literal>]
    let LargeWireBytes = 1048576

    /// The global wire maximum every manifest additionally honours:
    /// ClusterOptions.MaxWirePayloadBytes defaults to this, and the
    /// serializer falls back to it when the HOCON carries no value.
    [<Literal>]
    let DefaultMaxWirePayloadBytes = 1048576

    /// One wire case: its family, its case name, its DTO type, its current
    /// version, and its per-case byte bound.
    type WireCase =
        {
            /// actor, router, entity, subscription, or event.
            Family: string
            /// The wire-case name, matching its DTO's message name.
            Name: string
            /// The DTO type the manifest deserialises into. Never a raw DU.
            DtoType: Type
            /// The current version. Readers accept current and
            /// current-minus-one and refuse anything else.
            Version: int
            /// The per-case byte bound, refused before deserialising.
            MaxBytes: int
        }

    /// Renders a wire case as its manifest string:
    /// legate.<family>.<MessageName>.v<version>.
    /// <param name="wireCase">The wire case.</param>
    /// <returns>The manifest string.</returns>
    let manifestOf (wireCase: WireCase) : string =
        ArgumentNullException.ThrowIfNull(wireCase)
        $"legate.%s{wireCase.Family}.%s{wireCase.Name}.v%d{wireCase.Version}"

    /// Builds one table row.
    /// <param name="family">The manifest family.</param>
    /// <param name="name">The wire-case name.</param>
    /// <param name="dtoType">The DTO type. Must not be null.</param>
    /// <param name="maxBytes">The per-case byte bound. Must be at least 1.</param>
    /// <returns>The wire case at version 1.</returns>
    let private row (family: string) (name: string) (dtoType: Type) (maxBytes: int) : WireCase =
        ArgumentNullException.ThrowIfNull(dtoType)

        if maxBytes < 1 then
            raise (ArgumentOutOfRangeException(nameof maxBytes, "The per-case byte bound must be at least 1."))

        {
            Family = family
            Name = name
            DtoType = dtoType
            Version = 1
            MaxBytes = maxBytes
        }

    /// Every registered wire case, in manifest order. Actor rows cover the
    /// base session protocol and its replies; the router row covers session
    /// resolve; entity rows cover the suspendable protocol and its replies.
    let cases: WireCase list =
        [
            row ActorFamily "AbortSession" typeof<WireDtos.AbortSessionDto> LargeWireBytes
            row ActorFamily "CloseSession" typeof<WireDtos.CloseSessionDto> SmallWireBytes
            row ActorFamily "CompactCompleted" typeof<WireDtos.CompactCompletedDto> SmallWireBytes
            row ActorFamily "CompactDeferred" typeof<WireDtos.CompactDeferredDto> SmallWireBytes
            row ActorFamily "CompactFenced" typeof<WireDtos.CompactFencedDto> SmallWireBytes
            row ActorFamily "CompactNotNeeded" typeof<WireDtos.CompactNotNeededDto> SmallWireBytes
            row ActorFamily "CompactRejected" typeof<WireDtos.CompactRejectedDto> SmallWireBytes
            row ActorFamily "CompactSession" typeof<WireDtos.CompactSessionDto> SmallWireBytes
            row ActorFamily "GetSnapshot" typeof<WireDtos.GetSnapshotDto> SmallWireBytes
            row ActorFamily "InjectPrompt" typeof<WireDtos.InjectPromptDto> LargeWireBytes
            row ActorFamily "InterruptPrompt" typeof<WireDtos.InterruptPromptDto> LargeWireBytes
            row ActorFamily "PromptAccepted" typeof<WireDtos.PromptAcceptedDto> LargeWireBytes
            row ActorFamily "PromptRejected" typeof<WireDtos.PromptRejectedDto> SmallWireBytes
            row ActorFamily "QueuePrompt" typeof<WireDtos.QueuePromptDto> LargeWireBytes
            row ActorFamily "SessionClosed" typeof<WireDtos.SessionClosedDto> LargeWireBytes
            row ActorFamily "SessionSnapshot" typeof<WireDtos.SnapshotDto> SmallWireBytes
            row ActorFamily "TurnFaulted" typeof<WireDtos.TurnFaultedDto> LargeWireBytes
            row ActorFamily "TurnSettled" typeof<WireDtos.TurnSettledDto> LargeWireBytes
            row RouterFamily "ResolveSession" typeof<WireDtos.ResolveSessionDto> SmallWireBytes
            row EntityFamily "ReplyAccepted" typeof<WireDtos.ReplyAcceptedDto> LargeWireBytes
            row EntityFamily "ReplyEntry" typeof<WireDtos.ReplyEntryDto> LargeWireBytes
            row EntityFamily "ReplyRejected" typeof<WireDtos.ReplyRejectedDto> SmallWireBytes
            row EntityFamily "SetAgentApplied" typeof<WireDtos.SetAgentAppliedDto> LargeWireBytes
            row EntityFamily "SetAgentPending" typeof<WireDtos.SetAgentPendingDto> LargeWireBytes
            row EntityFamily "SetAgentRejected" typeof<WireDtos.SetAgentRejectedDto> SmallWireBytes
            row EntityFamily "SuspendTimedOut" typeof<WireDtos.SuspendTimedOutDto> SmallWireBytes
            row EntityFamily "SuspendableAbortSession" typeof<WireDtos.SuspendableAbortSessionDto> LargeWireBytes
            row EntityFamily "SuspendableCheckInbox" typeof<WireDtos.SuspendableCheckInboxDto> SmallWireBytes
            row EntityFamily "SuspendableCloseSession" typeof<WireDtos.SuspendableCloseSessionDto> SmallWireBytes
            row EntityFamily "SuspendableCompactSession" typeof<WireDtos.SuspendableCompactSessionDto> SmallWireBytes
            row EntityFamily "SuspendableFinished" typeof<WireDtos.SuspendableFinishedDto> LargeWireBytes
            row EntityFamily "SuspendableFaulted" typeof<WireDtos.SuspendableFaultedDto> LargeWireBytes
            row EntityFamily "SuspendableGetSnapshot" typeof<WireDtos.SuspendableGetSnapshotDto> SmallWireBytes
            row EntityFamily "SuspendableInjectPrompt" typeof<WireDtos.SuspendableInjectPromptDto> LargeWireBytes
            row EntityFamily "SuspendableInterruptPrompt" typeof<WireDtos.SuspendableInterruptPromptDto> LargeWireBytes
            row EntityFamily "SuspendableQueuePrompt" typeof<WireDtos.SuspendableQueuePromptDto> LargeWireBytes
            row EntityFamily "SuspendableSetAgent" typeof<WireDtos.SuspendableSetAgentDto> SmallWireBytes
            row SubscriptionFamily "Subscribe" typeof<WireDtos.SubscribeDto> SmallWireBytes
            row SubscriptionFamily "Unsubscribe" typeof<WireDtos.UnsubscribeDto> SmallWireBytes
            row SubscriptionFamily "EventBatch" typeof<WireDtos.EventBatchDto> LargeWireBytes
            row EventFamily "SessionEvent" typeof<WireDtos.SessionEventDto> LargeWireBytes
        ]

    /// Manifests reserved for future wire cases. The subscription and event
    /// namespaces promoted to table rows in issue 133, so no reservations
    /// remain; the list stays so the envelope still has a named place for
    /// the next family to reserve.
    let reservedManifests: string list = []

    /// Finds the wire case a manifest names, ignoring its version.
    /// <param name="family">The manifest family.</param>
    /// <param name="name">The wire-case name.</param>
    /// <returns>The wire case, or None when the family and name name nothing registered.</returns>
    let tryFindCase (family: string) (name: string) : WireCase option =
        cases
        |> List.tryFind (fun wireCase ->
            String.Equals(wireCase.Family, family, StringComparison.Ordinal)
            && String.Equals(wireCase.Name, name, StringComparison.Ordinal))

    /// Finds the wire case a DTO type belongs to.
    /// <param name="dtoType">The DTO type.</param>
    /// <returns>The wire case, or None when the type is no registered DTO.</returns>
    let tryFindByDtoType (dtoType: Type) : WireCase option =
        if isNull (box dtoType) then
            None
        else
            cases |> List.tryFind (fun wireCase -> wireCase.DtoType = dtoType)

    /// Splits a manifest into its family, case name, and version. Returns
    /// None when the text is not a legate.<family>.<name>.v<version> shape.
    /// <param name="manifest">The manifest text, or null.</param>
    /// <returns>The family, name, and version, or None.</returns>
    let tryParseManifest (manifest: string | null) : (string * string * int) option =
        match manifest with
        | null -> None
        | text ->
            let parts = text.Split('.')

            if parts.Length <> 4 then
                None
            elif not (String.Equals(parts[0], "legate", StringComparison.Ordinal)) then
                None
            elif String.IsNullOrEmpty parts[1] || String.IsNullOrEmpty parts[2] then
                None
            elif parts[3].Length < 2 || parts[3][0] <> 'v' then
                None
            else
                let mutable version = 0

                if Int32.TryParse(parts[3].Substring(1), &version) && version >= 0 then
                    Some(parts[1], parts[2], version)
                else
                    None

    /// A wire refusal: the envelope fails closed and Akka drops the message.
    /// The rejection reason travels on Reason for the metric tag; the
    /// manifest that refused travels on Manifest for the log line.
    type WireRejectedException(reason: string, manifest: string | null, message: string) =
        inherit InvalidOperationException(message)

        /// Why the envelope refused the payload: unknownManifest,
        /// newerVersion, oversized, or failed.
        member _.Reason = reason

        /// The manifest that refused, or null when no manifest parsed.
        member _.Manifest = manifest
