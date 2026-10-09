// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.Cluster
open Akka.FSharp

type internal SessionRouteAdmitted =
    {
        Context: SessionExecutionContext
        Address: SessionAddress
        Request: SessionRouteRequest
        Sender: IActorRef
    }

module internal SessionRouting =
    let refusal reason = SessionScopeRejectedException(reason)

    let validatePayload (address: SessionAddress) (payload: obj) =
        match payload with
        | :? SessionRouteProbe -> ()
        | :? SessionActor.SuspendableActorMessage as command ->
            match command with
            | SessionActor.SessionReplyPayload _ -> ()
            | SessionActor.SuspendableQueuePrompt _
            | SessionActor.SuspendableInjectPrompt _
            | SessionActor.SuspendableInterruptPrompt _
            | SessionActor.SuspendableGetSnapshot
            | SessionActor.SuspendableCloseSession _
            | SessionActor.SuspendableCompactSession _
            | SessionActor.SuspendableSetAgent _
            | SessionActor.SuspendableCheckInbox -> ()
            | SessionActor.SuspendableObserveHostAbort(tenant, sessionId, _) ->
                if tenant <> address.Tenant || sessionId <> address.SessionId then
                    raise (refusal SessionScopeRejectionReason.AddressMismatch)
            | _ -> raise (refusal SessionScopeRejectionReason.UnsupportedOperation)
        | :? CrossNodeSubscriptions.CrossNodeSubscribeRequest as subscribe ->
            if
                subscribe.Tenant <> address.Tenant
                || subscribe.SessionId <> address.SessionId
                || String.IsNullOrWhiteSpace subscribe.SubscriberToken
            then
                raise (refusal SessionScopeRejectionReason.AddressMismatch)
        | :? CrossNodeSubscriptions.CrossNodeUnsubscribe as unsubscribe ->
            if
                unsubscribe.Tenant <> address.Tenant
                || unsubscribe.SessionId <> address.SessionId
                || String.IsNullOrWhiteSpace unsubscribe.SubscriberToken
            then
                raise (refusal SessionScopeRejectionReason.AddressMismatch)
        | _ -> raise (refusal SessionScopeRejectionReason.UnsupportedOperation)

    let validateResponse (address: SessionAddress) (payload: obj) =
        let checkId id =
            if id <> address.SessionId then
                raise (refusal SessionScopeRejectionReason.ResponseMismatch)

        match payload with
        | :? Session as session ->
            checkId session.Id

            if session.Tenant <> address.Tenant then
                raise (refusal SessionScopeRejectionReason.ResponseMismatch)
        | :? SessionPromptReply as reply ->
            match reply with
            | PromptAccepted entry -> checkId entry.SessionId
            | PromptRejected _ -> ()
        | :? SessionActor.SessionReplyReply as reply ->
            match reply with
            | SessionActor.ReplyAccepted entry -> checkId entry.SessionId
            | SessionActor.ReplyRejected _ -> ()
        | :? SessionActor.SessionSetAgentReply as reply ->
            match reply with
            | SessionActor.SetAgentApplied session
            | SessionActor.SetAgentPending session ->
                checkId session.Id

                if session.Tenant <> address.Tenant then
                    raise (refusal SessionScopeRejectionReason.ResponseMismatch)
            | SessionActor.SetAgentRejected _ -> ()
        | :? CrossNodeSubscriptions.CrossNodeEventBatch as batch ->
            if batch.Tenant <> address.Tenant then
                raise (refusal SessionScopeRejectionReason.ResponseMismatch)

            if String.IsNullOrWhiteSpace batch.SubscriberToken then
                raise (refusal SessionScopeRejectionReason.ResponseMismatch)

            checkId batch.SessionId

            if isNull (box batch.Events) then
                raise (refusal SessionScopeRejectionReason.ResponseMismatch)

            for evt in batch.Events do
                if isNull (box evt) || evt.SessionId <> address.SessionId then
                    raise (refusal SessionScopeRejectionReason.ResponseMismatch)
        | _ -> ()

    let validateResponseForRequest (address: SessionAddress) (requestPayload: obj) (responsePayload: obj) =
        validateResponse address responsePayload

        match requestPayload, responsePayload with
        | (:? CrossNodeSubscriptions.CrossNodeSubscribeRequest as request),
          (:? CrossNodeSubscriptions.CrossNodeEventBatch as response) when
            not (String.Equals(request.SubscriberToken, response.SubscriberToken, StringComparison.Ordinal))
            ->
            raise (refusal SessionScopeRejectionReason.ResponseMismatch)
        | _ -> ()

    let private respond (owner: string) (address: string) (sender: IActorRef) (payload: obj) =
        sender.Tell(
            {
                Address = address
                Owner = owner
                Payload = payload
            }
            : SessionRouteResponse
        )

    let gateBehavior
        (entityKey: string)
        (contexts: ISessionHostContexts)
        (hubs: ConcurrentDictionary<TenantId * string, CrossNodeSubscriptions.SubscriptionHub>)
        (mailbox: Actor<obj>)
        =
        let owner =
            try
                Cluster.Get(mailbox.Context.System).SelfAddress.ToString()
            with _ ->
                mailbox.Context.System.Name

        let mutable execution: IActorRef option = None
        let mutable nextReply = 0

        let routeReply (sender: IActorRef) =
            let name = $"route-reply-{nextReply}"
            nextReply <- nextReply + 1

            spawn mailbox.Context name (fun (replybox: Actor<obj>) ->
                actor {
                    let! reply = replybox.Receive()
                    respond owner entityKey sender reply
                    replybox.Context.Stop(replybox.Self)
                })

        let admit (request: SessionRouteRequest) (sender: IActorRef) =
            task {
                try
                    let address =
                        match SessionAddress.TryParse entityKey with
                        | Some parsed -> parsed
                        | None -> raise (refusal SessionScopeRejectionReason.InvalidScope)

                    if request.Address <> entityKey || request.Scope <> address.Tenant.Value then
                        raise (refusal SessionScopeRejectionReason.AddressMismatch)

                    validatePayload address request.Payload
                    let context = contexts.Get(address.Tenant)

                    do!
                        context.WorkTracker.Track(fun () ->
                            task {
                                let! stored =
                                    context.Store.GetSession(address.Tenant, address.SessionId, CancellationToken.None)

                                match stored with
                                | null ->
                                    raise (
                                        SessionNotFoundException(
                                            address.SessionId,
                                            "No session exists in the authorized scope."
                                        )
                                    )
                                | session when session.Tenant <> address.Tenant || session.Id <> address.SessionId ->
                                    raise (refusal SessionScopeRejectionReason.AddressMismatch)
                                | _ -> ()

                                mailbox.Self.Tell(
                                    {
                                        Context = context
                                        Address = address
                                        Request = request
                                        Sender = sender
                                    }
                                    : SessionRouteAdmitted
                                )
                            })
                with error ->
                    respond owner entityKey sender (error :> obj)
            }

        let serve (admitted: SessionRouteAdmitted) =
            let address = admitted.Address
            let request = admitted.Request
            let sender = admitted.Sender

            task {
                try
                    let context = contexts.Get(address.Tenant)

                    do!
                        context.WorkTracker.Track(fun () ->
                            task {
                                match request.Payload with
                                | :? SessionRouteProbe -> respond owner entityKey sender (SessionRouteAccepted :> obj)
                                | :? CrossNodeSubscriptions.CrossNodeSubscribeRequest as subscribe ->
                                    let hub =
                                        hubs.GetOrAdd(
                                            (address.Tenant, entityKey),
                                            fun _ ->
                                                CrossNodeSubscriptions.SubscriptionHub(context.SubscriptionOptions)
                                        )

                                    if not (hub.TryAttach subscribe.SubscriberToken) then
                                        raise (
                                            SessionSubscriptionLimitExceededException(
                                                address.SessionId,
                                                context.SubscriptionOptions.MaxSubscribersPerSession,
                                                "The session subscriber limit was reached."
                                            )
                                        )

                                    let! outcome =
                                        CrossNodeSubscriptions.serveBatchAsync (
                                            context.EventStore,
                                            hub,
                                            address.Tenant,
                                            address.SessionId,
                                            subscribe.FromSequence,
                                            CrossNodeSubscriptions.MaxBatchEvents,
                                            context.SubscriptionOptions.MaxEventPayloadBytes,
                                            CancellationToken.None
                                        )

                                    CrossNodeSubscriptions.raiseForOutcome address.Tenant outcome

                                    match outcome with
                                    | CrossNodeSubscriptions.BatchPage batch ->
                                        respond
                                            owner
                                            entityKey
                                            sender
                                            ({ batch with
                                                SubscriberToken = subscribe.SubscriberToken
                                            }
                                            :> obj)
                                    | _ -> invalidOp "The authorized subscription returned no batch."
                                | :? CrossNodeSubscriptions.CrossNodeUnsubscribe as unsubscribe ->
                                    match hubs.TryGetValue((address.Tenant, entityKey)) with
                                    | true, hub -> hub.Detach unsubscribe.SubscriberToken
                                    | _ -> ()

                                    respond owner entityKey sender (SessionRouteAccepted :> obj)
                                | :? SessionActor.SuspendableActorMessage as hint when
                                    (match hint with
                                     | SessionActor.SuspendableObserveHostAbort _ -> true
                                     | _ -> false)
                                    ->
                                    // Wake hints are nonactivating. Persisted exact-target intent remains authority.
                                    match execution with
                                    | Some child -> child.Tell(hint)
                                    | None -> ()
                                | :? SessionActor.SuspendableActorMessage as command ->
                                    let child =
                                        match execution with
                                        | Some child -> child
                                        | None ->
                                            let child = context.Spawn address mailbox.Context "execution"
                                            execution <- Some child
                                            child

                                    match command with
                                    | SessionActor.SuspendableCheckInbox ->
                                        // CheckInbox is a durable wake hint. It
                                        // deliberately has no reply actor.
                                        child.Tell(command)
                                    | SessionActor.SessionReplyPayload value ->
                                        let receiver = routeReply sender
                                        // The child mailbox is the serialized reply gate.
                                        // It validates the live suspension before appending
                                        // and prevents concurrent direct replies from both
                                        // passing a snapshot-before-append race.
                                        child.Tell(SessionActor.SessionReplyPayload value, receiver)
                                    | _ -> child.Tell(command, routeReply sender)
                                | _ ->
                                    respond
                                        owner
                                        entityKey
                                        sender
                                        (refusal SessionScopeRejectionReason.UnsupportedOperation)
                            })
                with error ->
                    respond owner entityKey sender (error :> obj)
            }

        let rec loop () =
            actor {
                let! message = mailbox.Receive()
                let sender = mailbox.Sender()

                match message with
                | :? SessionRouteRequest as request -> admit request sender |> ignore
                | :? SessionRouteAdmitted as admitted -> serve admitted |> ignore
                | :? SessionAddressedIngress as ingress ->
                    match ingress.Request with
                    | :? SessionRouteRequest as request when ingress.EntityKey = entityKey ->
                        admit request sender |> ignore
                    | _ -> respond owner entityKey sender (refusal SessionScopeRejectionReason.AddressMismatch)
                | _ -> respond owner entityKey sender (refusal SessionScopeRejectionReason.InvalidScope)

                return! loop ()
            }

        loop ()

    let spawnGate contexts hubs key context name =
        spawn context name (gateBehavior key contexts hubs)

    let boundProxy (address: SessionAddress) (send: obj -> IActorRef -> unit) (mailbox: Actor<obj>) =
        let mutable next = 0

        let rec loop () =
            actor {
                let! payload = mailbox.Receive()
                let sender = mailbox.Sender()

                let oneWay =
                    match payload with
                    | :? SessionActor.SuspendableActorMessage as command ->
                        match command with
                        | SessionActor.SuspendableCheckInbox
                        | SessionActor.SuspendableObserveHostAbort _ -> true
                        | _ -> false
                    | _ -> false

                if oneWay then
                    // These are durable wake hints.  They do not answer an
                    // Ask, so allocating a reply actor would leak one per
                    // hint forever.
                    send
                        ({
                            Address = address.Key
                            Scope = address.Tenant.Value
                            Payload = payload
                        }
                        : SessionRouteRequest)
                        sender
                else
                    let replyActor =
                        spawn mailbox.Context $"reply-{next}" (fun (replybox: Actor<obj>) ->
                            actor {
                                let! response = replybox.Receive()

                                try
                                    match response with
                                    | :? SessionRouteResponse as reply when reply.Address = address.Key ->
                                        validateResponseForRequest address payload reply.Payload

                                        match reply.Payload with
                                        | :? Exception as error -> sender.Tell(Status.Failure error)
                                        | value -> sender.Tell(value)
                                    | _ ->
                                        sender.Tell(
                                            Status.Failure(refusal SessionScopeRejectionReason.ResponseMismatch)
                                        )
                                with error ->
                                    sender.Tell(Status.Failure error)

                                replybox.Context.Stop(replybox.Self)
                            })

                    next <- next + 1

                    send
                        ({
                            Address = address.Key
                            Scope = address.Tenant.Value
                            Payload = payload
                        }
                        : SessionRouteRequest)
                        replyActor

                return! loop ()
            }

        loop ()
