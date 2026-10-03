// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open Microsoft.Extensions.DependencyInjection

/// Resolves only exact tenant-scoped host registrations, never persisted runtime types.
type internal CompletionDestinations(provider: IServiceProvider) =
    member _.Resolve(tenant: TenantId, sessionId: Nullable<SessionId>, destinationId: string) : ISessionCompletionSink =
        CompletionDestinationRules.Validate destinationId

        let refuse reason =
            CompletionRoutingException(Nullable tenant, sessionId, destinationId, reason)

        try
            match provider.GetKeyedService<ISessionCompletionSink>(box (tenant, destinationId)) with
            | null -> raise (refuse CompletionRoutingReason.Unknown)
            | sink -> sink
        with
        | :? CompletionRoutingException as refusal -> raise refusal
        | _ -> raise (refuse CompletionRoutingReason.Unavailable)

    member this.Validate(session: Session) =
        session.Options.ValidatePersistence()

        match session.Options.CompletionDestinationId with
        | null -> ()
        | id -> this.Resolve(session.Tenant, Nullable session.Id, id) |> ignore

/// Tokenless typed refusal used by both local and remote actor asks.
[<CLIMutable>]
type internal CompletionRoutingRefused =
    {
        Tenant: TenantId
        SessionId: SessionId
        DestinationId: string | null
        Reason: CompletionRoutingReason
    }

    member this.Exception =
        CompletionRoutingException(Nullable this.Tenant, Nullable this.SessionId, this.DestinationId, this.Reason)
