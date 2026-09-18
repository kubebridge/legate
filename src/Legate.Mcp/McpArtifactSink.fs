// SPDX-License-Identifier: Apache-2.0
namespace Legate.Mcp

open System
open System.Threading
open System.Threading.Tasks
open Legate

// Session artifact sink for MCP tool results (issue 117). IArtifactSink is
// the one-method contract the per-call path stores binaries through: the
// caller builds the storage name, the sink stages it with quota accounting,
// and answers with the substitution. Stored carries the reference text the
// caller substitutes for the placeholder; Rejected carries the bounded
// reason the caller formats as a rejection; KeepPlaceholder tells the
// caller storage is unavailable right now (no sink, no store, or a
// mid-stage fault), so it keeps the placeholder text and the turn
// continues. ServiceArtifactSink implements the contract over the public
// ISessionArtifactService, mapping its typed outcomes onto sink answers.

/// What one sink store decided about a binary: the caller substitutes the
/// reference, formats the bounded rejection, or keeps the placeholder.
type internal ArtifactSinkOutcome =
    /// The binary is staged: the reference text replaces the placeholder,
    /// and the name is the storage name it landed under.
    | StoredArtifact of reference: string * name: string
    /// The binary was refused with a bounded reason (quota exhaustion or a
    /// validation rejection): the caller formats a rejection and the turn
    /// continues.
    | RejectedArtifact of reason: string
    /// Storage is unavailable right now: the caller keeps the placeholder
    /// text and the turn continues.
    | KeepPlaceholder

/// How the per-call path stages one tool-returned binary with quota
/// accounting: implementations scope the sink to the asking session when
/// they are built, so this layer never derives tenant keys.
type internal IArtifactSink =

    /// Stages one binary under the caller's storage name.
    /// <param name="name">The storage name to stage under.</param>
    /// <param name="content">The payload and media type to stage.</param>
    /// <param name="cancellationToken">The turn's token: abandoning it abandons the store.</param>
    /// <returns>How the caller substitutes the binary's placeholder.</returns>
    abstract StoreAsync:
        name: string * content: BlobContent * cancellationToken: CancellationToken -> Task<ArtifactSinkOutcome>

/// Quota-accounted sink over <see cref="T:Legate.ISessionArtifactService" />:
/// every binary stages through the service's reserve/store/commit path for
/// the sink's tenant and session. Stored outcomes answer with the
/// service's reference text; exhausted and rejected outcomes answer with
/// bounded reasons; failures answer KeepPlaceholder so the turn continues
/// on fallback text. Cancellation propagates and answers nothing.
/// <param name="service">The artifact service staging through. Must not be null.</param>
/// <param name="tenant">The tenant the artifacts belong to.</param>
/// <param name="sessionId">The session the artifacts belong to.</param>
type internal ServiceArtifactSink(service: ISessionArtifactService, tenant: TenantId, sessionId: SessionId) =

    do ArgumentNullException.ThrowIfNull(service)

    interface IArtifactSink with
        member _.StoreAsync(name, content, cancellationToken) =
            if isNull (box name) then
                raise (ArgumentNullException(nameof name))

            task {
                try
                    let! outcome = service.StageAsync(tenant, sessionId, name, content, cancellationToken)

                    match outcome with
                    | :? ArtifactStored as stored when not (isNull (box stored)) ->
                        let descriptor = stored.Descriptor

                        if isNull (box descriptor) || isNull (box descriptor.Reference) then
                            return KeepPlaceholder
                        else
                            return StoredArtifact(descriptor.Reference, descriptor.Name)
                    | :? ArtifactQuotaExhausted as exhausted when not (isNull (box exhausted)) ->
                        return
                            RejectedArtifact(
                                $"quota exhausted: requested {exhausted.RequestedBytes} bytes, {exhausted.AllowedBytes} bytes available"
                            )
                    | :? ArtifactRejected as rejected when not (isNull (box rejected)) ->
                        return RejectedArtifact(rejected.Reason)
                    | _ -> return KeepPlaceholder
                with
                | :? OperationCanceledException as canceled -> return! Task.FromException<ArtifactSinkOutcome>(canceled)
                | _ -> return KeepPlaceholder
            }
