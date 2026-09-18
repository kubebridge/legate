// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpArtifactSinkTests

open System
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Mcp
open Xunit

// Service-backed artifact sink (issue 117): the typed service outcomes map
// onto sink answers. Stored answers with the service's reference text;
// exhausted and rejected outcomes answer with bounded reasons; failures
// and faults keep the placeholder so the turn continues; cancellation
// propagates.

let tenant = TenantId.Create "acme"

/// A service scripted to answer every stage with the fixed outcome.
type ScriptedArtifactService(outcome: ArtifactStageOutcome) =
    let mutable seen: (TenantId * SessionId * string) list = []

    interface ISessionArtifactService with
        member _.StageAsync(tenant, sessionId, name, _, cancellationToken) =
            cancellationToken.ThrowIfCancellationRequested()
            seen <- (tenant, sessionId, name) :: seen
            Task.FromResult(outcome)

        member _.DescribeAsync(_, _, _, _) : Task<ArtifactDescribeOutcome> =
            raise (NotSupportedException("The scripted service only stages."))

        member _.ClearSessionAsync(_, _, _) : Task<int> =
            raise (NotSupportedException("The scripted service only stages."))

        member _.ReclaimStaleReservationsAsync(_) : Task<int> =
            raise (NotSupportedException("The scripted service only stages."))

    /// The stages the service observed, newest first.
    member _.Seen: (TenantId * SessionId * string) list = seen

/// A service whose stage always throws: faults keep the placeholder.
type ThrowingArtifactService() =

    interface ISessionArtifactService with
        member _.StageAsync(_, _, _, _, _) : Task<ArtifactStageOutcome> =
            raise (InvalidOperationException("service unavailable"))

        member _.DescribeAsync(_, _, _, _) : Task<ArtifactDescribeOutcome> =
            raise (NotSupportedException("The throwing service only stages."))

        member _.ClearSessionAsync(_, _, _) : Task<int> =
            raise (NotSupportedException("The throwing service only stages."))

        member _.ReclaimStaleReservationsAsync(_) : Task<int> =
            raise (NotSupportedException("The throwing service only stages."))

let storedOutcome () : ArtifactStageOutcome =
    ArtifactStored(
        {
            Name = "shot-abc.png"
            MediaType = "image/png"
            SizeBytes = 70L
            Width = 1
            Height = 1
            Duration = Nullable()
            PreviewName = null
            DownloadUrl = null
            Reference = ArtifactReference.Format("shot-abc.png", "image/png", 70L, 1, 1)
        }
    )
    :> ArtifactStageOutcome

let private store (sink: IArtifactSink) (name: string) : ArtifactSinkOutcome =
    sink.StoreAsync(name, BlobContent([| 1uy |], "image/png"), CancellationToken.None).GetAwaiter().GetResult()

[<Fact>]
let ``Stored answers with the service reference for the asking session`` () =
    let service = ScriptedArtifactService(storedOutcome ())
    let sessionId = SessionId.New()
    let sink = ServiceArtifactSink(service, tenant, sessionId) :> IArtifactSink

    match store sink "shot-abc.png" with
    | StoredArtifact(reference, name) ->
        name |> should equal "shot-abc.png"
        reference.Contains("shot-abc.png") |> should equal true
        reference.Contains("dimensions=\"1x1\"") |> should equal true
    | outcome -> failwith $"Expected StoredArtifact, observed %A{outcome}."

    service.Seen |> should equal [ (tenant, sessionId, "shot-abc.png") ]

[<Fact>]
let ``Exhaustion answers with the bounded quota reason`` () =
    let service =
        ScriptedArtifactService(ArtifactQuotaExhausted(100L, 40L) :> ArtifactStageOutcome)

    let sink = ServiceArtifactSink(service, tenant, SessionId.New()) :> IArtifactSink

    match store sink "shot.png" with
    | RejectedArtifact reason ->
        reason.Contains("quota exhausted") |> should equal true
        reason.Contains("100") |> should equal true
        reason.Contains("40") |> should equal true
    | outcome -> failwith $"Expected RejectedArtifact, observed %A{outcome}."

[<Fact>]
let ``Rejection answers with the bounded validation reason`` () =
    let service =
        ScriptedArtifactService(ArtifactRejected("undecodable", -1L, -1L) :> ArtifactStageOutcome)

    let sink = ServiceArtifactSink(service, tenant, SessionId.New()) :> IArtifactSink

    match store sink "shot.png" with
    | RejectedArtifact reason -> reason |> should equal "undecodable"
    | outcome -> failwith $"Expected RejectedArtifact, observed %A{outcome}."

[<Fact>]
let ``Stage failure keeps the placeholder`` () =
    let service =
        ScriptedArtifactService(ArtifactStageFailed("storeFault") :> ArtifactStageOutcome)

    let sink = ServiceArtifactSink(service, tenant, SessionId.New()) :> IArtifactSink

    match store sink "shot.png" with
    | KeepPlaceholder -> ()
    | outcome -> failwith $"Expected KeepPlaceholder, observed %A{outcome}."

[<Fact>]
let ``Service fault keeps the placeholder`` () =
    let sink =
        ServiceArtifactSink(ThrowingArtifactService() :> ISessionArtifactService, tenant, SessionId.New())
        :> IArtifactSink

    match store sink "shot.png" with
    | KeepPlaceholder -> ()
    | outcome -> failwith $"Expected KeepPlaceholder, observed %A{outcome}."

[<Fact>]
let ``Null name throws`` () =
    let service = ScriptedArtifactService(storedOutcome ())
    let sink = ServiceArtifactSink(service, tenant, SessionId.New()) :> IArtifactSink

    (fun () -> store sink Unchecked.defaultof<string> |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``Cancellation propagates`` () =
    let service = ScriptedArtifactService(storedOutcome ())
    let sink = ServiceArtifactSink(service, tenant, SessionId.New()) :> IArtifactSink

    use cancelled = new CancellationTokenSource()
    cancelled.Cancel()

    (fun () ->
        sink.StoreAsync("shot.png", BlobContent([| 1uy |], "image/png"), cancelled.Token).GetAwaiter().GetResult()
        |> ignore)
    |> should throw typeof<OperationCanceledException>
