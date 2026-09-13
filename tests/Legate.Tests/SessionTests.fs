// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionTests

open System
open System.Collections.Generic
open System.Text.Json
open FsUnit.Xunit
open Legate
open Xunit

// The generic Deserialize<'T> overload is annotated to return 'T | null,
// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

// Unchecked.defaultof<string> rather than a bare null literal: under
// Nullable=enable the literal trips F# nullness checking on string-typed
// parameters. Deliberate: this is the null string value for rejection cases.
let nullString = Unchecked.defaultof<string>

// Empty Nullable<'T> spellings; Nullable() inline reads ambiguously.
let noTurnId = Unchecked.defaultof<Nullable<TurnId>>
let noStamp = Unchecked.defaultof<Nullable<DateTimeOffset>>

/// A permission policy fake that allows every call: enough to put a
/// non-null, interface-typed value into SessionOptions.Permissions.
type FakePermissionPolicy() =

    interface IPermissionPolicy with
        member _.Evaluate(_request: PermissionRequest) = PermissionVerdict.Allow

/// Builds a session with every field set and non-null option values, the
/// common shape for construction and serialisation-shape tests.
let sampleSession () =
    {
        Id = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        Tenant = TenantId.Create "acme"
        AgentId = AgentId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        Title = "checkout"
        State = SessionState.Running
        CurrentTurnId = Nullable(TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV")
        CreatedAt = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
        UpdatedAt = DateTimeOffset(2024, 6, 7, 8, 9, 10, TimeSpan.Zero)
        ClosedAt = noStamp
        WorkspaceBinding = "ws://acme/checkout"
        Options =
            let options = SessionOptions()
            options.Title <- "checkout"
            options.AutoClose <- true
            options.Outcome <- SessionOutcomeMode.Structured
            options.Permissions <- FakePermissionPolicy() :> IPermissionPolicy

            options.CompletionSink <-
                { new ISessionCompletionSink with
                    member _.Notify(_completion: SessionCompletion) = ()
                }

            options.MaxIterations <- 24
            options.Timeout <- Nullable(TimeSpan.FromMinutes 30.)

            options.Metadata <-
                Dictionary<string, string>(dict [ ("source", "cli") ]) :> IReadOnlyDictionary<string, string>

            options
    }

/// Builds a session whose marker-interface options are null: the shape
/// System.Text.Json can round-trip, since interface-typed properties
/// deserialise only as null.
let sessionWithoutMarkers () =
    let options = SessionOptions()
    options.Title <- "checkout"
    options.AutoClose <- true
    options.Outcome <- SessionOutcomeMode.Structured
    options.MaxIterations <- 24
    options.Timeout <- Nullable(TimeSpan.FromMinutes 30.)
    options.Metadata <- Dictionary<string, string>(dict [ ("source", "cli") ]) :> IReadOnlyDictionary<string, string>

    { sampleSession () with
        Options = options
    }

// ───────────────────────────────────────────────────────────────────────────
// SessionState and SessionOutcomeMode

[<Fact>]
let ``SessionState has exactly the four documented members`` () =
    Enum.GetNames<SessionState>()
    |> should
        equal
        [|
            "Idle"
            "Running"
            "WaitingForInput"
            "Closed"
        |]

    int SessionState.Idle |> should equal 0
    int SessionState.Running |> should equal 1
    int SessionState.WaitingForInput |> should equal 2
    int SessionState.Closed |> should equal 3

[<Fact>]
let ``SessionOutcomeMode has exactly the two documented members`` () =
    Enum.GetNames<SessionOutcomeMode>() |> should equal [| "None"; "Structured" |]

    int SessionOutcomeMode.None |> should equal 0
    int SessionOutcomeMode.Structured |> should equal 1

    JsonSerializer.Serialize(SessionOutcomeMode.Structured) |> should equal "1"

// ───────────────────────────────────────────────────────────────────────────
// SessionOptions defaults and mutation

[<Fact>]
let ``SessionOptions defaults to an interactive session`` () =
    let options = SessionOptions()

    options.Title |> should equal null
    options.AutoClose |> should equal false
    options.Outcome |> should equal SessionOutcomeMode.None
    options.Permissions |> should equal null
    options.CompletionSink |> should equal null
    options.MaxIterations |> should equal 0
    options.Timeout.HasValue |> should equal false
    options.Metadata |> should equal null

[<Fact>]
let ``SessionOptions accepts every option through the object initialiser`` () =
    let permissions = FakePermissionPolicy() :> IPermissionPolicy

    let sink =
        { new ISessionCompletionSink with
            member _.Notify(_completion: SessionCompletion) = ()
        }

    let metadata =
        Dictionary<string, string>(dict [ ("source", "headless") ]) :> IReadOnlyDictionary<string, string>

    let options =
        SessionOptions(
            Title = "nightly sweep",
            AutoClose = true,
            Outcome = SessionOutcomeMode.Structured,
            Permissions = permissions,
            CompletionSink = sink,
            MaxIterations = 12,
            Timeout = Nullable(TimeSpan.FromSeconds 90.),
            Metadata = metadata
        )

    options.Title |> should equal "nightly sweep"
    options.AutoClose |> should equal true
    options.Outcome |> should equal SessionOutcomeMode.Structured
    options.Permissions |> should equal permissions
    options.CompletionSink |> should equal sink
    options.MaxIterations |> should equal 12
    options.Timeout.Value |> should equal (TimeSpan.FromSeconds 90.)
    options.Metadata |> should equal metadata

[<Fact>]
let ``SessionOptions mutates in place like a C# host would`` () =
    let options = SessionOptions()
    options.Title <- "renamed"
    options.AutoClose <- true
    options.Outcome <- SessionOutcomeMode.Structured
    options.Permissions <- FakePermissionPolicy() :> IPermissionPolicy

    options.CompletionSink <-
        { new ISessionCompletionSink with
            member _.Notify(_completion: SessionCompletion) = ()
        }

    options.MaxIterations <- 64
    options.Timeout <- Nullable(TimeSpan.FromHours 1.)
    options.Metadata <- Dictionary<string, string>() :> IReadOnlyDictionary<string, string>

    options.Title |> should equal "renamed"
    options.AutoClose |> should equal true
    options.Outcome |> should equal SessionOutcomeMode.Structured
    options.Permissions |> should not' (equal null)
    options.CompletionSink |> should not' (equal null)
    options.MaxIterations |> should equal 64
    options.Timeout.HasValue |> should equal true
    options.Metadata |> should not' (equal null)

[<Fact>]
let ``SessionOptions Timeout distinguishes zero from the runtime default`` () =
    let defaulted = SessionOptions()
    defaulted.Timeout.HasValue |> should equal false

    let zero = SessionOptions(Timeout = Nullable(TimeSpan.Zero))
    zero.Timeout.HasValue |> should equal true
    zero.Timeout.Value |> should equal TimeSpan.Zero

// ───────────────────────────────────────────────────────────────────────────
// Session construction and JSON

[<Fact>]
let ``Session record constructs with F# syntax and compares structurally`` () =
    let session = sampleSession ()

    let changed =
        { session with
            State = SessionState.Closed
        }

    changed |> should not' (equal session)

    let copy =
        { session with
            State = SessionState.Running
        }

    copy |> should equal session

[<Fact>]
let ``Session CLIMutable setters drive object-initialiser construction`` () =
    let session = sampleSession ()

    session.Title |> should equal "checkout"
    session.State |> should equal SessionState.Running

    session.CurrentTurnId.HasValue |> should equal true

    session.CurrentTurnId.Value
    |> should equal (TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV")

    session.ClosedAt.HasValue |> should equal false
    session.WorkspaceBinding |> should equal "ws://acme/checkout"
    session.Options.AutoClose |> should equal true
    session.Options.MaxIterations |> should equal 24

[<Fact>]
let ``Session JSON round-trip preserves every field with default options`` () =
    let session = sessionWithoutMarkers ()

    let json = JsonSerializer.Serialize session
    let roundTripped = deserialize<Session> json

    roundTripped.Id |> should equal session.Id
    roundTripped.Tenant |> should equal session.Tenant
    roundTripped.AgentId |> should equal session.AgentId
    roundTripped.Title |> should equal session.Title
    roundTripped.State |> should equal session.State

    roundTripped.CurrentTurnId.HasValue |> should equal true
    roundTripped.CurrentTurnId.Value |> should equal session.CurrentTurnId.Value

    roundTripped.CreatedAt |> should equal session.CreatedAt
    roundTripped.UpdatedAt |> should equal session.UpdatedAt
    roundTripped.ClosedAt.HasValue |> should equal false
    roundTripped.WorkspaceBinding |> should equal session.WorkspaceBinding

    roundTripped.Options.Title |> should equal session.Options.Title
    roundTripped.Options.AutoClose |> should equal true
    roundTripped.Options.Outcome |> should equal SessionOutcomeMode.Structured
    roundTripped.Options.Permissions |> should equal null
    roundTripped.Options.CompletionSink |> should equal null
    roundTripped.Options.MaxIterations |> should equal 24
    roundTripped.Options.Timeout.Value |> should equal (TimeSpan.FromMinutes 30.)

    let metadata = roundTripped.Options.Metadata |> Option.ofObj
    metadata.Value["source"] |> should equal "cli"

[<Fact>]
let ``Session JSON round-trips an open idle session with every null in place`` () =
    let session =
        { sessionWithoutMarkers () with
            State = SessionState.Idle
            CurrentTurnId = noTurnId
            WorkspaceBinding = nullString
            Options = SessionOptions()
        }

    let json = JsonSerializer.Serialize session
    let roundTripped = deserialize<Session> json

    roundTripped.State |> should equal SessionState.Idle
    roundTripped.CurrentTurnId.HasValue |> should equal false
    roundTripped.ClosedAt.HasValue |> should equal false
    roundTripped.WorkspaceBinding |> should equal null
    roundTripped.Options.Title |> should equal null
    roundTripped.Options.AutoClose |> should equal false
    roundTripped.Options.Outcome |> should equal SessionOutcomeMode.None
    roundTripped.Options.Permissions |> should equal null
    roundTripped.Options.CompletionSink |> should equal null
    roundTripped.Options.MaxIterations |> should equal 0
    roundTripped.Options.Timeout.HasValue |> should equal false
    roundTripped.Options.Metadata |> should equal null

[<Fact>]
let ``Session JSON round-trips a closed session with a close stamp`` () =
    let closedAt = DateTimeOffset(2024, 9, 10, 11, 12, 13, TimeSpan.Zero)

    let session =
        { sessionWithoutMarkers () with
            State = SessionState.Closed
            CurrentTurnId = noTurnId
            ClosedAt = Nullable closedAt
        }

    let json = JsonSerializer.Serialize session
    let roundTripped = deserialize<Session> json

    roundTripped.State |> should equal SessionState.Closed
    roundTripped.CurrentTurnId.HasValue |> should equal false
    roundTripped.ClosedAt.HasValue |> should equal true
    roundTripped.ClosedAt.Value |> should equal closedAt

[<Fact>]
let ``Session JSON serialises identifiers and tenant as plain strings`` () =
    let document = JsonSerializer.Serialize(sampleSession ()) |> JsonDocument.Parse

    document.RootElement.GetProperty("Id").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    document.RootElement.GetProperty("Tenant").GetString() |> should equal "acme"

    document.RootElement.GetProperty("AgentId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    document.RootElement.GetProperty("CurrentTurnId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

[<Fact>]
let ``Session JSON serialises non-null marker options as empty objects`` () =
    let document = JsonSerializer.Serialize(sampleSession ()) |> JsonDocument.Parse

    document.RootElement.GetProperty("Options").GetProperty("Permissions").ValueKind
    |> should equal JsonValueKind.Object

    document.RootElement.GetProperty("Options").GetProperty("CompletionSink").ValueKind
    |> should equal JsonValueKind.Object

[<Fact>]
let ``Session JSON deserialises a persisted payload without the options`` () =
    let json =
        """{"Id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Tenant":"acme","AgentId":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Title":"checkout","State":1,"CreatedAt":"2024-01-02T03:04:05+00:00","UpdatedAt":"2024-01-02T03:04:05+00:00"}"""

    let session = deserialize<Session> json

    session.Id |> should equal (SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV")
    session.Tenant.Value |> should equal "acme"
    session.State |> should equal SessionState.Running
    session.CurrentTurnId.HasValue |> should equal false
    session.ClosedAt.HasValue |> should equal false
    session.WorkspaceBinding |> should equal null
    session.Options |> should equal null
