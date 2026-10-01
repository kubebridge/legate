// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ProviderFailureReasonTests

open System
open FsUnit.Xunit
open Legate
open Xunit

// Issue 349: the pure ProviderException-property formatter both the
// TurnLoop provider-call failure path and the SessionActor fault fallback
// share. Reasons carry provider id + HTTP status (or no-status) +
// provider message, never key material or raw bodies.

[<Fact>]
let ``A 401 shapes provider id, status, and message`` () =
    let failure =
        ProviderException(
            "ollamacloud",
            Nullable 401,
            Nullable<TimeSpan>(),
            "The 'ollamacloud' provider request failed with HTTP 401."
        )

    ProviderFailureReason.formatProviderFailure failure
    |> should
        equal
        "The 'ollamacloud' provider call failed with HTTP 401: The 'ollamacloud' provider request failed with HTTP 401."

[<Fact>]
let ``A 404 shapes provider id, status, and message`` () =
    let failure =
        ProviderException(
            "ollamacloud",
            Nullable 404,
            Nullable<TimeSpan>(),
            "The 'ollamacloud' provider request failed with HTTP 404."
        )

    ProviderFailureReason.formatProviderFailure failure
    |> should
        equal
        "The 'ollamacloud' provider call failed with HTTP 404: The 'ollamacloud' provider request failed with HTTP 404."

[<Fact>]
let ``A network failure without status shapes no-status`` () =
    let failure =
        ProviderException(
            "ollamacloud",
            Nullable<int>(),
            Nullable<TimeSpan>(),
            "The 'ollamacloud' provider call failed with HttpRequestException."
        )

    ProviderFailureReason.formatProviderFailure failure
    |> should
        equal
        "The 'ollamacloud' provider call failed with no-status: The 'ollamacloud' provider call failed with HttpRequestException."

[<Fact>]
let ``An empty message shapes without the message part`` () =
    let failure =
        ProviderException("ollamacloud", Nullable 429, Nullable<TimeSpan>(), "")

    ProviderFailureReason.formatProviderFailure failure
    |> should equal "The 'ollamacloud' provider call failed with HTTP 429."

[<Fact>]
let ``A blank provider id shapes as unknown`` () =
    let failure =
        ProviderException("  ", Nullable 500, Nullable<TimeSpan>(), "backend unavailable")

    ProviderFailureReason.formatProviderFailure failure
    |> should equal "The 'unknown' provider call failed with HTTP 500: backend unavailable"

[<Fact>]
let ``A long message truncates bounded`` () =
    let message = String('x', ProviderFailureReason.MaxMessageChars + 10)

    let failure =
        ProviderException("ollamacloud", Nullable 500, Nullable<TimeSpan>(), message)

    let reason = ProviderFailureReason.formatProviderFailure failure

    reason.EndsWith("[truncated]") |> should equal true
    Assert.True(reason.Length < message.Length + 100)

[<Fact>]
let ``formatFault passes a provider fault through the formatter`` () =
    let failure =
        ProviderException(
            "ollamacloud",
            Nullable 404,
            Nullable<TimeSpan>(),
            "The 'ollamacloud' provider request failed with HTTP 404."
        )

    ProviderFailureReason.formatFault failure
    |> should
        equal
        "The 'ollamacloud' provider call failed with HTTP 404: The 'ollamacloud' provider request failed with HTTP 404."

[<Fact>]
let ``formatFault keeps the type-name shape for other faults`` () =
    ProviderFailureReason.formatFault (InvalidOperationException("boom"))
    |> should equal "The turn faulted: InvalidOperationException."

[<Fact>]
let ``formatFault reads a null error as Exception`` () =
    ProviderFailureReason.formatFault Unchecked.defaultof<exn>
    |> should equal "The turn faulted: Exception."

[<Fact>]
let ``Shaped reasons carry no secret shapes`` () =
    let failure =
        ProviderException(
            "ollamacloud",
            Nullable 401,
            Nullable<TimeSpan>(),
            "The 'ollamacloud' provider request failed with HTTP 401."
        )

    let reason = ProviderFailureReason.formatProviderFailure failure

    JournalWriter.redactText reason |> should equal reason
    reason.Contains("sk-") |> should equal false
