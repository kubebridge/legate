// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic

// Per-provider coordination options and the model catalog contract. The
// coordinator reads its rate, concurrency, and retry knobs from
// LlmCoordinationOptions (bound from the Legate configuration section) and
// per-model context limits from ILlmModelCatalog, so hosts override catalog
// data rather than recompile constants. Pricing is deliberately absent.

/// Per-provider coordination settings: concurrency, rate limits, request
/// shaping, and retry behaviour for the LLM coordinator. Bound from the
/// <c>Legate</c> configuration section; mutable so hosts can set properties
/// before registering. Defaults mirror BridgeMCP's proven coordinator values.
type LlmCoordinationOptions() =

    /// The maximum requests the provider runs concurrently. Default 4.
    member val MaxConcurrentRequests: int = 4 with get, set

    /// The steady-state requests per minute the provider accepts. Default 60.
    member val RequestsPerMinute: int = 60 with get, set

    /// The tokens per minute the provider accepts, or null for unlimited.
    member val TokensPerMinute: Nullable<int> = Nullable<int>() with get, set

    /// The output tokens budgeted for a typical response before the request
    /// is sent. Default 4096.
    member val EstimatedOutputTokens: int = 4096 with get, set

    /// The requests queued above the concurrency limit before new requests
    /// are rejected. Default 100.
    member val MaxQueuedRequests: int = 100 with get, set

    /// The per-request timeout in seconds. Default 120.
    member val RequestTimeoutSeconds: int = 120 with get, set

    /// The retry attempts per failed request, before the request surfaces as
    /// a turn failure. Default 2.
    member val RetryCount: int = 2 with get, set

    /// The minimum jittered backoff between retries. Default 250 ms.
    member val MinRetryBackoff: TimeSpan = TimeSpan.FromMilliseconds 250.0 with get, set

    /// The maximum jittered backoff between retries. Default 10 s.
    member val MaxRetryBackoff: TimeSpan = TimeSpan.FromSeconds 10.0 with get, set

    /// The pause after an HTTP 429 before requests resume. Default 30 s.
    member val RateLimitCooldown: TimeSpan = TimeSpan.FromSeconds 30.0 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation. The coordinator throws on a non-null result before a
    /// turn runs.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let violations =
            [|
                if this.MaxConcurrentRequests < 1 then
                    "MaxConcurrentRequests must be at least 1."
                if this.RequestsPerMinute < 1 then
                    "RequestsPerMinute must be at least 1."
                if this.TokensPerMinute.HasValue && this.TokensPerMinute.Value < 1 then
                    "TokensPerMinute must be at least 1 when set."
                if this.EstimatedOutputTokens < 1 then
                    "EstimatedOutputTokens must be at least 1."
                if this.MaxQueuedRequests < 0 then
                    "MaxQueuedRequests must be at least 0."
                if this.RequestTimeoutSeconds < 1 then
                    "RequestTimeoutSeconds must be at least 1."
                if this.RetryCount < 0 then
                    "RetryCount must be at least 0."
                if this.MinRetryBackoff < TimeSpan.Zero then
                    "MinRetryBackoff must not be negative."
                if this.MaxRetryBackoff < this.MinRetryBackoff then
                    "MaxRetryBackoff must be at least MinRetryBackoff."
                if this.RateLimitCooldown < TimeSpan.Zero then
                    "RateLimitCooldown must not be negative."
            |]

        if violations.Length = 0 then
            null
        else
            Array.head violations

/// Per-model metadata: feature flags plus the context limits the turn loop
/// consumes. Hosts construct it with object initialisers and it serialises
/// with System.Text.Json.
[<CLIMutable>]
type ModelCatalogEntry =
    {
        /// The model the entry describes.
        Model: ModelReference
        /// The total tokens the model's context window holds.
        ContextWindowTokens: int
        /// The maximum output tokens a response may carry.
        MaxOutputTokens: int
        /// The output tokens held in reserve inside the context window.
        ReservedOutputTokens: int
        /// The features the model supports.
        Capabilities: LlmCapabilities
    }

/// Resolves a <see cref="T:Legate.ModelReference" /> to its per-model
/// metadata. Implemented by the runtime's <see cref="T:Legate.DefaultModelCatalog" />
/// or by hosts; the turn loop reads context limits from the catalog rather
/// than constants. Lookup is read-only; mutating methods belong to host
/// configuration, not this contract.
type ILlmModelCatalog =

    /// Returns the entry for a model, or null when the catalog has none.
    /// <param name="reference">The model to look up.</param>
    /// <returns>The model's entry, or null when the model is unknown.</returns>
    abstract GetEntry: reference: ModelReference -> ModelCatalogEntry | null

    /// Reports whether the catalog has an entry for a model.
    /// <param name="reference">The model to look up.</param>
    /// <returns>true when the catalog has an entry; otherwise false.</returns>
    abstract HasEntry: reference: ModelReference -> bool

/// The default catalog: conservative fallback metadata for every model, with
/// overrides shipped for the well-known model families. Context limits match
/// BridgeMCP's hard-coded values (128,000 total, 20,000 reserved, 8,192 max
/// output). Sealed; hosts replace it rather than derive from it.
[<Sealed>]
type DefaultModelCatalog() =

    /// The conservative fallback used for unknown models.
    [<Literal>]
    static let fallbackContextWindowTokens = 128_000

    /// The conservative fallback output reserve for unknown models.
    [<Literal>]
    static let fallbackReservedOutputTokens = 20_000

    /// The conservative fallback maximum output for unknown models.
    [<Literal>]
    static let fallbackMaxOutputTokens = 8_192

    static let entries =
        let known =
            [
                // Anthropic
                "anthropic/claude-sonnet", (200_000, 64_000, true, true, true)
                "anthropic/claude-haiku", (200_000, 64_000, true, true, true)
                "anthropic/claude-opus", (200_000, 32_000, true, true, true)
                // OpenAI
                "openai/gpt-4o", (128_000, 16_384, true, true, true)
                "openai/gpt-4o-mini", (128_000, 16_384, true, true, true)
                "openai/o3-mini", (200_000, 100_000, false, true, true)
                // Google
                "google/gemini-2.5-pro", (1_048_576, 65_536, true, true, true)
                "google/gemini-2.5-flash", (1_048_576, 65_536, true, true, true)
            ]

        Map.ofList
            [
                for model, (context, output, reasoning, streaming, tools) in known ->
                    model,
                    {
                        Model = ModelReference.Parse model
                        ContextWindowTokens = context
                        MaxOutputTokens = output
                        ReservedOutputTokens = min fallbackReservedOutputTokens context
                        Capabilities =
                            {
                                Streaming = streaming
                                Reasoning = reasoning
                                ToolCalling = tools
                            }
                    }
            ]

    /// The context window the catalog falls back to for unknown models.
    static member FallbackContextWindowTokens = fallbackContextWindowTokens

    /// The maximum output the catalog falls back to for unknown models.
    static member FallbackMaxOutputTokens = fallbackMaxOutputTokens

    /// The output reserve the catalog falls back to for unknown models.
    static member FallbackReservedOutputTokens = fallbackReservedOutputTokens

    /// Returns the entry for a model, or null when the catalog has none.
    /// <param name="reference">The model to look up.</param>
    /// <returns>The model's entry, or null when the model is unknown.</returns>
    member this.GetEntry(reference: ModelReference) : ModelCatalogEntry | null =
        match Map.tryFind reference.Value entries with
        | Some entry -> entry
        | None -> null

    /// Reports whether the catalog has an entry for a model.
    /// <param name="reference">The model to look up.</param>
    /// <returns>true when the catalog has an entry; otherwise false.</returns>
    member this.HasEntry(reference: ModelReference) : bool = entries.ContainsKey reference.Value

    interface ILlmModelCatalog with

        member this.GetEntry(reference) : ModelCatalogEntry | null = this.GetEntry reference

        member this.HasEntry(reference) : bool = this.HasEntry reference
