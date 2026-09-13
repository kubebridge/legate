// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ModelCatalogTests

open System
open System.Text.Json
open FsUnit.Xunit
open Legate
open Xunit

let camelCaseOptions =
    JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase)

/// The generic Deserialize<'T> overload is annotated to return 'T | null,
/// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

/// Deserialises with the camelCase naming policy; round-trip tests pair this
/// with camelCase-serialised payloads, since default options do not bind
/// camelCase keys to PascalCase properties.
let deserializeCamel<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>, camelCaseOptions) |> unbox

/// Narrows a possibly-null catalog result to a non-null entry, failing the
/// test when the catalog unexpectedly returned null.
let requireEntry (entry: ModelCatalogEntry | null) : ModelCatalogEntry =
    match entry with
    | null -> failwith "expected a catalog entry"
    | entry -> entry

// ───────────────────────────────────────────────────────────────────────────
// LlmCoordinationOptions defaults

[<Fact>]
let ``Defaults match the issue's coordination knobs`` () =
    let options = LlmCoordinationOptions()

    options.MaxConcurrentRequests |> should equal 4
    options.RequestsPerMinute |> should equal 60
    options.TokensPerMinute.HasValue |> should equal false
    options.EstimatedOutputTokens |> should equal 4096
    options.MaxQueuedRequests |> should equal 100
    options.RequestTimeoutSeconds |> should equal 120
    options.RetryCount |> should equal 2
    options.MinRetryBackoff |> should equal (TimeSpan.FromMilliseconds 250.0)
    options.MaxRetryBackoff |> should equal (TimeSpan.FromSeconds 10.0)
    options.RateLimitCooldown |> should equal (TimeSpan.FromSeconds 30.0)

[<Fact>]
let ``Defaults validate to null and are mutable`` () =
    let options = LlmCoordinationOptions()
    options.Validate() |> should equal null

    options.MaxConcurrentRequests <- 8
    options.MaxConcurrentRequests |> should equal 8

[<Fact>]
let ``Validate flags each knob out of range`` () =
    let options = LlmCoordinationOptions(MaxConcurrentRequests = 0)
    options.Validate() |> should equal "MaxConcurrentRequests must be at least 1."

    let options = LlmCoordinationOptions(RequestsPerMinute = -1)
    options.Validate() |> should equal "RequestsPerMinute must be at least 1."

    let options = LlmCoordinationOptions(EstimatedOutputTokens = 0)
    options.Validate() |> should equal "EstimatedOutputTokens must be at least 1."

    let options = LlmCoordinationOptions(MaxQueuedRequests = -1)
    options.Validate() |> should equal "MaxQueuedRequests must be at least 0."

    let options = LlmCoordinationOptions(RequestTimeoutSeconds = 0)
    options.Validate() |> should equal "RequestTimeoutSeconds must be at least 1."

    let options = LlmCoordinationOptions(RetryCount = -1)
    options.Validate() |> should equal "RetryCount must be at least 0."

    let options = LlmCoordinationOptions(MaxRetryBackoff = TimeSpan.Zero)

    options.Validate()
    |> should equal "MaxRetryBackoff must be at least MinRetryBackoff."

    let options = LlmCoordinationOptions(RateLimitCooldown = TimeSpan.FromSeconds(-1.0))
    options.Validate() |> should equal "RateLimitCooldown must not be negative."

[<Fact>]
let ``TokensPerMinute validates null vs positive`` () =
    LlmCoordinationOptions().Validate() |> should equal null

    LlmCoordinationOptions(TokensPerMinute = Nullable<int> 50_000).Validate()
    |> should equal null

    LlmCoordinationOptions(TokensPerMinute = Nullable<int> 0).Validate()
    |> should equal "TokensPerMinute must be at least 1 when set."

[<Fact>]
let ``Coordination options serialise camelCase and round-trip`` () =
    let options =
        LlmCoordinationOptions(
            MaxConcurrentRequests = 8,
            RequestsPerMinute = 30,
            TokensPerMinute = Nullable<int> 50_000
        )

    let json = JsonSerializer.Serialize(options, camelCaseOptions)

    json.Contains("\"maxConcurrentRequests\":8") |> should equal true
    json.Contains("\"requestsPerMinute\":30") |> should equal true
    json.Contains("\"tokensPerMinute\":50000") |> should equal true

    let roundTripped = deserializeCamel<LlmCoordinationOptions> json

    roundTripped.MaxConcurrentRequests |> should equal 8
    roundTripped.RequestsPerMinute |> should equal 30
    roundTripped.TokensPerMinute.Value |> should equal 50_000

[<Fact>]
let ``Default coordination options serialise camelCase with unlimited TPM`` () =
    let json = JsonSerializer.Serialize(LlmCoordinationOptions(), camelCaseOptions)

    json.Contains("\"maxConcurrentRequests\":4") |> should equal true
    json.Contains("\"tokensPerMinute\":null") |> should equal true

    let roundTripped = deserializeCamel<LlmCoordinationOptions> json

    roundTripped.MaxConcurrentRequests |> should equal 4
    roundTripped.EstimatedOutputTokens |> should equal 4096
    roundTripped.MinRetryBackoff |> should equal (TimeSpan.FromMilliseconds 250.0)
    roundTripped.TokensPerMinute.HasValue |> should equal false

// ───────────────────────────────────────────────────────────────────────────
// ModelCatalogEntry

[<Fact>]
let ``Entry constructs and round-trips camelCase`` () =
    let entry =
        {
            Model = ModelReference.Parse("anthropic/claude-sonnet")
            ContextWindowTokens = 200_000
            MaxOutputTokens = 64_000
            ReservedOutputTokens = 20_000
            Capabilities =
                {
                    Streaming = true
                    Reasoning = true
                    ToolCalling = true
                }
        }

    entry.ContextWindowTokens |> should equal 200_000
    entry.Capabilities.Reasoning |> should equal true

    let json = JsonSerializer.Serialize(entry, camelCaseOptions)

    json.Contains("\"contextWindowTokens\":200000") |> should equal true
    json.Contains("\"capabilities\":") |> should equal true
    json.Contains("\"reasoning\":true") |> should equal true

    let roundTripped = deserializeCamel<ModelCatalogEntry> json

    roundTripped |> should equal entry
    roundTripped.Model.Value |> should equal "anthropic/claude-sonnet"

// ───────────────────────────────────────────────────────────────────────────
// DefaultModelCatalog

[<Fact>]
let ``Catalog resolves known models with conservative context limits`` () =
    let catalog = DefaultModelCatalog()

    let sonnet =
        requireEntry (catalog.GetEntry(ModelReference.Parse("anthropic/claude-sonnet")))

    sonnet.ContextWindowTokens |> should equal 200_000
    sonnet.MaxOutputTokens |> should equal 64_000
    sonnet.ReservedOutputTokens |> should equal 20_000
    sonnet.Capabilities.ToolCalling |> should equal true

    let flash =
        requireEntry (catalog.GetEntry(ModelReference.Parse("google/gemini-2.5-flash")))

    flash.ContextWindowTokens |> should equal 1_048_576

    // Reserved output can never exceed the context window.
    flash.ReservedOutputTokens |> should equal 20_000

[<Fact>]
let ``Catalog embeds LlmCapabilities rather than duplicating booleans`` () =
    let catalog = DefaultModelCatalog()

    // o3-mini is the one shipped entry with reasoning disabled, so the
    // embedded payload is per-model, not a provider-wide copy.
    let o3 = requireEntry (catalog.GetEntry(ModelReference.Parse("openai/o3-mini")))

    o3.Capabilities.Reasoning |> should equal false
    o3.Capabilities.Streaming |> should equal true
    o3.Capabilities.ToolCalling |> should equal true

    let sonnet =
        requireEntry (catalog.GetEntry(ModelReference.Parse("anthropic/claude-sonnet")))

    sonnet.Capabilities.Reasoning |> should equal true

[<Fact>]
let ``Catalog returns null for unknown models`` () =
    let catalog = DefaultModelCatalog() :> ILlmModelCatalog

    catalog.GetEntry(ModelReference.Parse("mystery/llama-3-70b"))
    |> should equal null

    catalog.GetEntry(ModelReference.Parse("anthropic/claude-42"))
    |> should equal null

[<Fact>]
let ``HasEntry agrees with GetEntry nullability`` () =
    let catalog = DefaultModelCatalog() :> ILlmModelCatalog

    let known = ModelReference.Parse("openai/gpt-4o")
    let unknown = ModelReference.Parse("mystery/llama-3-70b")

    catalog.HasEntry(known) |> should equal true
    catalog.GetEntry(known) |> should not' (be null)

    catalog.HasEntry(unknown) |> should equal false
    catalog.GetEntry(unknown) |> should equal null

[<Fact>]
let ``Catalog lookup is provider-cased and model-case-sensitive`` () =
    let catalog = DefaultModelCatalog() :> ILlmModelCatalog

    catalog.HasEntry(ModelReference.Parse("ANTHROPIC/claude-sonnet"))
    |> should equal true

    // The model segment keeps its case, so the lookup is case-sensitive
    // there: the shipped key is lowercase.
    catalog.HasEntry(ModelReference.Parse("anthropic/Claude-Sonnet"))
    |> should equal false

[<Fact>]
let ``Host override replaces defaults without touching them`` () =
    let defaults = DefaultModelCatalog() :> ILlmModelCatalog
    let mutable overridden = 0

    let hostCatalog =
        { new ILlmModelCatalog with
            member _.GetEntry(reference) =
                if reference.Value = "anthropic/claude-sonnet" then
                    overridden <- overridden + 1

                    {
                        Model = reference
                        ContextWindowTokens = 1_000_000
                        MaxOutputTokens = 100_000
                        ReservedOutputTokens = 10_000
                        Capabilities =
                            {
                                Streaming = true
                                Reasoning = false
                                ToolCalling = true
                            }
                    }
                else
                    defaults.GetEntry(reference)

            member _.HasEntry(reference) =
                reference.Value = "anthropic/claude-sonnet" || defaults.HasEntry(reference)
        }

    hostCatalog.GetEntry(ModelReference.Parse("anthropic/claude-sonnet"))
    |> requireEntry
    |> _.ContextWindowTokens
    |> should equal 1_000_000

    overridden |> should equal 1

    let defaultContext =
        defaults.GetEntry(ModelReference.Parse("openai/gpt-4o"))
        |> requireEntry
        |> _.ContextWindowTokens

    hostCatalog.GetEntry(ModelReference.Parse("openai/gpt-4o"))
    |> requireEntry
    |> _.ContextWindowTokens
    |> should equal defaultContext

    hostCatalog.HasEntry(ModelReference.Parse("anthropic/claude-sonnet"))
    |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Contract nullability on the interface surface

[<Fact>]
let ``Catalog contract methods are null-tolerant on the boundary`` () =
    // A host-implemented catalog returns null through the interface; the
    // call site consumes the null without F# option machinery.
    let emptyCatalog =
        { new ILlmModelCatalog with
            member _.GetEntry(_) = null
            member _.HasEntry(_) = false
        }

    emptyCatalog.GetEntry(ModelReference.Parse("any/model")) |> should equal null

    emptyCatalog.HasEntry(ModelReference.Parse("any/model")) |> should equal false
