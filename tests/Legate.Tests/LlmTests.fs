// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LlmTests

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Text.Json
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Xunit

/// The generic Deserialize<'T> overload is annotated to return 'T | null,
/// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

let nullString = Unchecked.defaultof<string>

// ───────────────────────────────────────────────────────────────────────────
// ModelReference

[<Fact>]
let ``Parse accepts and canonicalises provider segments`` () =
    let reference = ModelReference.Parse(" ANTHROPIC/claude-sonnet ")

    reference.Provider |> should equal "anthropic"
    reference.Model |> should equal "claude-sonnet"
    reference.Value |> should equal "anthropic/claude-sonnet"

[<Fact>]
let ``Parse and ToString round-trip`` () =
    ModelReference.Parse("anthropic/claude-sonnet").ToString()
    |> should equal "anthropic/claude-sonnet"

    let fresh = ModelReference.Parse("openai/gpt-4o")
    ModelReference.Parse(fresh.ToString()) |> should equal fresh

[<Fact>]
let ``Only the first slash separates the segments`` () =
    let reference = ModelReference.Parse("huggingface/meta-llama/Llama-3-70B")

    reference.Provider |> should equal "huggingface"
    reference.Model |> should equal "meta-llama/Llama-3-70B"
    reference.ToString() |> should equal "huggingface/meta-llama/Llama-3-70B"

[<Fact>]
let ``Parse throws LegateIdentifierException on invalid input`` () =
    (fun () -> ModelReference.Parse "noglash" |> ignore)
    |> should throw typeof<LegateIdentifierException>

    (fun () -> ModelReference.Parse "/leading" |> ignore)
    |> should throw typeof<LegateIdentifierException>

    (fun () -> ModelReference.Parse "trailing/" |> ignore)
    |> should throw typeof<LegateIdentifierException>

    (fun () -> ModelReference.Parse "" |> ignore)
    |> should throw typeof<LegateIdentifierException>

    (fun () -> ModelReference.Parse "   " |> ignore)
    |> should throw typeof<LegateIdentifierException>

[<Fact>]
let ``Parse throws ArgumentNullException on null input`` () =
    (fun () -> ModelReference.Parse nullString |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``TryParse mirrors the out parameter form`` () =
    let mutable reference = Unchecked.defaultof<ModelReference>

    ModelReference.TryParse("anthropic/claude-sonnet", &reference)
    |> should equal true

    reference.Value |> should equal "anthropic/claude-sonnet"

    ModelReference.TryParse("OpenAI/gpt-4o", &reference) |> should equal true
    reference.Value |> should equal "openai/gpt-4o"

[<Fact>]
let ``TryParse returns false for invalid input without throwing`` () =
    let mutable reference = Unchecked.defaultof<ModelReference>

    ModelReference.TryParse("noglash", &reference) |> should equal false
    ModelReference.TryParse("/leading", &reference) |> should equal false
    ModelReference.TryParse("trailing/", &reference) |> should equal false
    ModelReference.TryParse("", &reference) |> should equal false
    ModelReference.TryParse("   ", &reference) |> should equal false
    ModelReference.TryParse(nullString, &reference) |> should equal false

[<Fact>]
let ``ModelReference equality and hashing are ordinal and case-insensitive on the provider`` () =
    let a = ModelReference.Parse("anthropic/claude")
    let b = ModelReference.Parse("ANTHROPIC/claude")
    let c = ModelReference.Parse("anthropic/claude-sonnet")

    a |> should equal b
    a.GetHashCode() |> should equal (b.GetHashCode())
    a |> should not' (equal c)

[<Fact>]
let ``Boxed Equals delegates without recursion`` () =
    let a = ModelReference.Parse("anthropic/claude")
    let b = ModelReference.Parse("ANTHROPIC/claude")
    let c = ModelReference.Parse("openai/gpt-4o")

    (a :> obj).Equals(b :> obj) |> should equal true
    (a :> obj).Equals(c :> obj) |> should equal false
    (a :> obj).Equals("anthropic/claude" :> obj) |> should equal false
    (a :> obj).Equals(null) |> should equal false

    (a :> IEquatable<ModelReference>).Equals(c) |> should equal false

[<Fact>]
let ``ModelReference serialises and deserialises as a plain string`` () =
    let reference = ModelReference.Parse("anthropic/claude-sonnet")

    JsonSerializer.Serialize reference |> should equal "\"anthropic/claude-sonnet\""

    deserialize<ModelReference> "\"anthropic/claude-sonnet\""
    |> should equal reference

    deserialize<ModelReference> "\"ANTHROPIC/claude-sonnet\""
    |> should equal reference

[<Fact>]
let ``JSON deserialisation rejects invalid and non-string payloads`` () =
    (fun () -> deserialize<ModelReference> "\"noglash\"" |> ignore)
    |> should throw typeof<LegateIdentifierException>

    (fun () -> deserialize<ModelReference> "null" |> ignore)
    |> should throw typeof<JsonException>

    (fun () -> deserialize<ModelReference> "12345" |> ignore)
    |> should throw typeof<JsonException>

[<Fact>]
let ``Segment constructor validates and canonicalises`` () =
    let reference = ModelReference("OpenAI", "gpt-4o", 0)

    reference.Provider |> should equal "openai"
    reference.Model |> should equal "gpt-4o"

[<Fact>]
let ``Segment constructor rejects invalid segments`` () =
    (fun () -> ModelReference(nullString, "gpt-4o", 0) |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> ModelReference("openai", nullString, 0) |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> ModelReference("", "gpt-4o", 0) |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> ModelReference(" openai", "gpt-4o", 0) |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> ModelReference("has space", "gpt-4o", 0) |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> ModelReference("open/ai", "gpt-4o", 0) |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> ModelReference("openai", "has space", 0) |> ignore)
    |> should throw typeof<ArgumentException>

// ───────────────────────────────────────────────────────────────────────────
// ModelReferenceInference (compatibility helper)

[<Fact>]
let ``TryInfer maps documented vendor prefixes`` () =
    let mutable provider = null

    ModelReferenceInference.TryInfer("gpt-4o", &provider) |> should equal true
    provider |> should equal "openai"

    ModelReferenceInference.TryInfer("GPT-4o", &provider) |> should equal true
    provider |> should equal "openai"

    ModelReferenceInference.TryInfer("claude-sonnet", &provider)
    |> should equal true

    provider |> should equal "anthropic"

    ModelReferenceInference.TryInfer("gemini-2.5-flash", &provider)
    |> should equal true

    provider |> should equal "google"

    ModelReferenceInference.TryInfer("o3-mini", &provider) |> should equal true
    provider |> should equal "openai"

[<Fact>]
let ``TryInfer returns false for blank, provider-slash, and unknown names`` () =
    let mutable provider = null

    ModelReferenceInference.TryInfer("   ", &provider) |> should equal false
    provider |> should equal null

    ModelReferenceInference.TryInfer(nullString, &provider) |> should equal false
    provider |> should equal null

    ModelReferenceInference.TryInfer("anthropic/claude", &provider)
    |> should equal false

    provider |> should equal null

    ModelReferenceInference.TryInfer("llama-3-70b", &provider) |> should equal false
    provider |> should equal null

[<Fact>]
let ``Infer returns the provider or throws for unknown names`` () =
    ModelReferenceInference.Infer("claude-sonnet") |> should equal "anthropic"

    (fun () -> ModelReferenceInference.Infer "mystery-model" |> ignore)
    |> should throw typeof<ArgumentException>

// ───────────────────────────────────────────────────────────────────────────
// LlmCapabilities and LlmProviderOptions

[<Fact>]
let ``LlmCapabilities constructs and mutates`` () =
    let capabilities =
        {
            Streaming = true
            Reasoning = false
            ToolCalling = true
        }

    capabilities.Streaming |> should equal true
    capabilities.Reasoning |> should equal false
    capabilities.ToolCalling |> should equal true

    let updated = { capabilities with Streaming = false }
    updated.Streaming |> should equal false
    capabilities.Streaming |> should equal true

[<Fact>]
let ``LlmProviderOptions holds the API key`` () =
    let options = LlmProviderOptions()
    options.ApiKey |> should equal null

    options.ApiKey <- "sk-test"
    options.ApiKey |> should equal "sk-test"

// ───────────────────────────────────────────────────────────────────────────
// ILlmProvider, IApiKeyProvider, ILlmProviderRegistry

/// Test double: an IChatClient that accepts construction without services.
type FakeChatClient() =
    interface IChatClient with
        member _.GetResponseAsync(_, _, _) = raise (NotImplementedException())
        member _.GetStreamingResponseAsync(_, _, _) = raise (NotImplementedException())
        member _.GetService(_, _) = null
        member _.Dispose() = ()

/// Test double: a provider that records what it was asked to build.
type FakeProvider(id: string, defaultModel: string) =
    let mutable built: string list = []

    interface ILlmProvider with
        member _.Id = id
        member _.DefaultModel = defaultModel

        member _.Capabilities =
            {
                Streaming = true
                Reasoning = false
                ToolCalling = true
            }

        member _.CreateChatClient(model, _options) =
            built <- model.Value :: built
            new FakeChatClient() :> IChatClient

    member _.Built = List.rev built

/// Test double: a registry resolving from a map, missing with the typed
/// exception.
type FakeRegistry(providers: (string * ILlmProvider) list) =

    let byId = providers |> dict

    interface ILlmProviderRegistry with
        member _.Resolve(reference) =
            match byId.TryGetValue reference.Provider with
            | true, provider -> provider
            | false, _ ->
                raise (
                    ProviderNotRegisteredException(
                        reference.Provider,
                        ([ for id, _ in providers -> id ] :> IReadOnlyList<string>),
                        sprintf
                            "No LLM provider is registered under '%s'. Registered providers: %s."
                            reference.Provider
                            (String.Join(", ", [ for id, _ in providers -> id ]))
                    )
                )

        member _.RegisteredProviders =
            [ for id, _ in providers -> id ] :> IReadOnlyList<string>

/// Test double: an API key provider backed by a table.
type FakeApiKeyProvider(keys: (string * string) list) =
    interface IApiKeyProvider with
        member _.GetApiKey(providerId, _tenantId) =
            keys
            |> List.tryFind (fun (id, _) -> id = providerId)
            |> Option.map snd
            |> Option.toObj

[<Fact>]
let ``Registry resolves a reference to its provider`` () =
    let anthropic = FakeProvider("anthropic", "claude-sonnet")
    let openai = FakeProvider("openai", "gpt-4o")

    let registry =
        FakeRegistry(
            [
                "anthropic", anthropic :> ILlmProvider
                "openai", openai :> ILlmProvider
            ]
        )
        :> ILlmProviderRegistry

    let resolved = registry.Resolve(ModelReference.Parse("anthropic/claude-sonnet"))

    (resolved :?> FakeProvider) |> should equal anthropic

    registry.RegisteredProviders
    |> should equal ([ "anthropic"; "openai" ] :> IReadOnlyList<string>)

[<Fact>]
let ``Registry miss throws the typed exception listing registered providers`` () =
    let registry =
        FakeRegistry(
            [
                "anthropic", FakeProvider("anthropic", "claude") :> ILlmProvider
            ]
        )
        :> ILlmProviderRegistry

    try
        registry.Resolve(ModelReference.Parse("openai/gpt-4o")) |> ignore
        failwith "expected ProviderNotRegisteredException"
    with :? ProviderNotRegisteredException as ex ->
        ex.ProviderId |> should equal "openai"

        ex.RegisteredProviders
        |> should equal ([ "anthropic" ] :> IReadOnlyList<string>)

        ex.Message.Contains("Registered providers: anthropic") |> should equal true
        ex.Message.Contains("sk-") |> should equal false

[<Fact>]
let ``Provider builds a chat client for a resolved model`` () =
    let provider = FakeProvider("anthropic", "claude-sonnet")
    let reference = ModelReference.Parse("anthropic/claude")

    let client =
        (provider :> ILlmProvider).CreateChatClient(reference, LlmProviderOptions(ApiKey = "sk-test"))

    client |> should not' (be null)
    provider.Built |> should equal [ "anthropic/claude" ]

[<Fact>]
let ``API key provider resolves per-provider keys or null`` () =
    let keyProvider =
        FakeApiKeyProvider(
            [
                "anthropic", "sk-anthropic"
                "openai", "sk-openai"
            ]
        )
        :> IApiKeyProvider

    keyProvider.GetApiKey("anthropic", TenantId.Default)
    |> should equal "sk-anthropic"

    keyProvider.GetApiKey("openai", TenantId.Default) |> should equal "sk-openai"
    keyProvider.GetApiKey("google", TenantId.Default) |> should equal null

[<Fact>]
let ``Provider contracts expose ids, defaults, and capabilities`` () =
    let provider = FakeProvider("anthropic", "claude-sonnet") :> ILlmProvider

    provider.Id |> should equal "anthropic"
    provider.DefaultModel |> should equal "claude-sonnet"
    provider.Capabilities.Streaming |> should equal true
    provider.Capabilities.ToolCalling |> should equal true

[<Fact>]
let ``Sub-agent override uses the same ModelReference form`` () =
    let overrideReference = ModelReference.Parse("openai/o3-mini")

    overrideReference.Value |> should equal "openai/o3-mini"

    let mutable provider = null
    ModelReferenceInference.TryInfer("o3-mini", &provider) |> should equal true
    provider |> should equal "openai"
