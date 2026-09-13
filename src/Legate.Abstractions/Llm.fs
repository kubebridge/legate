// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Text.Json
open System.Text.Json.Serialization
open Microsoft.Extensions.AI

// LLM provider contracts. A host registers providers that own a provider id
// and build IChatClient instances for models; the runtime resolves a
// ModelReference through the registry before a turn touches the provider.
// API keys travel through provider options or an IApiKeyProvider, never
// through identifiers, and are never embedded in messages or diagnostics.

/// A validated reference to a model on a provider, written
/// <c>provider/model</c> (for example <c>anthropic/claude-sonnet</c>). The
/// provider segment is canonicalised to lowercase; the model segment keeps
/// its case and may itself contain slashes (only the first slash separates
/// the segments). Both segments are non-empty, trimmed, and contain no
/// Unicode whitespace (tabs and line breaks included); the provider segment
/// additionally contains no slashes. A value type like the other Legate
/// identifiers; invalid input throws
/// <see cref="T:Legate.LegateIdentifierException" />.
[<Struct; CustomEquality; NoComparison>]
[<JsonConverter(typeof<ModelReferenceJsonConverter>)>]
type ModelReference private (provider: string, model: string) =

    /// The one segment rule, shared by the validating constructor and
    /// TryParse so the contract lives in a single place: a segment is
    /// non-null, non-empty, trimmed, and contains no Unicode whitespace
    /// (tabs and line breaks included). The provider segment may not
    /// contain a slash; the model segment may (first-slash split).
    /// <param name="segment">The segment to validate.</param>
    /// <param name="allowSlash">true for the model segment, which may contain slashes.</param>
    /// <returns>true when the segment is valid; otherwise false.</returns>
    static member private SegmentIsValid(segment: string, allowSlash: bool) =
        not (String.IsNullOrWhiteSpace segment)
        && segment.Trim() = segment
        && not (segment |> Seq.exists Char.IsWhiteSpace)
        && (allowSlash || segment.IndexOf('/') < 0)

    /// Creates a reference from validated segments. Prefer
    /// <see cref="M:Legate.ModelReference.Parse(System.String)" /> over raw
    /// strings.
    /// <param name="provider">The provider id; non-empty, no Unicode whitespace or slashes.</param>
    /// <param name="model">The model id; non-empty, no Unicode whitespace; slashes allowed.</param>
    /// <exception cref="T:System.ArgumentNullException">A segment is null.</exception>
    /// <exception cref="T:System.ArgumentException">A segment is empty, untrimmed, or contains whitespace (or the provider segment a slash).</exception>
    new(provider: string, model: string, _dummy: int) =
        if isNull (box provider) then
            raise (ArgumentNullException(nameof provider))

        if isNull (box model) then
            raise (ArgumentNullException(nameof model))

        if
            not (ModelReference.SegmentIsValid(provider, false))
            || not (ModelReference.SegmentIsValid(model, true))
        then
            raise (
                ArgumentException(
                    "A model reference segment must be a trimmed, non-empty string without whitespace or slashes."
                )
            )

        ModelReference(provider.ToLowerInvariant(), model)

    /// Parses a model reference in <c>provider/model</c> form; the provider
    /// segment is canonicalised to lowercase. Throws
    /// LegateIdentifierException on invalid input and ArgumentNullException
    /// on null.
    /// <param name="value">The string to parse, for example "anthropic/claude-sonnet".</param>
    /// <returns>The parsed model reference.</returns>
    static member Parse(value: string) =
        if isNull (box value) then
            raise (ArgumentNullException(nameof value))

        let mutable parsed = Unchecked.defaultof<ModelReference>

        if ModelReference.TryParse(value, &parsed) then
            parsed
        else
            raise (LegateIdentifierException(sprintf "'%s' is not a valid model reference." value))

    /// Attempts to parse a model reference in <c>provider/model</c> form;
    /// returns false for null, blank, or otherwise invalid input. The
    /// segment rule is the validating constructor's; TryParse only splits
    /// and delegates.
    /// <param name="value">The string to parse.</param>
    /// <param name="result">Receives the parsed reference when parsing succeeds.</param>
    /// <returns>true when <paramref name="value" /> parsed; otherwise false.</returns>
    static member TryParse(value: string, [<Out>] result: byref<ModelReference>) : bool =
        if String.IsNullOrWhiteSpace value then
            result <- Unchecked.defaultof<ModelReference>
            false
        else
            let trimmed = value.Trim()
            let first = trimmed.IndexOf('/')

            if first <= 0 || first = trimmed.Length - 1 then
                result <- Unchecked.defaultof<ModelReference>
                false
            else
                let provider = trimmed.Substring(0, first).ToLowerInvariant()
                let model = trimmed.Substring(first + 1)

                if
                    ModelReference.SegmentIsValid(provider, false)
                    && ModelReference.SegmentIsValid(model, true)
                then
                    result <- ModelReference(provider, model)
                    true
                else
                    result <- Unchecked.defaultof<ModelReference>
                    false

    /// The canonical lowercase provider id, for example "anthropic".
    member _.Provider = provider

    /// The model id on the provider, case preserved, for example
    /// "meta-llama/Llama-3-70B".
    member _.Model = model

    /// The canonical <c>provider/model</c> string.
    member _.Value = provider + "/" + model

    /// Compares two model references ordinally.
    /// <param name="other">The reference to compare against.</param>
    /// <returns>true when both segments match ordinally.</returns>
    member this.Equals(other: ModelReference) =
        String.Equals(this.Value, other.Value, StringComparison.Ordinal)

    /// Returns the canonical <c>provider/model</c> string.
    override _.ToString() = provider + "/" + model

    /// Hashes the canonical string ordinally.
    override _.GetHashCode() =
        StringComparer.Ordinal.GetHashCode(provider + "/" + model)

    /// Compares against a boxed model reference without recursing.
    /// <param name="other">The object to compare against.</param>
    /// <returns>true when <paramref name="other" /> is an equal reference.</returns>
    override this.Equals(other: obj) =
        match other with
        | :? ModelReference as reference -> this.Equals(reference: ModelReference)
        | _ -> false

    /// Implements ordinal equality for the generic collection surface.
    /// <param name="other">The reference to compare against.</param>
    /// <returns>true when both segments match ordinally.</returns>
    interface IEquatable<ModelReference> with
        member this.Equals(other: ModelReference) = this.Equals(other: ModelReference)

/// JSON converter plumbing for <see cref="T:Legate.ModelReference" />, kept
/// internal and concrete so the [<JsonConverter>] attribute, which needs a
/// parameterless constructor, can register it on the struct.
and internal ModelReferenceJsonConverter() =
    inherit JsonConverter<ModelReference>()

    override _.Read(reader: byref<Utf8JsonReader>, _typeToConvert: Type, _options: JsonSerializerOptions) =
        if reader.TokenType <> JsonTokenType.String then
            raise (JsonException "A model reference must be a non-null JSON string.")

        match reader.GetString() with
        | null -> raise (JsonException "A model reference must be a non-null JSON string.")
        | raw -> ModelReference.Parse raw

    override _.Write(writer: Utf8JsonWriter, value: ModelReference, _options: JsonSerializerOptions) =
        writer.WriteStringValue(value.ToString())

// ───────────────────────────────────────────────────────────────────────────
// Compatibility: vendor-prefix inference
//
// Pre-Legate configuration named a model without its provider and inferred
// the provider from well-known vendor prefixes. ModelReference is the
// primary mechanism; the helper below only maps legacy values during
// migration. It is a lookup of documented public vendor prefixes, nothing
// else consumes it, and it never fetches anything.

/// Maps bare model names to provider ids for configuration that predates the
/// <c>provider/model</c> form. Compatibility only: hosts should prefer
/// explicit <see cref="T:Legate.ModelReference" /> values.
type ModelReferenceInference() =

    /// Attempts to infer the provider id from a bare model name by its
    /// documented vendor prefix (for example "gpt-" and "o" to "openai",
    /// "claude-" to "anthropic", "gemini-" to "google"). Case-insensitive on
    /// the prefix. Returns false for blank input, unknown prefixes, and
    /// values already in <c>provider/model</c> form.
    /// <param name="modelName">A bare model name, for example "gpt-4o".</param>
    /// <param name="providerId">Receives the inferred provider id when a prefix matched.</param>
    /// <returns>true when a known vendor prefix matched; otherwise false.</returns>
    static member TryInfer(modelName: string, [<Out>] providerId: byref<string | null>) : bool =
        if String.IsNullOrWhiteSpace modelName then
            providerId <- null
            false
        else
            let name = modelName.Trim()

            if name.IndexOf('/') >= 0 then
                providerId <- null
                false
            else
                // (prefix, providerId) pairs, longest prefixes first so a
                // table row never shadows a more specific one.
                let prefixes =
                    [
                        "claude-", "anthropic"
                        "gemini-", "google"
                        "gpt-", "openai"
                        "o1", "openai"
                        "o3", "openai"
                        "o4", "openai"
                    ]

                let matched =
                    prefixes
                    |> List.tryFind (fun (prefix, _) -> name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))

                match matched with
                | Some(_, provider) ->
                    providerId <- provider
                    true
                | None ->
                    providerId <- null
                    false

    /// Infers the provider id for a bare model name or throws
    /// ArgumentException when no known vendor prefix matched. Compatibility
    /// only; prefer explicit <see cref="T:Legate.ModelReference" /> values.
    /// <param name="modelName">A bare model name, for example "claude-sonnet".</param>
    /// <returns>The inferred provider id.</returns>
    /// <exception cref="T:System.ArgumentException">No known vendor prefix matched.</exception>
    static member Infer(modelName: string) =
        let mutable provider = null

        if ModelReferenceInference.TryInfer(modelName, &provider) then
            provider
        else
            raise (ArgumentException("No provider prefix matched the model name.", nameof modelName))

/// What an <see cref="T:Legate.ILlmProvider" /> supports on the models it
/// serves. Declares the features the coordinator may use; hosts construct it
/// with object initialisers and it serialises with System.Text.Json.
[<CLIMutable>]
type LlmCapabilities =
    {
        /// Whether the provider streams tokens while a turn runs.
        Streaming: bool
        /// Whether the provider emits reasoning content.
        Reasoning: bool
        /// Whether the provider supports tool calling.
        ToolCalling: bool
    }

/// Per-provider settings: the API key plus whatever a provider package
/// extends. Bound from the <c>Legate</c> configuration section; mutable so
/// hosts can set properties before registering.
type LlmProviderOptions() =

    /// The provider's API key, or null when the host supplies keys another
    /// way (for example an <see cref="T:Legate.IApiKeyProvider" />).
    /// Never logged or embedded in diagnostics.
    member val ApiKey: string | null = null with get, set

/// Builds <see cref="T:Microsoft.Extensions.AI.IChatClient" /> instances for
/// the models a provider serves. Implemented by provider packages; the
/// coordinator resolves providers through
/// <see cref="T:Legate.ILlmProviderRegistry" /> before a turn runs.
type ILlmProvider =

    /// The provider id, matching the segment of a
    /// <see cref="T:Legate.ModelReference" />, for example "anthropic".
    abstract Id: string

    /// The model the provider serves when a reference names only the
    /// provider, for example "claude-sonnet".
    abstract DefaultModel: string

    /// What the provider supports on the models it serves.
    abstract Capabilities: LlmCapabilities

    /// Creates a chat client bound to the model. Synchronous: providers wrap
    /// their SDK's construction, which never performs network I/O; calls
    /// through the client stay asynchronous.
    /// <param name="model">The model to bind; the reference's provider segment must equal <see cref="P:Legate.ILlmProvider.Id" />.</param>
    /// <param name="options">Provider settings (API key, endpoints), or null to use defaults.</param>
    /// <returns>A chat client for the model.</returns>
    abstract CreateChatClient: model: ModelReference * options: LlmProviderOptions | null -> IChatClient

/// Resolves API keys per tenant when a host keeps keys outside
/// <see cref="T:Legate.LlmProviderOptions" />. Implemented by hosts; consumed
/// by the coordinator in a later issue.
type IApiKeyProvider =

    /// Returns the API key for a provider and tenant, or null when the host
    /// has none for that pair.
    /// <param name="providerId">The provider the key is for, for example "anthropic".</param>
    /// <param name="tenantId">The tenant the key is scoped to.</param>
    /// <returns>The API key, or null when the host has none.</returns>
    abstract GetApiKey: providerId: string * tenantId: TenantId -> string | null

/// Resolves a <see cref="T:Legate.ModelReference" /> to the provider that
/// serves it. Implemented by the runtime (issue 54); hosts register
/// <see cref="T:Legate.ILlmProvider" /> instances with it.
type ILlmProviderRegistry =

    /// Returns the provider owning the reference's provider segment.
    /// <param name="reference">The reference to resolve.</param>
    /// <returns>The registered provider.</returns>
    /// <exception cref="T:Legate.ProviderNotRegisteredException">No provider registered under the reference's provider segment.</exception>
    abstract Resolve: reference: ModelReference -> ILlmProvider

    /// The ids of the registered providers.
    abstract RegisteredProviders: IReadOnlyList<string>
