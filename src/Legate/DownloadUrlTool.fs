// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// The get_download_url built-in tool: presigned download URLs over the
// session's blob store. The tool binds the store, tenant, and session at
// construction and takes the blob name, scope, and expiry per call, so the
// model can only ever address blobs inside its own session scope. Every
// key is derived through BlobKeys (ForSession/ForArtifact) and that exact
// stored key is what gets presigned: presigning the caller-supplied name
// directly would recur the dangling-prefix bug this tool exists to fix.
// A null presign result becomes a ToolException, never a dangling URL.

/// Options for the <c>get_download_url</c> tool. A mutable options class,
/// mirroring <see cref="T:Legate.SessionOptions" />, so C# hosts configure
/// through object initialisers.
type DownloadUrlToolOptions() =

    /// How long presigned URLs stay valid, in minutes, when the caller
    /// passes no <c>expiryMinutes</c>. Must be finite and positive.
    member val DefaultExpiryMinutes: float = 60.0 with get, set

/// The tool's identity: its model-facing name and description, kept in
/// one internal module so the function type (compiled before the public
/// factory) and the factory serve the same literals.
module internal DownloadUrlIdentity =

    /// The tool's name as the model calls it.
    let name = "get_download_url"

    /// The tool's description as the model reads it: what the tool does,
    /// its arguments and defaults, and how it fails.
    let description =
        "Returns a presigned download URL for one blob the session's blob store holds. "
        + "Pass the blob's relative name in 'name' and its scope in 'scope' ('session' or 'artifact'; default 'artifact'); "
        + "'expiryMinutes' optionally sets how long the URL stays valid (default 60). "
        + "The URL signs the stored scope key, never a bare prefix. "
        + "The call fails when the blob is missing or the backend cannot presign."

/// Which blob scope holds the blob a <c>get_download_url</c> call names.
/// Internal: the model passes the scope as a string and the tool parses
/// it, so no F# type crosses the public boundary.
type internal DownloadUrlScope =
    | SessionScope
    | ArtifactScope

/// The outcome of reading the caller's <c>expiryMinutes</c> argument.
/// Internal: missing means the configured default, anything else is either
/// minutes or a caller error.
type internal ExpiryParse =
    | UseDefault
    | Minutes of float
    | InvalidExpiry

/// The tool's argument schema and shared parsing, kept in one internal
/// module so the schema tests assert on the same JSON the tool serves.
module internal DownloadUrlSchema =

    /// The JSON schema served on the tool's JsonSchema: name is required,
    /// scope is the closed session/artifact pair defaulting to artifact,
    /// expiryMinutes is a positive minute count defaulting to 60.
    let json =
        """{"type":"object","description":"Arguments for the get_download_url tool.","properties":{"name":{"type":"string","description":"The blob's relative name under the session, for example 'turn-03-summary.md'."},"scope":{"type":"string","enum":["session","artifact"],"default":"artifact","description":"Which blob scope holds the blob: session blobs or artifact outputs. Defaults to 'artifact'."},"expiryMinutes":{"type":"number","default":60,"description":"How long the presigned URL stays valid, in minutes. Must be positive. Defaults to 60."}},"required":["name"],"additionalProperties":false}"""

    /// The single parsed schema document backing every tool instance; the
    /// JsonSchema elements borrow it, so it lives as long as the process.
    let document = JsonDocument.Parse json

    /// Reads one argument value as text: plain strings and JSON string
    /// elements; anything else is not text.
    /// <param name="value">The raw argument value.</param>
    /// <returns>The text, or null when the value is not text.</returns>
    let asText (value: obj | null) : string | null =
        match value with
        | null -> null
        | :? string as text -> text
        | :? JsonElement as element when element.ValueKind = JsonValueKind.String -> element.GetString()
        | _ -> null

    /// Parses the scope argument: missing and null mean artifact outputs,
    /// the two scope words (any casing) select their scope, anything else
    /// is a caller error.
    /// <param name="value">The raw scope argument.</param>
    /// <returns>The scope, or null when the argument is not a scope word.</returns>
    let parseScope (value: obj | null) : DownloadUrlScope option =
        match asText value with
        | null when isNull (box value) -> Some ArtifactScope
        | null -> None
        | text when String.Equals(text, "session", StringComparison.OrdinalIgnoreCase) -> Some SessionScope
        | text when String.Equals(text, "artifact", StringComparison.OrdinalIgnoreCase) -> Some ArtifactScope
        | _ -> None

    /// Reads the expiryMinutes argument: missing and null mean the
    /// configured default; JSON and CLR numbers are minutes; anything else
    /// is a caller error.
    /// <param name="value">The raw expiry argument.</param>
    /// <returns>The parse outcome.</returns>
    let parseExpiry (value: obj | null) : ExpiryParse =
        if isNull (box value) then
            UseDefault
        else
            match value with
            | :? double as minutes -> Minutes minutes
            | :? float32 as minutes -> Minutes(float minutes)
            | :? int as minutes -> Minutes(float minutes)
            | :? int64 as minutes -> Minutes(float minutes)
            | :? decimal as minutes -> Minutes(float minutes)
            | :? JsonElement as element when element.ValueKind = JsonValueKind.Number -> Minutes(element.GetDouble())
            | _ -> InvalidExpiry

    /// Turns validated minutes into a span: finite, positive, and within
    /// the representable range; anything else is a caller error.
    /// <param name="minutes">The minute count to convert.</param>
    /// <returns>The span, or null when the count is not usable.</returns>
    let toExpiry (minutes: float) : TimeSpan option =
        if Double.IsNaN minutes || Double.IsInfinity minutes then
            None
        elif minutes <= 0.0 || minutes > TimeSpan.MaxValue.TotalMinutes then
            None
        else
            Some(TimeSpan.FromMinutes minutes)

/// The <c>get_download_url</c> function served to the model. Internal: hosts
/// hold the <see cref="T:Microsoft.Extensions.AI.AIFunction" /> that
/// <see cref="T:Legate.DownloadUrlTool" /> hands back, never this type.
[<Sealed>]
type internal DownloadUrlFunction(store: IBlobStore, tenant: TenantId, sessionId: SessionId, defaultExpiry: TimeSpan) =
    inherit AIFunction()

    /// Reads one raw argument by name; missing reads as null.
    let argument (args: AIFunctionArguments) (name: string) : obj | null =
        let mutable value: obj | null = null

        if args.TryGetValue(name, &value) then value else null

    /// This tool's name for error text.
    override _.Name = DownloadUrlIdentity.name

    /// This tool's description for the model.
    override _.Description = DownloadUrlIdentity.description

    /// This tool's argument schema.
    override _.JsonSchema = DownloadUrlSchema.document.RootElement

    /// Presigns the stored scope key for the call's blob: derives the key
    /// through <see cref="T:Legate.BlobKeys" />, requires the blob to
    /// exist, and presigns exactly that key. A missing blob raises
    /// <see cref="T:System.IO.FileNotFoundException" /> (the
    /// <see cref="M:Legate.IBlobStore.OpenRead*" /> precedent); a backend
    /// that cannot presign raises <see cref="T:Legate.ToolException" />,
    /// never a dangling URL. Cancellation propagates as-is.
    override _.InvokeCoreAsync
        (args: AIFunctionArguments, cancellationToken: CancellationToken)
        : ValueTask<obj | null> =
        ValueTask<obj | null>(
            task {
                cancellationToken.ThrowIfCancellationRequested()

                let args = if isNull (box args) then AIFunctionArguments() else args

                let name =
                    match Option.ofObj (DownloadUrlSchema.asText (argument args "name")) with
                    | Some valid when not (String.IsNullOrWhiteSpace valid) -> valid
                    | _ ->
                        raise (
                            ToolException(
                                DownloadUrlIdentity.name,
                                "The get_download_url tool needs a blob name: pass the blob's relative name in 'name'."
                            )
                        )

                let scope =
                    match DownloadUrlSchema.parseScope (argument args "scope") with
                    | Some scope -> scope
                    | None ->
                        raise (
                            ToolException(
                                DownloadUrlIdentity.name,
                                "The get_download_url scope must be 'session' or 'artifact'."
                            )
                        )

                let expiry =
                    match DownloadUrlSchema.parseExpiry (argument args "expiryMinutes") with
                    | UseDefault -> defaultExpiry
                    | Minutes minutes ->
                        match DownloadUrlSchema.toExpiry minutes with
                        | Some expiry -> expiry
                        | None ->
                            raise (
                                ToolException(
                                    DownloadUrlIdentity.name,
                                    "The get_download_url expiry must be a positive number of minutes."
                                )
                            )
                    | InvalidExpiry ->
                        raise (
                            ToolException(
                                DownloadUrlIdentity.name,
                                "The get_download_url expiry must be a positive number of minutes."
                            )
                        )

                // The stored key, derived through BlobKeys so the name is
                // validated and scoped: an invalid name raises
                // InvalidBlobKeyException here, before any store call.
                let key =
                    match scope with
                    | SessionScope -> BlobKeys.ForSession(tenant, sessionId, name)
                    | ArtifactScope -> BlobKeys.ForArtifact(tenant, sessionId, name)

                let! metadata = store.GetMetadata(key, cancellationToken)

                match metadata with
                | null ->
                    let scopeWord =
                        match scope with
                        | SessionScope -> "session"
                        | ArtifactScope -> "artifact"

                    raise (
                        System.IO.FileNotFoundException(
                            sprintf "No %s blob exists under name '%s'." scopeWord name,
                            key
                        )
                    )
                | _ -> ()

                let! presigned = store.TryGetPresignedUrl(key, expiry, cancellationToken)

                let url =
                    match presigned with
                    | null ->
                        raise (
                            ToolException(
                                DownloadUrlIdentity.name,
                                "The blob store cannot presign download URLs: the backend returned no URL for this blob."
                            )
                        )
                    | valid -> valid

                return box (url.ToString())
            }
        )

/// Builds the <c>get_download_url</c> built-in tool: a presigned download
/// URL for one blob the session's blob store holds. The store, tenant, and
/// session bind at construction, so the model can only address blobs inside
/// its own session scope; each call names the blob and optionally its scope
/// and expiry. The URL always signs the stored scope key the blob was
/// written under, never a bare prefix.
type DownloadUrlTool private () =

    /// The tool's name as the model calls it. Validated against
    /// <see cref="T:Legate.ToolNameRules" /> when the tool is built.
    static member ToolName: string = DownloadUrlIdentity.name

    /// The tool's description as the model reads it: what the tool does,
    /// its arguments and defaults, and how it fails.
    static member Description: string = DownloadUrlIdentity.description

    /// Validates the configured default expiry: finite and positive
    /// minutes, the same rule a per-call expiry must satisfy.
    static member private RequireOptions(options: DownloadUrlToolOptions | null) : TimeSpan =
        let minutes =
            match options with
            | null -> 60.0
            | configured -> configured.DefaultExpiryMinutes

        match DownloadUrlSchema.toExpiry minutes with
        | Some expiry -> expiry
        | None ->
            raise (
                ArgumentOutOfRangeException(
                    nameof options,
                    "The default download URL expiry must be a positive number of minutes."
                )
            )

    /// Builds the tool with default options (presigned URLs valid for 60
    /// minutes unless the caller passes <c>expiryMinutes</c>).
    /// <param name="store">The blob store holding the session's blobs. Must not be null.</param>
    /// <param name="tenant">The tenant that owns the session.</param>
    /// <param name="sessionId">The session whose blobs the tool may address.</param>
    /// <returns>The tool to offer to the model.</returns>
    static member Create(store: IBlobStore, tenant: TenantId, sessionId: SessionId) : AIFunction =
        DownloadUrlTool.Create(store, tenant, sessionId, null)

    /// Builds the tool with explicit options.
    /// <param name="store">The blob store holding the session's blobs. Must not be null.</param>
    /// <param name="tenant">The tenant that owns the session.</param>
    /// <param name="sessionId">The session whose blobs the tool may address.</param>
    /// <param name="options">The tool options, or null for the defaults.</param>
    /// <returns>The tool to offer to the model.</returns>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The configured default expiry is not a positive number of minutes.</exception>
    static member Create
        (store: IBlobStore, tenant: TenantId, sessionId: SessionId, options: DownloadUrlToolOptions | null)
        : AIFunction =
        ArgumentNullException.ThrowIfNull(store)
        ToolNameRules.Validate(DownloadUrlTool.ToolName) |> ignore

        let defaultExpiry = DownloadUrlTool.RequireOptions options

        DownloadUrlFunction(store, tenant, sessionId, defaultExpiry) :> AIFunction
