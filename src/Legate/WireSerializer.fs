// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text
open System.Text.Json
open System.Text.Json.Serialization
open Akka.Actor
open Akka.Event
open Akka.Serialization

// The versioned envelope over DTOs only (issue 130): a
// SerializerWithStringManifest subclass whose wire bytes are always a DTO
// from WireDtos, never a raw DU. ToBinary maps the live message through
// WireDtos.toWire and refuses oversized payloads after serialising;
// FromBinary refuses unknown manifests, newer versions, and oversized
// payloads before deserialising, then maps the DTO back through
// WireDtos.ofWire. Every refusal records legate.serialization.rejected
// with its reason tag and logs a warning, then throws so Akka drops the
// message: fail-closed throughout. The reader accepts the current and the
// current-minus-one manifest version and refuses anything newer or older;
// older-than-current records failed.
module internal WireSerialization =

    /// The Akka serializer identifier. Unique to this serializer; outside
    /// the range Akka reserves for its built-ins.
    [<Literal>]
    let SerializerIdentifier = 1310130

    /// The HOCON id the serializers and bindings sections share.
    [<Literal>]
    let SerializerId = "legate-wire"

    /// The config path carrying the global wire maximum in bytes.
    [<Literal>]
    let MaxBytesConfigPath = "legate.wire.max-payload-bytes"

    /// STJ converter for one wire DTO type.
    /// <typeparam name="T">The DTO type.</typeparam>
    type WireDtoConverter<'T>() =
        inherit JsonConverter<'T>()

        let flags =
            Reflection.BindingFlags.Public
            ||| Reflection.BindingFlags.NonPublic
            ||| Reflection.BindingFlags.Instance

        let properties: Reflection.PropertyInfo[] =
            typeof<'T>.GetProperties(flags)
            |> Array.filter (fun property ->
                property.CanRead
                && property.CanWrite
                && property.GetIndexParameters().Length = 0
                && not (isNull (box (property.GetGetMethod(true))))
                && not (isNull (box (property.GetSetMethod(true)))))

        let byName =
            let table = Dictionary<string, Reflection.PropertyInfo>(StringComparer.Ordinal)

            for property in properties do
                table[property.Name] <- property

            table

        override _.Read(reader: byref<Utf8JsonReader>, typeToConvert: Type, options: JsonSerializerOptions) : 'T =
            if reader.TokenType <> JsonTokenType.StartObject then
                raise (JsonException($"A wire DTO must be a JSON object, not '{reader.TokenType}'."))

            let instance = Activator.CreateInstance(typeToConvert, true)
            let mutable finished = false

            while not finished do
                if not (reader.Read()) then
                    raise (JsonException("The wire DTO JSON ended inside its object."))

                match reader.TokenType with
                | JsonTokenType.EndObject -> finished <- true
                | JsonTokenType.PropertyName ->
                    match reader.GetString() with
                    | null ->
                        if not (reader.Read()) then
                            raise (JsonException("The wire DTO JSON ended inside a property value."))

                        reader.Skip()
                    | name ->
                        if not (reader.Read()) then
                            raise (JsonException("The wire DTO JSON ended inside a property value."))

                        let mutable property: Reflection.PropertyInfo =
                            Unchecked.defaultof<Reflection.PropertyInfo>

                        if byName.TryGetValue(name, &property) then
                            property.SetValue(
                                instance,
                                JsonSerializer.Deserialize(&reader, property.PropertyType, options)
                            )
                        else
                            reader.Skip()
                | token -> raise (JsonException($"Unexpected JSON token '{token}' inside a wire DTO."))

            unbox<'T> instance

        override _.Write(writer: Utf8JsonWriter, value: 'T, options: JsonSerializerOptions) : unit =
            ArgumentNullException.ThrowIfNull(writer)

            writer.WriteStartObject()

            for property in properties do
                writer.WritePropertyName(property.Name)
                JsonSerializer.Serialize(writer, property.GetValue(box value), property.PropertyType, options)

            writer.WriteEndObject()

    /// STJ converter factory for the WireDtos namespace: every DTO class in
    /// it round-trips through its read/write properties (of any IL
    /// visibility) and its parameterless constructor. Domain types keep
    /// their default contracts; unknown JSON fields are ignored so minor
    /// payload changes cross versions silently.
    type WireDtoConverterFactory() =
        inherit JsonConverterFactory()

        /// Whether STJ routes the type through the DTO converter: DTO
        /// namespace classes with a parameterless constructor.
        /// <param name="typeToConvert">The candidate type.</param>
        /// <returns>True for wire DTOs.</returns>
        override _.CanConvert(typeToConvert: Type) : bool =
            if isNull (box typeToConvert) then
                false
            else
                match box typeToConvert.FullName with
                | null -> false
                | name ->
                    (name :?> string).StartsWith("Legate.WireDtos+", StringComparison.Ordinal)
                    && not (
                        isNull (
                            box (
                                typeToConvert.GetConstructor(
                                    Reflection.BindingFlags.Public
                                    ||| Reflection.BindingFlags.NonPublic
                                    ||| Reflection.BindingFlags.Instance,
                                    null,
                                    Type.EmptyTypes,
                                    null
                                )
                            )
                        )
                    )

        /// Builds the DTO converter for the type.
        /// <param name="typeToConvert">The DTO type.</param>
        /// <param name="options">The wire options (unused).</param>
        /// <returns>The DTO converter.</returns>
        override _.CreateConverter(typeToConvert: Type, _options: JsonSerializerOptions) : JsonConverter =
            let converterType = typedefof<WireDtoConverter<_>>.MakeGenericType(typeToConvert)

            match Activator.CreateInstance(converterType) with
            | :? JsonConverter as converter -> converter
            | _ -> raise (InvalidOperationException("The wire DTO converter failed to construct."))

    /// The wire JSON settings: the plain STJ defaults the whole durable
    /// layer already speaks (so the $type discriminators match), plus the
    /// DTO converter above. DTOs stay internal, and F# compiles internal
    /// members to assembly-only IL which STJ's default contract skips, so
    /// the converter reads and writes them through reflection instead.
    /// <returns>The shared wire options.</returns>
    let wireOptions () : JsonSerializerOptions =
        let options = JsonSerializerOptions()
        options.Converters.Add(WireDtoConverterFactory() :> JsonConverter)
        options

    /// Every live type the envelope binds: the actor, router, and entity
    /// protocol messages plus the session records that answer Asks. Raw DUs
    /// bind here so Akka routes them to this serializer; the bytes on the
    /// wire are still always DTOs.
    let boundTypes: Type list =
        [
            typeof<SessionActorMessage>
            typeof<SessionActor.SuspendableActorMessage>
            typeof<SessionPromptReply>
            typeof<SessionCompactReply>
            typeof<SessionSnapshot>
            typeof<Session>
            typeof<SessionRouterMessage>
            typeof<SessionActor.SessionReplyReply>
            typeof<SessionActor.SessionSetAgentReply>
        ]

    /// Renders one bound type as its HOCON binding key: FullName,
    /// AssemblyName, so nested types keep their + separator.
    /// <param name="boundType">The bound type. Must not be null.</param>
    /// <returns>The HOCON binding key.</returns>
    let private bindingKeyOf (boundType: Type) : string =
        ArgumentNullException.ThrowIfNull(boundType)
        $"{boundType.FullName}, {boundType.Assembly.GetName().Name}"

    /// Builds the HOCON wiring the cluster actor system inlines: the global
    /// wire maximum under legate.wire plus the akka.actor serializers and
    /// serialization-bindings sections for the DTO wire set.
    /// <param name="maxWirePayloadBytes">The global wire maximum in bytes. Must be at least 1.</param>
    /// <returns>The HOCON fragment.</returns>
    let hoconFragment (maxWirePayloadBytes: int) : string =
        if maxWirePayloadBytes < 1 then
            raise (ArgumentOutOfRangeException(nameof maxWirePayloadBytes, "The wire maximum must be at least 1."))

        let bindings =
            boundTypes
            |> List.map (fun boundType -> $"    \"{bindingKeyOf boundType}\" = {SerializerId}")
            |> String.concat "\n"

        // The serializer class name is a literal on purpose: the class is
        // declared below, and the wire test pins the literal to
        // typeof<WireSerializer> so drift fails fast.
        let serializerClass = "Legate.WireSerializer, Legate"

        $"""legate.wire {{
  max-payload-bytes = {maxWirePayloadBytes}
}}
akka.actor {{
  serializers {{
    {SerializerId} = "{serializerClass}"
  }}
  serialization-bindings {{
{bindings}
  }}
}}"""

    /// Reads the global wire maximum from the actor system config, falling
    /// back to the manifest default when the HOCON carries no value.
    /// <param name="system">The actor system. Must not be null.</param>
    /// <returns>The global wire maximum in bytes.</returns>
    let maxBytesOf (system: ExtendedActorSystem) : int =
        ArgumentNullException.ThrowIfNull(system)

        try
            let config = system.Settings.Config

            if config.HasPath(MaxBytesConfigPath) then
                max 1 (config.GetInt(MaxBytesConfigPath))
            else
                WireManifests.DefaultMaxWirePayloadBytes
        with _ ->
            WireManifests.DefaultMaxWirePayloadBytes

/// The versioned-envelope serializer: DTOs only on the wire, fail-closed
/// on every refusal. Internal so no Akka type ever crosses the public API;
/// Akka instantiates it from the HOCON wiring WireSerialization builds.
type internal WireSerializer(system: ExtendedActorSystem) =
    inherit SerializerWithStringManifest(system)

    let maxBytes = WireSerialization.maxBytesOf system
    let options = WireSerialization.wireOptions ()

    /// Logs one refusal without ever throwing: telemetry stays silent and
    /// the throw below still fails the message closed.
    /// <param name="reason">The rejection reason tag.</param>
    /// <param name="manifest">The manifest that refused, or null.</param>
    /// <param name="detail">Why the payload refused.</param>
    member private _.logRefusal (reason: string) (manifest: string | null) (detail: string) : unit =
        try
            system.Log.Warning(
                "Legate wire envelope refused a payload: reason {0}, manifest {1}, detail {2}.",
                box reason,
                (if isNull (box manifest) then box "<none>" else box manifest),
                box detail
            )
        with _ ->
            ()

    /// Records the metric, logs the line, and raises the refusal the
    /// envelope fails closed with.
    /// <param name="reason">The rejection reason tag.</param>
    /// <param name="manifest">The manifest that refused, or null.</param>
    /// <param name="detail">Why the payload refused.</param>
    /// <returns>Never returns; always raises.</returns>
    member private this.refuse<'T> (reason: string) (manifest: string | null) (detail: string) : 'T =
        Telemetry.recordSerializationRejected reason
        this.logRefusal reason manifest detail
        raise (WireManifests.WireRejectedException(reason, manifest, detail))

    /// Resolves the wire case a live message maps onto, refusing closed
    /// when the message maps to no DTO.
    /// <param name="message">The live message.</param>
    /// <returns>The DTO and its wire case.</returns>
    member private this.caseForMessage(message: obj) : obj * WireManifests.WireCase =
        try
            let dto = WireDtos.toWire message

            match WireManifests.tryFindByDtoType (dto.GetType()) with
            | Some wireCase -> (dto, wireCase)
            | None ->
                this.refuse Telemetry.RejectionFailed null $"No wire case registers the DTO '{dto.GetType().FullName}'."
        with
        | :? WireManifests.WireRejectedException -> reraise ()
        | ex ->
            this.refuse
                Telemetry.RejectionFailed
                null
                $"The message '{message.GetType().FullName}' maps to no wire DTO: {ex.Message}."

    override _.Identifier: int = WireSerialization.SerializerIdentifier

    override this.Manifest(o: obj) : string =
        if isNull (box o) then
            this.refuse Telemetry.RejectionFailed null "The wire envelope never carries a null message."

        let _, wireCase = this.caseForMessage o
        WireManifests.manifestOf wireCase

    override this.ToBinary(o: obj) : byte[] =
        if isNull (box o) then
            this.refuse Telemetry.RejectionFailed null "The wire envelope never carries a null message."

        let dto, wireCase = this.caseForMessage o
        let manifest = WireManifests.manifestOf wireCase

        let bytes: byte[] =
            try
                JsonSerializer.SerializeToUtf8Bytes(dto, dto.GetType(), options)
            with ex ->
                this.refuse
                    Telemetry.RejectionFailed
                    manifest
                    $"The DTO '{dto.GetType().FullName}' failed to serialise: {ex.Message}."

        if bytes.Length > wireCase.MaxBytes || bytes.Length > maxBytes then
            this.refuse
                Telemetry.RejectionOversized
                manifest
                $"The payload is {bytes.Length} bytes, above the {min wireCase.MaxBytes maxBytes} byte bound."

        bytes

    override this.FromBinary(bytes: byte[], manifest: string) : obj =
        if isNull (box bytes) then
            this.refuse Telemetry.RejectionFailed manifest "The wire envelope never carries a null payload."

        match WireManifests.tryParseManifest manifest with
        | None ->
            let shown = if isNull (box manifest) then "<none>" else manifest

            this.refuse Telemetry.RejectionUnknownManifest manifest $"Unknown wire manifest '{shown}'."
        | Some(family, name, version) ->
            match WireManifests.tryFindCase family name with
            | None -> this.refuse Telemetry.RejectionUnknownManifest manifest $"Unknown wire manifest '{manifest}'."
            | Some wireCase ->
                if version > wireCase.Version then
                    this.refuse
                        Telemetry.RejectionNewerVersion
                        manifest
                        $"Wire manifest '{manifest}' is newer than the registered v{wireCase.Version}."
                elif
                    version <> wireCase.Version
                    && not (wireCase.Version > 1 && version = wireCase.Version - 1)
                then
                    this.refuse
                        Telemetry.RejectionFailed
                        manifest
                        $"Wire manifest '{manifest}' is older than the accepted v{wireCase.Version}."
                elif bytes.Length > wireCase.MaxBytes || bytes.Length > maxBytes then
                    this.refuse
                        Telemetry.RejectionOversized
                        manifest
                        $"The payload is {bytes.Length} bytes, above the {min wireCase.MaxBytes maxBytes} byte bound."
                else
                    let dto: obj =
                        try
                            match JsonSerializer.Deserialize(bytes, wireCase.DtoType, options) with
                            | null ->
                                this.refuse
                                    Telemetry.RejectionFailed
                                    manifest
                                    $"The DTO '{wireCase.DtoType.FullName}' deserialised to null."
                            | live -> live
                        with
                        | :? WireManifests.WireRejectedException -> reraise ()
                        | ex ->
                            this.refuse
                                Telemetry.RejectionFailed
                                manifest
                                $"The DTO '{wireCase.DtoType.FullName}' failed to deserialise: {ex.Message}."

                    try
                        WireDtos.ofWire dto
                    with
                    | :? WireManifests.WireRejectedException -> reraise ()
                    | ex ->
                        this.refuse
                            Telemetry.RejectionFailed
                            manifest
                            $"The DTO '{wireCase.DtoType.FullName}' maps to no live message: {ex.Message}."
