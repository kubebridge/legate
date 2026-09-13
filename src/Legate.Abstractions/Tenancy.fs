// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Text.Json
open System.Text.Json.Serialization

/// Scope under which sessions, agents, and stores are partitioned.
/// Legate never interprets the value; a single-tenant host uses <see cref="TenantId.Default"/>.
/// Serialises with System.Text.Json as a plain JSON string.
[<Struct; CustomEquality; NoComparison>]
[<JsonConverter(typeof<TenantIdJsonConverter>)>]
type TenantId private (value: string) =

    /// The tenant used when a host has no tenancy of its own.
    static member Default = TenantId "default"

    /// Creates a tenant id from a non-empty, trimmed value.
    static member Create(value: string) =
        if String.IsNullOrWhiteSpace value then
            raise (ArgumentException("A tenant id must be a non-empty string.", nameof value))

        TenantId(value.Trim())

    member _.Value = value

    member _.Equals(other: TenantId) =
        String.Equals(value, other.Value, StringComparison.Ordinal)

    override _.ToString() = value

    override _.GetHashCode() =
        StringComparer.Ordinal.GetHashCode value

    override this.Equals(other: obj) =
        match other with
        | :? TenantId as tenant -> this.Equals(tenant: TenantId)
        | _ -> false

    interface IEquatable<TenantId> with
        member this.Equals(other: TenantId) = this.Equals(other: TenantId)

/// JSON converter for <see cref="T:Legate.TenantId" />, kept internal and
/// concrete so the [<JsonConverter>] attribute, which needs a parameterless
/// constructor, can register it on the struct. Self-contained in Tenancy.fs
/// (declared with <c>and</c>) because the attribute's declaration point
/// cannot reach the shared helper in Identifiers.fs, which compiles later.
and internal TenantIdJsonConverter() =
    inherit JsonConverter<TenantId>()

    override _.Read(reader: byref<Utf8JsonReader>, _typeToConvert: Type, _options: JsonSerializerOptions) =
        if reader.TokenType <> JsonTokenType.String then
            raise (JsonException "A tenant id must be a non-null JSON string.")

        match reader.GetString() with
        | null -> raise (JsonException "A tenant id must be a non-null JSON string.")
        | raw ->
            // Create trims and rejects blank input, so deserialisation
            // canonicalises and any rejection surfaces as JsonException.
            try
                TenantId.Create raw
            with :? ArgumentException ->
                raise (JsonException(sprintf "'%s' is not a valid tenant id." raw))

    override _.Write(writer: Utf8JsonWriter, value: TenantId, _options: JsonSerializerOptions) =
        writer.WriteStringValue(value.ToString())
