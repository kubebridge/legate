// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Runtime.InteropServices
open System.Text.Json
open System.Text.Json.Serialization

/// Base type for exceptions raised by the Legate runtime and its contracts,
/// so hosts can catch Legate failures without matching concrete library types.
type LegateException(message: string) =
    inherit Exception(message)

/// Raised when a string is not a valid canonical Legate identifier.
type LegateIdentifierException(message: string) =
    inherit LegateException(message)

/// Shared string plumbing for the per-identifier JSON converters. Kept
/// internal and concrete per identifier so [<JsonConverter>], which needs a
/// parameterless constructor, can register one on each struct.
[<AbstractClass>]
type internal IdentifierJsonConverter<'T when 'T: not null>() =
    inherit JsonConverter<'T>()

    /// Parses an identifier string, returning false when the input is invalid.
    abstract TryGet: value: string -> bool * 'T

    override this.Read(reader: byref<Utf8JsonReader>, _typeToConvert: Type, _options: JsonSerializerOptions) =
        if reader.TokenType <> JsonTokenType.String then
            raise (JsonException "A Legate identifier must be a non-null JSON string.")

        match reader.GetString() with
        | null -> raise (JsonException "A Legate identifier must be a non-null JSON string.")
        | raw ->
            match this.TryGet raw with
            | true, parsed -> parsed
            | false, _ -> raise (JsonException(sprintf "'%s' is not a valid Legate identifier." raw))

    override _.Write(writer: Utf8JsonWriter, value: 'T, _options: JsonSerializerOptions) =
        writer.WriteStringValue(value.ToString())

/// A durable identifier for an agent session.
[<Struct; CustomEquality; NoComparison>]
[<JsonConverter(typeof<SessionIdJsonConverter>)>]
type SessionId private (value: string) =

    /// Creates a fresh session id from the current time.
    static member New() = SessionId(Ulid.NewUlid().ToString())

    /// Parses a session id; lowercase input is canonicalised to uppercase.
    /// Throws LegateIdentifierException on invalid input and
    /// ArgumentNullException on null.
    static member Parse(value: string) =
        if isNull (box value) then
            raise (ArgumentNullException(nameof value))

        let mutable parsed = Unchecked.defaultof<SessionId>

        if SessionId.TryParse(value, &parsed) then
            parsed
        else
            raise (LegateIdentifierException(sprintf "'%s' is not a valid session id." value))

    /// Attempts to parse a session id; returns false for null, blank, or
    /// otherwise invalid input.
    static member TryParse(value: string, [<Out>] result: byref<SessionId>) : bool =
        if String.IsNullOrWhiteSpace value then
            result <- Unchecked.defaultof<SessionId>
            false
        else
            let trimmed = value.Trim()
            let mutable ulid = Unchecked.defaultof<Ulid>

            // Ulid.TryParse accepts out-of-alphabet characters (they map to an
            // invalid marker without error), so the input is only accepted when
            // it round-trips to its own canonical uppercase form.
            if
                Ulid.TryParse(trimmed, &ulid)
                && String.Equals(trimmed, ulid.ToString(), StringComparison.OrdinalIgnoreCase)
            then
                result <- SessionId(ulid.ToString())
                true
            else
                result <- Unchecked.defaultof<SessionId>
                false

    /// The canonical uppercase 26-character Crockford ULID string.
    member _.Value = value

    /// Compares two session ids ordinally.
    member _.Equals(other: SessionId) =
        String.Equals(value, other.Value, StringComparison.Ordinal)

    /// Returns the canonical identifier string.
    override _.ToString() = value

    /// Hashes the canonical identifier string ordinally.
    override _.GetHashCode() =
        StringComparer.Ordinal.GetHashCode value

    /// Compares against a boxed session id without recursing.
    override this.Equals(other: obj) =
        match other with
        | :? SessionId as session -> this.Equals(session: SessionId)
        | _ -> false

    /// Implements ordinal equality for the generic collection surface.
    interface IEquatable<SessionId> with
        member this.Equals(other: SessionId) = this.Equals(other: SessionId)

and internal SessionIdJsonConverter() =
    inherit IdentifierJsonConverter<SessionId>()

    override _.TryGet(value: string) =
        let mutable parsed = Unchecked.defaultof<SessionId>
        let ok = SessionId.TryParse(value, &parsed)
        ok, parsed

/// A durable identifier for one turn of an agent session.
[<Struct; CustomEquality; NoComparison>]
[<JsonConverter(typeof<TurnIdJsonConverter>)>]
type TurnId private (value: string) =

    /// Creates a fresh turn id from the current time.
    static member New() = TurnId(Ulid.NewUlid().ToString())

    /// Parses a turn id; lowercase input is canonicalised to uppercase.
    /// Throws LegateIdentifierException on invalid input and
    /// ArgumentNullException on null.
    static member Parse(value: string) =
        if isNull (box value) then
            raise (ArgumentNullException(nameof value))

        let mutable parsed = Unchecked.defaultof<TurnId>

        if TurnId.TryParse(value, &parsed) then
            parsed
        else
            raise (LegateIdentifierException(sprintf "'%s' is not a valid turn id." value))

    /// Attempts to parse a turn id; returns false for null, blank, or
    /// otherwise invalid input.
    static member TryParse(value: string, [<Out>] result: byref<TurnId>) : bool =
        if String.IsNullOrWhiteSpace value then
            result <- Unchecked.defaultof<TurnId>
            false
        else
            let trimmed = value.Trim()
            let mutable ulid = Unchecked.defaultof<Ulid>

            // Ulid.TryParse accepts out-of-alphabet characters (they map to an
            // invalid marker without error), so the input is only accepted when
            // it round-trips to its own canonical uppercase form.
            if
                Ulid.TryParse(trimmed, &ulid)
                && String.Equals(trimmed, ulid.ToString(), StringComparison.OrdinalIgnoreCase)
            then
                result <- TurnId(ulid.ToString())
                true
            else
                result <- Unchecked.defaultof<TurnId>
                false

    /// The canonical uppercase 26-character Crockford ULID string.
    member _.Value = value

    /// Compares two turn ids ordinally.
    member _.Equals(other: TurnId) =
        String.Equals(value, other.Value, StringComparison.Ordinal)

    /// Returns the canonical identifier string.
    override _.ToString() = value

    /// Hashes the canonical identifier string ordinally.
    override _.GetHashCode() =
        StringComparer.Ordinal.GetHashCode value

    /// Compares against a boxed turn id without recursing.
    override this.Equals(other: obj) =
        match other with
        | :? TurnId as turn -> this.Equals(turn: TurnId)
        | _ -> false

    /// Implements ordinal equality for the generic collection surface.
    interface IEquatable<TurnId> with
        member this.Equals(other: TurnId) = this.Equals(other: TurnId)

and internal TurnIdJsonConverter() =
    inherit IdentifierJsonConverter<TurnId>()

    override _.TryGet(value: string) =
        let mutable parsed = Unchecked.defaultof<TurnId>
        let ok = TurnId.TryParse(value, &parsed)
        ok, parsed

/// A durable identifier for an agent registered with the runtime.
[<Struct; CustomEquality; NoComparison>]
[<JsonConverter(typeof<AgentIdJsonConverter>)>]
type AgentId private (value: string) =

    /// Creates a fresh agent id from the current time.
    static member New() = AgentId(Ulid.NewUlid().ToString())

    /// Parses an agent id; lowercase input is canonicalised to uppercase.
    /// Throws LegateIdentifierException on invalid input and
    /// ArgumentNullException on null.
    static member Parse(value: string) =
        if isNull (box value) then
            raise (ArgumentNullException(nameof value))

        let mutable parsed = Unchecked.defaultof<AgentId>

        if AgentId.TryParse(value, &parsed) then
            parsed
        else
            raise (LegateIdentifierException(sprintf "'%s' is not a valid agent id." value))

    /// Attempts to parse an agent id; returns false for null, blank, or
    /// otherwise invalid input.
    static member TryParse(value: string, [<Out>] result: byref<AgentId>) : bool =
        if String.IsNullOrWhiteSpace value then
            result <- Unchecked.defaultof<AgentId>
            false
        else
            let trimmed = value.Trim()
            let mutable ulid = Unchecked.defaultof<Ulid>

            // Ulid.TryParse accepts out-of-alphabet characters (they map to an
            // invalid marker without error), so the input is only accepted when
            // it round-trips to its own canonical uppercase form.
            if
                Ulid.TryParse(trimmed, &ulid)
                && String.Equals(trimmed, ulid.ToString(), StringComparison.OrdinalIgnoreCase)
            then
                result <- AgentId(ulid.ToString())
                true
            else
                result <- Unchecked.defaultof<AgentId>
                false

    /// The canonical uppercase 26-character Crockford ULID string.
    member _.Value = value

    /// Compares two agent ids ordinally.
    member _.Equals(other: AgentId) =
        String.Equals(value, other.Value, StringComparison.Ordinal)

    /// Returns the canonical identifier string.
    override _.ToString() = value

    /// Hashes the canonical identifier string ordinally.
    override _.GetHashCode() =
        StringComparer.Ordinal.GetHashCode value

    /// Compares against a boxed agent id without recursing.
    override this.Equals(other: obj) =
        match other with
        | :? AgentId as agent -> this.Equals(agent: AgentId)
        | _ -> false

    /// Implements ordinal equality for the generic collection surface.
    interface IEquatable<AgentId> with
        member this.Equals(other: AgentId) = this.Equals(other: AgentId)

and internal AgentIdJsonConverter() =
    inherit IdentifierJsonConverter<AgentId>()

    override _.TryGet(value: string) =
        let mutable parsed = Unchecked.defaultof<AgentId>
        let ok = AgentId.TryParse(value, &parsed)
        ok, parsed

/// A durable identifier for one transcript cell of a session. The session
/// store stamps it when it persists the cell, mirroring how the event
/// sequence is stamped on append; before that stamp the id is empty.
[<Struct; CustomEquality; NoComparison>]
[<JsonConverter(typeof<CellIdJsonConverter>)>]
type CellId private (value: string) =

    /// Creates a fresh cell id from the current time.
    static member New() = CellId(Ulid.NewUlid().ToString())

    /// Parses a cell id; lowercase input is canonicalised to uppercase.
    /// Throws LegateIdentifierException on invalid input and
    /// ArgumentNullException on null.
    static member Parse(value: string) =
        if isNull (box value) then
            raise (ArgumentNullException(nameof value))

        let mutable parsed = Unchecked.defaultof<CellId>

        if CellId.TryParse(value, &parsed) then
            parsed
        else
            raise (LegateIdentifierException(sprintf "'%s' is not a valid cell id." value))

    /// Attempts to parse a cell id; returns false for null, blank, or
    /// otherwise invalid input.
    static member TryParse(value: string, [<Out>] result: byref<CellId>) : bool =
        if String.IsNullOrWhiteSpace value then
            result <- Unchecked.defaultof<CellId>
            false
        else
            let trimmed = value.Trim()
            let mutable ulid = Unchecked.defaultof<Ulid>

            // Ulid.TryParse accepts out-of-alphabet characters (they map to an
            // invalid marker without error), so the input is only accepted when
            // it round-trips to its own canonical uppercase form.
            if
                Ulid.TryParse(trimmed, &ulid)
                && String.Equals(trimmed, ulid.ToString(), StringComparison.OrdinalIgnoreCase)
            then
                result <- CellId(ulid.ToString())
                true
            else
                result <- Unchecked.defaultof<CellId>
                false

    /// The canonical uppercase 26-character Crockford ULID string, or null
    /// while the id is still unstamped.
    member _.Value = value

    /// Compares two cell ids ordinally.
    member _.Equals(other: CellId) =
        String.Equals(value, other.Value, StringComparison.Ordinal)

    /// Returns the canonical identifier string, or null for the unstamped
    /// default id.
    override _.ToString() = value

    /// Hashes the canonical identifier string ordinally. An unstamped id
    /// hashes to 0 instead of throwing.
    override _.GetHashCode() =
        if isNull (box value) then
            0
        else
            StringComparer.Ordinal.GetHashCode value

    /// Compares against a boxed cell id without recursing.
    override this.Equals(other: obj) =
        match other with
        | :? CellId as cell -> this.Equals(cell: CellId)
        | _ -> false

    /// Implements ordinal equality for the generic collection surface.
    interface IEquatable<CellId> with
        member this.Equals(other: CellId) = this.Equals(other: CellId)

and internal CellIdJsonConverter() =
    inherit JsonConverter<CellId>()

    // Unlike the other identifiers, JSON null is the unstamped id: the
    // store stamps the cell id on persist, so a derived cell before that
    // stamp carries the default id and serialises as null, mirroring how
    // an in-flight event's empty Sequence serialises.
    override _.Read(reader: byref<Utf8JsonReader>, _typeToConvert: Type, _options: JsonSerializerOptions) =
        if reader.TokenType = JsonTokenType.Null then
            Unchecked.defaultof<CellId>
        elif reader.TokenType <> JsonTokenType.String then
            raise (JsonException "A cell id must be a JSON string or null.")
        else
            match reader.GetString() with
            | null -> raise (JsonException "A cell id must be a JSON string or null.")
            | raw ->
                let mutable parsed = Unchecked.defaultof<CellId>

                if CellId.TryParse(raw, &parsed) then
                    parsed
                else
                    raise (JsonException(sprintf "'%s' is not a valid cell id." raw))

    override _.Write(writer: Utf8JsonWriter, value: CellId, _options: JsonSerializerOptions) =
        // The unstamped id's Value is null; boxing avoids passing the
        // null through a non-nullable string binding.
        if box value.Value |> isNull then
            writer.WriteNullValue()
        else
            writer.WriteStringValue(value.ToString())
