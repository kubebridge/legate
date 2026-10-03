// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Text

[<Struct; CustomEquality; NoComparison>]
type internal SessionAddress =
    val Tenant: TenantId
    val SessionId: SessionId

    new(tenant: TenantId, sessionId: SessionId) =
        if String.IsNullOrWhiteSpace(tenant.Value) then
            invalidArg (nameof tenant) "A session address requires an explicit tenant."

        let mutable parsed = Unchecked.defaultof<SessionId>

        if not (SessionId.TryParse(sessionId.Value, &parsed)) then
            invalidArg (nameof sessionId) "A session address requires a valid session id."

        { Tenant = tenant; SessionId = parsed }

    member this.Key =
        let encoded =
            UTF8Encoding(false, true).GetBytes(this.Tenant.Value) |> Convert.ToBase64String

        "s2."
        + encoded.TrimEnd('=').Replace('+', '-').Replace('/', '_')
        + "."
        + this.SessionId.Value

    member this.Equals(other: SessionAddress) =
        this.Tenant.Equals(other.Tenant) && this.SessionId.Equals(other.SessionId)

    override this.Equals(other: obj) =
        match other with
        | :? SessionAddress as address -> this.Equals(address)
        | _ -> false

    override this.GetHashCode() =
        HashCode.Combine(this.Tenant, this.SessionId)

    interface IEquatable<SessionAddress> with
        member this.Equals(other) = this.Equals(other)

    static member TryParse(key: string) : SessionAddress option =
        if String.IsNullOrWhiteSpace key then
            None
        else
            let parts = key.Split('.')

            if parts.Length <> 3 || parts[0] <> "s2" then
                None
            else
                try
                    let encoded = parts[1].Replace('-', '+').Replace('_', '/')
                    let padded = encoded.PadRight(encoded.Length + (4 - encoded.Length % 4) % 4, '=')
                    let tenant = UTF8Encoding(false, true).GetString(Convert.FromBase64String padded)
                    let address = SessionAddress(TenantId.Create tenant, SessionId.Parse parts[2])
                    if address.Key = key then Some address else None
                with
                | :? ArgumentException
                | :? FormatException
                | :? LegateIdentifierException -> None
