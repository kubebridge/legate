// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System

/// Scope under which sessions, agents, and stores are partitioned.
/// Legate never interprets the value; a single-tenant host uses <see cref="TenantId.Default"/>.
[<Struct; CustomEquality; NoComparison>]
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
