// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Net
open System.Threading
open System.Threading.Tasks

// SSRF guard contracts (issue 75). The guard itself lives in the Legate
// runtime; this file carries only what the host configures and injects:
// the host-level allow/deny lists and the address-resolver seam. Both use
// BCL types only so Abstractions keeps its inward-only references. Lists
// hold hostnames and literal IP addresses, never CIDR ranges: matching is
// an exact hostname comparison (ordinal, case-insensitive) or an exact
// parsed-address comparison, so there is no range parser to disagree on.

/// Host-level allow/deny lists for the SSRF guard. Bound through
/// <c>IOptions&lt;SsrfGuardOptions&gt;</c> by the host; the guard reads the
/// configured instance at call time. Entries are hostnames (for example
/// <c>internal.example</c>) or literal IP addresses (for example
/// <c>10.1.2.3</c> or <c>fd00::1</c>); CIDR ranges are rejected by
/// <see cref="M:Legate.SsrfGuardOptions.Validate" />. A deny entry wins over
/// an allow entry for the same host; an allow entry bypasses the
/// reserved-address deny for that host only. There are deliberately no
/// per-tool lists: a tool definition can never weaken host posture.
type SsrfGuardOptions() =

    /// Hostnames or literal IP addresses the guard lets through even when
    /// their resolved addresses are reserved. Empty means nothing is
    /// allowed past the reserved-address deny. Never null.
    member val AllowList: List<string> = List<string>() with get, set

    /// Hostnames or literal IP addresses the guard denies before any other
    /// check. Empty means nothing is denied by configuration. Never null.
    member val DenyList: List<string> = List<string>() with get, set

    /// Returns null when every list is usable, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let checkEntries (name: string) (entries: List<string>) : string | null =
            if isNull (box entries) then
                $"%s{name} must not be null."
            else
                let mutable violation: string | null = null
                let mutable index = 0

                while isNull (box violation) && index < entries.Count do
                    let entry = entries[index]

                    if String.IsNullOrWhiteSpace entry then
                        violation <- $"%s{name}[%d{index}] must be a non-empty hostname or IP address."
                    elif entry.Contains "/" then
                        violation <- $"%s{name}[%d{index}] must be a hostname or literal IP address, not a CIDR range."
                    elif entry.Contains " " then
                        violation <- $"%s{name}[%d{index}] must be a single hostname or IP address."

                    index <- index + 1

                violation

        let allowViolation = checkEntries "AllowList" this.AllowList

        if not (isNull (box allowViolation)) then
            allowViolation
        else
            checkEntries "DenyList" this.DenyList

/// Resolves an endpoint host to its addresses on behalf of the SSRF guard,
/// so tests inject canned answers and production resolves for real. The
/// guard calls this once per check at call time and pins the checked
/// result; the request path never resolves again.
type IHostAddressResolver =

    /// Resolves <paramref name="host" /> to its addresses.
    /// <param name="host">The DNS host to resolve, never null or blank.</param>
    /// <param name="cancellationToken">Token that abandons the resolution.</param>
    /// <returns>The host's addresses, never null: an empty list reads as a resolution failure downstream.</returns>
    abstract ResolveAsync: host: string * cancellationToken: CancellationToken -> Task<IReadOnlyList<IPAddress>>
