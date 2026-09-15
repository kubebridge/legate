// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.IO
open System.Net
open System.Net.Http
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks

// Call-time SSRF guard (issue 75). The custom-tool invoker (issue 74)
// calls SsrfGuard.checkAsync before each POST: scheme check first
// (http/https only, no resolution on reject), then one resolution through
// the injected IHostAddressResolver, then deny/allow classification over
// every returned address, then socket-layer pinning. The request reuses the
// pinned address through a ConnectCallback that never resolves again, so a
// post-check DNS change cannot rebind the connection; the original host
// stays on the request URI, which is what preserves the Host header and
// the TLS SNI. Denials and resolution failures are Error results with a
// bounded string mapping for issue 74, never throws and never logs
// addresses: only the host travels far enough for diagnostics, and only in
// the private connector path, never in the error strings.

/// Why the SSRF guard denied an endpoint. Internal: issue 74 maps the case
/// to its bounded error string through
/// <see cref="M:Legate.SsrfDenyReason.ToBoundedString" />. Carries no
/// addresses, hosts, or secrets, so the string is safe to surface.
type internal SsrfDenyReason =
    /// The endpoint scheme is not http or https.
    | SchemeNotAllowed
    /// The endpoint host is on the configured deny list.
    | HostDenied
    /// At least one resolved address is reserved.
    | AddressDenied
    /// The host could not be resolved to a usable address list.
    | ResolutionFailed

    /// Maps the denial to the bounded error string issue 74 surfaces.
    /// Stable per case, free of addresses, hosts, and secrets.
    /// <returns>The bounded error string for this denial.</returns>
    member this.ToBoundedString() : string =
        match this with
        | SchemeNotAllowed ->
            "Error: the tool endpoint scheme is not allowed: only http and https endpoints may be called."
        | HostDenied -> "Error: the tool endpoint host is denied by configuration."
        | AddressDenied -> "Error: the tool endpoint resolves to a denied address."
        | ResolutionFailed -> "Error: the tool endpoint host could not be resolved."

/// Connects the pinned address for one guarded request. Receives the pinned
/// address, its port, and the original host (for SNI diagnostics only) and
/// returns the connected stream. Tests inject a stub; production opens a
/// TCP socket to the pinned address.
type internal PinnedConnector = IPAddress -> int -> string -> CancellationToken -> Task<Stream>

/// The endpoint the guard approved: the original URI plus the resolved
/// addresses with the one the request must connect to. Internal: issue 74
/// holds this across its POST and builds the handler from it, never
/// re-resolving.
type internal PinnedEndpoint =
    {
        /// The endpoint URI exactly as the tool configured it: scheme, host,
        /// and port the request sends to, preserving Host and SNI.
        OriginalUri: Uri
        /// The endpoint host the guard checked, exactly as the URI carries it.
        Host: string
        /// The endpoint port the guard checked: the explicit port or the
        /// scheme default.
        Port: int
        /// Every address the resolver returned for the host, in order.
        Addresses: IPAddress[]
        /// The address the request must connect to: the first checked
        /// address, pinned before any second resolution could rebind it.
        PinnedAddress: IPAddress
    }

    /// Opens a TCP socket to the pinned address. Never resolves: the only
    /// DNS the guard performs is the single checked resolution.
    /// <param name="address">The pinned address to connect to.</param>
    /// <param name="port">The endpoint port to connect to.</param>
    /// <param name="_host">The original host, unused by the socket path: TLS SNI comes from the request URI.</param>
    /// <param name="cancellationToken">Token that abandons the connect.</param>
    /// <returns>The connected stream.</returns>
    static member private DefaultConnector
        (address: IPAddress)
        (port: int)
        (_host: string)
        (cancellationToken: CancellationToken)
        : Task<Stream> =
        task {
            ArgumentNullException.ThrowIfNull(address)
            let socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp)

            try
                do! socket.ConnectAsync(IPEndPoint(address, port), cancellationToken).AsTask()
                return new NetworkStream(socket, true) :> Stream
            with
            | :? OperationCanceledException as canceled ->
                socket.Dispose()
                return! Task.FromException<Stream>(canceled)
            | _ ->
                socket.Dispose()
                return! Task.FromException<Stream>(SocketException(int SocketError.HostUnreachable))
        }

    /// Builds the message handler for the guarded request: a socket handler
    /// whose connect callback dials the pinned address while the request URI
    /// keeps the original host, preserving the Host header and TLS SNI. The
    /// callback captures the pinned result and never resolves again.
    /// <param name="connector">The connector that dials the pinned address.</param>
    /// <returns>The handler issue 74 sends the POST through.</returns>
    member this.CreateHandler(connector: PinnedConnector) : SocketsHttpHandler =
        let pinned = this
        let handler = new SocketsHttpHandler()

        handler.ConnectCallback <-
            Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>>(fun _ cancellationToken ->
                ValueTask<Stream>(connector pinned.PinnedAddress pinned.Port pinned.Host cancellationToken))

        handler

    /// Builds the message handler with the production socket connector.
    /// <returns>The handler issue 74 sends the POST through.</returns>
    member this.CreateDefaultHandler() : SocketsHttpHandler =
        this.CreateHandler(PinnedEndpoint.DefaultConnector)

/// The guard. Internal: every check runs through
/// <see cref="M:Legate.SsrfGuard.checkAsync" />.
module internal SsrfGuard =

    /// One denied network plus its prefix length in bits: the single
    /// reserved-address table both classification and its tests read.
    /// IPv4 entries carry 4 bytes, IPv6 entries 16 bytes.
    let private deniedPrefixes: (byte[] * int)[] =
        [|
            // IPv4 software scope, private use, shared CGNAT, loopback,
            // link-local, and the 172.16/12 private block.
            ([| 0uy; 0uy; 0uy; 0uy |], 8)
            ([| 10uy; 0uy; 0uy; 0uy |], 8)
            ([| 100uy; 64uy; 0uy; 0uy |], 10)
            ([| 127uy; 0uy; 0uy; 0uy |], 8)
            ([| 169uy; 254uy; 0uy; 0uy |], 16)
            ([| 172uy; 16uy; 0uy; 0uy |], 12)
            // IPv4 IETF assignments, documentation, private use, benchmark,
            // documentation, multicast, and reserved/broadcast.
            ([| 192uy; 0uy; 0uy; 0uy |], 24)
            ([| 192uy; 0uy; 2uy; 0uy |], 24)
            ([| 192uy; 168uy; 0uy; 0uy |], 16)
            ([| 198uy; 18uy; 0uy; 0uy |], 15)
            ([| 198uy; 51uy; 100uy; 0uy |], 24)
            ([| 203uy; 0uy; 113uy; 0uy |], 24)
            ([| 224uy; 0uy; 0uy; 0uy |], 4)
            ([| 240uy; 0uy; 0uy; 0uy |], 4)
            // IPv6 unspecified, loopback, discard, documentation, Teredo,
            // 6to4, unique-local, link-local, deprecated site-local, and
            // multicast. IPv4-mapped addresses never reach this table: they
            // normalise to their embedded IPv4 first.
            (Array.zeroCreate<byte> 16, 128)
            ([|
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                1uy
             |],
             128)
            ([|
                1uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
             |],
             64)
            ([|
                0x20uy
                0x01uy
                0x0duy
                0xb8uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
             |],
             32)
            ([|
                0x20uy
                0x01uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
             |],
             32)
            ([|
                0x20uy
                0x02uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
             |],
             16)
            ([|
                0xfcuy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
             |],
             7)
            ([|
                0xfeuy
                0x80uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
             |],
             10)
            ([|
                0xfeuy
                0xc0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
             |],
             10)
            ([|
                0xffuy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
                0uy
             |],
             8)
        |]

    /// Tests one address against one table entry: the first
    /// <c>bits / 8</c> bytes compare whole, the remaining bits compare
    /// under a mask.
    /// <param name="address">The address bytes under test.</param>
    /// <param name="network">The denied network bytes.</param>
    /// <param name="bits">The denied prefix length in bits.</param>
    /// <returns>True when the address falls inside the denied prefix.</returns>
    let private matchesPrefix (address: byte[]) (network: byte[]) (bits: int) : bool =
        let fullBytes = bits / 8
        let restBits = bits % 8

        if fullBytes > address.Length || fullBytes > network.Length then
            false
        else
            let mutable whole = true
            let mutable index = 0

            while whole && index < fullBytes do
                if address[index] <> network[index] then
                    whole <- false

                index <- index + 1

            if not whole then
                false
            elif restBits = 0 then
                true
            elif fullBytes >= address.Length || fullBytes >= network.Length then
                false
            else
                let mask = byte (0xFF <<< (8 - restBits) &&& 0xFF)
                (address[fullBytes] &&& mask) = (network[fullBytes] &&& mask)

    /// NAT64 well-known prefix marker: 64:ff9b::/96 carries the destination
    /// IPv4 in its last four bytes, so the guard classifies the embedded
    /// address instead of trusting the carrier.
    /// <param name="bytes">The 16 address bytes.</param>
    /// <returns>True when the address is a NAT64 well-known-prefix address.</returns>
    let private isNat64 (bytes: byte[]) : bool =
        bytes.Length = 16
        && bytes[0] = 0uy
        && bytes[1] = 0x64uy
        && bytes[2] = 0xFFuy
        && bytes[3] = 0x9Buy
        && bytes[4] = 0uy
        && bytes[5] = 0uy
        && bytes[6] = 0uy
        && bytes[7] = 0uy
        && bytes[8] = 0uy
        && bytes[9] = 0uy
        && bytes[10] = 0uy
        && bytes[11] = 0uy

    /// Classifies one address against the single reserved table: loopback,
    /// private, link-local, multicast, documentation, and the other denied
    /// prefixes for both families. IPv4-mapped IPv6 normalises to its
    /// embedded IPv4; NAT64 addresses classify their embedded IPv4; unknown
    /// families deny closed-world.
    /// <param name="address">The address to classify.</param>
    /// <returns>True when the address is denied.</returns>
    let rec isDeniedAddress (address: IPAddress) : bool =
        if isNull (box address) then
            true
        else
            let normalized =
                if address.IsIPv4MappedToIPv6 then
                    address.MapToIPv4()
                else
                    address

            let bytes = normalized.GetAddressBytes()

            if bytes.Length = 4 then
                deniedPrefixes
                |> Array.exists (fun (network, bits) -> network.Length = 4 && matchesPrefix bytes network bits)
            elif bytes.Length = 16 then
                if isNat64 bytes then
                    let embedded = IPAddress(bytes[12..15])
                    isDeniedAddress embedded
                else
                    deniedPrefixes
                    |> Array.exists (fun (network, bits) -> network.Length = 16 && matchesPrefix bytes network bits)
            else
                true

    /// Reads one options list defensively: null reads as empty, null and
    /// blank entries drop out.
    /// <param name="entries">The configured entries, possibly null.</param>
    /// <returns>The usable entries.</returns>
    let private usableEntries (entries: List<string>) : string list =
        if isNull (box entries) then
            []
        else
            entries
            |> Seq.filter (fun entry -> not (String.IsNullOrWhiteSpace entry))
            |> List.ofSeq

    /// Matches a host against one entry: an exact ordinal case-insensitive
    /// hostname comparison. Address entries never match here; they match
    /// through <c>addressMatchesEntry</c>.
    /// <param name="host">The endpoint host.</param>
    /// <param name="entry">The configured entry.</param>
    /// <returns>True when the entry names this host.</returns>
    let private hostMatchesEntry (host: string) (entry: string) : bool =
        String.Equals(host, entry, StringComparison.OrdinalIgnoreCase)

    /// Matches one resolved address against one entry: the entry must parse
    /// as a literal IP and equal the address.
    /// <param name="address">The resolved address.</param>
    /// <param name="entry">The configured entry.</param>
    /// <returns>True when the entry is this address.</returns>
    let private addressMatchesEntry (address: IPAddress) (entry: string) : bool =
        let mutable parsed = Unchecked.defaultof<IPAddress>

        IPAddress.TryParse(entry, &parsed)
        && not (isNull (box parsed))
        && parsed.Equals(address)

    /// Resolves once, at call time: literal IPs answer themselves without
    /// touching the resolver; hostnames resolve through the seam. Resolver
    /// failures, nulls, and empties read as None downstream, which the check
    /// turns into a bounded deny. Cancellation propagates.
    /// <param name="resolver">The injected resolver seam.</param>
    /// <param name="host">The endpoint host to resolve.</param>
    /// <param name="cancellationToken">Token that abandons the resolution.</param>
    /// <returns>Some addresses, or None when resolution failed.</returns>
    let private tryResolveAsync
        (resolver: IHostAddressResolver)
        (host: string)
        (cancellationToken: CancellationToken)
        : Task<IPAddress[] option> =
        task {
            try
                let mutable literal = Unchecked.defaultof<IPAddress>

                if IPAddress.TryParse(host, &literal) && not (isNull (box literal)) then
                    return Some [| literal |]
                else
                    let! resolved = resolver.ResolveAsync(host, cancellationToken)

                    if isNull (box resolved) then
                        return None
                    else
                        return
                            Some(
                                resolved
                                |> Seq.filter (fun address -> not (isNull (box address)))
                                |> Array.ofSeq
                            )
            with
            | :? OperationCanceledException as canceled -> return! Task.FromException<IPAddress[] option>(canceled)
            | _ -> return None
        }

    /// Checks one endpoint: scheme first with no resolution on reject, then
    /// the configured deny list by host with no resolution on match, then
    /// the single resolution, then deny by resolved address, then allow
    /// (host or resolved address) past the reserved deny, then the
    /// every-address reserved check. Returns the pinned endpoint or the
    /// bounded denial; never throws except for null arguments and
    /// cancellation, which are caller contract violations and aborts.
    /// <param name="resolver">The injected resolver seam.</param>
    /// <param name="options">The host-level allow/deny lists.</param>
    /// <param name="endpoint">The absolute endpoint URI to check.</param>
    /// <param name="cancellationToken">Token that abandons the check.</param>
    /// <returns>The pinned endpoint, or the bounded denial.</returns>
    let checkAsync
        (resolver: IHostAddressResolver)
        (options: SsrfGuardOptions)
        (endpoint: Uri)
        (cancellationToken: CancellationToken)
        : Task<Result<PinnedEndpoint, SsrfDenyReason>> =
        ArgumentNullException.ThrowIfNull(resolver)
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(endpoint)
        cancellationToken.ThrowIfCancellationRequested()

        task {
            if
                not (String.Equals(endpoint.Scheme, "http", StringComparison.OrdinalIgnoreCase))
                && not (String.Equals(endpoint.Scheme, "https", StringComparison.OrdinalIgnoreCase))
            then
                return Error SchemeNotAllowed
            else
                let host = endpoint.Host

                if String.IsNullOrWhiteSpace host then
                    return Error ResolutionFailed
                else
                    let denyEntries = usableEntries options.DenyList
                    let allowEntries = usableEntries options.AllowList

                    if denyEntries |> List.exists (hostMatchesEntry host) then
                        return Error HostDenied
                    else
                        let! resolved = tryResolveAsync resolver host cancellationToken

                        match resolved with
                        | None -> return Error ResolutionFailed
                        | Some addresses when addresses.Length = 0 -> return Error ResolutionFailed
                        | Some addresses ->
                            if
                                addresses
                                |> Array.exists (fun address ->
                                    denyEntries |> List.exists (addressMatchesEntry address))
                            then
                                return Error HostDenied
                            elif
                                (allowEntries |> List.exists (hostMatchesEntry host))
                                || (addresses
                                    |> Array.exists (fun address ->
                                        allowEntries |> List.exists (addressMatchesEntry address)))
                            then
                                return
                                    Ok
                                        {
                                            OriginalUri = endpoint
                                            Host = host
                                            Port = endpoint.Port
                                            Addresses = addresses
                                            PinnedAddress = addresses[0]
                                        }
                            elif addresses |> Array.exists isDeniedAddress then
                                return Error AddressDenied
                            else
                                return
                                    Ok
                                        {
                                            OriginalUri = endpoint
                                            Host = host
                                            Port = endpoint.Port
                                            Addresses = addresses
                                            PinnedAddress = addresses[0]
                                        }
        }

/// System-backed host resolver over <see cref="T:System.Net.Dns" />: the
/// production <see cref="T:Legate.IHostAddressResolver" /> the host
/// registers when it has no fake to inject.
type SystemHostAddressResolver() =

    interface IHostAddressResolver with
        member _.ResolveAsync(host, cancellationToken) =
            task {
                ArgumentException.ThrowIfNullOrWhiteSpace(host)
                let! addresses = Dns.GetHostAddressesAsync(host, cancellationToken)

                if isNull (box addresses) then
                    return [||] :> IReadOnlyList<IPAddress>
                else
                    return addresses :> IReadOnlyList<IPAddress>
            }
