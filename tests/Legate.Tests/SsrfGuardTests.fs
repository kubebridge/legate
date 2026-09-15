// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Collections.Generic
open System.IO
open System.Net
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Xunit

module SsrfGuardTests =

    /// Canned resolver seam: scripted answers per host, scripted failures,
    /// and a call log. No DNS, no sockets.
    type FakeResolver() =
        let answers = Dictionary<string, IPAddress[]>(StringComparer.OrdinalIgnoreCase)
        let mutable failures = Set.empty<string>
        let mutable nullHosts = Set.empty<string>
        let calls = ResizeArray<string>()

        /// Scripts the addresses one host resolves to.
        member _.Set(host: string, addresses: IPAddress[]) = answers[host] <- addresses

        /// Scripts one host to raise, modelling a DNS failure.
        member _.Fail(host: string) = failures <- failures.Add host

        /// Scripts one host to answer null, modelling a broken seam.
        member _.ReturnNull(host: string) = nullHosts <- nullHosts.Add host

        /// Every host resolved so far, oldest first.
        member _.Calls: string list = calls |> List.ofSeq

        interface IHostAddressResolver with
            member _.ResolveAsync(host, _) =
                calls.Add(host)

                if failures.Contains host then
                    Task.FromException<IReadOnlyList<IPAddress>>(InvalidOperationException("boom"))
                elif nullHosts.Contains host then
                    Task.FromResult(Unchecked.defaultof<IReadOnlyList<IPAddress>>)
                else
                    match answers.TryGetValue host with
                    | true, found -> Task.FromResult(found :> IReadOnlyList<IPAddress>)
                    | false, _ -> Task.FromResult([||] :> IReadOnlyList<IPAddress>)

    let private address (value: string) = IPAddress.Parse value

    let private options () = SsrfGuardOptions()

    let private optionsWith (allow: string list) (deny: string list) =
        let current = SsrfGuardOptions()
        current.AllowList.AddRange allow
        current.DenyList.AddRange deny
        current

    let private check (resolver: FakeResolver) (guardOptions: SsrfGuardOptions) (url: string) =
        SsrfGuard.checkAsync (resolver :> IHostAddressResolver) guardOptions (Uri url) CancellationToken.None
        |> fun call -> call.GetAwaiter().GetResult()

    let private deniedCase (result: Result<PinnedEndpoint, SsrfDenyReason>) =
        match result with
        | Ok _ -> failwith "Expected a denial but the guard approved the endpoint."
        | Error reason -> reason

    // ───────────────────────────────────────────────────────────────────
    // Scheme check precedes resolution

    [<Fact>]
    let ``ftp scheme is denied without resolving`` () =
        let resolver = FakeResolver()

        deniedCase (check resolver (options ()) "ftp://example.com/tool")
        |> should equal SchemeNotAllowed

        resolver.Calls.Length |> should equal 0

    [<Fact>]
    let ``file scheme is denied without resolving`` () =
        let resolver = FakeResolver()

        deniedCase (check resolver (options ()) "file:///etc/passwd")
        |> should equal SchemeNotAllowed

        resolver.Calls.Length |> should equal 0

    [<Fact>]
    let ``uppercase https scheme passes the scheme check`` () =
        let resolver = FakeResolver()
        resolver.Set("example.com", [| address "93.184.216.34" |])

        match check resolver (options ()) "HTTPS://example.com/tool" with
        | Ok pinned -> pinned.PinnedAddress |> should equal (address "93.184.216.34")
        | Error reason -> failwith $"Expected approval but got %A{reason}."

    // ───────────────────────────────────────────────────────────────────
    // IPv4 classification

    [<Fact>]
    let ``ipv4 loopback is denied`` () =
        let resolver = FakeResolver()

        deniedCase (check resolver (options ()) "http://127.0.0.1/tool")
        |> should equal AddressDenied

    [<Fact>]
    let ``ipv4 private ranges are denied`` () =
        for host in
            [
                "10.0.0.5"
                "172.16.0.9"
                "172.31.255.1"
                "192.168.1.1"
            ] do
            let resolver = FakeResolver()

            deniedCase (check resolver (options ()) $"http://%s{host}/tool")
            |> should equal AddressDenied

    [<Fact>]
    let ``ipv4 link-local is denied`` () =
        let resolver = FakeResolver()

        deniedCase (check resolver (options ()) "http://169.254.10.20/tool")
        |> should equal AddressDenied

    [<Fact>]
    let ``ipv4 reserved ranges are denied`` () =
        for host in
            [
                "0.0.0.0"
                "100.64.0.1"
                "192.0.2.1"
                "198.51.100.7"
                "203.0.113.9"
                "198.18.0.1"
                "224.0.0.1"
                "255.255.255.255"
            ] do
            let resolver = FakeResolver()

            deniedCase (check resolver (options ()) $"http://%s{host}/tool")
            |> should equal AddressDenied

    [<Fact>]
    let ``ipv4 public literal passes without resolving`` () =
        let resolver = FakeResolver()

        match check resolver (options ()) "http://8.8.8.8/tool" with
        | Ok pinned ->
            pinned.PinnedAddress |> should equal (address "8.8.8.8")
            pinned.Port |> should equal 80
        | Error reason -> failwith $"Expected approval but got %A{reason}."

        resolver.Calls.Length |> should equal 0

    [<Fact>]
    let ``resolved public ipv4 passes and pins the answer`` () =
        let resolver = FakeResolver()
        resolver.Set("example.com", [| address "93.184.216.34" |])

        match check resolver (options ()) "http://example.com/tool" with
        | Ok pinned ->
            pinned.Host |> should equal "example.com"
            pinned.Port |> should equal 80
            pinned.PinnedAddress |> should equal (address "93.184.216.34")
        | Error reason -> failwith $"Expected approval but got %A{reason}."

    [<Fact>]
    let ``explicit port is carried on the pinned endpoint`` () =
        let resolver = FakeResolver()
        resolver.Set("example.com", [| address "93.184.216.34" |])

        match check resolver (options ()) "https://example.com:8443/tool" with
        | Ok pinned -> pinned.Port |> should equal 8443
        | Error reason -> failwith $"Expected approval but got %A{reason}."

    // ───────────────────────────────────────────────────────────────────
    // IPv6 classification

    [<Fact>]
    let ``ipv6 loopback and unspecified are denied`` () =
        for host in [ "::1"; "::" ] do
            let resolver = FakeResolver()

            deniedCase (check resolver (options ()) $"http://[%s{host}]/tool")
            |> should equal AddressDenied

    [<Fact>]
    let ``ipv6 link-local unique-local multicast and documentation are denied`` () =
        for host in
            [
                "fe80::1"
                "fc00::1"
                "fd00::99"
                "ff02::1"
                "2001:db8::1"
                "100::1"
            ] do
            let resolver = FakeResolver()

            deniedCase (check resolver (options ()) $"http://[%s{host}]/tool")
            |> should equal AddressDenied

    [<Fact>]
    let ``ipv6 public address passes`` () =
        let resolver = FakeResolver()

        match check resolver (options ()) "https://[2001:4860:4860::8888]/tool" with
        | Ok pinned -> pinned.PinnedAddress |> should equal (address "2001:4860:4860::8888")
        | Error reason -> failwith $"Expected approval but got %A{reason}."

    [<Fact>]
    let ``ipv4-mapped ipv6 classifies its embedded address`` () =
        let resolver = FakeResolver()

        deniedCase (check resolver (options ()) "http://[::ffff:10.0.0.1]/tool")
        |> should equal AddressDenied

        match check resolver (options ()) "http://[::ffff:8.8.8.8]/tool" with
        | Ok pinned -> pinned.PinnedAddress |> should equal (address "::ffff:8.8.8.8")
        | Error reason -> failwith $"Expected approval but got %A{reason}."

    [<Fact>]
    let ``nat64 address with private embedded ipv4 is denied`` () =
        let resolver = FakeResolver()

        deniedCase (check resolver (options ()) "http://[64:ff9b::a00:1]/tool")
        |> should equal AddressDenied

    // ───────────────────────────────────────────────────────────────────
    // Every-address checks over mixed answers

    [<Fact>]
    let ``mixed answers deny when any address is reserved`` () =
        let resolver = FakeResolver()

        resolver.Set(
            "mixed.example",
            [|
                address "93.184.216.34"
                address "10.0.0.1"
            |]
        )

        deniedCase (check resolver (options ()) "http://mixed.example/tool")
        |> should equal AddressDenied

    [<Fact>]
    let ``mixed ipv4 and ipv6 answers deny when the ipv6 is reserved`` () =
        let resolver = FakeResolver()

        resolver.Set(
            "mixed.example",
            [|
                address "93.184.216.34"
                address "fe80::1"
            |]
        )

        deniedCase (check resolver (options ()) "http://mixed.example/tool")
        |> should equal AddressDenied

    [<Fact>]
    let ``all-public answers pass and pin the first address`` () =
        let resolver = FakeResolver()

        resolver.Set(
            "multi.example",
            [|
                address "93.184.216.34"
                address "1.1.1.1"
            |]
        )

        match check resolver (options ()) "http://multi.example/tool" with
        | Ok pinned -> pinned.PinnedAddress |> should equal (address "93.184.216.34")
        | Error reason -> failwith $"Expected approval but got %A{reason}."

    // ───────────────────────────────────────────────────────────────────
    // Resolution failure is a bounded deny, never a throw

    [<Fact>]
    let ``throwing resolver is a bounded deny`` () =
        let resolver = FakeResolver()
        resolver.Fail "down.example"

        deniedCase (check resolver (options ()) "http://down.example/tool")
        |> should equal ResolutionFailed

    [<Fact>]
    let ``empty answers are a bounded deny`` () =
        let resolver = FakeResolver()
        resolver.Set("empty.example", [||])

        deniedCase (check resolver (options ()) "http://empty.example/tool")
        |> should equal ResolutionFailed

    [<Fact>]
    let ``null answers are a bounded deny`` () =
        let resolver = FakeResolver()
        resolver.ReturnNull "null.example"

        deniedCase (check resolver (options ()) "http://null.example/tool")
        |> should equal ResolutionFailed

    [<Fact>]
    let ``cancelled check propagates cancellation instead of denying`` () =
        let resolver = FakeResolver()
        use source = new CancellationTokenSource()
        source.Cancel()

        (fun () ->
            SsrfGuard.checkAsync
                (resolver :> IHostAddressResolver)
                (options ())
                (Uri "http://example.com/tool")
                source.Token
            |> fun call -> call.GetAwaiter().GetResult() |> ignore)
        |> should throw typeof<OperationCanceledException>

    // ───────────────────────────────────────────────────────────────────
    // Allow and deny precedence

    [<Fact>]
    let ``allow-listed internal host passes`` () =
        let resolver = FakeResolver()
        resolver.Set("internal.example", [| address "10.1.2.3" |])

        match check resolver (optionsWith [ "internal.example" ] []) "http://internal.example/tool" with
        | Ok pinned -> pinned.PinnedAddress |> should equal (address "10.1.2.3")
        | Error reason -> failwith $"Expected approval but got %A{reason}."

    [<Fact>]
    let ``non-listed internal host does not pass`` () =
        let resolver = FakeResolver()
        resolver.Set("internal.example", [| address "10.1.2.3" |])

        deniedCase (check resolver (options ()) "http://internal.example/tool")
        |> should equal AddressDenied

    [<Fact>]
    let ``allow by literal ip passes without resolving`` () =
        let resolver = FakeResolver()

        match check resolver (optionsWith [ "10.9.9.9" ] []) "http://10.9.9.9/tool" with
        | Ok pinned -> pinned.PinnedAddress |> should equal (address "10.9.9.9")
        | Error reason -> failwith $"Expected approval but got %A{reason}."

        resolver.Calls.Length |> should equal 0

    [<Fact>]
    let ``deny-listed public host is denied`` () =
        let resolver = FakeResolver()
        resolver.Set("public.example", [| address "8.8.8.8" |])

        deniedCase (check resolver (optionsWith [] [ "public.example" ]) "http://public.example/tool")
        |> should equal HostDenied

    [<Fact>]
    let ``deny by resolved ip address wins over a public host`` () =
        let resolver = FakeResolver()
        resolver.Set("public.example", [| address "93.184.216.34" |])

        deniedCase (check resolver (optionsWith [] [ "93.184.216.34" ]) "http://public.example/tool")
        |> should equal HostDenied

    [<Fact>]
    let ``deny wins when a host is on both lists`` () =
        let resolver = FakeResolver()
        resolver.Set("both.example", [| address "8.8.8.8" |])

        deniedCase (check resolver (optionsWith [ "both.example" ] [ "both.example" ]) "http://both.example/tool")
        |> should equal HostDenied

    [<Fact>]
    let ``deny match is case-insensitive`` () =
        let resolver = FakeResolver()

        deniedCase (check resolver (optionsWith [] [ "Public.Example" ]) "http://PUBLIC.EXAMPLE/tool")
        |> should equal HostDenied

        resolver.Calls.Length |> should equal 0

    // ───────────────────────────────────────────────────────────────────
    // Pinning defeats rebinding while preserving host

    [<Fact>]
    let ``pinned handler dials the pinned address after answers change`` () =
        let resolver = FakeResolver()
        resolver.Set("flapping.example", [| address "93.184.216.34" |])

        let pinned =
            match check resolver (options ()) "https://flapping.example/tool" with
            | Ok approved -> approved
            | Error reason -> failwith $"Expected approval but got %A{reason}."

        // The attack: DNS now answers with loopback. A second check denies,
        // proving the window existed; the first pin must still dial safe.
        resolver.Set("flapping.example", [| address "127.0.0.1" |])

        deniedCase (check resolver (options ()) "https://flapping.example/tool")
        |> should equal AddressDenied

        let mutable dialed: (IPAddress * int * string) option = None

        let stub (dialAddress: IPAddress) (port: int) (host: string) (_: CancellationToken) =
            dialed <- Some(dialAddress, port, host)
            Task.FromResult(new MemoryStream() :> Stream)

        use handler = pinned.CreateHandler stub

        // The callback captures the pinned result and ignores the
        // runtime-supplied endpoint, so a null context exercises exactly
        // the production path: dial what was pinned, never resolve again.
        handler.ConnectCallback
            .Invoke(Unchecked.defaultof<SocketsHttpConnectionContext>, CancellationToken.None)
            .AsTask()
            .GetAwaiter()
            .GetResult()
        |> ignore

        match dialed with
        | Some(dialAddress, port, host) ->
            dialAddress |> should equal (address "93.184.216.34")
            port |> should equal 443
            host |> should equal "flapping.example"
        | None -> failwith "Expected the connect callback to dial the pinned address."

    [<Fact>]
    let ``pinned handler keeps the original host on the request`` () =
        let resolver = FakeResolver()
        resolver.Set("flapping.example", [| address "93.184.216.34" |])

        let pinned =
            match check resolver (options ()) "https://flapping.example/tool" with
            | Ok approved -> approved
            | Error reason -> failwith $"Expected approval but got %A{reason}."

        pinned.OriginalUri.Host |> should equal "flapping.example"

        let stub (_: IPAddress) (_: int) (_: string) (_: CancellationToken) =
            Task.FromResult(new MemoryStream() :> Stream)

        use handler = pinned.CreateHandler stub

        // The handler never rewrites the URI: the request the invoker sends
        // through it keeps the original host for the Host header and SNI,
        // while the socket underneath dials the pinned address (proven above).
        handler.ConnectCallback
            .Invoke(Unchecked.defaultof<SocketsHttpConnectionContext>, CancellationToken.None)
            .AsTask()
            .GetAwaiter()
            .GetResult()
        |> ignore

        pinned.OriginalUri.Host |> should equal "flapping.example"

    // ───────────────────────────────────────────────────────────────────
    // Bounded error strings and option validation

    [<Fact>]
    let ``deny reasons map to stable bounded strings`` () =
        SchemeNotAllowed.ToBoundedString()
        |> should equal "Error: the tool endpoint scheme is not allowed: only http and https endpoints may be called."

        HostDenied.ToBoundedString()
        |> should equal "Error: the tool endpoint host is denied by configuration."

        AddressDenied.ToBoundedString()
        |> should equal "Error: the tool endpoint resolves to a denied address."

        ResolutionFailed.ToBoundedString()
        |> should equal "Error: the tool endpoint host could not be resolved."

    [<Fact>]
    let ``options validate their lists`` () =
        options().Validate() |> should equal null

        let nullAllow = SsrfGuardOptions()
        nullAllow.AllowList <- Unchecked.defaultof<List<string>>
        nullAllow.Validate() |> should equal "AllowList must not be null."

        let nullDeny = SsrfGuardOptions()
        nullDeny.DenyList <- Unchecked.defaultof<List<string>>
        nullDeny.Validate() |> should equal "DenyList must not be null."

        let blank = optionsWith [ "" ] []

        blank.Validate()
        |> should equal "AllowList[0] must be a non-empty hostname or IP address."

        let cidr = optionsWith [] [ "10.0.0.0/8" ]

        cidr.Validate()
        |> should equal "DenyList[0] must be a hostname or literal IP address, not a CIDR range."

        let valid = optionsWith [ "internal.example"; "10.1.2.3" ] [ "blocked.example" ]
        valid.Validate() |> should equal null
