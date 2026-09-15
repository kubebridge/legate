// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Collections.Generic
open System.IO
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.AI
open Xunit

module DownloadUrlToolTests =

    /// A blob store that says whether the blob exists (through its
    /// configured metadata) and presigns through its configured URL,
    /// recording every presigned key and expiry. The in-memory backend
    /// cannot presign, so tests use this fake instead of a service.
    type FakePresigningStore() =
        let mutable metadata: BlobMetadata | null = null
        let mutable url: Uri | null = null
        let mutable presigned: (string * TimeSpan) list = []

        /// The metadata GetMetadata hands back, or null meaning the blob
        /// is missing.
        member _.Metadata
            with get () = metadata
            and set value = metadata <- value

        /// The URL TryGetPresignedUrl hands back, or null meaning the
        /// backend cannot presign.
        member _.Url
            with get () = url
            and set value = url <- value

        /// Every (key, expiry) pair presigned so far, oldest first.
        member _.Presigned: (string * TimeSpan) list = List.rev presigned

        interface IBlobStore with
            member _.Get(_, _) : Task<byte[] | null> = raise (NotSupportedException())
            member _.Put(_, _, _) : Task<BlobMetadata> = raise (NotSupportedException())

            member _.CompareExchange(_, _, _, _) : Task<BlobMetadata | null> = raise (NotSupportedException())

            member _.OpenRead(_, _) : Task<Stream> = raise (NotSupportedException())
            member _.OpenWrite(_, _, _) : Task<Stream> = raise (NotSupportedException())
            member _.List(_, _) : IAsyncEnumerable<string> = raise (NotSupportedException())
            member _.DeletePrefix(_, _) : Task<int> = raise (NotSupportedException())

            member _.GetMetadata(_, _) = Task.FromResult metadata

            member _.TryGetPresignedUrl(key, expiry, _) =
                presigned <- (key, expiry) :: presigned
                Task.FromResult url

    let private tenant = TenantId.Create "download-tests"

    let private sessionId = SessionId.New()

    let private storedUrl = Uri "https://blobs.example/download/abc"

    let private presentStore () =
        let store = FakePresigningStore()
        store.Metadata <- BlobMetadata("text/plain", 3L, "etag-1")
        store.Url <- storedUrl
        store

    let private toolFor (store: FakePresigningStore) : AIFunction =
        DownloadUrlTool.Create(store :> IBlobStore, tenant, sessionId)

    let private invoke (tool: AIFunction) (pairs: (string * obj | null) list) : string =
        let args = AIFunctionArguments(dict pairs)

        match tool.InvokeAsync(args, CancellationToken.None).AsTask().GetAwaiter().GetResult() with
        | :? string as text -> text
        | _ -> raise (Xunit.Sdk.XunitException "the tool returned no URL text")

    [<Fact>]
    let ``The tool presigns the stored artifact key, never the bare name`` () =
        let store = presentStore ()
        let name = "turn-03/result.txt"

        let result =
            invoke
                (toolFor store)
                [
                    "name", box name
                    "scope", box "artifact"
                ]

        Assert.Equal(storedUrl.ToString(), result)

        let (key, _) = Assert.Single(store.Presigned)
        Assert.Equal(BlobKeys.ForArtifact(tenant, sessionId, name), key)
        Assert.NotEqual<string>(name, key)

    [<Fact>]
    let ``The tool presigns the stored session key`` () =
        let store = presentStore ()
        let name = "transcript.json"

        let result =
            invoke
                (toolFor store)
                [
                    "name", box name
                    "scope", box "session"
                ]

        Assert.Equal(storedUrl.ToString(), result)

        let (key, _) = Assert.Single(store.Presigned)
        Assert.Equal(BlobKeys.ForSession(tenant, sessionId, name), key)

    [<Fact>]
    let ``Scope defaults to artifact when omitted`` () =
        let store = presentStore ()
        let name = "turn-03/result.txt"

        invoke (toolFor store) [ "name", box name ] |> ignore

        let (key, _) = Assert.Single(store.Presigned)
        Assert.Equal(BlobKeys.ForArtifact(tenant, sessionId, name), key)

    [<Fact>]
    let ``Scope matching ignores casing`` () =
        let store = presentStore ()

        invoke
            (toolFor store)
            [
                "name", box "transcript.json"
                "scope", box "Session"
            ]
        |> ignore

        let (key, _) = Assert.Single(store.Presigned)
        Assert.Equal(BlobKeys.ForSession(tenant, sessionId, "transcript.json"), key)

    [<Fact>]
    let ``The tool accepts JSON string arguments`` () =
        let store = presentStore ()

        let nameElement = JsonDocument.Parse("\"turn-03/result.txt\"").RootElement
        let scopeElement = JsonDocument.Parse("\"artifact\"").RootElement

        let result =
            invoke
                (toolFor store)
                [
                    "name", box nameElement
                    "scope", box scopeElement
                ]

        Assert.Equal(storedUrl.ToString(), result)

        let (key, _) = Assert.Single(store.Presigned)
        Assert.Equal(BlobKeys.ForArtifact(tenant, sessionId, "turn-03/result.txt"), key)

    [<Fact>]
    let ``Expiry defaults to 60 minutes`` () =
        let store = presentStore ()

        invoke (toolFor store) [ "name", box "turn-03/result.txt" ] |> ignore

        let (_, expiry) = Assert.Single(store.Presigned)
        Assert.Equal(TimeSpan.FromMinutes 60.0, expiry)

    [<Fact>]
    let ``Expiry honours caller minutes`` () =
        let store = presentStore ()

        invoke
            (toolFor store)
            [
                "name", box "turn-03/result.txt"
                "expiryMinutes", box 15.0
            ]
        |> ignore

        let (_, expiry) = Assert.Single(store.Presigned)
        Assert.Equal(TimeSpan.FromMinutes 15.0, expiry)

    [<Fact>]
    let ``A non-positive expiry raises ToolException`` () =
        let tool = toolFor (presentStore ())

        let error =
            Assert.Throws<ToolException>(fun () ->
                invoke
                    tool
                    [
                        "name", box "turn-03/result.txt"
                        "expiryMinutes", box 0.0
                    ]
                |> ignore)

        Assert.Contains("expiry", error.Message)

    [<Fact>]
    let ``A non-numeric expiry raises ToolException`` () =
        let tool = toolFor (presentStore ())

        let error =
            Assert.Throws<ToolException>(fun () ->
                invoke
                    tool
                    [
                        "name", box "turn-03/result.txt"
                        "expiryMinutes", box "soon"
                    ]
                |> ignore)

        Assert.Contains("expiry", error.Message)

    [<Fact>]
    let ``A backend that cannot presign raises ToolException, never a URL`` () =
        let store = presentStore ()
        store.Url <- null
        let tool = toolFor store

        let error =
            Assert.Throws<ToolException>(fun () -> invoke tool [ "name", box "turn-03/result.txt" ] |> ignore)

        Assert.Equal(DownloadUrlTool.ToolName, error.ToolName)
        Assert.Contains("presign", error.Message)

    [<Fact>]
    let ``A missing blob raises FileNotFoundException`` () =
        let store = FakePresigningStore()
        store.Url <- storedUrl
        let tool = toolFor store

        Assert.Throws<System.IO.FileNotFoundException>(fun () -> invoke tool [ "name", box "absent.txt" ] |> ignore)
        |> ignore

    [<Fact>]
    let ``A missing name raises ToolException`` () =
        let tool = toolFor (presentStore ())

        let error = Assert.Throws<ToolException>(fun () -> invoke tool [] |> ignore)

        Assert.Contains("name", error.Message)

    [<Fact>]
    let ``An invalid name raises InvalidBlobKeyException`` () =
        let tool = toolFor (presentStore ())

        Assert.Throws<InvalidBlobKeyException>(fun () -> invoke tool [ "name", box "../escape" ] |> ignore)
        |> ignore

    [<Fact>]
    let ``An unknown scope raises ToolException`` () =
        let tool = toolFor (presentStore ())

        let error =
            Assert.Throws<ToolException>(fun () ->
                invoke
                    tool
                    [
                        "name", box "turn-03/result.txt"
                        "scope", box "vault"
                    ]
                |> ignore)

        Assert.Contains("scope", error.Message)

    [<Fact>]
    let ``The tool carries its name description and schema`` () =
        let tool = toolFor (presentStore ())

        Assert.Equal("get_download_url", tool.Name)
        Assert.Equal(DownloadUrlTool.ToolName, tool.Name)
        Assert.False(String.IsNullOrWhiteSpace tool.Description)
        Assert.Contains("presign", tool.Description)

        let schema = tool.JsonSchema
        Assert.Equal(JsonValueKind.Object, schema.ValueKind)

        let required =
            schema.GetProperty("required").EnumerateArray()
            |> Seq.map (fun element -> element.GetString() |> Option.ofObj)
            |> Seq.choose id
            |> Seq.toList

        Assert.Contains("name", required)

        let properties = schema.GetProperty("properties")

        let hasProperty (name: string) =
            let mutable element = Unchecked.defaultof<JsonElement>
            properties.TryGetProperty(name, &element)

        Assert.True(hasProperty "name")
        Assert.True(hasProperty "scope")
        Assert.True(hasProperty "expiryMinutes")

        let scopes =
            properties.GetProperty("scope").GetProperty("enum").EnumerateArray()
            |> Seq.map (fun element -> element.GetString() |> Option.ofObj)
            |> Seq.choose id
            |> Seq.toList

        Assert.Contains("session", scopes)
        Assert.Contains("artifact", scopes)

        let defaultExpiry =
            properties.GetProperty("expiryMinutes").GetProperty("default").GetDouble()

        Assert.Equal(60.0, defaultExpiry)

    [<Fact>]
    let ``Create rejects a null store`` () =
        // Unchecked.defaultof rather than a bare null literal: under
        // Nullable=enable the literal does not satisfy a non-nullable
        // parameter, but the runtime value is still null.
        let nullStore = Unchecked.defaultof<IBlobStore>

        Assert.Throws<ArgumentNullException>(fun () -> DownloadUrlTool.Create(nullStore, tenant, sessionId) |> ignore)
        |> ignore

    [<Fact>]
    let ``Create rejects a non-positive default expiry`` () =
        let options = DownloadUrlToolOptions()
        options.DefaultExpiryMinutes <- 0.0

        Assert.Throws<ArgumentOutOfRangeException>(fun () ->
            DownloadUrlTool.Create(presentStore () :> IBlobStore, tenant, sessionId, options)
            |> ignore)
        |> ignore

    [<Fact>]
    let ``Create honours an explicit default expiry`` () =
        let store = presentStore ()
        let options = DownloadUrlToolOptions()
        options.DefaultExpiryMinutes <- 30.0

        let tool = DownloadUrlTool.Create(store :> IBlobStore, tenant, sessionId, options)

        invoke tool [ "name", box "turn-03/result.txt" ] |> ignore

        let (_, expiry) = Assert.Single(store.Presigned)
        Assert.Equal(TimeSpan.FromMinutes 30.0, expiry)
