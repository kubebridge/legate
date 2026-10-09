// SPDX-License-Identifier: Apache-2.0
module Dot.DotModels

open System
open System.Net.Http
open System.Net.Http.Headers
open System.Text.Json
open System.Threading
open Microsoft.Extensions.Configuration

/// Fetches selectable models, retaining defaults and the current selection
/// when discovery is offline. Credentials never leave the request headers.
let discoverAsync
    (configuration: IConfiguration)
    (providers: ReplEngine.ProviderOption list)
    (current: Legate.ModelReference)
    (ct: CancellationToken)
    =
    task {
        use http = new HttpClient()
        use timeout = CancellationTokenSource.CreateLinkedTokenSource(ct)
        timeout.CancelAfter(TimeSpan.FromSeconds 5.)
        let items = ResizeArray<DotPicker.PickerItem>()

        for provider in providers do
            let names = Collections.Generic.HashSet<string>(StringComparer.Ordinal)
            names.Add(provider.DefaultModel) |> ignore

            if current.Provider = provider.Id then
                names.Add(current.Model) |> ignore

            for model in configuration.GetSection($"Dot:Providers:{provider.Id}:Models").GetChildren() do
                match model.Value with
                | null -> ()
                | value when not (String.IsNullOrWhiteSpace value) -> names.Add(value.Trim()) |> ignore
                | _ -> ()

            if
                provider.Id <> "scripted"
                && provider.Id <> "google"
                && provider.Id <> "anthropic"
            then
                let endpoint =
                    match configuration[$"Legate:Llm:Providers:{provider.Id}:Endpoint"] with
                    | null when provider.Id = "ollamacloud" -> "https://ollama.com/v1"
                    | null -> "https://api.openai.com/v1"
                    | configured -> configured.TrimEnd('/')

                let key = DotConfig.getProviderKey configuration provider.Id

                if not (String.IsNullOrWhiteSpace key) then
                    try
                        use request = new HttpRequestMessage(HttpMethod.Get, endpoint + "/models")
                        request.Headers.Authorization <- AuthenticationHeaderValue("Bearer", key)
                        use! response = http.SendAsync(request, timeout.Token)
                        response.EnsureSuccessStatusCode() |> ignore
                        use! stream = response.Content.ReadAsStreamAsync(timeout.Token)
                        use! document = JsonDocument.ParseAsync(stream, cancellationToken = timeout.Token)
                        let mutable data = Unchecked.defaultof<JsonElement>

                        if
                            document.RootElement.TryGetProperty("data", &data)
                            && data.ValueKind = JsonValueKind.Array
                        then
                            for entry in data.EnumerateArray() do
                                let mutable id = Unchecked.defaultof<JsonElement>

                                if entry.TryGetProperty("id", &id) && id.ValueKind = JsonValueKind.String then
                                    match id.GetString() with
                                    | null -> ()
                                    | value -> names.Add(value) |> ignore
                    with
                    | :? OperationCanceledException when ct.IsCancellationRequested -> ct.ThrowIfCancellationRequested()
                    | _ -> ()

            for name in names |> Seq.filter (String.IsNullOrWhiteSpace >> not) |> Seq.sort do
                let reference =
                    if name.StartsWith(provider.Id + "/") then
                        name
                    else
                        provider.Id + "/" + name

                items.Add(
                    {
                        Key = reference
                        Label = name
                        Detail = if reference = current.Value then "current" else provider.Id
                    }
                )

        return List.ofSeq items
    }
