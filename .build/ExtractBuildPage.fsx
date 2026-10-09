// SPDX-License-Identifier: Apache-2.0
open System
open System.IO
open System.Net
open System.Text.RegularExpressions

let args = fsi.CommandLineArgs |> Array.skip 1
let html = File.ReadAllText(args[0])
let snippets =
    Regex.Matches(html, "<code data-snippet=\"([^\"]+)\">(.*?)</code>", RegexOptions.Singleline)
    |> Seq.map (fun m -> m.Groups[1].Value, WebUtility.HtmlDecode(Regex.Replace(m.Groups[2].Value, "<[^>]+>", "")))
    |> Map.ofSeq
let code name = snippets |> Map.find name
let guide = File.ReadAllText(Path.Combine(Path.GetDirectoryName(Path.GetFullPath(args[0])), "..", "Docs", "getting-started-hosted.md"))
let documented = Regex.Match(guide, "```csharp\\s+(.*?)```", RegexOptions.Singleline).Groups[1].Value
let progression = ["imports"; "register"; "pipeline"; "handler"; "run"] |> List.map code |> String.concat "\n"
let normalized text = Regex.Replace(text, "\\s+", " ").Trim()
if normalized documented <> normalized progression then failwith "Hosted-guide progression differs from compiled Build-page fragments."
let deployment = Regex.Match(guide, "<!-- compile:deployment -->\\s*```csharp\\s+(.*?)```", RegexOptions.Singleline).Groups[1].Value
if String.IsNullOrWhiteSpace deployment then failwith "Missing documented distributed composition fixture."
let output =
    "// SPDX-License-Identifier: Apache-2.0\n// Generated from the delivered Build page, never a maintained approximation.\n"
    + code "imports"
    + "\nusing Microsoft.AspNetCore.Builder;\nusing Microsoft.AspNetCore.Http;\nusing Microsoft.Extensions.DependencyInjection;\nusing Legate.Storage;\n"
    + "namespace Legate.AspNetCore.Tests;\ninternal static class BuildPageFixture {\n"
    + "internal static WebApplication Build(string[] args, Action<WebApplicationBuilder> configure) {\n"
    + code "register" + "\nconfigure(builder);\n" + code "pipeline" + "\n" + code "handler" + "\nreturn app;\n}\n"
    + "internal static async Task Read(SessionClient client, Session session, CancellationToken cancellationToken) {\n" + code "events" + "\n}\n"
    + "internal static async Task<TurnResult> Wait(SessionClient client, Session session) {\n" + code "wait" + "\nreturn result;\n}\n"
    + "internal static async Task Controls(SessionClient client, Session session, HttpContext context) {\n" + code "controls" + "\n}\n"
    + "internal static async Task Run(WebApplication app) {\n" + code "run" + "\n}\n" + deployment + "\n}\n"
Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(args[1]))) |> ignore
File.WriteAllText(args[1], output, Text.UTF8Encoding(false))
printfn "Extracted %d Build-page C# fragments into %s" snippets.Count args[1]
