// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.AI

// In-memory IToolSource: serves a fixed tool set to every session. The
// contract's naming rule is enforced at construction (every tool name must
// satisfy ToolNameRules, unique within the source), so a misconfigured
// source fails fast instead of failing the turn it first serves.

/// An in-memory <see cref="T:Legate.IToolSource" /> serving a fixed tool
/// set to every session context: the same tools come back no matter which
/// tenant, agent, or session asks. Construction validates the contract's
/// naming rule once (per the IToolSource contract), so a bad name or a
/// duplicate fails here, never mid-turn.
/// <param name="tools">The tools the source contributes. Must not be null and must hold no null entries; may be empty (the source then contributes nothing).</param>
[<Sealed>]
type StaticToolSource(tools: IReadOnlyList<AITool>) =

    do ArgumentNullException.ThrowIfNull(tools)

    let snapshot = Array.ofSeq tools

    do
        let names = HashSet<string>(StringComparer.Ordinal)

        snapshot
        |> Array.iteri (fun index tool ->
            if isNull (box tool) then
                raise (ArgumentException($"The tool list must hold no null tools (index {index}).", nameof tools))

            if isNull (box tool.Name) then
                raise (ArgumentException($"Every tool needs a name (index {index}).", nameof tools))

            ToolNameRules.Validate(tool.Name) |> ignore

            if not (names.Add(tool.Name)) then
                raise (
                    ArgumentException(
                        $"The tool name '{tool.Name}' appears twice: names are unique within a source.",
                        nameof tools
                    )
                ))

    /// The tools the source contributes, in construction order.
    member _.Tools: IReadOnlyList<AITool> =
        ResizeArray<AITool>(snapshot) :> IReadOnlyList<AITool>

    /// The tools keyed by name for the turn loop's resolved map.
    /// <returns>The tools by ordinal name.</returns>
    member _.AsDictionary() : IReadOnlyDictionary<string, AITool> =
        let table = Dictionary<string, AITool>(StringComparer.Ordinal)

        for tool in snapshot do
            table[tool.Name] <- tool

        table :> IReadOnlyDictionary<string, AITool>

    interface IToolSource with
        member _.GetTools(context: ToolSourceContext) =
            if isNull (box context) then
                raise (ArgumentNullException(nameof context))

            Task.FromResult(ResizeArray<AITool>(snapshot) :> IReadOnlyList<AITool>)
