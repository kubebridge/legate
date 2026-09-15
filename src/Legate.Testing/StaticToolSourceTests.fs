// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.AI
open Xunit

module StaticToolSourceTests =

    let private tool (name: string) : AITool =
        let method = Func<string>(fun () -> "out")

        AIFunctionFactory.Create(method, name, Unchecked.defaultof<string>, Unchecked.defaultof<JsonSerializerOptions>)
        :> AITool

    let private sourced (names: string list) : StaticToolSource =
        StaticToolSource(ResizeArray<AITool>(names |> List.map tool) :> IReadOnlyList<AITool>)

    let private context () : ToolSourceContext =
        {
            Tenant = TenantId.Create "test"
            AgentId = AgentId.New()
            SessionId = SessionId.New()
        }

    [<Fact>]
    let ``GetTools serves the fixed tools in order`` () : Task =
        task {
            let source = sourced [ "lookup"; "exec" ]

            let! tools = (source :> IToolSource).GetTools(context ())

            let names = tools |> Seq.map (fun tool -> tool.Name) |> List.ofSeq

            Assert.Equal<string list>([ "lookup"; "exec" ], names)
        }

    [<Fact>]
    let ``An empty source contributes nothing`` () : Task =
        task {
            let source = StaticToolSource(ResizeArray<AITool>() :> IReadOnlyList<AITool>)

            let! tools = (source :> IToolSource).GetTools(context ())

            Assert.Empty(tools)
            Assert.Empty(source.Tools)
        }

    [<Fact>]
    let ``Tools and AsDictionary project the source`` () =
        let source = sourced [ "lookup" ]

        Assert.Equal(1, source.Tools.Count)
        Assert.Equal("lookup", source.Tools[0].Name)
        Assert.True(source.AsDictionary().ContainsKey("lookup"))

    [<Fact>]
    let ``A null tool list is rejected`` () =
        Assert.Throws<ArgumentNullException>(fun () ->
            new StaticToolSource(Unchecked.defaultof<IReadOnlyList<AITool>>) |> ignore)
        |> ignore

    [<Fact>]
    let ``A null tool entry is rejected`` () =
        let tools =
            ResizeArray<AITool>([| Unchecked.defaultof<AITool> |]) :> IReadOnlyList<AITool>

        Assert.Throws<ArgumentException>(fun () -> new StaticToolSource(tools) |> ignore)
        |> ignore

    [<Fact>]
    let ``A non-conforming name is rejected`` () =
        let tools = ResizeArray<AITool>([| tool "has space" |]) :> IReadOnlyList<AITool>

        Assert.Throws<ArgumentException>(fun () -> new StaticToolSource(tools) |> ignore)
        |> ignore

    [<Fact>]
    let ``A duplicate name is rejected`` () =
        let tools =
            ResizeArray<AITool>([| tool "lookup"; tool "lookup" |]) :> IReadOnlyList<AITool>

        Assert.Throws<ArgumentException>(fun () -> new StaticToolSource(tools) |> ignore)
        |> ignore

    [<Fact>]
    let ``A null context is rejected`` () =
        let source = sourced [ "lookup" ]

        Assert.Throws<ArgumentNullException>(fun () ->
            (source :> IToolSource).GetTools(Unchecked.defaultof<ToolSourceContext>)
            |> ignore)
        |> ignore
