// SPDX-License-Identifier: Apache-2.0
module MinimalHost.Program

open Legate
open Legate.Storage.InMemory
open Microsoft.AspNetCore.Builder
open Microsoft.Extensions.DependencyInjection
open MinimalHost.HostWiring
open MinimalHost.Routes
open Giraffe

// MinimalHost entry point: builds the web container (InMemory stores,
// process workspace, scripted-by-default providers), resolves the session
// client before the actor system starts so the router wiring lands first,
// then serves the Giraffe routes. Keys come from the environment through
// configuration binding only; nothing is printed or persisted.

[<EntryPoint>]
let main (args: string[]) : int =
    let builder = WebApplication.CreateBuilder(args)
    let database = InMemoryDatabase()
    buildServices builder.Services builder.Configuration database

    let app = builder.Build()

    // Resolve before running: the resolve triggers the session router
    // wiring, which must land before the actor system spawns its router.
    app.Services.GetRequiredService<SessionClient>() |> ignore

    app.UseGiraffe(webApp) |> ignore
    app.Run()
    0
