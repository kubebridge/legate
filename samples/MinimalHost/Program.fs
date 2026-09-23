// SPDX-License-Identifier: Apache-2.0
module MinimalHost.Program

open Legate
open Legate.Storage.InMemory
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Diagnostics.HealthChecks
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Diagnostics.HealthChecks
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

    // Readiness on the app port, filtered to the legate-cluster check:
    // the manifest probes this (not the Akka.Management port) for
    // cluster membership. Middleware (not MapHealthChecks: the terminal
    // Giraffe middleware handles every request itself, so an endpoint
    // registered with MapHealthChecks never runs) before Giraffe;
    // /healthz stays the static liveness probe.
    let readyOptions =
        HealthCheckOptions(Predicate = System.Func<_, _>(fun check -> check.Name = "legate-cluster"))

    app.UseHealthChecks("/ready", readyOptions) |> ignore

    app.UseGiraffe(webApp) |> ignore
    app.Run()
    0
