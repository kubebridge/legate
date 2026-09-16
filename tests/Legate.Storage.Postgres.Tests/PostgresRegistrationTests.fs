// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open System
open System.Collections.Generic
open Legate
open Legate.Storage
open Legate.Storage.Postgres
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options
open Xunit

// UsePostgres wiring: the Legate:Storage:Postgres section binds onto
// PostgresOptions and every store resolves from the container. No Docker
// is needed here: registration never touches the database (stores migrate
// lazily on first use).
module PostgresRegistrationTests =

    /// Builds the application configuration from in-memory pairs.
    let private buildConfiguration (pairs: (string * string) seq) : IConfiguration =
        let keyValues =
            pairs |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))

        ConfigurationBuilder().AddInMemoryCollection(keyValues).Build()

    [<Fact>]
    let ``PostgresOptions defaults describe the legate schema`` () =
        let options = PostgresOptions()
        Assert.Equal("legate", options.Schema)
        Assert.Equal("", options.TablePrefix)
        Assert.True(options.RunMigrations)
        // Defaults carry no connection string, so they do not validate
        // until the host supplies one.
        Assert.NotNull(options.Validate())

        options.ConnectionString <- "Host=127.0.0.1;Database=legate"
        Assert.Null(options.Validate())

    [<Fact>]
    let ``PostgresOptions rejects a blank connection string and bad names`` () =
        let blank = PostgresOptions()
        Assert.NotNull(blank.Validate())

        let badSchema = PostgresOptions()
        badSchema.ConnectionString <- "Host=127.0.0.1;Database=legate"
        badSchema.Schema <- "has-dash"
        Assert.NotNull(badSchema.Validate())

        let negative = PostgresOptions()
        negative.ConnectionString <- "Host=127.0.0.1;Database=legate"
        negative.MaxEventsPerSession <- -1L
        Assert.NotNull(negative.Validate())

    [<Fact>]
    let ``UsePostgres binds the section and registers every store`` () =
        let configuration =
            buildConfiguration
                [
                    "Legate:Storage:Postgres:ConnectionString", "Host=127.0.0.1;Database=legate"
                    "Legate:Storage:Postgres:MaxEventsPerSession", "100"
                    "Legate:Storage:Postgres:RunMigrations", "false"
                ]

        let services = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(
            services,
            ?configure = Some(fun (builder: LegateBuilder) -> builder.UsePostgres(configuration) |> ignore)
        )
        |> ignore

        use provider = services.BuildServiceProvider()

        let options = provider.GetRequiredService<IOptions<PostgresOptions>>().Value
        Assert.Equal("Host=127.0.0.1;Database=legate", options.ConnectionString)
        Assert.Equal("legate", options.Schema)
        Assert.Equal(100L, options.MaxEventsPerSession)
        Assert.False(options.RunMigrations)

        Assert.IsType<PostgresSessionStore>(provider.GetRequiredService<ISessionStore>())
        |> ignore

        Assert.IsType<PostgresSessionEventStore>(provider.GetRequiredService<ISessionEventStore>())
        |> ignore

        Assert.IsType<PostgresAgentStore>(provider.GetRequiredService<IAgentStore>())
        |> ignore

        Assert.IsType<PostgresAgentStore>(provider.GetRequiredService<IAgentCustomToolStore>())
        |> ignore

    [<Fact>]
    let ``UsePostgres rejects an empty connection string before registering`` () =
        let configuration =
            buildConfiguration
                [
                    "Legate:Storage:Postgres:Schema", "legate"
                ]

        let services = ServiceCollection()

        Assert.Throws<InvalidOperationException>(fun () ->
            LegateServiceCollectionExtensions.AddLegate(
                services,
                ?configure = Some(fun (builder: LegateBuilder) -> builder.UsePostgres(configuration) |> ignore)
            )
            |> ignore)
        |> ignore

    [<Fact>]
    let ``UsePostgres applies the configure callback over the section`` () =
        let configuration =
            buildConfiguration
                [
                    "Legate:Storage:Postgres:ConnectionString", "Host=127.0.0.1;Database=legate"
                ]

        let services = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(
            services,
            ?configure =
                Some(fun (builder: LegateBuilder) ->
                    builder.UsePostgres(
                        configuration,
                        Action<PostgresOptions>(fun options -> options.TablePrefix <- "pg_")
                    )
                    |> ignore)
        )
        |> ignore

        use provider = services.BuildServiceProvider()

        let options = provider.GetRequiredService<IOptions<PostgresOptions>>().Value
        Assert.Equal("pg_", options.TablePrefix)
