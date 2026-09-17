// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3.Tests

open System
open System.Collections.Generic
open Amazon.S3
open Legate
open Legate.Storage
open Legate.Storage.S3
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options
open Xunit

// UseS3 wiring: the Legate:Storage:S3 section binds onto S3StorageOptions
// and every store resolves from the container. No Docker is needed here:
// registration never touches the endpoint (the client constructs lazily
// and the bucket materialises on first write).
module S3RegistrationTests =

    /// Builds the application configuration from in-memory pairs.
    let private buildConfiguration (pairs: (string * string) seq) : IConfiguration =
        let keyValues =
            pairs |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))

        ConfigurationBuilder().AddInMemoryCollection(keyValues).Build()

    [<Fact>]
    let ``S3StorageOptions defaults describe AWS virtual-hosted style`` () =
        let options = S3StorageOptions()
        Assert.Equal("", options.ServiceUrl)
        Assert.Equal("us-east-1", options.Region)
        Assert.Equal("", options.Bucket)
        Assert.False(options.ForcePathStyle)
        Assert.Equal(TimeSpan.FromMinutes 15., options.PresignExpiry)
        // Defaults carry no bucket, so they do not validate until the
        // host supplies one.
        Assert.NotNull(options.Validate())

        options.Bucket <- "legate-test"
        Assert.Null(options.Validate())

    [<Fact>]
    let ``S3StorageOptions rejects a bad endpoint, half credentials, and a dead presign expiry`` () =
        let blank = S3StorageOptions()
        Assert.NotNull(blank.Validate())

        let badEndpoint = S3StorageOptions()
        badEndpoint.Bucket <- "legate-test"
        badEndpoint.ServiceUrl <- "not-a-url"
        Assert.NotNull(badEndpoint.Validate())

        let halfCreds = S3StorageOptions()
        halfCreds.Bucket <- "legate-test"
        halfCreds.AccessKeyId <- "key-without-secret"
        Assert.NotNull(halfCreds.Validate())

        let deadPresign = S3StorageOptions()
        deadPresign.Bucket <- "legate-test"
        deadPresign.PresignExpiry <- TimeSpan.Zero
        Assert.NotNull(deadPresign.Validate())

    [<Fact>]
    let ``UseS3 binds the section and registers the client and every store`` () =
        let configuration =
            buildConfiguration
                [
                    "Legate:Storage:S3:ServiceUrl", "http://127.0.0.1:9000"
                    "Legate:Storage:S3:Bucket", "legate-test"
                    "Legate:Storage:S3:AccessKeyId", "test-key"
                    "Legate:Storage:S3:SecretAccessKey", "test-secret"
                    "Legate:Storage:S3:ForcePathStyle", "true"
                    "Legate:Storage:S3:PresignExpiry", "00:05:00"
                ]

        let services = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(
            services,
            ?configure = Some(fun (builder: LegateBuilder) -> builder.UseS3(configuration) |> ignore)
        )
        |> ignore

        use provider = services.BuildServiceProvider()

        let options = provider.GetRequiredService<IOptions<S3StorageOptions>>().Value
        Assert.Equal("http://127.0.0.1:9000", options.ServiceUrl)
        Assert.Equal("legate-test", options.Bucket)
        Assert.True(options.ForcePathStyle)
        Assert.Equal(TimeSpan.FromMinutes 5., options.PresignExpiry)

        Assert.IsType<AmazonS3Client>(provider.GetRequiredService<AmazonS3Client>())
        |> ignore

        Assert.IsType<S3BlobStore>(provider.GetRequiredService<IBlobStore>()) |> ignore

        Assert.IsType<S3AgentPackageStore>(provider.GetRequiredService<IAgentPackageStore>())
        |> ignore

    [<Fact>]
    let ``UseS3 rejects an empty bucket before registering`` () =
        let configuration =
            buildConfiguration
                [
                    "Legate:Storage:S3:ServiceUrl", "http://127.0.0.1:9000"
                ]

        let services = ServiceCollection()

        Assert.Throws<InvalidOperationException>(fun () ->
            LegateServiceCollectionExtensions.AddLegate(
                services,
                ?configure = Some(fun (builder: LegateBuilder) -> builder.UseS3(configuration) |> ignore)
            )
            |> ignore)
        |> ignore

    [<Fact>]
    let ``UseS3 applies the configure callback over the section`` () =
        let configuration =
            buildConfiguration
                [
                    "Legate:Storage:S3:Bucket", "legate-test"
                ]

        let services = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(
            services,
            ?configure =
                Some(fun (builder: LegateBuilder) ->
                    builder.UseS3(
                        configuration,
                        Action<S3StorageOptions>(fun options -> options.ForcePathStyle <- true)
                    )
                    |> ignore)
        )
        |> ignore

        use provider = services.BuildServiceProvider()

        let options = provider.GetRequiredService<IOptions<S3StorageOptions>>().Value
        Assert.True(options.ForcePathStyle)
