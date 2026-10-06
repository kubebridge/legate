// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SqliteOperationWaitTests

open System
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.Sqlite
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Xunit

// Split-store receipt waits (issue 383 revision): SQLite registers the
// session store and the atomic settlement capability as separate
// services, while InMemory unifies them. The suspendable settle must
// commit through the container-registered capability: with the
// store-cast-only resolution the SQLite turn settled legacy store-first
// with no execution_settlements row, so receipt-bound waits polled
// Pending until their bound while the journaled turn had already
// completed (the Dot scripted smoke hung this way).

let private waitBound = TimeSpan.FromSeconds(30.0)

let private awaitWhat (work: Task<'T>) (what: string) : Task<'T> =
    task {
        try
            return! work.WaitAsync(TimeSpan.FromSeconds(60.0), CancellationToken.None)
        with :? TimeoutException ->
            return raise (TimeoutException($"The test timed out waiting for {what}."))
    }

let private createSqliteServices (path: string) (chatClient: ScriptedChatClient) : IServiceCollection =
    let services = ServiceCollection() :> IServiceCollection

    // Dot's composition: one UseSqlite file plus the stores over it, then
    // AddLegate without Storage builder calls (the facade resolves
    // ISessionStore from the container, like the sample host).
    SqliteServiceCollectionExtensions.UseSqlite(services, path) |> ignore

    LegateServiceCollectionExtensions.AddLegate(
        services,
        ?configure =
            Some(fun (builder: LegateBuilder) ->
                builder.Llm.AddProvider(BuilderTests.StubLlmProvider()) |> ignore
                builder.Workspace.UseRuntime(BuilderTests.StubWorkspaceRuntime()) |> ignore)
    )
    |> ignore

    services.AddSingleton<IChatClient>(chatClient) |> ignore
    services

let private actorServiceOf (provider: IServiceProvider) : LocalActorSystemService =
    provider.GetServices<IHostedService>()
    |> Seq.pick (fun service ->
        match service with
        | :? LocalActorSystemService as local -> Some local
        | _ -> None)

let private stopQuietly (service: LocalActorSystemService) : Task =
    task {
        try
            do! (service :> IHostedService).StopAsync(CancellationToken.None)
        with _ ->
            ()
    }

let private agentDefinition (id: AgentId) : Agent =
    {
        Id = id
        Tenant = TenantId.Default
        Name = "split-store smoke agent"
        Description = "Carries the scripted model for the split-store wait test."
        Model = ModelReference.Parse "stub/stub-model"
        SystemPrompt = "You answer from the script."
        EnvironmentVariables = null
        PermissionDefaults = null
        ToolSelection = null
        PackageReference = null
        Enabled = true
        Schedule = null
        RowVersion = 0UL
        CreatedAt = DateTimeOffset.UtcNow
        UpdatedAt = DateTimeOffset.UtcNow
    }

let private insertAgent (agents: IAgentStore) (agent: Agent) : Task =
    task {
        let! outcome = agents.UpdateIfUnchanged(TenantId.Default, agent, 0UL, CancellationToken.None)

        match outcome with
        | :? AgentUpdated -> ()
        | _ -> failwith "Expected the agent insert to apply."
    }

[<Fact>]
let ``split-store SQLite host settles PromptAndWaitAsync from the committed row`` () : Task =
    task {
        let path = SqliteTestFixture.tempDatabasePath ()

        try
            let chat =
                new ScriptedChatClient(
                    ResizeArray<ScriptStep>(
                        [
                            ScriptStep.Text "sqlite split-store answer"
                        ]
                    )
                    :> System.Collections.Generic.IReadOnlyList<ScriptStep>
                )

            use provider = (createSqliteServices path chat).BuildServiceProvider()
            let service = actorServiceOf provider
            do! (service :> IHostedService).StartAsync(CancellationToken.None)

            try
                // The runtime authority gate rejects turns for missing
                // agents, so the test ensures its agent row like a real
                // host (Dot's ensureModelAgentAsync) instead of prompting
                // under a random id.
                let agents = provider.GetRequiredService<IAgentStore>()
                let agentId = AgentId.New()
                do! insertAgent agents (agentDefinition agentId)

                let client = provider.GetRequiredService<SessionClient>()
                let options = SessionOptions()
                options.Timeout <- Nullable<TimeSpan>(waitBound)

                let! created =
                    awaitWhat
                        (SessionClientOperations.OpenSessionAsync(client, agentId, options, CancellationToken.None))
                        "the session to open"

                let! result =
                    awaitWhat
                        (SessionClientExtensions.PromptAndWaitAsync(
                            client,
                            created.Id,
                            UserMessage.Text "hi",
                            CancellationToken.None
                        ))
                        "the SQLite turn to settle"

                Assert.Equal(TurnStatus.Completed, result.Status)
                Assert.Equal("sqlite split-store answer", result.AssistantText)
                do! stopQuietly service
            with ex ->
                do! stopQuietly service
                return raise ex
        finally
            SqliteTestFixture.deleteDatabaseFiles path
    }
