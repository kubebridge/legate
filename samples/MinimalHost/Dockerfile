# Sample-only image for the MinimalHost Kubernetes manifests.
# Build from the repo root so project references resolve:
#   docker build -f samples/MinimalHost/Dockerfile -t legate-minimalhost:local .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY global.json ./
COPY Directory.Build.props ./
COPY Directory.Packages.props ./
COPY src/Legate.Abstractions/ src/Legate.Abstractions/
COPY src/Legate/ src/Legate/
COPY src/Legate.Cluster.Kubernetes/ src/Legate.Cluster.Kubernetes/
COPY src/Legate.Coordination.Redis/ src/Legate.Coordination.Redis/
COPY src/Legate.Llm.Google/ src/Legate.Llm.Google/
COPY src/Legate.Llm.OpenAI/ src/Legate.Llm.OpenAI/
COPY src/Legate.Storage.InMemory/ src/Legate.Storage.InMemory/
COPY src/Legate.Storage.Migrations/ src/Legate.Storage.Migrations/
COPY src/Legate.Storage.Postgres/ src/Legate.Storage.Postgres/
COPY src/Legate.Workspace.Process/ src/Legate.Workspace.Process/
COPY samples/MinimalHost/ samples/MinimalHost/
RUN dotnet publish samples/MinimalHost/MinimalHost.fsproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
# curl serves the compose harness healthchecks (per-node /ready probes
# gate the sequenced StaticSeeds bootstrap); sample-only.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080 8558
ENTRYPOINT ["dotnet", "MinimalHost.dll"]
