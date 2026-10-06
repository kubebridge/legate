# SPDX-License-Identifier: Apache-2.0
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/Legate.Abstractions/ src/Legate.Abstractions/
COPY src/Legate/ src/Legate/
COPY src/Legate.AspNetCore/ src/Legate.AspNetCore/
COPY src/Legate.Storage.InMemory/ src/Legate.Storage.InMemory/
COPY src/Legate.Storage.Migrations/ src/Legate.Storage.Migrations/
COPY src/Legate.Storage.Postgres/ src/Legate.Storage.Postgres/
COPY src/Legate.Coordination.Redis/ src/Legate.Coordination.Redis/
COPY src/Legate.Workspace.Process/ src/Legate.Workspace.Process/
COPY samples/CSharpWebHost/ samples/CSharpWebHost/
RUN dotnet publish samples/CSharpWebHost/CSharpWebHost.csproj -c Release -o /publish
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /publish .
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["dotnet", "CSharpWebHost.dll"]
