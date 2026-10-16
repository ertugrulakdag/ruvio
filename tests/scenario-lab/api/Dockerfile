FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Directory.Build.props Directory.Packages.props ./
COPY clients/csharp/Ruvio.Client ./clients/csharp/Ruvio.Client
COPY clients/csharp/Ruvio.Client.Generators ./clients/csharp/Ruvio.Client.Generators
COPY clients/csharp/Ruvio.Client.AspNetCore ./clients/csharp/Ruvio.Client.AspNetCore
COPY integrations/dotnet/Ruvio.AspNetCore.Idempotency ./integrations/dotnet/Ruvio.AspNetCore.Idempotency
COPY integrations/dotnet/Ruvio.AspNetCore.OutputCaching ./integrations/dotnet/Ruvio.AspNetCore.OutputCaching
COPY integrations/dotnet/Ruvio.AspNetCore.RateLimiting ./integrations/dotnet/Ruvio.AspNetCore.RateLimiting
COPY tests/scenario-lab/api/ScenarioStorage.cs ./tests/scenario-lab/api/ScenarioStorage.cs
COPY tests/scenario-lab/api ./tests/scenario-lab/api

RUN dotnet publish tests/scenario-lab/api/Ruvio.ScenarioLab.csproj \
    --configuration Release --output /out /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /out .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "Ruvio.ScenarioLab.dll"]
