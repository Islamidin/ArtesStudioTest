# syntax=docker/dockerfile:1
ARG DOTNET_VERSION=9.0

FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION} AS build
WORKDIR /src

# Copy csproj and restore as distinct layers
COPY --link MatchMaking.Worker.csproj ./
RUN --mount=type=cache,target=/root/.nuget/packages \
    --mount=type=cache,target=/root/.cache/msbuild \
    dotnet restore "MatchMaking.Worker.csproj"

# Copy everything else and build
COPY --link . .
RUN dotnet publish "MatchMaking.Worker.csproj" -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime:${DOTNET_VERSION} AS runtime
WORKDIR /app

# Security: create non-root user
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
USER appuser

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "MatchMaking.Worker.dll"]
