FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

RUN apt-get update && apt-get install -y curl

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["MatchMaking.Service.csproj", "./"]
RUN dotnet restore "MatchMaking.Service.csproj"

COPY . .
RUN dotnet build "MatchMaking.Service.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "MatchMaking.Service.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MatchMaking.Service.dll"]