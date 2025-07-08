# MatchMaking.Service/Dockerfile (Development)
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY MatchMaking.Service/*.csproj .
RUN dotnet restore

COPY . .

RUN dotnet publish -c Debug -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app
COPY --from=build /app .

EXPOSE 9998 9999

ENTRYPOINT ["dotnet", "MatchMaking.Service.dll"]