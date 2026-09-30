# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Primero solo los proyectos, para cachear el restore mientras no cambien las dependencias.
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/K0sStreams.Contracts/K0sStreams.Contracts.csproj src/K0sStreams.Contracts/
COPY src/K0sStreams.Storage/K0sStreams.Storage.csproj src/K0sStreams.Storage/
COPY src/K0sStreams.Queue/K0sStreams.Queue.csproj src/K0sStreams.Queue/
COPY src/K0sStreams.Replication/K0sStreams.Replication.csproj src/K0sStreams.Replication/
COPY src/K0sStreams.Coordination/K0sStreams.Coordination.csproj src/K0sStreams.Coordination/
COPY src/K0sStreams.Broker/K0sStreams.Broker.csproj src/K0sStreams.Broker/
RUN dotnet restore src/K0sStreams.Broker/K0sStreams.Broker.csproj

COPY src/ src/
RUN dotnet publish src/K0sStreams.Broker/K0sStreams.Broker.csproj -c Release --no-restore -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

# /data es donde se monta el PVC con el log.
RUN mkdir /data && chown "$APP_UID" /data
VOLUME /data
ENV Broker__DataDir=/data

COPY --from=build /app .
USER $APP_UID

# 9090: API REST para clientes. 9091: gRPC entre brokers.
EXPOSE 9090 9091
ENTRYPOINT ["dotnet", "K0sStreams.Broker.dll"]
