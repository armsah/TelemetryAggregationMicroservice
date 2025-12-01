# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /app

# Copy project files
COPY *.csproj ./
RUN dotnet restore

COPY . ./
RUN dotnet publish -c Release -o out

# Stage 2: Runtime
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/out .

# Set environment variables (optional, can also be set in docker-compose)
# ENV SERVICE_BUS_CONNECTION_STRING=""
# ENV SERVICE_BUS_TOPIC_NAME=""

ENTRYPOINT ["dotnet", "DataIngestionService.dll"]
