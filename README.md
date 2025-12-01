# TelemetryAggregationMicroservice
Project Name: Fault-Tolerant Telemetry Aggregation Microservice
•	This is a system that collects and processes data (telemetry) from many network devices — things like routers, sensors, or IoT devices — and makes sure it keeps running even if something goes wrong.

This project consists of two main services:
1.	Data Ingestion Service: Simulates IoT devices and sends telemetry messages to an Azure Service Bus topic.
2.	Worker Service: Consumes telemetry messages from the Service Bus, persists device info to SQL Server, and stores raw telemetry in Cosmos DB.

Main Goal
•	Keeps working even if parts of it fail.
•	Can handle lots of incoming data.
•	Uses the right tools for each job (SQL + NoSQL).

Prerequisites
•	Docker & Docker Compose
•	.NET 8 SDK
•	Azure Service Bus (or emulator for local dev)
•	SQL Server (or use the provided docker-compose override)
•	Optional: Azure Cosmos DB (or local emulator)


Running with Docker Compose
1. Build and start services:
>> docker-compose -f docker-compose.yml -f docker-compose.override.yml up --build
where:
     az-servicebus – Service Bus emulator
     sqlserver – SQL Server
     data-ingestion – Simulated IoT devices
     worker-service – Telemetry consumer

2. View logs:
>> docker-compose logs -f
where:
     use -f to follow logs in real-time
2.1 To see logs of a specific service:
>> docker-compose logs -f data-ingestion
>> docker-compose logs -f worker-service

3. Stop and remove containers:
>> docker-compose down
where:
     stops all running containers and removes networks
3.1 Add --volumes to also remove named volumes (e.g., SQL Server data) if needed:
>> docker-compose down --volumes

Running Data Ingestion Service Manually
>> cd DataIngestionService
>> dotnet run -- [deviceCount] [messagesPerSecond] [runSeconds]
where:
    Example: 15 devices, 30 messages/sec, run for 60 seconds
    >> dotnet run -- 15 30 60

Running Worker Service Manually
>> cd WorkerService
>> dotnet run
