/*
 * PROJECT DESCRIPTION
 * 
 * The Telemetry Aggregation Worker Service is a background service, designed to reliably ingest, 
 * process, and store telemetry data from IoT devices. 
 * It integrates with Azure Service Bus for message delivery, SQL Server for structured device information, 
 * and Azure Cosmos DB for raw telemetry storage, providing a scalable and 
 * fault-tolerant solution for real-time device monitoring.
 * 
 */



using System;
using System.Data;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

public class DataReceiver : BackgroundService
{
    private readonly ILogger<DataReceiver> _logger;
    private readonly ServiceBusClient _client;
    private readonly ServiceBusProcessor _processor;
    private readonly SqlConnection _sqlConnection;
    private readonly CosmosClient _cosmosClient;
    private readonly Container _cosmosContainer;

    // Environment variables
    private readonly string _connectionString = Environment.GetEnvironmentVariable("SERVICEBUS_CONNECTIONSTR") ?? "";
    private readonly string _queueName = Environment.GetEnvironmentVariable("QUEUE_NAME") ?? "";
    private readonly string _sqlConnectionString = Environment.GetEnvironmentVariable("SQL_CONNECTIONSTR") ?? "";
    private readonly string _cosmosConnectionString = Environment.GetEnvironmentVariable("COSMOS_CONNECTIONSTR") ?? "";
    private readonly string _cosmosDbName = Environment.GetEnvironmentVariable("COSMOS_DB_NAME") ?? "telemetrydb";
    private readonly string _cosmosContainerName = Environment.GetEnvironmentVariable("COSMOS_CONTAINER_NAME") ?? "telemetry";

    public DataReceiver(ILogger<DataReceiver> logger)
    {
        _logger = logger;

        if (string.IsNullOrWhiteSpace(_connectionString) || string.IsNullOrWhiteSpace(_queueName))
            throw new ArgumentException("Missing SERVICEBUS_CONNECTIONSTR or QUEUE_NAME.");

        if (string.IsNullOrWhiteSpace(_sqlConnectionString))
            throw new ArgumentException("Missing SQL_CONNECTIONSTR.");

        if (string.IsNullOrWhiteSpace(_cosmosConnectionString))
            throw new ArgumentException("Missing COSMOS_CONNECTIONSTR.");

        _client = new ServiceBusClient(_connectionString);
        _processor = _client.CreateProcessor(_queueName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 5
        });

        _sqlConnection = new SqlConnection(_sqlConnectionString);
        _cosmosClient = new CosmosClient(_cosmosConnectionString);
        _cosmosContainer = _cosmosClient.GetContainer(_cosmosDbName, _cosmosContainerName);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _sqlConnection.OpenAsync(stoppingToken);

        _processor.ProcessMessageAsync += MessageHandler;
        _processor.ProcessErrorAsync += ErrorHandler;

        _logger.LogInformation("Starting queue processor...");
        await _processor.StartProcessingAsync(stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task MessageHandler(ProcessMessageEventArgs args)
    {
        var body = args.Message.Body.ToString();
        var messageId = args.Message.MessageId;

        _logger.LogInformation($"Received message {messageId}");

        try
        {
            var telemetry = JsonSerializer.Deserialize<TelemetryMessage>(body);
            if (telemetry == null)
            {
                _logger.LogWarning("Invalid telemetry JSON");
                await args.AbandonMessageAsync(args.Message);
                return;
            }

            // 1️⃣ Save device info to SQL
            await UpsertDeviceInfoAsync(telemetry);

            // 2️⃣ Save raw telemetry to Cosmos DB
            await SaveTelemetryToCosmosAsync(telemetry);

            // 3️⃣ Complete message after successful processing
            await args.CompleteMessageAsync(args.Message);
            _logger.LogInformation($"Message {messageId} processed successfully.");
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error processing message {messageId}: {ex.Message}");
            await args.AbandonMessageAsync(args.Message);
        }
    }

    private async Task UpsertDeviceInfoAsync(TelemetryMessage telemetry)
    {
        string sql = @"
            MERGE INTO Devices AS target
            USING (SELECT @DeviceId AS DeviceId) AS source
            ON target.DeviceId = source.DeviceId
            WHEN MATCHED THEN 
                UPDATE SET LastSeen = @Timestamp, LastTemperature = @Temperature, LastHumidity = @Humidity
            WHEN NOT MATCHED THEN 
                INSERT (DeviceId, LastSeen, LastTemperature, LastHumidity)
                VALUES (@DeviceId, @Timestamp, @Temperature, @Humidity);";

        using var cmd = new SqlCommand(sql, _sqlConnection);
        cmd.Parameters.AddWithValue("@DeviceId", telemetry.DeviceId);
        cmd.Parameters.AddWithValue("@Timestamp", DateTime.UtcNow);
        cmd.Parameters.AddWithValue("@Temperature", telemetry.Temperature);
        cmd.Parameters.AddWithValue("@Humidity", telemetry.Humidity);

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SaveTelemetryToCosmosAsync(TelemetryMessage telemetry)
    {
        var telemetryDoc = new
        {
            id = $"{telemetry.DeviceId}-{telemetry.Sequence}",
            deviceId = telemetry.DeviceId,
            timestamp = DateTime.UtcNow,
            temperature = telemetry.Temperature,
            humidity = telemetry.Humidity
        };

        await _cosmosContainer.UpsertItemAsync(telemetryDoc, new PartitionKey(telemetry.DeviceId));
    }

    private Task ErrorHandler(ProcessErrorEventArgs args)
    {
        _logger.LogError($"Service Bus Error: {args.Exception.Message}");
        return Task.CompletedTask;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping queue processor...");
        await _processor.StopProcessingAsync(cancellationToken);
        await _processor.DisposeAsync();
        await _client.DisposeAsync();
        await _sqlConnection.DisposeAsync();
        _cosmosClient.Dispose();
        await base.StopAsync(cancellationToken);
    }

    private class TelemetryMessage
    {
        public string DeviceId { get; set; } = "";
        public long Sequence { get; set; }
        public double Temperature { get; set; }
        public double Humidity { get; set; }
    }
}
