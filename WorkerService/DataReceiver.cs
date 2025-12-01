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

namespace WorkerService.Models
{
    public interface IMessageArgs
    {
        string Body { get; }
        Task CompleteAsync();
        Task AbandonAsync();
    }
    public interface IServiceBusProcessorWrapper
    {
        event Func<ProcessMessageEventArgs, Task> ProcessMessageAsync;
        event Func<ProcessErrorEventArgs, Task> ProcessErrorAsync;

        Task StartProcessingAsync(CancellationToken token);
        Task StopProcessingAsync(CancellationToken token);
    }

    public interface IServiceBusClientWrapper
    {
        IServiceBusProcessorWrapper CreateProcessor(string queueName);
    }

    public interface ISqlDatabase
    {
        Task UpsertDeviceInfoAsync(string deviceId, double temp, double humidity);
    }

    public interface ITelemetryRepository
    {
        Task SaveTelemetryAsync(string id, string deviceId, double temp, double humidity);
    }

    public class TelemetryMessage
    {
        public string DeviceId { get; set; } = "";
        public long Sequence { get; set; }
        public double Temperature { get; set; }
        public double Humidity { get; set; }
    }

    public class DataReceiver : BackgroundService
    {
        private readonly ILogger<DataReceiver> _logger;
        private readonly IServiceBusProcessorWrapper _processor;
        private readonly ISqlDatabase _sqlDb;
        private readonly ITelemetryRepository _telemetryRepo;

        public DataReceiver(ILogger<DataReceiver> logger, IServiceBusClientWrapper busClient, ISqlDatabase sqlDb, ITelemetryRepository telemetryRepo, string? queueName = null)
        {
            _logger = logger;
            _sqlDb = sqlDb;
            _telemetryRepo = telemetryRepo;

            queueName ??= Environment.GetEnvironmentVariable("QUEUE_NAME")
                ?? throw new ArgumentException("Missing QUEUE_NAME");

            _processor = busClient.CreateProcessor(queueName);
            _processor.ProcessMessageAsync += MessageHandler;
            _processor.ProcessErrorAsync += ErrorHandler;
        }

        // PUBLIC wrapper for unit testing
        public async Task HandleMessageAsync(IMessageArgs args)
        {
            try
            {
                var telemetry = JsonSerializer.Deserialize<TelemetryMessage>(args.Body);
                if (telemetry == null)
                {
                    await args.AbandonAsync();
                    return;
                }

                await _sqlDb.UpsertDeviceInfoAsync(telemetry.DeviceId, telemetry.Temperature, telemetry.Humidity);
                await _telemetryRepo.SaveTelemetryAsync(
                    $"{telemetry.DeviceId}-{telemetry.Sequence}",
                    telemetry.DeviceId,
                    telemetry.Temperature,
                    telemetry.Humidity);

                await args.CompleteAsync();
            }
            catch
            {
                await args.AbandonAsync();
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Starting queue processor...");
            await _processor.StartProcessingAsync(stoppingToken);
        }

        private async Task MessageHandler(ProcessMessageEventArgs args)
        {
            // Original ServiceBus handler remains unchanged
            var body = args.Message.Body.ToString();

            try
            {
                TelemetryMessage? telemetry = JsonSerializer.Deserialize<TelemetryMessage>(body);
                if (telemetry == null)
                {
                    _logger.LogWarning("Invalid telemetry JSON");
                    await args.AbandonMessageAsync(args.Message);
                    return;
                }

                await _sqlDb.UpsertDeviceInfoAsync(telemetry.DeviceId, telemetry.Temperature, telemetry.Humidity);
                await _telemetryRepo.SaveTelemetryAsync(
                    $"{telemetry.DeviceId}-{telemetry.Sequence}",
                    telemetry.DeviceId,
                    telemetry.Temperature,
                    telemetry.Humidity);

                await args.CompleteMessageAsync(args.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message");
                await args.AbandonMessageAsync(args.Message);
            }
        }

        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            _logger.LogError(args.Exception, "Service Bus error");
            return Task.CompletedTask;
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Stopping processor...");
            return _processor.StopProcessingAsync(cancellationToken);
        }
    }

}
