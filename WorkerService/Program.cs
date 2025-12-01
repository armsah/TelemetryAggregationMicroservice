/*
 * PROJECT DESCRIPTION
 * 
 * The Program class serves as the entry point for the Telemetry Aggregation Worker Service, 
 * and is a background service that processes IoT telemetry data. 
 * It configures hosting, logging, and dependency injection to run the DataReceiver background service in a reliable 
 * and scalable manner.
 * 
 */



using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkerService.Models;

public class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, services) =>
            {
                // Register your Worker background service
                services.AddHostedService<DataReceiver>();
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
            })
            .Build();

        await host.RunAsync();
    }
}

