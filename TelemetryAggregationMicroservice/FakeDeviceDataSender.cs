
/*
 * PROJECT DESCRIPTION
 * 
 * This program simulates a set of fake IoT devices and sends telemetry messages to an Azure Service Bus topic. 
 * It reads connection info from environment variables, lets you optionally override device count / message rate / run time with command-line arguments, 
 * then loops every second creating msgsPerSecond JSON telemetry messages (random device selection and small random variations in temp/humidity). 
 * It batches messages for efficiency, sends them to the topic, logs sends, and respects Ctrl+C cancellation. 
 * Resources are cleaned up and the program returns appropriate exit codes on success or error.
 * 
 */


using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

public class FakeDeviceDataSender
{
    static async Task<int> Main(string[] args)
    {
        string connectionString = Environment.GetEnvironmentVariable("SERVICE_BUS_CONNECTION_STRING") ?? string.Empty;
        string topicName = Environment.GetEnvironmentVariable("SERVICE_BUS_TOPIC_NAME") ?? string.Empty;

        if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(topicName))
        {
            Console.WriteLine("ERROR: Set SERVICEBUS_CONNECTIONSTR and TOPIC_NAME environment variables before running.");
            return 1;
        }

        int deviceCount = args.Length > 0 && int.TryParse(args[0], out var d) ? Math.Max(1, d) : 5;
        int msgsPerSecond = args.Length > 1 && int.TryParse(args[1], out var m) ? Math.Max(1, m) : 10;
        int runSeconds = args.Length > 2 && int.TryParse(args[2], out var s) ? Math.Max(5, s) : 30;

        Console.WriteLine($"Starting fake devices: {deviceCount} devices, {msgsPerSecond} messages/sec for {runSeconds} seconds...");
        Console.WriteLine($"Topic: {topicName}");

        await using var client = new ServiceBusClient(connectionString);
        ServiceBusSender sender = client.CreateSender(topicName);

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            Console.WriteLine("Cancellation requested");
            e.Cancel = true;
            cts.Cancel();
        };

        var runUntil = DateTime.UtcNow.AddSeconds(runSeconds);

        // Seed devices
        var devices = new List<Device>();
        var rnd = new Random();
        for (int i = 0; i < deviceCount; i++)
        {
            devices.Add(new Device
            {
                DeviceId = $"device-{1000 + i}",
                BaseTemp = 18 + rnd.NextDouble() * 10, // 18..28
                BaseHumidity = 30 + rnd.NextDouble() * 30, // 30..60
                Seq = 0
            });
        }

        // We'll try to send msgsPerSecond messages across all devices every second.
        var delayPerTick = TimeSpan.FromSeconds(1);

        try
        {
            while (DateTime.UtcNow < runUntil && !cts.IsCancellationRequested)
            {
                var tickStart = DateTime.UtcNow;

                // generate messages for this second
                var messagesToSend = new List<ServiceBusMessage>();
                for (int n = 0; n < msgsPerSecond; n++)
                {
                    // pick a random device for each message
                    var device = devices[rnd.Next(devices.Count)];
                    device.Seq++;

                    var telemtry = new
                    {
                        deviceId = device.DeviceId,
                        timestamp = DateTimeOffset.UtcNow.ToString("o"),
                        sequence = device.Seq,
                        temperature = Math.Round(device.BaseTemp + (rnd.NextDouble() - 0.5) * 4.0, 2),
                        humidity = Math.Round(device.BaseHumidity + (rnd.NextDouble() - 0.5) * 10.0, 2)
                    };

                    string json = JsonSerializer.Serialize(telemtry);
                    var msg = new ServiceBusMessage(Encoding.UTF8.GetBytes(json))
                    {
                        MessageId = $"{device.DeviceId}-{device.Seq}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}",
                        Subject = "telemetry"
                    };
                    msg.ApplicationProperties["deviceId"] = device.DeviceId;
                    msg.ApplicationProperties["type"] = "telemetry";
                    messagesToSend.Add(msg);

                }

                // Send messages in batches for efficiency
                var batch = await sender.CreateMessageBatchAsync(cts.Token);
                foreach (var n in messagesToSend)
                {
                    if (!batch.TryAddMessage(n))
                    {
                        // send current batch and create a new one
                        await sender.SendMessagesAsync(batch, cts.Token);
                        Console.WriteLine($"Sent batch ({batch.Count} msgs) at {DateTime.UtcNow:O}");
                        batch = await sender.CreateMessageBatchAsync(cts.Token);

                        // try again; if single message too large it will throw
                        if (!batch.TryAddMessage(n))
                        {
                            throw new Exception("Message too large to fit in an empty batch.");
                        }
                    }
                }
                if (batch.Count > 0)
                {
                    await sender.SendMessagesAsync(batch, cts.Token);
                    Console.WriteLine($"Sent batch ({batch.Count} msgs) at {DateTime.UtcNow:O}");
                }

                // Sleep until next second, adjusted for time already spent
                var elapsed = DateTime.UtcNow - tickStart;
                var wait = delayPerTick - elapsed;
                if (wait > TimeSpan.Zero)
                {
                    await Task.Delay(wait, cts.Token);
                }
            }
            Console.WriteLine("Finished sending telemetry.");
        }
        catch (TaskCanceledException)
        {
            Console.WriteLine("Operation cancelled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ERROR: {ex}"); return 2;
        }
        finally
        {
            await sender.DisposeAsync();
            await client.DisposeAsync();
        }

        return 0;
    }

    class Device 
    { 
        public string DeviceId { get; set; } = string.Empty;
        public double BaseTemp { get; set; } 
        public double BaseHumidity { get; set; } 
        public long Seq { get; set; } 
    }
}