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

namespace DataIngestionService
{
    // Create an interface for sending messages
    public interface IServiceBusSenderWrapper
    {
        Task SendMessagesAsync(IEnumerable<ServiceBusMessage> messages, CancellationToken cancellationToken = default);
        ValueTask<ServiceBusMessageBatch> CreateMessageBatchAsync(CancellationToken cancellationToken = default);
    }

    // Implementing Service Bus Sender wrapper in production code
    public class ServiceBusSenderWrapper : IServiceBusSenderWrapper
    {
        private readonly ServiceBusSender _sender;

        public ServiceBusSenderWrapper(ServiceBusSender sender)
        {
            _sender = sender;
        }

        public Task SendMessagesAsync(IEnumerable<ServiceBusMessage> messages, CancellationToken cancellationToken = default)
        {
            return _sender.SendMessagesAsync(messages, cancellationToken);
        }

        public ValueTask<ServiceBusMessageBatch> CreateMessageBatchAsync(CancellationToken cancellationToken = default)
        {
            return _sender.CreateMessageBatchAsync(cancellationToken);
        }
    }


    // Simple device model
    public class Device
    {
        public string DeviceId { get; set; } = string.Empty;
        public double BaseTemp { get; set; }
        public double BaseHumidity { get; set; }
        public long Seq { get; set; }
    }

    // Telemetry message model matching DataReceiver
    public class TelemetryMessage
    {
        public string DeviceId { get; set; } = string.Empty;
        public long Sequence { get; set; }
        public double Temperature { get; set; }
        public double Humidity { get; set; }
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    }

    // Generates telemetry data for devices
    public static class TelemetryGenerator
    {
        public static TelemetryMessage Generate(Device device, Random rnd)
        {
            device.Seq++;
            return new TelemetryMessage
            {
                DeviceId = device.DeviceId,
                Sequence = device.Seq,
                Temperature = Math.Round(device.BaseTemp + (rnd.NextDouble() - 0.5) * 4.0, 2),
                Humidity = Math.Round(device.BaseHumidity + (rnd.NextDouble() - 0.5) * 10.0, 2),
                Timestamp = DateTime.UtcNow
            };
        }
    }

    // Converts telemetry to ServiceBusMessage
    public static class TelemetryMessageExtensions
    {
        public static ServiceBusMessage ToServiceBusMessage(this TelemetryMessage telemetry)
        {
            var json = JsonSerializer.Serialize(telemetry);
            var msg = new ServiceBusMessage(Encoding.UTF8.GetBytes(json))
            {
                MessageId = $"{telemetry.DeviceId}-{telemetry.Sequence}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}",
                Subject = "telemetry"
            };
            msg.ApplicationProperties["deviceId"] = telemetry.DeviceId;
            msg.ApplicationProperties["type"] = "telemetry";
            return msg;
        }
    }

    // Seed devices with random base temp/humidity
    public static class DeviceSeeder
    {
        public static List<Device> Seed(int count, Random rnd)
        {
            var devices = new List<Device>();
            for (int i = 0; i < count; i++)
            {
                devices.Add(new Device
                {
                    DeviceId = $"device-{1000 + i}",
                    BaseTemp = 18 + rnd.NextDouble() * 10,
                    BaseHumidity = 30 + rnd.NextDouble() * 30,
                    Seq = 0
                });
            }
            return devices;
        }
    }

    // Handles sending telemetry in a loop, abstracting the ServiceBusSender
    public class TelemetryLoop
    {
        private readonly List<Device> _devices;
        private readonly IServiceBusSenderWrapper _senderWrapper;
        private readonly int _msgsPerSecond;
        private readonly CancellationToken _ct;
        private readonly Random _rnd = new();
        private readonly bool _useBatch;

        public TelemetryLoop(List<Device> devices, IServiceBusSenderWrapper senderWrapper, int msgsPerSecond, CancellationToken ct, bool useBatch = true)
        {
            _devices = devices;
            _senderWrapper = senderWrapper;
            _msgsPerSecond = msgsPerSecond;
            _ct = ct;
            _useBatch = useBatch; // for skipping batching for tests
        }

        public async Task RunAsync(TimeSpan runDuration)
        {
            var runUntil = DateTime.UtcNow.Add(runDuration);
            var delayPerTick = TimeSpan.FromSeconds(1);

            while (DateTime.UtcNow < runUntil && !_ct.IsCancellationRequested)
            {
                var tickStart = DateTime.UtcNow;

                var messagesToSend = new List<ServiceBusMessage>();
                for (int i = 0; i < _msgsPerSecond; i++)
                {
                    var device = _devices[_rnd.Next(_devices.Count)];
                    var telemetry = TelemetryGenerator.Generate(device, _rnd);
                    messagesToSend.Add(telemetry.ToServiceBusMessage());
                }

                await SendBatchAsync(messagesToSend);

                var elapsed = DateTime.UtcNow - tickStart;
                var wait = delayPerTick - elapsed;
                if (wait > TimeSpan.Zero) await Task.Delay(wait, _ct);
            }
        }

        private async Task SendBatchAsync(List<ServiceBusMessage> messages)
        {
            if (messages.Count == 0) return;

            if (!_useBatch)
            {
                await _senderWrapper.SendMessagesAsync(messages, _ct);
                return;
            }

            // Create a new message batch
            var batch = await _senderWrapper.CreateMessageBatchAsync(_ct);
            var batchMessages = new List<ServiceBusMessage>();

            foreach (var msg in messages)
            {
                if (!batch.TryAddMessage(msg))
                {
                    // Send previous batch
                    await _senderWrapper.SendMessagesAsync(messages, _ct);

                    // Create new batch
                    batch = await _senderWrapper.CreateMessageBatchAsync(_ct);
                    batchMessages.Clear();

                    if (!batch.TryAddMessage(msg))
                        throw new Exception("Message too large to fit in empty batch.");
                }
            }

            if (batchMessages.Count > 0)
                await _senderWrapper.SendMessagesAsync(batchMessages, _ct);
        }
    }

    // Program entrypoint
    public static class Program
    {
        public static async Task<int> Main(string[] args)
        {
            string connectionString = Environment.GetEnvironmentVariable("SERVICE_BUS_CONNECTION_STRING") ?? string.Empty;
            string topicName = Environment.GetEnvironmentVariable("SERVICE_BUS_TOPIC_NAME") ?? string.Empty;

            if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(topicName))
            {
                Console.WriteLine("ERROR: Set SERVICEBUS_CONNECTION_STRING and SERVICEBUS_TOPIC_NAME environment variables.");
                return 1;
            }

            int deviceCount = args.Length > 0 && int.TryParse(args[0], out var d) ? Math.Max(1, d) : 5;
            int msgsPerSecond = args.Length > 1 && int.TryParse(args[1], out var m) ? Math.Max(1, m) : 10;
            int runSeconds = args.Length > 2 && int.TryParse(args[2], out var s) ? Math.Max(5, s) : 30;

            var devices = DeviceSeeder.Seed(deviceCount, new Random());

            await using var client = new ServiceBusClient(connectionString);
            var sender = client.CreateSender(topicName);

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                Console.WriteLine("Cancellation requested");
                e.Cancel = true;
                cts.Cancel();
            };

            // In Program.cs
            var senderWrapper = new ServiceBusSenderWrapper(sender); // wrap the sender
            var loop = new TelemetryLoop(devices, senderWrapper, msgsPerSecond, cts.Token);
            await loop.RunAsync(TimeSpan.FromSeconds(runSeconds));

            return 0;
        }
    }
}
