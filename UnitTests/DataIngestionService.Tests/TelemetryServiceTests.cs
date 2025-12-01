using Azure.Messaging.ServiceBus;
using DataIngestionService;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Xunit;
using static Microsoft.Azure.Amqp.Serialization.SerializableType;

namespace FakeDeviceDataSender.Tests
{
    public class TelemetryServiceTests
    {
        [Fact]
        public void DeviceSeeder_ShouldCreateCorrectNumberOfDevices()
        {
            var rnd = new Random(123);
            var devices = DeviceSeeder.Seed(5, rnd);

            Assert.Equal(5, devices.Count);
            foreach (var d in devices)
            {
                Assert.StartsWith("device-", d.DeviceId);
                Assert.InRange(d.BaseTemp, 18, 28);
                Assert.InRange(d.BaseHumidity, 30, 60);
            }
        }

        [Fact]
        public void TelemetryGenerator_ShouldGenerateValidTelemetry()
        {
            var rnd = new Random(42);
            var device = new Device { DeviceId = "device-1001", BaseTemp = 20, BaseHumidity = 40, Seq = 0 };
            var telemetry = TelemetryGenerator.Generate(device, rnd);

            Assert.Equal("device-1001", telemetry.DeviceId);
            Assert.Equal(1, telemetry.Sequence); // Seq incremented
            Assert.InRange(telemetry.Temperature, 18, 22);
            Assert.InRange(telemetry.Humidity, 35, 45);
            Assert.True((DateTime.UtcNow - telemetry.Timestamp).TotalSeconds < 1);
        }

        [Fact]
        public void TelemetryMessage_ToServiceBusMessage_ShouldSetPropertiesCorrectly()
        {
            var telemetry = new TelemetryMessage
            {
                DeviceId = "device-1002",
                Sequence = 5,
                Temperature = 21.5,
                Humidity = 42
            };

            var msg = telemetry.ToServiceBusMessage();

            Assert.Equal("telemetry", msg.Subject);
            Assert.Equal("device-1002", msg.ApplicationProperties["deviceId"]);
            Assert.Equal("telemetry", msg.ApplicationProperties["type"]);

            var json = Encoding.UTF8.GetString(msg.Body.ToArray());
            var deserialized = JsonSerializer.Deserialize<TelemetryMessage>(json);
            Assert.NotNull(deserialized);
            Assert.Equal(telemetry.DeviceId, deserialized!.DeviceId);
            Assert.Equal(telemetry.Sequence, deserialized.Sequence);
            Assert.Equal(telemetry.Temperature, deserialized.Temperature);
            Assert.Equal(telemetry.Humidity, deserialized.Humidity);
        }

        [Fact]
        public async Task TelemetryLoop_RunAsync_ShouldCallSender()
        {
            var rnd = new Random(123);
            var devices = DeviceSeeder.Seed(2, rnd);

            // Capture messages sent
            var sentMessages = new List<ServiceBusMessage>();

            //Mock the wrapper interface
            var mockSender = new Mock<IServiceBusSenderWrapper>();
            mockSender.Setup(s => s.SendMessagesAsync(It.IsAny<IEnumerable<ServiceBusMessage>>(), It.IsAny<CancellationToken>()))
                      .Callback<IEnumerable<ServiceBusMessage>, CancellationToken>((msgs, _) => sentMessages.AddRange(msgs))
                      .Returns(Task.CompletedTask);

            var cts = new CancellationTokenSource();
            var loop = new TelemetryLoop(devices, mockSender.Object, msgsPerSecond: 3, cts.Token, useBatch: false);

            // Run briefly
            await loop.RunAsync(TimeSpan.FromSeconds(1));

            // Verify wrapper calls
            mockSender.Verify(s => s.SendMessagesAsync(It.IsAny<IEnumerable<ServiceBusMessage>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);

            // Assert that some messages were actually captured
            Assert.True(sentMessages.Count > 0);
        }
    }
}
