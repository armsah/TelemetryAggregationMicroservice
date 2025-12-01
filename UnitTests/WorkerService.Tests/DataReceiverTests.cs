using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Moq;
using System.Threading.Tasks;
using Xunit;
using WorkerService.Models;


public class DataReceiverTests
{
    private readonly Mock<ILogger<DataReceiver>> _logger = new();
    private readonly Mock<ISqlDatabase> _sql = new();
    private readonly Mock<ITelemetryRepository> _repo = new();
    private readonly Mock<IServiceBusClientWrapper> _client = new();
    private readonly Mock<IServiceBusProcessorWrapper> _processor = new();

    private DataReceiver CreateReceiver()
    {
        _client.Setup(c => c.CreateProcessor(It.IsAny<string>()))
               .Returns(_processor.Object);

        return new DataReceiver(_logger.Object, _client.Object, _sql.Object, _repo.Object, "test-queue");
    }

    [Fact]
    public async Task ValidTelemetry_CompletesMessage()
    {
        var receiver = CreateReceiver();

        var args = new Mock<WorkerService.Models.IMessageArgs>();
        args.Setup(a => a.Body).Returns("{\"DeviceId\":\"dev01\",\"Sequence\":1,\"Temperature\":22,\"Humidity\":44}");

        bool completed = false;
        args.Setup(a => a.CompleteAsync()).Callback(() => completed = true)
                                          .Returns(Task.CompletedTask);

        await receiver.HandleMessageAsync(args.Object);


        Assert.True(completed);

        _sql.Verify(s => s.UpsertDeviceInfoAsync(
            It.IsAny<string>(), It.IsAny<double>(), It.IsAny<double>()), Times.Once);

        _repo.Verify(r => r.SaveTelemetryAsync(
            It.IsAny<string>(), It.IsAny<string>(), It.IsAny<double>(), It.IsAny<double>()), Times.Once);
    }

    [Fact]
    public async Task InvalidJson_AbandonsMessage()
    {
        var receiver = CreateReceiver();

        var args = new Mock<IMessageArgs>();
        args.Setup(a => a.Body).Returns("BAD JSON");

        bool abandoned = false;
        args.Setup(a => a.AbandonAsync()).Callback(() => abandoned = true)
                                         .Returns(Task.CompletedTask);

        await receiver.HandleMessageAsync(args.Object);

        Assert.True(abandoned);
        _sql.Verify(s => s.UpsertDeviceInfoAsync(It.IsAny<string>(), It.IsAny<double>(), It.IsAny<double>()), Times.Never);
        _repo.Verify(r => r.SaveTelemetryAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<double>(), It.IsAny<double>()), Times.Never);
    }
}
