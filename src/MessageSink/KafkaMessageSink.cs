using System.Threading.Channels;
using Confluent.Kafka;

namespace Kafka.Examples.MessageSink;

public class KafkaMessageSink<TMessage> : BackgroundService, IMessageSink<TMessage>
{
    private const int BufferSize = 10;
    private const int FlushIntervalMs = 500;
    private const string TopicName = "test-topic";

    private readonly Channel<TMessage> _channel;
    private readonly IProducer<string, TMessage> _kafkaProducer;
    private readonly List<Task> _buffer = [];
    private readonly ILogger<KafkaMessageSink<TMessage>> _logger;
        
    public KafkaMessageSink(ILogger<KafkaMessageSink<TMessage>> logger)
    {
        var kafkaConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };
        
        _channel = Channel.CreateUnbounded<TMessage>();
        _kafkaProducer = new ProducerBuilder<string, TMessage>(kafkaConfig).Build();
        _logger = logger;
    }
    
    public ValueTask Send(TMessage msg) => _channel.Writer.WriteAsync(msg);
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DrainAsync();
                await Task.Delay(TimeSpan.FromMilliseconds(FlushIntervalMs), stoppingToken);
                await _channel.Reader.WaitToReadAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                await DrainAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError("An error occurred while sending messages to Kafka: {ErrorMessage}", ex.Message);
                throw;
            }
        }
    }

    private async Task DrainAsync()
    {
        do
        {
            _buffer.Clear();
            while (_channel.Reader.TryRead(out var msg)
                   && _buffer.Count < BufferSize)
            {
                _buffer.Add(
                    _kafkaProducer.ProduceAsync(TopicName, new Message<string, TMessage> { Value = msg }));
            }
            
            if(_buffer.Count == 0) return;

            await Task.WhenAll(_buffer);
        } while (_buffer.Count == BufferSize);
    }
}