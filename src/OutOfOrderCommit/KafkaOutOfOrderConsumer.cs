using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Kafka.Examples.OutOfOrderCommit;

public class KafkaOutOfOrderConsumer : BackgroundService
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly IConsumer<Null, string> _consumer;
    private readonly ILogger<KafkaOutOfOrderConsumer> _logger;
    private readonly OutOfOrderCommitCoordinator _outOfOrderCommitCoordinator;
    
    private BufferBlock<ConsumeResult<Null,string>> _messageBuffer = null!;
    private ActionBlock<ConsumeResult<Null,string>> _workers = null!;
    
    private const int WorkerCount = 4;
    private const int MessageBufferSize = 1000;

    public KafkaOutOfOrderConsumer(
        IOptions<KafkaOptions> kafkaOptions, 
        ILogger<KafkaOutOfOrderConsumer> logger, 
        IDateTimeProvider dateTimeProvider)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;

        var consumerCfg = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.ConsumerGroupId,
            SessionTimeoutMs = 30_000,
            AutoOffsetReset = AutoOffsetReset.Earliest, //указание с какого offset'a начинать чтение
            MaxPollIntervalMs = 5 * 60 * 1000, //время на обработку одного сообщения для consumer'а
            EnableAutoCommit = false //явно коммитим сообщение при обработке
        };
        
        _consumer = new ConsumerBuilder<Null, string>(consumerCfg).Build();
        _outOfOrderCommitCoordinator = new OutOfOrderCommitCoordinator(_consumer, dateTimeProvider);
    }
    
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _consumer.Subscribe(_kafkaOptions.TopicName);
        _logger.LogInformation("Kafka consumer started for topic {Topic}", _kafkaOptions.TopicName);

        SetupBlocksPipeline(ct);
        
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var msg = _consumer.Consume(ct);
                await _messageBuffer.SendAsync(msg, ct);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer stopping..");
        }

        _messageBuffer.Complete();
        await _workers.Completion;
    }

    private void SetupBlocksPipeline(CancellationToken ct)
    {
        _messageBuffer = new BufferBlock<ConsumeResult<Null,string>>(
            new DataflowBlockOptions
            {
                BoundedCapacity = MessageBufferSize
            });

        _workers = new ActionBlock<ConsumeResult<Null, string>>(
            msg => ProcessMessageAsync(msg, ct),
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = WorkerCount,
                BoundedCapacity = WorkerCount * 2,
                CancellationToken = ct
            });

        _messageBuffer.LinkTo(_workers, new DataflowLinkOptions
        {
            PropagateCompletion = true
        });
    }
    
    private async Task ProcessMessageAsync(ConsumeResult<Null,string> msg, CancellationToken ct)
    {
        try
        {
            await Task.Delay(Random.Shared.Next(50,200), ct); // имитация обработки сообщения
            _outOfOrderCommitCoordinator.MarkProcessed(msg.TopicPartition, msg.Message.Headers.GetSequenceId(), msg.Offset.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Processing failed");
            // Стратегии обработки ошибок: retry (delay topic) / dlq (dead letter queue) / skip + commit
        }
    }
}