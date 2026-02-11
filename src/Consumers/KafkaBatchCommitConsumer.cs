using System.Threading.Channels;
using Confluent.Kafka;
using Kafka.Examples.Consumers.BatchCommit;

namespace Kafka.Examples.Consumers;

public sealed record KafkaMessage(
    string Value,
    TopicPartition Partition,
    Offset Offset);

//Обработка по схеме consume → enqueue → consume → enqueue → worker pool → commit batch
public class KafkaBatchCommitConsumer : BackgroundService
{
    private readonly IConsumer<Null, string> _consumer;
    private readonly OffsetsManager _offsetsManager;
    private readonly Channel<KafkaMessage> _commitLog; // Внутренняя очередь сообщений на обработку воркерами
    private readonly ILogger<KafkaBatchCommitConsumer> _logger;
    
    private const int WorkerCount = 4;
    private const int CommitLogSize = 1000;
    private static readonly TimeSpan CommiterDelay = TimeSpan.FromSeconds(3);
    
    private readonly ConsumerConfig _consumerCfg = new()
    {
        BootstrapServers = KafkaMetadata.BootstrapServers,
        GroupId = KafkaMetadata.ConsumerGroupId, //Индентификатор группы потребителей.
                                                 //Изменение идентификатора приведет к переобработке всех обработанных сообщений из топика
        SessionTimeoutMs = 30_000,
        AutoOffsetReset = AutoOffsetReset.Earliest, //указание с какого offset'a начинать чтение
        MaxPollIntervalMs = 5 * 60 * 1000, //время на обработку одного сообщения для consumer'а
        EnableAutoCommit = false //явно коммитим сообщение при обработке
    };

    public KafkaBatchCommitConsumer(ILogger<KafkaBatchCommitConsumer> logger)
    {
        _logger = logger;
        _consumer = new ConsumerBuilder<Null, string>(_consumerCfg).Build();
        _offsetsManager = new OffsetsManager(_consumer);
        _commitLog = Channel.CreateBounded<KafkaMessage>(new BoundedChannelOptions(CommitLogSize)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var workerTasks = Array.Empty<Task>();
        Task? commitTask = null;
        
        try
        {
            _consumer.Subscribe(KafkaMetadata.TopicName); //подписка consumer_group на topic
            
            _logger.LogInformation("Kafka consumer started for topic {Topic}", KafkaMetadata.TopicName);
            
            workerTasks = Enumerable.Range(0, WorkerCount)
                .Select(_ => Task.Run(() => WorkerTask(ct), ct))
                .ToArray();

            commitTask = Task.Run(() => CommiterTask(ct), ct);
            
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(ct);

                    var msg = new KafkaMessage(
                        consumeResult.Message.Value,
                        consumeResult.TopicPartition,
                        consumeResult.Offset);

                    await _commitLog.Writer.WriteAsync(msg, ct);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consume error");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer stopping..");
        }
        finally
        {
            _commitLog.Writer.Complete();

            await Task.WhenAll(workerTasks);
            if (commitTask != null) await commitTask;

            _consumer.Close();
            _consumer.Dispose();
        }
    }
    
    private async Task ProcessMessageAsync(KafkaMessage msg, CancellationToken ct)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(200), ct); 
        _logger.LogInformation(
            "Processed {Message}, Partition: {Partition}, Offset: {Offset}", msg.Value,
            msg.Partition.Partition.Value, msg.Offset.Value);
    }
    
    private async Task WorkerTask(CancellationToken ct)
    {
        await foreach (var msg in _commitLog.Reader.ReadAllAsync(ct))
        {
            try
            {
                await ProcessMessageAsync(msg, ct); // имитация обработки сообщения
                _offsetsManager.MarkProcessed(msg.Partition, msg.Offset); // помечаем сообщение как обработанное и добавляем его в коммит-лог
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Processing failed");
            }
        }
    }

    private async Task CommiterTask(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(CommiterDelay, ct); //ждем время CommiterDelay, потом коммитим все сообщения из коммит-лога

            try
            {
                _offsetsManager.Commit();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Commit failed");
            }
        }
    }
}