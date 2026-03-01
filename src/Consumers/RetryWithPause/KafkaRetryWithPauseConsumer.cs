using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Kafka.Examples.Consumers.RetryWithPause;

//консьмер с обработкой ошибок через паузу консьюма
public class KafkaRetryWithPauseConsumer<TMessage> : BackgroundService
    where TMessage : class
{
    private const int WorkerCount = 4;
    private const int RetryMessageCount = 5;
    private readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(1);
    
    private readonly KafkaOptions _kafkaOptions;
    private readonly IConsumer<Null, TMessage> _consumer;
    private readonly ILogger<KafkaRetryWithPauseConsumer<TMessage>> _logger;
    
    private readonly Channel<KafkaMessage<TMessage>> _messageBuffer;

    public KafkaRetryWithPauseConsumer(
        IOptions<KafkaOptions> kafkaOptions,
        ILogger<KafkaRetryWithPauseConsumer<TMessage>> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;

        var consumerCfg = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest, //указание с какого offset'a начинать чтение
            EnableAutoCommit = false, //явно коммитим сообщение при обработке
        };

        _consumer = new ConsumerBuilder<Null, TMessage>(consumerCfg)
            .SetValueDeserializer(new JsonValueSerializer<TMessage>()) //установка десериализатора для сообщений Кафки
            .Build();
        
        _messageBuffer = Channel.CreateUnbounded<KafkaMessage<TMessage>>();
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var workerTasks = Array.Empty<Task>();
        
        try
        {
            _consumer.Subscribe(_kafkaOptions.TopicName); //подписка consumer_group на topic
        
            workerTasks = Enumerable.Range(0, WorkerCount)
                .Select(_ => Task.Run(() => WorkerTask(ct), ct))
                .ToArray();

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));
                    if (consumeResult is null)
                        continue;

                    var message = new KafkaMessage<TMessage>(
                        consumeResult.Message.Value, 
                        consumeResult.TopicPartition,
                        consumeResult.Offset,
                        consumeResult.Message.Timestamp);

                    Enqueue(message);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consume failed with error. Error code : {ErrorCode}. Reason {Reason}",
                        ex.Error.Code, ex.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer stopping..");
        }
        finally
        {
            _messageBuffer.Writer.Complete();

            await Task.WhenAll(workerTasks);

            _consumer.Close();
            _consumer.Dispose();
        }
    }
    
    private void Enqueue(KafkaMessage<TMessage> kafkaMessage)
        => _messageBuffer.Writer.TryWrite(kafkaMessage);

    private async Task WorkerTask(CancellationToken ct)
    {
        await foreach (var msg in _messageBuffer.Reader.ReadAllAsync(ct))
        {
            try
            {
                await ProcessMessageAsync(msg, ct);

                _consumer.Commit([new TopicPartitionOffset(msg.Partition, msg.Offset + 1)]);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Processing failed → pause partition");

                // останавливаем дальнейший приём сообщений
                _consumer.Pause([msg.Partition]);

                // пытаемся повторить обработку
                await RetryProcessMessageAsync(msg, ct);

                // возобновляем приём после успешного восстановления
                _consumer.Resume([msg.Partition]);
            }
        }
    }

    private async Task ProcessMessageAsync(KafkaMessage<TMessage> message, CancellationToken ct)
    {
        await Task.Delay(Random.Shared.Next(50, 200), ct); // имитация обработки сообщения

        _logger.LogInformation(
            "Consumed message [{Timestamp}]: {Value}", message.Timestamp, message.Value);
    }

    //Пробуем обработать retryCount кол-во раз, если не вышло бросаем сообщение в dlq и скипаем
    private async Task RetryProcessMessageAsync(KafkaMessage<TMessage> message, CancellationToken ct)
    {
        var retryDelay = _retryDelay;
        for (var i = 0; i < RetryMessageCount; i++)
        {
            try
            {
                await ProcessMessageAsync(message, ct);
                _consumer.Commit([new TopicPartitionOffset(message.Partition, message.Offset + 1)]);
                return;
            }
            catch
            {
                await Task.Delay(retryDelay, ct);
                retryDelay *= 2;
            }
        }
        
        //send dlq & skip message
    }
}