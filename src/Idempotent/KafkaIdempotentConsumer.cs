using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Kafka.Examples.Idempotent;

public class KafkaIdempotentConsumer<TMessage> : BackgroundService
    where TMessage: KafkaEvent 
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly IConsumer<string, TMessage> _consumer;
    private readonly IMessageDeduplicator _messageDeduplicator;
    private readonly NpgsqlDataSource _dataSource;
    private readonly ILogger<KafkaIdempotentConsumer<TMessage>> _logger;

    public KafkaIdempotentConsumer(
        IOptions<KafkaOptions> kafkaOptions, 
        IMessageDeduplicator messageDeduplicator,
        ILogger<KafkaIdempotentConsumer<TMessage>> logger, 
        NpgsqlDataSource dataSource)
    {
        _kafkaOptions = kafkaOptions.Value;
        _messageDeduplicator = messageDeduplicator;
        _logger = logger;
        _dataSource = dataSource;

        var consumerCfg = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.ConsumerGroupId,
            SessionTimeoutMs = 30_000,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, //явно коммитим сообщение при обработке
            EnablePartitionEof = false
        };
        
        _consumer = new ConsumerBuilder<string, TMessage>(consumerCfg)
            .SetValueDeserializer(new JsonValueSerializer<TMessage>())
            .Build();
    }
    
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        try
        {
            _consumer.Subscribe(_kafkaOptions.TopicName); //подписка consumer_group на topic
            
            _logger.LogInformation("Kafka consumer started for topic {Topic}", _kafkaOptions.TopicName);
            
            while (!ct.IsCancellationRequested)
            {
                await using var dbConnection = await _dataSource.OpenConnectionAsync(ct);
                await using var dbTransaction = await dbConnection.BeginTransactionAsync(ct);
                
                var consumeResult = _consumer.Consume(ct);

                if (!await _messageDeduplicator.TryMarkProcessedAsync(dbConnection, consumeResult.Message.Key,
                        ct))
                {
                    _logger.LogWarning("Duplicate message with id={Id}", consumeResult.Message.Key);
                    _consumer.Commit(consumeResult);

                    await dbTransaction.RollbackAsync(ct);
                    continue;
                }

                await ProcessMessageAsync(consumeResult, ct);
                
                await dbTransaction.CommitAsync(ct);
                _consumer.Commit(consumeResult);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer stopping");
        }
        finally
        {
            _consumer.Close();
            _consumer.Dispose();
        }
    }

    private async Task ProcessMessageAsync(ConsumeResult<string, TMessage> consumeResult, CancellationToken ct)
    {
        try
        {
            var message = consumeResult.Message.Value;

            await Task.Delay(Random.Shared.Next(50,200), ct); // имитация обработки сообщения
            
            _logger.LogInformation("Message with type={EntityType} id={Id} is processed", 
                message.EntityType,
                message.Id);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Processing failed for message with id={Id}", consumeResult.Message.Key);
        }
    }
}