using Confluent.Kafka;

namespace Kafka.Examples.Consumers;

//Обработка по схеме consume → process → commit → consume → process → commit
public class KafkaConsumer : BackgroundService
{
    private readonly IConsumer<Null, string> _consumer;
    private readonly ILogger<KafkaConsumer> _logger;
    
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

    public KafkaConsumer(ILogger<KafkaConsumer> logger)
    {
        _logger = logger;
        _consumer = new ConsumerBuilder<Null, string>(_consumerCfg).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            _consumer.Subscribe(KafkaMetadata.TopicName); //подписка consumer_group на topic
            
            _logger.LogInformation("Kafka consumer started for topic {Topic}", KafkaMetadata.TopicName);
            
            while (!stoppingToken.IsCancellationRequested)
            {
                await ProcessMessageAsync(stoppingToken);
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

    private async Task ProcessMessageAsync(CancellationToken stoppingToken)
    {
        try
        {
            var result = _consumer.Consume(stoppingToken);

            await Task.Delay(1000, stoppingToken); // имитация обработки сообщения
            _consumer.Commit(result);

            _logger.LogInformation(
                "Consumed {Message}, Partition: {Partition}, Offset: {Offset}", result.Message.Value,
                result.Partition.Value, result.Offset.Value);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Consume error");
        }
    }
}