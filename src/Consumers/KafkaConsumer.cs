using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Kafka.Examples.Consumers;

//Обработка по схеме consume → process → commit → consume → process → commit
public class KafkaConsumer<TMessage> : BackgroundService
    where TMessage: class 
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly IConsumer<Null, TMessage> _consumer;
    private readonly ILogger<KafkaConsumer<TMessage>> _logger;

    public KafkaConsumer(IOptions<KafkaOptions> kafkaOptions, ILogger<KafkaConsumer<TMessage>> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
        
         var consumerCfg = new ConsumerConfig()
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.ConsumerGroupId, //Индентификатор группы потребителей.
            //Изменение идентификатора приведет к переобработке всех обработанных сообщений из топика
            SessionTimeoutMs = 30_000,
            AutoOffsetReset = AutoOffsetReset.Earliest, //указание с какого offset'a начинать чтение
            MaxPollIntervalMs = 5 * 60 * 1000, //время на обработку одного сообщения для consumer'а
            EnableAutoCommit = false //явно коммитим сообщение при обработке
        };
        
        _consumer = new ConsumerBuilder<Null, TMessage>(consumerCfg)
            .SetValueDeserializer(new JsonValueSerializer<TMessage>()) //установка десериализатора для сообщений Кафки
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
                await ProcessMessageAsync(ct);
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

    private async Task ProcessMessageAsync(CancellationToken ct)
    {
        try
        {
            var result = _consumer.Consume(ct);

            await Task.Delay(Random.Shared.Next(50,200), ct); // имитация обработки сообщения
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
            // Стратегии обработки ошибок: retry (delay topic) / dlq (dead letter queue) / skip + commit
        }
    }
}