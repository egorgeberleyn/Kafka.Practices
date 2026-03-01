using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Examples.Producers;

public sealed class KafkaFireAndForgetProducer<TMessage> : IProducer<TMessage>
    where TMessage: class 
{
     private const string TopicName = "demo-fire-forget-produce-topic";
    
    private readonly ProducerConfig _producerCfg = new()
    {
        BootstrapServers = "localhost:29091,localhost:29092,localhost:29093", //указание брокеров Кафки
        
        // Отключаем подтверждения
        Acks = Acks.None,  

        // Можно отключить идемпотентность (т.к. подтверждений всё равно нет)
        EnableIdempotence = false,

        // Увеличиваем производительность
        LingerMs = 5,               // ждем 5ms чтобы собрать батч
        BatchSize = 64 * 1024,      // увеличенный размер батча
        CompressionType = CompressionType.Snappy, // компрессия сообщений
    };

    private readonly IProducer<Null, string> _producer;
    private readonly ILogger<KafkaFireAndForgetProducer<TMessage>> _logger;
    
    public KafkaFireAndForgetProducer(
        ILogger<KafkaFireAndForgetProducer<TMessage>> logger, 
        IKafkaTopicsCreator topicsCreator)
    {
        _logger = logger;
        _producer = new ProducerBuilder<Null, string>(_producerCfg)
            .SetValueSerializer(new JsonValueSerializer<string>()) //установка сериализатора для сообщений Кафки
            .Build();
        topicsCreator.CreateTopicAsync(_producerCfg.BootstrapServers, GetTopicSpecification());
    }
    
    //Пример fire and forget отправки в Кафку
    public Task ProduceAsync(TMessage message, CancellationToken cancellationToken)
    {
        try
        {
            _producer.Produce(TopicName, new Message<Null, string> { Value = JsonSerializer.Serialize(message) });
        }
        catch (ProduceException<string, string> e) //Контролировать ошибки через ProduceException
        {
            _logger.LogError("Ошибка отправки: {ErrorReason}", e.Error.Reason);
        }
        
        return Task.CompletedTask;
    }

    private static TopicSpecification GetTopicSpecification()
    {
        return new TopicSpecification
        {
            Name = TopicName,
            NumPartitions = 3,
            ReplicationFactor = 1,
            Configs = new Dictionary<string, string>
            {
                { "min.insync.replicas", "1" }
            }
        };
    }
}