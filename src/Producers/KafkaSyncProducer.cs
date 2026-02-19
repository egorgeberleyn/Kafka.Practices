using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Examples.Producers;

public sealed class KafkaSyncProducer : BackgroundService
{
    private const string TopicName = "demo-sync-produce-topic";
    
    private readonly ProducerConfig _producerCfg = new()
    {
        BootstrapServers = "localhost:29091,localhost:29092,localhost:29093", //указание брокеров Кафки
        Acks = Acks.All,              // ждём подтверждения от всех реплик
        EnableIdempotence = true,     // гарантируем exactly-once семантику
        
        // увеличиваем число повторных попыток и паузу между ретраями
        MessageSendMaxRetries = 5,    
        RetryBackoffMs = 100,
        
        // небольшая задержка для батчинга и размер батча (опционально)
        LingerMs = 5,                 
        BatchSize = 32 * 1024
    };

    private readonly IProducer<Null, string> _producer;
    private readonly ILogger<KafkaSyncProducer> _logger;

    public KafkaSyncProducer(ILogger<KafkaSyncProducer> logger, IKafkaTopicsCreator topicsCreator)
    {
        _logger = logger;
        _producer = new ProducerBuilder<Null, string>(_producerCfg)
            .SetValueSerializer(new JsonValueSerializer<string>()) //установка сериализатора для сообщений Кафки
            .Build();
        topicsCreator.CreateTopicAsync(_producerCfg.BootstrapServers, GetTopicSpecification());
    }
    
    //Пример синхронной отправки сообщения в Кафку
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        try
        {
            var result = await _producer.ProduceAsync(
                TopicName,
                new Message<Null, string> { Value = "Hello Kafka" },
                cancellationToken
            );

            _logger.LogInformation("Сообщение доставлено в {ResultTopicPartitionOffset}", result.TopicPartitionOffset);
        }
        catch (ProduceException<string, string> e) //Контролировать ошибки через ProduceException
        {
            _logger.LogError("Ошибка отправки: {ErrorReason}", e.Error.Reason);
        }
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

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _producer.Flush(cancellationToken);
        _producer.Dispose();
        
        return Task.CompletedTask;
    }
}