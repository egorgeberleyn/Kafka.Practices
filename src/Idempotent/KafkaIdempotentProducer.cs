using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Examples.Producers;
using Microsoft.Extensions.Options;

namespace Kafka.Examples.Idempotent;

public class KafkaIdempotentProducer<TMessage> : IProducer<TMessage>
    where TMessage : KafkaEvent
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<KafkaIdempotentProducer<TMessage>> _logger;

    public KafkaIdempotentProducer(
        IOptions<KafkaOptions> kafkaOptions, 
        ILogger<KafkaIdempotentProducer<TMessage>> logger, 
        IKafkaTopicsCreator topicsCreator)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
        
        var producerCfg = new ProducerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            Acks = Acks.All, //ждем подтверждения от всех in-sync-реплик
            EnableIdempotence = true, //включаем идемпотентность на стороне продюсера
            
            //алгоритм выбора партиции для сообщения (random, consistent, consistent_random, murmur, murmur_random)
            Partitioner = Partitioner.ConsistentRandom,
            
            //кол-во in-flight запросов (для поддержки идемпотентности не больше 5, лучше меньше)
            MaxInFlight = 5, 
            
            // увеличиваем число повторных попыток и паузу между ретраями
            MessageSendMaxRetries = 5,    
            RetryBackoffMs = 100,
        
            // небольшая задержка для батчинга и размер батча (опционально)
            LingerMs = 5,                 
            BatchSize = 32 * 1024
        };
        
        _producer = new ProducerBuilder<string, string>(producerCfg)
            .SetValueSerializer(new JsonValueSerializer<string>()) //установка сериализатора для сообщений Кафки
            .Build();
        topicsCreator.CreateTopicAsync(producerCfg.BootstrapServers, GetTopicSpecification());
    }
    
    public async Task ProduceAsync(TMessage message, CancellationToken cancellationToken)
    {
        try
        {
            var deliveryResult = await _producer.ProduceAsync(
                _kafkaOptions.TopicName,
                new Message<string, string>
                {
                    Key = message.Id, //чтобы работала идемпотентность на консьюмере передаем уникальный per entity ключ сообщения
                    Headers = new Headers
                    {
                        {"Producer", Encoding.Default.GetBytes("KafkaIdempotentProducer")},
                        {"Machine", Encoding.Default.GetBytes(Environment.MachineName)}
                    },
                    Value = JsonSerializer.Serialize(message)
                },
                cancellationToken
            );

            _logger.LogInformation("Message produced in {Partition} partition with {Offset}", 
                deliveryResult.Partition.Value,
                deliveryResult.Offset.Value);
        }
        catch (ProduceException<string, string> e)
        {
            _logger.LogError("Produce error: {ErrorReason}", e.Error.Reason);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Internal error");
        }
    }

    private TopicSpecification GetTopicSpecification()
    {
        return new TopicSpecification
        {
            Name = _kafkaOptions.TopicName,
            NumPartitions = 3,
            ReplicationFactor = 3,
            Configs = new Dictionary<string, string>
            {
                { "min.insync.replicas", "2" }
            }
        };
    }
}