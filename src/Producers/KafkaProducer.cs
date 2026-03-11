using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;

namespace Kafka.Examples.Producers;

public sealed class KafkaProducer<TMessage> : IProducer<TMessage>
    where TMessage: class 
{
    private readonly IProducer<Null, string> _producer;
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<KafkaProducer<TMessage>> _logger;

    public KafkaProducer(
        IOptions<KafkaOptions> kafkaOptions, 
        ILogger<KafkaProducer<TMessage>> logger, 
        IKafkaTopicsCreator topicsCreator)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
        
        var producerCfg = new ProducerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers, //указание брокеров Кафки
            Acks = Acks.All,  // ждём подтверждения от всех in-sync реплик
            EnableIdempotence = true,     // включаем идемпотентность (гарантируем exactly-once семантику на стороне продюсера)
            
            
            BatchNumMessages = 1_000, // макс кол-во сообщений в батче, который отправляется в Кафку (опционально)
            BatchSize = 32 * 1024, // размер батча сообщений в байтах (опционально)
            
            //задержка на отправку пачки сообщений в Кафку (опционально)
            LingerMs = 50, 
            
            // число повторных попыток и пауза между ретраями (опционально)
            MessageSendMaxRetries = 3,    
            RetryBackoffMs = 100,
            
            //Partitioner = Partitioner.ConsistentRandom // выбор стратегии распределения сообщений по партициям, можно настроить свою ч-з SetPartitioner
        };
        
        _producer = new ProducerBuilder<Null, string>(producerCfg)
            .SetPartitioner(_kafkaOptions.TopicName, (topicName, partitionCount, keyData, keyIsNull) => //настройка своей стратегии распределения по партициям
            {
                var keyString = Encoding.UTF8.GetString(keyData.ToArray());
                return int.Parse(keyString.Split(" ").Last()) % partitionCount;
            })
            .SetValueSerializer(new JsonValueSerializer<string>()) //установка сериализатора для сообщений Кафки
            .Build();
        topicsCreator.CreateTopicAsync(producerCfg.BootstrapServers, GetTopicSpecification());
    }
    
    //Пример отправки сообщения в Кафку
    public async Task ProduceAsync(TMessage message, CancellationToken cancellationToken)
    {
        try
        {
            var deliveryResult = await _producer.ProduceAsync(
                _kafkaOptions.TopicName,
                new Message<Null, string>
                {
                    Headers = new Headers
                    {
                        {"Producer", Encoding.Default.GetBytes("KafkaDemoProducer")},
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
        catch (ProduceException<string, string> e) //Контролируем ошибки через ProduceException
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
            ReplicationFactor = 3, //для повышения отказоустойчивости репликация на 3 брокера
            Configs = new Dictionary<string, string>
            {
                { "min.insync.replicas", "2" } // на одну меньше чем replication.factor
            }
        };
    }
}