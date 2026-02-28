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
            Acks = Acks.Leader, //указание acks параметра
            BatchNumMessages = 100_000, // макс.размер пачки сообщений, которая отправляется в Кафку
            LingerMs = 50, //задержка на отправку пачки сообщений в Кафку
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
            ReplicationFactor = 1,
            Configs = new Dictionary<string, string>
            {
                { "min.insync.replicas", "1" }
            }
        };
    }
}