using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Examples.Producers;
using KafkaProtobuf.Grpc;
using Microsoft.Extensions.Options;

namespace Kafka.Examples.Protobuf;

public class KafkaProtobufProducer : IProducer<OrderEvent>
{
    private readonly IProducer<long, OrderEvent> _producer;
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<KafkaProtobufProducer> _logger;

    public KafkaProtobufProducer(
        IOptions<KafkaOptions> kafkaOptions, 
        ILogger<KafkaProtobufProducer> logger,
        IKafkaTopicsCreator topicsCreator)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
        
        var producerCfg = new ProducerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers, //указание брокеров Кафки
            Acks = Acks.Leader, //указание acks параметра
            //LingerMs = 50, - задержка на отправку сообщения в Кафку
            //BatchNumMessages = 100_000 - макс.размер пачки сообщений, которая отправляется в Кафку
        };
        
        _producer = new ProducerBuilder<long, OrderEvent>(producerCfg)
            .SetValueSerializer(new ProtobufSerializer<OrderEvent>()) //установка сериализатора для protobuf-сообщений Кафки
            .Build();
        topicsCreator.CreateTopicAsync(producerCfg.BootstrapServers, GetTopicSpecification());
    }
    
    public async Task ProduceAsync(OrderEvent orderEvent, CancellationToken cancellationToken)
    {
        await _producer.ProduceAsync(_kafkaOptions.TopicName, new Message<long, OrderEvent>
        {
            Headers = new Headers
            {
                {"Producer", Encoding.Default.GetBytes("KafkaTestProducer")},
                {"Machine", Encoding.Default.GetBytes(Environment.MachineName)}
            },
            Key = orderEvent.OrderId,
            Value = orderEvent
        }, cancellationToken);
        
        _logger.LogInformation("Order event with id={Id}", orderEvent.OrderId);
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