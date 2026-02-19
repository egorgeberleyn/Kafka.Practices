using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;

namespace Kafka.Examples.OutOfOrderCommit;

public class KafkaOutOfOrderProducer : BackgroundService
{
    private readonly IProducer<Null, string> _producer;
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<KafkaOutOfOrderProducer> _logger;

    public KafkaOutOfOrderProducer(
        IOptions<KafkaOptions> kafkaOptions, 
        ILogger<KafkaOutOfOrderProducer> logger,
        IKafkaTopicsCreator topicsCreator)
    {
        _logger = logger;
        _kafkaOptions = kafkaOptions.Value;
        
        var producerCfg = new ProducerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers, //указание брокеров Кафки
            Acks = Acks.All, //указание acks параметра
            LingerMs = 50, //задержка на отправку сообщения в Кафку
            BatchNumMessages = 100_000, // макс.размер пачки сообщений, которая отправляется в Кафку
            EnableIdempotence = true, // включение идемпотентного продюсера 
            MaxInFlight = 5
        };
        
        _producer = new ProducerBuilder<Null, string>(producerCfg)
            .SetValueSerializer(new JsonValueSerializer<string>()) //установка сериализатора для сообщений Кафки
            .Build();
        topicsCreator.CreateTopicAsync(producerCfg.BootstrapServers, GetTopicSpecification());
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        long counter = 0;
        
        while (!ct.IsCancellationRequested)
        {
            await _producer.ProduceAsync(
                _kafkaOptions.TopicName,
                new Message<Null, string>
                {
                    Headers = new Headers
                    {
                        {"producer", Encoding.Default.GetBytes("KafkaOutOfOrderProducer")},
                        {"machine", Encoding.Default.GetBytes(Environment.MachineName)},
                        {"sequenceId", Encoding.Default.GetBytes(counter.ToString())}
                    },
                    Value = $"Message #{counter} at {DateTime.UtcNow:O}"
                },
                ct);

            counter++;
            
            if (counter > 1000)
                throw new OperationCanceledException();
        }
        
    }
    
    private TopicSpecification GetTopicSpecification()
    {
        return new TopicSpecification
        {
            Name = _kafkaOptions.TopicName,
            NumPartitions = 3,
            ReplicationFactor = 2,
            Configs = new Dictionary<string, string>
            {
                { "min.insync.replicas", "2" }
            }
        };
    }
}