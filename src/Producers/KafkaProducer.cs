using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Examples.Producers;

public sealed class KafkaProducer : BackgroundService
{
    private const string TopicName = "demo-topic";
    
    private readonly ProducerConfig _producerCfg = new()
    {
        BootstrapServers = "localhost:29091,localhost:29092,localhost:29093", //указание брокеров Кафки
        Acks = Acks.Leader, //указание acks параметра
        LingerMs = 50, //задержка на отправку сообщения в Кафку
        BatchNumMessages = 100_000, // макс.размер пачки сообщений, которая отправляется в Кафку
    };

    private readonly IProducer<Null, string> _producer;
    private readonly ILogger<KafkaProducer> _logger;

    public KafkaProducer(ILogger<KafkaProducer> logger, IKafkaTopicsCreator topicsCreator)
    {
        _logger = logger;
        _producer = new ProducerBuilder<Null, string>(_producerCfg)
            .SetPartitioner(TopicName, (topicName, partitionCount, keyData, keyIsNull) => //стратегия распределения по партициям
            {
                var keyString = Encoding.UTF8.GetString(keyData.ToArray());
                return int.Parse(keyString.Split(" ").Last()) % partitionCount;
            })
            .SetValueSerializer(new JsonValueSerializer<string>()) //установка сериализатора для сообщений Кафки
            .Build();
        topicsCreator.CreateTopicAsync(_producerCfg.BootstrapServers, GetTopicSpecification());
    }
    
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var random = new Random();
        var counter = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            var batchSize = GetBatchSize(random);

            var messages = Enumerable
                .Range(counter, batchSize)
                .Select(i => $"Message #{i} at {DateTime.UtcNow:O}");

            counter += batchSize;

            var options = new ParallelOptions
            {
                MaxDegreeOfParallelism = 20,
                CancellationToken = cancellationToken
            };

            await Parallel.ForEachAsync(
                messages,
                options,
                async (message, token) =>
                {
                    _logger.LogInformation("Produced: {Message}", message);

                    await _producer.ProduceAsync(
                        "demo-topic",
                        new Message<Null, string>
                        {
                            Headers = new Headers
                            {
                                {"Producer", Encoding.Default.GetBytes("KafkaDemoProducer")},
                                {"Machine", Encoding.Default.GetBytes(Environment.MachineName)}
                            },
                            Value = message
                        },
                        token);
                });

            if (counter > 1000)
                throw new OperationCanceledException();
        }
    }

    //Пример синхронной отправки сообщения в Кафку
    private async Task SendSync(CancellationToken stoppingToken)
    {
        try
        {
            var result = await _producer.ProduceAsync(
                TopicName,
                new Message<Null, string> { Value = "Hello Kafka" },
                stoppingToken
            );

            _logger.LogInformation("Сообщение доставлено в {ResultTopicPartitionOffset}", result.TopicPartitionOffset);
        }
        catch (ProduceException<string, string> e)
        {
            _logger.LogError("Ошибка отправки: {ErrorReason}", e.Error.Reason);
        }
    }
    
    private static int GetBatchSize(Random random)
    {
        var batchMessagesCount = random.Next(1, 6);

        if (batchMessagesCount > 4)
            batchMessagesCount = 100;

        return batchMessagesCount;
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