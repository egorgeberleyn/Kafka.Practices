using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Examples.Producers;

//Паттерн consume–process–produce
public sealed class KafkaTransactionalProducer : BackgroundService
{
    private const string InitialTopicName = "initial-demo-topic";
    private const string DestinationTopicName = "destination-demo-topic";

    private readonly ConsumerConfig _consumerConfig = new()
    {
        BootstrapServers = "localhost:9092",
        GroupId = "processor-group",
        EnableAutoCommit = false, // offsets фиксируем транзакционно
        IsolationLevel = IsolationLevel.ReadCommitted // консьюмер с isolation.level=read_committed видит только закоммиченные транзакции
    };
    
    private readonly ProducerConfig _producerCfg = new()
    {
        BootstrapServers = "localhost:29091,localhost:29092,localhost:29093", //указание брокеров Кафки
        Acks = Acks.All,
        EnableIdempotence = true,
        TransactionalId = "producer-tx-1", // обязательный параметр для транзакций
    };

    private readonly IConsumer<string, string> _consumer;
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaTransactionalProducer> _logger;

    public KafkaTransactionalProducer(ILogger<KafkaTransactionalProducer> logger, IKafkaTopicsCreator topicsCreator)
    {
        _logger = logger;
        _consumer = new ConsumerBuilder<string, string>(_consumerConfig)
            .SetValueDeserializer(new JsonValueSerializer<string>()) //установка десериализатора для сообщений Кафки
            .Build();
        _consumer.Subscribe(InitialTopicName);
        
        _producer = new ProducerBuilder<string, string>(_producerCfg)
            .SetValueSerializer(new JsonValueSerializer<string>()) //установка сериализатора для сообщений Кафки
            .Build();
        topicsCreator.CreateTopicAsync(_producerCfg.BootstrapServers, GetTopicSpecification());
        _producer.InitTransactions(TimeSpan.FromSeconds(10));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!stoppingToken.IsCancellationRequested)
        {
            // читаем из исходного топика
            var message = _consumer.Consume(stoppingToken);
            
            _producer.BeginTransaction();
            
            // пишем в следующий топик
            await _producer.ProduceAsync(DestinationTopicName, new Message<string, string>
            {
                Key = "test-key",
                Value = "test-value"
            }, stoppingToken);
            
            // фиксируем offset вместе с транзакцией
            _producer.SendOffsetsToTransaction(
                [new TopicPartitionOffset(message.TopicPartition, message.Offset + 1)],
                _consumer.ConsumerGroupMetadata,
                TimeSpan.FromSeconds(5)
            );

            // подтвержаем отправку в следующий топик
            _producer.CommitTransaction();
            
            //Так достигается атомарность:
            //если приложение упадёт до коммита → сообщение снова попадёт в обработку, но без дублей в выходном топике.
        }
    }
    
    private static TopicSpecification GetTopicSpecification()
    {
        return new TopicSpecification
        {
            Name = DestinationTopicName,
            NumPartitions = 3,
            ReplicationFactor = 1,
            Configs = new Dictionary<string, string>
            {
                { "min.insync.replicas", "2" }
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