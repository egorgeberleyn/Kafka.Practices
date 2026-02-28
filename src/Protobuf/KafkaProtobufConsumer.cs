using Confluent.Kafka;
using KafkaProtobuf.Grpc;
using Microsoft.Extensions.Options;

namespace Kafka.Examples.Protobuf;

public class KafkaProtobufConsumer : BackgroundService
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly IConsumer<long, OrderEvent> _consumer;
    private readonly ILogger<KafkaProtobufConsumer> _logger;
    
    public KafkaProtobufConsumer(IOptions<KafkaOptions> kafkaOptions, ILogger<KafkaProtobufConsumer> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
        
         var consumerCfg = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest, //указание с какого offset'a начинать чтение
            MaxPollIntervalMs = 1 * 60 * 1000, //время на обработку одного сообщения для consumer'а
            AutoCommitIntervalMs = 5 * 1000,

            //более надежно отключать эти параметры и выполнять коммит вручную,
            //но это влияет на производительность
            EnableAutoCommit = false, //фиксация обработки сообщения и установка offset'а в брокере
            EnableAutoOffsetStore = false //фиксация и хранение offset'а в приложении
        };
        
        _consumer = new ConsumerBuilder<long, OrderEvent>(consumerCfg)
            .SetValueDeserializer(new ProtobufSerializer<OrderEvent>()) //установка десериализатора для protobuf-сообщений Кафки
            .Build();
    }
    
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _consumer.Subscribe(_kafkaOptions.TopicName); //подписка consumer_group на topic
        
        while (_consumer.Consume(ct) is { } result)
        {
            ct.ThrowIfCancellationRequested();

            await Task.Delay(1000, ct);
            _consumer.Commit(result);
    
            //Самостоятельная команда на фиксацию сообщения в приложении (EnableAutoOffsetStore=false, но EnableAutoCommit=true)
            //consumer.StoreOffset(result); 

            _logger.LogInformation("Consumed message with id={Id}", result.Message.Key);
        }
    }
}