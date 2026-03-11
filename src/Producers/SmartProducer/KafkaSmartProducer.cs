using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Examples.Producers.SmartProducer.Partitioning;
using Microsoft.Extensions.Options;

namespace Kafka.Examples.Producers.SmartProducer;

public class KafkaSmartProducer : ISmartProducer
{
    private const int MaxRetryAttempt = 10;
    private const int PartitionCooldownMs = 5000;
    
    private readonly IProducer<Null, byte[]> _producer;
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<KafkaSmartProducer> _logger;
    private readonly IKafkaTopicsManager _topicsManager;
    private readonly IPartitionStateTracker _partitionStateTracker;

    private TopicSpecification TopicSpecification => new()
    {
        Name = _kafkaOptions.TopicName,
        NumPartitions = 3,
        ReplicationFactor = 3, //для повышения отказоустойчивости репликация на 3 брокера
        Configs = new Dictionary<string, string>
        {
            { "min.insync.replicas", "2" } // на одну меньше чем replication.factor
        }
    };
    
    public KafkaSmartProducer(
        IOptions<KafkaOptions> kafkaOptions, 
        ILogger<KafkaSmartProducer> logger, 
        IKafkaTopicsManager topicsManager, 
        IPartitionStateTracker partitionStateTracker)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
        _topicsManager = topicsManager;
        _partitionStateTracker = partitionStateTracker;

        var producerCfg = new ProducerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = 20,
            CompressionType = CompressionType.Snappy,
            MessageTimeoutMs = 6000, // 6 sec, макс.время, которое продюсер будет пытаться доставить сообщение
            MessageMaxBytes = 2097176, // 2 mb, макс.размер одного сообщения
            
            // отключает алгоритм Nagle (TCP оптимизация: маленькие пакеты -> буферизуются -> отправляются вместе)
            // Nagle уменьшает сетевой overhead, но увеличивает latency
            SocketNagleDisable = true,
            
            Partitioner = null // используем свой partitioner при продюсе и явно выбираем партицию
        };
        
        _producer = new ProducerBuilder<Null, byte[]>(producerCfg)
            .SetErrorHandler(OnError)
            .SetLogHandler(OnLog)
            .Build();
    }
    
    public async Task<DeliveryResult<Null, byte[]>> ProduceAsync<TMessage>(
        string topicName, 
        TMessage message,
        CancellationToken cancellationToken)
    {
        var serializedMessage = SerializeMessage(message);
        var attemptCount = 0;

        while (true)
        {
            attemptCount++;
            
            var partition = KafkaSmartPartitioner.ChoosePartition(
                _partitionStateTracker,
                partitionCount: TopicSpecification.NumPartitions);
            
            _partitionStateTracker.OnProduce(partition);
            var startProduce = Stopwatch.GetTimestamp();
        
            try
            {
                var deliveryResult = await _producer.ProduceAsync(
                    new TopicPartition(_kafkaOptions.TopicName, partition),
                    new Message<Null, byte[]>
                    {
                        Headers = new Headers
                        {
                            { "Producer", Encoding.Default.GetBytes("KafkaSmartProducer") },
                            { "Machine", Encoding.Default.GetBytes(Environment.MachineName) }
                        },
                        Value = serializedMessage,
                    },
                    cancellationToken
                );
            
                var produceLatency = Stopwatch.GetElapsedTime(startProduce);
                _partitionStateTracker.OnDelivery(partition, produceLatency);
            
                return deliveryResult;
            }
            catch (ProduceException<Null, byte[]> ex)
                when (ex.Error.Code is ErrorCode.Local_UnknownTopic or ErrorCode.UnknownTopicOrPart)
            {
                // если топика нет, создаем
                await _topicsManager.CreateTopicIfNotExistsAsync(_kafkaOptions.BootstrapServers, TopicSpecification);
            }
            catch (ProduceException<Null, byte[]> ex)
                when (KafkaErrorClassifier.IsRetryable(ex.Error))
            {
                _partitionStateTracker.OnError(ex.DeliveryResult.Partition.Value);
                
                if (attemptCount >= MaxRetryAttempt)
                    throw;
                
                if(KafkaErrorClassifier.ShouldPartitionCooldown(ex.Error))
                    _partitionStateTracker.Cooldown(partition, PartitionCooldownMs); // временно убираем партицию из выбора
            
                // retry produce
            }
            catch (ProduceException<Null, byte[]> ex)
            {
                //sent message to DLQ (dead letter queue)
                _logger.LogError(ex, "Message sent to DLQ");

                throw;
            }
        }
    }

    private byte[] SerializeMessage<TMessage>(TMessage message)
    {
        try
        {
            return JsonSerializer.SerializeToUtf8Bytes(message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Serialized error");
            throw;
        }
    }

    private void OnError(IProducer<Null, byte[]> producer, Error error)
    {
        if (error.IsFatal)
            _logger.LogCritical("Kafka fatal error: {Error}", error);

        else
            _logger.LogWarning("Kafka error: {Error}", error);
    }
    
    private void OnLog(IProducer<Null, byte[]> producer, LogMessage logMessage)
    {
        _logger.LogDebug(
            "Kafka log [{Facility}] {Message}",
            logMessage.Facility,
            logMessage.Message);
    }
}