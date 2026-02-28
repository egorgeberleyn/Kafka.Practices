using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Kafka.Examples.Consumers;

//автономный консьюмер без группы (который может подписаться на весь топик или заассайнится на определенные партиции)
public class KafkaAutonomousConsumer<TMessage> : BackgroundService
    where TMessage: class 
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly IConsumer<Null, TMessage> _consumer;
    private readonly ILogger<KafkaAutonomousConsumer<TMessage>> _logger;
    private readonly IDateTimeProvider _dateTimeProvider;

    private readonly HashSet<int> _assignedPartitions = [];
    private readonly HashSet<int>? _selectedPartitions = [0, 1, 2]; //чтобы выбрать и заассайнится на определенные партиции
    private readonly TimeSpan _partitionsMetadataRefreshInterval = TimeSpan.FromSeconds(60);
    
    public KafkaAutonomousConsumer(
        IOptions<KafkaOptions> kafkaOptions, 
        ILogger<KafkaAutonomousConsumer<TMessage>> logger,
        IDateTimeProvider dateTimeProvider)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
        _dateTimeProvider = dateTimeProvider;

        var consumerCfg = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.ConsumerGroupId, // GroupId всё равно обязателен в конфиге librdkafka
            SessionTimeoutMs = 30_000,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, //явно коммитим сообщение при обработке
            EnablePartitionEof = true //включает генерацию специального уведомления от Кафки о достижении конца партиции
        };
        
        _consumer = new ConsumerBuilder<Null, TMessage>(consumerCfg)
            .SetValueDeserializer(new JsonValueSerializer<TMessage>())
            .SetErrorHandler((_, e) => _logger.LogError("Kafka error: {Code} {Reason}", e.Code, e.Reason)) // ловит ошибки клиента Kafka
            .Build();
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            ConfigureAssignments();
            var nextRefresh = _dateTimeProvider.GetUtcNow() + _partitionsMetadataRefreshInterval;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(stoppingToken);

                    if (result == null || result.IsPartitionEOF)
                        continue;

                    await ProcessMessage(result.Message.Value, stoppingToken);

                    _consumer.Commit(result);
                    
                    //check refresh partitions metadata interval
                    if (_dateTimeProvider.GetUtcNow() < nextRefresh) continue;
                    RefreshPartitionsIfNeeded();
                    nextRefresh = _dateTimeProvider.GetUtcNow() + _partitionsMetadataRefreshInterval;
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consume error");
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Processing error");
                }
            }
        }
        finally
        {
            _consumer.Close();
            _consumer.Dispose();
        }
    }
    
    protected virtual async Task ProcessMessage(TMessage message, CancellationToken ct)
    {
        await Task.Delay(Random.Shared.Next(50,200), ct); // имитация обработки сообщения
    }
    
    private void ConfigureAssignments()
    {
        // если заданы конкретные партиции — используем Assign на них
        if (_selectedPartitions is not null)
        {
            var assignments = _selectedPartitions
                .Select(p => new TopicPartitionOffset(
                    _kafkaOptions.TopicName,
                    new Partition(p),
                    Offset.Beginning))
                .ToList();

            _consumer.Assign(assignments);

            _logger.LogInformation("Assigned partitions: {Partitions}", string.Join(",", _selectedPartitions));
        }
        else
        {
            // иначе подписываемся на все партиции
            AssignAllPartitions();
        }
    }
    
    private void AssignAllPartitions()
    {
        var topicMetadata = GetTopicMetadata();

        var assignments = topicMetadata.Partitions
            .Select(p => new TopicPartitionOffset(
                _kafkaOptions.TopicName,
                new Partition(p.PartitionId),
                Offset.Beginning))
            .ToList();

        _consumer.Assign(assignments);
        
        SetAssignedPartitions(topicMetadata.Partitions.Select(p => p.PartitionId).ToHashSet());

        _logger.LogInformation("Assigned all partitions: {Count}", assignments.Count);
    }
    
    private void ReassignPartitions(HashSet<int> actualPartitions)
    {
        var offsetPositions = _consumer.Assignment
            .Select(tp => new TopicPartitionOffset(tp, _consumer.Position(tp)))
            .ToList();

        var newAssignments = actualPartitions
            .Select(p =>
            {
                var existingPartitionOffset = offsetPositions.FirstOrDefault(x => x.Partition.Value == p);

                return existingPartitionOffset != null
                    ? existingPartitionOffset
                    : new TopicPartitionOffset(_kafkaOptions.TopicName, new Partition(p), Offset.Beginning);
            })
            .ToList();

        _consumer.Assign(newAssignments);

        SetAssignedPartitions(actualPartitions);

        _logger.LogInformation("Reassigned partitions: {Partitions}", string.Join(",", _assignedPartitions));
    }

    //Проверяем через каждый partitionsMetadataRefreshInterval на случай если в топик добавились/удалились партиции
    private void RefreshPartitionsIfNeeded()
    {
        var topicMetadata = GetTopicMetadata();

        var actualPartitions = topicMetadata.Partitions.Select(p => p.PartitionId).ToHashSet();

        if (actualPartitions.SetEquals(_assignedPartitions))
            return;

        var newPartitions = actualPartitions.Except(_assignedPartitions).ToList();

        _logger.LogInformation("Detected new partitions: {Partitions}", string.Join(",", newPartitions));

        ReassignPartitions(actualPartitions);
    }
    
    private TopicMetadata GetTopicMetadata()
    {
        using var admin = new AdminClientBuilder(
                new AdminClientConfig
                {
                    BootstrapServers = _kafkaOptions.BootstrapServers
                })
            .Build();
        
        var metadata = admin.GetMetadata(_kafkaOptions.TopicName, timeout: TimeSpan.FromSeconds(30));
        return metadata.Topics.FirstOrDefault() ?? 
                            throw new NullReferenceException("Topic not found");
    }
    
    private void SetAssignedPartitions(HashSet<int> partitions)
    {
        _assignedPartitions.Clear();
        foreach (var p in partitions)
            _assignedPartitions.Add(p);
    }
}