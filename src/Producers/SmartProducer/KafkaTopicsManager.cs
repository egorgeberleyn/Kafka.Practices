using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Examples.Producers.SmartProducer;

public interface IKafkaTopicsManager
{
    Task CreateTopicIfNotExistsAsync(string broker, TopicSpecification topicSpecification);
}

public class KafkaTopicsManager(
    ILogger<KafkaTopicsManager> logger,
    IDateTimeProvider dateTimeProvider) : IKafkaTopicsManager
{
    private ConcurrentDictionary<string, DateTime> TopicsCache { get; } = new();
    private TimeSpan TopicCacheTtl { get; } = TimeSpan.FromSeconds(30.0);
    
    public async Task CreateTopicIfNotExistsAsync(string broker, TopicSpecification topicSpecification)
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = broker
        };

        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        try
        {
            if (TopicExistsInCache(topicSpecification.Name))
                return;

            await adminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames([topicSpecification.Name]));
            
            await adminClient.CreateTopicsAsync([topicSpecification]);

            TopicsCache[topicSpecification.Name] = dateTimeProvider.GetUtcNow() + TopicCacheTtl;
        }
        catch (CreateTopicsException ex)
        {
            if (ex.Results[0].Error.Reason != $"Topic '{topicSpecification.Name}' already exists.")
                throw;
            logger.LogDebug("An error occurred creating topic {Topic}: {Reason}", 
                ex.Results[0].Topic,
                ex.Results[0].Error.Reason);
        }
    }
    
    private bool TopicExistsInCache(string topicName) =>
        TopicsCache.TryGetValue(topicName, out var dateTime) && dateTime > dateTimeProvider.GetUtcNow();
}