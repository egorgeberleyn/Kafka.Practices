using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Examples;


public interface IKafkaTopicsCreator
{
    public Task CreateTopicAsync(string broker, TopicSpecification topicSpecification);
}

public class KafkaTopicsCreator(ILogger<KafkaTopicsCreator> logger) : IKafkaTopicsCreator
{
    public async Task CreateTopicAsync(string broker, TopicSpecification topicSpecification)
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = broker
        };

        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        try
        {
            await adminClient.CreateTopicsAsync([topicSpecification]);
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
}