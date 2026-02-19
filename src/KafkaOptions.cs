namespace Kafka.Examples;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";
    
    public required string TopicName { get; init; }
    public required string BootstrapServers { get; init; }
    public required string ConsumerGroupId { get; init; }
}