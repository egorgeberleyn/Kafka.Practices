using Confluent.Kafka;

namespace Kafka.Examples.Consumers.BatchCommit;

public sealed record KafkaMessage(
    string Value,
    TopicPartition Partition,
    Offset Offset);