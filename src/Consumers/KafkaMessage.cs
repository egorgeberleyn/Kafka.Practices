using Confluent.Kafka;

namespace Kafka.Examples.Consumers;

public sealed record KafkaMessage<TValue>(
    TValue Value,
    TopicPartition Partition,
    Offset Offset,
    Timestamp? Timestamp = null);