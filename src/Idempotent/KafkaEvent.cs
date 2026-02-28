namespace Kafka.Examples.Idempotent;

public record KafkaEvent(
    string Id,
    string EntityType,
    string Payload);
