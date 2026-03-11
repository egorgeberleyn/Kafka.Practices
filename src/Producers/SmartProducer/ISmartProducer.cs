using Confluent.Kafka;

namespace Kafka.Examples.Producers.SmartProducer;

public interface ISmartProducer
{
    Task<DeliveryResult<Null, byte[]>> ProduceAsync<TMessage>(
        string topicName,
        TMessage message,
        CancellationToken cancellationToken);
}