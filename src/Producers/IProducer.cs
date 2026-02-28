namespace Kafka.Examples.Producers;

public interface IProducer<in TMessage>
{
    Task ProduceAsync(TMessage message, CancellationToken cancellationToken);
}