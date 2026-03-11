namespace Kafka.Examples.Producers.SmartProducer;

public interface IProducerMetricsCollector
{
    
}

//produce_latency,  retry_count, partition_load, delivery_errors, serialization_errors, queue_full
public class KafkaSmartProducerMetricsCollector : IProducerMetricsCollector
{
    
}