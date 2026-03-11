using System.Collections.Concurrent;

namespace Kafka.Examples.Producers.SmartProducer.Partitioning;


public interface IPartitionStateTracker
{
    PartitionState Get(int partition);
    IEnumerable<PartitionState> GetAll(int partitionCount);

    void OnProduce(int partition);
    void OnDelivery(int partition, TimeSpan latency);
    void OnError(int partition);
    void Cooldown(int partition, int milliseconds);
}

// PartitionState
//  ├ inflightMessages
//  ├ errorCount
//  ├ avgLatency
//  ├ lastFailure
public class KafkaPartitionStateTracker : IPartitionStateTracker
{
    private readonly ConcurrentDictionary<int, PartitionState> _partitionStates = new();
    
    public PartitionState Get(int partition)
    {
        return _partitionStates.GetOrAdd(partition, p => new PartitionState(p));
    }

    public IEnumerable<PartitionState> GetAll(int partitionCount)
    {
        for (var i = 0; i < partitionCount; i++)
            yield return Get(i);
    }

    public void OnProduce(int partition)
    {
        var partitionState = Get(partition);
        partitionState.IncrementInflightMessages();
    }

    public void OnDelivery(int partition, TimeSpan latency)
    {
        var partitionState = Get(partition);

        partitionState.DecrementInflightMessages();

        //экспоненциальное скользящее среднее (EWMA)
        //EWMA быстрее реагирует на изменения (статистический метод анализа временных рядов, который придает больший вес недавним данным, а не старым)
        partitionState.AvgLatencyMs = partitionState.AvgLatencyMs * 0.8 + latency.TotalMilliseconds * 0.2;

        if (partitionState.RecentErrorsCount > 0)
            partitionState.DecrementErrorsCount();
    }

    public void OnError(int partition)
    {
        var partitionState = Get(partition);

        partitionState.IncrementErrorsCount();
        partitionState.LastErrorTimestamp = Environment.TickCount64;
    }

    //circuit breaker для partition
    //партиция временно исключается из выбора
    public void Cooldown(int partition, int milliseconds)
    {
        var partitionState = Get(partition);
        partitionState.UnavailableUntilTimestamp = Environment.TickCount64 + milliseconds;
    }
}