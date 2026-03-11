namespace Kafka.Examples.Producers.SmartProducer.Partitioning;

public class PartitionState(int partitionId)
{
    private int _inflightMessages;
    private int _recentErrorsCountCount;
    
    public int PartitionId { get; } = partitionId;

    public int InflightMessages => _inflightMessages;

    public int RecentErrorsCount => _recentErrorsCountCount;
    
    public double AvgLatencyMs { get; set; }

    public long LastErrorTimestamp { get; set; }
    
    public long UnavailableUntilTimestamp { get; set; }

    public bool IsAvailable() =>
       Environment.TickCount64 > UnavailableUntilTimestamp;
    
    public void IncrementInflightMessages() =>
        Interlocked.Increment(ref _inflightMessages);
    
    public void DecrementInflightMessages() =>
        Interlocked.Decrement(ref _inflightMessages);
    
    public void IncrementErrorsCount() =>
        Interlocked.Increment(ref _recentErrorsCountCount);
    
    public void DecrementErrorsCount() => 
        _recentErrorsCountCount = Math.Max(0, _recentErrorsCountCount - 1);
}