namespace Kafka.Examples.Producers.SmartProducer.Partitioning;

public static class KafkaPartitionScoreCalculator
{
    private const double InflightWeight = 1.0;
    private const double LatencyWeight = 0.05;
    private const double ErrorWeight = 5.0;

    public static double Calculate(PartitionState partitionState)
    {
        var now = Environment.TickCount64;
        var recentErrorPenalty = now - partitionState.LastErrorTimestamp switch
        {
            < 3000 => 8,
            < 10000 => 4,
            _ => 0
        };
        
        return
            partitionState.InflightMessages * InflightWeight +
            partitionState.AvgLatencyMs * LatencyWeight +
            partitionState.RecentErrorsCount * ErrorWeight
            - recentErrorPenalty;
    }
}