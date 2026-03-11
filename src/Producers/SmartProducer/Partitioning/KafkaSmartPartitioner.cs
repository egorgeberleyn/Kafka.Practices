namespace Kafka.Examples.Producers.SmartProducer.Partitioning;

public static class KafkaSmartPartitioner
{
    public static int ChoosePartition(IPartitionStateTracker partitionStateTracker, int partitionCount)
    {
        PartitionState? bestPartition = null;
        var bestScore = double.MinValue;

        foreach (var state in partitionStateTracker.GetAll(partitionCount))
        {
            if (!state.IsAvailable())
                continue;

            var score = KafkaPartitionScoreCalculator.Calculate(state);

            if (!(score < bestScore)) continue;
            
            bestScore = score;
            bestPartition = state;
        }

        return bestPartition?.PartitionId ?? Random.Shared.Next(partitionCount);
    }
}