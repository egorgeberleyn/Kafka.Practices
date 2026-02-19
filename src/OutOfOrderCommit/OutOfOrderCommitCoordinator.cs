using System.Collections.Concurrent;
using Confluent.Kafka;

namespace Kafka.Examples.OutOfOrderCommit;

public sealed class OutOfOrderCommitCoordinator : IAsyncDisposable
{
    private readonly TimeSpan _gapTimeout = TimeSpan.FromSeconds(30);
    private const int PendingMessagesLimit = 10_000;
    
    private readonly IConsumer<Null, string> _consumer;
    private readonly IDateTimeProvider _dateTimeProvider;
    
    private readonly ConcurrentDictionary<TopicPartition, PartitionState> _partitionStates = new();
    private readonly PeriodicTimer _periodicTimer;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _timerTask;

    public OutOfOrderCommitCoordinator(IConsumer<Null, string> consumer, IDateTimeProvider dateTimeProvider)
    {
        _consumer = consumer;
        _dateTimeProvider = dateTimeProvider;
        _periodicTimer = new PeriodicTimer(TimeSpan.FromSeconds(10));
        _timerTask = RunTimerLoopAsync(_cts.Token);
    }
    
    public void MarkProcessed(TopicPartition topicPartition, long sequenceId, long offset)
    {
        var partitionState = _partitionStates.GetOrAdd(topicPartition, _ => new PartitionState());

        lock (partitionState)
        {
            if (sequenceId <= partitionState.LastCommittedSequenceId || offset <= partitionState.LastCommittedOffset)
                return;
            
            if (partitionState.PendingMessages.Count > PendingMessagesLimit)
                throw new OverflowException("Pending messages overflow"); // or pause partition/skip oldest

            if (!partitionState.PendingMessages.TryAdd(sequenceId,
                    new PendingMessage(offset, _dateTimeProvider.GetUtcNow())))
                return; // duplicate

            TryAdvance(topicPartition, partitionState);
        }
    }
    
    private void TryAdvance(TopicPartition topicPartition, PartitionState partitionState)
    {
        var advanced = false;
        var nextSequenceId = partitionState.LastCommittedSequenceId + 1;

        while (partitionState.PendingMessages.TryGetValue(nextSequenceId, out var pendingMessage))
        {
            // monotonic offset protection
            if (pendingMessage.Offset <= partitionState.LastCommittedOffset)
            {
                partitionState.PendingMessages.Remove(nextSequenceId);
                partitionState.LastCommittedSequenceId = nextSequenceId;
                continue;
            }
            
            partitionState.PendingMessages.Remove(nextSequenceId);
            
            partitionState.LastCommittedSequenceId = nextSequenceId;
            partitionState.LastCommittedOffset = pendingMessage.Offset;

            nextSequenceId++;
            advanced = true;
        }

        if (advanced)
            Commit(topicPartition, partitionState.LastCommittedOffset);
    }

    private void Commit(TopicPartition topicPartition, long offset)
    {
        if (offset < 0) return;

        _consumer.Commit([
            new TopicPartitionOffset(topicPartition, new Offset(offset + 1)) //Kafka коммитит следующий offset, а не текущий обработанный
        ]);
    }
    
    private async Task RunTimerLoopAsync(CancellationToken ct)
    {
        try
        {
            while (await _periodicTimer.WaitForNextTickAsync(ct))
            {
                CheckTimeouts();
            }
        }
        catch (OperationCanceledException)
        {
            // graceful shutdown
        }
        catch (Exception ex)
        {
            // log
        }
    }
    
    private void CheckTimeouts()
    {
        var dateTimeNow = _dateTimeProvider.GetUtcNow();

        foreach (var (topicPartition, partitionState) in _partitionStates)
        {
            lock (partitionState)
            {
                var advanced = false;
                
                while (true)
                {
                    var nextSequenceId = partitionState.LastCommittedSequenceId + 1;

                    if (partitionState.PendingMessages.Count == 0)
                        break;
                    
                    if (!partitionState.PendingMessages.TryGetValue(nextSequenceId, out var pendingMessage))
                        break;
                    
                    //earliest timestamp from pending messages
                    var oldestMessage = partitionState.PendingMessages.Values.MinBy(x => x.MessageProcessedTimestamp);
                    if(oldestMessage is null)
                        break;

                    if (dateTimeNow - pendingMessage.MessageProcessedTimestamp < _gapTimeout)
                        break;
                
                    partitionState.LastCommittedSequenceId = nextSequenceId;
                    partitionState.PendingMessages.Remove(nextSequenceId);
                    
                    //log warning
                    //send message (with nextSequenceId) to dlq 

                    advanced = true;
                }
                
                if(advanced)
                    TryAdvance(topicPartition, partitionState);
            }
        }
    }
    
    private class PartitionState
    {
        public long LastCommittedSequenceId = -1;
        public long LastCommittedOffset = -1;
        
        //хранит пары (sequenceId, pendingMessage), которые уже обработаны, но ещё не закоммичены
        //маппинг соответствия sequenceId к offset
        public readonly Dictionary<long, PendingMessage> PendingMessages = new();
        
        //Почему Dictionary?
        //- unique (no duplicates offsets)
        //- fast lookup (Contains/Remove)
    }

    private record PendingMessage(long Offset, DateTime MessageProcessedTimestamp);

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _periodicTimer.Dispose();
        await _timerTask;
        _cts.Dispose();
    }
}