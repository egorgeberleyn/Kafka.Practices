using System.Collections.Concurrent;
using Npgsql;

namespace Kafka.Examples.Idempotent;

public class InMemoryMessageDeduplicator : IMessageDeduplicator
{
    private readonly ConcurrentDictionary<string, byte> _processedMessages = new();

    public Task<bool> TryMarkProcessedAsync(NpgsqlConnection _, string messageId, CancellationToken ct)
    {
        return Task.FromResult(_processedMessages.TryAdd(messageId, 0));
    }
}