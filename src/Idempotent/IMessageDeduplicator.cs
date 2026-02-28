using Npgsql;

namespace Kafka.Examples.Idempotent;

public interface IMessageDeduplicator
{
    Task<bool> TryMarkProcessedAsync(NpgsqlConnection dbConnection, string messageId, CancellationToken ct);
}