using Npgsql;

namespace Kafka.Examples.Idempotent;

public class DatabaseMessageDeduplicator : IMessageDeduplicator
{
    //CREATE UNIQUE INDEX ux_processed_messages_MessageId
    // ON processed_messages(MessageId);
    
    public async Task<bool> TryMarkProcessedAsync(
        NpgsqlConnection dbConnection, 
        string messageId, 
        CancellationToken ct)
    {
        var markProcessedMessageCommand = new NpgsqlCommand(
            "INSERT INTO processed_messages(MessageId) VALUES(@messageId) ON CONFLICT DO NOTHING",
            dbConnection);
        markProcessedMessageCommand.Parameters.AddWithValue("messageId", messageId);

        var messagesMarkProcessed = await markProcessedMessageCommand.ExecuteNonQueryAsync(ct);
        
        return messagesMarkProcessed == 1;
    }
}