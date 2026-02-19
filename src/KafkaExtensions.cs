using System.Text;
using Confluent.Kafka;

namespace Kafka.Examples;

public static class KafkaExtensions
{
    private const string SequenceIdHeaderName = "sequenceId";
    
    public static long GetSequenceId(this Headers headers)
    {
        var header = headers.LastOrDefault(h => h.Key == SequenceIdHeaderName);

        if (header == null) throw new InvalidOperationException("Header 'sequenceId' not found");
        var value = Encoding.UTF8.GetString(header.GetValueBytes());

        return int.Parse(value);

    }
}