using System.Collections.Concurrent;
using Confluent.Kafka;

namespace Kafka.Examples.Consumers.BatchCommit;

public sealed class OffsetsManager(IConsumer<Null, string> consumer)
{
    private readonly ConcurrentDictionary<TopicPartition, Offset> _offsets = new();
    
    public void MarkProcessed(TopicPartition partition, Offset offset)
    {
        //Kafka гарантирует порядок доставки внутри partition, но мы не гарантируем порядок завершения обработки
        
        // Worker pool = 3 потока
        // 
        // Worker1 берет offset 100 (обработка 5 сек)
        // Worker2 берет offset 101 (обработка 1 сек)
        // Worker3 берет offset 102 (обработка 0.5 сек)
        // 
        // Порядок завершения:
        // offset 102 → Worker3
        // offset 101 → Worker2
        // offset 100 → Worker1
        
        //Без проверки на olderOffset мы сохраним как последний оффсет - 100 и закоммитим его
        //Не смотря на то, что мы уже обработали следующие сообщения (101, 102), что приведет к повторной обработке
        
        _offsets.AddOrUpdate(partition, offset, (_, olderOffset) =>
            offset > olderOffset ? offset : olderOffset);
    }

    public void Commit()
    {
        if (_offsets.IsEmpty)
            return;
        
        var commits = _offsets
            .Select(x => new TopicPartitionOffset(x.Key, x.Value + 1)) //committed offset = last processed offset + 1 (first message not processed yet)
            .ToList();

        consumer.Commit(commits);
        _offsets.Clear();
    }
}