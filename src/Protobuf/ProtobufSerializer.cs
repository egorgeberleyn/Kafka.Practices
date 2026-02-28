using Confluent.Kafka;
using Google.Protobuf;

namespace Kafka.Examples.Protobuf;

//Сериализатор для protobuf сообщений в Кафку
public sealed class ProtobufSerializer<T> : ISerializer<T>, IDeserializer<T>
    where T: IMessage<T>, new()
{
    private static readonly MessageParser<T> MessageParser = new(() => new T());
    
    public byte[] Serialize(T data, SerializationContext context) => data.ToByteArray();

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        return isNull 
            ? throw new ArgumentNullException(nameof(data), "Null data encountered") 
            : MessageParser.ParseFrom(data);
    }
}