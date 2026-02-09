namespace Kafka.Examples.MessageSink;

public interface IMessageSink<in T>
{
    ValueTask Send(T msg);
}