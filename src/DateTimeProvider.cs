namespace Kafka.Examples;

public interface IDateTimeProvider
{
    DateTime GetUtcNow();
}

public class DateTimeProvider : IDateTimeProvider
{
    public DateTime GetUtcNow() => DateTime.UtcNow;
}