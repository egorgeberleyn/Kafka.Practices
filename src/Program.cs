using Kafka.Examples;
using Kafka.Examples.MessageSink;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

var dbConnectionString = "Host=localhost;Port=5432;Username=kafka-user;Password=kafkapasswrd00dd;Database=kafka-db;" +
                         "Pooling=true;Maximum Pool Size=20;Command Timeout=30;";
var dataSource = NpgsqlDataSource.Create(dbConnectionString);
builder.Services.AddSingleton(dataSource);

builder.Services.AddSingleton<IDateTimeProvider, DateTimeProvider>();
builder.Services.AddOptions<KafkaOptions>(name: KafkaOptions.SectionName);

builder.Services.AddHostedService<KafkaMessageSink<string>>();
builder.Services.AddSingleton<IMessageSink<string>>(sp => 
    (sp.GetRequiredService<IEnumerable<IHostedService>>()
            .First(x => x is KafkaMessageSink<string>)
        as KafkaMessageSink<string>)!);

var app = builder.Build();

app.UseHttpsRedirection();

app.MapGet("/sink", async (IMessageSink<string> sink) =>
{
    await sink.Send(Guid.NewGuid().ToString());
    return Results.Ok("Good stuff!");
});
    
app.Run();