using Kafka.Examples;
using Kafka.Examples.MessageSink;

var builder = WebApplication.CreateBuilder(args);

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