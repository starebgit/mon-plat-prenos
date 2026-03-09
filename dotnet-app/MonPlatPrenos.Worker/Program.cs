using MonPlatPrenos.Worker.Services;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<PrenosOptions>(builder.Configuration.GetSection("Prenos"));
builder.Services.AddSingleton<ISapClient>(sp =>
{
    var options = sp.GetRequiredService<IOptions<PrenosOptions>>().Value;

    if (options.Sap.UseMock)
    {
        return new MockSapClient();
    }

    var logger = sp.GetRequiredService<ILogger<SapDllSapClient>>();
    return new SapDllSapClient(options.Sap, logger);
});
builder.Services.AddSingleton<PrenosJob>();
builder.Services.AddHostedService<SchedulerWorker>();

var app = builder.Build();

if (args.Contains("--run-once"))
{
    using var scope = app.Services.CreateScope();
    var job = scope.ServiceProvider.GetRequiredService<PrenosJob>();
    await job.RunAsync(CancellationToken.None);
    return;
}

await app.RunAsync();
