using MonPlatPrenos.Worker.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<PrenosOptions>(builder.Configuration.GetSection("Prenos"));
builder.Services.AddSingleton<ISapClient, MockSapClient>();
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
