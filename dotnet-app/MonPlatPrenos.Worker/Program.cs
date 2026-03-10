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
    var options = scope.ServiceProvider.GetRequiredService<IOptions<PrenosOptions>>().Value;
    var startupLogger = scope.ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("Startup");

    startupLogger.LogInformation("Run mode: run-once. SAP mode: {SapMode}", options.Sap.UseMock ? "Mock" : "sap.dll");
    startupLogger.LogInformation("Filters: scheduler={Scheduler}, materialFrom={From}, materialTo={To}", options.SchedulerCode, options.PlateMaterialFrom, options.PlateMaterialTo);
    if (options.Sap.UseMock)
    {
        startupLogger.LogWarning("UseMock=true, so no real SAP login is attempted. Set Prenos:Sap:UseMock=false to use sap.dll integration.");
    }

    var fromDate = TryGetDateArg(args, "--from-date");
    var toDate = TryGetDateArg(args, "--to-date");

    if (fromDate.HasValue || toDate.HasValue)
    {
        var start = fromDate ?? toDate!.Value;
        var end = toDate ?? fromDate!.Value;
        if (end < start)
        {
            (start, end) = (end, start);
        }

        for (var day = start.Date; day <= end.Date; day = day.AddDays(1))
        {
            await job.RunAsync(day, CancellationToken.None);
        }
    }
    else
    {
        await job.RunAsync(CancellationToken.None);
    }

    return;
}

await app.RunAsync();

static DateTime? TryGetDateArg(string[] args, string key)
{
    for (var i = 0; i < args.Length; i++)
    {
        if (!string.Equals(args[i], key, StringComparison.OrdinalIgnoreCase))
        {
            continue;
        }

        if (i + 1 >= args.Length)
        {
            return null;
        }

        if (DateTime.TryParse(args[i + 1], out var parsed))
        {
            return parsed.Date;
        }

        return null;
    }

    return null;
}
