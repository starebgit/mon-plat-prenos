using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MonPlatPrenos.Worker.Services;

namespace MonPlatPrenos.Worker;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((_, config) =>
            {
                config.SetBasePath(AppContext.BaseDirectory);
                config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: false);
            })
            .ConfigureServices((context, services) =>
            {
                services.Configure<PrenosOptions>(context.Configuration.GetSection("Prenos"));
                services.AddSingleton<ISapClient>(sp =>
                {
                    var options = sp.GetRequiredService<IOptions<PrenosOptions>>().Value;

                    if (options.Sap.UseMock)
                    {
                        return new MockSapClient();
                    }

                    var logger = sp.GetRequiredService<ILogger<SapDllSapClient>>();
                    return new SapDllSapClient(options.Sap, logger);
                });

                services.AddSingleton<PrenosJob>();
                services.AddHostedService<SchedulerWorker>();
            })
            .Build();

        if (args.Contains("--run-once", StringComparer.OrdinalIgnoreCase))
        {
            using (var scope = host.Services.CreateScope())
            {
                var job = scope.ServiceProvider.GetRequiredService<PrenosJob>();
                var fromDate = TryGetDateArg(args, "--from-date");
                var toDate = TryGetDateArg(args, "--to-date");

                if (fromDate.HasValue || toDate.HasValue)
                {
                    var start = fromDate ?? toDate.Value;
                    var end = toDate ?? fromDate.Value;
                    if (end < start)
                    {
                        var tmp = start;
                        start = end;
                        end = tmp;
                    }

                    for (var day = start.Date; day <= end.Date; day = day.AddDays(1))
                    {
                        await job.RunAsync(day, CancellationToken.None).ConfigureAwait(false);
                    }
                }
                else
                {
                    await job.RunAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }

            return 0;
        }

        await host.RunAsync().ConfigureAwait(false);
        return 0;
    }

    private static DateTime? TryGetDateArg(string[] args, string key)
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

            DateTime parsed;
            if (DateTime.TryParse(args[i + 1], out parsed))
            {
                return parsed.Date;
            }

            return null;
        }

        return null;
    }
}
