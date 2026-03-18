using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using MonPlatPrenos.Worker.Services;

namespace MonPlatPrenos.Worker;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var runOnce = args.Any(a => string.Equals(a, "--run-once", StringComparison.OrdinalIgnoreCase));
        var runParityBenchmark = args.Any(a => string.Equals(a, "--parity-benchmark", StringComparison.OrdinalIgnoreCase));
        if (runParityBenchmark)
        {
            runOnce = true;
        }

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
                    options.Sap.UseTypedHotPath = options.UseTypedHotPath;
                    return new SapDllSapClient(options.Sap);
                });

                services.AddSingleton<PrenosJob>();
                services.AddHostedService<SchedulerWorker>();
            })
            .Build();

        if (runOnce)
        {
            using (var scope = host.Services.CreateScope())
            {
                var job = scope.ServiceProvider.GetRequiredService<PrenosJob>();
                var runDate = TryGetDateArg(args, "--from-date");
                Console.WriteLine(runParityBenchmark
                    ? "RUN PARITY-BENCHMARK MODE"
                    : $"RUN-ONCE PRENOS DAY : {(runDate.HasValue ? runDate.Value.ToString("yyyy-MM-dd") : "ALL")}");
                await job.RunAsync(runDate, System.Threading.CancellationToken.None).ConfigureAwait(false);
                Console.WriteLine("RUN-ONCE PRENOS DONE");
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

            if (DateTime.TryParse(args[i + 1], out var parsed))
            {
                return parsed.Date;
            }

            return null;
        }

        return null;
    }
}
