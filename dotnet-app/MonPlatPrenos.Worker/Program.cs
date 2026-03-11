using System;
using System.Linq;
using System.Threading.Tasks;
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
        var runOnce = args.Any(a => string.Equals(a, "--run-once", StringComparison.OrdinalIgnoreCase));

        var host = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((_, config) =>
            {
                config.SetBasePath(AppContext.BaseDirectory);
                config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: false);
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
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
                var sapClient = scope.ServiceProvider.GetRequiredService<ISapClient>();

                if (sapClient is SapDllSapClient realSap)
                {
                    var login = realSap.GetLoginPreview();
                    Console.WriteLine("RUN-ONCE LOGIN CHECK");
                    Console.WriteLine($"DestinationName: {login.DestinationName}");
                    Console.WriteLine($"AppServerHost : {login.AppServerHost}");
                    Console.WriteLine($"SystemNumber  : {login.SystemNumber}");
                    Console.WriteLine($"Client        : {login.Client}");
                    Console.WriteLine($"User          : {login.User}");
                    Console.WriteLine($"Language      : {login.Language}");
                    Console.WriteLine($"Password      : {login.PasswordMasked}");
                    Console.WriteLine($"IsComplete    : {login.IsComplete}");
                    Console.WriteLine($"LoginSource   : {login.LoginSource}");
                    Console.WriteLine($"LoginMessage  : {login.LoginMessage}");
                }
                else
                {
                    Console.WriteLine("RUN-ONCE LOGIN CHECK");
                    Console.WriteLine("ISapClient is MockSapClient. Set Prenos:Sap:UseMock=false to test DB login lookup.");
                }

                var job = scope.ServiceProvider.GetRequiredService<PrenosJob>();
                var runDate = TryGetDateArg(args, "--from-date") ?? DateTime.Today;
                Console.WriteLine($"RUN-ONCE PRENOS DAY : {runDate:yyyy-MM-dd}");
                await job.RunAsync(runDate, CancellationToken.None).ConfigureAwait(false);
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
