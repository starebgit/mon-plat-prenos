using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace MonPlatPrenos.Worker.Services;

public sealed class SchedulerWorker : BackgroundService
{
    private readonly PrenosJob _job;
    private readonly PrenosOptions _options;
    public SchedulerWorker(PrenosJob job, IOptions<PrenosOptions> options)
    {
        _job = job;
        _options = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var next = GetNextRun(DateTime.Now, _options.DailyRunTime);
            var delay = next - DateTime.Now;

            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, stoppingToken);
            }

            if (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            try
            {
                await _job.RunAsync(stoppingToken);
            }
            catch (Exception)
            {
            }
        }
    }

    private static DateTime GetNextRun(DateTime now, string dailyRunTime)
    {
        TimeSpan runAt;
        if (!TimeSpan.TryParse(dailyRunTime, out runAt))
        {
            runAt = new TimeSpan(7, 30, 0);
        }

        var todayRun = now.Date.Add(runAt);
        return now < todayRun ? todayRun : todayRun.AddDays(1);
    }
}
