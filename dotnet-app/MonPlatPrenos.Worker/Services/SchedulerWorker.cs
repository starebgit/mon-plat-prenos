using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MonPlatPrenos.Worker.Services;

public sealed class SchedulerWorker : BackgroundService
{
    private readonly PrenosJob _job;
    private readonly PrenosOptions _options;
    private readonly ILogger<SchedulerWorker> _logger;

    public SchedulerWorker(PrenosJob job, IOptions<PrenosOptions> options, ILogger<SchedulerWorker> logger)
    {
        _job = job;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Scheduler worker running.");

        while (!stoppingToken.IsCancellationRequested)
        {
            var next = GetNextRun(DateTime.Now, _options.DailyRunTime);
            var delay = next - DateTime.Now;

            if (delay > TimeSpan.Zero)
            {
                _logger.LogInformation("Next run at {NextRun} (in {Delay}).", next, delay);
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
            catch (Exception ex)
            {
                _logger.LogError(ex, "Job run failed.");
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
