using Microsoft.Extensions.Options;

namespace MonPlatPrenos.Worker.Services;

public sealed class SchedulerWorker(
    PrenosJob job,
    IOptions<PrenosOptions> options,
    ILogger<SchedulerWorker> logger) : BackgroundService
{
    private readonly PrenosOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Scheduler worker running.");

        while (!stoppingToken.IsCancellationRequested)
        {
            var next = GetNextRun(DateTime.Now, _options.DailyRunTime);
            var delay = next - DateTime.Now;

            if (delay > TimeSpan.Zero)
            {
                logger.LogInformation("Next run at {NextRun} (in {Delay}).", next, delay);
                await Task.Delay(delay, stoppingToken);
            }

            if (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            try
            {
                await job.RunAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Job run failed.");
            }
        }
    }

    private static DateTime GetNextRun(DateTime now, string dailyRunTime)
    {
        if (!TimeSpan.TryParse(dailyRunTime, out var runAt))
        {
            runAt = new TimeSpan(7, 30, 0);
        }

        var todayRun = now.Date.Add(runAt);
        return now < todayRun ? todayRun : todayRun.AddDays(1);
    }
}
