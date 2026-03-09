using System.Text;
using MonPlatPrenos.Worker.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Windows.Forms;

ApplicationConfiguration.Initialize();
Application.Run(new DebugRunnerForm());

internal sealed class DebugRunnerForm : Form
{
    private readonly Button _runButton;
    private readonly TextBox _logBox;

    public DebugRunnerForm()
    {
        Text = "MonPlat Prenos Debug Runner";
        Width = 900;
        Height = 600;

        _runButton = new Button
        {
            Text = "Run Prenos",
            Top = 12,
            Left = 12,
            Width = 160,
            Height = 32
        };

        _logBox = new TextBox
        {
            Top = 56,
            Left = 12,
            Width = 860,
            Height = 490,
            Multiline = true,
            ScrollBars = ScrollBars.Both,
            ReadOnly = true,
            Font = new System.Drawing.Font("Consolas", 10)
        };

        _runButton.Click += async (_, _) => await RunPrenosAsync();

        Controls.Add(_runButton);
        Controls.Add(_logBox);
    }

    private async Task RunPrenosAsync()
    {
        _runButton.Enabled = false;
        try
        {
            LogLine($"[{DateTime.Now:HH:mm:ss}] Starting prenos...");

            var root = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "MonPlatPrenos.Worker"));
            var cfg = new ConfigurationBuilder()
                .SetBasePath(root)
                .AddJsonFile("appsettings.json", optional: false)
                .Build();

            var options = cfg.GetSection("Prenos").Get<PrenosOptions>() ?? new PrenosOptions();
            options.EnableDebugJson = true;
            options.EnableDebugTextDump = true;

            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddSimpleConsole();
                builder.SetMinimumLevel(LogLevel.Information);
            });

            var logger = loggerFactory.CreateLogger<PrenosJob>();
            ISapClient client = options.Sap.UseMock
                ? new MockSapClient()
                : new SapDllSapClient(options.Sap, loggerFactory.CreateLogger<SapDllSapClient>());

            var job = new PrenosJob(client, Options.Create(options), logger);
            await job.RunAsync(CancellationToken.None);

            var latest = Directory.GetFiles(options.OutputDirectory, "prenos-debug-*.txt")
                .OrderByDescending(f => f)
                .FirstOrDefault();

            if (latest is not null)
            {
                LogLine($"[{DateTime.Now:HH:mm:ss}] Prenos done. Debug file: {latest}");
                LogLine(new string('-', 80));
                LogLine(await File.ReadAllTextAsync(latest, Encoding.UTF8));
            }
            else
            {
                LogLine($"[{DateTime.Now:HH:mm:ss}] Prenos done. No debug txt found in {options.OutputDirectory}");
            }
        }
        catch (Exception ex)
        {
            LogLine($"[{DateTime.Now:HH:mm:ss}] ERROR: {ex}");
        }
        finally
        {
            _runButton.Enabled = true;
        }
    }

    private void LogLine(string line)
    {
        _logBox.AppendText(line + Environment.NewLine);
    }
}
