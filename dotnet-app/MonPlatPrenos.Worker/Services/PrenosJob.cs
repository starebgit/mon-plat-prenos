using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Text.Json;
using System.Diagnostics;
using MonPlatPrenos.Worker.Models;
using Microsoft.Extensions.Options;

namespace MonPlatPrenos.Worker.Services;

public sealed class PrenosJob
{
    private readonly ISapClient _sapClient;
    private readonly PrenosOptions _options;
    public PrenosJob(ISapClient sapClient, IOptions<PrenosOptions> options)
    {
        _sapClient = sapClient;
        _options = options.Value;
    }

    public Task RunAsync(CancellationToken cancellationToken)
        => RunAsync(forDate: null, cancellationToken);

    public async Task RunAsync(DateTime? forDate, CancellationToken cancellationToken)
    {

        var plant = "1061";
        var orderFrom = "000005223286";

        var stats = new ProcessingStats();
        var status = new ProgressStatus();
        var timing = new TimingCollector(20);
        var operationCodes = new HashSet<string>(_options.OperationCodes, StringComparer.OrdinalIgnoreCase);
        var allRules = _options.DefaultTerms.Concat(_options.ExtraTerms).ToList();

        RenderSingleLineStatus("Fetching production orders from SAP...");
        var orders = await TimedAsync(
            timing,
            "GetProductionOrdersForPlates",
            () => _sapClient.GetProductionOrdersForPlatesAsync(
                plant,
                _options.SchedulerCode,
                _options.PlateMaterialFrom,
                _options.PlateMaterialTo,
                orderFrom,
                cancellationToken));

        stats.TotalOrdersFetched = orders.Count;
        RenderSingleLineStatus($"Fetched {orders.Count} production orders. Processing...");
        status.ForceNext();

        var plateDemands = new List<PlateDemandRecord>();
        var unified = new List<UnifiedItem>();
        var semiFinished = new List<SemiFinishedTrace>();
        var processedSemiMaterials = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        for (var orderIndex = 0; orderIndex < orders.Count; orderIndex++)
        {
            var order = orders[orderIndex];
            var progressBar = BuildProgressBar(orderIndex + 1, orders.Count, 22);
            RenderSingleLineStatus($"{progressBar} {orderIndex + 1}/{orders.Count} | {order.OrderNumber} | Plates:{stats.PlateRecordsWritten} Unified:{stats.UnifiedRowsWritten}");
            if (forDate.HasValue && order.StartDate.Date != forDate.Value.Date)
            {
                stats.SkippedByDateFilter++;
                continue;
            }

            if (order.Status is "TEHZ" or "ZAKL")
            {
                stats.SkippedByStatus++;
                continue;
            }

            if (order.Material.Length < 9 || (order.Material[8] is not ('4' or '3' or '2')))
            {
                stats.SkippedByMaterialRule++;
                continue;
            }

            stats.OrdersAfterCoreFilters++;
            var operations = await TimedAsync(timing, "GetOperations", () => _sapClient.GetOperationsAsync(order.OrderNumber, cancellationToken));
            var validOperations = operations
                .Where(o => operationCodes.Contains(o.OperationCode))
                .Where(o => o.ConfirmableQty > 0)
                .Where(o => o.StepCode == "0010")
                .ToList();

            stats.OperationRowsRead += operations.Count;
            stats.ValidOperations += validOperations.Count;

            var totalYield = 0;
            var maxConcurrency = Math.Max(1, _options.ConfirmationConcurrency);
            if (maxConcurrency == 1 || validOperations.Count <= 1)
            {
                for (var operationIndex = 0; operationIndex < validOperations.Count; operationIndex++)
                {
                    var op = validOperations[operationIndex];
                    if (operationIndex == 0 || operationIndex % 5 == 0)
                    {
                        TryRenderSingleLineStatus($"{progressBar} {orderIndex + 1}/{orders.Count} | {order.OrderNumber} | Op {operationIndex + 1}/{validOperations.Count}", status);
                    }

                    var confirmations = await TimedAsync(timing, "GetConfirmations", () => _sapClient.GetConfirmationsAsync(order.OrderNumber, op.Confirmation, cancellationToken));
                    stats.ConfirmationRowsRead += confirmations.Count;
                    totalYield += confirmations.Sum(c => c.Yield);
                }
            }
            else
            {
                using var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
                var confirmationTasks = validOperations.Select(async (op, operationIndex) =>
                {
                    await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        if (operationIndex == 0 || operationIndex % 5 == 0)
                        {
                            TryRenderSingleLineStatus($"{progressBar} {orderIndex + 1}/{orders.Count} | {order.OrderNumber} | Op {operationIndex + 1}/{validOperations.Count}", status);
                        }

                        var confirmations = await TimedAsync(timing, "GetConfirmations", () => _sapClient.GetConfirmationsAsync(order.OrderNumber, op.Confirmation, cancellationToken)).ConfigureAwait(false);
                        return (Count: confirmations.Count, Yield: confirmations.Sum(c => c.Yield));
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                });

                var confirmationResults = await Task.WhenAll(confirmationTasks).ConfigureAwait(false);
                foreach (var result in confirmationResults)
                {
                    stats.ConfirmationRowsRead += result.Count;
                    totalYield += result.Yield;
                }
            }

            var missingQty = order.PlannedQuantity - totalYield;
            if (missingQty <= 0)
            {
                stats.SkippedByMissingQty++;
                continue;
            }

            var operationTrackCode = validOperations
                .Select(o => o.WorkCenterCode)
                .FirstOrDefault(code => !string.IsNullOrWhiteSpace(code));
            var track = ParseTrack(operationTrackCode ?? order.WorkCenterTrackCode);
            var formattedPlateMaterial = FormatMaterialLikeDelphi(order.Material);
            plateDemands.Add(new PlateDemandRecord(track, order.OrderNumber, formattedPlateMaterial, missingQty, order.StartDate));
            stats.PlateRecordsWritten++;

            var components = await TimedAsync(timing, "GetComponents", () => _sapClient.GetComponentsAsync(order.OrderNumber, cancellationToken));
            stats.ComponentRowsRead += components.Count;

            foreach (var component in components)
            {
                foreach (var rule in allRules)
                {
                    if (!rule.IsMatch(component.Description))
                    {
                        continue;
                    }

                    unified.Add(new UnifiedItem(
                        order.OrderNumber,
                        formattedPlateMaterial,
                        FormatMaterialLikeDelphi(component.Material),
                        component.Description,
                        rule.Name,
                        missingQty,
                        DateTime.UtcNow));

                    stats.UnifiedRowsWritten++;
                    stats.AddCategoryHit(rule.Name);

                    if (rule.Name.Equals("Samot", StringComparison.OrdinalIgnoreCase))
                    {
                        await ObdelajSamotAsync(order, component, semiFinished, unified, stats, timing, processedSemiMaterials, cancellationToken);
                    }
                    else if (rule.Name.Equals("Protektor", StringComparison.OrdinalIgnoreCase)
                             || rule.Name.Equals("Sponka", StringComparison.OrdinalIgnoreCase)
                             || rule.Name.Equals("Obroc", StringComparison.OrdinalIgnoreCase))
                    {
                        await ObdelajPolIzdAsync(order, component, rule.Name, semiFinished, unified, stats, timing, processedSemiMaterials, cancellationToken, depth: 0);
                    }

                    break;
                }
            }
        }

        ClearSingleLineStatus();
        Console.WriteLine($"Processed {orders.Count} orders. Plates={plateDemands.Count}, Unified={unified.Count}, SemiFinished={semiFinished.Count}");
        await WriteOutputAsync(plateDemands, unified, semiFinished, cancellationToken);
    }

    private async Task ObdelajSamotAsync(
        SapOrderHeader plateOrder,
        SapComponent samotComponent,
        ICollection<SemiFinishedTrace> semiFinished,
        ICollection<UnifiedItem> unified,
        ProcessingStats stats,
        TimingCollector timing,
        IDictionary<string, HashSet<string>> processedSemiMaterials,
        CancellationToken cancellationToken)
    {
        var samotOrders = await ObdelajPolIzdAsync(plateOrder, samotComponent, "Samot", semiFinished, unified, stats, timing, processedSemiMaterials, cancellationToken, depth: 0);

        foreach (var samotOrder in samotOrders)
        {
            await ObdelajUliAsync(plateOrder, samotComponent, samotOrder, semiFinished, unified, stats, timing, processedSemiMaterials, cancellationToken);
        }
    }

    private async Task<IReadOnlyList<SapOrderHeader>> ObdelajPolIzdAsync(
        SapOrderHeader plateOrder,
        SapComponent semiComponent,
        string category,
        ICollection<SemiFinishedTrace> semiFinished,
        ICollection<UnifiedItem> unified,
        ProcessingStats stats,
        TimingCollector timing,
        IDictionary<string, HashSet<string>> processedSemiMaterials,
        CancellationToken cancellationToken,
        int depth)
    {
        if (depth > 2)
        {
            return Array.Empty<SapOrderHeader>();
        }

        if (IsAlreadyProcessed(category, semiComponent.Material, processedSemiMaterials))
        {
            return Array.Empty<SapOrderHeader>();
        }

        var shouldRunAfru = !_options.StrictTransferParity || category.Equals("Samot", StringComparison.OrdinalIgnoreCase);
        var lookbackDays = Math.Max(0, _options.SubOrderLookbackDays);
        var minDate = DateTime.Today.AddDays(-lookbackDays);

        var subOrders = await TimedAsync(
            timing,
            "GetProductionOrdersByMaterial",
            () => _sapClient.GetProductionOrdersByMaterialAsync(
                plateOrder.Plant,
                semiComponent.Material,
                plateOrder.OrderNumber,
                cancellationToken));

        if (subOrders.Count == 0)
        {
            subOrders = await TimedAsync(
                timing,
                "GetProductionOrdersByMaterialFallback",
                () => _sapClient.GetProductionOrdersByMaterialAsync(
                    plateOrder.Plant,
                    semiComponent.Material,
                    null,
                    cancellationToken));
        }

        var filteredSubOrders = subOrders
            .Where(subOrder => !string.Equals(subOrder.Status, "TEHZ", StringComparison.OrdinalIgnoreCase)
                               && !string.Equals(subOrder.Status, "ZAKL", StringComparison.OrdinalIgnoreCase))
            .Where(subOrder => subOrder.StartDate.Date >= minDate)
            .ToList();

        foreach (var subOrder in filteredSubOrders)
        {
            var afruDelta = 0;
            if (shouldRunAfru)
            {
                afruDelta = await TimedAsync(timing, "GetAfruYieldDelta", () => _sapClient.GetAfruYieldDeltaAsync(subOrder.OrderNumber, subOrder.StartDate, cancellationToken));
            }

            semiFinished.Add(new SemiFinishedTrace(
                plateOrder.OrderNumber,
                plateOrder.Material,
                category,
                semiComponent.Material,
                subOrder.OrderNumber,
                afruDelta,
                DateTime.UtcNow));
            stats.SemiFinishedRowsWritten++;

            unified.Add(new UnifiedItem(
                plateOrder.OrderNumber,
                FormatMaterialLikeDelphi(plateOrder.Material),
                FormatMaterialLikeDelphi(semiComponent.Material),
                shouldRunAfru ? $"AFRU delta for {subOrder.OrderNumber}" : $"Parity mode (no AFRU) for {subOrder.OrderNumber}",
                $"{category}_AFRU",
                afruDelta,
                DateTime.UtcNow));
            stats.UnifiedRowsWritten++;
            stats.AddCategoryHit($"{category}_AFRU");
        }

        return filteredSubOrders;
    }

    private static bool IsAlreadyProcessed(string category, string material, IDictionary<string, HashSet<string>> processedSemiMaterials)
    {
        var normalizedCategory = category?.Trim() ?? string.Empty;
        var normalizedMaterial = FormatMaterialLikeDelphi(material);
        if (string.IsNullOrWhiteSpace(normalizedMaterial))
        {
            return true;
        }

        if (!processedSemiMaterials.TryGetValue(normalizedCategory, out var processedByCategory))
        {
            processedByCategory = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            processedSemiMaterials[normalizedCategory] = processedByCategory;
        }

        if (processedByCategory.Contains(normalizedMaterial))
        {
            return true;
        }

        processedByCategory.Add(normalizedMaterial);
        return false;
    }

    private async Task ObdelajUliAsync(
        SapOrderHeader plateOrder,
        SapComponent samotComponent,
        SapOrderHeader samotOrder,
        ICollection<SemiFinishedTrace> semiFinished,
        ICollection<UnifiedItem> unified,
        ProcessingStats stats,
        TimingCollector timing,
        IDictionary<string, HashSet<string>> processedSemiMaterials,
        CancellationToken cancellationToken)
    {
        var components = await TimedAsync(timing, "GetComponents", () => _sapClient.GetComponentsAsync(samotOrder.OrderNumber, cancellationToken));
        stats.ComponentRowsRead += components.Count;

        foreach (var cmp in components)
        {
            if (cmp.Description.IndexOf("ULITEK", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                await ObdelajPolIzdAsync(plateOrder, cmp, "Ulitki", semiFinished, unified, stats, timing, processedSemiMaterials, cancellationToken, depth: 1);
                continue;
            }

            if (cmp.Description.IndexOf("SPIRALA", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                semiFinished.Add(new SemiFinishedTrace(
                    plateOrder.OrderNumber,
                    FormatMaterialLikeDelphi(plateOrder.Material),
                    "Spirala",
                    FormatMaterialLikeDelphi(cmp.Material),
                    samotOrder.OrderNumber,
                    0,
                    DateTime.UtcNow));
                stats.SemiFinishedRowsWritten++;

                unified.Add(new UnifiedItem(
                    plateOrder.OrderNumber,
                    FormatMaterialLikeDelphi(plateOrder.Material),
                    FormatMaterialLikeDelphi(cmp.Material),
                    cmp.Description,
                    "Spirala",
                    0,
                    DateTime.UtcNow));
                stats.UnifiedRowsWritten++;
                stats.AddCategoryHit("Spirala");
            }
        }
    }

    private sealed class ProcessingStats
    {
        public int TotalOrdersFetched { get; set; }
        public int SkippedByDateFilter { get; set; }
        public int SkippedByStatus { get; set; }
        public int SkippedByMaterialRule { get; set; }
        public int OrdersAfterCoreFilters { get; set; }
        public int OperationRowsRead { get; set; }
        public int ValidOperations { get; set; }
        public int ConfirmationRowsRead { get; set; }
        public int SkippedByMissingQty { get; set; }
        public int ComponentRowsRead { get; set; }
        public int PlateRecordsWritten { get; set; }
        public int UnifiedRowsWritten { get; set; }
        public int SemiFinishedRowsWritten { get; set; }
        public Dictionary<string, int> CategoryHits { get; } = new(StringComparer.OrdinalIgnoreCase);

        public void AddCategoryHit(string category)
        {
            if (CategoryHits.ContainsKey(category))
            {
                CategoryHits[category] += 1;
            }
            else
            {
                CategoryHits.Add(category, 1);
            }
        }
    }

    private async Task WriteOutputAsync(
        IReadOnlyList<PlateDemandRecord> plateDemands,
        IReadOnlyList<UnifiedItem> unified,
        IReadOnlyList<SemiFinishedTrace> semiFinished,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(_options.OutputDirectory);

        var stamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
        var platePath = Path.Combine(_options.OutputDirectory, $"plates-{stamp}.json");
        var unifiedPath = Path.Combine(_options.OutputDirectory, $"unified-{stamp}.json");

        await WriteAllTextCompatAsync(platePath, JsonSerializer.Serialize(plateDemands, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
        await WriteAllTextCompatAsync(unifiedPath, JsonSerializer.Serialize(unified, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);

        var semiPath = Path.Combine(_options.OutputDirectory, $"semi-finished-{stamp}.json");
        await WriteAllTextCompatAsync(semiPath, JsonSerializer.Serialize(semiFinished, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
    }


    private static async Task<T> TimedAsync<T>(TimingCollector collector, string step, Func<Task<T>> action)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var result = await action().ConfigureAwait(false);
            collector.Add(step, sw.ElapsedMilliseconds);
            return result;
        }
        catch
        {
            collector.Add(step + "(FAIL)", sw.ElapsedMilliseconds);
            throw;
        }
    }

    private sealed class TimingCollector
    {
        private sealed class Item
        {
            public long Count;
            public long TotalMs;
            public long MaxMs;
            public readonly List<long> Samples = new List<long>();
        }

        private readonly object _sync = new object();
        private readonly int _sampleLimit;
        private readonly Dictionary<string, Item> _map = new Dictionary<string, Item>(StringComparer.Ordinal);

        public TimingCollector(int sampleLimit)
        {
            _sampleLimit = Math.Max(1, sampleLimit);
        }

        public void Add(string step, long ms)
        {
            lock (_sync)
            {
                if (!_map.TryGetValue(step, out var item))
                {
                    item = new Item();
                    _map.Add(step, item);
                }

                item.Count++;
                item.TotalMs += ms;
                if (ms > item.MaxMs)
                {
                    item.MaxMs = ms;
                }

                if (item.Samples.Count < _sampleLimit)
                {
                    item.Samples.Add(ms);
                }
            }
        }

        public string ToReportText()
        {
            var sb = new StringBuilder();
            sb.AppendLine("Prenos timing report");
            sb.AppendLine($"Generated: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine();

            List<KeyValuePair<string, Item>> entries;
            lock (_sync)
            {
                entries = _map.ToList();
            }

            foreach (var kv in entries.OrderByDescending(e => e.Value.TotalMs))
            {
                var avg = kv.Value.Count == 0 ? 0 : (double)kv.Value.TotalMs / kv.Value.Count;
                sb.AppendLine($"{kv.Key}");
                sb.AppendLine($"  calls={kv.Value.Count}, totalMs={kv.Value.TotalMs}, avgMs={avg:F2}, maxMs={kv.Value.MaxMs}");
                sb.AppendLine($"  firstSamplesMs=[{string.Join(", ", kv.Value.Samples)}]");
            }

            return sb.ToString();
        }
    }


    private sealed class ProgressStatus
    {
        private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(250);
        private readonly Stopwatch _watch = Stopwatch.StartNew();
        private long _lastUpdateMs = long.MinValue;

        public bool ShouldRender()
        {
            var now = _watch.ElapsedMilliseconds;
            if (now - _lastUpdateMs < Interval.TotalMilliseconds)
            {
                return false;
            }

            _lastUpdateMs = now;
            return true;
        }

        public void ForceNext()
        {
            _lastUpdateMs = long.MinValue;
        }
    }

    private static void TryRenderSingleLineStatus(string message, ProgressStatus status)
    {
        if (!status.ShouldRender())
        {
            return;
        }

        RenderSingleLineStatus(message);
    }

    private static string BuildProgressBar(int current, int total, int width)
    {
        if (total <= 0)
        {
            return "[" + new string('-', Math.Max(1, width)) + "]";
        }

        var safeWidth = Math.Max(5, width);
        var ratio = ClampDouble((double)current / total, 0d, 1d);
        var filled = (int)Math.Round(ratio * safeWidth, MidpointRounding.AwayFromZero);
        filled = ClampInt(filled, 0, safeWidth);

        return "[" + new string('#', filled) + new string('-', safeWidth - filled) + "]";
    }


    private static int ClampInt(int value, int min, int max)
    {
        if (value < min)
        {
            return min;
        }

        if (value > max)
        {
            return max;
        }

        return value;
    }

    private static double ClampDouble(double value, double min, double max)
    {
        if (value < min)
        {
            return min;
        }

        if (value > max)
        {
            return max;
        }

        return value;
    }

    private static void RenderSingleLineStatus(string message)
    {
        if (Console.IsOutputRedirected)
        {
            return;
        }

        var width = Math.Max(20, Console.WindowWidth - 1);
        var text = message.Length > width ? message.Substring(0, width) : message;
        Console.Write("\r" + text.PadRight(width));
        Console.Out.Flush();
    }

    private static void ClearSingleLineStatus()
    {
        if (Console.IsOutputRedirected)
        {
            return;
        }

        var width = Math.Max(20, Console.WindowWidth - 1);
        Console.Write("\r" + new string(' ', width) + "\r");
        Console.Out.Flush();
    }

    private static Task WriteAllTextCompatAsync(string path, string content, CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            File.WriteAllText(path, content);
        }, cancellationToken);
    }


    private static string FormatMaterialLikeDelphi(string material)
    {
        if (string.IsNullOrWhiteSpace(material))
        {
            return string.Empty;
        }

        var trimmed = material.Trim();
        if (trimmed.Length < 18 || trimmed.Any(c => !char.IsDigit(c)))
        {
            return trimmed;
        }

        return string.Concat(
            trimmed.Substring(4, 2),
            ".",
            trimmed.Substring(6, 5),
            ".",
            trimmed.Substring(11, 3),
            "/",
            trimmed.Substring(16, 2));
    }

    private static int ParseTrack(string trackCode)
    {
        var digits = new string(trackCode.Where(char.IsDigit).ToArray());
        return int.TryParse(digits, out var parsed) ? parsed : 0;
    }
}
