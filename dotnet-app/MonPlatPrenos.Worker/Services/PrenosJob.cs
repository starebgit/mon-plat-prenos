using System.Text;
using System.Text.Json;
using System.Diagnostics;
using MonPlatPrenos.Worker.Models;
using Microsoft.Extensions.Options;

namespace MonPlatPrenos.Worker.Services;

public sealed class PrenosJob(
    ISapClient sapClient,
    IOptions<PrenosOptions> options,
    ILogger<PrenosJob> logger)
{
    private readonly PrenosOptions _options = options.Value;

    public Task RunAsync(CancellationToken cancellationToken)
        => RunAsync(forDate: null, cancellationToken);

    public async Task RunAsync(DateTime? forDate, CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting prenos job at {Time}. Date filter: {DateFilter}", DateTimeOffset.Now, forDate?.ToString("yyyy-MM-dd") ?? "<none>");

        var plant = "1061";
        var orderFrom = "000005223286";

        var stats = new ProcessingStats();

        var orders = await sapClient.GetProductionOrdersForPlatesAsync(
            plant,
            _options.SchedulerCode,
            _options.PlateMaterialFrom,
            _options.PlateMaterialTo,
            orderFrom,
            cancellationToken);

        stats.TotalOrdersFetched = orders.Count;
        if (orders.Count > 0)
        {
            var minDate = orders.Min(o => o.StartDate).ToString("yyyy-MM-dd");
            var maxDate = orders.Max(o => o.StartDate).ToString("yyyy-MM-dd");
            logger.LogInformation("Fetched orders date span: {MinDate} .. {MaxDate}", minDate, maxDate);
        }

        using var progress = new ConsoleProgressTracker(orders.Count);
        var orderIndex = 0;

        var plateDemands = new List<PlateDemandRecord>();
        var unified = new List<UnifiedItem>();
        var semiFinished = new List<SemiFinishedTrace>();

        foreach (var order in orders)
        {
            orderIndex++;
            progress.Tick($"Order {orderIndex}/{orders.Count}: {order.OrderNumber}");

            if (forDate.HasValue && order.StartDate.Date != forDate.Value.Date)
            {
                stats.SkippedByDateFilter++;
                logger.LogInformation("Skip order {OrderNumber}: StartDate={StartDate} does not match filter {FilterDate}", order.OrderNumber, order.StartDate.ToString("yyyy-MM-dd"), forDate.Value.ToString("yyyy-MM-dd"));
                continue;
            }

            if (order.Status is "TEHZ" or "ZAKL")
            {
                stats.SkippedByStatus++;
                logger.LogInformation("Skip order {OrderNumber}: status={Status}", order.OrderNumber, order.Status);
                continue;
            }

            if (order.Material.Length < 9 || (order.Material[8] is not ('4' or '3' or '2')))
            {
                stats.SkippedByMaterialRule++;
                logger.LogInformation("Skip order {OrderNumber}: material={Material}, char[8]={Marker}", order.OrderNumber, order.Material, order.Material.Length > 8 ? order.Material[8] : '?');
                continue;
            }

            stats.OrdersAfterCoreFilters++;
            var operations = await sapClient.GetOperationsAsync(order.OrderNumber, cancellationToken);
            var validOperations = operations
                .Where(o => _options.OperationCodes.Contains(o.OperationCode, StringComparer.OrdinalIgnoreCase))
                .Where(o => o.ConfirmableQty > 0)
                .Where(o => o.StepCode == "0010")
                .ToList();

            stats.OperationRowsRead += operations.Count;
            stats.ValidOperations += validOperations.Count;

            var totalYield = 0;
            foreach (var op in validOperations)
            {
                var confirmations = await sapClient.GetConfirmationsAsync(order.OrderNumber, op.Confirmation, cancellationToken);
                stats.ConfirmationRowsRead += confirmations.Count;
                totalYield += confirmations.Sum(c => c.Yield);
            }

            var missingQty = order.PlannedQuantity - totalYield;
            if (missingQty <= 0)
            {
                stats.SkippedByMissingQty++;
                logger.LogInformation("Skip order {OrderNumber}: planned={Planned}, yield={Yield}, missing={Missing}", order.OrderNumber, order.PlannedQuantity, totalYield, missingQty);
                continue;
            }

            var track = ParseTrack(order.WorkCenterTrackCode);
            plateDemands.Add(new PlateDemandRecord(track, order.OrderNumber, order.Material, missingQty, order.StartDate));
            stats.PlateRecordsWritten++;

            var components = await sapClient.GetComponentsAsync(order.OrderNumber, cancellationToken);
            var allRules = _options.DefaultTerms.Concat(_options.ExtraTerms).ToList();
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
                        order.Material,
                        component.Material,
                        component.Description,
                        rule.Name,
                        missingQty,
                        DateTime.UtcNow));

                    stats.UnifiedRowsWritten++;
                    stats.AddCategoryHit(rule.Name);

                    if (rule.Name.Equals("Samot", StringComparison.OrdinalIgnoreCase))
                    {
                        await ObdelajSamotAsync(order, component, semiFinished, unified, stats, cancellationToken);
                    }
                    else if (rule.Name.Equals("Protektor", StringComparison.OrdinalIgnoreCase)
                             || rule.Name.Equals("Sponka", StringComparison.OrdinalIgnoreCase)
                             || rule.Name.Equals("Obroc", StringComparison.OrdinalIgnoreCase))
                    {
                        await ObdelajPolIzdAsync(order, component, rule.Name, semiFinished, unified, stats, cancellationToken, depth: 0);
                    }

                    break;
                }
            }
        }

        await WriteOutputAsync(plateDemands, unified, semiFinished, cancellationToken);

        logger.LogInformation(
            "Summary: fetched={Fetched}, dateSkip={DateSkip}, statusSkip={StatusSkip}, materialSkip={MaterialSkip}, afterCore={AfterCore}, validOps={ValidOps}/{OpsRead}, confRows={ConfRows}, missingQtySkip={MissingSkip}, componentsRead={ComponentsRead}, plateOut={PlateOut}, unifiedOut={UnifiedOut}, semiOut={SemiOut}",
            stats.TotalOrdersFetched,
            stats.SkippedByDateFilter,
            stats.SkippedByStatus,
            stats.SkippedByMaterialRule,
            stats.OrdersAfterCoreFilters,
            stats.ValidOperations,
            stats.OperationRowsRead,
            stats.ConfirmationRowsRead,
            stats.SkippedByMissingQty,
            stats.ComponentRowsRead,
            stats.PlateRecordsWritten,
            stats.UnifiedRowsWritten,
            stats.SemiFinishedRowsWritten);

        foreach (var hit in stats.CategoryHits.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
        {
            logger.LogInformation("Category hit: {Category}={Count}", hit.Key, hit.Value);
        }

        if (forDate.HasValue && stats.TotalOrdersFetched > 0 && stats.SkippedByDateFilter == stats.TotalOrdersFetched)
        {
            var sampleDates = orders
                .Select(o => o.StartDate.ToString("yyyy-MM-dd"))
                .Distinct(StringComparer.Ordinal)
                .OrderBy(d => d, StringComparer.Ordinal)
                .Take(10)
                .ToList();

            logger.LogWarning("All fetched orders were skipped by date filter {FilterDate}. Sample available StartDate values: {SampleDates}", forDate.Value.ToString("yyyy-MM-dd"), string.Join(", ", sampleDates));
        }

        logger.LogInformation("Finished prenos job. Plate records: {PlateCount}, Unified items: {UnifiedCount}, Semi-finished traces: {SemiCount}", plateDemands.Count, unified.Count, semiFinished.Count);
    }

    private sealed class ConsoleProgressTracker : IDisposable
    {
        private readonly int _total;
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        private int _current;

        public ConsoleProgressTracker(int total)
        {
            _total = Math.Max(total, 1);
            if (!Console.IsOutputRedirected)
            {
                Console.WriteLine("Starting order loop...");
            }
        }

        public void Tick(string label)
        {
            _current = Math.Min(_current + 1, _total);
            var pct = (_current * 100) / _total;
            const int barSize = 30;
            var filled = (_current * barSize) / _total;
            var bar = new string('#', filled) + new string('-', barSize - filled);
            var line = $"[{bar}] {pct,3}% ({_current}/{_total}) {label}";

            if (!Console.IsOutputRedirected)
            {
                Console.Write($"\r{line}");
                if (_current == _total)
                {
                    Console.WriteLine();
                }
            }
            else if (_current == 1 || _current == _total || _current % 25 == 0)
            {
                Console.WriteLine(line);
            }
        }

        public void Dispose()
        {
            if (!Console.IsOutputRedirected)
            {
                Console.WriteLine($"Processed {_current} order(s) in {_stopwatch.Elapsed}.");
            }
        }
    }

    private async Task ObdelajSamotAsync(
        SapOrderHeader plateOrder,
        SapComponent samotComponent,
        ICollection<SemiFinishedTrace> semiFinished,
        ICollection<UnifiedItem> unified,
        ProcessingStats stats,
        CancellationToken cancellationToken)
    {
        var samotOrders = await ObdelajPolIzdAsync(plateOrder, samotComponent, "Samot", semiFinished, unified, stats, cancellationToken, depth: 0);

        foreach (var samotOrder in samotOrders)
        {
            await ObdelajUliAsync(plateOrder, samotComponent, samotOrder, semiFinished, unified, stats, cancellationToken);
        }
    }

    private async Task<IReadOnlyList<SapOrderHeader>> ObdelajPolIzdAsync(
        SapOrderHeader plateOrder,
        SapComponent semiComponent,
        string category,
        ICollection<SemiFinishedTrace> semiFinished,
        ICollection<UnifiedItem> unified,
        ProcessingStats stats,
        CancellationToken cancellationToken,
        int depth)
    {
        if (depth > 2)
        {
            return [];
        }

        var subOrders = await sapClient.GetProductionOrdersByMaterialAsync(
            plateOrder.Plant,
            semiComponent.Material,
            plateOrder.OrderNumber,
            cancellationToken);

        if (subOrders.Count == 0)
        {
            subOrders = await sapClient.GetProductionOrdersByMaterialAsync(
                plateOrder.Plant,
                semiComponent.Material,
                null,
                cancellationToken);
        }

        foreach (var subOrder in subOrders)
        {
            var afruDelta = await sapClient.GetAfruYieldDeltaAsync(subOrder.OrderNumber, plateOrder.StartDate, cancellationToken);

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
                plateOrder.Material,
                semiComponent.Material,
                $"AFRU delta for {subOrder.OrderNumber}",
                $"{category}_AFRU",
                afruDelta,
                DateTime.UtcNow));
            stats.UnifiedRowsWritten++;
            stats.AddCategoryHit($"{category}_AFRU");
        }

        return subOrders;
    }

    private async Task ObdelajUliAsync(
        SapOrderHeader plateOrder,
        SapComponent samotComponent,
        SapOrderHeader samotOrder,
        ICollection<SemiFinishedTrace> semiFinished,
        ICollection<UnifiedItem> unified,
        ProcessingStats stats,
        CancellationToken cancellationToken)
    {
        var components = await sapClient.GetComponentsAsync(samotOrder.OrderNumber, cancellationToken);
        stats.ComponentRowsRead += components.Count;

        foreach (var cmp in components)
        {
            if (cmp.Description.Contains("ULITEK", StringComparison.OrdinalIgnoreCase))
            {
                await ObdelajPolIzdAsync(plateOrder, cmp, "Ulitki", semiFinished, unified, stats, cancellationToken, depth: 1);
                continue;
            }

            if (cmp.Description.Contains("SPIRALA", StringComparison.OrdinalIgnoreCase))
            {
                semiFinished.Add(new SemiFinishedTrace(
                    plateOrder.OrderNumber,
                    plateOrder.Material,
                    "Spirala",
                    cmp.Material,
                    samotOrder.OrderNumber,
                    0,
                    DateTime.UtcNow));
                stats.SemiFinishedRowsWritten++;

                unified.Add(new UnifiedItem(
                    plateOrder.OrderNumber,
                    plateOrder.Material,
                    cmp.Material,
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
            if (!CategoryHits.TryAdd(category, 1))
            {
                CategoryHits[category] += 1;
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

        await File.WriteAllTextAsync(platePath, JsonSerializer.Serialize(plateDemands, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
        await File.WriteAllTextAsync(unifiedPath, JsonSerializer.Serialize(unified, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);

        if (_options.EnableDebugJson)
        {
            var semiPath = Path.Combine(_options.OutputDirectory, $"semi-finished-{stamp}.json");
            await File.WriteAllTextAsync(semiPath, JsonSerializer.Serialize(semiFinished, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
        }

        if (_options.EnableDebugTextDump)
        {
            var textPath = Path.Combine(_options.OutputDirectory, $"prenos-debug-{stamp}.txt");
            var sb = new StringBuilder();
            sb.AppendLine("=== PLATES ===");
            foreach (var p in plateDemands)
            {
                sb.AppendLine($"{p.OrderNumber};{p.Material};Q={p.Quantity};Track={p.Track};Start={p.StartDate:yyyy-MM-dd}");
            }

            sb.AppendLine();
            sb.AppendLine("=== UNIFIED ===");
            foreach (var u in unified)
            {
                sb.AppendLine($"{u.OrderNumber};{u.Category};{u.ComponentMaterial};Req={u.RequiredQty};{u.ComponentDescription}");
            }

            sb.AppendLine();
            sb.AppendLine("=== SEMI-FINISHED ===");
            foreach (var s in semiFinished)
            {
                sb.AppendLine($"{s.PlateOrder};{s.Category};Semi={s.SemiMaterial};Order={s.SemiOrder};AFRU={s.AfruYieldDelta}");
            }

            await File.WriteAllTextAsync(textPath, sb.ToString(), cancellationToken);
        }
    }

    private static int ParseTrack(string trackCode)
    {
        var digits = new string(trackCode.Where(char.IsDigit).ToArray());
        return int.TryParse(digits, out var parsed) ? parsed : 0;
    }
}
