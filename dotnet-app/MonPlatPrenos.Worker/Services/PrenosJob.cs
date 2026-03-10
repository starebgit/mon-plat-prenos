using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.Json;
using MonPlatPrenos.Worker.Models;
using Microsoft.Extensions.Options;

namespace MonPlatPrenos.Worker.Services;

public sealed class PrenosJob
{
    private readonly ISapClient _sapClient;
    private readonly PrenosOptions _options;
    private readonly ILogger<PrenosJob> _logger;

    public PrenosJob(ISapClient sapClient, IOptions<PrenosOptions> options, ILogger<PrenosJob> logger)
    {
        _sapClient = sapClient;
        _options = options.Value;
        _logger = logger;
    }

    public Task RunAsync(CancellationToken cancellationToken)
        => RunAsync(forDate: null, cancellationToken);

    public async Task RunAsync(DateTime? forDate, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting prenos job at {Time}. Date filter: {DateFilter}", DateTimeOffset.Now, forDate?.ToString("yyyy-MM-dd") ?? "<none>");

        var plant = "1061";
        var orderFrom = "000005223286";

        var stats = new ProcessingStats();

        var orders = await _sapClient.GetProductionOrdersForPlatesAsync(
            plant,
            _options.SchedulerCode,
            _options.PlateMaterialFrom,
            _options.PlateMaterialTo,
            orderFrom,
            cancellationToken);

        stats.TotalOrdersFetched = orders.Count;

        var plateDemands = new List<PlateDemandRecord>();
        var unified = new List<UnifiedItem>();
        var semiFinished = new List<SemiFinishedTrace>();

        foreach (var order in orders)
        {
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
            var operations = await _sapClient.GetOperationsAsync(order.OrderNumber, cancellationToken);
            var validOperations = operations
                .Where(o => _options.OperationCodes.Any(code => string.Equals(code, o.OperationCode, StringComparison.OrdinalIgnoreCase)))
                .Where(o => o.ConfirmableQty > 0)
                .Where(o => o.StepCode == "0010")
                .ToList();

            stats.OperationRowsRead += operations.Count;
            stats.ValidOperations += validOperations.Count;

            var totalYield = 0;
            foreach (var op in validOperations)
            {
                var confirmations = await _sapClient.GetConfirmationsAsync(order.OrderNumber, op.Confirmation, cancellationToken);
                stats.ConfirmationRowsRead += confirmations.Count;
                totalYield += confirmations.Sum(c => c.Yield);
            }

            var missingQty = order.PlannedQuantity - totalYield;
            if (missingQty <= 0)
            {
                stats.SkippedByMissingQty++;
                continue;
            }

            var track = ParseTrack(order.WorkCenterTrackCode);
            plateDemands.Add(new PlateDemandRecord(track, order.OrderNumber, order.Material, missingQty, order.StartDate));
            stats.PlateRecordsWritten++;

            var components = await _sapClient.GetComponentsAsync(order.OrderNumber, cancellationToken);
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

        _logger.LogInformation(
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
            _logger.LogInformation("Category hit: {Category}={Count}", hit.Key, hit.Value);
        }

        _logger.LogInformation("Finished prenos job. Plate records: {PlateCount}, Unified items: {UnifiedCount}, Semi-finished traces: {SemiCount}", plateDemands.Count, unified.Count, semiFinished.Count);
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
            return Array.Empty<SapOrderHeader>();
        }

        var subOrders = await _sapClient.GetProductionOrdersByMaterialAsync(
            plateOrder.Plant,
            semiComponent.Material,
            plateOrder.OrderNumber,
            cancellationToken);

        if (subOrders.Count == 0)
        {
            subOrders = await _sapClient.GetProductionOrdersByMaterialAsync(
                plateOrder.Plant,
                semiComponent.Material,
                null,
                cancellationToken);
        }

        foreach (var subOrder in subOrders)
        {
            var afruDelta = await _sapClient.GetAfruYieldDeltaAsync(subOrder.OrderNumber, plateOrder.StartDate, cancellationToken);

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
        var components = await _sapClient.GetComponentsAsync(samotOrder.OrderNumber, cancellationToken);
        stats.ComponentRowsRead += components.Count;

        foreach (var cmp in components)
        {
            if (cmp.Description.IndexOf("ULITEK", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                await ObdelajPolIzdAsync(plateOrder, cmp, "Ulitki", semiFinished, unified, stats, cancellationToken, depth: 1);
                continue;
            }

            if (cmp.Description.IndexOf("SPIRALA", StringComparison.OrdinalIgnoreCase) >= 0)
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

        if (_options.EnableDebugJson)
        {
            var semiPath = Path.Combine(_options.OutputDirectory, $"semi-finished-{stamp}.json");
            await WriteAllTextCompatAsync(semiPath, JsonSerializer.Serialize(semiFinished, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
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

            await WriteAllTextCompatAsync(textPath, sb.ToString(), cancellationToken);
        }
    }

    private static Task WriteAllTextCompatAsync(string path, string content, CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            File.WriteAllText(path, content);
        }, cancellationToken);
    }

    private static int ParseTrack(string trackCode)
    {
        var digits = new string(trackCode.Where(char.IsDigit).ToArray());
        return int.TryParse(digits, out var parsed) ? parsed : 0;
    }
}
