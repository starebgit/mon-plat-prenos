using System.Text;
using System.Text.Json;
using MonPlatPrenos.Worker.Models;
using Microsoft.Extensions.Options;

namespace MonPlatPrenos.Worker.Services;

public sealed class PrenosJob(
    ISapClient sapClient,
    IOptions<PrenosOptions> options,
    ILogger<PrenosJob> logger)
{
    private readonly PrenosOptions _options = options.Value;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting prenos job at {Time}", DateTimeOffset.Now);

        var plant = "1061";
        var orderFrom = "000005223286";

        var orders = await sapClient.GetProductionOrdersForPlatesAsync(
            plant,
            _options.SchedulerCode,
            _options.PlateMaterialFrom,
            _options.PlateMaterialTo,
            orderFrom,
            cancellationToken);

        var plateDemands = new List<PlateDemandRecord>();
        var unified = new List<UnifiedItem>();
        var semiFinished = new List<SemiFinishedTrace>();

        foreach (var order in orders)
        {
            if (order.Status is "TEHZ" or "ZAKL")
            {
                continue;
            }

            if (order.Material.Length < 9 || (order.Material[8] is not ('4' or '3' or '2')))
            {
                continue;
            }

            var operations = await sapClient.GetOperationsAsync(order.OrderNumber, cancellationToken);
            var validOperations = operations
                .Where(o => _options.OperationCodes.Contains(o.OperationCode, StringComparer.OrdinalIgnoreCase))
                .Where(o => o.ConfirmableQty > 0)
                .Where(o => o.StepCode == "0010")
                .ToList();

            var totalYield = 0;
            foreach (var op in validOperations)
            {
                var confirmations = await sapClient.GetConfirmationsAsync(order.OrderNumber, op.Confirmation, cancellationToken);
                totalYield += confirmations.Sum(c => c.Yield);
            }

            var missingQty = order.PlannedQuantity - totalYield;
            if (missingQty <= 0)
            {
                continue;
            }

            var track = ParseTrack(order.WorkCenterTrackCode);
            plateDemands.Add(new PlateDemandRecord(track, order.OrderNumber, order.Material, missingQty, order.StartDate));

            var components = await sapClient.GetComponentsAsync(order.OrderNumber, cancellationToken);
            var allRules = _options.DefaultTerms.Concat(_options.ExtraTerms).ToList();

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

                    if (rule.Name.Equals("Samot", StringComparison.OrdinalIgnoreCase))
                    {
                        await ObdelajSamotAsync(order, component, semiFinished, unified, cancellationToken);
                    }
                    else if (rule.Name.Equals("Protektor", StringComparison.OrdinalIgnoreCase)
                             || rule.Name.Equals("Sponka", StringComparison.OrdinalIgnoreCase)
                             || rule.Name.Equals("Obroc", StringComparison.OrdinalIgnoreCase))
                    {
                        await ObdelajPolIzdAsync(order, component, rule.Name, semiFinished, unified, cancellationToken, depth: 0);
                    }

                    break;
                }
            }
        }

        await WriteOutputAsync(plateDemands, unified, semiFinished, cancellationToken);

        logger.LogInformation("Finished prenos job. Plate records: {PlateCount}, Unified items: {UnifiedCount}, Semi-finished traces: {SemiCount}", plateDemands.Count, unified.Count, semiFinished.Count);
    }

    private async Task ObdelajSamotAsync(
        SapOrderHeader plateOrder,
        SapComponent samotComponent,
        ICollection<SemiFinishedTrace> semiFinished,
        ICollection<UnifiedItem> unified,
        CancellationToken cancellationToken)
    {
        var samotOrders = await ObdelajPolIzdAsync(plateOrder, samotComponent, "Samot", semiFinished, unified, cancellationToken, depth: 0);

        foreach (var samotOrder in samotOrders)
        {
            await ObdelajUliAsync(plateOrder, samotComponent, samotOrder, semiFinished, unified, cancellationToken);
        }
    }

    private async Task<IReadOnlyList<SapOrderHeader>> ObdelajPolIzdAsync(
        SapOrderHeader plateOrder,
        SapComponent semiComponent,
        string category,
        ICollection<SemiFinishedTrace> semiFinished,
        ICollection<UnifiedItem> unified,
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

            unified.Add(new UnifiedItem(
                plateOrder.OrderNumber,
                plateOrder.Material,
                semiComponent.Material,
                $"AFRU delta for {subOrder.OrderNumber}",
                $"{category}_AFRU",
                afruDelta,
                DateTime.UtcNow));
        }

        return subOrders;
    }

    private async Task ObdelajUliAsync(
        SapOrderHeader plateOrder,
        SapComponent samotComponent,
        SapOrderHeader samotOrder,
        ICollection<SemiFinishedTrace> semiFinished,
        ICollection<UnifiedItem> unified,
        CancellationToken cancellationToken)
    {
        var components = await sapClient.GetComponentsAsync(samotOrder.OrderNumber, cancellationToken);

        foreach (var cmp in components)
        {
            if (cmp.Description.Contains("ULITEK", StringComparison.OrdinalIgnoreCase))
            {
                await ObdelajPolIzdAsync(plateOrder, cmp, "Ulitki", semiFinished, unified, cancellationToken, depth: 1);
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

                unified.Add(new UnifiedItem(
                    plateOrder.OrderNumber,
                    plateOrder.Material,
                    cmp.Material,
                    cmp.Description,
                    "Spirala",
                    0,
                    DateTime.UtcNow));
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
