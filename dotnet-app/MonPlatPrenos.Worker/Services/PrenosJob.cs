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

                    break;
                }
            }
        }

        await WriteOutputAsync(plateDemands, unified, cancellationToken);

        logger.LogInformation("Finished prenos job. Plate records: {PlateCount}, Unified items: {UnifiedCount}", plateDemands.Count, unified.Count);
    }

    private async Task WriteOutputAsync(
        IReadOnlyList<PlateDemandRecord> plateDemands,
        IReadOnlyList<UnifiedItem> unified,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(_options.OutputDirectory);

        var stamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
        var platePath = Path.Combine(_options.OutputDirectory, $"plates-{stamp}.json");
        var unifiedPath = Path.Combine(_options.OutputDirectory, $"unified-{stamp}.json");

        await File.WriteAllTextAsync(platePath, JsonSerializer.Serialize(plateDemands, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
        await File.WriteAllTextAsync(unifiedPath, JsonSerializer.Serialize(unified, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
    }

    private static int ParseTrack(string trackCode)
    {
        var digits = new string(trackCode.Where(char.IsDigit).ToArray());
        return int.TryParse(digits, out var parsed) ? parsed : 0;
    }
}
