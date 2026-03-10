using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MonPlatPrenos.Worker.Models;

namespace MonPlatPrenos.Worker.Services;

public sealed class MockSapClient : ISapClient
{
    private static readonly IReadOnlyList<SapOrderHeader> Orders = new List<SapOrderHeader>
    {
        new SapOrderHeader("000005223286", "000012080000000000", "REL", 110, DateTime.Today, "TRAK-01", "200", "1061"),
        new SapOrderHeader("000005223287", "000019789000000000", "REL", 95, DateTime.Today, "TRAK-02", "200", "1061"),
        new SapOrderHeader("000005223288", "000011000000000000", "TEHZ", 80, DateTime.Today, "TRAK-03", "200", "1061")
    };

    public Task<IReadOnlyList<SapOrderHeader>> GetProductionOrdersForPlatesAsync(
        string plant,
        string schedulerCode,
        string materialFrom,
        string materialTo,
        string orderFrom,
        CancellationToken cancellationToken)
    {
        var filtered = Orders
            .Where(o => o.Plant == plant)
            .Where(o => o.SchedulerCode == schedulerCode)
            .Where(o => string.CompareOrdinal(o.Material, materialFrom) >= 0 && string.CompareOrdinal(o.Material, materialTo) <= 0)
            .Where(o => string.CompareOrdinal(o.OrderNumber, orderFrom) >= 0)
            .ToList();

        return Task.FromResult<IReadOnlyList<SapOrderHeader>>(filtered);
    }

    public Task<IReadOnlyList<SapOperation>> GetOperationsAsync(string orderNumber, CancellationToken cancellationToken)
    {
        IReadOnlyList<SapOperation> result = new List<SapOperation>
        {
            new SapOperation(orderNumber, "C1", "PP04", 10, "0010"),
            new SapOperation(orderNumber, "C2", "PP99", 5, "0010")
        };

        return Task.FromResult(result);
    }

    public Task<IReadOnlyList<SapConfirmation>> GetConfirmationsAsync(string orderNumber, string confirmation, CancellationToken cancellationToken)
    {
        IReadOnlyList<SapConfirmation> result = new List<SapConfirmation>
        {
            new SapConfirmation(confirmation, "1", 15),
            new SapConfirmation(confirmation, "2", 7)
        };

        return Task.FromResult(result);
    }

    public Task<IReadOnlyList<SapComponent>> GetComponentsAsync(string orderNumber, CancellationToken cancellationToken)
    {
        IReadOnlyList<SapComponent> result = new List<SapComponent>
        {
            new SapComponent(orderNumber, "000000000000000111", "SAMOT A"),
            new SapComponent(orderNumber, "000000000000000222", "PROTEKTOR B"),
            new SapComponent(orderNumber, "000000000000000333", "SPONKA C"),
            new SapComponent(orderNumber, "000000000000000444", "OBRO D"),
            new SapComponent(orderNumber, "000000000000000555", "PAKIRNI OBRO"),
            new SapComponent(orderNumber, "000000000000000666", "YOUR_TERM_HERE")
        };

        return Task.FromResult(result);
    }

    public Task<IReadOnlyList<SapOrderHeader>> GetProductionOrdersByMaterialAsync(string plant, string material, string? orderFrom, CancellationToken cancellationToken)
    {
        IReadOnlyList<SapOrderHeader> result = new List<SapOrderHeader>
        {
            new SapOrderHeader("000006945920", material, "REL", 120, DateTime.Today, "TRAK-01", "200", plant)
        };

        return Task.FromResult(result);
    }

    public Task<int> GetAfruYieldDeltaAsync(string orderNumber, DateTime fromDate, CancellationToken cancellationToken)
    {
        return Task.FromResult(12);
    }
}
