using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MonPlatPrenos.Worker.Models;

namespace MonPlatPrenos.Worker.Services;

public interface ISapClient
{
    Task<IReadOnlyList<SapOrderHeader>> GetProductionOrdersForPlatesAsync(
        string plant,
        string schedulerCode,
        string materialFrom,
        string materialTo,
        string orderFrom,
        DateTime? fromDate,
        DateTime? toDate,
        CancellationToken cancellationToken);

    Task<IReadOnlyList<SapOperation>> GetOperationsAsync(string orderNumber, CancellationToken cancellationToken);

    Task<IReadOnlyList<SapConfirmation>> GetConfirmationsAsync(string orderNumber, string confirmation, CancellationToken cancellationToken);

    Task<IReadOnlyList<SapComponent>> GetComponentsAsync(string orderNumber, CancellationToken cancellationToken);

    Task<IReadOnlyList<SapOrderHeader>> GetProductionOrdersByMaterialAsync(
        string plant,
        string material,
        string? orderFrom,
        CancellationToken cancellationToken);

    Task<int> GetAfruYieldDeltaAsync(string orderNumber, DateTime fromDate, CancellationToken cancellationToken);

    Task<int> GetMaterialStockAsync(string material18, string plant, CancellationToken cancellationToken);
}
