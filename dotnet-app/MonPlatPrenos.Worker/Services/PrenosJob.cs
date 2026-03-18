using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
        var runSw = Stopwatch.StartNew();
        var parityBenchmarkModeEnabled = _options.ParityBenchmarkMode.Enabled;
        var parityModeEnabled = _options.ParityMode.Enabled || parityBenchmarkModeEnabled;
        var benchmarkEnabled = _options.Benchmark.Enabled || parityBenchmarkModeEnabled;
        var outputDirectory = GetOutputDirectoryPath();
        var effectiveFromDate = ResolveFromDate(forDate, parityModeEnabled);
        var activeFromDateFilter = _options.ApplyFromDateFilter ? effectiveFromDate : null;

        var plant = _options.Plant;
        var orderFrom = ResolveOrderFrom(parityModeEnabled);
        Console.WriteLine($"Run mode: {(parityBenchmarkModeEnabled ? "PARITY-BENCHMARK" : parityModeEnabled ? "PARITY" : "NORMAL")}");
        Console.WriteLine($"Output directory: {outputDirectory}");
        Console.WriteLine($"Effective fromDate: {(activeFromDateFilter.HasValue ? activeFromDateFilter.Value.ToString("yyyy-MM-dd") : "ALL")}, orderFrom: {orderFrom}");

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
                activeFromDateFilter,
                activeFromDateFilter,
                cancellationToken));

        stats.TotalOrdersFetched = orders.Count;
        await WriteFetchedCodesLogAsync(orders, cancellationToken);
        var maxFetchedOrderNumber = orders.Select(o => o.OrderNumber).Where(o => !string.IsNullOrWhiteSpace(o)).OrderBy(o => o, StringComparer.Ordinal).LastOrDefault();
        RenderSingleLineStatus($"Fetched {orders.Count} production orders. Processing...");
        status.ForceNext();

        var sapCallLimit = Math.Max(1, _options.MaxSapCallsInFlight);
        using var sapCallSemaphore = new SemaphoreSlim(sapCallLimit, sapCallLimit);
        var orderConcurrency = Math.Max(1, _options.OrderConcurrency);

        var orderTasks = orders.Select((order, orderIndex) => (Func<Task<OrderProcessingResult>>)(() => ProcessOrderAsync(
            order,
            orderIndex,
            orders.Count,
            activeFromDateFilter,
            operationCodes,
            allRules,
            timing,
            sapCallSemaphore,
            status,
            cancellationToken)));

        var orderedResults = orderConcurrency <= 1
            ? await RunSequentialAsync(orderTasks, cancellationToken)
            : await RunConcurrentAsync(orderTasks, orderConcurrency, cancellationToken);

        var plateDemands = new List<PlateDemandRecord>();
        var unified = new List<UnifiedItem>();
        var semiFinished = new List<SemiFinishedTrace>();

        foreach (var result in orderedResults)
        {
            stats.MergeFrom(result.Stats);
            plateDemands.AddRange(result.PlateDemands);
            unified.AddRange(result.Unified);
            semiFinished.AddRange(result.SemiFinished);
        }

        ClearSingleLineStatus();
        var runtimeMs = runSw.ElapsedMilliseconds;
        Console.WriteLine($"Processed {orders.Count} orders. Plates={plateDemands.Count}, Unified={unified.Count}, SemiFinished={semiFinished.Count}");
        await WriteOutputAsync(plateDemands, unified, semiFinished, cancellationToken);

        if (benchmarkEnabled)
        {
            await WriteBenchmarkArtifactsAsync(
                plateDemands,
                unified,
                semiFinished,
                stats,
                timing,
                runtimeMs,
                activeFromDateFilter,
                orderFrom,
                cancellationToken);
        }

        if (parityBenchmarkModeEnabled)
        {
            await WriteParityBenchmarkRunLogAsync(runtimeMs, activeFromDateFilter, orderFrom, plateDemands.Count, unified.Count, semiFinished.Count, cancellationToken);
        }

        if (!parityModeEnabled)
        {
            TryPersistOrderFromWatermark(maxFetchedOrderNumber);
        }
    }

    private DateTime? ResolveFromDate(DateTime? requestedFromDate, bool parityModeEnabled)
    {
        if (!parityModeEnabled)
        {
            return requestedFromDate;
        }

        var (fixedFromDate, fixedOrderFrom, sourceKey) = GetParityLockValues();
        if (string.IsNullOrWhiteSpace(fixedFromDate) || string.IsNullOrWhiteSpace(fixedOrderFrom))
        {
            throw new InvalidOperationException($"{sourceKey} requires both FixedFromDate and FixedOrderFrom to be set.");
        }

        if (!DateTime.TryParse(fixedFromDate, out var parsed))
        {
            throw new InvalidOperationException($"Parity mode fixed date is invalid: '{fixedFromDate}'. Set {sourceKey}:FixedFromDate to a valid date string.");
        }

        return parsed.Date;
    }

    private string ResolveOrderFrom(bool parityModeEnabled)
    {
        if (parityModeEnabled)
        {
            var (_, fixedOrderFrom, sourceKey) = GetParityLockValues();
            if (string.IsNullOrWhiteSpace(fixedOrderFrom))
            {
                throw new InvalidOperationException($"{sourceKey}:FixedOrderFrom must be set when parity mode is enabled.");
            }

            return fixedOrderFrom.Trim();
        }
        var configured = _options.OrderFrom?.Trim() ?? string.Empty;
        if (!_options.Watermark.Enabled)
        {
            return configured;
        }

        var watermark = TryReadOrderFromWatermark();
        if (string.IsNullOrWhiteSpace(watermark))
        {
            return configured;
        }

        if (string.IsNullOrWhiteSpace(configured))
        {
            return watermark;
        }

        return string.CompareOrdinal(watermark, configured) > 0 ? watermark : configured;
    }

    private string? TryReadOrderFromWatermark()
    {
        try
        {
            var path = ResolvePathFromCurrentDirectory(_options.Watermark.FilePath);
            if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            {
                return null;
            }

            var value = File.ReadAllText(path).Trim();
            return string.IsNullOrWhiteSpace(value) ? null : value;
        }
        catch
        {
            return null;
        }
    }

    private void TryPersistOrderFromWatermark(string? maxFetchedOrderNumber)
    {
        if (!_options.Watermark.Enabled || string.IsNullOrWhiteSpace(maxFetchedOrderNumber))
        {
            return;
        }

        try
        {
            var path = ResolvePathFromCurrentDirectory(_options.Watermark.FilePath);
            if (string.IsNullOrWhiteSpace(path))
            {
                return;
            }

            var existing = TryReadOrderFromWatermark();
            if (!string.IsNullOrWhiteSpace(existing) && string.CompareOrdinal(existing, maxFetchedOrderNumber) >= 0)
            {
                return;
            }

            var dir = Path.GetDirectoryName(path);
            if (!string.IsNullOrWhiteSpace(dir))
            {
                Directory.CreateDirectory(dir);
            }

            File.WriteAllText(path, maxFetchedOrderNumber);
        }
        catch
        {
            // watermark is best-effort and should never fail the job
        }
    }

    private async Task<List<OrderProcessingResult>> RunSequentialAsync(IEnumerable<Func<Task<OrderProcessingResult>>> orderTaskFactories, CancellationToken cancellationToken)
    {
        var results = new List<OrderProcessingResult>();
        foreach (var taskFactory in orderTaskFactories)
        {
            cancellationToken.ThrowIfCancellationRequested();
            results.Add(await taskFactory().ConfigureAwait(false));
        }

        return results.OrderBy(r => r.OrderIndex).ToList();
    }

    private static async Task<List<OrderProcessingResult>> RunConcurrentAsync(IEnumerable<Func<Task<OrderProcessingResult>>> taskFactories, int concurrency, CancellationToken cancellationToken)
    {
        using var orderSemaphore = new SemaphoreSlim(concurrency, concurrency);
        var wrappedTasks = taskFactories.Select(async taskFactory =>
        {
            await orderSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await taskFactory().ConfigureAwait(false);
            }
            finally
            {
                orderSemaphore.Release();
            }
        });

        var results = await Task.WhenAll(wrappedTasks).ConfigureAwait(false);
        return results.OrderBy(r => r.OrderIndex).ToList();
    }

    private async Task<OrderProcessingResult> ProcessOrderAsync(
        SapOrderHeader order,
        int orderIndex,
        int totalOrders,
        DateTime? forDate,
        HashSet<string> operationCodes,
        IReadOnlyList<TermRule> allRules,
        TimingCollector timing,
        SemaphoreSlim sapCallSemaphore,
        ProgressStatus status,
        CancellationToken cancellationToken)
    {
        var result = new OrderProcessingResult(orderIndex);
        var stats = result.Stats;

        if (_options.OrderConcurrency <= 1)
        {
            var progressBar = BuildProgressBar(orderIndex + 1, totalOrders, 22);
            RenderSingleLineStatus($"{progressBar} {orderIndex + 1}/{totalOrders} | {order.OrderNumber}");
            status.ForceNext();
        }

        if (forDate.HasValue && order.StartDate.Date != forDate.Value.Date)
        {
            stats.SkippedByDateFilter++;
            return result;
        }

        if (IsTechnicallyClosedStatus(order.Status))
        {
            stats.SkippedByStatus++;
            return result;
        }

        if (order.Material.Length < 9 || (order.Material[8] is not ('4' or '3' or '2')))
        {
            stats.SkippedByMaterialRule++;
            return result;
        }

        stats.OrdersAfterCoreFilters++;
        var operations = await TimedSapCallAsync(timing, "GetOperations", sapCallSemaphore, cancellationToken, () => _sapClient.GetOperationsAsync(order.OrderNumber, cancellationToken));
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
            foreach (var op in validOperations)
            {
                var confirmations = await TimedSapCallAsync(timing, "GetConfirmations", sapCallSemaphore, cancellationToken, () => _sapClient.GetConfirmationsAsync(order.OrderNumber, op.Confirmation, cancellationToken));
                stats.ConfirmationRowsRead += confirmations.Count;
                totalYield += confirmations.Sum(c => c.Yield);
            }
        }
        else
        {
            using var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
            var confirmationTasks = validOperations.Select(async op =>
            {
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    var confirmations = await TimedSapCallAsync(timing, "GetConfirmations", sapCallSemaphore, cancellationToken, () => _sapClient.GetConfirmationsAsync(order.OrderNumber, op.Confirmation, cancellationToken)).ConfigureAwait(false);
                    return (Count: confirmations.Count, Yield: confirmations.Sum(c => c.Yield));
                }
                finally
                {
                    semaphore.Release();
                }
            });

            var confirmationResults = await Task.WhenAll(confirmationTasks).ConfigureAwait(false);
            foreach (var item in confirmationResults)
            {
                stats.ConfirmationRowsRead += item.Count;
                totalYield += item.Yield;
            }
        }

        var missingQty = order.PlannedQuantity - totalYield;
        if (missingQty <= 0)
        {
            stats.SkippedByMissingQty++;
            return result;
        }

        var operationTrackCode = validOperations
            .Select(o => o.WorkCenterCode)
            .FirstOrDefault(code => !string.IsNullOrWhiteSpace(code));
        var track = ParseTrack(operationTrackCode ?? order.WorkCenterTrackCode);
        var formattedPlateMaterial = FormatMaterialLikeDelphi(order.Material);
        result.PlateDemands.Add(new PlateDemandRecord(track, order.OrderNumber, formattedPlateMaterial, missingQty, order.StartDate));
        stats.PlateRecordsWritten++;

        var components = await TimedSapCallAsync(timing, "GetComponents", sapCallSemaphore, cancellationToken, () => _sapClient.GetComponentsAsync(order.OrderNumber, cancellationToken));
        stats.ComponentRowsRead += components.Count;

        foreach (var component in components)
        {
            foreach (var rule in allRules)
            {
                if (!rule.IsMatch(component.Description))
                {
                    continue;
                }

                result.Unified.Add(new UnifiedItem(
                    order.OrderNumber,
                    formattedPlateMaterial,
                    FormatMaterialLikeDelphi(component.Material),
                    component.Description,
                    rule.Name,
                    missingQty,
                    DateTime.UtcNow));

                stats.UnifiedRowsWritten++;
                stats.AddCategoryHit(rule.Name);

                if (_options.EnableSemiFinishedExpansion)
                {
                    if (rule.Name.Equals("Samot", StringComparison.OrdinalIgnoreCase))
                    {
                        await ObdelajSamotAsync(order, component, result.SemiFinished, result.Unified, stats, timing, sapCallSemaphore, cancellationToken);
                    }
                    else if (rule.Name.Equals("Protektor", StringComparison.OrdinalIgnoreCase)
                             || rule.Name.Equals("Sponka", StringComparison.OrdinalIgnoreCase)
                             || rule.Name.Equals("Obroc", StringComparison.OrdinalIgnoreCase))
                    {
                        await ObdelajPolIzdAsync(order, component, rule.Name, result.SemiFinished, result.Unified, stats, timing, sapCallSemaphore, cancellationToken, depth: 0);
                    }
                }

                break;
            }
        }

        return result;
    }

    private async Task ObdelajSamotAsync(
        SapOrderHeader plateOrder,
        SapComponent samotComponent,
        ICollection<SemiFinishedTrace> semiFinished,
        ICollection<UnifiedItem> unified,
        ProcessingStats stats,
        TimingCollector timing,
        SemaphoreSlim sapCallSemaphore,
        CancellationToken cancellationToken)
    {
        var samotOrders = await ObdelajPolIzdAsync(plateOrder, samotComponent, "Samot", semiFinished, unified, stats, timing, sapCallSemaphore, cancellationToken, depth: 0);

        foreach (var samotOrder in samotOrders)
        {
            await ObdelajUliAsync(plateOrder, samotComponent, samotOrder, semiFinished, unified, stats, timing, sapCallSemaphore, cancellationToken);
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
        SemaphoreSlim sapCallSemaphore,
        CancellationToken cancellationToken,
        int depth)
    {
        if (depth > 2)
        {
            return Array.Empty<SapOrderHeader>();
        }

        var subOrders = await TimedSapCallAsync(
            timing,
            "GetProductionOrdersByMaterial",
            sapCallSemaphore,
            cancellationToken,
            () => _sapClient.GetProductionOrdersByMaterialAsync(
                plateOrder.Plant,
                semiComponent.Material,
                plateOrder.OrderNumber,
                cancellationToken));

        if (subOrders.Count == 0)
        {
            subOrders = await TimedSapCallAsync(
                timing,
                "GetProductionOrdersByMaterialFallback",
                sapCallSemaphore,
                cancellationToken,
                () => _sapClient.GetProductionOrdersByMaterialAsync(
                    plateOrder.Plant,
                    semiComponent.Material,
                    null,
                    cancellationToken));
        }

        foreach (var subOrder in subOrders)
        {
            var afruDelta = await TimedSapCallAsync(timing, "GetAfruYieldDelta", sapCallSemaphore, cancellationToken, () => _sapClient.GetAfruYieldDeltaAsync(subOrder.OrderNumber, plateOrder.StartDate, cancellationToken));

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
        TimingCollector timing,
        SemaphoreSlim sapCallSemaphore,
        CancellationToken cancellationToken)
    {
        var components = await TimedSapCallAsync(timing, "GetComponents", sapCallSemaphore, cancellationToken, () => _sapClient.GetComponentsAsync(samotOrder.OrderNumber, cancellationToken));
        stats.ComponentRowsRead += components.Count;

        foreach (var cmp in components)
        {
            if (cmp.Description.IndexOf("ULITEK", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                await ObdelajPolIzdAsync(plateOrder, cmp, "Ulitki", semiFinished, unified, stats, timing, sapCallSemaphore, cancellationToken, depth: 1);
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

    private sealed class OrderProcessingResult
    {
        public OrderProcessingResult(int orderIndex)
        {
            OrderIndex = orderIndex;
        }

        public int OrderIndex { get; }
        public ProcessingStats Stats { get; } = new ProcessingStats();
        public List<PlateDemandRecord> PlateDemands { get; } = new List<PlateDemandRecord>();
        public List<UnifiedItem> Unified { get; } = new List<UnifiedItem>();
        public List<SemiFinishedTrace> SemiFinished { get; } = new List<SemiFinishedTrace>();
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

        public void MergeFrom(ProcessingStats other)
        {
            TotalOrdersFetched += other.TotalOrdersFetched;
            SkippedByDateFilter += other.SkippedByDateFilter;
            SkippedByStatus += other.SkippedByStatus;
            SkippedByMaterialRule += other.SkippedByMaterialRule;
            OrdersAfterCoreFilters += other.OrdersAfterCoreFilters;
            OperationRowsRead += other.OperationRowsRead;
            ValidOperations += other.ValidOperations;
            ConfirmationRowsRead += other.ConfirmationRowsRead;
            SkippedByMissingQty += other.SkippedByMissingQty;
            ComponentRowsRead += other.ComponentRowsRead;
            PlateRecordsWritten += other.PlateRecordsWritten;
            UnifiedRowsWritten += other.UnifiedRowsWritten;
            SemiFinishedRowsWritten += other.SemiFinishedRowsWritten;

            foreach (var hit in other.CategoryHits)
            {
                if (CategoryHits.ContainsKey(hit.Key))
                {
                    CategoryHits[hit.Key] += hit.Value;
                }
                else
                {
                    CategoryHits.Add(hit.Key, hit.Value);
                }
            }
        }
    }

    private async Task WriteOutputAsync(
        IReadOnlyList<PlateDemandRecord> plateDemands,
        IReadOnlyList<UnifiedItem> unified,
        IReadOnlyList<SemiFinishedTrace> semiFinished,
        CancellationToken cancellationToken)
    {
        var outputDirectory = GetOutputDirectoryPath();
        Directory.CreateDirectory(outputDirectory);

        var stamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
        var platePath = Path.Combine(outputDirectory, $"plates-{stamp}.json");
        var unifiedPath = Path.Combine(outputDirectory, $"unified-{stamp}.json");

        await WriteAllTextCompatAsync(platePath, JsonSerializer.Serialize(plateDemands, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
        await WriteAllTextCompatAsync(unifiedPath, JsonSerializer.Serialize(unified, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);

        var semiPath = Path.Combine(outputDirectory, $"semi-finished-{stamp}.json");
        await WriteAllTextCompatAsync(semiPath, JsonSerializer.Serialize(semiFinished, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
    }

    private async Task WriteFetchedCodesLogAsync(IReadOnlyList<SapOrderHeader> orders, CancellationToken cancellationToken)
    {
        var outputDirectory = GetOutputDirectoryPath();
        Directory.CreateDirectory(outputDirectory);

        var stamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
        var filePattern = string.IsNullOrWhiteSpace(_options.FetchedCodeLogFilePattern)
            ? "fetched-codes-{timestamp}.txt"
            : _options.FetchedCodeLogFilePattern;
        var fileName = filePattern.Replace("{timestamp}", stamp);
        var path = Path.IsPathRooted(fileName)
            ? fileName
            : Path.Combine(outputDirectory, fileName);

        var lines = orders
            .OrderBy(o => o.OrderNumber, StringComparer.Ordinal)
            .Select(o => $"{o.OrderNumber}|{FormatMaterialLikeDelphi(o.Material)}|{o.Status}|{o.StartDate:yyyy-MM-dd}")
            .ToList();
        if (lines.Count == 0)
        {
            lines.Add("# NO_ORDERS_FETCHED");
        }

        await WriteAllTextCompatAsync(path, string.Join(Environment.NewLine, lines) + Environment.NewLine, cancellationToken);
        Console.WriteLine($"Fetched code log written: {path}");
    }



    private async Task WriteBenchmarkArtifactsAsync(
        IReadOnlyList<PlateDemandRecord> plateDemands,
        IReadOnlyList<UnifiedItem> unified,
        IReadOnlyList<SemiFinishedTrace> semiFinished,
        ProcessingStats stats,
        TimingCollector timing,
        long runtimeMs,
        DateTime? effectiveFromDate,
        string effectiveOrderFrom,
        CancellationToken cancellationToken)
    {
        var outputDirectory = GetOutputDirectoryPath();
        Directory.CreateDirectory(outputDirectory);

        var stamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
        var benchmarkPath = Path.Combine(outputDirectory, $"benchmark-{stamp}.json");
        var benchmark = BuildBenchmarkSnapshot(plateDemands, unified, semiFinished, stats, timing, runtimeMs, effectiveFromDate, effectiveOrderFrom);

        await WriteAllTextCompatAsync(
            benchmarkPath,
            JsonSerializer.Serialize(benchmark, new JsonSerializerOptions { WriteIndented = true }),
            cancellationToken);

        Console.WriteLine($"Benchmark snapshot written: {benchmarkPath}");

        if (!string.IsNullOrWhiteSpace(_options.Benchmark.SnapshotPath))
        {
            var snapshotPath = _options.Benchmark.SnapshotPath!;
            var snapshotDir = Path.GetDirectoryName(snapshotPath);
            if (!string.IsNullOrWhiteSpace(snapshotDir))
            {
                Directory.CreateDirectory(snapshotDir);
            }

            await WriteAllTextCompatAsync(
                snapshotPath,
                JsonSerializer.Serialize(benchmark, new JsonSerializerOptions { WriteIndented = true }),
                cancellationToken);

            Console.WriteLine($"Benchmark baseline snapshot saved: {snapshotPath}");
        }

        if (!string.IsNullOrWhiteSpace(_options.Benchmark.CompareSnapshotPath))
        {
            var comparePath = _options.Benchmark.CompareSnapshotPath!;
            if (!File.Exists(comparePath))
            {
                Console.WriteLine($"Benchmark compare snapshot not found: {comparePath}");
                return;
            }

            var baselineJson = await ReadAllTextCompatAsync(comparePath, cancellationToken);
            var baseline = JsonSerializer.Deserialize<BenchmarkSnapshot>(baselineJson);
            if (baseline is null)
            {
                throw new InvalidOperationException($"Failed to parse benchmark compare snapshot: {comparePath}");
            }

            var diffs = GetOutputDiffs(baseline.OutputDigest, benchmark.OutputDigest).ToList();
            if (diffs.Count == 0)
            {
                Console.WriteLine("Benchmark parity check PASSED (no output digest differences).");
                return;
            }

            var message = "Benchmark parity check FAILED:" + Environment.NewLine + string.Join(Environment.NewLine, diffs.Select(d => " - " + d));
            throw new InvalidOperationException(message);
        }
    }

    private async Task WriteParityBenchmarkRunLogAsync(
        long runtimeMs,
        DateTime? effectiveFromDate,
        string effectiveOrderFrom,
        int platesCount,
        int unifiedCount,
        int semiFinishedCount,
        CancellationToken cancellationToken)
    {
        var outputDirectory = GetOutputDirectoryPath();
        Directory.CreateDirectory(outputDirectory);
        var stamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
        var path = Path.Combine(outputDirectory, $"parity-benchmark-run-{stamp}.log");
        var lines = new[]
        {
            "MODE=PARITY_BENCHMARK",
            $"UTC={DateTime.UtcNow:O}",
            $"FROM_DATE={(effectiveFromDate.HasValue ? effectiveFromDate.Value.ToString("yyyy-MM-dd") : "ALL")}",
            $"ORDER_FROM={effectiveOrderFrom}",
            $"RUNTIME_MS={runtimeMs}",
            $"PLATES={platesCount}",
            $"UNIFIED={unifiedCount}",
            $"SEMI_FINISHED={semiFinishedCount}",
            $"OUTPUT_DIR={outputDirectory}"
        };

        await WriteAllTextCompatAsync(path, string.Join(Environment.NewLine, lines) + Environment.NewLine, cancellationToken);
        Console.WriteLine($"Parity benchmark run log written: {path}");
    }

    private (string? FixedFromDate, string? FixedOrderFrom, string SourceKey) GetParityLockValues()
    {
        if (_options.ParityBenchmarkMode.Enabled)
        {
            return (
                _options.ParityBenchmarkMode.FixedFromDate?.Trim(),
                _options.ParityBenchmarkMode.FixedOrderFrom?.Trim(),
                "Prenos:ParityBenchmarkMode");
        }

        return (
            _options.ParityMode.FixedFromDate?.Trim(),
            _options.ParityMode.FixedOrderFrom?.Trim(),
            "Prenos:ParityMode");
    }

    private string GetOutputDirectoryPath()
        => ResolvePathFromCurrentDirectory(_options.OutputDirectory);

    private static string ResolvePathFromCurrentDirectory(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return Directory.GetCurrentDirectory();
        }

        if (Path.IsPathRooted(path))
        {
            return path;
        }

        return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), path));
    }

    private BenchmarkSnapshot BuildBenchmarkSnapshot(
        IReadOnlyList<PlateDemandRecord> plateDemands,
        IReadOnlyList<UnifiedItem> unified,
        IReadOnlyList<SemiFinishedTrace> semiFinished,
        ProcessingStats stats,
        TimingCollector timing,
        long runtimeMs,
        DateTime? effectiveFromDate,
        string effectiveOrderFrom)
    {
        var outputDigest = new OutputDigest
        {
            Plates = plateDemands
                .Select(p => $"{p.Track}|{p.OrderNumber}|{p.Material}|{p.Quantity}|{p.StartDate:yyyy-MM-dd}")
                .OrderBy(v => v, StringComparer.Ordinal)
                .ToList(),
            Unified = unified
                .Select(u => $"{u.OrderNumber}|{u.PlateMaterial}|{u.ComponentMaterial}|{u.ComponentDescription}|{u.Category}|{u.RequiredQty}")
                .OrderBy(v => v, StringComparer.Ordinal)
                .ToList(),
            SemiFinished = semiFinished
                .Select(s => $"{s.PlateOrder}|{s.PlateMaterial}|{s.Category}|{s.SemiMaterial}|{s.SemiOrder}|{s.AfruYieldDelta}")
                .OrderBy(v => v, StringComparer.Ordinal)
                .ToList()
        };

        return new BenchmarkSnapshot
        {
            GeneratedAtUtc = DateTime.UtcNow,
            RuntimeMs = runtimeMs,
            Plant = _options.Plant,
            FromDate = effectiveFromDate?.ToString("yyyy-MM-dd"),
            OrderFrom = effectiveOrderFrom,
            OrderConcurrency = Math.Max(1, _options.OrderConcurrency),
            MaxSapCallsInFlight = Math.Max(1, _options.MaxSapCallsInFlight),
            ConfirmationConcurrency = Math.Max(1, _options.ConfirmationConcurrency),
            SchedulerCode = _options.SchedulerCode,
            PlateMaterialFrom = _options.PlateMaterialFrom,
            PlateMaterialTo = _options.PlateMaterialTo,
            Stats = stats,
            JobTiming = timing.GetEntries(),
            SapClientTimingReport = (_sapClient as SapDllSapClient)?.BuildDetailedTimingReport() ?? string.Empty,
            OutputDigest = outputDigest
        };
    }

    private static IEnumerable<string> GetOutputDiffs(OutputDigest baseline, OutputDigest current)
    {
        foreach (var diff in GetListDiff("Plates", baseline.Plates, current.Plates))
        {
            yield return diff;
        }

        foreach (var diff in GetListDiff("Unified", baseline.Unified, current.Unified))
        {
            yield return diff;
        }

        foreach (var diff in GetListDiff("SemiFinished", baseline.SemiFinished, current.SemiFinished))
        {
            yield return diff;
        }
    }

    private static IEnumerable<string> GetListDiff(string name, IReadOnlyList<string> baseline, IReadOnlyList<string> current)
    {
        if (baseline.Count != current.Count)
        {
            yield return $"{name} count mismatch: baseline={baseline.Count}, current={current.Count}";
        }

        var max = Math.Min(baseline.Count, current.Count);
        for (var i = 0; i < max; i++)
        {
            if (string.Equals(baseline[i], current[i], StringComparison.Ordinal))
            {
                continue;
            }

            yield return $"{name} first mismatch at index {i}: baseline='{baseline[i]}', current='{current[i]}'";
            yield break;
        }
    }

    private static bool IsTechnicallyClosedStatus(string? status)
    {
        // Delphi parity: statdn := copy(tab.value(i,42),1,4) and then TEHZ/ZAKL check.
        if (string.IsNullOrWhiteSpace(status))
        {
            return false;
        }

        var trimmed = status.Trim();
        var prefix = trimmed.Length >= 4 ? trimmed.Substring(0, 4) : trimmed;
        return string.Equals(prefix, "TEHZ", StringComparison.Ordinal)
            || string.Equals(prefix, "ZAKL", StringComparison.Ordinal);
    }

    private static async Task<T> TimedSapCallAsync<T>(
        TimingCollector collector,
        string step,
        SemaphoreSlim sapCallSemaphore,
        CancellationToken cancellationToken,
        Func<Task<T>> action)
    {
        await sapCallSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await TimedAsync(collector, step, action).ConfigureAwait(false);
        }
        finally
        {
            sapCallSemaphore.Release();
        }
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

        public List<TimingEntry> GetEntries()
        {
            List<KeyValuePair<string, Item>> entries;
            lock (_sync)
            {
                entries = _map.ToList();
            }

            return entries
                .OrderByDescending(e => e.Value.TotalMs)
                .Select(e => new TimingEntry
                {
                    Step = e.Key,
                    Calls = e.Value.Count,
                    TotalMs = e.Value.TotalMs,
                    MaxMs = e.Value.MaxMs,
                    AvgMs = e.Value.Count == 0 ? 0 : (double)e.Value.TotalMs / e.Value.Count
                })
                .ToList();
        }

    }



    private sealed class BenchmarkSnapshot
    {
        public DateTime GeneratedAtUtc { get; set; }
        public long RuntimeMs { get; set; }
        public string Plant { get; set; } = string.Empty;
        public string? FromDate { get; set; }
        public string OrderFrom { get; set; } = string.Empty;
        public int OrderConcurrency { get; set; }
        public int MaxSapCallsInFlight { get; set; }
        public int ConfirmationConcurrency { get; set; }
        public string SchedulerCode { get; set; } = string.Empty;
        public string PlateMaterialFrom { get; set; } = string.Empty;
        public string PlateMaterialTo { get; set; } = string.Empty;
        public ProcessingStats Stats { get; set; } = new ProcessingStats();
        public List<TimingEntry> JobTiming { get; set; } = new List<TimingEntry>();
        public string SapClientTimingReport { get; set; } = string.Empty;
        public OutputDigest OutputDigest { get; set; } = new OutputDigest();
    }

    private sealed class OutputDigest
    {
        public List<string> Plates { get; set; } = new List<string>();
        public List<string> Unified { get; set; } = new List<string>();
        public List<string> SemiFinished { get; set; } = new List<string>();
    }

    private sealed class TimingEntry
    {
        public string Step { get; set; } = string.Empty;
        public long Calls { get; set; }
        public long TotalMs { get; set; }
        public long MaxMs { get; set; }
        public double AvgMs { get; set; }
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

    private static Task<string> ReadAllTextCompatAsync(string path, CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            return File.ReadAllText(path);
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
