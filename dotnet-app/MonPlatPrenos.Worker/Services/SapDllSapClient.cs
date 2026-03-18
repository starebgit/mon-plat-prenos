using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Reflection;
using System.Linq.Expressions;
using System.Diagnostics;
using System.Text;
using System.Data;
using System.Data.OleDb;
using System.Collections.Concurrent;
using MonPlatPrenos.Worker.Models;

namespace MonPlatPrenos.Worker.Services;


public sealed class SapDllSapClient : ISapClient
{
    public sealed class LoginPreview
    {
        public string DestinationName { get; set; } = string.Empty;
        public string AppServerHost { get; set; } = string.Empty;
        public string SystemNumber { get; set; } = string.Empty;
        public string Client { get; set; } = string.Empty;
        public string User { get; set; } = string.Empty;
        public string Language { get; set; } = string.Empty;
        public string PasswordMasked { get; set; } = string.Empty;
        public bool IsComplete { get; set; }
        public string LoginSource { get; set; } = string.Empty;
        public string LoginMessage { get; set; } = string.Empty;
    }
    private readonly string _sapDllFullPath;
    private readonly string _saUtilsDllFullPath;
    private readonly Assembly _sapAssembly;
    private readonly Assembly _sapUtilsAssembly;
    private readonly SapIntegrationOptions _options;
    private readonly SapFieldMapOptions _fieldMap;
    private readonly object _destinationLock = new object();
    private object? _cachedDestination;
    private object? _cachedRepository;
    private string _loginSource = "config";
    private string _loginMessage = "Using direct Prenos:Sap values if provided.";
    private readonly object _timingLock = new object();
    private readonly Dictionary<string, DetailedTimingItem> _detailedTimings = new Dictionary<string, DetailedTimingItem>(StringComparer.Ordinal);


    private sealed class DetailedTimingItem
    {
        public long Count;
        public long TotalMs;
        public long MaxMs;
        public readonly List<long> Samples = new List<long>();
    }

    private sealed class RowAccessorCacheItem
    {
        public Func<object, string, string>? GetStringByName;
        public Func<object, string, string>? GetValueByName;
        public readonly ConcurrentDictionary<string, NameAccessorKind> NameAccessorKinds = new(StringComparer.Ordinal);
    }

    private sealed class FastTableAccessor
    {
        public Func<object, int>? GetCount;
        public Func<object, int, object?>? GetRow;
    }

    private sealed class FastRowAccessor
    {
        public Func<object, string, string>? GetStringByName;
    }

    private enum NameAccessorKind
    {
        None = 0,
        GetString = 1,
        GetValue = 2
    }

    private static readonly ConcurrentDictionary<Type, MethodInfo?> InvokeNoArgCache = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo?> InvokeSingleArgCache = new();
    private static readonly ConcurrentDictionary<Type, PropertyInfo?> CountPropertyCache = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo?> RowIndexerCache = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo?> CreateFunctionCache = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo?> GetTableMethodCache = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo?> GetStructureMethodCache = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo?> SetValueNameObjectCache = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo?> SetValueIndexObjectCache = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo?> AppendMethodCache = new();
    private static readonly ConcurrentDictionary<Type, PropertyInfo?> CurrentRowPropertyCache = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo?> ConfigSetItemCache = new();
    private static readonly ConcurrentDictionary<Type, RowAccessorCacheItem> RowAccessorCache = new();
    private static readonly ConcurrentDictionary<Type, FastTableAccessor> FastTableAccessorCache = new();
    private static readonly ConcurrentDictionary<Type, FastRowAccessor> FastRowAccessorCache = new();
    private readonly ConcurrentDictionary<string, bool> _validatedFieldScopes = new(StringComparer.Ordinal);

    public SapDllSapClient(SapIntegrationOptions options)
    {
        _options = options;
        _fieldMap = options.FieldMap ?? new SapFieldMapOptions();
        _sapDllFullPath = ResolveSapPath(options.SapDllPath);
        _saUtilsDllFullPath = ResolveSapPath(options.SaUtilsDllPath);

        if (!File.Exists(_sapDllFullPath))
        {
            throw new FileNotFoundException($"SAP library not found: {_sapDllFullPath}");
        }

        if (!File.Exists(_saUtilsDllFullPath))
        {
            throw new FileNotFoundException($"SA utils library not found: {_saUtilsDllFullPath}");
        }

        if (!Environment.Is64BitProcess)
        {
            throw new InvalidOperationException("SAP NCo requires a 64-bit process. Configure the worker to run as x64.");
        }

        try
        {
            _sapAssembly = Assembly.LoadFrom(_sapDllFullPath);
            _sapUtilsAssembly = Assembly.LoadFrom(_saUtilsDllFullPath);
        }
        catch (BadImageFormatException ex)
        {
            throw new InvalidOperationException(
                $"Failed to load SAP assemblies due to architecture mismatch. Process is {(Environment.Is64BitProcess ? "x64" : "x86")}. " +
                $"Configured SAP assemblies: '{_sapDllFullPath}' and '{_saUtilsDllFullPath}'. Ensure both are x64 SAP NCo binaries.",
                ex);
        }
        catch (FileNotFoundException ex)
        {
            throw new InvalidOperationException(
                $"Failed to load SAP assemblies from '{_sapDllFullPath}' and '{_saUtilsDllFullPath}'. " +
                "One of the dependencies of SAP NCo is missing (most commonly Visual C++ runtime) or sapnco/sapnco_utils versions do not match.",
                ex);
        }
        catch (FileLoadException ex)
        {
            throw new InvalidOperationException(
                $"Failed to load SAP assemblies. A dependent module is missing or incompatible for '{ex.FileName}'. " +
                "Ensure SAP NCo dependencies are installed (for example required Visual C++ runtime) and that sapnco/sapnco_utils versions match.",
                ex);
        }


        TryLoadSapLoginFromDatabase();
    }



    private void AddDetailedTiming(string key, long elapsedMs)
    {
        lock (_timingLock)
        {
            if (!_detailedTimings.TryGetValue(key, out var item))
            {
                item = new DetailedTimingItem();
                _detailedTimings.Add(key, item);
            }

            item.Count++;
            item.TotalMs += elapsedMs;
            if (elapsedMs > item.MaxMs)
            {
                item.MaxMs = elapsedMs;
            }

            if (item.Samples.Count < 20)
            {
                item.Samples.Add(elapsedMs);
            }
        }
    }

    public string BuildDetailedTimingReport()
    {
        var sb = new StringBuilder();
        sb.AppendLine();
        sb.AppendLine("SAP client detailed timing");

        List<KeyValuePair<string, DetailedTimingItem>> snapshot;
        lock (_timingLock)
        {
            snapshot = _detailedTimings.ToList();
        }

        foreach (var kv in snapshot.OrderByDescending(e => e.Value.TotalMs))
        {
            var avg = kv.Value.Count == 0 ? 0d : (double)kv.Value.TotalMs / kv.Value.Count;
            sb.AppendLine($"{kv.Key}");
            sb.AppendLine($"  calls={kv.Value.Count}, totalMs={kv.Value.TotalMs}, avgMs={avg:F2}, maxMs={kv.Value.MaxMs}");
            sb.AppendLine($"  firstSamplesMs=[{string.Join(", ", kv.Value.Samples)}]");
        }

        return sb.ToString();
    }



    private static string ResolveSapPath(string configuredPath)
    {
        if (Path.IsPathRooted(configuredPath))
        {
            return configuredPath;
        }

        var baseDir = AppContext.BaseDirectory;
        return Path.GetFullPath(Path.Combine(baseDir, configuredPath));
    }

    public Task<IReadOnlyList<SapOrderHeader>> GetProductionOrdersForPlatesAsync(string plant, string schedulerCode, string materialFrom, string materialTo, string orderFrom, DateTime? fromDate, DateTime? toDate, CancellationToken cancellationToken)
    {
        var function = CreateFunction("BAPI_PRODORD_GET_LIST");

        FillRange(function, "PRODPLANT_RANGE", "EQ", plant);
        FillRange(function, "PROD_SCHED_RANGE", "EQ", schedulerCode);
        FillRange(function, "ORDER_NUMBER_RANGE", "GE", orderFrom);
        FillRange(function, "MATERIAL_RANGE", "BT", materialFrom, materialTo);

        var invokeSw = Stopwatch.StartNew();
        InvokeFunction(function);
        AddDetailedTiming("GetProductionOrdersForPlates.Invoke", invokeSw.ElapsedMilliseconds);

        var parseSw = Stopwatch.StartNew();
        var orderHeader = GetTable(function, "ORDER_HEADER");
        ValidateFieldsOnce("BAPI_PRODORD_GET_LIST.ORDER_HEADER", orderHeader,
            _fieldMap.OrderHeader.OrderNumber,
            _fieldMap.OrderHeader.Material,
            _fieldMap.OrderHeader.SystemStatus,
            _fieldMap.OrderHeader.PlannedQuantity,
            _fieldMap.OrderHeader.StartDate,
            _fieldMap.OrderHeader.Plant);
        IReadOnlyList<SapOrderHeader> results = _options.UseTypedHotPath
            ? ParsePlateOrderHeadersFast(orderHeader, plant, schedulerCode)
            : ParsePlateOrderHeadersReflection(orderHeader, plant, schedulerCode);

        if (fromDate.HasValue || toDate.HasValue)
        {
            var effectiveFromDate = (fromDate ?? toDate).GetValueOrDefault().Date;
            var effectiveToDate = (toDate ?? fromDate).GetValueOrDefault().Date;

            results = results
                .Where(order => order.StartDate.Date >= effectiveFromDate && order.StartDate.Date <= effectiveToDate)
                .ToList();
        }


        AddDetailedTiming("GetProductionOrdersForPlates.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult(results);
    }

    public Task<IReadOnlyList<SapOperation>> GetOperationsAsync(string orderNumber, CancellationToken cancellationToken)
    {
        var function = CreateFunction("BAPI_PRODORD_GET_DETAIL");
        SetImport(function, "NUMBER", orderNumber);
        SetOrderObjectFlagWithDelphiParity(function, delphiIndex: 4, fieldName: "OPERATION");

        var invokeSw = Stopwatch.StartNew();
        InvokeFunction(function);
        AddDetailedTiming("GetOperations.Invoke", invokeSw.ElapsedMilliseconds);

        var parseSw = Stopwatch.StartNew();
        var operationTable = GetTable(function, "OPERATION");
        ValidateFieldsOnce("BAPI_PRODORD_GET_DETAIL.OPERATION", operationTable,
            _fieldMap.Operation.Confirmation,
            _fieldMap.Operation.OperationCode,
            _fieldMap.Operation.StepCode,
            _fieldMap.Operation.ConfirmableQuantity,
            _fieldMap.Operation.WorkCenterCode);

        IReadOnlyList<SapOperation> results = _options.UseTypedHotPath
            ? ParseOperationsFast(operationTable, orderNumber)
            : ParseOperationsReflection(operationTable, orderNumber);

        AddDetailedTiming("GetOperations.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult(results);
    }

    public Task<IReadOnlyList<SapConfirmation>> GetConfirmationsAsync(string orderNumber, string confirmation, CancellationToken cancellationToken)
    {
        var listFunction = CreateFunction("BAPI_PRODORDCONF_GETLIST");
        FillRange(listFunction, "ORDER_RANGE", "EQ", orderNumber);
        FillRange(listFunction, "CONF_RANGE", "EQ", confirmation);
        var listInvokeSw = Stopwatch.StartNew();
        InvokeFunction(listFunction);
        AddDetailedTiming("GetConfirmations.ListInvoke", listInvokeSw.ElapsedMilliseconds);

        var parseSw = Stopwatch.StartNew();
        var confirmationsTable = GetTable(listFunction, "CONFIRMATIONS");
        ValidateFieldsOnce("BAPI_PRODORDCONF_GETLIST.CONFIRMATIONS", confirmationsTable,
            _fieldMap.Confirmation.Confirmation,
            _fieldMap.Confirmation.ConfirmationCounter,
            _fieldMap.Confirmation.Yield);

        IReadOnlyList<SapConfirmation> results = _options.UseTypedHotPath
            ? ParseConfirmationsFast(confirmationsTable)
            : ParseConfirmationsReflection(confirmationsTable);

        AddDetailedTiming("GetConfirmations.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult(results);
    }

    public Task<IReadOnlyList<SapComponent>> GetComponentsAsync(string orderNumber, CancellationToken cancellationToken)
    {
        var function = CreateFunction("BAPI_PRODORD_GET_DETAIL");
        SetImport(function, "NUMBER", orderNumber);
        SetOrderObjectFlagWithDelphiParity(function, delphiIndex: 5, fieldName: "COMPONENT");

        var invokeSw = Stopwatch.StartNew();
        InvokeFunction(function);
        AddDetailedTiming("GetComponents.Invoke", invokeSw.ElapsedMilliseconds);

        var parseSw = Stopwatch.StartNew();
        var componentTable = GetTable(function, "COMPONENT");
        ValidateFieldsOnce("BAPI_PRODORD_GET_DETAIL.COMPONENT", componentTable,
            _fieldMap.Component.Material,
            _fieldMap.Component.Description);

        IReadOnlyList<SapComponent> results = _options.UseTypedHotPath
            ? ParseComponentsFast(componentTable, orderNumber)
            : ParseComponentsReflection(componentTable, orderNumber);

        AddDetailedTiming("GetComponents.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult(results);
    }


    public Task<IReadOnlyList<SapOrderHeader>> GetProductionOrdersByMaterialAsync(string plant, string material, string? orderFrom, CancellationToken cancellationToken)
    {
        var function = CreateFunction("BAPI_PRODORD_GET_LIST");
        FillRange(function, "PRODPLANT_RANGE", "EQ", plant);
        FillRange(function, "MATERIAL_RANGE", "EQ", material);
        if (!string.IsNullOrWhiteSpace(orderFrom))
        {
            FillRange(function, "ORDER_NUMBER_RANGE", "GE", orderFrom);
        }

        var invokeSw = Stopwatch.StartNew();
        InvokeFunction(function);
        AddDetailedTiming("GetProductionOrdersByMaterial.Invoke", invokeSw.ElapsedMilliseconds);

        var parseSw = Stopwatch.StartNew();
        var orderHeader = GetTable(function, "ORDER_HEADER");
        ValidateFieldsOnce("BAPI_PRODORD_GET_LIST.ORDER_HEADER_BY_MATERIAL", orderHeader,
            _fieldMap.OrderHeader.OrderNumber,
            _fieldMap.OrderHeader.Material,
            _fieldMap.OrderHeader.SystemStatus,
            _fieldMap.OrderHeader.PlannedQuantity,
            _fieldMap.OrderHeader.StartDate,
            _fieldMap.OrderHeader.WorkCenter,
            _fieldMap.OrderHeader.Plant);
        IReadOnlyList<SapOrderHeader> results = _options.UseTypedHotPath
            ? ParseOrderHeadersByMaterialFast(orderHeader)
            : ParseOrderHeadersByMaterialReflection(orderHeader);

        AddDetailedTiming("GetProductionOrdersByMaterial.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult(results);
    }

    public Task<int> GetAfruYieldDeltaAsync(string orderNumber, DateTime fromDate, CancellationToken cancellationToken)
    {
        var function = CreateFunction("ZETA_RFC_READ_AFRU");
        SetImport(function, "dday", fromDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture));
        SetImport(function, "stnal", orderNumber);
        var invokeSw = Stopwatch.StartNew();
        InvokeFunction(function);
        AddDetailedTiming("GetAfruYieldDelta.Invoke", invokeSw.ElapsedMilliseconds);

        var parseSw = Stopwatch.StartNew();
        var table = GetTable(function, "IT_AFRU");
        ValidateFieldsOnce("ZETA_RFC_READ_AFRU.IT_AFRU", table,
            _fieldMap.Afru.WorkCenterId,
            _fieldMap.Afru.Yield,
            _fieldMap.Afru.Reversed);
        var (yi1, yi2) = _options.UseTypedHotPath
            ? ParseAfruYieldBucketsFast(table)
            : ParseAfruYieldBucketsReflection(table);

        AddDetailedTiming("GetAfruYieldDelta.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult(yi1 - yi2);
    }

    private IReadOnlyList<SapOrderHeader> ParsePlateOrderHeadersReflection(object orderHeader, string defaultPlant, string schedulerCode)
    {
        var results = new List<SapOrderHeader>();
        foreach (var row in EnumerateRows(orderHeader))
        {
            var orderNumber = GetString(row, _fieldMap.OrderHeader.OrderNumber);
            if (string.IsNullOrWhiteSpace(orderNumber))
            {
                continue;
            }

            var mappedPlant = GetString(row, _fieldMap.OrderHeader.Plant);
            if (string.IsNullOrWhiteSpace(mappedPlant))
            {
                mappedPlant = defaultPlant;
            }

            results.Add(new SapOrderHeader(
                orderNumber.Trim(),
                GetString(row, _fieldMap.OrderHeader.Material).Trim(),
                GetString(row, _fieldMap.OrderHeader.SystemStatus).Trim(),
                ParseInt(GetString(row, _fieldMap.OrderHeader.PlannedQuantity)),
                ParseDate(GetString(row, _fieldMap.OrderHeader.StartDate)),
                string.Empty,
                schedulerCode.Trim(),
                mappedPlant.Trim()));
        }

        return results;
    }

    private IReadOnlyList<SapOrderHeader> ParsePlateOrderHeadersFast(object orderHeader, string defaultPlant, string schedulerCode)
    {
        var (count, rowGetter) = GetFastTableAccessors(orderHeader);
        if (count == 0)
        {
            return Array.Empty<SapOrderHeader>();
        }

        var firstRow = rowGetter(orderHeader, 0);
        if (firstRow is null)
        {
            return ParsePlateOrderHeadersReflection(orderHeader, defaultPlant, schedulerCode);
        }

        var getField = GetFastRowStringAccessor(firstRow.GetType());
        var results = new List<SapOrderHeader>(count);

        for (var i = 0; i < count; i++)
        {
            var row = rowGetter(orderHeader, i);
            if (row is null)
            {
                continue;
            }

            var orderNumber = SafeGetFastField(getField, row, _fieldMap.OrderHeader.OrderNumber);
            if (string.IsNullOrWhiteSpace(orderNumber))
            {
                continue;
            }

            var mappedPlant = SafeGetFastField(getField, row, _fieldMap.OrderHeader.Plant);
            if (string.IsNullOrWhiteSpace(mappedPlant))
            {
                mappedPlant = defaultPlant;
            }

            results.Add(new SapOrderHeader(
                orderNumber.Trim(),
                SafeGetFastField(getField, row, _fieldMap.OrderHeader.Material).Trim(),
                SafeGetFastField(getField, row, _fieldMap.OrderHeader.SystemStatus).Trim(),
                ParseInt(SafeGetFastField(getField, row, _fieldMap.OrderHeader.PlannedQuantity)),
                ParseDate(SafeGetFastField(getField, row, _fieldMap.OrderHeader.StartDate)),
                string.Empty,
                schedulerCode.Trim(),
                mappedPlant.Trim()));
        }

        return results;
    }

    private IReadOnlyList<SapOrderHeader> ParseOrderHeadersByMaterialReflection(object orderHeader)
    {
        var results = new List<SapOrderHeader>();

        foreach (var row in EnumerateRows(orderHeader))
        {
            var orderNumber = GetString(row, _fieldMap.OrderHeader.OrderNumber);
            if (string.IsNullOrWhiteSpace(orderNumber))
            {
                continue;
            }

            results.Add(new SapOrderHeader(
                orderNumber.Trim(),
                GetString(row, _fieldMap.OrderHeader.Material).Trim(),
                GetString(row, _fieldMap.OrderHeader.SystemStatus).Trim(),
                ParseInt(GetString(row, _fieldMap.OrderHeader.PlannedQuantity)),
                ParseDate(GetString(row, _fieldMap.OrderHeader.StartDate)),
                GetString(row, _fieldMap.OrderHeader.WorkCenter).Trim(),
                GetString(row, _fieldMap.OrderHeader.SchedulerCode).Trim(),
                GetString(row, _fieldMap.OrderHeader.Plant).Trim()));
        }

        return results;
    }

    private IReadOnlyList<SapOrderHeader> ParseOrderHeadersByMaterialFast(object orderHeader)
    {
        var (count, rowGetter) = GetFastTableAccessors(orderHeader);
        if (count == 0)
        {
            return Array.Empty<SapOrderHeader>();
        }

        var firstRow = rowGetter(orderHeader, 0);
        if (firstRow is null)
        {
            return ParseOrderHeadersByMaterialReflection(orderHeader);
        }

        var getField = GetFastRowStringAccessor(firstRow.GetType());
        var results = new List<SapOrderHeader>(count);

        for (var i = 0; i < count; i++)
        {
            var row = rowGetter(orderHeader, i);
            if (row is null)
            {
                continue;
            }

            var orderNumber = SafeGetFastField(getField, row, _fieldMap.OrderHeader.OrderNumber);
            if (string.IsNullOrWhiteSpace(orderNumber))
            {
                continue;
            }

            results.Add(new SapOrderHeader(
                orderNumber.Trim(),
                SafeGetFastField(getField, row, _fieldMap.OrderHeader.Material).Trim(),
                SafeGetFastField(getField, row, _fieldMap.OrderHeader.SystemStatus).Trim(),
                ParseInt(SafeGetFastField(getField, row, _fieldMap.OrderHeader.PlannedQuantity)),
                ParseDate(SafeGetFastField(getField, row, _fieldMap.OrderHeader.StartDate)),
                SafeGetFastField(getField, row, _fieldMap.OrderHeader.WorkCenter).Trim(),
                SafeGetFastField(getField, row, _fieldMap.OrderHeader.SchedulerCode).Trim(),
                SafeGetFastField(getField, row, _fieldMap.OrderHeader.Plant).Trim()));
        }

        return results;
    }

    private (int Yi1, int Yi2) ParseAfruYieldBucketsReflection(object table)
    {
        var yi1 = 0;
        var yi2 = 0;

        foreach (var row in EnumerateRows(table))
        {
            var arbidNum = DigitsOnly(GetString(row, _fieldMap.Afru.WorkCenterId));
            if (arbidNum < 10004712 || arbidNum > 10004720)
            {
                continue;
            }

            var yie = ParseInt(GetString(row, _fieldMap.Afru.Yield));
            if (string.Equals(GetString(row, _fieldMap.Afru.Reversed), "X", StringComparison.OrdinalIgnoreCase))
            {
                yie = -yie;
            }

            if (arbidNum >= 10004712 && arbidNum <= 10004718)
            {
                yi1 += yie;
            }

            if (arbidNum is 10004719 or 10004720)
            {
                yi2 += yie;
            }
        }

        return (yi1, yi2);
    }

    private (int Yi1, int Yi2) ParseAfruYieldBucketsFast(object table)
    {
        var (count, rowGetter) = GetFastTableAccessors(table);
        if (count == 0)
        {
            return (0, 0);
        }

        var firstRow = rowGetter(table, 0);
        if (firstRow is null)
        {
            return ParseAfruYieldBucketsReflection(table);
        }

        var getField = GetFastRowStringAccessor(firstRow.GetType());
        var yi1 = 0;
        var yi2 = 0;

        for (var i = 0; i < count; i++)
        {
            var row = rowGetter(table, i);
            if (row is null)
            {
                continue;
            }

            var arbidNum = DigitsOnly(SafeGetFastField(getField, row, _fieldMap.Afru.WorkCenterId));
            if (arbidNum < 10004712 || arbidNum > 10004720)
            {
                continue;
            }

            var yie = ParseInt(SafeGetFastField(getField, row, _fieldMap.Afru.Yield));
            if (string.Equals(SafeGetFastField(getField, row, _fieldMap.Afru.Reversed), "X", StringComparison.OrdinalIgnoreCase))
            {
                yie = -yie;
            }

            if (arbidNum >= 10004712 && arbidNum <= 10004718)
            {
                yi1 += yie;
            }

            if (arbidNum is 10004719 or 10004720)
            {
                yi2 += yie;
            }
        }

        return (yi1, yi2);
    }


    private IReadOnlyList<SapConfirmation> ParseConfirmationsReflection(object confirmationsTable)
    {
        var results = new List<SapConfirmation>();

        foreach (var row in EnumerateRows(confirmationsTable))
        {
            var confNo = GetString(row, _fieldMap.Confirmation.Confirmation);
            var confCounter = GetString(row, _fieldMap.Confirmation.ConfirmationCounter);
            if (string.IsNullOrWhiteSpace(confNo) || string.IsNullOrWhiteSpace(confCounter))
            {
                continue;
            }

            var yield = ParseInt(GetString(row, _fieldMap.Confirmation.Yield));
            if (yield == 0)
            {
                yield = LoadConfirmationDetailYieldReflection(confNo, confCounter);
            }

            results.Add(new SapConfirmation(confNo.Trim(), confCounter.Trim(), yield));
        }

        return results;
    }

    private IReadOnlyList<SapConfirmation> ParseConfirmationsFast(object confirmationsTable)
    {
        var (count, rowGetter) = GetFastTableAccessors(confirmationsTable);
        if (count == 0)
        {
            return Array.Empty<SapConfirmation>();
        }

        var firstRow = rowGetter(confirmationsTable, 0);
        if (firstRow is null)
        {
            return ParseConfirmationsReflection(confirmationsTable);
        }

        var getField = GetFastRowStringAccessor(firstRow.GetType());
        var results = new List<SapConfirmation>(count);

        for (var i = 0; i < count; i++)
        {
            var row = rowGetter(confirmationsTable, i);
            if (row is null)
            {
                continue;
            }

            var confNo = SafeGetFastField(getField, row, _fieldMap.Confirmation.Confirmation);
            var confCounter = SafeGetFastField(getField, row, _fieldMap.Confirmation.ConfirmationCounter);
            if (string.IsNullOrWhiteSpace(confNo) || string.IsNullOrWhiteSpace(confCounter))
            {
                continue;
            }

            var yield = ParseInt(SafeGetFastField(getField, row, _fieldMap.Confirmation.Yield));
            if (yield == 0)
            {
                yield = LoadConfirmationDetailYieldFast(confNo, confCounter);
            }

            results.Add(new SapConfirmation(confNo.Trim(), confCounter.Trim(), yield));
        }

        return results;
    }

    private int LoadConfirmationDetailYieldReflection(string confNo, string confCounter)
    {
        var detailFunction = CreateFunction("BAPI_PRODORDCONF_GETDETAIL");
        SetImport(detailFunction, "CONFIRMATION", confNo);
        SetImport(detailFunction, "CONFIRMATIONCOUNTER", confCounter);
        var detailInvokeSw = Stopwatch.StartNew();
        InvokeFunction(detailFunction);
        AddDetailedTiming("GetConfirmations.DetailInvoke", detailInvokeSw.ElapsedMilliseconds);

        var confDetail = GetStructure(detailFunction, "CONF_DETAIL");
        ValidateStructureFieldsOnce("BAPI_PRODORDCONF_GETDETAIL.CONF_DETAIL", confDetail, _fieldMap.Confirmation.DetailYield);
        return ParseInt(GetString(confDetail, _fieldMap.Confirmation.DetailYield));
    }

    private int LoadConfirmationDetailYieldFast(string confNo, string confCounter)
    {
        var detailFunction = CreateFunction("BAPI_PRODORDCONF_GETDETAIL");
        SetImport(detailFunction, "CONFIRMATION", confNo);
        SetImport(detailFunction, "CONFIRMATIONCOUNTER", confCounter);
        var detailInvokeSw = Stopwatch.StartNew();
        InvokeFunction(detailFunction);
        AddDetailedTiming("GetConfirmations.DetailInvoke", detailInvokeSw.ElapsedMilliseconds);

        var confDetail = GetStructure(detailFunction, "CONF_DETAIL");
        ValidateStructureFieldsOnce("BAPI_PRODORDCONF_GETDETAIL.CONF_DETAIL", confDetail, _fieldMap.Confirmation.DetailYield);
        var getField = GetFastRowStringAccessor(confDetail.GetType());
        return ParseInt(SafeGetFastField(getField, confDetail, _fieldMap.Confirmation.DetailYield));
    }

    private IReadOnlyList<SapOperation> ParseOperationsReflection(object operationTable, string orderNumber)
    {
        var results = new List<SapOperation>();
        foreach (var row in EnumerateRows(operationTable))
        {
            var confirmation = GetString(row, _fieldMap.Operation.Confirmation);
            var operationCode = GetString(row, _fieldMap.Operation.OperationCode);
            var stepCode = GetString(row, _fieldMap.Operation.StepCode);
            var confirmableQty = ParseInt(GetString(row, _fieldMap.Operation.ConfirmableQuantity));
            var workCenterCode = GetString(row, _fieldMap.Operation.WorkCenterCode);

            results.Add(new SapOperation(orderNumber.Trim(), confirmation.Trim(), operationCode.Trim(), confirmableQty, stepCode.Trim(), workCenterCode.Trim()));
        }

        return results;
    }

    private IReadOnlyList<SapOperation> ParseOperationsFast(object operationTable, string orderNumber)
    {
        var (count, rowGetter) = GetFastTableAccessors(operationTable);
        if (count == 0)
        {
            return Array.Empty<SapOperation>();
        }

        var firstRow = rowGetter(operationTable, 0);
        if (firstRow is null)
        {
            return ParseOperationsReflection(operationTable, orderNumber);
        }

        var getField = GetFastRowStringAccessor(firstRow.GetType());
        var results = new List<SapOperation>(count);

        for (var i = 0; i < count; i++)
        {
            var row = rowGetter(operationTable, i);
            if (row is null)
            {
                continue;
            }

            var confirmation = SafeGetFastField(getField, row, _fieldMap.Operation.Confirmation);
            var operationCode = SafeGetFastField(getField, row, _fieldMap.Operation.OperationCode);
            var stepCode = SafeGetFastField(getField, row, _fieldMap.Operation.StepCode);
            var confirmableQty = ParseInt(SafeGetFastField(getField, row, _fieldMap.Operation.ConfirmableQuantity));
            var workCenterCode = SafeGetFastField(getField, row, _fieldMap.Operation.WorkCenterCode);

            results.Add(new SapOperation(orderNumber.Trim(), confirmation.Trim(), operationCode.Trim(), confirmableQty, stepCode.Trim(), workCenterCode.Trim()));
        }

        return results;
    }

    private IReadOnlyList<SapComponent> ParseComponentsReflection(object componentTable, string orderNumber)
    {
        var results = new List<SapComponent>();
        foreach (var row in EnumerateRows(componentTable))
        {
            var material = GetString(row, _fieldMap.Component.Material);
            var description = GetString(row, _fieldMap.Component.Description);

            if (string.IsNullOrWhiteSpace(material))
            {
                continue;
            }

            results.Add(new SapComponent(orderNumber.Trim(), material.Trim(), description.Trim()));
        }

        return results;
    }

    private IReadOnlyList<SapComponent> ParseComponentsFast(object componentTable, string orderNumber)
    {
        var (count, rowGetter) = GetFastTableAccessors(componentTable);
        if (count == 0)
        {
            return Array.Empty<SapComponent>();
        }

        var firstRow = rowGetter(componentTable, 0);
        if (firstRow is null)
        {
            return ParseComponentsReflection(componentTable, orderNumber);
        }

        var getField = GetFastRowStringAccessor(firstRow.GetType());
        var results = new List<SapComponent>(count);

        for (var i = 0; i < count; i++)
        {
            var row = rowGetter(componentTable, i);
            if (row is null)
            {
                continue;
            }

            var material = SafeGetFastField(getField, row, _fieldMap.Component.Material);
            var description = SafeGetFastField(getField, row, _fieldMap.Component.Description);

            if (string.IsNullOrWhiteSpace(material))
            {
                continue;
            }

            results.Add(new SapComponent(orderNumber.Trim(), material.Trim(), description.Trim()));
        }

        return results;
    }

    private static (int Count, Func<object, int, object?> RowGetter) GetFastTableAccessors(object table)
    {
        var access = FastTableAccessorCache.GetOrAdd(table.GetType(), CreateFastTableAccessor);
        var countGetter = access.GetCount ?? throw new InvalidOperationException("Could not read SAP table Count.");
        var rowGetter = access.GetRow ?? throw new InvalidOperationException("Could not access SAP table row indexer.");
        return (countGetter(table), rowGetter);
    }

    private static Func<object, string, string> GetFastRowStringAccessor(Type rowType)
    {
        var access = FastRowAccessorCache.GetOrAdd(rowType, CreateFastRowAccessor);
        if (access.GetStringByName is null)
        {
            throw new InvalidOperationException($"Could not find row.GetString(string) for {rowType.FullName}.");
        }

        return access.GetStringByName;
    }

    private static FastTableAccessor CreateFastTableAccessor(Type tableType)
    {
        var accessor = new FastTableAccessor();

        var countProperty = tableType.GetProperty("Count");
        if (countProperty is not null)
        {
            var tableArg = Expression.Parameter(typeof(object), "table");
            var cast = Expression.Convert(tableArg, tableType);
            var countAccess = Expression.Property(cast, countProperty);
            var boxedInt = Expression.Convert(countAccess, typeof(int));
            accessor.GetCount = Expression.Lambda<Func<object, int>>(boxedInt, tableArg).Compile();
        }

        var indexer = tableType.GetMethod("get_Item", new[] { typeof(int) });
        if (indexer is not null)
        {
            var tableArg = Expression.Parameter(typeof(object), "table");
            var indexArg = Expression.Parameter(typeof(int), "index");
            var cast = Expression.Convert(tableArg, tableType);
            var call = Expression.Call(cast, indexer, indexArg);
            var boxed = Expression.Convert(call, typeof(object));
            accessor.GetRow = Expression.Lambda<Func<object, int, object?>>(boxed, tableArg, indexArg).Compile();
        }

        return accessor;
    }

    private static FastRowAccessor CreateFastRowAccessor(Type rowType)
    {
        var accessor = new FastRowAccessor();
        var getString = rowType.GetMethod("GetString", new[] { typeof(string) });
        if (getString is not null)
        {
            var rowArg = Expression.Parameter(typeof(object), "row");
            var nameArg = Expression.Parameter(typeof(string), "name");
            var cast = Expression.Convert(rowArg, rowType);
            var call = Expression.Call(cast, getString, nameArg);
            var normalize = Expression.Call(typeof(SapDllSapClient), nameof(NormalizeString), Type.EmptyTypes, Expression.Convert(call, typeof(object)));
            accessor.GetStringByName = Expression.Lambda<Func<object, string, string>>(normalize, rowArg, nameArg).Compile();
        }

        return accessor;
    }

    private static string NormalizeString(object? value)
    {
        return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static string SafeGetFastField(Func<object, string, string> getter, object row, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(fieldName))
        {
            return string.Empty;
        }

        try
        {
            return getter(row, fieldName);
        }
        catch
        {
            return string.Empty;
        }
    }

    private object CreateFunction(string functionName)
    {
        var repository = GetRepository();

        var repositoryType = repository.GetType();
        var createFunction = CreateFunctionCache.GetOrAdd(repositoryType, t => t.GetMethod("CreateFunction", new[] { typeof(string) }))
                            ?? throw new InvalidOperationException("Could not find Repository.CreateFunction(string).");

        return createFunction.Invoke(repository, new object[] { functionName })
               ?? throw new InvalidOperationException($"Failed to create SAP function '{functionName}'.");
    }

    private object GetRepository()
    {
        if (_cachedRepository is not null)
        {
            return _cachedRepository;
        }

        lock (_destinationLock)
        {
            if (_cachedRepository is not null)
            {
                return _cachedRepository;
            }

            var destination = GetDestination();
            _cachedRepository = destination.GetType().GetProperty("Repository")?.GetValue(destination)
                                ?? throw new InvalidOperationException("SAP destination repository is not available.");
            return _cachedRepository;
        }
    }

    private object GetDestination()
    {
        if (_cachedDestination is not null)
        {
            return _cachedDestination;
        }

        lock (_destinationLock)
        {
            if (_cachedDestination is not null)
            {
                return _cachedDestination;
            }

        if (!HasInlineDestinationConfig())
        {
            throw new InvalidOperationException("SAP destination parameters are incomplete. Expected AppServerHost/SystemNumber/Client/User/Password from config or DB (prijava).");
        }

        var destinationManagerType = _sapAssembly.GetType("SAP.Middleware.Connector.RfcDestinationManager")
                                     ?? throw new InvalidOperationException("Type SAP.Middleware.Connector.RfcDestinationManager not found in sapnco.dll.");

        var configType = _sapAssembly.GetType("SAP.Middleware.Connector.RfcConfigParameters")
                         ?? throw new InvalidOperationException("Type SAP.Middleware.Connector.RfcConfigParameters not found in sapnco.dll.");

        var getByConfig = destinationManagerType
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .FirstOrDefault(m => m.Name == "GetDestination"
                                 && m.GetParameters().Length == 1
                                 && m.GetParameters()[0].ParameterType.FullName == configType.FullName)
            ?? throw new InvalidOperationException("RfcDestinationManager.GetDestination(RfcConfigParameters) not found.");

        var config = Activator.CreateInstance(configType)
                     ?? throw new InvalidOperationException("Could not instantiate RfcConfigParameters.");

        var language = string.IsNullOrWhiteSpace(_options.Language) ? "EN" : _options.Language;
        var destinationName = ResolveRuntimeDestinationName();

        // Delphi sets concrete connection fields directly; this mirrors that model.
        // NAME is only a local NCo destination identifier required by GetDestination(RfcConfigParameters).
        SetParam(config, "NAME", destinationName);
        SetParam(config, "ASHOST", _options.AppServerHost);
        SetParam(config, "SYSNR", _options.SystemNumber);
        SetParam(config, "CLIENT", _options.Client);
        SetParam(config, "USER", _options.User);
        SetParam(config, "PASSWD", _options.Password);
        SetParam(config, "LANG", language);
        if (!string.IsNullOrWhiteSpace(_options.Router))
        {
            SetParam(config, "SAPROUTER", _options.Router);
        }

            _cachedDestination = getByConfig.Invoke(null, new[] { config })
                                ?? throw new InvalidOperationException("GetDestination(RfcConfigParameters) returned null.");
            return _cachedDestination;
        }
    }

    private string ResolveRuntimeDestinationName()
    {
        if (!string.IsNullOrWhiteSpace(_options.DestinationName))
        {
            return _options.DestinationName!;
        }

        return string.Format(
            CultureInfo.InvariantCulture,
            "{0}_{1}_{2}_{3}",
            _options.User ?? string.Empty,
            _options.AppServerHost ?? string.Empty,
            _options.SystemNumber ?? string.Empty,
            _options.Client ?? string.Empty);
    }

    private void TryLoadSapLoginFromDatabase()
    {
        if (HasInlineDestinationConfig())
        {
            _loginSource = "config";
            _loginMessage = "Inline SAP values are already present; DB lookup skipped.";
            return;
        }

        if (string.IsNullOrWhiteSpace(_options.SapLoginConnectionString))
        {
            _loginSource = "none";
            _loginMessage = "SapLoginConnectionString is empty; DB lookup skipped.";
            return;
        }

        try
        {
            using (var connection = new OleDbConnection(_options.SapLoginConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    if (_options.SapLoginIdent.HasValue)
                    {
                        command.CommandText = "select top 1 uporab, sistem, client, streznik, sysnnum, pass, jezik from prijava where ident = ?";
                        command.Parameters.AddWithValue("@p1", _options.SapLoginIdent.Value);
                    }
                    else
                    {
                        command.CommandText = "select top 1 uporab, sistem, client, streznik, sysnnum, pass, jezik from prijava where glavni = 'X'";
                    }

                    using (var reader = command.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        if (reader is null || !reader.Read())
                        {
                            _loginSource = "db";
                            _loginMessage = string.Format(CultureInfo.InvariantCulture, "No row found in table prijava for ident={0}.", _options.SapLoginIdent.HasValue ? _options.SapLoginIdent.Value.ToString(CultureInfo.InvariantCulture) : "<default glavni='X'>");
                            return;
                        }

                        _options.User = SafeGetString(reader, 0);
                        _options.SystemNumber = SafeGetIntString(reader, 4);
                        _options.SystemNumber = string.IsNullOrWhiteSpace(_options.SystemNumber) ? _options.SystemNumber : _options.SystemNumber.PadLeft(2, '0');
                        _options.Client = SafeGetString(reader, 2);
                        _options.AppServerHost = SafeGetString(reader, 3);
                        _options.Password = SafeGetString(reader, 5);
                        _options.Language = SafeGetString(reader, 6);

                        _options.DestinationName = SafeGetString(reader, 1);
                    }
                }
            }

            if (HasInlineDestinationConfig())
            {
                _loginSource = "db";
                _loginMessage = "Loaded SAP login values from table prijava.";
            }
            else
            {
                _loginSource = "db";
                _loginMessage = "DB lookup executed, but required fields are still incomplete.";
            }
        }
        catch (Exception ex)
        {
            _loginSource = "db";
            _loginMessage = string.Format(CultureInfo.InvariantCulture, "DB lookup failed: {0}: {1}", ex.GetType().Name, ex.Message);
        }
    }

    private static string SafeGetString(IDataRecord record, int ordinal)
    {
        return record.IsDBNull(ordinal) ? string.Empty : Convert.ToString(record.GetValue(ordinal), CultureInfo.InvariantCulture)?.Trim() ?? string.Empty;
    }

    private static string SafeGetIntString(IDataRecord record, int ordinal)
    {
        if (record.IsDBNull(ordinal))
        {
            return string.Empty;
        }

        var value = record.GetValue(ordinal);
        if (value is short s)
        {
            return s.ToString(CultureInfo.InvariantCulture);
        }

        if (value is int i)
        {
            return i.ToString(CultureInfo.InvariantCulture);
        }

        return Convert.ToString(value, CultureInfo.InvariantCulture)?.Trim() ?? string.Empty;
    }

    public LoginPreview GetLoginPreview()
    {
        var password = _options.Password ?? string.Empty;
        var masked = string.IsNullOrEmpty(password) ? string.Empty : new string('*', Math.Min(password.Length, 8));

        return new LoginPreview
        {
            DestinationName = _options.DestinationName ?? string.Empty,
            AppServerHost = _options.AppServerHost ?? string.Empty,
            SystemNumber = _options.SystemNumber ?? string.Empty,
            Client = _options.Client ?? string.Empty,
            User = _options.User ?? string.Empty,
            Language = _options.Language ?? string.Empty,
            PasswordMasked = masked,
            IsComplete = HasInlineDestinationConfig(),
            LoginSource = _loginSource,
            LoginMessage = _loginMessage
        };
    }



    private bool HasInlineDestinationConfig()
    {
        return !string.IsNullOrWhiteSpace(_options.AppServerHost)
               && !string.IsNullOrWhiteSpace(_options.SystemNumber)
               && !string.IsNullOrWhiteSpace(_options.Client)
               && !string.IsNullOrWhiteSpace(_options.User)
               && !string.IsNullOrWhiteSpace(_options.Password);
    }

    private static void SetParam(object configParams, string key, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        var configType = configParams.GetType();
        var setValue = ConfigSetItemCache.GetOrAdd(configType, t => t.GetMethod("set_Item", new[] { typeof(string), typeof(string) }))
                       ?? throw new InvalidOperationException("RfcConfigParameters indexer setter not found.");

        setValue.Invoke(configParams, new object[] { key, value });
    }


    private static void FillRange(object function, string tableName, string option, string low, string? high = null)
    {
        var table = GetTable(function, tableName);

        var tableType = table.GetType();
        var append = AppendMethodCache.GetOrAdd(tableType, t => t.GetMethod("Append", Type.EmptyTypes))
                     ?? throw new InvalidOperationException($"Could not find Append() for table {tableName}.");
        append.Invoke(table, null);

        if (TrySetFieldOnTable(table, "SIGN", "I")
            && TrySetFieldOnTable(table, "OPTION", option)
            && TrySetFieldOnTable(table, "LOW", low))
        {
            if (!string.IsNullOrWhiteSpace(high))
            {
                TrySetFieldOnTable(table, "HIGH", high);
            }

            return;
        }

        var currentRow = CurrentRowPropertyCache.GetOrAdd(tableType, t => t.GetProperty("CurrentRow"))?.GetValue(table);
        if (currentRow is null)
        {
            var countObj = CountPropertyCache.GetOrAdd(tableType, t => t.GetProperty("Count"))?.GetValue(table)
                           ?? throw new InvalidOperationException($"Could not read Count for table {tableName}.");
            var count = Convert.ToInt32(countObj, CultureInfo.InvariantCulture);
            if (count <= 0)
            {
                throw new InvalidOperationException($"Table {tableName} has no rows after Append().");
            }

            var getRow = RowIndexerCache.GetOrAdd(tableType, t => t.GetMethod("get_Item", new[] { typeof(int) }))
                         ?? throw new InvalidOperationException($"Could not access row indexer for table {tableName}.");

            currentRow = getRow.Invoke(table, new object[] { count - 1 });
        }

        if (currentRow is null)
        {
            throw new InvalidOperationException($"Could not access current row for table {tableName}.");
        }

        SetField(currentRow, "SIGN", "I");
        SetField(currentRow, "OPTION", option);
        SetField(currentRow, "LOW", low);
        if (!string.IsNullOrWhiteSpace(high))
        {
            SetField(currentRow, "HIGH", high);
        }
    }

    private static bool TrySetFieldOnTable(object table, string fieldName, string value)
    {
        var setValue = SetValueNameObjectCache.GetOrAdd(table.GetType(), t => t.GetMethod("SetValue", new[] { typeof(string), typeof(object) }));
        if (setValue is null)
        {
            return false;
        }

        setValue.Invoke(table, new object[] { fieldName, value });
        return true;
    }

    private static void SetImport(object function, string importName, string value)
    {
        var setValue = SetValueNameObjectCache.GetOrAdd(function.GetType(), t => t.GetMethod("SetValue", new[] { typeof(string), typeof(object) }))
                       ?? throw new InvalidOperationException("Could not find function.SetValue(string, object).");
        setValue.Invoke(function, new object[] { importName, value });
    }

    private static void SetOrderObjectsByIndex(object function, int index)
    {
        var functionType = function.GetType();
        var getStructure = GetStructureMethodCache.GetOrAdd(functionType, t => t.GetMethod("GetStructure", new[] { typeof(string) }))
                          ?? throw new InvalidOperationException("Could not find function.GetStructure(string).");

        var structure = getStructure.Invoke(function, new object[] { "ORDER_OBJECTS" })
                       ?? throw new InvalidOperationException("ORDER_OBJECTS structure is null.");

        var structureType = structure.GetType();
        var setValueByIndex = SetValueIndexObjectCache.GetOrAdd(structureType, t => t.GetMethod("SetValue", new[] { typeof(int), typeof(object) }))
                              ?? throw new InvalidOperationException("Could not find structure.SetValue(int, object) for ORDER_OBJECTS strict index mode.");

        if (index < 0)
        {
            throw new InvalidOperationException($"ORDER_OBJECTS index must be non-negative, got {index}.");
        }

        try
        {
            setValueByIndex.Invoke(structure, new object[] { index, "X" });
        }
        catch (TargetInvocationException ex)
        {
            throw new InvalidOperationException($"Could not set ORDER_OBJECTS at strict index {index}.", ex);
        }
    }

    private static void SetOrderObjectFlagWithDelphiParity(object function, int delphiIndex, string fieldName)
    {
        // Delphi parity path: funct.exports('ORDER_OBJECTS').value(4|5) := 'X'
        // Prefer strict positional write first because some wrappers silently ignore name-based writes.
        try
        {
            SetOrderObjectsByIndex(function, delphiIndex);
            return;
        }
        catch
        {
            // fallback to existing robust name/index strategy
        }

        SetOrderObjectFlag(function, fieldName, delphiIndex, 4, 5, 3);
    }

    private static void SetOrderObjectFlag(object function, string fieldName, params int[] fallbackIndexes)
    {
        var structure = GetStructure(function, "ORDER_OBJECTS");
        var setValueByName = SetValueNameObjectCache.GetOrAdd(structure.GetType(), t => t.GetMethod("SetValue", new[] { typeof(string), typeof(object) }));
        if (setValueByName is not null)
        {
            try
            {
                setValueByName.Invoke(structure, new object[] { fieldName, "X" });
                return;
            }
            catch
            {
                // fall through to strict index mode for wrappers that reject name-based writes here
            }
        }

        var indexes = fallbackIndexes is { Length: > 0 } ? fallbackIndexes : new[] { 0 };
        Exception? lastException = null;
        foreach (var index in indexes)
        {
            try
            {
                SetOrderObjectsByIndex(function, index);
                return;
            }
            catch (Exception ex)
            {
                lastException = ex;
            }
        }

        throw new InvalidOperationException($"Failed to set ORDER_OBJECTS flag '{fieldName}' using fallback index mode.", lastException);
    }

    private static object GetStructure(object function, string structureName)
    {
        var getStructure = GetStructureMethodCache.GetOrAdd(function.GetType(), t => t.GetMethod("GetStructure", new[] { typeof(string) }))
                           ?? throw new InvalidOperationException("Could not find function.GetStructure(string).");

        return getStructure.Invoke(function, new object[] { structureName })
               ?? throw new InvalidOperationException($"SAP structure '{structureName}' was null.");
    }

    private static object GetTable(object function, string tableName)
    {
        var getTable = GetTableMethodCache.GetOrAdd(function.GetType(), t => t.GetMethod("GetTable", new[] { typeof(string) }))
                       ?? throw new InvalidOperationException("Could not find function.GetTable(string).");

        return getTable.Invoke(function, new object[] { tableName })
               ?? throw new InvalidOperationException($"SAP table '{tableName}' was null.");
    }

    private static IEnumerable<object> EnumerateRows(object table)
    {
        var tableType = table.GetType();
        var countProperty = CountPropertyCache.GetOrAdd(tableType, t => t.GetProperty("Count"));
        var countObj = countProperty?.GetValue(table)
                       ?? throw new InvalidOperationException("Could not read SAP table Count.");
        var count = Convert.ToInt32(countObj, CultureInfo.InvariantCulture);

        var getRow = RowIndexerCache.GetOrAdd(tableType, t => t.GetMethod("get_Item", new[] { typeof(int) }))
                     ?? throw new InvalidOperationException("Could not access SAP table row indexer.");

        for (var i = 0; i < count; i++)
        {
            var row = getRow.Invoke(table, new object[] { i });
            if (row is not null)
            {
                yield return row;
            }
        }
    }

    private void InvokeFunction(object function)
    {
        var functionType = function.GetType();

        var invokeWithoutParameters = InvokeNoArgCache.GetOrAdd(functionType, t => t.GetMethod("Invoke", Type.EmptyTypes));
        if (invokeWithoutParameters is not null)
        {
            invokeWithoutParameters.Invoke(function, null);
            return;
        }

        var invokeWithDestination = InvokeSingleArgCache.GetOrAdd(functionType,
            t => t.GetMethods(BindingFlags.Instance | BindingFlags.Public)
                  .FirstOrDefault(m => m.Name == "Invoke" && m.GetParameters().Length == 1));

        if (invokeWithDestination is null)
        {
            throw new InvalidOperationException("Could not find function.Invoke() overload.");
        }

        var destination = GetDestination();
        invokeWithDestination.Invoke(function, new[] { destination });
    }

    private static void SetField(object row, string fieldName, string value)
    {
        var setValue = SetValueNameObjectCache.GetOrAdd(row.GetType(), t => t.GetMethod("SetValue", new[] { typeof(string), typeof(object) }))
                       ?? throw new InvalidOperationException("Could not find row.SetValue(string, object).");
        setValue.Invoke(row, new object[] { fieldName, value });
    }

    private void ValidateFieldsOnce(string scope, object table, params string[] fieldNames)
    {
        if (!_validatedFieldScopes.TryAdd(scope, true))
        {
            return;
        }

        foreach (var row in EnumerateRows(table))
        {
            ValidateFieldsOnRow(scope, row, fieldNames);
            return;
        }
    }

    private void ValidateStructureFieldsOnce(string scope, object structure, params string[] fieldNames)
    {
        if (!_validatedFieldScopes.TryAdd(scope, true))
        {
            return;
        }

        ValidateFieldsOnRow(scope, structure, fieldNames);
    }

    private static void ValidateFieldsOnRow(string scope, object rowOrStructure, params string[] fieldNames)
    {
        foreach (var fieldName in fieldNames)
        {
            if (string.IsNullOrWhiteSpace(fieldName))
            {
                continue;
            }

            if (!CanReadField(rowOrStructure, fieldName))
            {
                throw new InvalidOperationException($"Strict SAP field validation failed for {scope}: field '{fieldName}' is not readable.");
            }
        }
    }

    private static bool CanReadField(object rowOrStructure, string fieldName)
    {
        var rowType = rowOrStructure.GetType();
        var cache = RowAccessorCache.GetOrAdd(rowType, CreateRowAccessorCacheItem);

        if (TryInvokeStringAccessor(rowOrStructure, cache.GetStringByName, fieldName, out _))
        {
            return true;
        }

        if (TryInvokeStringAccessor(rowOrStructure, cache.GetValueByName, fieldName, out _))
        {
            return true;
        }

        return false;
    }

    private static string GetString(object row, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(fieldName))
        {
            return string.Empty;
        }

        var rowType = row.GetType();
        var cache = RowAccessorCache.GetOrAdd(rowType, CreateRowAccessorCacheItem);
        var kind = cache.NameAccessorKinds.GetOrAdd(fieldName, _ => NameAccessorKind.None);

        if (kind == NameAccessorKind.GetString)
        {
            if (TryInvokeStringAccessor(row, cache.GetStringByName, fieldName, out var value))
            {
                return value;
            }

            cache.NameAccessorKinds[fieldName] = NameAccessorKind.None;
            return string.Empty;
        }

        if (kind == NameAccessorKind.GetValue)
        {
            if (TryInvokeStringAccessor(row, cache.GetValueByName, fieldName, out var value))
            {
                return value;
            }

            cache.NameAccessorKinds[fieldName] = NameAccessorKind.None;
            return string.Empty;
        }

        if (TryInvokeStringAccessor(row, cache.GetStringByName, fieldName, out var getStringValue))
        {
            cache.NameAccessorKinds[fieldName] = NameAccessorKind.GetString;
            return getStringValue;
        }

        if (TryInvokeStringAccessor(row, cache.GetValueByName, fieldName, out var getValueResult))
        {
            cache.NameAccessorKinds[fieldName] = NameAccessorKind.GetValue;
            return getValueResult;
        }

        cache.NameAccessorKinds[fieldName] = NameAccessorKind.None;

        return string.Empty;
    }

    private static RowAccessorCacheItem CreateRowAccessorCacheItem(Type rowType)
    {
        return new RowAccessorCacheItem
        {
            GetStringByName = CreateRowStringDelegate(rowType, "GetString"),
            GetValueByName = CreateRowStringDelegate(rowType, "GetValue")
        };
    }

    private static Func<object, string, string>? CreateRowStringDelegate(Type rowType, string methodName)
    {
        var accessor = rowType.GetMethod(methodName, new[] { typeof(string) });
        if (accessor is null)
        {
            return null;
        }

        var rowArg = Expression.Parameter(typeof(object), "row");
        var nameArg = Expression.Parameter(typeof(string), "fieldName");
        var cast = Expression.Convert(rowArg, rowType);
        var call = Expression.Call(cast, accessor, nameArg);
        var normalize = Expression.Call(
            typeof(SapDllSapClient),
            nameof(NormalizeString),
            Type.EmptyTypes,
            Expression.Convert(call, typeof(object)));

        return Expression.Lambda<Func<object, string, string>>(normalize, rowArg, nameArg).Compile();
    }

    private static bool TryInvokeStringAccessor(object target, Func<object, string, string>? accessor, string fieldName, out string value)
    {
        if (accessor is null)
        {
            value = string.Empty;
            return false;
        }

        try
        {
            value = accessor(target, fieldName);
            return true;
        }
        catch
        {
            value = string.Empty;
            return false;
        }
    }

    private static int DigitsOnly(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return 0;
        }

        var digits = new string(input.Where(char.IsDigit).ToArray());
        return int.TryParse(digits, out var parsed) ? parsed : 0;
    }

    private static int ParseInt(string input)
    {
        if (int.TryParse(input, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        if (decimal.TryParse(input.Replace('.', ',').Trim(), NumberStyles.Any, CultureInfo.GetCultureInfo("sl-SI"), out var asDecimal))
        {
            return (int)Math.Round(asDecimal, MidpointRounding.AwayFromZero);
        }

        return 0;
    }

    private static DateTime ParseDate(string input)
    {
        var formats = new[] { "yyyyMMdd", "dd.MM.yyyy", "yyyy-MM-dd" };
        if (DateTime.TryParseExact(input.Trim(), formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var exact))
        {
            return exact;
        }

        if (DateTime.TryParse(input, CultureInfo.GetCultureInfo("sl-SI"), DateTimeStyles.None, out var fallback))
        {
            return fallback;
        }

        return DateTime.MinValue;
    }
}
