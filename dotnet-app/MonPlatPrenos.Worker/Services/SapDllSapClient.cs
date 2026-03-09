using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Reflection;
using MonPlatPrenos.Worker.Models;

namespace MonPlatPrenos.Worker.Services;

public sealed class SapDllSapClient : ISapClient
{
    private readonly string _sapDllFullPath;
    private readonly string _saUtilsDllFullPath;
    private readonly ILogger<SapDllSapClient> _logger;
    private readonly Assembly _sapAssembly;

    public SapDllSapClient(SapIntegrationOptions options, ILogger<SapDllSapClient> logger)
    {
        _sapDllFullPath = Path.GetFullPath(options.SapDllPath);
        _saUtilsDllFullPath = Path.GetFullPath(options.SaUtilsDllPath);
        _logger = logger;

        if (!File.Exists(_sapDllFullPath))
        {
            throw new FileNotFoundException($"SAP library not found: {_sapDllFullPath}");
        }

        if (!File.Exists(_saUtilsDllFullPath))
        {
            throw new FileNotFoundException($"SA utils library not found: {_saUtilsDllFullPath}");
        }

        _sapAssembly = Assembly.LoadFrom(_sapDllFullPath);
        Assembly.LoadFrom(_saUtilsDllFullPath);

        logger.LogInformation("Loaded SAP libraries: {SapDll} and {SaUtilsDll}", _sapDllFullPath, _saUtilsDllFullPath);
    }

    public Task<IReadOnlyList<SapOrderHeader>> GetProductionOrdersForPlatesAsync(string plant, string schedulerCode, string materialFrom, string materialTo, string orderFrom, CancellationToken cancellationToken)
    {
        var function = CreateFunction("BAPI_PRODORD_GET_LIST");

        FillRange(function, "PRODPLANT_RANGE", "EQ", plant);
        FillRange(function, "PROD_SCHED_RANGE", "EQ", schedulerCode);
        FillRange(function, "ORDER_NUMBER_RANGE", "GE", orderFrom);
        FillRange(function, "MATERIAL_RANGE", "BT", materialFrom, materialTo);

        InvokeFunction(function);

        var orderHeader = GetTable(function, "ORDER_HEADER");
        var results = new List<SapOrderHeader>();

        foreach (var row in EnumerateRows(orderHeader))
        {
            var orderNumber = GetFirstString(row, "ORDER_NUMBER", "AUFNR");
            if (string.IsNullOrWhiteSpace(orderNumber))
            {
                continue;
            }

            var material = GetFirstString(row, "MATERIAL", "MATERIAL_EXTERNAL", "MATERIAL_LONG", "MATNR");
            var status = GetFirstString(row, "SYSTEM_STATUS", "SYS_STATUS", "STAT");
            var plannedQuantity = ParseInt(GetFirstString(row, "TARGET_QUANTITY", "TOTAL_PLORD_QTY", "GAMNG"));
            var startDate = ParseDate(GetFirstString(row, "START_DATE", "BASIC_START_DATE", "GSTRP"));
            var mappedSchedulerCode = GetFirstString(row, "PROD_SCHED", "PROD_SCHEDULER", "FEVOR", "PROD_S") ?? schedulerCode;
            var mappedPlant = GetFirstString(row, "PRODUCTION_PLANT", "PLANT", "WERKS", "PLAN") ?? plant;

            results.Add(new SapOrderHeader(
                orderNumber.Trim(),
                material.Trim(),
                status.Trim(),
                plannedQuantity,
                startDate,
                string.Empty,
                mappedSchedulerCode.Trim(),
                mappedPlant.Trim()));
        }

        _logger.LogInformation("BAPI_PRODORD_GET_LIST returned {Count} ORDER_HEADER rows.", results.Count);
        _logger.LogInformation("Mapped ORDER_HEADER fields used: ORDER_NUMBER, MATERIAL(_EXTERNAL/_LONG), SYSTEM_STATUS, TARGET_QUANTITY, START_DATE, PROD_SCHED/PROD_SCHEDULER, PRODUCTION_PLANT/PLANT.");

        return Task.FromResult<IReadOnlyList<SapOrderHeader>>(results);
    }

    public Task<IReadOnlyList<SapOperation>> GetOperationsAsync(string orderNumber, CancellationToken cancellationToken)
        => throw new NotImplementedException("sapnco.dll/sapnco_utils.dll loaded. Next step: implement calls and mapping for operations.");

    public Task<IReadOnlyList<SapConfirmation>> GetConfirmationsAsync(string orderNumber, string confirmation, CancellationToken cancellationToken)
        => throw new NotImplementedException("sapnco.dll/sapnco_utils.dll loaded. Next step: implement calls and mapping for confirmations.");

    public Task<IReadOnlyList<SapComponent>> GetComponentsAsync(string orderNumber, CancellationToken cancellationToken)
        => throw new NotImplementedException("sapnco.dll/sapnco_utils.dll loaded. Next step: implement calls and mapping for components.");

    private object CreateFunction(string functionName)
    {
        var destination = GetDestination();
        var repository = destination.GetType().GetProperty("Repository")?.GetValue(destination)
                         ?? throw new InvalidOperationException("SAP destination repository is not available.");

        var createFunction = repository.GetType().GetMethod("CreateFunction", new[] { typeof(string) })
                            ?? throw new InvalidOperationException("Could not find Repository.CreateFunction(string).");

        return createFunction.Invoke(repository, new object[] { functionName })
               ?? throw new InvalidOperationException($"Failed to create SAP function '{functionName}'.");
    }

    private object GetDestination()
    {
        var destinationManagerType = _sapAssembly.GetType("SAP.Middleware.Connector.RfcDestinationManager")
                                     ?? throw new InvalidOperationException("Type SAP.Middleware.Connector.RfcDestinationManager not found in sapnco.dll.");

        var getDestination = destinationManagerType
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .FirstOrDefault(m => m.Name == "GetDestination" && m.GetParameters().Length == 1 && m.GetParameters()[0].ParameterType == typeof(string))
            ?? throw new InvalidOperationException("RfcDestinationManager.GetDestination(string) not found.");

        return getDestination.Invoke(null, new object[] { "MONPLAT" })
               ?? throw new InvalidOperationException("GetDestination returned null for destination MONPLAT.");
    }

    private static void FillRange(object function, string tableName, string option, string low, string? high = null)
    {
        var table = GetTable(function, tableName);
        table.GetType().GetMethod("Append", Type.EmptyTypes)?.Invoke(table, null);

        var currentRow = table.GetType().GetMethod("CurrentRow", Type.EmptyTypes)?.Invoke(table, null)
                         ?? throw new InvalidOperationException($"Could not access current row for table {tableName}.");

        SetField(currentRow, "SIGN", "I");
        SetField(currentRow, "OPTION", option);
        SetField(currentRow, "LOW", low);
        if (!string.IsNullOrWhiteSpace(high))
        {
            SetField(currentRow, "HIGH", high);
        }
    }

    private static object GetTable(object function, string tableName)
    {
        var getTable = function.GetType().GetMethod("GetTable", new[] { typeof(string) })
                       ?? throw new InvalidOperationException("Could not find function.GetTable(string).");

        return getTable.Invoke(function, new object[] { tableName })
               ?? throw new InvalidOperationException($"SAP table '{tableName}' was null.");
    }

    private static IEnumerable<object> EnumerateRows(object table)
    {
        var countObj = table.GetType().GetProperty("Count")?.GetValue(table)
                       ?? throw new InvalidOperationException("Could not read SAP table Count.");
        var count = Convert.ToInt32(countObj, CultureInfo.InvariantCulture);

        var getRow = table.GetType().GetMethod("get_Item", new[] { typeof(int) })
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

    private static void InvokeFunction(object function)
    {
        var invoke = function.GetType().GetMethod("Invoke")
                     ?? throw new InvalidOperationException("Could not find function.Invoke().");
        invoke.Invoke(function, null);
    }

    private static void SetField(object row, string fieldName, string value)
    {
        var setValue = row.GetType().GetMethod("SetValue", new[] { typeof(string), typeof(object) })
                       ?? throw new InvalidOperationException("Could not find row.SetValue(string, object).");
        setValue.Invoke(row, new object[] { fieldName, value });
    }

    private static string GetFirstString(object row, params string[] fieldNames)
    {
        foreach (var fieldName in fieldNames)
        {
            var value = GetString(row, fieldName);
            if (!string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        return string.Empty;
    }

    private static string GetString(object row, string fieldName)
    {
        try
        {
            var getString = row.GetType().GetMethod("GetString", new[] { typeof(string) });
            if (getString is not null)
            {
                return Convert.ToString(getString.Invoke(row, new object[] { fieldName }), CultureInfo.InvariantCulture) ?? string.Empty;
            }

            var getValue = row.GetType().GetMethod("GetValue", new[] { typeof(string) });
            if (getValue is not null)
            {
                return Convert.ToString(getValue.Invoke(row, new object[] { fieldName }), CultureInfo.InvariantCulture) ?? string.Empty;
            }
        }
        catch
        {
            return string.Empty;
        }

        return string.Empty;
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
