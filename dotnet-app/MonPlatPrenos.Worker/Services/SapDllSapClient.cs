using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Reflection;
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
        public MethodInfo? GetStringByName;
        public MethodInfo? GetValueByName;
        public readonly ConcurrentDictionary<string, NameAccessorKind> NameAccessorKinds = new(StringComparer.Ordinal);
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
    private readonly ConcurrentDictionary<string, bool> _validatedFieldScopes = new(StringComparer.Ordinal);

    public SapDllSapClient(SapIntegrationOptions options)
    {
        _options = options;
        _fieldMap = options.FieldMap ?? new SapFieldMapOptions();
        _sapDllFullPath = ResolveSapPath(options.SapDllPath, "sapnco.dll");
        _saUtilsDllFullPath = ResolveSapPath(options.SaUtilsDllPath, "sapnco_utils.dll");

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



    private static string ResolveSapPath(string configuredPath, string defaultFileName)
    {
        if (Path.IsPathRooted(configuredPath))
        {
            return configuredPath;
        }

        var baseDir = AppContext.BaseDirectory;
        var primary = Path.GetFullPath(Path.Combine(baseDir, configuredPath));
        if (File.Exists(primary))
        {
            return primary;
        }

        var fallbackInRoot = Path.GetFullPath(Path.Combine(baseDir, defaultFileName));
        if (File.Exists(fallbackInRoot))
        {
            return fallbackInRoot;
        }

        var fallbackInLib = Path.GetFullPath(Path.Combine(baseDir, "lib", defaultFileName));
        if (File.Exists(fallbackInLib))
        {
            return fallbackInLib;
        }

        return primary;
    }

    public Task<IReadOnlyList<SapOrderHeader>> GetProductionOrdersForPlatesAsync(string plant, string schedulerCode, string materialFrom, string materialTo, string orderFrom, CancellationToken cancellationToken)
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
        var results = new List<SapOrderHeader>();

        foreach (var row in EnumerateRows(orderHeader))
        {
            var orderNumber = GetString(row, _fieldMap.OrderHeader.OrderNumber);
            if (string.IsNullOrWhiteSpace(orderNumber))
            {
                continue;
            }

            var material = GetString(row, _fieldMap.OrderHeader.Material);
            var status = GetString(row, _fieldMap.OrderHeader.SystemStatus);
            var plannedQuantity = ParseInt(GetString(row, _fieldMap.OrderHeader.PlannedQuantity));
            var startDate = ParseDate(GetString(row, _fieldMap.OrderHeader.StartDate));
            var mappedSchedulerCode = schedulerCode;

            var mappedPlant = GetString(row, _fieldMap.OrderHeader.Plant);
            if (string.IsNullOrWhiteSpace(mappedPlant))
            {
                mappedPlant = plant;
            }

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


        AddDetailedTiming("GetProductionOrdersForPlates.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult<IReadOnlyList<SapOrderHeader>>(results);
    }

    public Task<IReadOnlyList<SapOperation>> GetOperationsAsync(string orderNumber, CancellationToken cancellationToken)
    {
        var function = CreateFunction("BAPI_PRODORD_GET_DETAIL");
        SetImport(function, "NUMBER", orderNumber);
        SetOrderObjectsByIndex(function, 4);

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
        var results = new List<SapOperation>();

        foreach (var row in EnumerateRows(operationTable))
        {
            var confirmation = GetString(row, _fieldMap.Operation.Confirmation);
            var operationCode = GetString(row, _fieldMap.Operation.OperationCode);
            var stepCode = GetString(row, _fieldMap.Operation.StepCode);
            var confirmableQty = ParseInt(GetString(row, _fieldMap.Operation.ConfirmableQuantity));
            var workCenterCode = GetString(row, _fieldMap.Operation.WorkCenterCode);

            if (string.IsNullOrWhiteSpace(operationCode))
            {
                continue;
            }

            results.Add(new SapOperation(
                orderNumber.Trim(),
                confirmation.Trim(),
                operationCode.Trim(),
                confirmableQty,
                stepCode.Trim(),
                workCenterCode.Trim()));
        }


        AddDetailedTiming("GetOperations.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult<IReadOnlyList<SapOperation>>(results);
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
                var detailFunction = CreateFunction("BAPI_PRODORDCONF_GETDETAIL");
                SetImport(detailFunction, "CONFIRMATION", confNo);
                SetImport(detailFunction, "CONFIRMATIONCOUNTER", confCounter);
                var detailInvokeSw = Stopwatch.StartNew();
                InvokeFunction(detailFunction);
                AddDetailedTiming("GetConfirmations.DetailInvoke", detailInvokeSw.ElapsedMilliseconds);

                var confDetail = GetStructure(detailFunction, "CONF_DETAIL");
                ValidateStructureFieldsOnce("BAPI_PRODORDCONF_GETDETAIL.CONF_DETAIL", confDetail, _fieldMap.Confirmation.DetailYield);
                yield = ParseInt(GetString(confDetail, _fieldMap.Confirmation.DetailYield));
            }

            results.Add(new SapConfirmation(confNo.Trim(), confCounter.Trim(), yield));
        }


        AddDetailedTiming("GetConfirmations.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult<IReadOnlyList<SapConfirmation>>(results);
    }

    public Task<IReadOnlyList<SapComponent>> GetComponentsAsync(string orderNumber, CancellationToken cancellationToken)
    {
        var function = CreateFunction("BAPI_PRODORD_GET_DETAIL");
        SetImport(function, "NUMBER", orderNumber);
        SetOrderObjectsByIndex(function, 3);

        var invokeSw = Stopwatch.StartNew();
        InvokeFunction(function);
        AddDetailedTiming("GetComponents.Invoke", invokeSw.ElapsedMilliseconds);

        var parseSw = Stopwatch.StartNew();
        var componentTable = GetTable(function, "COMPONENT");
        ValidateFieldsOnce("BAPI_PRODORD_GET_DETAIL.COMPONENT", componentTable,
            _fieldMap.Component.Material,
            _fieldMap.Component.Description);
        var results = new List<SapComponent>();

        foreach (var row in EnumerateRows(componentTable))
        {
            var material = GetString(row, _fieldMap.Component.Material);
            var description = GetString(row, _fieldMap.Component.Description);

            if (string.IsNullOrWhiteSpace(material))
            {
                continue;
            }

            results.Add(new SapComponent(
                orderNumber.Trim(),
                material.Trim(),
                description.Trim()));
        }


        AddDetailedTiming("GetComponents.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult<IReadOnlyList<SapComponent>>(results);
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

        AddDetailedTiming("GetProductionOrdersByMaterial.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult<IReadOnlyList<SapOrderHeader>>(results);
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
        var yi1 = 0;
        var yi2 = 0;

        foreach (var row in EnumerateRows(table))
        {
            var arbid = GetString(row, _fieldMap.Afru.WorkCenterId);

            var arbidNum = DigitsOnly(arbid);
            if (arbidNum < 10004712 || arbidNum > 10004720)
            {
                continue;
            }

            var yieString = GetString(row, _fieldMap.Afru.Yield);

            var yie = ParseInt(yieString);
            var reversed = GetString(row, _fieldMap.Afru.Reversed);

            if (string.Equals(reversed, "X", StringComparison.OrdinalIgnoreCase))
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

        AddDetailedTiming("GetAfruYieldDelta.Parse", parseSw.ElapsedMilliseconds);
        return Task.FromResult(yi1 - yi2);
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
            Console.WriteLine("SAP-LOGIN: SapLoginConnectionString is empty; DB lookup skipped.");
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
                            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "SAP-LOGIN: No row found in table prijava (ident={0}).", _options.SapLoginIdent.HasValue ? _options.SapLoginIdent.Value.ToString(CultureInfo.InvariantCulture) : "<default glavni=\'X\'>"));
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
                Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "SAP-LOGIN: Loaded login from DB (sistem={0}).", _options.DestinationName));
            }
            else
            {
                _loginSource = "db";
                _loginMessage = "DB lookup executed, but required fields are still incomplete.";
                Console.WriteLine("SAP-LOGIN: DB lookup ran, but required fields are still incomplete.");
            }
        }
        catch (Exception ex)
        {
            _loginSource = "db";
            _loginMessage = string.Format(CultureInfo.InvariantCulture, "DB lookup failed: {0}: {1}", ex.GetType().Name, ex.Message);
            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "SAP-LOGIN: DB lookup failed: {0}: {1}", ex.GetType().Name, ex.Message));
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
            GetStringByName = rowType.GetMethod("GetString", new[] { typeof(string) }),
            GetValueByName = rowType.GetMethod("GetValue", new[] { typeof(string) })
        };
    }

    private static bool TryInvokeStringAccessor(object target, MethodInfo? accessor, string fieldName, out string value)
    {
        if (accessor is null)
        {
            value = string.Empty;
            return false;
        }

        try
        {
            value = Convert.ToString(accessor.Invoke(target, new object[] { fieldName }), CultureInfo.InvariantCulture) ?? string.Empty;
            return true;
        }
        catch (TargetInvocationException)
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
