using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Reflection;
using System.Data;
using System.Data.OleDb;
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
    private readonly ILogger<SapDllSapClient> _logger;
    private readonly Assembly _sapAssembly;
    private readonly Assembly _sapUtilsAssembly;
    private readonly SapIntegrationOptions _options;
    private string _loginSource = "config";
    private string _loginMessage = "Using direct Prenos:Sap values if provided.";

    public SapDllSapClient(SapIntegrationOptions options, ILogger<SapDllSapClient> logger)
    {
        _options = options;
        _sapDllFullPath = ResolveSapPath(options.SapDllPath, "sapnco.dll");
        _saUtilsDllFullPath = ResolveSapPath(options.SaUtilsDllPath, "sapnco_utils.dll");
        _logger = logger;

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

        logger.LogInformation("Loaded SAP libraries: {SapDll} and {SaUtilsDll}. Loaded assemblies: {SapAsm}, {UtilsAsm}", _sapDllFullPath, _saUtilsDllFullPath, _sapAssembly.FullName, _sapUtilsAssembly.FullName);

        TryLoadSapLoginFromDatabase();
        RegisterDestinationConfigurationIfConfigured();
    }


    private sealed class ReflectionDestinationConfigurationProxy : DispatchProxy
    {
        private Assembly _sapAssembly = null!;
        private SapIntegrationOptions _options = null!;
        private ILogger? _logger;

        public void Initialize(Assembly sapAssembly, SapIntegrationOptions options, ILogger logger)
        {
            _sapAssembly = sapAssembly;
            _options = options;
            _logger = logger;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod is null)
            {
                throw new InvalidOperationException("Destination configuration proxy received null targetMethod.");
            }

            if (string.Equals(targetMethod.Name, "get_ChangeEventsSupported", StringComparison.Ordinal))
            {
                return false;
            }

            if (string.Equals(targetMethod.Name, "GetParameters", StringComparison.Ordinal))
            {
                var destinationName = Convert.ToString(args?[0], CultureInfo.InvariantCulture);
                var expectedName = string.IsNullOrWhiteSpace(_options.DestinationName) ? "MONPLAT" : _options.DestinationName;
                if (!string.Equals(destinationName, expectedName, StringComparison.OrdinalIgnoreCase))
                {
                    return null;
                }

                var configParametersType = _sapAssembly.GetType("SAP.Middleware.Connector.RfcConfigParameters")
                                           ?? throw new InvalidOperationException("Type SAP.Middleware.Connector.RfcConfigParameters not found.");

                var instance = Activator.CreateInstance(configParametersType)
                               ?? throw new InvalidOperationException("Could not instantiate RfcConfigParameters.");

                SetParam(instance, "Name", expectedName);
                SetParam(instance, "AppServerHost", _options.AppServerHost);
                SetParam(instance, "SystemNumber", _options.SystemNumber);
                SetParam(instance, "Client", _options.Client);
                SetParam(instance, "User", _options.User);
                SetParam(instance, "Password", _options.Password);
                SetParam(instance, "Language", string.IsNullOrWhiteSpace(_options.Language) ? "EN" : _options.Language);

                if (!string.IsNullOrWhiteSpace(_options.Router))
                {
                    SetParam(instance, "SAPRouter", _options.Router);
                }

                _logger?.LogInformation("Providing SAP parameters for destination {DestinationName}.", expectedName);
                return instance;
            }

            if (string.Equals(targetMethod.Name, "add_ConfigurationChanged", StringComparison.Ordinal)
                || string.Equals(targetMethod.Name, "remove_ConfigurationChanged", StringComparison.Ordinal))
            {
                return null;
            }

            throw new NotSupportedException($"IDestinationConfiguration method '{targetMethod.Name}' is not supported by proxy.");
        }

        private static void SetParam(object configParams, string key, string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return;
            }

            var setValue = configParams.GetType().GetMethod("set_Item", new[] { typeof(string), typeof(string) })
                           ?? throw new InvalidOperationException("RfcConfigParameters indexer setter not found.");

            setValue.Invoke(configParams, new object[] { key, value });
        }
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
    {
        var function = CreateFunction("BAPI_PRODORD_GET_DETAIL");
        SetImport(function, "NUMBER", orderNumber);
        SetOrderObjectsFlag(function, "OPERATION");

        InvokeFunction(function);

        var operationTable = GetTable(function, "OPERATION");
        var results = new List<SapOperation>();

        foreach (var row in EnumerateRows(operationTable))
        {
            var confirmation = GetFirstString(row, "CONF_NO", "CONFIRMATION", "RUECK");
            var operationCode = GetFirstString(row, "OPR", "ACTIVITY", "LTXA1");
            var stepCode = GetFirstString(row, "OPER", "VORNR", "SUB_ACTIVITY");
            var confirmableQty = ParseInt(GetFirstString(row, "QUANTITY", "CONFIRMABLE_QTY", "BMSCH"));

            if (string.IsNullOrWhiteSpace(operationCode))
            {
                continue;
            }

            results.Add(new SapOperation(
                orderNumber.Trim(),
                confirmation.Trim(),
                operationCode.Trim(),
                confirmableQty,
                stepCode.Trim()));
        }

        _logger.LogInformation("BAPI_PRODORD_GET_DETAIL returned {Count} OPERATION rows for order {OrderNumber}.", results.Count, orderNumber);
        _logger.LogInformation("Mapped OPERATION fields used: CONF_NO/CONFIRMATION, OPR/ACTIVITY, OPER/VORNR, QUANTITY/CONFIRMABLE_QTY.");

        return Task.FromResult<IReadOnlyList<SapOperation>>(results);
    }

    public Task<IReadOnlyList<SapConfirmation>> GetConfirmationsAsync(string orderNumber, string confirmation, CancellationToken cancellationToken)
    {
        var listFunction = CreateFunction("BAPI_PRODORDCONF_GETLIST");
        FillRange(listFunction, "ORDER_RANGE", "EQ", orderNumber);
        FillRange(listFunction, "CONF_RANGE", "EQ", confirmation);
        InvokeFunction(listFunction);

        var confirmationsTable = GetTable(listFunction, "CONFIRMATIONS");
        var results = new List<SapConfirmation>();

        foreach (var row in EnumerateRows(confirmationsTable))
        {
            var confNo = GetFirstString(row, "CONF_NO", "CONFIRMATION", "RUECK");
            var confCounter = GetFirstString(row, "CONF_CNT", "CONFIRMATION_COUNTER", "RMZHL");
            if (string.IsNullOrWhiteSpace(confNo) || string.IsNullOrWhiteSpace(confCounter))
            {
                continue;
            }

            var detailFunction = CreateFunction("BAPI_PRODORDCONF_GETDETAIL");
            SetImport(detailFunction, "CONFIRMATION", confNo);
            SetImport(detailFunction, "CONFIRMATIONCOUNTER", confCounter);
            InvokeFunction(detailFunction);

            var confDetail = GetStructure(detailFunction, "CONF_DETAIL");
            var yield = ParseInt(GetFirstString(confDetail, "YIELD", "CONFIRMED_YIELD", "LMNGA"));

            results.Add(new SapConfirmation(confNo.Trim(), confCounter.Trim(), yield));
        }

        _logger.LogInformation("BAPI_PRODORDCONF_GETLIST/GETDETAIL returned {Count} confirmations for order {OrderNumber}, confirmation {Confirmation}.", results.Count, orderNumber, confirmation);
        _logger.LogInformation("Mapped CONFIRMATIONS/CONF_DETAIL fields used: CONF_NO/CONFIRMATION, CONF_CNT/CONFIRMATION_COUNTER, YIELD.");

        return Task.FromResult<IReadOnlyList<SapConfirmation>>(results);
    }

    public Task<IReadOnlyList<SapComponent>> GetComponentsAsync(string orderNumber, CancellationToken cancellationToken)
    {
        var function = CreateFunction("BAPI_PRODORD_GET_DETAIL");
        SetImport(function, "NUMBER", orderNumber);
        SetOrderObjectsFlag(function, "COMPONENT");

        InvokeFunction(function);

        var componentTable = GetTable(function, "COMPONENT");
        var results = new List<SapComponent>();

        foreach (var row in EnumerateRows(componentTable))
        {
            var material = GetFirstString(row, "MATERIAL", "MATERIAL_LONG", "MATERIAL_EXTERNAL", "MATNR");
            var description = GetFirstString(row, "MATERIAL_DESCRIPTION", "DESCRIPTION", "MAKTX", "DESCRIPTION1");

            if (string.IsNullOrWhiteSpace(material))
            {
                continue;
            }

            results.Add(new SapComponent(
                orderNumber.Trim(),
                material.Trim(),
                description.Trim()));
        }

        _logger.LogInformation("BAPI_PRODORD_GET_DETAIL returned {Count} COMPONENT rows for order {OrderNumber}.", results.Count, orderNumber);
        _logger.LogInformation("Mapped COMPONENT fields used: MATERIAL/MATERIAL_LONG/MATERIAL_EXTERNAL, MATERIAL_DESCRIPTION/DESCRIPTION.");

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

            results.Add(new SapOrderHeader(
                orderNumber.Trim(),
                GetFirstString(row, "MATERIAL", "MATERIAL_LONG", "MATERIAL_EXTERNAL", "MATNR").Trim(),
                GetFirstString(row, "SYSTEM_STATUS", "SYS_STATUS", "STAT").Trim(),
                ParseInt(GetFirstString(row, "TARGET_QUANTITY", "TOTAL_PLORD_QTY", "GAMNG")),
                ParseDate(GetFirstString(row, "START_DATE", "BASIC_START_DATE", "GSTRP")),
                GetFirstString(row, "WORK_CENTER", "WORK_CENT", "ARBPL").Trim(),
                GetFirstString(row, "PROD_SCHED", "PROD_SCHEDULER", "FEVOR").Trim(),
                GetFirstString(row, "PRODUCTION_PLANT", "PLANT", "WERKS").Trim()));
        }

        return Task.FromResult<IReadOnlyList<SapOrderHeader>>(results);
    }

    public Task<int> GetAfruYieldDeltaAsync(string orderNumber, DateTime fromDate, CancellationToken cancellationToken)
    {
        var function = CreateFunction("ZETA_RFC_READ_AFRU");
        SetImport(function, "dday", fromDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture));
        SetImport(function, "stnal", orderNumber);
        InvokeFunction(function);

        var table = GetTable(function, "IT_AFRU");
        var yi1 = 0;
        var yi2 = 0;

        foreach (var row in EnumerateRows(table))
        {
            var arbid = GetFirstString(row, "ARBID", "WORK_CENTER_ID");
            if (string.IsNullOrWhiteSpace(arbid))
            {
                arbid = GetStringByIndex(row, 9);
            }

            var arbidNum = DigitsOnly(arbid);
            if (arbidNum < 10004712 || arbidNum > 10004720)
            {
                continue;
            }

            var yieString = GetFirstString(row, "YIELD", "LMNGA");
            if (string.IsNullOrWhiteSpace(yieString))
            {
                yieString = GetStringByIndex(row, 38);
            }

            var yie = ParseInt(yieString);
            var reversed = GetFirstString(row, "REVERSED", "STOKZ");
            if (string.IsNullOrWhiteSpace(reversed))
            {
                reversed = GetStringByIndex(row, 95);
            }

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

        return Task.FromResult(yi1 - yi2);
    }

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

        var destinationName = string.IsNullOrWhiteSpace(_options.DestinationName) ? "MONPLAT" : _options.DestinationName;

        return getDestination.Invoke(null, new object[] { destinationName })
               ?? throw new InvalidOperationException($"GetDestination returned null for destination {destinationName}.");
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
            _logger.LogInformation("SapLoginConnectionString is empty; skipping DB login lookup.");
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
                            _logger.LogWarning("No SAP login row found in table prijava (ident={Ident}).", _options.SapLoginIdent);
                            return;
                        }

                        _options.User = SafeGetString(reader, 0);
                        _options.SystemNumber = SafeGetIntString(reader, 4);
                        _options.SystemNumber = string.IsNullOrWhiteSpace(_options.SystemNumber) ? _options.SystemNumber : _options.SystemNumber.PadLeft(2, '0');
                        _options.Client = SafeGetString(reader, 2);
                        _options.AppServerHost = SafeGetString(reader, 3);
                        _options.Password = SafeGetString(reader, 5);
                        _options.Language = SafeGetString(reader, 6);

                        var systemName = SafeGetString(reader, 1);
                        if (!string.IsNullOrWhiteSpace(systemName)
                            && string.Equals(_options.DestinationName, "MONPLAT", StringComparison.OrdinalIgnoreCase))
                        {
                            _options.DestinationName = systemName;
                        }
                    }
                }
            }

            if (HasInlineDestinationConfig())
            {
                _loginSource = "db";
                _loginMessage = "Loaded SAP login values from table prijava.";
                _logger.LogInformation("Loaded SAP login from DB for destination {DestinationName}.", _options.DestinationName);
            }
            else
            {
                _loginSource = "db";
                _loginMessage = "DB lookup executed, but required fields are still incomplete.";
                _logger.LogWarning("SAP login lookup from DB succeeded but required fields are still incomplete.");
            }
        }
        catch (Exception ex)
        {
            _loginSource = "db";
            _loginMessage = string.Format(CultureInfo.InvariantCulture, "DB lookup failed: {0}: {1}", ex.GetType().Name, ex.Message);
            _logger.LogWarning(ex, "Failed to load SAP login from DB.");
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

    private void RegisterDestinationConfigurationIfConfigured()
    {
        if (!HasInlineDestinationConfig())
        {
            _logger.LogInformation("SAP destination provider not registered (DestinationName={DestinationName} only).", _options.DestinationName);
            return;
        }

        var providerType = _sapAssembly.GetType("SAP.Middleware.Connector.IDestinationConfiguration")
                          ?? throw new InvalidOperationException("Type SAP.Middleware.Connector.IDestinationConfiguration not found in sapnco.dll.");

        var destinationManagerType = _sapAssembly.GetType("SAP.Middleware.Connector.RfcDestinationManager")
                                     ?? throw new InvalidOperationException("Type SAP.Middleware.Connector.RfcDestinationManager not found in sapnco.dll.");

        var registerMethod = destinationManagerType
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .FirstOrDefault(m => m.Name == "RegisterDestinationConfiguration"
                              && m.GetParameters().Length == 1
                              && m.GetParameters()[0].ParameterType.FullName == providerType.FullName)
            ?? throw new InvalidOperationException("RfcDestinationManager.RegisterDestinationConfiguration(...) not found.");

        var createMethod = typeof(DispatchProxy)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .FirstOrDefault(m => m.Name == "Create" && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 2)
            ?? throw new InvalidOperationException("DispatchProxy.Create<T, TProxy>() not found.");

        var closedCreate = createMethod.MakeGenericMethod(providerType, typeof(ReflectionDestinationConfigurationProxy));
        var proxy = closedCreate.Invoke(null, null)
            ?? throw new InvalidOperationException("Could not create SAP destination configuration proxy.");

        if (proxy is not ReflectionDestinationConfigurationProxy impl)
        {
            throw new InvalidOperationException("Created SAP destination proxy has unexpected runtime type.");
        }

        impl.Initialize(_sapAssembly, _options, _logger);

        try
        {
            registerMethod.Invoke(null, new object[] { proxy });
            _logger.LogInformation("Registered inline SAP destination configuration for {DestinationName} ({Host}/{SystemNumber}/{Client}/{Language}).",
                _options.DestinationName,
                _options.AppServerHost,
                _options.SystemNumber,
                _options.Client,
                _options.Language ?? "EN");
        }
        catch (TargetInvocationException ex)
        {
            _logger.LogWarning(ex.InnerException ?? ex, "RegisterDestinationConfiguration failed. Continuing, assuming an external SAP destination config is already available.");
        }
    }

    private bool HasInlineDestinationConfig()
    {
        return !string.IsNullOrWhiteSpace(_options.AppServerHost)
               && !string.IsNullOrWhiteSpace(_options.SystemNumber)
               && !string.IsNullOrWhiteSpace(_options.Client)
               && !string.IsNullOrWhiteSpace(_options.User)
               && !string.IsNullOrWhiteSpace(_options.Password);
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

    private static void SetImport(object function, string importName, string value)
    {
        var setValue = function.GetType().GetMethod("SetValue", new[] { typeof(string), typeof(object) })
                       ?? throw new InvalidOperationException("Could not find function.SetValue(string, object).");
        setValue.Invoke(function, new object[] { importName, value });
    }

    private static void SetOrderObjectsFlag(object function, string fieldName)
    {
        var getStructure = function.GetType().GetMethod("GetStructure", new[] { typeof(string) })
                          ?? throw new InvalidOperationException("Could not find function.GetStructure(string).");

        var structure = getStructure.Invoke(function, new object[] { "ORDER_OBJECTS" })
                       ?? throw new InvalidOperationException("ORDER_OBJECTS structure is null.");

        var setValue = structure.GetType().GetMethod("SetValue", new[] { typeof(string), typeof(object) })
                       ?? throw new InvalidOperationException("Could not find structure.SetValue(string, object).");

        setValue.Invoke(structure, new object[] { fieldName, "X" });
    }

    private static object GetStructure(object function, string structureName)
    {
        var getStructure = function.GetType().GetMethod("GetStructure", new[] { typeof(string) })
                           ?? throw new InvalidOperationException("Could not find function.GetStructure(string).");

        return getStructure.Invoke(function, new object[] { structureName })
               ?? throw new InvalidOperationException($"SAP structure '{structureName}' was null.");
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


    private static string GetStringByIndex(object row, int index)
    {
        try
        {
            var getStringByIndex = row.GetType().GetMethod("GetString", new[] { typeof(int) });
            if (getStringByIndex is not null)
            {
                return Convert.ToString(getStringByIndex.Invoke(row, new object[] { index }), CultureInfo.InvariantCulture) ?? string.Empty;
            }

            var getValueByIndex = row.GetType().GetMethod("GetValue", new[] { typeof(int) });
            if (getValueByIndex is not null)
            {
                return Convert.ToString(getValueByIndex.Invoke(row, new object[] { index }), CultureInfo.InvariantCulture) ?? string.Empty;
            }
        }
        catch
        {
            return string.Empty;
        }

        return string.Empty;
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
