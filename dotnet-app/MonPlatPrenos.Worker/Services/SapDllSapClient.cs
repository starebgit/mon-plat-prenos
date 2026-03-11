using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
    private readonly Assembly _sapAssembly;
    private readonly Assembly _sapUtilsAssembly;
    private readonly SapIntegrationOptions _options;
    private string _loginSource = "config";
    private string _loginMessage = "Using direct Prenos:Sap values if provided.";

    public SapDllSapClient(SapIntegrationOptions options)
    {
        _options = options;
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

        return getByConfig.Invoke(null, new[] { config })
               ?? throw new InvalidOperationException("GetDestination(RfcConfigParameters) returned null.");
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

        var setValue = configParams.GetType().GetMethod("set_Item", new[] { typeof(string), typeof(string) })
                       ?? throw new InvalidOperationException("RfcConfigParameters indexer setter not found.");

        setValue.Invoke(configParams, new object[] { key, value });
    }


    private static void FillRange(object function, string tableName, string option, string low, string? high = null)
    {
        var table = GetTable(function, tableName);

        var append = table.GetType().GetMethod("Append", Type.EmptyTypes)
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

        var currentRow = table.GetType().GetProperty("CurrentRow")?.GetValue(table);
        if (currentRow is null)
        {
            var countObj = table.GetType().GetProperty("Count")?.GetValue(table)
                           ?? throw new InvalidOperationException($"Could not read Count for table {tableName}.");
            var count = Convert.ToInt32(countObj, CultureInfo.InvariantCulture);
            if (count <= 0)
            {
                throw new InvalidOperationException($"Table {tableName} has no rows after Append().");
            }

            var getRow = table.GetType().GetMethod("get_Item", new[] { typeof(int) })
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
        var setValue = table.GetType().GetMethod("SetValue", new[] { typeof(string), typeof(object) });
        if (setValue is null)
        {
            return false;
        }

        setValue.Invoke(table, new object[] { fieldName, value });
        return true;
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
