using System.Reflection;
using MonPlatPrenos.Worker.Models;

namespace MonPlatPrenos.Worker.Services;

public sealed class SapDllSapClient : ISapClient
{
    private readonly string _sapDllFullPath;
    private readonly string _saUtilsDllFullPath;

    public SapDllSapClient(SapIntegrationOptions options, ILogger<SapDllSapClient> logger)
    {
        _sapDllFullPath = Path.GetFullPath(options.SapDllPath);
        _saUtilsDllFullPath = Path.GetFullPath(options.SaUtilsDllPath);

        if (!File.Exists(_sapDllFullPath))
        {
            throw new FileNotFoundException($"SAP library not found: {_sapDllFullPath}");
        }

        if (!File.Exists(_saUtilsDllFullPath))
        {
            throw new FileNotFoundException($"SA utils library not found: {_saUtilsDllFullPath}");
        }

        Assembly.LoadFrom(_sapDllFullPath);
        Assembly.LoadFrom(_saUtilsDllFullPath);

        logger.LogInformation("Loaded SAP libraries: {SapDll} and {SaUtilsDll}", _sapDllFullPath, _saUtilsDllFullPath);
    }

    public Task<IReadOnlyList<SapOrderHeader>> GetProductionOrdersForPlatesAsync(string plant, string schedulerCode, string materialFrom, string materialTo, string orderFrom, CancellationToken cancellationToken)
        => throw new NotImplementedException("sap.dll/sa_utils.dll loaded. Next step: implement calls and mapping for production orders.");

    public Task<IReadOnlyList<SapOperation>> GetOperationsAsync(string orderNumber, CancellationToken cancellationToken)
        => throw new NotImplementedException("sap.dll/sa_utils.dll loaded. Next step: implement calls and mapping for operations.");

    public Task<IReadOnlyList<SapConfirmation>> GetConfirmationsAsync(string orderNumber, string confirmation, CancellationToken cancellationToken)
        => throw new NotImplementedException("sap.dll/sa_utils.dll loaded. Next step: implement calls and mapping for confirmations.");

    public Task<IReadOnlyList<SapComponent>> GetComponentsAsync(string orderNumber, CancellationToken cancellationToken)
        => throw new NotImplementedException("sap.dll/sa_utils.dll loaded. Next step: implement calls and mapping for components.");
}
