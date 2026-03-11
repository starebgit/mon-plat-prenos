using System;
using System.Collections.Generic;

namespace MonPlatPrenos.Worker.Services;

public sealed class PrenosOptions
{
    public string DailyRunTime { get; set; } = "07:30";
    public string OutputDirectory { get; set; } = "output";
    public string PlateMaterialFrom { get; set; } = "000012080000000000";
    public string PlateMaterialTo { get; set; } = "000019789000000000";
    public string SchedulerCode { get; set; } = "200";
    public List<string> OperationCodes { get; set; } = new List<string> { "PP04", "PP14", "PP02", "PP10" };
    public List<TermRule> DefaultTerms { get; set; } = new List<TermRule>();
    public List<TermRule> ExtraTerms { get; set; } = new List<TermRule>();
    public bool EnableDebugJson { get; set; } = true;
    public bool EnableDebugTextDump { get; set; } = true;
    public int ConfirmationConcurrency { get; set; } = 4;
    public bool EnableTimingLog { get; set; } = true;
    public int TimingSampleLimit { get; set; } = 20;
    public SapIntegrationOptions Sap { get; set; } = new SapIntegrationOptions();
}

public sealed class SapIntegrationOptions
{
    public bool UseMock { get; set; } = false;
    public string SapDllPath { get; set; } = "sapnco.dll";
    public string SaUtilsDllPath { get; set; } = "sapnco_utils.dll";
    public string? DestinationName { get; set; }
    public string? AppServerHost { get; set; }
    public string? SystemNumber { get; set; }
    public string? Client { get; set; }
    public string? User { get; set; }
    public string? Password { get; set; }
    public string? Language { get; set; }
    public string? Router { get; set; }
    public string? SapLoginConnectionString { get; set; }
    public int? SapLoginIdent { get; set; }
}

public sealed class TermRule
{
    public string Name { get; set; } = string.Empty;
    public string Contains { get; set; } = string.Empty;
    public string? ExcludeContains { get; set; }
    public int? MaxLength { get; set; }

    public bool IsMatch(string input)
    {
        if (string.IsNullOrWhiteSpace(input) || string.IsNullOrWhiteSpace(Contains))
        {
            return false;
        }

        if (input.IndexOf(Contains, StringComparison.OrdinalIgnoreCase) < 0)
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(ExcludeContains) && input.IndexOf(ExcludeContains, StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return false;
        }

        if (MaxLength.HasValue && input.Trim().Length > MaxLength.Value)
        {
            return false;
        }

        return true;
    }
}
