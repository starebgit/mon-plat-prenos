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
    public int ConfirmationConcurrency { get; set; } = 4;
    public int OrderConcurrency { get; set; } = 1;
    public int MaxSapCallsInFlight { get; set; } = 8;
    public string Plant { get; set; } = "1061";
    public string OrderFrom { get; set; } = "000005223286";
    public bool ApplyFromDateFilter { get; set; } = true;
    public BenchmarkOptions Benchmark { get; set; } = new BenchmarkOptions();
    public ParityBenchmarkModeOptions ParityBenchmarkMode { get; set; } = new ParityBenchmarkModeOptions();
    public bool UseTypedHotPath { get; set; } = true;
    public bool EnableSemiFinishedExpansion { get; set; } = true;
    public bool EnableSapCallTrace { get; set; }
    public int SapCallWarnMs { get; set; } = 2000;
    public int SapWaitWarnMs { get; set; } = 1000;
    public int OrderTraceWarnMs { get; set; } = 5000;
    public bool EnableDiagnosticsFileLog { get; set; } = true;
    public string DiagnosticsLogFilePattern { get; set; } = "diagnostics-{timestamp}.log";
    public WatermarkOptions Watermark { get; set; } = new WatermarkOptions();
    public PrenosParityModeOptions ParityMode { get; set; } = new PrenosParityModeOptions();
    public string FetchedCodeLogFilePattern { get; set; } = "fetched-codes-{timestamp}.txt";
    public SapIntegrationOptions Sap { get; set; } = new SapIntegrationOptions();
}

public sealed class PrenosParityModeOptions
{
    public bool Enabled { get; set; }
    public string? FixedFromDate { get; set; }
    public string? FixedOrderFrom { get; set; }
}

public sealed class ParityBenchmarkModeOptions
{
    public bool Enabled { get; set; }
    public string? FixedFromDate { get; set; }
    public string? FixedOrderFrom { get; set; }
}

public sealed class BenchmarkOptions
{
    public bool Enabled { get; set; }
    public string? SnapshotPath { get; set; }
    public string? CompareSnapshotPath { get; set; }
}

public sealed class WatermarkOptions
{
    public bool Enabled { get; set; }
    public string FilePath { get; set; } = "output/orderfrom.watermark.txt";
}

public sealed class SapIntegrationOptions
{
    public bool UseTypedHotPath { get; set; } = true;
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
    public SapFieldMapOptions FieldMap { get; set; } = new SapFieldMapOptions();
}

public sealed class SapFieldMapOptions
{
    public OrderHeaderFieldMap OrderHeader { get; set; } = new OrderHeaderFieldMap();
    public OperationFieldMap Operation { get; set; } = new OperationFieldMap();
    public ComponentFieldMap Component { get; set; } = new ComponentFieldMap();
    public ConfirmationFieldMap Confirmation { get; set; } = new ConfirmationFieldMap();
    public AfruFieldMap Afru { get; set; } = new AfruFieldMap();
}

public sealed class OrderHeaderFieldMap
{
    public string OrderNumber { get; set; } = "ORDER_NUMBER";
    public string Material { get; set; } = "MATERIAL";
    public string SystemStatus { get; set; } = "SYSTEM_STATUS";
    public string PlannedQuantity { get; set; } = "TARGET_QUANTITY";
    public string StartDate { get; set; } = "START_DATE";
    public string SchedulerCode { get; set; } = "PROD_SCHED";
    public string Plant { get; set; } = "PRODUCTION_PLANT";
    public string WorkCenter { get; set; } = "WORK_CENTER";
}

public sealed class OperationFieldMap
{
    public string Confirmation { get; set; } = "CONF_NO";
    public string OperationCode { get; set; } = "OPR_CNTRL_KEY";
    public string StepCode { get; set; } = "OPERATION_NUMBER";
    public string ConfirmableQuantity { get; set; } = "QUANTITY";
    public string WorkCenterCode { get; set; } = "WORK_CENTER";
}

public sealed class ComponentFieldMap
{
    public string Material { get; set; } = "MATERIAL";
    public string Description { get; set; } = "MATERIAL_DESCRIPTION";
}

public sealed class ConfirmationFieldMap
{
    public string Confirmation { get; set; } = "CONF_NO";
    public string ConfirmationCounter { get; set; } = "CONF_CNT";
    public string Yield { get; set; } = "YIELD";
    public string DetailYield { get; set; } = "YIELD";
}

public sealed class AfruFieldMap
{
    public string WorkCenterId { get; set; } = "ARBID";
    public string Yield { get; set; } = "YIELD";
    public string Reversed { get; set; } = "REVERSED";
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
