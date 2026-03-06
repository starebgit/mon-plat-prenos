namespace MonPlatPrenos.Worker.Services;

public sealed class PrenosOptions
{
    public string DailyRunTime { get; set; } = "07:30";
    public string OutputDirectory { get; set; } = "output";
    public string PlateMaterialFrom { get; set; } = "000012080000000000";
    public string PlateMaterialTo { get; set; } = "000019789000000000";
    public string SchedulerCode { get; set; } = "200";
    public List<string> OperationCodes { get; set; } = ["PP04", "PP14", "PP02", "PP10"];
    public List<TermRule> DefaultTerms { get; set; } = new();
    public List<TermRule> ExtraTerms { get; set; } = new();
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

        if (!input.Contains(Contains, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(ExcludeContains) && input.Contains(ExcludeContains, StringComparison.OrdinalIgnoreCase))
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
