namespace checksumer;

public static class Consts
{
    public static readonly string[] IgnoredFiles = new[] {".DS_Store"}
        .Select(file => $"{Path.PathSeparator}{file}")
        .ToArray();

    public static readonly TimeSpan ReportInterval = TimeSpan.FromSeconds(5);
}