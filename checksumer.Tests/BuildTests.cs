namespace checksumer.Tests;

public class BuildTests
{
    [Test]
    public void Build()
    {
        var databaseFile = Path.GetTempFileName();
        Assert.AreEqual(0, Program.Build(Path.Combine(".", "TestData", "dir1"), databaseFile, Array.Empty<string>()));
    }
}