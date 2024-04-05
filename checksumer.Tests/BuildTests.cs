using Microsoft.Data.Sqlite;

namespace checksumer.Tests;

public class BuildTests
{
    [Test]
    public void EmptySourceDirectory()
    {
        var databaseFile = TempFileName();
        Assert.AreEqual(1, Program.Build(Path.Combine(".", "TestData", "empty"), databaseFile, [".gitkeep"]));
    }

    [Test]
    public void ExistingIndex()
    {
        var databaseFile = TempFileName();
        File.Create(databaseFile).Dispose();
        Assert.AreEqual(1, Program.Build(Path.Combine(".", "TestData", "dir1"), databaseFile, Array.Empty<string>()));
    }

    [Test]
    public void Build()
    {
        var databaseFile = TempFileName();
        Assert.AreEqual(0, Program.Build(Path.Combine(".", "TestData", "dir1"), databaseFile, [".gitkeep"]));

        using SqliteConnection db = new($"Data Source={databaseFile}");
        db.Open();
        using var filesCmd = db.CreateCommand();
        filesCmd.CommandText = "SELECT path, size, created, modified, hash, hash_of_hash FROM files";
        using var filesReader = filesCmd.ExecuteReader();

        var rows = new List<(string path, long size, long created, long modified, byte[] hash, byte[] hashOfHash)>();
        while (filesReader.Read())
        {
            rows.Add((
                filesReader.GetString(0),
                filesReader.GetInt64(1),
                filesReader.GetInt64(2),
                filesReader.GetInt64(3),
                filesReader.GetFieldValue<byte[]>(4),
                filesReader.GetFieldValue<byte[]>(5)
            ));
        }

        Assert.AreEqual(3, rows.Count);
        var file1 = rows.Single(r => r.path == "file1.txt");
        var file2 = rows.Single(r => r.path == "subdir/file2.txt");
        var empty = rows.Single(r => r.path == "empty.txt");

        Assert.AreEqual(15, file1.size);
        Assert.AreEqual(638478492238318832, file1.created);
        Assert.AreEqual(638478492396376005, file1.modified);
        Assert.AreEqual("4D78990B1F2B696B9BEF40509A05625956AA42E2", BitConverter.ToString(file1.hash).Replace("-", ""));
        Assert.AreEqual("5E1A4EB288B6862B2CAD83143942907E19B093B6", BitConverter.ToString(file1.hashOfHash).Replace("-", ""));

        Assert.AreEqual(16, file2.size);
        Assert.AreEqual(638479235133912349, file2.created);
        Assert.AreEqual(638479403762984694, file2.modified);
        Assert.AreEqual("D5229A9B110B72F7C42A782428034645E1245D50", BitConverter.ToString(file2.hash).Replace("-", ""));
        Assert.AreEqual("98C02E5A6A3B82FD567B2A7DAEBE673F04CB4873", BitConverter.ToString(file2.hashOfHash).Replace("-", ""));

        Assert.AreEqual(0, empty.size);
        Assert.AreEqual(638478495801362860, empty.created);
        Assert.AreEqual(638478495801362860, empty.modified);
        Assert.AreEqual("DA39A3EE5E6B4B0D3255BFEF95601890AFD80709", BitConverter.ToString(empty.hash).Replace("-", ""));
        Assert.AreEqual("BE1BDEC0AA74B4DCB079943E70528096CCA985F8", BitConverter.ToString(empty.hashOfHash).Replace("-", ""));

        using var metaCmd = db.CreateCommand();
        metaCmd.CommandText = "SELECT version, initial_path, created FROM meta";
        using var metaReader = metaCmd.ExecuteReader();
        metaReader.Read();

        Assert.AreEqual(1, metaReader.GetInt32(0));
        Assert.AreEqual(Path.Combine(".", "TestData", "dir1"), metaReader.GetString(1));
        var created = new DateTime(metaReader.GetInt64(2));
        Assert.LessOrEqual(DateTime.UtcNow - created, TimeSpan.FromSeconds(1));

        Assert.False(metaReader.Read());
    }

    private string TempFileName() => Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
}