using Microsoft.Data.Sqlite;

namespace checksumer.Tests;

public class BuildTests
{
    [Test]
    public void EmptySourceDirectory()
    {
        var databaseFile = TempFileName();
        Assert.AreEqual(1, checksumer.Build.Run(Path.Combine(".", "TestData", "empty"), databaseFile, [".gitkeep"]));
    }

    [Test]
    public void ExistingIndex()
    {
        var databaseFile = TempFileName();
        File.Create(databaseFile).Dispose();
        Assert.AreEqual(1,
            checksumer.Build.Run(Path.Combine(".", "TestData", "dir1"), databaseFile, Array.Empty<string>()));
    }

    [Test]
    public void Build()
    {
        var databaseFile = TempFileName();
        var path = Path.Combine(".", "TestData", "dir1");
        Assert.AreEqual(0, checksumer.Build.Run(path, databaseFile, [".gitkeep"]));

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
        Assert.AreEqual(Created(file1.path, path), file1.created);
        Assert.AreEqual(Modified(file1.path, path), file1.modified);
        Assert.AreEqual("4D78990B1F2B696B9BEF40509A05625956AA42E2", BitConverter.ToString(file1.hash).Replace("-", ""));
        Assert.AreEqual("5E1A4EB288B6862B2CAD83143942907E19B093B6", BitConverter.ToString(file1.hashOfHash).Replace("-", ""));

        Assert.AreEqual(16, file2.size);
        Assert.AreEqual(Created(file2.path, path), file2.created);
        Assert.AreEqual(Modified(file2.path, path), file2.modified);
        Assert.AreEqual("D5229A9B110B72F7C42A782428034645E1245D50", BitConverter.ToString(file2.hash).Replace("-", ""));
        Assert.AreEqual("98C02E5A6A3B82FD567B2A7DAEBE673F04CB4873", BitConverter.ToString(file2.hashOfHash).Replace("-", ""));

        Assert.AreEqual(0, empty.size);
        Assert.AreEqual(Created(empty.path, path), empty.created);
        Assert.AreEqual(Modified(empty.path, path), empty.modified);
        Assert.AreEqual("DA39A3EE5E6B4B0D3255BFEF95601890AFD80709", BitConverter.ToString(empty.hash).Replace("-", ""));
        Assert.AreEqual("BE1BDEC0AA74B4DCB079943E70528096CCA985F8", BitConverter.ToString(empty.hashOfHash).Replace("-", ""));

        using var metaCmd = db.CreateCommand();
        metaCmd.CommandText = "SELECT version, algorithm, initial_path, created, last_updated FROM meta";
        using var metaReader = metaCmd.ExecuteReader();
        metaReader.Read();

        Assert.AreEqual(1, metaReader.GetInt32(0));
        Assert.AreEqual("sha1", metaReader.GetString(1));
        Assert.AreEqual(Path.GetFullPath(Path.Combine(".", "TestData", "dir1")), metaReader.GetString(2));
        var created = new DateTime(metaReader.GetInt64(3));
        Assert.LessOrEqual(DateTime.UtcNow - created, TimeSpan.FromSeconds(1));
        Assert.IsTrue(metaReader.IsDBNull(4));

        Assert.False(metaReader.Read());

        long Created(string fileName, string path) => new FileInfo(Path.Combine(path, fileName)).CreationTimeUtc.ToUnixTime();
        long Modified(string fileName, string path) => new FileInfo(Path.Combine(path, fileName)).LastWriteTimeUtc.ToUnixTime();
    }

    private string TempFileName() => Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
}