using System.Diagnostics;
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using static checksumer.Utils;

namespace checksumer;

public static class Build
{
    public static int Run(IReadOnlyCollection<string> args)
    {
        var path = args.FirstOrDefault()?.Trim();
        if (string.IsNullOrWhiteSpace(path))
        {
            Console.WriteLine("Please specify path");
            return 1;
        }

        var databaseFile = args.Skip(1).FirstOrDefault()?.Trim();
        if (string.IsNullOrWhiteSpace(databaseFile))
        {
            Console.WriteLine("Please specify database file");
            return 1;
        }

        try
        {
            return Run(path, databaseFile, Consts.IgnoredFiles);
        }
        catch (Exception e)
        {
            Console.WriteLine($"Error: {e.Message}");
            return 1;
        }
    }

    public static int Run(string path, string databaseFile, IReadOnlyCollection<string> ignoredFiles)
    {
        databaseFile = Path.GetFullPath(databaseFile);
        path = Path.GetFullPath(path);

        if (File.Exists(databaseFile))
        {
            Console.WriteLine($"Database file '{databaseFile}' already exists");
            return 1;
        }

        if (!Directory.Exists(path))
        {
            Console.WriteLine($"Path '{path}' does not exist");
            return 1;
        }

        var sw = Stopwatch.StartNew();
        Console.WriteLine($"Building index ('{databaseFile}') for path '{path}'");

        var files = GetFiles(path, databaseFile, ignoredFiles);
        if (files.Length == 0)
        {
            return 1;
        }

        using var db = new SqliteConnection($"Data Source={databaseFile};");
        db.Open();
        CreateDatabase(db, path);

        using var transaction = db.BeginTransaction();
        using var cmd = db.CreateCommand();
        cmd.CommandText = "INSERT INTO files (path, size, created, modified, hash, hash_of_hash) VALUES (@path, @size, @created, @modified, @hash, @hash_of_hash);";
        cmd.Prepare();

        var hash = new byte[SHA1.HashSizeInBytes];
        var hashOfHash = new byte[SHA1.HashSizeInBytes];
        var failures = new List<string>();
        var stats = new Stats(files.Length, Consts.ReportInterval);
        foreach (var file in files)
        {
            try
            {
                BuildFile(path, file, stats, hash, hashOfHash, cmd);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing file '{file}': {ex.Message}");
                failures.Add(file);
                stats.IncrementProcessed(0);
            }

            stats.ReportProgress();
        }

        transaction.Commit();
        Console.WriteLine("Finished in " + sw.Elapsed);

        ListFilesToOutput(failures, "Failed to process the following files:");
        return failures.Count > 0 ? 1 : 0;
    }

    private static void BuildFile(string path, string file, Stats stats, byte[] hash, byte[] hashOfHash,
        SqliteCommand cmd)
    {
        var fileInfo = new FileInfo(file);
        GetFileHash(file, hash);
        VerifyHashLength(SHA1.HashData(hash, hashOfHash));

        cmd.Parameters.Clear();
        cmd.Parameters.AddWithValue("@path", Path.GetRelativePath(path, file));
        cmd.Parameters.AddWithValue("@size", fileInfo.Length);
        cmd.Parameters.AddWithValue("@created", fileInfo.CreationTimeUtc.ToUnixTime());
        cmd.Parameters.AddWithValue("@modified", fileInfo.LastWriteTimeUtc.ToUnixTime());
        cmd.Parameters.AddWithValue("@hash", hash);
        cmd.Parameters.AddWithValue("@hash_of_hash", hashOfHash);

        VerifySingleRow(cmd.ExecuteNonQuery());

        stats.IncrementProcessed(fileInfo.Length);
    }

    private static void CreateDatabase(SqliteConnection db, string initialPath)
    {
        using var cmd = db.CreateCommand();
        cmd.CommandText =
            $"""
             PRAGMA journal_mode = WAL;
             CREATE TABLE files (
                 path TEXT NOT NULL PRIMARY KEY,
                 size INTEGER NOT NULL,
                 created INTEGER NOT NULL,
                 modified INTEGER NOT NULL,
                 hash BLOB({SHA1.HashSizeInBytes}) NOT NULL,
                 hash_of_hash BLOB({SHA1.HashSizeInBytes}) NOT NULL
             );
             CREATE TABLE meta (
                 version INTEGER NOT NULL,
                 algorithm TEXT NOT NULL,
                 initial_path TEXT NOT NULL,
                 created INTEGER NOT NULL,
                 last_updated INTEGER
             );
             """;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO meta (version, algorithm, initial_path, created) VALUES (1, 'sha1', @initial_path, @created);";
        cmd.Parameters.AddWithValue("@initial_path", initialPath);
        cmd.Parameters.AddWithValue("@created", DateTime.UtcNow.Ticks);
        VerifySingleRow(cmd.ExecuteNonQuery());
    }
}