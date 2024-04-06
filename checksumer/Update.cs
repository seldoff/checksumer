using System.Diagnostics;
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using static checksumer.Utils;

namespace checksumer;

public static class Update
{
    private enum UpdateFileResult
    {
        Unchanged,
        Changed,
        New
    }

    public static int Run(IReadOnlyCollection<string> args)
    {
        var path = args.FirstOrDefault()?.Trim();
        if (string.IsNullOrWhiteSpace(path))
        {
            Console.WriteLine("Please specify path");
            return 1;
        }

        if (!Directory.Exists(path))
        {
            Console.WriteLine($"Path '{path}' does not exist");
            return 1;
        }

        var databaseFile = args.Skip(1).FirstOrDefault()?.Trim();
        if (!File.Exists(databaseFile))
        {
            Console.WriteLine($"Database file '{databaseFile}' does not exist");
            return 1;
        }

        try
        {
            var code = Run(path, databaseFile, Consts.IgnoredFiles);
            return code;
        }
        catch (Exception e)
        {
            Console.WriteLine($"Error: {e.Message}");
            return 1;
        }
    }

    private static int Run(string path, string databaseFile, IReadOnlyCollection<string> ignoredFiles)
    {
        var sw = Stopwatch.StartNew();
        databaseFile = Path.GetFullPath(databaseFile);
        Console.WriteLine($"Updating index ('{databaseFile}') for path '{path}'");

        var files = GetFiles(path, databaseFile, ignoredFiles);
        if (files.Length == 0)
        {
            return 1;
        }

        using var db = new SqliteConnection($"Data Source={databaseFile};");
        db.Open();

        using var transaction = db.BeginTransaction();

        using var selectCmd = db.CreateCommand();
        selectCmd.CommandText = "SELECT size, created, modified FROM files WHERE path = @path;";
        selectCmd.Prepare();

        using var insertCmd = db.CreateCommand();
        insertCmd.CommandText =
            "INSERT INTO files (path, size, created, modified, hash, hash_of_hash) VALUES (@path, @size, @created, @modified, @hash, @hash_of_hash);";
        insertCmd.Prepare();

        using var updateCmd = db.CreateCommand();
        updateCmd.CommandText =
            "UPDATE files SET size = @size, created = @created, modified = @modified, hash = @hash, hash_of_hash = @hash_of_hash WHERE path = @path;";
        updateCmd.Prepare();

        var hash = new byte[SHA1.HashSizeInBytes];
        var hashOfHash = new byte[SHA1.HashSizeInBytes];
        var failures = new List<string>();
        var changedFiles = new List<string>();
        var newFiles = new List<string>();
        var stats = new Stats(files.Length, Consts.ReportInterval);
        foreach (var file in files)
        {
            try
            {
                var result = UpdateFile(path, file, hash, hashOfHash, selectCmd, insertCmd, updateCmd);
                switch (result)
                {
                    case UpdateFileResult.Unchanged:
                        break;
                    case UpdateFileResult.Changed:
                        changedFiles.Add(file);
                        break;
                    case UpdateFileResult.New:
                        newFiles.Add(file);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(result));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing file '{file}': {ex.Message}");
                failures.Add(file);
            }
            finally
            {
                stats.IncrementProcessed(0);
            }

            stats.ReportProgress(includeThroughput: false);
        }

        var cmd = db.CreateCommand();
        cmd.CommandText = "UPDATE meta SET last_updated = @last_updated;";
        cmd.Parameters.AddWithValue("@last_updated", DateTime.UtcNow.Ticks);
        VerifySingleRow(cmd.ExecuteNonQuery());

        transaction.Commit();
        Console.WriteLine("Finished in " + sw.Elapsed);

        ListFilesToOutput(newFiles, "New files:");
        ListFilesToOutput(changedFiles, "Changed files:");

        if (changedFiles.Count == 0 && newFiles.Count == 0)
        {
            Console.WriteLine("No changed files");
        }

        ListFilesToOutput(failures, "Failed to process the following files:");
        return failures.Count > 0 ? 1 : 0;
    }

    private static UpdateFileResult UpdateFile(string path, string file, byte[] hash, byte[] hashOfHash,
        SqliteCommand selectCmd, SqliteCommand insertCmd, SqliteCommand updateCmd)
    {
        var fileInfo = new FileInfo(file);
        selectCmd.Parameters.Clear();
        selectCmd.Parameters.AddWithValue("@path", Path.GetRelativePath(path, file));
        using var reader = selectCmd.ExecuteReader();
        if (reader.Read())
        {
            var size = reader.GetInt64(0);
            var created = reader.GetInt64(1);
            var modified = reader.GetInt64(2);

            if (fileInfo.Length == size &&
                fileInfo.CreationTimeUtc.ToUnixTime() == created &&
                fileInfo.LastWriteTimeUtc.ToUnixTime() == modified)
            {
                return UpdateFileResult.Unchanged;
            }

            Console.WriteLine($"File '{file}' has changed");
            GetFileHash(file, hash);
            VerifyHashLength(SHA1.HashData(hash, hashOfHash));

            updateCmd.Parameters.Clear();
            updateCmd.Parameters.AddWithValue("@path", Path.GetRelativePath(path, file));
            updateCmd.Parameters.AddWithValue("@size", fileInfo.Length);
            updateCmd.Parameters.AddWithValue("@created", fileInfo.CreationTimeUtc.ToUnixTime());
            updateCmd.Parameters.AddWithValue("@modified", fileInfo.LastWriteTimeUtc.ToUnixTime());
            updateCmd.Parameters.AddWithValue("@hash", hash);
            updateCmd.Parameters.AddWithValue("@hash_of_hash", hashOfHash);

            VerifySingleRow(updateCmd.ExecuteNonQuery());

            if (reader.Read())
            {
                throw new Exception("More than one row returned");
            }

            return UpdateFileResult.Changed;
        }

        Console.WriteLine($"File '{file}' is new");
        GetFileHash(file, hash);
        VerifyHashLength(SHA1.HashData(hash, hashOfHash));

        insertCmd.Parameters.Clear();
        insertCmd.Parameters.AddWithValue("@path", Path.GetRelativePath(path, file));
        insertCmd.Parameters.AddWithValue("@size", fileInfo.Length);
        insertCmd.Parameters.AddWithValue("@created", fileInfo.CreationTimeUtc.ToUnixTime());
        insertCmd.Parameters.AddWithValue("@modified", fileInfo.LastWriteTimeUtc.ToUnixTime());
        insertCmd.Parameters.AddWithValue("@hash", hash);
        insertCmd.Parameters.AddWithValue("@hash_of_hash", hashOfHash);

        VerifySingleRow(insertCmd.ExecuteNonQuery());
        return UpdateFileResult.New;
    }
}