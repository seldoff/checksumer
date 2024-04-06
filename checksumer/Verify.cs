using System.Diagnostics;
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using static checksumer.Utils;

namespace checksumer;

public static class Verify
{
    private enum VerifyFileResult
    {
        Ok,
        Failed,
        HashVerificationFailed,
        FileChanged,
        NotFound
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
        Console.WriteLine($"Verifying index ('{databaseFile}') for path '{path}'");

        var files = GetFiles(path, databaseFile, ignoredFiles);
        if (files.Length == 0)
        {
            return 1;
        }

        using var db = new SqliteConnection($"Data Source={databaseFile};");
        db.Open();
        using var transaction = db.BeginTransaction();

        using var selectCmd = db.CreateCommand();
        selectCmd.CommandText = "SELECT size, created, modified, hash, hash_of_hash FROM files WHERE path = @path;";
        selectCmd.Prepare();

        var dbHash = new byte[SHA1.HashSizeInBytes];
        var dbHashOfHash = new byte[SHA1.HashSizeInBytes];
        var hashOfHash = new byte[SHA1.HashSizeInBytes];
        var fsHash = new byte[SHA1.HashSizeInBytes];
        var failures = new List<string>();
        var verificationFailures = new List<string>();
        var hashVerificationFailures = new List<string>();
        var changedFiles = new List<string>();
        var notFoundFiles = new List<string>();
        var stats = new Stats(files.Length, Consts.ReportInterval);
        foreach (var file in files)
        {
            try
            {
                var result = VerifyFile(path, file, stats, dbHash, dbHashOfHash, hashOfHash, fsHash, selectCmd);
                switch (result)
                {
                    case VerifyFileResult.Ok:
                        break;
                    case VerifyFileResult.Failed:
                        verificationFailures.Add(file);
                        break;
                    case VerifyFileResult.HashVerificationFailed:
                        hashVerificationFailures.Add(file);
                        break;
                    case VerifyFileResult.FileChanged:
                        changedFiles.Add(file);
                        break;
                    case VerifyFileResult.NotFound:
                        notFoundFiles.Add(file);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(result));
                }
            }
            catch
            {
                failures.Add(file);
                stats.IncrementProcessed(0);
            }

            stats.ReportProgress();
        }

        Console.WriteLine("Finished in " + sw.Elapsed);

        ListFilesToOutput(changedFiles, "Changed files:");
        ListFilesToOutput(notFoundFiles, "Files not found in the index:");
        ListFilesToOutput(verificationFailures, "Verification failed for the following files:");
        ListFilesToOutput(hashVerificationFailures, "Verification failed for hash of the following files:");
        ListFilesToOutput(failures, "Failed to process the following files:");

        return verificationFailures.Count > 0 || hashVerificationFailures.Count > 0 || failures.Count > 0 ? 1 : 0;
    }

    private static VerifyFileResult VerifyFile(string path, string file, Stats stats,
        byte[] dbHash, byte[] dbHashOfHash, byte[] hashOfHash, byte[] fsHash, SqliteCommand selectCmd)
    {
        var fileInfo = new FileInfo(file);
        selectCmd.Parameters.Clear();
        selectCmd.Parameters.AddWithValue("@path", Path.GetRelativePath(path, file));
        using var reader = selectCmd.ExecuteReader();
        if (!reader.Read())
        {
            Console.WriteLine($"File '{file}' not found in the index");
            return VerifyFileResult.NotFound;
        }

        var size = reader.GetInt64(0);
        var created = reader.GetInt64(1);
        var modified = reader.GetInt64(2);

        if (fileInfo.Length != size ||
            fileInfo.CreationTimeUtc.ToUnixTime() != created ||
            fileInfo.LastWriteTimeUtc.ToUnixTime() != modified)
        {
            Console.WriteLine($"File '{file}' has changed");
            return VerifyFileResult.FileChanged;
        }

        var read = reader.GetBytes(3, 0, dbHash, 0, dbHash.Length);
        if (read != dbHash.Length)
        {
            throw new Exception($"Error reading hash from the database: invalid hash length {read}");
        }

        read = reader.GetBytes(4, 0, dbHashOfHash, 0, dbHashOfHash.Length);
        if (read != dbHashOfHash.Length)
        {
            throw new Exception($"Error reading hash of hash from the database: invalid hash length {read}");
        }

        VerifyHashLength(SHA1.HashData(dbHash, hashOfHash));
        if (!VerifyHash(hashOfHash, dbHashOfHash))
        {
            Console.WriteLine($"Stored hash of file '{file}' failed verification");
            return VerifyFileResult.HashVerificationFailed;
        }

        GetFileHash(file, fsHash);
        if (!VerifyHash(fsHash, dbHash))
        {
            Console.WriteLine($"File '{file}' failed verification");
            stats.IncrementProcessed(fileInfo.Length);
            return VerifyFileResult.Failed;
        }

        stats.IncrementProcessed(fileInfo.Length);
        return VerifyFileResult.Ok;
    }
}