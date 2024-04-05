using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;

namespace checksumer;

public enum UpdateFileResult
{
    Unchanged,
    Changed,
    New
}

public enum VerifyFileResult
{
    Ok,
    Failed,
    HashVerificationFailed,
    FileChanged,
    NotFound
}

public class Stats(int total, TimeSpan reportInterval)
{
    private readonly Stopwatch _sw = Stopwatch.StartNew();
    private DateTimeOffset _lastProgressReport = DateTimeOffset.Now;
    private int _processed;
    private long _bytesProcessed;

    public void IncrementProcessed(long bytes)
    {
        _processed++;
        _bytesProcessed += bytes;
    }

    public void ReportProgress(bool includeThroughput = true)
    {
        if (DateTimeOffset.Now - _lastProgressReport >= reportInterval)
        {
            var percentage = (int)((double)_processed / total * 100);
            var elapsedSeconds = _sw.Elapsed.TotalSeconds;
            var gps = elapsedSeconds > 0 ? _bytesProcessed / elapsedSeconds / 1024 / 1024 / 1024 : 0;
            Console.WriteLine(includeThroughput
                ? $"[{percentage}%] Processed {_processed} of {total} files ({gps:0.0} GB/s)"
                : $"[{percentage}%] Processed {_processed} of {total} files");
            _lastProgressReport = DateTimeOffset.Now;
        }
    }
}

public static class Program
{
    private static readonly string[] IgnoredFiles = new[] {".DS_Store"}
        .Select(file => $"{Path.PathSeparator}{file}")
        .ToArray();

    private static readonly TimeSpan ReportInterval = TimeSpan.FromSeconds(5);

    public static void Main(string[] args)
    {
        var verb = args.FirstOrDefault();
        switch (verb)
        {
            case "build":
                Environment.Exit(Build(args.Skip(1).ToArray()));
                break;
            case "update":
                Environment.Exit(Update(args.Skip(1).ToArray()));
                break;
            case "verify":
                Environment.Exit(Verify(args.Skip(1).ToArray()));
                break;
            case null:
            {
                Console.WriteLine("No command");
                Environment.Exit(1);
                break;
            }
            default:
            {
                Console.WriteLine($"Unknown command {verb}");
                Environment.Exit(1);
                break;
            }
        }
    }

    private static int Verify(IReadOnlyCollection<string> args)
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
            var code = Verify(path, databaseFile, IgnoredFiles);
            return code;
        }
        catch (Exception e)
        {
            Console.WriteLine($"Error: {e.Message}");
            return 1;
        }
    }

    private static int Verify(string path, string databaseFile, IReadOnlyCollection<string> ignoredFiles)
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
        var stats = new Stats(files.Length, ReportInterval);
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

        if (fileInfo.Length != size || fileInfo.CreationTimeUtc.Ticks != created ||
            fileInfo.LastWriteTimeUtc.Ticks != modified)
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

    private static int Update(IReadOnlyCollection<string> args)
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
            var code = Update(path, databaseFile, IgnoredFiles);
            return code;
        }
        catch (Exception e)
        {
            Console.WriteLine($"Error: {e.Message}");
            return 1;
        }
    }

    private static int Update(string path, string databaseFile, IReadOnlyCollection<string> ignoredFiles)
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
        insertCmd.CommandText = "INSERT INTO files (path, size, created, modified, hash, hash_of_hash) VALUES (@path, @size, @created, @modified, @hash, @hash_of_hash);";
        insertCmd.Prepare();

        using var updateCmd = db.CreateCommand();
        updateCmd.CommandText = "UPDATE files SET size = @size, created = @created, modified = @modified, hash = @hash, hash_of_hash = @hash_of_hash WHERE path = @path;";
        updateCmd.Prepare();

        var hash = new byte[SHA1.HashSizeInBytes];
        var hashOfHash = new byte[SHA1.HashSizeInBytes];
        var failures = new List<string>();
        var changedFiles = new List<string>();
        var newFiles = new List<string>();
        var stats = new Stats(files.Length, ReportInterval);
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

            if (fileInfo.Length == size && fileInfo.CreationTimeUtc.Ticks == created &&
                fileInfo.LastWriteTimeUtc.Ticks == modified)
            {
                return UpdateFileResult.Unchanged;
            }

            Console.WriteLine($"File '{file}' has changed");
            GetFileHash(file, hash);
            VerifyHashLength(SHA1.HashData(hash, hashOfHash));

            updateCmd.Parameters.Clear();
            updateCmd.Parameters.AddWithValue("@path", Path.GetRelativePath(path, file));
            updateCmd.Parameters.AddWithValue("@size", fileInfo.Length);
            updateCmd.Parameters.AddWithValue("@created", fileInfo.CreationTimeUtc.Ticks);
            updateCmd.Parameters.AddWithValue("@modified", fileInfo.LastWriteTimeUtc.Ticks);
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
        insertCmd.Parameters.AddWithValue("@created", fileInfo.CreationTimeUtc.Ticks);
        insertCmd.Parameters.AddWithValue("@modified", fileInfo.LastWriteTimeUtc.Ticks);
        insertCmd.Parameters.AddWithValue("@hash", hash);
        insertCmd.Parameters.AddWithValue("@hash_of_hash", hashOfHash);

        VerifySingleRow(insertCmd.ExecuteNonQuery());
        return UpdateFileResult.New;
    }

    private static int Build(IReadOnlyCollection<string> args)
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
            return Build(path, databaseFile, IgnoredFiles);
        }
        catch (Exception e)
        {
            Console.WriteLine($"Error: {e.Message}");
            return 1;
        }
    }

    public static int Build(string path, string databaseFile, IReadOnlyCollection<string> ignoredFiles)
    {
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
        databaseFile = Path.GetFullPath(databaseFile);
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
        var stats = new Stats(files.Length, ReportInterval);
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
        cmd.Parameters.AddWithValue("@created", fileInfo.CreationTimeUtc.Ticks);
        cmd.Parameters.AddWithValue("@modified", fileInfo.LastWriteTimeUtc.Ticks);
        cmd.Parameters.AddWithValue("@hash", hash);
        cmd.Parameters.AddWithValue("@hash_of_hash", hashOfHash);

        VerifySingleRow(cmd.ExecuteNonQuery());

        stats.IncrementProcessed(fileInfo.Length);
    }

    private static void GetFileHash(string file, byte[] hash)
    {
        using var fileStream = File.Open(file, FileMode.Open, FileAccess.Read, FileShare.Read);
        VerifyHashLength(SHA1.HashData(fileStream, hash));
    }

    private static void VerifyHashLength(int hashLength)
    {
        if (hashLength != SHA1.HashSizeInBytes)
        {
            throw new Exception($"Error calculating hash: invalid hash length of {hashLength} bytes");
        }
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

    private static void VerifySingleRow(int count)
    {
        if (count != 1)
        {
            throw new Exception($"Expected to insert/update 1 row, got {count}");
        }
    }

    private static void ListFilesToOutput(IReadOnlyCollection<string> files, string header)
    {
        if (files.Count > 0)
        {
            Console.WriteLine(header);
            foreach (var file in files)
            {
                Console.WriteLine(file);
            }
        }
    }

    private static string[] GetFiles(string path, string databaseFile, IReadOnlyCollection<string> ignoredFiles)
    {
        var files = Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories)
            .Where(file => file != databaseFile)
            .Where(file => !ignoredFiles.Any(file.EndsWith))
            .ToArray();

        Console.WriteLine(files.Length == 0 ? "No files found" : $"Found {files.Length} files");

        return files;
    }

    private static bool VerifyHash(byte[] hash, byte[] expectedHash)
    {
        if (hash.Length != expectedHash.Length)
        {
            throw new Exception($"Invalid hash length: expected {expectedHash.Length}, got {hash.Length}");
        }

        for (var i = 0; i < hash.Length; i++)
        {
            if (hash[i] != expectedHash[i])
            {
                return false;
            }
        }

        return true;
    }

    private static void ReportProgress(int processed, int total, ref DateTimeOffset last)
    {
        var now = DateTimeOffset.Now;
        if (now - last > TimeSpan.FromSeconds(5))
        {
            Console.WriteLine($"Processed {processed} of {total} files");
            last = now;
        }
    }
}