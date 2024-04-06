using System.Security.Cryptography;

namespace checksumer;

public static class Utils
{
    public static bool VerifyHash(byte[] hash, byte[] expectedHash)
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

    public static long ToUnixTime(this DateTime date) => new DateTimeOffset(date.ToUniversalTime()).ToUnixTimeSeconds();

    public static void ListFilesToOutput(IReadOnlyCollection<string> files, string header)
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

    public static void VerifyHashLength(int hashLength)
    {
        if (hashLength != SHA1.HashSizeInBytes)
        {
            throw new Exception($"Error calculating hash: invalid hash length of {hashLength} bytes");
        }
    }

    public static void VerifySingleRow(int count)
    {
        if (count != 1)
        {
            throw new Exception($"Expected to insert/update 1 row, got {count}");
        }
    }

    public static string[] GetFiles(string path, string databaseFile, IReadOnlyCollection<string> ignoredFiles)
    {
        var files = Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories)
            .Where(file => file != databaseFile)
            .Where(file => !ignoredFiles.Any(file.EndsWith))
            .ToArray();

        Console.WriteLine(files.Length == 0 ? "No files found" : $"Found {files.Length} files");

        return files;
    }

    public static void GetFileHash(string file, byte[] hash)
    {
        using var fileStream = File.Open(file, FileMode.Open, FileAccess.Read, FileShare.Read);
        VerifyHashLength(SHA1.HashData(fileStream, hash));
    }
}