using System.Diagnostics;

namespace checksumer;

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