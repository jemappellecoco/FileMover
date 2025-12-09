// Services/MoveWorker.cs
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using FileMoverWeb.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

namespace FileMoverWeb.Services
{
    public sealed class MoveWorker
    {
        private readonly IJobProgress _progress;
        private readonly ILogger<MoveWorker> _logger;
        private readonly IConfiguration _cfg;   // ⭐ 真的存下來，RunAsync 要用

        // ===== 調整參數（視環境可微調） =====
        private const int REPORT_INTERVAL_MS = 300;               // 至少每 300ms 回報一次
        private const long REPORT_BYTES_STEP = 4L * 1024 * 1024;  // 或每累積 ≥ 4 MB 回報
        private readonly ICancelStore _cancelStore;
        public MoveWorker(IJobProgress progress, ILogger<MoveWorker> logger, IConfiguration cfg, ICancelStore cancelStore)
            {
                _progress = progress;
                _logger = logger;
                _cfg = cfg;
                _cancelStore = cancelStore;
            }

        /// <summary>
        /// 執行一個搬運批次，回傳每筆結果。
        /// </summary>
        public Task<List<MoveResult>> RunAsync(
            MoveBatchRequest req, 
            CancellationToken ct = default)
            => RunAsync(req, onItemDone: null, ct);

        /// <summary>
        /// 執行一個搬運批次，回傳每筆結果，並可在每筆完成時回呼 onItemDone。
        /// </summary>
        public async Task<List<MoveResult>> RunAsync(
        MoveBatchRequest req,
        Func<MoveResult, Task>? onItemDone,
        CancellationToken ct = default)
    {
        if (req is null) throw new ArgumentNullException(nameof(req));
        if (req.Items is null || req.Items.Count == 0)
            return new List<MoveResult>(0);

    // ❌ 不再在這裡讀 GlobalMaxConcurrentMoves 來控並行
    // 併發數 = HistoryWatchService 的 slot 數量

    // 預估總量（跟以前一樣，給 progress 用）
    var totals = req.Items
        .GroupBy(i => i.DestId, StringComparer.OrdinalIgnoreCase)
        .ToDictionary(
            g => g.Key,
            g => g.Sum(i =>
            {
                try
                {
                    var fi = new FileInfo(i.SourcePath);
                    return fi.Exists ? fi.Length : 0L;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "無法讀取檔案大小：{Src}", i.SourcePath);
                    return 0L;
                }
            }),
            StringComparer.OrdinalIgnoreCase
        );

    _progress.InitTotals(req.JobId, totals);

    // ⭐ 不再有 destLimiter、不再平行 group
    //    這裡就單純一個一個呼叫 MoveGroupAsync
    var bag = new ConcurrentBag<MoveResult>();

    try
    {
        // 一個 destId 一次搬一組（slot 已經控好併行數了）
        foreach (var g in req.Items.GroupBy(i => i.DestId, StringComparer.OrdinalIgnoreCase))
        {
            await MoveGroupAsync(
                req.JobId,
                g.Key,
                g.ToList(),
                bag,
                onItemDone,
                ct
            ).ConfigureAwait(false);
        }

        return bag.ToList();
    }
    finally
    {
        // ⭐ 不管成功 / 失敗 / 被使用者取消，這一批 job 都結束了 → 把進度清掉
        _progress.CompleteJob(req.JobId);
    }
}
    

private async Task MoveGroupAsync(
    string jobId,
    string destId,
    List<MoveItem> items,
    ConcurrentBag<MoveResult> results,
    Func<MoveResult, Task>? onItemDone,
    CancellationToken ct)
{
    foreach (var item in items)
    {
        ct.ThrowIfCancellationRequested();
        var histId = item.HistoryId ?? 0;
         // === 使用者取消 ===
                if (_cancelStore.ShouldCancel(histId))
                {
                    var cancelResult = new MoveResult
                    {
                        HistoryId  = histId,
                        Success    = false,
                        StatusCode = 999,
                        Error      = "Canceled by user"
                    };

                    results.Add(cancelResult);
                    _cancelStore.Clear(histId);

                    if (onItemDone != null)
                        await onItemDone(cancelResult).ConfigureAwait(false);

                    continue; // 跳過此筆
                }
        Console.WriteLine($"[MOVE] job={jobId}, historyId={item.HistoryId}, src={item.SourcePath}");

        MoveResult result;

        try
        {
            // 路徑拼不出來（多半是沒 FileData / 沒 UserBit） → 911
            if (string.IsNullOrWhiteSpace(item.SourcePath))
            {
                _logger.LogWarning(
                    "[{Job}] Source path empty (HistoryId={HistoryId})，多半是缺 FileData/UserBit。",
                    jobId, item.HistoryId);

                result = new MoveResult
                {
                    HistoryId  = item.HistoryId ?? 0,
                    Success    = false,
                    StatusCode = 911,
                    Error      = "Source path empty (no FileData/UserBit)"
                };
            }
            // 911：來源不存在
            else if (!File.Exists(item.SourcePath))
            {
                _logger.LogWarning("[{Job}] Source not found: {Src}", jobId, item.SourcePath);

                result = new MoveResult
                {
                    HistoryId  = item.HistoryId ?? 0,
                    Success    = false,
                    StatusCode = 911,
                    Error      = $"Source not found: {item.SourcePath}"
                };
            }
            


           else
                {       
                    
                    
                    // 先讓前端知道目前在處理哪一個檔案（進度條上會顯示檔名）
                    _progress.SetCurrentFile(
                        jobId,
                        destId,
                        Path.GetFileName(item.SourcePath) ?? item.SourcePath);

                    // ★ 在真正搬檔之前，確認來源檔案大小是否穩定
                    var stable = await WaitFileSizeStableAsync(
                        item.SourcePath,
                        probes: 3,
                        intervalMs: 800,
                        ct: ct);

                    if (!stable)
                    {
                        // 檔案大小仍在變化 → 視為正在寫入 / 使用中，不搬
                        _logger.LogWarning(
                            "[{Job}] Source file still changing, skip move: {Src}",
                            jobId, item.SourcePath);

                        result = new MoveResult
                        {
                            HistoryId  = item.HistoryId ?? 0,
                            Success    = false,
                            StatusCode = 912,  // 跟檔案使用中一樣，用 912 表示
                            Error      = "Source file still changing (size not stable)"
                        };
                    }
                    else
                    {
                        // ✅ 檔案穩定了，才開始真正搬
                        var dstPath = NormalizeDestPath(item.SourcePath, item.DestPath);

                        await CopyFileAsync(jobId, destId, item.SourcePath, dstPath, histId,ct)
    .ConfigureAwait(false);

                        result = new MoveResult
                        {
                            HistoryId  = item.HistoryId ?? 0,
                            Success    = true,
                            StatusCode = 11,
                            Error      = null
                        };
                    }
                }
                }
        catch (OperationCanceledException ex)
        {
            // 👇 這邊用 Warning 就好，代表是使用者要求的中止
            _logger.LogWarning(ex, "[{Job}] 搬運已被使用者取消：{Src}", jobId, item.SourcePath);

            result = new MoveResult
            {
                HistoryId  = item.HistoryId ?? 0,
                Success    = false,
                StatusCode = 999,                 // ⭐ 關鍵：用 999 表示「使用者取消」
                Error      = "Canceled by user"
            };
        }
        catch (IOException ex) when (IsSharingOrLockViolation(ex))   // 912
        {
            _logger.LogWarning(ex, "[{Job}] 檔案使用中（搬移失敗）：{Src}", jobId, item.SourcePath);
            result = new MoveResult
            {
                HistoryId  = item.HistoryId ?? 0,
                Success    = false,
                StatusCode = 912,
                Error      = ex.Message
            };
        }
        catch (DirectoryNotFoundException ex)                       // 914
        {
            _logger.LogWarning(ex, "[{Job}] 目的地路徑不存在（搬移失敗）：{Src}", jobId, item.SourcePath);
            result = new MoveResult
            {
                HistoryId  = item.HistoryId ?? 0,
                Success    = false,
                StatusCode = 914,
                Error      = ex.Message
            };
        }
        catch (UnauthorizedAccessException ex)                       // 913
        {
            _logger.LogWarning(ex, "[{Job}] 權限不足（搬移失敗）：{Src}", jobId, item.SourcePath);
            result = new MoveResult
            {
                HistoryId  = item.HistoryId ?? 0,
                Success    = false,
                StatusCode = 913,
                Error      = ex.Message
            };
        }
        catch (Exception ex)                                        // 91
        {
            _logger.LogError(ex, "[{Job}] 搬運失敗：{Src}", jobId, item.SourcePath);
            result = new MoveResult
            {
                HistoryId  = item.HistoryId ?? 0,
                Success    = false,
                StatusCode = 91,
                Error      = ex.Message
            };
        }

        // ⭐ 不管成功/失敗，都統一在這裡加入 results + 呼叫 callback
        results.Add(result);
        if (onItemDone != null)
        {
            await onItemDone(result).ConfigureAwait(false);
        }
    }
}
    /// <summary>
/// 檔案大小在固定時間內維持不變才視為「穩定」
/// 例：probes=3, intervalMs=800 → 約 1.6 秒內都沒有變化
/// </summary>
private static async Task<bool> WaitFileSizeStableAsync(
    string path,
    int probes = 3,
    int intervalMs = 800,
    CancellationToken ct = default)
{
    if (string.IsNullOrWhiteSpace(path))
        return false;

    if (!File.Exists(path))
        return false;

    long? lastSize = null;

    for (int i = 0; i < probes; i++)
    {
        ct.ThrowIfCancellationRequested();

        long size;
        try
        {
            var fi = new FileInfo(path);
            if (!fi.Exists)
                return false;

            size = fi.Length;
        }
        catch
        {
            // 讀不到大小就當作不穩定
            return false;
        }

        if (lastSize.HasValue && size != lastSize.Value)
        {
            // 任兩次量測不一致 → 視為正在變化
            return false;
        }

        lastSize = size;

        // 最後一次不用再等
        if (i < probes - 1)
            await Task.Delay(intervalMs, ct).ConfigureAwait(false);
    }

    return true;
}
private async Task CopyFileAsync(
    string jobId,
    string destId,
    string srcPath,
    string dstPath,
    int historyId,
    CancellationToken ct)
{
    if (string.IsNullOrWhiteSpace(srcPath))
        throw new ArgumentException("SourcePath 不能為空白", nameof(srcPath));
    if (string.IsNullOrWhiteSpace(dstPath))
        throw new ArgumentException("DestPath 不能為空白", nameof(dstPath));

    if (!File.Exists(srcPath))
        throw new FileNotFoundException("Source not found", srcPath);

    var destDir = Path.GetDirectoryName(dstPath)
                ?? throw new InvalidOperationException($"DestPath 無法取得目錄：{dstPath}");

    Directory.CreateDirectory(destDir);

    // 來源大小（如果之後想比對可以用）
    long srcSize = 0;
    try
    {
        srcSize = new FileInfo(srcPath).Length;
    }
    catch
    {
        srcSize = 0;
    }

    bool success = false;   // ⭐用來判斷要不要刪 dst 檔

    try
    {
        // 來源：只讀，允許別人讀
        using var inFs = new FileStream(
            srcPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 1024 * 1024,
            useAsync: true);

        // 目的：直接寫到最後檔名，從一開始就 truncate / create
        using var outFs = new FileStream(
            dstPath,
            FileMode.Create,     // 有檔就清空，沒有就建立
            FileAccess.Write,
            FileShare.None,      // ❗整個 copy 過程禁止其他人開啟
            bufferSize: 1024 * 1024,
            useAsync: true);

        var buffer = new byte[1024 * 1024];
        int read;
        long sinceLastReport = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        while ((read = await inFs.ReadAsync(buffer.AsMemory(0, buffer.Length), ct)
                                 .ConfigureAwait(false)) > 0)
        {
            // 使用者取消 → 丟 OCE，外層會變成 999
            if (_cancelStore.ShouldCancel(historyId))
                throw new OperationCanceledException("Canceled by user");

            await outFs.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
            sinceLastReport += read;

            bool timeOk  = sw.ElapsedMilliseconds >= REPORT_INTERVAL_MS;
            bool bytesOk = sinceLastReport >= REPORT_BYTES_STEP;

            if (timeOk || bytesOk)
            {
                _progress.AddCopied(jobId, destId, sinceLastReport);
                sinceLastReport = 0;
                sw.Restart();
            }
        }

        if (sinceLastReport > 0)
            _progress.AddCopied(jobId, destId, sinceLastReport);

        await outFs.FlushAsync(ct).ConfigureAwait(false);

        // ⭐ 如果你想再嚴格一點，可以在這裡做 size 檢查：
        if (srcSize > 0 && outFs.Length != srcSize)
        {
            throw new IOException(
                $"Destination size mismatch: src={srcSize}, dst={outFs.Length}");
        }

        success = true;   // ✅ 走到這裡才算成功
    }
    finally
    {
        // ❗只要沒成功（例外 / cancel），就刪掉 dstPath，避免留半截檔
        if (!success)
        {
            try
            {
                if (File.Exists(dstPath))
                    File.Delete(dstPath);
            }
            catch
            {
                // 刪不掉就算了，至少我們有試
            }
        }
    }

    // ✅ 結果：
    // - 成功：目的端是完整新檔，舊檔被覆蓋
    // - 失敗 / 取消：目的端不會殘留修改到一半的檔案（我們會刪掉）
}





        // ===== Helpers =====

        private static string NormalizeDestPath(string srcPath, string destPath)
        {
            if (string.IsNullOrWhiteSpace(destPath))
                throw new ArgumentException("destPath 不能為空白", nameof(destPath));

            bool looksDir =
                Directory.Exists(destPath) ||
                destPath.EndsWith("\\", StringComparison.Ordinal) ||
                destPath.EndsWith("/",  StringComparison.Ordinal);

            if (looksDir)
            {
                var fileName = Path.GetFileName(srcPath);
                destPath = Path.Combine(destPath, fileName);
            }

            return destPath;
        }

        private static bool IsSharingOrLockViolation(IOException ex)
        {
            // 32: ERROR_SHARING_VIOLATION, 33: ERROR_LOCK_VIOLATION
            int code = ex.HResult & 0xFFFF;
            return code == 32 || code == 33;
        }

        private async Task RetryReplaceAsync(string tmpPath, string dstPath, CancellationToken ct, int maxRetries = 3)
        {
            var delay = TimeSpan.FromMilliseconds(200);

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    if (File.Exists(dstPath))
                    {
                        File.Replace(tmpPath, dstPath, destinationBackupFileName: null, ignoreMetadataErrors: true);
                    }
                    else
                    {
                        File.Move(tmpPath, dstPath);
                    }
                    return; // 成功
                }
                catch (IOException ex) when (IsSharingOrLockViolation(ex))
                {
                    // 目的檔被占用 → 指數退避重試
                    _logger.LogWarning("Replace/Move 重試 {Attempt}/{Max}：{Dst} 被佔用", attempt, maxRetries, dstPath);
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                    var next = Math.Min(delay.TotalMilliseconds * 1.8, 3000); // 上限 3 秒
                    delay = TimeSpan.FromMilliseconds(next);
                }
            }

            throw new IOException($"目的檔仍被佔用，無法取代：{dstPath}");
        }
    }
}
