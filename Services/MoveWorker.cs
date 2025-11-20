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

namespace FileMoverWeb.Services
{
    /// <summary>
    /// 實際執行檔案搬運的工作器。
    /// 特色：
    /// - 依 DestId 分組，限制同時併發的目標數量，降低同一顆磁碟 I/O 爭用
    /// - 大檔分段複製，節流回報進度（時間或位元組累積到門檻就回報一次）
    /// - 目的端以暫存檔寫入，最後以 Replace/Move 原子取代，並對 sharing/lock 過程做退避重試
    /// - 回傳每筆結果（HistoryId、Success、Error）
    /// </summary>
    public sealed class MoveWorker
    {
        private readonly IJobProgress _progress;
        private readonly ILogger<MoveWorker> _logger;

        // ===== 調整參數（視環境可微調） =====
        private const int REPORT_INTERVAL_MS = 300;               // 至少每 300ms 回報一次
        private const long REPORT_BYTES_STEP = 4L * 1024 * 1024;  // 或每累積 ≥ 4 MB 回報
        private const int MAX_PARALLEL_DEST = 4;                  // 同時最多幾個 DestId 併發

        private readonly SemaphoreSlim _destLimiter = new SemaphoreSlim(MAX_PARALLEL_DEST);

        public MoveWorker(IJobProgress progress, ILogger<MoveWorker> logger)
        {
            _progress = progress;
            _logger = logger;
        }

        /// <summary>
        /// 執行一個搬運批次，回傳每筆結果。
        /// </summary>
        public async Task<List<MoveResult>> RunAsync(MoveBatchRequest req, CancellationToken ct = default)
        {
            if (req is null) throw new ArgumentNullException(nameof(req));
            if (req.Items is null || req.Items.Count == 0) return new List<MoveResult>(0);

            // 新任務開始：清掉前一輪快照，避免 UI 還停在 100%
            // _progress.CompleteJob(req.JobId);

            // 1) 預估總量（按 DestId 彙總），避免中途才知道總大小
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
 

            // 2) 依 DestId 分組處理（同一 Dest 串行，不同 Dest 受限度並行）
            var destGroups = req.Items
                .GroupBy(i => i.DestId, StringComparer.OrdinalIgnoreCase)
                .Select(g => (destId: g.Key, items: g.ToList()))
                .ToList();

            var results = new ConcurrentBag<MoveResult>();
            var tasks = destGroups.Select(async g =>
            {
                await _destLimiter.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    await MoveGroupAsync(req.JobId, g.destId, g.items, results, ct).ConfigureAwait(false);
                }
                finally
                {
                    _destLimiter.Release();
                }
            });

            await Task.WhenAll(tasks).ConfigureAwait(false);

            // 視需求決定是否在全部完成後清空快照：
            // _progress.CompleteJob(req.JobId);
            // _progress.CompleteJob(req.JobId);
            return results.ToList();
        }

       /// <summary>
        /// 同一個 DestId 共用一個 progress job
        /// </summary>
private async Task MoveGroupAsync(
    string jobId, string destId, List<MoveItem> items,
    ConcurrentBag<MoveResult> results, CancellationToken ct)
{
    foreach (var item in items)
    {
        ct.ThrowIfCancellationRequested();
        Console.WriteLine($"[MOVE] job={jobId}, historyId={item.HistoryId}, src={item.SourcePath}");
        try
        {
            // 路徑拼不出來（多半是沒 FileData / 沒 UserBit） → 911
            if (string.IsNullOrWhiteSpace(item.SourcePath))
            {
                _logger.LogWarning(
                    "[{Job}] Source path empty (HistoryId={HistoryId})，多半是缺 FileData/UserBit。",
                    jobId, item.HistoryId);

                results.Add(new MoveResult
                {
                    HistoryId  = item.HistoryId ?? 0,
                    Success    = false,
                    StatusCode = 911,
                    Error      = "Source path empty (no FileData/UserBit)"
                });
                continue;
            }

            // 911：來源不存在
            if (!File.Exists(item.SourcePath))
            {
                _logger.LogWarning("[{Job}] Source not found: {Src}", jobId, item.SourcePath);
                results.Add(new MoveResult
                {
                    HistoryId  = item.HistoryId ?? 0,
                    Success    = false,
                    StatusCode = 911,
                    Error      = $"Source not found: {item.SourcePath}"
                });
                continue;
            }

            // 正常搬移流程
            var dstPath = NormalizeDestPath(item.SourcePath, item.DestPath);

            // ⭐ 重點：這裡用「同一個 jobId (整批) + 同一個 destId (STO-x)」
            //    不再用 HistoryId 當 jobId/destId
            await CopyFileAsync(jobId, destId, item.SourcePath, dstPath, ct).ConfigureAwait(false);

            results.Add(new MoveResult
            {
                HistoryId  = item.HistoryId ?? 0,
                Success    = true,
                StatusCode = 11,
                Error      = null
            });
        }
        catch (IOException ex) when (IsSharingOrLockViolation(ex))   // 912
        {
            _logger.LogWarning(ex, "[{Job}] 檔案使用中（搬移失敗）：{Src}", jobId, item.SourcePath);
            results.Add(new MoveResult
            {
                HistoryId  = item.HistoryId ?? 0,
                Success    = false,
                StatusCode = 912,
                Error      = ex.Message
            });
        }
        catch (DirectoryNotFoundException ex)                       // 914
        {
            _logger.LogWarning(ex, "[{Job}] 目的地路徑不存在（搬移失敗）：{Src}", jobId, item.SourcePath);
            results.Add(new MoveResult
            {
                HistoryId  = item.HistoryId ?? 0,
                Success    = false,
                StatusCode = 914,
                Error      = ex.Message
            });
        }
        catch (UnauthorizedAccessException ex)                       // 913
        {
            _logger.LogWarning(ex, "[{Job}] 權限不足（搬移失敗）：{Src}", jobId, item.SourcePath);
            results.Add(new MoveResult
            {
                HistoryId  = item.HistoryId ?? 0,
                Success    = false,
                StatusCode = 913,
                Error      = ex.Message
            });
        }
        catch (Exception ex)                                        // 91
        {
            _logger.LogError(ex, "[{Job}] 搬運失敗：{Src}", jobId, item.SourcePath);
            results.Add(new MoveResult
            {
                HistoryId  = item.HistoryId ?? 0,
                Success    = false,
                StatusCode = 91,
                Error      = ex.Message
            });
        }
    }
}





        /// <summary>
        /// 將單一檔案以暫存檔寫入 → 最後 Replace/Move 成目的檔（含進度回報、重試退避）。
        /// </summary>
        private async Task CopyFileAsync(string jobId, string destId, string srcPath, string dstPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(srcPath))
                throw new ArgumentException("SourcePath 不能為空白", nameof(srcPath));
            if (string.IsNullOrWhiteSpace(dstPath))
                throw new ArgumentException("DestPath 不能為空白", nameof(dstPath));

            var destDir = Path.GetDirectoryName(dstPath)
                        ?? throw new InvalidOperationException($"DestPath 無法取得目錄：{dstPath}");

            // 暫存檔放在目的目錄（避免跨磁碟 move）
            var tmpPath = Path.Combine(destDir, $".~{Path.GetFileName(dstPath)}.{Guid.NewGuid():N}.part");

            Directory.CreateDirectory(destDir);

            long srcSize = 0;                  // ★ 記下來源大小，等等核對
            try
            {
                using var inFs = new FileStream(
                srcPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 1024 * 1024,
                useAsync: true);

            srcSize = inFs.Length;         // ★ 讀取來源檔大小

            // ⭐ 這裡把「這個 HistoryId 的 Job」初始化 total bytes
            // jobId = 呼叫者傳進來的 progId（HistoryId 字串）
            // destId = 也是 progId（前端就可以用 historyId 當 data-to）
            // _progress.InitTotals(jobId, new Dictionary<string, long>
            // {
            //     [destId] = srcSize
            // });

            using var outFs = new FileStream(
                tmpPath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.Read,
                bufferSize: 1024 * 1024,
                useAsync: true);



                var buffer = new byte[1024 * 1024];
                int read;
                long sinceLastReport = 0;
                var sw = System.Diagnostics.Stopwatch.StartNew();

                while ((read = await inFs.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false)) > 0)
                {
                    await outFs.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                    sinceLastReport += read;

                    bool timeOk = sw.ElapsedMilliseconds >= REPORT_INTERVAL_MS;
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
            }
            catch
            {
                // 寫入失敗時往外丟，finally 會清除暫存檔
                throw;
            }
            finally
            {
                // outFs 會先被 using 處置，再進行 Replace/Move
            }

            // 原子取代（若目的檔被佔用則退避重試）
            await RetryReplaceAsync(tmpPath, dstPath, ct).ConfigureAwait(false);

            // 清理殘留暫存檔（正常情況下 Replace/Move 後應不存在）
            try
            {
                if (File.Exists(tmpPath))
                    File.Delete(tmpPath);
            }
            catch { /* 忽略清理失敗 */ }

            // // ★ 搬完後刪除來源：先核對目的檔大小一致再刪
            // try
            // {
            //     var dstInfo = new FileInfo(dstPath);
            //     if (dstInfo.Exists && dstInfo.Length == srcSize)
            //     {
            //         File.Delete(srcPath);      // ✅ 確認成功才刪來源
            //     }
            //     // 若大小不一致就保留來源，交由重試/人工處理
            // }
            // catch
            // {
            //     // 刪除來源失敗不影響搬運成果（例如權限/鎖定），安全忽略或之後再處理
            // }
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

        private async Task RetryReplaceAsync(string tmpPath, string dstPath, CancellationToken ct, int maxRetries = 10)
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
