// HistoryWatchService.cs
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using FileMoverWeb.Models;
using FileMoverWeb.Services;
using System.Collections.Generic;
namespace FileMoverWeb.Services
{
    public sealed class HistoryWatchService : BackgroundService
    {
        private readonly ILogger<HistoryWatchService> _log;
        
        private readonly IServiceProvider _sp;
        private readonly IConfiguration _cfg;

        // ★ 記錄搬移失敗次數（key = HistoryId, value = 已失敗次數）
        private readonly Dictionary<int, int> _moveRetryCounter = new();

        // ★ 最多嘗試幾次（第一次 + 兩次 retry = 3 次）
        private const int MaxMoveAttempts = 3;
        private readonly IJobProgress _progress;
       public HistoryWatchService(
            ILogger<HistoryWatchService> log,
            IServiceProvider sp,
            IConfiguration cfg,
            IJobProgress progress)
        {
            _log = log;
            _sp = sp;
            _cfg = cfg;
            _progress = progress;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var interval = _cfg.GetValue<int>("Watcher:IntervalSeconds", 5);
            var batch = _cfg.GetValue<int>("Watcher:BatchSize", 20);
            var retryMin  = _cfg.GetValue<int>("Watcher:RetryMinutes", 5);
            _log.LogInformation("HistoryWatchService started: every {sec}s, batch {batch}", interval, batch);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var scope = _sp.CreateScope();
                    var repo = scope.ServiceProvider.GetRequiredService<HistoryRepository>();
                    var mover = scope.ServiceProvider.GetRequiredService<MoveWorker>();

                    // 1) 搬移任務（status = 0 → 1）
                    var moveTasks = await repo.ClaimAsync(batch, retryMin, stoppingToken);
                    Console.WriteLine($"[MOVE-CLAIM] {DateTime.Now:HH:mm:ss} claimed {moveTasks.Count} tasks: " +
                    string.Join(",", moveTasks.Select(t => t.HistoryId)));
                    if (moveTasks.Count > 0)
                    {
                        var jobId = Guid.NewGuid().ToString("N");
                        var req = new MoveBatchRequest
                        {
                            JobId = jobId,
                            Items = moveTasks.Select(t =>
                            {
                                var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
                                    ? $"{t.UserBit}.MXF"
                                    : t.FileName;

                                var src = Path.Combine(t.FromPath, fileName);
                                var dst = Path.Combine(t.ToPath, fileName);

                                return new MoveItem
                                    {
                                        HistoryId     = t.HistoryId,
                                        FileId        = t.FileId,
                                        FromStorageId = t.FromStorageId,
                                        ToStorageId   = t.ToStorageId,
                                        SourcePath    = src,
                                        DestPath      = dst,

                                        // ⭐ 用 ToStorageId 當成 group key，同一顆目的 Storage 共用一條進度
                                        // 例：ToStorageId = 3 → DestId = "STO-3"
                                        DestId = t.ToStorageId.HasValue
                                            ? $"TO-{t.ToStorageId.Value}"
                                            : $"TO-{t.FromStorageId}"   // 萬一 ToStorageId 是 null，就退回用 FromStorageId
                                    };
                            }).ToList()
                        };

                        _log.LogInformation("Start move batch {jobId} with {count} items", jobId, req.Items.Count);

                        var results = await mover.RunAsync(req, stoppingToken);
                        foreach (var r in results)
                        {
                            if (r.Success)
                            {
                                await repo.CompleteAsync(r.HistoryId, stoppingToken);
                                _log.LogInformation("[{hid}] Move success", r.HistoryId);
                                 // 成功就清掉 retry 紀錄（保險）
                                _moveRetryCounter.Remove(r.HistoryId);
                            }
                            else
                            {
                                // 先決定這次的錯誤碼（911/912/913/914...）
                                var code = r.StatusCode.HasValue
                                        ? r.StatusCode.Value
                                        : MapMoveErrorCode(r.Error);

                                // 讀取目前這個 HistoryId 已經失敗幾次
                                _moveRetryCounter.TryGetValue(r.HistoryId, out var failCount);
                                failCount++;
                                _moveRetryCounter[r.HistoryId] = failCount;

                                if (failCount >= MaxMoveAttempts)
                                {
                                    // 第 3 次（或以上）失敗：正式寫入那個錯誤碼，之後不再 retry
                                    await repo.FailAsync(r.HistoryId, code, r.Error, stoppingToken);
                                    _log.LogWarning(
                                        "[{hid}] Move failed ({code}) {fail}/{max}, give up and mark error.",
                                        r.HistoryId, code, failCount, MaxMoveAttempts);

                                    // 用完就把 counter 清掉，避免記憶體累積
                                    _moveRetryCounter.Remove(r.HistoryId);
                                }
                                else
                                {
                                    // 第 1、2 次失敗：只記錄 log，不呼叫 FailAsync，用 SQL 的 retryMin 再撿
                                    _log.LogWarning(
                                        "[{hid}] Move failed ({code}) {fail}/{max}, will retry later.",
                                        r.HistoryId, code, failCount, MaxMoveAttempts);

                                    // 不呼叫 FailAsync → DB 裡 file_status 仍然是 1
                                    // SQL 的 ClaimAsync 會在 RetryMinutes 之後再撿這筆回來
                                    //（如果你想順便記錯誤訊息到 DB，可以另外寫一個只更新 error_msg 的小 UPDATE，但你說先不要動 SQL，就先純程式控管）
                                }
                            }

                        }

                    }

                // 2) 刪除任務（status = -1 → 1）
           // 2) 刪除任務（status = -1 → 1）
var deleteTasks = await repo.ClaimDeleteAsync(batch, retryMin, stoppingToken);
Console.WriteLine($"[DEL-CLAIM]  {DateTime.Now:HH:mm:ss} claimed {deleteTasks.Count} tasks: " +
    string.Join(",", deleteTasks.Select(t => t.HistoryId)));

if (deleteTasks.Count > 0)
{
    _log.LogInformation("Start delete batch with {count} items", deleteTasks.Count);

    foreach (var t in deleteTasks)
    {
        // 先組檔名
        var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
            ? $"{t.UserBit}.MXF"
            : t.FileName;

        var src = Path.Combine(t.FromPath, fileName);

        // ⭐ 每一筆刪除也當成一個 progress job（jobId = historyId）
        var jobId = t.HistoryId.ToString();

        try
        {
            // 先確認檔案是否存在
            if (!File.Exists(src))
            {
                _log.LogWarning("[{hid}] Delete failed (921: not found): {path}", t.HistoryId, src);
                await repo.FailDeleteAsync(t.HistoryId, 921, "File not found when deleting", stoppingToken);
                _progress.CompleteJob(jobId);
                continue;
            }

            long total = 0;
            try
            {
                var fi = new FileInfo(src);
                total = fi.Length;
            }
            catch (Exception exSize)
            {
                _log.LogWarning(exSize, "[{hid}] Get file size for delete failed: {path}", t.HistoryId, src);
                total = 0; // 抓不到大小就當 0，不影響刪除本身
            }

            // 如果抓得到大小，就用「真實檔案大小」初始化進度
            if (total > 0)
            {
                _progress.InitTotals(jobId, new Dictionary<string, long>
                {
                    [jobId] = total
                });
            }

            // ⭐ 讀取檔案並回報進度（像 copy 一樣 read bytes）
            long reported = 0;
            byte[] buffer = new byte[1024 * 1024]; // 1MB buffer

            if (total > 0)
            {
                using (var fs = new FileStream(src, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    int read;
                    while ((read = await fs.ReadAsync(buffer.AsMemory(0, buffer.Length), stoppingToken)) > 0)
                    {
                        reported += read;
                        _progress.AddCopied(jobId, jobId, read);   // destId = jobId = HistoryId
                        await Task.Delay(10, stoppingToken);
                    }
                }

                if (reported < total)
                {
                    _progress.AddCopied(jobId, jobId, total - reported);
                }
            }

            File.Delete(src);
            _log.LogInformation("[{hid}] Deleted file: {path}", t.HistoryId, src);

            _progress.CompleteJob(jobId);
            await repo.CompleteDeleteAsync(t.HistoryId, stoppingToken); // 12
        }
        catch (IOException ex)
        {
            _log.LogWarning(ex, "[{hid}] Delete failed (922: in use): {path}", t.HistoryId, src);
            await repo.FailDeleteAsync(t.HistoryId, 922, ex.Message, stoppingToken);
            _progress.CompleteJob(jobId);
        }
        catch (UnauthorizedAccessException ex)
        {
            _log.LogWarning(ex, "[{hid}] Delete failed (923: access denied): {path}", t.HistoryId, src);
            await repo.FailDeleteAsync(t.HistoryId, 923, ex.Message, stoppingToken);
            _progress.CompleteJob(jobId);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "[{hid}] Delete failed (923: other): {path}", t.HistoryId, src);
            await repo.FailDeleteAsync(t.HistoryId, 923, ex.Message, stoppingToken);
            _progress.CompleteJob(jobId);
        }
    }
}


                    // 若本輪兩種任務都沒有，就睡一下
                    if (moveTasks.Count == 0 && deleteTasks.Count == 0)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    // app 關閉 / 取消
                    break;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Watcher loop error");
                    // 保持節流，避免狂刷錯
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);
                    }
                    catch { /* ignore */ }
                }
            }

            _log.LogInformation("HistoryWatchService stopped.");
        }

        /// <summary>
        /// 依錯誤訊息判斷搬移失敗狀態碼：
        /// 911/912/913/914
        /// </summary>
        private static int MapMoveErrorCode(string? error)
        {
            var msg = (error ?? string.Empty).ToLowerInvariant();

            // 911: 找不到來源 / 檔案不存在
            if (msg.Contains("could not find file") || msg.Contains("does not exist") || msg.Contains("找不到")|| msg.Contains("source not found"))
                return 911;

            // 912: 檔案使用中 (sharing violation / being used by another process)
            if (msg.Contains("being used by another process") || msg.Contains("sharing violation"))
                return 912;

            // 914: 找不到目的地 (路徑不存在)
            if (msg.Contains("could not find a part of the path") || msg.Contains("path not found") || msg.Contains("找不到路徑"))
                return 914;

            // 913: 權限不足 or 其他錯誤
            if (msg.Contains("access is denied") || msg.Contains("未經授權") || msg.Contains("unauthorized"))
                return 913;

            // 預設也歸類成 913
            return 913;
        }
    }
}
