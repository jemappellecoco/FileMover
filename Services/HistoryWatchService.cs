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
using System.Collections.Concurrent;
namespace FileMoverWeb.Services
{
    public sealed class HistoryWatchService : BackgroundService
    {
        private readonly ILogger<HistoryWatchService> _log;
        private readonly IServiceProvider _sp;
        private readonly IConfiguration _cfg;

        // ★ 記錄搬移失敗次數（key = HistoryId, value = 已失敗次數）
        private readonly ConcurrentDictionary<int, int> _moveRetryCounter = new();

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
            var batch    = _cfg.GetValue<int>("Watcher:BatchSize", 20);
            var retryMin = _cfg.GetValue<int>("Watcher:RetryMinutes", 5);

            // ★ 樓層 group 由 appsettings.XXX.json 決定
            var group = _cfg.GetValue<string>("FloorRouting:Group");

            _log.LogInformation("HistoryWatchService starting: every {sec}s, batch {batch}", interval, batch);

            if (string.IsNullOrWhiteSpace(group))
            {
                throw new InvalidOperationException(
                    "請在 appsettings 裡設定 FloorRouting:Group（例如：\"4F\" 或 \"7F\"）");
            }

            // ★ 先從 DB 找到本樓層的 RESTORE StorageId（之後跨層兩階段會用）
            int restoreId;
            string restorePath;
            using (var scope = _sp.CreateScope())
            {
                var repo = scope.ServiceProvider.GetRequiredService<HistoryRepository>();
                restoreId = await repo.GetRestoreStorageIdAsync(group, stoppingToken);
                restorePath = await repo.GetStorageLocationAsync(restoreId, stoppingToken);
            }
            

            _log.LogInformation(
                "HistoryWatchService started for group {group}, RESTORE = {restoreId}",
                group, restoreId);

            // ==========================
            //       主要輪詢迴圈
            // ==========================
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var scope = _sp.CreateScope();
                    var repo  = scope.ServiceProvider.GetRequiredService<HistoryRepository>();
                    var mover = scope.ServiceProvider.GetRequiredService<MoveWorker>();

                    // 1) 搬移任務（status = 0 → 1）
                    var moveTasks = await repo.ClaimAsync(batch, retryMin, group, stoppingToken);

                    Console.WriteLine(
                            $"[MOVE-CLAIM] {DateTime.Now:HH:mm:ss} claimed {moveTasks.Count} tasks: " +
                            string.Join(",", moveTasks.Select(t => $"{t.HistoryId}({t.FromGroup}->{t.ToGroup})")));
                    if (moveTasks.Count > 0)
                    {
                        // ==========================
                        // A. 先拆成「同層 / 跨層」
                        // ==========================
                        var sameFloorTasks = moveTasks
                            .Where(t =>
                                !string.IsNullOrEmpty(t.ToGroup) &&
                                t.FromGroup != null &&
                                t.FromGroup.Equals(t.ToGroup, StringComparison.OrdinalIgnoreCase))
                            .ToList();

                        var crossFloorTasks = moveTasks
                            .Where(t =>
                                string.IsNullOrEmpty(t.ToGroup) ||
                                t.FromGroup == null ||
                                !t.FromGroup.Equals(t.ToGroup, StringComparison.OrdinalIgnoreCase))
                            .ToList();

                        // =======================
                        // A-1. 同層任務：照原本邏輯搬
                        // =======================
                        if (sameFloorTasks.Count > 0)
                        {
                            var jobId = Guid.NewGuid().ToString("N");

                            var req = new MoveBatchRequest
                            {
                                JobId = jobId,
                                Items = sameFloorTasks.Select(t =>
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

                            _log.LogInformation("Start SAME-FLOOR batch {jobId} with {count} items", jobId, req.Items.Count);

                            var results = await mover.RunAsync(
                                req,
                                onItemDone: async r =>
                                {
                                    // 沒 HistoryId 就不用寫 DB（理論上不會）
                                    if (r.HistoryId == 0)
                                        return;

                                    if (r.Success)
                                    {
                                        // ✅ 搬完這一筆就立刻標成功 (11)
                                        await repo.CompleteAsync(r.HistoryId, stoppingToken);
                                        _log.LogInformation("[{hid}] Move success", r.HistoryId);

                                        // 成功就清掉 retry 紀錄
                                        _moveRetryCounter.TryRemove(r.HistoryId, out _);
                                    }
                                    else
                                    {
                                        // 先決定錯誤碼（911/912/913/914...）
                                        var code = r.StatusCode.HasValue
                                            ? r.StatusCode.Value
                                            : MapMoveErrorCode(r.Error);

                                        // 用 ConcurrentDictionary 累加失敗次數
                                        var failCount = _moveRetryCounter.AddOrUpdate(
                                            r.HistoryId,
                                            1,
                                            (_, current) => current + 1);

                                        if (failCount >= MaxMoveAttempts)
                                        {
                                            // 第 3 次（或以上）失敗：正式寫入錯誤碼
                                            await repo.FailAsync(r.HistoryId, code, r.Error, stoppingToken);
                                            _log.LogWarning(
                                                "[{hid}] Move failed ({code}) {fail}/{max}, give up and mark error.",
                                                r.HistoryId, code, failCount, MaxMoveAttempts);

                                            _moveRetryCounter.TryRemove(r.HistoryId, out _);
                                        }
                                        else
                                        {
                                            // 第 1、2 次：只記次數 + log，讓 SQL RetryMinutes 過後再撿
                                            _log.LogWarning(
                                                "[{hid}] Move failed ({code}) {fail}/{max}, will retry later.",
                                                r.HistoryId, code, failCount, MaxMoveAttempts);
                                        }
                                    }
                                },
                                ct: stoppingToken);



                        }

                        // =======================
                    // A-2. 跨層任務：Phase 1 先搬到本層 RESTORE
                    // =======================
                        if (crossFloorTasks.Count > 0)
                        {
                            _log.LogInformation(
                                "Start CROSS-FLOOR Phase1 batch: {count} items -> RESTORE({restoreId})",
                                crossFloorTasks.Count, restoreId);

                            // 讓所有跨層任務共用同一個 RESTORE 進度條
                            var crossJobId = Guid.NewGuid().ToString("N");

                            var crossReq = new MoveBatchRequest
                            {
                                JobId = crossJobId,
                                Items = crossFloorTasks.Select(t =>
                                {
                                    var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
                                        ? $"{t.UserBit}.MXF"
                                        : t.FileName;

                                    var src = Path.Combine(t.FromPath, fileName);
                                    // ⭐ Phase1：目的地改成「本層 RESTORE 路徑」
                                    var dst = Path.Combine(restorePath, fileName);

                                    return new MoveItem
                                    {
                                        HistoryId     = t.HistoryId,
                                        FileId        = t.FileId,
                                        FromStorageId = t.FromStorageId,
                                        ToStorageId   = restoreId,  // 先寫成 RESTORE
                                        SourcePath    = src,
                                        DestPath      = dst,

                                        // RESTORE 共用一條進度
                                            DestId = t.ToStorageId.HasValue
                                                ? $"TO-{t.ToStorageId.Value}"
                                                : $"TO-{t.FromStorageId}"   // 防禦性 fallback

                                    };
                                }).ToList()
                            };

                            var crossResults = await mover.RunAsync(
                                crossReq,
                                onItemDone: async r =>
                                {
                                    var t = crossFloorTasks.FirstOrDefault(x => x.HistoryId == r.HistoryId);
                                    if (t == null)
                                    {
                                        _log.LogWarning("[CROSS-FLOOR] result HistoryId={hid} 找不到對應 task", r.HistoryId);
                                        return;
                                    }

                                    if (r.Success)
                                    {
                                        // Phase1 成功：4F → 14，7F → 17
                                        var statusCode = string.Equals(group, "4F", StringComparison.OrdinalIgnoreCase)
                                            ? 14
                                            : 17;

                                        await repo.MarkPhase1DoneAsync(
                                            r.HistoryId,
                                            statusCode,
                                            stoppingToken);

                                        _log.LogInformation(
                                            "[{hid}] CROSS-FLOOR Phase1 success: From {fromGroup} -> RESTORE({restoreId}), status={status}",
                                            r.HistoryId, t.FromGroup, restoreId, statusCode);

                                        _moveRetryCounter.TryRemove(r.HistoryId, out _);
                                    }
                                    else
                                    {
                                        var code = r.StatusCode.HasValue
                                            ? r.StatusCode.Value
                                            : MapMoveErrorCode(r.Error);

                                        var failCount = _moveRetryCounter.AddOrUpdate(
                                            r.HistoryId,
                                            1,
                                            (_, current) => current + 1);

                                        if (failCount >= MaxMoveAttempts)
                                        {
                                            await repo.FailAsync(r.HistoryId, code, r.Error, stoppingToken);
                                            _log.LogWarning(
                                                "[{hid}] CROSS-FLOOR Phase1 failed ({code}) {fail}/{max}, give up and mark error.",
                                                r.HistoryId, code, failCount, MaxMoveAttempts);

                                            _moveRetryCounter.TryRemove(r.HistoryId, out _);
                                        }
                                        else
                                        {
                                            _log.LogWarning(
                                                "[{hid}] CROSS-FLOOR Phase1 failed ({code}) {fail}/{max}, will retry later.",
                                                r.HistoryId, code, failCount, MaxMoveAttempts);
                                        }
                                    }
                                },
                                ct: stoppingToken);
                        
                        }
                        
                    }
                    // 1-b) Phase2 回遷任務（status = 24 / 27）
                    var restoreTasks = await repo.ClaimPhase2Async(batch, retryMin, group, stoppingToken);

                    if (restoreTasks.Count > 0)
                    {
                        _log.LogInformation(
                            "Start PHASE2 RESTORE batch for group {group}: {count} items from RESTORE({restoreId})",
                            group, restoreTasks.Count, restoreId);

                        var jobId = Guid.NewGuid().ToString("N");

                        var req = new MoveBatchRequest
                        {
                            JobId = jobId,
                            Items = restoreTasks.Select(t =>
                            {
                                var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
                                    ? $"{t.UserBit}.MXF"
                                    : t.FileName;

                                // ⭐ Phase2：來源一律改用本樓層 RESTORE 路徑
                                var src = Path.Combine(restorePath, fileName);
                                // ⭐ 目的地用原本歷史紀錄的 ToPath（真正目的 Storage）
                                var dst = Path.Combine(t.ToPath, fileName);

                                return new MoveItem
                                {
                                    HistoryId     = t.HistoryId,
                                    FileId        = t.FileId,
                                    FromStorageId = restoreId,                      // Phase2 視為從 RESTORE 出發
                                    ToStorageId   = t.ToStorageId ?? t.FromStorageId,
                                    SourcePath    = src,
                                    DestPath      = dst,
                                    DestId        = t.ToStorageId.HasValue
                                        ? $"TO-{t.ToStorageId.Value}"               // 給進度條用
                                        : $"TO-{restoreId}"
                                };
                            }).ToList()
                        };

                        var results = await mover.RunAsync(
                            req,
                            onItemDone: async r =>
                            {
                                if (r.HistoryId == 0)
                                    return;

                                if (r.Success)
                                {
                                    // Phase2 成功：直接標 11（整個跨樓層完成）
                                    await repo.CompleteAsync(r.HistoryId, stoppingToken);
                                    _log.LogInformation("[{hid}] PHASE2 restore success", r.HistoryId);
                                    _moveRetryCounter.TryRemove(r.HistoryId, out _);
                                }
                                else
                                {
                                    var code = r.StatusCode.HasValue
                                        ? r.StatusCode.Value
                                        : MapMoveErrorCode(r.Error);

                                    var failCount = _moveRetryCounter.AddOrUpdate(
                                        r.HistoryId,
                                        1,
                                        (_, current) => current + 1);

                                    if (failCount >= MaxMoveAttempts)
                                    {
                                        await repo.FailAsync(r.HistoryId, code, r.Error, stoppingToken);
                                        _log.LogWarning(
                                            "[{hid}] PHASE2 restore failed ({code}) {fail}/{max}, give up and mark error.",
                                            r.HistoryId, code, failCount, MaxMoveAttempts);
                                        _moveRetryCounter.TryRemove(r.HistoryId, out _);
                                    }
                                    else
                                    {
                                        _log.LogWarning(
                                            "[{hid}] PHASE2 restore failed ({code}) {fail}/{max}, will retry later.",
                                            r.HistoryId, code, failCount, MaxMoveAttempts);
                                    }
                                }
                            },
                            ct: stoppingToken);
                    }


                    // 2) 刪除任務（status = -1 → 1）
                    var deleteTasks = await repo.ClaimDeleteAsync(batch, retryMin, group, stoppingToken);
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

                            var src   = Path.Combine(t.FromPath, fileName);
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
            if (msg.Contains("could not find file") || msg.Contains("does not exist") || msg.Contains("找不到") || msg.Contains("source not found"))
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
