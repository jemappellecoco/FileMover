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
    /// <summary>
    /// 2025-12 Slot-based 版本：
    /// - 所有任務（Phase1 搬移 / Phase2 回遷 / 刪除）都由 slot 去一筆一筆撿出來執行
    /// - slot 數量 = GlobalMaxConcurrentMoves
    /// - 優先順序：Phase2 ＞ Phase1 搬移 ＞ 刪除
    /// - 前端仍然可以看到所有 file_status = 0 / 1 / 24 / 27 / -1 的「排隊中 / 進行中」任務
    /// 
    /// ⚠ 依賴 HistoryRepository 另外實作：
    ///   Task<HistoryTask?> ClaimPhase2TopOneAsync(string? group, CancellationToken ct)
    ///   Task<HistoryTask?> ClaimCopyTopOneAsync(int retryMinutes, string? group, CancellationToken ct)
    ///   Task<HistoryTask?> ClaimDeleteTopOneAsync(int retryMinutes, string? group, CancellationToken ct)
    /// </summary>
    public sealed class HistoryWatchService : BackgroundService
    {
        private readonly ILogger<HistoryWatchService> _log;
        private readonly IServiceProvider _sp;
        private readonly IConfiguration _cfg;

        private const int MaxMoveAttempts = 3;
        private int _lastEnabledSlots = -1;
        private readonly IJobProgress _progress;
        private readonly IMoveRetryStore _retryStore;

        public HistoryWatchService(
            ILogger<HistoryWatchService> log,
            IServiceProvider sp,
            IConfiguration cfg,
            IJobProgress progress,
            IMoveRetryStore retryStore)
        {
            _log = log;
            _sp = sp;
            _cfg = cfg;
            _progress = progress;
            _retryStore = retryStore;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
            {
                var interval = _cfg.GetValue<int>("Watcher:IntervalSeconds", 5);
                var retryMin = _cfg.GetValue<int>("Watcher:RetryMinutes", 1);

                // ★ 樓層 group 由 appsettings... 決定
                var group = _cfg.GetValue<string>("FloorRouting:Group");

                if (string.IsNullOrWhiteSpace(group))
                    throw new InvalidOperationException("請在 appsettings 裡設定 FloorRouting:Group（例如 \"4F\" 或 \"7F\"）");

                // ★ RESTORE 改成「可無」，失敗不 throw
                int? restoreId = null;
                string? restorePath = null;

                using (var scope = _sp.CreateScope())
                {
                    var repo = scope.ServiceProvider.GetRequiredService<HistoryRepository>();
                    try
                    {
                        var id = await repo.GetRestoreStorageIdAsync(group, stoppingToken);
                        var path = await repo.GetStorageLocationAsync(id, stoppingToken);
                        restoreId = id;
                        restorePath = path;
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning(ex,
                            "Group={group} 尚未設定 RESTORE，跨層/回遷任務將無法處理（但服務仍會啟動）",
                            group);
                    }
                }

    // ⭐ slot 數量 = GlobalMaxConcurrentMoves
    // int slotCount = _cfg.GetValue<int>("GlobalMaxConcurrentMoves", 2);
    // ⭐ 初始 slot 數量：依照 GlobalMaxConcurrentMoves 來開
    const int MaxSlots = 10;   // 只是上限，用來 clamp

  
    int slotCount = MaxSlots;
    // 從配置讀取當前啟用的數量，僅用於 Log 輸出
    var initialEnabledSlots = _cfg.GetValue<int>("GlobalMaxConcurrentMoves", 2);
    if (initialEnabledSlots < 1) initialEnabledSlots = 1;
    if (initialEnabledSlots > MaxSlots) initialEnabledSlots = MaxSlots;
    _log.LogInformation(
        "HistoryWatchService starting: group={group}, RESTORE={restoreId}, initialSlots={slots}, interval={interval}s, retryMin={retryMin}",
        group,
        restoreId?.ToString() ?? "(none)",
        slotCount,
        interval,
        retryMin);

    var tasks = new List<Task>();

    // ⭐ 開 N 個 Slot — 注意 restoreId / restorePath 都是 nullable
    for (int i = 0; i < slotCount; i++)
    {
        var slotIndex = i;
        tasks.Add(Task.Run(
            () => SlotLoopAsync(slotIndex, group, retryMin, restoreId, restorePath, interval, stoppingToken),
            stoppingToken));
    }

    await Task.WhenAll(tasks);

    _log.LogInformation("HistoryWatchService stopped.");
}


        /// <summary>
        /// 單一 slot 的主迴圈：
        /// 依照優先順序：
        ///   1) Phase2 回遷任務（status = 24 / 27）
        ///   2) Phase1 / 同層 搬移任務（status = 0 or 1 且超過 RetryMinutes）
        ///   3) 刪除任務
        /// 只要有任務就一直做，三種都沒有就休息 interval 秒。
        /// </summary>
        private async Task SlotLoopAsync(
            int slotIndex,
            string group,
            int retryMin,
            int? restoreId,
            string? restorePath,
            int idleIntervalSeconds,
            CancellationToken ct)
        {
            _log.LogInformation("Slot #{slot} started.", slotIndex);

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    using var scope = _sp.CreateScope();
                    var repo  = scope.ServiceProvider.GetRequiredService<HistoryRepository>();
                    var mover = scope.ServiceProvider.GetRequiredService<MoveWorker>();
                    var enabledSlots = _cfg.GetValue<int>("GlobalMaxConcurrentMoves", 2);
                    if (enabledSlots < 1) enabledSlots = 1;
                    if (enabledSlots > 10) enabledSlots = 10;

                    // 只在「值改變」的那一瞬間印一次
                    if (enabledSlots != _lastEnabledSlots)
                    {
                        _lastEnabledSlots = enabledSlots;
                        _log.LogInformation("Concurrency changed: enabledSlots = {enabledSlots}", enabledSlots);
}
                    
                    // ① Phase2 回遷任務優先
                   // ⭐ slotIndex 超過可用並行數 → 此 slot 休息
                    if (slotIndex >= enabledSlots)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(idleIntervalSeconds), ct);
                        continue;
                    }
                   
                    var phase2Task = await repo.ClaimPhase2TopOneAsync(group, ct);
                    if (phase2Task != null)
                    {
                        _log.LogInformation("[Slot {slot}] Pick PHASE2 task #{hid}", slotIndex, phase2Task.HistoryId);
                        // ★ 沒有 RESTORE → 這筆無法處理，標為錯誤
                        if (!restoreId.HasValue || string.IsNullOrWhiteSpace(restorePath))
                        {
                            var msg = $"Group={group} 未設定 RESTORE，無法處理回遷任務（Phase2）。";
                            _log.LogError("[Slot {slot}] PHASE2 task #{hid} 無法處理：{msg}",
                                slotIndex, phase2Task.HistoryId, msg);

                            await repo.FailAsync(phase2Task.HistoryId, 903, msg, ct);
                            await Task.Delay(TimeSpan.FromMilliseconds(200), ct);
                            continue;
                        }

                        // ★ 有 RESTORE → 才能正常執行 Phase2
                        await RunSinglePhase2Async(
                            repo,
                            mover,
                            phase2Task,
                            restoreId.Value,   // <-- 用 Value
                            restorePath,       // <-- 字串本來就 nullable 可以直接傳
                            ct);
                        continue;
                    }

                    // ② 一般搬移（同層 / 跨層 Phase1）
                    var moveTask = await repo.ClaimCopyTopOneAsync(retryMin, group, ct);
                    if (moveTask != null)
                    {
                        _log.LogInformation("[Slot {slot}] Pick MOVE task #{hid} (pri={pri})", slotIndex, moveTask.HistoryId, moveTask.Priority);

                        bool sameFloor =
                            !string.IsNullOrEmpty(moveTask.ToGroup) &&
                            moveTask.FromGroup != null &&
                            moveTask.FromGroup.Equals(moveTask.ToGroup, StringComparison.OrdinalIgnoreCase);

                        if (sameFloor)
                        {
                            await RunSingleSameFloorAsync(repo, mover, moveTask, ct);
                        }
                        else
                        {
                            // ★ 如果沒有 RESTORE，跨樓層 Phase1 無法執行
                            if (!restoreId.HasValue || string.IsNullOrWhiteSpace(restorePath))
                            {
                                var msg = $"Group={group} 未設定 RESTORE，無法處理跨樓層搬移（Phase1）。";
                                _log.LogError("[Slot {slot}] MOVE task #{hid} 無法處理：{msg}",
                                    slotIndex, moveTask.HistoryId, msg);

                                await repo.FailAsync(moveTask.HistoryId, 903, msg, ct);
                                continue;
                            }

                            // ★ 有 RESTORE 才能跑 Phase1
                            await RunSingleCrossFloorPhase1Async(
                                repo,
                                mover,
                                moveTask,
                                group,
                                restoreId.Value,   //  ← 關鍵
                                restorePath,       //  ← 可直接傳
                                ct);
                        }
                        await Task.Delay(TimeSpan.FromMilliseconds(200), ct);
                        continue;
                    }

                    // ③ 刪除任務
                    var deleteTask = await repo.ClaimDeleteTopOneAsync(retryMin, group, ct);
                    if (deleteTask != null)
                    {
                        _log.LogInformation("[Slot {slot}] Pick DELETE task #{hid}", slotIndex, deleteTask.HistoryId);
                        await RunSingleDeleteAsync(repo, deleteTask, ct);
                        continue;
                    }

                    // 三種任務都沒有 → 稍微休息一下
                    await Task.Delay(TimeSpan.FromSeconds(idleIntervalSeconds), ct);
                }
                catch (OperationCanceledException)
                {
                    // app 關閉 / 取消
                    break;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Slot #{slot} loop error", slotIndex);
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(idleIntervalSeconds), ct);
                    }
                    catch { /* ignore */ }
                }
            }

            _log.LogInformation("Slot #{slot} stopped.", slotIndex);
        }

        #region 單筆任務執行（Phase1 / Phase2 / Delete）

        /// <summary>
        /// 同層搬移：來源與目標在同一個 group（舊的 A-1 部分，但改成單筆）
        /// </summary>
        private async Task RunSingleSameFloorAsync(
            HistoryRepository repo,
            MoveWorker mover,
            HistoryTask t,
            CancellationToken ct)
        {
            var jobId = Guid.NewGuid().ToString("N");

            var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
                ? $"{t.UserBit}.MXF"
                : t.FileName;

            var src = Path.Combine(t.FromPath, fileName);
            var dst = Path.Combine(t.ToPath, fileName);

            var req = new MoveBatchRequest
            {
                JobId = jobId,
                Items = new List<MoveItem>
                {
                    new MoveItem
                    {
                        HistoryId     = t.HistoryId,
                        FileId        = t.FileId,
                        FromStorageId = t.FromStorageId,
                        ToStorageId   = t.ToStorageId,
                        SourcePath    = src,
                        DestPath      = dst,
                        DestId        = $"TO-{t.HistoryId}"
                    }
                }
            };

            _log.LogInformation("[{hid}] SAME-FLOOR move from {from} to {to}", t.HistoryId, src, dst);

           
            await mover.RunAsync(
    req,
        onItemDone: async r =>
        {
            if (r.HistoryId == 0)
                return;

            if (r.Success)
            {
                await repo.CompleteAsync(r.HistoryId, ct);
                _log.LogInformation("[{hid}] Move success", r.HistoryId);
                _retryStore.Clear(r.HistoryId);
                return;
            }

            // ❌ 搬移失敗
            var code = r.StatusCode.HasValue
                ? r.StatusCode.Value
                : MapMoveErrorCode(r.Error);

            // 999 = 使用者取消 → 不再 retry，也不覆蓋狀態
            if (code == 999)
            {
                _log.LogInformation(
                    "[{hid}] Move canceled by user (code=999), no retry.",
                    r.HistoryId);

                _retryStore.Clear(r.HistoryId);
                return;
            }

        var failCount = _retryStore.IncrementFail(r.HistoryId, code, r.Error);

        if (failCount >= MaxMoveAttempts)
        {
            // 第三次：寫 9xx，放棄
            await repo.FailAsync(r.HistoryId, code, r.Error, ct);
            _log.LogWarning(
                "[{hid}] Move failed ({code}) {fail}/{max}, give up and mark error.",
                r.HistoryId, code, failCount, MaxMoveAttempts);
            _retryStore.Clear(r.HistoryId);
        }
        else
        {
            // 第 1、2 次：一樣寫 9xx，之後會被 ClaimCopyTopOneAsync 當「待重試」撿回來
            await repo.FailAsync(r.HistoryId, 800, r.Error, ct);
            _log.LogWarning(
                "[{hid}] Move failed ({code}) {fail}/{max}, will retry later.",
                r.HistoryId, code, failCount, MaxMoveAttempts);
        }
    },
    ct: ct);

        }

        /// <summary>
        /// 跨層 Phase1：先從來源搬到本層 RESTORE
        /// （舊的 A-2 部分，但改成單筆）
        /// </summary>
        private async Task RunSingleCrossFloorPhase1Async(
            HistoryRepository repo,
            MoveWorker mover,
            HistoryTask t,
            string group,
            int restoreId,
            string restorePath,
            CancellationToken ct)
        {
            var jobId = Guid.NewGuid().ToString("N");

            var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
                ? $"{t.UserBit}.MXF"
                : t.FileName;

            var src = Path.Combine(t.FromPath, fileName);
            var dst = Path.Combine(restorePath, fileName); // Phase1 目標：本層 RESTORE

            var req = new MoveBatchRequest
            {
                JobId = jobId,
                Items = new List<MoveItem>
                {
                    new MoveItem
                    {
                        HistoryId     = t.HistoryId,
                        FileId        = t.FileId,
                        FromStorageId = t.FromStorageId,
                        ToStorageId   = restoreId,
                        SourcePath    = src,
                        DestPath      = dst,
                        DestId        = $"TO-{t.HistoryId}"
                    }
                }
            };

            _log.LogInformation(
                "[{hid}] CROSS-FLOOR Phase1: {fromGroup} -> RESTORE({restoreId}), {src} -> {dst}",
                t.HistoryId, t.FromGroup, restoreId, src, dst);

          await mover.RunAsync(
    req,
    onItemDone: async r =>
    {
        if (r.HistoryId == 0)
            return;

        if (r.Success)
        {
            var statusCode = string.Equals(group, "4F", StringComparison.OrdinalIgnoreCase)
                ? 14
                : 17;

            await repo.MarkPhase1DoneAsync(r.HistoryId, statusCode, ct);

            _log.LogInformation(
                "[{hid}] CROSS-FLOOR Phase1 success: status={status}",
                r.HistoryId, statusCode);

            _retryStore.Clear(r.HistoryId);
            return;
        }

        // ❌ 失敗
        var code = r.StatusCode.HasValue
            ? r.StatusCode.Value
            : MapMoveErrorCode(r.Error);

        // 999 = 使用者取消 → 不 retry、不改 DB 狀態
        if (code == 999)
        {
            _log.LogInformation(
                "[{hid}] CROSS-FLOOR Phase1 canceled by user (code=999), no retry.",
                r.HistoryId);

            _retryStore.Clear(r.HistoryId);
            return;
        }

        var failCount = _retryStore.IncrementFail(r.HistoryId, code, r.Error);

        if (failCount >= MaxMoveAttempts)
        {
            // 最後一次：寫真正錯誤碼 9xx
            await repo.FailAsync(r.HistoryId, code, r.Error, ct);
            _log.LogWarning(
                "[{hid}] CROSS-FLOOR Phase1 failed ({code}) {fail}/{max}, give up and mark error.",
                r.HistoryId, code, failCount, MaxMoveAttempts);

            _retryStore.Clear(r.HistoryId);
        }
        else
        {
            // 第 1、2 次：寫 800，之後再撿回來 retry
            await repo.FailAsync(r.HistoryId, 800, r.Error, ct);
            _log.LogWarning(
                "[{hid}] CROSS-FLOOR Phase1 failed ({code}) {fail}/{max}, will retry later.",
                r.HistoryId, code, failCount, MaxMoveAttempts);
        }
    },
    ct: ct);


        }

        /// <summary>
        /// Phase2 回遷：從本層 RESTORE 搬到真正目的地
        /// （舊的 1-b 部分，但改成單筆）
        /// </summary>
        private async Task RunSinglePhase2Async(
            HistoryRepository repo,
            MoveWorker mover,
            HistoryTask t,
            int restoreId,
            string restorePath,
            CancellationToken ct)
        {
            var jobId = Guid.NewGuid().ToString("N");

            var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
                ? $"{t.UserBit}.MXF"
                : t.FileName;

            var src = Path.Combine(restorePath, fileName);      // Phase2 來源：RESTORE
            var dst = Path.Combine(t.ToPath, fileName);         // 目的地：真正 Storage

            var req = new MoveBatchRequest
            {
                JobId = jobId,
                Items = new List<MoveItem>
                {
                    new MoveItem
                    {
                        HistoryId     = t.HistoryId,
                        FileId        = t.FileId,
                        FromStorageId = restoreId,
                        ToStorageId   = t.ToStorageId ?? t.FromStorageId,
                        SourcePath    = src,
                        DestPath      = dst,
                        DestId        = $"TO-{t.HistoryId}"
                    }
                }
            };

            _log.LogInformation(
                "[{hid}] PHASE2 restore: RESTORE({restoreId}) -> {toPath}, {src} -> {dst}",
                t.HistoryId, restoreId, t.ToPath, src, dst);

            await mover.RunAsync(
            req,
            onItemDone: async r =>
            {
                if (r.HistoryId == 0)
                    return;

                if (r.Success)
                {
                    await repo.CompleteAsync(r.HistoryId, ct);
                    _log.LogInformation("[{hid}] PHASE2 restore success", r.HistoryId);
                    _retryStore.Clear(r.HistoryId);
                    return;
                }

                var code = r.StatusCode.HasValue
                    ? r.StatusCode.Value
                    : MapMoveErrorCode(r.Error);

                if (code == 999)
                {
                    _log.LogInformation(
                        "[{hid}] PHASE2 restore canceled by user (code=999), no retry.",
                        r.HistoryId);
                    _retryStore.Clear(r.HistoryId);
                    return;
                }

                var failCount = _retryStore.IncrementFail(r.HistoryId, code, r.Error);

                await repo.FailAsync(r.HistoryId, 800, r.Error, ct);

                if (failCount >= MaxMoveAttempts)
                {
                    _log.LogWarning(
                        "[{hid}] PHASE2 restore failed ({code}) {fail}/{max}, give up and mark error.",
                        r.HistoryId, code, failCount, MaxMoveAttempts);
                    _retryStore.Clear(r.HistoryId);
                }
                else
                {
                    _log.LogWarning(
                        "[{hid}] PHASE2 restore failed ({code}) {fail}/{max}, will retry later.",
                        r.HistoryId, code, failCount, MaxMoveAttempts);
                }
            },
                ct: ct);
        }

        /// <summary>
        /// 單筆刪除任務（保留你原本的 delete 進度條＆錯誤碼 921/922/923）
        /// </summary>
        private async Task RunSingleDeleteAsync(
            HistoryRepository repo,
            HistoryTask t,
            CancellationToken ct)
        {
            var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
                ? $"{t.UserBit}.MXF"
                : t.FileName;

            var src   = Path.Combine(t.FromPath, fileName);
            var jobId = t.HistoryId.ToString();

            try
            {
                if (!File.Exists(src))
                {
                    _log.LogWarning("[{hid}] Delete failed (921) File not found: {path}", t.HistoryId, src);
                    await repo.FailDeleteAsync(t.HistoryId, 921, "File not found when deleting", ct);
                    _progress.CompleteJob(jobId);
                    return;
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
                    total = 0;
                }

                if (total > 0)
                {
                    _progress.InitTotals(jobId, new Dictionary<string, long>
                    {
                        [jobId] = total
                    });
                }

                long reported = 0;
                byte[] buffer = new byte[1024 * 1024]; // 1MB

                if (total > 0)
                {
                    using (var fs = new FileStream(src, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        int read;
                        while ((read = await fs.ReadAsync(buffer.AsMemory(0, buffer.Length), ct)) > 0)
                        {
                            reported += read;
                            _progress.AddCopied(jobId, jobId, read);
                            await Task.Delay(10, ct);
                        }
                    }

                    if (reported < total)
                    {
                        _progress.AddCopied(jobId, jobId, total - reported);
                    }
                }

                File.Delete(src);
        _log.LogInformation("[{hid}] Delete success: {path}", t.HistoryId, src);

        await repo.CompleteDeleteAsync(t.HistoryId, ct);
        _retryStore.Clear(t.HistoryId);
    }
            catch (IOException ex)
            {
                // ⭐ 922: 檔案使用中 → 可重試
                int realCode = 922;
                var failCount = _retryStore.IncrementFail(t.HistoryId, realCode, ex.Message);

                if (failCount >= MaxMoveAttempts)
                {
                    // 最後一次：寫真正錯誤碼 922
                    _log.LogWarning(ex,
                        "[{hid}] Delete failed (922) {fail}/{max}, give up: {path}",
                        t.HistoryId, failCount, MaxMoveAttempts, src);

                    await repo.FailDeleteAsync(t.HistoryId, realCode, ex.Message, ct);
                    _retryStore.Clear(t.HistoryId);
                }
                else
                {
                    // 前幾次：寫 800，之後再撿回來 retry
                    _log.LogWarning(ex,
                        "[{hid}] Delete failed (922) {fail}/{max}, will retry later: {path}",
                        t.HistoryId, failCount, MaxMoveAttempts, src);

                    await repo.FailDeleteAsync(t.HistoryId, 800, ex.Message, ct);
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                // ⭐ 923: 權限/其他 → 也可以重試幾次（看你習慣）
                int realCode = 923;
                var failCount = _retryStore.IncrementFail(t.HistoryId, realCode, ex.Message);

                if (failCount >= MaxMoveAttempts)
                {
                    _log.LogWarning(ex,
                        "[{hid}] Delete failed (923) {fail}/{max}, give up: {path}",
                        t.HistoryId, failCount, MaxMoveAttempts, src);

                    await repo.FailDeleteAsync(t.HistoryId, realCode, ex.Message, ct);
                    _retryStore.Clear(t.HistoryId);
                }
                else
                {
                    _log.LogWarning(ex,
                        "[{hid}] Delete failed (923) {fail}/{max}, will retry later: {path}",
                        t.HistoryId, failCount, MaxMoveAttempts, src);

                    await repo.FailDeleteAsync(t.HistoryId, 800, ex.Message, ct);
                }
            }
            catch (Exception ex)
            {
                // 其他一律當 923 類型處理
                int realCode = 923;
                var failCount = _retryStore.IncrementFail(t.HistoryId, realCode, ex.Message);

                if (failCount >= MaxMoveAttempts)
                {
                    _log.LogWarning(ex,
                        "[{hid}] Delete failed (923-other) {fail}/{max}, give up: {path}",
                        t.HistoryId, failCount, MaxMoveAttempts, src);

                    await repo.FailDeleteAsync(t.HistoryId, realCode, ex.Message, ct);
                    _retryStore.Clear(t.HistoryId);
                }
                else
                {
                    _log.LogWarning(ex,
                        "[{hid}] Delete failed (923-other) {fail}/{max}, will retry later: {path}",
                        t.HistoryId, failCount, MaxMoveAttempts, src);

                    await repo.FailDeleteAsync(t.HistoryId, 800, ex.Message, ct);
                }
            }
            finally
            {
                // ⭐ 不管成功/失敗，這個 delete job 一定要收尾
                _progress.CompleteJob(jobId);
            }
        }

        #endregion

        /// <summary>
        /// 依錯誤訊息判斷搬移失敗狀態碼：911/912/913/914
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
