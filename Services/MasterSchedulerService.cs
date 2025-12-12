using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FileMoverWeb.Services
{
    public sealed class MasterSchedulerService : BackgroundService
    {
        private readonly DbConnectionFactory _factory;
        private readonly IConfiguration _cfg;
        private readonly ILogger<MasterSchedulerService> _log;

        public MasterSchedulerService(
            DbConnectionFactory factory,
            IConfiguration cfg, 
            ILogger<MasterSchedulerService> log)
        {
            _factory = factory;
            _cfg = cfg;
            _log   = log;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var role  = _cfg["Cluster:Role"] ?? "Slave";
            var group = _cfg["Cluster:Group"] ?? "";

            // 只讓 Master 跑，保險一下
            if (!string.Equals(role, "Master", StringComparison.OrdinalIgnoreCase))
            {
                _log.LogInformation("MasterSchedulerService not started because Role={role}", role);
                return;
            }

            var intervalSec = _cfg.GetValue<int>("Cluster:ScheduleIntervalSeconds", 2);
            if (intervalSec < 1) intervalSec = 1;

            _log.LogInformation("MasterSchedulerService started, group={group}, interval={interval}s",
                group, intervalSec);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await RunOnceAsync(group, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "MasterSchedulerService loop error");
                }

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(intervalSec), stoppingToken);
                }
                catch { }
            }

            _log.LogInformation("MasterSchedulerService stopped.");
        }

        private sealed class WorkerRow
        {
            public string NodeName { get; set; } = "";
            public string GroupCode { get; set; } = "";
            public int MaxConcurrency { get; set; }
            public DateTime LastHeartbeat { get; set; }
        }
        private sealed class SlotState
        {
            public string NodeName { get; set; } = "";
            public int Running { get; set; }
            public int Capacity { get; set; }
        }
        private async Task RunOnceAsync(string group, CancellationToken ct)
        {
            using var conn = _factory.Create();
            var db = (DbConnection)conn;
            await db.OpenAsync(ct);
            // 🔍 DEBUG：檢查實際連到哪顆 DB、FileData_History 有沒有 assigned_node
    // try
    // {
    //     var dbName = await conn.ExecuteScalarAsync<string>("SELECT DB_NAME()");
    //     _log.LogInformation("[DEBUG] MasterScheduler DB = {DbName}", dbName);

    //     var colCount = await conn.ExecuteScalarAsync<int>(@"
    //         SELECT COUNT(*)
    //         FROM INFORMATION_SCHEMA.COLUMNS
    //         WHERE TABLE_NAME = 'FileData_History'
    //           AND COLUMN_NAME = 'assigned_node';
    //     ");
    //     _log.LogInformation("[DEBUG] FileData_History.assigned_node exists? {ColCount}", colCount);
    // }
    // catch (Exception ex)
    // {
    //     _log.LogError(ex, "[DEBUG] check DB schema failed");
    // }

            // 1) 抓目前 Online 的 worker（同一個 group）
            var timeoutSec = _cfg.GetValue<int>("Cluster:HeartbeatTimeoutSeconds", 30);

            var workers = (await conn.QueryAsync<WorkerRow>(@"
SELECT NodeName, GroupCode, MaxConcurrency, LastHeartbeat
FROM dbo.WorkerNode
WHERE GroupCode = @GroupCode;
", new { GroupCode = group }))
            .Where(w => (DateTime.Now - w.LastHeartbeat).TotalSeconds <= timeoutSec)
            .ToList();

            if (workers.Count == 0)
                return;

            // 2) 算各節點目前已分配多少 active 任務
           var runningDict = (await conn.QueryAsync<(string NodeName, int RunningCount)>(@"
SELECT assigned_node AS NodeName, COUNT(*) AS RunningCount
FROM dbo.FileData_History
WHERE assigned_node IS NOT NULL
  AND file_status = 1          -- ⭐ 唯一佔用實際並行的狀態
GROUP BY assigned_node;
"))
.ToDictionary(x => x.NodeName, x => x.RunningCount);

           var slots = workers
            .Select(w =>
            {
                runningDict.TryGetValue(w.NodeName, out var running);
                var cap = w.MaxConcurrency - running;
                if (cap < 0) cap = 0;

                return new SlotState
                {
                    NodeName = w.NodeName,
                    Running  = running,
                    Capacity = cap
                };
            })
            .Where(x => x.Capacity > 0)
            .ToList();

            if (slots.Count == 0)
                return;

            // 3) 依照「目前 Running 最少」優先分配
            while (slots.Any())
            {
                var target = slots.OrderBy(s => s.Running).First();

                // 嘗試分配一筆任務給這個節點
                var assigned = await AssignOneTaskAsync(conn, target.NodeName, group, ct);
                if (!assigned)
                    break;  // 沒未分配任務了

                target.Running++;
                
                target.Capacity--;

                slots = slots
                    .Select(s => s.NodeName == target.NodeName ? target : s)
                    .Where(s => s.Capacity > 0)
                    .ToList();
            }
        }

        /// <summary>
        /// 把一筆「未分配的任務」指派給指定 Node。
        /// 回傳 true 表示有分配到，false = 已經沒任務。
        /// </summary>
        private async Task<bool> AssignOneTaskAsync(
            System.Data.IDbConnection conn,
            string nodeName,
            string group,
            CancellationToken ct)
        {
            // 用 CTE + UPDATE TOP (1) + UPDLOCK 避免搶同一筆
            var affected = await conn.ExecuteAsync(new CommandDefinition(@"
;WITH C AS (
    SELECT TOP (1) 
        h.id,
        h.assigned_node   -- ★ 把欄位帶進 CTE，之後 UPDATE C 才看得到
    FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
    JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
    WHERE h.assigned_node IS NULL
      AND (
             (h.action = 'copy'   AND h.file_status IN (0, 800))
          OR (h.action = 'delete' AND h.file_status = -1)
          )
      AND (@group IS NULL OR s_from.set_group = @group)
    ORDER BY ISNULL(h.priority,1) DESC, h.create_time, h.id
)
UPDATE C
SET assigned_node = @NodeName;
",
                new { NodeName = nodeName, group },
                cancellationToken: ct));

            return affected > 0;
        }
    }
}
