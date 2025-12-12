// Controllers/NodesController.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using FileMoverWeb.Services;

namespace FileMoverWeb.Controllers
{
    /// <summary>
    /// 主控端用來：
    /// 1) 接收各個 worker node 的心跳（POST /api/nodes/heartbeat）
    /// 2) 提供節點清單給前端 UI 查詢（GET /api/nodes）
    /// </summary>
    [ApiController]
    [Route("api/nodes")]
    public sealed class NodesController : ControllerBase
    {
        private readonly DbConnectionFactory _factory;
        private readonly IConfiguration _cfg;
        
        public NodesController(DbConnectionFactory factory, IConfiguration cfg)
        {
            _factory = factory;
            _cfg = cfg;
        }
    
        /// <summary>
        /// 給前端 nodes.js 用的：取得所有節點 + 判斷 Online / Offline
        /// GET /api/nodes
        /// </summary>
        [HttpGet]
        public async Task<IEnumerable<NodeStatusDto>> GetAsync()
        {
            using var conn = _factory.Create();
const string sql = @"
SELECT
  n.NodeName,
  n.Role,
  n.GroupCode,
  n.MaxConcurrency,
  CurrentRunning = COUNT(h.id),
  n.LastHeartbeat,
  n.HostName,
  n.IpAddress
FROM dbo.WorkerNode n
LEFT JOIN dbo.FileData_History h
  ON h.assigned_node = n.NodeName
 AND h.file_status = 1
GROUP BY
  n.NodeName, n.Role, n.GroupCode, n.MaxConcurrency,
  n.LastHeartbeat, n.HostName, n.IpAddress
ORDER BY n.NodeName;
";
//             const string sql = @"
// SELECT NodeName, Role, GroupCode, MaxConcurrency, CurrentRunning, LastHeartbeat, HostName, IpAddress
// FROM dbo.WorkerNode
// ORDER BY NodeName;
// ";

            var rows = await conn.QueryAsync<Row>(sql);
            var now = DateTime.Now;

            // 例如 30 秒內有心跳就算 online，可以寫在 appsettings.json:
            //  "Cluster": { "HeartbeatTimeoutSeconds": 30 }
            var timeoutSec = int.Parse(_cfg["Cluster:HeartbeatTimeoutSeconds"] ?? "30");

            return rows.Select(r =>
            {
                var diffSec = (now - r.LastHeartbeat).TotalSeconds;
                var status  = diffSec < timeoutSec ? "Online" : "Offline";

                return new NodeStatusDto
                {
                    NodeName       = r.NodeName,
                    Role           = r.Role,
                    Group          = r.GroupCode,
                    MaxConcurrency = r.MaxConcurrency,
                    CurrentRunning = r.CurrentRunning,
                    LastHeartbeat  = r.LastHeartbeat,
                    HostName       = r.HostName,
                    IpAddress      = r.IpAddress,
                    Status         = status
                };
            });
        }

        

        /// <summary>
        /// 給 worker node 回報「我還活著」＋目前執行數用
        /// POST /api/nodes/heartbeat
        /// </summary>
        [HttpPost("heartbeat")]
        public async Task<IActionResult> HeartbeatAsync([FromBody] HeartbeatRequest req)
        {
            if (req == null || string.IsNullOrWhiteSpace(req.NodeName))
                return BadRequest("NodeName is required.");

            using var conn = _factory.Create();

            // 如果這個 NodeName 已經存在就 UPDATE，否則 INSERT
            const string sql = @"
IF EXISTS (SELECT 1 FROM dbo.WorkerNode WHERE NodeName = @NodeName)
BEGIN
    UPDATE dbo.WorkerNode
    SET 
        Role           = @Role,
        GroupCode      = @Group,
        
       -- MaxConcurrency = @MaxConcurrency,
        CurrentRunning = @CurrentRunning,
        LastHeartbeat  = GETDATE(),
        HostName       = @HostName,
        IpAddress      = @IpAddress
    WHERE NodeName = @NodeName;
END
ELSE
BEGIN
    INSERT INTO dbo.WorkerNode
    (
        NodeName, Role, GroupCode, MaxConcurrency, CurrentRunning,
        LastHeartbeat, HostName, IpAddress
    )
    VALUES
    (
        @NodeName, @Role, @Group, @MaxConcurrency, @CurrentRunning,
        GETDATE(), @HostName, @IpAddress
    );
END
";

            await conn.ExecuteAsync(sql, new
            {
                req.NodeName,
                req.Role,
                req.Group,
                req.MaxConcurrency,
                req.CurrentRunning,
                req.HostName,
                req.IpAddress
            });

            return Ok(new { status = "ok" });
        }
/// <summary>
        /// 從管理介面調整某個節點的並行數上限
        /// PUT /api/nodes/{nodeName}/concurrency
        /// </summary>
        [HttpPut("{nodeName}/concurrency")]
        public async Task<IActionResult> UpdateConcurrency(
            string nodeName,
            [FromBody] UpdateConcurrencyDto body)
        {
            if (body == null || body.MaxConcurrency <= 0)
                return BadRequest("MaxConcurrency must be > 0");

            using var conn = _factory.Create();

            const string sql = @"
UPDATE dbo.WorkerNode
SET MaxConcurrency = @MaxConcurrency
WHERE NodeName = @NodeName;
";

            var rows = await conn.ExecuteAsync(sql, new
            {
                NodeName       = nodeName,
                MaxConcurrency = body.MaxConcurrency
            });

            if (rows == 0)
                return NotFound();   // 沒有這台節點

            return Ok(new { status = "ok" });
        }


        // ========== 內部用 DTO ==========

        private sealed class Row
        {
            public string NodeName { get; set; } = "";
            public string Role { get; set; } = "";
            public string GroupCode { get; set; } = "";
            public int MaxConcurrency { get; set; }
            public int CurrentRunning { get; set; }
            public DateTime LastHeartbeat { get; set; }
            public string? HostName { get; set; }
            public string? IpAddress { get; set; }
        }


        
        /// <summary>
        /// 回傳給前端 UI 看節點狀態用
        /// </summary>
        public sealed class NodeStatusDto
        {
            public string NodeName { get; set; } = "";
            public string Role { get; set; } = "";
            public string Group { get; set; } = "";
            public int MaxConcurrency { get; set; }
            public int CurrentRunning { get; set; }
            public DateTime LastHeartbeat { get; set; }
            public string? HostName { get; set; }
            public string? IpAddress { get; set; }
            public string Status { get; set; } = "";   // Online / Offline
        }

        /// <summary>
        /// worker node 呼叫 POST /api/nodes/heartbeat 時帶進來的資料
        // </summary>
        public sealed class HeartbeatRequest
        {
            public string NodeName { get; set; } = ""; // 節點代號，例如 "Node-7F-01"
            public string Role     { get; set; } = ""; // "Master" / "Worker"
            public string Group    { get; set; } = ""; // "4F" / "7F" / "RESTORE"
            public int MaxConcurrency { get; set; }    // 這台最多能同時跑幾個任務
            public int CurrentRunning { get; set; }    // 目前正在跑幾個
            public string? HostName   { get; set; }    // 機器名稱
            public string? IpAddress  { get; set; }    // IP (可選)
        }
          /// <summary>
        /// 從前端調整併發數用的 DTO
        /// </summary>
        public sealed class UpdateConcurrencyDto
        {
            public int MaxConcurrency { get; set; }
        }
    }
}
