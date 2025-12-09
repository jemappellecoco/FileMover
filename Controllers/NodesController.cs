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

        [HttpGet]
        public async Task<IEnumerable<NodeStatusDto>> GetAsync()
        {
            using var conn = _factory.Create();

            const string sql = @"
SELECT NodeName, Role, GroupCode, MaxConcurrency, CurrentRunning, LastHeartbeat, HostName, IpAddress
FROM dbo.WorkerNode
ORDER BY NodeName;
";

            var rows = await conn.QueryAsync<Row>(sql);

            var now = DateTime.UtcNow;
            // 例如 30 秒內有心跳就算 online，可以寫到設定檔
            var timeoutSec = int.Parse(_cfg["Cluster:HeartbeatTimeoutSeconds"] ?? "30");

            return rows.Select(r =>
            {
                var hbUtc = DateTime.SpecifyKind(r.LastHeartbeat, DateTimeKind.Utc);
                var diffSec = (now - hbUtc).TotalSeconds;

                var status = diffSec < timeoutSec ? "Online" : "Offline";

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
            public string Status { get; set; } = "";   // ⭐ 這裡就會是 Online / Offline
        }
    }
}
