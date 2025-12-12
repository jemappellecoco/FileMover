using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FileMoverWeb.Services
{
    /// <summary>
    /// 每台節點固定寫入/更新 WorkerNode，讓 DB 知道誰在線
    /// </summary>
    public sealed class WorkerHeartbeatService : BackgroundService
    {
        private readonly DbConnectionFactory _factory;
        private readonly IConfiguration _cfg;
        private readonly ILogger<WorkerHeartbeatService> _logger;

        public WorkerHeartbeatService(
            DbConnectionFactory factory,
            IConfiguration cfg,
            ILogger<WorkerHeartbeatService> logger)
        {
            _factory = factory;
            _cfg = cfg;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
{
    var nodeName = _cfg["Cluster:NodeName"] ?? "Unknown";
    var role     = _cfg["Cluster:Role"]     ?? "Slave";
    var group    = _cfg["Cluster:Group"]    ?? "";
    var hostName = Dns.GetHostName();
    var ip       = ResolveLocalIp() ?? "127.0.0.1";

    _logger.LogInformation(
        "WorkerHeartbeatService started for node {Node}, role={Role}, group={Group}, host={Host}, ip={Ip}",
        nodeName, role, group, hostName, ip);

    while (!stoppingToken.IsCancellationRequested)
    {
        try
        {
            // ✅ currentRunning 你之後可以改成真的值
            var currentRunning = 0;

            using var conn = _factory.Create();

            const string sql = @"
IF EXISTS (SELECT 1 FROM dbo.WorkerNode WHERE NodeName = @NodeName)
BEGIN
    UPDATE dbo.WorkerNode
    SET Role           = @Role,
        GroupCode      = @GroupCode,
        CurrentRunning = @CurrentRunning,
        LastHeartbeat  = SYSDATETIME(),
        HostName       = @HostName,
        IpAddress      = @IpAddress
    WHERE NodeName = @NodeName;
END
ELSE
BEGIN
    INSERT INTO dbo.WorkerNode
        (NodeName, Role, GroupCode, MaxConcurrency, CurrentRunning, LastHeartbeat, HostName, IpAddress)
    VALUES
        (@NodeName, @Role, @GroupCode, @InsertMaxConcurrency, @CurrentRunning, SYSDATETIME(), @HostName, @IpAddress);
END
";

            // ✅ 只在「第一次 insert」時給一個預設值（避免 NULL）
            var insertMaxConc = _cfg.GetValue<int>("GlobalMaxConcurrentMoves", 2);
            if (insertMaxConc < 1) insertMaxConc = 1;
            if (insertMaxConc > 10) insertMaxConc = 10;

            await conn.ExecuteAsync(sql, new
            {
                NodeName = nodeName,
                Role = role,
                GroupCode = group,
                CurrentRunning = currentRunning,
                HostName = hostName,
                IpAddress = ip,
                InsertMaxConcurrency = insertMaxConc
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Heartbeat failed for node");
        }

        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
    }
}

        private static string? ResolveLocalIp()
        {
            try
            {
                var host = Dns.GetHostEntry(Dns.GetHostName());
                foreach (var ip in host.AddressList)
                {
                    if (ip.AddressFamily == AddressFamily.InterNetwork)
                        return ip.ToString();
                }
            }
            catch { }
            return null;
        }
    }
}
