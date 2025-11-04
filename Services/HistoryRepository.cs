// Services/HistoryRepository.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Data.Common;
using System.IO;                 // ← for Path.Combine
using Dapper;
using Microsoft.Extensions.Configuration;

namespace FileMoverWeb.Services
{
    public sealed class HistoryTask
    {
        public int HistoryId { get; set; }
        public int FileId { get; set; }

        // 檔案資訊
        public string FileName { get; set; } = "";
        public long FileSize { get; set; }             // FileData.filesize
        public string? UserBit { get; set; }           // FileData.UserBit
        public string? ChannelName { get; set; }       // Channel.channel_name

        // 來源/目的 Storage
        public int FromStorageId { get; set; }
        public string? FromName { get; set; }          // Storage.storage_name
        public string FromPath { get; set; } = "";
        public int ToStorageId { get; set; }
        public string? ToName { get; set; }            // Storage.storage_name
        public string ToPath { get; set; } = "";

        // 申請者＆動作
        public string? RequestedBy { get; set; }       // UserData.username
        public string? Action { get; set; }            // FileData_History.action
        public DateTime CreateTime { get; set; }       // FileData_History.create_time

        // 後端自動組完整路徑（含 .mxf）
        public string? FullSourcePath =>
            string.IsNullOrWhiteSpace(FromPath) || string.IsNullOrWhiteSpace(UserBit)
                ? null
                : Path.Combine(FromPath, $"{UserBit}.mxf");

        public string? FullDestPath =>
            string.IsNullOrWhiteSpace(ToPath) || string.IsNullOrWhiteSpace(UserBit)
                ? null
                : Path.Combine(ToPath, $"{UserBit}.mxf");
    }

    public sealed class HistoryRepository
    {
        private readonly DbConnectionFactory _factory;
        private readonly IConfiguration _cfg;

        public HistoryRepository(DbConnectionFactory factory, IConfiguration cfg)
        {
            _factory = factory;
            _cfg = cfg;
        }

        /// <summary>取出「待處理」清單（狀態='0'），僅讀不改狀態</summary>
        public async Task<List<HistoryTask>> ListPendingAsync(int topN, CancellationToken ct)
        {
            if (topN <= 0) topN = 50;

            using var conn = _factory.Create();

            var sql = @"
SELECT TOP (@n)
    h.id                 AS HistoryId,
    h.file_id            AS FileId,
    f.filename           AS FileName,
    f.filesize           AS FileSize,
    f.UserBit            AS UserBit,
    c.channel_name       AS ChannelName,
    s_from.id            AS FromStorageId,
    s_from.storage_name  AS FromName,
    s_from.location      AS FromPath,
    s_to.id              AS ToStorageId,
    s_to.storage_name    AS ToName,
    s_to.location        AS ToPath,
    u.username           AS RequestedBy,
    h.action             AS Action,
    h.create_time        AS CreateTime
FROM dbo.FileData_History AS h
JOIN dbo.FileData   AS f       ON f.id = h.file_id
JOIN dbo.Storage    AS s_from  ON s_from.id = h.from_storage_id
JOIN dbo.Storage    AS s_to    ON s_to.id   = h.to_storage_id
LEFT JOIN dbo.UserData AS u    ON u.id = h.user_id
LEFT JOIN dbo.Channel  AS c    ON c.id = f.channel_id
WHERE h.status = '0'
ORDER BY h.create_time ASC;";

            var rows = await conn.QueryAsync<HistoryTask>(
                new CommandDefinition(sql, new { n = topN }, cancellationToken: ct));

            return rows.ToList();
        }

        /// <summary>以鎖定+狀態更新方式領取一批（'0'→'I'）</summary>
        public async Task<List<HistoryTask>> ClaimAsync(int batchSize, CancellationToken ct)
        {
            using var conn = _factory.Create();
            await (conn as DbConnection)!.OpenAsync(ct);
            using var tran = (conn as DbConnection)!.BeginTransaction();

            var ids = await conn.QueryAsync<int>(
                new CommandDefinition(@"
;WITH P AS (
  SELECT TOP (@n) h.id
  FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
  JOIN dbo.Storage s_to ON s_to.id = h.to_storage_id
  WHERE h.status = '0'
  ORDER BY s_to.priority ASC, h.create_time ASC
)
UPDATE h
SET h.status = 'I', h.update_time = GETDATE()
OUTPUT inserted.id
FROM dbo.FileData_History h
JOIN P ON P.id = h.id;",
                    new { n = batchSize }, transaction: tran, cancellationToken: ct));

            if (!ids.Any())
            {
                tran.Commit();
                return new();
            }

            var tasks = (await conn.QueryAsync<HistoryTask>(
                new CommandDefinition(@"
SELECT 
  h.id              AS HistoryId,
  h.file_id         AS FileId,
  h.from_storage_id AS FromStorageId,
  h.to_storage_id   AS ToStorageId,
  f.filename        AS FileName,
  f.UserBit         AS UserBit,
  s_from.location   AS FromPath,
  s_to.location     AS ToPath
FROM dbo.FileData_History h
JOIN dbo.FileData f     ON f.id       = h.file_id
JOIN dbo.Storage s_from ON s_from.id  = h.from_storage_id
JOIN dbo.Storage s_to   ON s_to.id    = h.to_storage_id
WHERE h.id IN @ids;",
                    new { ids }, transaction: tran, cancellationToken: ct))).ToList();

            tran.Commit();
            return tasks;
        }

        public async Task CompleteAsync(int historyId, CancellationToken ct)
        {
            using var conn = _factory.Create();
            const string sql = @"UPDATE dbo.FileData_History SET status='10', update_time=GETDATE() WHERE id=@historyId;";
            await conn.ExecuteAsync(new CommandDefinition(sql, new { historyId }, cancellationToken: ct));
        }

        public async Task FailAsync(int historyId, CancellationToken ct)
        {
            using var conn = _factory.Create();
            const string sql = @"UPDATE dbo.FileData_History SET status='90', update_time=GETDATE() WHERE id=@historyId;";
            await conn.ExecuteAsync(new CommandDefinition(sql, new { historyId }, cancellationToken: ct));
        }
    }
}
