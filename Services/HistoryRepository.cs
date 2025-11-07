// Services/HistoryRepository.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using System.Data.Common;
using System.IO;                 // for Path.Combine
using Dapper;
using Microsoft.Extensions.Configuration;
using FileMoverWeb.Services;
namespace FileMoverWeb.Services
{
    #region DTOs
    /// <summary>
    /// 排程端使用的「待處理/領取」工作模型
    /// </summary>
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
        // public int ToStorageId { get; set; }
        public int? ToStorageId { get; set; } 
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
                : Path.Combine(FromPath, $"{UserBit}.MXF");

        public string? FullDestPath =>
            string.IsNullOrWhiteSpace(ToPath) || string.IsNullOrWhiteSpace(UserBit)
                ? null
                : Path.Combine(ToPath, $"{UserBit}.MXF");
    }

   
    #endregion

    public sealed class HistoryRepository
    {
        private readonly DbConnectionFactory _factory;
        private readonly IConfiguration _cfg;

        public HistoryRepository(DbConnectionFactory factory, IConfiguration cfg)
        {
            _factory = factory;
            _cfg = cfg;
        }

        /// <summary>取出「待處理」清單（狀態=0），僅讀不改狀態</summary>
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
LEFT JOIN dbo.Storage AS s_to  ON s_to.id   = h.to_storage_id   -- ★ 刪除歷史顯示
LEFT JOIN dbo.UserData AS u    ON u.id = h.user_id
LEFT JOIN dbo.Channel  AS c    ON c.id = f.channel_id
WHERE h.status IN ('0','-1')  -- 狀態=0/1/-1
ORDER BY h.create_time ASC;";

            var rows = await conn.QueryAsync<HistoryTask>(
                new CommandDefinition(sql, new { n = topN }, cancellationToken: ct));

            return rows.ToList();
        }

        /// <summary>以鎖定+狀態更新方式領取一批（0→1）</summary>
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
  WHERE h.status = 0                     -- ★ 只撿搬運
  ORDER BY h.create_time ASC
)
UPDATE h
SET h.status = 1, h.update_time = GETDATE()
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
LEFT JOIN dbo.Storage s_to   ON s_to.id    = h.to_storage_id
WHERE h.id IN @ids;",
                    new { ids }, transaction: tran, cancellationToken: ct))).ToList();

            tran.Commit();
            return tasks;
        }

        /// <summary>標記成功：status='11'</summary>
        public async Task CompleteAsync(int historyId, CancellationToken ct)
        {
            using var conn = _factory.Create();
            const string sql =  @"
            UPDATE dbo.FileData_History
            SET status='11', update_time=GETDATE()
            WHERE id=@historyId;

            UPDATE f
            SET f.file_status='11'
            FROM dbo.FileData f
            JOIN dbo.FileData_History h ON f.id = h.file_id
            WHERE h.id=@historyId;";

            await conn.ExecuteAsync(new CommandDefinition(sql, new { historyId }, cancellationToken: ct));
        }

        /// <summary>
        /// 標記失敗：status='91'，並寫入 error_msg（**CHANGED**）
        /// </summary>
        public async Task FailAsync(int historyId, string? errorMessage, CancellationToken ct)
        {
            using var conn = _factory.Create();
            const string sql = @"
UPDATE dbo.FileData_History
SET status='91', update_time=GETDATE(), error_msg=@errorMessage
WHERE id=@historyId;";
            await conn.ExecuteAsync(new CommandDefinition(sql, new { historyId, errorMessage }, cancellationToken: ct));
        }

        /// <summary>
        /// 依狀態撈搬運歷史（11=成功、91=失敗、all=全部），呼叫 dbo.sp_ListFileMoveHistory（**NEW**）
        /// </summary>
        public async Task<List<HistoryRow>> ListHistoryAsync(string status, int take, CancellationToken ct)
        {
            if (take <= 0) take = 200;

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
            h.create_time        AS CreateTime,
            h.update_time        AS UpdateTime,
            CAST(h.status AS int) AS Status   -- 11 / 91 轉 int
        FROM dbo.FileData_History AS h
        JOIN dbo.FileData   AS f       ON f.id = h.file_id
        JOIN dbo.Storage    AS s_from  ON s_from.id = h.from_storage_id
        LEFT JOIN dbo.Storage AS s_to  ON s_to.id   = h.to_storage_id
        LEFT JOIN dbo.UserData AS u    ON u.id = h.user_id
        LEFT JOIN dbo.Channel  AS c    ON c.id = f.channel_id
        WHERE h.status IN ('11','91','12','92')            -- 只看已完成（成功/失敗）
        ORDER BY h.update_time DESC, h.id DESC;  -- 最新在前
        ";

            var rows = await conn.QueryAsync<HistoryRow>(
                new CommandDefinition(sql, new { n = take }, cancellationToken: ct));

            return rows.ToList();
        }
/// <summary>
/// 以鎖定+狀態更新方式「領取刪除任務」（-1→1）
/// </summary>
public async Task<List<HistoryTask>> ClaimDeleteAsync(int batchSize, CancellationToken ct)
{
    using var conn = _factory.Create();
    await (conn as DbConnection)!.OpenAsync(ct);
    using var tran = (conn as DbConnection)!.BeginTransaction();

    var ids = await conn.QueryAsync<int>(
        new CommandDefinition(@"
;WITH P AS (
  SELECT TOP (@n) h.id
  FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
  WHERE h.status = -1
  ORDER BY h.create_time ASC
)
UPDATE h
SET h.status = 1, h.update_time = GETDATE()
OUTPUT inserted.id
FROM dbo.FileData_History h
JOIN P ON P.id = h.id;",
            new { n = batchSize }, transaction: tran, cancellationToken: ct));

    List<HistoryTask> tasks = new();
    if (ids.Any())
    {
        tasks = (await conn.QueryAsync<HistoryTask>(
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
LEFT JOIN dbo.Storage s_to   ON s_to.id    = h.to_storage_id   -- ★ 刪除可無 to_storage
WHERE h.id IN @ids;",
                new { ids }, transaction: tran, cancellationToken: ct))).ToList();
    }

    tran.Commit();
    return tasks;
}

/// <summary>刪除成功：status='12'</summary>
public async Task CompleteDeleteAsync(int historyId, CancellationToken ct)
{
    using var conn = _factory.Create();
    const string sql = @"UPDATE dbo.FileData_History SET status='12', update_time=GETDATE() WHERE id=@historyId;";
    await conn.ExecuteAsync(new CommandDefinition(sql, new { historyId }, cancellationToken: ct));
}

/// <summary>刪除失敗：status='92'（含錯誤訊息）</summary>
public async Task FailDeleteAsync(int historyId, string? errorMessage, CancellationToken ct)
{
    using var conn = _factory.Create();
    const string sql = @"
UPDATE dbo.FileData_History
SET status='92', update_time=GETDATE(), error_msg=@errorMessage
WHERE id=@historyId;";
    await conn.ExecuteAsync(new CommandDefinition(sql, new { historyId, errorMessage }, cancellationToken: ct));
}


    }
}
