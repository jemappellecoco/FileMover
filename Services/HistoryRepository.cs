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

namespace FileMoverWeb.Services
{
    #region DTOs
    /// <summary>
    /// 排程端使用的「待處理/領取」工作模型
    /// </summary>
    public sealed class HistoryTask
    {
            // ⭐ 來源 / 目的 Storage 所屬樓層 group
        public string? FromGroup { get; set; }
        public string? ToGroup   { get; set; }

        public int HistoryId { get; set; }
        public int FileId { get; set; }

        // 檔案資訊
        public string FileName { get; set; } = "";
        public long FileSize { get; set; }             // FileData.filesize
        public string? UserBit { get; set; }           // FileData.UserBit

        // 這筆 history 有沒有對到 FileData
        public bool HasFileData { get; set; }          // 0 = 沒有, 1 = 有

        // 來源/目的 Storage
        public int FromStorageId { get; set; }
        public string? FromName { get; set; }          // Storage.storage_name
        public string FromPath { get; set; } = "";
        public int? ToStorageId { get; set; }
        public string? ToName { get; set; }            // Storage.storage_name
        public string ToPath { get; set; } = "";

        // 申請者＆動作
        public string? RequestedBy { get; set; }       // UserData.username
        public string? Action { get; set; }            // FileData_History.action
        public DateTime CreateTime { get; set; }       // FileData_History.create_time
        // 目前狀態（0 / 1 / -1）
        public int FileStatus { get; set; }
       
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
            var group = _cfg.GetValue<string>("FloorRouting:Group");
            using var conn = _factory.Create();

            var sql = @"
SELECT TOP (@n)
    h.id                 AS HistoryId,
    h.file_id            AS FileId,
    f.filename           AS FileName,
    CAST(COALESCE(f.filesize_7F, f.filesize_4F) AS BIGINT) AS FileSize,
    f.UserBit            AS UserBit,
  
    s_from.id            AS FromStorageId,
    s_from.storage_name  AS FromName,
    s_from.location      AS FromPath,
    s_from.set_group     AS FromGroup,
    s_to.id              AS ToStorageId,
    s_to.storage_name    AS ToName,
    s_to.location        AS ToPath,
    u.username           AS RequestedBy,
    h.action             AS Action,
    h.create_time        AS CreateTime,
    CAST(h.file_status AS int) AS FileStatus   -- ⭐ 多這一欄
FROM dbo.FileData_History AS h
JOIN dbo.FileData   AS f       ON f.id = h.file_id
JOIN dbo.Storage    AS s_from  ON s_from.id = h.from_storage_id
LEFT JOIN dbo.Storage AS s_to  ON s_to.id   = h.to_storage_id
LEFT JOIN dbo.UserData AS u    ON u.id = h.user_id

WHERE h.file_status IN (0, 1, -1)                      -- ⭐ 把 1 加進來
 AND (@group IS NULL OR @group = '' OR s_from.set_group = @group)  -- ★ 依樓層過濾
ORDER BY 
    CASE WHEN h.file_status = 1 THEN 0 ELSE 1 END,    -- ⭐ 讓「執行中」排上面
    h.create_time ASC;";

            var rows = await conn.QueryAsync<HistoryTask>(
                new CommandDefinition(sql, new { n = topN, group }, cancellationToken: ct));

            return rows.ToList();
        }

        /// <summary>
            /// 以鎖定+狀態更新方式領取一批「搬移任務」：
            /// - 僅處理 action='copy'
            /// - file_status = 0 為新任務
            /// - file_status = 1 且 update_time 超過 retryMinutes 視為「卡住，需重試」
            /// </summary>
            public async Task<List<HistoryTask>> ClaimAsync(
                int batchSize,
                int retryMinutes,
                string? group,
                CancellationToken ct)
            {
                using var conn = _factory.Create();
                await (conn as DbConnection)!.OpenAsync(ct);
                using var tran = (conn as DbConnection)!.BeginTransaction();

                var ids = await conn.QueryAsync<int>(
                    new CommandDefinition(@"
                ;WITH P AS (
                    SELECT TOP (@n) h.id
                    FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
                    JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id 
                    WHERE
                            -- ✅ 只處理 copy 任務
                            h.action = 'copy'
                        AND (@group IS NULL OR s_from.set_group = @group)
                        AND (
                                -- 新任務
                                h.file_status = 0
                            OR (h.file_status = 1
                                AND DATEDIFF(MINUTE, h.update_time, GETDATE()) >= @retryMin)
                            )
                    ORDER BY h.create_time ASC
                )
                UPDATE h
                SET h.file_status = 1,
                    h.update_time = GETDATE()
                OUTPUT inserted.id
                FROM dbo.FileData_History h
                JOIN P ON P.id = h.id;",
                        new { n = batchSize, retryMin = retryMinutes, group },
                        transaction: tran,
                        cancellationToken: ct));


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
                s_to.location     AS ToPath,
                s_from.set_group  AS FromGroup,   
                s_to.set_group    AS ToGroup,     
                CASE WHEN f.id IS NULL THEN 0 ELSE 1 END AS HasFileData   -- ★ NEW
            FROM dbo.FileData_History h
            LEFT JOIN dbo.FileData   f     ON f.id      = h.file_id       -- ★ 改 LEFT JOIN
            JOIN dbo.Storage         s_from ON s_from.id = h.from_storage_id
            LEFT JOIN dbo.Storage    s_to   ON s_to.id   = h.to_storage_id
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
            SET file_status='11', update_time=GETDATE()
            WHERE id=@historyId;

            UPDATE f
            SET f.file_status='11'
            FROM dbo.FileData f
            JOIN dbo.FileData_History h ON f.id = h.file_id
            WHERE h.id=@historyId;";

            await conn.ExecuteAsync(new CommandDefinition(sql, new { historyId }, cancellationToken: ct));
        }

       // <summary>
/// 搬移失敗：status = 9xx（911/912/913/914）
/// </summary>
public async Task FailAsync(int historyId, int statusCode, string? errorMessage, CancellationToken ct)
{
    using var conn = _factory.Create();
    const string sql = @"
UPDATE dbo.FileData_History
SET file_status = @statusCode,
    update_time = GETDATE()
WHERE id = @historyId;";

    await conn.ExecuteAsync(
        new CommandDefinition(sql, new { historyId, statusCode }, cancellationToken: ct));
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
            CAST(COALESCE(f.filesize_7F, f.filesize_4F) AS BIGINT) AS FileSize,
            f.UserBit            AS UserBit,
         
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
            CAST(h.file_status AS int) AS Status   -- 11 / 91 轉 int
        FROM dbo.FileData_History AS h
        JOIN dbo.FileData   AS f       ON f.id = h.file_id
        JOIN dbo.Storage    AS s_from  ON s_from.id = h.from_storage_id
        LEFT JOIN dbo.Storage AS s_to  ON s_to.id   = h.to_storage_id
        LEFT JOIN dbo.UserData AS u    ON u.id = h.user_id
      
        WHERE h.file_status IN (
                11,12,          -- 成功
                91,92,          -- 失敗（其他）
                911,912,913,914,  -- 搬移失敗細項
                921,922,923       -- 刪除失敗細項
            )
                
        ORDER BY h.update_time DESC, h.id DESC;  -- 最新在前
        ";

            var rows = await conn.QueryAsync<HistoryRow>(
                new CommandDefinition(sql, new { n = take }, cancellationToken: ct));

            return rows.ToList();
        }
/// <summary>
/// 以鎖定+狀態更新方式「領取刪除任務」：
/// - 僅處理 action='delete'
/// - file_status = -1 為新刪除任務
/// - file_status = 1 且 update_time 超過 retryMinutes 視為「卡住，需重試」
/// </summary>
public async Task<List<HistoryTask>> ClaimDeleteAsync(
    int batchSize,
    int retryMinutes,
    string? group,
    CancellationToken ct)
{
    using var conn = _factory.Create();
    await (conn as DbConnection)!.OpenAsync(ct);
    using var tran = (conn as DbConnection)!.BeginTransaction();

    var ids = await conn.QueryAsync<int>(
        new CommandDefinition(@"
;WITH P AS (
  SELECT TOP (@n) h.id
  FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
  JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id      -- ★ 補上 JOIN
  WHERE
        h.action = 'delete'
    AND (@group IS NULL OR s_from.set_group = @group)           -- ★ 依樓層過濾
    AND (
            h.file_status = -1
         OR (h.file_status = 1
             AND DATEDIFF(MINUTE, h.update_time, GETDATE()) >= @retryMin)
        )
  ORDER BY h.create_time ASC
)
UPDATE h
SET h.file_status = 1, h.update_time = GETDATE()
OUTPUT inserted.id
FROM dbo.FileData_History h
JOIN P ON P.id = h.id;",
            new { n = batchSize, retryMin = retryMinutes, group },  // ★ 加上 group
            transaction: tran, cancellationToken: ct));

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
  s_to.location     AS ToPath,
  s_from.set_group  AS FromGroup,         -- ★ 可選：要的話就補上
  s_to.set_group    AS ToGroup,           -- ★ 可選
  CASE WHEN f.id IS NULL THEN 0 ELSE 1 END AS HasFileData
FROM dbo.FileData_History h
LEFT JOIN dbo.FileData f     ON f.id       = h.file_id
JOIN dbo.Storage s_from      ON s_from.id  = h.from_storage_id
LEFT JOIN dbo.Storage s_to   ON s_to.id    = h.to_storage_id
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
    const string sql = @"UPDATE dbo.FileData_History SET file_status='12', update_time=GETDATE() WHERE id=@historyId;";
    await conn.ExecuteAsync(new CommandDefinition(sql, new { historyId }, cancellationToken: ct));
}

/// <summary>
/// 刪除失敗：status = 92x（921/922/923）
/// </summary>
public async Task FailDeleteAsync(int historyId, int statusCode, string? errorMessage, CancellationToken ct)
{
    using var conn = _factory.Create();
    const string sql = @"
UPDATE dbo.FileData_History
SET file_status = @statusCode,
    update_time = GETDATE()
WHERE id = @historyId;";

    await conn.ExecuteAsync(
        new CommandDefinition(sql, new { historyId, statusCode }, cancellationToken: ct));
}




public async Task<int> GetRestoreStorageIdAsync(string group, CancellationToken ct)
{
    using var conn = _factory.Create();

    var ids = (await conn.QueryAsync<int>(
        new CommandDefinition(@"
SELECT id
FROM dbo.Storage
WHERE set_group = @g
  AND type = 'RESTORE';
", new { g = group }, cancellationToken: ct))).ToList();

    if (ids.Count == 0)
        throw new InvalidOperationException($"找不到 {group} 的 RESTORE storage (type='RESTORE')");

    if (ids.Count > 1)
        throw new InvalidOperationException($"{group} 有超過一個 RESTORE，請檢查 Storage 設定");

    return ids[0];
}

/// <summary>
/// 取得某個 Storage 的實際路徑 (location)
/// </summary>
public async Task<string> GetStorageLocationAsync(int storageId, CancellationToken ct)
{
    using var conn = _factory.Create();

    var path = await conn.ExecuteScalarAsync<string>(
        new CommandDefinition(@"
SELECT location 
FROM dbo.Storage
WHERE id = @id;",
            new { id = storageId }, cancellationToken: ct));

    if (string.IsNullOrWhiteSpace(path))
        throw new InvalidOperationException($"找不到 StorageId={storageId} 的路徑 (location)");

    return path;
}

/// <summary>
/// 跨樓層搬運：階段一完成（已搬到本樓層 RESTORE）
/// ✅ 只更新 file_status = 14 / 17，不動 from_storage_id / to_storage_id
/// </summary>
public async Task MarkPhase1DoneAsync(
    int historyId,
    int statusCode,            // 14 或 17
    CancellationToken ct)
{
    using var conn = _factory.Create();

    const string sql = @"
UPDATE dbo.FileData_History
SET file_status = @statusCode,
    update_time = GETDATE()
WHERE id = @historyId;";

    await conn.ExecuteAsync(
        new CommandDefinition(
            sql,
            new { historyId, statusCode },
            cancellationToken: ct));
}
    
}


}
