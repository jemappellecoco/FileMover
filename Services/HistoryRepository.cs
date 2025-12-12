// Services/HistoryRepository.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Data.Common;
using System.IO;                 // for Path.Combine
using Dapper;

using System.Data.SqlClient; 
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
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
        public int FileId    { get; set; }

        // 檔案資訊
        public string   FileName { get; set; } = "";
        public long     FileSize { get; set; }             // FileData.filesize
        public string?  UserBit  { get; set; }             // FileData.UserBit

        // 這筆 history 有沒有對到 FileData
        public bool HasFileData { get; set; }              // 0 = 沒有, 1 = 有

        // 來源/目的 Storage
        public int    FromStorageId { get; set; }
        public string? FromName      { get; set; }         // Storage.storage_name
        public string  FromPath      { get; set; } = "";
        public int?    ToStorageId   { get; set; }
        public string? ToName        { get; set; }         // Storage.storage_name
        public string  ToPath        { get; set; } = "";

        // 申請者＆動作
        public string?  RequestedBy { get; set; }          // UserData.username
        public string?  Action      { get; set; }          // FileData_History.action
        public DateTime CreateTime  { get; set; }          // FileData_History.create_time
        
        // 目前指派給哪個 node
        public string? AssignedNode { get; set; }          // FileData_History.assigned_node    
        // 目前狀態（0 / 1 / -1 / 24 / 27 / 9xx...）
        public int   FileStatus { get; set; }
        public int?  Priority   { get; set; }

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
        private readonly IConfiguration      _cfg;
        private static readonly SemaphoreSlim _copyClaimLock = new(1, 1);
        private readonly string? _nodeName;
        public HistoryRepository(DbConnectionFactory factory, IConfiguration cfg)
        {
            _factory = factory;
            _cfg     = cfg;
             // ⭐ 這台程式實例對應的節點名稱，例如 4F-M1 / 4F-S1
            _nodeName = _cfg.GetValue<string>("Cluster:NodeName");
        }

        /// <summary>
        /// 取出「待處理＋進行中」清單，僅讀不改狀態
        /// - Phase1 / 刪除：來源樓層 = 本層 group
        /// - Phase2：依 status 決定要給哪一層
        /// </summary>
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
    h.priority           AS Priority,
    s_to.id              AS ToStorageId,
    s_to.storage_name    AS ToName,
    s_to.location        AS ToPath,
    u.username           AS RequestedBy,
    h.action             AS Action,
    h.create_time        AS CreateTime,
    h.assigned_node      AS AssignedNode,
    CAST(h.file_status AS int) AS FileStatus   -- ⭐ 狀態欄
FROM dbo.FileData_History AS h
JOIN dbo.FileData       AS f      ON f.id = h.file_id
JOIN dbo.Storage        AS s_from ON s_from.id = h.from_storage_id
LEFT JOIN dbo.Storage   AS s_to   ON s_to.id   = h.to_storage_id
LEFT JOIN dbo.UserData  AS u      ON u.id = h.user_id
WHERE 
(
     -- ⭐ Phase 1：由來源樓層負責的任務
    -- 包含：
    --   0   → 新的 copy 任務
    --   1   → 正在搬移中的任務
    --  -1   → delete 任務（待刪除）
    --  800  → copy/delete 需要在本樓層重試的任務
    h.file_status IN (0, 1, -1, 800)
    AND s_from.set_group = @group
)
OR
(
    -- ⭐ Phase 2：跨樓層回遷（RESTORE → 目的地）
    -- 24 → 4F → 7F 回遷，由 7F 執行
    -- 27 → 7F → 4F 回遷，由 4F 執行
    (h.file_status = 24 AND @group = '7F')     -- 4F → 7F 回遷
    OR
    (h.file_status = 27 AND @group = '4F')     -- 7F → 4F 回遷
)
ORDER BY
    CASE WHEN h.file_status = 1 THEN 0 ELSE 1 END,    -- 先把進行中排前面
    h.priority DESC,                                  -- 再依 Priority
    h.create_time ASC;                                -- 同優先級比時間";

            var rows = await conn.QueryAsync<HistoryTask>(
                new CommandDefinition(sql, new { n = topN, group }, cancellationToken: ct));

            return rows.ToList();
        }

        /// <summary>取得本樓層 RESTORE storage 的名稱</summary>
        public async Task<string?> GetRestoreNameAsync(string group, CancellationToken ct)
        {
            using var conn = _factory.Create();

            const string sql = @"
SELECT TOP 1 storage_name
FROM dbo.Storage
WHERE [type] = 'RESTORE'
  AND set_group = @group
ORDER BY priority;";

            return await conn.ExecuteScalarAsync<string?>(
                new CommandDefinition(sql, new { group }, cancellationToken: ct));
        }

        /// <summary>
        /// Phase2：列出所有等待回遷的歷史紀錄（file_status = 14 / 17）
        /// （前端「回遷清單」頁面用）
        /// </summary>
        public async Task<List<HistoryTask>> ListPhase2PendingAsync(int topN, CancellationToken ct)
        {
            if (topN <= 0) topN = 50;

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
    CAST(h.file_status AS int) AS FileStatus
FROM dbo.FileData_History AS h
JOIN dbo.FileData   AS f      ON f.id = h.file_id
JOIN dbo.Storage    AS s_from ON s_from.id = h.from_storage_id
LEFT JOIN dbo.Storage AS s_to ON s_to.id   = h.to_storage_id
LEFT JOIN dbo.UserData AS u   ON u.id = h.user_id
WHERE h.file_status IN (14, 17)      -- ⭐ Phase2：只抓 14 / 17
ORDER BY h.priority, h.update_time DESC, h.id DESC;";

            var rows = await conn.QueryAsync<HistoryTask>(
                new CommandDefinition(sql, new { n = topN }, cancellationToken: ct));

            return rows.ToList();
        }

        /// <summary>
        /// Phase2：使用者在前端勾選「回遷」後，將 14/17 改成 24/27（等待回遷）
        /// </summary>
        public async Task MarkPhase2ToReadyAsync(int[] historyIds, CancellationToken ct)
        {
            if (historyIds == null || historyIds.Length == 0) return;

            using var conn = _factory.Create();

            const string sql = @"
;WITH T AS (
    SELECT id, file_status
    FROM dbo.FileData_History
    WHERE id IN @ids
      AND file_status IN (14, 17)
)
UPDATE h
SET file_status = CASE WHEN T.file_status = 14 THEN 24 ELSE 27 END,
    update_time = GETDATE()
FROM dbo.FileData_History h
JOIN T ON T.id = h.id;";

            await conn.ExecuteAsync(
                new CommandDefinition(sql, new { ids = historyIds }, cancellationToken: ct));
        }

        /// <summary>
        /// Phase2：以批次方式領取一批「回遷任務」（舊有 batch 版本，現在 slot 模式可不再使用）
        /// </summary>
        public async Task<List<HistoryTask>> ClaimPhase2Async(
            int batchSize,
            string? group,
            CancellationToken ct)
        {
            using var conn = _factory.Create();
            await (conn as DbConnection)!.OpenAsync(ct);
            using var tran = (conn as DbConnection)!.BeginTransaction();
            var nodeName = _nodeName;
           var ids = await conn.QueryAsync<int>(
                new CommandDefinition(@"
;WITH P AS (
  SELECT TOP (@n) h.id
  FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
  JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
  WHERE
        h.action = 'copy'
    AND h.file_status IN (24, 27)          -- ⭐ Phase2 待回遷
    AND (@group IS NULL OR s_from.set_group = @group)
    -- ⭐ Node 篩選：如果有設定 NodeName，就只撿指派給自己或尚未指派的
    AND (
          @nodeName IS NULL
       OR @nodeName = ''
       OR h.assigned_node IS NULL
       OR h.assigned_node = @nodeName
    )
  ORDER BY ISNULL(h.priority, 1) DESC,        -- ⭐ 優先級大的先回遷
        h.update_time ASC,
        h.id ASC
)
UPDATE h
SET h.file_status = 1,
    h.update_time = GETDATE()
OUTPUT inserted.id
FROM dbo.FileData_History h
JOIN P ON P.id = h.id;",
                    new { n = batchSize, group, nodeName },   // ⭐ 記得把 nodeName 傳進去
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
    CAST(h.file_status AS int) AS FileStatus,
    h.priority        AS Priority,
    CASE WHEN f.id IS NULL THEN 0 ELSE 1 END AS HasFileData
FROM dbo.FileData_History h
LEFT JOIN dbo.FileData   f      ON f.id      = h.file_id
JOIN dbo.Storage         s_from ON s_from.id = h.from_storage_id
LEFT JOIN dbo.Storage    s_to   ON s_to.id   = h.to_storage_id
WHERE h.id IN @ids;",
                    new { ids }, transaction: tran, cancellationToken: ct))).ToList();

            tran.Commit();
            return tasks;
        }

        /// <summary>
        /// Phase2（slot 版）：以 TOP 1 領取一筆「回遷任務」，依 priority 排序
        /// </summary>
public async Task<HistoryTask?> ClaimPhase2TopOneAsync(
    string? group,
    CancellationToken ct)
{
    using var conn = _factory.Create();
    await (conn as DbConnection)!.OpenAsync(ct);
    using var tran = (conn as DbConnection)!.BeginTransaction();

    var nodeName      = _nodeName;
    var useNodeFilter = !string.IsNullOrWhiteSpace(nodeName);

    var id = await conn.ExecuteScalarAsync<int?>(
        new CommandDefinition(@"
;WITH P AS (
  SELECT TOP (1) h.id
  FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
  JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
  WHERE
        h.action = 'copy'
    AND h.file_status IN (24, 27)          -- ⭐ Phase2 待回遷
    AND (@group IS NULL OR s_from.set_group = @group)
    -- ⭐ 有啟用 Node 篩選 → 只撿 assigned_node = 自己
    AND (
      @useNodeFilter = 0
   OR h.assigned_node = @nodeName
)
  ORDER BY ISNULL(h.priority, 1) DESC,
           h.update_time ASC,
           h.id ASC
)
UPDATE h
SET h.file_status = 1,
    h.update_time = GETDATE()
OUTPUT inserted.id
FROM dbo.FileData_History h
JOIN P ON P.id = h.id;",
            new { group, nodeName, useNodeFilter },
            transaction: tran,
            cancellationToken: ct));

    if (!id.HasValue)
    {
        tran.Commit();
        return null;
    }

    var task = await conn.QuerySingleOrDefaultAsync<HistoryTask>(
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
    CAST(h.file_status AS int) AS FileStatus,
    h.priority        AS Priority,
    CASE WHEN f.id IS NULL THEN 0 ELSE 1 END AS HasFileData
FROM dbo.FileData_History h
LEFT JOIN dbo.FileData   f      ON f.id      = h.file_id
JOIN dbo.Storage         s_from ON s_from.id = h.from_storage_id
LEFT JOIN dbo.Storage    s_to   ON s_to.id   = h.to_storage_id
WHERE h.id = @id;",
            new { id },
            transaction: tran,
            cancellationToken: ct));

    tran.Commit();
    return task;
}


        /// <summary>
        /// 批次版 Claim（舊的 batch 版本，slot 模式不強制使用）
        /// </summary>
//         public async Task<List<HistoryTask>> ClaimAsync(
//             int batchSize,
//             int retryMinutes,
//             string? group,
//             CancellationToken ct)
//         {
//             using var conn = _factory.Create();
//             await (conn as DbConnection)!.OpenAsync(ct);
//             using var tran = (conn as DbConnection)!.BeginTransaction();

//             // 🔹 先標記「來源 StorageId 無效」→ 901
//             await conn.ExecuteAsync(new CommandDefinition(@"
// UPDATE h
// SET h.file_status = 901,
//     h.update_time = GETDATE()
// FROM dbo.FileData_History h
// LEFT JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
// WHERE h.action = 'copy'
//   AND h.file_status IN (0, 1)
//   AND s_from.id IS NULL;      -- 找不到來源 Storage
// ",
//                 transaction: tran, cancellationToken: ct));

//             // 🔹 再標記「目的地 StorageId 無效」→ 902
//             await conn.ExecuteAsync(new CommandDefinition(@"
// UPDATE h
// SET h.file_status = 902,
//     h.update_time = GETDATE()
// FROM dbo.FileData_History h
// JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
// LEFT JOIN dbo.Storage s_to ON s_to.id = h.to_storage_id
// WHERE h.action = 'copy'
//   AND h.file_status IN (0, 1)
//   AND h.to_storage_id IS NOT NULL
//   AND s_to.id IS NULL;        -- 找不到目的地 Storage
// ",
//                 transaction: tran, cancellationToken: ct));

//             var ids = await conn.QueryAsync<int>(
//                 new CommandDefinition(@"
// ;WITH P AS (
//     SELECT TOP (@n) h.id
//     FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
//     JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id 
//     WHERE
//             -- ✅ 只處理 copy 任務
//             h.action = 'copy'
//         AND (@group IS NULL OR s_from.set_group = @group)
//         AND (
//                 -- 新任務
//                 h.file_status = 0
//             OR (h.file_status = 1
//                 AND DATEDIFF(MINUTE, h.update_time, GETDATE()) >= @retryMin)
//             )
//     ORDER BY 
//         ISNULL(h.priority, 1) DESC,   -- ⭐ 優先級高的先撿
//         h.create_time ASC,            -- 同優先級才比建立時間
//         h.id ASC
// )
// UPDATE h
// SET h.file_status = 1,
//     h.update_time = GETDATE()
// OUTPUT inserted.id
// FROM dbo.FileData_History h
// JOIN P ON P.id = h.id;",
//                     new { n = batchSize, retryMin = retryMinutes, group },
//                     transaction: tran,
//                     cancellationToken: ct));

//             if (!ids.Any())
//             {
//                 tran.Commit();
//                 return new();
//             }

//             var tasks = (await conn.QueryAsync<HistoryTask>(
//                 new CommandDefinition(@"
// SELECT 
//     h.id              AS HistoryId,
//     h.file_id         AS FileId,
//     h.from_storage_id AS FromStorageId,
//     h.to_storage_id   AS ToStorageId,
//     f.filename        AS FileName,
//     f.UserBit         AS UserBit,
//     s_from.location   AS FromPath,
//     s_to.location     AS ToPath,
//     s_from.set_group  AS FromGroup,
//     s_to.set_group    AS ToGroup,
//     h.priority        AS Priority,
//     CASE WHEN f.id IS NULL THEN 0 ELSE 1 END AS HasFileData
// FROM dbo.FileData_History h
// LEFT JOIN dbo.FileData   f      ON f.id      = h.file_id
// JOIN dbo.Storage         s_from ON s_from.id = h.from_storage_id
// LEFT JOIN dbo.Storage    s_to   ON s_to.id   = h.to_storage_id
// WHERE h.id IN @ids;",
//                     new { ids }, transaction: tran, cancellationToken: ct))).ToList();

//             tran.Commit();
//             return tasks;
//         }

        /// <summary>
/// Slot-based 搬移：一次領取「一筆」 copy 任務：
/// - 僅處理 action='copy'
/// - file_status = 0 為新任務
/// - file_status = 1 且 update_time 超過 retryMinutes 視為「卡住，需重試」
/// ✅ 加上應用程式層級鎖，避免多個 slot 互搶造成死結 / 重複領取
/// </summary>
public async Task<HistoryTask?> ClaimCopyTopOneAsync(
    int retryMinutes,
    string? group,
    CancellationToken ct)
{
    // 🔒 一次只允許一個 slot 進來 Claim，避免死結 & 重複領取同一筆
    await _copyClaimLock.WaitAsync(ct);
    try
    {
        using var conn = _factory.Create();
        var dbConn = (DbConnection)conn;
        await dbConn.OpenAsync(ct);
        using var tran = dbConn.BeginTransaction();
        var nodeName = _nodeName; // ⭐ 這台節點名稱
        var useNodeFilter = !string.IsNullOrWhiteSpace(nodeName);

        // 🔹 先標記「來源 StorageId 無效」→ 901
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE h
SET h.file_status = 901,
    h.update_time = GETDATE()
FROM dbo.FileData_History h
LEFT JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
WHERE h.action = 'copy'
  AND h.file_status IN (0, 1)
  AND s_from.id IS NULL;      -- 找不到來源 Storage
",
            transaction: tran,
            cancellationToken: ct));

        // 🔹 再標記「目的地 StorageId 無效」→ 902
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE h
SET h.file_status = 902,
    h.update_time = GETDATE()
FROM dbo.FileData_History h
JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
LEFT JOIN dbo.Storage s_to ON s_to.id = h.to_storage_id
WHERE h.action = 'copy'
  AND h.file_status IN (0, 1)
  AND h.to_storage_id IS NOT NULL
  AND s_to.id IS NULL;        -- 找不到目的地 Storage
",
            transaction: tran,
            cancellationToken: ct
            ));
       
        // 🔹 撿一筆「目前最該跑的任務」→ 立刻改成 1，並直接輸出成 HistoryTask
        var task = await conn.QueryFirstOrDefaultAsync<HistoryTask>(
            new CommandDefinition(@"
;WITH P AS (
    SELECT TOP (1) h.id
    FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
    JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id 
    WHERE
            h.action = 'copy'
        AND (@group IS NULL OR s_from.set_group = @group)
        AND h.file_status IN (0, 800)
        -- ⭐ 有啟用 Node 篩選 → 只撿 assigned_node = 自己
        AND (
      @useNodeFilter = 0
   OR h.assigned_node = @nodeName
)
    ORDER BY 
        ISNULL(h.priority, 1) DESC,
        h.create_time ASC,
        h.id ASC
)
UPDATE h
SET h.file_status = 1,
    h.update_time = GETDATE()
OUTPUT 
    inserted.id              AS HistoryId,
    inserted.file_id         AS FileId,
    inserted.from_storage_id AS FromStorageId,
    inserted.to_storage_id   AS ToStorageId,
    f.filename               AS FileName,
    f.UserBit                AS UserBit,
    s_from.location          AS FromPath,
    s_to.location            AS ToPath,
    s_from.set_group         AS FromGroup,
    s_to.set_group           AS ToGroup,
    inserted.priority        AS Priority,
    CASE WHEN f.id IS NULL THEN 0 ELSE 1 END AS HasFileData
FROM dbo.FileData_History h
LEFT JOIN dbo.FileData   f      ON f.id      = h.file_id
JOIN dbo.Storage         s_from ON s_from.id = h.from_storage_id
LEFT JOIN dbo.Storage    s_to   ON s_to.id   = h.to_storage_id
JOIN P ON P.id = h.id;",
                new { group, nodeName, useNodeFilter },
        transaction: tran,
        cancellationToken: ct));

        tran.Commit();
        // 如果沒有撿到（task == null），SlotLoop 那邊就會去 sleep 一下
        return task;
    }
    catch (SqlException ex) when (ex.Number == 1205)
    {
        // 🧯 被 SQL 選為死結犧牲者 → 這輪當作沒撿到就好，避免整個 Slot 掛掉
        return null;
    }
    finally
    {
        _copyClaimLock.Release();
    }
}

        /// <summary>標記搬移成功：status='11'</summary>
        public async Task CompleteAsync(int historyId, CancellationToken ct)
        {
            using var conn = _factory.Create();

            const string sql = @"
DECLARE @now DATETIME = GETDATE();

-- 1) 更新 History：搬移成功
UPDATE dbo.FileData_History
SET file_status = '11',
    update_time = @now
WHERE id = @historyId;

-- 2) 更新 FileData
UPDATE f
SET f.file_status = '11'
FROM dbo.FileData f
JOIN dbo.FileData_History h ON f.id = h.file_id
WHERE h.id = @historyId;

-- 3) 嘗試更新 FileData_Storage 既有紀錄 (file_id + storage_id)
UPDATE s
SET 
    s.create_time = h.update_time,
    s.file_status = 11
FROM dbo.FileData_Storage s
JOIN dbo.FileData_History h
    ON s.file_id   = h.file_id
   AND s.storage_id = h.to_storage_id
WHERE h.id = @historyId
  AND h.to_storage_id IS NOT NULL
  AND h.action = 'copy';

-- 4) 如果上面一筆都沒更新到，就 INSERT 新的一筆
IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.FileData_Storage (file_id, storage_id, create_time, file_status)
    SELECT 
        h.file_id,
        h.to_storage_id   AS storage_id,
        h.update_time     AS create_time,
        11
    FROM dbo.FileData_History h
    WHERE h.id = @historyId
      AND h.to_storage_id IS NOT NULL
      AND h.action = 'copy';
END";

            await conn.ExecuteAsync(
                new CommandDefinition(sql, new { historyId }, cancellationToken: ct));
        }

        /// <summary>
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
        /// 歷史紀錄清單（成功＋失敗）
        /// </summary>
        public async Task<List<HistoryRow>> ListHistoryAsync(string status, int take, string? group, CancellationToken ct)
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
    h.assigned_node      AS AssignedNode,
    CAST(h.file_status AS int) AS Status
FROM dbo.FileData_History AS h
JOIN dbo.FileData   AS f       ON f.id = h.file_id
JOIN dbo.Storage    AS s_from  ON s_from.id = h.from_storage_id
LEFT JOIN dbo.Storage AS s_to  ON s_to.id   = h.to_storage_id
LEFT JOIN dbo.UserData AS u    ON u.id = h.user_id
WHERE h.file_status IN (
        11,12,          -- 成功
        14,17,          -- Phase1 完成、等待回遷
        901,902,903,        -- StorageId 無效
        91,92,999,      -- 失敗（其他）
        911,912,913,914,-- 搬移失敗細項
        921,922,923     -- 刪除失敗細項
    )
    AND (
        @group IS NULL
        OR s_from.set_group = @group
    )
ORDER BY h.update_time DESC, h.id DESC;";

            var rows = await conn.QueryAsync<HistoryRow>(
                new CommandDefinition(sql, new { n = take,group }, cancellationToken: ct));

            return rows.ToList();
        }

        /// <summary>
        /// 批次版：領取刪除任務
        /// </summary>
//         public async Task<List<HistoryTask>> ClaimDeleteAsync(
//             int batchSize,
//             int retryMinutes,
//             string? group,
//             CancellationToken ct)
//         {
//             using var conn = _factory.Create();
//             await (conn as DbConnection)!.OpenAsync(ct);
//             using var tran = (conn as DbConnection)!.BeginTransaction();

//             // 🔹 delete 任務：來源 StorageId 無效 → 901
//             await conn.ExecuteAsync(new CommandDefinition(@"
// UPDATE h
// SET h.file_status = 901,
//     h.update_time = GETDATE()
// FROM dbo.FileData_History h
// LEFT JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
// WHERE h.action = 'delete'
//   AND h.file_status IN (-1, 1)
//   AND s_from.id IS NULL;      -- 找不到來源 Storage
// ",
//                 transaction: tran, cancellationToken: ct));

//             var ids = await conn.QueryAsync<int>(
//                 new CommandDefinition(@"
// ;WITH P AS (
//   SELECT TOP (@n) h.id
//   FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
//   JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
//   WHERE
//         h.action = 'delete'
//     AND (@group IS NULL OR s_from.set_group = @group)
//     AND (
//             h.file_status = -1
//          OR (h.file_status = 1
//              AND DATEDIFF(MINUTE, h.update_time, GETDATE()) >= @retryMin)
//         )
//   ORDER BY 
//     ISNULL(h.priority, 1) DESC,
//     h.create_time ASC,
//     h.id ASC
// )
// UPDATE h
// SET h.file_status = 1,
//     h.update_time = GETDATE()
// OUTPUT inserted.id
// FROM dbo.FileData_History h
// JOIN P ON P.id = h.id;",
//                     new { n = batchSize, retryMin = retryMinutes, group },
//                     transaction: tran, cancellationToken: ct));

//             List<HistoryTask> tasks = new();
//             if (ids.Any())
//             {
//                 tasks = (await conn.QueryAsync<HistoryTask>(
//                     new CommandDefinition(@"
// SELECT 
//   h.id              AS HistoryId,
//   h.file_id         AS FileId,
//   h.from_storage_id AS FromStorageId,
//   h.to_storage_id   AS ToStorageId,
//   f.filename        AS FileName,
//   f.UserBit         AS UserBit,
//   s_from.location   AS FromPath,
//   s_to.location     AS ToPath,
//   s_from.set_group  AS FromGroup,
//   s_to.set_group    AS ToGroup,
//   CASE WHEN f.id IS NULL THEN 0 ELSE 1 END AS HasFileData
// FROM dbo.FileData_History h
// LEFT JOIN dbo.FileData f     ON f.id       = h.file_id
// JOIN dbo.Storage s_from      ON s_from.id  = h.from_storage_id
// LEFT JOIN dbo.Storage s_to   ON s_to.id    = h.to_storage_id
// WHERE h.id IN @ids;",
//                         new { ids }, transaction: tran, cancellationToken: ct))).ToList();
//             }

//             tran.Commit();
//             return tasks;
//         }

        /// <summary>
        /// slot 版：領取一筆刪除任務
        /// </summary>
        public async Task<HistoryTask?> ClaimDeleteTopOneAsync(
            int retryMinutes,
            string? group,
            CancellationToken ct)
        {
            using var conn = _factory.Create();
            await (conn as DbConnection)!.OpenAsync(ct);
            using var tran = (conn as DbConnection)!.BeginTransaction();
            // ⭐ 這台節點名稱
            var nodeName = _nodeName;
            var useNodeFilter = !string.IsNullOrWhiteSpace(nodeName);
            // 🔹 delete 任務：來源 StorageId 無效 → 901
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE h
SET h.file_status = 901,
    h.update_time = GETDATE()
FROM dbo.FileData_History h
LEFT JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
WHERE h.action = 'delete'
  AND h.file_status IN (-1, 1)
  AND s_from.id IS NULL;      -- 找不到來源 Storage
",
                transaction: tran, cancellationToken: ct));

            var id = await conn.ExecuteScalarAsync<int?>(
                new CommandDefinition(@"
;WITH P AS (
  SELECT TOP (1) h.id
  FROM dbo.FileData_History h WITH (UPDLOCK, READPAST, ROWLOCK)
  JOIN dbo.Storage s_from ON s_from.id = h.from_storage_id
  WHERE
        h.action = 'delete'
    AND (@group IS NULL OR s_from.set_group = @group)
    AND (
            h.file_status = -1
              OR h.file_status = 800
         OR (h.file_status = 1
             AND DATEDIFF(MINUTE, h.update_time, GETDATE()) >= @retryMin)
        )
            -- ⭐ Node 篩選：有設定 NodeName → 撿 NULL 或自己的
  AND (
      @useNodeFilter = 0
   OR h.assigned_node = @nodeName
)
  ORDER BY 
    ISNULL(h.priority, 1) DESC,
    h.create_time ASC,
    h.id ASC
)
UPDATE h
SET h.file_status = 1,
    h.update_time = GETDATE()
OUTPUT inserted.id
FROM dbo.FileData_History h
JOIN P ON P.id = h.id;",
                   new { retryMin = retryMinutes, group, nodeName, useNodeFilter },
            transaction: tran,
            cancellationToken: ct));

            if (!id.HasValue)
            {
                tran.Commit();
                return null;
            }

            var task = await conn.QuerySingleOrDefaultAsync<HistoryTask>(
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
  CASE WHEN f.id IS NULL THEN 0 ELSE 1 END AS HasFileData
FROM dbo.FileData_History h
LEFT JOIN dbo.FileData f     ON f.id       = h.file_id
JOIN dbo.Storage s_from      ON s_from.id  = h.from_storage_id
LEFT JOIN dbo.Storage s_to   ON s_to.id    = h.to_storage_id
WHERE h.id = @id;",
                    new { id },
        transaction: tran,
        cancellationToken: ct));

            tran.Commit();
            return task;
        }

        /// <summary>刪除成功：status='12'</summary>
        public async Task CompleteDeleteAsync(int historyId, CancellationToken ct)
        {
            using var conn = _factory.Create();

            const string sql = @"
DECLARE @now DATETIME = GETDATE();
DECLARE @fid INT;
DECLARE @sid INT;

-- 找到 file_id 和來源 storage_id
SELECT 
    @fid = file_id,
    @sid = from_storage_id
FROM dbo.FileData_History
WHERE id = @historyId;


-- 1) 更新 History：刪除成功 = 12
UPDATE dbo.FileData_History
SET file_status = '12',
    update_time = @now
WHERE id = @historyId;


-- 2) 直接移除來源 storage row
DELETE FROM dbo.FileData_Storage
WHERE file_id = @fid
  AND storage_id = @sid;
";

            await conn.ExecuteAsync(
                new CommandDefinition(sql, new { historyId }, cancellationToken: ct));
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

        /// <summary>
        /// 取得本樓層 RESTORE storage 的 id
        /// </summary>
        public async Task<int> GetRestoreStorageIdAsync(string group, CancellationToken ct)
        {
            using var conn = _factory.Create();

            var ids = (await conn.QueryAsync<int>(
                new CommandDefinition(@"
SELECT id
FROM dbo.Storage
WHERE set_group = @g
  AND [type] = 'RESTORE';",
                    new { g = group }, cancellationToken: ct))).ToList();

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
        /// 跨樓層搬運：階段一完成（已搬到本樓層 RESTORE），更新 file_status=14/17
        /// </summary>
        public async Task MarkPhase1DoneAsync(
            int historyId,
            int statusCode,
            CancellationToken ct)
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
        /// 重試機制：將失敗的紀錄狀態改回 0 / -1
        /// </summary>
        public async Task<bool> RetryAsync(int historyId, CancellationToken ct)
        {
            using var conn = _factory.Create();

            const string sql = @"
UPDATE dbo.FileData_History
SET file_status = CASE 
        WHEN action = 'delete' THEN -1   -- 刪除任務 → 回到 -1
        ELSE 0                           -- 其他（copy）→ 回到 0
    END,
    assigned_node = NULL,
    update_time = GETDATE()
WHERE id = @id
  AND file_status IN (
        91, 92, 999,901, 902,903,        -- 失敗（其他）
        911, 912, 913, 914,  -- 搬移失敗細項
        921, 922, 923        -- 刪除失敗細項
    );";

            var affected = await conn.ExecuteAsync(
                new CommandDefinition(sql, new { id = historyId }, cancellationToken: ct));

            return affected > 0;
        }

        /// <summary>
        /// 調整單筆 History 的 priority（1～10），delta 可為 +1 / -1
        /// 回傳更新後的 priority 值
        /// </summary>
        public async Task<int?> AdjustPriorityAsync(int historyId, int delta, CancellationToken ct)
        {
            using var conn = _factory.Create();

            const string sql = @"
UPDATE dbo.FileData_History
SET priority =
    CASE 
        WHEN priority IS NULL THEN 1 + @delta         -- 原本沒值就從 1 開始加
        WHEN priority + @delta > 10 THEN 10           -- 上限 10
        WHEN priority + @delta < 1  THEN 1            -- 下限 1
        ELSE priority + @delta
    END,
    update_time = GETDATE()
WHERE id = @id;

SELECT priority
FROM dbo.FileData_History
WHERE id = @id;";

            var newPri = await conn.ExecuteScalarAsync<int>(
                new CommandDefinition(sql, new { id = historyId, delta }, cancellationToken: ct));

            return newPri;
        }

// 重啟的時候 我要撿回1->0
        // 重啟的時候，把「進行中」的工作撿回來：
// - copy 任務：1 → 0
// - delete 任務：1 → -1
            public async Task ResetRunningJobsAsync(CancellationToken ct)
            {
                using var conn = _factory.Create();
                var nodeName = _nodeName;
                const string sql = @"
            UPDATE dbo.FileData_History
            SET 
                file_status = CASE 
                    WHEN action = 'delete' THEN -1   -- 刪除任務：回到 -1（待刪除）
                    ELSE 0                           -- 其他（目前就是 copy）：回到 0（待搬移）
                END,
                assigned_node = NULL, 
                update_time = GETDATE()
            WHERE file_status = 1
            AND action IN ('copy', 'delete')     -- 只處理這兩種任務
            AND (@nodeName IS NULL OR @nodeName = '' OR assigned_node = @nodeName);
            ";

                await conn.ExecuteAsync(new CommandDefinition(sql,  new { nodeName }, cancellationToken: ct));
            }
    }
    
}
