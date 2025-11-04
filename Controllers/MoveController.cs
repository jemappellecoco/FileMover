using Microsoft.AspNetCore.Mvc;
using FileMoverWeb.Models;
using FileMoverWeb.Services;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System;
namespace FileMoverWeb.Controllers;

[ApiController]
[Route("api/move")]
[Produces("application/json")]
public sealed class MoveController : ControllerBase
{
    private readonly MoveWorker _worker;
    private readonly IJobProgress _progress;

    public MoveController(MoveWorker worker, IJobProgress progress)
    {
        _worker = worker;
        _progress = progress;
    }

    // 共用：正規化目的路徑（若給的是資料夾就補上來源檔名）
    private static string NormalizeDestPath(string srcPath, string destPath)
    {
        bool looksDir = Directory.Exists(destPath) || destPath.EndsWith("\\") || destPath.EndsWith("/");
        return looksDir ? Path.Combine(destPath, Path.GetFileName(srcPath)) : destPath;
    }

    // 比較 (DestId, DestPath) 的自訂 comparer（忽略大小寫）
    private sealed class DestKeyComparer : IEqualityComparer<(string DestId, string DestPath)>
    {
        public bool Equals((string DestId, string DestPath) x, (string DestId, string DestPath) y) =>
            string.Equals(x.DestId, y.DestId, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(x.DestPath, y.DestPath, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode((string DestId, string DestPath) obj) =>
            HashCode.Combine(
                StringComparer.OrdinalIgnoreCase.GetHashCode(obj.DestId),
                StringComparer.OrdinalIgnoreCase.GetHashCode(obj.DestPath)
            );
    }

    [HttpPost("precheck")]
    [Consumes("application/json")]
    [ProducesResponseType(typeof(PrecheckResponse), StatusCodes.Status200OK)]
    public ActionResult<PrecheckResponse> Precheck([FromBody] PrecheckRequest req)
    {
        // 1) 先把所有目標路徑正規化
        var normalized = req.Items.Select(it => new MoveItem
        {
            SourcePath = it.SourcePath,
            DestPath   = NormalizeDestPath(it.SourcePath, it.DestPath),
            DestId     = it.DestId
        }).ToList();

        var conflicts = new List<ConflictItem>();

        // 2) 檢查同批次內是否有重覆目的路徑（相同 DestId + 相同最終路徑）
        var dupGroups = normalized
            .GroupBy(x => (x.DestId, x.DestPath), new DestKeyComparer())
            .Where(g => g.Count() > 1);

        foreach (var g in dupGroups)
        {
            foreach (var it in g)
            {
                // ✅ 這裡只用 SourcePath/DestPath，不要用不存在的 FromPath/UserBit/ToPath
                var srcFi = new FileInfo(it.SourcePath);
                var dstFi = new FileInfo(it.DestPath);

                conflicts.Add(new ConflictItem
                {
                    SourcePath    = it.SourcePath,
                    DestPath      = it.DestPath,
                    DestId        = it.DestId,
                    Kind          = ConflictKind.DuplicateInBatch,
                    ExistingSize  = dstFi.Exists ? dstFi.Length : (long?)null,
                    ExistingMtime = dstFi.Exists ? dstFi.LastWriteTimeUtc : (DateTime?)null,
                    SourceSize    = srcFi.Exists ? srcFi.Length : (long?)null,
                    SourceMtime   = srcFi.Exists ? srcFi.LastWriteTimeUtc : (DateTime?)null
                });
            }
        }

        // 3) 檢查目的端已有檔案
        foreach (var it in normalized)
        {
            var dstFi = new FileInfo(it.DestPath);
            if (dstFi.Exists)
            {
                var srcFi = new FileInfo(it.SourcePath);
                conflicts.Add(new ConflictItem
                {
                    SourcePath    = it.SourcePath,
                    DestPath      = it.DestPath,
                    DestId        = it.DestId,
                    Kind          = ConflictKind.ExistsOnDisk,
                    ExistingSize  = dstFi.Length,
                    ExistingMtime = dstFi.LastWriteTimeUtc,
                    SourceSize    = srcFi.Exists ? srcFi.Length : (long?)null,
                    SourceMtime   = srcFi.Exists ? srcFi.LastWriteTimeUtc : (DateTime?)null
                });
            }
        }

        return Ok(new PrecheckResponse
        {
            JobId = req.JobId,
            Conflicts = conflicts,
            NormalizedItems = normalized
        });
    }

    // 使用者已決策後，才真正開始搬運
    [HttpPost("batch/resolved")]
    [Consumes("application/json")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public ActionResult StartResolved([FromBody] MoveBatchResolvedRequest req)
    {
        // 1) 基本驗證：Rename 必須有 RenameTo
        foreach (var it in req.Items.Where(i => i.Decision == ConflictDecision.Rename))
        {
            if (string.IsNullOrWhiteSpace(it.RenameTo))
                return BadRequest(new { message = $"Rename 缺少 RenameTo：{it.SourcePath}" });
        }

        // 2) 套用決策後的最終目的路徑，並且只保留非 Skip
        var finalItems = req.Items
            .Where(x => x.Decision != ConflictDecision.Skip)
            .Select(x => new MoveItem
            {
                SourcePath = x.SourcePath,
                DestPath   = x.Decision == ConflictDecision.Rename ? x.RenameTo! : x.DestPath,
                DestId     = x.DestId
            })
            .ToList();

        // 3) 決策後再檢查一次是否仍有重覆目的路徑
        var dupFinal = finalItems
            .GroupBy(i => (i.DestId, i.DestPath), new DestKeyComparer())
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        if (dupFinal.Count > 0)
            return Conflict(new { message = "決策後仍有目的路徑衝突", keys = dupFinal });

        // 4) 送給 Worker 執行
        var batch = new MoveBatchRequest { JobId = req.JobId, Items = finalItems };
        _ = Task.Run(() => _worker.RunAsync(batch, HttpContext.RequestAborted));

        return Accepted(new { message = "started", jobId = req.JobId });
    }
}
