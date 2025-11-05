// Controllers/JobsController.cs
using System.IO;
using System.Linq;
using System.Text.Json.Serialization;
using FileMoverWeb.Models;
using FileMoverWeb.Services;
using Microsoft.AspNetCore.Mvc;

namespace FileMoverWeb.Controllers;

[ApiController]
[Route("jobs")]
[ApiExplorerSettings(IgnoreApi = true)]
public class JobsController : ControllerBase
{
    private readonly IJobProgress _progress;
    private readonly MoveWorker _worker;
    private readonly HistoryRepository _repo;
    
    // 修正點：把 HistoryRepository 正式注入進來
    public JobsController(IJobProgress progress, MoveWorker worker, HistoryRepository repo)
    {
        _progress = progress;
        _worker = worker;
        _repo = repo;
    }

    // 0) 列出 status=0 的待搬任務：GET /jobs/pending
    [HttpGet("pending")]
    public async Task<IActionResult> GetPending([FromQuery] int take = 50, CancellationToken ct = default)
    {
        if (take <= 0) take = 50;
        if (take > 500) take = 500;

        var rows = await _repo.ListPendingAsync(take, ct);

        var data = rows.Select(x => new
            {
                x.HistoryId,
                x.FileId,
                x.FromStorageId,
                x.ToStorageId,
                ProgramName = x.FileName,
                FileName = x.UserBit,
                SourcePath = x.FullSourcePath,   // 直接用唯讀屬性
                DestPath = x.FullDestPath
            });

        return Ok(data);
    }
    [HttpGet("history")]
    public async Task<IActionResult> GetHistory(
        [FromQuery] string status = "all",
        [FromQuery] int take = 200,
        CancellationToken ct = default)
    {
        var rows = await _repo.ListHistoryAsync(status, take, ct);

        var data = rows.Select(r => new
        {
            r.HistoryId,
            r.FileId,

            // 沒有 ProgramName → 用 FileName 或 UserBit 代替
            ProgramName = r.FileName ?? r.UserBit ?? string.Empty,
            FileName    = r.UserBit  ?? r.FileName ?? string.Empty,

            // 以現有欄位組完整路徑（FromPath/ToPath + UserBit.MXF）
            SourcePath = (!string.IsNullOrWhiteSpace(r.FromPath) && !string.IsNullOrWhiteSpace(r.UserBit))
                ? Path.Combine(r.FromPath!, $"{r.UserBit}.MXF")
                : null,
            DestPath = (!string.IsNullOrWhiteSpace(r.ToPath) && !string.IsNullOrWhiteSpace(r.UserBit))
                ? Path.Combine(r.ToPath!, $"{r.UserBit}.MXF")
                : null,

            r.FromStorageId,
            r.ToStorageId,
            r.UpdateTime,

            // Status 是 int，不能用 ?? "字串"
            Status = r.Status,
            StatusText = r.Status switch
            {
                10 => "成功",
                90 => "失敗",
                1  => "進行中",
                0  => "待處理",
                _  => r.Status.ToString()
            },

            Error = r.Error
        });

        return Ok(data);
}
    // 支援兩種命名：srcPath/dstPath 與 sourcePath/destObjectPath
    public sealed class CreateJobRequest
    {
        [JsonPropertyName("srcPath")] public string? SrcPath { get; init; }
        [JsonPropertyName("dstPath")] public string? DstPath { get; init; }

        [JsonPropertyName("sourcePath")] public string? SourcePath { get; init; }
        [JsonPropertyName("destObjectPath")] public string? DestObjectPath { get; init; }

        [JsonIgnore]
        public string Src => SrcPath ?? SourcePath
            ?? throw new ArgumentNullException(nameof(SrcPath), "請提供 srcPath 或 sourcePath");

        [JsonIgnore]
        public string Dst => DstPath ?? DestObjectPath
            ?? throw new ArgumentNullException(nameof(DstPath), "請提供 dstPath 或 destObjectPath");
    }

    // 建立單檔搬運（相容舊 /api/jobs）
    [HttpPost]
    public IActionResult Create([FromBody] CreateJobRequest req, CancellationToken ct)
    {
        if (!System.IO.File.Exists(req.Src))
            return BadRequest(new { message = "Source not found." });

        // 產生新的 jobId，包成「批次請求」送給新版 MoveWorker
        var jobId = Guid.NewGuid().ToString("N");
        var batch = new MoveBatchRequest
        {
            JobId = jobId,
            Items = new List<MoveItem>
            {
                new MoveItem
                {
                    SourcePath = req.Src,
                    DestPath   = req.Dst,
                    DestId     = "Default" // 統一到單一目的地群組
                }
            }
        };

        _ = Task.Run(() => _worker.RunAsync(batch, ct)); // 背景執行

        return Ok(new { jobId, status = "Pending" });
    }

    // 查詢單檔進度（把新版的「依目的地多筆」聚合成一筆回傳）
    [HttpGet("{jobId}")]
    public IActionResult Get(string jobId)
    {
        var list = _progress.Snapshot(jobId);
        if (list == null || list.Count == 0)
            return NotFound();

        long total = list.Sum(x => x.TotalBytes);
        long copied = list.Sum(x => x.CopiedBytes);
        int percent = total > 0 ? (int)Math.Clamp(copied * 100L / total, 0, 100) : 0;

        string status = "Pending";
        if (total > 0 && copied > 0 && copied < total) status = "Running";
        if (total > 0 && copied >= total)             status = "Completed";

        return Ok(new
        {
            jobId,
            bytesCopied = copied,
            totalBytes = total,
            percent,
            status
        });
    }

    // SSE 事件（相容舊前端）：根據聚合結果推送事件
    [HttpGet("{jobId}/events")]
    public async Task GetEvents(string jobId, CancellationToken ct)
    {
        Response.Headers["Cache-Control"] = "no-cache";
        Response.Headers["X-Accel-Buffering"] = "no";
        Response.ContentType = "text/event-stream";

        // 簡易版本：每 300ms 取一次 snapshot
        while (!ct.IsCancellationRequested)
        {
            var list = _progress.Snapshot(jobId);
            if (list == null || list.Count == 0)
            {
                await Task.Delay(300, ct);
                continue;
            }

            long total = list.Sum(x => x.TotalBytes);
            long copied = list.Sum(x => x.CopiedBytes);
            int percent = total > 0 ? (int)Math.Clamp(copied * 100L / total, 0, 100) : 0;

            string status = "progress";
            if (total > 0 && copied >= total) status = "completed";

            await Response.WriteAsync($"event: {status}\n", ct);
            await Response.WriteAsync(
                $"data: {{\"jobId\":\"{jobId}\",\"bytes\":{copied},\"total\":{total},\"percent\":{percent}}}\n\n",
                ct);
            await Response.Body.FlushAsync(ct);

            if (status == "completed") break;
            await Task.Delay(300, ct);
        }
    }

    // 取消（相容舊前端）：這裡做簡單清理
    [HttpPost("{jobId}/cancel")]
    public IActionResult Cancel(string jobId)
    {
        _progress.CompleteJob(jobId); // 簡單移除快照；如需真正取消，另行加上 CancellationToken 管理
        return Ok(new { jobId, status = "Canceled" });
    }
}
