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
    
    public JobsController(IJobProgress progress, MoveWorker worker, HistoryRepository repo)
    {
        _progress = progress;
        _worker = worker;
        _repo = repo;
    }

    // 0) 列出 status=0/-1/1 的待處理任務：GET /jobs/pending
    [HttpGet("pending")]
    public async Task<IActionResult> GetPending([FromQuery] int take = 50, CancellationToken ct = default)
    {
        if (take <= 0) take = 50;
        if (take > 500) take = 500;

        var rows = await _repo.ListPendingAsync(take, ct);

        var data = rows.Select(x =>
        {
            // 以 Action 判斷任務類型（你的 repo 有 SELECT h.action AS Action）
            var kind = (x.Action ?? "").Trim().ToLowerInvariant() switch
            {
                "delete" or "刪除" => "delete",
                _ => "move"
            };

            return new
            {
                x.HistoryId,
                x.FileId,
                x.FromStorageId,
                x.ToStorageId,

                // 顯示名稱：優先 UserBit，再退回 FileName
                ProgramName = string.IsNullOrWhiteSpace(x.UserBit) ? x.FileName : x.UserBit,
                FileName    = x.UserBit ?? x.FileName,

                // 直接用 HistoryTask 的唯讀屬性（已自動組 .MXF）
                SourcePath  = x.FullSourcePath,
                DestPath    = x.FullDestPath,

                // 新增：任務類型給前端顯示/過濾用
                TaskKind    = kind,                // "move" 或 "delete"
                RequestedBy = x.RequestedBy ?? "-" // 誰發的
            };
        });

        return Ok(data);
    }

    // 歷史：GET /jobs/history?status=all|move|delete|success|fail|11|12|91|92
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

            ProgramName = r.FileName ?? r.UserBit ?? string.Empty,
            FileName    = r.UserBit  ?? r.FileName ?? string.Empty,

            SourcePath = (!string.IsNullOrWhiteSpace(r.FromPath) && !string.IsNullOrWhiteSpace(r.UserBit))
                ? Path.Combine(r.FromPath!, $"{r.UserBit}.MXF")
                : null,
            DestPath = (!string.IsNullOrWhiteSpace(r.ToPath) && !string.IsNullOrWhiteSpace(r.UserBit))
                ? Path.Combine(r.ToPath!, $"{r.UserBit}.MXF")
                : null,

            r.FromStorageId,
            r.ToStorageId,
            r.UpdateTime,

            Status = r.Status,
            // ★ 新增 12/92 的顯示文字
            StatusText = r.Status switch
            {
                12 => "刪除成功",
                92 => "刪除失敗",
                11 => "成功",
                91 => "失敗",
                1  => "進行中",
                0  => "待處理",
                _  => r.Status.ToString()
            },

            r.Error
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
                    DestId     = "Default"
                }
            }
        };

        _ = Task.Run(() => _worker.RunAsync(batch, ct)); // 背景執行

        return Ok(new { jobId, status = "Pending" });
    }

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

    [HttpGet("{jobId}/events")]
    public async Task GetEvents(string jobId, CancellationToken ct)
    {
        Response.Headers["Cache-Control"] = "no-cache";
        Response.Headers["X-Accel-Buffering"] = "no";
        Response.ContentType = "text/event-stream";

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

    [HttpPost("{jobId}/cancel")]
    public IActionResult Cancel(string jobId)
    {
        _progress.CompleteJob(jobId);
        return Ok(new { jobId, status = "Canceled" });
    }
}
