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
    private readonly IMoveRetryStore _retryStore;
    private readonly IConfiguration _cfg; 
    public JobsController(IJobProgress progress, MoveWorker worker, HistoryRepository repo,IMoveRetryStore retryStore,IConfiguration cfg )
    {
        _progress = progress;
        _worker = worker;
        _repo = repo;
        _retryStore = retryStore;
         _cfg = cfg;
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
            //  從記憶體的 retry store 抓目前的重試狀態
                int retryCount = 0;
                int? retryCode = null;
                string? retryMessage = null;

                if (_retryStore.TryGet(x.HistoryId, out var info))
                {
                    retryCount   = info.FailCount;
                    retryCode    = info.LastStatusCode;
                    retryMessage = info.LastError;
                }
            return new
            {
                x.HistoryId,
                x.FileId,
                x.FromStorageId,
                x.ToStorageId,
                
                // 顯示名稱：優先 UserBit，再退回 FileName
                ProgramName = x.FileName ?? x.UserBit ?? string.Empty,
                FileName    = x.UserBit ?? x.FileName,

                //
                SourceStorage = x.FromName,
                DestStorage   = x.ToName,
                // 新增：任務類型給前端顯示/過濾用
                TaskKind    = kind,                // "move" 或 "delete"
                RequestedBy = x.RequestedBy ?? "-", // 誰發的
                 SourceGroup = x.FromGroup,
                 // ⭐ 新增：讓前端知道現在是 0 / 1 / 24 / 27 / -1
                Status      = x.FileStatus,
                 priority = x.Priority,
                Tag = (x.FileStatus == 24 || x.FileStatus == 27)
                ? "回遷任務"
                : null,
                //  新增：給前端顯示「正在重試、第幾次、錯誤原因」
            RetryCount   = retryCount,
            RetryCode    = retryCode,
            RetryMessage = retryMessage,
            AssignedNode = x.AssignedNode
            
            };
        });

        return Ok(data);
    }
    // 手動取消：直接寫入 999（User canceled）
   [HttpPost("{historyId}/cancel-hard")]
        public async Task<IActionResult> CancelHard(
            int historyId,
            [FromServices] HistoryRepository repo,
            [FromServices] ICancelStore cancelStore)
        {
    // 設這筆為「要取消」
    cancelStore.Cancel(historyId);

    // 直接在 DB 標記 999
    await repo.FailAsync(historyId, 999, "User canceled", CancellationToken.None);

    // 把進度清掉
    _progress.CompleteJob(historyId.ToString());

    return Ok(new { historyId, status = 999, message = "Canceled by user" });
}

    // 歷史：GET /jobs/history?status=all|move|delete|success|fail|11|12|91|92
    [HttpGet("history")]
    public async Task<IActionResult> GetHistory(
        [FromQuery] string status = "all",
        [FromQuery] int take = 200,
        CancellationToken ct = default)
    {    
        var group = _cfg.GetValue<string>("FloorRouting:Group");   // e.g. "4F" 或 "7F"
         var rows = await _repo.ListHistoryAsync(status, take, group, ct);

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
             AssignedNode = r.AssignedNode, 

            r.Error
        });

        return Ok(data);
    }
// Phase2：列出等待回遷
// Controllers/JobsController.cs

[HttpGet("phase2-pending")]
public async Task<IActionResult> GetPhase2Pending(
    [FromQuery] int take = 200,
    CancellationToken ct = default)
{
    if (take <= 0) take = 200;
    if (take > 500) take = 500;

    var rows = await _repo.ListPhase2PendingAsync(take, ct);

    // ⭐ 從 DB 先查出 7F / 4F 的 RESTORE「名稱」
    var restore7F = await _repo.GetRestoreNameAsync("7F", ct);  // group = 7F, type = RESTORE
    var restore4F = await _repo.GetRestoreNameAsync("4F", ct);  // group = 4F, type = RESTORE

    var data = rows.Select(x =>
    {
        // ⭐ 顯示用來源名稱：
        // 14 → 顯示「7F 的 RESTORE 名稱」
        // 17 → 顯示「4F 的 RESTORE 名稱」
        // 其他 → 顯示原本 FromName
        string sourceName = x.FileStatus switch
        {
            14 => restore7F ?? x.FromName ?? "",
            17 => restore4F ?? x.FromName ?? "",
            _  => x.FromName ?? ""
        };

        return new
        {
            x.HistoryId,
            x.FileId,
            x.FromStorageId,
            x.ToStorageId,

            ProgramName = x.FileName ?? x.UserBit ?? string.Empty,
            FileName    = x.UserBit ?? x.FileName,

            // ✅ 給 HTML 顯示的：只用「名稱」，不是路徑
            SourceStorage = sourceName,       // ← 這就是你說的「找到的 name」
            DestStorage   = x.ToName ?? "",   // 目的地一樣顯示 storage_name

            // ✅ 下面兩個只是給搬運程式用，前端不要拿來顯示就好
            SourcePath  = x.FullSourcePath,   // 實際路徑（不顯示）
            DestPath    = x.FullDestPath,     // 實際路徑（不顯示）

            RequestedBy = x.RequestedBy ?? "-",
            FileStatus  = x.FileStatus
        };
    });

    return Ok(data);
}


// Phase2：啟動回遷（把選到的變成 status=0）
public sealed class Phase2StartRequest
{
    public List<int> HistoryIds { get; init; } = new();
}

[HttpPost("phase2/start")]
public async Task<IActionResult> StartPhase2(
    [FromBody] Phase2StartRequest req,
    CancellationToken ct = default)
{
    if (req.HistoryIds == null || req.HistoryIds.Count == 0)
        return BadRequest(new { message = "請至少勾選一筆回遷任務" });

    await _repo.MarkPhase2ToReadyAsync(req.HistoryIds.ToArray(), ct);

    return Ok(new { count = req.HistoryIds.Count, message = "已送出回遷，稍後由背景服務處理" });
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

   public sealed class PriorityRequest
{
    public int HistoryId { get; set; }   // 要調整哪一筆
    public int Delta     { get; set; }   // +1 或 -1
}

[HttpPost("priority")]
public async Task<IActionResult> ChangePriority(
    [FromBody] PriorityRequest req,
    CancellationToken ct = default)
{
    if (req == null || req.HistoryId <= 0)
        return BadRequest(new { message = "HistoryId 不可為空" });

    var newPri = await _repo.AdjustPriorityAsync(req.HistoryId, req.Delta, ct);

    if (!newPri.HasValue)
        return NotFound(new { message = $"找不到 HistoryId={req.HistoryId}" });

    return Ok(new
    {
        historyId = req.HistoryId,
        priority  = newPri.Value
    });
}

}
