// Controllers/ProgressController.cs
using System.Text.Json;
using FileMoverWeb.Models;
using FileMoverWeb.Services;
using Microsoft.AspNetCore.Mvc;

namespace FileMoverWeb.Controllers;

[ApiController]
[Route("api/progress")]
public sealed class ProgressController : ControllerBase
{
    private readonly IJobProgress _progress;
    public ProgressController(IJobProgress progress) => _progress = progress;

    // 既有：查單一 job 的目標清單（每個目標對應一個 DestId）
    [HttpGet("{jobId}")]
    [Produces("application/json")]
    [ProducesResponseType(typeof(IEnumerable<TargetProgress>), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<IEnumerable<TargetProgress>> Get(string jobId)
        => Ok(_progress.Snapshot(jobId));

    // 私有：組裝所有活躍工作的彙總資料，給 GetAll() 與 SSE 共用
    private IEnumerable<object> BuildAllJobs()
    {
        if (_progress is not JobProgress jp)
            return Array.Empty<object>();

        var jobs = jp.ActiveJobIds();
        var list = new List<object>(capacity: Math.Max(1, jobs.Count));

        foreach (var jobId in jobs)
        {
            var targets = _progress.Snapshot(jobId);
            long total  = targets.Sum(t => t.TotalBytes);
            long copied = targets.Sum(t => t.CopiedBytes);
            int percent = total > 0 ? (int)Math.Clamp(copied * 100L / total, 0, 100) : 0;

            list.Add(new
            {
                jobId,
                total,
                copied,
                percent,
                targets = targets.Select(t => new
                {
                    destId  = t.DestId,
                    copied  = t.CopiedBytes,
                    total   = t.TotalBytes,
                    percent = t.TotalBytes > 0
                        ? Math.Min(100, Math.Max(0, (int)(t.CopiedBytes * 100L / t.TotalBytes)))
                        : 0,
                        currentFile = t.CurrentFile
                })
            });
        }

        return list;
    }


    // 全部工作彙總列表：給前端一次性拉取或除錯用
    [HttpGet]
    [Produces("application/json")]
    [ProducesResponseType(typeof(IEnumerable<object>), StatusCodes.Status200OK)]
    public ActionResult<IEnumerable<object>> GetAll()
        => Ok(BuildAllJobs());

    // SSE：持續推送全部工作的彙總進度
    [HttpGet("events")]
    public async Task GetAllEvents(CancellationToken ct)
    {
        Response.Headers["Cache-Control"] = "no-cache";
        Response.Headers["X-Accel-Buffering"] = "no";
        Response.ContentType = "text/event-stream";

        // 300ms 推一次即可，與 MoveWorker 的回報頻率相近
        const int intervalMs = 300;

        while (!ct.IsCancellationRequested)
        {
            var payload = JsonSerializer.Serialize(BuildAllJobs());
            // Console.WriteLine("[SSE] payload = " + payload);
            await Response.WriteAsync("event: progress\n", ct);
            await Response.WriteAsync($"data: {payload}\n\n", ct);
            // await Response.WriteAsync($"data: {payload}\n\n", ct); // 不要 event: progress
            await Response.Body.FlushAsync(ct);

            try
            {
                await Task.Delay(intervalMs, ct);
            }
            catch (TaskCanceledException)
            {
                // 正常結束
                break;
            }
        }
    }
}
