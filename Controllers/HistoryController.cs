// Controllers/HistoryController.cs
using FileMoverWeb.Services;
using Microsoft.AspNetCore.Mvc;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FileMoverWeb.Controllers
{
    [ApiController]
    [Route("history")]
    public class HistoryController : ControllerBase
    {
        private readonly HistoryRepository _repo;
        public HistoryController(HistoryRepository repo) { _repo = repo; }

        // GET /history?status=10|90|1|0|all&take=200
        [HttpGet]
        public async Task<IActionResult> Get([FromQuery] string status = "all",
                                             [FromQuery] int take = 200,
                                             CancellationToken ct = default)
        {
            var rows = await _repo.ListHistoryAsync(status, take, ct);

            var data = rows.Select(r => new
            {
                r.HistoryId,
                r.FileId,

                // HistoryRow 沒有 ProgramName → 用 FileName 或 UserBit 代替
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
                r.FromName,
                r.ToStorageId,
                r.ToName,

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
    }
}
