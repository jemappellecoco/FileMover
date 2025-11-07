// Controllers/HistoryController.cs
using FileMoverWeb.Services;
using Microsoft.AspNetCore.Mvc;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FileMoverWeb.Controllers
{
   // Controllers/HistoryController.cs
    [ApiController]
    [Route("history")]
    public class HistoryController : ControllerBase
    {
        private readonly HistoryRepository _repo;
        public HistoryController(HistoryRepository repo) { _repo = repo; }

        // 只顯示成功/失敗（預設 200 筆，可用 take 覆寫）
        [HttpGet]
        public async Task<IActionResult> Get([FromQuery] string status = "all",
                                            [FromQuery] int take = 200,
                                            CancellationToken ct = default)
        {
            if (take <= 0) take = 50;
            if (take > 1000) take = 1000;

            // 這裡 status = all 時，repo 會只撈 10/90（見上方 whereStatus）
            var rows = await _repo.ListHistoryAsync(status, take, ct);

            var data = rows.Select(r => new
            {
                r.HistoryId,
                r.FileId,
                ProgramName = r.FileName ?? r.UserBit ?? string.Empty,
                FileName    = r.UserBit  ?? r.FileName ?? string.Empty,
                SourcePath  = (!string.IsNullOrWhiteSpace(r.FromPath) && !string.IsNullOrWhiteSpace(r.UserBit))
                                ? Path.Combine(r.FromPath!, $"{r.UserBit}.mxf") : null,
                DestPath    = (!string.IsNullOrWhiteSpace(r.ToPath) && !string.IsNullOrWhiteSpace(r.UserBit))
                                ? Path.Combine(r.ToPath!, $"{r.UserBit}.mxf") : null,
                r.FromStorageId,
                r.ToStorageId,
                r.UpdateTime,
                Status = r.Status,
                StatusText = r.Status == 11 ? "成功" : "失敗"
            });

            return Ok(data);
        }
    }

}
