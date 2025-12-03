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

                // 檔名相關
                ProgramName = r.FileName ?? r.UserBit ?? string.Empty,
                FileName    = r.UserBit  ?? r.FileName ?? string.Empty,

                // ✅ 用 Storage.storage_name 當來源 / 目的地顯示
                SourceStorage = r.FromName,   // 例如：7F L2、4F Temp
                DestStorage   = r.ToName,     // 例如：IC1、watch_4F

                // 如需完整路徑（含檔名）給 CSV / tooltip 可保留
                SourcePath = (!string.IsNullOrWhiteSpace(r.FromPath) && !string.IsNullOrWhiteSpace(r.UserBit))
                               ? Path.Combine(r.FromPath!, $"{r.UserBit}.mxf")
                               : null,
                DestPath   = (!string.IsNullOrWhiteSpace(r.ToPath) && !string.IsNullOrWhiteSpace(r.UserBit))
                               ? Path.Combine(r.ToPath!, $"{r.UserBit}.mxf")
                               : null,
                r.FromStorageId,
                r.ToStorageId,
                r.UpdateTime,
                Status = r.Status,
                StatusText = r.Status switch
                {
                    11  => "搬移成功",
                    12  => "刪除成功",
                    91  => "搬移失敗（其他）",
                    92  => "刪除失敗（其他）",
                    14 or 17 => "等待回遷",
                    911 => "搬移失敗－找不到來源/檔案不存在",
                    912 => "搬移失敗－檔案使用中",
                    913 => "搬移失敗－權限不足",
                    914 => "搬移失敗－找不到目的地",
                    921 => "刪除失敗－找不到來源/檔案不存在",
                    922 => "刪除失敗－檔案使用中",
                    923 => "刪除失敗－權限不足",
                    999 => "使用者取消",
                    901 => "資料庫錯誤[From]",
                    902 => "資料庫錯誤[To]",
                    903 => "未設定restore錯誤",
                    _   => r.Status.ToString()
                }
            });

            return Ok(data);
        }

        [HttpPost("{id:int}/retry")]
            public async Task<IActionResult> Retry(int id, CancellationToken ct)
            {
                var ok = await _repo.RetryAsync(id, ct);
                if (!ok)
                {
                    return BadRequest(new
                    {
                        message = "此筆紀錄目前不能重試（可能不是錯誤狀態或已被處理）。"
                    });
                }

                return Ok(new
                {
                    historyId = id,
                    message = "已將此筆任務重新排入佇列。"
                });
            }
    }

}
