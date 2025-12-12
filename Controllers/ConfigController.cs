using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace FileMoverWeb.Controllers
{
    [ApiController]
    [Route("api/config")]
    public class ConfigController : ControllerBase
    {
        private readonly IWebHostEnvironment _env;

        public ConfigController(IWebHostEnvironment env)
        {
            _env = env;
        }

        // GET /api/config/concurrency
        // [HttpGet("concurrency")]
        // public IActionResult GetConcurrency()
        // {
        //     var path = Path.Combine(_env.ContentRootPath, "wwwroot/dynamic-config.json");
        //     var value = 2; // 預設值

        //     if (System.IO.File.Exists(path))
        //     {
        //         try
        //         {
        //             var text = System.IO.File.ReadAllText(path);
        //             using var doc = JsonDocument.Parse(text);

        //             if (doc.RootElement.TryGetProperty("GlobalMaxConcurrentMoves", out var ele))
        //             {
        //                 value = ele.GetInt32();
        //             }
        //         }
        //         catch
        //         {
        //             // 解析失敗就維持預設值 2
        //         }
        //     }

        //     return Ok(new { current = value });
        // }

        // POST /api/config/concurrency
        // [HttpPost("concurrency")]
        // public IActionResult UpdateConcurrency([FromBody] int value)
        // {
        //     if (value < 1 || value > 10)
        //         return BadRequest("value must be between 1–10");

        //     var path = Path.Combine(_env.ContentRootPath, "wwwroot/dynamic-config.json");

        //     var json = new
        //     {
        //         GlobalMaxConcurrentMoves = value
        //     };

        //     System.IO.File.WriteAllText(
        //         path,
        //         JsonSerializer.Serialize(json, new JsonSerializerOptions
        //         {
        //             WriteIndented = true
        //         }));

        //     return Ok(new { ok = true, current = value });
        // }
    }
}
