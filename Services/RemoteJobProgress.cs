// Services/RemoteJobProgress.cs
using System.Net.Http.Json;
using FileMoverWeb.Models;
using Microsoft.Extensions.Configuration;

namespace FileMoverWeb.Services
{
    /// <summary>
    /// Slave 上用的 IJobProgress 實作：
    /// 不在本機記進度，而是用 HTTP 回報給 Master。
    /// </summary>
    public sealed class RemoteJobProgress : IJobProgress
    {
        private readonly HttpClient _http;
        private readonly string _masterBaseUrl;

        public RemoteJobProgress(HttpClient http, IConfiguration cfg)
        {
            _http = http;
            _masterBaseUrl = cfg["Cluster:MasterBaseUrl"]
                             ?? "http://127.0.0.1:5089";
        }

        public void InitTotals(string jobId, Dictionary<string, long> totalsByDest)
        {
            var dto = new
            {
                JobId = jobId,
                TotalsByDest = totalsByDest
            };
            _ = _http.PostAsJsonAsync($"{_masterBaseUrl}/api/progress/report/init", dto);
        }

        public void AddCopied(string jobId, string destId, long deltaBytes)
        {
            var dto = new
            {
                JobId = jobId,
                DestId = destId,
                DeltaBytes = deltaBytes
            };
            _ = _http.PostAsJsonAsync($"{_masterBaseUrl}/api/progress/report/delta", dto);
        }

        public void CompleteJob(string jobId)
        {
            var dto = new { JobId = jobId };
            _ = _http.PostAsJsonAsync($"{_masterBaseUrl}/api/progress/report/complete", dto);
        }

        // ★ 如果你的介面裡有 SetCurrentFile，就補實作
        public void SetCurrentFile(string jobId, string destId, string fileName)
        {
            var dto = new
            {
                JobId = jobId,
                DestId = destId,
                FileName = fileName
            };
            _ = _http.PostAsJsonAsync($"{_masterBaseUrl}/api/progress/report/file", dto);
        }

        // ★ Snapshot 在 Slave 其實不會被用到（前端只連 Master），可以回空清單
        public IReadOnlyList<TargetProgress> Snapshot(string jobId)
        {
            return Array.Empty<TargetProgress>();
        }
    }
}
