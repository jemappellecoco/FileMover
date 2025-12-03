// Services/JobProgress.cs
using System.Collections.Concurrent;
using System.Linq;
using FileMoverWeb.Models;
using System.Collections.Generic;
namespace FileMoverWeb.Services
{
    public interface IJobProgress
    {
        void InitTotals(string jobId, Dictionary<string, long> totalsByDest);
        void AddCopied(string jobId, string destId, long deltaBytes);
        void SetCurrentFile(string jobId, string destId, string fileName); 
        IReadOnlyList<TargetProgress> Snapshot(string jobId);
        void CompleteJob(string jobId);
        
    }

    public sealed class JobProgress : IJobProgress
    {
        // key = (jobId, destId)
        private readonly ConcurrentDictionary<(string jobId, string destId), TargetProgress> _map = new();

        public void InitTotals(string jobId, Dictionary<string, long> totalsByDest)
        {
            foreach (var kv in totalsByDest)
            {


                var key = (jobId: jobId, destId: kv.Key);
                _map.AddOrUpdate(
                    key,
                    _ => new TargetProgress { 
                        JobId = jobId, 
                        DestId = kv.Key, 
                        TotalBytes = kv.Value, 
                        CopiedBytes = 0 ,
                        CurrentFile = null
                        },
                    (_, exist) => { 
                        exist.TotalBytes = kv.Value; 
                        exist.CopiedBytes = 0;      // ⭐ 重點：重新起跑一定歸零
                        exist.CurrentFile = null;   // 可選：檔名也清掉
                        return exist; }
                );
            }
            //  Console.WriteLine($"[Progress] InitTotals job={jobId}, dests={string.Join(",", totalsByDest.Select(k => $"{k.Key}:{k.Value}"))}");
        }
          // ★ 新增：設定某個 jobId / destId 的「目前檔名」
        public void SetCurrentFile(string jobId, string destId, string fileName)
        {
            var key = (jobId: jobId, destId: destId);

            _map.AddOrUpdate(
                key,
                _ => new TargetProgress
                {
                    JobId       = jobId,
                    DestId      = destId,
                    TotalBytes  = 0,
                    CopiedBytes = 0,
                    CurrentFile = fileName
                },
                (_, exist) =>
                {
                    exist.CurrentFile = fileName;
                    return exist;
                }
            );
        }
        // 回報「增量 bytes」
        public void AddCopied(string jobId, string destId, long deltaBytes)
        {
            var key = (jobId: jobId, destId: destId);
            _map.AddOrUpdate(
                key,
                _ => new TargetProgress { JobId = jobId, DestId = destId, TotalBytes = 0, CopiedBytes = Math.Max(0, deltaBytes) },
                (_, exist) =>
                {
                    // 若已知總量 → 夾住不超過 TotalBytes
                    if (exist.TotalBytes > 0)
                        exist.CopiedBytes = Math.Min(exist.CopiedBytes + Math.Max(0, deltaBytes), exist.TotalBytes);
                    else
                        exist.CopiedBytes += Math.Max(0, deltaBytes);
                    return exist;
                }
            );
            // Console.WriteLine($"[Progress] AddCopied job={jobId}, dest={destId}, +{deltaBytes} bytes");
        }


        public IReadOnlyList<TargetProgress> Snapshot(string jobId)
        {
            return _map
                .Where(kv => kv.Key.jobId == jobId)
                .Select(kv => kv.Value)
                .OrderBy(tp => tp.DestId)
                .ToList();
        }

        public void CompleteJob(string jobId)
        {
            foreach (var key in _map.Keys.Where(k => k.jobId == jobId).ToList())
                _map.TryRemove(key, out _);
        }
        public IReadOnlyCollection<string> ActiveJobIds()
        {
            return _map.Keys.Select(k => k.jobId).Distinct().ToList();
        }
    }
}
