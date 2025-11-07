using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using FileMoverWeb.Models;
using FileMoverWeb.Services;

namespace FileMoverWeb.Services
{
    public sealed class HistoryWatchService : BackgroundService
    {
        private readonly ILogger<HistoryWatchService> _log;
        private readonly IServiceProvider _sp;
        private readonly IConfiguration _cfg;

        public HistoryWatchService(ILogger<HistoryWatchService> log, IServiceProvider sp, IConfiguration cfg)
        {
            _log = log; _sp = sp; _cfg = cfg;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var interval = _cfg.GetValue<int>("Watcher:IntervalSeconds", 5);
            var batch    = _cfg.GetValue<int>("Watcher:BatchSize", 20);

            _log.LogInformation("HistoryWatchService started: every {sec}s, batch {batch}", interval, batch);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var scope = _sp.CreateScope();
                    var repo  = scope.ServiceProvider.GetRequiredService<HistoryRepository>();
                    var mover = scope.ServiceProvider.GetRequiredService<MoveWorker>();

                    // 1) 搬移任務（status=0）
                    var moveTasks = await repo.ClaimAsync(batch, stoppingToken);

                    if (moveTasks.Count > 0)
                    {
                        var jobId = Guid.NewGuid().ToString("N");
                        var req = new MoveBatchRequest
                        {
                            JobId = jobId,
                            Items = moveTasks.Select(t =>
                            {
                                var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
                                    ? $"{t.UserBit}.MXF"
                                    : t.FileName;

                                var src = Path.Combine(t.FromPath, fileName);
                                var dst = Path.Combine(t.ToPath,   fileName);

                                return new MoveItem
                                {
                                    HistoryId     = t.HistoryId,
                                    FileId        = t.FileId,
                                    FromStorageId = t.FromStorageId,
                                    ToStorageId   = t.ToStorageId,
                                    SourcePath    = src,
                                    DestPath      = dst,
                                    DestId        = $"{t.HistoryId}"  // 以歷史ID分流，同顆碟的 I/O 限制由 MoveWorker 控制
                                };
                            }).ToList()
                        };

                        _log.LogInformation("Start move batch {jobId} with {count} items", jobId, req.Items.Count);

                        var results = await mover.RunAsync(req, stoppingToken);

                        foreach (var r in results)
                        {
                            if (r.Success)
                            {
                                await repo.CompleteAsync(r.HistoryId, stoppingToken); // 11 並同步 FileData.file_status=11
                                _log.LogInformation("[{hid}] Move success", r.HistoryId);
                            }
                            else
                            {
                                await repo.FailAsync(r.HistoryId, r.Error, stoppingToken); // 91 + error_msg
                                _log.LogWarning("[{hid}] Move failed: {err}", r.HistoryId, r.Error);
                            }
                        }
                    }

                    // 2) 刪除任務（status=-1）
                    var deleteTasks = await repo.ClaimDeleteAsync(batch, stoppingToken);

                    if (deleteTasks.Count > 0)
                    {
                        _log.LogInformation("Start delete batch with {count} items", deleteTasks.Count);

                        foreach (var t in deleteTasks)
                        {
                            try
                            {
                                // 刪除來源檔（若你要刪目的端，改成 t.ToPath）
                                var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
                                    ? $"{t.UserBit}.MXF"
                                    : t.FileName;

                                var src = Path.Combine(t.FromPath, fileName);

                                if (File.Exists(src))
                                {
                                    File.Delete(src);
                                    _log.LogInformation("[{hid}] Deleted file: {path}", t.HistoryId, src);
                                }
                                else
                                {
                                    _log.LogInformation("[{hid}] File not found when deleting: {path}", t.HistoryId, src);
                                    // 即使不存在，一般也視為刪除完成（依你的規則維持 12）
                                }

                                await repo.CompleteDeleteAsync(t.HistoryId, stoppingToken); // 12
                            }
                            catch (Exception ex)
                            {
                                _log.LogWarning(ex, "[{hid}] Delete failed", t.HistoryId);
                                await repo.FailDeleteAsync(t.HistoryId, ex.Message, stoppingToken); // 92
                            }
                        }
                    }

                    // 若本輪兩種任務都沒有，就睡一下
                    if (moveTasks.Count == 0 && deleteTasks.Count == 0)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    // app 關閉/取消
                    break;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Watcher loop error");
                    // 保持節流，避免狂刷錯
                    try { await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken); } catch { /* ignore */ }
                }
            }

            _log.LogInformation("HistoryWatchService stopped.");
        }
    }
}
