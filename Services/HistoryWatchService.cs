using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using FileMoverWeb.Models;

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
            var batch = _cfg.GetValue<int>("Watcher:BatchSize", 20);

            _log.LogInformation("HistoryWatchService started: every {sec}s, batch {batch}", interval, batch);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var scope = _sp.CreateScope();
                    var repo = scope.ServiceProvider.GetRequiredService<HistoryRepository>();
                    var mover = scope.ServiceProvider.GetRequiredService<MoveWorker>();

                    var tasks = await repo.ClaimAsync(batch, stoppingToken);
                    if (tasks.Count == 0)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);
                        continue;
                    }

                    var jobId = Guid.NewGuid().ToString("N");
                    var req = new MoveBatchRequest
                    {
                        JobId = jobId,
                        Items = tasks.Select(t =>
                        {
                            // 優先用 UserBit.mxf；沒有 UserBit 時才使用原 FileName
                            var fileName = !string.IsNullOrWhiteSpace(t.UserBit)
                                ? $"{t.UserBit}.mxf"
                                : t.FileName;

                            var src = Path.Combine(t.FromPath, fileName);
                            var dst = Path.Combine(t.ToPath, fileName);

                            return new MoveItem
                            {
                                HistoryId = t.HistoryId,
                                FileId = t.FileId,
                                FromStorageId = t.FromStorageId,
                                ToStorageId = t.ToStorageId,
                                SourcePath = src,
                                DestPath = dst,
                                // DestId = t.ToStorageId.ToString()
                                DestId = $"{t.HistoryId}"
                            };
                        }).ToList()
};

                    var results = await mover.RunAsync(req, stoppingToken);

                    foreach (var r in results)
                    {
                        if (r.Success) await repo.CompleteAsync(r.HistoryId, stoppingToken);
                        // else await repo.FailAsync(r.HistoryId, stoppingToken);
                        else await repo.FailAsync(r.HistoryId, r.Error, stoppingToken);
                    }
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Watcher loop error");
                }

                await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);
            }
        }
    }
}
