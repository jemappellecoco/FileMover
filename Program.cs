using FileMoverWeb.Services;
using FileMoverWeb.Extensions;
using System.Text.Json.Serialization;

var builder = WebApplication.CreateBuilder(args);

// MVC + Swagger
builder.Services
    .AddControllers()
    .AddJsonOptions(o =>
    {
        o.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
    });

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerDocumentation();

// 取消 store
builder.Services.AddSingleton<ICancelStore, CancelStore>();

// 先把額外設定檔掛上去（可放在這裡或稍後，CreateBuilder 本來就會載 appsettings.*）
builder.Configuration
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", optional: true)
    // .AddJsonFile("wwwroot/dynamic-config.json", optional: true, reloadOnChange: true)
    .AddJsonFile("ftpsettings.json", optional: true, reloadOnChange: true);

// ===== Master / Slave 分開註冊 IJobProgress =====
var role = builder.Configuration["Cluster:Role"] ?? "Master";

if (string.Equals(role, "Master", StringComparison.OrdinalIgnoreCase))
{
    // Master 自己保存進度在記憶體
    builder.Services.AddSingleton<IJobProgress, JobProgress>();
}
else
{
    // Slave 不自己存進度，全部丟回 Master
    builder.Services.AddHttpClient<IJobProgress, RemoteJobProgress>();
}

// 其他 DI
builder.Services.AddTransient<MoveWorker>();
builder.Services.AddSingleton<IMoveRetryStore, MoveRetryStore>();

// CORS
builder.Services.AddCors(opt =>
{
    opt.AddPolicy("frontend", p => p
        .WithOrigins("http://localhost:5173")
        .AllowAnyHeader()
        .AllowAnyMethod()
        .AllowCredentials());
});

builder.Services.AddSingleton<DbConnectionFactory>();
builder.Services.AddScoped<HistoryRepository>();

// === 依 Role 註冊背景服務 ===
var watcherEnabled  = builder.Configuration.GetValue("Watcher:Enabled", true);
var allowMasterWork = builder.Configuration.GetValue("Cluster:AllowMasterWork", false);

// 所有節點都送心跳
builder.Services.AddHostedService<WorkerHeartbeatService>();

if (string.Equals(role, "Master", StringComparison.OrdinalIgnoreCase))
{
    // Master：只分配任務
    builder.Services.AddHostedService<MasterSchedulerService>();

    // 如需讓 Master 也執行搬移，才開這個
    if (watcherEnabled && allowMasterWork)
    {
        builder.Services.AddHostedService<HistoryWatchService>();
    }
}
else
{
    // Slave：負責搬檔
    if (watcherEnabled)
    {
        builder.Services.AddHostedService<HistoryWatchService>();
    }
}

// ⭐ Debug 印出目前設定
Console.WriteLine("=== CONFIG DEBUG ===");
Console.WriteLine("ENV              = " + builder.Environment.EnvironmentName);
Console.WriteLine("Cluster:Role     = " + builder.Configuration["Cluster:Role"]);
Console.WriteLine("Cluster:NodeName = " + builder.Configuration["Cluster:NodeName"]);
Console.WriteLine("Cluster:Group    = " + builder.Configuration["Cluster:Group"]);
Console.WriteLine("Watcher:Enabled  = " + watcherEnabled);
Console.WriteLine("AllowMasterWork  = " + allowMasterWork);
Console.WriteLine("====================");

var app = builder.Build();

// 重置卡住的任務
using (var scope = app.Services.CreateScope())
{
    var repo = scope.ServiceProvider.GetRequiredService<HistoryRepository>();
    await repo.ResetRunningJobsAsync(CancellationToken.None);
}

// Middlewares
app.UseSwaggerDocumentation();
app.UseCors("frontend");
app.UseDefaultFiles();
app.UseStaticFiles();
app.MapControllers();
app.MapFallbackToFile("/index.html");
app.Run();
