using FileMoverWeb.Services;
using FileMoverWeb.Extensions;
using System.Text.Json.Serialization;
var builder = WebApplication.CreateBuilder(args);

builder.WebHost.UseUrls("http://0.0.0.0:5089");
// MVC + Swagger
builder.Services
    .AddControllers()
    .AddJsonOptions(o =>
    {
        // 讓 enum 用 "Overwrite"/"Skip"/"Rename" 這種字串也能綁定
        o.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
    });
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerDocumentation();
builder.Services.AddSingleton<ICancelStore, CancelStore>();

// DI
builder.Services.AddSingleton<IJobProgress, JobProgress>();
builder.Services.AddTransient<MoveWorker>();
builder.Configuration
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddJsonFile("wwwroot/dynamic-config.json", optional: true, reloadOnChange: true)
    .AddJsonFile("ftpsettings.json", optional: true, reloadOnChange: true);
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

// builder.Services.AddHostedService<HistoryWatchService>();
// 加這段：用設定控制是否啟動背景搬運
var watcherEnabled = builder.Configuration.GetValue("Watcher:Enabled", false);
if (watcherEnabled)
{
    builder.Services.AddHostedService<HistoryWatchService>();
}
var app = builder.Build();

// Middlewares
app.UseSwaggerDocumentation();
app.UseCors("frontend");
app.UseDefaultFiles();
app.UseStaticFiles();
app.MapControllers();
app.MapFallbackToFile("/index.html");
app.Run();
