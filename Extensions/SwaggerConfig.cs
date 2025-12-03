// Extensions/SwaggerConfig.cs
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.Filters;
using System.Reflection;
using FileMoverWeb.Controllers;

namespace FileMoverWeb.Extensions
{
    public static class SwaggerConfig
    {
        /// <summary>統一註冊 Swagger（在 Program.cs 呼叫）</summary>
        public static IServiceCollection AddSwaggerDocumentation(this IServiceCollection services)
        {
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo
                {
                    Title = "FileMover API",
                    Version = "v1",
                    Description = "多檔搬運任務（依 DestId 分組進度）",
                    Contact = new OpenApiContact
                    {
                        Name = "Stonebooks Studio",
                        Email = "support@stonebooks.tw"
                    }
                });

                // XML 註解（需在 .csproj 開啟 GenerateDocumentationFile）
                var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
                var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
                if (File.Exists(xmlPath))
                    c.IncludeXmlComments(xmlPath);

                // 啟用 Example 支援（若無 Example Provider 也不會出錯）
                c.ExampleFilters();
            });

            // 🔧 這裡要改成「非 static 類別」，例如任何一支 Controller
            services.AddSwaggerExamplesFromAssemblyOf<MoveController>();

            return services;
        }

        /// <summary>統一啟用 Swagger UI（在 Program.cs 呼叫）</summary>
        public static IApplicationBuilder UseSwaggerDocumentation(this IApplicationBuilder app)
        {
            app.UseSwagger();
            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "FileMover v1");
                c.RoutePrefix = "swagger";          // 想改成根路徑就設 ""
                c.DocumentTitle = "FileMover API Docs";
                c.DisplayRequestDuration();         // 顯示請求時間
                c.EnableFilter();                   // 支援搜尋
            });

            return app;
        }
    }
}
