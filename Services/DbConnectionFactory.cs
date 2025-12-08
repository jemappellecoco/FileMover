// Services/DbConnectionFactory.cs
using System.Data.Common;
using System.Data.SqlClient;
// using Microsoft.Data.SqlClient;
namespace FileMoverWeb.Services
{
    public sealed class DbConnectionFactory
    {
        private readonly string _connStr;

        public DbConnectionFactory(IConfiguration cfg)
        {
            _connStr = cfg.GetConnectionString("Default")
                ?? throw new InvalidOperationException("Missing conn string 'Default'");
        }

        public DbConnection Create()
        {
            return new SqlConnection(_connStr);
        }
    }
}
