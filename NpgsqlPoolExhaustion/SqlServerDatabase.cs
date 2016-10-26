using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace NpgsqlPoolExhaustion
{
    public class SqlServerDatabase
    {
        private readonly string _connectionString;

        public SqlServerDatabase(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task CreateTestTable(string tableName)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {tableName} (id int IDENTITY(1,1), message nvarchar(100) NOT NULL, created_at datetime NOT NULL);";
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task DropTable(string tableName)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"DROP TABLE {tableName};";
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task Save(string message, string tableName)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"insert into {tableName} (message, created_at) values (@message, getDate());";
                    command.Parameters.Add(new SqlParameter("message", DbType.AnsiString));
                    await command.ExecuteNonQueryAsync();
                }
            }
        }
    }
}
