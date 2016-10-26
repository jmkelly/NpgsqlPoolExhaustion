using System.Threading.Tasks;
using NpgsqlTypes;

namespace NpgsqlPoolExhaustion
{
    public class PostgreSqlDatabase
    {
        private readonly string _connectionString;

        public PostgreSqlDatabase(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task CreateTestTable(string tableName)
        {
            using (var connection = new Npgsql.NpgsqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {tableName} (id serial, message text NOT NULL, created_at timestamp with time zone NOT NULL);";
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task DropTable(string tableName)
        {
            using (var connection = new Npgsql.NpgsqlConnection(_connectionString))
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
            using (var connection = new Npgsql.NpgsqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"insert into {tableName} (message, created_at) values (@message, clock_timestamp());";
                    command.Parameters.Add("message", NpgsqlDbType.Text).Value = message;
                    await command.ExecuteNonQueryAsync();
                }
            }
        }
        
    }
}