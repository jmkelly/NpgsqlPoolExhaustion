using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace NpgsqlPoolExhaustion
{
    public class XunitTests : IDisposable
    {
        private readonly string _tableName;

        public XunitTests()
        {
            _tableName = "xxx";
            NpgsqlConnection.ClearAllPools();
        }

        [Theory]
        //[InlineData(70, "server=localhost;port=5433;database=rebus2_test;user id=test;password=test")]
        //[InlineData(60, "server=localhost;port=5433;database=rebus2_test;user id=test;password=test;Pooling=false")]
        //[InlineData(101,"server=localhost;port=5433;database=rebus2_test;user id=test;password=test;Maximum Pool Size=1000")]
        //[InlineData(50, "server=localhost;port=5433;database=rebus2_test;user id=test;password=test;Maximum Pool Size=100; Connection Idle Lifetime=1; Connection Pruning Interval 0.5;")]
        [InlineData(95, "server=localhost;port=5433;database=rebus2_test;user id=test;password=test;Maximum Pool Size=1;")]
        [InlineData(95, "server=localhost;port=5433;database=rebus2_test;user id=test;password=test;Maximum Pool Size=1;")]
        public async Task SaveABunchOfData(int messageCount, string connectionString)
        {

            var db = new PostgreSqlDatabase(GetConnectionStringForDatabase("rebus2_test"));
            await db.CreateTestTable(_tableName);

            await Task.WhenAll(Enumerable.Range(0, messageCount)
                .Select(async i =>
                {
                    await db.Save($"THIS IS MESSAGE {i}", _tableName);

                }));
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            return Environment.GetEnvironmentVariable("REBUS_POSTGRES")
                   ?? $"server=localhost; database={databaseName}; user id=postgres; password=postgres;";
        }

        public void Dispose()
        {  
            var db = new PostgreSqlDatabase(GetConnectionStringForDatabase("rebus2_test"));
            db.DropTable(_tableName);
            NpgsqlConnection.ClearAllPools();
        }
    }
}
