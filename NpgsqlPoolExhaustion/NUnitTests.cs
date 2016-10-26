using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NUnit.Framework;

namespace NpgsqlPoolExhaustion
{
    [TestFixture]
    public class NUnitTests
    {
        private string _tableName;

        [SetUp]
        public void Initialise()
        {
            _tableName = "xxx";
        }

        [TearDown]
        public async Task Destroy()
        {
            var db = new PostgreSqlDatabase(GetConnectionStringForDatabase("rebus2_test"));
            await db.DropTable(_tableName);
        }

        //[TestCase(98,"server=localhost;port=5433;database=rebus2_test;user id=test;password=test")]
        //[TestCase(98,"server=localhost;port=5433;database=rebus2_test;user id=test;password=test;Pooling=false")]
        //[TestCase(10000,"server=localhost;port=5433;database=rebus2_test;user id=test;password=test;Maximum Pool Size=100000")]
        //[TestCase(98,"server=localhost;port=5433;database=rebus2_test;user id=test;password=test;Maximum Pool Size=1000; Connection Idle Lifetime=1; Connection Pruning Interval 0.5;")]
        [TestCase(95,"server=localhost;port=5433;database=rebus2_test;user id=test;password=test; maximum pool size=1;")]
        [TestCase(101,"server=localhost;port=5433;database=rebus2_test;user id=test;password=test; maximum pool size=1;")]
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
    }
}

