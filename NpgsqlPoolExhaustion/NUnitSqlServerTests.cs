using System;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace NpgsqlPoolExhaustion
{
    [TestFixture]
    public class NUnitSqlServerTests

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
            var db = new SqlServerDatabase(GetConnectionStringForDatabase("rebus_test"));
            await db.DropTable(_tableName);
        }

        [TestCase(95, "server=.; database=rebus_test;trusted_connection=true;")]
        [TestCase(96, "server=.; database=rebus_test;trusted_connection=true;")]
  //      [TestCase(200000, "server=.; database=rebus_test;trusted_connection=true;")]
        public async Task SaveABunchOfData(int messageCount, string connectionString)
        {

            var db = new SqlServerDatabase(GetConnectionStringForDatabase("rebus_test"));
            await db.CreateTestTable(_tableName);

            await Task.WhenAll(Enumerable.Range(0, messageCount)
                .Select(async i =>
                {
                    await db.Save($"THIS IS MESSAGE {i}", _tableName);

                }));
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            return Environment.GetEnvironmentVariable("REBUS_SQLSERVER")
                   ?? $"server=.; database={databaseName}; trusted_connection=true";
        }
    }
}

