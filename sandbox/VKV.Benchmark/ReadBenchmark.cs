using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using CsSqlite;

namespace VKV.Benchmark;

class BenchmarkConfig : ManualConfig
{
    public BenchmarkConfig()
    {
        AddDiagnoser(MemoryDiagnoser.Default);
        AddJob(Job.ShortRun
            .WithWarmupCount(10)
            .WithIterationCount(10)
        );
    }
}

[Config(typeof(BenchmarkConfig))]
public class ReadBenchmark
{
    const int N = 10000;

    DirectoryInfo directory;
    ReadOnlyDatabase database;
    SqliteConnection sqliteConnection;

    string findKey = "key0000001234";

    [GlobalSetup]
    public async Task CreateDB()
    {
        directory = Directory.CreateTempSubdirectory("vkv_benchmarks");
        var sqlitePath = Path.Combine(directory.FullName, "bench.sqlite");
        var drydbPath = Path.Combine(directory.FullName, "bench.dry");

        // Setup sqlite
        using (var sqlite = new SqliteConnection(sqlitePath))
        {
            sqlite.Open();

            sqlite.ExecuteNonQuery("DROP TABLE IF EXISTS items;");

            sqlite.ExecuteNonQuery("PRAGMA page_size = 4096;");

            sqlite.ExecuteNonQuery(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER NOT NULL PRIMARY KEY,
                    data TEXT NOT NULL
                );
                """);

            for (var i = 0; i < N; i++)
            {
                sqlite.ExecuteNonQuery(
                    $"""
                     INSERT INTO items (id, data) VALUES ({i}, 'val{i:D10}');
                     """);
            }
        }

        // Setup  DryDB
        using var builder = new DatabaseBuilder
        {
            PageSize = 4096,
        };
        var tableBuilder = builder.CreateTable("items", KeyEncoding.Int64LittleEndian);
        for (var i = 0; i < N; i++)
        {
            tableBuilder.Append(i, Encoding.UTF8.GetBytes($"val{i:D10}"));
        }
        await builder.SaveToFileAsync(drydbPath);

        database = await ReadOnlyDatabase.OpenFileAsync(drydbPath, new DatabaseLoadOptions
        {
        });

        sqliteConnection = new SqliteConnection(sqlitePath);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        sqliteConnection.Dispose();
        database.Dispose();

        try
        {
            directory.Delete(true);
        }
        catch (DirectoryNotFoundException) { }
    }

    [Benchmark(Baseline = true)]
    public void DryDB_FindByKey()
    {
        for (var i = 0; i < 1000; i++)
        {
            var table = database.GetTable("items");
            using var _ = table.Get(123);
        }
    }

    [Benchmark]
    public void Sqlite_FindByKey()
    {
        for (var i = 0; i < 1000; i++)
        {
            using var command = sqliteConnection.CreateCommand(
                "SELECT data FROM items WHERE id = $id");

            command.Parameters.Add("$id", 123);
            using var reader = command.ExecuteReader();
            reader.Read();
            reader.GetString(0);
        }
    }
}
