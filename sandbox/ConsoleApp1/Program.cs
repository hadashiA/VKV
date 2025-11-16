using System.Buffers;
using System.Text;
// using CsSqlite;
using VKV;

var directory = Directory.CreateTempSubdirectory("vkv_benchmarks");
// var sqlitePath = Path.Combine(directory.FullName, "bench.sqlite");
var drydbPath = Path.Combine(directory.FullName, "bench.vkv");

// Setup DryDB
using (var builder = new DatabaseBuilder
       {
           PageSize = 4096
       })
{
    var tableBuilder = builder.CreateTable("items", KeyEncoding.Int64LittleEndian);
    for (var i = 0; i < 1000; i++)
    {
        tableBuilder.Append(i, Encoding.UTF8.GetBytes($"{i:D10}"));
    }
    await builder.SaveToFileAsync(drydbPath);
    // var memoryStream = new MemoryStream();
    // await builder.SaveToStreamAsync(memoryStream);
    Console.WriteLine(drydbPath);
}

using (var database = await ReadOnlyDatabase.OpenFileAsync(drydbPath))
{
    var table = database.GetTable("items");

    while (true)
    {
        using var result = table.Get(123);
        // Console.WriteLine(Encoding.ASCII.GetString(result.Span));
    }
}

// using (var sqlite = new SqliteConnection(sqlitePath))
// {
//     sqlite.Open();
//
//     sqlite.ExecuteNonQuery("DROP TABLE IF EXISTS items;");
//
//     sqlite.ExecuteNonQuery("PRAGMA page_size = 4096;");
//
//     sqlite.ExecuteNonQuery(
//         """
//         CREATE TABLE IF NOT EXISTS items (
//             id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
//             data TEXT NOT NULL
//         );
//         """);
//
//     for (var i = 0; i < 1000; i++)
//     {
//         sqlite.ExecuteNonQuery(
//             $"""
//              INSERT INTO items (id, data) VALUES ({i}, '{i:D10}');
//              """);
//     }
// }
//
// using (var sqlite = new SqliteConnection(sqlitePath))
// {
//     using var command = sqlite.CreateCommand(
//         "SELECT data FROM items WHERE id = $id");
//
//     command.Parameters.Add("$id", 123);
//     using var reader = command.ExecuteReader();
//     reader.Read();
//     var result = reader.GetString(0);
//     Console.WriteLine(result);
// }
//
// Console.WriteLine($"{sqlitePath}");
