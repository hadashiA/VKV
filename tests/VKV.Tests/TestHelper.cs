using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using VKV.BTree;
using VKV.Internal;
using VKV.Storages;

namespace VKV.Tests;

static class TestHelper
{
    public static async ValueTask<TreeWalker> BuildTreeAsync(KeyValueList keyValues, int pageSize)
    {
        var memoryStream = new MemoryStream();
        var pos = await TreeBuilder.BuildToAsync(
            memoryStream,
            pageSize,
            keyValues);

        var result = memoryStream.ToArray();
        var storage = new InMemoryStorage(result.ToArray(), pageSize);
        var pageCache = new PageCache(storage, 8);
        return new TreeWalker(pos, pageCache, KeyEncoding.Ascii);
    }

    public static async ValueTask<ReadOnlyTable> BuildTableAsync(
        KeyEncoding keyEncoding = KeyEncoding.Ascii,
        DatabaseLoadOptions? loadOptions = null,
        Action<DatabaseBuilder>? databaseConfigure = null,
        Action<TableBuilder>? tableConfigure = null)
    {
        var builder = new DatabaseBuilder();
        databaseConfigure?.Invoke(builder);

        var tableBuilder = builder.CreateTable("items", keyEncoding);
        tableConfigure?.Invoke(tableBuilder);

        var memoryStream = new MemoryStream(1024);
        await builder.SaveToStreamAsync(memoryStream);

        memoryStream.Seek(0, SeekOrigin.Begin);
        var database = await ReadOnlyDatabase.OpenAsync(memoryStream, loadOptions);
        return database.GetTable("items");
    }

    public static KeyValueList CreateKeyValues(int size, KeyEncoding keyEncoding = KeyEncoding.Ascii)
    {
        var keyValues = new KeyValueList(keyEncoding);
        if (keyEncoding == KeyEncoding.Int64LittleEndian)
        {
            for (var i = 0; i < size; i++)
            {
                keyValues.Add(i, "value01"u8.ToArray());
            }
        }
        else
        {
            for (var i = 0; i < size; i++)
            {
                keyValues.Add("key01"u8.ToArray(), "value01"u8.ToArray());
            }
        }
        return keyValues;
    }

    static void AddUtf8KeyValues(TableBuilder tableBuilder, int size)
    {
        for (var i = 0; i < size; i++)
        {
            var key = $"key{i}";
            var value = $"value{i}";

            tableBuilder.Append(
                Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(value));
        }
    }
}