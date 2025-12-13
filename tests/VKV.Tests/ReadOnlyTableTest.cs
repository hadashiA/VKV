using System;
using System.Text;
using System.Threading.Tasks;
using VKV.Compression;

namespace VKV.Tests;

[TestFixture]
public class ReadOnlyTableTest
{
    [Test]
    public async Task Get_ByteKey()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Ascii,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                for (var i = 0; i < 1000; i++)
                {
                    builder.Append(
                        Encoding.ASCII.GetBytes($"key{i:D5}"),
                        Encoding.ASCII.GetBytes($"value{i:D5}"));
                }
            });

        using var result1 = await table.GetAsync("key00123"u8.ToArray());
        Assert.That(result1.HasValue, Is.True);
        Assert.That(result1.Value.Span.SequenceEqual("value00123"u8), Is.True);

        using var result2 = await table.GetAsync("aaaaa"u8.ToArray());
        Assert.That(result2.HasValue, Is.False);
    }

    [Test]
    public async Task Get_TypedKey()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Ascii,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                for (var i = 0; i < 100; i++)
                {
                    builder.Append($"key{i:D3}", Encoding.ASCII.GetBytes($"value{i:D3}"));
                }
            });

        using var result1 = table.Get("key050");
        Assert.That(result1.HasValue, Is.True);
        Assert.That(result1.Value.Span.SequenceEqual("value050"u8), Is.True);

        using var result2 = table.Get("notfound");
        Assert.That(result2.HasValue, Is.False);
    }

    [Test]
    public async Task Get_Int64Key()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Int64LittleEndian,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                for (var i = 0; i < 100; i++)
                {
                    builder.Append((long)i, Encoding.ASCII.GetBytes($"value{i:D3}"));
                }
            });

        using var result1 = await table.GetAsync(50L);
        Assert.That(result1.HasValue, Is.True);
        Assert.That(result1.Value.Span.SequenceEqual("value050"u8), Is.True);

        using var result2 = await table.GetAsync(999L);
        Assert.That(result2.HasValue, Is.False);
    }

    [Test]
    public async Task GetCompressed()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Ascii,
            databaseConfigure: builder =>
            {
                builder.PageSize = 128;
                builder.AddPageFilter(x => x.AddZstandardCompression());
            },
            tableConfigure: builder =>
            {
                for (var i = 0; i < 10; i++)
                {
                    builder.Append(
                        Encoding.ASCII.GetBytes($"k{i:D2}"),
                        Encoding.ASCII.GetBytes($"v{i:D2}"));
                }
            });

        using var result1 = await table.GetAsync("k02"u8.ToArray());
        Assert.That(result1.HasValue, Is.True);
        Assert.That(result1.Value.Span.SequenceEqual("v02"u8), Is.True);

        using var result2 = await table.GetAsync("aaaaa"u8.ToArray());
        Assert.That(result2.HasValue, Is.False);
    }

    [Test]
    public async Task GetRange_Between()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Ascii,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                for (var i = 0; i < 100; i++)
                {
                    builder.Append(
                        Encoding.ASCII.GetBytes($"key{i:D3}"),
                        Encoding.ASCII.GetBytes($"value{i:D3}"));
                }
            });

        // key050 ~ key060 (inclusive)
        using var result1 = await table.GetRangeAsync(
            "key050"u8.ToArray(),
            "key060"u8.ToArray());
        Assert.That(result1.Count, Is.EqualTo(11)); // 050, 051, ..., 060
    }

    [Test]
    public async Task GetRange_GreaterThan()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Ascii,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                for (var i = 0; i < 10; i++)
                {
                    builder.Append(
                        Encoding.ASCII.GetBytes($"key{i:D2}"),
                        Encoding.ASCII.GetBytes($"value{i:D2}"));
                }
            });

        // > key07
        using var result = await table.GetRangeAsync(
            "key07"u8.ToArray(),
            KeyRange.Unbound,
            startKeyExclusive: true,
            endKeyExclusive: false,
            SortOrder.Ascending);
        Assert.That(result.Count, Is.EqualTo(2)); // key08, key09
    }

    [Test]
    public async Task GetRange_LessThan()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Ascii,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                for (var i = 0; i < 10; i++)
                {
                    builder.Append(
                        Encoding.ASCII.GetBytes($"key{i:D2}"),
                        Encoding.ASCII.GetBytes($"value{i:D2}"));
                }
            });

        // < key03
        using var result = await table.GetRangeAsync(
            KeyRange.Unbound,
            "key03"u8.ToArray(),
            startKeyExclusive: false,
            endKeyExclusive: true,
            SortOrder.Ascending);
        Assert.That(result.Count, Is.EqualTo(3)); // key00, key01, key02
    }

    [Test]
    public async Task CountRange()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Ascii,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                for (var i = 0; i < 100; i++)
                {
                    builder.Append(
                        Encoding.ASCII.GetBytes($"key{i:D3}"),
                        Encoding.ASCII.GetBytes($"value{i:D3}"));
                }
            });

        var count = await table.CountRangeAsync(
            "key020"u8.ToArray(),
            "key030"u8.ToArray(),
            startKeyExclusive: false,
            endKeyExclusive: false);
        Assert.That(count, Is.EqualTo(11)); // 020, 021, ..., 030
    }

    [Test]
    public async Task GetRange_TypedKey()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Int64LittleEndian,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                for (var i = 0; i < 100; i++)
                {
                    builder.Append((long)i, Encoding.ASCII.GetBytes($"value{i:D3}"));
                }
            });

        using var result = table.GetRange(10L, 20L,
            startKeyExclusive: false,
            endKeyExclusive: false,
            SortOrder.Ascending);
        Assert.That(result.Count, Is.EqualTo(11)); // 10, 11, ..., 20
    }
}
