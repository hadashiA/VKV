using System;
using System.Text;
using System.Threading.Tasks;
using VKV.Compression;

namespace VKV.Tests;

[TestFixture]
public class ReadOnlyTableTest
{
    [Test]
    public async Task Get()
    {
        var table = await TestHelper.BuildTableAsync(
            databaseConfigure: builder =>
            {
                builder.PageSize = 128;
            },
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
    public async Task GetCompressed()
    {
        var table = await TestHelper.BuildTableAsync(
            databaseConfigure: builder =>
            {
                builder.PageSize = 128;
                builder.AddPageFilter(x =>
                {
                    x.AddZstandardCompression();
                });
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
}
