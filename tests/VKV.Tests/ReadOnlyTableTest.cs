using System;
using System.Text;
using System.Threading.Tasks;

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
}
