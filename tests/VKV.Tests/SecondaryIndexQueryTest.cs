using System.Text;
using System.Threading.Tasks;

namespace VKV.Tests;

[TestFixture]
public class SecondaryIndexQueryTest
{
    [Test]
    public async Task Get()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Ascii,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                builder.AddSecondaryIndex("category", true, KeyEncoding.Ascii, (key, value) =>
                {
                    var keyStr = Encoding.ASCII.GetString(key.Span);
                    return Encoding.ASCII.GetBytes($"category:{keyStr}");
                });

                for (var i = 0; i < 1000; i++)
                {
                    builder.Append(
                        Encoding.ASCII.GetBytes($"key{i:D5}"),
                        Encoding.ASCII.GetBytes($"value{i:D5}"));
                }
            });

        var query = table.WithIndex("category");
        var result = query.Get("category:key00010");
        Assert.That(result.HasValue, Is.True);
        Assert.That(Encoding.ASCII.GetString(result.Value.Span), Is.EqualTo("value00010"));
    }

    [Test]
    public async Task GetAsync()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Ascii,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                builder.AddSecondaryIndex("category", true, KeyEncoding.Ascii, (key, value) =>
                {
                    var keyStr = Encoding.ASCII.GetString(key.Span);
                    return Encoding.ASCII.GetBytes($"category:{keyStr}");
                });

                for (var i = 0; i < 1000; i++)
                {
                    builder.Append(
                        Encoding.ASCII.GetBytes($"key{i:D5}"),
                        Encoding.ASCII.GetBytes($"value{i:D5}"));
                }
            });

        var seconaryIndex = table.WithIndex("category");
        var result = await seconaryIndex.GetAsync("category:key00010");
        Assert.That(result.HasValue, Is.True);
        Assert.That(Encoding.ASCII.GetString(result.Value.Span), Is.EqualTo("value00010"));
    }


    [Test]
    public async Task GetRange()
    {
        var table = await TestHelper.BuildTableAsync(
            KeyEncoding.Ascii,
            databaseConfigure: builder => builder.PageSize = 128,
            tableConfigure: builder =>
            {
                builder.AddSecondaryIndex("category", true, KeyEncoding.Ascii, (key, value) =>
                {
                    var keyStr = Encoding.ASCII.GetString(key.Span);
                    return Encoding.ASCII.GetBytes($"category:{keyStr}");
                });

                for (var i = 0; i < 1000; i++)
                {
                    builder.Append(
                        Encoding.ASCII.GetBytes($"key{i:D5}"),
                        Encoding.ASCII.GetBytes($"value{i:D5}"));
                }
            });

        var query = table.WithIndex("category");
        var result = query.GetRange(
            "category:key00001"u8,
            "category:key00010"u8);
        Assert.That(result.Count, Is.EqualTo(11));
    }
}