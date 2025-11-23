using System.Text;
using System.Threading.Tasks;
using VKV.Internal;

namespace VKV.Tests;

[TestFixture]
public class RangeIteratorTest
{
    [Test]
    public async Task MoveNext_FirstValue()
    {
        var tree = await TestHelper.BuildTreeAsync(
            new KeyValueList(KeyEncoding.Ascii)
            {
                { "key1"u8.ToArray(), "value1"u8.ToArray() },
                { "key2"u8.ToArray(), "value2"u8.ToArray() },
                { "key3"u8.ToArray(), "value3"u8.ToArray() },
                { "key5"u8.ToArray(), "value5"u8.ToArray() },
                { "key7"u8.ToArray(), "value7"u8.ToArray() },
                { "key8"u8.ToArray(), "value8"u8.ToArray() },
                { "key9"u8.ToArray(), "value9"u8.ToArray() },
            }, 128);

        var iterator = tree.GetIterator();
        Assert.That(iterator.MoveNext(), Is.True);

        Assert.That(
            Encoding.ASCII.GetString(iterator.Current.Span),
            Is.EqualTo("value1"));
    }
}