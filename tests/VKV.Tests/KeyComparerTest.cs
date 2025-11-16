namespace VKV.Tests;

[TestFixture]
public class KeyComparerTest
{
    [Test]
    public void Hoge()
    {
        var comparer = AsciiOrdinalComparer.Instance;

        Assert.That(
            comparer.Compare(
                "key01"u8.ToArray(),
                "key02"u8.ToArray()),
            Is.LessThan(0));
    }
}