using System;
using System.Threading.Tasks;
using VKV.BTree;
using VKV.Internal;

namespace VKV.Tests;

[TestFixture]
public class TreeWalkerTest
{
    [Test]
    public async Task SearchOperator_Equal()
    {
        var tree = await TestHelper.BuildTreeAsync(
            new KeyValueList(KeyEncoding.Ascii)
            {
                { "key1"u8.ToArray(), "value1"u8.ToArray() },
                { "key2"u8.ToArray(), "value2"u8.ToArray() },
                { "key3"u8.ToArray(), "value3"u8.ToArray() },
                { "key4"u8.ToArray(), "value4"u8.ToArray() },
                { "key5"u8.ToArray(), "value5"u8.ToArray() },
                { "key6"u8.ToArray(), "value6"u8.ToArray() },
                { "key7"u8.ToArray(), "value7"u8.ToArray() },
                { "key8"u8.ToArray(), "value8"u8.ToArray() },
                { "key9"u8.ToArray(), "value9"u8.ToArray() },
            }, 128);

        await tree.PageCache.LoadAsync(tree.RootPageNumber);

        var result = tree.TrySearch(
            "key3"u8.ToArray(),
            SearchOperator.Equal,
            out var index,
            out var nextPageNumber);

        Assert.That(result, Is.False);
        Assert.That(nextPageNumber.HasValue, Is.True);

        var pageNumber = nextPageNumber.Value;
        await tree.PageCache.LoadAsync(pageNumber);

        result = tree.TrySearch(
            "key3"u8.ToArray(),
            SearchOperator.Equal,
            out index,
            out nextPageNumber);
        Assert.That(result, Is.True);
        Assert.That(nextPageNumber.HasValue, Is.False);

        Assert.That(tree.PageCache.TryGet(pageNumber, out var page), Is.True);

        NodeHeader.Parse(page.Memory.Span, out var header, out var payload);
        var leafNode = new LeafNodeReader(header, payload);

        leafNode.GetAt(index, out var key, out var value, out _);
        Assert.That(key.SequenceEqual("key3"u8), Is.True);
        Assert.That(value.SequenceEqual("value3"u8), Is.True);
    }

    [Test]
    public async Task SearchOperator_LowerBound()
    {
        var tree = await TestHelper.BuildTreeAsync(
            new KeyValueList(KeyEncoding.Ascii)
            {
                { "key01"u8.ToArray(), "value01"u8.ToArray() },
                { "key02"u8.ToArray(), "value02"u8.ToArray() },
                { "key03"u8.ToArray(), "value03"u8.ToArray() },
                { "key04"u8.ToArray(), "value04"u8.ToArray() },
                { "key05"u8.ToArray(), "value05"u8.ToArray() },
                { "key06"u8.ToArray(), "value06"u8.ToArray() },
                { "key07"u8.ToArray(), "value07"u8.ToArray() },
                { "key08"u8.ToArray(), "value08"u8.ToArray() },
                { "key09"u8.ToArray(), "value09"u8.ToArray() },
                { "key10"u8.ToArray(), "value10"u8.ToArray() },
                { "key11"u8.ToArray(), "value11"u8.ToArray() },
                { "key12"u8.ToArray(), "value12"u8.ToArray() },
                { "key13"u8.ToArray(), "value13"u8.ToArray() },
                { "key14"u8.ToArray(), "value14"u8.ToArray() },
                { "key15"u8.ToArray(), "value15"u8.ToArray() },
                { "key16"u8.ToArray(), "value16"u8.ToArray() },
            }, 128);

        await tree.PageCache.LoadAsync(tree.RootPageNumber);

        var result = tree.TrySearch(
            "key03"u8.ToArray(),
            SearchOperator.LowerBound,
            out var pos,
            out var nextPageNumber);

        Assert.That(result, Is.False);
        Assert.That(nextPageNumber.HasValue, Is.True);

        await tree.PageCache.LoadAsync(nextPageNumber!.Value);

        var pageNumber = nextPageNumber.Value;

        result = tree.TrySearch(
            pageNumber,
            "key03"u8.ToArray(),
            SearchOperator.LowerBound,
            out pos,
            out nextPageNumber);

        Assert.That(result, Is.True);
        Assert.That(nextPageNumber.HasValue, Is.False);

        Assert.That(tree.PageCache.TryGet(pageNumber, out var page), Is.True);

        NodeHeader.Parse(page.Memory.Span, out var header, out var payload);
        var leafNode = new LeafNodeReader(header, payload);

        leafNode.GetAt(pos, out var key, out var value, out _);
        Assert.That(key.SequenceEqual("key03"u8), Is.True);
        Assert.That(value.SequenceEqual("value03"u8), Is.True);
    }

    [Test]
    public async Task SearchOperator_UpperBound()
    {
        var tree = await TestHelper.BuildTreeAsync(
            new KeyValueList(KeyEncoding.Ascii)
            {
                { "key01"u8.ToArray(), "value01"u8.ToArray() },
                { "key02"u8.ToArray(), "value02"u8.ToArray() },
                { "key03"u8.ToArray(), "value03"u8.ToArray() },
                { "key04"u8.ToArray(), "value04"u8.ToArray() },
                { "key05"u8.ToArray(), "value05"u8.ToArray() },
                { "key06"u8.ToArray(), "value06"u8.ToArray() },
                { "key07"u8.ToArray(), "value07"u8.ToArray() },
                { "key08"u8.ToArray(), "value08"u8.ToArray() },
                { "key09"u8.ToArray(), "value09"u8.ToArray() },
                { "key10"u8.ToArray(), "value10"u8.ToArray() },
                { "key11"u8.ToArray(), "value11"u8.ToArray() },
                { "key12"u8.ToArray(), "value12"u8.ToArray() },
                { "key13"u8.ToArray(), "value13"u8.ToArray() },
                { "key14"u8.ToArray(), "value14"u8.ToArray() },
                { "key15"u8.ToArray(), "value15"u8.ToArray() },
                { "key16"u8.ToArray(), "value16"u8.ToArray() },
            }, 128);

        await tree.PageCache.LoadAsync(tree.RootPageNumber);

        var result = tree.TrySearch(
            "key03"u8.ToArray(),
            SearchOperator.UpperBound,
            out var pos,
            out var nextPageNumber);

        Assert.That(result, Is.False);
        Assert.That(nextPageNumber.HasValue, Is.True);

        await tree.PageCache.LoadAsync(nextPageNumber!.Value);

        var pageNumber = nextPageNumber.Value;

        result = tree.TrySearch(
            pageNumber,
            "key03"u8.ToArray(),
            SearchOperator.UpperBound,
            out pos,
            out nextPageNumber);

        Assert.That(result, Is.True);
        Assert.That(nextPageNumber.HasValue, Is.False);

        Assert.That(tree.PageCache.TryGet(pageNumber, out var page), Is.True);

        NodeHeader.Parse(page.Memory.Span, out var header, out var payload);
        var leafNode = new LeafNodeReader(header, payload);

        leafNode.GetAt(pos, out var key, out var value, out _);
        Assert.That(key.SequenceEqual("key04"u8), Is.True);
        Assert.That(value.SequenceEqual("value04"u8), Is.True);
    }

    [Test]
    public async Task Get_SingleLeaf()
    {
        var tree = await TestHelper.BuildTreeAsync(
            new KeyValueList(KeyEncoding.Ascii, true)
            {
                { "key1"u8.ToArray(), "value1"u8.ToArray() },
                { "key2"u8.ToArray(), "value2"u8.ToArray() },
                { "key3"u8.ToArray(), "value3"u8.ToArray() },
                { "key4"u8.ToArray(), "value4"u8.ToArray() },
                { "key5"u8.ToArray(), "value5"u8.ToArray() },
            }, 128);

        using var result1 = await tree.GetAsync("key4"u8.ToArray());
        Assert.That(result1.HasValue, Is.True);
        Assert.That(result1.Value.Span.SequenceEqual("value4"u8), Is.True);

        using var result2 = await tree.GetAsync("aaaaa"u8.ToArray());
        Assert.That(result2.HasValue, Is.False);
    }

    [Test]
    public async Task Get_MultipleNode()
    {
        var tree = await TestHelper.BuildTreeAsync(
            new KeyValueList(KeyEncoding.Ascii, true)
            {
                { "key1"u8.ToArray(), "value1"u8.ToArray() },
                { "key2"u8.ToArray(), "value2"u8.ToArray() },
                { "key3"u8.ToArray(), "value3"u8.ToArray() },
                { "key4"u8.ToArray(), "value4"u8.ToArray() },
                { "key5"u8.ToArray(), "value5"u8.ToArray() },
                { "key6"u8.ToArray(), "value6"u8.ToArray() },
            }, 128);

        using var result1 = await tree.GetAsync("key3"u8.ToArray());
        Assert.That(result1.HasValue, Is.True);
        Assert.That(result1.Value.Span.SequenceEqual("value3"u8), Is.True);

        using var result2 = await tree.GetAsync("aaaaa"u8.ToArray());
        Assert.That(result2.HasValue, Is.False);
    }

    [Test]
    public async Task GetRange_NotFound()
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

        using var result1 = await tree.GetRangeAsync(
            "key9"u8.ToArray(),
            null,
            startKeyExclusive: true);

        Assert.That(result1.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task GetRange_StartInclusiveEndInclusive()
    {
        var table = await TestHelper.BuildTreeAsync(
            new KeyValueList(KeyEncoding.Ascii, true)
            {
                { "key01"u8.ToArray(), "value01"u8.ToArray() },
                { "key02"u8.ToArray(), "value02"u8.ToArray() },
                { "key03"u8.ToArray(), "value03"u8.ToArray() },
                { "key05"u8.ToArray(), "value05"u8.ToArray() },
                { "key07"u8.ToArray(), "value07"u8.ToArray() },
                { "key08"u8.ToArray(), "value08"u8.ToArray() },
                { "key09"u8.ToArray(), "value09"u8.ToArray() },
                { "key10"u8.ToArray(), "value10"u8.ToArray() },
                { "key11"u8.ToArray(), "value11"u8.ToArray() },
            }, 128);

        using var result1 = await table.GetRangeAsync(
            "key03"u8.ToArray(),
            "key09"u8.ToArray());

        Assert.That(result1.Count, Is.EqualTo(5));
    }
}
