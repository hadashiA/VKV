using System;
using System.Threading;
using System.Threading.Tasks;
using VKV.BTree;

namespace VKV;

public readonly struct SecondaryIndexQuery : IKeyValueStore
{
    public IKeyEncoding KeyEncoding => tree.KeyEncoding;

    readonly TreeWalker tree;

    internal SecondaryIndexQuery(TreeWalker tree)
    {
        this.tree = tree;
    }

    public SingleValueResult Get(ReadOnlySpan<byte> key)
    {
        throw new NotImplementedException();
    }

    public ValueTask<SingleValueResult> GetAsync(
        ReadOnlyMemory<byte> key,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public RangeResult GetRange(in QueryRef query)
    {
        throw new NotImplementedException();
    }

    public ValueTask<RangeResult> GetRangeAsync(Query query, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public int CountRange(in QueryRef query)
    {
        throw new NotImplementedException();
    }

    public ValueTask<int> CountRangeAsync(Query query, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public RangeResult GetRange(
        ReadOnlySpan<byte> startKey,
        ReadOnlySpan<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending)
    {
        throw new NotImplementedException();
    }

    public ValueTask<RangeResult> GetRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public int CountRange(
        ReadOnlySpan<byte> startKey,
        ReadOnlySpan<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false)
    {
        throw new NotImplementedException();
    }

    public ValueTask<int> CountRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public RangeIterator CreateIterator(IteratorDirection iteratorDirection = IteratorDirection.Forward)
    {
        throw new NotImplementedException();
    }
}
