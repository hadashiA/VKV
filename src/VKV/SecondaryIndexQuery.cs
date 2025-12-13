using System;
using System.Threading;
using System.Threading.Tasks;
using VKV.BTree;

namespace VKV;

// TODO:
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
        return tree.Get(key);
    }

    public ValueTask<SingleValueResult> GetAsync(
        ReadOnlyMemory<byte> key,
        CancellationToken cancellationToken = default) =>
        tree.GetAsync(key, cancellationToken);

    public RangeResult GetRange(
        ReadOnlySpan<byte> startKey,
        ReadOnlySpan<byte> endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        SortOrder sortOrder) =>
        tree.GetRange(startKey, endKey, startKeyExclusive, endKeyExclusive, sortOrder);

    public ValueTask<RangeResult> GetRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        SortOrder sortOrder,
        CancellationToken cancellationToken = default) =>
        tree.GetRangeAsync(startKey, endKey, startKeyExclusive, endKeyExclusive, sortOrder, cancellationToken);

    public int CountRange(
        ReadOnlySpan<byte> startKey,
        ReadOnlySpan<byte> endKey,
        bool startKeyExclusive,
        bool endKeyExclusive) =>
        tree.CountRange(startKey, endKey, startKeyExclusive, endKeyExclusive);

    public ValueTask<int> CountRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        CancellationToken cancellationToken = default) =>
        tree.CountRangeAsync(startKey, endKey, startKeyExclusive, endKeyExclusive, cancellationToken);

    public RangeIterator CreateIterator(IteratorDirection iteratorDirection = IteratorDirection.Forward) =>
        tree.CreateIterator(iteratorDirection);
}
