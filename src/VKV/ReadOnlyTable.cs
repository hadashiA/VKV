using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using VKV.BTree;
using VKV.Internal;

namespace VKV;

public class ReadOnlyTable : IKeyValueStore
{
    public string Name => descriptor.Name;
    public IKeyEncoding KeyEncoding { get; }

    readonly TableDescriptor descriptor;
    readonly TreeWalker primaryKeyTree;
    readonly Dictionary<string, TreeWalker> secondaryIndexTrees;

    internal ReadOnlyTable(TableDescriptor descriptor, PageCache pageCache)
    {
        this.descriptor = descriptor;
        KeyEncoding = descriptor.PrimaryKeyDescriptor.KeyEncoding;

        primaryKeyTree = new TreeWalker(
            descriptor.PrimaryKeyDescriptor.RootPageNumber,
            pageCache,
            descriptor.PrimaryKeyDescriptor.KeyEncoding);

        secondaryIndexTrees = new Dictionary<string, TreeWalker>(descriptor.IndexDescriptors.Count);
        foreach (var indexDescriptor in descriptor.IndexDescriptors)
        {
            var indexIterator = new TreeWalker(
                indexDescriptor.RootPageNumber,
                pageCache,
                indexDescriptor.KeyEncoding);
            secondaryIndexTrees.Add(indexDescriptor.Name, indexIterator);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SingleValueResult Get(ReadOnlySpan<byte> key) => primaryKeyTree.Get(key);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SingleValueResult Get(long key)
    {
        KeyEncodingMismatchException.ThrowIfCannotEncodeInt64(KeyEncoding);
        Span<byte> keyBuffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(keyBuffer, key);
        return Get(keyBuffer);
    }

    /// <summary>
    /// Find value by key.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="cancellationToken"></param>
    /// <returns>
    ///   return byte array if found, otherwise null.
    /// </returns>
    public ValueTask<SingleValueResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        primaryKeyTree.GetAsync(key, cancellationToken);

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
        CancellationToken cancellationToken = default) =>
        primaryKeyTree.GetRangeAsync(startKey, endKey, startKeyExclusive, endKeyExclusive, sortOrder, cancellationToken: cancellationToken);

    public int CountRange(
        ReadOnlySpan<byte> startKey,
        ReadOnlySpan<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false) =>
        primaryKeyTree.CountRange(startKey, endKey, startKeyExclusive, endKeyExclusive);

    public ValueTask<int> CountRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        CancellationToken cancellationToken = default) =>
        primaryKeyTree.CountRangeAsync(startKey, endKey, startKeyExclusive, endKeyExclusive, cancellationToken);

    public RangeIterator CreateIterator(IteratorDirection iteratorDirection = IteratorDirection.Forward) =>
        new(primaryKeyTree, iteratorDirection);

    public SecondaryIndexQuery IndexBy(string indexName) =>
        new(secondaryIndexTrees[indexName]);
}
