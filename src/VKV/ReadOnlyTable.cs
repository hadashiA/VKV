using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using VKV.BTree;
using VKV.Internal;

namespace VKV;

public class ReadOnlyTable
{
    public string Name => descriptor.Name;

    readonly TableDescriptor descriptor;
    readonly TreeWalker primaryKeyTree;
    readonly Dictionary<string, TreeWalker> secondaryIndexTrees;

    internal ReadOnlyTable(TableDescriptor descriptor, PageCache pageCache)
    {
        this.descriptor = descriptor;

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
    public SingleValueResult Get(long key) => primaryKeyTree.Get(key);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SingleValueResult Get(string key) => primaryKeyTree.Get(key);

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

    public ValueTask<RangeResult> GetRangeAsync(
        ReadOnlyMemory<byte>? startKey,
        ReadOnlyMemory<byte>? endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        CancellationToken cancellationToken = default)
    {
        return primaryKeyTree.GetRangeAsync(
            startKey,
            endKey,
            startKeyExclusive,
            endKeyExclusive,
            cancellationToken: cancellationToken);
    }


    public SecondaryIndexQuery IndexBy(string indexName) =>
        new(secondaryIndexTrees[indexName]);
}
