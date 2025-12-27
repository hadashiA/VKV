using System;
using System.Threading;
using System.Threading.Tasks;
using VKV.BTree;
using VKV.Internal;

namespace VKV;

public class NonUniqueSecondaryIndexQuery : IKeyValueStore
{
    public IKeyEncoding KeyEncoding { get; }

    readonly TreeWalker duplicateKeyTree;

    internal NonUniqueSecondaryIndexQuery(IndexDescriptor descriptor, PageCache pageCache)
    {
        KeyEncoding = descriptor.KeyEncoding;

        duplicateKeyTree = new TreeWalker(
            descriptor.RootPageNumber,
            pageCache,
            new DuplicateKeyEncoding(descriptor.KeyEncoding));
    }

    public SingleValueResult Get(ReadOnlySpan<byte> key)
    {
        throw new NotImplementedException();
    }

    public ValueTask<SingleValueResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default)
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
        Span<byte> minKeyBuffer = stackalloc byte[DuplicateKey.SizeOf(startKey.Length)];
        Span<byte> maxKeyBuffer = stackalloc byte[DuplicateKey.SizeOf(endKey.Length)];

        if (!startKey.IsEmpty)
        {
            DuplicateKey.TryEncode(startKey, 0, minKeyBuffer);
        }
        if (!endKey.IsEmpty)
        {
            DuplicateKey.TryEncode(endKey, int.MaxValue, maxKeyBuffer);
        }

        using var valueRefs = duplicateKeyTree.GetRange(
            minKeyBuffer,
            maxKeyBuffer,
            startKeyExclusive,
            endKeyExclusive,
            sortOrder);
        if (valueRefs.Count <= 0)
        {
            return RangeResult.Empty;
        }

        var result = RangeResult.Rent();
        foreach (var x in valueRefs)
        {
            var pageRef = PageRef.Parse(x.Span);
            IPageEntry page;
            while (!duplicateKeyTree.PageCache.TryGet(pageRef.PageNumber, out page))
            {
                duplicateKeyTree.PageCache.Load(pageRef.PageNumber);
            }
            result.Add(page, pageRef.Start, pageRef.Length);
        }
        return result;
    }

    public ValueTask<RangeResult> GetRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false, SortOrder sortOrder = SortOrder.Ascending,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public int CountRange(ReadOnlySpan<byte> startKey, ReadOnlySpan<byte> endKey, bool startKeyExclusive = false,
        bool endKeyExclusive = false)
    {
        throw new NotImplementedException();
    }

    public ValueTask<int> CountRangeAsync(ReadOnlyMemory<byte> startKey, ReadOnlyMemory<byte> endKey, bool startKeyExclusive = false,
        bool endKeyExclusive = false, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}