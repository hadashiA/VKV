using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using VKV.Internal;

namespace VKV.BTree;

enum SearchOperator
{
    Equal,
    LowerBound,
    UpperBound,
}

class TreeWalker
{
    public PageNumber RootPageNumber { get;  }
    public PageCache PageCache { get; }
    public KeyEncoding KeyEncoding { get; }

    readonly IKeyComparer keyComparer;

    internal TreeWalker(
        PageNumber rootPageNumber,
        PageCache pageCache,
        KeyEncoding keyEncoding)
    {
        RootPageNumber = rootPageNumber;
        PageCache = pageCache;
        KeyEncoding = keyEncoding;
        keyComparer = KeyComparer.From(KeyEncoding);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SingleValueResult Get(ReadOnlySpan<byte> key)
    {
        PageNumber? next = RootPageNumber;
        PageSlice pageSlice;

        while (!TryFindFrom(next.Value, key, out pageSlice, out next))
        {
            if (!next.HasValue) return SingleValueResult.Empty;
            PageCache.Load(next.Value);
        }
        return new SingleValueResult(pageSlice, true);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SingleValueResult Get(string key)
    {
        KeyEncodingMismatchException.ThrowIfCannotEncodeString(KeyEncoding);
        var textEncoding = KeyEncoding.ToTextEncoding();

        Span<byte> keyBuffer = stackalloc byte[textEncoding.GetMaxByteCount(key.Length)];
        var bytesWritten = textEncoding.GetBytes(key, keyBuffer);
        return Get(keyBuffer[..bytesWritten]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SingleValueResult Get(long key)
    {
        KeyEncodingMismatchException.ThrowIfCannotEncodeInt64(KeyEncoding);

        Span<byte> keyBuffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(keyBuffer, key);
        return Get(keyBuffer);
    }

    public async ValueTask<SingleValueResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default)
    {
        PageSlice resultValue;
        PageNumber? next = RootPageNumber;

        while (!TryFindFrom(next.Value, key.Span, out resultValue, out next))
        {
            if (!next.HasValue)
            {
                return SingleValueResult.Empty;
            }
            await PageCache.LoadAsync(next.Value, cancellationToken).ConfigureAwait(false);
        }

        return new SingleValueResult(resultValue, true);
    }

    public ValueTask<RangeResult> GetRangeAsync(
        ReadOnlyMemory<byte>? startKey,
        ReadOnlyMemory<byte>? endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending,
        CancellationToken cancellationToken = default)
    {
        if (startKey.HasValue && endKey.HasValue)
        {
            // validate start/end order
            if (keyComparer.Compare(startKey.Value.Span, endKey.Value.Span) > 0)
            {
                throw new ArgumentException("startKey is greater than endKey");
            }
        }

        return sortOrder switch
        {
            SortOrder.Ascending => GetRangeByAscendingAsync(
                startKey,
                endKey,
                startKeyExclusive,
                endKeyExclusive,
                cancellationToken),
            SortOrder.Descending => GetRangeByDescendingAsync(
                startKey,
                endKey,
                startKeyExclusive,
                endKeyExclusive,
                cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(sortOrder), sortOrder, null)
        };
    }

    async ValueTask<int> CountAsync(
        ReadOnlyMemory<byte>? startKey,
        ReadOnlyMemory<byte>? endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        CancellationToken cancellationToken = default)
    {
        var pageNumber = RootPageNumber;
        var index = 0;

        // find start position
        if (startKey.HasValue)
        {
            PageNumber? nextPage = pageNumber;
            while (!TrySearch(
                       nextPage.Value,
                       startKey.Value.Span,
                       startKeyExclusive ? SearchOperator.UpperBound : SearchOperator.LowerBound,
                       out index,
                       out nextPage))
            {
                if (nextPage.HasValue)
                {
                    await PageCache.LoadAsync(nextPage.Value, cancellationToken).ConfigureAwait(false);
                    pageNumber = nextPage.Value;
                }
                else
                {
                    return 0;
                }
            }
        }

        var count = 0;

        while (true)
        {
            IPageEntry page;
            while (!PageCache.TryGet(pageNumber, out page))
            {
                await PageCache.LoadAsync(pageNumber, cancellationToken)
                    .ConfigureAwait(false);
            }

            try
            {
                NodeHeader.Parse(page.Memory.Span, out var header, out var payload);
                if (header.Kind != NodeKind.Leaf)
                {
                    throw new InvalidOperationException("Invalid node kind");
                }

                var leafNode = new LeafNodeReader(header, payload);
                while (true)
                {
                    leafNode.GetAt(index, out var key, out _, out _, out var nextIndex);
                    count++;

                    // check end key
                    if (endKey.HasValue)
                    {
                        var compared = keyComparer.Compare(key, endKey.Value.Span);
                        if (endKeyExclusive)
                        {
                            if (compared > 0) return count;
                        }
                        else
                        {
                            if (compared >= 0) return count;
                        }
                    }
                    if (!nextIndex.HasValue) break;
                    index = nextIndex.Value;
                }

                // next node
                if (header.RightSiblingPageNumber.IsEmpty)
                {
                    return count;
                }
                pageNumber = header.RightSiblingPageNumber;
                index = 0;
            }
            finally
            {
                page.Release();
            }
        }
    }

    async ValueTask<RangeResult> GetRangeByAscendingAsync(
        ReadOnlyMemory<byte>? startKey,
        ReadOnlyMemory<byte>? endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        CancellationToken cancellationToken = default)
    {
        var pageNumber = RootPageNumber;
        var index = 0;

        // find start position
        if (startKey.HasValue)
        {
            PageNumber? nextPage = pageNumber;
            while (!TrySearch(
                       nextPage.Value,
                       startKey.Value.Span,
                       startKeyExclusive ? SearchOperator.UpperBound : SearchOperator.LowerBound,
                       out index,
                       out nextPage))
            {
                if (nextPage.HasValue)
                {
                    await PageCache.LoadAsync(nextPage.Value, cancellationToken).ConfigureAwait(false);
                    pageNumber = nextPage.Value;
                }
                else
                {
                    return RangeResult.Empty;
                }
            }
        }

        var result = RangeResult.Rent();

        while (true)
        {
            IPageEntry page;
            while (!PageCache.TryGet(pageNumber, out page))
            {
                await PageCache.LoadAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            }

            try
            {
                NodeHeader.Parse(page.Memory.Span, out var header, out var payload);
                if (header.Kind != NodeKind.Leaf)
                {
                    throw new InvalidOperationException("Invalid node kind");
                }

                var leafNode = new LeafNodeReader(header, payload);
                while (true)
                {
                    leafNode.GetAt(
                        index,
                        out var key,
                        out var valuePayloadOffset,
                        out var valueLength,
                        out var nextIndex);
                    result.Add(new PageSlice(page, Unsafe.SizeOf<NodeHeader>() + valuePayloadOffset, valueLength));

                    // check end key
                    if (endKey.HasValue)
                    {
                        var compared = keyComparer.Compare(key, endKey.Value.Span);
                        if (endKeyExclusive)
                        {
                            if (compared > 0) return result;
                        }
                        else
                        {
                            if (compared >= 0) return result;
                        }
                    }

                    if (!nextIndex.HasValue) break;
                    index = nextIndex.Value;
                }

                // next node
                if (header.RightSiblingPageNumber.IsEmpty)
                {
                    return result;
                }

                pageNumber = header.RightSiblingPageNumber;
                index = 0;
            }
            finally
            {
                page.Release();
            }
        }
    }

    ValueTask<RangeResult> GetRangeByDescendingAsync(
        ReadOnlyMemory<byte>? startKey,
        ReadOnlyMemory<byte>? endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    internal bool TryFind(
        scoped ReadOnlySpan<byte> key,
        out PageSlice value,
        out PageNumber? next) =>
        TryFindFrom(RootPageNumber, key, out value, out next);

    internal bool TryFindFrom(
        PageNumber from,
        scoped ReadOnlySpan<byte> key,
        out PageSlice value,
        out PageNumber? next)
    {
        var pageNumber = from;
        while (true)
        {
            if (!PageCache.TryGet(pageNumber, out var page))
            {
                value = default;
                next = pageNumber;
                return false;
            }

            NodeHeader.Parse(page.Memory.Span, out var header, out var payload);
            if (header.Kind == NodeKind.Internal)
            {
                var internalNode = new InternalNodeReader(in header, payload);
                if (!internalNode.TrySearch(key, keyComparer, out pageNumber))
                {
                    page.Release();
                    value = default;
                    next = null;
                    return false;
                }
            }
            else // Leaf
            {
                next = null;

                var leafNode = new LeafNodeReader(header, payload);
                if (leafNode.TryFindValue(key, keyComparer, out var valuePayloadOffset, out var valueLength))
                {
                    value = new PageSlice(page, NodeHeader.Size + valuePayloadOffset, valueLength);
                    return true;
                }

                page.Release();
                value = default;
                return false;
            }
            page.Release();
        }
    }

    internal bool TrySearch(
        scoped ReadOnlySpan<byte> key,
        SearchOperator op,
        out int index,
        out PageNumber? nextPageNumber) =>
        TrySearch(RootPageNumber, key, op, out index, out nextPageNumber);

    internal bool TrySearch(
        PageNumber from,
        scoped ReadOnlySpan<byte> key,
        SearchOperator op,
        out int index,
        out PageNumber? nextPageNumber)
    {
        var pageNumber = from;
        while (true)
        {
            if (!PageCache.TryGet(pageNumber, out var page))
            {
                index = default;
                nextPageNumber = pageNumber;
                return false;
            }

            NodeHeader.Parse(page.Memory.Span, out var header, out var payload);
            if (header.Kind == NodeKind.Internal)
            {
                var internalNode = new InternalNodeReader(in header, payload);
                if (!internalNode.TrySearch(key, keyComparer, out pageNumber))
                {
                    page.Release();
                    index = default;
                    nextPageNumber = null;
                    return false;
                }
            }
            else // Leaf
            {
                nextPageNumber = null;

                var leafNode = new LeafNodeReader(header, payload);
                if (leafNode.TrySearch(key, op, keyComparer, out index))
                {
                    return true;
                }
                page.Release();
                index = default;
                return false;
            }
            page.Release();
        }
    }
}