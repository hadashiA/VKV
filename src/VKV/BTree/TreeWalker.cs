using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

    public RangeIterator GetIterator(IteratorDirection iteratorDirection = IteratorDirection.Forward) =>
        new(this, iteratorDirection);

    public async ValueTask<RangeResult> GetRangeAsync(
        ReadOnlyMemory<byte>? startKey,
        ReadOnlyMemory<byte>? endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending,
        CancellationToken cancellationToken = default)
    {
        ValidateRange(startKey, endKey);

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
                if (!nextPage.HasValue) return RangeResult.Empty;

                await PageCache.LoadAsync(nextPage.Value, cancellationToken).ConfigureAwait(false);
                pageNumber = nextPage.Value;
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
                var pageSpan = page.Memory.Span;
                var header = NodeHeader.Parse(pageSpan);
                if (header.Kind != NodeKind.Leaf)
                {
                    throw new InvalidOperationException("Invalid node kind");
                }

                var leafNode = new LeafNodeReader(pageSpan, header.EntryCount);
                while (index < header.EntryCount)
                {
                    leafNode.GetAt(
                        index,
                        out var key,
                        out var valuePageOffset,
                        out var valueLength );
                    result.Add(page, valuePageOffset, valueLength);

                    // check end key
                    if (endKey.HasValue)
                    {
                        var compared = keyComparer.Compare(key, endKey.Value.Span);
                        if (compared > 0 || (!endKeyExclusive && compared == 0))
                        {
                            return result;
                        }
                    }
                    index++;
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

    async ValueTask<int> CountAsync(
        ReadOnlyMemory<byte>? startKey,
        ReadOnlyMemory<byte>? endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        CancellationToken cancellationToken = default)
    {
        var pageNumber = RootPageNumber;
        var index = 0;
        var count = 0;

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
                if (!nextPage.HasValue) return count;

                await PageCache.LoadAsync(nextPage.Value, cancellationToken).ConfigureAwait(false);
                pageNumber = nextPage.Value;
            }
        }

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
                var span = page.Memory.Span;
                var header = Unsafe.ReadUnaligned<NodeHeader>(ref MemoryMarshal.GetReference(span));
                if (header.Kind != NodeKind.Leaf)
                {
                    throw new InvalidOperationException("Invalid node kind");
                }

                var leafNode = new LeafNodeReader(span, header.EntryCount);
                while (index < header.EntryCount)
                {
                    leafNode.GetAt(index, out var key, out _);

                    // check end key
                    if (endKey.HasValue)
                    {
                        var compared = keyComparer.Compare(key, endKey.Value.Span);
                        if (compared > 0 || (!endKeyExclusive && compared == 0))
                        {
                            return count;
                        }
                    }
                    count++;
                    index++;
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

            var pageSpan = page.Memory.Span;
            var header = NodeHeader.Parse(pageSpan);
            if (header.Kind == NodeKind.Internal)
            {
                var internalNode = new InternalNodeReader(pageSpan, header.EntryCount);
                if (!internalNode.TrySearch(key, keyComparer, out pageNumber))
                {
                    page.Release();
                    value = default;
                    next = null;
                    return false;
                }
                page.Release();
            }
            else // Leaf
            {
                next = null;

                var leafNode = new LeafNodeReader(pageSpan, header.EntryCount);
                if (leafNode.TryFindValue(key, keyComparer, out var valueOffset, out var valueLength))
                {
                    value = new PageSlice(page, valueOffset, valueLength);
                    return true;
                }

                page.Release();
                value = default;
                return false;
            }
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

            var pageSpan = page.Memory.Span;
            var header = NodeHeader.Parse(pageSpan);
            if (header.Kind == NodeKind.Internal)
            {
                var internalNode = new InternalNodeReader(pageSpan, header.EntryCount);
                if (!internalNode.TrySearch(key, keyComparer, out pageNumber))
                {
                    page.Release();
                    index = default;
                    nextPageNumber = null;
                    return false;
                }
                page.Release();
            }
            else // Leaf
            {
                nextPageNumber = null;

                var leafNode = new LeafNodeReader(pageSpan, header.EntryCount);
                if (leafNode.TrySearch(key, op, keyComparer, out index))
                {
                    return true;
                }
                page.Release();
                index = default;
                return false;
            }
        }
    }

    internal SingleValueResult GetMinValue()
    {
        var pageNumber = RootPageNumber;
        while (true)
        {
            IPageEntry page;
            while (!PageCache.TryGet(pageNumber, out page))
            {
                PageCache.Load(pageNumber);
            }

            var pageSpan = page.Memory.Span;
            var header = NodeHeader.Parse(pageSpan);
            if (header.Kind == NodeKind.Internal)
            {
                if (header.EntryCount <= 0)
                {
                    return SingleValueResult.Empty;
                }
                var internalNode = new InternalNodeReader(pageSpan, header.EntryCount);
                internalNode.GetAt(0, out _, out pageNumber);
                page.Release();
            }
            else // Leaf
            {
                if (header.EntryCount <= 0)
                {
                    return SingleValueResult.Empty;
                }
                var leafNode = new LeafNodeReader(pageSpan, header.EntryCount);
                leafNode.GetAt(0, out _, out var valuePageOffset, out var valueLength);
                var pageSlice = new PageSlice(page, valuePageOffset, valueLength);
                return new SingleValueResult(pageSlice, true);
            }
        }
    }

    internal SingleValueResult GetMaxValue()
    {
        var pageNumber = RootPageNumber;
        while (true)
        {
            IPageEntry page;
            while (!PageCache.TryGet(pageNumber, out page))
            {
                PageCache.Load(pageNumber);
            }

            var pageSpan = page.Memory.Span;
            var header = NodeHeader.Parse(pageSpan);
            if (header.Kind == NodeKind.Internal)
            {
                if (header.EntryCount <= 0)
                {
                    return SingleValueResult.Empty;
                }
                var internalNode = new InternalNodeReader(pageSpan, header.EntryCount);
                internalNode.GetAt(header.EntryCount - 1, out _, out pageNumber);
                page.Release();
            }
            else // Leaf
            {
                if (header.EntryCount <= 0)
                {
                    return SingleValueResult.Empty;
                }
                var leafNode = new LeafNodeReader(pageSpan, header.EntryCount);
                leafNode.GetAt(header.EntryCount - 1, out _, out var valuePageOffset, out var valueLength);
                var pageSlice = new PageSlice(page, valuePageOffset, valueLength);
                return new SingleValueResult(pageSlice, true);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ValidateRange(ReadOnlyMemory<byte>? start, ReadOnlyMemory<byte>? end)
    {
        if (start.HasValue && end.HasValue)
        {
            // validate start/end order
            if (keyComparer.Compare(start.Value.Span, end.Value.Span) > 0)
            {
                throw new ArgumentException("startKey is greater than endKey");
            }
        }
    }
}