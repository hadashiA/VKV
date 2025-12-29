using System;
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
    public PageNumber RootPageNumber { get; }
    public PageCache PageCache { get; }
    public IKeyEncoding KeyEncoding { get; }

    readonly IKeyEncoding comparer;

    internal TreeWalker(
        PageNumber rootPageNumber,
        PageCache pageCache,
        IKeyEncoding keyEncoding)
    {
        RootPageNumber = rootPageNumber;
        PageCache = pageCache;
        KeyEncoding = keyEncoding;

        // optimize
        comparer = keyEncoding switch
        {
            AsciiOrdinalEncoding => AsciiOrdinalEncoding.Instance,
            Int64LittleEndianEncoding => Int64LittleEndianEncoding.Instance,
            _ => keyEncoding
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SingleValueResult Get(ReadOnlySpan<byte> key)
    {
        PageNumber? next = RootPageNumber;
        PageSlice pageSlice;

        while (!TryFindFrom(next.Value, key, out _, out pageSlice, out next))
        {
            if (!next.HasValue) return SingleValueResult.Empty;
            PageCache.Load(next.Value);
        }

        return new SingleValueResult(pageSlice, true);
    }

    public async ValueTask<SingleValueResult> GetAsync(
        ReadOnlyMemory<byte> key,
        CancellationToken cancellationToken = default)
    {
        PageSlice resultValue;
        PageNumber? next = RootPageNumber;

        while (!TryFindFrom(next.Value, key.Span, out _, out resultValue, out next))
        {
            if (!next.HasValue)
            {
                return SingleValueResult.Empty;
            }

            await PageCache.LoadAsync(next.Value, cancellationToken).ConfigureAwait(false);
        }

        return new SingleValueResult(resultValue, true);
    }

    public RangeIterator CreateIterator(IteratorDirection iteratorDirection = IteratorDirection.Forward) =>
        new(this, iteratorDirection);

    public RangeResult GetRange(
        ReadOnlySpan<byte> startKey,
        ReadOnlySpan<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending)
    {
        ValidateRange(startKey, endKey);

        int entryIndex;
        PageEntry page = default!;

        // find start position
        if (startKey.IsEmpty)
        {
            var minValue = GetMinValue();
            if (!minValue.HasValue)
            {
                return RangeResult.Empty;
            }
            page = minValue.Value.Page;
            entryIndex = 0;
        }
        else
        {
            PageNumber? nextPageNumber = RootPageNumber;
            while (!TrySearch(
                       nextPageNumber.Value,
                       startKey,
                       startKeyExclusive ? SearchOperator.UpperBound : SearchOperator.LowerBound,
                       out page,
                       out entryIndex,
                       out nextPageNumber))
            {
                if (!nextPageNumber.HasValue) return RangeResult.Empty;

                PageCache.Load(nextPageNumber.Value);
            }
        }

        var result = RangeResult.Rent();

        while (true)
        {
            var retain = false;
            try
            {
                var pageSpan = page.Memory.Span;
                var header = NodeHeader.Parse(pageSpan);
                if (header.Kind != NodeKind.Leaf)
                {
                    throw new InvalidOperationException("Invalid node kind");
                }

                var leafNode = new LeafNodeReader(pageSpan, header.EntryCount);
                while (entryIndex < header.EntryCount)
                {
                    leafNode.GetAt(
                        entryIndex,
                        out var key,
                        out var valuePageOffset,
                        out var valueLength);

                    // check end key
                    if (!endKey.IsEmpty)
                    {
                        var compared = KeyEncoding.Compare(key, endKey);
                        if (compared > 0 || (endKeyExclusive && compared == 0))
                        {
                            return result;
                        }
                    }
                    result.Add(page, valuePageOffset, valueLength);
                    entryIndex++;
                    retain = true;
                }

                // next node
                if (header.RightSiblingPageNumber.IsEmpty)
                {
                    return result;
                }

                var pageNumber = header.RightSiblingPageNumber;
                while (!PageCache.TryGet(pageNumber, out page))
                {
                    PageCache.Load(pageNumber);
                }
                entryIndex = 0;
            }
            finally
            {
                if (!retain) page.Release();
            }
        }
    }

    public async ValueTask<RangeResult> GetRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending,
        CancellationToken cancellationToken = default)
    {
        ValidateRange(startKey, endKey);

        int entryIndex;
        PageEntry page = default!;

        // find start position
        if (startKey.IsEmpty)
        {
            var minValue = GetMinValue();
            if (!minValue.HasValue)
            {
                return RangeResult.Empty;
            }
            page = minValue.Value.Page;
            entryIndex = 0;
        }
        else
        {
            PageNumber? nextPageNumber = RootPageNumber;
            while (!TrySearch(
                       nextPageNumber.Value,
                       startKey.Span,
                       startKeyExclusive ? SearchOperator.UpperBound : SearchOperator.LowerBound,
                       out page,
                       out entryIndex,
                       out nextPageNumber))
            {
                if (!nextPageNumber.HasValue) return RangeResult.Empty;
                await PageCache.LoadAsync(nextPageNumber.Value, cancellationToken).ConfigureAwait(false);
            }
        }

        var result = RangeResult.Rent();

        while (true)
        {
            var retain = false;
            try
            {
                var pageSpan = page.Memory.Span;
                var header = NodeHeader.Parse(pageSpan);
                if (header.Kind != NodeKind.Leaf)
                {
                    throw new InvalidOperationException("Invalid node kind");
                }

                var leafNode = new LeafNodeReader(pageSpan, header.EntryCount);
                while (entryIndex < header.EntryCount)
                {
                    leafNode.GetAt(
                        entryIndex,
                        out var key,
                        out var valuePageOffset,
                        out var valueLength);

                    // check end key
                    if (!endKey.IsEmpty)
                    {
                        var compared = KeyEncoding.Compare(key, endKey.Span);
                        if (compared > 0 || (endKeyExclusive && compared == 0))
                        {
                            return result;
                        }
                    }
                    result.Add(page, valuePageOffset, valueLength);

                    entryIndex++;
                    retain = true;
                }

                // next node
                if (header.RightSiblingPageNumber.IsEmpty)
                {
                    return result;
                }

                var pageNumber = header.RightSiblingPageNumber;
                while (!PageCache.TryGet(pageNumber, out page))
                {
                    await PageCache.LoadAsync(pageNumber, cancellationToken).ConfigureAwait(false);
                }
                entryIndex = 0;
            }
            finally
            {
                if (!retain) page.Release();
            }
        }
    }

    public int CountRange(
        ReadOnlySpan<byte> startKey,
        ReadOnlySpan<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false)
    {
        ValidateRange(startKey, endKey);

        PageEntry page;

        int entryIndex;
        // find start position
        if (startKey.IsEmpty)
        {
            var minValue = GetMinValue();
            if (!minValue.HasValue)
            {
                return 0;
            }
            page = minValue.Value.Page;
            entryIndex = 0;
        }
        else
        {
            PageNumber? nextPageNumber = RootPageNumber;
            while (!TrySearch(
                       nextPageNumber.Value,
                       startKey,
                       startKeyExclusive ? SearchOperator.UpperBound : SearchOperator.LowerBound,
                       out page,
                       out entryIndex,
                       out nextPageNumber))
            {
                if (!nextPageNumber.HasValue) return 0;

                while (!PageCache.TryGet(nextPageNumber.Value, out page))
                {
                    PageCache.Load(nextPageNumber.Value);
                }
            }
        }

        var count = 0;

        while (true)
        {
            try
            {
                var pageSpan = page.Memory.Span;
                var header = NodeHeader.Parse(pageSpan);
                if (header.Kind != NodeKind.Leaf)
                {
                    throw new InvalidOperationException("Invalid node kind");
                }

                var leafNode = new LeafNodeReader(pageSpan, header.EntryCount);
                while (entryIndex < header.EntryCount)
                {
                    leafNode.GetAt(
                        entryIndex,
                        out var key,
                        out var valuePageOffset,
                        out var valueLength);

                    // check end key
                    if (!endKey.IsEmpty)
                    {
                        var compared = KeyEncoding.Compare(key, endKey);
                        if (compared > 0 || (endKeyExclusive && compared == 0))
                        {
                            return count;
                        }
                    }

                    count++;
                    entryIndex++;
                }

                // next node
                if (header.RightSiblingPageNumber.IsEmpty)
                {
                    return count;
                }

                var pageNumber = header.RightSiblingPageNumber;
                while (!PageCache.TryGet(pageNumber, out page))
                {
                    PageCache.Load(pageNumber);
                }
                entryIndex = 0;
            }
            finally
            {
                page.Release();
            }
        }
    }

    public async ValueTask<int> CountRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        CancellationToken cancellationToken = default)
    {
        ValidateRange(startKey, endKey);

        int entryIndex;
        PageEntry page = default!;

        // find start position
        if (startKey.IsEmpty)
        {
            var minValue = GetMinValue();
            if (!minValue.HasValue)
            {
                return 0;
            }
            page = minValue.Value.Page;
            entryIndex = 0;
        }
        else
        {
            PageNumber? nextPageNumber = RootPageNumber;
            while (!TrySearch(
                       nextPageNumber.Value,
                       startKey.Span,
                       startKeyExclusive ? SearchOperator.UpperBound : SearchOperator.LowerBound,
                       out page,
                       out entryIndex,
                       out nextPageNumber))
            {
                if (!nextPageNumber.HasValue) return 0;

                while (!PageCache.TryGet(nextPageNumber.Value, out page))
                {
                    await PageCache.LoadAsync(nextPageNumber.Value, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        var count = 0;

        while (true)
        {
            try
            {
                var pageSpan = page.Memory.Span;
                var header = NodeHeader.Parse(pageSpan);
                if (header.Kind != NodeKind.Leaf)
                {
                    throw new InvalidOperationException("Invalid node kind");
                }

                var leafNode = new LeafNodeReader(pageSpan, header.EntryCount);
                while (entryIndex < header.EntryCount)
                {
                    leafNode.GetAt(
                        entryIndex,
                        out var key,
                        out var valuePageOffset,
                        out var valueLength);

                    // check end key
                    if (!endKey.IsEmpty)
                    {
                        var compared = KeyEncoding.Compare(key, endKey.Span);
                        if (compared > 0 || (endKeyExclusive && compared == 0))
                        {
                            return count;
                        }
                    }

                    count++;
                    entryIndex++;
                }

                // next node
                if (header.RightSiblingPageNumber.IsEmpty)
                {
                    return count;
                }

                var pageNumber = header.RightSiblingPageNumber;
                while (!PageCache.TryGet(pageNumber, out page))
                {
                    await PageCache.LoadAsync(pageNumber, cancellationToken).ConfigureAwait(false);
                }
                entryIndex = 0;
            }
            finally
            {
                page.Release();
            }
        }
    }

    internal bool TryFind(
        scoped ReadOnlySpan<byte> key,
        out ushort entryIndex,
        out PageSlice value,
        out PageNumber? next) =>
        TryFindFrom(RootPageNumber, key, out entryIndex, out value, out next);

    internal bool TryFindFrom(
        PageNumber from,
        scoped ReadOnlySpan<byte> key,
        out ushort entryIndex,
        out PageSlice value,
        out PageNumber? next)
    {
        var pageNumber = from;
        while (true)
        {
            if (!PageCache.TryGet(pageNumber, out var page))
            {
                entryIndex = default;
                value = default;
                next = pageNumber;
                return false;
            }

            var pageSpan = page.Memory.Span;
            var header = NodeHeader.Parse(pageSpan);
            if (header.Kind == NodeKind.Internal)
            {
                var internalNode = new InternalNodeReader(pageSpan, header.EntryCount);
                if (!internalNode.TrySearch(key, KeyEncoding, out pageNumber))
                {
                    page.Release();
                    entryIndex = default;
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
                if (leafNode.TryFindValue(key, KeyEncoding, out var valueOffset, out var valueLength, out entryIndex))
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
        out PageEntry page,
        out int index,
        out PageNumber? nextPageNumber) =>
        TrySearch(RootPageNumber, key, op, out page, out index, out nextPageNumber);

    internal bool TrySearch(
        PageNumber from,
        scoped ReadOnlySpan<byte> key,
        SearchOperator op,
        out PageEntry page,
        out int index,
        out PageNumber? nextPageNumber)
    {
        var pageNumber = from;
        while (true)
        {
            if (!PageCache.TryGet(pageNumber, out page))
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
                if (!internalNode.TrySearch(key, comparer, out pageNumber))
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
                if (leafNode.TrySearch(key, op, comparer, out index))
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
            PageEntry page;
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
            PageEntry page;
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

    void ValidateRange(ReadOnlySpan<byte> startKey, ReadOnlySpan<byte> endKey)
    {
        if (!startKey.IsEmpty && !endKey.IsEmpty)
        {
            if (KeyEncoding.Compare(startKey, endKey) > 0)
            {
                throw new ArgumentException("startKey must be less than or equal to endKey");
            }
        }
    }

    void ValidateRange(ReadOnlyMemory<byte> startKey, ReadOnlyMemory<byte> endKey)
    {
        if (!startKey.IsEmpty && !endKey.IsEmpty)
        {
            if (KeyEncoding.Compare(startKey, endKey) > 0)
            {
                throw new ArgumentException("startKey must be less than or equal to endKey");
            }
        }
    }
}