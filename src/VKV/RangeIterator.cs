using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using VKV.BTree;

namespace VKV;

public enum IteratorDirection
{
    Forward,
    Backward
}

public class RangeIterator :
    IEnumerable<ReadOnlyMemory<byte>>,
    IEnumerator<ReadOnlyMemory<byte>>,
    IAsyncEnumerable<ReadOnlyMemory<byte>>,
    IAsyncEnumerator<ReadOnlyMemory<byte>>
{
    object? IEnumerator.Current => Current;

    public ReadOnlyMemory<byte> Current =>
        currentPage?.Memory.Slice(currentValueOffset, currentValueLength) ?? default;

    // iterator state
    readonly TreeWalker treeWalker;
    IPageEntry? currentPage;
    int currentNodeEntryCount = -1;
    ushort currentEntryIndex;
    int currentValueOffset;
    int currentValueLength;

    internal RangeIterator(
        TreeWalker treeWalker,
        IteratorDirection iteratorDirection = IteratorDirection.Forward)
    {
        this.treeWalker = treeWalker;
        if (iteratorDirection == IteratorDirection.Backward)
        {
            throw new NotImplementedException();
        }
    }

    public RangeIterator GetEnumerator() => this;
    IEnumerator<ReadOnlyMemory<byte>> IEnumerable<ReadOnlyMemory<byte>>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public RangeIterator GetAsyncEnumerator(CancellationToken cancellationToken = default) => this;
    IAsyncEnumerator<ReadOnlyMemory<byte>> IAsyncEnumerable<ReadOnlyMemory<byte>>.GetAsyncEnumerator(
        CancellationToken cancellationToken) =>
        GetAsyncEnumerator(cancellationToken);

    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }

    public void Dispose()
    {
        currentPage?.Release();
        currentPage = null;
    }

    public void Reset()
    {
        currentPage?.Release();
        currentPage = null;
    }

    public bool TrySeek(ReadOnlySpan<byte> key)
    {
        PageNumber? nextPageNumber = treeWalker.RootPageNumber;
        PageSlice pageSlice;

        ushort entryIndex;
        while (!treeWalker.TryFindFrom(
                   nextPageNumber.Value,
                   key,
                   out entryIndex,
                   out pageSlice,
                   out nextPageNumber))
        {
            if (!nextPageNumber.HasValue) return false;
            treeWalker.PageCache.Load(nextPageNumber.Value);
        }

        if (currentPage != pageSlice.Page)
        {
            currentPage?.Release();
            currentPage = pageSlice.Page;
            currentNodeEntryCount = currentPage.GetEntryCount();
        }

        currentEntryIndex = entryIndex;
        currentValueOffset = pageSlice.Start;
        currentValueLength = pageSlice.Length;
        return true;
    }

    public async ValueTask<bool> TrySeekAsync(
        ReadOnlyMemory<byte> key,
        CancellationToken cancellationToken = default)
    {
        PageNumber? nextPageNumber = treeWalker.RootPageNumber;
        PageSlice pageSlice;

        ushort entryIndex;
        while (!treeWalker.TryFindFrom(
                   nextPageNumber.Value,
                   key.Span,
                   out entryIndex,
                   out pageSlice,
                   out nextPageNumber))
        {
            if (!nextPageNumber.HasValue) return false;
            await treeWalker.PageCache.LoadAsync(nextPageNumber.Value, cancellationToken);
        }

        if (currentPage != pageSlice.Page)
        {
            currentPage?.Release();
            currentPage = pageSlice.Page;
            currentNodeEntryCount = currentPage.GetEntryCount();
        }
        currentValueOffset = pageSlice.Start;
        currentValueLength = pageSlice.Length;
        currentEntryIndex = entryIndex;
        return true;
    }

    public bool MoveNext()
    {
        var pageCache = treeWalker.PageCache;

        // first item
        if (currentPage is null)
        {
            var minimumValue = treeWalker.GetMinValue();
            if (!minimumValue.HasValue)
            {
                return false;
            }

            currentPage = minimumValue.Value.Page;
            currentValueOffset = minimumValue.Value.Start;
            currentValueLength = minimumValue.Value.Length;
            currentNodeEntryCount = currentPage.GetEntryCount();
            return true;
        }

        // tail of node
        if (currentEntryIndex >= currentNodeEntryCount - 1)
        {
            var header = currentPage.GetNodeHeader();
            // check right node exists
            if (header.RightSiblingPageNumber.IsEmpty)
            {
                return false;
            }

            currentPage.Release();
            while (!pageCache.TryGet(header.RightSiblingPageNumber, out currentPage))
            {
                treeWalker.PageCache.Load(header.RightSiblingPageNumber);
            }

            header = currentPage.GetNodeHeader();
            currentNodeEntryCount = header.EntryCount;
            if (header.Kind != NodeKind.Leaf)
            {
                throw new InvalidOperationException("Invalid node kind");
            }
            currentEntryIndex = 0;
            if (currentNodeEntryCount < 0)
            {
                return false;
            }
        }
        else
        {
            currentEntryIndex++;
        }

        var leafNode = new LeafNodeReader(currentPage.Memory.Span, currentNodeEntryCount);
        leafNode.GetAt(currentEntryIndex,
            out _,
            out currentValueOffset,
            out currentValueLength);
        return true;
    }

    public async ValueTask<bool> MoveNextAsync()
    {
        var pageCache = treeWalker.PageCache;

        // first item
        if (currentPage is null)
        {
            var minimumValue = treeWalker.GetMinValue();
            if (!minimumValue.HasValue)
            {
                return false;
            }

            currentPage = minimumValue.Value.Page;
            currentValueOffset = minimumValue.Value.Start;
            currentValueLength = minimumValue.Value.Length;
            currentNodeEntryCount = currentPage.GetEntryCount();
            return true;
        }

        // tail of node
        if (currentEntryIndex >= currentNodeEntryCount - 1)
        {
            var header = currentPage.GetNodeHeader();
            // check right node exists
            if (header.RightSiblingPageNumber.IsEmpty)
            {
                return false;
            }

            currentPage.Release();
            while (!pageCache.TryGet(header.RightSiblingPageNumber, out currentPage))
            {
                await treeWalker.PageCache.LoadAsync(header.RightSiblingPageNumber);
            }

            header = currentPage.GetNodeHeader();
            currentNodeEntryCount = header.EntryCount;
            if (header.Kind != NodeKind.Leaf)
            {
                throw new InvalidOperationException("Invalid node kind");
            }
            currentEntryIndex = 0;
            if (currentNodeEntryCount < 0)
            {
                return false;
            }
        }
        else
        {
            currentEntryIndex++;
        }

        var leafNode = new LeafNodeReader(currentPage.Memory.Span, currentNodeEntryCount);
        leafNode.GetAt(currentEntryIndex,
            out _,
            out currentValueOffset,
            out currentValueLength);
        return true;
    }
}
