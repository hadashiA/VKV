using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace VKV.Internal;

public readonly struct PageSlice(IPageEntry entry, int start, int length) : IDisposable
{
    public IPageEntry Page => entry;
    public int Length => length;

    public ReadOnlySpan<byte> Span
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Page.Memory.Span.Slice(start, length);
    }

    public ReadOnlyMemory<byte> Memory
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Page.Memory.Slice(start, length);
    }

    public void Dispose()
    {
        entry.Release();
    }
}

public interface IPageEntry
{
    public PageNumber PageNumber { get; }
    public ReadOnlyMemory<byte> Memory { get; }
    public void Release();
}

public sealed class PageCache : IDisposable
{
    class Entry : IPageEntry
    {
        public PageNumber PageNumber { get; set; }
        public IMemoryOwner<byte>? Buffer { get; set; }
        public ReadOnlyMemory<byte> Memory
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Buffer!.Memory;
        }

        public int PrevIndex;
        public int NextIndex;
        public int RefCount;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Retain()
        {
            Interlocked.Increment(ref RefCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release()
        {
            if (Interlocked.Decrement(ref RefCount) == 0)
            {
                Buffer?.Dispose();
            }
        }
    }

    readonly Entry[] entries;
    readonly Dictionary<PageNumber, int> table;

#if NET9_0_OR_GREATER
    readonly Lock gate = new();
#else
    readonly object gate = new();
#endif

    readonly IStorage storage;
    readonly int capacity;
    int head = -1;
    int tail = -1;
    int freeHead;
    int count;

    bool disposed;

    internal PageCache(IStorage storage, int capacity)
    {
        this.storage = storage;
        this.capacity = capacity;

        entries = new Entry[capacity];
        table = new Dictionary<PageNumber, int>(capacity);

        for (var i = 0; i < capacity; i++)
        {
            var entry = new Entry
            {
                NextIndex = i + 1,
                RefCount = 1,
            };
            entries[i] =  entry;
        }
        entries[capacity - 1].NextIndex = -1;
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed) return;

            foreach (var t in entries)
            {
                t.Release();
            }

            table.Clear();
            disposed = true;
        }
    }

    public bool TryGet(PageNumber pageNumber, out IPageEntry page)
    {
        int index;
        bool found;
        lock (gate)
        {
            found = table.TryGetValue(pageNumber, out index);
            if (found)
            {
                MoveToFront(index);
            }
        }

        if (found)
        {
            var entry = entries[index];
            entry.Retain();
            page = entry;
            return true;
        }

        page = default!;
        return false;
    }

    public void Load(PageNumber pageNumber)
    {
        var buffer = storage.ReadPage(pageNumber);
        AddEntry(pageNumber, buffer);
    }

    public async ValueTask LoadAsync(PageNumber pageNumber, CancellationToken cancellationToken = default)
    {
        var buffer = await storage.ReadPageAsync(pageNumber, cancellationToken);
        AddEntry(pageNumber, buffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void AddEntry(PageNumber pageNumber, IMemoryOwner<byte> buffer)
    {
        lock (gate)
        {
            if (table.TryGetValue(pageNumber, out var existingIndex))
            {
                var existingEntry = entries[existingIndex];
                existingEntry.Release();
                existingEntry.Buffer = buffer;
                MoveToFront(existingIndex);
                return;
            }

            int newIndex;
            if (count < entries.Length)
            {
                newIndex = freeHead;
                freeHead = entries[freeHead].NextIndex;
                count++;
            }
            else
            {
                // LRU から削除
                newIndex = tail;
                RemoveFromLRU(tail);
                table.Remove(entries[newIndex].PageNumber);
                entries[newIndex].Release();
            }

            var newEntry = entries[newIndex];
            newEntry.PageNumber = pageNumber;
            newEntry.Buffer = buffer;
            newEntry.RefCount = 1;
            table[pageNumber] = newIndex;
            AddToFront(newIndex);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void MoveToFront(int index)
    {
        if (head == index) return;
        RemoveFromLRU(index);
        AddToFront(index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void RemoveFromLRU(int index)
    {
        var entry = entries[index];
        if (entry.PrevIndex >= 0)
        {
            entries[entry.PrevIndex].NextIndex = entry.NextIndex;
        }
        else
        {
            head = entry.NextIndex;
        }

        if (entry.NextIndex >= 0)
        {
            entries[entry.NextIndex].PrevIndex = entry.PrevIndex;
        }
        else
        {
            tail = entry.PrevIndex;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void AddToFront(int index)
    {
        var entry = entries[index];
        entry.NextIndex = head;
        entry.PrevIndex = -1;

        if (head >= 0)
        {
            entries[head].PrevIndex = index;
        }

        head = index;

        if (tail < 0)
        {
            tail = index;
        }
    }
}
