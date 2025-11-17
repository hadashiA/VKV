using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace VKV.Internal;

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

        public int RefCount;
        public long LastAccess;

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


#if NET9_0_OR_GREATER
    readonly Lock gate = new();
#else
    readonly object gate = new();
#endif

    readonly ConcurrentDictionary<PageNumber, Entry> entries = new();
    readonly IStorage storage;
    readonly int capacity;
    long globalClock;

    bool disposed;

    internal PageCache(IStorage storage, int capacity)
    {
        this.storage = storage;
        this.capacity = capacity;
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed) return;

            foreach (var t in entries)
            {
                t.Value.Release();
            }
            entries.Clear();
            disposed = true;
        }
    }

    public bool TryGet(PageNumber pageNumber, out IPageEntry page)
    {
        if (entries.TryGetValue(pageNumber, out var entry))
        {
            entry.Retain();
            entry.LastAccess = Interlocked.Increment(ref globalClock);
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
        var newEntry = new Entry
        {
            PageNumber = pageNumber,
            Buffer = buffer,
            RefCount = 1,
            LastAccess = Interlocked.Increment(ref globalClock)
        };
        if (!entries.TryAdd(pageNumber, newEntry))
        {
            // already exists
            return;
        }

        if (entries.Count > capacity)
        {
            EvictOne();
        }
    }

    void EvictOne()
    {
        var minStamp = long.MaxValue;
        PageNumber? victimKey = null;
        foreach (var (key, e) in entries)
        {
            if (e.RefCount <= 1 && e.LastAccess < minStamp)
            {
                minStamp = e.LastAccess;
                victimKey = key;
            }
        }

        if (victimKey.HasValue)
        {
            if (entries.TryRemove(victimKey.Value, out var victim))
            {
                victim.Release();
            }
        }
        else
        {
            foreach (var kv in entries)
            {
                if (entries.TryRemove(kv.Key, out var victim2))
                {
                    victim2.Release();
                    break;
                }
            }
        }
    }
}