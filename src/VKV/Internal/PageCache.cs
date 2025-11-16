using System;
using System.Buffers;
using System.Collections.Concurrent;
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

    readonly ConcurrentDictionary<PageNumber, Entry> entries;
    readonly IStorage storage;
    readonly int capacity;
    long globalClock;

    bool disposed;

    internal PageCache(IStorage storage, int capacity)
    {
        this.storage = storage;
        this.capacity = capacity;

        entries = new ConcurrentDictionary<PageNumber, Entry>();
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
        var stamp = Interlocked.Increment(ref globalClock);

        entries.AddOrUpdate(
            pageNumber,
            static (key, arg) => new Entry
            {
                PageNumber = key,
                Buffer = arg.buffer,
                RefCount = 1,
                LastAccess = arg.stamp
            },
            static (key, existing, arg) =>
            {
                // 既存を更新（古いバッファ解放して入れ替え）
                existing.Release();
                existing.Buffer = arg.buffer;
                existing.RefCount = 1;
                existing.LastAccess = arg.stamp;
                existing.PageNumber = key;
                return existing;
            }, (buffer, stamp));

        // キャパシティ超過したら追い出し
        if (entries.Count > capacity)
        {
            EvictOne();
        }
    }

    void EvictOne()
    {
        // 単純な実装：全テーブル走査して最古 LastAccess を探す
        var minStamp = long.MaxValue;
        PageNumber? victimKey = null;
        foreach (var kv in entries)
        {
            var e = kv.Value;
            // 参照中でない (RefCount == 0) を条件にするなら変更可
            if (e.RefCount <= 0 && e.LastAccess < minStamp)
            {
                minStamp = e.LastAccess;
                victimKey = kv.Key;
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
            // 全部参照中 or同等なら簡易版として一つ適当に Remove
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