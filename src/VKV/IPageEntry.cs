using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace VKV;

public readonly struct PageSlice(PageEntry entry, int start, int length) : IDisposable
{
    public PageEntry Page => entry;
    public int Start => start;
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

enum QueueTag : byte
{
    None,
    S,
    M
}

public class PageEntry
{
    public PageNumber PageNumber { get; init; }
    public IMemoryOwner<byte>? Buffer { get; init; }

    public int RefCount
    {
        get => refCount;
        init => refCount = value;
    }

    internal QueueTag Tag { get; set; }
    internal int Frequency;

    int refCount;

    public ReadOnlyMemory<byte> Memory
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Buffer!.Memory;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Retain()
    {
        Interlocked.Increment(ref refCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryRetainIfAlive()
    {
        while (true)
        {
            var current = Volatile.Read(ref refCount);
            if (current <= 0)
                return false;

            var next = current + 1;

            int original = Interlocked.CompareExchange(ref refCount, next, current);
            if (original == current)
                return true;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Release()
    {
        if (Interlocked.Decrement(ref refCount) == 0)
        {
            Buffer?.Dispose();
        }
    }
}

