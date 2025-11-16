using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using VKV.Internal;

namespace VKV;

public class RangeResult : IDisposable, IEnumerable<PageSlice>
{
    static readonly ConcurrentQueue<RangeResult> Pool = new();

    public static readonly RangeResult Empty = new();

    internal static RangeResult Rent()
    {
        if (Pool.TryDequeue(out var result))
        {
            return result;
        }
        return new RangeResult();
    }

    public int Count => list.Count;

    readonly List<PageSlice> list = [];

    internal void Add(PageSlice pageSlice)
    {
        list.Add(pageSlice);
    }

    public void Dispose()
    {
        foreach (var pageSlice in list)
        {
            pageSlice.Dispose();
        }
        list.Clear();
        Pool.Enqueue(this);
    }

    public List<PageSlice>.Enumerator GetEnumerator() => list.GetEnumerator();
    IEnumerator<PageSlice> IEnumerable<PageSlice>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
