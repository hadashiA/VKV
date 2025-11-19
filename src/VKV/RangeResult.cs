using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace VKV;

public class RangeResult : IDisposable, IEnumerable<ReadOnlyMemory<byte>>
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

    readonly List<ReadOnlyMemory<byte>> list = [];
    readonly List<IPageEntry> referencePages = [];

    internal void Add(IPageEntry page, int start, int length)
    {
        list.Add(page.Memory.Slice(start, length));
        if (!referencePages.Contains(page))
        {
            referencePages.Add(page);
        }
    }

    public void Dispose()
    {
        foreach (var referencePage in referencePages)
        {
            referencePage.Release();
        }
        list.Clear();
        referencePages.Clear();
        Pool.Enqueue(this);
    }

    public List<ReadOnlyMemory<byte>>.Enumerator GetEnumerator() => list.GetEnumerator();
    IEnumerator<ReadOnlyMemory<byte>> IEnumerable<ReadOnlyMemory<byte>>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
