using System;
using System.Collections.Concurrent;

namespace VKV;

public interface IPageFilter
{
    string Id { get; }

    int GetMaxEncodedLength(int decodedLength);

    int Encode(ReadOnlySpan<byte> input, Span<byte> output);
    int Decode(ReadOnlySpan<byte> input, Span<byte> output);
}

public static class PageFilterRegistry
{
    static readonly ConcurrentDictionary<string, IPageFilter> table = new();

    public static IPageFilter Resolve(string id) => table[id];

    public static void Register(IPageFilter filter)
    {
        if (!table.TryAdd(filter.Id, filter))
        {
            throw new ArgumentException($"The filter {filter.Id} already exists.");
        }
    }
}
