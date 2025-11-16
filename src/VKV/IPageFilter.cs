using System;
using System.Collections.Concurrent;

namespace VKV;

public interface IPageFilter
{
    byte Flag { get; }

    int GetMaxEncodedLength(int decodedLength);

    int Encode(ReadOnlySpan<byte> input, Span<byte> output);
    int Decode(ReadOnlySpan<byte> input, Span<byte> output);
}

public static class PageFilterResolver
{
    static readonly ConcurrentDictionary<byte, IPageFilter> filters = new();

    public static void Register(IPageFilter filter)
    {
        filters.AddOrUpdate(
            filter.Flag,
            static (key, arg) => arg,
            static (key, old, arg) => arg,
            filter);
    }

    public static IPageFilter Resolve(byte flag)
    {
        return filters[flag];
    }
}