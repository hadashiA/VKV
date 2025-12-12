using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace VKV;

public interface IKeyValueStore
{
    IKeyEncoding KeyEncoding { get; }

    SingleValueResult Get(ReadOnlySpan<byte> key);
    ValueTask<SingleValueResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default);

    RangeResult GetRange(in QueryRef query);
    ValueTask<RangeResult> GetRangeAsync(Query query, CancellationToken cancellationToken = default);

    int CountRange(in QueryRef query);
    ValueTask<int> CountRangeAsync(Query query, CancellationToken cancellationToken = default);

    RangeIterator CreateIterator(IteratorDirection iteratorDirection = IteratorDirection.Forward);
}

public static class KeyValueStoreExtensions
{
    public static SingleValueResult Get<TKey>(this IKeyValueStore kv, TKey key) where TKey : IComparable<TKey>
    {
        var bufferLength = kv.KeyEncoding.GetMaxEncodedByteCount(key);
        Span<byte> buffer = stackalloc byte[bufferLength];
        kv.KeyEncoding.TryEncode(key, buffer, out var bytesWritten);
        return kv.Get(buffer[..bytesWritten]);
    }

    public static async ValueTask<SingleValueResult> GetAsync<TKey>(
        this IKeyValueStore kv,
        TKey key,
        CancellationToken cancellationToken = default)
        where TKey : IComparable<TKey>
    {
        var initialBufferSize = kv.KeyEncoding.GetMaxEncodedByteCount(key);
        var buffer = ArrayPool<byte>.Shared.Rent(initialBufferSize);
        int bytesWritten;
        while (!kv.KeyEncoding.TryEncode(key, buffer, out bytesWritten))
        {
            ArrayPool<byte>.Shared.Return(buffer);
            buffer = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
        }
        try
        {
            return await kv.GetAsync(buffer.AsMemory(0, bytesWritten), cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public static RangeResult GetRange<TKey>(
        this IKeyValueStore kv,
        in Query<TKey> query)
        where TKey : IComparable<TKey>
    {
        var startKeyBuffer = query.StartKey != null
            ? stackalloc byte[kv.KeyEncoding.GetMaxEncodedByteCount(query.StartKey)]
            : [];

        var endKeyBuffer = query.EndKey != null
            ? stackalloc byte[kv.KeyEncoding.GetMaxEncodedByteCount(query.EndKey)]
            : [];

        var startKeyLength = 0;
        var endKeyLength = 0;

        if (query.StartKey is { } s)
        {
            while (!kv.KeyEncoding.TryEncode(s, startKeyBuffer, out startKeyLength))
            {
                startKeyBuffer = stackalloc byte[startKeyBuffer.Length * 2];
            }
        }
        if (query.EndKey is { } e)
        {
            while (!kv.KeyEncoding.TryEncode(e, endKeyBuffer, out endKeyLength))
            {
                endKeyBuffer = stackalloc byte[endKeyBuffer.Length * 2];
            }
        }

        return kv.GetRange(new QueryRef
        {
            StartKey = startKeyLength > 0 ? startKeyBuffer[..startKeyLength] : KeyRange.Unbound,
            EndKey = endKeyLength > 0 ? endKeyBuffer[..endKeyLength] : KeyRange.Unbound,
            StartKeyExclusive = query.StartKeyExclusive,
            EndKeyExclusive = query.EndKeyExclusive,
            SortOrder = query.SortOrder,
        });
    }

    public static ValueTask<RangeResult> GetRangeAsync<TKey>(
        this IKeyValueStore kv,
        Query<TKey> query,
        CancellationToken cancellationToken = default)
        where TKey : IComparable<TKey>
    {
        using var encodedQuery = query.ToEncodedQuery(kv.KeyEncoding);
        return kv.GetRangeAsync(encodedQuery.Query, cancellationToken);
    }

    public static int CountRange<TKey>(
        this IKeyValueStore kv,
        in Query<TKey> query)
        where TKey : IComparable<TKey>
    {
        var startKeyBuffer = query.StartKey != null
            ? stackalloc byte[kv.KeyEncoding.GetMaxEncodedByteCount(query.StartKey)]
            : [];

        var endKeyBuffer = query.EndKey != null
            ? stackalloc byte[kv.KeyEncoding.GetMaxEncodedByteCount(query.EndKey)]
            : [];

        var startKeyLength = 0;
        var endKeyLength = 0;

        if (query.StartKey is { } s)
        {
            while (!kv.KeyEncoding.TryEncode(s, startKeyBuffer, out startKeyLength))
            {
                startKeyBuffer = stackalloc byte[startKeyBuffer.Length * 2];
            }
        }
        if (query.EndKey is { } e)
        {
            while (!kv.KeyEncoding.TryEncode(e, endKeyBuffer, out endKeyLength))
            {
                endKeyBuffer = stackalloc byte[endKeyBuffer.Length * 2];
            }
        }

        return kv.CountRange(new QueryRef
        {
            StartKey = startKeyLength > 0 ? startKeyBuffer[..startKeyLength] : KeyRange.Unbound,
            EndKey = endKeyLength > 0 ? endKeyBuffer[..endKeyLength] : KeyRange.Unbound,
            StartKeyExclusive = query.StartKeyExclusive,
            EndKeyExclusive = query.EndKeyExclusive,
            SortOrder = query.SortOrder,
        });
    }

    public static ValueTask<int> CountRangeAsync<TKey>(
        this IKeyValueStore kv,
        Query<TKey> query,
        CancellationToken cancellationToken = default)
        where TKey : IComparable<TKey>
    {
        using var encodedQuery = query.ToEncodedQuery(kv.KeyEncoding);
        return kv.CountRangeAsync(encodedQuery.Query, cancellationToken);
    }
}
