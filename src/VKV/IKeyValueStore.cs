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

    RangeResult GetRange(
        ReadOnlySpan<byte> startKey,
        ReadOnlySpan<byte> endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        SortOrder sortOrder);

    ValueTask<RangeResult> GetRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        SortOrder sortOrder,
        CancellationToken cancellationToken = default);

    int CountRange(
        ReadOnlySpan<byte> startKey,
        ReadOnlySpan<byte> endKey,
        bool startKeyExclusive,
        bool endKeyExclusive);

    ValueTask<int> CountRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        CancellationToken cancellationToken = default);
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
        TKey? startKey,
        TKey? endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        SortOrder sortOrder)
        where TKey : IComparable<TKey>
    {
        var startKeyBuffer = startKey != null
            ? stackalloc byte[kv.KeyEncoding.GetMaxEncodedByteCount(startKey)]
            : [];

        var endKeyBuffer = endKey != null
            ? stackalloc byte[kv.KeyEncoding.GetMaxEncodedByteCount(endKey)]
            : [];

        var startKeyLength = 0;
        var endKeyLength = 0;

        if (startKey is { } s)
        {
            while (!kv.KeyEncoding.TryEncode(s, startKeyBuffer, out startKeyLength))
            {
                startKeyBuffer = stackalloc byte[startKeyBuffer.Length * 2];
            }
        }
        if (endKey is { } e)
        {
            while (!kv.KeyEncoding.TryEncode(e, endKeyBuffer, out endKeyLength))
            {
                endKeyBuffer = stackalloc byte[endKeyBuffer.Length * 2];
            }
        }

        return kv.GetRange(
            startKeyLength > 0 ? startKeyBuffer[..startKeyLength] : KeyRange.Unbound,
            endKeyLength > 0 ? endKeyBuffer[..endKeyLength] : KeyRange.Unbound,
            startKeyExclusive,
            endKeyExclusive,
            sortOrder);
    }

    public static async ValueTask<RangeResult> GetRangeAsync<TKey>(
        this IKeyValueStore kv,
        TKey? startKey,
        TKey? endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        SortOrder sortOrder,
        CancellationToken cancellationToken = default)
        where TKey : IComparable<TKey>
    {
        byte[]? startKeyBuffer = null;
        byte[]? endKeyBuffer = null;

        var startKeyLength = 0;
        var endKeyLength = 0;

        if (startKey != null)
        {
            var initialBufferSize = kv.KeyEncoding.GetMaxEncodedByteCount(startKey);
            startKeyBuffer = ArrayPool<byte>.Shared.Rent(initialBufferSize);
            while (!kv.KeyEncoding.TryEncode(startKey, startKeyBuffer, out startKeyLength))
            {
                var newLength = startKeyBuffer.Length * 2;
                ArrayPool<byte>.Shared.Return(startKeyBuffer);
                startKeyBuffer = ArrayPool<byte>.Shared.Rent(newLength);
            }
        }
        if (endKey != null)
        {
            var initialBufferSize = kv.KeyEncoding.GetMaxEncodedByteCount(endKey);
            endKeyBuffer = ArrayPool<byte>.Shared.Rent(initialBufferSize);
            while (!kv.KeyEncoding.TryEncode(endKey, endKeyBuffer, out endKeyLength))
            {
                var newLength = startKeyBuffer.Length * 2;
                ArrayPool<byte>.Shared.Return(endKeyBuffer);
                endKeyBuffer = ArrayPool<byte>.Shared.Rent(newLength);
            }
        }

        try
        {
            return await kv.GetRangeAsync(
                startKeyBuffer != null ? startKeyBuffer[..startKeyLength] : KeyRange.Unbound,
                endKeyBuffer != null ? endKeyBuffer[..endKeyLength] : KeyRange.Unbound,
                startKeyExclusive,
                endKeyExclusive,
                sortOrder,
                cancellationToken);
        }
        finally
        {
            if (startKeyBuffer != null) ArrayPool<byte>.Shared.Return(startKeyBuffer);
            if (endKeyBuffer != null) ArrayPool<byte>.Shared.Return(endKeyBuffer);
        }
    }

    public static int CountRange<TKey>(
        this IKeyValueStore kv,
        TKey? startKey,
        TKey? endKey,
        bool startKeyExclusive,
        bool endKeyExclusive)
        where TKey : IComparable<TKey>
    {
        var startKeyBuffer = startKey != null
            ? stackalloc byte[kv.KeyEncoding.GetMaxEncodedByteCount(startKey)]
            : [];

        var endKeyBuffer = endKey != null
            ? stackalloc byte[kv.KeyEncoding.GetMaxEncodedByteCount(endKey)]
            : [];

        var startKeyLength = 0;
        var endKeyLength = 0;

        if (startKey != null)
        {
            while (!kv.KeyEncoding.TryEncode(startKey, startKeyBuffer, out startKeyLength))
            {
                startKeyBuffer = stackalloc byte[startKeyBuffer.Length * 2];
            }
        }
        if (endKey != null)
        {
            while (!kv.KeyEncoding.TryEncode(endKey, endKeyBuffer, out endKeyLength))
            {
                endKeyBuffer = stackalloc byte[endKeyBuffer.Length * 2];
            }
        }

        return kv.CountRange(

            startKeyLength > 0 ? startKeyBuffer[..startKeyLength] : KeyRange.Unbound,
            endKeyLength > 0 ? endKeyBuffer[..endKeyLength] : KeyRange.Unbound,
            startKeyExclusive,
            endKeyExclusive);
    }

    public static async ValueTask<int> CountRangeAsync<TKey>(
        this IKeyValueStore kv,
        TKey? startKey,
        TKey? endKey,
        bool startKeyExclusive,
        bool endKeyExclusive,
        CancellationToken cancellationToken = default)
        where TKey : IComparable<TKey>
    {
        byte[]? startKeyBuffer = null;
        byte[]? endKeyBuffer = null;

        var startKeyLength = 0;
        var endKeyLength = 0;

        if (startKey != null)
        {
            var initialBufferSize = kv.KeyEncoding.GetMaxEncodedByteCount(startKey);
            startKeyBuffer = ArrayPool<byte>.Shared.Rent(initialBufferSize);
            while (!kv.KeyEncoding.TryEncode(startKey, startKeyBuffer, out startKeyLength))
            {
                var newLength = startKeyBuffer.Length * 2;
                ArrayPool<byte>.Shared.Return(startKeyBuffer);
                startKeyBuffer = ArrayPool<byte>.Shared.Rent(newLength);
            }
        }
        if (endKey != null)
        {
            var initialBufferSize = kv.KeyEncoding.GetMaxEncodedByteCount(endKey);
            endKeyBuffer = ArrayPool<byte>.Shared.Rent(initialBufferSize);
            while (!kv.KeyEncoding.TryEncode(endKey, endKeyBuffer, out endKeyLength))
            {
                var newLength = startKeyBuffer.Length * 2;
                ArrayPool<byte>.Shared.Return(endKeyBuffer);
                endKeyBuffer = ArrayPool<byte>.Shared.Rent(newLength);
            }
        }

        try
        {
            return await kv.CountRangeAsync(
                startKeyBuffer != null ? startKeyBuffer[..startKeyLength] : KeyRange.Unbound,
                endKeyBuffer != null ? endKeyBuffer[..endKeyLength] : KeyRange.Unbound,
                startKeyExclusive,
                endKeyExclusive,
                cancellationToken);
        }
        finally
        {
            if (startKeyBuffer != null) ArrayPool<byte>.Shared.Return(startKeyBuffer);
            if (endKeyBuffer != null) ArrayPool<byte>.Shared.Return(endKeyBuffer);
        }
    }
}
