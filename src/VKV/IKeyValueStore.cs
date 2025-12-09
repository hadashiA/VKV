using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
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
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending);

    ValueTask<RangeResult> GetRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending,
        CancellationToken cancellationToken = default);

    int CountRange(
        ReadOnlySpan<byte> startKey,
        ReadOnlySpan<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false);

    ValueTask<int> CountRangeAsync(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        CancellationToken cancellationToken = default);

    RangeIterator CreateIterator(IteratorDirection iteratorDirection = IteratorDirection.Forward);
}

public static class KeyValueStoreExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SingleValueResult Get(this IKeyValueStore kv, long key)
    {
        KeyEncodingMismatchException.ThrowIfCannotEncodeInt64(kv.KeyEncoding);
        Span<byte> keyBuffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(keyBuffer, key);
        return kv.Get(keyBuffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SingleValueResult Get(this IKeyValueStore kv, string key)
    {
        KeyEncodingMismatchException.ThrowIfCannotEncodeString(kv.KeyEncoding);
        var textEncoding = kv.KeyEncoding.ToTextEncoding();

        Span<byte> keyBuffer = stackalloc byte[textEncoding.GetMaxByteCount(key.Length)];
        var bytesWritten = textEncoding.GetBytes(key, keyBuffer);
        return kv.Get(keyBuffer[..bytesWritten]);
    }

    public static RangeResult GetRange(
        this IKeyValueStore kv,
        long? startKey,
        long? endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending)
    {
        KeyEncodingMismatchException.ThrowIfCannotEncodeInt64(kv.KeyEncoding);

        Span<byte> startKeyBuffer = stackalloc byte[sizeof(long)];
        Span<byte> endKeyBuffer = stackalloc byte[sizeof(long)];
        if (startKey.HasValue)
        {
            BinaryPrimitives.WriteInt64LittleEndian(startKeyBuffer, startKey.Value);
        }
        if (endKey.HasValue)
        {
            BinaryPrimitives.WriteInt64LittleEndian(endKeyBuffer, endKey.Value);
        }

        return kv.GetRange(
            startKey.HasValue ? startKeyBuffer : default,
            endKey.HasValue ? endKeyBuffer : default,
            startKeyExclusive,
            endKeyExclusive,
            sortOrder);
    }

    public static RangeResult GetRange(
        this IKeyValueStore kv,
        string? startKey,
        string? endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending)
    {
        var encoding = kv.KeyEncoding.ToTextEncoding();

        ReadOnlySpan<byte> startKeyBytes = KeyRange.Unbound;
        ReadOnlySpan<byte> endKeyBytes = KeyRange.Unbound;

        byte[]? startKeyBuffer = null;
        byte[]? endKeyBuffer = null;

        if (startKey != null)
        {
            var bufferLength = encoding.GetMaxByteCount(startKey.Length);
            var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);
            var bytesWritten = encoding.GetBytes(startKey, buffer);
            startKeyBytes = buffer.AsSpan(0, bytesWritten);
        }
        if (endKey != null)
        {
            var bufferLength = encoding.GetMaxByteCount(endKey.Length);
            var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);
            var bytesWritten = encoding.GetBytes(endKey, buffer);
            endKeyBytes = buffer.AsSpan(0, bytesWritten);
        }

        try
        {
            return kv.GetRange(
                startKey != null ? startKeyBytes : default,
                endKey != null ? endKeyBytes : default,
                startKeyExclusive,
                endKeyExclusive,
                sortOrder);
        }
        finally
        {
            if (startKeyBuffer != null) ArrayPool<byte>.Shared.Return(startKeyBuffer);
            if (endKeyBuffer != null) ArrayPool<byte>.Shared.Return(endKeyBuffer);
        }
    }
}
