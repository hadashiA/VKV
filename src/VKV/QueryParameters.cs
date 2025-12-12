using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace VKV;

public enum SortOrder
{
    Ascending,
    Descending,
}

public static class KeyRange
{
    public static byte[] Unbound => [];
}

public readonly partial struct Query
{
    public ReadOnlyMemory<byte> StartKey { get; init; }
    public ReadOnlyMemory<byte> EndKey { get; init; }
    public bool StartKeyExclusive { get; init; }
    public bool EndKeyExclusive { get; init; }
    public SortOrder SortOrder { get; init; }

    public static implicit operator QueryRef(Query q) => new()
    {
        StartKey = q.StartKey.Span,
        EndKey = q.EndKey.Span,
        StartKeyExclusive = q.StartKeyExclusive,
        EndKeyExclusive = q.EndKeyExclusive,
        SortOrder = q.SortOrder
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ValidateRange(IKeyEncoding keyEncoding)
    {
        if (!StartKey.IsEmpty && !EndKey.IsEmpty)
        {
            // validate start/end order
            if (keyEncoding.Compare(StartKey, EndKey) > 0)
            {
                throw new ArgumentException("StartKey is greater than EndKey");
            }
        }
    }
}

public readonly struct Query<TKey> where TKey : IComparable<TKey>
{
    public TKey? StartKey { get; init; }
    public TKey? EndKey { get; init; }
    public bool StartKeyExclusive { get; init; }
    public bool EndKeyExclusive { get; init; }
    public SortOrder SortOrder { get; init; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ValidateRange()
    {
        if (StartKey != null && EndKey != null &&
            StartKey.CompareTo(EndKey) > 0)
        {
            throw new ArgumentException("StartKey is greater than EndKey");
        }
    }

    public QueryRef ToEncodedQueryRef(IKeyEncoding keyEncoding)
    {
    }

    public Query ToEncodedQuery(IKeyEncoding keyEncoding)
    {
        ReadOnlyMemory<byte> startKey;
        ReadOnlyMemory<byte> endKey;

        byte[]? startKeyBuffer;
        byte[]? endKeyBuffer;

        if (StartKey != null)
        {
            var initialBufferSize = keyEncoding.GetMaxEncodedByteCount(StartKey);
            startKeyBuffer = ArrayPool<byte>.Shared.Rent(initialBufferSize);

            int bytesWritten;
            while (!keyEncoding.TryEncode(StartKey, startKeyBuffer, out bytesWritten))
            {
                ArrayPool<byte>.Shared.Return(startKeyBuffer);
                startKeyBuffer = ArrayPool<byte>.Shared.Rent(startKeyBuffer.Length * 2);
            }
            startKey =  startKeyBuffer.AsMemory(0, bytesWritten);
        }

        if (EndKey != null)
        {
        }
    }
}

public readonly ref struct QueryRef
{
    public ReadOnlySpan<byte> StartKey { get; init; }
    public ReadOnlySpan<byte> EndKey { get; init; }
    public bool StartKeyExclusive { get; init; }
    public bool EndKeyExclusive { get; init; }
    public SortOrder SortOrder { get; init; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ValidateRange(IKeyEncoding keyEncoding)
    {
        if (!StartKey.IsEmpty && !EndKey.IsEmpty)
        {
            // validate start/end order
            if (keyEncoding.Compare(StartKey, EndKey) > 0)
            {
                throw new ArgumentException("StartKey is greater than EndKey");
            }
        }
    }
}

p
public readonly partial struct Query
{
    public static Query GreaterThan(
        ReadOnlyMemory<byte> key,
        SortOrder sortOrder = SortOrder.Ascending) => new()
    {
        StartKey = key,
        EndKey = KeyRange.Unbound,
        StartKeyExclusive = true,
        EndKeyExclusive = false,
        SortOrder = sortOrder
    };

    public static Query GreaterThanOrEqualTo(
        ReadOnlyMemory<byte> key,
        SortOrder sortOrder = SortOrder.Ascending) => new()
    {
        StartKey = key,
        EndKey = KeyRange.Unbound,
        StartKeyExclusive = false,
        EndKeyExclusive = false,
        SortOrder = sortOrder
    };

    public static Query LessThan(
        ReadOnlyMemory<byte> key,
        SortOrder sortOrder = SortOrder.Ascending) => new()
    {
        StartKey = KeyRange.Unbound,
        EndKey = key,
        StartKeyExclusive = true,
        EndKeyExclusive = false,
        SortOrder = sortOrder
    };

    public static Query LessThanOrEqualTo(
        ReadOnlyMemory<byte> key,
        SortOrder sortOrder = SortOrder.Ascending) => new()
    {
        StartKey = KeyRange.Unbound,
        EndKey = key,
        StartKeyExclusive = false,
        EndKeyExclusive = false,
        SortOrder = sortOrder
    };

    public static Query Between(
        ReadOnlyMemory<byte> startKey,
        ReadOnlyMemory<byte> endKey,
        bool startKeyExclusive = false,
        bool endKeyExclusive = false,
        SortOrder sortOrder = SortOrder.Ascending) => new()
    {
        StartKey = startKey,
        EndKey = endKey,
        StartKeyExclusive = startKeyExclusive,
        EndKeyExclusive = endKeyExclusive,
        SortOrder = sortOrder
    };

    public static QueryRef GreaterThan(
        ReadOnlySpan<byte> key,
        SortOrder sortOrder = SortOrder.Ascending) => new()
    {
        StartKey = key,
        EndKey = KeyRange.Unbound,
        StartKeyExclusive = true,
        EndKeyExclusive = false,
        SortOrder = sortOrder
    };

    public static QueryRef LessThan(
        ReadOnlySpan<byte> key,
        SortOrder sortOrder = SortOrder.Ascending) => new()
    {
        StartKey = KeyRange.Unbound,
        EndKey = key,
        StartKeyExclusive = true,
        EndKeyExclusive = false,
        SortOrder = sortOrder
    };
}
