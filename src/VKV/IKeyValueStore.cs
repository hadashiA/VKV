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
        TKey key, CancellationToken cancellationToken = default)
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
}
