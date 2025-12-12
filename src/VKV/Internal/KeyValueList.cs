using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;

namespace VKV.Internal;

class KeyValueList(
    IKeyEncoding keyEncoding,
    bool unique = true)
    : IEnumerable<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>>
{
    readonly SortedList<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> list = new(keyEncoding);

    public void Add(long key, ReadOnlyMemory<byte> value)
    {
        KeyEncodingMismatchException.ThrowIfCannotEncodeInt64(keyEncoding);

        var keyBytes = new byte[sizeof(long)];
        Utf8Formatter.TryFormat(key, keyBytes, out var bytesWritten);
        Add(keyBytes.AsMemory(0, bytesWritten), value);
    }

    public void Add<TKey>(TKey key, ReadOnlyMemory<byte> value) where TKey : IComparable<TKey>
    {
        var initialBufferSize = keyEncoding.GetMaxEncodedByteCount(key);
        var buffer = ArrayPool<byte>.Shared.Rent(initialBufferSize);
        int bytesWritten;
        while (!keyEncoding.TryEncode(key, buffer, out bytesWritten))
        {
            ArrayPool<byte>.Shared.Return(buffer);
            buffer = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
        }
        Add((ReadOnlyMemory<byte>)buffer.AsMemory(0, bytesWritten), value);
    }

    public void Add(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        // ArgumentOutOfRangeException.ThrowIfGreaterThan(key.Length,  ushort.MaxValue);
        // ArgumentOutOfRangeException.ThrowIfGreaterThan(value.Length, ushort.MaxValue);

        if (unique)
        {
            if (list.ContainsKey(key))
            {
                throw new ArgumentException("duplicate key");
            }
        }
        list.Add(key, value);
    }

    public IEnumerator<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> GetEnumerator() =>
        list.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}