using System;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;

namespace VKV.Internal;

class KeyValueList(
    IKeyEncoding keyEncoding,
    bool unique = true)
    : IEnumerable<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>>
{
    public int Count => list.Count;

    readonly SortedList<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> list = new(
        unique
            ? keyEncoding
            : new DuplicateComparer<ReadOnlyMemory<byte>>(keyEncoding));

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
        var buffer = new byte[initialBufferSize];
        int bytesWritten;
        while (!keyEncoding.TryEncode(key, buffer, out bytesWritten))
        {
            buffer = new byte[buffer.Length * 2];
        }
        Add(buffer.AsMemory(0, bytesWritten), value);
    }

    public void Add(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        // ArgumentOutOfRangeException.ThrowIfGreaterThan(key.Length,  ushort.MaxValue);
        // ArgumentOutOfRangeException.ThrowIfGreaterThan(value.Length, ushort.MaxValue);

        if (unique)
        {
            if (!list.TryAdd(key, value))
            {
                throw new ArgumentException("duplicate key");
            }
        }
        else
        {
            list.Add(key, value);
        }
    }

    public IEnumerator<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> GetEnumerator()
    {
        return unique
            ? list.GetEnumerator()
            : GenerateCompositeKeyValues();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    IEnumerator<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> GenerateCompositeKeyValues()
    {
        var rid = 0;
        var prevKey = default(ReadOnlyMemory<byte>);
        foreach (var x in list)
        {
            if (prevKey.IsEmpty || !prevKey.Span.SequenceEqual(x.Key.Span))
            {
                rid = 0;
            }
            var compositeKey = new byte[x.Key.Length + sizeof(int)];
            CompositeKey.TryGenerate(x.Key, ++rid, compositeKey);
            yield return new KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>(compositeKey, x.Value);
            prevKey = x.Key;
        }
    }
}