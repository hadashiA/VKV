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
    readonly SortedList<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> list = new(keyEncoding);

    public void Add(long key, ReadOnlyMemory<byte> value)
    {
        KeyEncodingMismatchException.ThrowIfCannotEncodeInt64(keyEncoding);

        var keyBytes = new byte[24];
        Utf8Formatter.TryFormat(key, keyBytes, out var bytesWritten);
        Add(keyBytes.AsMemory(0, bytesWritten), value);
    }

    public void Add(string key, ReadOnlyMemory<byte> value)
    {
        Add(keyEncoding.ToTextEncoding().GetBytes(key), value);
    }

    public void Add(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
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