using System;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace VKV.Internal;

class KeyValueList : IEnumerable<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>>
{
    readonly SortedList<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> list;
    readonly bool unique;
    readonly KeyEncoding keyEncoding;

    public KeyValueList(KeyEncoding keyEncoding, bool unique = true)
    {
        this.keyEncoding = keyEncoding;
        this.unique = unique;
        var comparer = KeyComparer.From(keyEncoding);
        list = new SortedList<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>((IComparer<ReadOnlyMemory<byte>>)comparer);
    }

    public void Add(long key, ReadOnlyMemory<byte> value)
    {
        if (keyEncoding != KeyEncoding.Int64LittleEndian)
        {
            throw new NotSupportedException($"{keyEncoding} cannot be used with Int64LittleEndian");
        }
        var keyBytes = new byte[24];
        Utf8Formatter.TryFormat(key, keyBytes, out var bytesWritten);
        Add(keyBytes.AsMemory(0, bytesWritten), value);
    }

    public void Add(string key, ReadOnlyMemory<byte> value)
    {
        var encoding = keyEncoding switch
        {
            KeyEncoding.Ascii => Encoding.ASCII,
            KeyEncoding.Utf8 => Encoding.UTF8,
            _ => throw new NotSupportedException($"{keyEncoding} cannot be used with string")
        };
        Add(encoding.GetBytes(key), value);
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