using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace VKV.Internal;

/// <summary>
/// Key encoding with int 32 `rid` appended at the end. Mainly for BTree that allows duplicate keys.
/// </summary>
public class CompositeKeyEncoding(IKeyEncoding sourceEncoding) : IKeyEncoding
{
    public string Id => sourceEncoding.Id;

    public int Compare(ReadOnlyMemory<byte> a, ReadOnlyMemory<byte> b)
    {
        var aOriginal = a.Span[..^sizeof(int)];
        var bOriginal = b.Span[..^sizeof(int)];
        var sourceResult = sourceEncoding.Compare(aOriginal, bOriginal);
        if (sourceResult == 0)
        {
            ref var aPtr = ref MemoryMarshal.GetReference(a.Span);
            ref var bPtr = ref MemoryMarshal.GetReference(b.Span);

            var aValueId = Unsafe.ReadUnaligned<int>(
                ref Unsafe.Add(ref aPtr, a.Length - sizeof(int)));
            var bValueId = Unsafe.ReadUnaligned<int>(
                ref Unsafe.Add(ref bPtr, b.Length - sizeof(int)));

            if (aValueId < bValueId) return -1;
            if (aValueId > bValueId) return 1;
            return 0;
        }
        return sourceResult;
    }

    public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        var aOriginal = a[..^sizeof(int)];
        var bOriginal = b[..^sizeof(int)];
        var sourceResult = sourceEncoding.Compare(aOriginal, bOriginal);
        if (sourceResult == 0)
        {
            ref var aPtr = ref MemoryMarshal.GetReference(a);
            ref var bPtr = ref MemoryMarshal.GetReference(b);

            var aValueId = Unsafe.ReadUnaligned<int>(
                ref Unsafe.Add(ref aPtr, a.Length - sizeof(int)));
            var bValueId = Unsafe.ReadUnaligned<int>(
                ref Unsafe.Add(ref bPtr, b.Length - sizeof(int)));

            if (aValueId < bValueId) return -1;
            if (aValueId > bValueId) return 1;
            return 0;
        }
        return sourceResult;
    }

    public int GetMaxEncodedByteCount<TKey>(TKey key) where TKey : IComparable<TKey>
    {
        return sourceEncoding.GetMaxEncodedByteCount(key) + sizeof(int);
    }

    public bool TryEncode<TKey>(TKey key, Span<byte> destination, out int bytesWritten)
        where TKey : IComparable<TKey>
    {
        throw new NotImplementedException();
    }

    public bool TryEncode(string formattedString, Span<byte> destination, out int bytesWritten)
    {
        throw new NotImplementedException();
    }
}