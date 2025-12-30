using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace VKV;

public interface IKeyEncoding : IComparer<ReadOnlyMemory<byte>>
{
    string Id { get; }

    int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b);

    int GetMaxEncodedByteCount<TKey>(TKey key)
        where TKey : IComparable<TKey>;

    bool TryEncode<TKey>(TKey key, Span<byte> destination, out int bytesWritten)
        where TKey : IComparable<TKey>;

    bool TryEncode(string formattedString, Span<byte> destination, out int bytesWritten);
}

public static class KeyEncoding
{
    public static Int64LittleEndianEncoding Int64LittleEndian => Int64LittleEndianEncoding.Instance;
    public static AsciiOrdinalEncoding Ascii => AsciiOrdinalEncoding.Instance;

    static readonly ConcurrentDictionary<string, IKeyEncoding> registry = new();

    public static IKeyEncoding FromId(string id)
    {
        return id switch
        {
            "i64" => Int64LittleEndianEncoding.Instance,
            "ascii" => AsciiOrdinalEncoding.Instance,
            _ => registry[id]
        };
    }

    public static void Register(IKeyEncoding customEncoding)
    {
        if (!registry.TryAdd(customEncoding.Id, customEncoding))
        {
            throw new ArgumentException($"The encoding {customEncoding.Id} already exists.");
        }
    }
}

public sealed class Int64LittleEndianEncoding : IKeyEncoding
{
    public static readonly Int64LittleEndianEncoding Instance = new();

    public string Id => "i64";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        var na = Unsafe.ReadUnaligned<long>(ref MemoryMarshal.GetReference(a));
        var nb = Unsafe.ReadUnaligned<long>(ref MemoryMarshal.GetReference(b));
        return (na > nb ? 1 : 0) - (na < nb ? 1 : 0);
    }

    public int GetMaxEncodedByteCount<TKey>(TKey key)
        where TKey : IComparable<TKey> => sizeof(long);

    public bool TryEncode<TKey>(TKey key, Span<byte> destination, out int bytesWritten)
        where TKey : IComparable<TKey>
    {
        long longKey = 0;
        switch (key)
        {
            case long i64:
                longKey = i64;
                break;
            case int i32:
                longKey = i32;
                break;
            case short i16:
                longKey = i16;
                break;
            case ulong u64:
                longKey = (long)u64;
                break;
            case uint u32:
                longKey = u32;
                break;
            case ushort u16:
                longKey = u16;
                break;
            default:
                KeyEncodingMismatchException.ThrowCannotEncodeInt64(typeof(TKey));
                break;
        }

        Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(destination), longKey);
        bytesWritten = sizeof(long);
        return true;
    }

    public bool TryEncode(string formattedString, Span<byte> destination, out int bytesWritten)
    {
        if (long.TryParse(formattedString, out var longValue))
        {
            Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(destination), longValue);
            bytesWritten = sizeof(long);
        }
        bytesWritten = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlyMemory<byte> a, ReadOnlyMemory<byte> b)
    {
        var na = Unsafe.ReadUnaligned<long>(ref MemoryMarshal.GetReference(a.Span));
        var nb = Unsafe.ReadUnaligned<long>(ref MemoryMarshal.GetReference(b.Span));
        return (na > nb ? 1 : 0) - (na < nb ? 1 : 0);
    }
}

public sealed class AsciiOrdinalEncoding : IKeyEncoding
{
    public static readonly AsciiOrdinalEncoding Instance = new();

    public string Id => "ascii";

    public bool IsSupportedType(Type type) => type == typeof(string);

    public void ThrowIfNotSupportedType(Type type)
    {
        if (type != typeof(string))
        {
            throw new KeyEncodingMismatchException($"Cannot convert as string from {type}");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        return a.SequenceCompareTo(b);
    }

    public int GetMaxEncodedByteCount<TKey>(TKey key) where TKey : IComparable<TKey>
    {
        var stringKey = Unsafe.As<TKey, string>(ref key);
        return Encoding.ASCII.GetMaxByteCount(stringKey.Length);
    }

    public bool TryEncode<TKey>(TKey key, Span<byte> destination, out int bytesWritten)
        where TKey : IComparable<TKey>
    {
        var stringKey = Unsafe.As<TKey, string>(ref key);
#if NET8_0_OR_GREATER
        return Encoding.ASCII.TryGetBytes(stringKey, destination, out bytesWritten);
#else
        try
        {
            bytesWritten = Encoding.ASCII.GetBytes(stringKey, destination);
            return true;
        }
        catch (ArgumentException ex)
        {
            bytesWritten = 0;
            return false;
        }
#endif
    }

    public bool TryEncode(string formattedString, Span<byte> destination, out int bytesWritten)
    {
#if NET8_0_OR_GREATER
        return Encoding.ASCII.TryGetBytes(formattedString, destination, out bytesWritten);
#else
        try
        {
            bytesWritten = Encoding.ASCII.GetBytes(formattedString, destination);
            return true;
        }
        catch (ArgumentException ex)
        {
            bytesWritten = 0;
            return false;
        }
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
    {
        return Compare(x.Span, y.Span);
    }
}

