using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using VKV.Internal;

namespace VKV;

public interface IKeyEncoding : IComparer<ReadOnlyMemory<byte>>
{
    string Id { get; }

    int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b);

    int GetMaxEncodedByteCount<TKey>(TKey key)
        where TKey : IComparable<TKey>;

    bool TryEncode<TKey>(TKey key, Span<byte> destination, out int bytesWritten)
        where TKey : IComparable<TKey>;
}

public static class KeyEncoding
{
    public static Int64LittleEndianEncoding Int64LittleEndian => Int64LittleEndianEncoding.Instance;
    public static AsciiOrdinalEncoding Ascii => AsciiOrdinalEncoding.Instance;
    public static Utf8OrdinalEncoding Utf8 => Utf8OrdinalEncoding.Instance;

    static readonly ConcurrentDictionary<string, IKeyEncoding> registry = new();

    public static IKeyEncoding FromId(string id)
    {
        return id switch
        {
            "i64" => Int64LittleEndianEncoding.Instance,
            "ascii" => AsciiOrdinalEncoding.Instance,
            "u8" => Utf8OrdinalEncoding.Instance,
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

    internal static PooledBuffer<byte> EncodeMemory<TKey>(this IKeyEncoding keyEncoding, TKey key)
        where TKey : IComparable<TKey>
    {
        var initialBufferSize = keyEncoding.GetMaxEncodedByteCount(key);
        var buffer = ArrayPool<byte>.Shared.Rent(initialBufferSize);
        int bytesWritten;
        while (!keyEncoding.TryEncode(key, buffer, out bytesWritten))
        {
            var newLength = buffer.Length * 2;
            ArrayPool<byte>.Shared.Return(buffer);
            buffer = ArrayPool<byte>.Shared.Rent(newLength);
        }
        return new PooledBuffer<byte>(buffer, 0, bytesWritten);
    }
}

public sealed class Int64LittleEndianEncoding : IKeyEncoding
{
    public static readonly Int64LittleEndianEncoding Instance = new();

    public string Id => "i64";

    public void ThrowIfNotSupportedType(Type type)
    {
        throw new NotImplementedException();
    }

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
        bytesWritten = Encoding.ASCII.GetBytes(stringKey, destination);
        return true;
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
    {
        return Compare(x.Span, y.Span);
    }
}

public sealed class Utf8OrdinalEncoding : IKeyEncoding
{
    public static readonly Utf8OrdinalEncoding Instance = new();

    public string Id => "u8";

    public void ThrowIfNotSupportedType(Type type)
    {
        if (type != typeof(string))
        {
            throw new KeyEncodingMismatchException($"Cannot convert as string from {type}");
        }
    }

    public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int ia = 0, ib = 0;

        while (ia < a.Length && ib < b.Length)
        {
            while (ia < a.Length && ib < b.Length)
            {
                var ba = a[ia];
                var bb = b[ib];
                var aAscii = ba < 0x80;
                var bAscii = bb < 0x80;

                if (aAscii && bAscii)
                {
                    if (ba != bb) return ba - bb;
                    ia++; ib++;
                    continue;
                }
                break;
            }

            if (ia >= a.Length || ib >= b.Length) break;

            throw new NotSupportedException("TODO");

            var sa = a[ia..];
            var sb = b[ib..];

            // var statusA = Rune.DecodeFromUtf8(sa, out var ra, out var ca);
            // var statusB = Rune.DecodeFromUtf8(sb, out var rb, out var cb);
            //
            // if (statusA != OperationStatus.Done)
            // {
            //     ra = new Rune(0xFFFD);
            //     ca = Math.Max(1, Math.Min(4, sa.Length));
            // }
            //
            // if (statusB != OperationStatus.Done)
            // {
            //     rb = new Rune(0xFFFD);
            //     cb = Math.Max(1, Math.Min(4, sb.Length));
            // }
            //
            // if (ra.Value != rb.Value)
            //     return ra.Value - rb.Value;
            //
            // ia += ca;
            // ib += cb;
        }

        return (a.Length - ia) - (b.Length - ib);
    }

    public int GetMaxEncodedByteCount<TKey>(TKey key) where TKey : IComparable<TKey>
    {
        var stringKey = Unsafe.As<TKey, string>(ref key);
        return Encoding.UTF8.GetMaxByteCount(stringKey.Length);
    }

    public bool TryEncode<TKey>(TKey key, Span<byte> destination, out int bytesWritten)
        where TKey : IComparable<TKey>
    {
        var stringKey = Unsafe.As<TKey, string>(ref key);
#if NET8_0_OR_GREATER
        return Encoding.UTF8.TryGetBytes(stringKey, destination, out bytesWritten);
#else
        bytesWritten = Encoding.UTF8.GetBytes(stringKey, destination);
        return true;
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
    {
        return Compare(x.Span, y.Span);
    }
}
