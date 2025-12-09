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

    bool IsSupportedType(Type type);
    int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b);
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

    public static Encoding ToTextEncoding(this IKeyEncoding keyEncoding) => keyEncoding switch
    {
        AsciiOrdinalEncoding => Encoding.ASCII,
        Utf8OrdinalEncoding => Encoding.UTF8,
        _ => throw new NotSupportedException($"{keyEncoding} cannot be used with string")
    };
}

public sealed class Int64LittleEndianEncoding : IKeyEncoding
{
    public static readonly Int64LittleEndianEncoding Instance = new();

    public string Id => "i64";

    public bool IsSupportedType(Type type) => type == typeof(long) ||
                                              type == typeof(int) ||
                                              type == typeof(short);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        var na = Unsafe.ReadUnaligned<long>(ref MemoryMarshal.GetReference(a));
        var nb = Unsafe.ReadUnaligned<long>(ref MemoryMarshal.GetReference(b));
        return (na > nb ? 1 : 0) - (na < nb ? 1 : 0);
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        return a.SequenceCompareTo(b);
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

    public bool IsSupportedType(Type type) => type == typeof(string);

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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
    {
        return Compare(x.Span, y.Span);
    }
}
