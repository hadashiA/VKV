using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace VKV;

public interface IKeyComparer
{
    int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b);
}

public static class KeyComparer
{
    public static IKeyComparer From(KeyEncoding encoding)
    {
        return encoding switch
        {
            KeyEncoding.Ascii => AsciiOrdinalComparer.Instance,
            KeyEncoding.Utf8 => Utf8OrdinalComparer.Instance,
            KeyEncoding.Int64LittleEndian => Int64LittleEndianComparer.Instance,
            _ => throw new ArgumentOutOfRangeException(nameof(encoding), encoding, null)
        };
    }
}

public sealed class Int64LittleEndianComparer : IKeyComparer, IComparer<ReadOnlyMemory<byte>>
{
    public static readonly IKeyComparer Instance = new Int64LittleEndianComparer();

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

public sealed class AsciiOrdinalComparer : IKeyComparer, IComparer<ReadOnlyMemory<byte>>
{
    public static readonly IKeyComparer Instance = new AsciiOrdinalComparer();

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

public sealed class Utf8OrdinalComparer : IKeyComparer, IComparer<ReadOnlyMemory<byte>>
{
    public static readonly IKeyComparer Instance = new Utf8OrdinalComparer();

    public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int ia = 0, ib = 0;

        while (ia < a.Length && ib < b.Length)
        {
            // ASCII 連続領域はバイト比較で前倒し
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
                break; // どちらか非ASCII
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

        // 先頭一致だったので長さで決着
        return (a.Length - ia) - (b.Length - ib);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
    {
        return Compare(x.Span, y.Span);
    }
}
