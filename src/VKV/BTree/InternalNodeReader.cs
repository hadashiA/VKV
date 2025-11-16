using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace VKV.BTree;

/// <summary>
///  Internal Node Reader
/// </summary>
/// <remarks>
/// (offset-table sizeof(int) * entryCount)
/// ┌────────----┬──────────────┬──────────────┐
/// │ keylen(2) │ key(klen) │ childPosition(8) │
/// ├────----────┼──────────────┼──────────────┤
/// │ keylen(2) │ key(klen) │ childPosition(8) │
/// ├────────┼──────────────┼──────────────┤
/// │   ...   │     ...      │     ...     │
/// └────────┴──────────────┴──────────────┘
/// </remarks>
readonly ref struct InternalNodeReader(in NodeHeader header, ReadOnlySpan<byte> payload)
{
    readonly int entryCount = header.EntryCount;
#if NETSTANDARD
    readonly ReadOnlySpan<byte> payload = payload;
#else
    readonly ref byte payloadReference = ref MemoryMarshal.GetReference(payload);
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetAt(int index, out ReadOnlySpan<byte> key, out long childPosition)
    {
        var offset = PayloadOffsetOf(index);
        ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
            ref MemoryMarshal.GetReference(payload),
#else
            ref payloadReference,
#endif
            offset);

        var keyLength = Unsafe.ReadUnaligned<ushort>(ref ptr);
        ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));

        key = MemoryMarshal.CreateReadOnlySpan(ref ptr, keyLength);
        ptr = ref Unsafe.Add(ref ptr, keyLength);

        childPosition = Unsafe.ReadUnaligned<long>(ref ptr);
    }

    public bool TrySearch(ReadOnlySpan<byte> key, IKeyComparer keyComparer, out PageNumber childPageNumber)
    {
        var min = 0;
        var max = entryCount;

        // upper_bound(keys, key) : 最初に key_i > key となる i を探す
        while (min < max)
        {
            var mid = min + ((max - min) >> 1);

            var offset = PayloadOffsetOf(mid);
            ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
                ref MemoryMarshal.GetReference(payload),
#else
                ref payloadReference,
#endif
                offset);

            var keyLen = Unsafe.ReadUnaligned<ushort>(ref ptr);
            ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));

            var midKey = MemoryMarshal.CreateReadOnlySpan(ref ptr, keyLen);
            ptr = ref Unsafe.Add(ref ptr, keyLen);

            var cmp = keyComparer.Compare(midKey, key);
            if (cmp <= 0) // upper bounds
            {
                min = mid + 1;
            }
            else
            {
                max = mid;
            }
        }

        var index = min == 0 ? 0 : min - 1;
        // 右子はエントリ (lo-1) の末尾にある
        var off = PayloadOffsetOf(index);
        ref var p = ref Unsafe.Add(
#if NETSTANDARD
            ref MemoryMarshal.GetReference(payload),
#else
            ref payloadReference,
#endif
            off);

        var len = Unsafe.ReadUnaligned<ushort>(ref p);
        p = ref Unsafe.Add(ref p, sizeof(ushort) + len);

        childPageNumber = new PageNumber(Unsafe.ReadUnaligned<long>(ref p));
        return true;
    }

    // for debug purpose
    public KeyValuePair<Memory<byte>, long>[] ToArray()
    {
        ref var ptr =
#if NETSTANDARD
            ref MemoryMarshal.GetReference(payload);
#else
            ref payloadReference;
#endif
        ptr = ref Unsafe.Add(ref ptr, entryCount * sizeof(int));

        var list = new List<KeyValuePair<Memory<byte>, long>>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            var keyLen = Unsafe.ReadUnaligned<ushort>(ref ptr);
            ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));

            var key = MemoryMarshal.CreateReadOnlySpan(ref ptr, keyLen);
            ptr = ref Unsafe.Add(ref ptr, keyLen);

            var childPosition = Unsafe.ReadUnaligned<long>(ref ptr);
            ptr = ref Unsafe.Add(ref ptr, sizeof(long));

            list.Add(new KeyValuePair<Memory<byte>, long>(key.ToArray(), childPosition));
        }

        return list.ToArray();
    }

    public string Dump()
    {
        var a = ToArray();
        var b = new StringBuilder();
        foreach (var (k, v) in a)
        {
            b.AppendLine($"k={Encoding.UTF8.GetString(k.Span)},v={v}");
        }
        return b.ToString();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    int PayloadOffsetOf(int index)
    {
        ref var offsetTableReference = ref Unsafe.Add(
#if NETSTANDARD
            ref MemoryMarshal.GetReference(payload),
#else
            ref payloadReference,
#endif
            index * sizeof(int));
        return Unsafe.ReadUnaligned<int>(ref offsetTableReference);
    }
}