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
/// </remarks>
readonly ref struct InternalNodeReader(ReadOnlySpan<byte> page, int entryCount)
{
    [StructLayout(LayoutKind.Explicit, Size = 6, Pack = 1)]
    struct NodeEntryMeta
    {
        [FieldOffset(0)]
        public int PageOffset;

        [FieldOffset(4)]
        public ushort KeyLength;
    }

#if NETSTANDARD
    readonly ReadOnlySpan<byte> page = page;
#else
    readonly ref byte pageReference = ref MemoryMarshal.GetReference(page);
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetAt(int index, out ReadOnlySpan<byte> key, out PageNumber childPageNumber)
    {
        var meta = GetMeta(index);
        ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
            ref MemoryMarshal.GetReference(page),
#else
            ref pageReference,
#endif
            meta.PageOffset);

        key = MemoryMarshal.CreateReadOnlySpan(ref ptr, meta.KeyLength);
        ptr = ref Unsafe.Add(ref ptr, meta.KeyLength);

        var childPosition = Unsafe.ReadUnaligned<long>(ref ptr);
        childPageNumber = new PageNumber(childPosition);
    }

    public bool TrySearch(ReadOnlySpan<byte> key, IKeyComparer keyComparer, out PageNumber childPageNumber)
    {
        var min = 0;
        var max = entryCount;

        NodeEntryMeta meta;
        while (min < max)
        {
            var mid = min + ((max - min) >> 1);

            meta = GetMeta(mid);
            ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
                ref MemoryMarshal.GetReference(page),
#else
                ref pageReference,
#endif
                meta.PageOffset);

            var midKey = MemoryMarshal.CreateReadOnlySpan(ref ptr, meta.KeyLength);
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
        meta = GetMeta(index);
        ref var p = ref Unsafe.Add(
#if NETSTANDARD
            ref MemoryMarshal.GetReference(page),
#else
            ref pageReference,
#endif
            meta.PageOffset +  meta.KeyLength);

        childPageNumber = new PageNumber(Unsafe.ReadUnaligned<long>(ref p));
        return true;
    }

    // for debug purpose
    public KeyValuePair<Memory<byte>, long>[] ToArray()
    {
        ref var ptr =
#if NETSTANDARD
            ref MemoryMarshal.GetReference(page);
#else
            ref pageReference;
#endif

        var list = new List<KeyValuePair<Memory<byte>, long>>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            var meta = Unsafe.ReadUnaligned<NodeEntryMeta>(
                ref Unsafe.Add(ref ptr, i * Unsafe.SizeOf<NodeEntryMeta>()));

            var key = MemoryMarshal.CreateReadOnlySpan(
                ref Unsafe.Add(ref ptr, meta.PageOffset),
                meta.KeyLength);

            var childPosition = Unsafe.ReadUnaligned<long>(
                ref Unsafe.Add(ref ptr, meta.PageOffset + meta.KeyLength));

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
    NodeEntryMeta GetMeta(int index)
    {
        ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
            ref MemoryMarshal.GetReference(page),
#else
            ref pageReference,
#endif
            sizeof(int) + Unsafe.SizeOf<NodeHeader>() + index * Unsafe.SizeOf<NodeEntryMeta>());
        return Unsafe.ReadUnaligned<NodeEntryMeta>(ref ptr);
    }
}