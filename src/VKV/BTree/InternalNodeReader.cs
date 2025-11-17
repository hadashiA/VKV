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
readonly ref struct InternalNodeReader(in NodeHeader header, ReadOnlySpan<byte> payload)
{
    [StructLayout(LayoutKind.Explicit, Size = 6, Pack = 1)]
    struct NodeEntryMeta
    {
        [FieldOffset(0)]
        public int PayloadOffset;

        [FieldOffset(4)]
        public ushort KeyLength;
    }

    readonly int entryCount = header.EntryCount;
#if NETSTANDARD
    readonly ReadOnlySpan<byte> payload = payload;
#else
    readonly ref byte payloadReference = ref MemoryMarshal.GetReference(payload);
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetAt(int index, out ReadOnlySpan<byte> key, out long childPosition)
    {
        var meta = GetMeta(index);
        ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
            ref MemoryMarshal.GetReference(payload),
#else
            ref payloadReference,
#endif
            meta.PayloadOffset);

        key = MemoryMarshal.CreateReadOnlySpan(ref ptr, meta.KeyLength);
        ptr = ref Unsafe.Add(ref ptr, meta.KeyLength);

        childPosition = Unsafe.ReadUnaligned<long>(ref ptr);
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
                ref MemoryMarshal.GetReference(payload),
#else
                ref payloadReference,
#endif
                meta.PayloadOffset);

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
            ref MemoryMarshal.GetReference(payload),
#else
            ref payloadReference,
#endif
            meta.PayloadOffset +  meta.KeyLength);

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

        var list = new List<KeyValuePair<Memory<byte>, long>>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            var meta = Unsafe.ReadUnaligned<NodeEntryMeta>(
                ref Unsafe.Add(ref ptr, i * Unsafe.SizeOf<NodeEntryMeta>()));

            var key = MemoryMarshal.CreateReadOnlySpan(
                ref Unsafe.Add(ref ptr, meta.PayloadOffset),
                meta.KeyLength);

            var childPosition = Unsafe.ReadUnaligned<long>(
                ref Unsafe.Add(ref ptr, meta.PayloadOffset + meta.KeyLength));

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
            ref MemoryMarshal.GetReference(payload),
#else
            ref payloadReference,
#endif
            index * Unsafe.SizeOf<NodeEntryMeta>());
        return Unsafe.ReadUnaligned<NodeEntryMeta>(ref ptr);
    }
}