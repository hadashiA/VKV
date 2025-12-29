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
    [StructLayout(LayoutKind.Explicit, Size = 10, Pack = 1)]
    struct NodeEntryMeta
    {
        [FieldOffset(0)]
        public int KeyStart;

        [FieldOffset(4)]
        public ushort KeyLength;

        [FieldOffset(6)]
        public int ValueStart;
    }

#if NETSTANDARD
    readonly ReadOnlySpan<byte> page = page;
#else
    readonly ref byte pageReference = ref MemoryMarshal.GetReference(page);
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetAt(int index, out ReadOnlySpan<byte> key, out PageNumber childPageNumber)
    {
        ref var ptr =
#if NETSTANDARD
            ref MemoryMarshal.GetReference(page);
#else
            ref pageReference;
#endif

        ref var metaPtr = ref Unsafe.Add(ref ptr,
            Unsafe.SizeOf<PageHeader>() + Unsafe.SizeOf<NodeHeader>() +
            index * Unsafe.SizeOf<NodeEntryMeta>());

        var meta = Unsafe.ReadUnaligned<NodeEntryMeta>(ref metaPtr);

        key = MemoryMarshal.CreateReadOnlySpan(
            ref Unsafe.Add(ref ptr, meta.KeyStart),
            meta.KeyLength);

        var childPosition = Unsafe.ReadUnaligned<long>(
            ref Unsafe.Add(ref ptr, meta.ValueStart));

        childPageNumber = new PageNumber(childPosition);
    }

    public bool TrySearch(ReadOnlySpan<byte> key, IKeyEncoding keyEncoding, out PageNumber childPageNumber)
    {
        var min = 0;
        var max = entryCount;

        while (min < max)
        {
            var mid = min + ((max - min) >> 1);

            ref var midKeyPtr = ref GetKeyMeta(mid, out var midKeyLength);

            var midKey = MemoryMarshal.CreateReadOnlySpan(ref midKeyPtr, midKeyLength);
            var cmp = keyEncoding.Compare(midKey, key);
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
        ref var valuePtr = ref GetValueMeta(index);
        childPageNumber = new PageNumber(Unsafe.ReadUnaligned<long>(ref valuePtr));
        return true;
    }

//     bool TrySearchInt64Simd(ReadOnlySpan<byte> key, out PageNumber childPageNumber)
//     {
//         if (entry.UserData is not long[] allKeys)
//         {
//             allKeys = new long[entryCount];
//             ref var ptr =
// #if NETSTANDARD
//                 ref MemoryMarshal.GetReference(page);
// #else
//                 ref pageReference;
// #endif
//
//             Span<long> keys = stackalloc long[entryCount];
//             for (var i = 0; i < entryCount; i++)
//             {
//                 var meta = GetMeta(i);
//                 var keyInt64 = Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref ptr, meta.PageOffset));
//                 allKeys[i] = keyInt64;
//             }
//
//             entry.UserData = allKeys;
//         }
//
//         allKeys
//     }

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
            ref var keyPtr = ref GetKeyMeta(i, out var keyLength);
            ref var valuePtr = ref GetValueMeta(i);

            var key = MemoryMarshal.CreateReadOnlySpan(ref keyPtr, keyLength);
            var childPosition = Unsafe.ReadUnaligned<long>(ref valuePtr);

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
    ref byte GetKeyMeta(int index, out ushort keyLength)
    {
        ref var ptr =
#if NETSTANDARD
            ref MemoryMarshal.GetReference(page);
#else
            ref pageReference;
#endif

        ref var metaPtr = ref Unsafe.Add(ref ptr,
            Unsafe.SizeOf<PageHeader>() + Unsafe.SizeOf<NodeHeader>() +
            index * Unsafe.SizeOf<NodeEntryMeta>());

        var keyStart = Unsafe.ReadUnaligned<int>(ref metaPtr);
        keyLength = Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref metaPtr, sizeof(int)));
        return ref Unsafe.Add(ref ptr, keyStart);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ref byte GetValueMeta(int index)
    {
        ref var ptr =
#if NETSTANDARD
            ref MemoryMarshal.GetReference(page);
#else
            ref pageReference;
#endif

        ref var metaPtr = ref Unsafe.Add(ref ptr,
            Unsafe.SizeOf<PageHeader>() + Unsafe.SizeOf<NodeHeader>() +
            index * Unsafe.SizeOf<NodeEntryMeta>());

        var valueStart = Unsafe.ReadUnaligned<int>(
            ref Unsafe.Add(ref metaPtr, sizeof(int) + sizeof(ushort)));
        return ref Unsafe.Add(ref ptr, valueStart);
    }
}