using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace VKV.BTree;

/// <summary>
///  Leaf Node Reader
/// </summary>>
/// <remarks>
/// </remarks>
readonly ref struct LeafNodeReader(ReadOnlySpan<byte> page, int entryCount)
{
    [StructLayout(LayoutKind.Explicit, Size = 12, Pack = 1)]
    struct NodeEntryMeta
    {
        [FieldOffset(0)]
        public int KeyStart;

        [FieldOffset(4)]
        public ushort KeyLength;

        [FieldOffset(6)]
        public int ValueStart;

        [FieldOffset(10)]
        public ushort ValueLength;
    }

#if NETSTANDARD
    readonly ReadOnlySpan<byte> page = page;
#else
    readonly ref byte pageReference = ref MemoryMarshal.GetReference(page);
#endif

    public void GetAt(int index, out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
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

        value = MemoryMarshal.CreateReadOnlySpan(
            ref Unsafe.Add(ref ptr, meta.ValueStart),
            meta.ValueLength);
    }

    public void GetAt(int index, out ReadOnlySpan<byte> key, out int valuePageOffset, out int valueLength)
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

        valuePageOffset = meta.ValueStart;
        valueLength = meta.ValueLength;
    }

    public bool TryFindValue(
        scoped ReadOnlySpan<byte> key,
        IKeyEncoding keyEncoding,
        out int valueOffset,
        out ushort valueLength,
        out ushort valueIndex)
    {
        if (TrySearch(key, SearchOperator.Equal, keyEncoding, out var index))
        {
            valueIndex = (ushort)index;
            (valueOffset, valueLength) = GetValueOffset(index);
            return true;
        }

        valueOffset = default;
        valueLength = default;
        valueIndex = default;
        return false;
    }

    public bool TrySearch(
        ReadOnlySpan<byte> key,
        SearchOperator op,
        IKeyEncoding keyEncoding,
        out int index)
    {
        var min = 0;
        var max = entryCount;
        var resultIndex = -1;

        while (min < max)
        {
            var midIndex = min + ((max - min) >> 1);
            ref var midKeyPtr = ref GetKeyMeta(midIndex, out var midKeyLength);

            var midKey = MemoryMarshal.CreateReadOnlySpan(ref midKeyPtr, midKeyLength);

            var compared = keyEncoding.Compare(midKey, key);
            switch (op)
            {
                case SearchOperator.Equal:
                    if (compared == 0)
                    {
                        index = midIndex;
                        return true;
                    }
                    if (compared < 0)
                    {
                        min = midIndex + 1;
                        resultIndex = min;
                    }
                    else
                    {
                        max = midIndex;
                    }
                    break;
                case SearchOperator.LowerBound:
                    if (compared < 0)
                    {
                        min = midIndex + 1;
                    }
                    else
                    {
                        max = midIndex;
                        resultIndex = midIndex;
                    }
                    break;
                case SearchOperator.UpperBound:
                    if (compared <= 0)
                    {
                        min = midIndex + 1;
                    }
                    else
                    {
                        max = midIndex;
                        resultIndex = max;
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(op), op, null);
            }
        }

        if (resultIndex < 0)
        {
            index = default;
            return false;
        }

        switch (op)
        {
            case SearchOperator.Equal:
                if (min < max)
                {
                    ref var foundKeyPtr = ref GetKeyMeta(min, out var foundKeyLength);
                    var foundKey = MemoryMarshal.CreateReadOnlySpan(ref foundKeyPtr, foundKeyLength);
                    if (keyEncoding.Compare(foundKey, key) == 0)
                    {
                        index = min;
                        return true;
                    }
                }
                index = default;
                return false;

            case SearchOperator.LowerBound:
                // >= key
                index = min;
                return true;

            case SearchOperator.UpperBound:
                // > key
                index = min;
                return true;

            default:
                index = default;
                return false;
        }
    }

    // for debug purpose
    public KeyValuePair<Memory<byte>, Memory<byte>>[] ToArray()
    {
        ref var ptr =
#if NETSTANDARD
            ref MemoryMarshal.GetReference(page);
#else
            ref pageReference;
#endif

        var list = new List<KeyValuePair<Memory<byte>, Memory<byte>>>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            ref var keyPtr = ref GetKeyMeta(i, out var keyLength);
            ref var valuePtr = ref GetKeyMeta(i, out var valueLength);

            var key = MemoryMarshal.CreateReadOnlySpan(ref keyPtr, keyLength);
            var value = MemoryMarshal.CreateReadOnlySpan(ref valuePtr, valueLength);

            list.Add(new KeyValuePair<Memory<byte>, Memory<byte>>(key.ToArray(), value.ToArray()));
        }
        return list.ToArray();
    }

    public string Dump()
    {
        var b =  new StringBuilder();
        var a = ToArray();
        foreach (var (k, v) in a)
        {
            b.AppendLine($"k={Encoding.UTF8.GetString(k.Span)}, v={Encoding.UTF8.GetString(v.Span)}");
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
    ref byte GetValueMeta(int index, out ushort valueLength)
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

        metaPtr = ref Unsafe.Add(ref metaPtr, sizeof(int) + sizeof(ushort));
        var valueStart = Unsafe.ReadUnaligned<int>(ref metaPtr);

        metaPtr = ref Unsafe.Add(ref metaPtr, sizeof(int));
        valueLength = Unsafe.ReadUnaligned<ushort>(ref metaPtr);
        return ref Unsafe.Add(ref ptr, valueStart);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    (int ValueStart, ushort ValueLength) GetValueOffset(int index)
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

        metaPtr = ref Unsafe.Add(ref metaPtr, sizeof(int) + sizeof(ushort));
        var valueStart = Unsafe.ReadUnaligned<int>(ref metaPtr);

        metaPtr = ref Unsafe.Add(ref metaPtr, sizeof(int));
        var valueLength = Unsafe.ReadUnaligned<ushort>(ref metaPtr);
        return (valueStart, valueLength);
    }
}
