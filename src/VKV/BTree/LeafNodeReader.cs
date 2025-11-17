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
readonly ref struct LeafNodeReader(NodeHeader header, ReadOnlySpan<byte> payload)
{
    [StructLayout(LayoutKind.Explicit, Size = 6, Pack = 1)]
    struct NodeEntryMeta
    {
        [FieldOffset(0)]
        public int PayloadOffset;

        [FieldOffset(4)]
        public ushort KeyLength;

        [FieldOffset(6)]
        public ushort ValueLength;
    }

#if NETSTANDARD
    readonly ReadOnlySpan<byte> payload = payload;
#else
    readonly ref byte payloadReference = ref MemoryMarshal.GetReference(payload);
#endif
    readonly int entryCount = header.EntryCount;

    public void GetAt(
        int index,
        out ReadOnlySpan<byte> key,
        out ReadOnlySpan<byte> value,
        out int? nextIndex)
    {
#if NETSTANDARD
        ref var payloadReference = ref MemoryMarshal.GetReference(payload);
#endif
        var meta = GetMeta(index);
        ref var ptr = ref Unsafe.Add(ref payloadReference, meta.PayloadOffset);
        key = MemoryMarshal.CreateReadOnlySpan(ref ptr, meta.KeyLength);
        ptr = ref Unsafe.Add(ref ptr, meta.KeyLength);

        value = MemoryMarshal.CreateReadOnlySpan(ref ptr, meta.ValueLength);

        nextIndex = index + 1 < entryCount ?  index + 1 : null;
    }

    public void GetAt(
        int index,
        out ReadOnlySpan<byte> key,
        out int valuePayloadOffset,
        out int valueLength,
        out int? nextIndex)
    {
#if NETSTANDARD
        ref var payloadReference = ref MemoryMarshal.GetReference(payload);
#endif
        var meta = GetMeta(index);
        ref var ptr = ref Unsafe.Add(ref payloadReference, meta.PayloadOffset);
        key = MemoryMarshal.CreateReadOnlySpan(ref ptr, meta.KeyLength);
        ptr = ref Unsafe.Add(ref ptr, meta.KeyLength);

        valuePayloadOffset = meta.PayloadOffset + meta.KeyLength;
        valueLength = meta.ValueLength;

        nextIndex = index + 1 < entryCount ?  index + 1 : null;
    }

    public bool TryFindValue(
        scoped ReadOnlySpan<byte> key,
        IKeyComparer keyComparer,
        out int valuePayloadOffset,
        out int valueLength)
    {
        if (TrySearch(key, SearchOperator.Equal, keyComparer, out var index))
        {
            var meta = GetMeta(index);

            ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
                ref MemoryMarshal.GetReference(payload),
#else
                ref payloadReference,
#endif
                meta.PayloadOffset);

            valuePayloadOffset = meta.PayloadOffset + meta.KeyLength;
            valueLength = meta.ValueLength;
            return true;
        }

        valuePayloadOffset = default;
        valueLength = default;
        return false;
    }

    public bool TrySearch(
        ReadOnlySpan<byte> key,
        SearchOperator op,
        IKeyComparer keyComparer,
        out int index)
    {
        var min = 0;
        var max = entryCount;
        var resultIndex = -1;

        while (min < max)
        {
            var midIndex = min + ((max - min) >> 1);
            var midMeta = GetMeta(midIndex);

            ref var ptr = ref Unsafe.Add(
#if  NETSTANDARD
                ref MemoryMarshal.GetReference(payload),
#else
                ref payloadReference,
#endif
                midMeta.PayloadOffset);

            var midKey = MemoryMarshal.CreateReadOnlySpan(ref ptr, midMeta.KeyLength);

            var compared = keyComparer.Compare(midKey, key);
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
                        resultIndex = min;
                    }
                    else
                    {
                        max = midIndex;
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
                    var meta = GetMeta(min);
                    ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
                        ref MemoryMarshal.GetReference(payload),
#else
                        ref payloadReference,
#endif
                        meta.PayloadOffset);
                    var foundKey = MemoryMarshal.CreateReadOnlySpan(ref ptr, meta.KeyLength);
                    if (keyComparer.Compare(foundKey, key) == 0)
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
            ref MemoryMarshal.GetReference(payload);
#else
            ref payloadReference;
#endif
        ptr = ref Unsafe.Add(ref ptr, entryCount * sizeof(int));

        var list = new List<KeyValuePair<Memory<byte>, Memory<byte>>>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            var meta = Unsafe.ReadUnaligned<NodeEntryMeta>(
                ref Unsafe.Add(ref ptr, i * Unsafe.SizeOf<NodeEntryMeta>()));

            var key = MemoryMarshal.CreateReadOnlySpan(
                ref Unsafe.Add(ref ptr, meta.PayloadOffset),
                meta.KeyLength);

            var value = MemoryMarshal.CreateReadOnlySpan(
                ref Unsafe.Add(ref ptr, meta.PayloadOffset + meta.KeyLength),
                meta.ValueLength);

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
