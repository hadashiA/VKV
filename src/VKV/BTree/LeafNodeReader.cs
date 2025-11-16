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
/// (offset-table sizeof(int) * entryCount)
/// ┌───────────┬───────────┬──────┬────────┐
/// │ keylen(2) │ vallen(2) │ key  │ value  |
/// ├───────────┼───────────┼──────┼────────┤
/// │ keylen(2) │ vallen(2) │ key  │ value |
/// ├────────┼──────────────┼──────────────┤
/// │   ...   │     ...      │     ...     │
/// └────────┴──────────────┴──────────────┘
/// </remarks>
readonly ref struct LeafNodeReader(NodeHeader header, ReadOnlySpan<byte> payload)
{
#if NETSTANDARD
    readonly ReadOnlySpan<byte> payload = payload;
#else
    readonly ref byte payloadReference = ref MemoryMarshal.GetReference(payload);
#endif
    readonly int entryCount = header.EntryCount;
    readonly int payloadLength = header.PayloadLength;

    public void GetAtOffset(
        int payloadOffset,
        out ReadOnlySpan<byte> key,
        out ReadOnlySpan<byte> value,
        out int? nextPayloadOffset)
    {
        ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
            ref MemoryMarshal.GetReference(payload),
#else
            ref payloadReference,
#endif
            payloadOffset);
        var keyLen = Unsafe.ReadUnaligned<ushort>(ref ptr);
        ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));
        var valLen = Unsafe.ReadUnaligned<ushort>(ref ptr);
        ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));
        key = MemoryMarshal.CreateReadOnlySpan(ref ptr, keyLen);
        ptr = ref Unsafe.Add(ref ptr, keyLen);
        value = MemoryMarshal.CreateReadOnlySpan(ref ptr, valLen);

        nextPayloadOffset = payloadOffset + sizeof(ushort) * 2 + keyLen + valLen;
        nextPayloadOffset = nextPayloadOffset < payloadLength ? nextPayloadOffset : null;
    }

    public bool TryFindValue(
        scoped ReadOnlySpan<byte> key,
        IKeyComparer keyComparer,
        out int valuePayloadOffset,
        out int valueLength)
    {
        if (TrySearch(key, SearchOperator.Equal, keyComparer, out var offset))
        {
            ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
                ref MemoryMarshal.GetReference(payload),
#else
                ref payloadReference,
#endif
                offset);
            var keyLength = Unsafe.ReadUnaligned<ushort>(ref ptr);
            ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));

            valueLength = Unsafe.ReadUnaligned<ushort>(ref ptr);
            ptr = ref Unsafe.Add(ref ptr, sizeof(ushort) + keyLength);

            valuePayloadOffset = offset + keyLength + sizeof(ushort) * 2;
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
        out int payloadOffset)
    {
        var min = 0;
        var max = entryCount;
        var resultIndex = -1;

        while (min < max)
        {
            var midIndex = min + ((max - min) >> 1);
            var midOffset = PayloadOffsetOf(midIndex);

            ref var ptr = ref Unsafe.Add(
#if  NETSTANDARD
                ref MemoryMarshal.GetReference(payload),
#else
                ref payloadReference,
#endif
                midOffset);
            var keyLen = Unsafe.ReadUnaligned<ushort>(ref ptr);
            ptr = ref Unsafe.Add(ref ptr, sizeof(ushort) * 2);

            var midKey = MemoryMarshal.CreateReadOnlySpan(ref ptr, keyLen);

            var compared = keyComparer.Compare(midKey, key);
            switch (op)
            {
                case SearchOperator.Equal:
                    if (compared == 0)
                    {
                        payloadOffset = PayloadOffsetOf(midIndex);
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
            payloadOffset = default;
            return false;
        }

        switch (op)
        {
            case SearchOperator.Equal:
                if (min < max)
                {
                    var off = PayloadOffsetOf(min);
                    ref var ptr = ref Unsafe.Add(
#if NETSTANDARD
                        ref MemoryMarshal.GetReference(payload),
#else
                        ref payloadReference,
#endif
                        off);
                    var len = Unsafe.ReadUnaligned<ushort>(ref ptr);
                    ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));
                    ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));
                    var foundKey = MemoryMarshal.CreateReadOnlySpan(ref ptr, len);

                    if (keyComparer.Compare(foundKey, key) == 0)
                    {
                        payloadOffset = PayloadOffsetOf(min);
                        return true;
                    }
                }
                payloadOffset = default;
                return false;

            case SearchOperator.LowerBound:
                // >= key
                payloadOffset = PayloadOffsetOf(min);
                return true;

            case SearchOperator.UpperBound:
                // > key
                payloadOffset = PayloadOffsetOf(min);
                return true;

            default:
                payloadOffset = default;
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
            var keyLen = Unsafe.ReadUnaligned<ushort>(ref ptr);
            ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));

            var valLen = Unsafe.ReadUnaligned<ushort>(ref ptr);
            ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));

            var key = MemoryMarshal.CreateReadOnlySpan(ref ptr, keyLen);
            ptr = ref Unsafe.Add(ref ptr, keyLen);

            var value = MemoryMarshal.CreateReadOnlySpan(ref ptr, valLen);
            ptr = ref Unsafe.Add(ref ptr, valLen);

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
