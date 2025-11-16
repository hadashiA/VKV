using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace VKV.BTree;

enum NodeKind : byte
{
    Leaf = 0,
    Internal = 1,
}

/// <summary>
///
/// </summary>
/// <remarks>
///  This implementation support only for little endian
/// </remarks>
[StructLayout(LayoutKind.Explicit, Pack = 1)]
unsafe struct NodeHeader
{
    public static readonly int Size = Unsafe.SizeOf<NodeHeader>();

    [FieldOffset(0)]
    public NodeKind Kind;

    [FieldOffset(4)]
    public fixed byte EntryCountBytes[4];

    [FieldOffset(4)]
    public int EntryCount;

    [FieldOffset(8)]
    public fixed byte PayloadLengthBytes[4];

    [FieldOffset(8)]
    public int PayloadLength;

    [FieldOffset(12)]
    public fixed byte LeftSiblingPositionBytes[8];

    [FieldOffset(12)]
    public PageNumber LeftSiblingPageNumber;

    [FieldOffset(20)]
    public fixed byte RightSiblingPageNumberBytes[8];

    [FieldOffset(20)]
    public PageNumber RightSiblingPageNumber;

    public int FirstPayloadOffset => sizeof(int) + EntryCount;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Parse(ReadOnlySpan<byte> page, out NodeHeader header, out ReadOnlySpan<byte> payload)
    {
        header = Unsafe.ReadUnaligned<NodeHeader>(ref MemoryMarshal.GetReference(page));
        payload = page.Slice(Unsafe.SizeOf<NodeHeader>(), header.PayloadLength);
    }
}
