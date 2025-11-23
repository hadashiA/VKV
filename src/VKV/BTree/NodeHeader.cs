using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace VKV.BTree;

enum NodeKind : byte
{
    Leaf = 0,
    Internal = 1,
}

static class NodeHeaderExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static NodeHeader GetNodeHeader(this IPageEntry page) =>
        NodeHeader.Parse(page.Memory.Span);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetEntryCount(this IPageEntry page) =>
        NodeHeader.ParseEntryCount(page.Memory.Span);
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
    public int NodeLength => Unsafe.SizeOf<NodeHeader>() + PayloadLength;

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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static NodeHeader Parse(ReadOnlySpan<byte> page)
    {
        return Unsafe.ReadUnaligned<NodeHeader>(ref MemoryMarshal.GetReference(page));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ParseEntryCount(ReadOnlySpan<byte> page)
    {
        return Unsafe.ReadUnaligned<int>(
            ref Unsafe.Add(
                ref MemoryMarshal.GetReference(page), 4));
    }
}

