using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using VKV.Internal;
#if NET7_0_OR_GREATER
using static System.Runtime.InteropServices.MemoryMarshal;
#else
using static System.Runtime.CompilerServices.MemoryMarshalEx;
#endif

namespace VKV.BTree;

sealed class NodeEntry(int pageSize)
{
    public readonly byte[] KeyBuffer = new byte[pageSize];
    public readonly byte[] ValueBuffer = new byte[pageSize];

    public int KeyBufferOffset;
    public int ValueBufferOffset;

    public readonly List<(int Start, int Length)> KeyLenghList = [];
    public readonly List<(int Start, int Length)> ValueLenghList = [];

    public PageNumber PrevNodeStartPageNumber = PageNumber.Empty;
    public ReadOnlyMemory<byte>? FirstKey;

    public int PageSize => pageSize;
    public int EntryCount => KeyLenghList.Count;

    // internal
    public void AddKeyValue(ReadOnlySpan<byte> key, long value)
    {
        Span<byte> valueBuffer = stackalloc byte[sizeof(long)];
        Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(valueBuffer), value);
        AddKeyValue(key, valueBuffer);
    }

    // leaf
    public void AddKeyValue(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ref var keyBufferReference = ref GetArrayDataReference(KeyBuffer);
        ref var valueBufferReference = ref GetArrayDataReference(ValueBuffer);

        keyBufferReference = ref Unsafe.Add(ref keyBufferReference, KeyBufferOffset);
        valueBufferReference = ref Unsafe.Add(ref valueBufferReference, ValueBufferOffset);

        Unsafe.CopyBlockUnaligned(
            ref keyBufferReference,
            ref MemoryMarshal.GetReference(key),
            (uint)key.Length);

        KeyLenghList.Add((KeyBufferOffset, key.Length));
        KeyBufferOffset += key.Length;

        Unsafe.CopyBlockUnaligned(
            ref valueBufferReference,
            ref MemoryMarshal.GetReference(value),
            (uint)value.Length);

        ValueLenghList.Add((ValueBufferOffset, value.Length));
        ValueBufferOffset += value.Length;
    }

    public void Reset()
    {
        Array.Clear(KeyBuffer, 0, KeyBuffer.Length);
        Array.Clear(ValueBuffer, 0, ValueBuffer.Length);
        KeyLenghList.Clear();
        ValueLenghList.Clear();
        KeyBufferOffset = 0;
        ValueBufferOffset = 0;
        PrevNodeStartPageNumber = PageNumber.Empty;
        FirstKey = null;
    }
}

sealed class TreeBuildResult
{
    public PageNumber RootPageNumber { get; init; }
    public List<PageRef> WroteValueRefs { get; init; }
}

static class TreeBuilder
{
    static readonly int PageHeaderSize = Unsafe.SizeOf<PageHeader>() + Unsafe.SizeOf<NodeHeader>();
    static readonly int RightSiblingPositionPageOffset = PageHeaderSize - sizeof(long);

    public static async ValueTask<TreeBuildResult> BuildToAsync(
        Stream outStream,
        int pageSize,
        KeyValueList keyValues,
        IReadOnlyList<IPageFilter>? pageFilters = null,
        CancellationToken cancellationToken = default)
    {
        if (pageSize < PageHeaderSize + 32)
        {
            throw new ArgumentException("pageSize too small");
        }

        var wroteValuePointers = new List<PageRef>(keyValues.Count);

        var nodes = new List<NodeEntry> { new(pageSize) };
        nodes[0].Reset();

        var leaf = nodes[0];

        var needsBytes = PageHeaderSize;

        foreach (var (key, value) in keyValues)
        {
            if (leaf.EntryCount <= 0)
            {
                leaf.FirstKey = key;
            }

            // keyOffset + valueOffset + keyLength + valueLength + key + value
            needsBytes += sizeof(int) * 2 + sizeof(ushort) * 2 + key.Length + value.Length;
            if (needsBytes > pageSize)
            {
                needsBytes = PageHeaderSize; // reset
                await RotatePageAsync(outStream, nodes, 0, true, wroteValuePointers, pageFilters, cancellationToken)
                    .ConfigureAwait(false);

                if (nodes[0].EntryCount <= 0)
                {
                    nodes[0].FirstKey = key;
                }
                leaf = nodes[0];
            }

            leaf.AddKeyValue(key.Span, value.Span);
        }

        while (true)
        {
            var levelsAtStart = nodes.Count;
            for (var level = 0; level < levelsAtStart - 1; level++)
            {
                if (nodes[level].EntryCount > 0)
                {
                    await RotatePageAsync(outStream, nodes, level, true, wroteValuePointers, pageFilters, cancellationToken).ConfigureAwait(false);
                }
            }

            // write top-level
            if (nodes[^1].EntryCount > 0)
            {
                await RotatePageAsync(outStream, nodes, nodes.Count - 1, false, wroteValuePointers, pageFilters, cancellationToken).ConfigureAwait(false);
            }

            if (nodes[^1].EntryCount <= 0) break;
        }

        var rootPageNumber = nodes[^1].PrevNodeStartPageNumber; // latest root
        return new TreeBuildResult
        {
            RootPageNumber = rootPageNumber,
            WroteValueRefs = wroteValuePointers
        };
    }

    static async ValueTask RotatePageAsync(
        Stream outStream,
        List<NodeEntry> nodeEntries,
        int level,
        bool promote,
        List<PageRef> wroteValueRefs,
        IReadOnlyList<IPageFilter>? pageFilters = null,
        CancellationToken cancellationToken = default)
    {
        var currentNode = nodeEntries[level];
        var currentPos = new PageNumber(outStream.Position);

        var kind = level == 0 ? NodeKind.Leaf : NodeKind.Internal;
        var nodeHeader = new NodeHeader
        {
            Kind = kind,
            EntryCount = currentNode.EntryCount,
            LeftSiblingPageNumber = currentNode.PrevNodeStartPageNumber,
            RightSiblingPageNumber = PageNumber.Empty
        };

        await FlushPageAsync(outStream, nodeHeader, currentNode, wroteValueRefs, pageFilters, cancellationToken)
            .ConfigureAwait(false);

        // Patch RightSiblingPosition
        if (!currentNode.PrevNodeStartPageNumber.IsEmpty)
        {
            var saved = outStream.Position;
            outStream.Seek(currentNode.PrevNodeStartPageNumber.Value + RightSiblingPositionPageOffset, SeekOrigin.Begin);
            var buffer = ArrayPool<byte>.Shared.Rent(sizeof(long));
            try
            {
                BinaryPrimitives.WriteInt64LittleEndian(buffer, currentPos.Value);
                await outStream.WriteAsync(buffer.AsMemory(0, sizeof(long)), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
            await outStream.FlushAsync(cancellationToken);
            outStream.Seek(saved, SeekOrigin.Begin);
        }

        if (!promote)
        {
            currentNode.Reset();
            currentNode.PrevNodeStartPageNumber = currentPos;
            return;
        }

        var sepKey = currentNode.FirstKey!.Value;
        var parentLevel = level + 1;

        if (parentLevel >= nodeEntries.Count)
        {
            nodeEntries.Add(new NodeEntry(nodeEntries[0].PageSize));
            nodeEntries[^1].Reset();
        }

        var parent = nodeEntries[parentLevel];
        if (parent.EntryCount <= 0)
        {
            parent.FirstKey = sepKey;
        }

        var needs = PageHeaderSize + (parent.EntryCount + 1) * (sizeof(int) * 2 + sizeof(ushort)) +
                    parent.KeyBufferOffset + parent.ValueBufferOffset +
                    sepKey.Length + sizeof(long);
        if (needs > parent.PageSize)
        {
            await RotatePageAsync(outStream, nodeEntries, parentLevel, true, wroteValueRefs, pageFilters, cancellationToken)
                .ConfigureAwait(false);
            if (parent.EntryCount == 0) parent.FirstKey = sepKey; // first key of new page
        }

        parent.AddKeyValue(sepKey.Span, currentPos.Value);

        currentNode.Reset();
        currentNode.PrevNodeStartPageNumber = currentPos;
    }

    static async ValueTask FlushPageAsync(
        Stream outStream,
        NodeHeader nodeHeader,
        NodeEntry node,
        List<PageRef> wroteValueRefs,
        IReadOnlyList<IPageFilter>? filters,
        CancellationToken cancellationToken = default)
    {
        var currentPageNumber = new PageNumber(outStream.Position);

        var pageLength = PageHeaderSize +
                         node.EntryCount * (nodeHeader.Kind == NodeKind.Leaf
                             ? sizeof(int) * 2 + sizeof(ushort) * 2
                             : sizeof(int) * 2 + sizeof(ushort)) +
                         node.KeyBufferOffset +
                         node.ValueBufferOffset;

        var buffer = ArrayPool<byte>.Shared.Rent(pageLength);
        ref var ptr = ref GetArrayDataReference(buffer);

        // write page header
        Unsafe.WriteUnaligned(ref ptr, new PageHeader
        {
            PageSize = pageLength
        });
        ptr = ref Unsafe.Add(ref ptr, Unsafe.SizeOf<PageHeader>());

        // write node header
        Unsafe.WriteUnaligned(ref ptr, nodeHeader);
        ptr = ref Unsafe.Add(ref ptr, Unsafe.SizeOf<NodeHeader>());

        // write meta(s)
        var metaSize = nodeHeader.Kind == NodeKind.Leaf
            ? sizeof(int) * 2 + sizeof(ushort) * 2
            : sizeof(int) * 2 + sizeof(ushort);

        var keyStartPosition = PageHeaderSize + node.EntryCount * metaSize;
        var valueStartPosition = keyStartPosition + node.KeyBufferOffset;

        // write key meta(s)
        for (var i = 0; i < node.EntryCount; i++)
        {
            var (keyOffset, keyLength) = node.KeyLenghList[i];
            var (valueOffset, valueLength) = node.ValueLenghList[i];

            if (nodeHeader.Kind == NodeKind.Leaf)
            {
                wroteValueRefs.Add(new PageRef(
                    currentPageNumber,
                    valueStartPosition + valueOffset,
                    valueLength));
            }

            Unsafe.WriteUnaligned(ref ptr, keyStartPosition + keyOffset);
            ptr = ref Unsafe.Add(ref ptr, sizeof(int));

            Unsafe.WriteUnaligned(ref ptr, (ushort)keyLength);
            ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));

            Unsafe.WriteUnaligned(ref ptr, valueStartPosition + valueOffset);
            ptr = ref Unsafe.Add(ref ptr, sizeof(int));

            if (nodeHeader.Kind == NodeKind.Leaf)
            {
                // variable length value
                Unsafe.WriteUnaligned(ref ptr, (ushort)valueLength);
                ptr = ref Unsafe.Add(ref ptr, sizeof(ushort));
            }
        }

        // write keys
        ref var keyReference = ref GetArrayDataReference(node.KeyBuffer);
        Unsafe.CopyBlockUnaligned(ref ptr, ref keyReference, (uint)node.KeyBufferOffset);
        ptr = ref Unsafe.Add(ref ptr, node.KeyBufferOffset);

        // write values
        ref var valueReference = ref GetArrayDataReference(node.ValueBuffer);
        Unsafe.CopyBlockUnaligned(ref ptr, ref valueReference, (uint)node.ValueBufferOffset);
        ptr = ref Unsafe.Add(ref ptr, node.ValueBufferOffset);

        if (filters is { Count: > 0 })
        {
            var source = buffer.AsSpan(0, pageLength);
            var output = BufferWriterPool.Rent(pageLength);

            // copy header
            output.Write(source[..PageHeaderSize]);

            // encode
            filters[0].Encode(source[PageHeaderSize..], output);

            // update page header
            Unsafe.WriteUnaligned(
                ref MemoryMarshal.GetReference(output.WrittenSpan),
                new PageHeader { PageSize = output.WrittenCount });

            if (filters.Count <= 1)
            {
                await outStream.WriteAsync(output.WrittenMemory, cancellationToken);
            }
            else
            {
                // double buffer
                var input = output;
                output = BufferWriterPool.Rent(output.WrittenCount);

                for (var i = 1; i < filters.Count; i++)
                {
                    // copy header
                    output.Write(source[..PageHeaderSize]);

                    // encode
                    filters[i].Encode(input.WrittenSpan[PageHeaderSize..], output);

                    // update page header
                    Unsafe.WriteUnaligned(
                        ref MemoryMarshal.GetReference(output.WrittenSpan),
                        new PageHeader{ PageSize = output.WrittenCount });
                    (output, input) = (input, output);
                }

                await outStream.WriteAsync(output.WrittenMemory, cancellationToken);
                BufferWriterPool.Return(input);
            }
            BufferWriterPool.Return(output);
        }
        else
        {
            await outStream.WriteAsync(buffer.AsMemory(0, pageLength), cancellationToken);
        }
        ArrayPool<byte>.Shared.Return(buffer);

        await outStream.FlushAsync(cancellationToken);
    }
}