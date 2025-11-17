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

namespace VKV.BTree;

sealed class NodeEntry(int pageSize)
{
    public readonly byte[] KeyValueBuffer = new byte[pageSize];
    public readonly List<(int KeyLength, int ValueLength)> KeyValueSizes = [];
    public int KeyValueBufferOffset;
    public PageNumber PrevNodeStartPageNumber = PageNumber.Empty;
    public ReadOnlyMemory<byte>? FirstKey;

    public int PageSize => pageSize;
    public int EntryCount => KeyValueSizes.Count;

    public void Reset()
    {
        Array.Clear(KeyValueBuffer, 0, KeyValueBuffer.Length);
        KeyValueSizes.Clear();
        KeyValueBufferOffset = 0;
        PrevNodeStartPageNumber = PageNumber.Empty;
        FirstKey = null;
    }
}

static class TreeBuilder
{
    internal static readonly int NodeHeaderSize = Unsafe.SizeOf<NodeHeader>();
    static readonly int NodeHeaderRightSiblingPositionOffset = NodeHeaderSize - sizeof(long);

    public static async ValueTask<PageNumber> BuildToAsync(
        Stream outStream,
        int pageSize,
        KeyValueList keyValues,
        IReadOnlyList<IPageFilter>? pageFilters = null,
        CancellationToken cancellationToken = default)
    {
        if (pageSize < NodeHeaderSize + 16)
            throw new ArgumentException("pageSize too small");

        var nodes = new List<NodeEntry> { new(pageSize) };
        nodes[0].Reset();

        var leaf = nodes[0];
        foreach (var (key, value) in keyValues)
        {
            if (leaf.EntryCount <= 0)
            {
                leaf.FirstKey = key;
            }

            var needs = NodeHeaderSize + (leaf.EntryCount + 1) * (sizeof(int) + sizeof(ushort) * 2) +
                        leaf.KeyValueBufferOffset + key.Length + value.Length;
            if (needs > pageSize)
            {
                await RotatePageAsync(outStream, nodes, 0, true, pageFilters, cancellationToken).ConfigureAwait(false);
                if (nodes[0].EntryCount <= 0)
                {
                    nodes[0].FirstKey = key;
                }
                leaf = nodes[0];
            }

            ref var keyValueBufferReference =
#if NETSTANDARD
                ref MemoryMarshal.GetReference(leaf.KeyValueBuffer.AsSpan());
#else
                ref MemoryMarshal.GetArrayDataReference(leaf.KeyValueBuffer);
#endif
            keyValueBufferReference = ref Unsafe.Add(ref keyValueBufferReference, leaf.KeyValueBufferOffset);

            Unsafe.CopyBlockUnaligned(
                ref keyValueBufferReference,
                ref MemoryMarshal.GetReference(key.Span),
                (uint)key.Length);
            keyValueBufferReference = ref Unsafe.Add(ref keyValueBufferReference, key.Length);

            Unsafe.CopyBlockUnaligned(
                ref keyValueBufferReference,
                ref MemoryMarshal.GetReference(value.Span),
                (uint)value.Length);

            leaf.KeyValueSizes.Add((key.Length, value.Length));
            leaf.KeyValueBufferOffset += key.Length + value.Length;
        }

        while (true)
        {
            var levelsAtStart = nodes.Count;
            for (var level = 0; level < levelsAtStart - 1; level++)
            {
                if (nodes[level].EntryCount > 0)
                {
                    await RotatePageAsync(outStream, nodes, level, true, pageFilters, cancellationToken).ConfigureAwait(false);
                }
            }

            // write top-level
            if (nodes[^1].EntryCount > 0)
            {
                await RotatePageAsync(outStream, nodes, nodes.Count - 1, false, pageFilters, cancellationToken).ConfigureAwait(false);
            }

            if (nodes[^1].EntryCount <= 0) break;
        }

        return nodes[^1].PrevNodeStartPageNumber; // latest root
    }

    static async ValueTask RotatePageAsync(
        Stream outStream,
        List<NodeEntry> nodeEntries,
        int level,
        bool promote,
        IReadOnlyList<IPageFilter>? pageFilters = null,
        CancellationToken cancellationToken = default)
    {
        var currentNode = nodeEntries[level];
        var currentPos = new PageNumber(outStream.Position);

        var kind = level == 0 ? NodeKind.Leaf : NodeKind.Internal;
        var header = new NodeHeader
        {
            Kind = kind,
            EntryCount = currentNode.EntryCount,
            PayloadLength = currentNode.EntryCount * (kind == NodeKind.Leaf
                                ? sizeof(int) + sizeof(ushort) * 2
                                : sizeof(int) + sizeof(ushort)) +
                            currentNode.KeyValueBufferOffset,
            LeftSiblingPageNumber = currentNode.PrevNodeStartPageNumber,
            RightSiblingPageNumber = PageNumber.Empty
        };

        await FlushPageAsync(outStream, header, currentNode, pageFilters, cancellationToken)
            .ConfigureAwait(false);

        // Patch RightSiblingPosition
        if (!currentNode.PrevNodeStartPageNumber.IsEmpty)
        {
            var saved = outStream.Position;
            outStream.Seek(currentNode.PrevNodeStartPageNumber.Value + NodeHeaderRightSiblingPositionOffset, SeekOrigin.Begin);
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

        var needs = NodeHeaderSize + (parent.EntryCount + 1) * (sizeof(int) + sizeof(ushort)) +
                    parent.KeyValueBufferOffset + sepKey.Length + sizeof(long);
        if (needs > parent.PageSize)
        {
            await RotatePageAsync(outStream, nodeEntries, parentLevel, true, pageFilters, cancellationToken)
                .ConfigureAwait(false);
            if (parent.EntryCount == 0) parent.FirstKey = sepKey; // first key of new page
        }

        ref var parentKeyValueBufferReference =
#if NETSTANDARD
            ref MemoryMarshal.GetReference(parent.KeyValueBuffer.AsSpan());
#else
            ref MemoryMarshal.GetArrayDataReference(parent.KeyValueBuffer);
#endif
        parentKeyValueBufferReference = ref Unsafe.Add(ref parentKeyValueBufferReference, parent.KeyValueBufferOffset);

        Unsafe.CopyBlockUnaligned(
            ref parentKeyValueBufferReference,
            ref MemoryMarshal.GetReference(sepKey.Span),
            (uint)sepKey.Length);
        parentKeyValueBufferReference = ref Unsafe.Add(ref parentKeyValueBufferReference, sepKey.Length);

        Unsafe.WriteUnaligned(ref parentKeyValueBufferReference, currentPos.Value);

        parent.KeyValueSizes.Add((sepKey.Length, sizeof(long)));
        parent.KeyValueBufferOffset += sepKey.Length + sizeof(long);

        currentNode.Reset();
        currentNode.PrevNodeStartPageNumber = currentPos;
    }

    static async ValueTask FlushPageAsync(
        Stream outStream,
        NodeHeader header,
        NodeEntry node,
        IReadOnlyList<IPageFilter>? filters,
        CancellationToken cancellationToken = default)
    {
        // write header
        var buffer = ArrayPool<byte>.Shared.Rent(NodeHeaderSize);
        Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(buffer.AsSpan()), header);
        await outStream.WriteAsync(
            buffer.AsMemory(0, NodeHeaderSize),
            cancellationToken).ConfigureAwait(false);
        ArrayPool<byte>.Shared.Return(buffer);

        // write meta(s)
        buffer =  ArrayPool<byte>.Shared.Rent(sizeof(int) + sizeof(ushort) * 2);

        var metaSize = header.Kind == NodeKind.Leaf
            ? sizeof(int) + sizeof(ushort) * 2
            : sizeof(int) + sizeof(ushort);

        var payloadOffset = node.EntryCount * metaSize;
        foreach (var (keyLength, valueLength) in node.KeyValueSizes)
        {
            BinaryPrimitives.WriteInt32LittleEndian(buffer, payloadOffset);
            var offset = sizeof(int);

            BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(offset), (ushort)keyLength);
            offset += sizeof(ushort);

            if (header.Kind == NodeKind.Leaf)
            {
                // variable length value
                BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(offset), (ushort)valueLength);
                offset += sizeof(ushort);
            }

            await outStream.WriteAsync(buffer.AsMemory(0, offset), cancellationToken )
                .ConfigureAwait(false);
            payloadOffset += keyLength + valueLength;
        }
        ArrayPool<byte>.Shared.Return(buffer);

        // write key/values
        await outStream.WriteAsync(
                node.KeyValueBuffer.AsMemory(0, node.KeyValueBufferOffset),
                cancellationToken)
            .ConfigureAwait(false);
        await outStream.FlushAsync(cancellationToken).ConfigureAwait(false);

        if (filters is { Count: > 0 })
        {
            throw new NotImplementedException();
            // var input = page;
            // var outputBuffer = ArrayPool<byte>.Shared.Rent(input.Length);
            // try
            // {
            //     foreach (var filter in filters)
            //     {
            //         var maxLength = filter.GetMaxEncodedLength(input.Length);
            //         if (maxLength > outputBuffer.Length)
            //         {
            //             ArrayPool<byte>.Shared.Return(outputBuffer);
            //             outputBuffer = ArrayPool<byte>.Shared.Rent(maxLength);
            //         }
            //
            //         filter.Encode(input, outputBuffer);
            //         input = outputBuffer;
            //     }
            //
            //     await outStream.WriteAsync(outputBuffer.AsMemory(0, page.Length), cancellationToken).ConfigureAwait(false);
            // }
            // finally
            // {
            //     ArrayPool<byte>.Shared.Return(outputBuffer);
            // }
        }
    }
}