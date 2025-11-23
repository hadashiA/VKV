using System;
using System.Buffers;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using VKV.BTree;

namespace VKV.Storages;

public sealed class PreadStorage(SafeFileHandle handle, int pageSize = 4096) : IStorage
{
    public int PageSize { get; } = pageSize;

    public void Dispose() => handle.Dispose();

    public async ValueTask<IMemoryOwner<byte>> ReadPageAsync(PageNumber pageNumber, CancellationToken cancellationToken = default)
    {
        var headerLength = Unsafe.SizeOf<NodeHeader>();
        var bytesRead = 0;
        int nodeLength;
        using (var buffer = MemoryPool<byte>.Shared.Rent(Unsafe.SizeOf<NodeHeader>()))
        {
            while (bytesRead < headerLength)
            {
                var n = await RandomAccess.ReadAsync(
                    handle,
                    buffer.Memory[bytesRead..(headerLength - bytesRead)],
                    pageNumber.Value + bytesRead,
                    cancellationToken);
                if (n == 0)
                {
                    throw new EndOfStreamException();
                }
                bytesRead += n;
            }

            var header = NodeHeader.Parse(buffer.Memory.Span);
            nodeLength = header.NodeLength;
        }

        bytesRead = 0;
        var destination = MemoryPool<byte>.Shared.Rent(nodeLength);
        while (bytesRead < nodeLength)
        {
            var buffer = destination.Memory[bytesRead..(nodeLength - bytesRead)];
            var n = await RandomAccess.ReadAsync(
                handle,
                buffer,
                pageNumber.Value + bytesRead,
                cancellationToken);
            if (n == 0)
            {
                throw new EndOfStreamException();
            }
            bytesRead += n;
        }
        return destination;
    }

    public IMemoryOwner<byte> ReadPage(PageNumber pageNumber)
    {
        var headerLength = Unsafe.SizeOf<NodeHeader>();
        Span<byte> headerBuffer = stackalloc byte[headerLength];

        var bytesRead = 0;
        while (bytesRead < headerLength)
        {
            var n = RandomAccess.Read(
                handle,
                headerBuffer[bytesRead..(headerLength - bytesRead)],
                pageNumber.Value + bytesRead);
            if (n == 0)
            {
                throw new EndOfStreamException();
            }
            bytesRead += n;
        }

        var header = NodeHeader.Parse(headerBuffer);
        var nodeLength = header.NodeLength;

        bytesRead = 0;
        var destination = MemoryPool<byte>.Shared.Rent(nodeLength);
        while (bytesRead < nodeLength)
        {
            var buffer = destination.Memory[bytesRead..(nodeLength - bytesRead)];
            var n = RandomAccess.Read(
                handle,
                buffer.Span,
                pageNumber.Value + bytesRead);
            if (n == 0)
            {
                throw new EndOfStreamException();
            }
            bytesRead += n;
        }
        return destination;
    }
}