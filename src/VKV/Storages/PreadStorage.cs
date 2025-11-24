using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace VKV.Storages;

public sealed class PreadStorage(SafeFileHandle handle) : IStorage
{
    public void Dispose() => handle.Dispose();

    public async ValueTask<IMemoryOwner<byte>> ReadPageAsync(PageNumber pageNumber, CancellationToken cancellationToken = default)
    {
        var lengthBuffer = ArrayPool<byte>.Shared.Rent(sizeof(int));
        var n = await RandomAccess.ReadAsync(
            handle,
            lengthBuffer.AsMemory(0, sizeof(int)),
            pageNumber.Value,
            cancellationToken);
        if (n == 0)
        {
            throw new EndOfStreamException();
        }

        var pageLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer);

        var bytesRead = 0;
        var destination = MemoryPool<byte>.Shared.Rent(pageLength);
        while (bytesRead < pageLength)
        {
            var buffer = destination.Memory[bytesRead..(pageLength - bytesRead)];
            n = await RandomAccess.ReadAsync(
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
        Span<byte> lengthBuffer = stackalloc byte[sizeof(int)];
        RandomAccess.Read(handle, lengthBuffer, pageNumber.Value);
        var pageLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer);

        var bytesRead = 0;
        var destination = MemoryPool<byte>.Shared.Rent(pageLength);
        while (bytesRead < pageLength)
        {
            var buffer = destination.Memory[bytesRead..(pageLength - bytesRead)];
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