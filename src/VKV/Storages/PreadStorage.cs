using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using VKV.Internal;
#if NET7_0_OR_GREATER
using static System.Runtime.InteropServices.MemoryMarshal;
#else
using static System.Runtime.CompilerServices.MemoryMarshalEx;
#endif

namespace VKV.Storages;

public sealed class PreadStorage(SafeFileHandle handle) : IStorage
{
    public void Dispose() => handle.Dispose();

    public async ValueTask<IMemoryOwner<byte>> ReadPageAsync(
        PageNumber pageNumber,
        IPageFilter[]? filters = null,
        CancellationToken cancellationToken = default)
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

        var payloadLength = Unsafe.ReadUnaligned<int>(ref GetArrayDataReference(lengthBuffer));

        var bytesRead = 0;
        var destination = MemoryPool<byte>.Shared.Rent(payloadLength);
        while (bytesRead < payloadLength)
        {
            var buffer = destination.Memory[bytesRead..(payloadLength - bytesRead)];
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

        if (filters is { Length: > 0 })
        {
            try
            {
                return ApplyFilter(destination, filters);
            }
            finally
            {
                destination.Dispose();
            }
        }
        return destination;
    }

    public IMemoryOwner<byte> ReadPage(PageNumber pageNumber, IPageFilter[]? filters = null)
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

        if (filters is { Length: > 0 })
        {
            try
            {
                return ApplyFilter(destination, filters);
            }
            finally
            {
                destination.Dispose();
            }
        }
        return destination;
    }


    static IMemoryOwner<byte> ApplyFilter(IMemoryOwner<byte> source, IPageFilter[] filters)
    {
        var output = BufferWriterPool.Rent(source.Memory.Length);
        filters[0].Encode(source.Memory.Span, output);
        if (filters.Length <= 1)
        {
            return output.ToPoolableMemory();
        }

        // double buffer
        var input = output;
        output = BufferWriterPool.Rent(output.WrittenCount);

        for (var i = 1; i < filters.Length; i++)
        {
            filters[i].Encode(input.WrittenSpan, output);

            // last
            if (i >= filters.Length - 1)
            {
                BufferWriterPool.Return(input);
                return output.ToPoolableMemory();
            }
            (output, input) = (input, output);
        }
        throw new InvalidOperationException("unreached");
    }
}