using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using VKV.Internal;

namespace VKV.Storages;

public class InMemoryStorage(Memory<byte> memory) : IStorage
{
    public void Dispose()
    {
    }

    public ValueTask<IMemoryOwner<byte>> ReadPageAsync(
        PageNumber pageNumber,
        IPageFilter[]? filters,
        CancellationToken cancellationToken = default)
    {
        var pageLength = Unsafe.ReadUnaligned<int>(
            ref Unsafe.Add(
                ref MemoryMarshal.GetReference(memory.Span),
                (int)pageNumber.Value));

        var destination = MemoryPool<byte>.Shared.Rent(pageLength);
        memory.Slice((int)pageNumber.Value, pageLength).CopyTo(destination.Memory);

        if (filters is { Length: > 0 })
        {
            try
            {
                destination = ApplyFilter(destination, filters);
            }
            finally
            {
                destination.Dispose();
            }
        }
        return new ValueTask<IMemoryOwner<byte>>(destination);
    }

    public IMemoryOwner<byte> ReadPage(PageNumber pageNumber, IPageFilter[] filters)
    {
        var pageLength = Unsafe.ReadUnaligned<int>(
            ref Unsafe.Add(
                ref MemoryMarshal.GetReference(memory.Span),
                (int)pageNumber.Value));

        var destination = MemoryPool<byte>.Shared.Rent(pageLength);

        memory.Span.Slice((int)pageNumber.Value, pageLength)
            .CopyTo(destination.Memory.Span);

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
