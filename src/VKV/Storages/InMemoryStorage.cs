using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace VKV.Storages;

public class InMemoryStorage(Memory<byte> memory) : IStorage
{
    public void Dispose()
    {
    }

    public ValueTask<IMemoryOwner<byte>> ReadPageAsync(PageNumber pageNumber, CancellationToken cancellationToken = default)
    {
        var pageLength = Unsafe.ReadUnaligned<int>(
            ref Unsafe.Add(
                ref MemoryMarshal.GetReference(memory.Span),
                (int)pageNumber.Value));

        var destination = MemoryPool<byte>.Shared.Rent(pageLength);
        memory.Slice((int)pageNumber.Value, pageLength).CopyTo(destination.Memory);
        return new ValueTask<IMemoryOwner<byte>>(destination);
    }

    public IMemoryOwner<byte> ReadPage(PageNumber pageNumber)
    {
        var pageLength = Unsafe.ReadUnaligned<int>(
            ref Unsafe.Add(
                ref MemoryMarshal.GetReference(memory.Span),
                (int)pageNumber.Value));

        var destination = MemoryPool<byte>.Shared.Rent(pageLength);

        memory.Span.Slice((int)pageNumber.Value, pageLength)
            .CopyTo(destination.Memory.Span);
        return destination;
    }
}
