using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace VKV.Storages;

public class InMemoryStorage(Memory<byte> memory, int pageSize) : IStorage
{
    public int PageSize => pageSize;

    public void Dispose()
    {
    }

    public ValueTask<IMemoryOwner<byte>> ReadPageAsync(PageNumber pageNumber, CancellationToken cancellationToken = default)
    {
        var destination = MemoryPool<byte>.Shared.Rent(PageSize);
        var length = (int)Math.Min(PageSize, memory.Length - pageNumber.Value);
        memory.Slice((int)pageNumber.Value, length).CopyTo(destination.Memory);
        return new ValueTask<IMemoryOwner<byte>>(destination);
    }

    public IMemoryOwner<byte> ReadPage(PageNumber pageNumber)
    {
        var destination = MemoryPool<byte>.Shared.Rent(PageSize);
        memory.Span.Slice((int)pageNumber.Value, PageSize).CopyTo(destination.Memory.Span);
        return destination;
    }
}
