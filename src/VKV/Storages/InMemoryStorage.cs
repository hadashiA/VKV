using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using VKV.BTree;

namespace VKV.Storages;

public class InMemoryStorage(Memory<byte> memory, int pageSize) : IStorage
{
    public int PageSize => pageSize;

    public void Dispose()
    {
    }

    public ValueTask<IMemoryOwner<byte>> ReadPageAsync(PageNumber pageNumber, CancellationToken cancellationToken = default)
    {
        var header = NodeHeader.Parse(memory.Span[(int)pageNumber.Value..]);
        var nodeLength = header.NodeLength;

        var destination = MemoryPool<byte>.Shared.Rent(nodeLength);
        memory.Slice((int)pageNumber.Value, nodeLength)
            .CopyTo(destination.Memory);
        return new ValueTask<IMemoryOwner<byte>>(destination);
    }

    public IMemoryOwner<byte> ReadPage(PageNumber pageNumber)
    {
        var header = NodeHeader.Parse(memory.Span[(int)pageNumber.Value..]);
        var nodeLength = header.NodeLength;

        var destination = MemoryPool<byte>.Shared.Rent(nodeLength);

        memory.Span.Slice((int)pageNumber.Value, nodeLength)
            .CopyTo(destination.Memory.Span);
        return destination;
    }
}
