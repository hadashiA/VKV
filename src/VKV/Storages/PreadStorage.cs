using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace VKV.Storages;

public sealed class PreadStorage(SafeFileHandle handle, int pageSize = 4096) : IStorage
{
    public int PageSize { get; } = pageSize;

    public void Dispose() => handle.Dispose();

    public async ValueTask<IMemoryOwner<byte>> ReadPageAsync(PageNumber pageNumber, CancellationToken cancellationToken = default)
    {
        var destination = MemoryPool<byte>.Shared.Rent(pageSize);
        var bytesRead = 0;
        while (bytesRead < PageSize)
        {
            var n = await RandomAccess.ReadAsync(handle, destination.Memory[bytesRead..], pageNumber.Value + bytesRead, cancellationToken);
            if (n == 0)
            {
                return destination;
            }
            bytesRead += n;
        }
        return destination;
    }

    public IMemoryOwner<byte> ReadPage(PageNumber pageNumber)
    {
        var destination = MemoryPool<byte>.Shared.Rent(pageSize);
        var bytesRead = 0;
        while (bytesRead < PageSize)
        {
            var n = RandomAccess.Read(
                handle,
                destination.Memory.Span[bytesRead..],
                pageNumber.Value + bytesRead);
            if (n == 0)
            {
                return destination;
            }
            bytesRead += n;
        }
        return destination;
    }
}
