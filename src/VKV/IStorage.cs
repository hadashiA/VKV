using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace VKV;

public readonly record struct PageNumber(long Value)
{
    public static PageNumber Empty => new(-1);

    public bool IsEmpty => Value == -1;
}

public interface IStorage : IDisposable
{
    int PageSize { get; }

    ValueTask<IMemoryOwner<byte>> ReadPageAsync(PageNumber pageNumber, CancellationToken cancellationToken = default);
    IMemoryOwner<byte> ReadPage(PageNumber pageNumber);
}
