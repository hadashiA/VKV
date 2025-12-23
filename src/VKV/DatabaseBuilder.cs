using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using VKV.BTree;
using VKV.Internal;
#if NET7_0_OR_GREATER
using static System.Runtime.InteropServices.MemoryMarshal;
#else
using static System.Runtime.CompilerServices.MemoryMarshalEx;
#endif

namespace VKV;

public abstract class IndexOptions(string name, bool isUnique)
{
    public string Name => name;
    public bool IsUnique => isUnique;

    public IKeyEncoding KeyEncoding { get; set; } = VKV.KeyEncoding.Ascii;
    public abstract ValueKind ValueKind { get; }
}

public class PrimaryKeyIndexOptions(string name) : IndexOptions(name, true)
{
    public override ValueKind ValueKind => ValueKind.RawData;
}

public class SecondaryIndexOptions(string name, bool isUnique) : IndexOptions(name, isUnique)
{
    public override ValueKind ValueKind => ValueKind.PageRef;
    public required Func<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> ValueToIndexFactory;
}

public class FilterOptions
{
    public IReadOnlyList<IPageFilter> Filters => pageFilters;

    readonly List<IPageFilter> pageFilters = [];

    public void AddFilter(IPageFilter filter)
    {
        pageFilters.Add(filter);
    }
}

public class TableBuilder
{
    readonly string name;
    readonly int pageSize;
    public IKeyEncoding PrimaryKeyEncoding { get; set; }
    public IReadOnlyList<SecondaryIndexOptions> SecondaryIndexOptions => secondaryIndexOptions;

    readonly List<SecondaryIndexOptions> secondaryIndexOptions = [];
    readonly KeyValueList keyValues;
    readonly IReadOnlyList<IPageFilter>? pageFilters;

    internal TableBuilder(
        string name,
        IKeyEncoding primaryKeyEncoding,
        int pageSize,
        IReadOnlyList<IPageFilter>? pageFilters)
    {
        this.name = name;
        this.pageSize = pageSize;
        this.pageFilters = pageFilters;
        PrimaryKeyEncoding = primaryKeyEncoding;
        keyValues = new KeyValueList(PrimaryKeyEncoding);
    }

    public void AddSecondaryIndex(
        string indexName,
        bool isUnique,
        IKeyEncoding keyEncoding,
        Func<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> valueToIndexFactory)
    {
        secondaryIndexOptions.Add(new SecondaryIndexOptions(indexName, isUnique)
        {
            KeyEncoding = keyEncoding,
            ValueToIndexFactory = valueToIndexFactory,
        });
    }

    public void Append(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        keyValues.Add(key, value);
    }

    public void Append<TKey>(TKey key, ReadOnlyMemory<byte> value) where TKey : IComparable<TKey>
    {
        keyValues.Add(key, value);
    }

    public async ValueTask BuildAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        var nameUtf8 = Encoding.UTF8.GetBytes(name);
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(int) + nameUtf8.Length);
        try
        {
            BinaryPrimitives.WriteInt32LittleEndian(buffer, nameUtf8.Length);
            nameUtf8.CopyTo(buffer.AsSpan(sizeof(int)));
            await stream.WriteAsync(buffer.AsMemory(0, sizeof(int) + nameUtf8.Length), cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        var descriptorEndPositions = new List<long>();

        // build primary index descriptor
        await WriteIndexDescriptorAsync(stream, GetPrimaryKeyIndexOptions(), cancellationToken);
        descriptorEndPositions.Add(stream.Position);

        // build secondary index length
        Span<byte> indexCountBuffer = stackalloc byte[sizeof(ushort)];
        BinaryPrimitives.WriteUInt16LittleEndian(indexCountBuffer, (ushort)SecondaryIndexOptions.Count);
        stream.Write(indexCountBuffer);

        // build secondary index descriptors
        foreach (var indexOptions in SecondaryIndexOptions)
        {
            await WriteIndexDescriptorAsync(stream, indexOptions, cancellationToken);
            descriptorEndPositions.Add(stream.Position);
        }

        // write primary tree
        var primaryKeyResult = await TreeBuilder.BuildToAsync(stream, pageSize, keyValues, pageFilters, cancellationToken);

        // write primary tree root position
        Span<byte> positionBuffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(positionBuffer, primaryKeyResult.RootPageNumber.Value);
        var currentPosition = stream.Position;
        stream.Seek(descriptorEndPositions[0] - sizeof(long), SeekOrigin.Begin);
        stream.Write(positionBuffer);
        stream.Seek(currentPosition, SeekOrigin.Begin);

        // write secondary tree root positions
        for (var i = 0; i < SecondaryIndexOptions.Count; i++)
        {
            var indexOptions = SecondaryIndexOptions[i];
            var secondaryKeyValues = new KeyValueList(indexOptions.KeyEncoding, indexOptions.IsUnique);
            var primaryKeyIndex = 0;
            foreach (var (_, value) in keyValues)
            {
                var secondaryKey = indexOptions.ValueToIndexFactory.Invoke(value);
                var valuePointer = primaryKeyResult.WroteValueRefs[primaryKeyIndex];
                secondaryKeyValues.Add(secondaryKey, valuePointer.Encode());
                primaryKeyIndex++;
            }

            // write secondary key tree
            var secondaryKeyResult = await TreeBuilder.BuildToAsync(stream, pageSize, secondaryKeyValues, pageFilters, cancellationToken);

            // write secondary tree root position
            Span<byte> positionBuffer2 = stackalloc byte[sizeof(long)];
            BinaryPrimitives.WriteInt64LittleEndian(positionBuffer2, secondaryKeyResult.RootPageNumber.Value);

            var currentPosition2 = stream.Position;
            stream.Seek(descriptorEndPositions[i + 1] - sizeof(long), SeekOrigin.Begin);
            stream.Write(positionBuffer2);
            stream.Seek(currentPosition2, SeekOrigin.Begin);
        }
    }

    async ValueTask WriteIndexDescriptorAsync(
        Stream stream,
        IndexOptions indexOptions,
        CancellationToken cancellationToken = default)
    {
        var indexNameUtf8 = Encoding.UTF8.GetBytes(indexOptions.Name);
        var keyEncodingIdUtf8 = Encoding.UTF8.GetBytes(indexOptions.KeyEncoding.Id);
        var descriptorLength = sizeof(ushort) * 2 + indexNameUtf8.Length + keyEncodingIdUtf8.Length + 1 + 1 + sizeof(long);

        var buffer = ArrayPool<byte>.Shared.Rent(descriptorLength);

        ref var bufferRef = ref GetArrayDataReference(buffer);

        Unsafe.WriteUnaligned(ref bufferRef, (ushort)indexNameUtf8.Length);
        bufferRef = ref Unsafe.Add(ref bufferRef, sizeof(ushort));

        Unsafe.WriteUnaligned(ref bufferRef, (ushort)keyEncodingIdUtf8.Length);
        bufferRef = ref Unsafe.Add(ref bufferRef, sizeof(ushort));

        Unsafe.CopyBlock(
            ref bufferRef,
            ref GetArrayDataReference(indexNameUtf8),
            (uint)indexNameUtf8.Length);
        bufferRef = ref Unsafe.Add(ref bufferRef, indexNameUtf8.Length);

        Unsafe.CopyBlock(
            ref bufferRef,
            ref GetArrayDataReference(keyEncodingIdUtf8),
            (uint)keyEncodingIdUtf8.Length);
        bufferRef = ref Unsafe.Add(ref bufferRef, keyEncodingIdUtf8.Length);

        bufferRef = (byte)(indexOptions.IsUnique ? 1 : 0);
        bufferRef = ref Unsafe.Add(ref bufferRef, 1);

        bufferRef = (byte)indexOptions.ValueKind;
        bufferRef = ref Unsafe.Add(ref bufferRef, 1);

        var payloadPosition = stream.Position + descriptorLength;
        Unsafe.WriteUnaligned(ref bufferRef, payloadPosition);

        await stream.WriteAsync(buffer.AsMemory(0, descriptorLength), cancellationToken);
    }

    PrimaryKeyIndexOptions GetPrimaryKeyIndexOptions()
    {
        return new PrimaryKeyIndexOptions($"{name}_pk")
        {
            KeyEncoding = PrimaryKeyEncoding,
        };
    }
}

public class DatabaseBuilder : IDisposable
{
    public int PageSize { get; set; } = 4096;

    readonly MemoryArena arena = new();
    readonly List<TableBuilder> tableBuilders = [];
    FilterOptions? filterOptions;

    public void AddPageFilter(Action<FilterOptions> configure)
    {
        filterOptions ??= new FilterOptions();
        configure.Invoke(filterOptions);
    }

    public TableBuilder CreateTable(string name)
    {
        return CreateTable(name, AsciiOrdinalEncoding.Instance);
    }

    public TableBuilder CreateTable(string name, IKeyEncoding primaryKeyEncoding)
    {
        var tableBuilder = new TableBuilder(name, primaryKeyEncoding, PageSize, filterOptions?.Filters);
        tableBuilders.Add(tableBuilder);
        return tableBuilder;
    }

    public async ValueTask BuildToFileAsync(string path, CancellationToken cancellationToken = default)
    {
        await using var fs = File.OpenWrite(path);
        await BuildToStreamAsync(fs, cancellationToken);
    }

    public async ValueTask BuildToStreamAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        var header = new Header();

        unsafe
        {
            Header.MagicBytesValue.CopyTo(new Span<byte>(header.MagicBytes, Header.MagicBytesValue.Length));
        }
        header.MajorVersion = 1;
        header.MinorVersion = 0;
        header.PageFilterCount = (ushort)(filterOptions?.Filters.Count ?? 0);
        header.PageSize = PageSize;
        header.TableCount = (ushort)tableBuilders.Count;

        Span<byte> headerBytes = stackalloc byte[Unsafe.SizeOf<Header>()];
        Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(headerBytes), header);
        stream.Write(headerBytes);

        // write filter ids
        if (filterOptions?.Filters is { Count: > 0 } filters)
        {
            Span<byte> filterIdBuffer = stackalloc byte[byte.MaxValue + 1];
            for (var i = 0; i < filters.Count; i++)
            {
                var filter = filters[i];
                var filterIdBytes = Encoding.UTF8.GetByteCount(filter.Id);
                if (filterIdBytes > byte.MaxValue)
                {
                    throw new InvalidOperationException($"Filter ID length must be less than 255: `{filter.Id}` ");
                }
                var bytesWritten = Encoding.UTF8.GetBytes(filter.Id,  filterIdBuffer[1..]);
                filterIdBuffer[0] = (byte)bytesWritten;
                stream.Write(filterIdBuffer[..(bytesWritten + 1)]);
            }
        }

        foreach (var tableBuilder in tableBuilders)
        {
            await tableBuilder.BuildAsync(stream, cancellationToken);
        }
        await stream.FlushAsync(cancellationToken);
    }

    public void Dispose()
    {
        arena.Dispose();
    }
}
