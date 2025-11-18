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

namespace VKV;

public abstract class IndexOptions(string name, bool isUnique)
{
    public string Name => name;
    public bool IsUnique => isUnique;

    public KeyEncoding KeyEncoding { get; set; } = KeyEncoding.Ascii;
    public abstract ValueKind ValueKind { get; }
}

public class PrimaryKeyIndexOptions(string name) : IndexOptions(name, true)
{
    public override ValueKind ValueKind => ValueKind.RawData;
}

public class SecondaryIndexOptions(string name, bool isUnique) : IndexOptions(name, isUnique)
{
    public override ValueKind ValueKind => ValueKind.PrimaryKey;
    public required Func<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> ValueToIndexFactory;
}

public class TableBuilder
{
    readonly string name;
    readonly int pageSize;
    public KeyEncoding PrimaryKeyEncoding { get; set; }
    public IReadOnlyList<SecondaryIndexOptions> SecondaryIndexOptions => secondaryIndexOptions;

    readonly List<SecondaryIndexOptions> secondaryIndexOptions = [];
    readonly KeyValueList keyValues;
    readonly IReadOnlyList<IPageFilter> pageFilters;

    internal TableBuilder(
        string name,
        KeyEncoding primaryKeyEncoding,
        int pageSize,
        IReadOnlyList<IPageFilter> pageFilters)
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
        KeyEncoding keyEncoding,
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
        if (key.Length > ushort.MaxValue)
        {
            throw new ArgumentException("key length must be less than 65536", nameof(key));
        }
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentException("value length must be less than 65536", nameof(value));
        }

        keyValues.Add(key, value);
    }

    public void Append(string key, ReadOnlyMemory<byte> value)
    {
        var encoding = PrimaryKeyEncoding switch
        {
            KeyEncoding.Ascii => Encoding.ASCII,
            KeyEncoding.Utf8 => Encoding.UTF8,
            _ => throw new NotSupportedException($"{PrimaryKeyEncoding} is not string")
        };
        Append(encoding.GetBytes(key), value);
    }

    public void Append(long key, ReadOnlyMemory<byte> value)
    {
        if (PrimaryKeyEncoding != KeyEncoding.Int64LittleEndian)
        {
            throw new NotSupportedException($"{PrimaryKeyEncoding} is not Int64LittleEndian");
        }
        var buffer = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(buffer, key);
        Append(buffer, value);
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

        // build primary key index
        await WriteIndexAsync(
            stream,
            GetPrimaryKeyIndexOptions(),
            keyValues,
            cancellationToken);

        // build secondary indices
        foreach (var indexOptions in SecondaryIndexOptions)
        {
            var indexToPrimaryKeyPairs = new KeyValueList(indexOptions.KeyEncoding, indexOptions.IsUnique);
            foreach (var (k, v) in keyValues)
            {
                var index = indexOptions.ValueToIndexFactory.Invoke(v);
                indexToPrimaryKeyPairs.Add(index, k);
            }
            await WriteIndexAsync(stream, indexOptions, indexToPrimaryKeyPairs, cancellationToken);
        }
    }

    async ValueTask WriteIndexAsync(
        Stream stream,
        IndexOptions indexOptions,
        KeyValueList keyValues,
        CancellationToken cancellationToken = default)
    {
        var indexNameUtf8 = Encoding.UTF8.GetBytes(indexOptions.Name);
        var descriptorLength = sizeof(int) + indexNameUtf8.Length + 1 + 1 + 1 + sizeof(long);

        var buffer = ArrayPool<byte>.Shared.Rent(descriptorLength);
        BinaryPrimitives.WriteInt32LittleEndian(buffer, indexNameUtf8.Length);

        var offset = sizeof(int);
        indexNameUtf8.CopyTo(buffer.AsSpan(offset));
        offset += indexNameUtf8.Length;

        buffer[offset++] = (byte)(indexOptions.IsUnique ? 1 : 0);
        buffer[offset++] = (byte)indexOptions.KeyEncoding;
        buffer[offset++] = (byte)indexOptions.ValueKind;

        var payloadPosition = stream.Position + descriptorLength;
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(offset), payloadPosition);

        await stream.WriteAsync(buffer.AsMemory(0, descriptorLength), cancellationToken);

        var rootPosition = await TreeBuilder.BuildToAsync(stream, pageSize, keyValues, pageFilters, cancellationToken);
        var lastPosition = stream.Position;

        // write root position
        stream.Seek(payloadPosition - sizeof(long), SeekOrigin.Begin);
        BinaryPrimitives.WriteInt64LittleEndian(buffer, rootPosition.Value);
        await stream.WriteAsync(buffer.AsMemory(0, sizeof(long)), cancellationToken);

        // seek to last
        stream.Seek(lastPosition, SeekOrigin.Begin);
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

    /// <summary>
    ///  If true, the encoded state is preserved even in page cache memory.
    /// </summary>
    public bool PageCacheEncodeEnabled { get; set; } = false;

    readonly MemoryArena arena = new();
    readonly List<TableBuilder> tableBuilders = [];
    readonly List<IPageFilter>  pageFilters = [];

    public void AddPageFilter(IPageFilter filter)
    {
        pageFilters.Add(filter);
    }

    public TableBuilder CreateTable(string name, KeyEncoding primaryKeyEncoding = KeyEncoding.Ascii)
    {
        var tableBuilder = new TableBuilder(name, primaryKeyEncoding, PageSize, pageFilters);
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
        header.PageSize = PageSize;
        header.TableCount = tableBuilders.Count;

        Span<byte> headerBytes = stackalloc byte[Unsafe.SizeOf<Header>()];
        Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(headerBytes), header);
        stream.Write(headerBytes);

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
