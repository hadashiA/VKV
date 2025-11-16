using System;
using System.Collections.Generic;
using System.Text;

namespace VKV;

public enum ValueKind : byte
{
    RawData,
    PrimaryKey,
    BlockRef,
}

public enum KeyEncoding : byte
{
    Ascii,
    Utf8,
    Int64LittleEndian,
}

public static class KeyEncodingExtensions
{
    public static Encoding ToTextEncoding(this KeyEncoding keyEncoding)
    {
        return keyEncoding == KeyEncoding.Utf8 ? Encoding.UTF8 : Encoding.ASCII;
    }

    public static IKeyComparer ToKeyComparer(this KeyEncoding keyEncoding)
    {
        return keyEncoding switch
        {
            KeyEncoding.Ascii => AsciiOrdinalComparer.Instance,
            KeyEncoding.Utf8 => Utf8OrdinalComparer.Instance,
            KeyEncoding.Int64LittleEndian => Int64LittleEndianComparer.Instance,
            _ => throw new ArgumentOutOfRangeException(nameof(keyEncoding), keyEncoding, null)
        };

    }
}

public class IndexDescriptor
{
    public required string Name { get; init; }
    public required bool IsUnique { get; init; }
    public required KeyEncoding KeyEncoding { get; init; }
    public required ValueKind ValueKind { get; init; }
    public required PageNumber RootPageNumber { get; init; }
}

public class TableDescriptor(
    string name,
    IndexDescriptor primaryKeyDescriptor,
    IReadOnlyList<IndexDescriptor> indexDescriptors)
{
    public string Name => name;
    public IndexDescriptor PrimaryKeyDescriptor => primaryKeyDescriptor;
    public IReadOnlyList<IndexDescriptor> IndexDescriptors => indexDescriptors;
}

public class Catalog(
    int pageSize,
    IReadOnlyDictionary<string, TableDescriptor> tableDescriptors)
{
    public int PageSize => pageSize;
    public IReadOnlyDictionary<string, TableDescriptor> TableDescriptors => tableDescriptors;
}
