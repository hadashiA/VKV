using System;

namespace VKV.UlidKey;

public class UlidKeyEncoding : IKeyEncoding
{
    static UlidKeyEncoding()
    {
        KeyEncoding.Register(Instance);
    }

    public static readonly UlidKeyEncoding Instance = new();

    public string Id => "ulid";

    public int Compare(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
    {
        return Compare(x.Span, y.Span);
    }

    public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        var aId = new Ulid(a);
        var bId = new Ulid(b);
        return aId.CompareTo(bId);
    }

    public int GetMaxEncodedByteCount<TKey>(TKey key)
        where TKey : IComparable<TKey> => 16;

    public bool TryEncode<TKey>(TKey key, Span<byte> destination, out int bytesWritten) where TKey : IComparable<TKey>
    {
        if (key is Ulid ulid)
        {
            bytesWritten = 16;
            return ulid.TryWriteBytes(destination);
        }

        KeyEncodingMismatchException.Throw(typeof(TKey), "UlidKeyEncoding");
        bytesWritten = default;
        return false;
    }

    public bool TryEncode(string formattedString, Span<byte> destination, out int bytesWritten)
    {
        if (Ulid.TryParse(formattedString, out var ulid))
        {
            bytesWritten = 16;
            return ulid.TryWriteBytes(destination);
        }
        throw new KeyEncodingMismatchException($"Cannot parse Ulid: {formattedString}");
    }
}