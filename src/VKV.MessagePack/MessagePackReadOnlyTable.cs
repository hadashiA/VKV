using MessagePack;

namespace VKV.MessagePack;

public class MessagePackReadOnlyTable<TValue>(
    ReadOnlyTable table,
    MessagePackSerializerOptions? options = null)
{
    public TValue? Get(ReadOnlySpan<byte> key)
    {
        using var result = table.Get(key);
        return result.HasValue
            ? MessagePackSerializer.Deserialize<TValue>(result.Value.Memory, options)
            : default;
    }

    public TValue? Get<TKey>(TKey key) where TKey : IComparable<TKey>
    {
        using var result = table.Get(key);
        return result.HasValue
            ? MessagePackSerializer.Deserialize<TValue>(result.Value.Memory, options)
            : default;
    }

    public async ValueTask<TValue> GetAsync(
        ReadOnlyMemory<byte> key,
        CancellationToken cancellationToken = default)
    {
        using var result = await table.GetAsync(key, cancellationToken);
        if (!result.HasValue)
        {
            return default!;
        }
        return MessagePackSerializer.Deserialize<TValue>(result.Value.Memory, options, cancellationToken);
    }

    public async ValueTask<TValue> GetAsync<TKey>(
        TKey key,
        CancellationToken cancellationToken = default)
        where TKey : IComparable<TKey>
    {
        using var result = await table.GetAsync(key, cancellationToken);
        if (!result.HasValue)
        {
            return default!;
        }
        return MessagePackSerializer.Deserialize<TValue>(result.Value.Memory, options, cancellationToken);
    }

    public IReadOnlyList<TValue> GetRange(in QueryRef query)
    {
        using var result = table.GetRange(in query);
        if (result.Count <= 0)
        {
            return [];
        }
        var list = new List<TValue>(result.Count);
        foreach (var value in result)
        {
            var deserializedValue = MessagePackSerializer.Deserialize<TValue>(value);
            list.Add(deserializedValue);
        }
        return list;
    }

    public IReadOnlyList<TValue> GetRangeAsync(in QueryRef query)
    {
        using var result = table.GetRange(in query);
        if (result.Count <= 0)
        {
            return [];
        }
        var list = new List<TValue>(result.Count);
        foreach (var value in result)
        {
            var deserializedValue = MessagePackSerializer.Deserialize<TValue>(value);
            list.Add(deserializedValue);
        }
        return list;
    }
}