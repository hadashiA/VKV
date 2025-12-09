using MessagePack;

namespace VKV.MessagePack;

public static class KeyValueStoreExtensions
{
    public static T? Get<T>(this IKeyValueStore kv, ReadOnlySpan<byte> key)
    {
        using var result = kv.Get(key);
        if (!result.HasValue)
        {
            return default;
        }
        return MessagePackSerializer.Deserialize<T>(result.Value.Memory);
    }

    public static async ValueTask<T?> GetAsync<T>(
        this IKeyValueStore kv,
        ReadOnlyMemory<byte> key,
        CancellationToken cancellationToken = default)
    {
        using var result = await kv.GetAsync(key, cancellationToken);
        if (!result.HasValue)
        {
            return default;
        }
        return MessagePackSerializer.Deserialize<T>(result.Value.Memory, cancellationToken: cancellationToken);
    }

    public static IReadOnlyList<T> GetRange<T>(
        this IKeyValueStore kv,
        )
    {
        using var result = kv.GetRange(query);
        if (result.Count <= 0)
        {
            return [];
        }
        var list = new List<T>(result.Count);
        foreach (var value in result)
        {
            var deserializedValue = MessagePackSerializer.Deserialize<T>(value);
            list.Add(deserializedValue);
        }
        return list;
    }
}