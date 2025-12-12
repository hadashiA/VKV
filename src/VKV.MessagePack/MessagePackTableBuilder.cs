using MessagePack;

namespace VKV.MessagePack;

public class MessagePackTableBuilder<TValue>(
    TableBuilder builder,
    MessagePackSerializerOptions? options = null)
{
    public void Append(ReadOnlyMemory<byte> key, TValue value)
    {
        var bytes = MessagePackSerializer.Serialize(value, options);
        builder.Append(key, bytes);
    }

    public void Append<TKey>(TKey key, TValue value)
        where TKey : IComparable<TKey>
    {
        var bytes = MessagePackSerializer.Serialize(value, options);
        builder.Append(key, bytes);
    }
}