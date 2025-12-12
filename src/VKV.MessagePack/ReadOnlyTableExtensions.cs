using MessagePack;

namespace VKV.MessagePack;

public static class ReadOnlyTableExtensions
{
    public static MessagePackReadOnlyTable<TValue> AsMessagePackSerializable<TValue>(
        this ReadOnlyTable table,
        MessagePackSerializerOptions? options = null)
    {
        return new MessagePackReadOnlyTable<TValue>(table, options);
    }
}