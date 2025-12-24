using System;

namespace VKV;

public class KeyEncodingMismatchException(string message) : Exception(message)
{
    public static void ThrowCannotEncodeInt64(Type type) =>
        throw new KeyEncodingMismatchException($"{type} cannot encode int64");

    public static void ThrowCannotEncodeString(Type type) =>
        throw new KeyEncodingMismatchException($"{type} cannot encode string");

    public static void ThrowIfCannotEncodeInt64(IKeyEncoding keyEncoding)
    {
        // if (!keyEncoding.IsSupportedType(typeof(long)))
        if (keyEncoding is not Int64LittleEndianEncoding)
        {
            throw new KeyEncodingMismatchException($"Cannot be used key as int64 from {keyEncoding}");
        }
    }
}