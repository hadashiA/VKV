using System;

namespace VKV;

public class KeyEncodingMismatchException(string message) : Exception(message)
{
    public static void ThrowIfCannotEncodeInt64(IKeyEncoding keyEncoding)
    {
        if (!keyEncoding.IsSupportedType(typeof(long)))
        {
            throw new KeyEncodingMismatchException($"Cannot be used key as int64 from {keyEncoding}");
        }
    }

    public static void ThrowIfCannotEncodeString(IKeyEncoding keyEncoding)
    {
        if (!keyEncoding.IsSupportedType(typeof(string)))
        {
            throw new KeyEncodingMismatchException($"Cannot be used key as string from {keyEncoding}");
        }
    }
}