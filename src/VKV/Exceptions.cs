using System;

namespace VKV;

public class KeyEncodingMismatchException(string message) : Exception(message)
{
    public static void ThrowIfCannotEncodeInt64(IKeyEncoding keyEncoding)
    {
        // if (!keyEncoding.IsSupportedType(typeof(long)))
        if (keyEncoding is not Int64LittleEndianEncoding)
        {
            throw new KeyEncodingMismatchException($"Cannot be used key as int64 from {keyEncoding}");
        }
    }

    public static void ThrowIfCannotEncodeString(IKeyEncoding keyEncoding)
    {
        // if (!keyEncoding.IsSupportedType(typeof(string)))
        if (keyEncoding is not (AsciiOrdinalEncoding or Utf8OrdinalEncoding))
        {
            throw new KeyEncodingMismatchException($"Cannot be used key as string from {keyEncoding}");
        }
    }
}