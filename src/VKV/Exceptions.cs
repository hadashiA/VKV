using System;

namespace VKV;

public class KeyEncodingMismatchException(string message) : Exception(message)
{
    public static void ThrowIfCannotEncodeInt64(KeyEncoding keyEncoding)
    {
        if (keyEncoding != KeyEncoding.Int64LittleEndian)
        {
            throw new KeyEncodingMismatchException($"Cannot be used key as int64 from {keyEncoding}");
        }
    }

    public static void ThrowIfCannotEncodeString(KeyEncoding keyEncoding)
    {
        if (keyEncoding == KeyEncoding.Int64LittleEndian)
        {
            throw new KeyEncodingMismatchException($"Cannot be used key as string from {keyEncoding}");
        }
    }
}