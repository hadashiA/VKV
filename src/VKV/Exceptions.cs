using System;

namespace VKV;

public class KeyEncodingMismatchException(string message) : Exception(message)
{
    public static void Throw(Type type, string expectedTypeName) =>
        throw new KeyEncodingMismatchException($"{type} cannot encode as {expectedTypeName}");
}