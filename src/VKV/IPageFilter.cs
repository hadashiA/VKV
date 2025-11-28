using System;

namespace VKV;

public interface IPageFilter
{
    string Id { get; }

    int GetMaxEncodedLength(int decodedLength);

    int Encode(ReadOnlySpan<byte> input, Span<byte> output);
    int Decode(ReadOnlySpan<byte> input, Span<byte> output);
}
