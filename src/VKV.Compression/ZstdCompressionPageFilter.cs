using System;
using NativeCompressions;

namespace VKV.Compression;

public class ZstdCompressionPageFilter : IPageFilter
{
    static ZstdCompressionPageFilter()
    {
        PageFilterRegistry.Register(new ZstdCompressionPageFilter());
    }

    public string Id => "VKV.ZstdCompression";

    public int GetMaxEncodedLength(int decodedLength)
    {
        return Zstandard.GetMaxCompressedLength(decodedLength);
    }

    public int Encode(ReadOnlySpan<byte> input, Span<byte> output)
    {
        return Zstandard.Compress(input, output);
    }

    public int Decode(ReadOnlySpan<byte> input, Span<byte> output)
    {
        return Zstandard.Decompress(input, output);
    }
}