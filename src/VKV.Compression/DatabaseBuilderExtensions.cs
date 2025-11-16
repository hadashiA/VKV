namespace VKV.Compression;

public static class DatabaseBuilderExtensions
{
    public static void AddZstandardCompression(this DatabaseBuilder builder)
    {
        builder.AddPageFilter(new ZstdCompressionPageFilter());
    }
}