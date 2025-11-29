using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using VKV.Internal;
using VKV.Storages;

namespace VKV;

public delegate IStorage StorageFactory(Stream stream, int pageSize);

public record DatabaseLoadOptions
{
    public static DatabaseLoadOptions Default => new();

    public static readonly StorageFactory DefaultStorageFactory = (stream, pageSize) =>
    {
        if (stream is FileStream fs)
        {
            return new PreadStorage(fs.SafeFileHandle);
        }

        if (stream is MemoryStream ms)
        {
            return new InMemoryStorage(ms.ToArray());
        }

        throw new NotSupportedException($"unsupported stream type: {stream.GetType().Name}");
    };

    public int PageCacheCapacity { get; set; } = 8;
    public StorageFactory StorageFactory { get; set; } = DefaultStorageFactory;
}

public sealed class ReadOnlyDatabase : IDisposable
{
    public static async ValueTask<ReadOnlyDatabase> OpenFileAsync(string path, DatabaseLoadOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= DatabaseLoadOptions.Default;
        var fs = File.OpenRead(path);
        return await OpenAsync(fs, options, cancellationToken);
    }

    public static async ValueTask<ReadOnlyDatabase> OpenAsync(Stream stream, DatabaseLoadOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= DatabaseLoadOptions.Default;
        var catalog = await CatalogParser.ParseAsync(stream, cancellationToken);
        var storage = options.StorageFactory.Invoke(stream, catalog.PageSize);
        return new ReadOnlyDatabase(catalog, storage, options.PageCacheCapacity);
    }

    public Catalog Catalog { get; }
    readonly IStorage storage;
    readonly PageCache pageCache;

    ReadOnlyDatabase(Catalog catalog, IStorage storage, int pageCacheCapacity)
    {
        Catalog = catalog;
        this.storage = storage;
        pageCache = new PageCache(storage, pageCacheCapacity, catalog.Filters?.ToArray() ?? []);
    }

    public void Dispose()
    {
        pageCache.Dispose();
        storage.Dispose();
    }

    public ReadOnlyTable GetTable(string name)
    {
        var descriptor = Catalog.TableDescriptors[name];
        return new ReadOnlyTable(descriptor, pageCache);
    }
}
