# VKV

> [!WARNING]
> This project is work in progress 

VKV is a red-only embedded B+Tree based key/value database, implemented pure C#.

```
| Method             | Mean        | Error     | StdDev    |
|------------------- |------------:|----------:|----------:|
| VKV_FindByKey      |    37.57 us |  0.230 us |  0.120 us |
| CsSqlite_FindByKey | 4,322.48 us | 44.492 us | 26.476 us |
```

## Features

- B+Tree based query
  - Read a value by primary key 
  - Read values by key range
  - Count by key range
  - Secondary index (wip)
- Multiple Tables
- Support async/sync
- Unity Integration
  - AsyncReadManager + `NativeArray<byte>` based optimized custom loader.     
- Page filter
  - Built-in filters
      - Cysharp/NativeCompression based page compression.
  - We can write custom filters in C#.
- Iterator API
  - By manipulating the cursor, large areas can be accessed sequentially.
- TODO
  - Read values by key prefix
  - C# Driect Serialization
  - Multiple table JOINs
  - Cli tools

## Why read-only ?

## Installation

## Usage

```cs
// Create DB

var builder = new DatabaseBuilder
{
     // The smallest unit of data loaded into memory
    PageSize = 4096,
};

// Create table (string key - ascii comparer)
var table1 = builder.CreateTable("items", KeyEncoding.Ascii);
table1.Append("key1", "value1"u8.ToArray()); // value is any `Memory<byte>` 
table1.Append("key2", "value2"u8.ToArray());
table1.Append("key3", "value3"u8.ToArray());
table1.Append("key4", "value4"u8.ToArray());


// Create table (Int64 key)
var table2 = builder.CreateTable("quests", KeyEncoding.Int64LittleEndian);
table2.Append(1, "hoge"u8.ToArray());

// Build
await builder.BuildToFileAsync("/path/to/bin.vkv");
```

```cs
// Open DB
var database = await ReadOnlyDatabase.OpenAsync("/pth/to/bin.vkv", new DatabaseLoadOptions
{
    PageCacheCapacity = 8, // Maximum number of pages to keep in memory
});

var table = database.GetTable("items");

// find by key (string key)
using var result = table.Get("key1");
result.IsExists //=> true
result.Span //=> "value1"u8

// byte sequence key (fatest)
using var result = table.Get("key1"u8);

// find key range
using var range = table.GetRange(
    startKey: "key1"u8, 
    endKey: "key3"u8,
    startKeyExclusive: false,
    endKeyExclusive: false,
    sortOrder: SortOrder.Ascending);
    
range.Count //=> 3

// greater than 
using var range = table.GetRange("key1"u8, KeyRange.Unbound, startKeyExclusive: true);

// greater than or equals
using var range = table.GetRange("key1"u8, KeyRange.Unbound, startKeyExclusive: false);

// less than
using var range = table.GetRange("key1"u8, KeyRange.Unbound, startKeyExclusive: false, sortOrder: SortOrder.Descending);

// count
var count = table.CountRange("key1", "key3", 
    startKeyExclusive: false,
    endKeyExclusive: false);
    
// async
using var value1 = await table.GetAsync("key1"u8);
using var range1 = await table.GetRangeAsync("key1"u8, "key3"u8);
var count = await table.CountRangeAsync();

// secondary index
using var result = table.IndexBy("label").Get("secondary_key"u8);
```

### Unity


```cs
// The page cache will use the unity native allocator.
var database = await ReadOnlyDatabase.OpenFromFileAsync(filePath, new DatabaseLoadOptions
{
    StorageFactory = UnityNativeAllocatorFileStorage.Factory,
});

```






