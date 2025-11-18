// using System;
// using System.Collections;
// using System.Collections.Generic;
// using System.Runtime.CompilerServices;
// using System.Runtime.InteropServices;
// using System.Threading;
// using System.Threading.Tasks;
// using VKV.BTree;
// using VKV.Internal;
//
// namespace VKV;
//
// public struct RangeIterator :
//     IAsyncEnumerable<PageSlice>,
//     IAsyncEnumerator<PageSlice>,
//     IEnumerable<PageSlice>,
//     IEnumerator<PageSlice>
// {
//     public PageSlice Current { get; private set; } = default;
//     PageSlice IEnumerator<PageSlice>.Current => Current;
//     PageSlice IAsyncEnumerator<PageSlice>.Current => Current;
//     object IEnumerator.Current => Current;
//
//     IPageEntry currentPage;
//     int currentIndex;
//     int currentNodeEntryCount;
//     PageCache pageCache;
//
//     public RangeIterator GetAsyncEnumerator(CancellationToken cancellationToken) => this;
//     public RangeIterator GetEnumerator() => this;
//
//     IAsyncEnumerator<PageSlice> IAsyncEnumerable<PageSlice>.GetAsyncEnumerator(CancellationToken cancellationToken) => GetAsyncEnumerator();
//     IEnumerator<PageSlice> IEnumerable<PageSlice>.GetEnumerator() => GetEnumerator();
//     IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
//
//     internal RangeIterator(PageCache pageCache, IPageEntry startPage, int startIndex)
//     {
//         this.pageCache = pageCache;
//         currentPage = startPage;
//         currentIndex = startIndex;
//
//         var header = Unsafe.ReadUnaligned<NodeHeader>(
//             ref MemoryMarshal.GetReference(startPage.Memory.Span));
//         currentNodeEntryCount = header.EntryCount;
//         if (header.Kind != NodeKind.Leaf)
//         {
//             throw new InvalidOperationException("Invalid node kind");
//         }
//     }
//
//     public bool MoveNext()
//     {
//         if (currentIndex >= currentNodeEntryCount)
//         {
//
//         }
//
//         try
//         {
//             var leafNode = new LeafNodeReader(currentPage.Memory.Span, currentNodeEntryCount);
//             leafNode.GetAt(
//                 currentIndex,
//                 out var key,
//                 out var valuePayloadOffset,
//                 out var valueLength,
//                 out var nextIndex);
//
//             if (!nextIndex.HasValue)
//             {
//                 currentIndex = -1;
//                 return false;
//             }
//
//             result.Add(new PageSlice(page, Unsafe.SizeOf<NodeHeader>() + valuePayloadOffset, valueLength));
//
//             // check end key
//             if (endKey.HasValue)
//             {
//                 var compared = keyComparer.Compare(key, endKey.Value.Span);
//                 if (endKeyExclusive)
//                 {
//                     if (compared > 0) return result;
//                 }
//                 else
//                 {
//                     if (compared >= 0) return result;
//                 }
//             }
//
//             if (!nextIndex.HasValue) break;
//             index = nextIndex.Value;
//         }
//
//         // next node
//         if (header.RightSiblingPageNumber.IsEmpty)
//         {
//             return result;
//         }
//
//         pageNumber = header.RightSiblingPageNumber;
//         index = 0;
//     }
//     finally
//     {
//         page.Release();
//     }
//
//     public void Dispose()
//     {
//         throw new NotImplementedException();
//     }
//
//     public ValueTask DisposeAsync()
//     {
//         throw new NotImplementedException();
//     }
//
//     public ValueTask<bool> MoveNextAsync()
//     {
//         throw new NotImplementedException();
//     }
//     }
//
//     public void Reset()
//     {
//         throw new NotImplementedException();
//     }
// }
